/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.weakref.nitro.tpcds;

import io.trino.testing.MaterializedResult;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.legacy.pipeline.CompiledPipeline;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.trino.TrinoTpcdsParquetSupport;

import java.util.concurrent.TimeUnit;

/**
 * End-to-end, apples-to-apples comparison of the data-centric compiler prototype against Trino on a real
 * TPC-DS star-schema query over real sf10 Parquet:
 * {@code store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk GROUP BY ss_item_sk, sum(ss_quantity)}.
 * <p>
 * All three engines read the same Parquet and run the same logical operator sequence (scan -> hash join ->
 * hash aggregation); there is no filter, so the work is identical. Each measurement is end-to-end including
 * the Parquet scan: the Trino operator tree, the interpreted Nitro operator tree, and the Nitro compiled
 * pipeline (which materializes the scanned columns into arrays -- its scan step -- then runs the fused loop).
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 2000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 2000, timeUnit = TimeUnit.MILLISECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCompiledVsTrino
{
    private TpcdsParquetTables tables;
    private TrinoTpcdsParquetSupport trino;
    private CompiledPipeline compiled;
    private Allocator allocator;

    @Setup
    public void setup()
    {
        tables = TpcdsParquetTables.requiredActual();
        trino = new TrinoTpcdsParquetSupport();
        compiled = CompiledQuerySupport.compile();
    }

    @TearDown
    public void tearDown()
    {
        trino.close();
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator(EngineResources.createDefault());
    }

    @Benchmark
    public long trino()
    {
        MaterializedResult result = trino.storeSalesQuantityByItem(tables);
        return result.getRowCount();
    }

    @Benchmark
    public long nitroInterpreted()
    {
        long checksum = 0;
        try (Operator aggregation = CompiledQuerySupport.interpretedFromParquet(allocator, tables)) {
            while (aggregation.hasNext()) {
                try (Batch batch = aggregation.next()) {
                    Mask mask = batch.borrowMask();
                    I64Vector sums = (I64Vector) batch.output(1).borrow(Stream.VALUES);
                    for (int index = 0; index < mask.count(); index++) {
                        checksum += sums.values()[mask.position(index)];
                    }
                }
            }
        }
        return checksum;
    }

    @Benchmark
    public long nitroCompiled()
    {
        CompiledQuerySupport.Loaded data = CompiledQuerySupport.loadUnfiltered(allocator, tables);
        CompiledPipeline.Result result = CompiledQuerySupport.runCompiled(compiled, data);
        long checksum = 0;
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            checksum += sums[g];
        }
        return checksum;
    }

    public static void main(String[] args)
            throws Exception
    {
        org.weakref.nitro.Benchmarks.benchmark(BenchmarkCompiledVsTrino.class).run();
    }
}
