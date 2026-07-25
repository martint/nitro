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
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.legacy.pipeline.CompiledPipeline;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;

import java.util.concurrent.TimeUnit;

/**
 * Compiled vs interpreted on a real TPC-DS star-schema query over real sf10 Parquet:
 * {@code store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk WHERE d_year = 2001 GROUP BY ss_item_sk,
 * sum(ss_quantity)}. Both engines start from identical in-memory column arrays loaded once from Parquet (see
 * {@link CompiledQuerySupport}); the measured difference is the join + group compute on real data
 * distributions, not the scan. The interpreted side is the same logical operator sequence
 * (TableOperator -> HashJoin -> GroupedAggregation) the compiled routine fuses.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 4, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCompiledQuery
{
    private CompiledQuerySupport.Loaded data;
    private CompiledPipeline compiled;
    private Allocator allocator;

    @Setup
    public void setup()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual();
        data = CompiledQuerySupport.load(new Allocator(EngineResources.createDefault()), tables);
        compiled = CompiledQuerySupport.compile();

        allocator = new Allocator(EngineResources.createDefault());
        if (jitCompiled() != interpreted()) {
            throw new IllegalStateException("mismatch: jit=" + jitCompiled() + " interpreted=" + interpreted());
        }
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator(EngineResources.createDefault());
    }

    @Benchmark
    public long jitCompiled()
    {
        CompiledPipeline.Result result = CompiledQuerySupport.runCompiled(compiled, data);
        long checksum = 0;
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            checksum += sums[g];
        }
        return checksum;
    }

    @Benchmark
    public long interpreted()
    {
        long checksum = 0;
        try (Operator aggregation = CompiledQuerySupport.interpreted(allocator, data)) {
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

    public static void main(String[] args)
            throws Exception
    {
        org.weakref.nitro.Benchmarks.benchmark(BenchmarkCompiledQuery.class).run();
    }
}
