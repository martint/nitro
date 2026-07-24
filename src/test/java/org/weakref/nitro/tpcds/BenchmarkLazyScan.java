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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.jit.Column;
import org.weakref.nitro.jit.PipelineCompiler;
import org.weakref.nitro.jit.Plan;
import org.weakref.nitro.jit.QueryLowering;
import org.weakref.nitro.jit.StreamingPipeline;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Measures selection-driven lazy materialization on a selective single-table scan:
 * {@code SELECT sum(of four price measures) FROM store_sales WHERE ss_quantity < threshold}, streamed two ways on
 * sf10 Parquet. {@link #eagerScan} converts every column for every row up front (the old behavior);
 * {@link #lazyScan} converts the four payload measures only for the rows that pass the {@code ss_quantity} filter.
 * The {@code threshold} param sweeps selectivity (lower = fewer survivors = bigger payload-conversion saving).
 * Both scan Parquet per invocation; Java compilation is one-time in {@link #setup}.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLazyScan
{
    private static final String TABLE = "store_sales";
    private static final Column[][] NO_BUILDS = new Column[0][];
    private static final int[] NO_BUILD_ROWS = new int[0];

    @Param({"5", "30", "90"})
    public int threshold;

    private Allocator allocator;
    private TpcdsParquetTables tables;
    private QueryLowering.Lowered lowered;
    private StreamingPipeline streaming;

    @Setup
    public void setup()
    {
        tables = TpcdsParquetTables.requiredActual();
        QueryLowering query = QueryLowering.scan(TABLE,
                new QueryLowering.Column("ss_quantity", org.weakref.nitro.jit.ColumnEncoding.FLAT, true),
                new QueryLowering.Column("ss_wholesale_cost", org.weakref.nitro.jit.ColumnEncoding.FLAT, true),
                new QueryLowering.Column("ss_list_price", org.weakref.nitro.jit.ColumnEncoding.FLAT, true),
                new QueryLowering.Column("ss_sales_price", org.weakref.nitro.jit.ColumnEncoding.FLAT, true),
                new QueryLowering.Column("ss_ext_sales_price", org.weakref.nitro.jit.ColumnEncoding.FLAT, true));
        query.where(new Plan.Predicate("<", query.column("ss_quantity"), new Plan.Lit(threshold)))
                .aggregate("sum", "ss_wholesale_cost")
                .aggregate("sum", "ss_list_price")
                .aggregate("sum", "ss_sales_price")
                .aggregate("sum", "ss_ext_sales_price");
        lowered = query.lower();
        streaming = PipelineCompiler.compileStreaming(lowered.pipeline(), lowered.encodings(), lowered.nullable());
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator(EngineResources.createDefault());
    }

    private List<QueryLowering.Column> probeColumns()
    {
        return lowered.inputs().get(0).columns();
    }

    @Benchmark
    public Object eagerScan()
    {
        return streaming.execute(
                CompiledQuerySupport.parquetFlatSource(allocator, tables, TABLE, probeColumns()),
                NO_BUILDS, NO_BUILD_ROWS);
    }

    @Benchmark
    public Object lazyScan()
    {
        // Staged: ss_quantity (the filter column) materializes; the four price measures materialize lazily, for the
        // surviving rows only.
        return streaming.execute(
                CompiledQuerySupport.parquetLazySource(allocator, tables, TABLE, probeColumns(), lowered.pipeline()),
                NO_BUILDS, NO_BUILD_ROWS);
    }
}
