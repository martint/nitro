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
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.jit.CompiledPipeline;
import org.weakref.nitro.jit.PipelineCompiler;
import org.weakref.nitro.jit.QueryLowering;
import org.weakref.nitro.jit.StreamingPipeline;

import java.util.concurrent.TimeUnit;

/**
 * Measures the streaming win directly: the same compiled aggregation over a wide store_sales scan (group by
 * ss_item_sk, sum of five measures), run two ways on sf10 Parquet -- {@link #eager} materializes every column up
 * front then executes; {@link #streaming} folds the scan batch-by-batch with nothing fully materialized. Both
 * scan Parquet per invocation (no preloading); Java compilation is one-time in {@link #setup}. The gap between
 * them is the materialization cost the Phase 41 split exposed.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkStreamingScan
{
    private static final String TABLE = "store_sales";
    private static final String[] COLUMNS = {"ss_item_sk", "ss_quantity", "ss_wholesale_cost", "ss_list_price", "ss_sales_price", "ss_ext_sales_price"};

    private Allocator allocator;
    private TpcdsParquetTables tables;
    private QueryLowering.Lowered lowered;
    private CompiledPipeline eager;
    private StreamingPipeline streaming;

    @Setup
    public void setup()
    {
        tables = TpcdsParquetTables.requiredActual();
        QueryLowering query = QueryLowering.scan(TABLE,
                new QueryLowering.Column("ss_item_sk"),
                new QueryLowering.Column("ss_quantity"),
                new QueryLowering.Column("ss_wholesale_cost"),
                new QueryLowering.Column("ss_list_price"),
                new QueryLowering.Column("ss_sales_price"),
                new QueryLowering.Column("ss_ext_sales_price"));
        query.groupBy("ss_item_sk")
                .aggregate("sum", "ss_quantity")
                .aggregate("sum", "ss_wholesale_cost")
                .aggregate("sum", "ss_list_price")
                .aggregate("sum", "ss_sales_price")
                .aggregate("sum", "ss_ext_sales_price");
        lowered = query.lower();
        eager = lowered.compile();                                                   // one-time
        streaming = PipelineCompiler.compileStreaming(lowered.pipeline(), lowered.encodings(), lowered.nullable());
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator();
    }

    @Benchmark
    public Object eager()
    {
        CompiledQuerySupport.LoadedInputs inputs = CompiledQuerySupport.loadLoweredInputs(allocator, tables, lowered);
        return eager.execute(inputs.inputs(), inputs.rowCounts());
    }

    @Benchmark
    public Object streaming()
    {
        return streaming.execute(CompiledQuerySupport.parquetFlatSource(allocator, tables, TABLE, COLUMNS));
    }
}
