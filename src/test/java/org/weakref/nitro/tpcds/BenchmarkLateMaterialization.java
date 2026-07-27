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
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.legacy.pipeline.CompilerResources;
import org.weakref.nitro.legacy.pipeline.PipelineCompiler;
import org.weakref.nitro.legacy.pipeline.Plan;
import org.weakref.nitro.legacy.pipeline.QueryLowering;
import org.weakref.nitro.legacy.pipeline.StreamingPipeline;

import java.util.concurrent.TimeUnit;

/**
 * Measures join-driven late materialization on a selective star query:
 * {@code store_sales JOIN date_dim (d_year = ?) GROUP BY ss_item_sk, sum(five measures)}, streamed two ways on
 * sf10 Parquet. {@link #streamingFlat} converts every probe column for every fact row; {@link #streamingLazy}
 * converts the five measures only for the fact rows that join a surviving date. The {@code year} param sweeps how
 * many fact rows survive the dimension join (the dimension filter is the selection source -- there is no
 * scan-WHERE on the fact). Build dimension and probe fact are scanned from Parquet per invocation.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLateMaterialization
{
    @Param("2001")
    public int year;

    private Allocator allocator;
    private TpcdsParquetTables tables;
    private QueryLowering.Lowered lowered;
    private StreamingPipeline streaming;

    @Setup
    public void setup()
    {
        tables = TpcdsParquetTables.requiredActual();
        QueryLowering query = QueryLowering.scan("store_sales",
                new QueryLowering.Column("ss_sold_date_sk", org.weakref.nitro.legacy.pipeline.ColumnEncoding.FLAT, true),
                new QueryLowering.Column("ss_item_sk", org.weakref.nitro.legacy.pipeline.ColumnEncoding.FLAT, true),
                new QueryLowering.Column("ss_quantity", org.weakref.nitro.legacy.pipeline.ColumnEncoding.FLAT, true),
                new QueryLowering.Column("ss_wholesale_cost", org.weakref.nitro.legacy.pipeline.ColumnEncoding.FLAT, true),
                new QueryLowering.Column("ss_list_price", org.weakref.nitro.legacy.pipeline.ColumnEncoding.FLAT, true),
                new QueryLowering.Column("ss_sales_price", org.weakref.nitro.legacy.pipeline.ColumnEncoding.FLAT, true),
                new QueryLowering.Column("ss_ext_sales_price", org.weakref.nitro.legacy.pipeline.ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"));
        query.where(new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(year)))
                .groupBy("ss_item_sk")
                .aggregate("sum", "ss_quantity")
                .aggregate("sum", "ss_wholesale_cost")
                .aggregate("sum", "ss_list_price")
                .aggregate("sum", "ss_sales_price")
                .aggregate("sum", "ss_ext_sales_price");
        lowered = query.lower();
        streaming = new PipelineCompiler(CompilerResources.createDefault()).compileStreaming(lowered.pipeline(), lowered.encodings(), lowered.nullable());
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator(EngineResources.createDefault());
    }

    @Benchmark
    public Object streamingFlat()
    {
        return CompiledQuerySupport.runStreamingLowered(allocator, tables, lowered, streaming, false);
    }

    @Benchmark
    public Object streamingLazy()
    {
        return CompiledQuerySupport.runStreamingLowered(allocator, tables, lowered, streaming, true);
    }
}
