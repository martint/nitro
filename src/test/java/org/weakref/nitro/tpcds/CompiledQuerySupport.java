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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.jit.CompiledPipeline;
import org.weakref.nitro.jit.PipelineCompiler;
import org.weakref.nitro.jit.Plan;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.MultiStageOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.TrinoParquetScanOperator;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.List;

/**
 * Wires the data-centric compiler prototype into a real TPC-DS query over real Parquet, so the compiled
 * pipeline can be compared apples-to-apples against the interpreted operator tree on actual data.
 * <p>
 * The query is the canonical star-schema shape:
 * <pre>{@code
 * SELECT ss_item_sk, sum(ss_quantity)
 * FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
 * WHERE d_year = 2001
 * GROUP BY ss_item_sk
 * }</pre>
 * The {@code d_year = 2001} predicate is applied as a scan-time pushdown when the dimension is loaded, so both
 * engines see the same filtered {@code date_dim} surrogate keys and the comparison isolates the join + group
 * compute. All columns are {@code long}-typed surrogate keys and integer measures, which is exactly what the
 * prototype IR expresses; reading them into column arrays is the compiled engine's scan step.
 */
public final class CompiledQuerySupport
{
    private static final int CHUNK = 4096;
    private static final long FILTER_YEAR = 2001;

    private CompiledQuerySupport() {}

    /** Loaded, null-free column arrays for both sides of the join. */
    public record Loaded(long[] soldDateSk, long[] itemSk, long[] quantity, long[] dateSk)
    {
        public int storeSalesRows()
        {
            return soldDateSk.length;
        }

        public int dateDimRows()
        {
            return dateSk.length;
        }
    }

    public static Loaded load(Allocator allocator, TpcdsParquetTables tables)
    {
        long[][] sales = drain(scan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_quantity"));

        // date_dim with the d_year = 2001 predicate pushed into the load; keep only the surrogate key.
        long[][] dates = drain(scan(allocator, tables, "date_dim", "d_date_sk", "d_year"));
        long[] dateSk = new long[dates[0].length];
        int kept = 0;
        for (int r = 0; r < dates[0].length; r++) {
            if (dates[1][r] == FILTER_YEAR) {
                dateSk[kept++] = dates[0][r];
            }
        }
        dateSk = java.util.Arrays.copyOf(dateSk, kept);

        return new Loaded(sales[0], sales[1], sales[2], dateSk);
    }

    /**
     * The compiled-pipeline plan: probe store_sales [0=ss_sold_date_sk, 1=ss_item_sk, 2=ss_quantity], inner
     * join date_dim [3=d_date_sk] on column 0, group by ss_item_sk, sum ss_quantity.
     */
    public static Plan.Pipeline plan()
    {
        return new Plan.Pipeline(
                3,
                new Plan.Build(1, 0),
                0,
                List.of(),
                List.of(new Plan.Col(1)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))));
    }

    public static CompiledPipeline compile()
    {
        return PipelineCompiler.compile(plan());
    }

    public static CompiledPipeline.Result runCompiled(CompiledPipeline compiled, Loaded data)
    {
        return compiled.execute(
                new long[][][] {
                        {data.soldDateSk(), data.itemSk(), data.quantity()},
                        {data.dateSk()}},
                new int[] {data.storeSalesRows(), data.dateDimRows()});
    }

    /** Interpreted operator tree for the same query: TableOperator -> HashJoin -> GroupedAggregation. */
    public static Operator interpreted(Allocator allocator, Loaded data)
    {
        Operator sales = new TableOperator(3, pages(data.soldDateSk(), data.itemSk(), data.quantity()));
        Operator dates = new TableOperator(1, pages(data.dateSk()));
        Operator joined = new HashJoinOperator(allocator, sales, 0, dates, 0);
        // joined columns: 0=ss_sold_date_sk, 1=ss_item_sk, 2=ss_quantity, 3=d_date_sk
        return new GroupedAggregationOperator(allocator, List.of(1), List.of(new Sum(2)), joined);
    }

    private static Operator scan(Allocator allocator, TpcdsParquetTables tables, String table, String... columns)
    {
        return new MultiStageOperator(
                columns.length,
                tables.tableFiles(table),
                path -> new TrinoParquetScanOperator(allocator, path, List.of(columns)));
    }

    /** Drain a scan into per-column arrays, dropping any row that is null in any selected column. */
    private static long[][] drain(Operator operator)
    {
        int columns = operator.outputCount();
        List<long[]>[] buffers = newBufferList(columns);
        int total = 0;
        try (operator) {
            while (operator.hasNext()) {
                try (Batch batch = operator.next()) {
                    Mask mask = batch.borrowMask();
                    int count = mask.count();
                    Vector[] values = new Vector[columns];
                    Vector[] nulls = new Vector[columns];
                    for (int c = 0; c < columns; c++) {
                        values[c] = batch.output(c).borrow(Stream.VALUES);
                        nulls[c] = batch.output(c).borrowOrNull(Stream.NULLS);
                    }
                    long[][] chunk = new long[columns][count];
                    int written = 0;
                    for (int index = 0; index < count; index++) {
                        int position = mask.position(index);
                        boolean anyNull = false;
                        for (int c = 0; c < columns; c++) {
                            if (isNull(nulls[c], position)) {
                                anyNull = true;
                                break;
                            }
                        }
                        if (anyNull) {
                            continue;
                        }
                        for (int c = 0; c < columns; c++) {
                            chunk[c][written] = longValue(values[c], position);
                        }
                        written++;
                    }
                    if (written > 0) {
                        for (int c = 0; c < columns; c++) {
                            buffers[c].add(java.util.Arrays.copyOf(chunk[c], written));
                        }
                        total += written;
                    }
                }
            }
        }
        long[][] result = new long[columns][total];
        for (int c = 0; c < columns; c++) {
            int offset = 0;
            for (long[] part : buffers[c]) {
                System.arraycopy(part, 0, result[c], offset, part.length);
                offset += part.length;
            }
        }
        return result;
    }

    private static boolean isNull(Vector nulls, int position)
    {
        return nulls instanceof BooleanVector booleans && booleans.values()[position];
    }

    private static long longValue(Vector vector, int position)
    {
        if (vector instanceof I64Vector i64) {
            return i64.values()[position];
        }
        if (vector instanceof I32Vector i32) {
            return i32.values()[position];
        }
        throw new IllegalArgumentException("Unsupported column vector type: " + vector.getClass().getName());
    }

    @SuppressWarnings("unchecked")
    private static List<long[]>[] newBufferList(int columns)
    {
        List<long[]>[] buffers = new List[columns];
        for (int c = 0; c < columns; c++) {
            buffers[c] = new ArrayList<>();
        }
        return buffers;
    }

    private static List<TableOperator.Page> pages(long[]... columns)
    {
        int rows = columns[0].length;
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int offset = 0; offset < rows; offset += CHUNK) {
            int length = Math.min(CHUNK, rows - offset);
            Streams[] streams = new Streams[columns.length];
            for (int c = 0; c < columns.length; c++) {
                long[] slice = new long[length];
                System.arraycopy(columns[c], offset, slice, 0, length);
                streams[c] = Streams.ofValues(new I64Vector(slice));
            }
            pages.add(new TableOperator.Page(length, streams, Mask.all(length)));
        }
        return pages;
    }
}
