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
     * The same join + group shape as {@link #load} but without the year predicate, so the dimension is the
     * full {@code date_dim}. Used for the end-to-end three-way comparison (Nitro compiled vs Nitro interpreted
     * vs Trino) where all three read Parquet and do identical work, with no filter anywhere.
     */
    public static Loaded loadUnfiltered(Allocator allocator, TpcdsParquetTables tables)
    {
        long[][] sales = drain(scan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_quantity"));
        long[][] dates = drain(scan(allocator, tables, "date_dim", "d_date_sk"));
        return new Loaded(sales[0], sales[1], sales[2], dates[0]);
    }

    /** Interpreted operator tree reading Parquet directly (scan -> HashJoin -> GroupedAggregation), no filter. */
    public static Operator interpretedFromParquet(Allocator allocator, TpcdsParquetTables tables)
    {
        Operator sales = scan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_quantity");
        Operator dates = scan(allocator, tables, "date_dim", "d_date_sk");
        Operator joined = new HashJoinOperator(allocator, sales, 0, dates, 0);
        return new GroupedAggregationOperator(allocator, List.of(1), List.of(new Sum(2)), joined);
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

    /** Loaded inputs plus the compiled result, so callers can reconstruct string columns from the dictionaries. */
    public record LoweredResult(CompiledPipeline.Result result, org.weakref.nitro.jit.Column[][] inputs) {}

    /** A lowered query's inputs loaded from Parquet: one {@link org.weakref.nitro.jit.Column}[] per relation, with row counts. */
    public record LoadedInputs(org.weakref.nitro.jit.Column[][] inputs, int[] rowCounts) {}

    /** Load a lowered query's inputs from Parquet (flat and dictionary-string columns, with null masks). */
    public static LoadedInputs loadLoweredInputs(Allocator allocator, TpcdsParquetTables tables, org.weakref.nitro.jit.QueryLowering.Lowered lowered)
    {
        List<org.weakref.nitro.jit.QueryLowering.Input> sources = lowered.inputs();
        org.weakref.nitro.jit.Column[][] inputs = new org.weakref.nitro.jit.Column[sources.size()][];
        int[] rowCounts = new int[sources.size()];
        for (int s = 0; s < sources.size(); s++) {
            org.weakref.nitro.jit.QueryLowering.Input source = sources.get(s);
            String[] names = source.columns().stream().map(org.weakref.nitro.jit.QueryLowering.Column::name).toArray(String[]::new);
            DrainedInput loaded = drainColumns(scan(allocator, tables, source.table(), names), source.columns());
            inputs[s] = loaded.columns;
            rowCounts[s] = loaded.rows;
        }
        return new LoadedInputs(inputs, rowCounts);
    }

    /** Load a lowered query's inputs from Parquet and run it. */
    public static LoweredResult runLowered(Allocator allocator, TpcdsParquetTables tables, org.weakref.nitro.jit.QueryLowering.Lowered lowered)
    {
        LoadedInputs loaded = loadLoweredInputs(allocator, tables, lowered);
        return new LoweredResult(lowered.compile().execute(loaded.inputs(), loaded.rowCounts()), loaded.inputs());
    }

    private record DrainedInput(org.weakref.nitro.jit.Column[] columns, int rows) {}

    /** Drain a scan into one {@link org.weakref.nitro.jit.Column} per spec, preserving nulls; string columns build a dictionary. */
    private static DrainedInput drainColumns(Operator operator, List<org.weakref.nitro.jit.QueryLowering.Column> specs)
    {
        int width = specs.size();
        long[][] values = new long[width][16];          // flat values
        int[][] ids = new int[width][16];               // string ids
        boolean[][] nulls = new boolean[width][16];
        List<java.util.Map<String, Integer>> dictionaryIndex = new ArrayList<>();
        List<List<byte[]>> dictionaries = new ArrayList<>();
        for (int c = 0; c < width; c++) {
            dictionaryIndex.add(new java.util.LinkedHashMap<>());
            dictionaries.add(new ArrayList<>());
        }
        int size = 0;
        try (operator) {
            while (operator.hasNext()) {
                try (Batch batch = operator.next()) {
                    Mask mask = batch.borrowMask();
                    int count = mask.count();
                    if (size + count > nulls[0].length) {
                        int capacity = Math.max(nulls[0].length * 2, size + count);
                        for (int c = 0; c < width; c++) {
                            values[c] = java.util.Arrays.copyOf(values[c], capacity);
                            ids[c] = java.util.Arrays.copyOf(ids[c], capacity);
                            nulls[c] = java.util.Arrays.copyOf(nulls[c], capacity);
                        }
                    }
                    for (int c = 0; c < width; c++) {
                        boolean string = specs.get(c).encoding() == org.weakref.nitro.jit.ColumnEncoding.STRING;
                        Vector valueVector = batch.output(c).borrow(Stream.VALUES);
                        Vector nullVector = batch.output(c).borrowOrNull(Stream.NULLS);
                        for (int index = 0; index < count; index++) {
                            int position = mask.position(index);
                            boolean isNull = isNull(nullVector, position);
                            int slot = size + index;
                            nulls[c][slot] = isNull;
                            if (string) {
                                ids[c][slot] = isNull ? 0 : intern(dictionaryIndex.get(c), dictionaries.get(c), stringBytes(valueVector, position));
                            }
                            else {
                                values[c][slot] = isNull ? 0 : longValue(valueVector, position);
                            }
                        }
                    }
                    size += count;
                }
            }
        }
        org.weakref.nitro.jit.Column[] columns = new org.weakref.nitro.jit.Column[width];
        for (int c = 0; c < width; c++) {
            boolean nullable = specs.get(c).nullable();
            boolean[] nullMask = nullable ? java.util.Arrays.copyOf(nulls[c], size) : null;
            if (specs.get(c).encoding() == org.weakref.nitro.jit.ColumnEncoding.STRING) {
                columns[c] = orderedStringColumn(ids[c], dictionaries.get(c), size, nullMask);
            }
            else {
                columns[c] = new org.weakref.nitro.jit.Column.FlatColumn(java.util.Arrays.copyOf(values[c], size), nullMask);
            }
        }
        return new DrainedInput(columns, size);
    }

    /**
     * Build a string column with an <em>ordered</em> dictionary: entries sorted by unsigned bytes (UTF-8 byte
     * order = code-point order) and ids remapped to that order. With a lexicographically-ordered dictionary, the
     * engine's id comparison is value comparison, so ORDER BY on the string is correct while the sort stays a fast
     * integer compare. This is the encoding contract {@code Types.STRING} relies on.
     */
    private static org.weakref.nitro.jit.Column.StringColumn orderedStringColumn(int[] ids, List<byte[]> dictionary, int size, boolean[] nullMask)
    {
        int entries = dictionary.size();
        if (entries == 0) {
            return new org.weakref.nitro.jit.Column.StringColumn(java.util.Arrays.copyOf(ids, size), new byte[0][], nullMask);
        }
        Integer[] order = new Integer[entries];
        for (int i = 0; i < entries; i++) {
            order[i] = i;
        }
        java.util.Arrays.sort(order, (a, b) -> java.util.Arrays.compareUnsigned(dictionary.get(a), dictionary.get(b)));
        int[] remap = new int[entries];        // old id -> position in sorted order
        byte[][] sorted = new byte[entries][];
        for (int newId = 0; newId < entries; newId++) {
            int oldId = order[newId];
            remap[oldId] = newId;
            sorted[newId] = dictionary.get(oldId);
        }
        int[] remapped = new int[size];
        for (int r = 0; r < size; r++) {
            remapped[r] = remap[ids[r]];
        }
        return new org.weakref.nitro.jit.Column.StringColumn(remapped, sorted, nullMask);
    }

    /** Bytes of a string value at {@code position}, whether the scan returned it flat or dictionary-encoded. */
    private static byte[] stringBytes(Vector vector, int position)
    {
        if (vector instanceof org.weakref.nitro.data.BinaryVector binary) {
            return binary.copyBytes(position);
        }
        if (vector instanceof org.weakref.nitro.data.DictionaryVector dictionary) {
            return stringBytes(dictionary.values(), dictionary.ids()[position]);
        }
        throw new IllegalArgumentException("Unsupported string vector: " + vector.getClass().getName());
    }

    private static int intern(java.util.Map<String, Integer> index, List<byte[]> dictionary, byte[] bytes)
    {
        return index.computeIfAbsent(new String(bytes, java.nio.charset.StandardCharsets.UTF_8), key -> {
            dictionary.add(bytes);
            return dictionary.size() - 1;
        });
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
