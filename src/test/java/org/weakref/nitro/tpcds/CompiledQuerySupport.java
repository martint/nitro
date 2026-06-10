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

    public static Loaded load(Allocator allocator, ParquetTables tables)
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
    public static Loaded loadUnfiltered(Allocator allocator, ParquetTables tables)
    {
        long[][] sales = drain(scan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_quantity"));
        long[][] dates = drain(scan(allocator, tables, "date_dim", "d_date_sk"));
        return new Loaded(sales[0], sales[1], sales[2], dates[0]);
    }

    /** Interpreted operator tree reading Parquet directly (scan -> HashJoin -> GroupedAggregation), no filter. */
    public static Operator interpretedFromParquet(Allocator allocator, ParquetTables tables)
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

    private static Operator scan(Allocator allocator, ParquetTables tables, String table, String... columns)
    {
        return new MultiStageOperator(
                columns.length,
                tables.tableFiles(table),
                path -> new TrinoParquetScanOperator(allocator, path, List.of(columns)));
    }

    /**
     * The runtime physical width (64 or 32 bits) of each numeric column of {@code table}, discovered by peeking the
     * first non-empty batch of a scan rather than assumed from the SQL/logical type. Under schema evolution the
     * physical type a file actually carries may differ from what the logical type implies, so the compiled engine
     * must specialize to the discovered profile (or fall back to an adaptation/widening layer), not to a declared
     * type. A non-numeric (string/dictionary) column reports 64 and is handled by its STRING encoding instead. This
     * is the per-file half of the variant profile; encoding (the per-batch half) is discovered separately.
     */
    public static int[] discoverNumericWidths(Allocator allocator, ParquetTables tables, String table, List<String> columns)
    {
        int[] widths = new int[columns.size()];
        java.util.Arrays.fill(widths, 64);
        Operator operator = scan(allocator, tables, table, columns.toArray(new String[0]));
        try (operator) {
            while (operator.hasNext()) {
                try (Batch batch = operator.next()) {
                    if (batch.borrowMask().count() == 0) {
                        continue;
                    }
                    for (int c = 0; c < columns.size(); c++) {
                        widths[c] = batch.output(c).borrow(Stream.VALUES) instanceof I32Vector ? 32 : 64;
                    }
                    return widths;   // physical type is fixed within a file, so the first non-empty batch suffices
                }
            }
        }
        return widths;
    }

    /**
     * Zero-copy streaming {@link org.weakref.nitro.jit.StreamingPipeline.Source}: when a batch is the dense full
     * range ({@code mask.all()}) and a column is a non-null {@code I64} vector, its backing {@code long[]} is
     * wrapped directly with no copy; otherwise the column is copied (widened {@code I32}, gathered sparse mask, or
     * null-zeroed). To stay safe against the scan's pooled/recycled vectors, the current batch is held open while
     * the compiled routine processes it and closed only on the next {@code advance()}.
     */
    public static org.weakref.nitro.jit.StreamingPipeline.Source parquetColumnarSource(Allocator allocator, ParquetTables tables, String table, String... columns)
    {
        int width = columns.length;
        Operator operator = scan(allocator, tables, table, columns);
        return new org.weakref.nitro.jit.StreamingPipeline.Source()
        {
            private Batch open;   // current batch, kept alive until the next advance()
            private org.weakref.nitro.jit.Column[] current;
            private int currentRows;

            @Override
            public boolean advance()
            {
                if (open != null) {
                    open.close();
                    open = null;
                }
                while (operator.hasNext()) {
                    Batch batch = operator.next();
                    Mask mask = batch.borrowMask();
                    int count = mask.count();
                    if (count == 0) {
                        batch.close();
                        continue;
                    }
                    boolean dense = mask.all();
                    org.weakref.nitro.jit.Column[] cols = new org.weakref.nitro.jit.Column[width];
                    for (int c = 0; c < width; c++) {
                        Vector values = batch.output(c).borrow(Stream.VALUES);
                        Vector nulls = batch.output(c).borrowOrNull(Stream.NULLS);
                        if (dense && nulls == null && values instanceof I64Vector i64) {
                            cols[c] = new org.weakref.nitro.jit.Column.FlatColumn(i64.values());   // no copy
                        }
                        else {
                            long[] copy = new long[count];
                            for (int index = 0; index < count; index++) {
                                int position = mask.position(index);
                                copy[index] = nulls != null && isNull(nulls, position) ? 0 : longValue(values, position);
                            }
                            cols[c] = new org.weakref.nitro.jit.Column.FlatColumn(copy);
                        }
                    }
                    current = cols;
                    currentRows = count;
                    open = batch;   // defer close to the next advance()
                    return true;
                }
                operator.close();
                return false;
            }

            @Override
            public int rows()
            {
                return currentRows;
            }

            @Override
            public org.weakref.nitro.jit.Column[] columns()
            {
                return current;
            }
        };
    }

    /**
     * A {@link org.weakref.nitro.jit.StreamingPipeline.Source} over {@code table}'s {@code columns}, scanned from
     * Parquet one batch at a time and exposed as flat {@code long} columns (value-only; the same per-position read
     * the eager loader uses). The compiled streaming routine folds each batch without the whole table ever being
     * materialized.
     */
    public static org.weakref.nitro.jit.StreamingPipeline.Source parquetFlatSource(Allocator allocator, ParquetTables tables, String table, String... columns)
    {
        List<org.weakref.nitro.jit.QueryLowering.Column> specs = new ArrayList<>();
        for (String column : columns) {
            specs.add(new org.weakref.nitro.jit.QueryLowering.Column(column));   // FLAT, non-null
        }
        return parquetFlatSource(allocator, tables, table, specs);
    }

    /**
     * Spec-aware flat streaming source: each batch produces one {@link org.weakref.nitro.jit.Column.FlatColumn}
     * per column, value-only, plus a null mask exactly when the spec is nullable -- mirroring the eager loader so
     * the compiled routine's null handling matches. String probe columns are not supported (they would need a
     * dictionary consistent across batches).
     */
    public static org.weakref.nitro.jit.StreamingPipeline.Source parquetFlatSource(Allocator allocator, ParquetTables tables, String table, List<org.weakref.nitro.jit.QueryLowering.Column> specs)
    {
        int width = specs.size();
        String[] names = specs.stream().map(org.weakref.nitro.jit.QueryLowering.Column::sourceName).toArray(String[]::new);
        Operator operator = scan(allocator, tables, table, names);
        return new org.weakref.nitro.jit.StreamingPipeline.Source()
        {
            private org.weakref.nitro.jit.Column[] current;
            private int currentRows;

            @Override
            public boolean advance()
            {
                while (operator.hasNext()) {
                    try (Batch batch = operator.next()) {
                        Mask mask = batch.borrowMask();
                        int count = mask.count();
                        if (count == 0) {
                            continue;
                        }
                        org.weakref.nitro.jit.Column[] columns = new org.weakref.nitro.jit.Column[width];
                        for (int c = 0; c < width; c++) {
                            if (specs.get(c).encoding() == org.weakref.nitro.jit.ColumnEncoding.STRING) {
                                throw new UnsupportedOperationException("streaming string probe columns not supported: " + specs.get(c).name());
                            }
                            Vector vector = batch.output(c).borrow(Stream.VALUES);
                            Vector nulls = batch.output(c).borrowOrNull(Stream.NULLS);
                            long[] values = new long[count];
                            boolean[] nullMask = specs.get(c).nullable() ? new boolean[count] : null;
                            for (int index = 0; index < count; index++) {
                                int position = mask.position(index);
                                boolean isNull = nulls != null && isNull(nulls, position);
                                if (nullMask != null) {
                                    nullMask[index] = isNull;
                                }
                                values[index] = isNull ? 0 : longValue(vector, position);
                            }
                            columns[c] = new org.weakref.nitro.jit.Column.FlatColumn(values, nullMask);
                        }
                        current = columns;
                        currentRows = count;
                        return true;
                    }
                }
                operator.close();
                return false;
            }

            @Override
            public int rows()
            {
                return currentRows;
            }

            @Override
            public org.weakref.nitro.jit.Column[] columns()
            {
                return current;
            }
        };
    }

    /**
     * A selection-driven lazy {@link org.weakref.nitro.jit.StreamingPipeline.Source}: nothing is converted on
     * {@code advance()}; each {@code materialize(columns, selection, count)} converts only the requested columns,
     * for only the selected rows. Staged filtering calls it once per conjunct (decoding that conjunct's column for
     * the rows that survived the earlier ones) and once for the payload -- so a column is converted only for the
     * rows that reach the stage that needs it. Flat columns only.
     */
    public static org.weakref.nitro.jit.StreamingPipeline.Source parquetLazySource(Allocator allocator, ParquetTables tables,
            String table, List<org.weakref.nitro.jit.QueryLowering.Column> specs, org.weakref.nitro.jit.Plan.Pipeline pipeline)
    {
        int width = specs.size();
        String[] names = specs.stream().map(org.weakref.nitro.jit.QueryLowering.Column::sourceName).toArray(String[]::new);
        // String columns whose ids land in cross-batch state (group keys, aggregate inputs, projected outputs)
        // are interned into a per-query global dictionary; filter-only strings keep cheap page-local dictionaries.
        GlobalStringDictionary[] globalDictionaries = new GlobalStringDictionary[width];
        for (int c = 0; c < width; c++) {
            org.weakref.nitro.jit.QueryLowering.Column spec = specs.get(c);
            if (spec.encoding() == org.weakref.nitro.jit.ColumnEncoding.STRING
                    && PipelineCompiler.stringIdsCrossBatches(pipeline, c)) {
                globalDictionaries[c] = new GlobalStringDictionary(regexpTransform(spec));
            }
            else if (spec.regexpPattern() != null) {
                throw new UnsupportedOperationException("regexp-derived column " + spec.name() + " requires a globally-interned streamed load");
            }
        }
        Operator operator = scan(allocator, tables, table, names);
        return new StringInterningSource()
        {
            private Batch open;   // current batch, kept open so any column can be converted on demand in materialize()
            private Mask mask;
            private int currentRows;
            // Transient per-batch buffers, allocated once and reused across batches. A streamed pipeline consumes each
            // batch fully before the next advance(), so a column's converted buffer (or the gather selection / output
            // wrapper array) is free to back the next batch -- this turns the per-batch widen/gather churn into a
            // bounded number of geometric growths. The zero-copy dense paths still wrap the decoder's array directly
            // and never touch these buffers.
            private final org.weakref.nitro.jit.Column[] out = new org.weakref.nitro.jit.Column[width];
            private final long[][] valueBuffers = new long[width][];
            private final boolean[][] nullBuffers = new boolean[width][];
            private final java.util.IdentityHashMap<Object, byte[][]> pageDictionaries = new java.util.IdentityHashMap<>();
            private int[] identityBuffer = new int[0];

            @Override
            public boolean advance()
            {
                if (open != null) {
                    open.close();
                    open = null;
                }
                while (operator.hasNext()) {
                    Batch batch = operator.next();
                    Mask batchMask = batch.borrowMask();
                    int count = batchMask.count();
                    if (count == 0) {
                        batch.close();
                        continue;
                    }
                    mask = batchMask;
                    currentRows = count;
                    open = batch;
                    return true;
                }
                operator.close();
                return false;
            }

            @Override
            public int rows()
            {
                return currentRows;
            }

            // The exact-width all-columns list for columns(): the identity scratch must not be used here, since
            // materialize(int[]) takes the array length as the column count and the scratch grows past width
            // (the dense string path uses identity(count) over rows).
            private final int[] allColumns = java.util.stream.IntStream.range(0, width).toArray();
            @Override
            public org.weakref.nitro.jit.Column[] columns()
            {
                // Non-staged callers (a pipeline with no deferrable payload) get every column over the whole batch
                // through the dense zero-copy path -- wrapping a non-null I64 column's array by reference -- not the
                // per-row convert gather.
                return materialize(allColumns);
            }

            @Override
            public org.weakref.nitro.jit.Column[] materialize(int[] columns, int[] selection, int count)
            {
                for (int column : columns) {
                    out[column] = convertColumn(open, mask, column, count, selection, specs.get(column).nullable(), column);
                }
                return out;
            }

            @Override
            public org.weakref.nitro.jit.Column[] materialize(int[] columns)
            {
                // Full-batch materialize (the eager join-key/filter phase): when the batch is dense, convert with a
                // type-dispatched bulk path (zero-copy for a non-null I64 column, else one tight loop) instead of the
                // per-row gather, which paid a mask.position indirection and a longValue type-test per row.
                if (!mask.all()) {
                    return materialize(columns, identity(currentRows), currentRows);
                }
                for (int column : columns) {
                    out[column] = denseColumn(open.output(column).borrow(Stream.VALUES), open.output(column).borrowOrNull(Stream.NULLS), currentRows, specs.get(column).nullable(), column);
                }
                return out;
            }

            /** Reusable identity selection [0, count), grown geometrically; valid only until the next call. */
            private int[] identity(int count)
            {
                if (identityBuffer.length < count) {
                    identityBuffer = new int[org.weakref.nitro.jit.StreamingScratch.grow(identityBuffer.length, count)];
                    for (int i = 0; i < identityBuffer.length; i++) {
                        identityBuffer[i] = i;
                    }
                }
                return identityBuffer;
            }

            /** A reusable value buffer for {@code column} holding at least {@code count} longs. */
            private long[] valueBuffer(int column, int count)
            {
                long[] buffer = valueBuffers[column];
                if (buffer == null || buffer.length < count) {
                    buffer = new long[org.weakref.nitro.jit.StreamingScratch.grow(buffer == null ? 0 : buffer.length, count)];
                    valueBuffers[column] = buffer;
                }
                return buffer;
            }

            /** A reusable null buffer for {@code column} holding at least {@code count} booleans. */
            private boolean[] nullBuffer(int column, int count)
            {
                boolean[] buffer = nullBuffers[column];
                if (buffer == null || buffer.length < count) {
                    buffer = new boolean[org.weakref.nitro.jit.StreamingScratch.grow(buffer == null ? 0 : buffer.length, count)];
                    nullBuffers[column] = buffer;
                }
                return buffer;
            }

            /**
             * Dense materialize of {@code column} into reusable per-column buffers, honoring the {@code nullable}
             * contract exactly as {@link #denseColumn}: a non-null I64 column (and a nullable I64 with a
             * {@link BooleanVector} nulls stream) stays zero-copy by wrapping the decoder's array; every other shape
             * widens/null-zeroes into the column's reused value (and, when nullable, null) buffer.
             */
            private org.weakref.nitro.jit.Column denseColumn(Vector values, Vector nulls, int count, boolean nullable, int column)
            {
                if (values instanceof I64Vector i64) {
                    if (nulls == null) {
                        return new org.weakref.nitro.jit.Column.FlatColumn(i64.values());   // no copy
                    }
                    if (nullable && nulls instanceof BooleanVector booleans) {
                        return new org.weakref.nitro.jit.Column.FlatColumn(i64.values(), booleans.values());   // no copy
                    }
                    long[] backing = i64.values();
                    long[] copy = valueBuffer(column, count);
                    boolean[] nullMask = nullable ? nullBuffer(column, count) : null;
                    for (int i = 0; i < count; i++) {
                        boolean isNull = isNull(nulls, i);
                        if (nullMask != null) {
                            nullMask[i] = isNull;
                        }
                        copy[i] = isNull ? 0 : backing[i];
                    }
                    return new org.weakref.nitro.jit.Column.FlatColumn(copy, nullMask);
                }
                if (values instanceof I32Vector i32) {
                    int[] backing = i32.values();
                    long[] copy = valueBuffer(column, count);
                    boolean[] nullMask = nullable ? nullBuffer(column, count) : null;
                    for (int i = 0; i < count; i++) {
                        boolean isNull = nulls != null && isNull(nulls, i);
                        if (nullMask != null) {
                            nullMask[i] = isNull;
                        }
                        copy[i] = isNull ? 0 : backing[i];
                    }
                    return new org.weakref.nitro.jit.Column.FlatColumn(copy, nullMask);
                }
                if (values instanceof org.weakref.nitro.data.BinaryVector || values instanceof org.weakref.nitro.data.DictionaryVector) {
                    return stringColumn(values, nulls, count, identity(count), null, nullable, column);
                }
                throw new IllegalArgumentException("Unsupported column vector type: " + values.getClass().getName());
            }

            /**
             * Gather {@code column} for the first {@code count} positions of {@code selection} into reusable
             * per-column buffers (see {@link #convertColumn}). Dispatches on the vector type once and hoists the
             * null vector's backing array, so the per-row work is tight typed array access -- per-row virtual
             * accessor calls dominated filtered global aggregates over wide facts.
             */
            private org.weakref.nitro.jit.Column convertColumn(Batch batch, Mask batchMask, int c, int count, int[] selection, boolean nullable, int column)
            {
                Vector vector = batch.output(c).borrow(Stream.VALUES);
                Vector nulls = batch.output(c).borrowOrNull(Stream.NULLS);
                long[] values = valueBuffer(column, count);
                boolean[] nullMask = nullable ? nullBuffer(column, count) : null;
                boolean allSelected = batchMask.all();
                boolean[] nullBacking = nulls instanceof BooleanVector booleans ? booleans.values() : null;
                if (vector instanceof I64Vector i64) {
                    long[] backing = i64.values();
                    for (int j = 0; j < count; j++) {
                        int position = allSelected ? selection[j] : batchMask.position(selection[j]);
                        boolean isNull = nullBacking != null && nullBacking[position];
                        if (nullMask != null) {
                            nullMask[j] = isNull;
                        }
                        values[j] = isNull ? 0 : backing[position];
                    }
                }
                else if (vector instanceof I32Vector i32) {
                    int[] backing = i32.values();
                    for (int j = 0; j < count; j++) {
                        int position = allSelected ? selection[j] : batchMask.position(selection[j]);
                        boolean isNull = nullBacking != null && nullBacking[position];
                        if (nullMask != null) {
                            nullMask[j] = isNull;
                        }
                        values[j] = isNull ? 0 : backing[position];
                    }
                }
                else if (vector instanceof org.weakref.nitro.data.BinaryVector || vector instanceof org.weakref.nitro.data.DictionaryVector) {
                    return stringColumn(vector, nulls, count, selection, allSelected ? null : batchMask, nullable, column);
                }
                else {
                    for (int j = 0; j < count; j++) {
                        int position = allSelected ? selection[j] : batchMask.position(selection[j]);
                        boolean isNull = nulls != null && isNull(nulls, position);
                        if (nullMask != null) {
                            nullMask[j] = isNull;
                        }
                        values[j] = isNull ? 0 : longValue(vector, position);
                    }
                }
                return new org.weakref.nitro.jit.Column.FlatColumn(values, nullMask);
            }

            /**
             * A streamed probe's string column for one batch: per-batch dictionary plus per-row ids, the contract
             * the generated string filters (predicate-over-dictionary masks rebuilt per batch) work against. A
             * dictionary-encoded page keeps its dictionary (ids gathered per row); a plain-encoded page interns
             * each selected row's bytes into a batch-local dictionary.
             */
            private org.weakref.nitro.jit.Column stringColumn(Vector vector, Vector nulls, int count, int[] selection, Mask batchMask, boolean nullable, int column)
            {
                int[] ids = new int[count];
                boolean[] nullMask = nullable ? new boolean[count] : null;
                GlobalStringDictionary global = globalDictionaries[column];
                if (global != null) {
                    if (vector instanceof org.weakref.nitro.data.DictionaryVector dictionaryVector
                            && dictionaryVector.values() instanceof org.weakref.nitro.data.BinaryVector entries) {
                        int[] remap = global.remap(entries);
                        int[] vectorIds = dictionaryVector.ids();
                        for (int j = 0; j < count; j++) {
                            int position = batchMask == null ? selection[j] : batchMask.position(selection[j]);
                            boolean isNull = nulls != null && CompiledQuerySupport.isNull(nulls, position);
                            if (nullMask != null) {
                                nullMask[j] = isNull;
                            }
                            ids[j] = isNull ? 0 : remap[vectorIds[position]];
                        }
                    }
                    else {
                        for (int j = 0; j < count; j++) {
                            int position = batchMask == null ? selection[j] : batchMask.position(selection[j]);
                            boolean isNull = nulls != null && CompiledQuerySupport.isNull(nulls, position);
                            if (nullMask != null) {
                                nullMask[j] = isNull;
                            }
                            ids[j] = isNull ? 0 : global.intern(stringBytes(vector, position));
                        }
                    }
                    return new org.weakref.nitro.jit.Column.StringColumn(ids, global.backing(), nullMask, global.size());
                }
                byte[][] dictionary;
                if (vector instanceof org.weakref.nitro.data.DictionaryVector dictionaryVector
                        && dictionaryVector.values() instanceof org.weakref.nitro.data.BinaryVector entries) {
                    // Convert each distinct page dictionary once and hand out the SAME array across the batches
                    // that share it, so the generated per-batch masks can cache by dictionary identity instead of
                    // re-evaluating their predicates every batch.
                    dictionary = pageDictionaries.computeIfAbsent(entries, ignored -> {
                        int dictionarySize = entries.length();
                        byte[][] converted = new byte[dictionarySize][];
                        for (int entry = 0; entry < dictionarySize; entry++) {
                            converted[entry] = stringBytes(entries, entry);
                        }
                        return converted;
                    });
                    int[] vectorIds = dictionaryVector.ids();
                    for (int j = 0; j < count; j++) {
                        int position = batchMask == null ? selection[j] : batchMask.position(selection[j]);
                        boolean isNull = nulls != null && CompiledQuerySupport.isNull(nulls, position);
                        if (nullMask != null) {
                            nullMask[j] = isNull;
                        }
                        ids[j] = isNull ? 0 : vectorIds[position];
                    }
                }
                else {
                    java.util.HashMap<String, Integer> index = new java.util.HashMap<>();
                    List<byte[]> entries = new ArrayList<>();
                    for (int j = 0; j < count; j++) {
                        int position = batchMask == null ? selection[j] : batchMask.position(selection[j]);
                        boolean isNull = nulls != null && CompiledQuerySupport.isNull(nulls, position);
                        if (nullMask != null) {
                            nullMask[j] = isNull;
                        }
                        ids[j] = isNull ? 0 : intern(index, entries, stringBytes(vector, position));
                    }
                    dictionary = entries.toArray(new byte[0][]);
                }
                return new org.weakref.nitro.jit.Column.StringColumn(ids, dictionary, nullMask);
            }

            @Override
            public byte[][] finalDictionary(int column)
            {
                return globalDictionaries[column] == null ? null : globalDictionaries[column].snapshot();
            }
        };
    }

    /**
     * Fill {@code values[offset..offset+count)} from a dense numeric batch column (batch position {@code i} == row
     * {@code i}), dispatching on the vector type once: a non-null I64 column is bulk-copied, otherwise one tight loop
     * fills values and null flags. Avoids the per-row {@code mask.position}/{@code longValue} of the generic gather.
     */
    private static void fillDenseNumeric(Vector valueVector, Vector nullVector, long[] values, boolean[] nulls, int offset, int count)
    {
        if (valueVector instanceof I64Vector i64) {
            long[] backing = i64.values();
            if (nullVector == null) {
                System.arraycopy(backing, 0, values, offset, count);
                return;
            }
            for (int i = 0; i < count; i++) {
                boolean isNull = isNull(nullVector, i);
                nulls[offset + i] = isNull;
                values[offset + i] = isNull ? 0 : backing[i];
            }
            return;
        }
        if (valueVector instanceof I32Vector i32) {
            int[] backing = i32.values();
            for (int i = 0; i < count; i++) {
                boolean isNull = nullVector != null && isNull(nullVector, i);
                nulls[offset + i] = isNull;
                values[offset + i] = isNull ? 0 : backing[i];
            }
            return;
        }
        throw new IllegalArgumentException("Unsupported column vector type: " + valueVector.getClass().getName());
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
    public static LoadedInputs loadLoweredInputs(Allocator allocator, ParquetTables tables, org.weakref.nitro.jit.QueryLowering.Lowered lowered)
    {
        List<org.weakref.nitro.jit.QueryLowering.Input> sources = lowered.inputs();
        org.weakref.nitro.jit.Column[][] inputs = new org.weakref.nitro.jit.Column[sources.size()][];
        int[] rowCounts = new int[sources.size()];
        for (int s = 0; s < sources.size(); s++) {
            org.weakref.nitro.jit.QueryLowering.Input source = sources.get(s);
            String[] names = source.columns().stream().map(org.weakref.nitro.jit.QueryLowering.Column::sourceName).toArray(String[]::new);
            DrainedInput loaded = drainColumns(scan(allocator, tables, source.table(), names), source.columns());
            inputs[s] = loaded.columns;
            rowCounts[s] = loaded.rows;
        }
        return new LoadedInputs(inputs, rowCounts);
    }

    /**
     * Run a lowered star query through a compiled {@link org.weakref.nitro.jit.StreamingPipeline}: the build
     * (dimension) inputs are materialized once into hash tables; the probe (fact) input is streamed from Parquet
     * batch-by-batch (flat columns). Returns the raw result.
     */
    public static CompiledPipeline.Result runStreamingLowered(Allocator allocator, ParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, org.weakref.nitro.jit.StreamingPipeline streaming)
    {
        return runStreamingLowered(allocator, tables, lowered, streaming, false);
    }

    /**
     * As {@link #runStreamingLowered}, but when {@code lazyProbe} is set the probe (fact) is streamed from a
     * selection-driven lazy source -- so join-driven late materialization converts the probe's payload columns only
     * for rows that survive the dimension joins.
     */
    public static CompiledPipeline.Result runStreamingLowered(Allocator allocator, ParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, org.weakref.nitro.jit.StreamingPipeline streaming, boolean lazyProbe)
    {
        return streamCapturingBuilds(allocator, tables, lowered, streaming, lazyProbe).result();
    }

    /** A streamed result together with its materialized build (dimension) inputs and the probe source, so a string output column can be resolved back to its dictionary. */
    public record StreamedResult(CompiledPipeline.Result result, org.weakref.nitro.jit.Column[][] builds, org.weakref.nitro.jit.StreamingPipeline.Source probeSource) {}

    /** A materialized relation (one {@link org.weakref.nitro.jit.Column} per output column, plus its row count) produced by an earlier pipeline stage. */
    public record Materialized(org.weakref.nitro.jit.Column[] columns, int rows) {}

    /**
     * Execute one pipeline stage of a multi-stage operator tree. Inputs named in {@code virtuals} are fed from the
     * given already-materialized relations (an earlier stage's output); every other input is read from Parquet. When
     * the probe (input 0) is a Parquet fact it is streamed batch-by-batch; when the probe is a materialized relation
     * the stage runs eagerly over it. This is the same input substitution {@link #runMultiStage} performs, generalized
     * to any number of materialized inputs, so the harness can wire compiled pipelines into the same tree shape the
     * Trino/Nitro operator trees use (recomputing a shared subtree rather than reusing it).
     */
    public static LoweredResult runStage(Allocator allocator, ParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, java.util.Map<String, Materialized> virtuals)
    {
        List<org.weakref.nitro.jit.QueryLowering.Input> sources = lowered.inputs();
        org.weakref.nitro.jit.QueryLowering.Input probe = sources.get(0);
        boolean probeVirtual = virtuals.containsKey(probe.table());
        // A string-carrying probe drains eagerly by default: the eager load builds ORDERED dictionaries, the
        // contract id-comparing consumers (string ORDER BY, column compares) rely on. A stream-only probe (a
        // fact too large to drain) instead streams through the per-query global intern, whose dictionaries are
        // captured for DictRef reconstruction; its consumers must need id equality only.
        boolean probeString = probe.columns().stream().anyMatch(column -> column.encoding() == org.weakref.nitro.jit.ColumnEncoding.STRING);

        if (probeVirtual || (probeString && !tables.streamOnly(probe.table()))) {
            return runStageEager(allocator, tables, lowered, virtuals);
        }

        org.weakref.nitro.jit.StreamingPipeline streaming =
                PipelineCompiler.compileStreaming(lowered.pipeline(), lowered.encodings(), lowered.nullable());
        int buildCount = sources.size() - 1;
        org.weakref.nitro.jit.Column[][] builds = new org.weakref.nitro.jit.Column[buildCount][];
        int[] buildRowCounts = new int[buildCount];
        org.weakref.nitro.jit.Column[][] inputsForDictRef = new org.weakref.nitro.jit.Column[sources.size()][];
        for (int b = 0; b < buildCount; b++) {
            resolveInput(allocator, tables, sources.get(b + 1), virtuals, builds, buildRowCounts, b);
            inputsForDictRef[b + 1] = builds[b];
        }
        org.weakref.nitro.jit.StreamingPipeline.Source source = parquetLazySource(allocator, tables, probe.table(), probe.columns(), lowered.pipeline());
        CompiledPipeline.Result result = streaming.execute(source, builds, buildRowCounts);
        inputsForDictRef[0] = internedProbeDictionaries(source, probe.columns().size());
        return new LoweredResult(result, inputsForDictRef);
    }

    /**
     * The probe pseudo-input for {@link CompiledTpcdsQueries.DictRef} reconstruction after a streamed run: for each
     * globally-interned string column, a value-less {@link org.weakref.nitro.jit.Column.StringColumn} carrying the
     * final dictionary. Null when the source interned nothing (numeric or filter-only probes).
     */
    private static org.weakref.nitro.jit.Column[] internedProbeDictionaries(org.weakref.nitro.jit.StreamingPipeline.Source source, int width)
    {
        if (!(source instanceof StringInterningSource interning)) {
            return null;
        }
        org.weakref.nitro.jit.Column[] dictionaries = null;
        for (int c = 0; c < width; c++) {
            byte[][] dictionary = interning.finalDictionary(c);
            if (dictionary != null) {
                if (dictionaries == null) {
                    dictionaries = new org.weakref.nitro.jit.Column[width];
                }
                dictionaries[c] = new org.weakref.nitro.jit.Column.StringColumn(new int[0], dictionary);
            }
        }
        return dictionaries;
    }

    /** Resolve input {@code slot} into {@code columns}/{@code rowCounts}[slot]: a materialized virtual relation if named in {@code virtuals}, else a Parquet drain. */
    private static void resolveInput(Allocator allocator, ParquetTables tables, org.weakref.nitro.jit.QueryLowering.Input source,
            java.util.Map<String, Materialized> virtuals, org.weakref.nitro.jit.Column[][] columns, int[] rowCounts, int slot)
    {
        Materialized virtual = virtuals.get(source.table());
        if (virtual != null) {
            columns[slot] = withDerivedColumns(virtual.columns(), source.columns());
            rowCounts[slot] = virtual.rows();
        }
        else {
            String[] names = source.columns().stream().map(org.weakref.nitro.jit.QueryLowering.Column::sourceName).toArray(String[]::new);
            DrainedInput loaded = drainColumns(scan(allocator, tables, source.table(), names), source.columns());
            columns[slot] = loaded.columns;
            rowCounts[slot] = loaded.rows;
        }
    }

    /**
     * Adapt loaded (or materialized virtual) string columns to their specs: a substring spec derives the truncated
     * canonical column; otherwise the spec's {@link org.weakref.nitro.jit.QueryLowering.LoadMode} decides how much
     * dictionary canonicalization the consumer needs -- ORDERED re-canonicalizes (an upstream stage may have
     * materialized an unsorted or duplicate-laden dictionary), DEDUPED dedupes without sorting (id equality for
     * keys), VERBATIM passes through. Identity when nothing needs adapting (the columns are returned as-is).
     */
    private static org.weakref.nitro.jit.Column[] withDerivedColumns(org.weakref.nitro.jit.Column[] columns,
            List<org.weakref.nitro.jit.QueryLowering.Column> specs)
    {
        org.weakref.nitro.jit.Column[] out = columns;
        for (int c = 0; c < specs.size(); c++) {
            org.weakref.nitro.jit.QueryLowering.Column spec = specs.get(c);
            // A nullable-declared column must carry a null mask -- generated code reads it unconditionally -- but a
            // materialized virtual column that never saw a null has none (e.g. a non-null relation consumed through
            // a LEFT join, where only the JOIN introduces nulls). Synthesize the all-false mask.
            if (spec.nullable()) {
                if (columns[c] instanceof org.weakref.nitro.jit.Column.FlatColumn flat && flat.nulls() == null) {
                    if (out == columns) {
                        out = columns.clone();
                    }
                    out[c] = new org.weakref.nitro.jit.Column.FlatColumn(flat.values(), new boolean[flat.values().length]);
                }
                else if (columns[c] instanceof org.weakref.nitro.jit.Column.StringColumn text && text.nulls() == null) {
                    if (out == columns) {
                        out = columns.clone();
                    }
                    out[c] = new org.weakref.nitro.jit.Column.StringColumn(text.ids(), text.dictionary(), new boolean[text.ids().length]);
                }
            }
            if (!(out[c] instanceof org.weakref.nitro.jit.Column.StringColumn source)) {
                continue;
            }
            org.weakref.nitro.jit.Column adapted;
            if (spec.substringLength() >= 0) {
                adapted = substringColumn(source, spec.substringStart(), spec.substringLength());
            }
            else if (spec.upperCase()) {
                adapted = upperColumn(source);
            }
            else if (spec.loadMode() == org.weakref.nitro.jit.QueryLowering.LoadMode.VERBATIM) {
                continue;
            }
            else {
                adapted = canonicalStringColumn(source, spec.loadMode() == org.weakref.nitro.jit.QueryLowering.LoadMode.ORDERED);
            }
            if (out == columns) {
                out = columns.clone();
            }
            out[c] = adapted;
        }
        return out;
    }

    /**
     * Dictionaries known to be duplicate-free (and the subset also known byte-sorted), tracked by identity so a
     * downstream stage's adaptation check is O(1) instead of re-probing a multi-million-entry dictionary on every
     * load. Weak keys: registration must not keep a dictionary alive.
     */
    private static final java.util.Map<byte[][], Boolean> DEDUPED_DICTIONARIES =
            java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());
    private static final java.util.Map<byte[][], Boolean> SORTED_DICTIONARIES =
            java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

    private static void registerCanonical(byte[][] dictionary, boolean sorted)
    {
        DEDUPED_DICTIONARIES.put(dictionary, Boolean.TRUE);
        if (sorted) {
            SORTED_DICTIONARIES.put(dictionary, Boolean.TRUE);
        }
    }

    /**
     * Dedupe (and for {@code sorted}, byte-order) a string column's dictionary, remapping the ids. Identity when the
     * dictionary is already canonical for the requested level, so repeated adaptation is cheap.
     */
    private static org.weakref.nitro.jit.Column.StringColumn canonicalStringColumn(org.weakref.nitro.jit.Column.StringColumn source, boolean sorted)
    {
        byte[][] dictionary = source.dictionary();
        if (dictionary.length == 0) {
            return source;
        }
        if (SORTED_DICTIONARIES.containsKey(dictionary) || (!sorted && DEDUPED_DICTIONARIES.containsKey(dictionary))) {
            return source;
        }
        java.util.TreeMap<byte[], Integer> ordered = new java.util.TreeMap<>(java.util.Arrays::compareUnsigned);
        for (byte[] value : dictionary) {
            ordered.putIfAbsent(value, 0);
        }
        boolean deduped = ordered.size() == dictionary.length;
        if (deduped) {
            boolean alreadySorted = true;
            for (int i = 1; alreadySorted && i < dictionary.length; i++) {
                alreadySorted = java.util.Arrays.compareUnsigned(dictionary[i - 1], dictionary[i]) < 0;
            }
            if (alreadySorted || !sorted) {
                registerCanonical(dictionary, alreadySorted);
                return source;
            }
        }
        int[] remap = new int[dictionary.length];
        byte[][] canonical;
        if (sorted) {
            canonical = new byte[ordered.size()][];
            int next = 0;
            for (java.util.Map.Entry<byte[], Integer> entry : ordered.entrySet()) {
                entry.setValue(next);
                canonical[next] = entry.getKey();
                next++;
            }
            for (int i = 0; i < dictionary.length; i++) {
                remap[i] = ordered.get(dictionary[i]);
            }
        }
        else {
            // Dedupe preserving first-seen order: enough for id equality without paying the sort.
            java.util.Map<byte[], Integer> firstSeen = new java.util.TreeMap<>(java.util.Arrays::compareUnsigned);
            List<byte[]> entries = new ArrayList<>();
            for (int i = 0; i < dictionary.length; i++) {
                Integer id = firstSeen.putIfAbsent(dictionary[i], entries.size());
                if (id == null) {
                    id = entries.size();
                    entries.add(dictionary[i]);
                }
                remap[i] = id;
            }
            canonical = entries.toArray(new byte[0][]);
        }
        int[] sourceIds = source.ids();
        int[] ids = new int[sourceIds.length];
        for (int r = 0; r < ids.length; r++) {
            ids[r] = remap[sourceIds[r]];
        }
        registerCanonical(canonical, sorted);
        return new org.weakref.nitro.jit.Column.StringColumn(ids, canonical, source.nulls());
    }

    private static org.weakref.nitro.jit.Column.StringColumn substringColumn(org.weakref.nitro.jit.Column.StringColumn source, int start, int length)
    {
        return derivedStringColumn(source, value -> utf8Substring(value, start, length));
    }

    private static org.weakref.nitro.jit.Column.StringColumn upperColumn(org.weakref.nitro.jit.Column.StringColumn source)
    {
        return derivedStringColumn(source, value -> org.weakref.nitro.function.scalar.builtin.Utf8Support.upper(value, 0, value.length));
    }

    /**
     * Apply a per-value transform to a string column's dictionary, then dedupe and re-sort the derived values into
     * an ordered dictionary (id order = value order), remapping each source id to its derived value's new id.
     */
    private static org.weakref.nitro.jit.Column.StringColumn derivedStringColumn(org.weakref.nitro.jit.Column.StringColumn source,
            java.util.function.UnaryOperator<byte[]> transform)
    {
        byte[][] dictionary = source.dictionary();
        if (dictionary.length == 0) {
            // An all-null column has nothing to transform; its ids reference no entry.
            return source;
        }
        byte[][] derivedValues = new byte[dictionary.length][];
        for (int i = 0; i < dictionary.length; i++) {
            derivedValues[i] = transform.apply(dictionary[i]);
        }
        java.util.TreeMap<byte[], Integer> ordered = new java.util.TreeMap<>(java.util.Arrays::compareUnsigned);
        for (byte[] value : derivedValues) {
            ordered.putIfAbsent(value, 0);
        }
        byte[][] derived = new byte[ordered.size()][];
        int next = 0;
        for (java.util.Map.Entry<byte[], Integer> entry : ordered.entrySet()) {
            entry.setValue(next);
            derived[next] = entry.getKey();
            next++;
        }
        int[] remap = new int[dictionary.length];
        for (int i = 0; i < dictionary.length; i++) {
            remap[i] = ordered.get(derivedValues[i]);
        }
        int[] sourceIds = source.ids();
        int[] ids = new int[sourceIds.length];
        for (int r = 0; r < ids.length; r++) {
            ids[r] = remap[sourceIds[r]];
        }
        return new org.weakref.nitro.jit.Column.StringColumn(ids, derived, source.nulls());
    }

    /** Run a pipeline stage (via {@link #runStage}) and materialize its result for use as a downstream stage's input. */
    public static Materialized materializeStage(Allocator allocator, ParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, java.util.Map<String, Materialized> virtuals)
    {
        return materializeStage(allocator, tables, lowered, virtuals, List.of());
    }

    /**
     * Row-wise concatenation (UNION ALL) of two already-materialized, same-schema relations. Unlike
     * {@link #materializeUnion}, the inputs are stages that scanned VIRTUAL relations (so they cannot be re-streamed
     * from Parquet); this concatenates their materialized columns directly. Used to union two windowed channels.
     */
    public static Materialized concatenate(Materialized first, Materialized second)
    {
        List<org.weakref.nitro.jit.Column[]> parts = new ArrayList<>();
        parts.add(first.columns());
        parts.add(second.columns());
        int[] counts = {first.rows(), second.rows()};
        int total = first.rows() + second.rows();
        return new Materialized(concatenateColumns(parts, counts, total, first.columns().length), total);
    }

    /**
     * As {@link #materializeStage(Allocator, TpcdsParquetTables, org.weakref.nitro.jit.QueryLowering.Lowered, Map)} but
     * reconstructing the stage's STRING output columns (per {@code stringColumns}, resolved against the stage's own
     * loaded inputs) into dictionary columns, so the virtual relation carries their dictionaries for a downstream stage
     * that passes the string through rather than re-joining its base table.
     */
    public static Materialized materializeStage(Allocator allocator, ParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, java.util.Map<String, Materialized> virtuals,
            List<CompiledTpcdsQueries.DictRef> stringColumns)
    {
        LoweredResult result = runStage(allocator, tables, lowered, virtuals);
        return new Materialized(materialize(result.result(), result.inputs(), stringColumns), result.result().rowCount());
    }

    /**
     * Run a stage by draining every input eagerly (the probe and each build, resolving virtuals), then executing the
     * compiled pipeline. Unlike {@link #runStage}, the probe is drained rather than streamed, so its column dictionaries
     * are captured in the returned inputs -- letting a probe-side string group key be reconstructed at materialization.
     */
    public static LoweredResult runStageEager(Allocator allocator, ParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, java.util.Map<String, Materialized> virtuals)
    {
        List<org.weakref.nitro.jit.QueryLowering.Input> sources = lowered.inputs();
        org.weakref.nitro.jit.Column[][] inputs = new org.weakref.nitro.jit.Column[sources.size()][];
        int[] rowCounts = new int[sources.size()];
        for (int s = 0; s < sources.size(); s++) {
            resolveInput(allocator, tables, sources.get(s), virtuals, inputs, rowCounts, s);
        }
        return new LoweredResult(lowered.compile().execute(inputs, rowCounts), inputs);
    }

    private static StreamedResult streamCapturingBuilds(Allocator allocator, ParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, org.weakref.nitro.jit.StreamingPipeline streaming, boolean lazyProbe)
    {
        List<org.weakref.nitro.jit.QueryLowering.Input> sources = lowered.inputs();
        int buildCount = sources.size() - 1;
        org.weakref.nitro.jit.Column[][] builds = new org.weakref.nitro.jit.Column[buildCount][];
        int[] buildRowCounts = new int[buildCount];
        for (int b = 0; b < buildCount; b++) {
            org.weakref.nitro.jit.QueryLowering.Input source = sources.get(b + 1);
            String[] names = source.columns().stream().map(org.weakref.nitro.jit.QueryLowering.Column::sourceName).toArray(String[]::new);
            DrainedInput loaded = drainColumns(scan(allocator, tables, source.table(), names), source.columns());
            builds[b] = loaded.columns;
            buildRowCounts[b] = loaded.rows;
        }
        org.weakref.nitro.jit.QueryLowering.Input probe = sources.get(0);
        org.weakref.nitro.jit.StreamingPipeline.Source source = lazyProbe
                ? parquetLazySource(allocator, tables, probe.table(), probe.columns(), lowered.pipeline())
                : parquetFlatSource(allocator, tables, probe.table(), probe.columns());
        return new StreamedResult(streaming.execute(source, builds, buildRowCounts), builds, source);
    }

    /** Load a lowered query's inputs from Parquet and run it. */
    public static LoweredResult runLowered(Allocator allocator, ParquetTables tables, org.weakref.nitro.jit.QueryLowering.Lowered lowered)
    {
        LoadedInputs loaded = loadLoweredInputs(allocator, tables, lowered);
        return new LoweredResult(lowered.compile().execute(loaded.inputs(), loaded.rowCounts()), loaded.inputs());
    }

    /**
     * Run a single-stage lowered query through the streaming path (the probe fact is streamed batch-by-batch instead
     * of drained), returning the result with the build inputs positioned for {@link CompiledTpcdsQueries.DictRef}
     * reconstruction. Needed when the probe is a huge fact (e.g. Q37/Q82 over inventory) that cannot be drained eagerly.
     */
    public static LoweredResult runStreamingPorted(Allocator allocator, ParquetTables tables, org.weakref.nitro.jit.QueryLowering.Lowered lowered)
    {
        org.weakref.nitro.jit.StreamingPipeline streaming =
                PipelineCompiler.compileStreaming(lowered.pipeline(), lowered.encodings(), lowered.nullable());
        StreamedResult streamed = streamCapturingBuilds(allocator, tables, lowered, streaming, true);
        org.weakref.nitro.jit.Column[][] inputsForDictRef = new org.weakref.nitro.jit.Column[lowered.inputs().size()][];
        for (int b = 0; b < streamed.builds().length; b++) {
            inputsForDictRef[b + 1] = streamed.builds()[b];
        }
        inputsForDictRef[0] = internedProbeDictionaries(streamed.probeSource(), lowered.inputs().get(0).columns().size());
        return new LoweredResult(streamed.result(), inputsForDictRef);
    }

    /**
     * Run a two-stage (pipeline-breaker) query: execute {@code subquery}, materialize its result into a relation,
     * and run {@code main} with that relation substituted for the input whose table name is {@code virtualTable}
     * (all other {@code main} inputs are read from Parquet). This is the decorrelated correlated-subquery shape --
     * the subquery's aggregate feeds {@code main} as a join build, exactly as the optimizer produces it.
     */
    public static LoweredResult runMultiStage(Allocator allocator, ParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered subquery, org.weakref.nitro.jit.QueryLowering.Lowered main, String virtualTable)
    {
        org.weakref.nitro.jit.StreamingPipeline subStreaming =
                PipelineCompiler.compileStreaming(subquery.pipeline(), subquery.encodings(), subquery.nullable());
        return runMultiStage(allocator, tables, subquery, subStreaming, main, main.compile(), virtualTable);
    }

    /**
     * As {@link #runMultiStage}, but with both stages already compiled -- so a benchmark amortizes the one-time Java
     * compilation (like a query plan) and measures only the per-invocation Parquet read + execution.
     */
    public static LoweredResult runMultiStage(Allocator allocator, ParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered subquery, org.weakref.nitro.jit.StreamingPipeline subStreaming,
            org.weakref.nitro.jit.QueryLowering.Lowered main, CompiledPipeline mainCompiled, String virtualTable)
    {
        // The subquery's fact side is read from Parquet, so run it through the lazy streaming path (decode only the
        // columns/rows that survive its joins and filters) rather than draining every fact column up front. The main
        // stage's probe is the in-memory subquery result, so it stays eager.
        CompiledPipeline.Result subResult = runStreamingLowered(allocator, tables, subquery, subStreaming, true);
        org.weakref.nitro.jit.Column[] materialized = materialize(subResult);
        int subRows = subResult.rowCount();

        List<org.weakref.nitro.jit.QueryLowering.Input> sources = main.inputs();
        org.weakref.nitro.jit.Column[][] inputs = new org.weakref.nitro.jit.Column[sources.size()][];
        int[] rowCounts = new int[sources.size()];
        for (int s = 0; s < sources.size(); s++) {
            org.weakref.nitro.jit.QueryLowering.Input source = sources.get(s);
            if (source.table().equals(virtualTable)) {
                inputs[s] = materialized;
                rowCounts[s] = subRows;
            }
            else {
                String[] names = source.columns().stream().map(org.weakref.nitro.jit.QueryLowering.Column::sourceName).toArray(String[]::new);
                DrainedInput loaded = drainColumns(scan(allocator, tables, source.table(), names), source.columns());
                inputs[s] = loaded.columns;
                rowCounts[s] = loaded.rows;
            }
        }
        return new LoweredResult(mainCompiled.execute(inputs, rowCounts), inputs);
    }

    /**
     * Run a UNION ALL: execute each branch sub-pipeline (streamed, like a multi-stage subquery), materialize its
     * result, concatenate the same-schema branch results row-wise into the {@code virtualTable}, and run {@code main}
     * over the concatenation. Branches are computed once each (not replayed); concatenation is the union.
     */
    public static LoweredResult runUnion(Allocator allocator, ParquetTables tables,
            List<org.weakref.nitro.jit.QueryLowering.Lowered> branches, org.weakref.nitro.jit.QueryLowering.Lowered main, String virtualTable)
    {
        return runUnion(allocator, tables, branches, main, virtualTable, List.of());
    }

    /**
     * Materialize a UNION ALL into a virtual relation: stream each branch, materialize its (filtered) output, and
     * concatenate the same-schema branch results row-wise -- merging branch string columns into one ordered unified
     * dictionary (per {@code branchStringColumns}). The result is a {@link Materialized} relation that can be registered
     * as a virtual table and consumed by a downstream stage (e.g. a grouping/HAVING/count over the union, or a join),
     * exactly as a single materialized stage is. Branches are computed once each (not replayed); concatenation is the
     * union.
     */
    public static Materialized materializeUnion(Allocator allocator, ParquetTables tables,
            List<org.weakref.nitro.jit.QueryLowering.Lowered> branches, List<CompiledTpcdsQueries.DictRef> branchStringColumns)
    {
        List<org.weakref.nitro.jit.Column[]> parts = new ArrayList<>();
        int[] counts = new int[branches.size()];
        int width = 0;
        for (int i = 0; i < branches.size(); i++) {
            org.weakref.nitro.jit.QueryLowering.Lowered branch = branches.get(i);
            // Stream every branch (grouped or projection-only): streaming materializes only the branch's filtered
            // output, not its whole fact -- draining an ungrouped branch's fact eagerly was the dominant cost.
            org.weakref.nitro.jit.StreamingPipeline streaming =
                    PipelineCompiler.compileStreaming(branch.pipeline(), branch.encodings(), branch.nullable());
            StreamedResult streamed = streamCapturingBuilds(allocator, tables, branch, streaming, true);
            CompiledPipeline.Result result = streamed.result();
            org.weakref.nitro.jit.Column[][] dictInputs = new org.weakref.nitro.jit.Column[branch.inputs().size()][];
            for (int b = 0; b < streamed.builds().length; b++) {
                dictInputs[b + 1] = streamed.builds()[b];   // probe (index 0) is streamed, has no materialized dictionary
            }
            org.weakref.nitro.jit.Column[] materialized = materialize(result, dictInputs, branchLiteralColumns(branch, branchStringColumns));
            parts.add(materialized);
            counts[i] = result.rowCount();
            width = materialized.length;
        }
        int total = 0;
        for (int count : counts) {
            total += count;
        }
        return new Materialized(concatenateColumns(parts, counts, total, width), total);
    }

    /**
     * The string-output reconstruction refs for one union branch: the shared {@code branchStringColumns} (reconstructed
     * dictionary columns, identical in shape across branches) plus each branch's OWN constant-label columns, derived
     * from its {@link org.weakref.nitro.jit.Plan.LitStr} projections. A {@code LitStr} carries its value in the plan, so
     * the per-branch label (e.g. a channel name that differs by branch) is self-describing -- no per-branch ref list is
     * needed in the union spec. A shared ref already covering a column wins (it is not overridden by a derived literal).
     */
    private static List<CompiledTpcdsQueries.DictRef> branchLiteralColumns(
            org.weakref.nitro.jit.QueryLowering.Lowered branch, List<CompiledTpcdsQueries.DictRef> branchStringColumns)
    {
        List<org.weakref.nitro.jit.Plan.Expr> projections = branch.pipeline().projections();
        if (projections.isEmpty()) {
            return branchStringColumns;
        }
        java.util.Set<Integer> covered = new java.util.HashSet<>();
        for (CompiledTpcdsQueries.DictRef ref : branchStringColumns) {
            covered.add(ref.resultColumn());
        }
        List<CompiledTpcdsQueries.DictRef> refs = new ArrayList<>(branchStringColumns);
        for (int c = 0; c < projections.size(); c++) {
            if (projections.get(c) instanceof org.weakref.nitro.jit.Plan.LitStr literal && covered.add(c)) {
                refs.add(CompiledTpcdsQueries.DictRef.literal(c, literal.value()));
            }
        }
        return refs;
    }

    /**
     * As {@link #runUnion}, but {@code branchStringColumns} names the branch-result columns that are dictionary
     * strings (the union's group key may be a string, e.g. Q56/Q60 group by {@code i_item_id}). Each branch carries
     * its own (filtered) source dictionary, so the branch string columns are merged into one ordered unified
     * dictionary during concatenation -- ids consistent across branches, and id order = value order for ORDER BY.
     */
    public static LoweredResult runUnion(Allocator allocator, ParquetTables tables,
            List<org.weakref.nitro.jit.QueryLowering.Lowered> branches, org.weakref.nitro.jit.QueryLowering.Lowered main,
            String virtualTable, List<CompiledTpcdsQueries.DictRef> branchStringColumns)
    {
        Materialized unionRelation = materializeUnion(allocator, tables, branches, branchStringColumns);
        org.weakref.nitro.jit.Column[] union = unionRelation.columns();
        int total = unionRelation.rows();

        List<org.weakref.nitro.jit.QueryLowering.Input> sources = main.inputs();
        org.weakref.nitro.jit.Column[][] inputs = new org.weakref.nitro.jit.Column[sources.size()][];
        int[] rowCounts = new int[sources.size()];
        for (int s = 0; s < sources.size(); s++) {
            org.weakref.nitro.jit.QueryLowering.Input source = sources.get(s);
            if (source.table().equals(virtualTable)) {
                inputs[s] = union;
                rowCounts[s] = total;
            }
            else {
                String[] names = source.columns().stream().map(org.weakref.nitro.jit.QueryLowering.Column::sourceName).toArray(String[]::new);
                DrainedInput loaded = drainColumns(scan(allocator, tables, source.table(), names), source.columns());
                inputs[s] = loaded.columns;
                rowCounts[s] = loaded.rows;
            }
        }
        return new LoweredResult(main.compile().execute(inputs, rowCounts), inputs);
    }

    /** Row-wise concatenation of same-schema branch results into one {@link org.weakref.nitro.jit.Column}[] (numeric and dictionary-string columns). */
    private static org.weakref.nitro.jit.Column[] concatenateColumns(List<org.weakref.nitro.jit.Column[]> parts, int[] counts, int total, int width)
    {
        org.weakref.nitro.jit.Column[] out = new org.weakref.nitro.jit.Column[width];
        for (int c = 0; c < width; c++) {
            if (parts.get(0)[c] instanceof org.weakref.nitro.jit.Column.StringColumn) {
                out[c] = concatenateStringColumn(parts, counts, total, c);
            }
            else {
                out[c] = concatenateNumericColumn(parts, counts, total, c);
            }
        }
        return out;
    }

    private static org.weakref.nitro.jit.Column concatenateNumericColumn(List<org.weakref.nitro.jit.Column[]> parts, int[] counts, int total, int c)
    {
        long[] values = new long[total];
        boolean[] nulls = null;
        int offset = 0;
        for (int p = 0; p < parts.size(); p++) {
            org.weakref.nitro.jit.Column.FlatColumn part = (org.weakref.nitro.jit.Column.FlatColumn) parts.get(p)[c];
            System.arraycopy(part.values(), 0, values, offset, counts[p]);
            if (part.nulls() != null) {
                if (nulls == null) {
                    nulls = new boolean[total];
                }
                System.arraycopy(part.nulls(), 0, nulls, offset, counts[p]);
            }
            offset += counts[p];
        }
        return new org.weakref.nitro.jit.Column.FlatColumn(values, nulls);
    }

    /**
     * Concatenate a dictionary-string column across branches into one ordered unified dictionary. Each branch's ids
     * point into its own (filtered) dictionary, so values are re-interned into a single dictionary and the resulting
     * ids are re-sorted into byte order -- consistent ids across branches, and id comparison = value comparison.
     */
    private static org.weakref.nitro.jit.Column concatenateStringColumn(List<org.weakref.nitro.jit.Column[]> parts, int[] counts, int total, int c)
    {
        java.util.Map<String, Integer> index = new java.util.LinkedHashMap<>();
        List<byte[]> dictionary = new ArrayList<>();
        int[] ids = new int[total];
        boolean[] nulls = null;
        int offset = 0;
        for (int p = 0; p < parts.size(); p++) {
            org.weakref.nitro.jit.Column.StringColumn part = (org.weakref.nitro.jit.Column.StringColumn) parts.get(p)[c];
            for (int r = 0; r < counts[p]; r++) {
                if (part.nulls() != null && part.nulls()[r]) {
                    if (nulls == null) {
                        nulls = new boolean[total];
                    }
                    nulls[offset + r] = true;
                    continue;
                }
                ids[offset + r] = intern(index, dictionary, part.dictionary()[part.ids()[r]]);
            }
            offset += counts[p];
        }
        return orderedStringColumn(ids, dictionary, total, nulls);
    }

    /** Materialize a pipeline result into {@link org.weakref.nitro.jit.Column}s so it can feed a downstream stage as a relation. */
    private static org.weakref.nitro.jit.Column[] materialize(CompiledPipeline.Result result)
    {
        return materialize(result, null, List.of());
    }

    /**
     * Materialize a pipeline result into {@link org.weakref.nitro.jit.Column}s, resolving each string result column
     * named in {@code stringColumns} back to a {@link org.weakref.nitro.jit.Column.StringColumn}: the result stores a
     * string as a dictionary id into one of the stage's build (dimension) inputs, so the id maps back through that
     * build's dictionary. {@code builds} are the dimension inputs (probe excluded), as captured by the streaming run.
     */
    private static org.weakref.nitro.jit.Column[] materialize(CompiledPipeline.Result result,
            org.weakref.nitro.jit.Column[][] dictInputs, List<CompiledTpcdsQueries.DictRef> stringColumns)
    {
        long[][] columns = result.columns();
        boolean[][] nulls = result.nulls();
        int rowCount = result.rowCount();
        java.util.Map<Integer, CompiledTpcdsQueries.DictRef> stringByColumn = new java.util.HashMap<>();
        for (CompiledTpcdsQueries.DictRef ref : stringColumns) {
            stringByColumn.put(ref.resultColumn(), ref);
        }
        org.weakref.nitro.jit.Column[] materialized = new org.weakref.nitro.jit.Column[columns.length];
        for (int c = 0; c < columns.length; c++) {
            org.weakref.nitro.jit.Type type = result.types()[c];
            if (type == org.weakref.nitro.jit.Types.STRING) {
                CompiledTpcdsQueries.DictRef ref = stringByColumn.get(c);
                // A constant-label (literal) column carries its own single-entry dictionary and needs no source input;
                // every other string column reconstructs through a build/probe dictionary, which must be present.
                if (ref == null || (ref.literal() == null && (dictInputs == null || dictInputs[ref.dictInput()] == null))) {
                    throw new UnsupportedOperationException("string result column " + c + " has no resolvable source dictionary");
                }
                byte[][] dictionary = dictionaryFor(dictInputs, ref);
                int[] ids = new int[rowCount];
                for (int r = 0; r < rowCount; r++) {
                    ids[r] = (int) columns[c][r];
                }
                materialized[c] = new org.weakref.nitro.jit.Column.StringColumn(ids, dictionary, nulls == null ? null : nulls[c]);
            }
            else {
                materialized[c] = new org.weakref.nitro.jit.Column.FlatColumn(columns[c], nulls == null ? null : nulls[c]);
            }
        }
        return materialized;
    }

    /**
     * The source dictionary for a {@link CompiledTpcdsQueries.DictRef}, with its optional UTF-8 substring applied to
     * every entry. Each entry is reconstructed with the query's trailing {@code substring(col, start, length)}
     * projection so the output strings match the harness exactly.
     */
    public static byte[][] dictionaryFor(org.weakref.nitro.jit.Column[][] dictInputs, CompiledTpcdsQueries.DictRef ref)
    {
        if (ref.literal() != null) {
            // Constant string column: a single-entry dictionary the placeholder column's all-zero ids map onto.
            return new byte[][] {ref.literal().getBytes(java.nio.charset.StandardCharsets.UTF_8)};
        }
        byte[][] dictionary = ((org.weakref.nitro.jit.Column.StringColumn) dictInputs[ref.dictInput()][ref.dictColumn()]).dictionary();
        if (ref.substringLength() >= 0) {
            byte[][] truncated = new byte[dictionary.length][];
            for (int i = 0; i < dictionary.length; i++) {
                truncated[i] = utf8Substring(dictionary[i], ref.substringStart(), ref.substringLength());
            }
            dictionary = truncated;
        }
        if (ref.prefix() != null) {
            // 'prefix' || value, applied per dictionary entry: a constant prefix keeps the entries' byte order, so
            // id-based ordering and grouping on the prefixed column are unaffected.
            byte[] prefix = ref.prefix().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[][] prefixed = new byte[dictionary.length][];
            for (int i = 0; i < dictionary.length; i++) {
                prefixed[i] = new byte[prefix.length + dictionary[i].length];
                System.arraycopy(prefix, 0, prefixed[i], 0, prefix.length);
                System.arraycopy(dictionary[i], 0, prefixed[i], prefix.length, dictionary[i].length);
            }
            dictionary = prefixed;
        }
        return dictionary;
    }

    /** UTF-8 substring by code point: {@code start} is 1-based, {@code length} a code-point count. */
    private static byte[] utf8Substring(byte[] value, int start, int length)
    {
        int begin = codePointOffset(value, start - 1);
        int end = codePointOffset(value, start - 1 + length);
        return java.util.Arrays.copyOfRange(value, begin, end);
    }

    /** Byte offset of the {@code codePoints}-th code-point boundary (clamped to the array length). */
    private static int codePointOffset(byte[] value, int codePoints)
    {
        int offset = 0;
        for (int seen = 0; offset < value.length && seen < codePoints; seen++) {
            offset++;
            while (offset < value.length && (value[offset] & 0xC0) == 0x80) {
                offset++;
            }
        }
        return offset;
    }

    private record DrainedInput(org.weakref.nitro.jit.Column[] columns, int rows) {}

    /** Drain a scan into one {@link org.weakref.nitro.jit.Column} per spec, preserving nulls; string columns build a dictionary. */
    private static DrainedInput drainColumns(Operator operator, List<org.weakref.nitro.jit.QueryLowering.Column> specs)
    {
        int width = specs.size();
        // Each column drains into exactly one value array -- a numeric column uses values[c] (long[]), a string
        // column uses ids[c] (int[]). Allocating (and growing) both per column wasted ~half the drain's allocation:
        // a low-cardinality string dimension over millions of rows grew a full-size long[] it never read.
        long[] emptyLong = new long[0];
        int[] emptyInt = new int[0];
        boolean[] string = new boolean[width];
        long[][] values = new long[width][];            // flat values (numeric columns only)
        int[][] ids = new int[width][];                 // string ids (string columns only)
        boolean[][] nulls = new boolean[width][16];
        List<java.util.Map<String, Integer>> dictionaryIndex = new ArrayList<>();
        List<List<byte[]>> dictionaries = new ArrayList<>();
        boolean[] verbatim = new boolean[width];
        for (int c = 0; c < width; c++) {
            string[c] = specs.get(c).encoding() == org.weakref.nitro.jit.ColumnEncoding.STRING;
            // A verbatim column appends dictionary entries as they arrive (no interning): nothing observes its id
            // equality or order, so the per-entry hash lookups are pure overhead.
            verbatim[c] = string[c] && specs.get(c).loadMode() == org.weakref.nitro.jit.QueryLowering.LoadMode.VERBATIM;
            values[c] = string[c] ? emptyLong : new long[16];
            ids[c] = string[c] ? new int[16] : emptyInt;
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
                            if (string[c]) {
                                ids[c] = java.util.Arrays.copyOf(ids[c], capacity);
                            }
                            else {
                                values[c] = java.util.Arrays.copyOf(values[c], capacity);
                            }
                            nulls[c] = java.util.Arrays.copyOf(nulls[c], capacity);
                        }
                    }
                    boolean dense = mask.all();
                    for (int c = 0; c < width; c++) {
                        Vector valueVector = batch.output(c).borrow(Stream.VALUES);
                        Vector nullVector = batch.output(c).borrowOrNull(Stream.NULLS);
                        if (!string[c] && dense) {
                            // Dense numeric column: dispatch on the vector type once and bulk-fill (arraycopy a
                            // non-null I64 column) instead of the per-row mask.position + longValue gather.
                            fillDenseNumeric(valueVector, nullVector, values[c], nulls[c], size, count);
                            continue;
                        }
                        if (string[c] && dense && valueVector instanceof org.weakref.nitro.data.DictionaryVector dictionaryVector) {
                            // Dense dictionary-encoded string column: intern each distinct dictionary entry once and
                            // remap the per-row ids, instead of interning byte-by-byte per row. For a low-cardinality
                            // dimension column (e.g. cd_gender over ~2M rows) this turns N interns into K (K = distinct
                            // values) plus N cheap id remaps -- the per-row interning was the dominant build cost.
                            Vector dictionary = dictionaryVector.values();
                            int[] dictionaryIds = dictionaryVector.ids();
                            int dictionarySize = dictionary.length();
                            int[] localToGlobal = new int[dictionarySize];
                            for (int entry = 0; entry < dictionarySize; entry++) {
                                if (verbatim[c]) {
                                    localToGlobal[entry] = dictionaries.get(c).size();
                                    dictionaries.get(c).add(stringBytes(dictionary, entry));
                                }
                                else {
                                    localToGlobal[entry] = intern(dictionaryIndex.get(c), dictionaries.get(c), stringBytes(dictionary, entry));
                                }
                            }
                            for (int index = 0; index < count; index++) {
                                boolean isNull = isNull(nullVector, index);
                                int slot = size + index;
                                nulls[c][slot] = isNull;
                                ids[c][slot] = isNull ? 0 : localToGlobal[dictionaryIds[index]];
                            }
                            continue;
                        }
                        for (int index = 0; index < count; index++) {
                            int position = mask.position(index);
                            boolean isNull = isNull(nullVector, position);
                            int slot = size + index;
                            nulls[c][slot] = isNull;
                            if (string[c]) {
                                if (isNull) {
                                    ids[c][slot] = 0;
                                }
                                else if (verbatim[c]) {
                                    ids[c][slot] = dictionaries.get(c).size();
                                    dictionaries.get(c).add(stringBytes(valueVector, position));
                                }
                                else {
                                    ids[c][slot] = intern(dictionaryIndex.get(c), dictionaries.get(c), stringBytes(valueVector, position));
                                }
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
                if (specs.get(c).loadMode() == org.weakref.nitro.jit.QueryLowering.LoadMode.ORDERED) {
                    columns[c] = orderedStringColumn(ids[c], dictionaries.get(c), size, nullMask);
                }
                else {
                    byte[][] dictionary = dictionaries.get(c).toArray(new byte[0][]);
                    if (specs.get(c).loadMode() == org.weakref.nitro.jit.QueryLowering.LoadMode.DEDUPED) {
                        registerCanonical(dictionary, false);   // interned: duplicate-free by construction
                    }
                    columns[c] = new org.weakref.nitro.jit.Column.StringColumn(
                            java.util.Arrays.copyOf(ids[c], size), dictionary, nullMask);
                }
            }
            else {
                columns[c] = new org.weakref.nitro.jit.Column.FlatColumn(java.util.Arrays.copyOf(values[c], size), nullMask);
            }
        }
        return new DrainedInput(withDerivedColumns(columns, specs), size);
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
        byte[][] dict = dictionary.toArray(new byte[0][]);
        int[] order = new int[entries];        // old ids in sorted (unsigned-byte) order
        for (int i = 0; i < entries; i++) {
            order[i] = i;
        }
        // A multikey (three-way radix) quicksort over the byte strings: partitions on one byte position at a time
        // and only the equal-on-that-byte range recurses to the next position. Produces the same unsigned-byte
        // lexicographic order as a comparison sort, but without boxing entry ids or re-comparing shared prefixes --
        // building the ordered dictionary for a high-cardinality column (e.g. c_customer_id, ~2M distinct) was the
        // dominant cost of those plans, almost all of it in compareUnsigned over a boxed Integer[] sort.
        multikeyQuickSort(order, 0, entries - 1, 0, dict);
        int[] remap = new int[entries];        // old id -> position in sorted order
        byte[][] sorted = new byte[entries][];
        for (int newId = 0; newId < entries; newId++) {
            int oldId = order[newId];
            remap[oldId] = newId;
            sorted[newId] = dict[oldId];
        }
        int[] remapped = new int[size];
        for (int r = 0; r < size; r++) {
            remapped[r] = remap[ids[r]];
        }
        registerCanonical(sorted, true);
        return new org.weakref.nitro.jit.Column.StringColumn(remapped, sorted, nullMask);
    }

    /** The byte at position {@code d} of {@code s} as an unsigned int, or -1 once the string has ended (sorts first). */
    private static int byteAt(byte[] s, int d)
    {
        return d < s.length ? (s[d] & 0xFF) : -1;
    }

    /**
     * Sort {@code order[lo..hi]} (inclusive) so the referenced strings {@code dict[order[*]]} are in ascending
     * unsigned-byte order, comparing from byte position {@code d}. Three-way (Bentley-McIlroy) partition on the byte
     * at {@code d} with a median-of-three pivot (so a nearly-sorted dictionary does not hit the quadratic case); the
     * middle (equal) partition recurses to {@code d + 1}.
     */
    private static void multikeyQuickSort(int[] order, int lo, int hi, int d, byte[][] dict)
    {
        while (hi - lo > 8) {
            int mid = lo + ((hi - lo) >>> 1);
            int pivot = medianOfThree(byteAt(dict[order[lo]], d), byteAt(dict[order[mid]], d), byteAt(dict[order[hi]], d));
            int lt = lo;
            int gt = hi;
            int i = lo;
            while (i <= gt) {
                int c = byteAt(dict[order[i]], d);
                if (c < pivot) {
                    swap(order, lt, i);
                    lt++;
                    i++;
                }
                else if (c > pivot) {
                    swap(order, gt, i);
                    gt--;
                }
                else {
                    i++;
                }
            }
            // order[lo..lt-1] < pivot ; order[lt..gt] == pivot ; order[gt+1..hi] > pivot
            multikeyQuickSort(order, lo, lt - 1, d, dict);
            if (pivot >= 0) {
                multikeyQuickSort(order, lt, gt, d + 1, dict);
            }
            // Tail-recurse the high partition by looping (keeps stack depth bounded).
            lo = gt + 1;
        }
        // Small range: insertion sort by full unsigned-byte comparison from position d.
        for (int i = lo + 1; i <= hi; i++) {
            for (int j = i; j > lo && compareFrom(dict[order[j - 1]], dict[order[j]], d) > 0; j--) {
                swap(order, j, j - 1);
            }
        }
    }

    private static void swap(int[] a, int i, int j)
    {
        int t = a[i];
        a[i] = a[j];
        a[j] = t;
    }

    private static int medianOfThree(int a, int b, int c)
    {
        if (a < b) {
            return b < c ? b : (a < c ? c : a);
        }
        return a < c ? a : (b < c ? c : b);
    }

    /** Unsigned-byte comparison of {@code a} and {@code b} starting at byte position {@code d}. */
    private static int compareFrom(byte[] a, byte[] b, int d)
    {
        int len = Math.min(a.length, b.length);
        for (int i = d; i < len; i++) {
            int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return a.length - b.length;
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

    /**
     * A streaming source that interns selected string columns into per-query global dictionaries, so their ids
     * stay stable across batches (required for group keys, aggregate inputs, and projected string outputs).
     */
    public interface StringInterningSource
            extends org.weakref.nitro.jit.StreamingPipeline.Source
    {
        /** The final interned dictionary for {@code column}, exact-sized; null when the column was not globally interned. */
        byte[][] finalDictionary(int column);
    }

    /**
     * The per-query global intern for one streamed string column: entries only ever append (ids are stable), and
     * the growing backing array is handed to the pipeline directly (capacity-padded), with
     * {@link org.weakref.nitro.jit.Column.StringColumn#dictionarySize} marking the valid prefix. A dictionary-encoded
     * page is folded in once per distinct page dictionary (identity-cached remap); plain pages intern per row.
     */
    private static final class GlobalStringDictionary
    {
        private final java.util.HashMap<String, Integer> index = new java.util.HashMap<>();
        private final java.util.IdentityHashMap<Object, int[]> pageRemaps = new java.util.IdentityHashMap<>();
        // An optional per-value derivation (e.g. a regexp host extraction) applied before interning, so the
        // dictionary holds the DERIVED values. The derivation runs once per distinct RAW value: a
        // dictionary-encoded page pays it per distinct page entry, and plain pages dedup raw bytes through
        // rawIndex first -- a per-row regexp over a 100M-row fact would otherwise dominate the whole query.
        private final java.util.function.UnaryOperator<byte[]> transform;
        private final java.util.HashMap<String, Integer> rawIndex;
        private byte[][] entries = new byte[16][];
        private int size;

        GlobalStringDictionary(java.util.function.UnaryOperator<byte[]> transform)
        {
            this.transform = transform;
            this.rawIndex = transform == null ? null : new java.util.HashMap<>();
        }

        int intern(byte[] bytes)
        {
            if (transform == null) {
                return internDerived(bytes);
            }
            return rawIndex.computeIfAbsent(new String(bytes, java.nio.charset.StandardCharsets.UTF_8),
                    key -> internDerived(transform.apply(bytes)));
        }

        private int internDerived(byte[] value)
        {
            return index.computeIfAbsent(new String(value, java.nio.charset.StandardCharsets.UTF_8), key -> {
                if (size == entries.length) {
                    entries = java.util.Arrays.copyOf(entries, size * 2);
                }
                entries[size] = value;
                return size++;
            });
        }

        /** Map a dictionary page's entries to global ids, computed once per distinct page dictionary. */
        int[] remap(org.weakref.nitro.data.BinaryVector pageDictionary)
        {
            return pageRemaps.computeIfAbsent(pageDictionary, ignored -> {
                int[] remap = new int[pageDictionary.length()];
                for (int e = 0; e < remap.length; e++) {
                    remap[e] = intern(stringBytes(pageDictionary, e));
                }
                return remap;
            });
        }

        byte[][] backing()
        {
            return entries;
        }

        int size()
        {
            return size;
        }

        byte[][] snapshot()
        {
            return java.util.Arrays.copyOf(entries, size);
        }
    }

    /** The spec's regexp derivation as a per-value transform (null when the column is loaded verbatim), with the exact replacement semantics of regexp_replace_utf8. */
    private static java.util.function.UnaryOperator<byte[]> regexpTransform(org.weakref.nitro.jit.QueryLowering.Column spec)
    {
        if (spec.regexpPattern() == null) {
            return null;
        }
        io.trino.re2j.Pattern pattern = io.trino.re2j.Pattern.compile(spec.regexpPattern());
        io.airlift.slice.Slice replacement = org.weakref.nitro.function.scalar.builtin.RegexpReplaceUtf8.translateReplacement(
                io.airlift.slice.Slices.utf8Slice(spec.regexpReplacement()));
        return value -> pattern.matcher(io.airlift.slice.Slices.wrappedBuffer(value)).replaceAll(replacement).getBytes();
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
