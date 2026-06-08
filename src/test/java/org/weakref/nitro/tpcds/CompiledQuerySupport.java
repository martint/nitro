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

    /**
     * The runtime physical width (64 or 32 bits) of each numeric column of {@code table}, discovered by peeking the
     * first non-empty batch of a scan rather than assumed from the SQL/logical type. Under schema evolution the
     * physical type a file actually carries may differ from what the logical type implies, so the compiled engine
     * must specialize to the discovered profile (or fall back to an adaptation/widening layer), not to a declared
     * type. A non-numeric (string/dictionary) column reports 64 and is handled by its STRING encoding instead. This
     * is the per-file half of the variant profile; encoding (the per-batch half) is discovered separately.
     */
    public static int[] discoverNumericWidths(Allocator allocator, TpcdsParquetTables tables, String table, List<String> columns)
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
    public static org.weakref.nitro.jit.StreamingPipeline.Source parquetColumnarSource(Allocator allocator, TpcdsParquetTables tables, String table, String... columns)
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
    public static org.weakref.nitro.jit.StreamingPipeline.Source parquetFlatSource(Allocator allocator, TpcdsParquetTables tables, String table, String... columns)
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
    public static org.weakref.nitro.jit.StreamingPipeline.Source parquetFlatSource(Allocator allocator, TpcdsParquetTables tables, String table, List<org.weakref.nitro.jit.QueryLowering.Column> specs)
    {
        int width = specs.size();
        String[] names = specs.stream().map(org.weakref.nitro.jit.QueryLowering.Column::name).toArray(String[]::new);
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
    public static org.weakref.nitro.jit.StreamingPipeline.Source parquetLazySource(Allocator allocator, TpcdsParquetTables tables,
            String table, List<org.weakref.nitro.jit.QueryLowering.Column> specs)
    {
        int width = specs.size();
        String[] names = specs.stream().map(org.weakref.nitro.jit.QueryLowering.Column::name).toArray(String[]::new);
        Operator operator = scan(allocator, tables, table, names);
        return new org.weakref.nitro.jit.StreamingPipeline.Source()
        {
            private Batch open;   // current batch, kept open so any column can be converted on demand in materialize()
            private Mask mask;
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

            @Override
            public org.weakref.nitro.jit.Column[] columns()
            {
                // Non-staged callers (a pipeline with no deferrable payload) get every column over the whole batch
                // through the dense zero-copy path -- wrapping a non-null I64 column's array by reference -- not the
                // per-row convert gather.
                return materialize(identity(width));
            }

            @Override
            public org.weakref.nitro.jit.Column[] materialize(int[] columns, int[] selection, int count)
            {
                org.weakref.nitro.jit.Column[] out = new org.weakref.nitro.jit.Column[width];
                for (int column : columns) {
                    out[column] = convertColumn(open, mask, column, count, selection, specs.get(column).nullable());
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
                org.weakref.nitro.jit.Column[] out = new org.weakref.nitro.jit.Column[width];
                for (int column : columns) {
                    out[column] = denseColumn(open.output(column).borrow(Stream.VALUES), open.output(column).borrowOrNull(Stream.NULLS), currentRows, specs.get(column).nullable());
                }
                return out;
            }
        };
    }

    private static int[] identity(int count)
    {
        int[] identity = new int[count];
        for (int i = 0; i < count; i++) {
            identity[i] = i;
        }
        return identity;
    }

    /**
     * Convert a dense batch column (logical row {@code j} == batch position {@code j}) to a flat column, honoring the
     * declared {@code nullable} contract so it is byte-exact with {@link #convertColumn}. When the column is declared
     * nullable, an {@code I64} column wraps the decoded vector's backing {@code long[]} by reference and a present
     * nulls stream wraps the {@code BooleanVector}'s backing {@code boolean[]} by reference -- both share the decoder's
     * batch (held open until the next advance), and the generated code skips null positions via the nulls array, so no
     * copy or null-zeroing is needed. When the column is declared non-nullable but the batch nonetheless carries
     * nulls, the generated code emits no null guard, so those positions must read as {@code 0} (as {@code convertColumn}
     * does) -- that requires a fresh zeroed copy, since the shared backing array cannot be mutated. Only the
     * non-nullable, no-nulls case (and the nullable I64 case) stays zero-copy; an {@code I32} column is always widened
     * (its {@code int[]} cannot alias a {@code long[]}).
     */
    private static org.weakref.nitro.jit.Column denseColumn(Vector values, Vector nulls, int count, boolean nullable)
    {
        if (values instanceof I64Vector i64) {
            if (nulls == null) {
                return new org.weakref.nitro.jit.Column.FlatColumn(i64.values());   // no copy
            }
            if (nullable && nulls instanceof BooleanVector booleans) {
                return new org.weakref.nitro.jit.Column.FlatColumn(i64.values(), booleans.values());   // no copy
            }
            long[] backing = i64.values();
            long[] copy = new long[count];
            boolean[] nullMask = nullable ? new boolean[count] : null;
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
            long[] copy = new long[count];
            boolean[] nullMask = nullable ? new boolean[count] : null;
            for (int i = 0; i < count; i++) {
                boolean isNull = nulls != null && isNull(nulls, i);
                if (nullMask != null) {
                    nullMask[i] = isNull;
                }
                copy[i] = isNull ? 0 : backing[i];
            }
            return new org.weakref.nitro.jit.Column.FlatColumn(copy, nullMask);
        }
        throw new IllegalArgumentException("Unsupported column vector type: " + values.getClass().getName());
    }

    /** Convert column {@code c} of {@code batch} to a flat column of {@code count} rows, row {@code j} being batch position {@code mask.position(selection[j])}. */
    private static org.weakref.nitro.jit.Column convertColumn(Batch batch, Mask mask, int c, int count, int[] selection, boolean nullable)
    {
        Vector vector = batch.output(c).borrow(Stream.VALUES);
        Vector nulls = batch.output(c).borrowOrNull(Stream.NULLS);
        long[] values = new long[count];
        boolean[] nullMask = nullable ? new boolean[count] : null;
        for (int j = 0; j < count; j++) {
            int position = mask.position(selection[j]);
            boolean isNull = nulls != null && isNull(nulls, position);
            if (nullMask != null) {
                nullMask[j] = isNull;
            }
            values[j] = isNull ? 0 : longValue(vector, position);
        }
        return new org.weakref.nitro.jit.Column.FlatColumn(values, nullMask);
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

    /**
     * Run a lowered star query through a compiled {@link org.weakref.nitro.jit.StreamingPipeline}: the build
     * (dimension) inputs are materialized once into hash tables; the probe (fact) input is streamed from Parquet
     * batch-by-batch (flat columns). Returns the raw result.
     */
    public static CompiledPipeline.Result runStreamingLowered(Allocator allocator, TpcdsParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, org.weakref.nitro.jit.StreamingPipeline streaming)
    {
        return runStreamingLowered(allocator, tables, lowered, streaming, false);
    }

    /**
     * As {@link #runStreamingLowered}, but when {@code lazyProbe} is set the probe (fact) is streamed from a
     * selection-driven lazy source -- so join-driven late materialization converts the probe's payload columns only
     * for rows that survive the dimension joins.
     */
    public static CompiledPipeline.Result runStreamingLowered(Allocator allocator, TpcdsParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, org.weakref.nitro.jit.StreamingPipeline streaming, boolean lazyProbe)
    {
        return streamCapturingBuilds(allocator, tables, lowered, streaming, lazyProbe).result();
    }

    /** A streamed result together with its materialized build (dimension) inputs, so a string output column can be resolved back to its dictionary. */
    public record StreamedResult(CompiledPipeline.Result result, org.weakref.nitro.jit.Column[][] builds) {}

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
    public static LoweredResult runStage(Allocator allocator, TpcdsParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, java.util.Map<String, Materialized> virtuals)
    {
        List<org.weakref.nitro.jit.QueryLowering.Input> sources = lowered.inputs();
        org.weakref.nitro.jit.QueryLowering.Input probe = sources.get(0);
        boolean probeVirtual = virtuals.containsKey(probe.table());

        if (probeVirtual) {
            org.weakref.nitro.jit.Column[][] inputs = new org.weakref.nitro.jit.Column[sources.size()][];
            int[] rowCounts = new int[sources.size()];
            for (int s = 0; s < sources.size(); s++) {
                resolveInput(allocator, tables, sources.get(s), virtuals, inputs, rowCounts, s);
            }
            return new LoweredResult(lowered.compile().execute(inputs, rowCounts), inputs);
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
        org.weakref.nitro.jit.StreamingPipeline.Source source = parquetLazySource(allocator, tables, probe.table(), probe.columns());
        return new LoweredResult(streaming.execute(source, builds, buildRowCounts), inputsForDictRef);
    }

    /** Resolve input {@code slot} into {@code columns}/{@code rowCounts}[slot]: a materialized virtual relation if named in {@code virtuals}, else a Parquet drain. */
    private static void resolveInput(Allocator allocator, TpcdsParquetTables tables, org.weakref.nitro.jit.QueryLowering.Input source,
            java.util.Map<String, Materialized> virtuals, org.weakref.nitro.jit.Column[][] columns, int[] rowCounts, int slot)
    {
        Materialized virtual = virtuals.get(source.table());
        if (virtual != null) {
            columns[slot] = virtual.columns();
            rowCounts[slot] = virtual.rows();
        }
        else {
            String[] names = source.columns().stream().map(org.weakref.nitro.jit.QueryLowering.Column::name).toArray(String[]::new);
            DrainedInput loaded = drainColumns(scan(allocator, tables, source.table(), names), source.columns());
            columns[slot] = loaded.columns;
            rowCounts[slot] = loaded.rows;
        }
    }

    /** Run a pipeline stage (via {@link #runStage}) and materialize its result for use as a downstream stage's input. */
    public static Materialized materializeStage(Allocator allocator, TpcdsParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, java.util.Map<String, Materialized> virtuals)
    {
        LoweredResult result = runStage(allocator, tables, lowered, virtuals);
        return new Materialized(materialize(result.result()), result.result().rowCount());
    }

    private static StreamedResult streamCapturingBuilds(Allocator allocator, TpcdsParquetTables tables,
            org.weakref.nitro.jit.QueryLowering.Lowered lowered, org.weakref.nitro.jit.StreamingPipeline streaming, boolean lazyProbe)
    {
        List<org.weakref.nitro.jit.QueryLowering.Input> sources = lowered.inputs();
        int buildCount = sources.size() - 1;
        org.weakref.nitro.jit.Column[][] builds = new org.weakref.nitro.jit.Column[buildCount][];
        int[] buildRowCounts = new int[buildCount];
        for (int b = 0; b < buildCount; b++) {
            org.weakref.nitro.jit.QueryLowering.Input source = sources.get(b + 1);
            String[] names = source.columns().stream().map(org.weakref.nitro.jit.QueryLowering.Column::name).toArray(String[]::new);
            DrainedInput loaded = drainColumns(scan(allocator, tables, source.table(), names), source.columns());
            builds[b] = loaded.columns;
            buildRowCounts[b] = loaded.rows;
        }
        org.weakref.nitro.jit.QueryLowering.Input probe = sources.get(0);
        org.weakref.nitro.jit.StreamingPipeline.Source source = lazyProbe
                ? parquetLazySource(allocator, tables, probe.table(), probe.columns())
                : parquetFlatSource(allocator, tables, probe.table(), probe.columns());
        return new StreamedResult(streaming.execute(source, builds, buildRowCounts), builds);
    }

    /** Load a lowered query's inputs from Parquet and run it. */
    public static LoweredResult runLowered(Allocator allocator, TpcdsParquetTables tables, org.weakref.nitro.jit.QueryLowering.Lowered lowered)
    {
        LoadedInputs loaded = loadLoweredInputs(allocator, tables, lowered);
        return new LoweredResult(lowered.compile().execute(loaded.inputs(), loaded.rowCounts()), loaded.inputs());
    }

    /**
     * Run a single-stage lowered query through the streaming path (the probe fact is streamed batch-by-batch instead
     * of drained), returning the result with the build inputs positioned for {@link CompiledTpcdsQueries.DictRef}
     * reconstruction. Needed when the probe is a huge fact (e.g. Q37/Q82 over inventory) that cannot be drained eagerly.
     */
    public static LoweredResult runStreamingPorted(Allocator allocator, TpcdsParquetTables tables, org.weakref.nitro.jit.QueryLowering.Lowered lowered)
    {
        org.weakref.nitro.jit.StreamingPipeline streaming =
                PipelineCompiler.compileStreaming(lowered.pipeline(), lowered.encodings(), lowered.nullable());
        StreamedResult streamed = streamCapturingBuilds(allocator, tables, lowered, streaming, true);
        org.weakref.nitro.jit.Column[][] inputsForDictRef = new org.weakref.nitro.jit.Column[lowered.inputs().size()][];
        for (int b = 0; b < streamed.builds().length; b++) {
            inputsForDictRef[b + 1] = streamed.builds()[b];
        }
        return new LoweredResult(streamed.result(), inputsForDictRef);
    }

    /**
     * Run a two-stage (pipeline-breaker) query: execute {@code subquery}, materialize its result into a relation,
     * and run {@code main} with that relation substituted for the input whose table name is {@code virtualTable}
     * (all other {@code main} inputs are read from Parquet). This is the decorrelated correlated-subquery shape --
     * the subquery's aggregate feeds {@code main} as a join build, exactly as the optimizer produces it.
     */
    public static LoweredResult runMultiStage(Allocator allocator, TpcdsParquetTables tables,
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
    public static LoweredResult runMultiStage(Allocator allocator, TpcdsParquetTables tables,
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
                String[] names = source.columns().stream().map(org.weakref.nitro.jit.QueryLowering.Column::name).toArray(String[]::new);
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
    public static LoweredResult runUnion(Allocator allocator, TpcdsParquetTables tables,
            List<org.weakref.nitro.jit.QueryLowering.Lowered> branches, org.weakref.nitro.jit.QueryLowering.Lowered main, String virtualTable)
    {
        return runUnion(allocator, tables, branches, main, virtualTable, List.of());
    }

    /**
     * As {@link #runUnion}, but {@code branchStringColumns} names the branch-result columns that are dictionary
     * strings (the union's group key may be a string, e.g. Q56/Q60 group by {@code i_item_id}). Each branch carries
     * its own (filtered) source dictionary, so the branch string columns are merged into one ordered unified
     * dictionary during concatenation -- ids consistent across branches, and id order = value order for ORDER BY.
     */
    public static LoweredResult runUnion(Allocator allocator, TpcdsParquetTables tables,
            List<org.weakref.nitro.jit.QueryLowering.Lowered> branches, org.weakref.nitro.jit.QueryLowering.Lowered main,
            String virtualTable, List<CompiledTpcdsQueries.DictRef> branchStringColumns)
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
            org.weakref.nitro.jit.Column[] materialized = materialize(result, dictInputs, branchStringColumns);
            parts.add(materialized);
            counts[i] = result.rowCount();
            width = materialized.length;
        }
        int total = 0;
        for (int count : counts) {
            total += count;
        }
        org.weakref.nitro.jit.Column[] union = concatenateColumns(parts, counts, total, width);

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
                String[] names = source.columns().stream().map(org.weakref.nitro.jit.QueryLowering.Column::name).toArray(String[]::new);
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
                if (ref == null || dictInputs == null || dictInputs[ref.dictInput()] == null) {
                    throw new UnsupportedOperationException("string result column " + c + " has no resolvable source dictionary");
                }
                byte[][] dictionary = ((org.weakref.nitro.jit.Column.StringColumn) dictInputs[ref.dictInput()][ref.dictColumn()]).dictionary();
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
        for (int c = 0; c < width; c++) {
            string[c] = specs.get(c).encoding() == org.weakref.nitro.jit.ColumnEncoding.STRING;
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
                                localToGlobal[entry] = intern(dictionaryIndex.get(c), dictionaries.get(c), stringBytes(dictionary, entry));
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
