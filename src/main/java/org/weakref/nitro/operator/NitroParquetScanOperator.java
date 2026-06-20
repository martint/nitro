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
package org.weakref.nitro.operator;

import org.apache.parquet.format.RowGroup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.parquet.ColumnReader;
import org.weakref.nitro.parquet.ParquetFile;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * A Nitro-native Parquet scan operator built on {@link org.weakref.nitro.parquet} — mmap input, full
 * buffer reuse, and decoding straight into Nitro's flat value arrays (no per-position bridge, no
 * intermediate copies). This is the apples-to-apples counterpart of {@link TrinoParquetScanOperator}'s
 * full-decode path for measuring the decoder rewrite.
 *
 * <p>First-slice scope: flat INT32/INT64 columns; produces {@code all()}-mask batches of up to 512 rows.
 */
public final class NitroParquetScanOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("NitroParquetScanOperator");
    // Match TrinoParquetScanOperator's default so the per-batch operator overhead (Output objects, pooled
    // vector alloc/release) is amortized over the same number of batches in an apples-to-apples comparison.
    private static final int MAX_BATCH_ROWS = Integer.getInteger("nitro.parquet.scan.maxBatchRows", 10_000);

    private final Allocator allocator;
    private final List<String> columnNames;
    private final ParquetFile[] files;
    private final ColumnReader[] readers;
    private final boolean[] nullable;
    private final long totalRows;

    private final boolean allNumeric;
    private final DynamicFilter[] filtersByColumn;
    private boolean hasFilters;
    // Per-column decode scratch (grows to high-water mark; reused across batches). A column is decoded into
    // colLong or colInt (by kind) at whatever survivor set it is read at; readPositions records that set so the
    // final emit gathers each column to the surviving rows. Filter columns are read progressively: the most
    // selective one full, then each next one only at the running survivor set.
    private final long[][] colLong;
    private final int[][] colInt;
    private final boolean[][] colNull;
    private final int[][] readPositions;
    private int[] filterOrder;
    private int[] nextSurvivors = new int[0];

    // The dynamic-filter path decodes a large window (page-spanning, decoupled from the output batch) so a
    // clustered lead filter lets the skip path drop whole non-surviving pages even when a page is larger than an
    // output batch. The window's surviving rows are buffered densely and then sliced into MAX_BATCH_ROWS output
    // batches. The window must comfortably exceed a Parquet page (~100K+ rows) for page-skip to be effective.
    private static final int FILTER_WINDOW = Integer.getInteger("nitro.parquet.scan.filterWindow", 1 << 20);
    // Densely-packed surviving values for the current window, per column (grown to high-water mark); sliced out.
    private final long[][] windowLong;
    private final int[][] windowInt;
    private final boolean[][] windowNull;
    private int windowSurvivorCount;
    private int windowSurvivorCursor;

    private long nextRow;
    private Batch currentBatch;
    private Vector[] currentValues;
    private Vector[] currentNulls;

    public NitroParquetScanOperator(Allocator allocator, List<Path> paths, List<String> columns)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.columnNames = List.copyOf(columns);
        checkArgument(!paths.isEmpty(), "paths is empty");

        this.files = paths.stream().map(ParquetFile::open).toArray(ParquetFile[]::new);
        int columnCount = columns.size();
        this.readers = new ColumnReader[columnCount];
        this.nullable = new boolean[columnCount];

        for (int c = 0; c < columnCount; c++) {
            ParquetFile.Column first = files[0].column(columns.get(c));
            readers[c] = new ColumnReader(first.type(), first.optional(), first.typeLength(), first.decimal());
            nullable[c] = first.optional();
        }
        long rows = 0;
        for (ParquetFile file : files) {
            for (int c = 0; c < columnCount; c++) {
                ParquetFile.Column column = file.column(columns.get(c));
                for (RowGroup rowGroup : file.rowGroups()) {
                    readers[c].addChunk(file.data(), file.columnChunk(rowGroup, column).meta_data);
                }
            }
            rows += file.numRows();
        }
        this.totalRows = rows;

        boolean numeric = true;
        for (ColumnReader reader : readers) {
            if (reader.kind() == ColumnReader.Kind.BINARY) {
                numeric = false;
            }
        }
        this.allNumeric = numeric;
        this.filtersByColumn = new DynamicFilter[columnCount];
        this.colLong = new long[columnCount][];
        this.colInt = new int[columnCount][];
        this.colNull = new boolean[columnCount][];
        this.readPositions = new int[columnCount][];
        this.windowLong = new long[columnCount][];
        this.windowInt = new int[columnCount][];
        this.windowNull = new boolean[columnCount][];
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        // Only all-numeric scans take the skip-decode DF path (mirrors SkipDecodeScanOperator's eligibility):
        // the survivor payload is then guaranteed INT/LONG, so readSelectedInts/Longs cover it.
        if (!allNumeric) {
            return;
        }
        int column = filter.column();
        if (column < 0 || column >= readers.length) {
            return;
        }
        filtersByColumn[column] = filter;
        hasFilters = true;
    }

    @Override
    public int outputCount()
    {
        return columnNames.size();
    }

    @Override
    public boolean hasNext()
    {
        return hasFilters ? ensureWindow() : nextRow < totalRows;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more Parquet rows");
        }
        closeCurrentBatch();

        if (hasFilters) {
            return emitSlice();
        }
        int count = toIntExact(Math.min(MAX_BATCH_ROWS, totalRows - nextRow));
        nextRow += count;
        return fullBatch(count);
    }

    /** Decode windows until one yields surviving rows (or input is exhausted). Returns whether rows are available. */
    private boolean ensureWindow()
    {
        if (windowSurvivorCursor < windowSurvivorCount) {
            return true;
        }
        while (nextRow < totalRows) {
            int windowCount = toIntExact(Math.min(FILTER_WINDOW, totalRows - nextRow));
            nextRow += windowCount;
            decodeFilterWindow(windowCount);
            windowSurvivorCursor = 0;
            if (windowSurvivorCount > 0) {
                return true;
            }
        }
        return false;
    }

    private Batch fullBatch(int count)
    {
        int columnCount = readers.length;
        currentValues = new Vector[columnCount];
        currentNulls = new Vector[columnCount];
        Output[] outputs = new Output[columnCount];

        for (int c = 0; c < columnCount; c++) {
            ColumnReader reader = readers[c];
            BooleanVector nullVector = nullable[c]
                    ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, count, BooleanVector::new)
                    : null;
            boolean[] nulls = nullVector == null ? null : nullVector.values();
            Vector valueVector = switch (reader.kind()) {
                case INT -> {
                    I32Vector vector = allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, count, I32Vector::new);
                    reader.readInts(vector.values(), nulls, count);
                    yield vector;
                }
                case LONG -> {
                    I64Vector vector = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, count, I64Vector::new);
                    reader.readLongs(vector.values(), nulls, count);
                    yield vector;
                }
                case BINARY -> {
                    org.weakref.nitro.data.BinaryVector vector = reader.readBinary(nulls, count);
                    vector.addTraits(java.util.Set.of(org.weakref.nitro.data.Utf8Traits.UTF8_STRING));
                    yield allocator.adopt(ALLOCATION_CONTEXT, vector);
                }
            };
            currentValues[c] = valueVector;
            currentNulls[c] = nullVector;

            int columnIndex = c;
            outputs[c] = new Output(
                    nullVector == null ? Set.of(Stream.VALUES) : Set.of(Stream.VALUES, Stream.NULLS),
                    stream -> switch (stream) {
                        case VALUES -> currentValues[columnIndex];
                        case NULLS -> requireNonNull(currentNulls[columnIndex], "NULLS stream is absent");
                        default -> throw new IllegalArgumentException("Output does not expose stream: " + stream);
                    },
                    (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector),
                    (stream, vector) -> allocator.release(ALLOCATION_CONTEXT, vector));
        }

        Mask mask = allocator.allocateAllMask(ALLOCATION_CONTEXT, count);
        Batch batch = new Batch(
                mask,
                ignored -> {},
                takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                releasedMask -> allocator.release(ALLOCATION_CONTEXT, releasedMask),
                () -> {},
                outputs);
        currentBatch = batch;
        return batch;
    }

    /**
     * Decode one filter window with progressive narrowing, leaving its surviving rows densely packed in the
     * {@code window*} scratch for {@link #emitSlice} to hand out in output-sized batches. The filter columns are
     * applied in selectivity order (fewest distinct build values first): the most selective one is full-decoded,
     * and every subsequent filter column is read <em>only at the running survivor set</em> via the skip-decode
     * path — so a clustered lead filter collapses survivors to a near-contiguous range and the later (often huge)
     * columns get their non-surviving pages skipped without decompression. A window spans many Parquet pages so
     * that page-skip is effective regardless of page size. Every column reader advances by exactly {@code count}.
     */
    private void decodeFilterWindow(int count)
    {
        int columnCount = readers.length;
        int[] order = filterOrder();
        ensureScratch(count);

        int[] survivors = null;     // window-relative positions, sorted ascending; null means "all count rows"
        int survivorCount = count;
        int applied = 0;
        for (; applied < order.length && survivorCount > 0; applied++) {
            int column = order[applied];
            DynamicFilter filter = filtersByColumn[column];
            int kept;
            if (survivors == null) {
                // Lead filter: predicate-over-dictionary. Read ids + test a per-chunk acceptById[] WITHOUT
                // materializing the column; only survivors get a value. Output is dense, aligned to the survivors
                // (readPositions records that), so the later gather two-pointers it to the final survivor set.
                boolean[] cn = nullable[column] ? colNull[column] : null;
                if (readers[column].kind() == ColumnReader.Kind.LONG) {
                    kept = readers[column].filterDictLongs(filter::accepts, count, nextSurvivors, colLong[column], cn);
                }
                else {
                    kept = readers[column].filterDictInts(filter::accepts, count, nextSurvivors, colInt[column], cn);
                }
                survivors = java.util.Arrays.copyOf(nextSurvivors, kept);
                readPositions[column] = survivors;
                survivorCount = kept;
                continue;
            }

            int rows = survivorCount;
            readPositions[column] = survivors;
            readColumnInto(column, survivors, rows, count);
            boolean isLong = readers[column].kind() == ColumnReader.Kind.LONG;
            boolean[] nulls = nullable[column] ? colNull[column] : null;
            long[] longValues = colLong[column];
            int[] intValues = colInt[column];
            int[] next = nextSurvivors;
            kept = 0;
            for (int i = 0; i < rows; i++) {
                if (nulls != null && nulls[i]) {
                    continue;
                }
                if (filter.accepts(isLong ? longValues[i] : intValues[i])) {
                    next[kept++] = survivors[i];
                }
            }
            survivors = java.util.Arrays.copyOf(next, kept);
            survivorCount = kept;
        }

        // If a filter wiped out the window, the remaining filter columns were never read — advance their readers
        // so they stay aligned to the window boundary for the next call.
        for (int i = applied; i < order.length; i++) {
            advanceColumn(order[i], count);
        }

        // Decode the payload (non-filter) columns at the final survivors, reading DIRECTLY into the dense window
        // buffer (no separate gather copy — the survivor set is already the final one). Always via the skip path:
        // the bulk-vs-skip decision must NOT vary per window, because mixing readSelected (skip) and readInts/
        // readLongs (full) on the same reader leaves incompatible page-cursor state and corrupts a later window.
        for (int c = 0; c < columnCount; c++) {
            if (filtersByColumn[c] != null) {
                continue;
            }
            boolean[] nulls = nullable[c] ? ensureWindowNull(c, survivorCount) : null;
            if (readers[c].kind() == ColumnReader.Kind.INT) {
                windowInt[c] = ensureInt(windowInt[c], survivorCount);
                readers[c].readSelectedInts(survivors, survivorCount, count, windowInt[c], nulls);
            }
            else {
                windowLong[c] = ensureLong(windowLong[c], survivorCount);
                readers[c].readSelectedLongs(survivors, survivorCount, count, windowLong[c], nulls);
            }
        }

        // Gather the FILTER columns (read at a wider survivor superset) down to the final survivors. Payload columns
        // were already read straight into the window buffer above.
        for (int c = 0; c < columnCount; c++) {
            if (filtersByColumn[c] == null) {
                continue;
            }
            boolean[] nulls = nullable[c] ? ensureWindowNull(c, survivorCount) : null;
            if (readers[c].kind() == ColumnReader.Kind.INT) {
                windowInt[c] = ensureInt(windowInt[c], survivorCount);
                gatherInt(colInt[c], colNull[c], readPositions[c], survivors, survivorCount, windowInt[c], nulls);
            }
            else {
                windowLong[c] = ensureLong(windowLong[c], survivorCount);
                gatherLong(colLong[c], colNull[c], readPositions[c], survivors, survivorCount, windowLong[c], nulls);
            }
        }
        windowSurvivorCount = survivorCount;
    }

    /** Hand out the next {@code MAX_BATCH_ROWS} surviving rows of the current window as an all-rows output batch. */
    private Batch emitSlice()
    {
        int columnCount = readers.length;
        int start = windowSurvivorCursor;
        int sliceCount = Math.min(MAX_BATCH_ROWS, windowSurvivorCount - start);
        windowSurvivorCursor += sliceCount;

        currentValues = new Vector[columnCount];
        currentNulls = new Vector[columnCount];
        Output[] outputs = new Output[columnCount];
        for (int c = 0; c < columnCount; c++) {
            BooleanVector nullVector = nullable[c]
                    ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, sliceCount, BooleanVector::new)
                    : null;
            Vector valueVector;
            if (readers[c].kind() == ColumnReader.Kind.INT) {
                I32Vector vector = allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, sliceCount, I32Vector::new);
                System.arraycopy(windowInt[c], start, vector.values(), 0, sliceCount);
                valueVector = vector;
            }
            else {
                I64Vector vector = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, sliceCount, I64Vector::new);
                System.arraycopy(windowLong[c], start, vector.values(), 0, sliceCount);
                valueVector = vector;
            }
            if (nullVector != null) {
                System.arraycopy(windowNull[c], start, nullVector.values(), 0, sliceCount);
            }
            currentValues[c] = valueVector;
            currentNulls[c] = nullVector;
            int columnIndex = c;
            outputs[c] = new Output(
                    nullVector == null ? Set.of(Stream.VALUES) : Set.of(Stream.VALUES, Stream.NULLS),
                    stream -> switch (stream) {
                        case VALUES -> currentValues[columnIndex];
                        case NULLS -> requireNonNull(currentNulls[columnIndex], "NULLS stream is absent");
                        default -> throw new IllegalArgumentException("Output does not expose stream: " + stream);
                    },
                    (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector),
                    (stream, vector) -> allocator.release(ALLOCATION_CONTEXT, vector));
        }
        Mask mask = allocator.allocateAllMask(ALLOCATION_CONTEXT, sliceCount);
        Batch batch = new Batch(
                mask,
                ignored -> {},
                takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                releasedMask -> allocator.release(ALLOCATION_CONTEXT, releasedMask),
                () -> {},
                outputs);
        currentBatch = batch;
        return batch;
    }

    /** Decode {@code column} into its scratch buffer: full when {@code survivors == null}, else at those positions. */
    private void readColumnInto(int column, int[] survivors, int rows, int batchRows)
    {
        ColumnReader reader = readers[column];
        boolean[] nulls = nullable[column] ? colNull[column] : null;
        if (reader.kind() == ColumnReader.Kind.LONG) {
            if (survivors == null) {
                reader.readLongs(colLong[column], nulls, batchRows);
            }
            else {
                reader.readSelectedLongs(survivors, rows, batchRows, colLong[column], nulls);
            }
        }
        else {
            if (survivors == null) {
                reader.readInts(colInt[column], nulls, batchRows);
            }
            else {
                reader.readSelectedInts(survivors, rows, batchRows, colInt[column], nulls);
            }
        }
    }

    /** Advance a reader past the batch without keeping any values (used when a prior filter emptied the batch). */
    private void advanceColumn(int column, int batchRows)
    {
        readColumnInto(column, EMPTY, 0, batchRows);
    }

    private static final int[] EMPTY = new int[0];

    private int[] filterOrder()
    {
        if (filterOrder == null) {
            int n = 0;
            for (DynamicFilter filter : filtersByColumn) {
                if (filter != null) {
                    n++;
                }
            }
            int[] columns = new int[n];
            int index = 0;
            for (int c = 0; c < filtersByColumn.length; c++) {
                if (filtersByColumn[c] != null) {
                    columns[index++] = c;
                }
            }
            // Insertion sort by distinct-build-value count ascending (most selective first).
            for (int i = 1; i < columns.length; i++) {
                int key = columns[i];
                int keySize = filtersByColumn[key].size();
                int j = i - 1;
                while (j >= 0 && filtersByColumn[columns[j]].size() > keySize) {
                    columns[j + 1] = columns[j];
                    j--;
                }
                columns[j + 1] = key;
            }
            filterOrder = columns;
        }
        return filterOrder;
    }

    private static long[] ensureLong(long[] array, int size)
    {
        return array == null || array.length < size ? new long[size] : array;
    }

    private static int[] ensureInt(int[] array, int size)
    {
        return array == null || array.length < size ? new int[size] : array;
    }

    private boolean[] ensureWindowNull(int column, int size)
    {
        if (windowNull[column] == null || windowNull[column].length < size) {
            windowNull[column] = new boolean[size];
        }
        return windowNull[column];
    }

    private void ensureScratch(int count)
    {
        if (nextSurvivors.length < count) {
            nextSurvivors = new int[count];
        }
        for (int c = 0; c < readers.length; c++) {
            if (readers[c].kind() == ColumnReader.Kind.LONG) {
                if (colLong[c] == null || colLong[c].length < count) {
                    colLong[c] = new long[count];
                }
            }
            else if (colInt[c] == null || colInt[c].length < count) {
                colInt[c] = new int[count];
            }
            if (nullable[c] && (colNull[c] == null || colNull[c].length < count)) {
                colNull[c] = new boolean[count];
            }
        }
    }

    /**
     * Gather a decoded column to the final survivor set. {@code readPositions} is the sorted set the values were
     * read at ({@code null} = identity over batch positions); {@code survivors} ⊆ {@code readPositions}.
     */
    private static void gatherLong(long[] values, boolean[] nulls, int[] readPositions, int[] survivors, int survivorCount, long[] out, boolean[] outNulls)
    {
        if (survivors == null) {
            // No filter survived: identity over the whole window (every row is a survivor).
            System.arraycopy(values, 0, out, 0, survivorCount);
            if (outNulls != null && nulls != null) {
                System.arraycopy(nulls, 0, outNulls, 0, survivorCount);
            }
            return;
        }
        if (readPositions == null) {
            for (int j = 0; j < survivorCount; j++) {
                int p = survivors[j];
                out[j] = values[p];
                if (outNulls != null) {
                    outNulls[j] = nulls != null && nulls[p];
                }
            }
            return;
        }
        int r = 0;
        for (int j = 0; j < survivorCount; j++) {
            int target = survivors[j];
            while (readPositions[r] < target) {
                r++;
            }
            out[j] = values[r];
            if (outNulls != null) {
                outNulls[j] = nulls != null && nulls[r];
            }
        }
    }

    /** INT counterpart of {@link #gatherLong}. */
    private static void gatherInt(int[] values, boolean[] nulls, int[] readPositions, int[] survivors, int survivorCount, int[] out, boolean[] outNulls)
    {
        if (survivors == null) {
            System.arraycopy(values, 0, out, 0, survivorCount);
            if (outNulls != null && nulls != null) {
                System.arraycopy(nulls, 0, outNulls, 0, survivorCount);
            }
            return;
        }
        if (readPositions == null) {
            for (int j = 0; j < survivorCount; j++) {
                int p = survivors[j];
                out[j] = values[p];
                if (outNulls != null) {
                    outNulls[j] = nulls != null && nulls[p];
                }
            }
            return;
        }
        int r = 0;
        for (int j = 0; j < survivorCount; j++) {
            int target = survivors[j];
            while (readPositions[r] < target) {
                r++;
            }
            out[j] = values[r];
            if (outNulls != null) {
                outNulls[j] = nulls != null && nulls[r];
            }
        }
    }

    @Override
    public void constrain(Mask mask) {}

    @Override
    public boolean supportsRetainedBatches()
    {
        return false;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        return false;
    }

    private boolean closed;

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        closeCurrentBatch();
        for (ColumnReader reader : readers) {
            reader.close();
        }
        for (ParquetFile file : files) {
            file.close();
        }
        allocator.release(ALLOCATION_CONTEXT);
    }

    private void closeCurrentBatch()
    {
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }
    }
}
