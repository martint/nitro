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
package org.weakref.nitro.operator.source.compatibility.parquet;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.PageReader;
import io.trino.parquet.reader.flat.SkipFlatColumnReader;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * A Parquet scan over flat {@code BIGINT} columns that applies pushed {@link DynamicFilter}s during decode, the
 * Velox-style dynamic-filtering path. It owns one {@link SkipFlatColumnReader} per column and, for each batch,
 * applies the pushed filters in selectivity order: the most selective filter's column is decoded in full, its
 * membership narrows the batch to a small survivor set, and every remaining column (further filtered key columns
 * and payload alike) is then value-level skip-decoded for those survivors only. The emitted batch is COMPACTED to
 * the survivors (mask = all over the survivor count); since a dynamic filter is a superset over the join key, an
 * exact downstream join still produces identical output, having decoded a fraction of the fact table's bytes.
 *
 * <p>With no filters pushed it decodes every column in full, behaving like an ordinary scan.
 */
public final class SkipDecodeScanOperator
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("SkipDecodeScanOperator", SkipDecodeScanOperator.class);
    private static final String BATCH_SIZE_PROPERTY = System.getProperty("nitro.skipScan.batchSize");
    private static final int BASE_BATCH_SIZE = Math.max(1, Integer.getInteger("nitro.skipScan.batchSize", 8192));
    private static final int FILTERED_BATCH_SIZE = Math.max(1, Integer.getInteger(
            "nitro.skipScan.filteredBatchSize",
            BATCH_SIZE_PROPERTY == null ? 12288 : BASE_BATCH_SIZE));
    private static final int SCRATCH_BATCH_SIZE = Math.max(BASE_BATCH_SIZE, FILTERED_BATCH_SIZE);

    /**
     * Selectivity guard: skip-decode the non-filter columns only when the surviving fraction of the batch is below
     * this threshold; above it, bulk-decode them (and gather the survivors). Per-position skip-decode loses to bulk
     * SIMD decode above ~8% selectivity, so this keeps the dynamic-filter scan no worse than an ordinary scan when a
     * pushed filter is weakly selective (e.g. category filters), while still compacting the batch to the survivors.
     */
    private static final double SKIP_GUARD = Double.parseDouble(System.getProperty("nitro.skipScan.guard", "0.08"));

    // Raw-array accessors on LongArrayBlock (not public) for the full-decode fast path: copy the block's backing
    // long[]/null[] in bulk instead of a per-position getLong/isNull, matching TrinoParquetScanOperator's bridge.
    private static final java.lang.reflect.Method LONG_ARRAY_RAW_VALUES = declaredMethod("getRawValues");
    private static final java.lang.reflect.Method LONG_ARRAY_RAW_VALUES_OFFSET = declaredMethod("getRawValuesOffset");
    private static final java.lang.reflect.Method LONG_ARRAY_RAW_NULLS = declaredMethod("getRawValueIsNull");

    private static java.lang.reflect.Method declaredMethod(String name)
    {
        try {
            java.lang.reflect.Method method = LongArrayBlock.class.getDeclaredMethod(name);
            method.setAccessible(true);
            return method;
        }
        catch (NoSuchMethodException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    /** Opt-in decode accounting (column-reads as full-decoded vs skip-decoded rows) for measurement harnesses. */
    public static final class Profile
    {
        private static final int MAX = 32;
        private long fullRows;
        private long skipRows;
        private long rowsScanned;       // batch rows seen (= total fact rows scanned)
        private long survivorsEmitted;  // rows emitted after all filters
        private final long[] colFull = new long[MAX];   // full-decoded values per column index
        private final long[] colSkip = new long[MAX];   // skip-decoded values per column index
        private final long[] filterInput = new long[MAX];      // rows fed to the filter on column c
        private final long[] filterSurvivors = new long[MAX];  // rows surviving the filter on column c

        public long fullRows()
        {
            return fullRows;
        }

        public long skipRows()
        {
            return skipRows;
        }

        private void recordFilter(int column, int input, int survivors)
        {
            filterInput[column] += input;
            filterSurvivors[column] += survivors;
        }

        public String summary()
        {
            StringBuilder out = new StringBuilder();
            out.append(String.format("rowsScanned=%,d survivorsEmitted=%,d fullDecoded=%,d skipDecoded=%,d totalDecoded=%,d%n",
                    rowsScanned, survivorsEmitted, fullRows, skipRows, fullRows + skipRows));
            for (int c = 0; c < MAX; c++) {
                if (colFull[c] + colSkip[c] + filterInput[c] > 0) {
                    out.append(String.format("  col %d: decodedFull=%,d decodedSkip=%,d", c, colFull[c], colSkip[c]));
                    if (filterInput[c] > 0) {
                        out.append(String.format(" | filter %,d -> %,d (%.2f%% pass)", filterInput[c], filterSurvivors[c],
                                100.0 * filterSurvivors[c] / filterInput[c]));
                    }
                    out.append('\n');
                }
            }
            return out.toString();
        }
    }

    private final Allocator allocator;
    private final Profile profile;
    private final List<Path> files;
    private final List<String> columnNames;
    private final int columnCount;
    private final AggregatedMemoryContext memoryContext = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();

    // Per-column metadata (resolved once from the first file's schema).
    private final boolean[] nullable;
    private final int[] kinds;   // 0 = INT64 -> I64Vector, 1 = INT32 -> I32Vector
    private final Map<List<String>, ColumnDescriptor>[] descriptorMaps;
    private final ColumnDescriptor[] descriptors;
    private final PrimitiveField[] fields;

    // Flattened row groups across all files, in scan order.
    private final List<int[]> rowGroups = new ArrayList<>();    // {fileIndex, blockIndex}
    private final ParquetReaderFile[] openFiles;

    // Pushed dynamic filters, one per target column (null when none).
    private final DynamicFilter[] filtersByColumn;
    private boolean hasFilters;
    // Adaptive selectivity gate: a filter that does not prune is pure overhead (key decode + per-row membership test +
    // survivor gather). After a per-column warmup of filtered rows, a filter that keeps too large a fraction is
    // abandoned, so the scan reverts to the full-decode fast path for a non-selective dimension.
    private static final long FILTER_WARMUP_ROWS = 256 * 1024;
    private static final double FILTER_MIN_PRUNE_RATIO = 0.30;   // keep applying only if it removes >= 30% of its input
    private final long[] filterRowsSeen;
    private final long[] filterRowsKept;
    // A filter found non-selective is not dropped mid-row-group (the stateful column readers cannot switch between
    // skip-decode and full-decode within a row group); the drop is deferred to the next openRowGroup, where readers
    // are recreated in the correct mode.
    private final boolean[] pendingAbandon;
    // Converted dictionary value vectors, cached by the parquet dictionary block's identity (stable across a row
    // group's batches), so the full-decode path preserves dictionary encoding without re-converting the dictionary
    // per batch -- matching the shared Trino scan, which keeps downstream scalar/grouping ops encoding-aware.
    private final java.util.IdentityHashMap<Block, Vector> convertedDictionaries = new java.util.IdentityHashMap<>();

    // Per-row-group reader state.
    private final SkipFlatColumnReader<long[]>[] readers;
    private final int[] consumed;
    // Dictionary-aware filter scratch, cached PER COLUMN by the dictionary block's identity: the parquet dictionary
    // is stable across the batches of a row group, so the predicate is evaluated over its entries ONCE per row group
    // (not per batch) and the per-row test is an id -> bool array read.
    private final io.trino.spi.block.Block[] cachedDictionary;
    private final boolean[][] entryPassByColumn;
    private final boolean[][] entryNullByColumn;
    private final long[][] entryValueByColumn;
    private int rowGroupIndex = -1;
    private int rowsInRowGroup;
    private int batchStart;
    private int batchRows;
    private boolean done;
    private boolean initialized;
    // Per-column scratch reused across batches: the decoded values/nulls for a column are gathered into the (pooled)
    // output vectors in emit(), so the scratch is free to overwrite next batch. Avoids a fresh new long[]/boolean[]
    // per column per batch (gigabytes/query of short-lived garbage). Sized to the largest possible batch once.
    private long[][] valueScratch;
    private boolean[][] nullScratch;
    private int[] nextScratch;

    private Batch currentBatch;

    @SuppressWarnings("unchecked")
    public SkipDecodeScanOperator(Allocator allocator, List<Path> files, List<String> columnNames)
    {
        this(allocator, files, columnNames, null);
    }

    public SkipDecodeScanOperator(Allocator allocator, List<Path> files, List<String> columnNames, Profile profile)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.profile = profile;
        this.files = List.copyOf(files);
        this.columnNames = List.copyOf(columnNames);
        this.columnCount = this.columnNames.size();
        this.nullable = new boolean[columnCount];
        this.kinds = new int[columnCount];
        this.descriptorMaps = new Map[columnCount];
        this.descriptors = new ColumnDescriptor[columnCount];
        this.fields = new PrimitiveField[columnCount];
        this.openFiles = new ParquetReaderFile[this.files.size()];
        this.filtersByColumn = new DynamicFilter[columnCount];
        this.filterRowsSeen = new long[columnCount];
        this.filterRowsKept = new long[columnCount];
        this.pendingAbandon = new boolean[columnCount];
        this.readers = new SkipFlatColumnReader[columnCount];
        this.consumed = new int[columnCount];
        this.cachedDictionary = new io.trino.spi.block.Block[columnCount];
        this.entryPassByColumn = new boolean[columnCount][];
        this.entryNullByColumn = new boolean[columnCount][];
        this.entryValueByColumn = new long[columnCount][];
    }

    @Override
    public int outputCount()
    {
        return columnCount;
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        int column = filter.column();
        if (column < 0 || column >= columnCount || filter.isEmpty()) {
            return;
        }
        // Intersect if a filter for this column was already pushed (keep the more selective one).
        DynamicFilter existing = filtersByColumn[column];
        if (existing == null || filter.size() < existing.size()) {
            filtersByColumn[column] = filter;
        }
        hasFilters = true;
    }

    private boolean anyFilterRemains()
    {
        for (DynamicFilter filter : filtersByColumn) {
            if (filter != null) {
                return true;
            }
        }
        return false;
    }

    private boolean batchReady;

    @Override
    public boolean hasNext()
    {
        if (!initialized) {
            initialize();
        }
        if (done) {
            return false;
        }
        if (!batchReady) {
            batchReady = advanceToNextBatch();
            if (!batchReady) {
                done = true;
            }
        }
        return batchReady;
    }

    @Override
    public Batch next()
    {
        closeCurrentBatch();
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        batchReady = false;
        return produceBatch();
    }

    @Override
    public void constrain(Mask mask)
    {
        // The batch is already materialized (compacted to survivors); a further downstream constraint is honored
        // by the Batch's own mask, so there is nothing to push back into the decode.
    }

    @Override
    public void close()
    {
        closeCurrentBatch();
        for (ParquetReaderFile file : openFiles) {
            if (file != null) {
                try {
                    file.close();
                }
                catch (IOException exception) {
                    throw new UncheckedIOException("Unable to close Parquet file", exception);
                }
            }
        }
        allocator.release(allocationContext);
    }

    private void closeCurrentBatch()
    {
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }
    }

    private Batch produceBatch()
    {
        if (profile != null) {
            profile.rowsScanned += batchRows;
        }
        if (valueScratch == null) {
            valueScratch = new long[columnCount][SCRATCH_BATCH_SIZE];
            nullScratch = new boolean[columnCount][SCRATCH_BATCH_SIZE];
            nextScratch = new int[SCRATCH_BATCH_SIZE];
        }
        int[] survivors;     // batch-relative positions, sorted ascending; null means "all rows"
        long[][] values = new long[columnCount][];
        boolean[][] nulls = new boolean[columnCount][];
        int[][] readPositions = new int[columnCount][];   // the survivor set each column was read at (sorted)

        if (!hasFilters) {
            // No dynamic filter reached this scan: behave exactly like an ordinary full scan, bridging each decoded
            // block straight into an output vector (bulk array copy, no per-value gather) so there is no penalty
            // relative to the shared Trino reader.
            Output[] outputs = new Output[columnCount];
            for (int c = 0; c < columnCount; c++) {
                outputs[c] = bridgeFull(c, decode(c, null, batchRows), batchRows);
            }
            return buildBatch(outputs, batchRows);
        }

        // Apply dynamic filters in selectivity order (fewest distinct build values first).
        int[] filterColumns = filterColumnsBySelectivity();
        survivors = null;
        int survivorCount = batchRows;
        for (int column : filterColumns) {
            int count = survivors == null ? batchRows : survivorCount;
            Block block = decode(column, survivors, count);
            long[] columnValues = valueScratch[column];
            boolean[] columnNulls = nullScratch[column];
            int[] next = nextScratch;
            int kept = filterBlock(column, block, count, filtersByColumn[column], survivors, columnValues, columnNulls, next);
            values[column] = columnValues;
            nulls[column] = nullable[column] ? columnNulls : null;
            readPositions[column] = survivors;    // remembered for the final gather

            if (profile != null) {
                profile.recordFilter(column, count, kept);
            }
            filterRowsSeen[column] += count;
            filterRowsKept[column] += kept;
            if (filtersByColumn[column] != null && filterRowsSeen[column] >= FILTER_WARMUP_ROWS
                    && filterRowsKept[column] > filterRowsSeen[column] * (1.0 - FILTER_MIN_PRUNE_RATIO)) {
                // Not selective enough to pay for: flag it for drop at the next row group (cannot switch a reader's
                // decode mode mid-row-group). Once every filter is gone the no-filter full-decode fast path resumes.
                pendingAbandon[column] = true;
            }
            survivors = java.util.Arrays.copyOf(next, kept);
            survivorCount = kept;
            if (survivorCount == 0) {
                break;
            }
        }
        if (profile != null) {
            profile.survivorsEmitted += survivorCount;
        }

        // Decode the remaining (non-filter) columns for the final survivors. Skip-decode when the survivors are a small
        // fraction of the batch; otherwise bulk-decode the whole batch and gather (skip-decode loses above ~8%).
        boolean bulkDecode = survivorCount > batchRows * SKIP_GUARD;
        for (int c = 0; c < columnCount; c++) {
            if (filtersByColumn[c] != null) {
                continue;
            }
            if (bulkDecode) {
                Block block = decode(c, null, batchRows);
                long[] columnValues = valueScratch[c];
                boolean[] columnNulls = nullScratch[c];
                extract(block, batchRows, columnValues, columnNulls);
                values[c] = columnValues;
                nulls[c] = nullable[c] ? columnNulls : null;
                readPositions[c] = null;   // identity over batchRows; emit() gathers the survivors
            }
            else {
                Block block = decode(c, survivors, survivorCount);
                long[] columnValues = valueScratch[c];
                boolean[] columnNulls = nullScratch[c];
                extract(block, survivorCount, columnValues, columnNulls);
                values[c] = columnValues;
                nulls[c] = nullable[c] ? columnNulls : null;
                readPositions[c] = survivors;
            }
        }

        return emit(values, nulls, readPositions, survivors, survivorCount);
    }

    /**
     * Build the output batch, gathering every column's read-time values to the final survivor set. Filter columns
     * were read at a larger survivor set (a superset of the final one); a two-pointer gather aligns them. Columns
     * read directly at the final survivors gather as identity.
     */
    private Batch emit(long[][] values, boolean[][] nulls, int[][] readPositions, int[] survivors, int survivorCount)
    {
        Output[] outputs = new Output[columnCount];
        for (int c = 0; c < columnCount; c++) {
            BooleanVector nullVector = nullable[c]
                    ? allocator.allocate(allocationContext, BooleanVector.class, survivorCount, BooleanVector::new)
                    : null;
            boolean[] outNulls = nullVector == null ? null : nullVector.values();
            if (kinds[c] == 1) {
                I32Vector valueVector = allocator.allocate(allocationContext, I32Vector.class, survivorCount, I32Vector::new);
                long[] widened = new long[survivorCount];
                gather(values[c], nulls[c], readPositions[c], survivors, survivorCount, widened, outNulls);
                int[] outValues = valueVector.values();
                for (int j = 0; j < survivorCount; j++) {
                    outValues[j] = (int) widened[j];
                }
                outputs[c] = output(valueVector, nullVector);
            }
            else {
                I64Vector valueVector = allocator.allocate(allocationContext, I64Vector.class, survivorCount, I64Vector::new);
                long[] outValues = valueVector.values();
                gather(values[c], nulls[c], readPositions[c], survivors, survivorCount, outValues, outNulls);
                outputs[c] = output(valueVector, nullVector);
            }
        }
        return buildBatch(outputs, survivorCount);
    }

    /** Wrap the column outputs in an all-rows batch over {@code count} positions. */
    private Batch buildBatch(Output[] outputs, int count)
    {
        Mask mask = allocator.allocateAllMask(allocationContext, count);
        Batch batch = new Batch(
                mask,
                pushed -> {},
                takenMask -> allocator.transfer(allocationContext, takenMask),
                releasedMask -> allocator.release(allocationContext, releasedMask),
                () -> {},
                outputs);
        currentBatch = batch;
        return batch;
    }

    /**
     * Bridge a fully-decoded column block straight into an output vector for the no-filter fast path: copy the
     * backing array in bulk (LongArrayBlock raw values/nulls) instead of a per-position getLong/isNull, with a
     * per-position fallback for other block layouts (RLE, dictionary). INT32 columns (kind 1) narrow long→int.
     */
    private Output bridgeFull(int columnIndex, Block block, int count)
    {
        BooleanVector nullVector = nullable[columnIndex]
                ? allocator.allocate(allocationContext, BooleanVector.class, count, BooleanVector::new)
                : null;
        if (nullVector != null && block.mayHaveNull()) {
            boolean[] outNulls = nullVector.values();
            boolean[] rawNulls = rawValueIsNull(block);
            if (rawNulls != null) {
                System.arraycopy(rawNulls, rawValuesOffset(block), outNulls, 0, count);
            }
            else {
                for (int i = 0; i < count; i++) {
                    outNulls[i] = block.isNull(i);
                }
            }
        }
        else if (nullVector != null) {
            java.util.Arrays.fill(nullVector.values(), 0, count, false);
            nullVector.markAllFalse();
        }

        // Preserve dictionary/RLE encoding (like the shared Trino scan) so downstream scalar, filter and grouping
        // ops stay encoding-aware (per-entry / per-run instead of per-row). Nulls are carried in the separate
        // per-position null stream above; the value vector only needs the values.
        if (block instanceof DictionaryBlock dictionaryBlock) {
            Vector dictionaryValues = convertedDictionaries.computeIfAbsent(
                    dictionaryBlock.getDictionary(),
                    dictionary -> convertFlatValues(columnIndex, dictionary, dictionary.getPositionCount()));
            int[] rawIds = dictionaryBlock.getRawIds();
            int idsOffset = dictionaryBlock.getRawIdsOffset();
            int[] ids = idsOffset == 0 && rawIds.length == count
                    ? rawIds
                    : java.util.Arrays.copyOfRange(rawIds, idsOffset, idsOffset + count);
            return output(allocator.adopt(allocationContext, DictionaryVector.wrap(ids, dictionaryValues)), nullVector);
        }
        if (block instanceof RunLengthEncodedBlock runLengthBlock) {
            Vector value = convertFlatValues(columnIndex, runLengthBlock.getValue(), 1);
            return output(allocator.allocateSingleRunRle(allocationContext, count, value), nullVector);
        }
        return output(convertFlatValues(columnIndex, block, count), nullVector);
    }

    /** Decode a flat (non-encoded) block's values into an {@link I64Vector}/{@link I32Vector}; null entries read 0. */
    private Vector convertFlatValues(int columnIndex, Block block, int count)
    {
        long[] rawValues = rawValues(block);
        int rawOffset = rawValues == null ? 0 : rawValuesOffset(block);
        if (kinds[columnIndex] == 1) {
            I32Vector valueVector = allocator.allocate(allocationContext, I32Vector.class, count, I32Vector::new);
            int[] out = valueVector.values();
            if (rawValues != null) {
                for (int i = 0; i < count; i++) {
                    out[i] = (int) rawValues[rawOffset + i];
                }
            }
            else {
                for (int i = 0; i < count; i++) {
                    out[i] = block.isNull(i) ? 0 : (int) BIGINT.getLong(block, i);
                }
            }
            return valueVector;
        }
        I64Vector valueVector = allocator.allocate(allocationContext, I64Vector.class, count, I64Vector::new);
        long[] out = valueVector.values();
        if (rawValues != null) {
            System.arraycopy(rawValues, rawOffset, out, 0, count);
        }
        else {
            for (int i = 0; i < count; i++) {
                out[i] = block.isNull(i) ? 0 : BIGINT.getLong(block, i);
            }
        }
        return valueVector;
    }

    private static long[] rawValues(Block block)
    {
        if (block instanceof LongArrayBlock longBlock) {
            try {
                return (long[]) LONG_ARRAY_RAW_VALUES.invoke(longBlock);
            }
            catch (ReflectiveOperationException exception) {
                throw new IllegalStateException("Unable to access LongArrayBlock raw values", exception);
            }
        }
        return null;
    }

    private static int rawValuesOffset(Block block)
    {
        try {
            return (int) LONG_ARRAY_RAW_VALUES_OFFSET.invoke(block);
        }
        catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Unable to access LongArrayBlock raw offset", exception);
        }
    }

    private static boolean[] rawValueIsNull(Block block)
    {
        if (block instanceof LongArrayBlock longBlock) {
            try {
                return (boolean[]) LONG_ARRAY_RAW_NULLS.invoke(longBlock);
            }
            catch (ReflectiveOperationException exception) {
                throw new IllegalStateException("Unable to access LongArrayBlock raw nulls", exception);
            }
        }
        return null;
    }

    /** Two-pointer gather of a column's read-time values (aligned to {@code readPositions}) onto {@code survivors}. */
    private static void gather(long[] columnValues, boolean[] columnNulls, int[] readPositions, int[] survivors, int survivorCount, long[] outValues, boolean[] outNulls)
    {
        if (readPositions == null) {
            // Column read over identity positions (0..n-1) or directly at the survivor set; index by survivor position.
            if (survivors == null) {
                System.arraycopy(columnValues, 0, outValues, 0, survivorCount);
                if (outNulls != null) {
                    System.arraycopy(columnNulls, 0, outNulls, 0, survivorCount);
                }
                return;
            }
            for (int j = 0; j < survivorCount; j++) {
                int p = survivors[j];
                outValues[j] = columnValues[p];
                if (outNulls != null) {
                    outNulls[j] = columnNulls[p];
                }
            }
            return;
        }
        if (readPositions == survivors) {
            System.arraycopy(columnValues, 0, outValues, 0, survivorCount);
            if (outNulls != null) {
                System.arraycopy(columnNulls, 0, outNulls, 0, survivorCount);
            }
            return;
        }
        // readPositions and survivors are both sorted ascending; survivors ⊆ readPositions. Walk together.
        int r = 0;
        for (int j = 0; j < survivorCount; j++) {
            int target = survivors[j];
            while (readPositions[r] != target) {
                r++;
            }
            outValues[j] = columnValues[r];
            if (outNulls != null) {
                outNulls[j] = columnNulls[r];
            }
        }
    }

    /** Decode column {@code c} for the current batch: full when {@code survivors == null}, else skip-decode for survivors. */
    private Block decode(int c, int[] survivors, int count)
    {
        alignTo(c);
        if (survivors == null) {
            if (profile != null) {
                profile.fullRows += count;
                profile.colFull[c] += count;
            }
            return readers[c].readPrimitive().getBlock();
        }
        if (profile != null) {
            profile.skipRows += count;
            profile.colSkip[c] += count;
        }
        return readers[c].readSelected(survivors, count).getBlock();
    }

    private void extract(Block block, int count, long[] outValues, boolean[] outNulls)
    {
        long[] rawValues = rawValues(block);
        if (!block.mayHaveNull()) {
            if (rawValues != null) {
                System.arraycopy(rawValues, rawValuesOffset(block), outValues, 0, count);
            }
            else {
                for (int i = 0; i < count; i++) {
                    outValues[i] = BIGINT.getLong(block, i);
                }
            }
            if (outNulls != null) {
                java.util.Arrays.fill(outNulls, 0, count, false);
            }
            return;
        }
        for (int i = 0; i < count; i++) {
            boolean isNull = block.isNull(i);
            if (outNulls != null) {
                outNulls[i] = isNull;
            }
            outValues[i] = isNull ? 0 : BIGINT.getLong(block, i);
        }
    }

    /**
     * Decode {@code block} into {@code outValues}/{@code outNulls} AND apply {@code filter}, recording the surviving
     * batch-relative positions in {@code outSurvivors} (mapped through {@code inSurvivors} when this is not the first
     * stage). For a dictionary-encoded block the filter is evaluated ONCE PER DICTIONARY ENTRY (predicate over
     * dictionary) and the per-row test collapses to an {@code id -> bool} array read -- avoiding a {@code getLong}
     * and a filter probe per row, which on the wide first-filter column (tens of millions of rows) is the dominant
     * saving. Returns the surviving count.
     */
    private int filterBlock(int column, Block block, int count, DynamicFilter filter, int[] inSurvivors,
            long[] outValues, boolean[] outNulls, int[] outSurvivors)
    {
        int kept = 0;
        if (block instanceof DictionaryBlock dictionaryBlock) {
            Block entries = dictionaryBlock.getDictionary();
            // The parquet dictionary is stable across a row group's batches: evaluate the predicate over its entries
            // once and cache by identity, so per-batch cost is just the id -> bool lookups below.
            if (cachedDictionary[column] != entries) {
                int dictionarySize = entries.getPositionCount();
                boolean[] pass = new boolean[dictionarySize];
                boolean[] entryNull = new boolean[dictionarySize];
                long[] entryValue = new long[dictionarySize];
                for (int e = 0; e < dictionarySize; e++) {
                    boolean isNull = entries.isNull(e);
                    entryNull[e] = isNull;
                    long value = isNull ? 0 : BIGINT.getLong(entries, e);
                    entryValue[e] = value;
                    pass[e] = !isNull && filter.accepts(value);
                }
                cachedDictionary[column] = entries;
                entryPassByColumn[column] = pass;
                entryNullByColumn[column] = entryNull;
                entryValueByColumn[column] = entryValue;
            }
            boolean[] entryPass = entryPassByColumn[column];
            boolean[] entryNull = entryNullByColumn[column];
            long[] entryValue = entryValueByColumn[column];
            int[] ids = dictionaryBlock.getRawIds();
            int idsOffset = dictionaryBlock.getRawIdsOffset();
            for (int i = 0; i < count; i++) {
                int id = ids[idsOffset + i];
                outNulls[i] = entryNull[id];
                outValues[i] = entryValue[id];
                if (entryPass[id]) {
                    outSurvivors[kept++] = inSurvivors == null ? i : inSurvivors[i];
                }
            }
            return kept;
        }
        // Non-dictionary block: decode per value, then a bitset/range membership test per row.
        extract(block, count, outValues, outNulls);
        for (int i = 0; i < count; i++) {
            if (!outNulls[i] && filter.accepts(outValues[i])) {
                outSurvivors[kept++] = inSurvivors == null ? i : inSurvivors[i];
            }
        }
        return kept;
    }

    /** Filter target columns ordered by ascending distinct-value count (apply the most selective first). */
    private int[] filterColumnsBySelectivity()
    {
        int count = 0;
        for (DynamicFilter filter : filtersByColumn) {
            if (filter != null) {
                count++;
            }
        }
        int[] columns = new int[count];
        int index = 0;
        for (int c = 0; c < columnCount; c++) {
            if (filtersByColumn[c] != null) {
                columns[index++] = c;
            }
        }
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
        return columns;
    }

    /** Catch a lagging column up to the current batch start with a cheap decoder skip, then read {@code batchRows}. */
    private void alignTo(int c)
    {
        int gap = batchStart - consumed[c];
        if (gap > 0) {
            readers[c].prepareNextRead(gap);
        }
        readers[c].prepareNextRead(batchRows);
        consumed[c] = batchStart + batchRows;
    }

    /** Position at the next non-empty batch; returns false when the scan is exhausted. */
    private boolean advanceToNextBatch()
    {
        if (rowGroupIndex < 0) {
            rowGroupIndex = 0;
            if (rowGroups.isEmpty()) {
                return false;
            }
            openRowGroup(rowGroupIndex);
        }
        else {
            batchStart += batchRows;
        }
        while (batchStart >= rowsInRowGroup) {
            rowGroupIndex++;
            if (rowGroupIndex >= rowGroups.size()) {
                return false;
            }
            openRowGroup(rowGroupIndex);
        }
        int targetBatchSize = hasFilters ? FILTERED_BATCH_SIZE : BASE_BATCH_SIZE;
        batchRows = Math.min(targetBatchSize, rowsInRowGroup - batchStart);
        return batchRows > 0;
    }

    private Output output(Vector valueVector, BooleanVector nullVector)
    {
        Set<Stream> streams = nullVector == null ? Set.of(Stream.VALUES) : Set.of(Stream.VALUES, Stream.NULLS);
        return new Output(
                streams,
                stream -> switch (stream) {
                    case VALUES -> valueVector;
                    case NULLS -> requireNonNull(nullVector, "NULLS stream is absent");
                    default -> throw new IllegalArgumentException("Output does not expose stream: " + stream);
                },
                (stream, vector) -> allocator.transfer(allocationContext, vector),
                (stream, vector) -> allocator.release(allocationContext, vector));
    }

    /**
     * Whether every requested column of the first file is a flat {@code INT64} or {@code INT32} column — the only
     * shapes this scan can decode. Lets a harness route eligible tables to the skip scan and fall back otherwise.
     */
    public static boolean isEligible(List<Path> files, List<String> columns)
    {
        if (files.isEmpty()) {
            return false;
        }
        try (ParquetReaderFile first = new ParquetReaderFile(files.get(0))) {
            MessageType schema = first.metadata.getFileMetaData().getSchema();
            for (String name : columns) {
                org.apache.parquet.schema.Type type = schema.getType(name);
                if (!type.isPrimitive()) {
                    return false;
                }
                // Short decimal (any physical encoding) is read into a long lane; accept it.
                if (type.getLogicalTypeAnnotation() instanceof org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
                    if (decimal.getPrecision() > io.trino.spi.type.Decimals.MAX_SHORT_PRECISION) {
                        return false;
                    }
                    continue;
                }
                // Plain INT64/INT32 (no logical annotation other than the integer width).
                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName physical = type.asPrimitiveType().getPrimitiveTypeName();
                if (physical != org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
                        && physical != org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32) {
                    return false;
                }
            }
            return true;
        }
        catch (RuntimeException | IOException exception) {
            return false;
        }
    }

    private void initialize()
    {
        try {
            ParquetReaderFile first = file(0);
            MessageType schema = first.metadata.getFileMetaData().getSchema();
            for (int c = 0; c < columnCount; c++) {
                String name = columnNames.get(c);
                org.apache.parquet.schema.Type type = schema.getType(name);
                if (!type.isPrimitive()) {
                    throw new IllegalArgumentException("SkipDecodeScanOperator supports only flat numeric columns: " + name);
                }
                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName physical = type.asPrimitiveType().getPrimitiveTypeName();
                Type trinoType;
                if (type.getLogicalTypeAnnotation() instanceof org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal
                        && decimal.getPrecision() <= io.trino.spi.type.Decimals.MAX_SHORT_PRECISION) {
                    // Short decimal (any physical encoding) reads into a long lane, like the page source -> I64Vector.
                    kinds[c] = 2;
                    trinoType = io.trino.spi.type.DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
                }
                else {
                    switch (physical) {
                        case INT64 -> kinds[c] = 0;
                        case INT32 -> kinds[c] = 1;
                        default -> throw new IllegalArgumentException("SkipDecodeScanOperator supports only INT64/INT32/short-decimal columns: " + name);
                    }
                    trinoType = BIGINT;
                }
                nullable[c] = type.getRepetition() != org.apache.parquet.schema.Type.Repetition.REQUIRED;
                MessageType requested = new MessageType(schema.getName(), schema.getType(name));
                descriptorMaps[c] = getDescriptors(schema, requested);
                descriptors[c] = descriptorMaps[c].values().iterator().next();
                MessageColumnIO columnIO = getColumnIO(schema, requested);
                fields[c] = (PrimitiveField) constructField(trinoType, lookupColumnByName(columnIO, name)).orElseThrow();
            }
            for (int f = 0; f < files.size(); f++) {
                ParquetReaderFile reader = file(f);
                List<BlockMetadata> blocks = reader.metadata.getBlocks();
                for (int b = 0; b < blocks.size(); b++) {
                    rowGroups.add(new int[] {f, b});
                }
            }
            done = rowGroups.isEmpty();
            initialized = true;
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to open Parquet metadata", exception);
        }
    }

    private void openRowGroup(int index)
    {
        // Apply any deferred filter abandons now, while readers are about to be recreated: a non-selective filter
        // dropped here lets its column open in flat full-decode mode for this and every later row group.
        for (int c = 0; c < columnCount; c++) {
            if (pendingAbandon[c] && filtersByColumn[c] != null) {
                filtersByColumn[c] = null;
            }
        }
        hasFilters = anyFilterRemains();
        try {
            int[] location = rowGroups.get(index);
            ParquetReaderFile reader = file(location[0]);
            BlockMetadata block = reader.metadata.getBlocks().get(location[1]);
            for (int c = 0; c < columnCount; c++) {
                PrunedBlockMetadata pruned = PrunedBlockMetadata.createPrunedColumnsMetadata(block, reader.source.getId(), descriptorMaps[c]);
                ColumnChunkMetadata chunkMeta = pruned.getColumnChunkMetaData(descriptors[c]);
                ListMultimap<Integer, DiskRange> diskRanges = ArrayListMultimap.create();
                diskRanges.put(0, new DiskRange(chunkMeta.getStartingPos(), chunkMeta.getTotalSize()));
                Map<Integer, ChunkedInputStream> chunks = reader.source.planRead(diskRanges, memoryContext);
                PageReader pageReader = PageReader.createPageReader(reader.source.getId(), chunks.get(0), chunkMeta, descriptors[c],
                        null, Optional.empty(), Optional.empty(), 8 * 1024 * 1024);
                SkipFlatColumnReader<long[]> columnReader = switch (kinds[c]) {
                    case 1 -> SkipFlatColumnReader.createForInt32AsLong(fields[c], true, memoryContext.newLocalMemoryContext("skip"));
                    case 2 -> SkipFlatColumnReader.createForShortDecimal(fields[c], true, memoryContext.newLocalMemoryContext("skip"));
                    default -> SkipFlatColumnReader.createForLong(fields[c], true, memoryContext.newLocalMemoryContext("skip"));
                };
                // Always preserve encoding on the no-filter full-decode path (bridgeFull emits DictionaryVector/
                // RleVector), and for filter columns (predicate-over-dictionary in filterBlock). Downstream operators
                // are expected to exploit the encoding; where one doesn't yet, that operator is the thing to fix.
                // (A column going through the filter+gather path flattens regardless, since survivors gather to flat.)
                columnReader.setPreserveDictionary(filtersByColumn[c] != null || !hasFilters);
                columnReader.setPageReader(pageReader, Optional.empty());
                readers[c] = columnReader;
                consumed[c] = 0;
            }
            rowsInRowGroup = toIntExact(block.rowCount());
            batchStart = 0;
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to open row group", exception);
        }
    }

    private ParquetReaderFile file(int index)
            throws IOException
    {
        ParquetReaderFile reader = openFiles[index];
        if (reader == null) {
            reader = new ParquetReaderFile(files.get(index));
            openFiles[index] = reader;
        }
        return reader;
    }

    private static final class ParquetReaderFile
            implements AutoCloseable
    {
        private final FileDataSource source;
        private final ParquetMetadata metadata;

        private ParquetReaderFile(Path path)
                throws IOException
        {
            this.source = new FileDataSource(path.toFile(), ParquetReaderOptions.builder().build());
            this.metadata = MetadataReader.readFooter(source, Optional.empty());
        }

        @Override
        public void close()
                throws IOException
        {
            source.close();
        }
    }

    private static final class FileDataSource
            extends AbstractParquetDataSource
    {
        private final RandomAccessFile input;

        private FileDataSource(File file, ParquetReaderOptions options)
                throws java.io.FileNotFoundException
        {
            super(new ParquetDataSourceId(file.toString()), file.length(), options);
            this.input = new RandomAccessFile(file, "r");
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            input.seek(position);
            input.readFully(buffer, bufferOffset, bufferLength);
        }

        @Override
        public void close()
                throws IOException
        {
            input.close();
        }
    }
}
