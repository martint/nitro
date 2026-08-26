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
package org.weakref.nitro.parquet;

import org.apache.parquet.format.RowGroup;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.function.VersionedLongPredicate;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.LongDomain;
import org.weakref.nitro.core.source.LongDomainCapability;
import org.weakref.nitro.core.source.OrdinalSourceColumnHandle;
import org.weakref.nitro.core.source.RuntimeFilter;
import org.weakref.nitro.core.source.RuntimeFilterAcceptance;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.core.source.SourceMetrics;
import org.weakref.nitro.core.source.SourceMetricsProtocol;
import org.weakref.nitro.core.source.SourceOutputDemand;
import org.weakref.nitro.core.source.SourceOutputDemandProtocol;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.source.SourceProtocol;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BatchBufferOwner;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorBatchScope;
import org.weakref.nitro.data.VectorColumnGeneration;
import org.weakref.nitro.data.VectorSourceBatch;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.addExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * A Parquet batch source that decodes compressed column chunks directly into Nitro vectors. Inputs come from
 * connector-supplied range I/O or an optional local mapping; deferred columns do not fetch their chunks until they
 * are consumed.
 */
public final class NitroParquetBatchSource
        implements BatchSource
{
    public record Split(Path path, long start, long length)
    {
        public Split
        {
            requireNonNull(path, "path is null");
            checkArgument(start >= 0, "start is negative");
            checkArgument(length >= 0, "length is negative");
        }

        public static Split wholeFile(Path path)
        {
            return new Split(path, 0, Long.MAX_VALUE);
        }
    }

    /** Transfers ownership of {@code input} to the batch source. */
    public record InputSplit(ParquetInput input, long start, long length)
    {
        public InputSplit
        {
            requireNonNull(input, "input is null");
            checkArgument(start >= 0, "start is negative");
            checkArgument(length >= 0, "length is negative");
        }
    }

    private sealed interface SourceSplit
            permits PathSourceSplit, InputSourceSplit
    {
        static SourceSplit path(Split split)
        {
            return new PathSourceSplit(split);
        }

        static SourceSplit input(InputSplit split)
        {
            return new InputSourceSplit(split);
        }

        String id();

        long start();

        long length();
    }

    private record PathSourceSplit(Path path, long start, long length)
            implements SourceSplit
    {
        private PathSourceSplit(Split split)
        {
            this(split.path(), split.start(), split.length());
        }

        @Override
        public String id()
        {
            return path.toAbsolutePath().normalize().toString();
        }
    }

    private record InputSourceSplit(ParquetInput input, long start, long length)
            implements SourceSplit
    {
        private InputSourceSplit(InputSplit split)
        {
            this(split.input(), split.start(), split.length());
        }

        @Override
        public String id()
        {
            return input.id();
        }
    }

    private record Splits(List<SourceSplit> values)
    {
        private Splits
        {
            values = List.copyOf(values);
        }
    }

    // A wide, all-numeric scan with no downstream constraint is usually feeding a cardinality-preserving operator.
    // In that shape, removing the page-sized materialization buffer can increase TLB pressure in the consumer even
    // though it saves allocation in the decoder. Wait for runtime evidence that a downstream operator is narrowing
    // the stream before enabling direct decode. Mixed scans retain immediate admission: their binary readers already
    // preserve page materialization, so the numeric decoder is not removing the pipeline's last page-local buffer.
    // Repeated scans of the same large physical column have already paid for independent reader state and will
    // revisit the same page-sized materialization. Decode dictionary/plain data directly into each public batch:
    // this removes that intermediate flat page while preserving the receiver's ordinary flat-vector contract.
    // Admission is execution-local and keyed only by normalized file/physical-column identity.
    // Late materialization (non-DF scans): defer per-column decode until the column is pulled, and once a filter
    // above the scan pushes a survivor mask via constrain(), decode the remaining columns only for survivor rows
    // (skip-decode + scatter to position) instead of every row. Mirrors TrinoParquetScanOperator's masked path.
    // A very sparse numeric mask can profit even when survivors are isolated: decoding the compact page IDs once
    // and gathering only live values avoids materializing/copying every wide value. Require multiple payloads to
    // amortize the alternate scan lifecycle, but cap their count because page-ID setup repeated over a very wide
    // projection retires more work than it saves. Keep the ordinary run-length proof for binary and denser numeric
    // masks. This admission depends only on physical density, encoding, and live scan width.
    // A later dynamic filter can sharply narrow values already decoded at the running survivor set. Preserve the
    // accepted dense ranks long enough to compact every aligned filter column in place, instead of discarding those
    // ranks and recovering them later with a branch-heavy two-pointer search over the wider survivor array. The
    // existing survivor scratch carries ranks temporarily, so this adds no buffer or steady-state allocation. Admit
    // only after a narrow scan has demonstrated a long, sparse reuse horizon: wider scans make the large aligned
    // column scratches themselves the locality cost, while short scans cannot amortize the extra rank pass.
    // A long scan need not physically process the entire reuse horizon before it can prove that horizon. Once at
    // least one million raw positions have established the lead-filter density, project its intermediate survivors
    // across the reader's exact physical row count. Require half of the physical scan as well as one million raw
    // positions so clustered early pages cannot govern a long tail. A statically sparse next filter may then use rank
    // compaction when the projected input has at least a 256K-row amortization horizon and every later filter has both
    // full-domain metadata and an observed all-pass history; otherwise a later narrowing can make the eager copy
    // throwaway work. The current batch's exact kept fraction still decides whether compaction occurs, so a misleading
    // range estimate pays at most one rank pass and cannot select a different result. This is scan/representation
    // evidence, not a query policy.

    // The DF-window payload path freezes one page path (skip vs bulk) for the whole scan from the FIRST window's
    // survival rate, but that single 1M-row window is positionally biased: a filter whose survivors happen to cluster
    // early reads dense there yet sparse overall. Because mis-freezing to bulk when the scan is actually sparse costs
    // ~20x (a full window decode per survivor batch) while the reverse is bounded, only commit to bulk when the first
    // window is convincingly dense — a genuinely non-selective filter reads dense everywhere, so even a biased sample
    // clears this higher bar. A merely-front-loaded window (q39: 21.9% first vs 1.5% overall) stays on skip.
    private final Allocator allocator;
    private Arena arena;
    private final VectorBatchScope batchBuffers;
    private final Allocator.Context allocationContext;
    private final NitroParquetScanResources.DecompressedPageCacheLease decompressedPageCacheLease;
    private final DecompressedPageCache decompressedPages;
    private final Allocator.SharedResource<DirectNumericBatchDecodeAdmission> directNumericBatchDecodeLease;
    private final DirectNumericBatchDecodeAdmission directNumericBatchDecodeAdmission;
    private final ParquetNumericDecodeAdmissionPolicy numericDecodeAdmissionPolicy;
    private final ParquetLateMaterializationPolicy lateMaterializationPolicy;
    private final ParquetLateMaterializationPolicy.SkipDecode skipDecodePolicy;
    private final ParquetProgressiveFilterCompactionPolicy progressiveFilterCompactionPolicy;
    private final ParquetFilteredPayloadPolicy filteredPayloadPolicy;
    private final ParquetFilterWindowPolicy filterWindowPolicy;
    private final ParquetFilterEvaluationPolicy filterEvaluationPolicy;
    private final ParquetRuntimeFilterPolicy runtimeFilterPolicy;
    private final ParquetDictionaryFilterPolicy dictionaryFilterPolicy;
    private final ParquetScanDiagnostics diagnostics;
    private final ParquetScanBatchPolicy batchPolicy;
    private final PrimitiveArrayPool arrayPool;
    private final List<String> columnNames;
    private final Schema outputSchema;
    private final SourceColumnHandle[] sourceColumns;
    private final boolean[] outputRequired;
    private final ParquetFile[] files;
    private final ParquetMappedFileCache.Lease[] mappedFileLeases;
    private final ColumnReader[] readers;
    private final ColumnReader[] nullReaders;
    private final boolean[] nullable;
    private final boolean[] intOutputAsLong;
    private final long totalRows;
    private final long[] rowGroupRows;

    private final boolean allNumeric;
    private int filterWindow;
    private boolean filterWindowBounded;
    private final boolean adaptiveNarrowFilterWindowCandidate;
    private boolean adaptiveNarrowFilterWindowDecided;
    private final LongDomain[] filtersByColumn;
    private final boolean[] lateRowLevelFiltersByColumn;
    private final boolean[] requiredFiltersByColumn;
    private final LongDomain[] rowGroupFiltersByColumn;
    private final VersionedLongPredicate[] filterVersionsByColumn;
    private boolean hasFilters;
    private boolean hasRowGroupFilters;
    private boolean filtersPruned;
    private int rowGroupIndex;
    private long rowGroupRemaining;
    private boolean rowGroupTracking;
    private long prunedRows;
    // Per-column decode scratch (grows to high-water mark; reused across batches). A column is decoded into
    // colLong or colInt (by kind) at whatever survivor set it is read at; readPositions records that set so the
    // final emit gathers each column to the surviving rows. Filter columns are read progressively: the most
    // selective one full, then each next one only at the running survivor set.
    private final long[][] colLong;
    private final int[][] colInt;
    private final boolean[][] colNull;
    private final int[][] readPositions;
    // Stable survivor snapshots for intermediate dynamic-filter stages. Each buffer grows to its column's high-water
    // mark and is reused across windows; readPositions may point at an earlier stage while this column owns the next.
    private final int[][] filterSurvivors;
    private final long[] progressiveFilterObservedRows;
    private final long[] progressiveFilterObservedKept;
    private boolean progressiveFilterCompactionAdmitted;
    private int[] filterOrder;
    private int[] nextSurvivors = new int[0];

    // The dynamic-filter path decodes a large window (page-spanning, decoupled from the output batch) so a
    // clustered lead filter lets the skip path drop whole non-surviving pages even when a page is larger than an
    // output batch. The window's surviving rows are buffered densely and then sliced into configured output
    // batches. The window must comfortably exceed a Parquet page (~100K+ rows) for page-skip to be effective.
    // Keep the window large enough to span Parquet pages for page-skip, but not so large that every dynamic-filter
    // scan walks multi-megabyte scratch arrays. 512K keeps q20's page-skip behavior while improving q45 locality.
    // Narrow numeric scans have a much smaller per-row scratch footprint and benefit from keeping a long selective
    // scan in one decoder window: this preserves RLE/page-reader state and avoids repeatedly compacting/gathering at
    // artificial window boundaries. Bound the policy by width and by the execution's total registered scan pressure
    // so the aggregate scratch retained by a many-branch operator graph remains predictable under the benchmark's
    // 12 GiB process cap. Wider scans and scan-heavy executions retain the base window. This is a physical execution
    // policy; it does not add a query predicate or alter the operator tree.
    // Densely-packed surviving values for the current window, per column (grown to high-water mark); sliced out.
    private final long[][] windowLong;
    private final int[][] windowInt;
    private final boolean[][] windowNull;
    private final Vector[] windowBinary;
    private int[] windowSlicePositions = new int[0];
    private int windowSurvivorCount;
    private int windowSurvivorCursor;
    // Payload decode path for the DF window, decided once from the first window's survival rate and frozen for the
    // whole scan: a column reader must use one page path (skip vs full) for its entire life, never mixing them across
    // windows (the skip path re-walks the RLE id stream and leaves the page decoder in a state incompatible with a
    // full decode). Skip-decode when few rows survive (it drops non-surviving pages); bulk-decode + gather when most
    // survive (a weak/unclustered filter, where per-run skip bookkeeping exceeds one sequential decode). This lets the
    // single reader serve both selective and weak dynamic filters with no mode flag.
    private boolean dfPayloadDecided;
    private boolean dfPayloadBulk;

    private long nextRow;
    private SourceBatch currentBatch;
    // Reused high-water scratch for decoding double columns (raw long bits -> reinterpreted into the value array),
    // so the eager full-batch path doesn't allocate a fresh long[] per batch per double column.
    private long[] doubleDecodeScratch;
    private final Vector[] currentValues;
    private final Vector[] currentNulls;
    private final ScanOutputResolver[] outputResolvers;
    private boolean lazyOutputResolution;
    private boolean deferredFilteredPayload;
    // Filter columns have already been decoded into the window scratch in order to build the survivor set, but a
    // downstream consumer may not project them. Publish those columns lazily so an unused filter key is not copied
    // once more into every public output batch. The window remains pinned until the slice is closed, so the ordinary
    // output resolver can materialize an exact slice on first demand without extending its ownership lifetime.
    private boolean lazyFilteredWindowOutputs;
    private int filteredWindowSliceStart;
    private int filteredWindowSliceCount;
    private int deferredWindowRows;
    private int[] deferredWindowSurvivors;
    private int[] deferredRawSurvivors = new int[0];
    private final java.util.function.Consumer<Mask> adaptiveConstrainer = this::observeAdaptiveSelection;
    private final java.util.function.Consumer<Mask> lazyMaskConstrainer = this::constrainLazyMaskAndObserveSelection;
    private final Runnable adaptiveClose = this::finishAdaptiveBatch;
    private final Runnable lazyClose = this::finishLazyBatch;
    private final Runnable deferredFilteredClose = this::finishDeferredFilteredBatch;
    private int currentBatchRows;
    private long adaptiveObservedRows;
    private long adaptiveSelectedRows;
    private int adaptiveBatchRows;
    private int adaptiveBatchSelectedRows;
    private boolean adaptiveDecided;

    // Late-materialization batch state: the active mask (narrowed by constrain) and per-column resolution cache for
    // the current batch. A column is decoded full when the mask is still all(), or skip-decoded at survivors + scattered
    // to position once the mask has been narrowed; unresolved columns are advanced past the batch when it closes.
    private int lazyCount;
    private Mask lazyMask;
    private boolean lazyConstrained;
    private boolean[] lazyResolved;
    private long[] lazyScratchLong = new long[0];
    private int[] lazyScratchInt = new int[0];
    private boolean[] lazyScratchNull = new boolean[0];
    private int[] lazyIdentity = new int[0];
    // Per-column decode path, decided once (on the column's first resolution) and fixed for the whole scan: a reader
    // must use one page path (full vs skip) for its entire life, never mixing them across batches. The decision can't
    // be recomputed per batch from lazyConstrained, because a short-circuiting predicate may pull a filter column in
    // some batches (full, before constrain) and leave it unpulled in others (where constrain has since fired).
    private boolean[] lazyPathDecided;
    private boolean[] lazySkipColumn;
    // Rows each un-pulled column's reader is behind the current batch start. A deferred column is not advanced per
    // batch; its skipped rows accumulate here and drain in one call the next time it is pulled, so whole data pages
    // (far larger than a batch) are byte-skipped instead of walked a batch at a time. Persists across batches.
    private long[] lazyPendingAdvance;
    private int lazyFragmentedNumericPayloadColumns = -1;
    private final boolean[][] directNullScratch;
    private final boolean[] directNullResolved;
    private final long[] directNullPendingAdvance;

    public NitroParquetBatchSource(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<Path> paths,
            Schema schema)
    {
        this(resources, allocator, new Splits(paths.stream().map(Split::wholeFile).map(SourceSplit::path).toList()), schema);
    }

    public static NitroParquetBatchSource forSplits(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<Split> splits,
            Schema schema)
    {
        return forSplits(resources, allocator, splits, schema, ParquetColumnNameMatching.EXACT);
    }

    public static NitroParquetBatchSource forSplits(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<Split> splits,
            Schema schema,
            ParquetColumnNameMatching columnNameMatching)
    {
        return new NitroParquetBatchSource(resources, allocator, new Splits(splits.stream().map(SourceSplit::path).toList()), schema, columnNameMatching, null);
    }

    public static NitroParquetBatchSource forSplitsByOrdinal(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<Split> splits,
            Schema schema,
            List<Integer> sourceOrdinals)
    {
        return new NitroParquetBatchSource(
                resources,
                allocator,
                new Splits(splits.stream().map(SourceSplit::path).toList()),
                schema,
                ParquetColumnNameMatching.EXACT,
                List.copyOf(sourceOrdinals));
    }

    public static NitroParquetBatchSource forInputs(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<InputSplit> splits,
            Schema schema)
    {
        return forInputs(resources, allocator, splits, schema, ParquetColumnNameMatching.EXACT);
    }

    public static NitroParquetBatchSource forInputs(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<InputSplit> splits,
            Schema schema,
            ParquetColumnNameMatching columnNameMatching)
    {
        return forInputs(resources, allocator, splits, schema, columnNameMatching, null);
    }

    public static NitroParquetBatchSource forInputsByOrdinal(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<InputSplit> splits,
            Schema schema,
            List<Integer> sourceOrdinals)
    {
        return forInputs(resources, allocator, splits, schema, ParquetColumnNameMatching.EXACT, List.copyOf(sourceOrdinals));
    }

    private static NitroParquetBatchSource forInputs(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<InputSplit> splits,
            Schema schema,
            ParquetColumnNameMatching columnNameMatching,
            List<Integer> sourceOrdinals)
    {
        splits = List.copyOf(splits);
        try {
            return new NitroParquetBatchSource(
                    resources,
                    allocator,
                    new Splits(splits.stream().map(SourceSplit::input).toList()),
                    schema,
                    columnNameMatching,
                    sourceOrdinals);
        }
        catch (RuntimeException | Error failure) {
            for (InputSplit split : splits) {
                try {
                    split.input().close();
                }
                catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    private NitroParquetBatchSource(
            NitroParquetScanResources resources,
            Allocator allocator,
            Splits splits,
            Schema schema)
    {
        this(resources, allocator, splits, schema, ParquetColumnNameMatching.EXACT, null);
    }

    private NitroParquetBatchSource(
            NitroParquetScanResources resources,
            Allocator allocator,
            Splits splits,
            Schema schema,
            ParquetColumnNameMatching columnNameMatching,
            List<Integer> sourceOrdinals)
    {
        this(
                allocator,
                splits.values(),
                requireColumnNames(schema),
                schema,
                requireNonNull(columnNameMatching, "columnNameMatching is null"),
                sourceOrdinals,
                requireNonNull(resources, "resources is null").batchBufferPool(),
                resources,
                resources.directNumericBatchDecodeAdmission(),
                resources.metadataCache(),
                resources.decompressedPageCachePolicy(),
                resources.readerPolicy(),
                resources.numericDecodeAdmissionPolicy(),
                resources.lateMaterializationPolicy(),
                resources.progressiveFilterCompactionPolicy(),
                resources.filteredPayloadPolicy(),
                resources.filterWindowPolicy(),
                resources.filterEvaluationPolicy(),
                resources.runtimeFilterPolicy(),
                resources.diagnostics(),
                resources.batchPolicy(),
                resources.arenaPolicy());
    }

    private NitroParquetBatchSource(
            Allocator allocator,
            List<SourceSplit> splits,
            List<String> columns,
            Schema schema,
            ParquetColumnNameMatching columnNameMatching,
            List<Integer> sourceOrdinals,
            Object batchBufferPoolKey,
            NitroParquetScanResources resources,
            Object directNumericBatchDecodeAdmissionKey,
            ParquetMetadataCache metadataCache,
            DecompressedPageCachePolicy decompressedPageCachePolicy,
            ParquetReaderPolicy readerPolicy,
            ParquetNumericDecodeAdmissionPolicy numericDecodeAdmissionPolicy,
            ParquetLateMaterializationPolicy lateMaterializationPolicy,
            ParquetProgressiveFilterCompactionPolicy progressiveFilterCompactionPolicy,
            ParquetFilteredPayloadPolicy filteredPayloadPolicy,
            ParquetFilterWindowPolicy filterWindowPolicy,
            ParquetFilterEvaluationPolicy filterEvaluationPolicy,
            ParquetRuntimeFilterPolicy runtimeFilterPolicy,
            ParquetScanDiagnostics diagnostics,
            ParquetScanBatchPolicy batchPolicy,
            ParquetArenaPolicy arenaPolicy)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        requireNonNull(arenaPolicy, "arenaPolicy is null");
        this.arrayPool = allocator.primitiveArrays();
        this.batchBuffers = new VectorBatchScope(allocator, "NitroParquetBatchSource", batchBufferPoolKey);
        this.allocationContext = batchBuffers.context();
        this.decompressedPageCacheLease = decompressedPageCachePolicy.enabled()
                ? resources.acquireDecompressedPageCache(allocator)
                : null;
        this.decompressedPages = decompressedPageCacheLease == null ? null : decompressedPageCacheLease.value();
        this.numericDecodeAdmissionPolicy = requireNonNull(numericDecodeAdmissionPolicy, "numericDecodeAdmissionPolicy is null");
        this.lateMaterializationPolicy = requireNonNull(lateMaterializationPolicy, "lateMaterializationPolicy is null");
        this.skipDecodePolicy = lateMaterializationPolicy.skipDecode();
        this.progressiveFilterCompactionPolicy = requireNonNull(
                progressiveFilterCompactionPolicy, "progressiveFilterCompactionPolicy is null");
        this.filteredPayloadPolicy = requireNonNull(filteredPayloadPolicy, "filteredPayloadPolicy is null");
        this.filterWindowPolicy = requireNonNull(filterWindowPolicy, "filterWindowPolicy is null");
        this.filterEvaluationPolicy = requireNonNull(filterEvaluationPolicy, "filterEvaluationPolicy is null");
        this.runtimeFilterPolicy = requireNonNull(runtimeFilterPolicy, "runtimeFilterPolicy is null");
        this.dictionaryFilterPolicy = requireNonNull(readerPolicy, "readerPolicy is null").dictionaryFilter();
        this.diagnostics = requireNonNull(diagnostics, "diagnostics is null");
        this.batchPolicy = requireNonNull(batchPolicy, "batchPolicy is null");
        this.currentBatchRows = batchPolicy.initialRows();
        this.directNumericBatchDecodeLease = allocator.acquireSharedResource(
                directNumericBatchDecodeAdmissionKey,
                () -> new DirectNumericBatchDecodeAdmission(numericDecodeAdmissionPolicy));
        this.directNumericBatchDecodeAdmission = directNumericBatchDecodeLease.value();
        this.columnNames = List.copyOf(columns);
        this.outputSchema = requireNonNull(schema, "schema is null");
        if (schema.size() != this.columnNames.size()) {
            throw new IllegalArgumentException("schema size does not match projected columns");
        }
        if (sourceOrdinals != null && sourceOrdinals.size() != this.columnNames.size()) {
            throw new IllegalArgumentException("source ordinals size does not match projected columns");
        }
        this.sourceColumns = new SourceColumnHandle[schema.size()];
        for (int column = 0; column < sourceColumns.length; column++) {
            sourceColumns[column] = new OrdinalSourceColumnHandle(column, schema.field(column).type());
        }
        checkArgument(!splits.isEmpty(), "splits is empty");

        this.mappedFileLeases = new ParquetMappedFileCache.Lease[splits.size()];
        this.files = new ParquetFile[splits.size()];
        for (int file = 0; file < splits.size(); file++) {
            SourceSplit split = splits.get(file);
            switch (split) {
                case InputSourceSplit inputSplit -> files[file] = ParquetFile.open(inputSplit.input());
                case PathSourceSplit pathSplit -> {
                    Path path = pathSplit.path();
                    mappedFileLeases[file] = resources.acquireMappedFile(path);
                    if (mappedFileLeases[file] == null) {
                        if (arena == null) {
                            arena = arenaPolicy.createArena();
                        }
                        files[file] = ParquetFile.open(path, arena, metadataCache);
                    }
                    else {
                        files[file] = mappedFileLeases[file].file();
                    }
                }
            }
        }
        int columnCount = columns.size();
        this.readers = new ColumnReader[columnCount];
        this.nullReaders = new ColumnReader[columnCount];
        this.outputRequired = new boolean[columnCount];
        java.util.Arrays.fill(outputRequired, true);
        this.nullable = new boolean[columnCount];
        this.intOutputAsLong = new boolean[columnCount];
        this.currentValues = new Vector[columnCount];
        this.currentNulls = new Vector[columnCount];
        this.outputResolvers = new ScanOutputResolver[columnCount];
        this.directNullScratch = new boolean[columnCount][];
        this.directNullResolved = new boolean[columnCount];
        this.directNullPendingAdvance = new long[columnCount];

        for (int c = 0; c < columnCount; c++) {
            ParquetFile.Column first = resolveColumn(files[0], c, columns, columnNameMatching, sourceOrdinals);
            readers[c] = new ColumnReader(
                    first.type(),
                    first.optional(),
                    first.typeLength(),
                    first.decimal(),
                    decompressedPages,
                    arrayPool,
                    readerPolicy,
                    resources.decodeScratchPool());
            nullable[c] = first.optional();
            if (readers[c].kind() == ColumnReader.Kind.INT) {
                Set<Class<? extends Vector>> supportedVectors = schema.field(c).type().supportedVectorTypes();
                if (supportedVectors.isEmpty() || supportedVectors.contains(I32Vector.class)) {
                    intOutputAsLong[c] = false;
                }
                else if (supportedVectors.contains(I64Vector.class)) {
                    intOutputAsLong[c] = true;
                }
                else {
                    throw new IllegalArgumentException("INT32 Parquet column has no supported I32 or I64 output representation");
                }
            }
            if (filterEvaluationPolicy.directNullMask().reader() && first.optional()) {
                directNullScratch[c] = new boolean[0];
            }
            outputResolvers[c] = new ScanOutputResolver(c);
        }
        long rows = 0;
        List<Long> rowsByGroup = new ArrayList<>();
        for (int fileIndex = 0; fileIndex < files.length; fileIndex++) {
            ParquetFile file = files[fileIndex];
            SourceSplit split = splits.get(fileIndex);
            List<RowGroup> rowGroups = file.rowGroups(split.start(), split.length());
            rowGroups.forEach(rowGroup -> rowsByGroup.add(rowGroup.num_rows));
            for (int c = 0; c < columnCount; c++) {
                ParquetFile.Column column = resolveColumn(file, c, columns, columnNameMatching, sourceOrdinals);
                DecompressedPageCache.Source source = decompressedPages == null
                        ? null
                        : new DecompressedPageCache.Source(splits.get(fileIndex).id(), column.name());
                if (source != null) {
                    decompressedPages.register(source);
                }
                for (RowGroup rowGroup : rowGroups) {
                    var columnMetadata = file.columnChunk(rowGroup, column).meta_data;
                    readers[c].addChunk(file, columnMetadata, rowGroup.num_rows, source);
                }
            }
            rows += rowGroups.stream()
                    .mapToLong(rowGroup -> rowGroup.num_rows)
                    .sum();
        }
        this.totalRows = rows;
        this.rowGroupRows = rowsByGroup.stream().mapToLong(Long::longValue).toArray();

        int intColumns = 0;
        for (ColumnReader reader : readers) {
            if (reader.kind() == ColumnReader.Kind.INT) {
                intColumns++;
            }
        }
        directNumericBatchDecodeAdmission.register(rows, intColumns);

        boolean numeric = true;
        for (ColumnReader reader : readers) {
            if (reader.kind() == ColumnReader.Kind.BINARY) {
                numeric = false;
            }
        }
        this.allNumeric = numeric;
        this.filterWindow = filterWindowPolicy.rows();
        ParquetFilterWindowPolicy.AdaptiveNarrow adaptiveNarrowPolicy = filterWindowPolicy.adaptiveNarrow();
        this.adaptiveNarrowFilterWindowCandidate =
                adaptiveNarrowPolicy.enabled() && numeric && columnCount <= adaptiveNarrowPolicy.maxColumns();
        this.filtersByColumn = new LongDomain[columnCount];
        this.lateRowLevelFiltersByColumn = new boolean[columnCount];
        this.requiredFiltersByColumn = new boolean[columnCount];
        this.rowGroupFiltersByColumn = new LongDomain[columnCount];
        this.filterVersionsByColumn = new VersionedLongPredicate[columnCount];
        this.colLong = new long[columnCount][];
        this.colInt = new int[columnCount][];
        this.colNull = new boolean[columnCount][];
        this.readPositions = new int[columnCount][];
        this.filterSurvivors = new int[columnCount][];
        this.progressiveFilterObservedRows = new long[columnCount];
        this.progressiveFilterObservedKept = new long[columnCount];
        this.windowLong = new long[columnCount][];
        this.windowInt = new int[columnCount][];
        this.windowNull = new boolean[columnCount][];
        this.windowBinary = new Vector[columnCount];
        this.debugFilterInputs = new long[columnCount];
        this.debugFilterOutputs = new long[columnCount];
        this.debugFullDecoded = new long[columnCount];
        this.debugSelectedDecoded = new long[columnCount];
        this.debugDictionaryExamined = new long[columnCount];
        this.debugSkipped = new long[columnCount];
        this.debugPublished = new long[columnCount];
        this.debugCopied = new long[columnCount];
        this.debugLazyOmitted = new long[columnCount];
        this.debugNullExamined = new long[columnCount];
    }

    private static ParquetFile.Column resolveColumn(
            ParquetFile file,
            int outputColumn,
            List<String> columns,
            ParquetColumnNameMatching columnNameMatching,
            List<Integer> sourceOrdinals)
    {
        if (sourceOrdinals != null) {
            return file.column(sourceOrdinals.get(outputColumn));
        }
        return file.column(columns.get(outputColumn), columnNameMatching);
    }

    private boolean pushLongDomain(int column, LongDomain filter, boolean enforcementRequired)
    {
        if (column < 0 || column >= readers.length) {
            return false;
        }
        if (readers[column].kind() == ColumnReader.Kind.BINARY || readers[column].isDouble()) {
            return false;
        }
        if (runtimeFilterPolicy.rowGroupFiltering()) {
            LongDomain existingRowGroupFilter = rowGroupFiltersByColumn[column];
            if (existingRowGroupFilter == null || filter.size() < existingRowGroupFilter.size()) {
                rowGroupFiltersByColumn[column] = filter;
                hasRowGroupFilters = true;
            }
        }
        // Complete predicate responsibility can move into the source only before reading begins: already published
        // rows did not observe a late predicate. Residual runtime filters remain safe to install between polls. Every
        // reader is aligned at nextRow, and the next filter window continues from its current page cursor, so a later
        // narrowing can avoid row-level work in unread portions of the current row group instead of waiting for its
        // boundary. Any already decoded output slice remains a safe superset and is still checked by the join.
        if (nextRow > 0 && enforcementRequired) {
            return false;
        }
        // Runtime filters are numeric, but their surviving payload need not be. The filtered-window path decodes
        // INT/LONG payload into reusable primitive scratch and preserves BINARY payload as an allocator-owned vector.
        // This matters for ordinary dimension scans such as numeric date keys projected alongside CHAR attributes:
        // downgrading those scans to row-group-only filtering makes a downstream dynamic-filter collector publish
        // the entire key domain instead of the selected rows.
        if (!runtimeFilterPolicy.rowLevelFiltering() ||
                (nullable[column] && !runtimeFilterPolicy.nullableRowLevelFiltering())) {
            return false;
        }
        // Several joins can push a filter on the same probe column (e.g. this scan's own dimension join and a
        // downstream join whose key survives through an aggregation). Each is an independent necessary condition, so
        // keeping the more selective one (fewer distinct values) is correct and prunes hardest; a blind overwrite
        // could otherwise replace a tight filter with an all-values one.
        LongDomain existing = filtersByColumn[column];
        if (existing != null) {
            if (requiredFiltersByColumn[column]) {
                return false;
            }
            if (!enforcementRequired && existing.size() <= filter.size()) {
                return false;
            }
        }
        filtersByColumn[column] = filter;
        lateRowLevelFiltersByColumn[column] |= nextRow > 0;
        requiredFiltersByColumn[column] = enforcementRequired;
        if (existing == null && filterOrder != null) {
            // Multiple filters can be installed before the first poll. Preserve the established order and append
            // the newly active column so it is decoded before the filtered-column gather below.
            filterOrder = appendFilterColumn(filterOrder, column);
        }
        int dictionaryEntries = readers[column].peekDictionarySize();
        filterVersionsByColumn[column] = dictionaryFilterPolicy.admitsVersionedPredicate(dictionaryEntries, filter.size())
                ? filter
                : null;
        hasFilters = true;
        return true;
    }

    static int[] appendFilterColumn(int[] filterOrder, int column)
    {
        int[] extended = java.util.Arrays.copyOf(filterOrder, filterOrder.length + 1);
        extended[extended.length - 1] = column;
        return extended;
    }

    @Override
    public Schema schema()
    {
        return outputSchema;
    }

    @Override
    public SourceColumnHandle column(int outputIndex)
    {
        return sourceColumns[outputIndex];
    }

    @Override
    public Set<SourceCapability> capabilities()
    {
        if (deferredFilteredPayload) {
            return allNumeric
                    ? Set.of(SourceCapability.LAZY_COLUMNS, SourceCapability.SELECTION_PUSHDOWN, SourceCapability.RUNTIME_FILTER, SourceCapability.CONSTRAINED_REBORROW)
                    : Set.of(SourceCapability.LAZY_COLUMNS, SourceCapability.SELECTION_PUSHDOWN, SourceCapability.RUNTIME_FILTER, SourceCapability.CONSTRAINED_REBORROW);
        }
        return allNumeric
                ? Set.of(SourceCapability.LAZY_COLUMNS, SourceCapability.SELECTION_PUSHDOWN, SourceCapability.RUNTIME_FILTER)
                : Set.of(SourceCapability.LAZY_COLUMNS, SourceCapability.SELECTION_PUSHDOWN, SourceCapability.RUNTIME_FILTER);
    }

    @Override
    public OptionalLong exactRows()
    {
        return OptionalLong.of(totalRows);
    }

    @Override
    public <T> Optional<T> protocol(SourceProtocol<T> protocol)
    {
        if (protocol == SourceOutputDemandProtocol.OUTPUT_DEMAND) {
            SourceOutputDemand demand = this::retainOutputs;
            return Optional.of(protocol.valueType().cast(demand));
        }
        if (protocol == SourceMetricsProtocol.METRICS) {
            SourceMetrics metrics = new SourceMetrics()
            {
                @Override
                public OptionalLong completedBytes()
                {
                    return nextRow == totalRows ? OptionalLong.of(consumedPageBytes()) : OptionalLong.empty();
                }

                @Override
                public OptionalLong completedPositions()
                {
                    return OptionalLong.of(nextRow - prunedRows);
                }

                @Override
                public OptionalLong readTimeNanos()
                {
                    return OptionalLong.empty();
                }
            };
            return Optional.of(protocol.valueType().cast(metrics));
        }
        return Optional.empty();
    }

    private void retainOutputs(Set<SourceColumnHandle> outputs)
    {
        checkOpen();
        if (nextRow != 0 || currentBatch != null) {
            throw new IllegalStateException("output demand must be declared before polling");
        }
        requireNonNull(outputs, "outputs is null");
        java.util.Arrays.fill(outputRequired, false);
        for (SourceColumnHandle output : outputs) {
            int column = columnIndex(requireNonNull(output, "output is null"));
            if (column < 0) {
                throw new IllegalArgumentException("output belongs to another source");
            }
            outputRequired[column] = true;
        }
    }

    private long consumedPageBytes()
    {
        long bytes = 0;
        for (ColumnReader reader : readers) {
            bytes = addExact(bytes, reader.consumedPageBytes());
        }
        for (ColumnReader reader : nullReaders) {
            if (reader != null) {
                bytes = addExact(bytes, reader.consumedPageBytes());
            }
        }
        return bytes;
    }

    @Override
    public SourcePoll poll()
    {
        checkOpen();
        advancePastRejectedRowGroups();
        if (!(filtersActive() ? ensureWindow() : nextRow < totalRows)) {
            return SourcePoll.Finished.FINISHED;
        }
        closeCurrentBatch();
        enableDirectNumericBatchDecodeIfAdmitted();

        if (filtersActive()) {
            return new SourcePoll.Ready(emitSlice());
        }
        int count = toIntExact(Math.min(currentBatchRows, totalRows - nextRow));
        if (rowGroupTracking) {
            count = toIntExact(Math.min(count, rowGroupRemaining));
            consumeRowGroupRows(count);
        }
        nextRow += count;
        return new SourcePoll.Ready(lateMaterializationPolicy.enabled() ? lazyBatch(count) : fullBatch(count));
    }

    @Override
    public RuntimeFilterAcceptance addRuntimeFilter(RuntimeFilter filter)
    {
        checkOpen();
        requireNonNull(filter, "filter is null");
        int column = columnIndex(filter.column());
        if (column < 0) {
            return RuntimeFilterAcceptance.REJECTED;
        }
        LongDomain domain = filter.domain().capability(LongDomainCapability.LONG_DOMAIN).orElse(null);
        if (domain == null) {
            return RuntimeFilterAcceptance.REJECTED;
        }
        // The long-domain capability describes non-null carriers. Nullable domain semantics remain on the enclosing
        // TypedDomain, and the numeric filter-window path currently treats every null as rejected. Keep such filters
        // as residuals until the source has a null-aware encoded-domain protocol; applying the value capability alone
        // would turn an only-null domain into an empty domain and discard matching rows.
        if (filter.domain().includesNull()) {
            return RuntimeFilterAcceptance.ACCEPTED_WITH_RESIDUAL;
        }
        boolean enforcementRequested = !filter.residualRequired() && !filter.approximate();
        boolean enforced = pushLongDomain(column, domain, enforcementRequested);
        return enforced && enforcementRequested
                ? RuntimeFilterAcceptance.ENFORCED
                : RuntimeFilterAcceptance.ACCEPTED_WITH_RESIDUAL;
    }

    @Override
    public boolean supportsRuntimeFilter(SourceColumnHandle column)
    {
        int index = columnIndex(requireNonNull(column, "column is null"));
        return index >= 0 && readers[index].kind() != ColumnReader.Kind.BINARY && !readers[index].isDouble();
    }

    /** Reject mixed-payload row groups from numeric min/max metadata before any column page is visited. */
    private void advancePastRejectedRowGroups()
    {
        // A filtered window may already have decoded survivors that still need to be emitted. Do not advance any
        // reader into a later row group until that window has been drained.
        if (!hasRowGroupFilters || nextRow >= totalRows || windowSurvivorCursor < windowSurvivorCount) {
            return;
        }
        closeCurrentBatch();
        initializeRowGroupTracking();
        while (rowGroupIndex < rowGroupRows.length && rowGroupRemaining == rowGroupRows[rowGroupIndex]) {
            if (rowGroupMayMatch(rowGroupIndex)) {
                return;
            }
            long rows = rowGroupRemaining;
            deferOrSkipRows(rows);
            nextRow += rows;
            prunedRows += rows;
            rowGroupIndex++;
            rowGroupRemaining = rowGroupIndex < rowGroupRows.length ? rowGroupRows[rowGroupIndex] : 0;
        }
    }

    private void initializeRowGroupTracking()
    {
        if (rowGroupTracking) {
            return;
        }
        long position = nextRow;
        while (rowGroupIndex < rowGroupRows.length && position >= rowGroupRows[rowGroupIndex]) {
            position -= rowGroupRows[rowGroupIndex++];
        }
        rowGroupRemaining = rowGroupIndex < rowGroupRows.length ? rowGroupRows[rowGroupIndex] - position : 0;
        rowGroupTracking = true;
    }

    private boolean rowGroupMayMatch(int index)
    {
        for (int column = 0; column < rowGroupFiltersByColumn.length; column++) {
            LongDomain filter = rowGroupFiltersByColumn[column];
            if (filter != null) {
                if (!readers[column].chunkMayMatch(index, filter)) {
                    return false;
                }
                if (!readers[column].dictionaryMayMatch(
                        index,
                        filter,
                        runtimeFilterPolicy.maxDictionaryPruningValues())) {
                    return false;
                }
            }
        }
        return true;
    }

    private void deferOrSkipRows(long rows)
    {
        for (int column = 0; column < readers.length; column++) {
            if (lazyPendingAdvance == null) {
                readers[column].skip(rows);
            }
            else {
                lazyPendingAdvance[column] += rows;
            }
            if (nullReaders[column] == null) {
                directNullPendingAdvance[column] += rows;
            }
            else {
                nullReaders[column].skipNulls(directNullPendingAdvance[column] + rows);
                directNullPendingAdvance[column] = 0;
            }
        }
    }

    private void consumeRowGroupRows(int rows)
    {
        rowGroupRemaining -= rows;
        if (rowGroupRemaining == 0) {
            rowGroupIndex++;
            rowGroupRemaining = rowGroupIndex < rowGroupRows.length ? rowGroupRows[rowGroupIndex] : 0;
        }
    }

    private boolean directNumericBatchDecodeConfigured;
    private boolean repeatedSourceAdmissionReported;

    private void enableDirectNumericBatchDecodeIfAdmitted()
    {
        if (directNumericBatchDecodeConfigured) {
            return;
        }
        boolean broadEligible = directNumericBatchDecodeAdmission.admitted();
        boolean broadAdmission = broadEligible &&
                (!numericDecodeAdmissionPolicy.requireConstraintForAllNumeric() || !allNumeric || lazyConstrained);
        boolean repeatedSourceCandidate = numericDecodeAdmissionPolicy.repeatedSourceDecode() &&
                totalRows >= numericDecodeAdmissionPolicy.repeatedSourceMinRows();
        int repeatedAnchorColumns = 0;
        if (repeatedSourceCandidate) {
            for (ColumnReader reader : readers) {
                if (reader.kind() != ColumnReader.Kind.BINARY &&
                        reader.hasRepeatedSource(numericDecodeAdmissionPolicy.repeatedSourceAnchorMinConsumers())) {
                    repeatedAnchorColumns++;
                }
            }
        }
        boolean repeatedSourceAdmission =
                repeatedAnchorColumns >= numericDecodeAdmissionPolicy.repeatedSourceAnchorMinColumns();
        if (numericDecodeAdmissionPolicy.diagnostics() && repeatedAnchorColumns > 0 && !repeatedSourceAdmissionReported) {
            repeatedSourceAdmissionReported = true;
            System.err.printf(
                    "[direct-numeric-repeated-source-admission] rows=%d repeatedAnchorColumns=%d admitted=%s%n",
                    totalRows,
                    repeatedAnchorColumns,
                    repeatedSourceAdmission);
        }
        boolean allConfigured = true;
        for (ColumnReader reader : readers) {
            if (reader.kind() == ColumnReader.Kind.BINARY) {
                continue;
            }
            if (broadAdmission ||
                    (repeatedSourceAdmission && reader.hasRepeatedSource(2))) {
                reader.enableDirectNumericBatchDecode();
            }
            else {
                // A later constrain() may admit the scan-wide path for the remaining numeric columns.
                allConfigured = false;
            }
        }
        // Registration is complete before execution begins. If no scan made the broad path eligible, a later
        // constrain() cannot change that fact; avoid re-evaluating repeated-source metadata for every output batch.
        directNumericBatchDecodeConfigured = allConfigured || !broadEligible;
    }

    /**
     * Whether the scan should run the eager filter-window path. On first consultation, if <em>every</em> pushed
     * dynamic filter is non-selective (each admits about its column's whole dictionary), all are dropped so the scan
     * reverts to the lazy late-materialization path — there a downstream operator's own predicate (e.g. an IS NULL)
     * drives the real narrowing, instead of the window path eagerly gathering every payload column at full row count.
     * Dropping them is safe because each join still enforces its condition.
     *
     * <p>The all-or-nothing test is deliberate: the win comes only from a full revert to lazy materialization. If any
     * filter is selective it leads the window path profitably, and a non-selective filter kept alongside it still
     * prunes at the margin via skip-decode — dropping only that one would keep the window path yet lose that pruning.
     */
    private boolean filtersActive()
    {
        if (hasFilters && !filtersPruned) {
            filtersPruned = true;
            if (filterEvaluationPolicy.nonSelectiveElision().enabled() && allFiltersNonSelective()) {
                for (int c = 0; c < filtersByColumn.length; c++) {
                    filtersByColumn[c] = null;
                    filterVersionsByColumn[c] = null;
                }
                hasFilters = false;
            }
        }
        return hasFilters;
    }

    /** Whether every pushed filter provably accepts every physical dictionary value in its column. */
    private boolean allFiltersNonSelective()
    {
        for (int c = 0; c < filtersByColumn.length; c++) {
            LongDomain filter = filtersByColumn[c];
            if (filter == null) {
                continue;
            }
            if (requiredFiltersByColumn[c]) {
                return false;
            }
            if (filterEvaluationPolicy.nonSelectiveElision().exactDictionaryCoverage()) {
                if (!readers[c].dictionaryValuesCovered(filter)) {
                    return false;
                }
                continue;
            }
            int cardinality = readers[c].peekDictionarySize();
            if (cardinality <= 0 || filter.size() != cardinality) {
                return false;
            }
        }
        return true;
    }

    private long debugRawRows;
    private long debugSurvivors;
    private final long[] debugFilterInputs;
    private final long[] debugFilterOutputs;
    private final long[] debugFullDecoded;
    private final long[] debugSelectedDecoded;
    private final long[] debugDictionaryExamined;
    private final long[] debugSkipped;
    private final long[] debugPublished;
    private final long[] debugCopied;
    private final long[] debugLazyOmitted;
    private final long[] debugNullExamined;
    private long debugProgressiveCompactionRows;
    private long debugProgressiveCompactionKept;
    private long debugProgressiveCompactionWindows;
    private boolean debugFusedCompactionCandidatePrinted;
    private boolean debugProspectiveCompactionCandidatePrinted;

    private void recordFullDecode(int column, long positions)
    {
        if (diagnostics.sourceWork()) {
            debugFullDecoded[column] += positions;
        }
    }

    private void recordSelectedDecode(int column, long positions)
    {
        if (diagnostics.sourceWork()) {
            debugSelectedDecoded[column] += positions;
        }
    }

    private void recordSkipped(int column, long positions)
    {
        if (diagnostics.sourceWork()) {
            debugSkipped[column] += positions;
        }
    }

    private void recordPublished(int column, long positions)
    {
        if (diagnostics.sourceWork()) {
            debugPublished[column] += positions;
        }
    }

    private void recordCopied(int column, long positions)
    {
        if (diagnostics.sourceWork()) {
            debugCopied[column] += positions;
        }
    }

    /** Decode windows until one yields surviving rows (or input is exhausted). Returns whether rows are available. */
    private boolean ensureWindow()
    {
        if (!adaptiveNarrowFilterWindowDecided) {
            adaptiveNarrowFilterWindowDecided = true;
            ParquetFilterWindowPolicy.AdaptiveNarrow adaptiveNarrowPolicy = filterWindowPolicy.adaptiveNarrow();
            if (adaptiveNarrowFilterWindowCandidate) {
                filterWindow = Math.max(filterWindow, adaptiveNarrowPolicy.rows());
            }
            if (adaptiveNarrowPolicy.diagnostics() && adaptiveNarrowFilterWindowCandidate) {
                System.err.printf(
                        "[narrow-filter-window] columns=%d rows=%d window=%d%n",
                        readers.length,
                        totalRows,
                        filterWindow);
            }
        }
        if (windowSurvivorCursor < windowSurvivorCount) {
            return true;
        }
        ParquetFilteredPayloadPolicy.Deferred deferredPolicy = filteredPayloadPolicy.deferred();
        ParquetFilteredPayloadPolicy.BoundedWindow boundedWindowPolicy = deferredPolicy.boundedWindow();
        if (boundedWindowPolicy.enabled() &&
                !filterWindowBounded &&
                readers.length <= boundedWindowPolicy.maxScanColumns() &&
                payloadColumnCount() >= deferredPolicy.minColumns()) {
            filterWindow = Math.min(filterWindow, boundedWindowPolicy.rows());
            filterWindowBounded = true;
            if (boundedWindowPolicy.diagnostics()) {
                System.err.printf("[bounded-filter-window] columns=%s payload=%s rows=%s%n", columnNames, payloadColumnCount(), filterWindow);
            }
        }
        while (nextRow < totalRows) {
            advancePastRejectedRowGroups();
            if (nextRow >= totalRows) {
                return false;
            }
            int windowCount = toIntExact(Math.min(filterWindow, totalRows - nextRow));
            if (rowGroupTracking) {
                windowCount = toIntExact(Math.min(windowCount, rowGroupRemaining));
                consumeRowGroupRows(windowCount);
            }
            nextRow += windowCount;
            decodeFilterWindow(windowCount);
            windowSurvivorCursor = 0;
            if (diagnostics.rowCounts()) {
                debugRawRows += windowCount;
                debugSurvivors += windowSurvivorCount;
            }
            if (windowSurvivorCount > 0) {
                return true;
            }
        }
        return false;
    }

    private SourceBatch fullBatch(int count)
    {
        int columnCount = readers.length;
        lazyOutputResolution = false;
        VectorColumnGeneration[] outputs = new VectorColumnGeneration[columnCount];

        for (int c = 0; c < columnCount; c++) {
            ColumnReader reader = readers[c];
            BooleanVector nullVector = nullable[c]
                    ? allocator.allocate(allocationContext, BooleanVector.class, count, BooleanVector::new)
                    : null;
            boolean[] nulls = nullVector == null ? null : nullVector.values();
            Vector valueVector = switch (reader.kind()) {
                case INT -> {
                    yield reader.readNumeric(allocator, allocationContext, nulls, count, intOutputAsLong[c]);
                }
                case LONG -> {
                    if (reader.isDouble()) {
                        org.weakref.nitro.data.F64Vector vector = org.weakref.nitro.data.F64Vector.allocate(allocator, allocationContext, count);
                        double[] values = vector.values();
                        // Decode raw bits into a reused scratch (the double bit pattern IS the long), then reinterpret
                        // into the value array. A fresh long[count] per batch per double column was ~1.4 GB/op of
                        // garbage on a wide double scan (TPC-H q06); the high-water scratch keeps it off the heap.
                        if (doubleDecodeScratch == null || doubleDecodeScratch.length < count) {
                            doubleDecodeScratch = replaceLongs(doubleDecodeScratch, count);
                        }
                        long[] bits = doubleDecodeScratch;
                        reader.readLongs(bits, nulls, count);
                        for (int i = 0; i < count; i++) {
                            values[i] = Double.longBitsToDouble(bits[i]);
                        }
                        recordCopied(c, count);
                        yield vector;
                    }
                    yield reader.readNumeric(allocator, allocationContext, nulls, count, false);
                }
                case BINARY -> {
                    Vector vector = reader.readBinary(allocator, allocationContext, nulls, count);
                    if (nullVector != null && reader.lastReadNullsProvenAbsent()) {
                        nullVector.declareAllFalse();
                    }
                    yield vector;
                }
            };
            recordFullDecode(c, count);
            recordPublished(c, count);
            currentValues[c] = valueVector;
            currentNulls[c] = nullVector;

            ScanOutputResolver outputResolver = outputResolvers[c];
            outputs[c] = new VectorColumnGeneration(
                    nullVector == null ? Set.of(Stream.VALUES) : Set.of(Stream.VALUES, Stream.NULLS),
                    outputResolver.resolver(),
                    outputResolver);
        }

        Mask mask = allocator.allocateAllMask(allocationContext, count);
        beginAdaptiveBatch(count);
        SourceBatch batch = new VectorSourceBatch(outputSchema, mask, outputs, batchBuffers, adaptiveConstrainer, adaptiveClose);
        currentBatch = batch;
        return batch;
    }

    /**
     * Late-materialization batch: defer per-column decode. A column pulled while the mask is still {@code all()}
     * decodes every row; once a filter above the scan narrows the mask via {@link #constrain}, columns pulled
     * afterwards skip-decode only the survivor rows and scatter them back to position. Any column never pulled is
     * advanced past the batch when it closes so every reader stays aligned to the batch boundary.
     */
    private SourceBatch lazyBatch(int count)
    {
        int columnCount = readers.length;
        lazyOutputResolution = true;
        lazyCount = count;
        lazyConstrained = false;
        beginAdaptiveBatch(count);
        lazyMask = allocator.allocateAllMask(allocationContext, count);
        if (lazyResolved == null || lazyResolved.length < columnCount) {
            lazyResolved = new boolean[columnCount];
            lazyPathDecided = new boolean[columnCount];  // per-scan, decided once and never reset
            lazySkipColumn = new boolean[columnCount];
            lazyPendingAdvance = new long[columnCount];  // per-scan, accumulates deferred rows, never reset per batch
        }
        else {
            java.util.Arrays.fill(lazyResolved, 0, columnCount, false);
        }
        java.util.Arrays.fill(directNullResolved, false);
        VectorColumnGeneration[] outputs = new VectorColumnGeneration[columnCount];
        for (int c = 0; c < columnCount; c++) {
            ScanOutputResolver outputResolver = outputResolvers[c];
            outputs[c] = new VectorColumnGeneration(
                    nullable[c] ? Set.of(Stream.VALUES, Stream.NULLS) : Set.of(Stream.VALUES),
                    outputResolver.resolver(),
                    null,
                    outputResolver.maskResolver(),
                    null,
                    null,
                    outputResolver);
        }
        SourceBatch batch = new VectorSourceBatch(outputSchema, lazyMask, outputs, batchBuffers, lazyMaskConstrainer, lazyClose);
        currentBatch = batch;
        return batch;
    }

    /**
     * The fixed decode path for {@code column}, decided on its first resolution and stable for the whole scan: skip
     * (survivors only) iff it was first pulled while the batch was constrained, for a non-binary column. Persisting
     * the decision keeps a reader on one page path even when a short-circuiting predicate leaves a filter column
     * unpulled in some batches (where it would otherwise be skip-advanced after constrain has fired).
     */
    private boolean useSkipDecode(int column)
    {
        if (!lazyPathDecided[column]) {
            lazyPathDecided[column] = true;
            // Skip-decode only a non-binary column under a SELECTIVE constraint: when most rows survive, the per-run
            // skip overhead exceeds a bulk full decode (mirrors SkipDecode's "not selective enough to pay for"
            // guard). Decided once, on first touch, and fixed for the scan so a reader keeps one page path.
            boolean constrained = lazyConstrained && !lazyMask.all();
            boolean sparse = constrained &&
                    (long) lazyMask.selectedCount() * 100 <=
                            (long) lazyCount * skipDecodePolicy.maxSurvivorPercent();
            boolean amortizedRuns = sparse && hasAmortizedSurvivorRuns(lazyMask);
            ParquetLateMaterializationPolicy.FragmentedNumeric fragmentedPolicy =
                    skipDecodePolicy.fragmentedNumeric();
            boolean fragmentedShape = sparse && fragmentedPolicy.enabled() &&
                    (long) lazyMask.selectedCount() * 100 <=
                            (long) lazyCount * fragmentedPolicy.maxSurvivorPercent() &&
                    readers.length <= fragmentedPolicy.maxScanColumns();
            // Do not inspect page dictionaries merely to reject a candidate. Reader metadata inspection can commit
            // page state, so the ordinary bulk path must remain bit-for-bit untouched when density fails admission.
            int fragmentedPayloadColumns = fragmentedShape
                    ? fragmentedNumericPayloadColumns(column)
                    : 0;
            boolean fragmentedNumeric = fragmentedShape &&
                    fragmentedPolicy.admits(
                            lazyMask.selectedCount(),
                            lazyCount,
                            readers.length,
                            fragmentedPayloadColumns);
            // Wide BINARY columns always participate. Numeric columns use the same physical selectivity and
            // survivor-run proof: clustered sparse masks can then avoid decoding almost an entire payload column,
            // while fragmented masks retain the bulk decoder. The switch is an experimental reverse control and is
            // intentionally independent of SQL type, column identity, or query shape.
            int dictionarySize = (amortizedRuns || fragmentedNumeric) &&
                    skipDecodePolicy.numeric() &&
                    readers[column].kind() != ColumnReader.Kind.BINARY
                    ? readers[column].peekDictionarySize()
                    : -1;
            boolean wideNumericDictionary = dictionarySize >= skipDecodePolicy.numericMinDictionarySize();
            lazySkipColumn[column] = readers[column].kind() == ColumnReader.Kind.BINARY
                    ? amortizedRuns
                    : wideNumericDictionary && (amortizedRuns || fragmentedNumeric);
            if (skipDecodePolicy.diagnostics() && wideNumericDictionary) {
                System.err.printf("[lazy-numeric-skip] column=%s dictionary=%s survivors=%s/%s fragmented=%s%n",
                        columnNames.get(column), dictionarySize, lazyMask.selectedCount(), lazyCount,
                        fragmentedNumeric && !amortizedRuns);
            }
        }
        return lazySkipColumn[column];
    }

    private boolean hasAmortizedSurvivorRuns(Mask mask)
    {
        return hasAmortizedSurvivorRuns(
                mask.selectedPositions(),
                mask.selectedCount(),
                skipDecodePolicy.minAverageRun());
    }

    private static boolean hasAmortizedSurvivorRuns(int[] positions, int selected, int minAverageRun)
    {
        if (selected == 0 || minAverageRun <= 1) {
            return true;
        }
        int runs = 1;
        for (int index = 1; index < selected; index++) {
            if (positions[index] != positions[index - 1] + 1) {
                runs++;
            }
        }
        return selected >= (long) runs * minAverageRun;
    }

    private int fragmentedNumericPayloadColumns(int currentColumn)
    {
        if (lazyFragmentedNumericPayloadColumns >= 0) {
            return lazyFragmentedNumericPayloadColumns;
        }
        int columns = 0;
        for (int column = 0; column < readers.length; column++) {
            // The lead filter has already resolved before constrain(). Count the current payload and unresolved
            // siblings only; projected-out inputs are absent from this scan entirely.
            if (column != currentColumn && lazyResolved[column]) {
                continue;
            }
            // Count the physical live numeric width without asking a reader to load or commit its current page.
            // The selected column's own dictionary eligibility is checked only after the width/density decision.
            if (readers[column].kind() != ColumnReader.Kind.BINARY) {
                columns++;
                if (columns > skipDecodePolicy.fragmentedNumeric().maxPayloadColumns()) {
                    break;
                }
            }
        }
        lazyFragmentedNumericPayloadColumns = columns;
        return columns;
    }

    /** Resolve (decode) one column for the current late-materialization batch, honoring the active mask. */
    private void resolveLazyColumn(int column)
    {
        if (lazyResolved[column]) {
            return;
        }
        lazyResolved[column] = true;
        int count = lazyCount;
        ColumnReader reader = readers[column];
        boolean isNullable = nullable[column];
        BooleanVector nullVector = isNullable
                ? allocator.allocate(allocationContext, BooleanVector.class, count, BooleanVector::new)
                : null;
        boolean[] nulls = nullVector == null ? null : nullVector.values();

        // A downstream operator can borrow payload vectors merely to establish their physical schema even when an
        // upstream filter removed every row in this batch (TopN is a common example). There is no live value to
        // decode, and using this empty mask as the column's permanent full-vs-selected decision is actively
        // misleading. Return correctly typed pooled vectors, leave the reader at its current position, and coalesce
        // this batch into the same pending advance used by an entirely unborrowed column. The first batch with actual
        // survivors then chooses the physical path and drains all preceding rows in that path. This is a generic
        // lifecycle rule: empty evidence neither pays decode work nor fixes an irreversible decoder strategy.
        if (lateMaterializationPolicy.deferEmptyConstrainedDecode() && lazyConstrained && lazyMask.none()) {
            lazyPendingAdvance[column] += count;
            if (diagnostics.sourceWork()) {
                debugLazyOmitted[column] += count;
            }
            currentValues[column] = allocateMaskedEmptyColumn(column, reader, count);
            currentNulls[column] = nullVector;
            return;
        }

        // Decide the reader's page path (fixed for life) before any read, then drain the rows this column has been
        // deferred over since it was last positioned. The drain uses the same page path as the decode below, so the
        // reader never mixes paths: a full-decode column fast-forwards via skip() (byte-skipping whole pages), a
        // skip-decode column drains through the readSelected page-skip with an empty survivor set.
        boolean skip = useSkipDecode(column);
        long pending = lazyPendingAdvance[column];
        if (pending > 0) {
            drainPendingAdvance(column, reader, skip, pending);
            lazyPendingAdvance[column] = 0;
        }

        // Path stability: a column decodes full only when this batch was never constrained (filter columns, read
        // before constrain; and every column of an unfiltered scan). Once constrain has fired, remaining columns
        // skip-decode — even if all rows happened to survive — so a reader never mixes the full and skip page paths
        // across batches (which would corrupt its cursor state). Binary has no skip path here, so it stays full;
        // a binary column is therefore always read before constrain or never (filter columns are numeric).
        if (!skip) {
            currentValues[column] = decodeFullColumn(column, reader, nulls, count);
            if (nullVector != null && reader.kind() == ColumnReader.Kind.BINARY && reader.lastReadNullsProvenAbsent()) {
                nullVector.declareAllFalse();
            }
            currentNulls[column] = nullVector;
            recordFullDecode(column, count);
            recordPublished(column, count);
            return;
        }

        // Skip-decode only the survivor rows (densely) then scatter to position. Non-survivor positions are left at
        // their default — the active mask excludes them, so no consumer reads them.
        int survivorCount;
        int[] survivors;
        if (lazyMask.all()) {
            survivorCount = count;
            survivors = identitySurvivors(count);
        }
        else {
            survivorCount = lazyMask.selectedCount();
            survivors = lazyMask.selectedPositions();
        }
        if (reader.kind() == ColumnReader.Kind.BINARY) {
            // readSelectedBinary returns a position-indexed vector (survivors at their positions, others zero-length)
            // and fills position-indexed nulls, so no scatter is needed.
            Vector vector = reader.readSelectedBinary(allocator, allocationContext, survivors, survivorCount, count, nulls);
            recordSelectedDecode(column, survivorCount);
            recordPublished(column, survivorCount);
            currentValues[column] = vector;
            currentNulls[column] = nullVector;
            return;
        }
        if (reader.kind() == ColumnReader.Kind.INT) {
            ensureLazyScratch(survivorCount, false);
            reader.readSelectedInts(survivors, survivorCount, count, lazyScratchInt, isNullable ? lazyScratchNull : null);
            recordSelectedDecode(column, survivorCount);
            Vector vector = allocateIntOutput(column, count);
            scatterIntOutput(vector, survivors, lazyScratchInt, survivorCount);
            recordCopied(column, survivorCount);
            if (nulls != null) {
                for (int j = 0; j < survivorCount; j++) {
                    nulls[survivors[j]] = lazyScratchNull[j];
                }
            }
            currentValues[column] = vector;
        }
        else {
            ensureLazyScratch(survivorCount, true);
            reader.readSelectedLongs(survivors, survivorCount, count, lazyScratchLong, isNullable ? lazyScratchNull : null);
            recordSelectedDecode(column, survivorCount);
            if (reader.isDouble()) {
                org.weakref.nitro.data.F64Vector vector = org.weakref.nitro.data.F64Vector.allocate(allocator, allocationContext, count);
                double[] out = vector.values();
                for (int j = 0; j < survivorCount; j++) {
                    out[survivors[j]] = Double.longBitsToDouble(lazyScratchLong[j]);
                }
                if (nulls != null) {
                    for (int j = 0; j < survivorCount; j++) {
                        nulls[survivors[j]] = lazyScratchNull[j];
                    }
                }
                currentValues[column] = vector;
                currentNulls[column] = nullVector;
                recordCopied(column, survivorCount);
                recordPublished(column, survivorCount);
                return;
            }
            I64Vector vector = I64Vector.allocate(allocator, allocationContext, count);
            long[] out = vector.values();
            for (int j = 0; j < survivorCount; j++) {
                out[survivors[j]] = lazyScratchLong[j];
            }
            if (nulls != null) {
                for (int j = 0; j < survivorCount; j++) {
                    nulls[survivors[j]] = lazyScratchNull[j];
                }
            }
            currentValues[column] = vector;
            recordCopied(column, survivorCount);
        }
        currentNulls[column] = nullVector;
        recordPublished(column, survivorCount);
    }

    /** Allocate a correctly typed vector for a batch whose active mask is empty; no value is semantically live. */
    private Vector allocateMaskedEmptyColumn(int column, ColumnReader reader, int count)
    {
        return switch (reader.kind()) {
            case INT -> allocateIntOutput(column, count);
            case LONG -> reader.isDouble()
                    ? org.weakref.nitro.data.F64Vector.allocate(allocator, allocationContext, count)
                    : I64Vector.allocate(allocator, allocationContext, count);
            case BINARY -> BinaryVector.allocate(allocator, allocationContext, count, 0);
        };
    }

    /** Reinterpret {@code count} raw long bits (read through the long path for a DOUBLE column) into a double vector. */
    private org.weakref.nitro.data.F64Vector longBitsToDoubles(long[] bits, int offset, int count)
    {
        return longBitsToDoubles(bits, offset, count, count);
    }

    private org.weakref.nitro.data.F64Vector longBitsToDoubles(long[] bits, int offset, int count, int capacity)
    {
        if (capacity < count) {
            throw new IllegalArgumentException("capacity is smaller than count");
        }
        org.weakref.nitro.data.F64Vector vector = org.weakref.nitro.data.F64Vector.allocate(allocator, allocationContext, capacity);
        double[] values = vector.values();
        for (int i = 0; i < count; i++) {
            values[i] = Double.longBitsToDouble(bits[offset + i]);
        }
        return vector;
    }

    private Vector allocateIntOutput(int column, int capacity)
    {
        if (intOutputAsLong[column]) {
            return I64Vector.allocate(allocator, allocationContext, capacity);
        }
        return I32Vector.allocate(allocator, allocationContext, capacity);
    }

    private Vector copyIntOutput(int column, int[] source, int offset, int count, int capacity)
    {
        Vector output = allocateIntOutput(column, capacity);
        if (output instanceof I64Vector vector) {
            long[] values = vector.values();
            for (int index = 0; index < count; index++) {
                values[index] = source[offset + index];
            }
        }
        else {
            System.arraycopy(source, offset, ((I32Vector) output).values(), 0, count);
        }
        return output;
    }

    private static void scatterIntOutput(Vector output, int[] positions, int[] source, int count)
    {
        if (output instanceof I64Vector vector) {
            long[] values = vector.values();
            for (int index = 0; index < count; index++) {
                values[positions[index]] = source[index];
            }
        }
        else {
            int[] values = ((I32Vector) output).values();
            for (int index = 0; index < count; index++) {
                values[positions[index]] = source[index];
            }
        }
    }

    private Vector decodeFullColumn(int column, ColumnReader reader, boolean[] nulls, int count)
    {
        return switch (reader.kind()) {
            case INT -> {
                yield reader.readNumeric(allocator, allocationContext, nulls, count, intOutputAsLong[column]);
            }
            case LONG -> {
                if (reader.isDouble()) {
                    if (doubleDecodeScratch == null || doubleDecodeScratch.length < count) {
                        doubleDecodeScratch = replaceLongs(doubleDecodeScratch, count);
                    }
                    long[] bits = doubleDecodeScratch;
                    reader.readLongs(bits, nulls, count);
                    recordCopied(column, count);
                    yield longBitsToDoubles(bits, 0, count);
                }
                yield reader.readNumeric(allocator, allocationContext, nulls, count, false);
            }
            case BINARY -> {
                yield reader.readBinary(allocator, allocationContext, nulls, count);
            }
        };
    }

    /**
     * Record that any column not pulled this batch is now {@code lazyCount} rows further behind. No reader work: the
     * deferred rows accumulate and are byte-skipped in one call the next time the column is pulled (see
     * {@link #drainPendingAdvance}). Coalescing the skip is what lets whole data pages be dropped without decoding.
     */
    private void advanceUnresolvedColumns()
    {
        if (lazyResolved == null) {
            return;
        }
        for (int c = 0; c < readers.length; c++) {
            if (filterEvaluationPolicy.directNullMask().reader() && nullable[c] && !directNullResolved[c]) {
                directNullPendingAdvance[c] += lazyCount;
            }
            if (lazyResolved[c]) {
                continue;
            }
            lazyResolved[c] = true;
            lazyPendingAdvance[c] += lazyCount;
            if (diagnostics.sourceWork()) {
                debugLazyOmitted[c] += lazyCount;
            }
        }
    }

    /**
     * Fast-forward {@code reader} past {@code pending} deferred rows to the current batch start, staying on the
     * column's fixed page path: a full-decode column via {@link ColumnReader#skip} (byte-skips whole pages), a
     * skip-decode column via the readSelected page-skip with an empty survivor set. Both drop whole data pages that
     * fall entirely within the skip without decoding them.
     */
    private void drainPendingAdvance(int column, ColumnReader reader, boolean skip, long pending)
    {
        recordSkipped(column, pending);
        if (!skip) {
            reader.skip(pending);
            return;
        }
        boolean longKind = reader.kind() == ColumnReader.Kind.LONG;
        while (pending > 0) {
            int rows = (int) Math.min(pending, 1 << 30);
            if (reader.kind() == ColumnReader.Kind.BINARY) {
                reader.skipSelectedBinary(rows);
            }
            else if (longKind) {
                ensureLazyScratch(0, true);
                reader.readSelectedLongs(EMPTY, 0, rows, lazyScratchLong, null);
            }
            else {
                ensureLazyScratch(0, false);
                reader.readSelectedInts(EMPTY, 0, rows, lazyScratchInt, null);
            }
            pending -= rows;
        }
    }

    /** A reusable identity array {@code [0, 1, ..., count)} used as the survivor set when a constrained batch kept all rows. */
    private int[] identitySurvivors(int count)
    {
        if (lazyIdentity.length < count) {
            int[] identity = arrayPool.borrowInts(count);
            for (int i = 0; i < count; i++) {
                identity[i] = i;
            }
            arrayPool.release(lazyIdentity);
            lazyIdentity = identity;
        }
        return lazyIdentity;
    }

    private void ensureLazyScratch(int survivorCount, boolean longKind)
    {
        if (longKind) {
            if (lazyScratchLong.length < survivorCount) {
                lazyScratchLong = replaceLongs(lazyScratchLong, survivorCount);
            }
        }
        else if (lazyScratchInt.length < survivorCount) {
            lazyScratchInt = replaceInts(lazyScratchInt, survivorCount);
        }
        if (lazyScratchNull.length < survivorCount) {
            lazyScratchNull = replaceBooleans(lazyScratchNull, survivorCount);
        }
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
        lazyOutputResolution = false;
        int[] order = filterOrder();
        ensureScratch(count);

        int[] survivors = null;     // window-relative positions, sorted ascending; null means "all count rows"
        int survivorCount = count;
        int applied = 0;
        for (; applied < order.length && survivorCount > 0; applied++) {
            int column = order[applied];
            LongDomain filter = filtersByColumn[column];
            int kept;
            if (survivors == null) {
                if (outputRequired[column]) {
                    ensureColumnScratch(column, count);
                }
                // Lead filter: predicate-over-dictionary. Read ids + test a per-chunk acceptById[] WITHOUT
                // materializing the column; only survivors get a value. Output is dense, aligned to the survivors
                // (readPositions records that), so the later gather two-pointers it to the final survivor set.
                boolean[] cn = outputRequired[column] && nullable[column] ? colNull[column] : null;
                java.util.function.LongPredicate predicate = filter;
                VersionedLongPredicate predicateVersion = filterVersionsByColumn[column];
                if (lateRowLevelFiltersByColumn[column]) {
                    kept = filterLateLeadColumn(column, filter, count, nextSurvivors);
                }
                else if (readers[column].kind() == ColumnReader.Kind.LONG) {
                    kept = readers[column].filterDictLongs(predicate, predicateVersion, count, nextSurvivors,
                            outputRequired[column] ? colLong[column] : null, cn);
                }
                else {
                    kept = readers[column].filterDictInts(predicate, predicateVersion, count, nextSurvivors,
                            outputRequired[column] ? colInt[column] : null, cn);
                }
                if (diagnostics.sourceWork() && !lateRowLevelFiltersByColumn[column]) {
                    debugDictionaryExamined[column] += count;
                }
                survivors = applied + 1 < order.length
                        ? snapshotFilterSurvivors(column, nextSurvivors, kept)
                        : nextSurvivors;
                readPositions[column] = survivors;
                if (diagnostics.rowCounts()) {
                    debugFilterInputs[column] += count;
                    debugFilterOutputs[column] += kept;
                }
                survivorCount = kept;
                checkSurvivorBounds(column, survivors, survivorCount, count);
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
            long observedRows = progressiveFilterObservedRows[column];
            long observedKept = progressiveFilterObservedKept[column];
            int alignedFilterColumns = alignedFilterColumnCount(order, applied, survivors);
            ParquetProgressiveFilterCompactionPolicy.Fused fusedPolicy =
                    progressiveFilterCompactionPolicy.fused();
            boolean fusedCompaction = fusedPolicy.enabled() &&
                    readers.length <= progressiveFilterCompactionPolicy.maxColumns() &&
                    alignedFilterColumns >= fusedPolicy.minAlignedColumns() &&
                    observedRows >= progressiveFilterCompactionPolicy.minObservedRows() &&
                    observedKept * 100 > observedRows * progressiveFilterCompactionPolicy.maxPercent() &&
                    observedKept * 100 <= observedRows * fusedPolicy.maxPercent() &&
                    sampledAverageRunAtLeast(
                            survivors,
                            rows,
                            fusedPolicy.minAverageRun());
            if (progressiveFilterCompactionPolicy.diagnostics() &&
                    fusedCompaction &&
                    !debugFusedCompactionCandidatePrinted) {
                debugFusedCompactionCandidatePrinted = true;
                System.err.printf("[fused-progressive-filter-candidate] columns=%s aligned=%d observed=%d/%d sampledAverageRun=%.3f%n",
                        columnNames,
                        alignedFilterColumns,
                        observedKept,
                        observedRows,
                        sampledAverageRun(survivors, rows));
            }
            boolean rankForCompaction = progressiveFilterCompactionPolicy.enabled() &&
                    !fusedCompaction &&
                    readers.length <= progressiveFilterCompactionPolicy.maxColumns() &&
                    ((observedRows == 0
                            ? progressiveFilterCompactionPolicy.minObservedRows() == 0
                            : observedKept * 100 <= observedRows * progressiveFilterCompactionPolicy.maxPercent()) &&
                            observedRows + rows >= progressiveFilterCompactionPolicy.minObservedRows() ||
                            prospectiveProgressiveFilterCompaction(order, applied, filter, observedRows + rows));
            kept = 0;
            if (fusedCompaction) {
                // The filter already has the accepted input rank hot. Compact every column aligned to this stage
                // before publishing the physical survivor position, avoiding both a retained rank vector and a
                // later branch-heavy two-pointer reconstruction. Output ranks never exceed input ranks, so each
                // in-place copy is stable even when the source and destination arrays are identical.
                for (int i = 0; i < rows; i++) {
                    if (nulls != null && nulls[i]) {
                        continue;
                    }
                    if (filter.test(isLong ? longValues[i] : intValues[i])) {
                        compactAlignedFilterRow(order, applied, survivors, i, kept);
                        next[kept++] = survivors[i];
                    }
                }
            }
            else if (rankForCompaction) {
                // Keep the accepted dense rank until all columns aligned to this input have had a chance to compact.
                // Physical survivor positions are restored in place below before the next reader sees the array.
                for (int i = 0; i < rows; i++) {
                    if (nulls != null && nulls[i]) {
                        continue;
                    }
                    if (filter.test(isLong ? longValues[i] : intValues[i])) {
                        next[kept++] = i;
                    }
                }
            }
            else {
                for (int i = 0; i < rows; i++) {
                    if (nulls != null && nulls[i]) {
                        continue;
                    }
                    if (filter.test(isLong ? longValues[i] : intValues[i])) {
                        next[kept++] = survivors[i];
                    }
                }
            }
            progressiveFilterObservedRows[column] = observedRows + rows;
            progressiveFilterObservedKept[column] = observedKept + kept;
            int[] inputSurvivors = survivors;
            if (progressiveFilterCompactionAdmitted && kept == rows) {
                // An all-pass stage must not replace an equal survivor set with a different array identity. Keeping
                // the existing identity lets all columns decoded at this stage flow directly to the output window.
                survivors = inputSurvivors;
            }
            else {
                boolean compact = fusedCompaction || (rankForCompaction &&
                        (long) kept * 100 <=
                                (long) rows * progressiveFilterCompactionPolicy.maxPercent());
                if (rankForCompaction && compact) {
                    compactAlignedFilterColumns(order, applied, inputSurvivors, next, kept);
                }
                if (rankForCompaction) {
                    for (int i = 0; i < kept; i++) {
                        next[i] = inputSurvivors[next[i]];
                    }
                }
                survivors = applied + 1 < order.length ? snapshotFilterSurvivors(column, next, kept) : next;
                if (compact) {
                    progressiveFilterCompactionAdmitted = true;
                    for (int i = 0; i <= applied; i++) {
                        int alignedColumn = order[i];
                        if (readPositions[alignedColumn] == inputSurvivors) {
                            readPositions[alignedColumn] = survivors;
                        }
                    }
                    if (progressiveFilterCompactionPolicy.diagnostics()) {
                        debugProgressiveCompactionRows += rows;
                        debugProgressiveCompactionKept += kept;
                        debugProgressiveCompactionWindows++;
                    }
                }
            }
            if (diagnostics.rowCounts()) {
                debugFilterInputs[column] += rows;
                debugFilterOutputs[column] += kept;
            }
            survivorCount = kept;
            checkSurvivorBounds(column, survivors, survivorCount, count);
        }

        // If a filter wiped out the window, the remaining filter columns were never read — advance their readers
        // so they stay aligned to the window boundary for the next call.
        for (int i = applied; i < order.length; i++) {
            advanceColumn(order[i], count);
        }
        // Decode the payload (non-filter) columns at the final survivors. Choose the page path ONCE, from the first
        // window's survival rate, and freeze it for the scan: skip-decode straight into the dense window buffer when
        // few rows survive (drops whole non-surviving pages), or bulk-decode the window once and gather to survivors
        // when most survive (a weak/unclustered filter). A reader must never mix the two page paths across windows,
        // so the decision is frozen rather than recomputed per window.
        if (!dfPayloadDecided) {
            dfPayloadBulk = filteredPayloadPolicy.useBulkDecode(survivors, survivorCount, count);
            dfPayloadDecided = true;
        }
        // A whole filtered window that fits in one public batch can remain open across the first downstream
        // boundary. Filter/key columns are already materialized; payload readers stay at the window start until a
        // consumer borrows them after optionally constraining the batch. Larger windows retain eager slicing because
        // several output batches would otherwise share one irreversible reader position.
        ParquetFilteredPayloadPolicy.Deferred deferredPolicy = filteredPayloadPolicy.deferred();
        deferredFilteredPayload = deferredPolicy.enabled() &&
                !dfPayloadBulk &&
                survivorCount > 0 &&
                survivorCount <= currentBatchRows &&
                payloadColumnCount() >= deferredPolicy.minColumns();
        deferredWindowRows = count;
        deferredWindowSurvivors = survivors;
        if (deferredFilteredPayload) {
            if (lazyResolved == null || lazyResolved.length < columnCount) {
                lazyResolved = new boolean[columnCount];
            }
            java.util.Arrays.fill(lazyResolved, 0, columnCount, false);
            for (int column : order) {
                lazyResolved[column] = true;
            }
        }
        for (int c = 0; c < columnCount; c++) {
            if (isFilterColumn(c)) {
                continue;
            }
            if (deferredFilteredPayload) {
                continue;
            }
            if (readers[c].kind() == ColumnReader.Kind.BINARY) {
                decodeBinaryWindow(c, survivors, survivorCount, count, dfPayloadBulk);
                continue;
            }
            boolean[] nulls = nullable[c] ? ensureWindowNull(c, survivorCount) : null;
            if (dfPayloadBulk) {
                readColumnInto(c, null, count, count);
                boolean[] columnNulls = nullable[c] ? colNull[c] : null;
                if (readers[c].kind() == ColumnReader.Kind.INT) {
                    windowInt[c] = ensureInt(windowInt[c], survivorCount);
                    gatherInt(colInt[c], columnNulls, null, survivors, survivorCount, windowInt[c], nulls);
                }
                else {
                    windowLong[c] = ensureLong(windowLong[c], survivorCount);
                    gatherLong(colLong[c], columnNulls, null, survivors, survivorCount, windowLong[c], nulls);
                }
                recordCopied(c, survivorCount);
            }
            else if (readers[c].kind() == ColumnReader.Kind.INT) {
                windowInt[c] = ensureInt(windowInt[c], survivorCount);
                readers[c].readSelectedInts(survivors, survivorCount, count, windowInt[c], nulls);
                recordSelectedDecode(c, survivorCount);
            }
            else {
                windowLong[c] = ensureLong(windowLong[c], survivorCount);
                readers[c].readSelectedLongs(survivors, survivorCount, count, windowLong[c], nulls);
                recordSelectedDecode(c, survivorCount);
            }
        }

        // Gather the FILTER columns (read at a wider survivor superset) down to the final survivors. Payload columns
        // were already read straight into the window buffer above.
        for (int c = 0; c < columnCount; c++) {
            if (!isFilterColumn(c) || !outputRequired[c]) {
                continue;
            }
            if (readPositions[c] == survivors) {
                if (readers[c].kind() == ColumnReader.Kind.INT) {
                    windowInt[c] = colInt[c];
                }
                else {
                    windowLong[c] = colLong[c];
                }
                if (nullable[c]) {
                    windowNull[c] = colNull[c];
                }
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
            recordCopied(c, survivorCount);
        }
        windowSurvivorCount = survivorCount;
    }

    /** Filter from the reader's current page cursor when a residual predicate narrows after scanning has begun. */
    private int filterLateLeadColumn(int column, LongDomain filter, int count, int[] survivors)
    {
        readColumnInto(column, null, count, count);
        boolean isLong = readers[column].kind() == ColumnReader.Kind.LONG;
        long[] longValues = colLong[column];
        int[] intValues = colInt[column];
        boolean[] nulls = nullable[column] ? colNull[column] : null;
        int kept = 0;
        for (int position = 0; position < count; position++) {
            if (nulls != null && nulls[position]) {
                continue;
            }
            if (filter.test(isLong ? longValues[position] : intValues[position])) {
                if (outputRequired[column]) {
                    if (isLong) {
                        longValues[kept] = longValues[position];
                    }
                    else {
                        intValues[kept] = intValues[position];
                    }
                    if (nulls != null) {
                        nulls[kept] = false;
                    }
                }
                survivors[kept++] = position;
            }
        }
        return kept;
    }

    /** Decode one mixed-scan BINARY payload densely at the final dynamic-filter survivor set. */
    private void decodeBinaryWindow(int column, int[] survivors, int survivorCount, int count, boolean bulk)
    {
        releaseWindowBinary(column);
        if (survivorCount == 0) {
            readers[column].skipSelectedBinary(count);
            return;
        }

        boolean[] rawNulls = nullable[column] ? ensureColumnNullScratch(column, count) : null;
        // For a dictionary-only physical column, reading the full logical window preserves the compact ids + values
        // domain and avoids copying a variable-width value for every survivor. Selection density alone is therefore
        // not a sufficient admission signal: the encoded read is normally cheaper even for a sparse survivor set.
        boolean preserveDictionary = readers[column].isDictionaryOnly();
        // Flat binary payloads can compact directly even when the window-level policy favors bulk decoding. The
        // selected reader still whole-decodes dense/fragmented pages, but writes only survivors to the destination;
        // constructing a full logical vector first would add another variable-width byte copy with no downstream
        // benefit. Dictionary-only columns retain their ids + shared value domain instead.
        if (!preserveDictionary) {
            boolean[] denseNulls = nullable[column] ? ensureWindowNull(column, survivorCount) : null;
            windowBinary[column] = readers[column].readCompactedBinary(
                    allocator,
                    allocationContext,
                    survivors,
                    survivorCount,
                    count,
                    denseNulls);
            recordSelectedDecode(column, survivorCount);
            recordCopied(column, survivorCount);
            return;
        }
        Vector raw = readers[column].readBinary(allocator, allocationContext, rawNulls, count);
        try {
            windowBinary[column] = raw instanceof DictionaryVector dictionary
                    ? dictionary.copyPositionsPreservingEncodingBorrowingValues(allocator, allocationContext, survivors, survivorCount)
                    : raw.copyPositionsInto(
                            allocator,
                            allocationContext,
                            null,
                            survivors,
                            survivorCount,
                            0,
                            survivorCount);
        }
        finally {
            allocator.release(allocationContext, raw);
        }
        if (nullable[column]) {
            boolean[] denseNulls = ensureWindowNull(column, survivorCount);
            for (int output = 0; output < survivorCount; output++) {
                denseNulls[output] = rawNulls[survivors[output]];
            }
        }
        if (bulk || preserveDictionary) {
            recordFullDecode(column, count);
        }
        else {
            recordSelectedDecode(column, survivorCount);
        }
        recordCopied(column, survivorCount);
    }

    private boolean[] ensureColumnNullScratch(int column, int size)
    {
        if (colNull[column] == null || colNull[column].length < size) {
            colNull[column] = replaceBooleans(colNull[column], size);
        }
        return colNull[column];
    }

    private void releaseWindowBinary(int column)
    {
        if (windowBinary[column] != null) {
            allocator.release(allocationContext, windowBinary[column]);
            windowBinary[column] = null;
        }
    }

    private static void checkSurvivorBounds(int column, int[] survivors, int survivorCount, int count)
    {
        if (survivorCount > 0 && (survivors[0] < 0 || survivors[survivorCount - 1] >= count)) {
            throw new IllegalStateException("Filter column %s produced survivor range [%s, %s] outside window of %s rows"
                    .formatted(column, survivors[0], survivors[survivorCount - 1], count));
        }
    }

    /** Compact every previously decoded filter column whose dense values align with {@code inputSurvivors}. */
    private void compactAlignedFilterColumns(int[] order, int applied, int[] inputSurvivors, int[] acceptedRanks, int kept)
    {
        for (int i = 0; i <= applied; i++) {
            int column = order[i];
            if (!outputRequired[column] || readPositions[column] != inputSurvivors) {
                continue;
            }
            if (readers[column].kind() == ColumnReader.Kind.LONG) {
                long[] values = colLong[column];
                for (int output = 0; output < kept; output++) {
                    values[output] = values[acceptedRanks[output]];
                }
            }
            else {
                int[] values = colInt[column];
                for (int output = 0; output < kept; output++) {
                    values[output] = values[acceptedRanks[output]];
                }
            }
            boolean[] nulls = colNull[column];
            if (nulls != null) {
                for (int output = 0; output < kept; output++) {
                    nulls[output] = nulls[acceptedRanks[output]];
                }
            }
        }
    }

    private int alignedFilterColumnCount(int[] order, int applied, int[] inputSurvivors)
    {
        int count = 0;
        for (int i = 0; i <= applied; i++) {
            if (readPositions[order[i]] == inputSurvivors) {
                count++;
            }
        }
        return count;
    }

    private boolean prospectiveProgressiveFilterCompaction(int[] order, int applied, LongDomain filter, long intermediateRows)
    {
        ParquetProgressiveFilterCompactionPolicy.Prospective prospectivePolicy =
                progressiveFilterCompactionPolicy.prospective();
        if (!prospectivePolicy.enabled() ||
                nextRow < prospectivePolicy.minRawRows() ||
                (double) nextRow / totalRows * 100 < prospectivePolicy.minObservedPercent() ||
                filter.rangeDensity() * 100 > progressiveFilterCompactionPolicy.maxPercent() ||
                !remainingFiltersObservedNonSelective(order, applied)) {
            return false;
        }
        // Compare the projected full-scan intermediate count without integer multiplication overflow.
        double projectedRows = (double) intermediateRows / nextRow * totalRows;
        boolean admitted = projectedRows >= prospectivePolicy.minProjectedRows();
        if (progressiveFilterCompactionPolicy.diagnostics() &&
                admitted &&
                !debugProspectiveCompactionCandidatePrinted) {
            debugProspectiveCompactionCandidatePrinted = true;
            System.err.printf("[prospective-progressive-filter-candidate] columns=%s raw=%d/%d intermediate=%d projected=%.0f rangeDensity=%.4f%n",
                    columnNames, nextRow, totalRows, intermediateRows, projectedRows, filter.rangeDensity());
        }
        return admitted;
    }

    private boolean remainingFiltersObservedNonSelective(int[] order, int applied)
    {
        for (int index = applied + 1; index < order.length; index++) {
            int column = order[index];
            long observedRows = progressiveFilterObservedRows[column];
            // Prospective compaction is useful only when this filter is the final effective narrowing stage. Use
            // work the scan has already performed: every later filter must have passed every observed row, and its
            // dictionary/range estimate must also describe a full-domain filter. This avoids decompressing every
            // later column dictionary merely to make an admission decision.
            if (observedRows == 0 ||
                    progressiveFilterObservedKept[column] != observedRows ||
                    estimateSelectivity(column) < 1.0) {
                return false;
            }
        }
        return true;
    }

    private static double sampledAverageRun(int[] positions, int count)
    {
        int sample = Math.min(count, 1024);
        int runs = 1;
        for (int index = 1; index < sample; index++) {
            if (positions[index] != positions[index - 1] + 1) {
                runs++;
            }
        }
        return (double) sample / runs;
    }

    private static boolean sampledAverageRunAtLeast(int[] positions, int count, int minimumAverageRun)
    {
        if (minimumAverageRun <= 1) {
            return true;
        }
        int sample = Math.min(count, 1024);
        int runs = 1;
        for (int index = 1; index < sample; index++) {
            if (positions[index] != positions[index - 1] + 1) {
                runs++;
            }
        }
        return sample >= (long) runs * minimumAverageRun;
    }

    private void compactAlignedFilterRow(int[] order, int applied, int[] inputSurvivors, int input, int output)
    {
        for (int i = 0; i <= applied; i++) {
            int column = order[i];
            if (!outputRequired[column] || readPositions[column] != inputSurvivors) {
                continue;
            }
            if (readers[column].kind() == ColumnReader.Kind.LONG) {
                colLong[column][output] = colLong[column][input];
            }
            else {
                colInt[column][output] = colInt[column][input];
            }
            if (colNull[column] != null) {
                colNull[column][output] = colNull[column][input];
            }
        }
    }

    private boolean isFilterColumn(int column)
    {
        return filtersByColumn[column] != null;
    }

    private int payloadColumnCount()
    {
        int count = 0;
        for (int column = 0; column < readers.length; column++) {
            if (!isFilterColumn(column)) {
                count++;
            }
        }
        return count;
    }

    /** Hand out the next configured maximum of surviving rows as an all-rows output batch. */
    private SourceBatch emitSlice()
    {
        int columnCount = readers.length;
        int start = windowSurvivorCursor;
        int sliceCount = Math.min(currentBatchRows, windowSurvivorCount - start);
        windowSurvivorCursor += sliceCount;
        lazyFilteredWindowOutputs = true;
        filteredWindowSliceStart = start;
        filteredWindowSliceCount = sliceCount;

        VectorColumnGeneration[] outputs = new VectorColumnGeneration[columnCount];
        for (int c = 0; c < columnCount; c++) {
            if (isFilterColumn(c)) {
                currentValues[c] = null;
                currentNulls[c] = null;
                ScanOutputResolver outputResolver = outputResolvers[c];
                outputs[c] = new VectorColumnGeneration(
                        nullable[c] ? Set.of(Stream.VALUES, Stream.NULLS) : Set.of(Stream.VALUES),
                        outputResolver.resolver(),
                        outputResolver);
                continue;
            }
            if (deferredFilteredPayload && !isFilterColumn(c)) {
                currentValues[c] = null;
                currentNulls[c] = null;
                ScanOutputResolver outputResolver = outputResolvers[c];
                outputs[c] = new VectorColumnGeneration(
                        nullable[c] ? Set.of(Stream.VALUES, Stream.NULLS) : Set.of(Stream.VALUES),
                        outputResolver.resolver(),
                        outputResolver);
                continue;
            }
            // Allocate at the fixed window batch capacity, not the (variable) partial-slice length, so the vector pool
            // hits every time instead of missing on each window's final short slice. Only [0, sliceCount) is written
            // and only that range is exposed (the batch mask below is sliceCount positions); consumers honor the mask,
            // never the backing length.
            BooleanVector nullVector = nullable[c]
                    ? allocator.allocate(allocationContext, BooleanVector.class, batchPolicy.maxRows(), BooleanVector::new)
                    : null;
            Vector valueVector;
            if (readers[c].kind() == ColumnReader.Kind.INT) {
                valueVector = copyIntOutput(c, windowInt[c], start, sliceCount, batchPolicy.maxRows());
            }
            else if (readers[c].kind() == ColumnReader.Kind.BINARY) {
                if (windowSlicePositions.length < sliceCount) {
                    windowSlicePositions = replaceInts(windowSlicePositions, sliceCount);
                }
                for (int position = 0; position < sliceCount; position++) {
                    windowSlicePositions[position] = start + position;
                }
                valueVector = windowBinary[c] instanceof DictionaryVector dictionary
                        ? dictionary.copyPositionsPreservingEncodingBorrowingValues(allocator, allocationContext, windowSlicePositions, sliceCount)
                        : windowBinary[c].copyPositionsInto(
                                allocator,
                                allocationContext,
                                null,
                                windowSlicePositions,
                                sliceCount,
                                0,
                                batchPolicy.maxRows());
            }
            else if (readers[c].isDouble()) {
                valueVector = longBitsToDoubles(windowLong[c], start, sliceCount, batchPolicy.maxRows());
            }
            else {
                I64Vector vector = I64Vector.allocate(allocator, allocationContext, batchPolicy.maxRows());
                System.arraycopy(windowLong[c], start, vector.values(), 0, sliceCount);
                valueVector = vector;
            }
            if (nullVector != null) {
                System.arraycopy(windowNull[c], start, nullVector.values(), 0, sliceCount);
            }
            currentValues[c] = valueVector;
            currentNulls[c] = nullVector;
            recordPublished(c, sliceCount);
            recordCopied(c, sliceCount);
            ScanOutputResolver outputResolver = outputResolvers[c];
            outputs[c] = new VectorColumnGeneration(
                    nullVector == null ? Set.of(Stream.VALUES) : Set.of(Stream.VALUES, Stream.NULLS),
                    outputResolver.resolver(),
                    outputResolver);
        }
        Mask mask = allocator.allocateAllMask(allocationContext, sliceCount);
        if (deferredFilteredPayload) {
            lazyMask = mask;
            lazyCount = sliceCount;
            lazyConstrained = false;
        }
        beginAdaptiveBatch(sliceCount);
        SourceBatch batch = new VectorSourceBatch(
                outputSchema,
                mask,
                outputs,
                batchBuffers,
                deferredFilteredPayload ? lazyMaskConstrainer : adaptiveConstrainer,
                deferredFilteredPayload ? deferredFilteredClose : adaptiveClose);
        currentBatch = batch;
        return batch;
    }

    private final class ScanOutputResolver
            implements BatchBufferOwner
    {
        private final int column;
        private final Function<Stream, Vector> resolver = this::resolve;
        private final VectorColumnGeneration.MaskResolver maskResolver = this::resolveMask;

        private ScanOutputResolver(int column)
        {
            this.column = column;
        }

        private Function<Stream, Vector> resolver()
        {
            return resolver;
        }

        private VectorColumnGeneration.MaskResolver maskResolver()
        {
            return maskResolver;
        }

        @Override
        public Vector take(Vector vector)
        {
            readers[column].markDictionaryValuesEscaped(vector);
            return batchBuffers.take(vector);
        }

        @Override
        public void release(Vector vector)
        {
            batchBuffers.release(vector);
        }

        @Override
        public void releaseAll()
        {
            batchBuffers.releaseAll();
        }

        private Mask resolveMask(Stream stream, Mask mask, boolean selectTrue, Allocator resultAllocator, Allocator.Context resultContext)
        {
            if (!lazyOutputResolution || stream != Stream.NULLS ||
                    !filterEvaluationPolicy.directNullMask().reader() || !nullable[column]) {
                return null;
            }
            ColumnReader reader = nullReaders[column];
            if (reader == null) {
                reader = readers[column].newSibling();
                nullReaders[column] = reader;
            }
            long pending = directNullPendingAdvance[column];
            if (pending > 0) {
                reader.skipNulls(pending);
                directNullPendingAdvance[column] = 0;
            }
            // The direct compactor consumes the sibling null reader without materializing a reusable Boolean stream.
            // A second mask request in the same batch therefore falls back to ordinary lazy stream resolution, which
            // remains at the correct cursor on the primary reader and caches the full stream for subsequent callers.
            if (filterEvaluationPolicy.directNullMask().compaction() && directNullResolved[column]) {
                return null;
            }
            if (!directNullResolved[column]) {
                Mask result = resultAllocator.copyMask(resultContext, mask);
                if (diagnostics.sourceWork()) {
                    debugNullExamined[column] += lazyCount;
                }
                if (filterEvaluationPolicy.directNullMask().compaction()) {
                    reader.retainNulls(result, selectTrue, lazyCount);
                    directNullResolved[column] = true;
                    return result;
                }
                if (directNullScratch[column].length < lazyCount) {
                    directNullScratch[column] = replaceBooleans(directNullScratch[column], lazyCount);
                }
                reader.readNulls(directNullScratch[column], lazyCount);
                directNullResolved[column] = true;
                result.retainBooleans(directNullScratch[column], selectTrue);
                return result;
            }
            Mask result = resultAllocator.copyMask(resultContext, mask);
            result.retainBooleans(directNullScratch[column], selectTrue);
            return result;
        }

        private Vector resolve(Stream stream)
        {
            if (!outputRequired[column]) {
                throw new IllegalStateException("source output was declared unused: " + column);
            }
            if (lazyFilteredWindowOutputs && isFilterColumn(column)) {
                resolveFilteredWindowColumn(column);
            }
            else if (deferredFilteredPayload) {
                resolveDeferredFilteredColumn(column);
            }
            else if (lazyOutputResolution) {
                resolveLazyColumn(column);
            }
            return switch (stream) {
                case VALUES -> requireNonNull(currentValues[column], "VALUES stream is absent");
                case NULLS -> requireNonNull(currentNulls[column], "NULLS stream is absent");
                default -> throw new IllegalArgumentException("Output does not expose stream: " + stream);
            };
        }
    }

    /** Materialize one already-filtered window column only when a downstream operator actually borrows it. */
    private void resolveFilteredWindowColumn(int column)
    {
        if (currentValues[column] != null) {
            return;
        }
        int start = filteredWindowSliceStart;
        int count = filteredWindowSliceCount;
        BooleanVector nullVector = nullable[column]
                ? allocator.allocate(allocationContext, BooleanVector.class, batchPolicy.maxRows(), BooleanVector::new)
                : null;
        Vector valueVector;
        if (readers[column].kind() == ColumnReader.Kind.INT) {
            valueVector = copyIntOutput(column, windowInt[column], start, count, batchPolicy.maxRows());
        }
        else if (readers[column].isDouble()) {
            valueVector = longBitsToDoubles(windowLong[column], start, count);
        }
        else {
            I64Vector vector = I64Vector.allocate(allocator, allocationContext, batchPolicy.maxRows());
            System.arraycopy(windowLong[column], start, vector.values(), 0, count);
            valueVector = vector;
        }
        if (nullVector != null) {
            System.arraycopy(windowNull[column], start, nullVector.values(), 0, count);
        }
        currentValues[column] = valueVector;
        currentNulls[column] = nullVector;
        recordPublished(column, count);
        recordCopied(column, count);
    }

    private void resolveDeferredFilteredColumn(int column)
    {
        if (lazyResolved[column]) {
            return;
        }
        lazyResolved[column] = true;
        int selected = lazyMask.selectedCount();
        boolean weakConstraint = (long) selected * 100 >
                (long) lazyCount * filteredPayloadPolicy.deferred().maxSurvivorPercent();
        int decodeCount = weakConstraint ? lazyCount : selected;
        if (deferredRawSurvivors.length < decodeCount) {
            deferredRawSurvivors = replaceInts(deferredRawSurvivors, decodeCount);
        }
        int[] outputPositions = lazyMask.selectedPositions();
        for (int index = 0; index < decodeCount; index++) {
            int outputPosition = weakConstraint || outputPositions == null ? index : outputPositions[index];
            deferredRawSurvivors[index] = deferredWindowSurvivors == null ? outputPosition : deferredWindowSurvivors[outputPosition];
        }

        ColumnReader reader = readers[column];
        boolean isNullable = nullable[column];
        BooleanVector nullVector = isNullable
                ? allocator.allocate(allocationContext, BooleanVector.class, batchPolicy.maxRows(), BooleanVector::new)
                : null;
        boolean[] nulls = nullVector == null ? null : nullVector.values();
        if (reader.kind() == ColumnReader.Kind.BINARY) {
            boolean[] rawNulls = isNullable ? ensureColumnNullScratch(column, deferredWindowRows) : null;
            Vector raw = reader.readSelectedBinary(
                    allocator,
                    allocationContext,
                    deferredRawSurvivors,
                    decodeCount,
                    deferredWindowRows,
                    rawNulls);
            Vector vector = null;
            try {
                for (int index = 0; index < decodeCount; index++) {
                    int outputPosition = weakConstraint || outputPositions == null ? index : outputPositions[index];
                    vector = raw.copySinglePositionInto(
                            allocator,
                            allocationContext,
                            vector,
                            deferredRawSurvivors[index],
                            outputPosition,
                            batchPolicy.maxRows());
                    if (nulls != null) {
                        nulls[outputPosition] = rawNulls[deferredRawSurvivors[index]];
                    }
                }
            }
            finally {
                allocator.release(allocationContext, raw);
            }
            currentValues[column] = vector == null
                    ? BinaryVector.allocate(allocator, allocationContext, batchPolicy.maxRows(), 0)
                    : vector;
            recordSelectedDecode(column, decodeCount);
            recordCopied(column, decodeCount);
        }
        else if (reader.kind() == ColumnReader.Kind.INT) {
            ensureLazyScratch(decodeCount, false);
            reader.readSelectedInts(deferredRawSurvivors, decodeCount, deferredWindowRows, lazyScratchInt, isNullable ? lazyScratchNull : null);
            recordSelectedDecode(column, decodeCount);
            Vector vector = allocateIntOutput(column, batchPolicy.maxRows());
            for (int index = 0; index < decodeCount; index++) {
                int outputPosition = weakConstraint || outputPositions == null ? index : outputPositions[index];
                if (nulls != null) {
                    nulls[outputPosition] = lazyScratchNull[index];
                }
            }
            scatterIntOutput(
                    vector,
                    weakConstraint || outputPositions == null ? identitySurvivors(decodeCount) : outputPositions,
                    lazyScratchInt,
                    decodeCount);
            currentValues[column] = vector;
            recordCopied(column, decodeCount);
        }
        else {
            ensureLazyScratch(decodeCount, true);
            reader.readSelectedLongs(deferredRawSurvivors, decodeCount, deferredWindowRows, lazyScratchLong, isNullable ? lazyScratchNull : null);
            recordSelectedDecode(column, decodeCount);
            if (reader.isDouble()) {
                org.weakref.nitro.data.F64Vector vector = org.weakref.nitro.data.F64Vector.allocate(allocator, allocationContext, batchPolicy.maxRows());
                for (int index = 0; index < decodeCount; index++) {
                    int outputPosition = weakConstraint || outputPositions == null ? index : outputPositions[index];
                    vector.values()[outputPosition] = Double.longBitsToDouble(lazyScratchLong[index]);
                    if (nulls != null) {
                        nulls[outputPosition] = lazyScratchNull[index];
                    }
                }
                currentValues[column] = vector;
                recordCopied(column, decodeCount);
            }
            else {
                I64Vector vector = I64Vector.allocate(allocator, allocationContext, batchPolicy.maxRows());
                for (int index = 0; index < decodeCount; index++) {
                    int outputPosition = weakConstraint || outputPositions == null ? index : outputPositions[index];
                    vector.values()[outputPosition] = lazyScratchLong[index];
                    if (nulls != null) {
                        nulls[outputPosition] = lazyScratchNull[index];
                    }
                }
                currentValues[column] = vector;
                recordCopied(column, decodeCount);
            }
        }
        currentNulls[column] = nullVector;
        recordPublished(column, decodeCount);
    }

    private void advanceUnresolvedFilteredPayload()
    {
        for (int column = 0; column < readers.length; column++) {
            if (!lazyResolved[column]) {
                lazyResolved[column] = true;
                if (diagnostics.sourceWork()) {
                    debugLazyOmitted[column] += lazyCount;
                }
                advanceColumn(column, deferredWindowRows);
            }
        }
        deferredFilteredPayload = false;
        deferredWindowSurvivors = null;
    }

    private void beginAdaptiveBatch(int rows)
    {
        adaptiveBatchRows = rows;
        adaptiveBatchSelectedRows = rows;
    }

    private void observeAdaptiveSelection(Mask mask)
    {
        adaptiveBatchSelectedRows = mask.count();
    }

    private void constrainLazyMaskAndObserveSelection(Mask mask)
    {
        constrainLazyMask(mask);
        observeAdaptiveSelection(mask);
    }

    private void finishLazyBatch()
    {
        try {
            advanceUnresolvedColumns();
        }
        finally {
            finishAdaptiveBatch();
        }
    }

    private void finishDeferredFilteredBatch()
    {
        try {
            advanceUnresolvedFilteredPayload();
        }
        finally {
            finishAdaptiveBatch();
        }
    }

    private void finishAdaptiveBatch()
    {
        if (adaptiveDecided || !batchPolicy.adaptive() || adaptiveBatchRows == 0) {
            return;
        }
        adaptiveObservedRows += adaptiveBatchRows;
        adaptiveSelectedRows += adaptiveBatchSelectedRows;
        adaptiveBatchRows = 0;
        if (adaptiveObservedRows < batchPolicy.adaptiveObservationRows()) {
            return;
        }
        adaptiveDecided = true;
        double selectedFraction = (double) adaptiveSelectedRows / adaptiveObservedRows;
        if (batchPolicy.adaptiveGrowth().admits(selectedFraction, batchPolicy.adaptiveSelectedFractionThreshold())) {
            int outputVectors = readers.length;
            for (boolean columnNullable : nullable) {
                if (columnNullable) {
                    outputVectors++;
                }
            }
            currentBatchRows = batchPolicy.adaptiveRows(outputVectors);
        }
        if (diagnostics.rowCounts()) {
            System.err.printf("[adaptive-batch] columns=%s observed=%s selected=%s fraction=%.4f rows=%s%n",
                    columnNames,
                    adaptiveObservedRows,
                    adaptiveSelectedRows,
                    selectedFraction,
                    currentBatchRows);
        }
    }

    private void constrainLazyMask(Mask mask)
    {
        lazyMask = mask;
        lazyConstrained = true;
    }

    /** Decode {@code column} into its scratch buffer: full when {@code survivors == null}, else at those positions. */
    private void readColumnInto(int column, int[] survivors, int rows, int batchRows)
    {
        if (readers[column].kind() == ColumnReader.Kind.BINARY) {
            if (rows != 0) {
                throw new IllegalArgumentException("BINARY filtered-window reads require the vector payload path");
            }
            readers[column].skipSelectedBinary(batchRows);
            recordSelectedDecode(column, 0);
            recordSkipped(column, batchRows);
            return;
        }
        ensureColumnScratch(column, rows);
        ColumnReader reader = readers[column];
        boolean[] nulls = nullable[column] ? colNull[column] : null;
        if (reader.kind() == ColumnReader.Kind.LONG) {
            if (survivors == null) {
                reader.readLongs(colLong[column], nulls, batchRows);
                recordFullDecode(column, batchRows);
            }
            else {
                reader.readSelectedLongs(survivors, rows, batchRows, colLong[column], nulls);
                recordSelectedDecode(column, rows);
                if (rows == 0) {
                    recordSkipped(column, batchRows);
                }
            }
        }
        else {
            if (survivors == null) {
                reader.readInts(colInt[column], nulls, batchRows);
                recordFullDecode(column, batchRows);
            }
            else {
                reader.readSelectedInts(survivors, rows, batchRows, colInt[column], nulls);
                recordSelectedDecode(column, rows);
                if (rows == 0) {
                    recordSkipped(column, batchRows);
                }
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
            for (LongDomain filter : filtersByColumn) {
                if (filter != null) {
                    n++;
                }
            }
            int[] columns = new int[n];
            double[] selectivity = new double[n];
            int index = 0;
            for (int c = 0; c < filtersByColumn.length; c++) {
                if (filtersByColumn[c] != null) {
                    columns[index] = c;
                    selectivity[index] = filterEvaluationPolicy.ordering().selectivity()
                            ? estimateSelectivity(c)
                            : filtersByColumn[c].size();
                    index++;
                }
            }
            // Insertion sort by estimated pass fraction ascending (most selective first). The lead column runs on the
            // fused, run-aware predicate-over-dictionary path; every later column is read at the lead's survivors, so a
            // poorly-pruning lead is very expensive. Distinct-build-value count alone is a bad proxy: a small-domain
            // column (e.g. a 10-value warehouse key whose filter admits all 10) has a tiny size yet prunes nothing.
            // Dividing the filter's value count by the column's dictionary cardinality recovers the true pass fraction.
            for (int i = 1; i < columns.length; i++) {
                int keyColumn = columns[i];
                double keySelectivity = selectivity[i];
                int j = i - 1;
                while (j >= 0 && selectivity[j] > keySelectivity) {
                    columns[j + 1] = columns[j];
                    selectivity[j + 1] = selectivity[j];
                    j--;
                }
                columns[j + 1] = keyColumn;
                selectivity[j + 1] = keySelectivity;
            }
            filterOrder = columns;
        }
        return filterOrder;
    }

    /**
     * Estimated fraction of rows a column's dynamic filter admits: {@code filterValues / columnCardinality}, using the
     * column's dictionary size as the cardinality. Falls back to a raw-size proxy when the column is not
     * dictionary-encoded (cardinality unknown). Smaller is more selective; the most selective column leads the scan.
     */
    private double estimateSelectivity(int column)
    {
        int exactDictionaryMaxEntries = filterEvaluationPolicy.ordering().exactDictionaryMaxEntries();
        double exactDictionaryFraction = readers[column].estimateDictionaryMatchFraction(
                filtersByColumn[column],
                exactDictionaryMaxEntries);
        if (!Double.isNaN(exactDictionaryFraction)) {
            return exactDictionaryFraction;
        }
        double filterValues = filtersByColumn[column].size();
        int cardinality = readers[column].peekDictionarySize();
        if (cardinality > 0) {
            // A Parquet dictionary is chunk-local while the pushed filter is query-global. Dividing a global value
            // count by one chunk's dictionary cardinality can badly overstate the pass fraction for a sparse key
            // spread across a broad domain. Use the tighter of that bound and the filter's exact range density.
            double dictionaryFraction = Math.min(1.0, filterValues / cardinality);
            return filterEvaluationPolicy.ordering().rangeDensity()
                    ? Math.min(dictionaryFraction, filtersByColumn[column].rangeDensity())
                    : dictionaryFraction;
        }
        // With no dictionary we lack a probe-domain cardinality. The build filter's range alone is not safe here:
        // probe values may be concentrated inside that range (q39), making a seemingly sparse filter weak. Keep
        // plain columns conservative and use range density only to correct chunk-local dictionary estimates above.
        return 1.0;
    }

    private long[] ensureLong(long[] array, int size)
    {
        return array == null || array.length < size ? replaceLongs(array, size) : array;
    }

    private int[] ensureInt(int[] array, int size)
    {
        return array == null || array.length < size ? replaceInts(array, size) : array;
    }

    private boolean[] ensureWindowNull(int column, int size)
    {
        if (windowNull[column] == null || windowNull[column].length < size) {
            windowNull[column] = replaceBooleans(windowNull[column], size);
        }
        return windowNull[column];
    }

    private void ensureScratch(int count)
    {
        if (nextSurvivors.length < count) {
            nextSurvivors = replaceInts(nextSurvivors, count);
        }
        if (filterWindowPolicy.eagerScratch()) {
            for (int c = 0; c < readers.length; c++) {
                ensureColumnScratch(c, count);
            }
        }
    }

    private int[] snapshotFilterSurvivors(int column, int[] source, int count)
    {
        int[] target = filterSurvivors[column];
        if (target == null || target.length < count) {
            target = replaceInts(target, count);
            filterSurvivors[column] = target;
        }
        System.arraycopy(source, 0, target, 0, count);
        return target;
    }

    private void ensureColumnScratch(int column, int rows)
    {
        if (readers[column].kind() == ColumnReader.Kind.LONG) {
            if (colLong[column] == null || colLong[column].length < rows) {
                // A final filter can publish its dense scratch directly as the window buffer. Once that window is
                // drained, growing the filter scratch ends the alias: detach it before replaceLongs releases the old
                // lease, otherwise releaseScanScratch can return the same array to the pool a second time.
                if (windowLong[column] == colLong[column]) {
                    windowLong[column] = null;
                }
                colLong[column] = replaceLongs(colLong[column], rows);
            }
        }
        else if (colInt[column] == null || colInt[column].length < rows) {
            if (windowInt[column] == colInt[column]) {
                windowInt[column] = null;
            }
            colInt[column] = replaceInts(colInt[column], rows);
        }
        if (nullable[column] && (colNull[column] == null || colNull[column].length < rows)) {
            if (windowNull[column] == colNull[column]) {
                windowNull[column] = null;
            }
            colNull[column] = replaceBooleans(colNull[column], rows);
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

    private boolean closed;

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (diagnostics.rowCounts() && hasFilters) {
            System.err.println("[rowcounts] " + columnNames + " raw=" + debugRawRows + " survivors=" + debugSurvivors);
            StringBuilder stages = new StringBuilder("[rowcounts] filter-stages");
            for (int column : filterOrder()) {
                stages.append(' ').append(columnNames.get(column)).append('=')
                        .append(debugFilterInputs[column]).append("->").append(debugFilterOutputs[column])
                        .append("[values=").append(filtersByColumn[column].size())
                        .append(",rangeDensity=").append(String.format(java.util.Locale.ROOT, "%.4f", filtersByColumn[column].rangeDensity()))
                        .append(",dict=").append(readers[column].peekDictionarySize()).append(']');
            }
            System.err.println(stages);
        }
        if (progressiveFilterCompactionPolicy.diagnostics() && debugProgressiveCompactionWindows > 0) {
            System.err.printf("[progressive-filter-compaction] columns=%s windows=%d rows=%d kept=%d%n",
                    columnNames,
                    debugProgressiveCompactionWindows,
                    debugProgressiveCompactionRows,
                    debugProgressiveCompactionKept);
        }
        String sourceIdentity = Integer.toHexString(System.identityHashCode(this));
        if (diagnostics.decompression()) {
            for (int column = 0; column < readers.length; column++) {
                System.err.println("[decompression] source=" + sourceIdentity + " column=" + columnNames.get(column) + " " + readers[column].decompressionSummary());
            }
        }
        if (diagnostics.sourceWork()) {
            System.err.println("[source-work-scan] source=" + sourceIdentity +
                    " totalRows=" + totalRows +
                    " admittedRows=" + (nextRow - prunedRows) +
                    " prunedRows=" + prunedRows +
                    " columns=" + readers.length);
            for (int column = 0; column < readers.length; column++) {
                System.err.println("[source-work] source=" + sourceIdentity +
                        " column=" + columnNames.get(column) +
                        " fullDecoded=" + debugFullDecoded[column] +
                        " selectedDecoded=" + debugSelectedDecoded[column] +
                        " dictionaryExamined=" + debugDictionaryExamined[column] +
                        " skipped=" + debugSkipped[column] +
                        " published=" + debugPublished[column] +
                        " copied=" + debugCopied[column] +
                        " lazyOmitted=" + debugLazyOmitted[column] +
                        " nullExamined=" + debugNullExamined[column]);
            }
        }
        closeCurrentBatch();
        releaseScanScratch();
        for (ColumnReader reader : readers) {
            reader.close();
        }
        for (ColumnReader reader : nullReaders) {
            if (reader != null) {
                reader.close();
            }
        }
        for (int file = 0; file < files.length; file++) {
            if (mappedFileLeases[file] == null) {
                files[file].close();
            }
            else {
                mappedFileLeases[file].close();
            }
        }
        if (arena != null) {
            arena.close();
        }
        if (decompressedPageCacheLease != null) {
            decompressedPageCacheLease.close();
        }
        directNumericBatchDecodeLease.close();
        batchBuffers.close();
    }

    private static final class DirectNumericBatchDecodeAdmission
            implements AutoCloseable
    {
        private final ParquetNumericDecodeAdmissionPolicy policy;
        private int scans;
        private int qualifyingScans;
        private boolean reported;

        private DirectNumericBatchDecodeAdmission(ParquetNumericDecodeAdmissionPolicy policy)
        {
            this.policy = requireNonNull(policy, "policy is null");
        }

        public void register(long rows, int intColumns)
        {
            scans++;
            if (rows >= policy.minRows() && intColumns >= policy.minIntColumns()) {
                qualifyingScans++;
            }
        }

        public boolean admitted()
        {
            boolean admitted = qualifyingScans > 0;
            if (policy.diagnostics() && !reported) {
                reported = true;
                System.err.printf(
                        "[direct-numeric-batch-admission] scans=%d qualifyingScans=%d admitted=%s%n",
                        scans,
                        qualifyingScans,
                        admitted);
            }
            return admitted;
        }

        @Override
        public void close() {}
    }

    private void releaseScanScratch()
    {
        arrayPool.release(nextSurvivors);
        arrayPool.release(filterOrder);
        arrayPool.release(doubleDecodeScratch);
        arrayPool.release(lazyScratchLong);
        arrayPool.release(lazyScratchInt);
        arrayPool.release(lazyScratchNull);
        arrayPool.release(lazyIdentity);
        arrayPool.release(deferredRawSurvivors);
        arrayPool.release(windowSlicePositions);
        arrayPool.release(lazyResolved);
        arrayPool.release(lazyPathDecided);
        arrayPool.release(lazySkipColumn);
        arrayPool.release(lazyPendingAdvance);
        for (boolean[] scratch : directNullScratch) {
            arrayPool.release(scratch);
        }
        for (int column = 0; column < readers.length; column++) {
            releaseWindowBinary(column);
            arrayPool.release(colLong[column]);
            arrayPool.release(colInt[column]);
            arrayPool.release(colNull[column]);
            arrayPool.release(filterSurvivors[column]);
            // A lead filter whose survivor set is already final exposes its dense col* scratch
            // directly as window* (see decodeFilterWindow). That is one owned buffer with two
            // semantic names, not two pool leases. Returning both aliases corrupts the pool by
            // allowing the same array to be borrowed concurrently by later scans.
            if (windowLong[column] != colLong[column]) {
                arrayPool.release(windowLong[column]);
            }
            if (windowInt[column] != colInt[column]) {
                arrayPool.release(windowInt[column]);
            }
            if (windowNull[column] != colNull[column]) {
                arrayPool.release(windowNull[column]);
            }
        }
    }

    private void closeCurrentBatch()
    {
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
            clearColumnVectors();
            for (ColumnReader reader : readers) {
                reader.finishBatch();
            }
        }
    }

    private int[] replaceInts(int[] previous, int size)
    {
        int[] replacement = arrayPool.borrowInts(size);
        arrayPool.release(previous);
        return replacement;
    }

    private long[] replaceLongs(long[] previous, int size)
    {
        long[] replacement = arrayPool.borrowLongs(size);
        arrayPool.release(previous);
        return replacement;
    }

    private boolean[] replaceBooleans(boolean[] previous, int size)
    {
        boolean[] replacement = arrayPool.borrowBooleans(size);
        arrayPool.release(previous);
        return replacement;
    }

    /** Drop stale aliases after the vector scope has returned the closed generation's buffers to its pools. */
    private void clearColumnVectors()
    {
        if (currentValues != null) {
            for (int c = 0; c < currentValues.length; c++) {
                currentValues[c] = null;
            }
        }
        if (currentNulls != null) {
            for (int c = 0; c < currentNulls.length; c++) {
                currentNulls[c] = null;
            }
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("source is closed");
        }
    }

    private int columnIndex(SourceColumnHandle handle)
    {
        for (int index = 0; index < sourceColumns.length; index++) {
            if (sourceColumns[index] == handle) {
                return index;
            }
        }
        return -1;
    }

    private static List<String> requireColumnNames(Schema schema)
    {
        requireNonNull(schema, "schema is null");
        return schema.fields().stream()
                .map(field -> field.name().orElseThrow(() -> new IllegalArgumentException("schema field name is required")))
                .toList();
    }
}
