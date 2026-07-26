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

import org.apache.parquet.format.RowGroup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.BatchBufferOwner;
import org.weakref.nitro.operator.BatchBufferScope;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.parquet.ColumnReader;
import org.weakref.nitro.parquet.DecompressedPageCache;
import org.weakref.nitro.parquet.DecompressedPageCachePolicy;
import org.weakref.nitro.parquet.ParquetFile;
import org.weakref.nitro.parquet.ParquetPageNavigationPolicy;
import org.weakref.nitro.parquet.RleReaderPolicy;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

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
    // Match TrinoParquetScanOperator's default so the per-batch operator overhead (Output objects, pooled
    // vector alloc/release) is amortized over the same number of batches in an apples-to-apples comparison.
    private static final int MAX_BATCH_ROWS = Integer.getInteger("nitro.parquet.scan.maxBatchRows", 10_000);
    private static final int DIRECT_NUMERIC_BATCH_DECODE_MIN_INT_COLUMNS =
            Integer.getInteger("nitro.parquet.directNumericBatchDecodeMinIntColumns", 3);
    private static final long DIRECT_NUMERIC_BATCH_DECODE_MIN_ROWS =
            Long.getLong("nitro.parquet.directNumericBatchDecodeMinRows", 1L << 24);
    // A wide, all-numeric scan with no downstream constraint is usually feeding a cardinality-preserving operator.
    // In that shape, removing the page-sized materialization buffer can increase TLB pressure in the consumer even
    // though it saves allocation in the decoder. Wait for runtime evidence that a downstream operator is narrowing
    // the stream before enabling direct decode. Mixed scans retain immediate admission: their binary readers already
    // preserve page materialization, so the numeric decoder is not removing the pipeline's last page-local buffer.
    private static final boolean DIRECT_NUMERIC_BATCH_DECODE_REQUIRE_CONSTRAINT_FOR_ALL_NUMERIC =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.directNumericBatchDecodeRequireConstraintForAllNumeric", "true"));
    // Repeated scans of the same large physical column have already paid for independent reader state and will
    // revisit the same page-sized materialization. Decode dictionary/plain data directly into each public batch:
    // this removes that intermediate flat page while preserving the receiver's ordinary flat-vector contract.
    // Admission is execution-local and keyed only by normalized file/physical-column identity.
    private static final boolean DIRECT_NUMERIC_REPEATED_SOURCE_DECODE =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.directNumericRepeatedSourceDecode", "true"));
    private static final int DIRECT_NUMERIC_REPEATED_SOURCE_ANCHOR_MIN_CONSUMERS =
            Integer.getInteger("nitro.parquet.directNumericRepeatedSourceAnchorMinConsumers", 3);
    private static final int DIRECT_NUMERIC_REPEATED_SOURCE_ANCHOR_MIN_COLUMNS =
            Integer.getInteger("nitro.parquet.directNumericRepeatedSourceAnchorMinColumns", 2);
    private static final long DIRECT_NUMERIC_REPEATED_SOURCE_MIN_ROWS =
            Long.getLong("nitro.parquet.directNumericRepeatedSourceMinRows", 1L << 25);
    private static final boolean DEBUG_DIRECT_NUMERIC_BATCH_DECODE =
            Boolean.getBoolean("nitro.debug.directNumericBatchDecode");
    // Late materialization (non-DF scans): defer per-column decode until the column is pulled, and once a filter
    // above the scan pushes a survivor mask via constrain(), decode the remaining columns only for survivor rows
    // (skip-decode + scatter to position) instead of every row. Mirrors TrinoParquetScanOperator's masked path.
    private static final boolean LATE_MATERIALIZATION = Boolean.parseBoolean(System.getProperty("nitro.parquet.lateMaterialization", "true"));
    // Skip-decode a constrained column only when at most this fraction of rows survive; above it the per-survivor-run
    // skip path (re-walking the RLE id stream) costs more than a single bulk decode, so full-decode instead.
    private static final int SKIP_DECODE_MAX_SURVIVOR_PERCENT = Integer.getInteger("nitro.parquet.skipMaxSurvivorPercent", 20);
    // A low survivor fraction can still be a bad skip-decode candidate when positions alternate densely. The
    // selected reader advances once per contiguous run; require enough survivors per run to amortize that state
    // transition. Clustered predicates retain page skipping, while fragmented masks fall back to one bulk decode.
    private static final int SKIP_DECODE_MIN_AVERAGE_RUN = Integer.getInteger("nitro.parquet.skipMinAverageRun", 4);
    private static final boolean LAZY_NUMERIC_SKIP_DECODE =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.lazyNumericSkipDecode", "true"));
    // A very sparse numeric mask can profit even when survivors are isolated: decoding the compact page IDs once
    // and gathering only live values avoids materializing/copying every wide value. Require multiple payloads to
    // amortize the alternate scan lifecycle, but cap their count because page-ID setup repeated over a very wide
    // projection retires more work than it saves. Keep the ordinary run-length proof for binary and denser numeric
    // masks. This admission depends only on physical density, encoding, and live scan width.
    private static final boolean LAZY_FRAGMENTED_NUMERIC_SKIP_DECODE =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.lazyFragmentedNumericSkipDecode", "true"));
    private static final int LAZY_FRAGMENTED_NUMERIC_SKIP_MAX_SURVIVOR_PERCENT =
            Integer.getInteger("nitro.parquet.lazyFragmentedNumericSkipMaxSurvivorPercent", 6);
    private static final int LAZY_FRAGMENTED_NUMERIC_SKIP_MAX_SCAN_COLUMNS =
            Integer.getInteger("nitro.parquet.lazyFragmentedNumericSkipMaxScanColumns", 6);
    private static final int LAZY_FRAGMENTED_NUMERIC_SKIP_MIN_PAYLOAD_COLUMNS =
            Integer.getInteger("nitro.parquet.lazyFragmentedNumericSkipMinPayloadColumns", 2);
    private static final int LAZY_FRAGMENTED_NUMERIC_SKIP_MAX_PAYLOAD_COLUMNS =
            Integer.getInteger("nitro.parquet.lazyFragmentedNumericSkipMaxPayloadColumns", 8);
    private static final int LAZY_NUMERIC_SKIP_MIN_DICTIONARY_SIZE =
            Integer.getInteger("nitro.parquet.lazyNumericSkipMinDictionarySize", 1);
    private static final boolean DEBUG_LAZY_NUMERIC_SKIP = Boolean.getBoolean("nitro.debug.lazyNumericSkip");
    private static final boolean DEFER_EMPTY_CONSTRAINED_DECODE =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.deferEmptyConstrainedDecode", "true"));
    // A later dynamic filter can sharply narrow values already decoded at the running survivor set. Preserve the
    // accepted dense ranks long enough to compact every aligned filter column in place, instead of discarding those
    // ranks and recovering them later with a branch-heavy two-pointer search over the wider survivor array. The
    // existing survivor scratch carries ranks temporarily, so this adds no buffer or steady-state allocation. Admit
    // only after a narrow scan has demonstrated a long, sparse reuse horizon: wider scans make the large aligned
    // column scratches themselves the locality cost, while short scans cannot amortize the extra rank pass.
    private static final boolean PROGRESSIVE_FILTER_COMPACTION =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.progressiveFilterCompaction", "true"));
    private static final int PROGRESSIVE_FILTER_COMPACTION_MAX_PERCENT =
            Integer.getInteger("nitro.parquet.progressiveFilterCompactionMaxPercent", 12);
    private static final int PROGRESSIVE_FILTER_COMPACTION_MIN_OBSERVED_ROWS =
            Integer.getInteger("nitro.parquet.progressiveFilterCompactionMinObservedRows", 1 << 20);
    // A long scan need not physically process the entire reuse horizon before it can prove that horizon. Once at
    // least one million raw positions have established the lead-filter density, project its intermediate survivors
    // across the reader's exact physical row count. Require half of the physical scan as well as one million raw
    // positions so clustered early pages cannot govern a long tail. A statically sparse next filter may then use rank
    // compaction when the projected input has at least a 256K-row amortization horizon and every later filter has both
    // full-domain metadata and an observed all-pass history; otherwise a later narrowing can make the eager copy
    // throwaway work. The current batch's exact kept fraction still decides whether compaction occurs, so a misleading
    // range estimate pays at most one rank pass and cannot select a different result. This is scan/representation
    // evidence, not a query policy.
    private static final boolean PROSPECTIVE_PROGRESSIVE_FILTER_COMPACTION =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.prospectiveProgressiveFilterCompaction", "true"));
    private static final int PROSPECTIVE_PROGRESSIVE_FILTER_COMPACTION_MIN_RAW_ROWS =
            Integer.getInteger("nitro.parquet.prospectiveProgressiveFilterCompactionMinRawRows", 1 << 20);
    private static final int PROSPECTIVE_PROGRESSIVE_FILTER_COMPACTION_MIN_OBSERVED_PERCENT =
            Integer.getInteger("nitro.parquet.prospectiveProgressiveFilterCompactionMinObservedPercent", 50);
    private static final int PROSPECTIVE_PROGRESSIVE_FILTER_COMPACTION_MIN_PROJECTED_ROWS =
            Integer.getInteger("nitro.parquet.prospectiveProgressiveFilterCompactionMinProjectedRows", 1 << 18);
    private static final int PROGRESSIVE_FILTER_COMPACTION_MAX_COLUMNS =
            Integer.getInteger("nitro.parquet.progressiveFilterCompactionMaxColumns", 4);
    private static final boolean FUSED_PROGRESSIVE_FILTER_COMPACTION =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.fusedProgressiveFilterCompaction", "true"));
    private static final int FUSED_PROGRESSIVE_FILTER_COMPACTION_MAX_PERCENT =
            Integer.getInteger("nitro.parquet.fusedProgressiveFilterCompactionMaxPercent", 60);
    private static final int FUSED_PROGRESSIVE_FILTER_COMPACTION_MIN_ALIGNED_COLUMNS =
            Integer.getInteger("nitro.parquet.fusedProgressiveFilterCompactionMinAlignedColumns", 2);
    private static final int FUSED_PROGRESSIVE_FILTER_COMPACTION_MIN_AVERAGE_RUN =
            Integer.getInteger("nitro.parquet.fusedProgressiveFilterCompactionMinAverageRun", SKIP_DECODE_MIN_AVERAGE_RUN);
    private static final boolean DEBUG_PROGRESSIVE_FILTER_COMPACTION =
            Boolean.getBoolean("nitro.debug.progressiveFilterCompaction");

    // The DF-window payload path freezes one page path (skip vs bulk) for the whole scan from the FIRST window's
    // survival rate, but that single 1M-row window is positionally biased: a filter whose survivors happen to cluster
    // early reads dense there yet sparse overall. Because mis-freezing to bulk when the scan is actually sparse costs
    // ~20x (a full window decode per survivor batch) while the reverse is bounded, only commit to bulk when the first
    // window is convincingly dense — a genuinely non-selective filter reads dense everywhere, so even a biased sample
    // clears this higher bar. A merely-front-loaded window (q39: 21.9% first vs 1.5% overall) stays on skip.
    private static final int DF_PAYLOAD_BULK_MIN_SURVIVOR_PERCENT = Integer.getInteger("nitro.parquet.dfPayloadBulkPercent", 50);
    private static final boolean DEFER_FILTERED_PAYLOAD =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.deferFilteredPayload", "true"));
    private static final int DEFERRED_PAYLOAD_MAX_SURVIVOR_PERCENT =
            Integer.getInteger("nitro.parquet.deferredPayloadMaxSurvivorPercent", 20);
    private static final int DEFERRED_PAYLOAD_MIN_COLUMNS =
            Integer.getInteger("nitro.parquet.deferredPayloadMinColumns", 4);
    private static final boolean BOUNDED_DEFERRED_PAYLOAD_WINDOW =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.boundedDeferredPayloadWindow", "false"));
    private static final int BOUNDED_DEFERRED_PAYLOAD_WINDOW_ROWS =
            Integer.getInteger("nitro.parquet.boundedDeferredPayloadWindowRows", 163_840);
    private static final int BOUNDED_DEFERRED_PAYLOAD_MAX_COLUMNS =
            Integer.getInteger("nitro.parquet.boundedDeferredPayloadMaxColumns", 8);
    private static final boolean DEBUG_BOUNDED_DEFERRED_PAYLOAD_WINDOW =
            Boolean.getBoolean("nitro.debug.boundedDeferredPayloadWindow");

    private final Allocator allocator;
    private final BatchBufferScope batchBuffers;
    private final Allocator.Context allocationContext;
    private final Allocator.SharedResource<DecompressedPageCache> decompressedPageCacheLease;
    private final DecompressedPageCache decompressedPages;
    private final Allocator.SharedResource<DirectNumericBatchDecodeAdmission> directNumericBatchDecodeLease;
    private final DirectNumericBatchDecodeAdmission directNumericBatchDecodeAdmission;
    private final PrimitiveArrayPool arrayPool;
    private final List<String> columnNames;
    private final ParquetFile[] files;
    private final ColumnReader[] readers;
    private final ColumnReader[] nullReaders;
    private final boolean[] nullable;
    private final long totalRows;

    private final boolean allNumeric;
    private int filterWindow;
    private boolean filterWindowBounded;
    private final boolean adaptiveNarrowFilterWindowCandidate;
    private boolean adaptiveNarrowFilterWindowDecided;
    private final DynamicFilter[] filtersByColumn;
    private final org.weakref.nitro.function.VersionedLongPredicate[] filterVersionsByColumn;
    private boolean hasFilters;
    private boolean filtersPruned;
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
    // output batch. The window's surviving rows are buffered densely and then sliced into MAX_BATCH_ROWS output
    // batches. The window must comfortably exceed a Parquet page (~100K+ rows) for page-skip to be effective.
    // Keep the window large enough to span Parquet pages for page-skip, but not so large that every dynamic-filter
    // scan walks multi-megabyte scratch arrays. 512K keeps q20's page-skip behavior while improving q45 locality.
    private static final int FILTER_WINDOW = Integer.getInteger("nitro.parquet.scan.filterWindow", 1 << 19);
    // Narrow numeric scans have a much smaller per-row scratch footprint and benefit from keeping a long selective
    // scan in one decoder window: this preserves RLE/page-reader state and avoids repeatedly compacting/gathering at
    // artificial window boundaries. Bound the policy by width and by the execution's total registered scan pressure
    // so the aggregate scratch retained by a many-branch operator graph remains predictable under the benchmark's
    // 12 GiB process cap. Wider scans and scan-heavy executions retain FILTER_WINDOW. This is a physical execution
    // policy; it does not add a query predicate or alter the operator tree.
    private static final boolean ADAPTIVE_NARROW_FILTER_WINDOW =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.scan.adaptiveNarrowFilterWindow", "true"));
    private static final int NARROW_FILTER_WINDOW_MAX_COLUMNS =
            Integer.getInteger("nitro.parquet.scan.narrowFilterWindowMaxColumns", 3);
    private static final int NARROW_FILTER_WINDOW =
            Integer.getInteger("nitro.parquet.scan.narrowFilterWindow", 1 << 24);
    private static final int NARROW_FILTER_WINDOW_MAX_EXECUTION_SCANS =
            Integer.getInteger("nitro.parquet.scan.narrowFilterWindowMaxExecutionScans", 38);
    private static final boolean DEBUG_NARROW_FILTER_WINDOW =
            Boolean.getBoolean("nitro.debug.narrowFilterWindow");
    private static final boolean EAGER_FILTER_WINDOW_SCRATCH = Boolean.parseBoolean(System.getProperty("nitro.parquet.scan.eagerFilterWindowScratch", "false"));
    // Order dynamic-filter columns by estimated pass fraction (filter values / column cardinality) rather than raw
    // filter value count, so the genuinely selective filter leads the scan on the fused run-aware path. Opt-out.
    private static final boolean SELECTIVITY_FILTER_ORDER = Boolean.parseBoolean(System.getProperty("nitro.parquet.selectivityFilterOrder", "true"));
    private static final boolean RANGE_DENSITY_FILTER_ORDER = Boolean.parseBoolean(System.getProperty("nitro.parquet.rangeDensityFilterOrder", "true"));
    // Drop a pushed dynamic filter whose build side admits every value in the probe column's dictionary: it prunes
    // nothing, so activating the eager filter-window path for it would gather every payload column at full row count
    // and defeat late materialization (a downstream operator's own predicate, e.g. an IS NULL, then drives the real
    // narrowing). The join still enforces the condition, so dropping it is always semantically safe. Opt-out.
    private static final boolean DROP_NON_SELECTIVE_FILTERS = Boolean.parseBoolean(System.getProperty("nitro.parquet.dropNonSelectiveFilters", "true"));
    private static final boolean EXACT_DYNAMIC_FILTER_DICTIONARY_COVERAGE =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.exactDynamicFilterDictionaryCoverage", "true"));
    private static final boolean DIRECT_NULL_MASK_READER =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.directNullMaskReader", "true"));
    private static final boolean DIRECT_NULL_MASK_COMPACTION =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.directNullMaskCompaction", "true"));
    // Densely-packed surviving values for the current window, per column (grown to high-water mark); sliced out.
    private final long[][] windowLong;
    private final int[][] windowInt;
    private final boolean[][] windowNull;
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
    private Batch currentBatch;
    // Reused high-water scratch for decoding double columns (raw long bits -> reinterpreted into the value array),
    // so the eager full-batch path doesn't allocate a fresh long[] per batch per double column.
    private long[] doubleDecodeScratch;
    private final Vector[] currentValues;
    private final Vector[] currentNulls;
    private final ScanOutputResolver[] outputResolvers;
    private boolean lazyOutputResolution;
    private boolean deferredFilteredPayload;
    private int deferredWindowRows;
    private int[] deferredWindowSurvivors;
    private int[] deferredRawSurvivors = new int[0];
    private final java.util.function.Consumer<Mask> noOpConstrainer = _ -> {};
    private final java.util.function.Consumer<Mask> lazyMaskConstrainer = this::constrainLazyMask;
    private final Runnable noOpClose = () -> {};
    private final Runnable lazyClose = this::advanceUnresolvedColumns;
    private final Runnable deferredFilteredClose = this::advanceUnresolvedFilteredPayload;

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

    public NitroParquetScanOperator(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<Path> paths,
            List<String> columns)
    {
        this(
                allocator,
                paths,
                columns,
                requireNonNull(resources, "resources is null").batchBufferPool(),
                resources.decompressedPageCache(),
                resources.directNumericBatchDecodeAdmission(),
                resources.decompressedPageCachePolicy(),
                resources.rleReaderPolicy(),
                resources.pageNavigationPolicy());
    }

    private NitroParquetScanOperator(
            Allocator allocator,
            List<Path> paths,
            List<String> columns,
            Object batchBufferPoolKey,
            Object decompressedPageCacheKey,
            Object directNumericBatchDecodeAdmissionKey,
            DecompressedPageCachePolicy decompressedPageCachePolicy,
            RleReaderPolicy rleReaderPolicy,
            ParquetPageNavigationPolicy pageNavigationPolicy)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.arrayPool = allocator.primitiveArrays();
        this.batchBuffers = new BatchBufferScope(allocator, "NitroParquetScanOperator", batchBufferPoolKey);
        this.allocationContext = batchBuffers.context();
        this.decompressedPageCacheLease = decompressedPageCachePolicy.enabled()
                ? allocator.acquireSharedResource(
                        decompressedPageCacheKey,
                        () -> new DecompressedPageCache(
                                allocator.nativeBuffers(),
                                decompressedPageCachePolicy,
                                allocator.nativeBufferAdvice()))
                : null;
        this.decompressedPages = decompressedPageCacheLease == null ? null : decompressedPageCacheLease.value();
        this.directNumericBatchDecodeLease = allocator.acquireSharedResource(
                directNumericBatchDecodeAdmissionKey, DirectNumericBatchDecodeAdmission::new);
        this.directNumericBatchDecodeAdmission = directNumericBatchDecodeLease.value();
        this.columnNames = List.copyOf(columns);
        checkArgument(!paths.isEmpty(), "paths is empty");

        this.files = paths.stream().map(ParquetFile::open).toArray(ParquetFile[]::new);
        int columnCount = columns.size();
        this.readers = new ColumnReader[columnCount];
        this.nullReaders = new ColumnReader[columnCount];
        this.nullable = new boolean[columnCount];
        this.currentValues = new Vector[columnCount];
        this.currentNulls = new Vector[columnCount];
        this.outputResolvers = new ScanOutputResolver[columnCount];
        this.directNullScratch = new boolean[columnCount][];
        this.directNullResolved = new boolean[columnCount];
        this.directNullPendingAdvance = new long[columnCount];

        for (int c = 0; c < columnCount; c++) {
            ParquetFile.Column first = files[0].column(columns.get(c));
            readers[c] = new ColumnReader(
                    first.type(),
                    first.optional(),
                    first.typeLength(),
                    first.decimal(),
                    decompressedPages,
                    arrayPool,
                    rleReaderPolicy,
                    pageNavigationPolicy);
            nullable[c] = first.optional();
            if (DIRECT_NULL_MASK_READER && first.optional()) {
                directNullScratch[c] = new boolean[0];
            }
            outputResolvers[c] = new ScanOutputResolver(c);
        }
        long rows = 0;
        for (int fileIndex = 0; fileIndex < files.length; fileIndex++) {
            ParquetFile file = files[fileIndex];
            for (int c = 0; c < columnCount; c++) {
                ParquetFile.Column column = file.column(columns.get(c));
                DecompressedPageCache.Source source = decompressedPages == null
                        ? null
                        : new DecompressedPageCache.Source(paths.get(fileIndex), columns.get(c));
                if (source != null) {
                    decompressedPages.register(source);
                }
                for (RowGroup rowGroup : file.rowGroups()) {
                    readers[c].addChunk(file.data(), file.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows, source);
                }
            }
            rows += file.numRows();
        }
        this.totalRows = rows;

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
        this.filterWindow = FILTER_WINDOW;
        this.adaptiveNarrowFilterWindowCandidate =
                ADAPTIVE_NARROW_FILTER_WINDOW && numeric && columnCount <= NARROW_FILTER_WINDOW_MAX_COLUMNS;
        this.filtersByColumn = new DynamicFilter[columnCount];
        this.filterVersionsByColumn = new org.weakref.nitro.function.VersionedLongPredicate[columnCount];
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
        this.debugFilterInputs = new long[columnCount];
        this.debugFilterOutputs = new long[columnCount];
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
        // Several joins can push a filter on the same probe column (e.g. this scan's own dimension join and a
        // downstream join whose key survives through an aggregation). Each is an independent necessary condition, so
        // keeping the more selective one (fewer distinct values) is correct and prunes hardest; a blind overwrite
        // could otherwise replace a tight filter with an all-values one.
        DynamicFilter existing = filtersByColumn[column];
        if (existing != null && existing.size() <= filter.size()) {
            return;
        }
        filtersByColumn[column] = filter;
        int dictionaryEntries = readers[column].peekDictionarySize();
        boolean warmBranchyTable = (long) filter.size() * VERSIONED_PREDICATE_WARM_BRANCHY_DENOMINATOR >= dictionaryEntries &&
                (long) filter.size() * DICTIONARY_BRANCHLESS_COMPACTION_DENOMINATOR < dictionaryEntries;
        filterVersionsByColumn[column] = REUSE_VERSIONED_DICTIONARY_PREDICATES &&
                dictionaryEntries >= MIN_VERSIONED_DICTIONARY_PREDICATE_ENTRIES &&
                !warmBranchyTable
                ? filter
                : null;
        hasFilters = true;
    }

    @Override
    public boolean supportsDynamicFilterPushdown(int column)
    {
        return allNumeric && column >= 0 && column < readers.length;
    }

    @Override
    public int outputCount()
    {
        return columnNames.size();
    }

    @Override
    public boolean hasNext()
    {
        return filtersActive() ? ensureWindow() : nextRow < totalRows;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more Parquet rows");
        }
        closeCurrentBatch();
        enableDirectNumericBatchDecodeIfAdmitted();

        if (filtersActive()) {
            return emitSlice();
        }
        int count = toIntExact(Math.min(MAX_BATCH_ROWS, totalRows - nextRow));
        nextRow += count;
        return LATE_MATERIALIZATION ? lazyBatch(count) : fullBatch(count);
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
                (!DIRECT_NUMERIC_BATCH_DECODE_REQUIRE_CONSTRAINT_FOR_ALL_NUMERIC || !allNumeric || lazyConstrained);
        boolean repeatedSourceCandidate = DIRECT_NUMERIC_REPEATED_SOURCE_DECODE &&
                totalRows >= DIRECT_NUMERIC_REPEATED_SOURCE_MIN_ROWS;
        int repeatedAnchorColumns = 0;
        if (repeatedSourceCandidate) {
            for (ColumnReader reader : readers) {
                if (reader.kind() != ColumnReader.Kind.BINARY &&
                        reader.hasRepeatedSource(DIRECT_NUMERIC_REPEATED_SOURCE_ANCHOR_MIN_CONSUMERS)) {
                    repeatedAnchorColumns++;
                }
            }
        }
        boolean repeatedSourceAdmission =
                repeatedAnchorColumns >= DIRECT_NUMERIC_REPEATED_SOURCE_ANCHOR_MIN_COLUMNS;
        if (DEBUG_DIRECT_NUMERIC_BATCH_DECODE && repeatedAnchorColumns > 0 && !repeatedSourceAdmissionReported) {
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
            if (DROP_NON_SELECTIVE_FILTERS && allFiltersNonSelective()) {
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
            DynamicFilter filter = filtersByColumn[c];
            if (filter == null) {
                continue;
            }
            if (EXACT_DYNAMIC_FILTER_DICTIONARY_COVERAGE) {
                if (!readers[c].dictionaryValuesCovered(filter::accepts)) {
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

    private static final boolean DEBUG_ROW_COUNTS = Boolean.getBoolean("nitro.debug.rowcounts");
    private static final boolean REUSE_VERSIONED_DICTIONARY_PREDICATES =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.reuseVersionedDictionaryPredicates", "true"));
    // A complete acceptance pass needs enough later row-id probes to amortize its table and code-shape cost. The
    // only 45.5K-entry activation shapes lost their three-fork counter controls; every retained activation is >=100K.
    private static final int MIN_VERSIONED_DICTIONARY_PREDICATE_ENTRIES =
            Integer.getInteger("nitro.parquet.minVersionedDictionaryPredicateEntries", 64 * 1024);
    private static final int VERSIONED_PREDICATE_WARM_BRANCHY_DENOMINATOR = 12;
    private static final int DICTIONARY_BRANCHLESS_COMPACTION_DENOMINATOR = 9;
    private static final boolean DEBUG_DECOMPRESSION = Boolean.getBoolean("nitro.debug.decompression");
    private long debugRawRows;
    private long debugSurvivors;
    private final long[] debugFilterInputs;
    private final long[] debugFilterOutputs;
    private long debugProgressiveCompactionRows;
    private long debugProgressiveCompactionKept;
    private long debugProgressiveCompactionWindows;
    private boolean debugFusedCompactionCandidatePrinted;
    private boolean debugProspectiveCompactionCandidatePrinted;

    /** Decode windows until one yields surviving rows (or input is exhausted). Returns whether rows are available. */
    private boolean ensureWindow()
    {
        if (!adaptiveNarrowFilterWindowDecided) {
            adaptiveNarrowFilterWindowDecided = true;
            if (adaptiveNarrowFilterWindowCandidate &&
                    directNumericBatchDecodeAdmission.scanCount() <= NARROW_FILTER_WINDOW_MAX_EXECUTION_SCANS) {
                filterWindow = Math.max(filterWindow, NARROW_FILTER_WINDOW);
            }
            if (DEBUG_NARROW_FILTER_WINDOW && adaptiveNarrowFilterWindowCandidate) {
                System.err.printf(
                        "[narrow-filter-window] scans=%d columns=%d rows=%d window=%d%n",
                        directNumericBatchDecodeAdmission.scanCount(),
                        readers.length,
                        totalRows,
                        filterWindow);
            }
        }
        if (windowSurvivorCursor < windowSurvivorCount) {
            return true;
        }
        if (BOUNDED_DEFERRED_PAYLOAD_WINDOW && !filterWindowBounded && readers.length <= BOUNDED_DEFERRED_PAYLOAD_MAX_COLUMNS && payloadColumnCount() >= DEFERRED_PAYLOAD_MIN_COLUMNS) {
            filterWindow = Math.min(filterWindow, BOUNDED_DEFERRED_PAYLOAD_WINDOW_ROWS);
            filterWindowBounded = true;
            if (DEBUG_BOUNDED_DEFERRED_PAYLOAD_WINDOW) {
                System.err.printf("[bounded-filter-window] columns=%s payload=%s rows=%s%n", columnNames, payloadColumnCount(), filterWindow);
            }
        }
        while (nextRow < totalRows) {
            int windowCount = toIntExact(Math.min(filterWindow, totalRows - nextRow));
            nextRow += windowCount;
            decodeFilterWindow(windowCount);
            windowSurvivorCursor = 0;
            if (DEBUG_ROW_COUNTS) {
                debugRawRows += windowCount;
                debugSurvivors += windowSurvivorCount;
            }
            if (windowSurvivorCount > 0) {
                return true;
            }
        }
        return false;
    }

    private Batch fullBatch(int count)
    {
        int columnCount = readers.length;
        lazyOutputResolution = false;
        Output[] outputs = new Output[columnCount];

        for (int c = 0; c < columnCount; c++) {
            ColumnReader reader = readers[c];
            BooleanVector nullVector = nullable[c]
                    ? allocator.allocate(allocationContext, BooleanVector.class, count, BooleanVector::new)
                    : null;
            boolean[] nulls = nullVector == null ? null : nullVector.values();
            Vector valueVector = switch (reader.kind()) {
                case INT -> {
                    I32Vector vector = allocator.allocate(allocationContext, I32Vector.class, count, I32Vector::new);
                    reader.readInts(vector.values(), nulls, count);
                    yield vector;
                }
                case LONG -> {
                    if (reader.isDouble()) {
                        org.weakref.nitro.data.F64Vector vector = allocator.allocate(allocationContext, org.weakref.nitro.data.F64Vector.class, count, org.weakref.nitro.data.F64Vector::new);
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
                        yield vector;
                    }
                    I64Vector vector = allocator.allocate(allocationContext, I64Vector.class, count, I64Vector::new);
                    reader.readLongs(vector.values(), nulls, count);
                    yield vector;
                }
                case BINARY -> {
                    yield reader.readBinary(allocator, allocationContext, nulls, count);
                }
            };
            currentValues[c] = valueVector;
            currentNulls[c] = nullVector;

            ScanOutputResolver outputResolver = outputResolvers[c];
            outputs[c] = new Output(
                    nullVector == null ? Set.of(Stream.VALUES) : Set.of(Stream.VALUES, Stream.NULLS),
                    outputResolver.resolver(),
                    outputResolver);
        }

        Mask mask = allocator.allocateAllMask(allocationContext, count);
        Batch batch = batchBuffers.batch(mask, noOpConstrainer, noOpClose, outputs);
        currentBatch = batch;
        return batch;
    }

    /**
     * Late-materialization batch: defer per-column decode. A column pulled while the mask is still {@code all()}
     * decodes every row; once a filter above the scan narrows the mask via {@link #constrain}, columns pulled
     * afterwards skip-decode only the survivor rows and scatter them back to position. Any column never pulled is
     * advanced past the batch when it closes so every reader stays aligned to the batch boundary.
     */
    private Batch lazyBatch(int count)
    {
        int columnCount = readers.length;
        lazyOutputResolution = true;
        lazyCount = count;
        lazyConstrained = false;
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
        Output[] outputs = new Output[columnCount];
        for (int c = 0; c < columnCount; c++) {
            ScanOutputResolver outputResolver = outputResolvers[c];
            outputs[c] = new Output(
                    nullable[c] ? Set.of(Stream.VALUES, Stream.NULLS) : Set.of(Stream.VALUES),
                    outputResolver.resolver(),
                    null,
                    outputResolver.maskResolver(),
                    null,
                    null,
                    outputResolver);
        }
        Batch batch = batchBuffers.batch(lazyMask, lazyMaskConstrainer, lazyClose, outputs);
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
                    (long) lazyMask.selectedCount() * 100 <= (long) lazyCount * SKIP_DECODE_MAX_SURVIVOR_PERCENT;
            boolean amortizedRuns = sparse && hasAmortizedSurvivorRuns(lazyMask);
            boolean fragmentedShape = sparse && LAZY_FRAGMENTED_NUMERIC_SKIP_DECODE &&
                    (long) lazyMask.selectedCount() * 100 <=
                            (long) lazyCount * LAZY_FRAGMENTED_NUMERIC_SKIP_MAX_SURVIVOR_PERCENT &&
                    readers.length <= LAZY_FRAGMENTED_NUMERIC_SKIP_MAX_SCAN_COLUMNS;
            // Do not inspect page dictionaries merely to reject a candidate. Reader metadata inspection can commit
            // page state, so the ordinary bulk path must remain bit-for-bit untouched when density fails admission.
            int fragmentedPayloadColumns = fragmentedShape
                    ? fragmentedNumericPayloadColumns(column)
                    : 0;
            boolean fragmentedNumeric = fragmentedShape &&
                    admitsFragmentedNumericSkip(
                            lazyMask.selectedCount(),
                            lazyCount,
                            readers.length,
                            fragmentedPayloadColumns,
                            LAZY_FRAGMENTED_NUMERIC_SKIP_MAX_SURVIVOR_PERCENT,
                            LAZY_FRAGMENTED_NUMERIC_SKIP_MAX_SCAN_COLUMNS,
                            LAZY_FRAGMENTED_NUMERIC_SKIP_MIN_PAYLOAD_COLUMNS,
                            LAZY_FRAGMENTED_NUMERIC_SKIP_MAX_PAYLOAD_COLUMNS);
            // Wide BINARY columns always participate. Numeric columns use the same physical selectivity and
            // survivor-run proof: clustered sparse masks can then avoid decoding almost an entire payload column,
            // while fragmented masks retain the bulk decoder. The switch is an experimental reverse control and is
            // intentionally independent of SQL type, column identity, or query shape.
            int dictionarySize = (amortizedRuns || fragmentedNumeric) &&
                    LAZY_NUMERIC_SKIP_DECODE &&
                    readers[column].kind() != ColumnReader.Kind.BINARY
                    ? readers[column].peekDictionarySize()
                    : -1;
            boolean wideNumericDictionary = dictionarySize >= LAZY_NUMERIC_SKIP_MIN_DICTIONARY_SIZE;
            lazySkipColumn[column] = readers[column].kind() == ColumnReader.Kind.BINARY
                    ? amortizedRuns
                    : wideNumericDictionary && (amortizedRuns || fragmentedNumeric);
            if (DEBUG_LAZY_NUMERIC_SKIP && wideNumericDictionary) {
                System.err.printf("[lazy-numeric-skip] column=%s dictionary=%s survivors=%s/%s fragmented=%s%n",
                        columnNames.get(column), dictionarySize, lazyMask.selectedCount(), lazyCount,
                        fragmentedNumeric && !amortizedRuns);
            }
        }
        return lazySkipColumn[column];
    }

    private static boolean hasAmortizedSurvivorRuns(Mask mask)
    {
        int selected = mask.selectedCount();
        if (selected == 0 || SKIP_DECODE_MIN_AVERAGE_RUN <= 1) {
            return true;
        }
        int[] positions = mask.selectedPositions();
        int runs = 1;
        for (int index = 1; index < selected; index++) {
            if (positions[index] != positions[index - 1] + 1) {
                runs++;
            }
        }
        return selected >= (long) runs * SKIP_DECODE_MIN_AVERAGE_RUN;
    }

    static boolean admitsFragmentedNumericSkip(
            int selected,
            int total,
            int scanColumns,
            int payloadColumns,
            int maxSurvivorPercent,
            int maxScanColumns,
            int minPayloadColumns,
            int maxPayloadColumns)
    {
        return total > 0 &&
                (long) selected * 100 <= (long) total * maxSurvivorPercent &&
                scanColumns <= maxScanColumns &&
                payloadColumns >= minPayloadColumns &&
                payloadColumns <= maxPayloadColumns;
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
                if (columns > LAZY_FRAGMENTED_NUMERIC_SKIP_MAX_PAYLOAD_COLUMNS) {
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
        if (DEFER_EMPTY_CONSTRAINED_DECODE && lazyConstrained && lazyMask.none()) {
            lazyPendingAdvance[column] += count;
            currentValues[column] = allocateMaskedEmptyColumn(reader, count);
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
            drainPendingAdvance(reader, skip, pending);
            lazyPendingAdvance[column] = 0;
        }

        // Path stability: a column decodes full only when this batch was never constrained (filter columns, read
        // before constrain; and every column of an unfiltered scan). Once constrain has fired, remaining columns
        // skip-decode — even if all rows happened to survive — so a reader never mixes the full and skip page paths
        // across batches (which would corrupt its cursor state). Binary has no skip path here, so it stays full;
        // a binary column is therefore always read before constrain or never (filter columns are numeric).
        if (!skip) {
            currentValues[column] = decodeFullColumn(reader, nulls, count);
            currentNulls[column] = nullVector;
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
            currentValues[column] = vector;
            currentNulls[column] = nullVector;
            return;
        }
        if (reader.kind() == ColumnReader.Kind.INT) {
            ensureLazyScratch(survivorCount, false);
            reader.readSelectedInts(survivors, survivorCount, count, lazyScratchInt, isNullable ? lazyScratchNull : null);
            I32Vector vector = allocator.allocate(allocationContext, I32Vector.class, count, I32Vector::new);
            int[] out = vector.values();
            for (int j = 0; j < survivorCount; j++) {
                out[survivors[j]] = lazyScratchInt[j];
            }
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
            if (reader.isDouble()) {
                org.weakref.nitro.data.F64Vector vector = allocator.allocate(allocationContext, org.weakref.nitro.data.F64Vector.class, count, org.weakref.nitro.data.F64Vector::new);
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
                return;
            }
            I64Vector vector = allocator.allocate(allocationContext, I64Vector.class, count, I64Vector::new);
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
        }
        currentNulls[column] = nullVector;
    }

    /** Allocate a correctly typed vector for a batch whose active mask is empty; no value is semantically live. */
    private Vector allocateMaskedEmptyColumn(ColumnReader reader, int count)
    {
        return switch (reader.kind()) {
            case INT -> allocator.allocate(allocationContext, I32Vector.class, count, I32Vector::new);
            case LONG -> reader.isDouble()
                    ? allocator.allocate(allocationContext, org.weakref.nitro.data.F64Vector.class, count, org.weakref.nitro.data.F64Vector::new)
                    : allocator.allocate(allocationContext, I64Vector.class, count, I64Vector::new);
            case BINARY -> BinaryVector.allocate(allocator, allocationContext, count, 0);
        };
    }

    /** Reinterpret {@code count} raw long bits (read through the long path for a DOUBLE column) into a double vector. */
    private org.weakref.nitro.data.F64Vector longBitsToDoubles(long[] bits, int offset, int count)
    {
        org.weakref.nitro.data.F64Vector vector = allocator.allocate(allocationContext, org.weakref.nitro.data.F64Vector.class, count, org.weakref.nitro.data.F64Vector::new);
        double[] values = vector.values();
        for (int i = 0; i < count; i++) {
            values[i] = Double.longBitsToDouble(bits[offset + i]);
        }
        return vector;
    }

    private Vector decodeFullColumn(ColumnReader reader, boolean[] nulls, int count)
    {
        return switch (reader.kind()) {
            case INT -> {
                I32Vector vector = allocator.allocate(allocationContext, I32Vector.class, count, I32Vector::new);
                reader.readInts(vector.values(), nulls, count);
                yield vector;
            }
            case LONG -> {
                if (reader.isDouble()) {
                    if (doubleDecodeScratch == null || doubleDecodeScratch.length < count) {
                        doubleDecodeScratch = replaceLongs(doubleDecodeScratch, count);
                    }
                    long[] bits = doubleDecodeScratch;
                    reader.readLongs(bits, nulls, count);
                    yield longBitsToDoubles(bits, 0, count);
                }
                I64Vector vector = allocator.allocate(allocationContext, I64Vector.class, count, I64Vector::new);
                reader.readLongs(vector.values(), nulls, count);
                yield vector;
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
            if (DIRECT_NULL_MASK_READER && nullable[c] && !directNullResolved[c]) {
                directNullPendingAdvance[c] += lazyCount;
            }
            if (lazyResolved[c]) {
                continue;
            }
            lazyResolved[c] = true;
            lazyPendingAdvance[c] += lazyCount;
        }
    }

    /**
     * Fast-forward {@code reader} past {@code pending} deferred rows to the current batch start, staying on the
     * column's fixed page path: a full-decode column via {@link ColumnReader#skip} (byte-skips whole pages), a
     * skip-decode column via the readSelected page-skip with an empty survivor set. Both drop whole data pages that
     * fall entirely within the skip without decoding them.
     */
    private void drainPendingAdvance(ColumnReader reader, boolean skip, long pending)
    {
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
            DynamicFilter filter = filtersByColumn[column];
            int kept;
            if (survivors == null) {
                ensureColumnScratch(column, count);
                // Lead filter: predicate-over-dictionary. Read ids + test a per-chunk acceptById[] WITHOUT
                // materializing the column; only survivors get a value. Output is dense, aligned to the survivors
                // (readPositions records that), so the later gather two-pointers it to the final survivor set.
                boolean[] cn = nullable[column] ? colNull[column] : null;
                java.util.function.LongPredicate predicate = filter::accepts;
                org.weakref.nitro.function.VersionedLongPredicate predicateVersion = filterVersionsByColumn[column];
                if (readers[column].kind() == ColumnReader.Kind.LONG) {
                    kept = readers[column].filterDictLongs(predicate, predicateVersion, count, nextSurvivors, colLong[column], cn);
                }
                else {
                    kept = readers[column].filterDictInts(predicate, predicateVersion, count, nextSurvivors, colInt[column], cn);
                }
                survivors = applied + 1 < order.length
                        ? snapshotFilterSurvivors(column, nextSurvivors, kept)
                        : nextSurvivors;
                readPositions[column] = survivors;
                if (DEBUG_ROW_COUNTS) {
                    debugFilterInputs[column] += count;
                    debugFilterOutputs[column] += kept;
                }
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
            long observedRows = progressiveFilterObservedRows[column];
            long observedKept = progressiveFilterObservedKept[column];
            int alignedFilterColumns = alignedFilterColumnCount(order, applied, survivors);
            boolean fusedCompaction = FUSED_PROGRESSIVE_FILTER_COMPACTION &&
                    readers.length <= PROGRESSIVE_FILTER_COMPACTION_MAX_COLUMNS &&
                    alignedFilterColumns >= FUSED_PROGRESSIVE_FILTER_COMPACTION_MIN_ALIGNED_COLUMNS &&
                    observedRows >= PROGRESSIVE_FILTER_COMPACTION_MIN_OBSERVED_ROWS &&
                    observedKept * 100 > observedRows * PROGRESSIVE_FILTER_COMPACTION_MAX_PERCENT &&
                    observedKept * 100 <= observedRows * FUSED_PROGRESSIVE_FILTER_COMPACTION_MAX_PERCENT &&
                    sampledAverageRunAtLeast(survivors, rows, FUSED_PROGRESSIVE_FILTER_COMPACTION_MIN_AVERAGE_RUN);
            if (DEBUG_PROGRESSIVE_FILTER_COMPACTION && fusedCompaction && !debugFusedCompactionCandidatePrinted) {
                debugFusedCompactionCandidatePrinted = true;
                System.err.printf("[fused-progressive-filter-candidate] columns=%s aligned=%d observed=%d/%d sampledAverageRun=%.3f%n",
                        columnNames,
                        alignedFilterColumns,
                        observedKept,
                        observedRows,
                        sampledAverageRun(survivors, rows));
            }
            boolean rankForCompaction = PROGRESSIVE_FILTER_COMPACTION &&
                    !fusedCompaction &&
                    readers.length <= PROGRESSIVE_FILTER_COMPACTION_MAX_COLUMNS &&
                    ((observedRows == 0
                            ? PROGRESSIVE_FILTER_COMPACTION_MIN_OBSERVED_ROWS == 0
                            : observedKept * 100 <= observedRows * PROGRESSIVE_FILTER_COMPACTION_MAX_PERCENT) &&
                            observedRows + rows >= PROGRESSIVE_FILTER_COMPACTION_MIN_OBSERVED_ROWS ||
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
                    if (filter.accepts(isLong ? longValues[i] : intValues[i])) {
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
                    if (filter.accepts(isLong ? longValues[i] : intValues[i])) {
                        next[kept++] = i;
                    }
                }
            }
            else {
                for (int i = 0; i < rows; i++) {
                    if (nulls != null && nulls[i]) {
                        continue;
                    }
                    if (filter.accepts(isLong ? longValues[i] : intValues[i])) {
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
                        (long) kept * 100 <= (long) rows * PROGRESSIVE_FILTER_COMPACTION_MAX_PERCENT);
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
                    if (DEBUG_PROGRESSIVE_FILTER_COMPACTION) {
                        debugProgressiveCompactionRows += rows;
                        debugProgressiveCompactionKept += kept;
                        debugProgressiveCompactionWindows++;
                    }
                }
            }
            if (DEBUG_ROW_COUNTS) {
                debugFilterInputs[column] += rows;
                debugFilterOutputs[column] += kept;
            }
            survivorCount = kept;
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
            dfPayloadBulk = survivorCount > (int) ((long) count * DF_PAYLOAD_BULK_MIN_SURVIVOR_PERCENT / 100);
            dfPayloadDecided = true;
        }
        // A whole filtered window that fits in one public batch can remain open across the first downstream
        // boundary. Filter/key columns are already materialized; payload readers stay at the window start until a
        // consumer borrows them after optionally constraining the batch. Larger windows retain eager slicing because
        // several output batches would otherwise share one irreversible reader position.
        deferredFilteredPayload = DEFER_FILTERED_PAYLOAD && !dfPayloadBulk && survivorCount > 0 && survivorCount <= MAX_BATCH_ROWS && payloadColumnCount() >= DEFERRED_PAYLOAD_MIN_COLUMNS;
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
            boolean[] nulls = nullable[c] ? ensureWindowNull(c, survivorCount) : null;
            if (dfPayloadBulk) {
                boolean[] columnNulls = nullable[c] ? colNull[c] : null;
                readColumnInto(c, null, count, count);
                if (readers[c].kind() == ColumnReader.Kind.INT) {
                    windowInt[c] = ensureInt(windowInt[c], survivorCount);
                    gatherInt(colInt[c], columnNulls, null, survivors, survivorCount, windowInt[c], nulls);
                }
                else {
                    windowLong[c] = ensureLong(windowLong[c], survivorCount);
                    gatherLong(colLong[c], columnNulls, null, survivors, survivorCount, windowLong[c], nulls);
                }
            }
            else if (readers[c].kind() == ColumnReader.Kind.INT) {
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
            if (!isFilterColumn(c)) {
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
        }
        windowSurvivorCount = survivorCount;
    }

    /** Compact every previously decoded filter column whose dense values align with {@code inputSurvivors}. */
    private void compactAlignedFilterColumns(int[] order, int applied, int[] inputSurvivors, int[] acceptedRanks, int kept)
    {
        for (int i = 0; i <= applied; i++) {
            int column = order[i];
            if (readPositions[column] != inputSurvivors) {
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

    private boolean prospectiveProgressiveFilterCompaction(int[] order, int applied, DynamicFilter filter, long intermediateRows)
    {
        if (!PROSPECTIVE_PROGRESSIVE_FILTER_COMPACTION ||
                nextRow < PROSPECTIVE_PROGRESSIVE_FILTER_COMPACTION_MIN_RAW_ROWS ||
                (double) nextRow / totalRows * 100 < PROSPECTIVE_PROGRESSIVE_FILTER_COMPACTION_MIN_OBSERVED_PERCENT ||
                filter.rangeDensity() * 100 > PROGRESSIVE_FILTER_COMPACTION_MAX_PERCENT ||
                !remainingFiltersObservedNonSelective(order, applied)) {
            return false;
        }
        // Compare the projected full-scan intermediate count without integer multiplication overflow.
        double projectedRows = (double) intermediateRows / nextRow * totalRows;
        boolean admitted = projectedRows >= PROSPECTIVE_PROGRESSIVE_FILTER_COMPACTION_MIN_PROJECTED_ROWS;
        if (DEBUG_PROGRESSIVE_FILTER_COMPACTION && admitted && !debugProspectiveCompactionCandidatePrinted) {
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
            if (readPositions[column] != inputSurvivors) {
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

    /** Hand out the next {@code MAX_BATCH_ROWS} surviving rows of the current window as an all-rows output batch. */
    private Batch emitSlice()
    {
        int columnCount = readers.length;
        int start = windowSurvivorCursor;
        int sliceCount = Math.min(MAX_BATCH_ROWS, windowSurvivorCount - start);
        windowSurvivorCursor += sliceCount;

        Output[] outputs = new Output[columnCount];
        for (int c = 0; c < columnCount; c++) {
            if (deferredFilteredPayload && !isFilterColumn(c)) {
                currentValues[c] = null;
                currentNulls[c] = null;
                ScanOutputResolver outputResolver = outputResolvers[c];
                outputs[c] = new Output(
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
                    ? allocator.allocate(allocationContext, BooleanVector.class, MAX_BATCH_ROWS, BooleanVector::new)
                    : null;
            Vector valueVector;
            if (readers[c].kind() == ColumnReader.Kind.INT) {
                I32Vector vector = allocator.allocate(allocationContext, I32Vector.class, MAX_BATCH_ROWS, I32Vector::new);
                System.arraycopy(windowInt[c], start, vector.values(), 0, sliceCount);
                valueVector = vector;
            }
            else if (readers[c].isDouble()) {
                valueVector = longBitsToDoubles(windowLong[c], start, sliceCount);
            }
            else {
                I64Vector vector = allocator.allocate(allocationContext, I64Vector.class, MAX_BATCH_ROWS, I64Vector::new);
                System.arraycopy(windowLong[c], start, vector.values(), 0, sliceCount);
                valueVector = vector;
            }
            if (nullVector != null) {
                System.arraycopy(windowNull[c], start, nullVector.values(), 0, sliceCount);
            }
            currentValues[c] = valueVector;
            currentNulls[c] = nullVector;
            ScanOutputResolver outputResolver = outputResolvers[c];
            outputs[c] = new Output(
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
        Batch batch = batchBuffers.batch(
                mask,
                deferredFilteredPayload ? lazyMaskConstrainer : noOpConstrainer,
                deferredFilteredPayload ? deferredFilteredClose : noOpClose,
                outputs);
        currentBatch = batch;
        return batch;
    }

    private final class ScanOutputResolver
            implements BatchBufferOwner
    {
        private final int column;
        private final Function<Stream, Vector> resolver = this::resolve;
        private final Output.MaskResolver maskResolver = this::resolveMask;

        private ScanOutputResolver(int column)
        {
            this.column = column;
        }

        private Function<Stream, Vector> resolver()
        {
            return resolver;
        }

        private Output.MaskResolver maskResolver()
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
            if (!lazyOutputResolution || stream != Stream.NULLS || !DIRECT_NULL_MASK_READER || !nullable[column]) {
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
            if (DIRECT_NULL_MASK_COMPACTION && directNullResolved[column]) {
                return null;
            }
            if (!directNullResolved[column]) {
                Mask result = resultAllocator.copyMask(resultContext, mask);
                if (DIRECT_NULL_MASK_COMPACTION) {
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
            if (deferredFilteredPayload) {
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

    private void resolveDeferredFilteredColumn(int column)
    {
        if (lazyResolved[column]) {
            return;
        }
        lazyResolved[column] = true;
        int selected = lazyMask.selectedCount();
        boolean weakConstraint = (long) selected * 100 > (long) lazyCount * DEFERRED_PAYLOAD_MAX_SURVIVOR_PERCENT;
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
                ? allocator.allocate(allocationContext, BooleanVector.class, MAX_BATCH_ROWS, BooleanVector::new)
                : null;
        boolean[] nulls = nullVector == null ? null : nullVector.values();
        if (reader.kind() == ColumnReader.Kind.INT) {
            ensureLazyScratch(decodeCount, false);
            reader.readSelectedInts(deferredRawSurvivors, decodeCount, deferredWindowRows, lazyScratchInt, isNullable ? lazyScratchNull : null);
            I32Vector vector = allocator.allocate(allocationContext, I32Vector.class, MAX_BATCH_ROWS, I32Vector::new);
            for (int index = 0; index < decodeCount; index++) {
                int outputPosition = weakConstraint || outputPositions == null ? index : outputPositions[index];
                vector.values()[outputPosition] = lazyScratchInt[index];
                if (nulls != null) {
                    nulls[outputPosition] = lazyScratchNull[index];
                }
            }
            currentValues[column] = vector;
        }
        else {
            ensureLazyScratch(decodeCount, true);
            reader.readSelectedLongs(deferredRawSurvivors, decodeCount, deferredWindowRows, lazyScratchLong, isNullable ? lazyScratchNull : null);
            if (reader.isDouble()) {
                org.weakref.nitro.data.F64Vector vector = allocator.allocate(allocationContext, org.weakref.nitro.data.F64Vector.class, MAX_BATCH_ROWS, org.weakref.nitro.data.F64Vector::new);
                for (int index = 0; index < decodeCount; index++) {
                    int outputPosition = weakConstraint || outputPositions == null ? index : outputPositions[index];
                    vector.values()[outputPosition] = Double.longBitsToDouble(lazyScratchLong[index]);
                    if (nulls != null) {
                        nulls[outputPosition] = lazyScratchNull[index];
                    }
                }
                currentValues[column] = vector;
            }
            else {
                I64Vector vector = allocator.allocate(allocationContext, I64Vector.class, MAX_BATCH_ROWS, I64Vector::new);
                for (int index = 0; index < decodeCount; index++) {
                    int outputPosition = weakConstraint || outputPositions == null ? index : outputPositions[index];
                    vector.values()[outputPosition] = lazyScratchLong[index];
                    if (nulls != null) {
                        nulls[outputPosition] = lazyScratchNull[index];
                    }
                }
                currentValues[column] = vector;
            }
        }
        currentNulls[column] = nullVector;
    }

    private void advanceUnresolvedFilteredPayload()
    {
        for (int column = 0; column < readers.length; column++) {
            if (!lazyResolved[column]) {
                lazyResolved[column] = true;
                advanceColumn(column, deferredWindowRows);
            }
        }
        deferredFilteredPayload = false;
        deferredWindowSurvivors = null;
    }

    private void constrainLazyMask(Mask mask)
    {
        lazyMask = mask;
        lazyConstrained = true;
    }

    /** Decode {@code column} into its scratch buffer: full when {@code survivors == null}, else at those positions. */
    private void readColumnInto(int column, int[] survivors, int rows, int batchRows)
    {
        ensureColumnScratch(column, rows);
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
            double[] selectivity = new double[n];
            int index = 0;
            for (int c = 0; c < filtersByColumn.length; c++) {
                if (filtersByColumn[c] != null) {
                    columns[index] = c;
                    selectivity[index] = SELECTIVITY_FILTER_ORDER ? estimateSelectivity(c) : filtersByColumn[c].size();
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
        double filterValues = filtersByColumn[column].size();
        int cardinality = readers[column].peekDictionarySize();
        if (cardinality > 0) {
            // A Parquet dictionary is chunk-local while the pushed filter is query-global. Dividing a global value
            // count by one chunk's dictionary cardinality can badly overstate the pass fraction for a sparse key
            // spread across a broad domain. Use the tighter of that bound and the filter's exact range density.
            double dictionaryFraction = Math.min(1.0, filterValues / cardinality);
            return RANGE_DENSITY_FILTER_ORDER ? Math.min(dictionaryFraction, filtersByColumn[column].rangeDensity()) : dictionaryFraction;
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
        if (EAGER_FILTER_WINDOW_SCRATCH) {
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
                colLong[column] = replaceLongs(colLong[column], rows);
            }
        }
        else if (colInt[column] == null || colInt[column].length < rows) {
            colInt[column] = replaceInts(colInt[column], rows);
        }
        if (nullable[column] && (colNull[column] == null || colNull[column].length < rows)) {
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

    @Override
    public void constrain(Mask mask)
    {
        // Late materialization: narrow the active mask so columns not yet pulled decode only for survivor rows.
        if (lazyMask != null) {
            lazyMask = mask;
            lazyConstrained = true;
        }
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return false;
    }

    @Override
    public long exactOutputRows()
    {
        return totalRows;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        return deferredFilteredPayload;
    }

    private boolean closed;

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (DEBUG_ROW_COUNTS && hasFilters) {
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
        if (DEBUG_PROGRESSIVE_FILTER_COMPACTION && debugProgressiveCompactionWindows > 0) {
            System.err.printf("[progressive-filter-compaction] columns=%s windows=%d rows=%d kept=%d%n",
                    columnNames,
                    debugProgressiveCompactionWindows,
                    debugProgressiveCompactionRows,
                    debugProgressiveCompactionKept);
        }
        if (DEBUG_DECOMPRESSION) {
            for (int column = 0; column < readers.length; column++) {
                System.err.println("[decompression] " + columnNames.get(column) + " " + readers[column].decompressionSummary());
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
        for (ParquetFile file : files) {
            file.close();
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
        private int scans;
        private int qualifyingScans;
        private boolean reported;

        public void register(long rows, int intColumns)
        {
            scans++;
            if (rows >= DIRECT_NUMERIC_BATCH_DECODE_MIN_ROWS && intColumns >= DIRECT_NUMERIC_BATCH_DECODE_MIN_INT_COLUMNS) {
                qualifyingScans++;
            }
        }

        public boolean admitted()
        {
            boolean admitted = qualifyingScans > 0;
            if (DEBUG_DIRECT_NUMERIC_BATCH_DECODE && !reported) {
                reported = true;
                System.err.printf(
                        "[direct-numeric-batch-admission] scans=%d qualifyingScans=%d admitted=%s%n",
                        scans,
                        qualifyingScans,
                        admitted);
            }
            return admitted;
        }

        public int scanCount()
        {
            return scans;
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
        arrayPool.release(lazyResolved);
        arrayPool.release(lazyPathDecided);
        arrayPool.release(lazySkipColumn);
        arrayPool.release(lazyPendingAdvance);
        for (boolean[] scratch : directNullScratch) {
            arrayPool.release(scratch);
        }
        for (int column = 0; column < readers.length; column++) {
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

    /** Drop stale aliases after BatchBufferScope has returned the closed generation's buffers to its pools. */
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
}
