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

import it.unimi.dsi.fastutil.longs.AbstractLongList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.ConcatenatedBooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HashJoinOperator
        implements Operator
{
    private static final boolean EXACT_STREAMING_BUILD_CARDINALITY =
            Boolean.parseBoolean(System.getProperty("nitro.join.exactStreamingBuildCardinality", "true"));
    private static final boolean DIRECT_BINARY_JOIN_FILTER_DISPATCH =
            Boolean.parseBoolean(System.getProperty("nitro.join.directBinaryFilterDispatch", "true"));
    // An exact equality residual is also an equi-key. Include it in the internal index so duplicate primary keys do
    // not generate candidates that the residual immediately rejects. This changes neither the public operator shape
    // nor null semantics: a null in either promoted key remains non-matching, exactly as in the residual predicate.
    private static final boolean PROMOTE_BINARY_EQUALITY_FILTER =
            Boolean.parseBoolean(System.getProperty("nitro.join.promoteBinaryEqualityFilter", "true"));
    // Resolve a left join's build-side null mask without materializing the build value. Consumers such as
    // count(column) need only this side stream; copying the value as well defeats Output's stream-level laziness.
    private static final boolean DIRECT_OUTER_JOIN_NULL_STREAM =
            Boolean.parseBoolean(System.getProperty("nitro.join.directOuterJoinNullStream", "true"));
    // A probe-outer batch with no build matches has one logical build-side value: NULL. Preserve that
    // shape as single-run vectors instead of allocating and clearing one dense values/nulls buffer per
    // projected build column. The representation is type-generic and remains readable through the
    // ordinary Vector/Streams APIs.
    private static final boolean RLE_ALL_UNMATCHED_OUTER_JOIN_OUTPUT =
            Boolean.parseBoolean(System.getProperty("nitro.join.rleAllUnmatchedOuterJoinOutput", "true"));
    private static final boolean DEBUG_RLE_ALL_UNMATCHED_OUTER_JOIN_OUTPUT =
            Boolean.getBoolean("nitro.join.debugRleAllUnmatchedOuterJoinOutput");
    private static final boolean CAP_DUPLICATE_PAIR_HASH =
            Boolean.parseBoolean(System.getProperty("nitro.join.capDuplicatePairHash", "true"));
    private static final boolean DIRECT_COMPACTED_RANGE_OUTPUT =
            Boolean.parseBoolean(System.getProperty("nitro.join.directCompactedRangeOutput", "true"));
    private static final long MAX_INITIAL_PAIR_HASH_BYTES =
            Long.getLong("nitro.join.maxInitialPairHashBytes", 512L << 20);

    @FunctionalInterface
    public interface JoinFilterFunction
    {
        boolean test(Vector outer, int outerPosition, Vector inner, int innerPosition);
    }

    /** A non-equi predicate evaluated for an equi-join candidate before an output mapping is emitted. */
    public record JoinFilter(int outerColumn, int innerColumn, JoinFilterFunction function, boolean encodedBinaryEquals, boolean longNotEqual, boolean longBitwiseOverlap)
    {
        public JoinFilter(int outerColumn, int innerColumn, JoinFilterFunction function)
        {
            this(outerColumn, innerColumn, requireNonNull(function, "function is null"), false, false, false);
        }

        public JoinFilter
        {
            if (outerColumn < 0 || innerColumn < 0) {
                throw new IllegalArgumentException("Join filter columns must be non-negative");
            }
            if (!encodedBinaryEquals && !longNotEqual && !longBitwiseOverlap) {
                requireNonNull(function, "function is null");
            }
        }

        public static JoinFilter binaryEquals(int outerColumn, int innerColumn)
        {
            return new JoinFilter(outerColumn, innerColumn, null, true, false, false);
        }

        public static JoinFilter binaryNotEquals(int outerColumn, int innerColumn)
        {
            return new JoinFilter(
                    outerColumn,
                    innerColumn,
                    (outer, outerPosition, inner, innerPosition) ->
                            !OperatorVectorSupport.binaryEquals(outer, outerPosition, inner, innerPosition));
        }

        public static JoinFilter longNotEqual(int outerColumn, int innerColumn)
        {
            return new JoinFilter(
                    outerColumn,
                    innerColumn,
                    (outer, outerPosition, inner, innerPosition) ->
                            OperatorVectorSupport.longValue(outer, outerPosition) != OperatorVectorSupport.longValue(inner, innerPosition),
                    false,
                    true,
                    false);
        }

        public static JoinFilter longBitwiseOverlap(int outerColumn, int innerColumn)
        {
            return new JoinFilter(
                    outerColumn,
                    innerColumn,
                    (outer, outerPosition, inner, innerPosition) ->
                            (OperatorVectorSupport.longValue(outer, outerPosition) & OperatorVectorSupport.longValue(inner, innerPosition)) != 0,
                    false,
                    false,
                    true);
        }
    }

    private static final int BATCH_SIZE = Integer.getInteger("nitro.hash.join.maxBatchRows", 10_000);
    private static final boolean POOL_JOIN_SCRATCH =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.poolScratch", "true"));
    // Build rows are addressed by a packed 16-bit position. Buffering up to that natural boundary reduces the
    // number of separately allocated retained-column arrays without changing probe/output batch sizing.
    private static final int BUILD_BATCH_SIZE = Math.min(1 << 16,
            Integer.getInteger("nitro.hash.join.maxBuildBatchRows", 1 << 16));
    private static final int BUILD_DICTIONARY_SPARSE_RATIO = Integer.getInteger("nitro.hash.join.buildDictionarySparseRatio", 8);
    private static final int DUPLICATE_LIST_INITIAL_CAPACITY =
            Integer.getInteger("nitro.hash.join.duplicateListInitialCapacity", 2);
    private static final boolean WRAP_NON_RETAINED_FIXED_WIDTH_BUILD_VALUES =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.wrapNonRetainedFixedWidthBuildValues", "true"));
    private static final boolean CACHE_INNER_DICTIONARY_IDS =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.cacheInnerDictionaryIds", "true"));
    private static final boolean ALIAS_BATCH_DICTIONARY_IDS =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.aliasBatchDictionaryIds", "true"));
    private static final boolean ALIAS_FULL_BATCH_DICTIONARY_IDS =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.aliasFullBatchDictionaryIds", "false"));
    private static final boolean WRAP_ENCODED_OUTER_DICTIONARIES =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.wrapEncodedOuterDictionaries", "true"));
    // Preserve shallow lazy mappings (the allocation/locality win established by q45), but do not let a long join
    // chain turn every downstream access into an unbounded pointer chase. At this source depth, compose the mapping
    // once and share it across columns with the same encoding chain.
    private static final int DEFAULT_COMPOSE_ENCODED_OUTER_DICTIONARY_DEPTH =
            Integer.getInteger("nitro.hash.join.composeEncodedOuterDictionaryDepth", Integer.MAX_VALUE);
    // Deep row mappings are cheaper to collapse when a selective join emits only a tiny batch. The composed id
    // array is shared by every output column with the same encoding chain; large batches retain lazy nesting.
    private static final boolean ADAPTIVE_TINY_DICTIONARY_COMPOSITION =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.adaptiveTinyDictionaryComposition", "true"));
    private static final int ADAPTIVE_COMPOSE_MAX_ROWS =
            Integer.getInteger("nitro.hash.join.adaptiveComposeMaxRows", 1024);
    private static final int ADAPTIVE_COMPOSE_DEPTH =
            Integer.getInteger("nitro.hash.join.adaptiveComposeDepth", 4);
    private static final boolean DEFAULT_LAZY_DUPLICATE_SLOT_STATE =
            Boolean.parseBoolean(System.getProperty("nitro.join.lazyDuplicateSlotState", "false"));
    private static final boolean IMPLICIT_SEQUENTIAL_BUILD_ROW_REFERENCES =
            Boolean.parseBoolean(System.getProperty("nitro.join.implicitSequentialBuildRowReferences", "true"));
    // Flat composite joins are usually unique. Store their common one-row case in one pooled primitive array and
    // allocate a list only after observing a duplicate, instead of allocating a list and backing array per key.
    // The switch retains the previous representation as a benchmark control.
    private static final boolean FLAT_PRIMITIVE_SINGLE_ROWS =
            Boolean.parseBoolean(System.getProperty("nitro.join.flatPrimitiveSingleRows", "true"));
    private static final boolean CACHE_COMPOSED_OUTER_DICTIONARY_IDS =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.cacheComposedOuterDictionaryIds", "true"));
    private static final boolean DIRECT_DENSE_SINGLE_MATCH_RANGE_OUTPUT =
            Boolean.parseBoolean(System.getProperty("nitro.join.directDenseSingleMatchRangeOutput", "true"));
    private static final boolean ORDERED_LONG_JOIN_FILTER_PAYLOAD =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.orderedLongFilterPayload", "true"));
    private static final boolean CACHE_CURRENT_OUTER_JOIN_FILTER =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.cacheCurrentOuterFilter", "true"));
    // A zero (or null) build operand can never satisfy a bitwise-overlap residual. Prune such rows before buffering
    // and hash-table insertion, while retaining the exact residual for non-zero operands whose bits may be disjoint.
    // This is predicate algebra owned by the generic join-filter implementation, not a query or type-shape path.
    private static final boolean PRUNE_ZERO_BITWISE_OVERLAP_BUILD_ROWS =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.pruneZeroBitwiseOverlapBuildRows", "true"));
    // Hoist fixed-width vector dispatch out of the row loop while building the ubiquitous single-long-key index.
    // This remains exact for retained/selected batches: the logical row reference uses the compact batch position,
    // while key/null reads use the corresponding source position.
    private static final boolean BATCH_SINGLE_LONG_BUILD =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.batchSingleLongBuild", "true"));
    private static final boolean BATCH_LONG_PAIR_BUILD =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.batchLongPairBuild", "true"));
    // If no build column survives the join and no residual predicate reads build payload, index each source batch
    // directly and release it. Buffering the complete build cannot affect the result in this shape: row references
    // are used only to preserve duplicate multiplicity, never to materialize a build value.
    private static final boolean STREAM_UNUSED_BUILD_PAYLOAD =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.streamUnusedBuildPayload", "true"));
    private static final boolean COMPACT_COMPLETED_DIRECT_RANGE_BUILD =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.compactCompletedDirectRangeBuild", "true"));
    private static final boolean DENSE_UNUSED_BUILD_MEMBERSHIP =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.denseUnusedBuildMembership", "true"));
    private static final int DENSE_UNUSED_BUILD_MEMBERSHIP_MIN_KEYS =
            Integer.getInteger("nitro.hash.join.denseUnusedBuildMembershipMinKeys", 1 << 12);
    private static final int COMPACT_COMPLETED_DIRECT_RANGE_MIN_SIZE =
            Integer.getInteger("nitro.hash.join.compactCompletedDirectRangeMinSize", 256);
    // Velox-style dynamic filtering: once the (small) build side is materialized, push its single-column key
    // membership down the probe chain so a skip-decode scan can eliminate non-matching rows during decode. On by
    // default (a non-selective filter self-abandons after a warmup in the scan, so the build-side collection is the
    // only residual cost); disable with -Dnitro.dynamicFilter=false.
    private static final boolean DYNAMIC_FILTER_ENABLED = Boolean.parseBoolean(System.getProperty("nitro.dynamicFilter", "true"));
    private static final boolean SHARE_SPARSE_LONG_RANGE_FILTER =
            Boolean.parseBoolean(System.getProperty("nitro.dynamicFilter.shareSparseLongRange", "true"));
    // A one-to-one identity join mapping needs neither a constrained re-borrow nor a copy: the upstream column is
    // already the exact result vector. Keep it borrowed under the still-open outer batch and forward it directly.
    // The rule depends only on the physical row mapping and works for every re-borrow-capable operator and vector.
    private static final boolean FORWARD_IDENTITY_REBORROW_OUTER =
            Boolean.parseBoolean(System.getProperty("nitro.join.forwardIdentityReborrowOuter", "true"));
    private static final boolean DEBUG_IDENTITY_REBORROW_OUTER =
            Boolean.getBoolean("nitro.debug.identityReborrowOuter");
    // A multi-key join emits one dynamic filter per key column (each a necessary join condition). Disable to restrict
    // dynamic filters to single-key joins with -Dnitro.dynamicFilter.multiKey=false.
    private static final boolean MULTI_KEY_DYNAMIC_FILTER = Boolean.parseBoolean(System.getProperty("nitro.dynamicFilter.multiKey", "true"));
    // Resolve RLE run indices for a monotonic (outer/probe-ordered) output column with a forward hint instead of a
    // per-position binary search. Set false to force the binary search (for A/B measurement of the two paths).
    private static final boolean RLE_RUN_INDEX_HINT = Boolean.parseBoolean(System.getProperty("nitro.join.rleRunIndexHint", "true"));
    private static final boolean POOL_BUILD_DICTIONARY_IDS =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.poolBuildDictionaryIds", "true"));
    // A 64K-entry set remains small relative to the join index and captures selective medium-sized dimensions (q54).
    // Collection is independently bounded by DYNAMIC_FILTER_BUILD_ROW_LIMIT, so large build sides do not pay for it.
    private static final int DYNAMIC_FILTER_MAX_VALUES = Integer.getInteger("nitro.dynamicFilter.maxValues", 1 << 16);
    // Buffered builds already expose their exact row count before indexing. Use that bounded cardinality to size the
    // optional membership set once instead of repeatedly rehashing it from FastUtil's tiny default. The value count
    // remains only an upper bound on distinct keys, and the existing distinct-value cap still abandons nonselective
    // filters. Disable only for adjacent A/B measurement.
    private static final boolean PRE_SIZE_DYNAMIC_FILTER_VALUE_SETS =
            Boolean.parseBoolean(System.getProperty("nitro.dynamicFilter.preSizeValueSets", "true"));
    // When the build-row admission bound is no larger than the distinct-value cap, an admitted single-key filter
    // can append every build key to bounded pooled storage and deduplicate once at publication. This removes a hash
    // probe from the build loop while preserving the exact final membership representation.
    private static final boolean DEFER_DYNAMIC_FILTER_DEDUPLICATION =
            Boolean.parseBoolean(System.getProperty("nitro.dynamicFilter.deferDeduplication", "true"));
    private static final int DEFER_DYNAMIC_FILTER_MIN_ROWS =
            Integer.getInteger("nitro.dynamicFilter.deferDeduplicationMinRows", 4096);
    private static final double DEFER_DYNAMIC_FILTER_MAX_DENSITY =
            Double.parseDouble(System.getProperty("nitro.dynamicFilter.deferDeduplicationMaxDensity", "0.005"));
    // Set construction already visits every build key. Retain exact bounds during that visit so conversion to the
    // immutable dense membership representation needs one distinct-key traversal rather than two.
    private static final boolean TRACK_DYNAMIC_FILTER_VALUE_RANGE =
            Boolean.parseBoolean(System.getProperty("nitro.dynamicFilter.trackValueRange", "true"));
    // Skip dynamic-filter key collection for large build sides. These sets are expensive to collect and often
    // non-selective (e.g. full dimensions), while genuinely useful runtime filters are usually small date/status
    // domains. Set to a huge value to disable the gate (always collect), for A/B measurement.
    private static final long DYNAMIC_FILTER_BUILD_ROW_LIMIT = Long.getLong("nitro.dynamicFilter.buildRowLimit", 1L << 16);
    // A conventional build-first hash join cannot filter a very large build from a much smaller probe. For an inner
    // join whose build pipeline advertises dynamic-filter support, spool the probe up to a strict bound. If it ends
    // within that bound, its complete per-key value sets are necessary join conditions and can be pushed into the
    // build before any build payload is decoded. If it does not end, the spool simply replays the prefix and the
    // ordinary build-first path remains exact. Admission uses only physical cardinality/capability signals.
    private static final boolean PROBE_FIRST_BUILD_FILTER =
            Boolean.parseBoolean(System.getProperty("nitro.hash.join.probeFirstBuildFilter", "true"));
    private static final long PROBE_FIRST_MIN_BUILD_ROWS =
            Long.getLong("nitro.hash.join.probeFirstMinBuildRows", 4L << 20);
    private static final int PROBE_FIRST_MAX_PROBE_ROWS =
            Integer.getInteger("nitro.hash.join.probeFirstMaxProbeRows", 1 << 16);
    private static final long NO_MATCH_ROW_REFERENCE = -1L;
    private static final boolean DEBUG_DYNAMIC_FILTER = Boolean.getBoolean("nitro.debug.dynamicFilter");
    private static final boolean DEBUG_JOIN_INDEX = Boolean.getBoolean("nitro.debug.joinIndex");
    private static final int NO_MATCH_COMPACT_ROW_REFERENCE = -1;
    private static final int VALUES_FLAG = 1;
    private static final int NULLS_FLAG = 1 << 1;
    private static final int ERRORS_FLAG = 1 << 2;
    private static final Vector[] NO_NULL_STREAMS = new Vector[0];
    private final Allocator allocator;
    private final OperatorResources operatorResources;
    private final HashJoinMaterializationListener materializationListener;
    // Build buffers outlive every individual result batch. Keep their ownership separate from result wrappers:
    // a dictionary result may borrow a build vector, and closing that result must release only the wrapper rather
    // than returning the still-live build vector to the shared pool. The two contexts deliberately share a pool so
    // their compatible buffers can still be recycled once the owning lifetime has ended. Independent join instances
    // use the same engine-owner compatibility key by default: a released sibling/subtree generation can feed the next
    // join without either context gaining authority over the other's live buffers. The Allocator itself owns the pool,
    // so the compatibility key never shares storage between queries or Allocator instances.
    private final Object allocationPoolGroup = new Object();
    private final Object allocationCompatibilityGroup;
    // Keep the established profile name for output buffers; allocation telemetry and external diagnostics aggregate
    // contexts by this stable name. Build ownership is still isolated below by a distinct scope and name.
    private final Allocator.Context allocationContext;
    private final Allocator.Context buildAllocationContext;
    private final Operator outer;
    private Operator probeSource;
    private final Operator inner;
    private final int outerOutputCount;
    private final int innerOutputCount;
    private final int totalOutputCount;
    // Public output ordinal -> physical concatenated [outer..., inner...] column. Keeping projection inside the join
    // preserves lazy materialization: columns omitted by the plan never get an Output wrapper or a payload borrow.
    private int[] outputChannels;
    private int composeEncodedOuterDictionaryDepth = DEFAULT_COMPOSE_ENCODED_OUTER_DICTIONARY_DEPTH;
    private boolean lazyDuplicateSlotState = DEFAULT_LAZY_DUPLICATE_SLOT_STATE;
    private boolean implicitSequentialBuildRowReferences;
    private final boolean probeOuterJoin;
    private final int[] outerJoinColumns;
    private final int[] innerJoinColumns;
    private final JoinFilter[] joinFilters;
    private final Vector[] currentOuterFilterValues;
    private final Vector[] currentOuterFilterNulls;
    private final VectorAccess.BooleanValues[] currentOuterFilterNullAccess;
    private final Vector[] currentOuterFilterBaseValues;
    private final int[] currentOuterFilterDictionaryDepths;
    private Vector[][] innerFilterValues;
    private Vector[][] innerFilterNulls;
    private VectorAccess.BooleanValues[][] innerFilterNullAccess;
    private Vector[][] innerFilterBaseValues;
    private int[][] innerFilterDictionaryDepths;
    // Lazily resolved per-build-batch NULLS accessors for stream-only outer-join materialization. A null accessor
    // denotes an absent or all-false source stream; the parallel resolved map distinguishes it from not-yet-read.
    private VectorAccess.BooleanValues[][] directInnerNullAccess;
    private boolean[][] directInnerNullAccessResolved;
    private final boolean singleEncodedBinaryJoinFilter;
    private final boolean promotedBinaryEqualityFilter;
    private final boolean singleLongNotEqualJoinFilter;
    private final boolean singleLongBitwiseOverlapJoinFilter;
    private VectorAccess.LongValues fastOuterFilterLongs;
    private VectorAccess.LongValues fastInnerFilterLongs;
    private long[] fastOuterFilterLongArray;
    private long[] fastInnerFilterLongArray;
    private int[] fastInnerFilterPositions;
    private int[] fastInnerOrderedIntFilterValues;
    private boolean fastInnerOrderedFilterAttempted;
    private boolean currentFastOuterFilterNull;
    private long currentFastOuterFilterLong;
    private DictionaryVector fastOuterFilterDictionary;
    private BinaryVector fastOuterFilterBase;
    private int fastOuterFilterDepth;
    private int currentFastOuterFilterBasePosition;
    private VectorAccess.BooleanValues fastOuterFilterNulls;
    private BufferedJoinInput.InnerBatch fastInnerFilterBatch;
    private DictionaryVector fastInnerFilterDictionary;
    private BinaryVector fastInnerFilterBase;
    private int fastInnerFilterDepth;
    private VectorAccess.BooleanValues fastInnerFilterNulls;
    private final JoinBufferSupport buffers;
    private final PrimitiveArrayPool arrayPool;
    private final BufferedJoinInput bufferedInner;
    private final Streams[] outerSchema;
    private final Streams[] innerSchema;
    private final Vector[] currentOuterJoinValues;
    private final Vector[] currentOuterJoinNulls;
    private final JoinScratch joinScratch;
    private final int[] outputOuterPositions;
    private final long[] outputInnerRows;
    private final int[] outputInnerLogicalPositions;
    private final int[] outputInnerRunStarts;
    private final int[] outputInnerRunLengths;
    private final int[] outputInnerRunBatchIndexes;
    private final int[] outputInnerRunUniqueStarts;
    private final int[] outputInnerRunUniqueCounts;
    private final int[] preparedOuterPositions;
    private final LongList[] preparedOuterMatches = new LongList[BATCH_SIZE];
    private SingleLongList[] preparedSingleMatches;
    // Flat single-match output: when the build is unique, the probe writes one build row reference per
    // outer row here and produceBatch emits from it without a LongList or per-row virtual dispatch.
    private final long[] preparedSingleRefs;
    private final int[] preparedSingleRefs32;
    private final int[] preparedRangeStarts;
    private final int[] preparedRangeCounts;
    private boolean joinScratchReleased;
    private boolean singleMatchProbe;
    private boolean singleMatchPositionProbe;
    private boolean compactSingleMatchProbe;
    private int currentMatchRangeStart;
    private long currentMatchRef;
    private int currentMatchPosition;
    private final Streams[] currentOutputs;
    private int[] retainedConstraintCountsByBatch = new int[16];
    private int[][] retainedConstraintPositionsByBatch = new int[16][];
    // Lazily built, per (inner batch, inner column) unified dictionary view for non-retained build
    // BinaryVector columns. Built once over the (small) build side; reused to emit every probe-output
    // batch's matched rows as a DictionaryVector over the shared dictionary instead of flattening the
    // bytes per output row. Keyed by batchIndex * innerOutputCount + innerOutputIndex.
    private final Map<Integer, BuildDictionary> buildDictionaries = new HashMap<>();
    private JoinIndex joinIndex;

    private Mask currentOuterMask;
    private Batch currentOuterBatch;
    private int currentOuterMaskIndex;
    private int outerRemaining;
    private int currentOuterPosition;
    private boolean currentOuterPositionReady;
    private LongList currentMatches = LongLists.emptyList();
    private int currentMatchCount;
    private boolean currentOuterJoinHasNulls;
    private int currentMatchIndex;
    private int currentOutputCount;
    // 0 = not inspected for this output batch, 1 = contains a match, 2 = entirely unmatched.
    private byte allRowsNoMatchState;
    // 0 = uninitialized, 1 = ordinary producer, 2 = compacted-range producer, 3 = empty inner join.
    private int probeOutputMode;
    private Mask currentOutputMask;
    private int[] currentOuterDictionaryIds;
    // When encoded outer columns share the same dictionary/RLE mapping chain, composing that chain separately for
    // every VALUES/NULLS/ERRORS stream repeats the same random gathers and allocates one int[] per stream. Retain one
    // representative chain and its composed ids for the current output batch; columns with the same mapping identity
    // reuse those ids over their own leaf vector. This is representation-driven and independent of logical type.
    private final Vector[] composedOuterMappingSources;
    private final int[][] composedOuterMappingIds;
    private int composedOuterMappingCount;
    private int[] currentInnerLogicalDictionaryIds;
    private int[] currentInnerSourceDictionaryIds;
    private JoinBufferSupport.PositionMappingCache innerPositionMappingCache;
    private int preparedOuterCount;
    private int preparedOuterIndex;
    private boolean preparedOuterRange;
    private int preparedOuterRangeStart;
    private boolean outputInnerLogicalPositionsReady;
    private int outputInnerLogicalPositionsBatchIndex;
    private boolean done;
    private boolean outerConstrained;
    private boolean outerConstraintApplied;
    // Proven once per result batch by constrainOuterIfNecessary(). Materializing every retained stream must not
    // rescan the complete row mapping merely to rediscover the same identity fact; wide joins otherwise turn a
    // zero-copy capability into O(output columns x rows) validation work even when no copy is avoided.
    private boolean forwardOuterIdentity;
    private boolean identityReborrowReported;
    private int[] matchedOuterPositions = new int[0];
    private boolean outerSupportsReborrow;
    private int preparedInnerRunCount = -1;
    private String profileName;
    // Dynamic-filter state: distinct single-column build keys collected during index build, and whether collection
    // is still viable (single long key, inner join, readable vector, under the cap). Pushed to the probe once.
    private final boolean buildKeysViable;
    // Per join-key column: the distinct build-side values, for a dynamic filter pushed to the matching probe column.
    // A multi-key join contributes one filter per column — each is a necessary condition (a row joins only if EVERY
    // key matches), so the per-column membership sets over-approximate the joinable set; the join still does the
    // exact tuple match. A column is abandoned on its own when its encoding is unsupported or it exceeds the cap.
    private LongOpenHashSet[] buildKeyValues;
    private boolean[] buildKeyColumnAbandoned;
    private long[] buildKeyMins;
    private long[] buildKeyMaxs;
    private long[] collectedBuildKeys;
    private int collectedBuildKeyCount;
    private byte collectedBuildKeyAdmission;
    private boolean buildKeysAbandoned;
    private boolean streamedInnerLoaded;
    private long streamedInnerRows;
    private boolean dynamicFilterPushed;
    private int expectedIndexedInnerRows = -1;

    public HashJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn}, false, new JoinFilter[0]);
    }

    public HashJoinOperator(OperatorResources operatorResources, Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(operatorResources, allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn}, false, new JoinFilter[0]);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn, JoinFilter... joinFilters)
    {
        this(allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn}, false, joinFilters);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn, boolean probeOuterJoin)
    {
        this(allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn}, probeOuterJoin, new JoinFilter[0]);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns)
    {
        this(allocator, outer, outerJoinColumns, inner, innerJoinColumns, false, new JoinFilter[0]);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns, boolean probeOuterJoin)
    {
        this(allocator, outer, outerJoinColumns, inner, innerJoinColumns, probeOuterJoin, new JoinFilter[0]);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns, JoinFilter... joinFilters)
    {
        this(allocator, outer, outerJoinColumns, inner, innerJoinColumns, false, joinFilters);
    }

    private HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns, boolean probeOuterJoin, JoinFilter[] joinFilters)
    {
        this(
                allocator.engineResources().operatorResources(),
                allocator,
                outer,
                outerJoinColumns,
                inner,
                innerJoinColumns,
                probeOuterJoin,
                joinFilters);
    }

    public HashJoinOperator(
            OperatorResources operatorResources,
            Allocator allocator,
            Operator outer,
            int[] outerJoinColumns,
            Operator inner,
            int[] innerJoinColumns,
            boolean probeOuterJoin,
            JoinFilter... joinFilters)
    {
        if (outerJoinColumns.length != innerJoinColumns.length) {
            throw new IllegalArgumentException("Join key counts must match");
        }
        if (outerJoinColumns.length == 0) {
            throw new IllegalArgumentException("Hash join requires at least one join key");
        }
        if (probeOuterJoin && joinFilters.length != 0) {
            throw new IllegalArgumentException("Join filters are not yet supported for probe outer joins");
        }

        this.allocator = allocator;
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        this.materializationListener = operatorResources.hashJoin().materializationListener();
        this.allocationCompatibilityGroup = operatorResources.hashJoin()
                .bufferPoolCompatibilityGroup(allocationPoolGroup);
        this.allocationContext = new Allocator.Context(
                "HashJoinOperator",
                allocationPoolGroup,
                allocationCompatibilityGroup);
        this.buildAllocationContext = new Allocator.Context(
                "HashJoinOperatorBuild",
                allocationPoolGroup,
                allocationCompatibilityGroup);
        this.arrayPool = allocator.primitiveArrays();
        allocator.register(allocationContext);
        allocator.register(buildAllocationContext);
        this.outer = outer;
        this.probeSource = outer;
        this.inner = inner;
        this.outerOutputCount = outer.outputCount();
        this.innerOutputCount = inner.outputCount();
        this.totalOutputCount = outerOutputCount + innerOutputCount;
        this.outputChannels = new int[totalOutputCount];
        java.util.Arrays.setAll(outputChannels, index -> index);
        this.composedOuterMappingSources = new Vector[totalOutputCount * 3];
        this.composedOuterMappingIds = new int[totalOutputCount * 3][];
        this.probeOuterJoin = probeOuterJoin;
        this.outerJoinColumns = outerJoinColumns.clone();
        this.innerJoinColumns = innerJoinColumns.clone();
        this.joinFilters = joinFilters.clone();
        this.currentOuterFilterValues = new Vector[joinFilters.length];
        this.currentOuterFilterNulls = new Vector[joinFilters.length];
        this.currentOuterFilterNullAccess = new VectorAccess.BooleanValues[joinFilters.length];
        this.currentOuterFilterBaseValues = new Vector[joinFilters.length];
        this.currentOuterFilterDictionaryDepths = new int[joinFilters.length];
        this.singleEncodedBinaryJoinFilter = joinFilters.length == 1 && joinFilters[0].encodedBinaryEquals();
        this.promotedBinaryEqualityFilter = PROMOTE_BINARY_EQUALITY_FILTER && singleEncodedBinaryJoinFilter;
        this.singleLongNotEqualJoinFilter = joinFilters.length == 1 && joinFilters[0].longNotEqual();
        this.singleLongBitwiseOverlapJoinFilter = joinFilters.length == 1 && joinFilters[0].longBitwiseOverlap();
        for (JoinFilter filter : joinFilters) {
            if (filter.outerColumn() >= outerOutputCount || filter.innerColumn() >= innerOutputCount) {
                throw new IllegalArgumentException("Join filter column is out of bounds");
            }
        }
        this.buffers = new JoinBufferSupport(allocator, allocationContext);
        this.bufferedInner = new BufferedJoinInput(new JoinBufferSupport(allocator, buildAllocationContext), innerOutputCount);
        this.outerSchema = new Streams[outerOutputCount];
        this.innerSchema = new Streams[innerOutputCount];
        int effectiveJoinKeyCount = outerJoinColumns.length + (promotedBinaryEqualityFilter ? 1 : 0);
        this.currentOuterJoinValues = new Vector[effectiveJoinKeyCount];
        this.currentOuterJoinNulls = new Vector[effectiveJoinKeyCount];
        JoinScratch pooledScratch = POOL_JOIN_SCRATCH
                ? arrayPool.borrow(JoinScratch.class, BATCH_SIZE, JoinScratch.class)
                : null;
        this.joinScratch = pooledScratch != null ? pooledScratch : new JoinScratch(BATCH_SIZE);
        this.outputOuterPositions = joinScratch.outputOuterPositions;
        this.outputInnerRows = joinScratch.outputInnerRows;
        this.outputInnerLogicalPositions = joinScratch.outputInnerLogicalPositions;
        this.outputInnerRunStarts = joinScratch.outputInnerRunStarts;
        this.outputInnerRunLengths = joinScratch.outputInnerRunLengths;
        this.outputInnerRunBatchIndexes = joinScratch.outputInnerRunBatchIndexes;
        this.outputInnerRunUniqueStarts = joinScratch.outputInnerRunUniqueStarts;
        this.outputInnerRunUniqueCounts = joinScratch.outputInnerRunUniqueCounts;
        this.preparedOuterPositions = joinScratch.preparedOuterPositions;
        this.preparedSingleRefs = joinScratch.preparedSingleRefs;
        this.preparedSingleRefs32 = joinScratch.preparedSingleRefs32;
        this.preparedRangeStarts = joinScratch.preparedRangeStarts;
        this.preparedRangeCounts = joinScratch.preparedRangeCounts;
        this.currentOutputs = new Streams[totalOutputCount];
        this.buildKeysViable = DYNAMIC_FILTER_ENABLED && !probeOuterJoin
                && (innerJoinColumns.length == 1 || MULTI_KEY_DYNAMIC_FILTER);
        Arrays.fill(retainedConstraintCountsByBatch, -1);
    }

    @Override
    public int outputCount()
    {
        return outputChannels.length;
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public Batch next()
    {
        long start = System.nanoTime();
        Mask batchMask = produceBatch();
        long afterProduceBatch = System.nanoTime();
        preparedInnerRunCount = -1;
        currentOutputMask = batchMask;
        outerConstrained = false;
        outerConstraintApplied = false;
        forwardOuterIdentity = false;
        currentOuterDictionaryIds = null;
        clearComposedOuterMappings();
        currentInnerLogicalDictionaryIds = null;
        currentInnerSourceDictionaryIds = null;
        innerPositionMappingCache = buffers.newPositionMappingCache();
        allRowsNoMatchState = 0;
        java.util.Arrays.fill(currentOutputs, null);
        Output[] outputs = new Output[outputChannels.length];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            outputs[outputIndex] = resultOutput(outputChannels[outputIndex]);
        }
        long afterBuildOutputs = System.nanoTime();
        return new Batch(
                batchMask,
                _ -> {},
                takenMask -> allocator.transfer(allocationContext, takenMask),
                releasedMask -> allocator.release(allocationContext, releasedMask),
                () -> {},
                outputs);
    }

    private Mask produceBatch()
    {
        outputInnerLogicalPositionsReady = false;
        outputInnerLogicalPositionsBatchIndex = -1;
        if (probeOutputMode == 0) {
            prepareProbeFirstBuildFilter();
            loadInnerIfNecessary();
            pushDynamicFilterIfReady();
            if ((joinIndex == null || joinIndex.isEmpty()) && !probeOuterJoin) {
                captureOuterSchemaIfAvailable();
                done = true;
                probeOutputMode = 3;
            }
            else if (!probeOuterJoin && joinFilters.length == 0 &&
                    !joinIndex.supportsSingleMatchRefs() && joinIndex.supportsRowRanges()) {
                probeOutputMode = 2;
            }
            else {
                probeOutputMode = 1;
            }
        }
        return switch (probeOutputMode) {
            case 1 -> produceOrdinaryBatch();
            case 2 -> produceRangeBatch();
            case 3 -> {
                currentOutputCount = 0;
                yield allocator.allocateAllMask(allocationContext, 0);
            }
            default -> throw new IllegalStateException("Unexpected probe output mode: " + probeOutputMode);
        };
    }

    private Mask produceOrdinaryBatch()
    {
        int outputPosition = 0;
        boolean logicalPositionsReady = false;
        boolean rowReferencesWritten = false;
        Batch outputOuterBatch = currentOuterBatch;
        while (outputPosition < BATCH_SIZE) {
            if (outerRemaining == 0) {
                if (outputPosition > 0) {
                    break;
                }
                if (!loadNextOuterBatch()) {
                    done = true;
                    break;
                }
                outputOuterBatch = currentOuterBatch;
            }

            if (!currentOuterPositionReady) {
                int emitted = tryEmitDirectSingleMatchPositionRange(outputPosition);
                if (emitted >= 0) {
                    outputPosition += emitted;
                    if (emitted > 0 && !rowReferencesWritten) {
                        logicalPositionsReady = true;
                    }
                    continue;
                }
                if (preparedOuterIndex >= preparedOuterCount) {
                    if (currentOuterMaskIndex >= currentOuterMask.count()) {
                        outerRemaining = 0;
                        continue;
                    }
                    prepareOuterProbeChunk();
                }
                if (preparedOuterIndex >= preparedOuterCount) {
                    outerRemaining = 0;
                    continue;
                }
                currentOuterPosition = preparedOuterRange ? preparedOuterRangeStart + preparedOuterIndex : preparedOuterPositions[preparedOuterIndex];
                if (singleMatchProbe) {
                    if (singleMatchPositionProbe) {
                        currentMatchPosition = preparedSingleRefs32[preparedOuterIndex];
                        currentMatchCount = currentMatchPosition == NO_MATCH_COMPACT_ROW_REFERENCE ? 0 : 1;
                    }
                    else {
                        currentMatchRef = compactSingleMatchProbe
                                ? joinIndex.unpackCompactSingleMatchRef(preparedSingleRefs32[preparedOuterIndex])
                                : preparedSingleRefs[preparedOuterIndex];
                        currentMatchCount = currentMatchRef == NO_MATCH_ROW_REFERENCE ? 0 : 1;
                    }
                }
                else {
                    currentMatches = preparedOuterMatches[preparedOuterIndex];
                    currentMatchCount = currentMatches.size();
                }
                preparedOuterIndex++;
                currentOuterPositionReady = true;
                currentMatchIndex = 0;
                cacheCurrentOuterFilterValue();
            }

            if (currentMatchCount == 0) {
                if (probeOuterJoin) {
                    outputOuterPositions[outputPosition] = currentOuterPosition;
                    outputInnerRows[outputPosition] = NO_MATCH_ROW_REFERENCE;
                    outputPosition++;
                }
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatches = LongLists.emptyList();
                currentMatchCount = 0;
                continue;
            }

            while (currentMatchIndex < currentMatchCount && outputPosition < BATCH_SIZE) {
                int matchIndex = currentMatchIndex;
                long rowReference;
                if (singleMatchPositionProbe) {
                    rowReference = packRowReference(joinIndex.singleMatchPositionBatchIndex(), currentMatchPosition);
                }
                else {
                    rowReference = singleMatchProbe ? currentMatchRef : currentMatches.getLong(currentMatchIndex);
                }
                currentMatchIndex++;
                boolean passes = promotedBinaryEqualityFilter ||
                        (DIRECT_BINARY_JOIN_FILTER_DISPATCH && singleEncodedBinaryJoinFilter
                                ? passesBinaryJoinFilter(currentOuterPosition, rowReference)
                                : passesJoinFilters(currentOuterPosition, rowReference, matchIndex));
                if (!passes) {
                    continue;
                }
                outputOuterPositions[outputPosition] = currentOuterPosition;
                if (singleMatchPositionProbe) {
                    outputInnerLogicalPositions[outputPosition] = currentMatchPosition;
                    if (!rowReferencesWritten) {
                        logicalPositionsReady = true;
                    }
                }
                else {
                    outputInnerRows[outputPosition] = rowReference;
                    rowReferencesWritten = true;
                    logicalPositionsReady = false;
                }
                outputPosition++;
            }

            if (currentMatchIndex == currentMatchCount) {
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatches = LongLists.emptyList();
                currentMatchCount = 0;
            }
        }

        if (outputPosition == 0) {
            currentOutputCount = 0;
            return allocator.allocateAllMask(allocationContext, 0);
        }
        currentOutputCount = outputPosition;
        if (logicalPositionsReady && !rowReferencesWritten) {
            outputInnerLogicalPositionsReady = true;
            outputInnerLogicalPositionsBatchIndex = joinIndex.singleMatchPositionBatchIndex();
        }
        return allocator.allocateRangeMask(allocationContext, 0, outputPosition);
    }

    /**
     * Filter-free inner join over a compacted one-to-many build. The index exposes each probe match as a contiguous
     * slice of pooled row references, so output production bulk-copies that slice and fills the repeated probe
     * position. This method is selected once per output batch; the ordinary per-row loop contains no range feature
     * branch.
     */
    private Mask produceRangeBatch()
    {
        int outputPosition = 0;
        while (outputPosition < BATCH_SIZE) {
            if (outerRemaining == 0) {
                if (outputPosition > 0) {
                    break;
                }
                if (!loadNextOuterBatch()) {
                    done = true;
                    break;
                }
            }

            if (!currentOuterPositionReady) {
                if (preparedOuterIndex >= preparedOuterCount) {
                    if (currentOuterMaskIndex >= currentOuterMask.count()) {
                        outerRemaining = 0;
                        continue;
                    }
                    preparedOuterCount = Math.min(currentOuterMask.count() - currentOuterMaskIndex, BATCH_SIZE);
                    preparedOuterIndex = 0;
                    for (int index = 0; index < preparedOuterCount; index++) {
                        preparedOuterPositions[index] = currentOuterMask.position(currentOuterMaskIndex++);
                    }
                    if (!joinIndex.matchRowRanges(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls,
                            preparedOuterPositions, preparedOuterCount, preparedRangeStarts, preparedRangeCounts)) {
                        throw new IllegalStateException("Compacted range index stopped supporting row ranges");
                    }
                }
                currentOuterPosition = preparedOuterPositions[preparedOuterIndex];
                currentMatchRangeStart = preparedRangeStarts[preparedOuterIndex];
                currentMatchCount = preparedRangeCounts[preparedOuterIndex];
                preparedOuterIndex++;
                currentOuterPositionReady = true;
                currentMatchIndex = 0;
            }

            if (currentMatchCount == 0) {
                outerRemaining--;
                currentOuterPositionReady = false;
                continue;
            }

            int emitted = Math.min(currentMatchCount - currentMatchIndex, BATCH_SIZE - outputPosition);
            Arrays.fill(outputOuterPositions, outputPosition, outputPosition + emitted, currentOuterPosition);
            joinIndex.copyRowRange(currentMatchRangeStart + currentMatchIndex, outputInnerRows, outputPosition, emitted);
            currentMatchIndex += emitted;
            outputPosition += emitted;

            if (currentMatchIndex == currentMatchCount) {
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatchCount = 0;
            }
        }

        if (outputPosition == 0) {
            currentOutputCount = 0;
            return allocator.allocateAllMask(allocationContext, 0);
        }
        currentOutputCount = outputPosition;
        return allocator.allocateRangeMask(allocationContext, 0, outputPosition);
    }

    private int tryEmitDirectSingleMatchPositionRange(int outputPosition)
    {
        if (!DIRECT_DENSE_SINGLE_MATCH_RANGE_OUTPUT ||
                joinFilters.length != 0 ||
                probeOuterJoin ||
                preparedOuterIndex < preparedOuterCount ||
                !currentOuterMask.all() ||
                currentOuterMaskIndex >= currentOuterMask.count()) {
            return -1;
        }
        int probeCount = Math.min(
                Math.min(currentOuterMask.count() - currentOuterMaskIndex, outerRemaining),
                BATCH_SIZE - outputPosition);
        if (probeCount <= 0) {
            return -1;
        }
        if (!joinIndex.supportsSingleMatchRefs() ||
                !joinIndex.supportsSingleMatchPositions() ||
                !joinIndex.supportsSingleMatchPositionRange() ||
                !joinIndex.supportsDirectSingleMatchPositionRangeOutput()) {
            return -1;
        }
        int probeStart = currentOuterMaskIndex;
        int emitted = joinIndex.emitSingleRowsPositionsRange(
                currentOuterJoinValues,
                currentOuterJoinNulls,
                currentOuterJoinHasNulls,
                probeStart,
                probeCount,
                outputOuterPositions,
                outputInnerLogicalPositions,
                outputPosition);
        currentOuterMaskIndex += probeCount;
        outerRemaining -= probeCount;
        return emitted;
    }

    private void prepareOuterProbeChunk()
    {
        long start = System.nanoTime();
        preparedOuterCount = Math.min(currentOuterMask.count() - currentOuterMaskIndex, BATCH_SIZE);
        preparedOuterIndex = 0;
        singleMatchProbe = joinIndex.supportsSingleMatchRefs();
        singleMatchPositionProbe = singleMatchProbe && !probeOuterJoin && joinIndex.supportsSingleMatchPositions();
        preparedOuterRange = singleMatchPositionProbe && currentOuterMask.all() && joinIndex.supportsSingleMatchPositionRange();
        if (preparedOuterRange) {
            preparedOuterRangeStart = currentOuterMaskIndex;
            currentOuterMaskIndex += preparedOuterCount;
        }
        else {
            for (int index = 0; index < preparedOuterCount; index++) {
                preparedOuterPositions[index] = currentOuterMask.position(currentOuterMaskIndex++);
            }
        }
        compactSingleMatchProbe = singleMatchProbe && !singleMatchPositionProbe && joinIndex.supportsCompactSingleMatchRefs();
        if (preparedOuterRange) {
            joinIndex.matchSingleRowsPositionsRange(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls, preparedOuterRangeStart, preparedOuterCount, preparedSingleRefs32);
        }
        else if (singleMatchPositionProbe) {
            joinIndex.matchSingleRowsPositions(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls, preparedOuterPositions, preparedOuterCount, preparedSingleRefs32);
        }
        else if (compactSingleMatchProbe) {
            joinIndex.matchSingleRowsCompact(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls, preparedOuterPositions, preparedOuterCount, preparedSingleRefs32);
        }
        else if (singleMatchProbe) {
            joinIndex.matchSingleRows(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls, preparedOuterPositions, preparedOuterCount, preparedSingleRefs);
        }
        else {
            joinIndex.matchRows(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls, preparedOuterPositions, preparedOuterCount, preparedOuterMatches, preparedSingleMatches());
        }
        cacheOrderedInnerFilterPayload();
    }

    private SingleLongList[] preparedSingleMatches()
    {
        if (preparedSingleMatches == null) {
            preparedSingleMatches = createSingleLongLists(BATCH_SIZE);
        }
        return preparedSingleMatches;
    }

    private String genericProbeKind()
    {
        return switch (joinIndex) {
            case FlatJoinIndex _ -> "flat";
            case LongPairJoinIndex _ -> "pair";
            case LongTripleJoinIndex _ -> "triple";
            case ObjectJoinIndex _ -> "object";
            case null, default -> "object";
        };
    }

    private boolean loadNextOuterBatch()
    {
        // The previous probe batch has been fully consumed before this method is reached. Closing it releases lazy
        // projection results and evaluator scratch back to their shared pools before the source advances. Without
        // this boundary, ProjectOperator loses its old BatchState on next() and retains one full set of vectors per
        // input batch until query teardown.
        if (currentOuterBatch != null) {
            currentOuterBatch.close();
            currentOuterBatch = null;
        }
        while (probeSource.hasNext()) {
            currentOuterBatch = probeSource.next();
            // Re-borrow is a property of the current open batch, not necessarily of every batch a source can emit.
            // A scan may offer it only for a selective filtered window that remains wholly live in one batch.
            outerSupportsReborrow = probeSource.supportsConstrainedReborrow();
            // When the outer can satisfy a constrained re-borrow, skip eager outer schema capture:
            // it borrows a representative VALUES vector for every outer column, forcing lazy
            // projected payloads to materialize. The schema is then derived on demand in
            // outputSchema() from currentOuterBatch. When the outer's reader advances irreversibly,
            // capture the schema eagerly now while the batch is live (deferring past the advance
            // would read an already-advanced source).
            if (!outerSupportsReborrow) {
                BufferedJoinInput.captureSchema(currentOuterBatch, outerSchema);
            }
            currentOuterMask = currentOuterBatch.borrowMask();
            if (!currentOuterMask.none()) {
                cacheOuterJoinInputs();
                cacheOuterFilterInputs();
                currentOuterMaskIndex = 0;
                outerRemaining = currentOuterMask.count();
                currentOuterPositionReady = false;
                preparedOuterCount = 0;
                preparedOuterIndex = 0;
                return true;
            }
        }
        return false;
    }

    private LongList matchesForOuterPosition()
    {
        return matchesForOuterPosition(currentOuterPosition);
    }

    private LongList matchesForOuterPosition(int outerPosition)
    {
        if (joinIndex == null) {
            return LongLists.emptyList();
        }
        String operatorName = profileName != null ? profileName : "hash_join";
        String probeKind = genericProbeKind();
        if (!currentOuterJoinHasNulls) {
            return joinIndex.matchesNoNulls(currentOuterJoinValues, outerPosition);
        }
        return joinIndex.matches(currentOuterJoinValues, currentOuterJoinNulls, outerPosition);
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        // Forward a downstream join's filter on a probe-side output column toward the probe source. This join's
        // projected public ordinal is first remapped to the physical concatenated join output.
        if (filter.column() < 0 || filter.column() >= outputChannels.length) {
            return;
        }
        int physicalColumn = outputChannels[filter.column()];
        if (physicalColumn < outerOutputCount) {
            outer.pushDynamicFilter(filter.withColumn(physicalColumn));
        }
    }

    /** Once the build side is materialized, push this join's own key membership down to the probe. Runs once. */
    private void pushDynamicFilterIfReady()
    {
        if (dynamicFilterPushed || !buildKeysViable) {
            return;
        }
        dynamicFilterPushed = true;
        if (DEBUG_DYNAMIC_FILTER) {
            System.err.printf("[dynamic-filter] join=%s viable=%s abandoned=%s keys=%s collected=%d%n",
                    profileName, buildKeysViable, buildKeysAbandoned,
                    buildKeyValues == null ? "null" : java.util.Arrays.toString(java.util.Arrays.stream(buildKeyValues)
                            .mapToInt(values -> values == null ? -1 : values.size()).toArray()),
                    collectedBuildKeyCount);
        }
        if (buildKeysAbandoned) {
            // A large single-long build may still have an exact bounded-range membership bitset owned by its join
            // index. Share that immutable representation with the probe scan instead of rebuilding a huge hash set.
            if (SHARE_SPARSE_LONG_RANGE_FILTER && innerJoinColumns.length == 1 && joinIndex instanceof LongJoinIndex longIndex) {
                DynamicFilter filter = longIndex.sparseDynamicFilter(outerJoinColumns[0]);
                if (filter != null) {
                    outer.pushDynamicFilter(filter);
                }
            }
            return;
        }
        if (collectedBuildKeys != null) {
            if (collectedBuildKeyCount > 0) {
                outer.pushDynamicFilter(DynamicFilter.fromCollectedValues(
                        outerJoinColumns[0],
                        collectedBuildKeys,
                        collectedBuildKeyCount,
                        buildKeyMins[0],
                        buildKeyMaxs[0]));
            }
            releaseCollectedBuildKeys();
            return;
        }
        if (buildKeyValues == null) {
            return;
        }
        for (int column = 0; column < buildKeyValues.length; column++) {
            if (!buildKeyColumnAbandoned[column] && buildKeyValues[column] != null && !buildKeyValues[column].isEmpty()) {
                DynamicFilter filter = TRACK_DYNAMIC_FILTER_VALUE_RANGE && buildKeyMins != null
                        ? DynamicFilter.fromValues(outerJoinColumns[column], buildKeyValues[column], buildKeyMins[column], buildKeyMaxs[column])
                        : DynamicFilter.fromValues(outerJoinColumns[column], buildKeyValues[column]);
                outer.pushDynamicFilter(filter);
            }
        }
    }

    private void prepareProbeFirstBuildFilter()
    {
        if (!PROBE_FIRST_BUILD_FILTER ||
                probeOuterJoin ||
                !supportsInnerDynamicFilterPushdown() ||
                inner.exactOutputRows() < PROBE_FIRST_MIN_BUILD_ROWS) {
            return;
        }
        ProbeSpool spool = new ProbeSpool(allocator, outer, outerOutputCount);
        probeSource = spool;
        it.unimi.dsi.fastutil.longs.LongSet[] values = spool.prepare(outerJoinColumns, PROBE_FIRST_MAX_PROBE_ROWS);
        if (DEBUG_DYNAMIC_FILTER) {
            System.err.printf("[probe-first-build-filter] join=%s probeRows=%d complete=%s keySizes=%s%n",
                    profileName != null ? profileName : "hash_join",
                    spool.bufferedRows(),
                    values != null,
                    values == null ? "-" : java.util.Arrays.toString(java.util.Arrays.stream(values).mapToInt(it.unimi.dsi.fastutil.longs.LongSet::size).toArray()));
        }
        if (values == null) {
            return;
        }
        for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
            inner.pushDynamicFilter(DynamicFilter.fromValues(innerJoinColumns[keyIndex], values[keyIndex]));
        }
    }

    private boolean supportsInnerDynamicFilterPushdown()
    {
        for (int column : innerJoinColumns) {
            if (!inner.supportsDynamicFilterPushdown(column)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Record one build row's join-key values into the per-column membership sets. A row with any null key can never
     * match an inner join, so it contributes nothing. Called once per build row while collection is still viable.
     */
    private void collectBuildKeys(VectorAccess.LongValues[] joinValues, Vector[] joinNulls, boolean hasNulls, int position)
    {
        if (buildKeysAbandoned) {
            return;
        }
        int keyCount = joinValues.length;
        if (usesCollectedBuildKeys(keyCount)) {
            collectBuildKey(joinValues[0], joinNulls[0], hasNulls, position);
            return;
        }
        if (buildKeyValues == null) {
            buildKeyValues = new LongOpenHashSet[keyCount];
            buildKeyColumnAbandoned = new boolean[keyCount];
            initializeBuildKeyRanges(keyCount);
        }
        if (hasNulls) {
            for (int column = 0; column < keyCount; column++) {
                if (joinNulls[column] != null && VectorAccess.isNull(joinNulls[column], position)) {
                    return;   // any null key => this row cannot join; omit all its values
                }
            }
        }
        boolean allAbandoned = true;
        for (int column = 0; column < keyCount; column++) {
            if (buildKeyColumnAbandoned[column]) {
                continue;
            }
            long value = joinValues[column].value(position);
            if (buildKeyValues[column] == null) {
                int expectedValues = PRE_SIZE_DYNAMIC_FILTER_VALUE_SETS
                        ? Math.min(expectedInnerRowCount(), DYNAMIC_FILTER_MAX_VALUES + 1)
                        : 16;
                buildKeyValues[column] = new LongOpenHashSet(expectedValues);
            }
            buildKeyValues[column].add(value);
            if (TRACK_DYNAMIC_FILTER_VALUE_RANGE) {
                buildKeyMins[column] = Math.min(buildKeyMins[column], value);
                buildKeyMaxs[column] = Math.max(buildKeyMaxs[column], value);
            }
            if (buildKeyValues[column].size() > DYNAMIC_FILTER_MAX_VALUES) {
                buildKeyColumnAbandoned[column] = true;   // not selective enough to be worth a runtime filter
                buildKeyValues[column] = null;
            }
            else if (column == 0 && keyCount == 1) {
                promoteBuildKeySetToCollectedIfReady();
            }
            allAbandoned &= buildKeyColumnAbandoned[column];
        }
        if (allAbandoned) {
            buildKeysAbandoned = true;   // every column dropped: stop collecting entirely
        }
    }

    private boolean usesCollectedBuildKeys(int keyCount)
    {
        return DEFER_DYNAMIC_FILTER_DEDUPLICATION && keyCount == 1 && collectedBuildKeyAdmission > 0;
    }

    private void promoteBuildKeySetToCollectedIfReady()
    {
        if (!DEFER_DYNAMIC_FILTER_DEDUPLICATION ||
                DYNAMIC_FILTER_BUILD_ROW_LIMIT > DYNAMIC_FILTER_MAX_VALUES ||
                collectedBuildKeyAdmission != 0 ||
                buildKeyValues[0].size() < DEFER_DYNAMIC_FILTER_MIN_ROWS) {
            return;
        }
        long span = buildKeyMaxs[0] - buildKeyMins[0] + 1;
        double density = span <= 0 ? 1 : (double) buildKeyValues[0].size() / span;
        boolean admitted = density <= DEFER_DYNAMIC_FILTER_MAX_DENSITY;
        if (DEBUG_DYNAMIC_FILTER) {
            System.err.printf("[deferred-dynamic-filter-admission] join=%s distinct=%d span=%d density=%.6f admitted=%s%n",
                    profileName,
                    buildKeyValues[0].size(),
                    span,
                    density,
                    admitted);
        }
        if (!admitted) {
            collectedBuildKeyAdmission = -1;
            return;
        }
        int expectedValues = Math.min(DYNAMIC_FILTER_MAX_VALUES, Math.max(expectedInnerRowCount(), buildKeyValues[0].size()));
        collectedBuildKeys = arrayPool.borrowLongs(expectedValues);
        for (long value : buildKeyValues[0]) {
            collectedBuildKeys[collectedBuildKeyCount++] = value;
        }
        buildKeyValues[0] = null;
        collectedBuildKeyAdmission = 1;
    }

    private void collectBuildKey(VectorAccess.LongValues joinValues, Vector joinNulls, boolean hasNulls, int position)
    {
        if (hasNulls && joinNulls != null && VectorAccess.isNull(joinNulls, position)) {
            return;
        }
        if (collectedBuildKeys == null) {
            int expectedValues = Math.min(expectedInnerRowCount(), DYNAMIC_FILTER_MAX_VALUES);
            collectedBuildKeys = arrayPool.borrowLongs(Math.max(16, expectedValues));
            initializeBuildKeyRanges(1);
        }
        if (collectedBuildKeyCount == collectedBuildKeys.length) {
            int newLength = Math.min(DYNAMIC_FILTER_MAX_VALUES, Math.multiplyExact(collectedBuildKeys.length, 2));
            if (newLength == collectedBuildKeys.length) {
                buildKeysAbandoned = true;
                releaseCollectedBuildKeys();
                return;
            }
            long[] expanded = arrayPool.borrowLongs(newLength);
            System.arraycopy(collectedBuildKeys, 0, expanded, 0, collectedBuildKeyCount);
            arrayPool.release(collectedBuildKeys);
            collectedBuildKeys = expanded;
        }
        long value = joinValues.value(position);
        collectedBuildKeys[collectedBuildKeyCount++] = value;
        buildKeyMins[0] = Math.min(buildKeyMins[0], value);
        buildKeyMaxs[0] = Math.max(buildKeyMaxs[0], value);
    }

    private void releaseCollectedBuildKeys()
    {
        arrayPool.release(collectedBuildKeys);
        collectedBuildKeys = null;
        collectedBuildKeyCount = 0;
    }

    private void initializeBuildKeyRanges(int keyCount)
    {
        if (!TRACK_DYNAMIC_FILTER_VALUE_RANGE) {
            return;
        }
        buildKeyMins = arrayPool.borrowLongs(keyCount);
        buildKeyMaxs = arrayPool.borrowLongs(keyCount);
        Arrays.fill(buildKeyMins, Long.MAX_VALUE);
        Arrays.fill(buildKeyMaxs, Long.MIN_VALUE);
    }

    private void loadInnerIfNecessary()
    {
        if (canStreamUnusedBuildPayload()) {
            loadStreamingInner();
            return;
        }
        int batchCountBefore = bufferedInner.batches().size();
        BufferedJoinInput.BatchMaskPruner maskPruner = PRUNE_ZERO_BITWISE_OVERLAP_BUILD_ROWS && singleLongBitwiseOverlapJoinFilter
                ? this::pruneZeroBitwiseOverlapBuildMask
                : null;
        bufferedInner.loadAll(
                inner,
                BUILD_BATCH_SIZE,
                innerJoinColumns,
                inner.supportsRetainedBatches(),
                !inner.supportsRetainedBatches() && inner.supportsConstrainedReborrow(),
                maskPruner);
        ensureRetainedConstraintCacheCapacity(bufferedInner.batches().size());
        copySchema(bufferedInner.schema(), innerSchema);
        cacheInnerFilterInputs();
        if (PRUNE_ZERO_BITWISE_OVERLAP_BUILD_ROWS && singleLongBitwiseOverlapJoinFilter) {
            expectedIndexedInnerRows = (int) Math.min(Integer.MAX_VALUE, bufferedInner.rowCount());
        }
        // A dynamic filter caps at DYNAMIC_FILTER_MAX_VALUES distinct build values. If the build side alone has more
        // rows than that, its key membership set will either overflow the cap (and be abandoned) or — for a rare
        // low-cardinality key — yield a value set so large the probe scan discards it as non-selective. Either way the
        // per-row set insertion is wasted, and it dominates large-build joins (TPC-DS q84). Skip collection up front.
        // Correctness is unaffected: the join still enforces the condition; the filter is a pure decode-pruning hint.
        if (buildKeysViable && !buildKeysAbandoned) {
            long totalInnerRows = 0;
            for (BufferedJoinInput.InnerBatch batch : bufferedInner.batches()) {
                totalInnerRows += batch.length();
            }
            if (totalInnerRows > DYNAMIC_FILTER_BUILD_ROW_LIMIT) {
                buildKeysAbandoned = true;
            }
        }
        for (int batchIndex = batchCountBefore; batchIndex < bufferedInner.batches().size(); batchIndex++) {
            BufferedJoinInput.InnerBatch batch = bufferedInner.batches().get(batchIndex);
            indexInnerRows(batch, 0, batch.length(), batchIndex);
        }
    }

    private void pruneZeroBitwiseOverlapBuildMask(Batch batch, Mask mask)
    {
        Output output = batch.output(joinFilters[0].innerColumn());
        Vector values = output.borrow(Stream.VALUES);
        Vector nulls = output.borrowOrNull(Stream.NULLS);
        if (values instanceof I64Vector longs && (nulls == null || nulls instanceof BooleanVector)) {
            mask.retainConstantComparison(
                    longs.values(),
                    0,
                    Mask.ComparisonOperator.NOT_EQUAL,
                    nulls == null ? null : ((BooleanVector) nulls).values());
            return;
        }
        if (values instanceof I32Vector ints && (nulls == null || nulls instanceof BooleanVector)) {
            mask.retainConstantComparison(
                    ints.values(),
                    0,
                    Mask.ComparisonOperator.NOT_EQUAL,
                    nulls == null ? null : ((BooleanVector) nulls).values());
            return;
        }
        VectorAccess.LongValues longValues = VectorAccess.longValues(values);
        VectorAccess.BooleanValues nullValues = nulls == null ? null : VectorAccess.booleanValues(nulls);
        mask.retainIf(position -> (nullValues == null || !nullValues.value(position)) && longValues.value(position) != 0);
    }

    private boolean canStreamUnusedBuildPayload()
    {
        if (!STREAM_UNUSED_BUILD_PAYLOAD || joinFilters.length != 0) {
            return false;
        }
        for (int outputChannel : outputChannels) {
            if (outputChannel >= outerOutputCount) {
                return false;
            }
        }
        return true;
    }

    private void loadStreamingInner()
    {
        if (streamedInnerLoaded) {
            return;
        }
        streamedInnerLoaded = true;
        int batchIndex = 0;
        while (inner.hasNext()) {
            try (Batch batch = inner.next()) {
                BufferedJoinInput.captureSchema(batch, innerSchema);
                Mask mask = batch.borrowMask();
                if (mask.none()) {
                    continue;
                }
                Vector[] joinValues = new Vector[innerJoinColumns.length];
                Vector[] joinNulls = new Vector[innerJoinColumns.length];
                boolean hasNulls = false;
                for (int keyIndex = 0; keyIndex < innerJoinColumns.length; keyIndex++) {
                    Output output = batch.output(innerJoinColumns[keyIndex]);
                    joinValues[keyIndex] = output.borrow(Stream.VALUES);
                    joinNulls[keyIndex] = output.borrowOrNull(Stream.NULLS);
                    hasNulls |= joinNulls[keyIndex] != null && !VectorAccess.isAllFalseNulls(joinNulls[keyIndex]);
                }
                if (joinIndex == null) {
                    if (joinValues.length == 1 && isSingleLongJoinCandidate(joinValues[0])) {
                        joinIndex = new LongJoinIndex(arrayPool, Math.max(16, mask.count()), true, true, true, lazyDuplicateSlotState, false, false, true);
                    }
                    else {
                        joinIndex = createJoinIndex(joinValues, false, true, false);
                    }
                }
                streamedInnerRows += mask.count();
                if (buildKeysViable && streamedInnerRows > DYNAMIC_FILTER_BUILD_ROW_LIMIT) {
                    buildKeysAbandoned = true;
                    releaseCollectedBuildKeys();
                    buildKeyValues = null;
                }
                boolean collectKeys = buildKeysViable && !buildKeysAbandoned;
                VectorAccess.LongValues[] buildKeyAccessors = null;
                if (collectKeys) {
                    buildKeyAccessors = new VectorAccess.LongValues[joinValues.length];
                    for (int keyIndex = 0; keyIndex < joinValues.length; keyIndex++) {
                        try {
                            buildKeyAccessors[keyIndex] = VectorAccess.longValues(joinValues[keyIndex]);
                        }
                        catch (IllegalArgumentException _) {
                            buildKeysAbandoned = true;
                            releaseCollectedBuildKeys();
                            buildKeyValues = null;
                            collectKeys = false;
                            break;
                        }
                    }
                }
                if (BATCH_SINGLE_LONG_BUILD && !collectKeys && joinIndex instanceof LongJoinIndex longJoinIndex) {
                    longJoinIndex.addRows(joinValues[0], joinNulls[0], hasNulls, mask, batchIndex++);
                    continue;
                }
                if (BATCH_LONG_PAIR_BUILD && !collectKeys && joinIndex instanceof LongPairJoinIndex longPairJoinIndex) {
                    longPairJoinIndex.addRows(joinValues, joinNulls, hasNulls, mask, batchIndex++);
                    continue;
                }
                for (int logicalPosition = 0; logicalPosition < mask.count(); logicalPosition++) {
                    int sourcePosition = mask.all() ? logicalPosition : mask.position(logicalPosition);
                    long rowReference = packRowReference(batchIndex, logicalPosition);
                    if (hasNulls) {
                        joinIndex.add(joinValues, joinNulls, sourcePosition, rowReference);
                    }
                    else {
                        joinIndex.addNoNulls(joinValues, sourcePosition, rowReference);
                    }
                    if (collectKeys) {
                        collectBuildKeys(buildKeyAccessors, joinNulls, hasNulls, sourcePosition);
                        collectKeys = !buildKeysAbandoned;
                    }
                }
                batchIndex++;
            }
        }
    }

    private void cacheOuterFilterInputs()
    {
        if (promotedBinaryEqualityFilter) {
            return;
        }
        for (int index = 0; index < joinFilters.length; index++) {
            Output output = currentOuterBatch.output(joinFilters[index].outerColumn());
            currentOuterFilterValues[index] = output.borrow(Stream.VALUES);
            currentOuterFilterNulls[index] = output.borrowOrNull(Stream.NULLS);
            if (currentOuterFilterNulls[index] != null && VectorAccess.isAllFalseNulls(currentOuterFilterNulls[index])) {
                currentOuterFilterNulls[index] = null;
            }
            currentOuterFilterNullAccess[index] = currentOuterFilterNulls[index] == null ? null : VectorAccess.booleanValues(currentOuterFilterNulls[index]);
            currentOuterFilterBaseValues[index] = baseValues(currentOuterFilterValues[index]);
            currentOuterFilterDictionaryDepths[index] = dictionaryDepth(currentOuterFilterValues[index]);
        }
        if (singleEncodedBinaryJoinFilter) {
            fastOuterFilterDictionary = currentOuterFilterValues[0] instanceof DictionaryVector dictionary ? dictionary : null;
            fastOuterFilterBase = (BinaryVector) currentOuterFilterBaseValues[0];
            fastOuterFilterDepth = currentOuterFilterDictionaryDepths[0];
            fastOuterFilterNulls = currentOuterFilterNullAccess[0];
        }
        else if (singleLongNotEqualJoinFilter || singleLongBitwiseOverlapJoinFilter) {
            fastOuterFilterLongs = VectorAccess.longValues(currentOuterFilterValues[0]);
            fastOuterFilterLongArray = currentOuterFilterValues[0] instanceof I64Vector values ? values.values() : null;
            fastOuterFilterNulls = currentOuterFilterNullAccess[0];
        }
    }

    private void cacheInnerFilterInputs()
    {
        if (joinFilters.length == 0 || promotedBinaryEqualityFilter || innerFilterValues != null) {
            return;
        }
        int batchCount = bufferedInner.batches().size();
        innerFilterValues = new Vector[joinFilters.length][batchCount];
        innerFilterNulls = new Vector[joinFilters.length][batchCount];
        innerFilterNullAccess = new VectorAccess.BooleanValues[joinFilters.length][batchCount];
        innerFilterBaseValues = new Vector[joinFilters.length][batchCount];
        innerFilterDictionaryDepths = new int[joinFilters.length][batchCount];
        for (int filterIndex = 0; filterIndex < joinFilters.length; filterIndex++) {
            int column = joinFilters[filterIndex].innerColumn();
            for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
                BufferedJoinInput.InnerBatch batch = bufferedInner.batches().get(batchIndex);
                if (batch.retained()) {
                    Output output = batch.retainedBatch().output(column);
                    innerFilterValues[filterIndex][batchIndex] = output.borrow(Stream.VALUES);
                    innerFilterNulls[filterIndex][batchIndex] = output.borrowOrNull(Stream.NULLS);
                }
                else {
                    Streams streams = batch.columns()[column];
                    innerFilterValues[filterIndex][batchIndex] = streams.values();
                    innerFilterNulls[filterIndex][batchIndex] = streams.getOrNull(Stream.NULLS);
                }
                if (innerFilterNulls[filterIndex][batchIndex] != null && VectorAccess.isAllFalseNulls(innerFilterNulls[filterIndex][batchIndex])) {
                    innerFilterNulls[filterIndex][batchIndex] = null;
                }
                innerFilterNullAccess[filterIndex][batchIndex] = innerFilterNulls[filterIndex][batchIndex] == null
                        ? null
                        : VectorAccess.booleanValues(innerFilterNulls[filterIndex][batchIndex]);
                innerFilterBaseValues[filterIndex][batchIndex] = baseValues(innerFilterValues[filterIndex][batchIndex]);
                innerFilterDictionaryDepths[filterIndex][batchIndex] = dictionaryDepth(innerFilterValues[filterIndex][batchIndex]);
            }
        }
        if (singleEncodedBinaryJoinFilter && batchCount == 1) {
            fastInnerFilterBatch = bufferedInner.batches().getFirst();
            fastInnerFilterPositions = fastInnerFilterBatch.positions();
            fastInnerFilterDictionary = innerFilterValues[0][0] instanceof DictionaryVector dictionary ? dictionary : null;
            fastInnerFilterBase = (BinaryVector) innerFilterBaseValues[0][0];
            fastInnerFilterDepth = innerFilterDictionaryDepths[0][0];
            fastInnerFilterNulls = innerFilterNullAccess[0][0];
        }
        else if ((singleLongNotEqualJoinFilter || singleLongBitwiseOverlapJoinFilter) && batchCount == 1) {
            fastInnerFilterBatch = bufferedInner.batches().getFirst();
            fastInnerFilterPositions = fastInnerFilterBatch.positions();
            fastInnerFilterLongs = VectorAccess.longValues(innerFilterValues[0][0]);
            fastInnerFilterLongArray = innerFilterValues[0][0] instanceof I64Vector values ? values.values() : null;
            fastInnerFilterNulls = innerFilterNullAccess[0][0];
        }
    }

    private boolean passesJoinFilters(int outerPosition, long rowReference, int matchIndex)
    {
        if (joinFilters.length == 0) {
            return true;
        }
        if (fastInnerFilterBatch != null) {
            if ((singleLongNotEqualJoinFilter || singleLongBitwiseOverlapJoinFilter) &&
                    fastInnerOrderedIntFilterValues != null &&
                    currentMatches instanceof ChainLongList chain) {
                int storageIndex = chain.storageIndex(matchIndex);
                if (storageIndex >= 0) {
                    boolean outerNull = CACHE_CURRENT_OUTER_JOIN_FILTER
                            ? currentFastOuterFilterNull
                            : fastOuterFilterNulls != null && fastOuterFilterNulls.value(outerPosition);
                    long outerValue = CACHE_CURRENT_OUTER_JOIN_FILTER
                            ? currentFastOuterFilterLong
                            : fastOuterFilterLongArray == null
                                    ? fastOuterFilterLongs.value(outerPosition)
                                    : fastOuterFilterLongArray[outerPosition];
                    long innerValue = fastInnerOrderedIntFilterValues[storageIndex];
                    return !outerNull && (singleLongNotEqualJoinFilter
                            ? outerValue != innerValue
                            : (outerValue & innerValue) != 0);
                }
            }
            return singleEncodedBinaryJoinFilter
                    ? passesFastBinaryJoinFilter(outerPosition, rowPosition(rowReference))
                    : passesFastLongJoinFilter(outerPosition, rowPosition(rowReference));
        }
        int batchIndex = batchIndex(rowReference);
        BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex);
        int innerPosition = innerBatch.sourcePosition(rowPosition(rowReference));
        if (joinFilters.length == 1 && joinFilters[0].encodedBinaryEquals()) {
            if ((currentOuterFilterNulls[0] != null && VectorAccess.isNull(currentOuterFilterNulls[0], outerPosition)) ||
                    (innerFilterNulls[0][batchIndex] != null && VectorAccess.isNull(innerFilterNulls[0][batchIndex], innerPosition))) {
                return false;
            }
            BinaryVector outerBase = (BinaryVector) currentOuterFilterBaseValues[0];
            BinaryVector innerBase = (BinaryVector) innerFilterBaseValues[0][batchIndex];
            int outerBasePosition = basePosition(currentOuterFilterValues[0], outerPosition, currentOuterFilterDictionaryDepths[0]);
            int innerBasePosition = basePosition(innerFilterValues[0][batchIndex], innerPosition, innerFilterDictionaryDepths[0][batchIndex]);
            int length = outerBase.length(outerBasePosition);
            return length == innerBase.length(innerBasePosition) && OperatorVectorSupport.binaryEquals(
                    outerBase.data(), outerBase.startOffset(outerBasePosition),
                    innerBase.data(), innerBase.startOffset(innerBasePosition),
                    length);
        }
        for (int index = 0; index < joinFilters.length; index++) {
            if ((currentOuterFilterNullAccess[index] != null && currentOuterFilterNullAccess[index].value(outerPosition)) ||
                    (innerFilterNullAccess[index][batchIndex] != null && innerFilterNullAccess[index][batchIndex].value(innerPosition))) {
                return false;
            }
            boolean matches = joinFilters[index].encodedBinaryEquals()
                    ? OperatorVectorSupport.binaryEquals(
                            currentOuterFilterBaseValues[index],
                            basePosition(currentOuterFilterValues[index], outerPosition, currentOuterFilterDictionaryDepths[index]),
                            innerFilterBaseValues[index][batchIndex],
                            basePosition(innerFilterValues[index][batchIndex], innerPosition, innerFilterDictionaryDepths[index][batchIndex]))
                    : joinFilters[index].function().test(currentOuterFilterValues[index], outerPosition, innerFilterValues[index][batchIndex], innerPosition);
            if (!matches) {
                return false;
            }
        }
        return true;
    }

    private boolean passesBinaryJoinFilter(int outerPosition, long rowReference)
    {
        if (fastInnerFilterBatch != null) {
            return passesFastBinaryJoinFilter(outerPosition, rowPosition(rowReference));
        }
        int batchIndex = batchIndex(rowReference);
        BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex);
        int innerPosition = innerBatch.sourcePosition(rowPosition(rowReference));
        if ((currentOuterFilterNulls[0] != null && VectorAccess.isNull(currentOuterFilterNulls[0], outerPosition)) ||
                (innerFilterNulls[0][batchIndex] != null && VectorAccess.isNull(innerFilterNulls[0][batchIndex], innerPosition))) {
            return false;
        }
        BinaryVector outerBase = (BinaryVector) currentOuterFilterBaseValues[0];
        BinaryVector innerBase = (BinaryVector) innerFilterBaseValues[0][batchIndex];
        int outerBasePosition = basePosition(currentOuterFilterValues[0], outerPosition, currentOuterFilterDictionaryDepths[0]);
        int innerBasePosition = basePosition(innerFilterValues[0][batchIndex], innerPosition, innerFilterDictionaryDepths[0][batchIndex]);
        int length = outerBase.length(outerBasePosition);
        return length == innerBase.length(innerBasePosition) && OperatorVectorSupport.binaryEquals(
                outerBase.data(), outerBase.startOffset(outerBasePosition),
                innerBase.data(), innerBase.startOffset(innerBasePosition),
                length);
    }

    private boolean passesFastBinaryJoinFilter(int outerPosition, int innerLogicalPosition)
    {
        int innerPosition = fastInnerFilterPositions == null ? innerLogicalPosition : fastInnerFilterPositions[innerLogicalPosition];
        boolean outerNull = CACHE_CURRENT_OUTER_JOIN_FILTER
                ? currentFastOuterFilterNull
                : fastOuterFilterNulls != null && fastOuterFilterNulls.value(outerPosition);
        if (outerNull ||
                (fastInnerFilterNulls != null && fastInnerFilterNulls.value(innerPosition))) {
            return false;
        }
        int outerBasePosition = CACHE_CURRENT_OUTER_JOIN_FILTER
                ? currentFastOuterFilterBasePosition
                : fastOuterFilterDictionary == null
                        ? outerPosition
                        : fastOuterFilterDictionary.basePosition(outerPosition, fastOuterFilterDepth);
        int innerBasePosition = fastInnerFilterDictionary == null
                ? innerPosition
                : fastInnerFilterDictionary.basePosition(innerPosition, fastInnerFilterDepth);
        int length = fastOuterFilterBase.length(outerBasePosition);
        return length == fastInnerFilterBase.length(innerBasePosition) && OperatorVectorSupport.binaryEquals(
                fastOuterFilterBase.data(), fastOuterFilterBase.startOffset(outerBasePosition),
                fastInnerFilterBase.data(), fastInnerFilterBase.startOffset(innerBasePosition),
                length);
    }

    private boolean passesFastLongJoinFilter(int outerPosition, int innerLogicalPosition)
    {
        int innerPosition = fastInnerFilterPositions == null ? innerLogicalPosition : fastInnerFilterPositions[innerLogicalPosition];
        boolean outerNull = CACHE_CURRENT_OUTER_JOIN_FILTER
                ? currentFastOuterFilterNull
                : fastOuterFilterNulls != null && fastOuterFilterNulls.value(outerPosition);
        if (outerNull ||
                (fastInnerFilterNulls != null && fastInnerFilterNulls.value(innerPosition))) {
            return false;
        }
        long outerValue = CACHE_CURRENT_OUTER_JOIN_FILTER
                ? currentFastOuterFilterLong
                : fastOuterFilterLongArray == null
                        ? fastOuterFilterLongs.value(outerPosition)
                        : fastOuterFilterLongArray[outerPosition];
        long innerValue = fastInnerFilterLongArray == null ? fastInnerFilterLongs.value(innerPosition) : fastInnerFilterLongArray[innerPosition];
        return singleLongNotEqualJoinFilter ? outerValue != innerValue : (outerValue & innerValue) != 0;
    }

    private void cacheCurrentOuterFilterValue()
    {
        if (!CACHE_CURRENT_OUTER_JOIN_FILTER || fastInnerFilterBatch == null) {
            return;
        }
        currentFastOuterFilterNull = fastOuterFilterNulls != null && fastOuterFilterNulls.value(currentOuterPosition);
        if (currentFastOuterFilterNull) {
            return;
        }
        if (singleLongNotEqualJoinFilter || singleLongBitwiseOverlapJoinFilter) {
            currentFastOuterFilterLong = fastOuterFilterLongArray == null
                    ? fastOuterFilterLongs.value(currentOuterPosition)
                    : fastOuterFilterLongArray[currentOuterPosition];
        }
        else {
            currentFastOuterFilterBasePosition = fastOuterFilterDictionary == null
                    ? currentOuterPosition
                    : fastOuterFilterDictionary.basePosition(currentOuterPosition, fastOuterFilterDepth);
        }
    }

    private void cacheOrderedInnerFilterPayload()
    {
        if (fastInnerOrderedFilterAttempted ||
                !ORDERED_LONG_JOIN_FILTER_PAYLOAD ||
                (!singleLongNotEqualJoinFilter && !singleLongBitwiseOverlapJoinFilter) ||
                fastInnerFilterBatch == null ||
                fastInnerFilterNulls != null ||
                !(joinIndex instanceof LongJoinIndex longJoinIndex)) {
            return;
        }
        fastInnerOrderedFilterAttempted = true;
        fastInnerOrderedIntFilterValues = longJoinIndex.buildOrderedIntPayload(
                fastInnerFilterLongs,
                fastInnerFilterLongArray,
                fastInnerFilterPositions);
    }

    private static Vector baseValues(Vector vector)
    {
        return vector instanceof DictionaryVector dictionary ? dictionary.baseValues() : vector;
    }

    private static int dictionaryDepth(Vector vector)
    {
        return vector instanceof DictionaryVector dictionary ? dictionary.dictionaryDepth() : 0;
    }

    private static int basePosition(Vector vector, int position, int dictionaryDepth)
    {
        return vector instanceof DictionaryVector dictionary ? dictionary.basePosition(position, dictionaryDepth) : position;
    }

    private void indexInnerRows(BufferedJoinInput.InnerBatch batch, int startPosition, int length, int batchIndex)
    {
        int effectiveJoinKeyCount = innerJoinColumns.length + (promotedBinaryEqualityFilter ? 1 : 0);
        Vector[] joinValues = new Vector[effectiveJoinKeyCount];
        Vector[] joinNulls = new Vector[effectiveJoinKeyCount];
        boolean hasNulls = false;
        for (int keyIndex = 0; keyIndex < innerJoinColumns.length; keyIndex++) {
            if (batch.retained()) {
                Output output = batch.retainedBatch().output(innerJoinColumns[keyIndex]);
                joinValues[keyIndex] = output.borrow(Stream.VALUES);
                joinNulls[keyIndex] = output.borrowOrNull(Stream.NULLS);
            }
            else {
                Streams streams = batch.columns()[innerJoinColumns[keyIndex]];
                joinValues[keyIndex] = streams.values();
                joinNulls[keyIndex] = streams.getOrNull(Stream.NULLS);
            }
            // A present NULLS stream that is provably all-false (e.g. a non-nullable key surfaced through a
            // prior join's DictionaryVector/ConcatenatedBooleanVector) carries no nulls; treat the key as
            // null-free so the index build and probe skip per-row null reads.
            hasNulls = hasNulls || (joinNulls[keyIndex] != null && !VectorAccess.isAllFalseNulls(joinNulls[keyIndex]));
        }
        if (promotedBinaryEqualityFilter) {
            int keyIndex = innerJoinColumns.length;
            int column = joinFilters[0].innerColumn();
            if (batch.retained()) {
                Output output = batch.retainedBatch().output(column);
                joinValues[keyIndex] = output.borrow(Stream.VALUES);
                joinNulls[keyIndex] = output.borrowOrNull(Stream.NULLS);
            }
            else {
                Streams streams = batch.columns()[column];
                joinValues[keyIndex] = streams.values();
                joinNulls[keyIndex] = streams.getOrNull(Stream.NULLS);
            }
            hasNulls = hasNulls || (joinNulls[keyIndex] != null && !VectorAccess.isAllFalseNulls(joinNulls[keyIndex]));
        }
        if (joinIndex == null) {
            boolean capInitialLongHash = shouldCapInitialLongHash(batch, joinValues);
            joinIndex = createJoinIndex(
                    joinValues,
                    capInitialLongHash,
                    shouldUseGroupedLongHash(batch, joinValues),
                    shouldUseKeyOnlyDirectRangeBuild(batch, joinValues));
        }

        boolean collectKeys = buildKeysViable && !buildKeysAbandoned;
        VectorAccess.LongValues[] buildKeyAccessors = null;
        if (collectKeys) {
            if (buildKeyValues == null) {
                buildKeyValues = new LongOpenHashSet[innerJoinColumns.length];
                buildKeyColumnAbandoned = new boolean[innerJoinColumns.length];
                initializeBuildKeyRanges(innerJoinColumns.length);
            }
            buildKeyAccessors = new VectorAccess.LongValues[innerJoinColumns.length];
            for (int column = 0; column < innerJoinColumns.length; column++) {
                try {
                    buildKeyAccessors[column] = VectorAccess.longValues(joinValues[column]);
                }
                catch (IllegalArgumentException _) {
                    buildKeyColumnAbandoned[column] = true;
                    buildKeyValues[column] = null;
                }
            }
            buildKeysAbandoned = true;
            for (boolean abandoned : buildKeyColumnAbandoned) {
                buildKeysAbandoned &= abandoned;
            }
            collectKeys = !buildKeysAbandoned;
        }
        if (BATCH_SINGLE_LONG_BUILD && !collectKeys && joinIndex instanceof LongJoinIndex longJoinIndex) {
            longJoinIndex.addRows(joinValues[0], joinNulls[0], hasNulls, batch, startPosition, length, batchIndex);
            return;
        }
        if (BATCH_LONG_PAIR_BUILD && !collectKeys && joinIndex instanceof LongPairJoinIndex longPairJoinIndex) {
            longPairJoinIndex.addRows(joinValues, joinNulls, hasNulls, batch, startPosition, length, batchIndex);
            return;
        }
        for (int position = startPosition; position < startPosition + length; position++) {
            int sourcePosition = batch.sourcePosition(position);
            if (hasNulls) {
                joinIndex.add(joinValues, joinNulls, sourcePosition, packRowReference(batchIndex, position));
            }
            else {
                joinIndex.addNoNulls(joinValues, sourcePosition, packRowReference(batchIndex, position));
            }
            if (collectKeys) {
                collectBuildKeys(buildKeyAccessors, joinNulls, hasNulls, sourcePosition);
                collectKeys = !buildKeysAbandoned;
            }
        }
    }

    private boolean shouldCapInitialLongHash(BufferedJoinInput.InnerBatch batch, Vector[] joinValues)
    {
        if (CAP_DUPLICATE_PAIR_HASH &&
                joinValues.length == 2 &&
                isSingleLongJoinCandidate(joinValues[0]) &&
                isSingleLongJoinCandidate(joinValues[1]) &&
                expectedInnerRowCount() >= 1_000_000) {
            long capacity = 16;
            while (capacity < expectedInnerRowCount() / 0.75) {
                capacity <<= 1;
            }
            if (capacity * (2L * Long.BYTES + Byte.BYTES) <= MAX_INITIAL_PAIR_HASH_BYTES) {
                return false;
            }
            int sampleSize = Math.min(batch.length(), 4096);
            it.unimi.dsi.fastutil.longs.LongOpenHashSet distinct = new it.unimi.dsi.fastutil.longs.LongOpenHashSet(sampleSize);
            long firstMin = Long.MAX_VALUE;
            long firstMax = Long.MIN_VALUE;
            long secondMin = Long.MAX_VALUE;
            long secondMax = Long.MIN_VALUE;
            for (int position = 0; position < sampleSize; position++) {
                int sourcePosition = batch.sourcePosition(position);
                long first = OperatorVectorSupport.longValue(joinValues[0], sourcePosition);
                long second = OperatorVectorSupport.longValue(joinValues[1], sourcePosition);
                distinct.add(LongPairJoinIndex.hash64(first, second));
                firstMin = Math.min(firstMin, first);
                firstMax = Math.max(firstMax, first);
                secondMin = Math.min(secondMin, second);
                secondMax = Math.max(secondMax, second);
            }
            // The sample controls initial capacity only. Exact key equality and ordinary rehash growth preserve
            // correctness even in the vanishingly unlikely event of a sampled hash collision.
            if (sampleSize < 16) {
                return false;
            }
            if (distinct.size() <= sampleSize / 2) {
                return true;
            }
            long firstRange = firstMax - firstMin + 1;
            long secondRange = secondMax - secondMin + 1;
            long boundedDomain = expectedInnerRowCount() / 4L;
            boolean boundedPairDomain = firstRange > 0 && secondRange > 0 &&
                    firstRange <= boundedDomain / secondRange;
            // A large ordered prefix can look unique despite a bounded duplicate domain. Admit that case only when
            // the sampled Cartesian range is small; a wide unique pair build keeps the one-allocation presized path.
            return boundedPairDomain;
        }
        if (joinValues.length != 1 || innerSchema.length == innerJoinColumns.length || !isSingleLongJoinCandidate(joinValues[0])) {
            return false;
        }
        int expectedRows = expectedInnerRowCount();
        if (expectedRows >= 40_000_000) {
            return true;
        }
        int sampleSize = Math.min(batch.length(), 4096);
        it.unimi.dsi.fastutil.longs.LongOpenHashSet distinct = new it.unimi.dsi.fastutil.longs.LongOpenHashSet(sampleSize);
        long sampleMin = Long.MAX_VALUE;
        long sampleMax = Long.MIN_VALUE;
        for (int position = 0; position < sampleSize; position++) {
            long key = OperatorVectorSupport.longValue(joinValues[0], batch.sourcePosition(position));
            distinct.add(key);
            sampleMin = Math.min(sampleMin, key);
            sampleMax = Math.max(sampleMax, key);
        }
        if (distinct.size() <= sampleSize / 4) {
            return true;
        }
        // A random prefix of a bounded duplicate domain can look entirely unique. For a very large build, admit
        // bounded range state when the observed domain itself fits; exact fallback remains available if later keys
        // escape the ceiling. Wide-domain samples retain the ordinary pre-sized hash path.
        return expectedRows >= 10_000_000 && sampleMin >= 0 && sampleMax < LongJoinIndex.MAX_DIRECT_BUILD_KEY;
    }

    private boolean shouldUseGroupedLongHash(BufferedJoinInput.InnerBatch batch, Vector[] joinValues)
    {
        if (!LongJoinIndex.GROUPED_HASH_TABLE || joinValues.length != 1 || !isSingleLongJoinCandidate(joinValues[0])) {
            return false;
        }
        if (!LongJoinIndex.SPARSE_AWARE_HASH_LAYOUT) {
            return true;
        }
        if (expectedInnerRowCount() < LongJoinIndex.SPARSE_AWARE_SCALAR_MIN_ROWS) {
            return true;
        }
        int sampleSize = Math.min(batch.length(), 4096);
        if (sampleSize < 16) {
            return true;
        }
        long sampleMin = Long.MAX_VALUE;
        long sampleMax = Long.MIN_VALUE;
        for (int position = 0; position < sampleSize; position++) {
            long key = OperatorVectorSupport.longValue(joinValues[0], batch.sourcePosition(position));
            sampleMin = Math.min(sampleMin, key);
            sampleMax = Math.max(sampleMax, key);
        }
        long range = sampleMax - sampleMin + 1;
        // A bounded sparse domain receives an exact membership filter before probing. Surviving hash lookups are
        // consequently hits, where scalar linear probing is cheaper than loading and comparing a SIMD tag group.
        // Compare with the known full build cardinality, not sample cardinality: a random prefix of a dense table
        // spans most of its domain and would otherwise be misclassified as sparse (TPC-H q7 customer/orders).
        long expectedRows = expectedInnerRowCount();
        return range <= 0 || range > LongJoinIndex.MAX_ARRAY_RANGE || range < expectedRows * LongJoinIndex.SPARSE_RANGE_MIN_RATIO;
    }

    private boolean shouldUseKeyOnlyDirectRangeBuild(BufferedJoinInput.InnerBatch batch, Vector[] joinValues)
    {
        if (!LongJoinIndex.KEY_ONLY_DIRECT_RANGE_BUILD ||
                joinValues.length != 1 ||
                innerSchema.length != innerJoinColumns.length ||
                !isSingleLongJoinCandidate(joinValues[0])) {
            return false;
        }
        int expectedRows = expectedInnerRowCount();
        if (expectedRows < LongJoinIndex.KEY_ONLY_DIRECT_RANGE_MIN_ROWS) {
            return false;
        }
        int sampleSize = Math.min(batch.length(), 4096);
        if (sampleSize < 16) {
            return false;
        }
        long sampleMin = Long.MAX_VALUE;
        long sampleMax = Long.MIN_VALUE;
        for (int position = 0; position < sampleSize; position++) {
            long key = OperatorVectorSupport.longValue(joinValues[0], batch.sourcePosition(position));
            sampleMin = Math.min(sampleMin, key);
            sampleMax = Math.max(sampleMax, key);
        }
        long sampleRange = sampleMax - sampleMin + 1;
        // The range builder remains exact and can fall back, but a clearly sparse first batch would reserve and
        // randomly probe a much larger map than the ordinary hash table. Compare the observed domain with the known
        // full build cardinality so shuffled dense dimensions admit while wide sparse fact-key domains reject.
        return sampleMin >= 0 &&
                sampleMax < LongJoinIndex.MAX_DIRECT_BUILD_KEY &&
                sampleRange > 0 &&
                sampleRange <= 2L * expectedRows;
    }

    private void cacheOuterJoinInputs()
    {
        currentOuterJoinHasNulls = false;
        for (int keyIndex = 0; keyIndex < outerJoinColumns.length; keyIndex++) {
            Output output = currentOuterBatch.output(outerJoinColumns[keyIndex]);
            long start = System.nanoTime();
            currentOuterJoinValues[keyIndex] = output.borrow(Stream.VALUES);
            Vector keyNulls = output.isKnownAllFalse(Stream.NULLS) ? null : output.borrowOrNull(Stream.NULLS);
            // A present-but-all-false NULLS stream (e.g. a non-nullable key surfaced through a prior join's
            // DictionaryVector/ConcatenatedBooleanVector) carries no nulls: drop it so the probe takes the
            // null-free match path instead of reading a per-row null (a binary search for that shape).
            if (keyNulls != null && VectorAccess.isAllFalseNulls(keyNulls)) {
                keyNulls = null;
            }
            currentOuterJoinNulls[keyIndex] = keyNulls;
            currentOuterJoinHasNulls = currentOuterJoinHasNulls || keyNulls != null;
        }
        if (promotedBinaryEqualityFilter) {
            int keyIndex = outerJoinColumns.length;
            Output output = currentOuterBatch.output(joinFilters[0].outerColumn());
            currentOuterJoinValues[keyIndex] = output.borrow(Stream.VALUES);
            Vector keyNulls = output.isKnownAllFalse(Stream.NULLS) ? null : output.borrowOrNull(Stream.NULLS);
            if (keyNulls != null && VectorAccess.isAllFalseNulls(keyNulls)) {
                keyNulls = null;
            }
            currentOuterJoinNulls[keyIndex] = keyNulls;
            currentOuterJoinHasNulls = currentOuterJoinHasNulls || keyNulls != null;
        }
    }

    private void captureOuterSchemaIfAvailable()
    {
        while (probeSource.hasNext()) {
            try (Batch batch = probeSource.next()) {
                BufferedJoinInput.captureSchema(batch, outerSchema);
            }
        }
    }

    private JoinIndex createJoinIndex(
            Vector[] joinValues,
            boolean capInitialHash,
            boolean groupedLongHashTable,
            boolean keyOnlyDirectRangeBuild)
    {
        int expectedSize = expectedInnerRowCount();
        if (DEBUG_JOIN_INDEX) {
            System.err.printf("[join-index] expected=%d fields=%d shape=%s%n",
                    expectedSize,
                    joinValues.length,
                    java.util.Arrays.stream(joinValues).map(value -> value.getClass().getSimpleName() + '(' + value.length() + ')').toList());
        }
        if (joinValues.length == 1 && isSingleLongJoinCandidate(joinValues[0])) {
            // For a key-only build, duplicate rows have identical output values. Preserve their exact multiplicity
            // through the existing chains, but avoid eagerly copying every reference into CSR form before a probe
            // whose output may be tiny (q84 builds 2.9M rows and emits about 1.2K matches).
            return new LongJoinIndex(
                    arrayPool,
                    expectedSize,
                    innerSchema.length == innerJoinColumns.length,
                    capInitialHash,
                    groupedLongHashTable,
                    lazyDuplicateSlotState,
                    implicitSequentialBuildRowReferences,
                    keyOnlyDirectRangeBuild,
                    false);
        }
        if (joinValues.length == 2 && isSingleLongJoinCandidate(joinValues[0]) && isSingleLongJoinCandidate(joinValues[1])) {
            // A schema containing only join keys is not enough to prove the stored row reference is disposable:
            // callers may still project those inner keys.  Omit the reference only for the streaming shape, whose
            // output and residual-filter checks establish that no downstream consumer can observe inner payload.
            return new LongPairJoinIndex(arrayPool, expectedSize, canStreamUnusedBuildPayload(), capInitialHash);
        }
        if (joinValues.length == 3 && isSingleLongJoinCandidate(joinValues[0]) && isSingleLongJoinCandidate(joinValues[1]) && isSingleLongJoinCandidate(joinValues[2])) {
            return new LongTripleJoinIndex(arrayPool, expectedSize);
        }
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(
                joinValues,
                arrayPool,
                operatorResources.codeGeneration(),
                operatorResources.flatKeyTablePolicy());
        if (layout != null) {
            return new FlatJoinIndex(layout, expectedSize);
        }
        return new ObjectJoinIndex(joinValues.length);
    }

    @Override
    public void constrain(Mask mask)
    {
        currentOutputMask = mask;
    }

    @Override
    public void close()
    {
        if (currentOuterBatch != null) {
            currentOuterBatch.close();
            currentOuterBatch = null;
        }
        probeSource.close();
        inner.close();
        if (joinIndex != null) {
            joinIndex.releaseBuffers();
            joinIndex = null;
        }
        bufferedInner.releaseBuffers();
        for (BuildDictionary dictionary : buildDictionaries.values()) {
            releaseBuildDictionaryIds(dictionary.idByPosition());
        }
        buildDictionaries.clear();
        directInnerNullAccess = null;
        directInnerNullAccessResolved = null;
        arrayPool.release(fastInnerOrderedIntFilterValues);
        fastInnerOrderedIntFilterValues = null;
        arrayPool.release(buildKeyMins);
        buildKeyMins = null;
        arrayPool.release(buildKeyMaxs);
        buildKeyMaxs = null;
        buildKeyValues = null;
        releaseCollectedBuildKeys();
        arrayPool.release(matchedOuterPositions);
        matchedOuterPositions = null;
        allocator.release(allocationContext);
        allocator.release(buildAllocationContext);
        if (POOL_JOIN_SCRATCH && !joinScratchReleased) {
            joinScratchReleased = true;
            arrayPool.retain(JoinScratch.class, BATCH_SIZE, joinScratch.retainedBytes(), joinScratch);
        }
    }

    public HashJoinOperator withProfileName(String profileName)
    {
        this.profileName = profileName;
        return this;
    }

    /**
     * Selects and orders the join's public outputs using physical concatenated column ordinals
     * ({@code outer columns} followed by {@code inner columns}). Join keys and residual columns remain available to
     * execution even when omitted. This is the join-output-layout operation exposed by native plan builders, and
     * avoids paying for a separate projection operator or materializing discarded payloads.
     */
    public HashJoinOperator withOutputs(int... outputChannels)
    {
        requireNonNull(outputChannels, "outputChannels is null");
        int[] selected = outputChannels.clone();
        for (int outputChannel : selected) {
            if (outputChannel < 0 || outputChannel >= totalOutputCount) {
                throw new IllegalArgumentException("Join output column is out of bounds: " + outputChannel);
            }
        }
        this.outputChannels = selected;
        return this;
    }

    /**
     * Bounds lazy output mapping depth at this physical join boundary. A depth of zero always composes an encoded
     * probe-side mapping; larger values preserve shallower nested mappings and compose only once the source reaches
     * the limit. Composition is shared across columns with the same mapping chain.
     */
    public HashJoinOperator withComposeEncodedOuterDictionaryDepth(int depth)
    {
        if (depth < 0) {
            throw new IllegalArgumentException("depth is negative");
        }
        this.composeEncodedOuterDictionaryDepth = depth;
        return this;
    }

    /**
     * Starts a single-long build index without duplicate-chain tail/count arrays. If a duplicate is observed, the
     * index promotes exactly by reconstructing that state from occupied slots. This is a safe physical-plan hint for
     * expected-unique build keys; it never changes duplicate semantics.
     */
    public HashJoinOperator withLazyDuplicateSlotState()
    {
        this.lazyDuplicateSlotState = true;
        return this;
    }

    /**
     * Writes a non-retained build with an exact, bounded cardinality directly into its final coalesced buffers.
     * Source batches are still closed as soon as their selected rows have been copied, so this removes an
     * intermediate copy without extending the lifetime of reader-owned buffers.
     */
    public HashJoinOperator withDirectExactBuildCoalescing()
    {
        bufferedInner.enableDirectExactCoalesce();
        implicitSequentialBuildRowReferences = IMPLICIT_SEQUENTIAL_BUILD_ROW_REFERENCES;
        return this;
    }

    /**
     * Writes a build expected to fit the bounded coalescing limit directly into one final buffer. If the estimate is
     * wrong, the full bounded page is emitted and subsequent rows continue in another page, preserving exact output
     * without exceeding the configured capacity. Copied source batches are still closed promptly.
     */
    public HashJoinOperator withDirectBoundedBuildCoalescing()
    {
        implicitSequentialBuildRowReferences = bufferedInner.enableDirectBoundedCoalesce() && IMPLICIT_SEQUENTIAL_BUILD_ROW_REFERENCES;
        return this;
    }

    private Output resultOutput(int outputIndex)
    {
        if (currentOutputCount == 0) {
            Streams schema = outputSchema(outputIndex);
            if (schema == null) {
                return new Output(Set.of(), stream -> {
                    throw new IllegalArgumentException("Output does not expose stream: " + stream);
                });
            }
            Streams empty = buffers.emptyLike(schema);
            return new Output(
                    empty.streams(),
                    empty::get,
                    (stream, vector) -> allocator.transfer(allocationContext, vector),
                    (stream, vector) -> allocator.release(allocationContext, vector));
        }

        Set<Stream> streams = outputIndex < outerOutputCount
                ? currentOuterBatch.output(outputIndex).streams()
                : innerOutputStreams(outputIndex - outerOutputCount);
        Set<Stream> knownAllFalseStreams = resultKnownAllFalseStreams(outputIndex, streams);
        return new Output(
                streams,
                stream -> {
                    // A known-false side stream is independent of the corresponding value. Check it before the
                    // whole-column fallback so a NULLS-only consumer does not accidentally materialize VALUES.
                    if (knownAllFalseStreams.contains(stream)) {
                        return allFalseBooleanStream(currentOutputCount);
                    }
                    if (stream == Stream.NULLS && canResolveInnerNullStreamDirectly(outputIndex)) {
                        return materializeInnerNullStreamDirectly(outputIndex - outerOutputCount);
                    }
                    Streams materialized = materializeOutput(outputIndex);
                    if (materialized.has(stream)) {
                        return materialized.get(stream);
                    }
                    throw new IllegalArgumentException("Output does not expose stream: " + stream);
                },
                (stream, vector) -> allocator.transfer(allocationContext, vector),
                (stream, vector) -> allocator.release(allocationContext, vector),
                null,
                null)
                .withKnownAllFalse(knownAllFalseStreams);
    }

    private boolean canResolveInnerNullStreamDirectly(int outputIndex)
    {
        if (!DIRECT_OUTER_JOIN_NULL_STREAM || !probeOuterJoin || outputIndex < outerOutputCount) {
            return false;
        }
        return bufferedInner.outputStreams(outputIndex - outerOutputCount) != null;
    }

    private Vector materializeInnerNullStreamDirectly(int innerOutputIndex)
    {
        long start = System.nanoTime();
        if (RLE_ALL_UNMATCHED_OUTER_JOIN_OUTPUT && allRowsHaveNoMatch()) {
            Vector nulls = allTrueBooleanStream(currentOutputCount);
            if (materializationListener != null) {
                materializationListener.record(profileName != null ? profileName : "hash_join", -1, Streams.of(Stream.NULLS, nulls), currentOutputCount, System.nanoTime() - start);
            }
            return nulls;
        }
        BooleanVector nulls = allocator.allocate(allocationContext, BooleanVector.class, currentOutputCount, BooleanVector::new);
        boolean[] values = nulls.values();
        // Allocator storage is recycled. Clear the visible range before marking the unmatched probe rows.
        Arrays.fill(values, 0, currentOutputCount, false);
        if (!outputInnerLogicalPositionsReady) {
            for (int position = 0; position < currentOutputCount; position++) {
                long rowReference = outputInnerRows[position];
                if (rowReference == NO_MATCH_ROW_REFERENCE) {
                    values[position] = true;
                    continue;
                }
                int innerBatchIndex = batchIndex(rowReference);
                VectorAccess.BooleanValues sourceNulls = directInnerNullAccess(innerOutputIndex, innerBatchIndex);
                if (sourceNulls != null) {
                    BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
                    int logicalPosition = rowPosition(rowReference);
                    int sourcePosition = innerBatch.retained() ? innerBatch.sourcePosition(logicalPosition) : logicalPosition;
                    values[position] = sourceNulls.value(sourcePosition);
                }
            }
        }
        if (materializationListener != null) {
            materializationListener.record(profileName != null ? profileName : "hash_join", -1, Streams.of(Stream.NULLS, nulls), currentOutputCount, System.nanoTime() - start);
        }
        return nulls;
    }

    private VectorAccess.BooleanValues directInnerNullAccess(int innerOutputIndex, int innerBatchIndex)
    {
        if (directInnerNullAccess == null) {
            directInnerNullAccess = new VectorAccess.BooleanValues[innerOutputCount][];
            directInnerNullAccessResolved = new boolean[innerOutputCount][];
        }
        int batchCount = bufferedInner.batches().size();
        if (directInnerNullAccess[innerOutputIndex] == null) {
            directInnerNullAccess[innerOutputIndex] = new VectorAccess.BooleanValues[batchCount];
            directInnerNullAccessResolved[innerOutputIndex] = new boolean[batchCount];
        }
        if (!directInnerNullAccessResolved[innerOutputIndex][innerBatchIndex]) {
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
            Vector sourceNulls;
            if (innerBatch.retained()) {
                Output output = innerBatch.retainedBatch().output(innerOutputIndex);
                sourceNulls = output.isKnownAllFalse(Stream.NULLS) ? null : output.borrowOrNull(Stream.NULLS);
            }
            else {
                Streams streams = innerBatch.columns()[innerOutputIndex];
                sourceNulls = streams == null ? null : streams.getOrNull(Stream.NULLS);
            }
            if (sourceNulls != null && !VectorAccess.isAllFalseNulls(sourceNulls)) {
                directInnerNullAccess[innerOutputIndex][innerBatchIndex] = VectorAccess.booleanValues(sourceNulls);
            }
            directInnerNullAccessResolved[innerOutputIndex][innerBatchIndex] = true;
        }
        return directInnerNullAccess[innerOutputIndex][innerBatchIndex];
    }

    private Streams outputSchema(int outputIndex)
    {
        if (outputIndex < outerOutputCount) {
            Streams schema = outerSchema[outputIndex];
            if (schema != null) {
                return schema;
            }
            if (currentOuterBatch != null) {
                Streams.Builder streams = Streams.builder();
                Output output = currentOuterBatch.output(outputIndex);
                if (output.hasValues()) {
                    streams.put(Stream.VALUES, output.borrow(Stream.VALUES));
                }
                if (output.hasNulls()) {
                    streams.put(Stream.NULLS, new BooleanVector(0));
                }
                if (output.hasErrors()) {
                    streams.put(Stream.ERRORS, new BooleanVector(0));
                }
                return streams.build();
            }
            return null;
        }
        Streams schema = innerSchema[outputIndex - outerOutputCount];
        if (schema != null) {
            return probeOuterJoin ? ensureNullStream(schema) : schema;
        }
        Streams bufferedSchema = bufferedInner.outputSchema(outputIndex - outerOutputCount);
        return probeOuterJoin && bufferedSchema != null ? ensureNullStream(bufferedSchema) : bufferedSchema;
    }

    private static void copySchema(Streams[] source, Streams[] target)
    {
        for (int index = 0; index < target.length; index++) {
            if (target[index] == null && source[index] != null) {
                target[index] = source[index];
            }
        }
    }

    private int[] innerSourcePositions()
    {
        if (joinScratch.outputInnerSourcePositions == null) {
            joinScratch.outputInnerSourcePositions = new int[BATCH_SIZE];
        }
        return joinScratch.outputInnerSourcePositions;
    }

    private int[] innerUniqueSourcePositions()
    {
        if (joinScratch.outputInnerUniqueSourcePositions == null) {
            joinScratch.outputInnerUniqueSourcePositions = new int[BATCH_SIZE];
        }
        return joinScratch.outputInnerUniqueSourcePositions;
    }

    private int[] retainedInnerMaskPositionsScratch()
    {
        if (joinScratch.retainedInnerMaskPositionsScratch == null) {
            joinScratch.retainedInnerMaskPositionsScratch = new int[BATCH_SIZE];
        }
        return joinScratch.retainedInnerMaskPositionsScratch;
    }

    private Streams materializeOutput(int outputIndex)
    {
        Streams existing = currentOutputs[outputIndex];
        if (existing != null) {
            return existing;
        }
        long start = System.nanoTime();
        Streams materialized = outputIndex < outerOutputCount
                ? materializeOuterOutput(outputIndex)
                : materializeInnerOutput(outputIndex - outerOutputCount);
        if (materializationListener != null) {
            materializationListener.record(profileName != null ? profileName : "hash_join", outputIndex, materialized, currentOutputCount, System.nanoTime() - start);
        }
        currentOutputs[outputIndex] = materialized;
        return materialized;
    }

    private Streams materializeOuterOutput(int outputIndex)
    {
        Output sourceOutput = currentOuterBatch.output(outputIndex);
        if (!outerSupportsReborrow) {
            // Outer cannot satisfy a constrained re-borrow (e.g. a Parquet-backed subplan or a join
            // result): keep the baseline dictionary-wrap, which borrows the live outer column once
            // and indexes it by the matched output positions. No constraint is pushed and no deferral
            // happens past the source's advance.
            return wrapOuterOutput(sourceOutput);
        }

        // Outer can satisfy a constrained re-borrow: narrow it to the matched rows so a lazy
        // projected payload computes only the rows the join emits, then flat-copy the matched
        // positions into a dense vector indexable directly by output position. Materialization stays
        // lazy: it only runs when an outer stream is borrowed.
        constrainOuterIfNecessary();
        if (forwardOuterIdentity) {
            return borrowOuterOutput(sourceOutput);
        }
        if (currentOutputMask.all()) {
            return buffers.copyPositions(sourceOutput, null, outputOuterPositions, currentOutputCount, 0, currentOutputCount);
        }

        Streams result = null;
        for (int index = 0; index < currentOutputMask.count(); index++) {
            int outputPosition = currentOutputMask.position(index);
            result = buffers.copySinglePosition(sourceOutput, result, currentOutputCount, outputPosition, outputOuterPositions[outputPosition]);
        }
        return result == null ? buffers.emptyLike(outputSchema(outputIndex)) : result;
    }

    /**
     * Forward an identity-mapped, fully-consumed probe column without changing its ownership. The outer batch stays
     * open until this join advances, so a normal borrow remains valid for the complete lifetime of the result batch;
     * if a consumer takes the result, Allocator.transfer finds and detaches the vector from its original context.
     */
    private static Streams borrowOuterOutput(Output sourceOutput)
    {
        Streams.Builder streams = Streams.builder();
        if (sourceOutput.hasValues()) {
            streams.put(Stream.VALUES, sourceOutput.borrow(Stream.VALUES));
        }
        if (sourceOutput.hasNulls() && !sourceOutput.isKnownAllFalse(Stream.NULLS)) {
            streams.put(Stream.NULLS, sourceOutput.borrow(Stream.NULLS));
        }
        if (sourceOutput.hasErrors() && !sourceOutput.isKnownAllFalse(Stream.ERRORS)) {
            streams.put(Stream.ERRORS, sourceOutput.borrow(Stream.ERRORS));
        }
        return streams.build();
    }

    private boolean outerOutputIsIdentity()
    {
        // The result batch is dense [0, currentOutputCount). A mapping that merely preserves the order of a sparse
        // source mask is not an identity mapping: its values still live at the sparse source positions and must be
        // compacted. Zero-copy forwarding is valid only when both masks cover the complete dense source batch.
        if (!currentOuterBatchFullyConsumed() || !currentOuterMask.all() || !currentOutputMask.all() ||
                currentOutputCount != currentOuterMask.size()) {
            return false;
        }
        for (int index = 0; index < currentOutputCount; index++) {
            if (outputOuterPositions[index] != index) {
                return false;
            }
        }
        return true;
    }

    private Streams wrapOuterOutput(Output sourceOutput)
    {
        if (sourceOutput.isValuesOnly()) {
            return Streams.ofValues(allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.VALUES))));
        }
        Streams.Builder streams = Streams.builder();
        if (sourceOutput.hasValues()) {
            streams.put(Stream.VALUES, allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.VALUES))));
        }
        if (sourceOutput.hasNulls() && !sourceOutput.isKnownAllFalse(Stream.NULLS)) {
            streams.put(Stream.NULLS, allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.NULLS))));
        }
        if (sourceOutput.hasErrors() && !sourceOutput.isKnownAllFalse(Stream.ERRORS)) {
            streams.put(Stream.ERRORS, allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.ERRORS))));
        }
        return streams.build();
    }

    private int[] outerDictionaryIds()
    {
        if (currentOuterDictionaryIds == null) {
            currentOuterDictionaryIds = aliasBatchDictionaryIds(outputOuterPositions)
                    ? outputOuterPositions
                    : Arrays.copyOf(outputOuterPositions, currentOutputCount);
        }
        return currentOuterDictionaryIds;
    }

    private Vector buildOuterDictionaryStream(Vector source)
    {
        if (source instanceof DictionaryVector || source instanceof org.weakref.nitro.data.RleVector) {
            int composeDepth = composeEncodedOuterDictionaryDepth;
            if (composeDepth == Integer.MAX_VALUE && ADAPTIVE_TINY_DICTIONARY_COMPOSITION && currentOutputCount <= ADAPTIVE_COMPOSE_MAX_ROWS) {
                composeDepth = ADAPTIVE_COMPOSE_DEPTH;
            }
            if (WRAP_ENCODED_OUTER_DICTIONARIES && encodingDepth(source) < composeDepth) {
                return DictionaryVector.wrapNested(outerDictionaryIds(), currentOutputCount, source);
            }
            if (CACHE_COMPOSED_OUTER_DICTIONARY_IDS) {
                for (int index = 0; index < composedOuterMappingCount; index++) {
                    if (sameEncodingMapping(source, composedOuterMappingSources[index])) {
                        return DictionaryVector.wrap(
                                composedOuterMappingIds[index],
                                currentOutputCount,
                                encodingLeaf(source));
                    }
                }
            }
            // wrapComposedDictionary rewrites the ids in place while collapsing nested encodings, so an
            // encoded source needs a private copy it can mutate. The outer positions ascend (one output row per
            // match, in probe order), so an RLE level resolves run indices with a forward hint instead of binary search.
            int[] ids = Arrays.copyOf(outerDictionaryIds(), currentOutputCount);
            DictionaryVector composed = wrapComposedDictionary(ids, source, true);
            if (CACHE_COMPOSED_OUTER_DICTIONARY_IDS) {
                composedOuterMappingSources[composedOuterMappingCount] = source;
                composedOuterMappingIds[composedOuterMappingCount] = composed.ids();
                composedOuterMappingCount++;
            }
            return composed;
        }
        // Flat source: wrapComposedDictionary leaves the ids untouched, so every flat outer column can share
        // the single cached id snapshot instead of allocating a per-column copy.
        return DictionaryVector.wrap(outerDictionaryIds(), currentOutputCount, source);
    }

    private void clearComposedOuterMappings()
    {
        Arrays.fill(composedOuterMappingSources, 0, composedOuterMappingCount, null);
        Arrays.fill(composedOuterMappingIds, 0, composedOuterMappingCount, null);
        composedOuterMappingCount = 0;
    }

    /** Returns true when two encoded columns apply the identical position mapping to different leaf vectors. */
    private static boolean sameEncodingMapping(Vector left, Vector right)
    {
        Vector leftLevel = left;
        Vector rightLevel = right;
        while (true) {
            if (leftLevel instanceof DictionaryVector leftDictionary && rightLevel instanceof DictionaryVector rightDictionary) {
                if (leftDictionary.ids() != rightDictionary.ids() || leftDictionary.length() != rightDictionary.length()) {
                    return false;
                }
                leftLevel = leftDictionary.values();
                rightLevel = rightDictionary.values();
                continue;
            }
            if (leftLevel instanceof org.weakref.nitro.data.RleVector leftRle && rightLevel instanceof org.weakref.nitro.data.RleVector rightRle) {
                if (leftRle.counts() != rightRle.counts() || leftRle.length() != rightRle.length()) {
                    return false;
                }
                leftLevel = leftRle.values();
                rightLevel = rightRle.values();
                continue;
            }
            return !(leftLevel instanceof DictionaryVector || leftLevel instanceof org.weakref.nitro.data.RleVector) &&
                    !(rightLevel instanceof DictionaryVector || rightLevel instanceof org.weakref.nitro.data.RleVector);
        }
    }

    private static Vector encodingLeaf(Vector vector)
    {
        Vector level = vector;
        while (true) {
            if (level instanceof DictionaryVector dictionary) {
                level = dictionary.values();
                continue;
            }
            if (level instanceof org.weakref.nitro.data.RleVector rle) {
                level = rle.values();
                continue;
            }
            return level;
        }
    }

    private static int encodingDepth(Vector vector)
    {
        int depth = 0;
        Vector level = vector;
        while (true) {
            if (level instanceof DictionaryVector dictionary) {
                depth++;
                level = dictionary.values();
                continue;
            }
            if (level instanceof org.weakref.nitro.data.RleVector rle) {
                depth++;
                level = rle.values();
                continue;
            }
            return depth;
        }
    }

    private boolean aliasBatchDictionaryIds(int[] positions)
    {
        return ALIAS_BATCH_DICTIONARY_IDS
                || (ALIAS_FULL_BATCH_DICTIONARY_IDS && currentOutputCount == positions.length);
    }

    private int[] currentBatchPositions(int[] positions)
    {
        return aliasBatchDictionaryIds(positions)
                ? positions
                : Arrays.copyOf(positions, currentOutputCount);
    }

    private int[] copyCurrentBatchPositions(int[] positions)
    {
        return Arrays.copyOf(positions, currentOutputCount);
    }

    private int[] innerLogicalDictionaryIds()
    {
        if (!CACHE_INNER_DICTIONARY_IDS) {
            return currentBatchPositions(outputInnerLogicalPositions);
        }
        if (currentInnerLogicalDictionaryIds == null) {
            currentInnerLogicalDictionaryIds = currentBatchPositions(outputInnerLogicalPositions);
        }
        return currentInnerLogicalDictionaryIds;
    }

    private int[] innerSourceDictionaryIds()
    {
        if (!CACHE_INNER_DICTIONARY_IDS) {
            return currentBatchPositions(innerSourcePositions());
        }
        if (currentInnerSourceDictionaryIds == null) {
            currentInnerSourceDictionaryIds = currentBatchPositions(innerSourcePositions());
        }
        return currentInnerSourceDictionaryIds;
    }

    private Vector wrapInnerLogicalDictionary(Vector source)
    {
        if (source instanceof DictionaryVector || source instanceof org.weakref.nitro.data.RleVector) {
            return wrapComposedDictionary(copyCurrentBatchPositions(outputInnerLogicalPositions), source);
        }
        return DictionaryVector.wrap(innerLogicalDictionaryIds(), currentOutputCount, source);
    }

    private Vector wrapInnerSourceDictionary(Vector source)
    {
        if (source instanceof DictionaryVector || source instanceof org.weakref.nitro.data.RleVector) {
            return wrapComposedDictionary(copyCurrentBatchPositions(innerSourcePositions()), source);
        }
        return DictionaryVector.wrap(innerSourceDictionaryIds(), currentOutputCount, source);
    }

    private void constrainOuterIfNecessary()
    {
        // The first time an outer column of this output batch is materialized, narrow the outer
        // operator to exactly the outer positions that survived the join for this batch. This lets a
        // lazy outer subplan (a deferred projected payload) compute only the rows the join emits.
        // Only push a constraint to an outer that can satisfy a constrained re-borrow; a source
        // whose reader advances irreversibly (a Parquet scan) must not be constrained and re-borrowed
        // - for those, the flat copy above borrows the live batch column once without deferral.
        if (!outerSupportsReborrow || outerConstrained || currentOuterBatch == null || !currentOuterBatchFullyConsumed()) {
            return;
        }
        outerConstrained = true;
        if (FORWARD_IDENTITY_REBORROW_OUTER && outerOutputIsIdentity()) {
            forwardOuterIdentity = true;
            if (DEBUG_IDENTITY_REBORROW_OUTER && !identityReborrowReported) {
                identityReborrowReported = true;
                System.err.printf("[identity-reborrow] join=%s rows=%d outputs=%d%n",
                        profileName != null ? profileName : "hash_join", currentOutputCount, outputChannels.length);
            }
            return;
        }
        outerConstraintApplied = true;
        probeSource.constrain(matchedOuterMask());
    }

    /**
     * Whether this output batch is the last one drawing from the current outer batch — i.e. the
     * outer batch's probe positions are fully exhausted, so no later output batch will reference
     * {@code currentOuterBatch}. Only then is it safe to push a narrowing constraint to the outer:
     * its borrowed columns would otherwise be shared by later output batches drawing from the same
     * outer batch.
     */
    private boolean currentOuterBatchFullyConsumed()
    {
        return outerRemaining == 0
                && !currentOuterPositionReady
                && preparedOuterIndex >= preparedOuterCount
                && currentOuterMaskIndex >= currentOuterMask.count();
    }

    private Mask matchedOuterMask()
    {
        int totalPositions = currentOuterMask.size();
        if (currentOutputCount == 0 || currentOutputMask.none()) {
            return allocator.allocateEmptyMask(allocationContext, totalPositions);
        }

        int count = currentOutputMask.count();
        int capacity = Math.min(count, currentOuterMask.count());
        if (matchedOuterPositions.length < capacity) {
            int[] previousPositions = matchedOuterPositions;
            matchedOuterPositions = arrayPool.borrowInts(capacity);
            arrayPool.release(previousPositions);
        }
        int selectedCount = 0;
        int previous = -1;
        for (int index = 0; index < count; index++) {
            int outputPosition = currentOutputMask.position(index);
            int outerPosition = outputOuterPositions[outputPosition];
            if (outerPosition != previous) {
                matchedOuterPositions[selectedCount++] = outerPosition;
                previous = outerPosition;
            }
        }
        return allocator.allocateSparseMask(allocationContext, matchedOuterPositions, selectedCount, totalPositions);
    }

    private static Set<Stream> sideStreams(Output output)
    {
        EnumSet<Stream> streams = EnumSet.noneOf(Stream.class);
        if (output.hasNulls()) {
            streams.add(Stream.NULLS);
        }
        if (output.hasErrors()) {
            streams.add(Stream.ERRORS);
        }
        return streams;
    }

    private Streams materializeInnerOutput(int innerOutputIndex)
    {
        if (RLE_ALL_UNMATCHED_OUTER_JOIN_OUTPUT && allRowsHaveNoMatch()) {
            Streams schema = outputSchema(innerOutputIndex + outerOutputCount);
            if (schema == null) {
                throw new IllegalStateException("Unable to determine inner output schema for left join");
            }
            return allNullInnerOutput(schema, currentOutputCount);
        }

        if (!hasNoMatchRows()) {
            // The dictionary-wrap shortcuts produce a full-length, position-indexed vector, so they are
            // correct whether or not the output mask is sparse (a downstream constraint). They also drop
            // the per-position byte copy that otherwise dominates high-fan-out joins. Deferred build
            // batches still fall through (the wrap methods bail on them) so their lazy payloads keep
            // materializing only the constrained rows.
            prepareInnerOutputRuns();
            Streams wrapped = tryWrapSingleBatchInnerOutput(innerOutputIndex);
            if (wrapped != null) {
                return wrapped;
            }
            Vector dictionaryValues = tryWrapNonRetainedValues(innerOutputIndex);
            if (dictionaryValues != null) {
                // VALUES are carried as a unified dictionary (no byte copy). The boolean side streams
                // are wrapped the same way -- a dictionary over the matched logical positions -- rather
                // than flattened per output row, mirroring the retained and multi-run wrap paths and
                // matching a build row's whole record (values + nulls + errors) by index. This is a
                // single-run path (tryWrapNonRetainedValues requires preparedInnerRunCount ==
                // 1), so the whole output draws from one non-retained build batch.
                Streams.Builder result = Streams.builder();
                result.put(Stream.VALUES, allocator.adopt(allocationContext, dictionaryValues));
                Streams column = bufferedInner.batches().get(outputInnerRunBatchIndexes[0]).columns()[innerOutputIndex];
                if (column != null && column.hasNulls()) {
                    result.put(Stream.NULLS, wrapInnerSideStream(column.get(Stream.NULLS)));
                }
                if (column != null && column.hasErrors()) {
                    result.put(Stream.ERRORS, wrapInnerSideStream(column.get(Stream.ERRORS)));
                }
                return result.build();
            }
            // No zero-copy wrap applies. The remaining per-run bulk copy materializes the full output
            // range, so only take it when the mask is full; a sparse mask falls through to the
            // per-position copy below, which honors the constraint (and any deferred-batch laziness).
            if (currentOutputMask.all()) {
                Streams wrappedSideStreams = tryWrapMultiRunInnerBooleanSideStreams(innerOutputIndex);
                Streams result = wrappedSideStreams;
                boolean copyNulls = wrappedSideStreams == null || !wrappedSideStreams.hasNulls();
                boolean copyErrors = wrappedSideStreams == null || !wrappedSideStreams.hasErrors();
                for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
                    int outputStart = outputInnerRunStarts[runIndex];
                    int runLength = outputInnerRunLengths[runIndex];
                    BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(outputInnerRunBatchIndexes[runIndex]);
                    result = copyInnerPositions(result, innerBatch, runIndex, outputInnerRunBatchIndexes[runIndex], innerOutputIndex, outputStart, runLength, currentOutputCount, true, copyNulls, copyErrors);
                }
                return result == null ? buffers.emptyLike(outputSchema(innerOutputIndex + outerOutputCount)) : result;
            }
        }

        Streams result = null;
        boolean exposeNulls = probeOuterJoin || innerOutputStreams(innerOutputIndex).contains(Stream.NULLS);
        Streams nullInnerSchema = null;
        // The constraint mask can select no rows while a downstream consumer still borrows this column
        // and indexes a sample position (for example ProjectOperator probes one row to discover which
        // streams a column exposes). Outer-side columns are always materialized over the full output
        // range, so an inner-side column must do the same when its constraint is empty; otherwise the
        // borrowed column has length zero and indexing the sample position throws.
        boolean materializeFullRange = currentOutputMask.none();
        int positionCount = materializeFullRange ? currentOutputCount : currentOutputMask.count();
        long[] innerRows = outputInnerRows;
        for (int index = 0; index < positionCount; index++) {
            int outputPosition = materializeFullRange ? index : currentOutputMask.position(index);
            long rowReference = innerRows[outputPosition];
            if (rowReference == NO_MATCH_ROW_REFERENCE) {
                if (nullInnerSchema == null) {
                    nullInnerSchema = outputSchema(innerOutputIndex + outerOutputCount);
                    if (nullInnerSchema == null) {
                        throw new IllegalStateException("Unable to determine inner output schema for left join");
                    }
                }
                result = copyNullInnerPosition(result, nullInnerSchema, currentOutputCount, outputPosition);
                continue;
            }
            int innerBatchIndex = batchIndex(rowReference);
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
            result = copyInnerSinglePosition(result, innerBatch, innerBatchIndex, innerOutputIndex, currentOutputCount, outputPosition, rowPosition(rowReference), exposeNulls);
        }
        return result == null ? buffers.emptyLike(outputSchema(innerOutputIndex + outerOutputCount)) : result;
    }

    private boolean allRowsHaveNoMatch()
    {
        if (!probeOuterJoin || outputInnerLogicalPositionsReady || currentOutputCount == 0) {
            return false;
        }
        if (allRowsNoMatchState != 0) {
            return allRowsNoMatchState == 2;
        }
        // Mixed batches overwhelmingly expose a match at one of their boundaries. Reject those with
        // two predictable loads; only a plausible all-unmatched batch pays the full verification scan.
        if (outputInnerRows[0] != NO_MATCH_ROW_REFERENCE ||
                outputInnerRows[currentOutputCount - 1] != NO_MATCH_ROW_REFERENCE) {
            allRowsNoMatchState = 1;
            return false;
        }
        for (int position = 1; position < currentOutputCount - 1; position++) {
            if (outputInnerRows[position] != NO_MATCH_ROW_REFERENCE) {
                allRowsNoMatchState = 1;
                return false;
            }
        }
        allRowsNoMatchState = 2;
        if (DEBUG_RLE_ALL_UNMATCHED_OUTER_JOIN_OUTPUT) {
            System.err.printf("[rle-all-unmatched-outer-join] join=%s rows=%d%n",
                    profileName != null ? profileName : "hash_join",
                    currentOutputCount);
        }
        return true;
    }

    private Streams allNullInnerOutput(Streams schema, int size)
    {
        Streams.Builder result = Streams.builder();
        Vector value = nullValuesLike(schema.values(), 1);
        result.put(Stream.VALUES, allocator.allocateSingleRunRle(allocationContext, size, value));

        result.put(Stream.NULLS, allTrueBooleanStream(size));

        if (schema.has(Stream.ERRORS)) {
            BooleanVector noError = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
            result.put(Stream.ERRORS, allocator.allocateSingleRunRle(allocationContext, size, noError));
        }
        return result.build();
    }

    private Vector allTrueBooleanStream(int size)
    {
        BooleanVector value = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
        value.values()[0] = true;
        return allocator.allocateSingleRunRle(allocationContext, size, value);
    }

    private Streams tryWrapMultiRunInnerBooleanSideStreams(int innerOutputIndex)
    {
        if (currentOutputCount == 0 || preparedInnerRunCount <= 1) {
            return null;
        }

        Streams.Builder wrapped = Streams.builder();
        Vector nulls = tryWrapMultiRunInnerBooleanStream(innerOutputIndex, Stream.NULLS);
        if (nulls != null) {
            wrapped.put(Stream.NULLS, allocator.adopt(allocationContext, nulls));
        }
        Vector errors = tryWrapMultiRunInnerBooleanStream(innerOutputIndex, Stream.ERRORS);
        if (errors != null) {
            wrapped.put(Stream.ERRORS, allocator.adopt(allocationContext, errors));
        }

        Streams wrappedStreams = wrapped.build();
        return wrappedStreams.streams().isEmpty() ? null : wrappedStreams;
    }

    private Vector tryWrapMultiRunInnerBooleanStream(int innerOutputIndex, Stream stream)
    {
        if (preparedInnerRunCount <= 1) {
            return null;
        }

        Vector[] segments = new Vector[preparedInnerRunCount];
        int[] dictionaryIds = new int[currentOutputCount];
        int segmentOffset = 0;
        for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
            int innerBatchIndex = outputInnerRunBatchIndexes[runIndex];
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
            int positionStart = outputInnerRunStarts[runIndex];
            int positionCount = outputInnerRunLengths[runIndex];

            Vector source;
            if (!innerBatch.retained()) {
                Streams output = innerBatch.columns()[innerOutputIndex];
                if (output == null || !output.has(stream)) {
                    return null;
                }
                source = output.get(stream);
                for (int index = 0; index < positionCount; index++) {
                    dictionaryIds[positionStart + index] = segmentOffset + outputInnerLogicalPositions[positionStart + index];
                }
            }
            else {
                constrainRetainedInnerBatch(innerBatchIndex, innerBatch, outputInnerRunUniqueStarts[runIndex], outputInnerRunUniqueCounts[runIndex]);
                Output output = innerBatch.retainedBatch().output(innerOutputIndex);
                if (!output.has(stream)) {
                    return null;
                }
                source = output.borrow(stream);
                int[] innerSourcePositions = innerSourcePositions();
                for (int index = 0; index < positionCount; index++) {
                    dictionaryIds[positionStart + index] = segmentOffset + innerSourcePositions[positionStart + index];
                }
            }

            segments[runIndex] = source;
            segmentOffset += source.length();
        }

        Vector values = new ConcatenatedBooleanVector(segments);
        return DictionaryVector.wrap(dictionaryIds, values);
    }

    private Streams tryWrapSingleBatchInnerOutput(int innerOutputIndex)
    {
        if (currentOutputCount == 0) {
            return null;
        }

        if (preparedInnerRunCount != 1) {
            return null;
        }

        int innerBatchIndex = outputInnerRunBatchIndexes[0];
        BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
        if (innerBatch.deferred() || !innerBatch.retained()) {
            // Deferred batches must materialize lazily by constraining the source to the matched
            // positions and re-borrowing; non-retained (compacted) batches are flat-copied so the
            // inner VALUES are a dense vector indexable directly by output position. Both fall
            // through to the per-position copy path (copyInnerPositions): for deferred batches it
            // constrains the retained batch before borrowing (keeping projected payloads deferred);
            // for non-retained batches it flat-copies without any constrain. Genuinely retained
            // batches keep the dictionary-wrap shortcut below.
            return null;
        }

        return tryWrapRetainedInnerOutput(innerOutputIndex, innerBatch);
    }

    /**
     * Emits a single-run, non-retained build column's VALUES as a {@link DictionaryVector} over the
     * build values rather than flattening once per matched output row. Binary values may use a unified
     * per-build-column dictionary when cardinality is low; fixed-width flat values wrap the raw build
     * vector directly. Restricted to the common, safe shape: the whole output batch draws from one
     * build batch ({@code preparedInnerRunCount == 1}) and there are no NO-MATCH rows. Any other shape
     * returns {@code null} so the caller falls back to the existing flatten path. Grouping downstream
     * still settles equality by value, so this only changes representation.
     */
    private Vector tryWrapNonRetainedValues(int innerOutputIndex)
    {
        if (currentOutputCount == 0 || preparedInnerRunCount != 1) {
            return null;
        }
        int innerBatchIndex = outputInnerRunBatchIndexes[0];
        BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
        if (innerBatch.retained()) {
            return null;
        }
        Streams column = innerBatch.columns()[innerOutputIndex];
        if (column == null || !column.hasValues()) {
            return null;
        }
        Vector values = column.values();
        if (!(values instanceof BinaryVector binarySource)) {
            if (WRAP_NON_RETAINED_FIXED_WIDTH_BUILD_VALUES && isRawWrappableFixedWidthValue(values)) {
                return wrapRawNonRetainedValues(values);
            }
            return null;
        }

        if ((long) innerBatch.length() > (long) currentOutputCount * BUILD_DICTIONARY_SPARSE_RATIO) {
            return wrapRawNonRetainedValues(binarySource);
        }

        BuildDictionary dictionary = buildDictionaryFor(innerBatchIndex, innerOutputIndex, binarySource, innerBatch.length());
        if (dictionary == NOT_DICTIONARY) {
            // High-cardinality build column: a dedup dictionary does not pay, but flattening would copy the
            // (variable-width) bytes once per matched output row. Wrap the raw build column directly instead --
            // ids are the matched build positions, values are the build column itself -- so each matched row is
            // referenced by id with no byte copy, at any cardinality (mirrors Trino's DictionaryBlock over a
            // build page). Safe for the same single-batch shape the dedup path requires; downstream grouping
            // still settles equality by value, so this only changes representation.
            return wrapRawNonRetainedValues(binarySource);
        }
        int[] sourceIdByPosition = dictionary.idByPosition();
        int[] valueIds = new int[currentOutputCount];
        for (int index = 0; index < currentOutputCount; index++) {
            valueIds[index] = sourceIdByPosition[outputInnerLogicalPositions[index]];
        }
        return DictionaryVector.wrap(valueIds, dictionary.values());
    }

    private static boolean isRawWrappableFixedWidthValue(Vector values)
    {
        return values instanceof I64Vector
                || values instanceof I32Vector
                || values instanceof F64Vector
                || values instanceof BooleanVector;
    }

    private Vector wrapRawNonRetainedValues(Vector source)
    {
        return DictionaryVector.wrap(innerLogicalDictionaryIds(), currentOutputCount, source);
    }

    private BuildDictionary buildDictionaryFor(int innerBatchIndex, int innerOutputIndex, BinaryVector source, int length)
    {
        int key = innerBatchIndex * Math.max(1, innerOutputCount) + innerOutputIndex;
        BuildDictionary cached = buildDictionaries.get(key);
        if (cached != null) {
            return cached;
        }

        // Scan only the build batch's valid row range: the buffered BinaryVector may be pre-sized with
        // stale trailing offsets beyond the real rows, which are never referenced by a logical position.
        int[] idByPosition = borrowBuildDictionaryIds(length);
        // Deduplicate distinct byte values into a single dictionary. This only pays off when the column
        // is low cardinality: a near-unique build column (e.g. a natural key carried straight to the
        // output and never grouped) gains nothing from dictionary grouping while the dedup scan and the
        // per-output-row id remap are pure overhead. Abandon as soon as the distinct count shows the
        // column is high cardinality and cache a NOT_DICTIONARY marker so the caller wraps the raw
        // build column directly.
        int distinctLimit = Math.max(16, length / 2);
        ValueIdInterner interner = new ValueIdInterner(distinctLimit);
        byte[] data = source.data();
        long totalBytes = 0;
        for (int position = 0; position < length; position++) {
            int start = source.startOffset(position);
            int valueLength = source.length(position);
            int distinctBefore = interner.distinctCount();
            int id = interner.intern(data, start, valueLength);
            if (id == ValueIdInterner.TOO_MANY) {
                releaseBuildDictionaryIds(idByPosition);
                buildDictionaries.put(key, NOT_DICTIONARY);
                return NOT_DICTIONARY;
            }
            if (interner.distinctCount() > distinctBefore) {
                totalBytes += valueLength;
            }
            idByPosition[position] = id;
        }

        if (totalBytes > Integer.MAX_VALUE) {
            releaseBuildDictionaryIds(idByPosition);
            buildDictionaries.put(key, NOT_DICTIONARY);
            return NOT_DICTIONARY;
        }
        BinaryVector values = interner.toBinaryVector(allocator, buildAllocationContext);
        values.clearTraits();
        values.addTraits(source.traits());
        BuildDictionary dictionary = new BuildDictionary(idByPosition, values);
        buildDictionaries.put(key, dictionary);
        return dictionary;
    }

    private int[] borrowBuildDictionaryIds(int length)
    {
        long bytes = (long) length * Integer.BYTES;
        return POOL_BUILD_DICTIONARY_IDS && arrayPool.isRetainable(bytes) ? arrayPool.borrowInts(length) : new int[length];
    }

    private void releaseBuildDictionaryIds(int[] ids)
    {
        if (POOL_BUILD_DICTIONARY_IDS && arrayPool.isRetainable((long) ids.length * Integer.BYTES)) {
            arrayPool.release(ids);
        }
    }

    private static final BuildDictionary NOT_DICTIONARY = new BuildDictionary(new int[0], null);

    private record BuildDictionary(int[] idByPosition, Vector values) {}

    private Streams tryWrapRetainedInnerOutput(int innerOutputIndex, BufferedJoinInput.InnerBatch innerBatch)
    {
        if (!innerBatch.retained()) {
            return null;
        }

        Output output = innerBatch.retainedBatch().output(innerOutputIndex);
        if (output.isValuesOnly()) {
            return Streams.ofValues(allocator.adopt(allocationContext, wrapInnerSourceDictionary(output.borrow(Stream.VALUES))));
        }

        Streams.Builder wrapped = Streams.builder();
        if (output.hasValues()) {
            wrapped.put(Stream.VALUES, allocator.adopt(allocationContext, wrapInnerSourceDictionary(output.borrow(Stream.VALUES))));
        }
        if (output.hasNulls()) {
            wrapped.put(Stream.NULLS, allocator.adopt(allocationContext, wrapInnerSourceDictionary(output.borrow(Stream.NULLS))));
        }
        if (output.hasErrors()) {
            wrapped.put(Stream.ERRORS, allocator.adopt(allocationContext, wrapInnerSourceDictionary(output.borrow(Stream.ERRORS))));
        }
        return wrapped.build();
    }

    private static Streams sideStreamValues(Streams streams)
    {
        Streams.Builder sideInput = Streams.builder();
        if (streams.hasNulls()) {
            sideInput.put(Stream.NULLS, streams.get(Stream.NULLS));
        }
        if (streams.hasErrors()) {
            sideInput.put(Stream.ERRORS, streams.get(Stream.ERRORS));
        }
        return sideInput.build();
    }

    private static DictionaryVector wrapComposedDictionary(int[] dictionaryIds, Vector values)
    {
        return wrapComposedDictionary(dictionaryIds, values, false);
    }

    /**
     * Rewrites {@code dictionaryIds} in place, collapsing nested dictionary/RLE encodings so the result is a single
     * dictionary over the base values. When {@code idsMonotonic} is set (the ids ascend, as the outer/probe positions
     * do -- one output row per match, in probe order), an RLE level resolves run indices with a forward-advancing hint
     * (amortized O(1) per position, O(runs) total) instead of a per-position binary search (O(positions * log runs));
     * for a many-run RLE that is a large win, and for a few-run RLE it is never worse. A dictionary level remaps
     * through arbitrary ids and so breaks the ordering; monotonicity is dropped there. Run indices of ascending
     * positions stay ascending, so a nested RLE keeps the fast path.
     */
    private static DictionaryVector wrapComposedDictionary(int[] dictionaryIds, Vector values, boolean idsMonotonic)
    {
        Vector baseValues = values;
        boolean monotonic = idsMonotonic;
        while (true) {
            if (baseValues instanceof DictionaryVector dictionary) {
                int[] baseIds = dictionary.ids();
                for (int index = 0; index < dictionaryIds.length; index++) {
                    dictionaryIds[index] = baseIds[dictionaryIds[index]];
                }
                baseValues = dictionary.values();
                monotonic = false;
                continue;
            }

            if (baseValues instanceof org.weakref.nitro.data.RleVector rle) {
                if (monotonic && RLE_RUN_INDEX_HINT) {
                    int hint = 0;
                    for (int index = 0; index < dictionaryIds.length; index++) {
                        hint = rle.runIndexFromHint(dictionaryIds[index], hint);
                        dictionaryIds[index] = hint;
                    }
                }
                else {
                    for (int index = 0; index < dictionaryIds.length; index++) {
                        dictionaryIds[index] = rle.runIndex(dictionaryIds[index]);
                    }
                }
                baseValues = rle.values();
                continue;
            }

            break;
        }
        return DictionaryVector.wrap(dictionaryIds, baseValues);
    }

    /**
     * Materializes a non-retained build column's boolean side stream (nulls or errors) for the output.
     * When the source is entirely false it is emitted as a 1-run all-false RLE of the output length --
     * O(1), no per-position id array -- keeping the stream present (its presence is a fixed schema
     * contract, so it cannot simply be dropped) while avoiding the dictionary wrap. Downstream null
     * propagation recognizes the all-false RLE shape (see {@link
     * VectorAccess#isAllFalseNulls}). Otherwise the stream is
     * wrapped as a dictionary over the matched logical positions, like the values alongside it.
     */
    private Vector wrapInnerSideStream(Vector source)
    {
        if (isKnownAllFalseSource(source)) {
            BooleanVector sentinel = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
            return allocator.allocateSingleRunRle(allocationContext, currentOutputCount, sentinel);
        }
        return allocator.adopt(allocationContext, wrapInnerLogicalDictionary(source));
    }

    /**
     * Whether a build column's boolean side stream is entirely false. Peels dictionary/RLE encodings to
     * the underlying boolean run values and relies on {@link BooleanVector#isAllFalse()}, which caches
     * its result -- and the buffered build column is one reused instance across every output batch, so
     * the scan happens at most once per column.
     */
    private static boolean isKnownAllFalseSource(Vector stream)
    {
        return switch (stream) {
            case BooleanVector booleans -> booleans.isAllFalse();
            case DictionaryVector dictionary -> isKnownAllFalseSource(dictionary.values());
            case org.weakref.nitro.data.RleVector rle -> isKnownAllFalseSource(rle.values());
            default -> false;
        };
    }

    private static Streams sideStreamValues(Output output)
    {
        Streams.Builder sideInput = Streams.builder();
        if (output.hasNulls()) {
            sideInput.put(Stream.NULLS, output.borrow(Stream.NULLS));
        }
        if (output.hasErrors()) {
            sideInput.put(Stream.ERRORS, output.borrow(Stream.ERRORS));
        }
        return sideInput.build();
    }

    private Streams copyInnerPositions(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int runIndex, int innerBatchIndex, int innerOutputIndex, int positionStart, int positionCount, int size)
    {
        return copyInnerPositions(existing, innerBatch, runIndex, innerBatchIndex, innerOutputIndex, positionStart, positionCount, size, true, true, true);
    }

    private Streams copyInnerPositions(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int runIndex, int innerBatchIndex, int innerOutputIndex, int positionStart, int positionCount, int size, boolean includeValues, boolean includeNulls, boolean includeErrors)
    {
        if (!includeValues && !includeNulls && !includeErrors) {
            return existing;
        }
        if (!innerBatch.retained()) {
            Streams input = selectedStreams(innerBatch.columns()[innerOutputIndex], includeValues, includeNulls, includeErrors);
            if (input.streams().isEmpty()) {
                return existing;
            }
            return buffers.copyPositionsFresh(existing, input, outputInnerLogicalPositions, positionStart, positionCount, positionStart, size, innerPositionMappingCache);
        }

        constrainRetainedInnerBatch(innerBatchIndex, innerBatch, outputInnerRunUniqueStarts[runIndex], outputInnerRunUniqueCounts[runIndex]);
        Output output = innerBatch.retainedBatch().output(innerOutputIndex);
        Output selected = selectedStreams(output, includeValues, includeNulls, includeErrors);
        if (selected == null) {
            return existing;
        }
        return buffers.copyPositionsFresh(selected, existing, innerSourcePositions(), positionStart, positionCount, positionStart, size, innerPositionMappingCache);
    }

    private static Streams selectedStreams(Streams input, boolean includeValues, boolean includeNulls, boolean includeErrors)
    {
        Streams.Builder selected = Streams.builder();
        if (includeValues && input.hasValues()) {
            selected.put(Stream.VALUES, input.values());
        }
        if (includeNulls && input.hasNulls()) {
            selected.put(Stream.NULLS, input.get(Stream.NULLS));
        }
        if (includeErrors && input.hasErrors()) {
            selected.put(Stream.ERRORS, input.get(Stream.ERRORS));
        }
        return selected.build();
    }

    private static Output selectedStreams(Output input, boolean includeValues, boolean includeNulls, boolean includeErrors)
    {
        Set<Stream> selected = EnumSet.noneOf(Stream.class);
        if (includeValues && input.hasValues()) {
            selected.add(Stream.VALUES);
        }
        if (includeNulls && input.hasNulls()) {
            selected.add(Stream.NULLS);
        }
        if (includeErrors && input.hasErrors()) {
            selected.add(Stream.ERRORS);
        }
        if (selected.isEmpty()) {
            return null;
        }
        return input.select(selected);
    }

    private Streams copyInnerSinglePosition(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int innerBatchIndex, int innerOutputIndex, int size, int outputPosition, int logicalPosition, boolean exposeNulls)
    {
        if (!innerBatch.retained()) {
            return withSyntheticNulls(existing, buffers.copySinglePositionFresh(existing, innerBatch.columns()[innerOutputIndex], size, outputPosition, logicalPosition), size, outputPosition, exposeNulls);
        }

        int sourcePosition = innerBatch.sourcePosition(logicalPosition);
        constrainRetainedInnerBatch(innerBatchIndex, innerBatch, logicalPosition);
        return withSyntheticNulls(existing, buffers.copySinglePositionFresh(innerBatch.retainedBatch().output(innerOutputIndex), existing, size, outputPosition, sourcePosition), size, outputPosition, exposeNulls);
    }

    private Streams copyNullInnerPosition(Streams existing, Streams schema, int size, int outputPosition)
    {
        Streams.Builder builder = Streams.builder();
        builder.put(Stream.VALUES, existing == null ? nullValuesLike(schema.values(), size) : existing.values());
        builder.put(Stream.NULLS, setBooleanPosition(existing == null ? null : existing.getOrNull(Stream.NULLS), size, outputPosition, true));
        if (schema.has(Stream.ERRORS)) {
            builder.put(Stream.ERRORS, setBooleanPosition(existing == null ? null : existing.getOrNull(Stream.ERRORS), size, outputPosition, false));
        }
        return builder.build();
    }

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int[] logicalPositions, int positionCount)
    {
        constrainRetainedInnerBatch(innerBatchIndex, innerBatch, logicalPositions, 0, positionCount);
    }

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int[] logicalPositions, int positionStart, int positionCount)
    {
        if (!innerBatch.retained()) {
            return;
        }
        boolean sorted = true;
        int previousSourcePosition = -1;
        int[] scratch = retainedInnerMaskPositionsScratch();
        for (int index = 0; index < positionCount; index++) {
            int sourcePosition = innerBatch.sourcePosition(logicalPositions[positionStart + index]);
            scratch[index] = sourcePosition;
            sorted &= sourcePosition >= previousSourcePosition;
            previousSourcePosition = sourcePosition;
        }
        if (!sorted) {
            Arrays.sort(scratch, 0, positionCount);
        }
        int uniqueCount = 0;
        int previous = -1;
        for (int index = 0; index < positionCount; index++) {
            int position = scratch[index];
            if (position != previous) {
                scratch[uniqueCount++] = position;
                previous = position;
            }
        }
        int cachedCount = retainedConstraintCountsByBatch[innerBatchIndex];
        int[] cachedPositions = retainedConstraintPositionsByBatch[innerBatchIndex];
        if (uniqueCount == cachedCount &&
                cachedPositions != null &&
                Arrays.equals(scratch, 0, uniqueCount, cachedPositions, 0, uniqueCount)) {
            return;
        }
        innerBatch.retainedBatch().constrain(allocator.allocateSparseMask(
                allocationContext,
                scratch,
                uniqueCount,
                innerBatch.retainedBatch().borrowMask().size()));
        if (cachedPositions == null || cachedPositions.length < uniqueCount) {
            cachedPositions = new int[Math.max(uniqueCount, 4)];
            retainedConstraintPositionsByBatch[innerBatchIndex] = cachedPositions;
        }
        System.arraycopy(scratch, 0, cachedPositions, 0, uniqueCount);
        retainedConstraintCountsByBatch[innerBatchIndex] = uniqueCount;
    }

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int uniquePositionStart, int uniqueCount)
    {
        if (!innerBatch.retained()) {
            return;
        }
        int cachedCount = retainedConstraintCountsByBatch[innerBatchIndex];
        int[] cachedPositions = retainedConstraintPositionsByBatch[innerBatchIndex];
        int[] uniqueSourcePositions = innerUniqueSourcePositions();
        if (uniqueCount == cachedCount &&
                cachedPositions != null &&
                Arrays.equals(uniqueSourcePositions, uniquePositionStart, uniquePositionStart + uniqueCount, cachedPositions, 0, uniqueCount)) {
            return;
        }
        int[] scratch = retainedInnerMaskPositionsScratch();
        System.arraycopy(uniqueSourcePositions, uniquePositionStart, scratch, 0, uniqueCount);
        innerBatch.retainedBatch().constrain(allocator.allocateSparseMask(
                allocationContext,
                scratch,
                uniqueCount,
                innerBatch.retainedBatch().borrowMask().size()));
        if (cachedPositions == null || cachedPositions.length < uniqueCount) {
            cachedPositions = new int[Math.max(uniqueCount, 4)];
            retainedConstraintPositionsByBatch[innerBatchIndex] = cachedPositions;
        }
        System.arraycopy(uniqueSourcePositions, uniquePositionStart, cachedPositions, 0, uniqueCount);
        retainedConstraintCountsByBatch[innerBatchIndex] = uniqueCount;
    }

    private int copySortedUniquePositions(int[] sourcePositions, int positionStart, int positionCount, int[] targetPositions, int targetStart)
    {
        int[] scratch = retainedInnerMaskPositionsScratch();
        System.arraycopy(sourcePositions, positionStart, scratch, 0, positionCount);
        Arrays.sort(scratch, 0, positionCount);
        int uniqueCount = 0;
        int previous = -1;
        for (int index = 0; index < positionCount; index++) {
            int position = scratch[index];
            if (uniqueCount == 0 || position != previous) {
                targetPositions[targetStart + uniqueCount] = position;
                previous = position;
                uniqueCount++;
            }
        }
        return uniqueCount;
    }

    private void prepareInnerOutputRuns()
    {
        if (preparedInnerRunCount >= 0) {
            return;
        }

        if (outputInnerLogicalPositionsReady) {
            int batchIndex = outputInnerLogicalPositionsBatchIndex;
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex);
            outputInnerRunStarts[0] = 0;
            outputInnerRunLengths[0] = currentOutputCount;
            outputInnerRunBatchIndexes[0] = batchIndex;
            outputInnerRunUniqueStarts[0] = 0;
            if (innerBatch.retained()) {
                int[] innerSourcePositions = innerSourcePositions();
                for (int index = 0; index < currentOutputCount; index++) {
                    innerSourcePositions[index] = innerBatch.sourcePosition(outputInnerLogicalPositions[index]);
                }
                outputInnerRunUniqueCounts[0] = copySortedUniquePositions(innerSourcePositions, 0, currentOutputCount, innerUniqueSourcePositions(), 0);
            }
            else {
                outputInnerRunUniqueCounts[0] = 0;
            }
            preparedInnerRunCount = 1;
            return;
        }

        long[] innerRows = outputInnerRows;
        int runCount = 0;
        int runStart = 0;
        int uniquePositionStart = 0;
        while (runStart < currentOutputCount) {
            long rowReference = innerRows[runStart];
            int batchIndex = batchIndex(rowReference);
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex);
            boolean retained = innerBatch.retained();
            int[] innerSourcePositions = retained ? innerSourcePositions() : null;

            int runEnd = runStart;
            while (runEnd < currentOutputCount && batchIndex(innerRows[runEnd]) == batchIndex) {
                int logicalPosition = rowPosition(innerRows[runEnd]);
                outputInnerLogicalPositions[runEnd] = logicalPosition;
                if (retained) {
                    innerSourcePositions[runEnd] = innerBatch.sourcePosition(logicalPosition);
                }
                runEnd++;
            }

            outputInnerRunStarts[runCount] = runStart;
            outputInnerRunLengths[runCount] = runEnd - runStart;
            outputInnerRunBatchIndexes[runCount] = batchIndex;
            if (retained) {
                int uniqueCount = copySortedUniquePositions(innerSourcePositions, runStart, runEnd - runStart, innerUniqueSourcePositions(), uniquePositionStart);
                outputInnerRunUniqueStarts[runCount] = uniquePositionStart;
                outputInnerRunUniqueCounts[runCount] = uniqueCount;
                uniquePositionStart += uniqueCount;
            }
            else {
                outputInnerRunUniqueStarts[runCount] = uniquePositionStart;
                outputInnerRunUniqueCounts[runCount] = 0;
            }
            runCount++;
            runStart = runEnd;
        }
        preparedInnerRunCount = runCount;
    }

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int logicalPosition)
    {
        if (!innerBatch.retained()) {
            return;
        }
        int sourcePosition = innerBatch.sourcePosition(logicalPosition);
        int cachedCount = retainedConstraintCountsByBatch[innerBatchIndex];
        int[] cachedPositions = retainedConstraintPositionsByBatch[innerBatchIndex];
        if (cachedCount == 1 &&
                cachedPositions != null &&
                cachedPositions[0] == sourcePosition) {
            return;
        }
        int[] scratch = retainedInnerMaskPositionsScratch();
        scratch[0] = sourcePosition;
        innerBatch.retainedBatch().constrain(allocator.allocateSparseMask(
                allocationContext,
                scratch,
                1,
                innerBatch.retainedBatch().borrowMask().size()));
        if (cachedPositions == null || cachedPositions.length == 0) {
            cachedPositions = new int[4];
            retainedConstraintPositionsByBatch[innerBatchIndex] = cachedPositions;
        }
        cachedPositions[0] = sourcePosition;
        retainedConstraintCountsByBatch[innerBatchIndex] = 1;
    }

    private void ensureRetainedConstraintCacheCapacity(int batchCount)
    {
        if (retainedConstraintCountsByBatch.length >= batchCount) {
            return;
        }
        int previousLength = retainedConstraintCountsByBatch.length;
        retainedConstraintCountsByBatch = Arrays.copyOf(retainedConstraintCountsByBatch, batchCount);
        Arrays.fill(retainedConstraintCountsByBatch, previousLength, batchCount, -1);
        retainedConstraintPositionsByBatch = Arrays.copyOf(retainedConstraintPositionsByBatch, batchCount);
    }

    private static long packRowReference(int batchIndex, int position)
    {
        return ((long) batchIndex << Integer.SIZE) | (position & 0xFFFF_FFFFL);
    }

    private int expectedInnerRowCount()
    {
        if (expectedIndexedInnerRows >= 0) {
            return Math.max(16, expectedIndexedInnerRows);
        }
        long rowCount = bufferedInner.rowCount();
        if (rowCount <= 0 && EXACT_STREAMING_BUILD_CARDINALITY) {
            rowCount = inner.exactOutputRows();
        }
        if (rowCount <= 0) {
            return 16;
        }
        return (int) Math.max(16L, Math.min(Integer.MAX_VALUE, rowCount));
    }

    private static int batchIndex(long rowReference)
    {
        return (int) (rowReference >>> Integer.SIZE);
    }

    private static int rowPosition(long rowReference)
    {
        return (int) rowReference;
    }

    private boolean hasNoMatchRows()
    {
        if (outputInnerLogicalPositionsReady) {
            return false;
        }
        for (int index = 0; index < currentOutputCount; index++) {
            if (outputInnerRows[index] == NO_MATCH_ROW_REFERENCE) {
                return true;
            }
        }
        return false;
    }

    private Set<Stream> innerOutputStreams(int innerOutputIndex)
    {
        Set<Stream> streams = bufferedInner.outputStreams(innerOutputIndex);
        if (!probeOuterJoin || streams == null || streams.contains(Stream.NULLS)) {
            return streams;
        }
        EnumSet<Stream> adjusted = EnumSet.copyOf(streams);
        adjusted.add(Stream.NULLS);
        return Set.copyOf(adjusted);
    }

    private Streams withSyntheticNulls(Streams existing, Streams streams, int size, int outputPosition, boolean exposeNulls)
    {
        if (!exposeNulls || streams.has(Stream.NULLS)) {
            return streams;
        }
        return streams;
    }

    private Set<Stream> resultKnownAllFalseStreams(int outputIndex, Set<Stream> streams)
    {
        if (streams.isEmpty()) {
            return Set.of();
        }

        int knownFlags = 0;
        if (outputIndex < outerOutputCount) {
            Output sourceOutput = currentOuterBatch.output(outputIndex);
            if (streams.contains(Stream.VALUES) && sourceOutput.isKnownAllFalse(Stream.VALUES)) {
                knownFlags |= VALUES_FLAG;
            }
            if (streams.contains(Stream.NULLS) && sourceOutput.isKnownAllFalse(Stream.NULLS)) {
                knownFlags |= NULLS_FLAG;
            }
            if (streams.contains(Stream.ERRORS) && sourceOutput.isKnownAllFalse(Stream.ERRORS)) {
                knownFlags |= ERRORS_FLAG;
            }
            return Output.streamSet(knownFlags);
        }

        int innerOutputIndex = outputIndex - outerOutputCount;
        if (streams.contains(Stream.NULLS) && innerOutputKnownAllFalseNulls(innerOutputIndex)) {
            knownFlags |= NULLS_FLAG;
        }
        if (streams.contains(Stream.ERRORS) && bufferedInner.outputKnownAllFalse(innerOutputIndex, Stream.ERRORS)) {
            knownFlags |= ERRORS_FLAG;
        }
        return Output.streamSet(knownFlags);
    }

    private boolean innerOutputKnownAllFalseNulls(int innerOutputIndex)
    {
        Set<Stream> streams = innerOutputStreams(innerOutputIndex);
        if (streams == null || !streams.contains(Stream.NULLS)) {
            return false;
        }
        if (hasNoMatchRows()) {
            return false;
        }

        Set<Stream> sourceStreams = bufferedInner.outputStreams(innerOutputIndex);
        boolean sourceExposesNulls = sourceStreams != null && sourceStreams.contains(Stream.NULLS);
        if (!sourceExposesNulls) {
            return probeOuterJoin;
        }
        return bufferedInner.outputKnownAllFalse(innerOutputIndex, Stream.NULLS);
    }

    private BooleanVector allFalseBooleanStream(int size)
    {
        return allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
    }

    private BooleanVector setBooleanPosition(Vector existing, int size, int outputPosition, boolean value)
    {
        BooleanVector vector;
        if (existing instanceof BooleanVector booleanVector) {
            vector = allocator.allocateOrGrow(allocationContext, booleanVector, BooleanVector.class, size, BooleanVector::new);
        }
        else {
            vector = allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
            if (existing != null) {
                VectorAccess.BooleanValues existingValues = VectorAccess.booleanValues(existing);
                int existingLength = Math.min(existing.length(), size);
                for (int position = 0; position < existingLength; position++) {
                    vector.values()[position] = existingValues.value(position);
                }
            }
        }
        vector.values()[outputPosition] = value;
        return vector;
    }

    private Streams ensureNullStream(Streams schema)
    {
        if (schema.has(Stream.NULLS)) {
            return schema;
        }
        return schema.with(Stream.NULLS, new BooleanVector(0));
    }

    private Vector nullValuesLike(Vector sample, int size)
    {
        return switch (sample) {
            case org.weakref.nitro.data.I64Vector _ -> allocator.allocate(allocationContext, org.weakref.nitro.data.I64Vector.class, size, org.weakref.nitro.data.I64Vector::new);
            case org.weakref.nitro.data.I32Vector _ -> allocator.allocate(allocationContext, org.weakref.nitro.data.I32Vector.class, size, org.weakref.nitro.data.I32Vector::new);
            case org.weakref.nitro.data.F64Vector _ -> allocator.allocate(allocationContext, org.weakref.nitro.data.F64Vector.class, size, org.weakref.nitro.data.F64Vector::new);
            case BooleanVector _ -> allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
            case org.weakref.nitro.data.BinaryVector binary -> {
                org.weakref.nitro.data.BinaryVector values = org.weakref.nitro.data.BinaryVector.allocate(allocator, allocationContext, size, 0);
                values.addTraits(binary.traits());
                yield values;
            }
            case org.weakref.nitro.data.DictionaryVector dictionary -> nullValuesLike(dictionary.values(), size);
            case org.weakref.nitro.data.RleVector rle -> nullValuesLike(rle.values(), size);
            case org.weakref.nitro.data.ArrayVector array -> {
                org.weakref.nitro.data.ArrayVector values = allocator.allocateArray(allocationContext, size);
                values.setElements(buffers.emptyLike(array.elements()));
                yield values;
            }
            case org.weakref.nitro.data.MapVector map -> {
                org.weakref.nitro.data.MapVector values = allocator.allocateMap(allocationContext, size);
                values.setEntries(buffers.emptyLike(map.keys()), buffers.emptyLike(map.values()));
                yield values;
            }
            case org.weakref.nitro.data.StructVector struct -> {
                org.weakref.nitro.data.StructVector values = allocator.allocate(allocationContext, org.weakref.nitro.data.StructVector.class, size, org.weakref.nitro.data.StructVector::new);
                for (Map.Entry<String, Streams> field : struct.fields().entrySet()) {
                    values.setField(field.getKey(), buffers.emptyLike(field.getValue()));
                }
                yield values;
            }
            default -> throw new IllegalArgumentException("Unsupported null materialization type: " + sample.getClass().getSimpleName());
        };
    }

    private static boolean isSingleLongJoinCandidate(Vector values)
    {
        FlatTypeHandler handler = FlatTypeHandlers.forVector(values);
        return handler != null && handler.kind() == FlatTypeHandler.Kind.LONG;
    }

    private static final class JoinScratch
    {
        private final int[] outputOuterPositions;
        private final long[] outputInnerRows;
        private final int[] outputInnerLogicalPositions;
        private final int[] outputInnerRunStarts;
        private final int[] outputInnerRunLengths;
        private final int[] outputInnerRunBatchIndexes;
        private final int[] outputInnerRunUniqueStarts;
        private final int[] outputInnerRunUniqueCounts;
        private final int[] preparedOuterPositions;
        private final long[] preparedSingleRefs;
        private final int[] preparedSingleRefs32;
        private final int[] preparedRangeStarts;
        private final int[] preparedRangeCounts;
        private int[] outputInnerSourcePositions;
        private int[] outputInnerUniqueSourcePositions;
        private int[] retainedInnerMaskPositionsScratch;

        private JoinScratch(int capacity)
        {
            outputOuterPositions = new int[capacity];
            outputInnerRows = new long[capacity];
            outputInnerLogicalPositions = new int[capacity];
            outputInnerRunStarts = new int[capacity];
            outputInnerRunLengths = new int[capacity];
            outputInnerRunBatchIndexes = new int[capacity];
            outputInnerRunUniqueStarts = new int[capacity];
            outputInnerRunUniqueCounts = new int[capacity];
            preparedOuterPositions = new int[capacity];
            preparedSingleRefs = new long[capacity];
            preparedSingleRefs32 = new int[capacity];
            preparedRangeStarts = new int[capacity];
            preparedRangeCounts = new int[capacity];
        }

        private long retainedBytes()
        {
            long bytes = (long) outputOuterPositions.length * (11 * Integer.BYTES + 2 * Long.BYTES);
            if (outputInnerSourcePositions != null) {
                bytes += (long) outputInnerSourcePositions.length * Integer.BYTES;
            }
            if (outputInnerUniqueSourcePositions != null) {
                bytes += (long) outputInnerUniqueSourcePositions.length * Integer.BYTES;
            }
            if (retainedInnerMaskPositionsScratch != null) {
                bytes += (long) retainedInnerMaskPositionsScratch.length * Integer.BYTES;
            }
            return bytes;
        }
    }

    private interface JoinIndex
    {
        boolean isEmpty();

        void add(Vector[] values, Vector[] nulls, int position, long rowReference);

        LongList matches(Vector[] values, Vector[] nulls, int position);

        default void addNoNulls(Vector[] values, int position, long rowReference)
        {
            add(values, NO_NULL_STREAMS, position, rowReference);
        }

        default LongList matchesNoNulls(Vector[] values, int position)
        {
            return matches(values, NO_NULL_STREAMS, position);
        }

        /**
         * Batched probe entry point. Looks up matches for {@code positionCount} outer rows listed in
         * {@code positions}, writing results into {@code matches} (which may reuse the supplied
         * {@code singleMatches} reusable wrappers for single-row matches).
         *
         * <p>Implementations should override this to hoist any Vector type dispatch once per call
         * rather than paying it per position. The default implementation delegates to the
         * per-position {@link #matches} / {@link #matchesNoNulls} entry points and exists so that
         * join-index implementations that have not yet been batch-aware continue to work — the
         * operator always calls this method.
         */
        default void matchRows(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                LongList result = hasNulls ? matches(values, nulls, position) : matchesNoNulls(values, position);
                if (result instanceof SingleLongList single) {
                    matches[index] = singleMatches[index].withValue(single.getLong(0));
                }
                else {
                    matches[index] = result;
                }
            }
        }

        /**
         * Probe a compacted one-to-many index into contiguous row-reference ranges. Returns false when this index
         * cannot expose stable ranges, in which case the operator falls back to {@link #matchRows}.
         */
        default boolean matchRowRanges(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, int[] starts, int[] counts)
        {
            return false;
        }

        default boolean supportsRowRanges()
        {
            return false;
        }

        default void copyRowRange(int start, long[] output, int outputOffset, int length)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * Whether this index has at most one build row per key (a unique build), so a probe can write a
         * single build row reference per outer row into a flat {@code long[]} instead of a {@link LongList}.
         * When true the operator uses {@link #matchSingleRows} and a flat output path. Off by default.
         */
        default boolean supportsSingleMatchRefs()
        {
            return false;
        }

        /**
         * Flat single-match probe: writes the matching build row reference for each outer row into
         * {@code refs} (or {@code NO_MATCH_ROW_REFERENCE} when the key has no match or is null). Only
         * called when {@link #supportsSingleMatchRefs()} is true.
         */
        default void matchSingleRows(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, long[] refs)
        {
            throw new UnsupportedOperationException();
        }

        default boolean supportsCompactSingleMatchRefs()
        {
            return false;
        }

        default boolean supportsSingleMatchPositions()
        {
            return false;
        }

        default boolean supportsSingleMatchPositionRange()
        {
            return false;
        }

        default int singleMatchPositionBatchIndex()
        {
            throw new UnsupportedOperationException();
        }

        default void matchSingleRowsPositions(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, int[] logicalPositions)
        {
            throw new UnsupportedOperationException();
        }

        default void matchSingleRowsPositionsRange(Vector[] values, Vector[] nulls, boolean hasNulls, int startPosition, int positionCount, int[] logicalPositions)
        {
            throw new UnsupportedOperationException();
        }

        default boolean supportsDirectSingleMatchPositionRangeOutput()
        {
            return false;
        }

        default int emitSingleRowsPositionsRange(Vector[] values, Vector[] nulls, boolean hasNulls, int startPosition, int positionCount, int[] outputOuterPositions, int[] outputInnerLogicalPositions, int outputStart)
        {
            throw new UnsupportedOperationException();
        }

        default void matchSingleRowsCompact(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, int[] refs)
        {
            throw new UnsupportedOperationException();
        }

        default long unpackCompactSingleMatchRef(int ref)
        {
            throw new UnsupportedOperationException();
        }

        default void releaseBuffers() {}
    }

    private static final class LongJoinIndex
            implements JoinIndex
    {
        private static final float LOAD_FACTOR = 0.75f;
        private static final int INITIAL_HASH_EXPECTED_CAP = Integer.getInteger("nitro.join.initialHashExpectedCap", 1 << 18);
        private static final int EMPTY = -1;
        private static final boolean DENSE_BUILD_FAST_PATH = Boolean.parseBoolean(System.getProperty("nitro.join.denseBuildFastPath", "true"));
        private static final boolean COMPACT_DIRECT_ROW_REFERENCES = Boolean.parseBoolean(System.getProperty("nitro.join.compactDirectRowReferences", "true"));
        private static final boolean COMPACT_CHAIN_ROW_REFERENCES = Boolean.parseBoolean(System.getProperty("nitro.join.compactChainRowReferences", "true"));
        private static final boolean PRE_SIZE_CAPPED_ROW_STORAGE =
                Boolean.parseBoolean(System.getProperty("nitro.join.preSizeCappedRowStorage", "true"));
        private static final boolean COMPUTE_DENSE_SINGLE_BATCH_ROW_REFERENCES =
                Boolean.parseBoolean(System.getProperty("nitro.join.computeDenseSingleBatchRowReferences", "true"));
        private static final boolean DENSE_SINGLE_BATCH_PROBE_SPECIALIZATION =
                Boolean.parseBoolean(System.getProperty("nitro.join.denseSingleBatchProbeSpecialization", "true"));
        private static final boolean DENSE_SINGLE_BATCH_MATCH_POSITIONS =
                Boolean.parseBoolean(System.getProperty("nitro.join.denseSingleBatchMatchPositions", "true"));
        private static final boolean DENSE_SINGLE_BATCH_RANGE_PROBE =
                Boolean.parseBoolean(System.getProperty("nitro.join.denseSingleBatchRangeProbe", "true"));
        private static final boolean COMPACT_DENSE_SINGLE_MATCH_REFS =
                Boolean.parseBoolean(System.getProperty("nitro.join.compactDenseSingleMatchRefs", "true"));
        private static final boolean DENSE_DICTIONARY_PROBE_CACHE =
                Boolean.parseBoolean(System.getProperty("nitro.join.denseDictionaryProbeCache", "true"));
        private static final boolean GROUPED_HASH_TABLE =
                Boolean.parseBoolean(System.getProperty("nitro.join.groupedLongHashTable", "true"));
        private static final boolean SPARSE_AWARE_HASH_LAYOUT =
                Boolean.parseBoolean(System.getProperty("nitro.join.sparseAwareLongHashLayout", "true"));
        private static final int SPARSE_AWARE_SCALAR_MIN_ROWS =
                Integer.getInteger("nitro.join.sparseAwareScalarMinRows", 1 << 20);
        private static final boolean LAZY_UNIQUE_CHAIN_STATE =
                Boolean.parseBoolean(System.getProperty("nitro.join.lazyUniqueChainState", "true"));
        private static final boolean SPARSE_DIRECT_DUPLICATE_STATE =
                Boolean.parseBoolean(System.getProperty("nitro.join.sparseDirectDuplicateState", "true"));
        private static final boolean COMPRESSED_DIRECT_BUILD_BATCH_LOOP =
                Boolean.parseBoolean(System.getProperty("nitro.join.compressedDirectBuildBatchLoop", "true"));
        private static final int SPARSE_DIRECT_DUPLICATE_MIN_EXPECTED_DOMAIN_RATIO =
                Integer.getInteger("nitro.join.sparseDirectDuplicateMinExpectedDomainRatio", 4);
        private static final int SPARSE_DIRECT_DUPLICATE_MIN_EXPECTED_ROWS =
                Integer.getInteger("nitro.join.sparseDirectDuplicateMinExpectedRows", 1 << 20);
        private static final boolean DEBUG_DIRECT_DUPLICATE_STATE =
                Boolean.getBoolean("nitro.join.debugDirectDuplicateState");
        private static final boolean DEBUG_COMPRESSED_DIRECT_RANGE =
                Boolean.getBoolean("nitro.join.debugCompressedDirectRange");
        private static final boolean COMPRESSED_DIRECT_RANGE =
                Boolean.parseBoolean(System.getProperty("nitro.join.compressedDirectRange", "true"));
        private static final int COMPRESSED_DIRECT_RANGE_MIN_KEYS =
                Integer.getInteger("nitro.join.compressedDirectRangeMinKeys", 1 << 20);
        private static final int COMPRESSED_DIRECT_RANGE_MAX_ENTRIES =
                Integer.getInteger("nitro.join.compressedDirectRangeMaxEntries", 1 << 24);
        private static final int COMPRESSED_DIRECT_RANGE_MAX_RATIO =
                Integer.getInteger("nitro.join.compressedDirectRangeMaxRatio", 6);
        private static final boolean SPARSE_RANGE_MEMBERSHIP =
                Boolean.parseBoolean(System.getProperty("nitro.join.sparseLongRangeMembership", "true"));
        private static final int SPARSE_RANGE_MIN_RATIO =
                Integer.getInteger("nitro.join.sparseLongRangeMinRatio", 4);
        private static final VectorSpecies<Byte> HASH_TAG_SPECIES = ByteVector.SPECIES_128;
        private static final int HASH_TAG_GROUP = HASH_TAG_SPECIES.length();
        private static final int NO_MATCH_ROW_REFERENCE32 = -1;
        private static final int MAX_PACKED_BATCH_INDEX = 0x7FFF;
        private static final int MAX_PACKED_ROW_POSITION = 0xFFFF;
        // A large key-only build does not need payload columns, and an exact non-negative range map can be built
        // without first paying for an open-addressed hash table. Keys outside the bounded domain fall back to the
        // ordinary hash representation; duplicates retain their exact multiplicity through the existing direct
        // builder. Keep small builds on the sequential detector/hash path, where a speculative range map is not
        // amortized.
        private static final boolean KEY_ONLY_DIRECT_RANGE_BUILD =
                Boolean.parseBoolean(System.getProperty("nitro.join.keyOnlyDirectRangeBuild", "true"));
        private static final int KEY_ONLY_DIRECT_RANGE_MIN_ROWS =
                Integer.getInteger("nitro.join.keyOnlyDirectRangeMinRows", 1 << 20);
        private final PrimitiveArrayPool arrayPool;

        // Open-addressing table of distinct keys; a slot is occupied iff slotHead[slot] != EMPTY.
        private long[] keys;
        private byte[] tags;
        private int[] slotHead;
        private int[] slotTail;
        private int[] slotCount;
        // Build rows, indexed by a dense ordinal: rowReferences[o] with chainNext[o] linking each key's
        // rows in insertion (FIFO) order. One flat int[] chain replaces a per-key growable list.
        private long[] rowReferences;
        private int[] compactRowReferences;
        private int[] chainNext;
        private int rowCount;
        private int rowCapacity;
        private final boolean preferCompactRowReferences;
        private final boolean buildRowReferencesUnused;
        // A capped build was classified from its observed shape as bounded or duplicate-heavy. Those tables either
        // remain in direct-range form or acquire an exact membership filter, so hash probes that survive are
        // predominantly hits. A scalar linear table is cheaper for that shape and avoids allocating control tags;
        // retain grouped tags for ordinary sparse tables where rejecting negative probes is their strength.
        private final boolean groupedHashTable;
        private boolean implicitSequentialRowReferences;
        private long implicitRowReferenceBase;
        private final int initialHashCapacity;
        private int mask;
        private int maxFill;
        private int size;
        private long buildKeyAnd = -1L;
        private long buildKeyOr;
        private int maximumMatchCount;
        // Array-mode (Velox kArray-style direct addressing): when the build keys are unique and form a
        // dense integer range, a probe is a bounds check plus one array index — no hash, no probe loop.
        // Built lazily on the first probe; the hash table is the fallback for sparse or duplicate keys.
        private static final int MAX_ARRAY_RANGE = 1 << 26; // cap direct array at ~64M entries (512MB)
        private static final int MAX_DIRECT_BUILD_KEY = Integer.getInteger("nitro.join.maxDirectBuildKey", 1 << 26);
        private long minKey = Long.MAX_VALUE;
        private long maxKey = Long.MIN_VALUE;
        private boolean hasDuplicates;
        private boolean finalized;
        private boolean arrayMode;
        private long[] directRows;
        private int[] directRows32;
        // Exact membership filter for a sparse but bounded integer domain.  One bit per possible key lets a
        // predominantly-negative probe avoid the larger tag/key hash table entirely.  Unlike a Bloom filter this
        // has no false positives; the hash table is consulted only for keys whose bit is present.
        private long[] sparseMembership;
        private int sparseMembershipRange;
        private boolean rowReferencesFit32 = COMPACT_DIRECT_ROW_REFERENCES;
        private boolean denseBuildCandidate = DENSE_BUILD_FAST_PATH;
        private long denseFirstKey;
        private long denseNextKey;
        private boolean denseSingleBatchRowReferenceCandidate = COMPUTE_DENSE_SINGLE_BATCH_ROW_REFERENCES;
        private boolean denseSingleBatchRowReferenceMode;
        private int denseRowReferenceBatchIndex;
        private int denseRowReferenceFirstPosition;
        private long denseRowReferenceBase;
        private int[] denseDictionaryPositionScratch;
        // Range mode (multi-row keys): at finalize each key's chain is compacted into a contiguous slice of
        // orderedRows[rangeStart[slot] .. +slotCount[slot]) in FIFO order, so a probe reads a sequential range instead
        // of pointer-chasing chainNext (the one-to-many output loop's cost). Opt-out for A/B.
        private static final boolean COMPACT_CHAINS = Boolean.parseBoolean(System.getProperty("nitro.join.compactChains", "true"));
        private static final int COMPACT_CHAINS_MIN_PROBE_ROWS = Integer.getInteger("nitro.join.compactChainsMinProbeRows", 256);
        private static final boolean COMPRESS_KEY_ONLY_DUPLICATES = Boolean.parseBoolean(System.getProperty("nitro.join.compressKeyOnlyDuplicates", "true"));
        private static final boolean SIZE_COMPRESSED_ROW_STORAGE_BY_DISTINCT_KEYS =
                Boolean.parseBoolean(System.getProperty("nitro.join.sizeCompressedRowsByDistinctKeys", "true"));
        private final boolean compactChains;
        private final boolean compressDuplicateReferences;
        private final boolean lazyDuplicateSlotState;
        private final int expectedBuildRows;
        private boolean rangeCompacted;
        private boolean directRangeBuild;
        private int directBuildRows;
        private int[] directBuildHead;
        private int[] directBuildTail;
        private int[] directBuildCount;
        private int[] directDuplicateHead;
        private int[] directDuplicateTail;
        private int[] directDuplicateCount;
        private int directDuplicateGroups;
        private boolean sparseDirectDuplicateStateAdmitted;
        private long[] orderedRows;
        private int[] rangeStart;
        // A completed duplicate build may have invariant bits inside an otherwise sparse physical key domain.
        // Removing those bits with Long.compress produces an exact dense ordinal without teaching the join about a
        // query, column, or logical type. Each nonzero entry packs an ordered-row start + 1 in 24 bits and the match
        // count in 8 bits, preserving the same insertion-ordered slices as the ordinary compacted hash representation.
        private static final int COMPRESSED_DIRECT_START_MASK = 0x00FF_FFFF;
        private static final int COMPRESSED_DIRECT_MAX_COUNT = 0xFF;
        private int[] compressedDirectRanges;
        private long compressedDirectVariableMask;
        private long compressedDirectInvariantBits;
        private long compressedDirectMin;
        private final SingleLongList singleMatch = new SingleLongList();
        private final ChainLongList scalarChain = new ChainLongList();
        private ChainLongList[] chainMatches;

        private LongJoinIndex(
                PrimitiveArrayPool arrayPool,
                int expectedSize,
                boolean keyOnlyBuild,
                boolean capInitialHash,
                boolean groupedHashTable,
                boolean lazyDuplicateSlotState,
                boolean implicitSequentialRowReferences,
                boolean keyOnlyDirectRangeBuild,
                boolean buildRowReferencesUnused)
        {
            this.arrayPool = arrayPool;
            this.compactChains = COMPACT_CHAINS && !keyOnlyBuild;
            this.compressDuplicateReferences = keyOnlyBuild && COMPRESS_KEY_ONLY_DUPLICATES;
            this.lazyDuplicateSlotState = lazyDuplicateSlotState;
            this.expectedBuildRows = expectedSize;
            int initialExpectedSize = capInitialHash ? Math.min(expectedSize, INITIAL_HASH_EXPECTED_CAP) : expectedSize;
            int capacity = 16;
            while (capacity < initialExpectedSize / LOAD_FACTOR) {
                capacity <<= 1;
            }
            initialHashCapacity = capacity;
            // Capping the hash table protects sparse/duplicate builds from speculative over-allocation, but a
            // payload-bearing build stores one row reference and one chain link for every buffered row regardless
            // of key cardinality. Its exact row count is already known, so sizing these append-only arrays from the
            // hash cap only forces geometric growth and temporarily retains every obsolete generation in the
            // engine-owned primitive pool.
            int initialRows = Math.max(16,
                    compressDuplicateReferences && SIZE_COMPRESSED_ROW_STORAGE_BY_DISTINCT_KEYS
                            ? initialExpectedSize
                            : capInitialHash && PRE_SIZE_CAPPED_ROW_STORAGE ? expectedSize : initialExpectedSize);
            rowCapacity = initialRows;
            preferCompactRowReferences = capInitialHash && COMPACT_CHAIN_ROW_REFERENCES;
            this.buildRowReferencesUnused = buildRowReferencesUnused;
            this.groupedHashTable = GROUPED_HASH_TABLE && groupedHashTable;
            this.implicitSequentialRowReferences = implicitSequentialRowReferences;
            if (!implicitSequentialRowReferences) {
                if (preferCompactRowReferences) {
                    compactRowReferences = arrayPool.borrowInts(initialRows);
                }
                else {
                    rowReferences = arrayPool.borrowLongs(initialRows);
                }
            }
            if (!lazyDuplicateSlotState || !LAZY_UNIQUE_CHAIN_STATE) {
                chainNext = arrayPool.borrowInts(initialRows);
            }
            if (DEBUG_JOIN_INDEX && keyOnlyDirectRangeBuild) {
                System.err.printf("[key-only-direct-range-build] expected=%d%n", expectedSize);
            }
            if (capInitialHash || keyOnlyDirectRangeBuild) {
                directRangeBuild = true;
                int directCapacity = 1024;
                if (keyOnlyDirectRangeBuild) {
                    long required = Math.min((long) MAX_DIRECT_BUILD_KEY, (long) expectedSize + 1);
                    while (directCapacity < required) {
                        directCapacity <<= 1;
                    }
                }
                directBuildHead = arrayPool.borrowInts(directCapacity);
                Arrays.fill(directBuildHead, EMPTY);
                denseBuildCandidate = false;
            }
        }

        @Override
        public boolean isEmpty()
        {
            return size == 0;
        }

        @Override
        public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
        {
            if (FlatJoinIndex.hasNull(nulls, position)) {
                return;
            }
            addNoNulls(values, position, rowReference);
        }

        @Override
        public void addNoNulls(Vector[] values, int position, long rowReference)
        {
            addRow(OperatorVectorSupport.longValue(values[0], position), rowReference);
        }

        private void addRows(
                Vector values,
                Vector nulls,
                boolean hasNulls,
                BufferedJoinInput.InnerBatch batch,
                int startPosition,
                int length,
                int batchIndex)
        {
            VectorAccess.LongValues longValues = VectorAccess.longValues(values);
            VectorAccess.BooleanValues nullValues = hasNulls ? VectorAccess.booleanValues(nulls) : null;
            int endPosition = startPosition + length;
            int[] sourcePositions = batch.positions();
            if (useCompressedDirectBuildBatchLoop()) {
                observeRowReferenceRange(batchIndex, endPosition - 1);
                addCompressedDirectRangeRows(
                        longValues,
                        nullValues,
                        sourcePositions,
                        startPosition,
                        endPosition,
                        (long) batchIndex << Integer.SIZE);
                return;
            }
            if (sourcePositions == null) {
                for (int position = startPosition; position < endPosition; position++) {
                    if (nullValues == null || !nullValues.value(position)) {
                        addRow(longValues.value(position), packRowReference(batchIndex, position));
                    }
                }
                return;
            }
            for (int position = startPosition; position < endPosition; position++) {
                int sourcePosition = sourcePositions[position];
                if (nullValues == null || !nullValues.value(sourcePosition)) {
                    addRow(longValues.value(sourcePosition), packRowReference(batchIndex, position));
                }
            }
        }

        private void addRows(Vector values, Vector nulls, boolean hasNulls, Mask mask, int batchIndex)
        {
            VectorAccess.LongValues longValues = VectorAccess.longValues(values);
            VectorAccess.BooleanValues nullValues = hasNulls ? VectorAccess.booleanValues(nulls) : null;
            int count = mask.count();
            if (useCompressedDirectBuildBatchLoop()) {
                observeRowReferenceRange(batchIndex, count - 1);
                addCompressedDirectRangeRows(longValues, nullValues, mask, count, (long) batchIndex << Integer.SIZE);
                return;
            }
            for (int logicalPosition = 0; logicalPosition < count; logicalPosition++) {
                int sourcePosition = mask.all() ? logicalPosition : mask.position(logicalPosition);
                if (nullValues == null || !nullValues.value(sourcePosition)) {
                    addRow(longValues.value(sourcePosition), packRowReference(batchIndex, logicalPosition));
                }
            }
        }

        private boolean useCompressedDirectBuildBatchLoop()
        {
            // Once the dense duplicate arrays exist, sparse state can no longer be admitted. Select the compact
            // key-only loop once per source batch instead of carrying generic and sparse representation branches
            // through every remaining build row.
            return COMPRESSED_DIRECT_BUILD_BATCH_LOOP && directRangeBuild && compressDuplicateReferences && directBuildTail != null;
        }

        private void addCompressedDirectRangeRows(
                VectorAccess.LongValues values,
                VectorAccess.BooleanValues nulls,
                int[] sourcePositions,
                int startPosition,
                int endPosition,
                long rowReferenceBase)
        {
            if (sourcePositions == null) {
                for (int position = startPosition; position < endPosition; position++) {
                    if (nulls == null || !nulls.value(position)) {
                        addCompressedDirectRangeRow(values.value(position), rowReferenceBase + position);
                    }
                }
                return;
            }
            for (int position = startPosition; position < endPosition; position++) {
                int sourcePosition = sourcePositions[position];
                if (nulls == null || !nulls.value(sourcePosition)) {
                    addCompressedDirectRangeRow(values.value(sourcePosition), rowReferenceBase + position);
                }
            }
        }

        private void addCompressedDirectRangeRows(
                VectorAccess.LongValues values,
                VectorAccess.BooleanValues nulls,
                Mask mask,
                int count,
                long rowReferenceBase)
        {
            if (mask.all()) {
                for (int position = 0; position < count; position++) {
                    if (nulls == null || !nulls.value(position)) {
                        addCompressedDirectRangeRow(values.value(position), rowReferenceBase + position);
                    }
                }
                return;
            }
            for (int logicalPosition = 0; logicalPosition < count; logicalPosition++) {
                int sourcePosition = mask.position(logicalPosition);
                if (nulls == null || !nulls.value(sourcePosition)) {
                    addCompressedDirectRangeRow(values.value(sourcePosition), rowReferenceBase + logicalPosition);
                }
            }
        }

        private void addCompressedDirectRangeRow(long key, long rowReference)
        {
            buildKeyAnd &= key;
            buildKeyOr |= key;
            if (key < 0 || key >= MAX_DIRECT_BUILD_KEY) {
                addRow(key, rowReference);
                return;
            }
            minKey = Math.min(minKey, key);
            maxKey = Math.max(maxKey, key);
            directBuildRows++;
            int intKey = (int) key;
            ensureDirectBuildCapacity(intKey + 1);
            int head = directBuildHead[intKey];
            if (head != EMPTY) {
                hasDuplicates = true;
                directBuildCount[intKey]++;
                return;
            }
            ensureRowCapacity();
            int ordinal = rowCount++;
            storeRowReference(ordinal, rowReference);
            if (chainNext != null) {
                chainNext[ordinal] = EMPTY;
            }
            directBuildHead[intKey] = ordinal;
            directBuildTail[intKey] = ordinal;
            directBuildCount[intKey] = 1;
            size++;
        }

        @Override
        public LongList matches(Vector[] values, Vector[] nulls, int position)
        {
            if (FlatJoinIndex.hasNull(nulls, position)) {
                return LongLists.emptyList();
            }
            return matchesNoNulls(values, position);
        }

        @Override
        public LongList matchesNoNulls(Vector[] values, int position)
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            return rowsForKey(OperatorVectorSupport.longValue(values[0], position), singleMatch, scalarChain);
        }

        @Override
        public void matchRows(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            chainMatches();
            Vector values = valuesArray[0];
            Vector nulls = nullsArray == null ? null : nullsArray[0];
            if (!hasNulls || nulls == null) {
                // Probe key is null-free: skip the per-row null read entirely.
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchLongRowsNullFree(longValues.values(), positions, positionCount, matches, singleMatches);
                    case org.weakref.nitro.data.I32Vector intValues -> matchIntRowsNullFree(intValues.values(), positions, positionCount, matches, singleMatches);
                    case DictionaryVector dictionary -> matchDictionaryRowsNullFree(dictionary, positions, positionCount, matches, singleMatches);
                    case org.weakref.nitro.data.RleVector rle -> matchRleRows(rle, VectorAccess.booleanValues(null), positions, positionCount, matches, singleMatches);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        for (int index = 0; index < positionCount; index++) {
                            int position = positions[index];
                            matches[index] = rowsForKey(rowValues.value(position), singleMatches[index], chainMatches[index]);
                        }
                    }
                }
                return;
            }
            VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> matchLongRows(longValues.values(), nullValues, positions, positionCount, matches, singleMatches);
                case org.weakref.nitro.data.I32Vector intValues -> matchIntRows(intValues.values(), nullValues, positions, positionCount, matches, singleMatches);
                case DictionaryVector dictionary -> matchDictionaryRows(dictionary, nullValues, positions, positionCount, matches, singleMatches);
                case org.weakref.nitro.data.RleVector rle -> matchRleRows(rle, nullValues, positions, positionCount, matches, singleMatches);
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForKey(rowValues.value(position), singleMatches[index], chainMatches[index]);
                        }
                    }
                }
            }
        }

        @Override
        public boolean supportsRowRanges()
        {
            if (!DIRECT_COMPACTED_RANGE_OUTPUT) {
                return false;
            }
            if (!finalized) {
                finalizeForProbe(BATCH_SIZE);
            }
            return rangeCompacted;
        }

        @Override
        public boolean matchRowRanges(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, int[] starts, int[] counts)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            if (!rangeCompacted) {
                return false;
            }
            VectorAccess.LongValues values = VectorAccess.longValues(valuesArray[0]);
            Vector nulls = nullsArray == null ? null : nullsArray[0];
            VectorAccess.BooleanValues nullValues = hasNulls && nulls != null ? VectorAccess.booleanValues(nulls) : null;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues != null && nullValues.value(position)) {
                    starts[index] = 0;
                    counts[index] = 0;
                    continue;
                }
                setRowRange(values.value(position), index, starts, counts);
            }
            return true;
        }

        private void setRowRange(long key, int index, int[] starts, int[] counts)
        {
            if (compressedDirectRanges != null) {
                int entry = compressedDirectRangeEntry(key);
                starts[index] = entry == 0 ? 0 : (entry & COMPRESSED_DIRECT_START_MASK) - 1;
                counts[index] = entry >>> 24;
                return;
            }
            if (!sparseRangeContains(key)) {
                starts[index] = 0;
                counts[index] = 0;
                return;
            }
            int slot = findSlot(key);
            int head = slotHead[slot];
            if (head == EMPTY) {
                starts[index] = 0;
                counts[index] = 0;
                return;
            }
            starts[index] = rangeStart[slot];
            counts[index] = slotCount[slot];
        }

        @Override
        public void copyRowRange(int start, long[] output, int outputOffset, int length)
        {
            System.arraycopy(orderedRows, start, output, outputOffset, length);
        }

        private ChainLongList[] chainMatches()
        {
            if (chainMatches == null) {
                chainMatches = createChainLongLists(BATCH_SIZE);
            }
            return chainMatches;
        }

        private void matchLongRowsNullFree(long[] values, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                matches[index] = rowsForKey(values[position], singleMatches[index], chainMatches[index]);
            }
        }

        private void matchIntRowsNullFree(int[] values, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                matches[index] = rowsForKey(values[position], singleMatches[index], chainMatches[index]);
            }
        }

        private void matchDictionaryRowsNullFree(DictionaryVector values, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            int[] ids = values.ids();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        matches[index] = rowsForKey(dictionaryValues[ids[position]], singleMatches[index], chainMatches[index]);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        matches[index] = rowsForKey(dictionaryValues[ids[position]], singleMatches[index], chainMatches[index]);
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        matches[index] = rowsForKey(dictionaryValues.value(ids[position]), singleMatches[index], chainMatches[index]);
                    }
                }
            }
        }

        @Override
        public boolean supportsSingleMatchRefs()
        {
            return !hasDuplicates;
        }

        @Override
        public void matchSingleRows(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, long[] refs)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            Vector values = valuesArray[0];
            Vector nulls = nullsArray == null ? null : nullsArray[0];
            if (DENSE_SINGLE_BATCH_PROBE_SPECIALIZATION && denseSingleBatchRowReferenceMode) {
                matchDenseSingleBatchRows(values, nulls, hasNulls, positions, positionCount, refs);
                return;
            }
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchSingleLongRows(longValues.values(), nullValues, positions, positionCount, refs);
                    case org.weakref.nitro.data.I32Vector intValues -> matchSingleIntRows(intValues.values(), nullValues, positions, positionCount, refs);
                    case DictionaryVector dictionary -> matchSingleDictionaryRows(dictionary, nullValues, positions, positionCount, refs);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        for (int index = 0; index < positionCount; index++) {
                            int position = positions[index];
                            refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(rowValues.value(position));
                        }
                    }
                }
                return;
            }
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] vv = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        refs[index] = singleRef(vv[positions[index]]);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] vv = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        refs[index] = singleRef(vv[positions[index]]);
                    }
                }
                case DictionaryVector dictionary -> {
                    int[] ids = dictionary.ids();
                    switch (dictionary.values()) {
                        case org.weakref.nitro.data.I64Vector lv -> {
                            long[] dv = lv.values();
                            for (int index = 0; index < positionCount; index++) {
                                refs[index] = singleRef(dv[ids[positions[index]]]);
                            }
                        }
                        case org.weakref.nitro.data.I32Vector iv -> {
                            int[] dv = iv.values();
                            for (int index = 0; index < positionCount; index++) {
                                refs[index] = singleRef(dv[ids[positions[index]]]);
                            }
                        }
                        default -> {
                            VectorAccess.LongValues dv = VectorAccess.longValues(dictionary.values());
                            for (int index = 0; index < positionCount; index++) {
                                refs[index] = singleRef(dv.value(ids[positions[index]]));
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    for (int index = 0; index < positionCount; index++) {
                        refs[index] = singleRef(rowValues.value(positions[index]));
                    }
                }
            }
        }

        @Override
        public boolean supportsCompactSingleMatchRefs()
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            return COMPACT_DENSE_SINGLE_MATCH_REFS && denseSingleBatchRowReferenceMode && rowReferencesFit32;
        }

        @Override
        public boolean supportsSingleMatchPositions()
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            return DENSE_SINGLE_BATCH_MATCH_POSITIONS && denseSingleBatchRowReferenceMode;
        }

        @Override
        public boolean supportsSingleMatchPositionRange()
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            return DENSE_SINGLE_BATCH_RANGE_PROBE && denseSingleBatchRowReferenceMode;
        }

        @Override
        public int singleMatchPositionBatchIndex()
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            if (!denseSingleBatchRowReferenceMode) {
                throw new IllegalStateException("Position single-match refs require dense single-batch row references");
            }
            return denseRowReferenceBatchIndex;
        }

        @Override
        public void matchSingleRowsPositions(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, int[] logicalPositions)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            if (!denseSingleBatchRowReferenceMode) {
                throw new IllegalStateException("Position single-match refs require dense single-batch row references");
            }
            matchDenseSingleBatchRowsPositions(valuesArray[0], nullsArray == null ? null : nullsArray[0], hasNulls, positions, positionCount, logicalPositions);
        }

        @Override
        public void matchSingleRowsPositionsRange(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int startPosition, int positionCount, int[] logicalPositions)
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            if (!denseSingleBatchRowReferenceMode) {
                throw new IllegalStateException("Range position single-match refs require dense single-batch row references");
            }
            matchDenseSingleBatchRowsPositionsRange(valuesArray[0], nullsArray == null ? null : nullsArray[0], hasNulls, startPosition, positionCount, logicalPositions);
        }

        @Override
        public boolean supportsDirectSingleMatchPositionRangeOutput()
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            return denseSingleBatchRowReferenceMode;
        }

        @Override
        public int emitSingleRowsPositionsRange(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int startPosition, int positionCount, int[] outputOuterPositions, int[] outputInnerLogicalPositions, int outputStart)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            if (!denseSingleBatchRowReferenceMode) {
                throw new IllegalStateException("Direct range output requires dense single-batch row references");
            }
            return emitDenseSingleBatchRowsPositionsRange(
                    valuesArray[0],
                    nullsArray == null ? null : nullsArray[0],
                    hasNulls,
                    startPosition,
                    positionCount,
                    outputOuterPositions,
                    outputInnerLogicalPositions,
                    outputStart);
        }

        @Override
        public void matchSingleRowsCompact(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, int[] refs)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            if (!denseSingleBatchRowReferenceMode || !rowReferencesFit32) {
                throw new IllegalStateException("Compact single-match refs require dense single-batch row references that fit 32 bits");
            }
            matchDenseSingleBatchRowsCompact(valuesArray[0], nullsArray == null ? null : nullsArray[0], hasNulls, positions, positionCount, refs);
        }

        @Override
        public long unpackCompactSingleMatchRef(int ref)
        {
            return ref == NO_MATCH_COMPACT_ROW_REFERENCE ? NO_MATCH_ROW_REFERENCE : unpackRowReference32(ref);
        }

        private void matchDenseSingleBatchRowsPositions(Vector values, Vector nulls, boolean hasNulls, int[] positions, int positionCount, int[] logicalPositions)
        {
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchDenseSingleBatchLongRowsPositions(longValues.values(), nullValues, positions, positionCount, logicalPositions);
                    case org.weakref.nitro.data.I32Vector intValues -> matchDenseSingleBatchIntRowsPositions(intValues.values(), nullValues, positions, positionCount, logicalPositions);
                    case DictionaryVector dictionary -> matchDenseSingleBatchDictionaryRowsPositions(dictionary, nullValues, positions, positionCount, logicalPositions);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        long min = minKey;
                        long max = maxKey;
                        int firstPosition = denseRowReferenceFirstPosition;
                        for (int index = 0; index < positionCount; index++) {
                            int position = positions[index];
                            if (nullValues.value(position)) {
                                logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                                continue;
                            }
                            long key = rowValues.value(position);
                            logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                        }
                    }
                }
                return;
            }
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] vv = longValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseRowReferenceFirstPosition;
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] vv = intValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseRowReferenceFirstPosition;
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case DictionaryVector dictionary -> {
                    int[] ids = dictionary.ids();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseRowReferenceFirstPosition;
                    switch (dictionary.values()) {
                        case org.weakref.nitro.data.I64Vector lv -> {
                            long[] dv = lv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                        case org.weakref.nitro.data.I32Vector iv -> {
                            int[] dv = iv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                        default -> {
                            VectorAccess.LongValues dv = VectorAccess.longValues(dictionary.values());
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv.value(ids[positions[index]]);
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseRowReferenceFirstPosition;
                    for (int index = 0; index < positionCount; index++) {
                        long key = rowValues.value(positions[index]);
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
            }
        }

        private void matchDenseSingleBatchRowsPositionsRange(Vector values, Vector nulls, boolean hasNulls, int startPosition, int positionCount, int[] logicalPositions)
        {
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchDenseSingleBatchLongRowsPositionsRange(longValues.values(), nullValues, startPosition, positionCount, logicalPositions);
                    case org.weakref.nitro.data.I32Vector intValues -> matchDenseSingleBatchIntRowsPositionsRange(intValues.values(), nullValues, startPosition, positionCount, logicalPositions);
                    case DictionaryVector dictionary -> matchDenseSingleBatchDictionaryRowsPositionsRange(dictionary, nullValues, startPosition, positionCount, logicalPositions);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        long min = minKey;
                        long max = maxKey;
                        int firstPosition = denseRowReferenceFirstPosition;
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                                continue;
                            }
                            long key = rowValues.value(position);
                            logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                        }
                    }
                }
                return;
            }
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] vv = longValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseRowReferenceFirstPosition;
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[startPosition + index];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] vv = intValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseRowReferenceFirstPosition;
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[startPosition + index];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case DictionaryVector dictionary -> {
                    int[] ids = dictionary.ids();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseRowReferenceFirstPosition;
                    switch (dictionary.values()) {
                        case org.weakref.nitro.data.I64Vector lv -> {
                            long[] dv = lv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[startPosition + index]];
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                        case org.weakref.nitro.data.I32Vector iv -> {
                            int[] dv = iv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[startPosition + index]];
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                        default -> {
                            VectorAccess.LongValues dv = VectorAccess.longValues(dictionary.values());
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv.value(ids[startPosition + index]);
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseRowReferenceFirstPosition;
                    for (int index = 0; index < positionCount; index++) {
                        long key = rowValues.value(startPosition + index);
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
            }
        }

        private int emitDenseSingleBatchRowsPositionsRange(Vector values, Vector nulls, boolean hasNulls, int startPosition, int positionCount, int[] outputOuterPositions, int[] outputInnerLogicalPositions, int outputStart)
        {
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                return switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> emitDenseSingleBatchLongRowsPositionsRange(longValues.values(), nullValues, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                    case org.weakref.nitro.data.I32Vector intValues -> emitDenseSingleBatchIntRowsPositionsRange(intValues.values(), nullValues, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                    case DictionaryVector dictionary -> emitDenseSingleBatchDictionaryRowsPositionsRange(dictionary, nullValues, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        long min = minKey;
                        long max = maxKey;
                        int firstPosition = denseRowReferenceFirstPosition;
                        int output = outputStart;
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                continue;
                            }
                            int logicalPosition = denseSingleBatchRowPositionForKey(rowValues.value(position), min, max, firstPosition);
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                        yield output - outputStart;
                    }
                };
            }
            return switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> emitDenseSingleBatchLongRowsPositionsRange(longValues.values(), null, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                case org.weakref.nitro.data.I32Vector intValues -> emitDenseSingleBatchIntRowsPositionsRange(intValues.values(), null, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                case DictionaryVector dictionary -> emitDenseSingleBatchDictionaryRowsPositionsRange(dictionary, null, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseRowReferenceFirstPosition;
                    int output = outputStart;
                    for (int index = 0; index < positionCount; index++) {
                        int position = startPosition + index;
                        int logicalPosition = denseSingleBatchRowPositionForKey(rowValues.value(position), min, max, firstPosition);
                        if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                            outputOuterPositions[output] = position;
                            outputInnerLogicalPositions[output] = logicalPosition;
                            output++;
                        }
                    }
                    yield output - outputStart;
                }
            };
        }

        private void matchDenseSingleBatchRows(Vector values, Vector nulls, boolean hasNulls, int[] positions, int positionCount, long[] refs)
        {
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchDenseSingleBatchLongRows(longValues.values(), nullValues, positions, positionCount, refs);
                    case org.weakref.nitro.data.I32Vector intValues -> matchDenseSingleBatchIntRows(intValues.values(), nullValues, positions, positionCount, refs);
                    case DictionaryVector dictionary -> matchDenseSingleBatchDictionaryRows(dictionary, nullValues, positions, positionCount, refs);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        long min = minKey;
                        long max = maxKey;
                        long base = denseRowReferenceBase;
                        for (int index = 0; index < positionCount; index++) {
                            int position = positions[index];
                            if (nullValues.value(position)) {
                                refs[index] = NO_MATCH_ROW_REFERENCE;
                                continue;
                            }
                            long key = rowValues.value(position);
                            refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                        }
                    }
                }
                return;
            }
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] vv = longValues.values();
                    long min = minKey;
                    long max = maxKey;
                    long base = denseRowReferenceBase;
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] vv = intValues.values();
                    long min = minKey;
                    long max = maxKey;
                    long base = denseRowReferenceBase;
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
                case DictionaryVector dictionary -> {
                    int[] ids = dictionary.ids();
                    long min = minKey;
                    long max = maxKey;
                    long base = denseRowReferenceBase;
                    switch (dictionary.values()) {
                        case org.weakref.nitro.data.I64Vector lv -> {
                            long[] dv = lv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                            }
                        }
                        case org.weakref.nitro.data.I32Vector iv -> {
                            int[] dv = iv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                            }
                        }
                        default -> {
                            VectorAccess.LongValues dv = VectorAccess.longValues(dictionary.values());
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv.value(ids[positions[index]]);
                                refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    long min = minKey;
                    long max = maxKey;
                    long base = denseRowReferenceBase;
                    for (int index = 0; index < positionCount; index++) {
                        long key = rowValues.value(positions[index]);
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
            }
        }

        private void matchDenseSingleBatchRowsCompact(Vector values, Vector nulls, boolean hasNulls, int[] positions, int positionCount, int[] refs)
        {
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchDenseSingleBatchLongRowsCompact(longValues.values(), nullValues, positions, positionCount, refs);
                    case org.weakref.nitro.data.I32Vector intValues -> matchDenseSingleBatchIntRowsCompact(intValues.values(), nullValues, positions, positionCount, refs);
                    case DictionaryVector dictionary -> matchDenseSingleBatchDictionaryRowsCompact(dictionary, nullValues, positions, positionCount, refs);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        long min = minKey;
                        long max = maxKey;
                        int batch = denseRowReferenceBatchIndex;
                        int firstPosition = denseRowReferenceFirstPosition;
                        for (int index = 0; index < positionCount; index++) {
                            int position = positions[index];
                            if (nullValues.value(position)) {
                                refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                                continue;
                            }
                            long key = rowValues.value(position);
                            refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                        }
                    }
                }
                return;
            }
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] vv = longValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int batch = denseRowReferenceBatchIndex;
                    int firstPosition = denseRowReferenceFirstPosition;
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] vv = intValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int batch = denseRowReferenceBatchIndex;
                    int firstPosition = denseRowReferenceFirstPosition;
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
                case DictionaryVector dictionary -> {
                    int[] ids = dictionary.ids();
                    long min = minKey;
                    long max = maxKey;
                    int batch = denseRowReferenceBatchIndex;
                    int firstPosition = denseRowReferenceFirstPosition;
                    switch (dictionary.values()) {
                        case org.weakref.nitro.data.I64Vector lv -> {
                            long[] dv = lv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                            }
                        }
                        case org.weakref.nitro.data.I32Vector iv -> {
                            int[] dv = iv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                            }
                        }
                        default -> {
                            VectorAccess.LongValues dv = VectorAccess.longValues(dictionary.values());
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv.value(ids[positions[index]]);
                                refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    long min = minKey;
                    long max = maxKey;
                    int batch = denseRowReferenceBatchIndex;
                    int firstPosition = denseRowReferenceFirstPosition;
                    for (int index = 0; index < positionCount; index++) {
                        long key = rowValues.value(positions[index]);
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
            }
        }

        private void matchDenseSingleBatchLongRows(long[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            long min = minKey;
            long max = maxKey;
            long base = denseRowReferenceBase;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    refs[index] = NO_MATCH_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
            }
        }

        private void matchDenseSingleBatchIntRows(int[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            long min = minKey;
            long max = maxKey;
            long base = denseRowReferenceBase;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    refs[index] = NO_MATCH_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
            }
        }

        private void matchDenseSingleBatchDictionaryRows(DictionaryVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            int[] ids = values.ids();
            long min = minKey;
            long max = maxKey;
            long base = denseRowReferenceBase;
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues.value(ids[position]);
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
            }
        }

        private int emitDenseSingleBatchLongRowsPositionsRange(long[] values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] outputOuterPositions, int[] outputInnerLogicalPositions, int outputStart)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseRowReferenceFirstPosition;
            int output = outputStart;
            if (nullValues == null) {
                for (int index = 0; index < positionCount; index++) {
                    int position = startPosition + index;
                    int logicalPosition = denseSingleBatchRowPositionForKey(values[position], min, max, firstPosition);
                    if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                        outputOuterPositions[output] = position;
                        outputInnerLogicalPositions[output] = logicalPosition;
                        output++;
                    }
                }
                return output - outputStart;
            }
            for (int index = 0; index < positionCount; index++) {
                int position = startPosition + index;
                if (nullValues.value(position)) {
                    continue;
                }
                int logicalPosition = denseSingleBatchRowPositionForKey(values[position], min, max, firstPosition);
                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                    outputOuterPositions[output] = position;
                    outputInnerLogicalPositions[output] = logicalPosition;
                    output++;
                }
            }
            return output - outputStart;
        }

        private int emitDenseSingleBatchIntRowsPositionsRange(int[] values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] outputOuterPositions, int[] outputInnerLogicalPositions, int outputStart)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseRowReferenceFirstPosition;
            int output = outputStart;
            if (nullValues == null) {
                for (int index = 0; index < positionCount; index++) {
                    int position = startPosition + index;
                    int logicalPosition = denseSingleBatchRowPositionForKey(values[position], min, max, firstPosition);
                    if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                        outputOuterPositions[output] = position;
                        outputInnerLogicalPositions[output] = logicalPosition;
                        output++;
                    }
                }
                return output - outputStart;
            }
            for (int index = 0; index < positionCount; index++) {
                int position = startPosition + index;
                if (nullValues.value(position)) {
                    continue;
                }
                int logicalPosition = denseSingleBatchRowPositionForKey(values[position], min, max, firstPosition);
                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                    outputOuterPositions[output] = position;
                    outputInnerLogicalPositions[output] = logicalPosition;
                    output++;
                }
            }
            return output - outputStart;
        }

        private int emitDenseSingleBatchDictionaryRowsPositionsRange(DictionaryVector values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] outputOuterPositions, int[] outputInnerLogicalPositions, int outputStart)
        {
            int[] ids = values.ids();
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseRowReferenceFirstPosition;
            int output = outputStart;
            if (nullValues == null) {
                switch (values.values()) {
                    case org.weakref.nitro.data.I64Vector longValues -> {
                        long[] dictionaryValues = longValues.values();
                        if (useDenseDictionaryProbeCache(dictionaryValues.length, positionCount)) {
                            int[] dictionaryPositions = denseDictionaryPositions(dictionaryValues, min, max, firstPosition);
                            for (int index = 0; index < positionCount; index++) {
                                int position = startPosition + index;
                                int logicalPosition = dictionaryPositions[ids[position]];
                                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                    outputOuterPositions[output] = position;
                                    outputInnerLogicalPositions[output] = logicalPosition;
                                    output++;
                                }
                            }
                        }
                        else {
                            for (int index = 0; index < positionCount; index++) {
                                int position = startPosition + index;
                                int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues[ids[position]], min, max, firstPosition);
                                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                    outputOuterPositions[output] = position;
                                    outputInnerLogicalPositions[output] = logicalPosition;
                                    output++;
                                }
                            }
                        }
                    }
                    case org.weakref.nitro.data.I32Vector intValues -> {
                        int[] dictionaryValues = intValues.values();
                        if (useDenseDictionaryProbeCache(dictionaryValues.length, positionCount)) {
                            int[] dictionaryPositions = denseDictionaryPositions(dictionaryValues, min, max, firstPosition);
                            for (int index = 0; index < positionCount; index++) {
                                int position = startPosition + index;
                                int logicalPosition = dictionaryPositions[ids[position]];
                                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                    outputOuterPositions[output] = position;
                                    outputInnerLogicalPositions[output] = logicalPosition;
                                    output++;
                                }
                            }
                        }
                        else {
                            for (int index = 0; index < positionCount; index++) {
                                int position = startPosition + index;
                                int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues[ids[position]], min, max, firstPosition);
                                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                    outputOuterPositions[output] = position;
                                    outputInnerLogicalPositions[output] = logicalPosition;
                                    output++;
                                }
                            }
                        }
                    }
                    default -> {
                        VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues.value(ids[position]), min, max, firstPosition);
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                    }
                }
                return output - outputStart;
            }
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    if (useDenseDictionaryProbeCache(dictionaryValues.length, positionCount)) {
                        int[] dictionaryPositions = denseDictionaryPositions(dictionaryValues, min, max, firstPosition);
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                continue;
                            }
                            int logicalPosition = dictionaryPositions[ids[position]];
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                    }
                    else {
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                continue;
                            }
                            int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues[ids[position]], min, max, firstPosition);
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    if (useDenseDictionaryProbeCache(dictionaryValues.length, positionCount)) {
                        int[] dictionaryPositions = denseDictionaryPositions(dictionaryValues, min, max, firstPosition);
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                continue;
                            }
                            int logicalPosition = dictionaryPositions[ids[position]];
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                    }
                    else {
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                continue;
                            }
                            int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues[ids[position]], min, max, firstPosition);
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = startPosition + index;
                        if (nullValues.value(position)) {
                            continue;
                        }
                        int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues.value(ids[position]), min, max, firstPosition);
                        if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                            outputOuterPositions[output] = position;
                            outputInnerLogicalPositions[output] = logicalPosition;
                            output++;
                        }
                    }
                }
            }
            return output - outputStart;
        }

        private boolean useDenseDictionaryProbeCache(int dictionarySize, int positionCount)
        {
            return DENSE_DICTIONARY_PROBE_CACHE && dictionarySize * 2 <= positionCount;
        }

        private int[] denseDictionaryPositions(long[] dictionaryValues, long min, long max, int firstPosition)
        {
            int[] positions = ensureDenseDictionaryPositionScratch(dictionaryValues.length);
            for (int id = 0; id < dictionaryValues.length; id++) {
                positions[id] = denseSingleBatchRowPositionForKey(dictionaryValues[id], min, max, firstPosition);
            }
            return positions;
        }

        private int[] denseDictionaryPositions(int[] dictionaryValues, long min, long max, int firstPosition)
        {
            int[] positions = ensureDenseDictionaryPositionScratch(dictionaryValues.length);
            for (int id = 0; id < dictionaryValues.length; id++) {
                positions[id] = denseSingleBatchRowPositionForKey(dictionaryValues[id], min, max, firstPosition);
            }
            return positions;
        }

        private int[] ensureDenseDictionaryPositionScratch(int dictionarySize)
        {
            if (denseDictionaryPositionScratch == null || denseDictionaryPositionScratch.length < dictionarySize) {
                denseDictionaryPositionScratch = new int[dictionarySize];
            }
            return denseDictionaryPositionScratch;
        }

        private void matchDenseSingleBatchLongRowsPositions(long[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] logicalPositions)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseRowReferenceFirstPosition;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
            }
        }

        private void matchDenseSingleBatchIntRowsPositions(int[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] logicalPositions)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseRowReferenceFirstPosition;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
            }
        }

        private void matchDenseSingleBatchDictionaryRowsPositions(DictionaryVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] logicalPositions)
        {
            int[] ids = values.ids();
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseRowReferenceFirstPosition;
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues.value(ids[position]);
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
            }
        }

        private void matchDenseSingleBatchLongRowsPositionsRange(long[] values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] logicalPositions)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseRowReferenceFirstPosition;
            for (int index = 0; index < positionCount; index++) {
                int position = startPosition + index;
                if (nullValues.value(position)) {
                    logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
            }
        }

        private void matchDenseSingleBatchIntRowsPositionsRange(int[] values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] logicalPositions)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseRowReferenceFirstPosition;
            for (int index = 0; index < positionCount; index++) {
                int position = startPosition + index;
                if (nullValues.value(position)) {
                    logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
            }
        }

        private void matchDenseSingleBatchDictionaryRowsPositionsRange(DictionaryVector values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] logicalPositions)
        {
            int[] ids = values.ids();
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseRowReferenceFirstPosition;
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = startPosition + index;
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = startPosition + index;
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = startPosition + index;
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues.value(ids[position]);
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
            }
        }

        private void matchDenseSingleBatchLongRowsCompact(long[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] refs)
        {
            long min = minKey;
            long max = maxKey;
            int batch = denseRowReferenceBatchIndex;
            int firstPosition = denseRowReferenceFirstPosition;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
            }
        }

        private void matchDenseSingleBatchIntRowsCompact(int[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] refs)
        {
            long min = minKey;
            long max = maxKey;
            int batch = denseRowReferenceBatchIndex;
            int firstPosition = denseRowReferenceFirstPosition;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
            }
        }

        private void matchDenseSingleBatchDictionaryRowsCompact(DictionaryVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] refs)
        {
            int[] ids = values.ids();
            long min = minKey;
            long max = maxKey;
            int batch = denseRowReferenceBatchIndex;
            int firstPosition = denseRowReferenceFirstPosition;
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues.value(ids[position]);
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
            }
        }

        private void matchSingleLongRows(long[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(values[position]);
            }
        }

        private void matchSingleIntRows(int[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(values[position]);
            }
        }

        private void matchSingleDictionaryRows(DictionaryVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            int[] ids = values.ids();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(dictionaryValues[ids[position]]);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(dictionaryValues[ids[position]]);
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(dictionaryValues.value(ids[position]));
                    }
                }
            }
        }

        private long singleRef(long key)
        {
            if (directRangeBuild) {
                if (key < 0 || key >= directBuildHead.length) {
                    return NO_MATCH_ROW_REFERENCE;
                }
                int entry = directBuildHead[(int) key];
                return entry == EMPTY ? NO_MATCH_ROW_REFERENCE : rowReferenceAt(directEntryHead(entry));
            }
            if (arrayMode) {
                if (key < minKey || key > maxKey) {
                    return NO_MATCH_ROW_REFERENCE;
                }
                if (denseSingleBatchRowReferenceMode) {
                    return denseSingleBatchRowReference((int) (key - minKey));
                }
                if (directRows32 != null) {
                    int rowReference = directRows32[(int) (key - minKey)];
                    return rowReference == NO_MATCH_ROW_REFERENCE32 ? NO_MATCH_ROW_REFERENCE : unpackRowReference32(rowReference);
                }
                return directRows[(int) (key - minKey)];
            }
            if (!sparseRangeContains(key)) {
                return NO_MATCH_ROW_REFERENCE;
            }
            int slot = findSlot(key);
            int head = slotHead[slot];
            return head == EMPTY ? NO_MATCH_ROW_REFERENCE : rowReferenceAt(head);
        }

        private long denseSingleBatchRowReference(int ordinal)
        {
            return denseRowReferenceBase + ordinal;
        }

        private static long denseSingleBatchRowReferenceForKey(long key, long minKey, long maxKey, long base)
        {
            if (key < minKey || key > maxKey) {
                return NO_MATCH_ROW_REFERENCE;
            }
            return base + (key - minKey);
        }

        private static int denseSingleBatchRowPositionForKey(long key, long minKey, long maxKey, int firstPosition)
        {
            if (key < minKey || key > maxKey) {
                return NO_MATCH_COMPACT_ROW_REFERENCE;
            }
            return (int) (firstPosition + (key - minKey));
        }

        private static int denseSingleBatchRowReference32ForKey(long key, long minKey, long maxKey, int batchIndex, int firstPosition)
        {
            if (key < minKey || key > maxKey) {
                return NO_MATCH_COMPACT_ROW_REFERENCE;
            }
            int rowPosition = (int) (firstPosition + (key - minKey));
            return (batchIndex << Short.SIZE) | (rowPosition & MAX_PACKED_ROW_POSITION);
        }

        private int findSlot(long key)
        {
            ensureHashTable();
            if (groupedHashTable) {
                long hash = hash64(key);
                byte tag = hashTag(hash);
                int group = ((int) hash) & mask & ~(HASH_TAG_GROUP - 1);
                while (true) {
                    ByteVector groupTags = ByteVector.fromArray(HASH_TAG_SPECIES, tags, group);
                    long matchBits = groupTags.compare(VectorOperators.EQ, tag).toLong();
                    while (matchBits != 0) {
                        int slot = group + Long.numberOfTrailingZeros(matchBits);
                        if (keys[slot] == key) {
                            return slot;
                        }
                        matchBits &= matchBits - 1;
                    }
                    long emptyBits = groupTags.compare(VectorOperators.EQ, (byte) 0).toLong();
                    if (emptyBits != 0) {
                        return group + Long.numberOfTrailingZeros(emptyBits);
                    }
                    group = (group + HASH_TAG_GROUP) & mask;
                }
            }
            int index = mix(key) & mask;
            while (true) {
                if (slotHead[index] == EMPTY || keys[index] == key) {
                    return index;
                }
                index = (index + 1) & mask;
            }
        }

        private void ensureHashTable()
        {
            if (keys != null) {
                return;
            }
            keys = arrayPool.borrowLongs(initialHashCapacity);
            if (groupedHashTable) {
                tags = arrayPool.borrowBytes(initialHashCapacity);
                Arrays.fill(tags, (byte) 0);
            }
            slotHead = arrayPool.borrowInts(initialHashCapacity);
            Arrays.fill(slotHead, EMPTY);
            if (!lazyDuplicateSlotState) {
                slotTail = arrayPool.borrowInts(initialHashCapacity);
                slotCount = arrayPool.borrowInts(initialHashCapacity);
            }
            mask = initialHashCapacity - 1;
            maxFill = (int) (initialHashCapacity * LOAD_FACTOR);
        }

        private void rehash()
        {
            long[] previousKeys = keys;
            byte[] previousTags = tags;
            int[] previousHead = slotHead;
            int[] previousTail = slotTail;
            int[] previousCount = slotCount;
            int capacity = previousKeys.length * 2;

            keys = arrayPool.borrowLongs(capacity);
            if (groupedHashTable) {
                tags = arrayPool.borrowBytes(capacity);
                Arrays.fill(tags, (byte) 0);
            }
            slotHead = arrayPool.borrowInts(capacity);
            Arrays.fill(slotHead, EMPTY);
            if (previousTail != null) {
                slotTail = arrayPool.borrowInts(capacity);
                slotCount = arrayPool.borrowInts(capacity);
            }
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            for (int index = 0; index < previousKeys.length; index++) {
                if (previousHead[index] == EMPTY) {
                    continue;
                }
                int newIndex = findSlot(previousKeys[index]);
                keys[newIndex] = previousKeys[index];
                occupySlot(newIndex, previousKeys[index]);
                slotHead[newIndex] = previousHead[index];
                if (previousTail != null) {
                    slotTail[newIndex] = previousTail[index];
                    slotCount[newIndex] = previousCount[index];
                }
            }
            arrayPool.release(previousKeys);
            arrayPool.release(previousTags);
            arrayPool.release(previousHead);
            arrayPool.release(previousTail);
            arrayPool.release(previousCount);
        }

        private void addRow(long key, long rowReference)
        {
            buildKeyAnd &= key;
            buildKeyOr |= key;
            observeRowReference(rowReference);
            if (key < minKey) {
                minKey = key;
            }
            if (key > maxKey) {
                maxKey = key;
            }
            if (directRangeBuild) {
                if (key >= 0 && key < MAX_DIRECT_BUILD_KEY) {
                    addDirectRangeRow((int) key, rowReference);
                    return;
                }
                materializeDirectRangeBuildAsHash();
                directRangeBuild = false;
            }
            if (denseBuildCandidate) {
                if (rowCount == 0 || key == denseNextKey) {
                    appendDenseRow(key, rowReference);
                    return;
                }
                materializeDenseBuildAsHash();
                denseBuildCandidate = false;
                denseSingleBatchRowReferenceCandidate = false;
            }
            int slot = findSlot(key);
            boolean newKey = slotHead[slot] == EMPTY;
            if (!newKey) {
                hasDuplicates = true;
                ensureDuplicateSlotState();
                if (compressDuplicateReferences) {
                    maximumMatchCount = Math.max(maximumMatchCount, ++slotCount[slot]);
                    return;
                }
                ensureChainState();
            }
            ensureRowCapacity();
            int ordinal = rowCount++;
            storeRowReference(ordinal, rowReference);
            if (chainNext != null) {
                chainNext[ordinal] = EMPTY;
            }
            if (newKey) {
                keys[slot] = key;
                occupySlot(slot, key);
                slotHead[slot] = ordinal;
                if (slotTail != null) {
                    slotTail[slot] = ordinal;
                    slotCount[slot] = 1;
                }
                maximumMatchCount = Math.max(maximumMatchCount, 1);
                size++;
                // Rehash after the slot is populated so it carries a non-empty head into the new table.
                if (size >= maxFill) {
                    rehash();
                }
                return;
            }
            // Append at the tail to preserve insertion (FIFO) order within a key.
            chainNext[slotTail[slot]] = ordinal;
            slotTail[slot] = ordinal;
            maximumMatchCount = Math.max(maximumMatchCount, ++slotCount[slot]);
        }

        private void ensureDuplicateSlotState()
        {
            if (slotTail != null) {
                return;
            }
            slotTail = arrayPool.borrowInts(slotHead.length);
            slotCount = arrayPool.borrowInts(slotHead.length);
            Arrays.fill(slotTail, EMPTY);
            Arrays.fill(slotCount, 0);
            for (int slot = 0; slot < slotHead.length; slot++) {
                int head = slotHead[slot];
                if (head != EMPTY) {
                    slotTail[slot] = head;
                    slotCount[slot] = 1;
                }
            }
        }

        private void addDirectRangeRow(int key, long rowReference)
        {
            directBuildRows++;
            ensureDirectBuildCapacity(key + 1);
            int entry = directBuildHead[key];
            if (entry != EMPTY) {
                addDirectRangeDuplicate(key, entry, rowReference);
                return;
            }
            ensureRowCapacity();
            int ordinal = rowCount++;
            storeRowReference(ordinal, rowReference);
            if (chainNext != null) {
                chainNext[ordinal] = EMPTY;
            }
            directBuildHead[key] = ordinal;
            if (directBuildTail != null) {
                directBuildTail[key] = ordinal;
                directBuildCount[key] = 1;
            }
            size++;
        }

        private void addDirectRangeDuplicate(int key, int entry, long rowReference)
        {
            hasDuplicates = true;
            if (useSparseDirectDuplicateState()) {
                addSparseDirectRangeDuplicate(key, entry, rowReference);
                return;
            }
            if (compressDuplicateReferences) {
                ensureDirectDuplicateArrays();
                if (directBuildCount[key] == 0) {
                    directBuildCount[key] = 1;
                }
                directBuildCount[key]++;
                return;
            }
            ensureChainState();
            ensureRowCapacity();
            int ordinal = rowCount++;
            storeRowReference(ordinal, rowReference);
            chainNext[ordinal] = EMPTY;
            ensureDirectDuplicateArrays();
            if (directBuildCount[key] == 0) {
                directBuildTail[key] = entry;
                directBuildCount[key] = 1;
            }
            chainNext[directBuildTail[key]] = ordinal;
            directBuildTail[key] = ordinal;
            directBuildCount[key]++;
        }

        private boolean useSparseDirectDuplicateState()
        {
            return directBuildTail == null &&
                    (sparseDirectDuplicateStateAdmitted ||
                            (SPARSE_DIRECT_DUPLICATE_STATE &&
                                    expectedBuildRows >= SPARSE_DIRECT_DUPLICATE_MIN_EXPECTED_ROWS &&
                                    (long) directBuildHead.length >= (long) expectedBuildRows * SPARSE_DIRECT_DUPLICATE_MIN_EXPECTED_DOMAIN_RATIO));
        }

        private void addSparseDirectRangeDuplicate(int key, int entry, long rowReference)
        {
            sparseDirectDuplicateStateAdmitted = true;
            int group = entry < EMPTY ? decodeDirectDuplicateGroup(entry) : createDirectDuplicateGroup(key, entry);
            if (compressDuplicateReferences) {
                directDuplicateCount[group]++;
                return;
            }
            ensureChainState();
            ensureRowCapacity();
            int ordinal = rowCount++;
            storeRowReference(ordinal, rowReference);
            chainNext[ordinal] = EMPTY;
            chainNext[directDuplicateTail[group]] = ordinal;
            directDuplicateTail[group] = ordinal;
            directDuplicateCount[group]++;
        }

        private int createDirectDuplicateGroup(int key, int head)
        {
            ensureDirectDuplicateGroupCapacity(directDuplicateGroups + 1);
            int group = directDuplicateGroups++;
            directDuplicateHead[group] = head;
            directDuplicateTail[group] = head;
            directDuplicateCount[group] = 1;
            directBuildHead[key] = encodeDirectDuplicateGroup(group);
            return group;
        }

        private void ensureDirectDuplicateGroupCapacity(int required)
        {
            if (directDuplicateHead != null && required <= directDuplicateHead.length) {
                return;
            }
            int oldLength = directDuplicateHead == null ? 0 : directDuplicateHead.length;
            int newLength = Math.max(1024, oldLength * 2);
            while (newLength < required) {
                newLength *= 2;
            }
            int[] previousHead = directDuplicateHead;
            int[] previousTail = directDuplicateTail;
            int[] previousCount = directDuplicateCount;
            directDuplicateHead = arrayPool.borrowInts(newLength);
            directDuplicateTail = arrayPool.borrowInts(newLength);
            directDuplicateCount = arrayPool.borrowInts(newLength);
            if (oldLength > 0) {
                System.arraycopy(previousHead, 0, directDuplicateHead, 0, directDuplicateGroups);
                System.arraycopy(previousTail, 0, directDuplicateTail, 0, directDuplicateGroups);
                System.arraycopy(previousCount, 0, directDuplicateCount, 0, directDuplicateGroups);
            }
            arrayPool.release(previousHead);
            arrayPool.release(previousTail);
            arrayPool.release(previousCount);
        }

        private int directEntryHead(int entry)
        {
            return SPARSE_DIRECT_DUPLICATE_STATE && entry < EMPTY
                    ? directDuplicateHead[decodeDirectDuplicateGroup(entry)]
                    : entry;
        }

        private int directEntryTail(int key, int entry)
        {
            return SPARSE_DIRECT_DUPLICATE_STATE && entry < EMPTY
                    ? directDuplicateTail[decodeDirectDuplicateGroup(entry)]
                    : directBuildTail == null ? directEntryHead(entry) : directBuildTail[key];
        }

        private int directEntryCount(int key, int entry)
        {
            if (SPARSE_DIRECT_DUPLICATE_STATE && entry < EMPTY) {
                return directDuplicateCount[decodeDirectDuplicateGroup(entry)];
            }
            if (directBuildCount == null || directBuildCount[key] == 0) {
                return 1;
            }
            return directBuildCount[key];
        }

        private static int encodeDirectDuplicateGroup(int group)
        {
            return -group - 2;
        }

        private static int decodeDirectDuplicateGroup(int entry)
        {
            return -entry - 2;
        }

        private void ensureDirectDuplicateArrays()
        {
            if (directBuildTail != null) {
                return;
            }
            directBuildTail = arrayPool.borrowInts(directBuildHead.length);
            directBuildCount = arrayPool.borrowInts(directBuildHead.length);
            // PrimitiveArrayPool returns recycled storage.  These arrays are sparse maps whose zero/EMPTY
            // defaults are semantic state, so stale values from an earlier join must not be observed when the
            // first duplicate for a key arrives.
            Arrays.fill(directBuildTail, EMPTY);
            Arrays.fill(directBuildCount, 0);
        }

        private void ensureDirectBuildCapacity(int required)
        {
            if (required <= directBuildHead.length) {
                return;
            }
            int oldLength = directBuildHead.length;
            int newLength = oldLength;
            while (newLength < required) {
                newLength *= 2;
            }
            int[] previousHead = directBuildHead;
            int[] previousTail = directBuildTail;
            int[] previousCount = directBuildCount;
            directBuildHead = arrayPool.borrowInts(newLength);
            System.arraycopy(previousHead, 0, directBuildHead, 0, oldLength);
            Arrays.fill(directBuildHead, oldLength, newLength, EMPTY);
            if (previousTail != null) {
                directBuildTail = arrayPool.borrowInts(newLength);
                System.arraycopy(previousTail, 0, directBuildTail, 0, oldLength);
                Arrays.fill(directBuildTail, oldLength, newLength, EMPTY);
                directBuildCount = arrayPool.borrowInts(newLength);
                System.arraycopy(previousCount, 0, directBuildCount, 0, oldLength);
                Arrays.fill(directBuildCount, oldLength, newLength, 0);
            }
            arrayPool.release(previousHead);
            arrayPool.release(previousTail);
            arrayPool.release(previousCount);
        }

        private void materializeDirectRangeBuildAsHash()
        {
            if (hasDuplicates) {
                ensureHashTable();
                ensureDuplicateSlotState();
            }
            size = 0;
            for (int key = 0; key < directBuildHead.length; key++) {
                int entry = directBuildHead[key];
                if (entry == EMPTY) {
                    continue;
                }
                int head = directEntryHead(entry);
                int slot = findSlot(key);
                keys[slot] = key;
                occupySlot(slot, key);
                slotHead[slot] = head;
                int count = directEntryCount(key, entry);
                if (slotTail != null) {
                    slotTail[slot] = compressDuplicateReferences ? head : directEntryTail(key, entry);
                    slotCount[slot] = count;
                }
                size++;
                if (size >= maxFill) {
                    rehash();
                }
            }
            releaseDirectBuildArrays();
        }

        private void appendDenseRow(long key, long rowReference)
        {
            observeDenseSingleBatchRowReference(rowReference);
            ensureRowCapacity();
            if (rowCount == 0) {
                denseFirstKey = key;
            }
            storeRowReference(rowCount, rowReference);
            if (chainNext != null) {
                chainNext[rowCount] = EMPTY;
            }
            rowCount++;
            size++;
            denseNextKey = key + 1;
        }

        private void observeDenseSingleBatchRowReference(long rowReference)
        {
            if (!denseSingleBatchRowReferenceCandidate) {
                return;
            }
            int batchIndex = batchIndex(rowReference);
            int rowPosition = rowPosition(rowReference);
            if (rowCount == 0) {
                denseRowReferenceBatchIndex = batchIndex;
                denseRowReferenceFirstPosition = rowPosition;
                denseRowReferenceBase = rowReference;
                return;
            }
            if (batchIndex != denseRowReferenceBatchIndex ||
                    rowPosition != (long) denseRowReferenceFirstPosition + rowCount) {
                denseSingleBatchRowReferenceCandidate = false;
            }
        }

        private void materializeDenseBuildAsHash()
        {
            int previousRows = rowCount;
            long key = denseFirstKey;
            size = 0;
            for (int ordinal = 0; ordinal < previousRows; ordinal++) {
                int slot = findSlot(key++);
                keys[slot] = key - 1;
                occupySlot(slot, key - 1);
                slotHead[slot] = ordinal;
                if (slotTail != null) {
                    slotTail[slot] = ordinal;
                    slotCount[slot] = 1;
                }
                size++;
                if (size >= maxFill) {
                    rehash();
                }
            }
        }

        private LongList rowsForSlot(int slot, SingleLongList single, ChainLongList chain)
        {
            int head = slotHead[slot];
            if (head == EMPTY) {
                return LongLists.emptyList();
            }
            int count = slotCount == null ? 1 : slotCount[slot];
            if (rangeCompacted) {
                int base = rangeStart[slot];
                return count == 1 ? single.withValue(orderedRows[base]) : chain.resetRange(orderedRows, base, count);
            }
            if (count == 1) {
                return single.withValue(rowReferenceAt(head));
            }
            if (compressDuplicateReferences) {
                return chain.resetRepeated(rowReferenceAt(head), count);
            }
            return compactRowReferences != null
                    ? chain.resetCompact(compactRowReferences, chainNext, head, count)
                    : chain.reset(rowReferences, chainNext, head, count);
        }

        /**
         * Compact each key's insertion-ordered chain into a contiguous slice of {@code orderedRows}, recording the
         * per-slot start in {@code rangeStart}. Walks every chain once (the pointer-chase paid here, at finalize,
         * instead of on every probe), after which the one-to-many probe reads a sequential range.
         */
        private void compactChains()
        {
            boolean compressedCandidate = prepareCompressedDirectRanges();
            long[] ordered = arrayPool.borrowLongs(rowCount);
            int[] starts = arrayPool.borrowInts(keys.length);
            int cursor = 0;
            int group = 0;
            long compressedMin = Long.MAX_VALUE;
            long compressedMax = Long.MIN_VALUE;
            for (int slot = 0; slot < keys.length; slot++) {
                int ordinal = slotHead[slot];
                if (ordinal == EMPTY) {
                    continue;
                }
                if (!compressedCandidate) {
                    starts[slot] = cursor;
                }
                else {
                    starts[group] = slot;
                    starts[size + group] = cursor;
                    long compressed = Long.compress(keys[slot], compressedDirectVariableMask);
                    compressedMin = Math.min(compressedMin, compressed);
                    compressedMax = Math.max(compressedMax, compressed);
                    group++;
                }
                while (ordinal != EMPTY) {
                    ordered[cursor++] = rowReferenceAt(ordinal);
                    ordinal = chainNext[ordinal];
                }
            }
            if (compressedCandidate) {
                int range = (int) (compressedMax - compressedMin + 1);
                int[] direct = arrayPool.borrowInts(range);
                Arrays.fill(direct, 0);
                for (int index = 0; index < size; index++) {
                    int slot = starts[index];
                    int directOrdinal = (int) (Long.compress(keys[slot], compressedDirectVariableMask) - compressedMin);
                    direct[directOrdinal] = slotCount[slot] << 24 | (starts[size + index] + 1);
                }
                compressedDirectRanges = direct;
                compressedDirectMin = compressedMin;
                arrayPool.release(starts);
                starts = null;
                if (DEBUG_COMPRESSED_DIRECT_RANGE) {
                    System.err.printf(
                            "[compressed-direct-range] admitted rows=%d keys=%d variableBits=%d range=%d ratio=%.3f bytes=%d%n",
                            rowCount,
                            size,
                            Long.bitCount(compressedDirectVariableMask),
                            range,
                            (double) range / size,
                            (long) range * Integer.BYTES);
                }
            }
            orderedRows = ordered;
            rangeStart = starts;
            rangeCompacted = true;
            releaseRowArrays();
            if (compressedDirectRanges != null) {
                releaseHashTable();
            }
        }

        private int[] buildOrderedIntPayload(VectorAccess.LongValues values, long[] directValues, int[] sourcePositions)
        {
            if (!rangeCompacted) {
                return null;
            }
            int[] payload = arrayPool.borrowInts(rowCount);
            for (int index = 0; index < rowCount; index++) {
                int logicalPosition = rowPosition(orderedRows[index]);
                int sourcePosition = sourcePositions == null ? logicalPosition : sourcePositions[logicalPosition];
                long value = directValues == null ? values.value(sourcePosition) : directValues[sourcePosition];
                if (value != (int) value) {
                    arrayPool.release(payload);
                    return null;
                }
                payload[index] = (int) value;
            }
            return payload;
        }

        // Chooses array mode when the build is unique and its keys form a dense integer range, so the
        // probe can index a direct array by (key - minKey) instead of hashing and probing.
        private void finalizeForProbe(int initialProbeRows)
        {
            finalized = true;
            if (DEBUG_JOIN_INDEX) {
                System.err.printf(
                        "[long-join-build] expected=%d rows=%d keys=%d min=%d max=%d dense=%s direct=%s duplicates=%s implicitReferences=%s compressedDuplicates=%s%n",
                        expectedBuildRows,
                        rowCount,
                        size,
                        minKey,
                        maxKey,
                        denseBuildCandidate,
                        directRangeBuild,
                        hasDuplicates,
                        implicitSequentialRowReferences,
                        compressDuplicateReferences);
            }
            if (size == 0) {
                return;
            }
            if (directRangeBuild) {
                if (COMPACT_COMPLETED_DIRECT_RANGE_BUILD) {
                    compactCompletedDirectRangeBuild();
                }
                if (DEBUG_DIRECT_DUPLICATE_STATE && hasDuplicates) {
                    System.err.printf(
                            "[direct-duplicate-state] representation=%s rows=%d keys=%d range=%d sparseGroups=%d expectedRows=%d%n",
                            directDuplicateGroups > 0 ? "sparse" : "dense",
                            directBuildRows,
                            size,
                            directBuildHead.length,
                            directDuplicateGroups,
                            expectedBuildRows);
                }
                return;
            }
            buildSparseRangeMembership();
            if (hasDuplicates) {
                // No direct array mode with duplicate keys; compact the multi-row chains so the probe reads a
                // contiguous range instead of chasing chainNext (the one-to-many output loop's dominant cost).
                if (compactChains && initialProbeRows >= COMPACT_CHAINS_MIN_PROBE_ROWS) {
                    compactChains();
                }
                return;
            }
            long range = maxKey - minKey + 1;
            if (range <= 0 || range > MAX_ARRAY_RANGE || range > 2L * size) {
                return;
            }
            if (denseBuildCandidate && range == size && denseSingleBatchRowReferenceCandidate) {
                denseSingleBatchRowReferenceMode = true;
                arrayMode = true;
                releaseHashTable();
                releaseRowArrays();
                return;
            }
            if (rowReferencesFit32) {
                if (denseBuildCandidate && range == size) {
                    directRows32 = packDenseDirectRows32(rowReferences, rowCount);
                }
                else {
                    int[] direct = arrayPool.borrowInts((int) range);
                    Arrays.fill(direct, NO_MATCH_ROW_REFERENCE32);
                    for (int slot = 0; slot < keys.length; slot++) {
                        int head = slotHead[slot];
                        if (head != EMPTY) {
                            direct[(int) (keys[slot] - minKey)] = packRowReference32(rowReferenceAt(head));
                        }
                    }
                    directRows32 = direct;
                }
                arrayMode = true;
                releaseHashTable();
                releaseRowArrays();
                return;
            }
            if (denseBuildCandidate && range == size) {
                directRows = rowReferences;
                rowReferences = null;
                arrayMode = true;
                releaseHashTable();
                releaseRowArrays();
                return;
            }
            long[] direct = arrayPool.borrowLongs((int) range);
            Arrays.fill(direct, NO_MATCH_ROW_REFERENCE);
            for (int slot = 0; slot < keys.length; slot++) {
                int head = slotHead[slot];
                if (head != EMPTY) {
                    direct[(int) (keys[slot] - minKey)] = rowReferenceAt(head);
                }
            }
            directRows = direct;
            arrayMode = true;
            // The hash table and chain are no longer consulted in array mode.
            releaseHashTable();
            releaseRowArrays();
        }

        /**
         * The bounded streaming builder initially uses absolute non-negative keys so it can grow without trusting
         * an estimated cardinality. Once the build is complete, its observed range may be both small and dense even
         * when the absolute keys are large. When row references are also sequential in one batch, convert that
         * proven shape to the buffered builder's arithmetic representation, which needs no lookup array.
         */
        private void compactCompletedDirectRangeBuild()
        {
            if (hasDuplicates || size < COMPACT_COMPLETED_DIRECT_RANGE_MIN_SIZE) {
                return;
            }
            long range = maxKey - minKey + 1;
            if (range <= 0 || range > MAX_ARRAY_RANGE || range > 2L * size) {
                return;
            }

            // A payload-free join needs build row references only to preserve duplicate multiplicity. Once the
            // completed build proves a unique, gap-free key range, membership is exactly a bounds check and the
            // physical build position is unobservable. Reuse the existing dense-position probe machinery with
            // synthetic positions; no query, table, column, or logical-type identity participates in admission.
            if (DENSE_UNUSED_BUILD_MEMBERSHIP &&
                    buildRowReferencesUnused &&
                    size >= DENSE_UNUSED_BUILD_MEMBERSHIP_MIN_KEYS &&
                    range == size) {
                directRangeBuild = false;
                arrayMode = true;
                denseSingleBatchRowReferenceMode = true;
                denseRowReferenceBatchIndex = 0;
                denseRowReferenceFirstPosition = 0;
                denseRowReferenceBase = 0;
                releaseDirectBuildArrays();
                releaseHashTable();
                releaseRowArrays();
                if (DEBUG_JOIN_INDEX) {
                    System.err.printf("[dense-unused-build-membership] keys=%d min=%d max=%d%n", size, minKey, maxKey);
                }
                return;
            }

            boolean sequentialReferences = range == size && denseSingleBatchRowReferenceCandidate;
            long firstReference = NO_MATCH_ROW_REFERENCE;
            int firstBatchIndex = 0;
            int firstPosition = 0;
            for (int offset = 0; offset < range; offset++) {
                int head = directBuildHead[(int) minKey + offset];
                if (head == EMPTY) {
                    sequentialReferences = false;
                    continue;
                }
                if (!sequentialReferences) {
                    continue;
                }
                long reference = rowReferenceAt(head);
                if (firstReference == NO_MATCH_ROW_REFERENCE) {
                    firstReference = reference;
                    firstBatchIndex = batchIndex(reference);
                    firstPosition = rowPosition(reference);
                }
                else if (batchIndex(reference) != firstBatchIndex || rowPosition(reference) != firstPosition + offset) {
                    sequentialReferences = false;
                }
            }
            // A shifted lookup array is not an unconditional improvement for multi-batch/non-sequential builds:
            // it adds another random table and can amplify TLB traffic. Keep the bounded construction layout unless
            // the completed shape proves that probe results can be derived arithmetically with no lookup at all.
            if (!sequentialReferences) {
                return;
            }
            directRangeBuild = false;
            arrayMode = true;
            denseSingleBatchRowReferenceMode = true;
            denseRowReferenceBatchIndex = firstBatchIndex;
            denseRowReferenceFirstPosition = firstPosition;
            denseRowReferenceBase = firstReference;
            releaseDirectBuildArrays();
            releaseHashTable();
            releaseRowArrays();
        }

        private LongList rowsForKey(long key, SingleLongList single, ChainLongList chain)
        {
            if (compressedDirectRanges != null) {
                int entry = compressedDirectRangeEntry(key);
                if (entry == 0) {
                    return LongLists.emptyList();
                }
                int start = (entry & COMPRESSED_DIRECT_START_MASK) - 1;
                int count = entry >>> 24;
                return count == 1 ? single.withValue(orderedRows[start]) : chain.resetRange(orderedRows, start, count);
            }
            if (directRangeBuild) {
                if (key < 0 || key >= directBuildHead.length) {
                    return LongLists.emptyList();
                }
                int entry = directBuildHead[(int) key];
                if (entry == EMPTY) {
                    return LongLists.emptyList();
                }
                int head = directEntryHead(entry);
                int count = directEntryCount((int) key, entry);
                if (count == 1) {
                    return single.withValue(rowReferenceAt(head));
                }
                if (compressDuplicateReferences) {
                    return chain.resetRepeated(rowReferenceAt(head), count);
                }
                return compactRowReferences != null
                        ? chain.resetCompact(compactRowReferences, chainNext, head, count)
                        : chain.reset(rowReferences, chainNext, head, count);
            }
            if (arrayMode) {
                if (key < minKey || key > maxKey) {
                    return LongLists.emptyList();
                }
                long rowReference;
                if (denseSingleBatchRowReferenceMode) {
                    rowReference = denseSingleBatchRowReference((int) (key - minKey));
                }
                else if (directRows32 != null) {
                    int compactReference = directRows32[(int) (key - minKey)];
                    rowReference = compactReference == NO_MATCH_ROW_REFERENCE32 ? NO_MATCH_ROW_REFERENCE : unpackRowReference32(compactReference);
                }
                else {
                    rowReference = directRows[(int) (key - minKey)];
                }
                return rowReference == NO_MATCH_ROW_REFERENCE ? LongLists.emptyList() : single.withValue(rowReference);
            }
            if (!sparseRangeContains(key)) {
                return LongLists.emptyList();
            }
            return rowsForSlot(findSlot(key), single, chain);
        }

        private void buildSparseRangeMembership()
        {
            if (!SPARSE_RANGE_MEMBERSHIP || keys == null || sparseMembership != null) {
                return;
            }
            long range = maxKey - minKey + 1;
            if (range <= 0 || range > MAX_ARRAY_RANGE || range < (long) size * SPARSE_RANGE_MIN_RATIO) {
                return;
            }
            sparseMembershipRange = (int) range;
            sparseMembership = arrayPool.borrowLongs((sparseMembershipRange + Long.SIZE - 1) / Long.SIZE);
            Arrays.fill(sparseMembership, 0L);
            for (int slot = 0; slot < keys.length; slot++) {
                if (slotHead[slot] == EMPTY) {
                    continue;
                }
                int ordinal = (int) (keys[slot] - minKey);
                sparseMembership[ordinal >>> 6] |= 1L << ordinal;
            }
        }

        private boolean prepareCompressedDirectRanges()
        {
            if (!COMPRESSED_DIRECT_RANGE ||
                    keys == null ||
                    size < COMPRESSED_DIRECT_RANGE_MIN_KEYS ||
                    rowCount > COMPRESSED_DIRECT_START_MASK ||
                    maximumMatchCount > COMPRESSED_DIRECT_MAX_COUNT ||
                    (long) size * 2 > keys.length) {
                return false;
            }
            long variableMask = buildKeyAnd ^ buildKeyOr;
            int variableBits = Long.bitCount(variableMask);
            if (variableBits >= Long.SIZE - 1) {
                return false;
            }
            long range = 1L << variableBits;
            if (range <= 0 ||
                    range > COMPRESSED_DIRECT_RANGE_MAX_ENTRIES ||
                    range > (long) size * COMPRESSED_DIRECT_RANGE_MAX_RATIO) {
                return false;
            }
            compressedDirectVariableMask = variableMask;
            compressedDirectInvariantBits = buildKeyAnd & ~variableMask;
            return true;
        }

        private int compressedDirectRangeEntry(long key)
        {
            long variableMask = compressedDirectVariableMask;
            if (((key ^ compressedDirectInvariantBits) & ~variableMask) != 0) {
                return 0;
            }
            long ordinal = Long.compress(key, variableMask) - compressedDirectMin;
            return ordinal >= 0 && ordinal < compressedDirectRanges.length
                    ? compressedDirectRanges[(int) ordinal]
                    : 0;
        }

        private DynamicFilter sparseDynamicFilter(int probeColumn)
        {
            buildSparseRangeMembership();
            if (sparseMembership == null) {
                return null;
            }
            return DynamicFilter.fromExactBitset(probeColumn, minKey, maxKey, sparseMembership, size);
        }

        private boolean sparseRangeContains(long key)
        {
            if (sparseMembership == null) {
                return true;
            }
            long ordinal = key - minKey;
            return ordinal >= 0 && ordinal < sparseMembershipRange &&
                    (sparseMembership[(int) ordinal >>> 6] & (1L << (int) ordinal)) != 0;
        }

        private void observeRowReference(long rowReference)
        {
            if (!rowReferencesFit32) {
                return;
            }
            if (batchIndex(rowReference) > MAX_PACKED_BATCH_INDEX || rowPosition(rowReference) > MAX_PACKED_ROW_POSITION) {
                rowReferencesFit32 = false;
            }
        }

        private void observeRowReferenceRange(int batchIndex, int maximumPosition)
        {
            if (rowReferencesFit32 &&
                    (batchIndex > MAX_PACKED_BATCH_INDEX || maximumPosition > MAX_PACKED_ROW_POSITION)) {
                rowReferencesFit32 = false;
            }
        }

        private void ensureRowCapacity()
        {
            if (rowCount < rowCapacity) {
                return;
            }
            growRowCapacity();
        }

        private void growRowCapacity()
        {
            int newCapacity = rowCapacity * 2;
            if (compactRowReferences != null) {
                int[] previous = compactRowReferences;
                compactRowReferences = arrayPool.borrowInts(newCapacity);
                System.arraycopy(previous, 0, compactRowReferences, 0, rowCount);
                arrayPool.release(previous);
            }
            else if (!implicitSequentialRowReferences) {
                long[] previous = rowReferences;
                rowReferences = arrayPool.borrowLongs(newCapacity);
                System.arraycopy(previous, 0, rowReferences, 0, rowCount);
                arrayPool.release(previous);
            }
            if (chainNext != null) {
                int[] previousChain = chainNext;
                chainNext = arrayPool.borrowInts(newCapacity);
                System.arraycopy(previousChain, 0, chainNext, 0, rowCount);
                arrayPool.release(previousChain);
            }
            rowCapacity = newCapacity;
        }

        private void ensureChainState()
        {
            if (chainNext != null) {
                return;
            }
            chainNext = arrayPool.borrowInts(rowCapacity);
            Arrays.fill(chainNext, 0, rowCount, EMPTY);
        }

        private void storeRowReference(int ordinal, long rowReference)
        {
            if (implicitSequentialRowReferences) {
                if (ordinal == 0) {
                    implicitRowReferenceBase = rowReference;
                    return;
                }
                if (rowReference == implicitRowReferenceBase + ordinal) {
                    return;
                }
                materializeImplicitRowReferences(ordinal);
            }
            if (compactRowReferences != null && rowReferencesFit32) {
                compactRowReferences[ordinal] = packRowReference32(rowReference);
                return;
            }
            if (compactRowReferences != null) {
                rowReferences = arrayPool.borrowLongs(compactRowReferences.length);
                for (int index = 0; index < ordinal; index++) {
                    rowReferences[index] = unpackRowReference32(compactRowReferences[index]);
                }
                arrayPool.release(compactRowReferences);
                compactRowReferences = null;
            }
            rowReferences[ordinal] = rowReference;
        }

        private void materializeImplicitRowReferences(int count)
        {
            implicitSequentialRowReferences = false;
            if (preferCompactRowReferences && rowReferencesFit32) {
                compactRowReferences = arrayPool.borrowInts(rowCapacity);
                for (int index = 0; index < count; index++) {
                    compactRowReferences[index] = packRowReference32(implicitRowReferenceBase + index);
                }
                return;
            }
            rowReferences = arrayPool.borrowLongs(rowCapacity);
            for (int index = 0; index < count; index++) {
                rowReferences[index] = implicitRowReferenceBase + index;
            }
        }

        private long rowReferenceAt(int ordinal)
        {
            if (implicitSequentialRowReferences) {
                return implicitRowReferenceBase + ordinal;
            }
            return compactRowReferences != null ? unpackRowReference32(compactRowReferences[ordinal]) : rowReferences[ordinal];
        }

        private int[] packDenseDirectRows32(long[] rowReferences, int rowCount)
        {
            int[] packed = arrayPool.borrowInts(rowCount);
            for (int index = 0; index < rowCount; index++) {
                packed[index] = packRowReference32(rowReferences[index]);
            }
            return packed;
        }

        @Override
        public void releaseBuffers()
        {
            releaseHashTable();
            releaseRowArrays();
            releaseDirectBuildArrays();
            arrayPool.release(directRows);
            directRows = null;
            arrayPool.release(directRows32);
            directRows32 = null;
            arrayPool.release(sparseMembership);
            sparseMembership = null;
            arrayPool.release(orderedRows);
            orderedRows = null;
            arrayPool.release(rangeStart);
            rangeStart = null;
            arrayPool.release(compressedDirectRanges);
            compressedDirectRanges = null;
            arrayPool.release(denseDictionaryPositionScratch);
            denseDictionaryPositionScratch = null;
        }

        private void releaseHashTable()
        {
            arrayPool.release(keys);
            keys = null;
            arrayPool.release(tags);
            tags = null;
            arrayPool.release(slotHead);
            slotHead = null;
            arrayPool.release(slotTail);
            slotTail = null;
            arrayPool.release(slotCount);
            slotCount = null;
        }

        private void releaseRowArrays()
        {
            arrayPool.release(rowReferences);
            rowReferences = null;
            arrayPool.release(compactRowReferences);
            compactRowReferences = null;
            arrayPool.release(chainNext);
            chainNext = null;
        }

        private void releaseDirectBuildArrays()
        {
            arrayPool.release(directBuildHead);
            directBuildHead = null;
            arrayPool.release(directBuildTail);
            directBuildTail = null;
            arrayPool.release(directBuildCount);
            directBuildCount = null;
            arrayPool.release(directDuplicateHead);
            directDuplicateHead = null;
            arrayPool.release(directDuplicateTail);
            directDuplicateTail = null;
            arrayPool.release(directDuplicateCount);
            directDuplicateCount = null;
            directDuplicateGroups = 0;
            sparseDirectDuplicateStateAdmitted = false;
        }

        private static int packRowReference32(long rowReference)
        {
            return (batchIndex(rowReference) << Short.SIZE) | (rowPosition(rowReference) & MAX_PACKED_ROW_POSITION);
        }

        private static long unpackRowReference32(int rowReference)
        {
            return packRowReference(rowReference >>> Short.SIZE, rowReference & MAX_PACKED_ROW_POSITION);
        }

        private static int mix(long key)
        {
            return (int) hash64(key);
        }

        private static long hash64(long key)
        {
            long hash = key ^ (key >>> 33);
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= (hash >>> 33);
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= (hash >>> 33);
            return hash;
        }

        private static byte hashTag(long hash)
        {
            return (byte) ((hash >>> 56) | 0x80L);
        }

        private void occupySlot(int slot, long key)
        {
            if (groupedHashTable) {
                tags[slot] = hashTag(hash64(key));
            }
        }

        private void matchLongRows(long[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = rowsForKey(values[position], singleMatches[index], chainMatches[index]);
                }
            }
        }

        private void matchIntRows(int[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = rowsForKey(values[position], singleMatches[index], chainMatches[index]);
                }
            }
        }

        private void matchDictionaryRows(DictionaryVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            int[] ids = values.ids();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForKey(dictionaryValues[ids[position]], singleMatches[index], chainMatches[index]);
                        }
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForKey(dictionaryValues[ids[position]], singleMatches[index], chainMatches[index]);
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForKey(dictionaryValues.value(ids[position]), singleMatches[index], chainMatches[index]);
                        }
                    }
                }
            }
        }

        private void matchRleRows(org.weakref.nitro.data.RleVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = rowsForKey(rowValues.value(position), singleMatches[index], chainMatches[index]);
                }
            }
        }
    }

    private static final class FlatJoinIndex
            implements JoinIndex
    {
        private static final boolean DICTIONARY_PROBE_CACHE =
                Boolean.parseBoolean(System.getProperty("nitro.join.flatDictionaryProbeCache", "true"));
        private static final int DICTIONARY_PROBE_CACHE_MAX_CARDINALITY =
                Integer.getInteger("nitro.join.flatDictionaryProbeCacheMaxCardinality", 1 << 16);
        private static final int DICTIONARY_PROBE_CACHE_MIN_ROWS_PER_ENTRY =
                Integer.getInteger("nitro.join.flatDictionaryProbeCacheMinRowsPerEntry", 2);
        private final PrimitiveArrayPool arrayPool;
        private final FlatGroupingTable table;
        private final boolean primitiveSingleRows;
        private long[] singleRows;
        private LongArrayList[] duplicateRows;
        private LongArrayList[] legacyRowsByGroup;
        private final SingleLongList singleMatch = new SingleLongList();
        private boolean hasDuplicates;
        private long nextGroupId;
        private boolean debugProbeShapePrinted;
        private Vector dictionaryProbeIdentity;
        private long dictionaryProbeGeneration = -1;
        private int[] dictionaryProbeGroups;
        private final Vector[] dictionaryProbeValues = new Vector[1];
        private boolean debugDictionaryProbeCachePrinted;

        private FlatJoinIndex(FlatKeyLayout layout, int expectedSize)
        {
            this.arrayPool = layout.primitiveArrays();
            int initialSize = Math.max(16, expectedSize);
            this.primitiveSingleRows = FLAT_PRIMITIVE_SINGLE_ROWS;
            this.table = new FlatGroupingTable(layout, primitiveSingleRows ? initialSize : 1024, true);
            if (primitiveSingleRows) {
                this.singleRows = arrayPool.borrowLongs(initialSize);
            }
            else {
                this.legacyRowsByGroup = new LongArrayList[16];
            }
        }

        @Override
        public boolean isEmpty()
        {
            return nextGroupId == 0;
        }

        @Override
        public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
        {
            if (hasNull(nulls, position)) {
                return;
            }
            long newGroupId = nextGroupId;
            long groupId = table.assignGroup(values, position, newGroupId);
            if (!primitiveSingleRows) {
                if (groupId == newGroupId) {
                    ensureLegacyGroupCapacity((int) groupId);
                    nextGroupId++;
                }
                legacyRowsByGroup[(int) groupId].add(rowReference);
                return;
            }
            if (groupId == newGroupId) {
                ensureGroupCapacity((int) groupId);
                singleRows[(int) groupId] = rowReference;
                nextGroupId++;
                return;
            }
            hasDuplicates = true;
            ensureDuplicateRows();
            LongArrayList rows = duplicateRows[(int) groupId];
            if (rows == null) {
                rows = new LongArrayList();
                rows.add(singleRows[(int) groupId]);
                duplicateRows[(int) groupId] = rows;
            }
            rows.add(rowReference);
        }

        @Override
        public LongList matches(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return LongLists.emptyList();
            }
            long groupId = table.findGroup(values, position);
            if (groupId < 0 || groupId >= nextGroupId) {
                return LongLists.emptyList();
            }
            if (!primitiveSingleRows) {
                return legacyRowsByGroup[(int) groupId];
            }
            LongArrayList rows = duplicateRows == null ? null : duplicateRows[(int) groupId];
            return rows == null ? singleMatch.withValue(singleRows[(int) groupId]) : rows;
        }

        private void ensureGroupCapacity(int groupId)
        {
            if (groupId < singleRows.length) {
                return;
            }
            int newLength = Math.max(groupId + 1, singleRows.length * 2);
            long[] previous = singleRows;
            singleRows = arrayPool.borrowLongs(newLength);
            System.arraycopy(previous, 0, singleRows, 0, previous.length);
            arrayPool.release(previous);
            if (duplicateRows != null) {
                duplicateRows = Arrays.copyOf(duplicateRows, newLength);
            }
        }

        private void ensureDuplicateRows()
        {
            if (duplicateRows == null) {
                duplicateRows = new LongArrayList[singleRows.length];
            }
        }

        private void ensureLegacyGroupCapacity(int groupId)
        {
            if (groupId >= legacyRowsByGroup.length) {
                legacyRowsByGroup = Arrays.copyOf(legacyRowsByGroup, Math.max(groupId + 1, legacyRowsByGroup.length * 2));
            }
            legacyRowsByGroup[groupId] = new LongArrayList();
        }

        @Override
        public void matchRows(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            int[] dictionaryGroups = prepareDictionaryProbeCache(values, positionCount);
            DictionaryVector dictionary = dictionaryGroups == null ? null : (DictionaryVector) values[0];
            int dictionaryDepth = dictionary == null ? 0 : dictionary.dictionaryDepth();
            int[] dictionaryIds = dictionaryDepth == 1 ? dictionary.ids() : null;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (hasNulls && hasNull(nulls, position)) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                long groupId = dictionaryGroups == null
                        ? table.findGroup(values, position)
                        : dictionaryGroups[dictionaryDepth == 1 ? dictionaryIds[position] : dictionary.basePosition(position, dictionaryDepth)];
                if (groupId < 0 || groupId >= nextGroupId) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                if (!primitiveSingleRows) {
                    matches[index] = legacyRowsByGroup[(int) groupId];
                    continue;
                }
                LongArrayList rows = duplicateRows == null ? null : duplicateRows[(int) groupId];
                matches[index] = rows == null
                        ? singleMatches[index].withValue(singleRows[(int) groupId])
                        : rows;
            }
        }

        @Override
        public boolean supportsSingleMatchRefs()
        {
            return primitiveSingleRows && !hasDuplicates;
        }

        @Override
        public void matchSingleRows(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, long[] refs)
        {
            if (DEBUG_JOIN_INDEX && !debugProbeShapePrinted) {
                debugProbeShapePrinted = true;
                System.err.printf("[flat-join-probe] groups=%d rows=%d fields=%d shape=%s%n",
                        nextGroupId,
                        positionCount,
                        values.length,
                        Arrays.stream(values).map(FlatJoinIndex::probeShape).toList());
            }
            int[] dictionaryGroups = prepareDictionaryProbeCache(values, positionCount);
            DictionaryVector dictionary = dictionaryGroups == null ? null : (DictionaryVector) values[0];
            int dictionaryDepth = dictionary == null ? 0 : dictionary.dictionaryDepth();
            int[] dictionaryIds = dictionaryDepth == 1 ? dictionary.ids() : null;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (hasNulls && hasNull(nulls, position)) {
                    refs[index] = NO_MATCH_ROW_REFERENCE;
                    continue;
                }
                if (dictionaryGroups != null) {
                    int groupId = dictionaryGroups[dictionaryDepth == 1 ? dictionaryIds[position] : dictionary.basePosition(position, dictionaryDepth)];
                    refs[index] = groupId < 0 || groupId >= nextGroupId
                            ? NO_MATCH_ROW_REFERENCE
                            : singleRows[groupId];
                }
                else {
                    long groupId = table.findGroup(values, position);
                    refs[index] = groupId < 0 || groupId >= nextGroupId
                            ? NO_MATCH_ROW_REFERENCE
                            : singleRows[(int) groupId];
                }
            }
        }

        /**
         * Resolves a small encoded domain once, then maps probe rows through dictionary ids. The exact flat table
         * remains authoritative for each base entry's first lookup. Identity alone is insufficient because pooled
         * binary vectors are reused; a cache generation is valid only while both identity and content generation
         * match. Large or weakly reused dictionaries retain the ordinary row-at-a-time probe path.
         */
        private int[] prepareDictionaryProbeCache(Vector[] values, int positionCount)
        {
            if (!DICTIONARY_PROBE_CACHE ||
                    values.length != 1 ||
                    !(values[0] instanceof DictionaryVector dictionary)) {
                return null;
            }
            Vector base = dictionary.baseValues();
            if (!(base instanceof BinaryVector)) {
                return null;
            }
            int cardinality = base.length();
            long generation = base.contentGeneration();
            if (generation < 0 ||
                    cardinality == 0 ||
                    cardinality > DICTIONARY_PROBE_CACHE_MAX_CARDINALITY ||
                    (long) cardinality * DICTIONARY_PROBE_CACHE_MIN_ROWS_PER_ENTRY > positionCount) {
                return null;
            }
            if (dictionaryProbeGroups == null || dictionaryProbeGroups.length < cardinality) {
                int[] previous = dictionaryProbeGroups;
                dictionaryProbeGroups = arrayPool.borrowInts(cardinality);
                arrayPool.release(previous);
                dictionaryProbeIdentity = null;
                dictionaryProbeGeneration = -1;
            }
            if (base != dictionaryProbeIdentity || generation != dictionaryProbeGeneration) {
                dictionaryProbeValues[0] = base;
                for (int dictionaryId = 0; dictionaryId < cardinality; dictionaryId++) {
                    dictionaryProbeGroups[dictionaryId] = (int) table.findGroup(dictionaryProbeValues, dictionaryId);
                }
                dictionaryProbeIdentity = base;
                dictionaryProbeGeneration = generation;
            }
            if (DEBUG_JOIN_INDEX && !debugDictionaryProbeCachePrinted) {
                debugDictionaryProbeCachePrinted = true;
                System.err.printf("[flat-join-dictionary-cache] groups=%d rows=%d cardinality=%d depth=%d%n",
                        nextGroupId,
                        positionCount,
                        cardinality,
                        dictionary.dictionaryDepth());
            }
            return dictionaryProbeGroups;
        }

        private static String probeShape(Vector value)
        {
            if (value instanceof DictionaryVector dictionary) {
                Vector base = dictionary.baseValues();
                return "DictionaryVector(" + dictionary.length() + ",depth=" + dictionary.dictionaryDepth() +
                        ",base=" + base.getClass().getSimpleName() + '(' + base.length() +
                        ",generation=" + base.contentGeneration() + "))";
            }
            return value.getClass().getSimpleName() + '(' + value.length() + ",generation=" + value.contentGeneration() + ')';
        }

        @Override
        public void releaseBuffers()
        {
            table.releaseBuffers();
            if (singleRows != null) {
                arrayPool.release(singleRows);
            }
            if (dictionaryProbeGroups != null) {
                arrayPool.release(dictionaryProbeGroups);
            }
            singleRows = null;
            dictionaryProbeGroups = null;
            dictionaryProbeIdentity = null;
            duplicateRows = null;
            legacyRowsByGroup = null;
        }

        private static boolean hasNull(Vector[] nulls, int position)
        {
            boolean hasNull = false;
            for (Vector nullsVector : nulls) {
                if (OperatorVectorSupport.isNull(nullsVector, position)) {
                    hasNull = true;
                    break;
                }
            }
            return hasNull;
        }
    }

    private static final class LongPairJoinIndex
            implements JoinIndex
    {
        private static final float LOAD_FACTOR = 0.75f;
        private static final boolean COMPACT_KEYS =
                Boolean.parseBoolean(System.getProperty("nitro.join.compactLongPairKeys", "true"));
        private static final boolean COMPACT_KEY_ONLY_BUILD =
                Boolean.parseBoolean(System.getProperty("nitro.join.compactKeyOnlyLongPairBuild", "true"));
        // Large compact pair tables otherwise scatter both the normalized key and row reference across the full
        // hash capacity. Keep only a dense entry ordinal in each random-access slot; append exact keys and row
        // references sequentially. This is the same general separation used by native row-container hash tables,
        // while small tables retain co-located slots for one-load hit verification.
        private static final boolean DENSE_COMPACT_ENTRIES =
                Boolean.parseBoolean(System.getProperty("nitro.join.denseCompactPairEntries", "true"));
        // Indirection is repaid only once the legacy random slot payload spans hundreds of MiB. At 2^25 slots the
        // compact {key,row} table is 512 MiB; dense ordinals reduce that random footprint to 128 MiB and keep exact
        // keys/rows append-only. Smaller tables retain co-located slots, avoiding an extra load on cache-resident
        // probes. This is a physical table-size boundary, independent of query, columns, or logical data types.
        private static final int DENSE_COMPACT_MIN_CAPACITY =
                Integer.getInteger("nitro.join.denseCompactPairMinCapacity", 1 << 25);
        // A power-of-two capacity jump can also leave a large table less than half occupied. At that point dense
        // records save at least half of the random {key,row} slot footprint and repay their ordinal indirection at a
        // lower absolute boundary. Use only the exact build-row upper bound and physical capacity: ordinary well-filled
        // tables (including negative-probe-heavy layouts) keep their co-located hit path.
        private static final boolean DENSE_COMPACT_SPARSE_ENTRIES =
                Boolean.parseBoolean(System.getProperty("nitro.join.denseCompactSparsePairEntries", "true"));
        private static final int DENSE_COMPACT_SPARSE_MIN_CAPACITY =
                Integer.getInteger("nitro.join.denseCompactSparsePairMinCapacity", 1 << 23);
        // A dense payload stream from one coalesced build batch needs only the logical row position. Preserve that
        // full non-negative int domain instead of prematurely promoting at the generic 16-bit packed-position limit;
        // if a later batch appears, promote existing positions and duplicate rows exactly to ordinary long refs.
        private static final boolean COMPACT_DENSE_SINGLE_BATCH_ROW_REFERENCES =
                Boolean.parseBoolean(System.getProperty("nitro.join.compactDensePairSingleBatchRowReferences", "true"));
        // Duplicate state is lazy, so a rare duplicate must not pre-size storage from the whole build. Once a dense
        // table fills its first bounded duplicate page and duplicate rows already cover at least half the distinct-key
        // count, the build has established a long reuse horizon; jump once to the exact build-row upper bound instead
        // of retaining every geometric generation in the engine-owned pool.
        private static final boolean PRE_SIZE_DENSE_DUPLICATE_ROWS =
                Boolean.parseBoolean(System.getProperty("nitro.join.preSizeDensePairDuplicateRows", "true"));
        // A tag match is rare on negative probes. On JDK 26, converting every 16-lane VectorMask to a scalar bitset
        // is substantially more expensive than an anyTrue reduction. Guard the conversion and pay it only when a
        // candidate key must be inspected; empty-slot tests never need lane bits during probing.
        private static final boolean GUARD_TAG_MASK_CONVERSION =
                Boolean.parseBoolean(System.getProperty("nitro.join.guardPairTagMaskConversion", "true"));
        // Swiss/F14-style SIMD-tag-bucket table (cf. Velox HashTable): probing scans a GROUP of slots at a time.
        // Each slot carries a 1-byte tag (top hash bits, high bit set so 0 means empty) held in a contiguous
        // byte[] separate from the keys/rows. A probe loads GROUP tags with one vector load and compares them
        // to the wanted tag in one instruction, so a whole bucket is filtered without touching any key; the full
        // key is compared only on a tag hit. This replaces the previous per-slot open-addressing linear probe,
        // where every collision step was another full {first,second,row} cache-miss load.
        private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_128;
        private static final int GROUP = SPECIES.length();
        private final PrimitiveArrayPool arrayPool;

        private byte[] tags;
        // Co-located entry table. Compact slots contain {normalizedKey,rowReference}; wide slots contain
        // {firstKey,secondKey,rowReference}. A matching probe's key verify and row read then hit one contiguous region
        // instead of three separate long[] (firstKeys/secondKeys/singleRows). Tags stay in their own byte[].
        private long[] entries;
        private int[] entryIds;
        private long[] denseKeys;
        // Non-negative values are packed row references. Negative values encode -(duplicateGroup + 1), avoiding
        // capacity-sized head/tail/count arrays when only a small fraction of keys has duplicates.
        private int[] denseRowStates32;
        private long[] denseRowStates;
        private boolean denseRowsFit32 = true;
        private boolean denseRowsSingleBatch = COMPACT_DENSE_SINGLE_BATCH_ROW_REFERENCES;
        private int denseRowsBatchIndex = -1;
        private boolean denseCompactEntries;
        private int denseEntryCount;
        private int denseEntryCapacity;
        private final int initialDenseEntryCapacity;
        // Most warehouse keys are logically INTEGER even though the vector contract exposes longs. Pack two
        // signed-32-bit keys into one normalized long, reducing each slot from three longs to two. If a later
        // key does not fit, promote every live entry once to the full-width layout before inserting it.
        private boolean compactKeys = COMPACT_KEYS;
        private final boolean keyOnlyBuild;
        private int[] keyOnlyCounts;
        // Duplicate rows live in one pooled append-only store. Per-slot head/tail/count metadata links each key's
        // rows without allocating a LongArrayList object and backing array for every duplicate key.
        private int[] duplicateHead;
        private int[] duplicateTail;
        private int[] duplicateCount;
        private int[] duplicateNext;
        private int[] duplicateRows32;
        private long[] duplicateRows;
        private int duplicateRowCount;
        private int duplicateRowCapacity;
        private int duplicateGroupCount;
        private int duplicateGroupCapacity;
        private boolean duplicateRowsFit32 = true;
        private final int initialDuplicateRowCapacity;
        private final int expectedBuildRows;
        private int mask;
        private int maxFill;
        private int size;
        private boolean pairHasDuplicates;
        private final SingleLongList singleMatch = new SingleLongList();
        private final ChainLongList scalarChain = new ChainLongList();
        private ChainLongList[] chainMatches;
        // Reusable per-batch key gather buffers for the native (Rust) prefetching probe (see NativeProbe).
        private long[] nativeFirst;
        private long[] nativeSecond;

        private LongPairJoinIndex(PrimitiveArrayPool arrayPool, int expectedSize, boolean keyOnlyBuild, boolean capInitialHash)
        {
            this.arrayPool = arrayPool;
            this.expectedBuildRows = expectedSize;
            this.keyOnlyBuild = keyOnlyBuild && COMPACT_KEY_ONLY_BUILD;
            this.initialDuplicateRowCapacity = capInitialHash ? expectedSize : Math.min(expectedSize, LongJoinIndex.INITIAL_HASH_EXPECTED_CAP);
            int initialExpectedSize = capInitialHash ? Math.min(expectedSize, LongJoinIndex.INITIAL_HASH_EXPECTED_CAP) : expectedSize;
            this.initialDenseEntryCapacity = Math.max(16, initialExpectedSize);
            int capacity = GROUP;
            while (capacity < initialExpectedSize / LOAD_FACTOR) {
                capacity <<= 1;
            }
            denseCompactEntries = DENSE_COMPACT_ENTRIES && compactKeys && !this.keyOnlyBuild &&
                    (capacity >= DENSE_COMPACT_MIN_CAPACITY ||
                            (DENSE_COMPACT_SPARSE_ENTRIES && capacity >= DENSE_COMPACT_SPARSE_MIN_CAPACITY &&
                                    expectedSize <= capacity / 2));
            allocate(capacity);
        }

        private void allocate(int capacity)
        {
            tags = arrayPool.borrowBytes(capacity);
            Arrays.fill(tags, (byte) 0);
            if (denseCompactEntries) {
                entryIds = arrayPool.borrowInts(capacity);
                Arrays.fill(entryIds, 0);
                denseEntryCapacity = initialDenseEntryCapacity;
                denseKeys = arrayPool.borrowLongs(denseEntryCapacity);
                denseRowStates32 = arrayPool.borrowInts(denseEntryCapacity);
            }
            else {
                entries = arrayPool.borrowLongs(capacity * entryStride());
            }
            keyOnlyCounts = null;
            duplicateHead = null;
            duplicateTail = null;
            duplicateCount = null;
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public boolean isEmpty()
        {
            return size == 0;
        }

        @Override
        public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
        {
            if (FlatJoinIndex.hasNull(nulls, position)) {
                return;
            }
            addNoNulls(values, position, rowReference);
        }

        @Override
        public void addNoNulls(Vector[] values, int position, long rowReference)
        {
            addRow(
                    OperatorVectorSupport.longValue(values[0], position),
                    OperatorVectorSupport.longValue(values[1], position),
                    rowReference);
        }

        private void addRows(
                Vector[] values,
                Vector[] nulls,
                boolean hasNulls,
                BufferedJoinInput.InnerBatch batch,
                int startPosition,
                int length,
                int batchIndex)
        {
            if (denseCompactEntries) {
                addDenseRows(values, nulls, hasNulls, batch, startPosition, length, batchIndex);
                return;
            }
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.BooleanValues firstNulls = hasNulls && nulls[0] != null ? VectorAccess.booleanValues(nulls[0]) : null;
            VectorAccess.BooleanValues secondNulls = hasNulls && nulls[1] != null ? VectorAccess.booleanValues(nulls[1]) : null;
            int endPosition = startPosition + length;
            int[] sourcePositions = batch.positions();
            if (sourcePositions == null) {
                for (int position = startPosition; position < endPosition; position++) {
                    if ((firstNulls == null || !firstNulls.value(position)) &&
                            (secondNulls == null || !secondNulls.value(position))) {
                        addRow(firstValues.value(position), secondValues.value(position), packRowReference(batchIndex, position));
                    }
                }
                return;
            }
            for (int position = startPosition; position < endPosition; position++) {
                int sourcePosition = sourcePositions[position];
                if ((firstNulls == null || !firstNulls.value(sourcePosition)) &&
                        (secondNulls == null || !secondNulls.value(sourcePosition))) {
                    addRow(firstValues.value(sourcePosition), secondValues.value(sourcePosition), packRowReference(batchIndex, position));
                }
            }
        }

        private void addRows(Vector[] values, Vector[] nulls, boolean hasNulls, Mask mask, int batchIndex)
        {
            if (denseCompactEntries) {
                addDenseRows(values, nulls, hasNulls, mask, batchIndex);
                return;
            }
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.BooleanValues firstNulls = hasNulls && nulls[0] != null ? VectorAccess.booleanValues(nulls[0]) : null;
            VectorAccess.BooleanValues secondNulls = hasNulls && nulls[1] != null ? VectorAccess.booleanValues(nulls[1]) : null;
            int count = mask.count();
            for (int logicalPosition = 0; logicalPosition < count; logicalPosition++) {
                int sourcePosition = mask.all() ? logicalPosition : mask.position(logicalPosition);
                if ((firstNulls == null || !firstNulls.value(sourcePosition)) &&
                        (secondNulls == null || !secondNulls.value(sourcePosition))) {
                    addRow(firstValues.value(sourcePosition), secondValues.value(sourcePosition), packRowReference(batchIndex, logicalPosition));
                }
            }
        }

        private void addDenseRows(
                Vector[] values,
                Vector[] nulls,
                boolean hasNulls,
                BufferedJoinInput.InnerBatch batch,
                int startPosition,
                int length,
                int batchIndex)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.BooleanValues firstNulls = hasNulls && nulls[0] != null ? VectorAccess.booleanValues(nulls[0]) : null;
            VectorAccess.BooleanValues secondNulls = hasNulls && nulls[1] != null ? VectorAccess.booleanValues(nulls[1]) : null;
            int endPosition = startPosition + length;
            int[] sourcePositions = batch.positions();
            if (sourcePositions == null) {
                for (int position = startPosition; position < endPosition; position++) {
                    if ((firstNulls == null || !firstNulls.value(position)) &&
                            (secondNulls == null || !secondNulls.value(position))) {
                        addDenseRow(firstValues.value(position), secondValues.value(position), packRowReference(batchIndex, position));
                    }
                }
                return;
            }
            for (int position = startPosition; position < endPosition; position++) {
                int sourcePosition = sourcePositions[position];
                if ((firstNulls == null || !firstNulls.value(sourcePosition)) &&
                        (secondNulls == null || !secondNulls.value(sourcePosition))) {
                    addDenseRow(firstValues.value(sourcePosition), secondValues.value(sourcePosition), packRowReference(batchIndex, position));
                }
            }
        }

        private void addDenseRows(Vector[] values, Vector[] nulls, boolean hasNulls, Mask mask, int batchIndex)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.BooleanValues firstNulls = hasNulls && nulls[0] != null ? VectorAccess.booleanValues(nulls[0]) : null;
            VectorAccess.BooleanValues secondNulls = hasNulls && nulls[1] != null ? VectorAccess.booleanValues(nulls[1]) : null;
            int count = mask.count();
            for (int logicalPosition = 0; logicalPosition < count; logicalPosition++) {
                int sourcePosition = mask.all() ? logicalPosition : mask.position(logicalPosition);
                if ((firstNulls == null || !firstNulls.value(sourcePosition)) &&
                        (secondNulls == null || !secondNulls.value(sourcePosition))) {
                    addDenseRow(firstValues.value(sourcePosition), secondValues.value(sourcePosition), packRowReference(batchIndex, logicalPosition));
                }
            }
        }

        @Override
        public LongList matches(Vector[] values, Vector[] nulls, int position)
        {
            if (FlatJoinIndex.hasNull(nulls, position)) {
                return LongLists.emptyList();
            }
            return matchesNoNulls(values, position);
        }

        @Override
        public LongList matchesNoNulls(Vector[] values, int position)
        {
            long first = OperatorVectorSupport.longValue(values[0], position);
            long second = OperatorVectorSupport.longValue(values[1], position);
            int slot = probe(first, second, hash64(first, second));
            if (slot < 0) {
                return LongLists.emptyList();
            }
            return matchesForSlot(slot, singleMatch, scalarChain);
        }

        @Override
        public void matchRows(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            if (!hasNulls) {
                for (int index = 0; index < positionCount; index++) {
                    int position = positions[index];
                    long first = firstValues.value(position);
                    long second = secondValues.value(position);
                    int slot = probe(first, second, hash64(first, second));
                    if (slot < 0) {
                        matches[index] = LongLists.emptyList();
                        continue;
                    }
                    matches[index] = matchesForSlot(slot, singleMatches[index], chainMatches()[index]);
                }
                return;
            }
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (firstNulls.value(position) || secondNulls.value(position)) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                long first = firstValues.value(position);
                long second = secondValues.value(position);
                int slot = probe(first, second, hash64(first, second));
                if (slot < 0) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                matches[index] = matchesForSlot(slot, singleMatches[index], chainMatches()[index]);
            }
        }

        private LongList matchesForSlot(int slot, SingleLongList single, ChainLongList chain)
        {
            if (keyOnlyBuild) {
                int count = keyOnlyCounts == null || keyOnlyCounts[slot] == 0 ? 1 : keyOnlyCounts[slot];
                return count == 1 ? single.withValue(0) : chain.resetRepeated(0, count);
            }
            if (denseCompactEntries) {
                int ordinal = denseOrdinal(slot);
                long state = denseRowState(ordinal);
                if (state >= 0) {
                    return single.withValue(decodeDenseRowReference(state));
                }
                int group = toIntExact(-state - 1);
                return duplicateRows32 != null
                        ? denseRowsSingleBatch
                                ? chain.resetCompactSingleBatch(duplicateRows32, duplicateNext, duplicateHead[group], duplicateCount[group], denseRowsBatchIndex)
                                : chain.resetCompact(duplicateRows32, duplicateNext, duplicateHead[group], duplicateCount[group])
                        : chain.reset(duplicateRows, duplicateNext, duplicateHead[group], duplicateCount[group]);
            }
            int head = duplicateHead == null ? -1 : duplicateHead[slot];
            if (head < 0) {
                return single.withValue(rowReference(slot));
            }
            return duplicateRows32 != null
                    ? denseRowsSingleBatch
                            ? chain.resetCompactSingleBatch(duplicateRows32, duplicateNext, head, duplicateCount[slot], denseRowsBatchIndex)
                            : chain.resetCompact(duplicateRows32, duplicateNext, head, duplicateCount[slot])
                    : chain.reset(duplicateRows, duplicateNext, head, duplicateCount[slot]);
        }

        private ChainLongList[] chainMatches()
        {
            if (chainMatches == null) {
                chainMatches = createChainLongLists(BATCH_SIZE);
            }
            return chainMatches;
        }

        @Override
        public boolean supportsSingleMatchRefs()
        {
            return !pairHasDuplicates;
        }

        @Override
        public void matchSingleRows(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, long[] refs)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(valuesArray[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(valuesArray[1]);
            if (!hasNulls) {
                if (NativeProbe.ENABLED && !compactKeys && !keyOnlyBuild) {
                    if (nativeFirst == null || nativeFirst.length < positionCount) {
                        long[] previousFirst = nativeFirst;
                        long[] previousSecond = nativeSecond;
                        nativeFirst = arrayPool.borrowLongs(positionCount);
                        nativeSecond = arrayPool.borrowLongs(positionCount);
                        arrayPool.release(previousFirst);
                        arrayPool.release(previousSecond);
                    }
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        nativeFirst[index] = firstValues.value(position);
                        nativeSecond[index] = secondValues.value(position);
                    }
                    NativeProbe.probePairs(tags, entries, nativeFirst, nativeSecond, positionCount, refs, NativeProbe.DISTANCE);
                    return;
                }
                for (int index = 0; index < positionCount; index++) {
                    int position = positions[index];
                    refs[index] = singleRef(firstValues.value(position), secondValues.value(position));
                }
                return;
            }
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nullsArray[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nullsArray[1]);
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                refs[index] = firstNulls.value(position) || secondNulls.value(position)
                        ? NO_MATCH_ROW_REFERENCE
                        : singleRef(firstValues.value(position), secondValues.value(position));
            }
        }

        private long singleRef(long first, long second)
        {
            int slot = probe(first, second, hash64(first, second));
            return slot < 0 ? NO_MATCH_ROW_REFERENCE : rowReference(slot);
        }

        // Returns the slot holding (first, second), or -1 if absent. Scans GROUP tags per step: one vector load
        // plus one tag compare filters the whole bucket; a key is only read when its tag matches. A bucket with
        // any empty slot ends the search (open-addressing invariant: a present key precedes any empty in its probe
        // sequence, and there are no deletions).
        private int probe(long first, long second, long hash)
        {
            // Truncating an out-of-domain probe key could alias a compact build key. A compact table only contains
            // signed-32-bit keys, so an out-of-domain probe cannot match without first changing the build layout.
            if (compactKeys && (!fitsSignedInt(first) || !fitsSignedInt(second))) {
                return -1;
            }
            return compactKeys ? probeCompact(first, second, hash) : probeWide(first, second, hash);
        }

        private int probeCompact(long first, long second, long hash)
        {
            byte[] tagTable = tags;
            byte tag = (byte) ((hash >>> 56) | 0x80L);
            int group = ((int) hash) & mask & ~(GROUP - 1);
            long normalizedKey = normalizedKey(first, second);
            while (true) {
                ByteVector groupTags = ByteVector.fromArray(SPECIES, tagTable, group);
                long matchBits = matchingTagBits(groupTags.compare(VectorOperators.EQ, tag));
                while (matchBits != 0) {
                    int slot = group + Long.numberOfTrailingZeros(matchBits);
                    long storedKey = denseCompactEntries ? denseKeys[denseOrdinal(slot)] : entries[slot * entryStride()];
                    if (storedKey == normalizedKey) {
                        return slot;
                    }
                    matchBits &= matchBits - 1;
                }
                if (hasEmptyTag(groupTags)) {
                    return -1;
                }
                group = (group + GROUP) & mask;
            }
        }

        private int probeWide(long first, long second, long hash)
        {
            byte[] tagTable = tags;
            byte tag = (byte) ((hash >>> 56) | 0x80L);
            int group = ((int) hash) & mask & ~(GROUP - 1);
            while (true) {
                ByteVector groupTags = ByteVector.fromArray(SPECIES, tagTable, group);
                long matchBits = matchingTagBits(groupTags.compare(VectorOperators.EQ, tag));
                while (matchBits != 0) {
                    int slot = group + Long.numberOfTrailingZeros(matchBits);
                    int base = slot * entryStride();
                    if (entries[base] == first && entries[base + 1] == second) {
                        return slot;
                    }
                    matchBits &= matchBits - 1;
                }
                if (hasEmptyTag(groupTags)) {
                    return -1;
                }
                group = (group + GROUP) & mask;
            }
        }

        private void addRow(long first, long second, long rowReference)
        {
            if (compactKeys && (!fitsSignedInt(first) || !fitsSignedInt(second))) {
                promoteToWideEntries();
            }
            long hash = hash64(first, second);
            byte tag = (byte) ((hash >>> 56) | 0x80L);
            int group = ((int) hash) & mask & ~(GROUP - 1);
            while (true) {
                ByteVector groupTags = ByteVector.fromArray(SPECIES, tags, group);
                long matchBits = matchingTagBits(groupTags.compare(VectorOperators.EQ, tag));
                while (matchBits != 0) {
                    int slot = group + Long.numberOfTrailingZeros(matchBits);
                    if (keysEqual(slot, first, second)) {
                        if (keyOnlyBuild) {
                            ensureKeyOnlyCounts();
                            int count = keyOnlyCounts[slot];
                            keyOnlyCounts[slot] = count == 0 ? 2 : count + 1;
                            pairHasDuplicates = true;
                            return;
                        }
                        appendDuplicateRow(slot, denseCompactEntries ? 0 : rowReference(slot), rowReference);
                        pairHasDuplicates = true;
                        return;
                    }
                    matchBits &= matchBits - 1;
                }
                int emptyLane = firstEmptyTag(groupTags);
                if (emptyLane < GROUP) {
                    int slot = group + emptyLane;
                    tags[slot] = tag;
                    writeEntry(slot, first, second, rowReference);
                    size++;
                    if (size >= maxFill) {
                        rehash();
                    }
                    return;
                }
                group = (group + GROUP) & mask;
            }
        }

        /**
         * Build loop selected once per batch for the dense compact representation. Keeping the representation
         * decision outside the row loop lets C2 compile the concrete memory shape instead of re-testing the
         * mutable promotion state at every key comparison and insertion. A key outside the compact domain performs
         * the ordinary exact promotion and then resumes through the general path.
         */
        private void addDenseRow(long first, long second, long rowReference)
        {
            if (!denseCompactEntries) {
                addRow(first, second, rowReference);
                return;
            }
            if (!fitsSignedInt(first) || !fitsSignedInt(second)) {
                promoteDenseToWideEntries();
                addRow(first, second, rowReference);
                return;
            }
            long hash = hash64(first, second);
            byte tag = (byte) ((hash >>> 56) | 0x80L);
            int group = ((int) hash) & mask & ~(GROUP - 1);
            long normalizedKey = normalizedKey(first, second);
            while (true) {
                ByteVector groupTags = ByteVector.fromArray(SPECIES, tags, group);
                long matchBits = matchingTagBits(groupTags.compare(VectorOperators.EQ, tag));
                while (matchBits != 0) {
                    int slot = group + Long.numberOfTrailingZeros(matchBits);
                    if (denseKeys[denseOrdinal(slot)] == normalizedKey) {
                        appendDenseDuplicateRow(slot, 0, rowReference);
                        pairHasDuplicates = true;
                        return;
                    }
                    matchBits &= matchBits - 1;
                }
                int emptyLane = firstEmptyTag(groupTags);
                if (emptyLane < GROUP) {
                    int slot = group + emptyLane;
                    tags[slot] = tag;
                    appendDenseEntry(slot, normalizedKey, rowReference);
                    size++;
                    if (size >= maxFill) {
                        rehashDense();
                    }
                    return;
                }
                group = (group + GROUP) & mask;
            }
        }

        private void rehash()
        {
            if (denseCompactEntries) {
                rehashDense();
                return;
            }
            byte[] oldTags = tags;
            long[] oldEntries = entries;
            int[] oldDuplicateHead = duplicateHead;
            int[] oldDuplicateTail = duplicateTail;
            int[] oldDuplicateCount = duplicateCount;
            int[] oldCounts = keyOnlyCounts;
            boolean oldCompactKeys = compactKeys;
            allocate(oldTags.length * 2);
            if (oldDuplicateHead != null) {
                ensureDuplicateSlotState();
            }
            if (oldCounts != null) {
                ensureKeyOnlyCounts();
            }
            size = 0;
            for (int oldSlot = 0; oldSlot < oldTags.length; oldSlot++) {
                if (oldTags[oldSlot] == 0) {
                    continue;
                }
                int oldStride = oldCompactKeys ? (keyOnlyBuild ? 1 : 2) : (keyOnlyBuild ? 2 : 3);
                int oldBase = oldSlot * oldStride;
                long first = oldCompactKeys ? (int) (oldEntries[oldBase] >>> Integer.SIZE) : oldEntries[oldBase];
                long second = oldCompactKeys ? (int) oldEntries[oldBase] : oldEntries[oldBase + 1];
                long hash = hash64(first, second);
                int slot = findEmpty(hash);
                tags[slot] = (byte) ((hash >>> 56) | 0x80L);
                writeEntry(slot, first, second, keyOnlyBuild ? 0 : oldEntries[oldBase + oldStride - 1]);
                if (oldDuplicateHead != null) {
                    duplicateHead[slot] = oldDuplicateHead[oldSlot];
                    duplicateTail[slot] = oldDuplicateTail[oldSlot];
                    duplicateCount[slot] = oldDuplicateCount[oldSlot];
                }
                if (oldCounts != null) {
                    keyOnlyCounts[slot] = oldCounts[oldSlot];
                }
                size++;
            }
            arrayPool.release(oldTags);
            arrayPool.release(oldEntries);
            arrayPool.release(oldDuplicateHead);
            arrayPool.release(oldDuplicateTail);
            arrayPool.release(oldDuplicateCount);
            arrayPool.release(oldCounts);
        }

        private void rehashDense()
        {
            byte[] oldTags = tags;
            int[] oldEntryIds = entryIds;
            int capacity = oldTags.length * 2;
            tags = arrayPool.borrowBytes(capacity);
            Arrays.fill(tags, (byte) 0);
            entryIds = arrayPool.borrowInts(capacity);
            Arrays.fill(entryIds, 0);
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;
            for (int oldSlot = 0; oldSlot < oldTags.length; oldSlot++) {
                if (oldTags[oldSlot] == 0) {
                    continue;
                }
                int entryId = oldEntryIds[oldSlot];
                long key = denseKeys[entryId - 1];
                long first = (int) (key >>> Integer.SIZE);
                long second = (int) key;
                long hash = hash64(first, second);
                int slot = findEmpty(hash);
                tags[slot] = (byte) ((hash >>> 56) | 0x80L);
                entryIds[slot] = entryId;
                size++;
            }
            arrayPool.release(oldTags);
            arrayPool.release(oldEntryIds);
        }

        private void promoteToWideEntries()
        {
            if (denseCompactEntries) {
                promoteDenseToWideEntries();
                return;
            }
            long[] compactEntries = entries;
            int wideStride = keyOnlyBuild ? 2 : 3;
            long[] wideEntries = arrayPool.borrowLongs(tags.length * wideStride);
            for (int slot = 0; slot < tags.length; slot++) {
                if (tags[slot] == 0) {
                    continue;
                }
                int compactStride = keyOnlyBuild ? 1 : 2;
                long key = compactEntries[slot * compactStride];
                int wideBase = slot * wideStride;
                wideEntries[wideBase] = (int) (key >>> Integer.SIZE);
                wideEntries[wideBase + 1] = (int) key;
                if (!keyOnlyBuild) {
                    wideEntries[wideBase + 2] = compactEntries[slot * compactStride + 1];
                }
            }
            entries = wideEntries;
            compactKeys = false;
            arrayPool.release(compactEntries);
        }

        private void promoteDenseToWideEntries()
        {
            long[] wideEntries = arrayPool.borrowLongs(tags.length * 3);
            int[] wideDuplicateHead = null;
            int[] wideDuplicateTail = null;
            int[] wideDuplicateCount = null;
            if (duplicateGroupCount > 0) {
                wideDuplicateHead = arrayPool.borrowInts(tags.length);
                wideDuplicateTail = arrayPool.borrowInts(tags.length);
                wideDuplicateCount = arrayPool.borrowInts(tags.length);
                Arrays.fill(wideDuplicateHead, -1);
                Arrays.fill(wideDuplicateTail, -1);
                Arrays.fill(wideDuplicateCount, 0);
            }
            for (int slot = 0; slot < tags.length; slot++) {
                if (tags[slot] == 0) {
                    continue;
                }
                int ordinal = denseOrdinal(slot);
                long key = denseKeys[ordinal];
                int base = slot * 3;
                wideEntries[base] = (int) (key >>> Integer.SIZE);
                wideEntries[base + 1] = (int) key;
                long state = denseRowState(ordinal);
                if (state >= 0) {
                    wideEntries[base + 2] = decodeDenseRowReference(state);
                }
                else {
                    int group = toIntExact(-state - 1);
                    int head = duplicateHead[group];
                    wideEntries[base + 2] = duplicateReference(head);
                    wideDuplicateHead[slot] = head;
                    wideDuplicateTail[slot] = duplicateTail[group];
                    wideDuplicateCount[slot] = duplicateCount[group];
                }
            }
            arrayPool.release(entryIds);
            entryIds = null;
            arrayPool.release(denseKeys);
            denseKeys = null;
            arrayPool.release(denseRowStates32);
            denseRowStates32 = null;
            arrayPool.release(denseRowStates);
            denseRowStates = null;
            arrayPool.release(duplicateHead);
            arrayPool.release(duplicateTail);
            arrayPool.release(duplicateCount);
            duplicateHead = wideDuplicateHead;
            duplicateTail = wideDuplicateTail;
            duplicateCount = wideDuplicateCount;
            entries = wideEntries;
            denseCompactEntries = false;
            compactKeys = false;
        }

        private int entryStride()
        {
            return compactKeys ? (keyOnlyBuild ? 1 : 2) : (keyOnlyBuild ? 2 : 3);
        }

        private boolean keysEqual(int slot, long first, long second)
        {
            if (compactKeys) {
                return (denseCompactEntries ? denseKeys[denseOrdinal(slot)] : entries[slot * entryStride()]) ==
                        normalizedKey(first, second);
            }
            int base = slot * entryStride();
            return entries[base] == first && entries[base + 1] == second;
        }

        private long rowReference(int slot)
        {
            if (keyOnlyBuild) {
                return 0;
            }
            if (denseCompactEntries) {
                long state = denseRowState(denseOrdinal(slot));
                if (state < 0) {
                    throw new IllegalStateException("duplicate pair key does not have a single row reference");
                }
                return decodeDenseRowReference(state);
            }
            return entries[slot * entryStride() + entryStride() - 1];
        }

        private void writeEntry(int slot, long first, long second, long rowReference)
        {
            if (denseCompactEntries) {
                appendDenseEntry(slot, normalizedKey(first, second), rowReference);
                return;
            }
            if (compactKeys) {
                int base = slot * entryStride();
                entries[base] = normalizedKey(first, second);
                if (!keyOnlyBuild) {
                    entries[base + 1] = rowReference;
                }
                return;
            }
            int base = slot * entryStride();
            entries[base] = first;
            entries[base + 1] = second;
            if (!keyOnlyBuild) {
                entries[base + 2] = rowReference;
            }
        }

        private static boolean fitsSignedInt(long value)
        {
            return value == (int) value;
        }

        private static long normalizedKey(long first, long second)
        {
            return ((first & 0xFFFF_FFFFL) << Integer.SIZE) | (second & 0xFFFF_FFFFL);
        }

        private int denseOrdinal(int slot)
        {
            return entryIds[slot] - 1;
        }

        private void appendDenseEntry(int slot, long key, long rowReference)
        {
            prepareDenseRowReference(rowReference);
            if (denseRowsFit32 &&
                    !denseRowsSingleBatch &&
                    (batchIndex(rowReference) > LongJoinIndex.MAX_PACKED_BATCH_INDEX ||
                            rowPosition(rowReference) > LongJoinIndex.MAX_PACKED_ROW_POSITION)) {
                promoteDenseRowsToLong();
            }
            ensureDenseEntryCapacity();
            int ordinal = denseEntryCount++;
            denseKeys[ordinal] = key;
            if (denseRowStates32 != null) {
                denseRowStates32[ordinal] = encodeDenseRowReference32(rowReference);
            }
            else {
                denseRowStates[ordinal] = rowReference;
            }
            entryIds[slot] = ordinal + 1;
        }

        private long denseRowState(int ordinal)
        {
            return denseRowStates32 != null ? denseRowStates32[ordinal] : denseRowStates[ordinal];
        }

        private long decodeDenseRowReference(long state)
        {
            if (denseRowStates32 == null) {
                return state;
            }
            return denseRowsSingleBatch
                    ? packRowReference(denseRowsBatchIndex, toIntExact(state))
                    : LongJoinIndex.unpackRowReference32((int) state);
        }

        private int encodeDenseRowReference32(long rowReference)
        {
            return denseRowsSingleBatch
                    ? rowPosition(rowReference)
                    : LongJoinIndex.packRowReference32(rowReference);
        }

        private void prepareDenseRowReference(long rowReference)
        {
            if (!denseRowsSingleBatch) {
                return;
            }
            int batchIndex = batchIndex(rowReference);
            if (denseRowsBatchIndex < 0) {
                denseRowsBatchIndex = batchIndex;
                return;
            }
            if (batchIndex == denseRowsBatchIndex) {
                return;
            }
            // Decode both stores while the shared single-batch representation is still active, then switch future
            // rows to ordinary packed/long references. Either store may already have been released during wide-key
            // promotion, so each promotion tolerates an absent compact array.
            promoteDenseRowsToLong();
            promoteDuplicateRowsToLong();
            denseRowsSingleBatch = false;
        }

        private void setDenseRowState(int ordinal, long state)
        {
            if (denseRowStates32 != null) {
                denseRowStates32[ordinal] = toIntExact(state);
            }
            else {
                denseRowStates[ordinal] = state;
            }
        }

        private void ensureDenseEntryCapacity()
        {
            if (denseEntryCount < denseEntryCapacity) {
                return;
            }
            int newCapacity = Math.multiplyExact(denseEntryCapacity, 2);
            long[] previousKeys = denseKeys;
            denseKeys = arrayPool.borrowLongs(newCapacity);
            System.arraycopy(previousKeys, 0, denseKeys, 0, denseEntryCount);
            arrayPool.release(previousKeys);
            if (denseRowStates32 != null) {
                int[] previousStates = denseRowStates32;
                denseRowStates32 = arrayPool.borrowInts(newCapacity);
                System.arraycopy(previousStates, 0, denseRowStates32, 0, denseEntryCount);
                arrayPool.release(previousStates);
            }
            else {
                long[] previousStates = denseRowStates;
                denseRowStates = arrayPool.borrowLongs(newCapacity);
                System.arraycopy(previousStates, 0, denseRowStates, 0, denseEntryCount);
                arrayPool.release(previousStates);
            }
            denseEntryCapacity = newCapacity;
        }

        private void promoteDenseRowsToLong()
        {
            denseRowsFit32 = false;
            if (denseRowStates32 == null) {
                return;
            }
            denseRowStates = arrayPool.borrowLongs(denseEntryCapacity);
            for (int ordinal = 0; ordinal < denseEntryCount; ordinal++) {
                int state = denseRowStates32[ordinal];
                denseRowStates[ordinal] = state < 0 ? state : decodeDenseRowReference(state);
            }
            arrayPool.release(denseRowStates32);
            denseRowStates32 = null;
        }

        @Override
        public void releaseBuffers()
        {
            arrayPool.release(tags);
            tags = null;
            arrayPool.release(entries);
            entries = null;
            arrayPool.release(entryIds);
            entryIds = null;
            arrayPool.release(denseKeys);
            denseKeys = null;
            arrayPool.release(denseRowStates32);
            denseRowStates32 = null;
            arrayPool.release(denseRowStates);
            denseRowStates = null;
            arrayPool.release(duplicateHead);
            duplicateHead = null;
            arrayPool.release(duplicateTail);
            duplicateTail = null;
            arrayPool.release(duplicateCount);
            duplicateCount = null;
            arrayPool.release(duplicateNext);
            duplicateNext = null;
            arrayPool.release(duplicateRows32);
            duplicateRows32 = null;
            arrayPool.release(duplicateRows);
            duplicateRows = null;
            arrayPool.release(keyOnlyCounts);
            keyOnlyCounts = null;
            arrayPool.release(nativeFirst);
            nativeFirst = null;
            arrayPool.release(nativeSecond);
            nativeSecond = null;
        }

        private void appendDuplicateRow(int slot, long existingRowReference, long rowReference)
        {
            if (denseCompactEntries) {
                appendDenseDuplicateRow(slot, existingRowReference, rowReference);
                return;
            }
            ensureDuplicateSlotState();
            int tail = duplicateTail[slot];
            if (tail < 0) {
                int head = appendDuplicateReference(existingRowReference);
                tail = appendDuplicateReference(rowReference);
                duplicateNext[head] = tail;
                duplicateHead[slot] = head;
                duplicateTail[slot] = tail;
                duplicateCount[slot] = 2;
                return;
            }
            int ordinal = appendDuplicateReference(rowReference);
            duplicateNext[tail] = ordinal;
            duplicateTail[slot] = ordinal;
            duplicateCount[slot]++;
        }

        private void appendDenseDuplicateRow(int slot, long existingRowReference, long rowReference)
        {
            int ordinal = denseOrdinal(slot);
            long state = denseRowState(ordinal);
            if (state >= 0) {
                int group = newDuplicateGroup();
                int head = appendDuplicateReference(decodeDenseRowReference(state));
                int tail = appendDuplicateReference(rowReference);
                duplicateNext[head] = tail;
                duplicateHead[group] = head;
                duplicateTail[group] = tail;
                duplicateCount[group] = 2;
                setDenseRowState(ordinal, -(group + 1L));
                return;
            }
            int group = toIntExact(-state - 1);
            int tail = duplicateTail[group];
            int duplicate = appendDuplicateReference(rowReference);
            duplicateNext[tail] = duplicate;
            duplicateTail[group] = duplicate;
            duplicateCount[group]++;
        }

        private int newDuplicateGroup()
        {
            if (duplicateHead == null) {
                duplicateGroupCapacity = 16;
                duplicateHead = arrayPool.borrowInts(duplicateGroupCapacity);
                duplicateTail = arrayPool.borrowInts(duplicateGroupCapacity);
                duplicateCount = arrayPool.borrowInts(duplicateGroupCapacity);
            }
            else if (duplicateGroupCount == duplicateGroupCapacity) {
                int newCapacity = Math.multiplyExact(duplicateGroupCapacity, 2);
                duplicateHead = growDuplicateGroups(duplicateHead, newCapacity);
                duplicateTail = growDuplicateGroups(duplicateTail, newCapacity);
                duplicateCount = growDuplicateGroups(duplicateCount, newCapacity);
                duplicateGroupCapacity = newCapacity;
            }
            return duplicateGroupCount++;
        }

        private int[] growDuplicateGroups(int[] values, int newCapacity)
        {
            int[] grown = arrayPool.borrowInts(newCapacity);
            System.arraycopy(values, 0, grown, 0, duplicateGroupCount);
            arrayPool.release(values);
            return grown;
        }

        private long duplicateReference(int ordinal)
        {
            if (duplicateRows32 == null) {
                return duplicateRows[ordinal];
            }
            return denseRowsSingleBatch
                    ? packRowReference(denseRowsBatchIndex, duplicateRows32[ordinal])
                    : LongJoinIndex.unpackRowReference32(duplicateRows32[ordinal]);
        }

        private int appendDuplicateReference(long rowReference)
        {
            prepareDenseRowReference(rowReference);
            if (duplicateRowsFit32 &&
                    !denseRowsSingleBatch &&
                    (batchIndex(rowReference) > LongJoinIndex.MAX_PACKED_BATCH_INDEX ||
                            rowPosition(rowReference) > LongJoinIndex.MAX_PACKED_ROW_POSITION)) {
                promoteDuplicateRowsToLong();
            }
            ensureDuplicateRowCapacity();
            int ordinal = duplicateRowCount++;
            if (duplicateRows32 != null) {
                duplicateRows32[ordinal] = encodeDenseRowReference32(rowReference);
            }
            else {
                duplicateRows[ordinal] = rowReference;
            }
            duplicateNext[ordinal] = -1;
            return ordinal;
        }

        private void ensureDuplicateSlotState()
        {
            if (duplicateHead != null) {
                return;
            }
            duplicateHead = arrayPool.borrowInts(tags.length);
            duplicateTail = arrayPool.borrowInts(tags.length);
            duplicateCount = arrayPool.borrowInts(tags.length);
            Arrays.fill(duplicateHead, -1);
            Arrays.fill(duplicateTail, -1);
            Arrays.fill(duplicateCount, 0);
        }

        private void ensureDuplicateRowCapacity()
        {
            if (duplicateNext == null) {
                duplicateRowCapacity = Math.max(16, initialDuplicateRowCapacity);
                duplicateNext = arrayPool.borrowInts(duplicateRowCapacity);
                if (duplicateRowsFit32) {
                    duplicateRows32 = arrayPool.borrowInts(duplicateRowCapacity);
                }
                else {
                    duplicateRows = arrayPool.borrowLongs(duplicateRowCapacity);
                }
                return;
            }
            if (duplicateRowCount < duplicateRowCapacity) {
                return;
            }
            int newCapacity = Math.multiplyExact(duplicateRowCapacity, 2);
            if (PRE_SIZE_DENSE_DUPLICATE_ROWS && denseCompactEntries &&
                    duplicateRowCount >= denseEntryCount / 2 && expectedBuildRows > newCapacity) {
                newCapacity = expectedBuildRows;
            }
            int[] previousNext = duplicateNext;
            duplicateNext = arrayPool.borrowInts(newCapacity);
            System.arraycopy(previousNext, 0, duplicateNext, 0, duplicateRowCount);
            arrayPool.release(previousNext);
            if (duplicateRows32 != null) {
                int[] previousRows = duplicateRows32;
                duplicateRows32 = arrayPool.borrowInts(newCapacity);
                System.arraycopy(previousRows, 0, duplicateRows32, 0, duplicateRowCount);
                arrayPool.release(previousRows);
            }
            else {
                long[] previousRows = duplicateRows;
                duplicateRows = arrayPool.borrowLongs(newCapacity);
                System.arraycopy(previousRows, 0, duplicateRows, 0, duplicateRowCount);
                arrayPool.release(previousRows);
            }
            duplicateRowCapacity = newCapacity;
        }

        private void promoteDuplicateRowsToLong()
        {
            duplicateRowsFit32 = false;
            if (duplicateRows32 == null) {
                return;
            }
            duplicateRows = arrayPool.borrowLongs(duplicateRowCapacity);
            for (int index = 0; index < duplicateRowCount; index++) {
                duplicateRows[index] = denseRowsSingleBatch
                        ? packRowReference(denseRowsBatchIndex, duplicateRows32[index])
                        : LongJoinIndex.unpackRowReference32(duplicateRows32[index]);
            }
            arrayPool.release(duplicateRows32);
            duplicateRows32 = null;
        }

        private void ensureKeyOnlyCounts()
        {
            if (keyOnlyCounts != null) {
                return;
            }
            keyOnlyCounts = arrayPool.borrowInts(tags.length);
            Arrays.fill(keyOnlyCounts, 0);
        }

        // Distinct keys only (rehash): returns the first empty slot in the key's probe sequence.
        private int findEmpty(long hash)
        {
            int group = ((int) hash) & mask & ~(GROUP - 1);
            while (true) {
                int emptyLane = firstEmptyTag(ByteVector.fromArray(SPECIES, tags, group));
                if (emptyLane < GROUP) {
                    return group + emptyLane;
                }
                group = (group + GROUP) & mask;
            }
        }

        private static long matchingTagBits(jdk.incubator.vector.VectorMask<Byte> matches)
        {
            if (GUARD_TAG_MASK_CONVERSION && !matches.anyTrue()) {
                return 0;
            }
            return matches.toLong();
        }

        private static boolean hasEmptyTag(ByteVector tags)
        {
            var empty = tags.compare(VectorOperators.EQ, (byte) 0);
            return GUARD_TAG_MASK_CONVERSION ? empty.anyTrue() : empty.toLong() != 0;
        }

        private static int firstEmptyTag(ByteVector tags)
        {
            var empty = tags.compare(VectorOperators.EQ, (byte) 0);
            return GUARD_TAG_MASK_CONVERSION ? empty.firstTrue() : Long.numberOfTrailingZeros(empty.toLong());
        }

        private static long hash64(long first, long second)
        {
            // Fibonacci-prime combine + Murmur3 64-bit finalizer; the low bits index the bucket, the top byte is
            // the tag. TPC-DS surrogate keys have zero upper 32 bits and collide heavily under a naive combine.
            long hash = first * 0x9E3779B97F4A7C15L + second * 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= hash >>> 33;
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            return hash;
        }
    }

    private static final class LongTripleJoinIndex
            implements JoinIndex
    {
        private static final float LOAD_FACTOR = 0.75f;
        // Swiss/F14-style SIMD-tag-bucket table (cf. LongPairJoinIndex): a probe scans a GROUP of 1-byte tags with
        // one vector load and touches the fat entry array only on a tag hit, so a collision step is a dense byte
        // read rather than a {first,second,third,row} cache-miss load.
        private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_128;
        private static final int GROUP = SPECIES.length();
        private final PrimitiveArrayPool arrayPool;

        private byte[] tags;
        // Co-located entry table: for slot s, entries[4*s] = firstKey, entries[4*s+1] = secondKey, entries[4*s+2] =
        // thirdKey, entries[4*s+3] = rowReference. A matching probe's key verify and row read then hit one contiguous
        // 32-byte region instead of four separate long[] (firstKeys/secondKeys/thirdKeys/singleRows), which cost four
        // random line fetches per matched probe. Tags stay in their own byte[] (scanned a GROUP at a time).
        private long[] entries;
        private LongArrayList[] rowsBySlot;
        private int mask;
        private int maxFill;
        private int size;
        private boolean tripleHasDuplicates;
        private final SingleLongList singleMatch = new SingleLongList();
        // Reusable per-batch key gather buffers for the native (Rust) prefetching probe (see NativeProbe).
        private long[] nativeFirst;
        private long[] nativeSecond;
        private long[] nativeThird;

        private LongTripleJoinIndex(PrimitiveArrayPool arrayPool, int expectedSize)
        {
            this.arrayPool = arrayPool;
            int capacity = GROUP;
            while (capacity < expectedSize / LOAD_FACTOR) {
                capacity <<= 1;
            }
            allocate(capacity);
        }

        private void allocate(int capacity)
        {
            tags = arrayPool.borrowBytes(capacity);
            Arrays.fill(tags, (byte) 0);
            entries = arrayPool.borrowLongs(capacity * 4);
            rowsBySlot = arrayPool.borrow(LongArrayList[].class, capacity, LongArrayList[].class);
            if (rowsBySlot == null) {
                rowsBySlot = new LongArrayList[capacity];
            }
            else {
                Arrays.fill(rowsBySlot, null);
            }
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public boolean isEmpty()
        {
            return size == 0;
        }

        @Override
        public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
        {
            if (FlatJoinIndex.hasNull(nulls, position)) {
                return;
            }
            addNoNulls(values, position, rowReference);
        }

        @Override
        public void addNoNulls(Vector[] values, int position, long rowReference)
        {
            addRow(
                    OperatorVectorSupport.longValue(values[0], position),
                    OperatorVectorSupport.longValue(values[1], position),
                    OperatorVectorSupport.longValue(values[2], position),
                    rowReference);
        }

        @Override
        public LongList matches(Vector[] values, Vector[] nulls, int position)
        {
            if (FlatJoinIndex.hasNull(nulls, position)) {
                return LongLists.emptyList();
            }
            return matchesNoNulls(values, position);
        }

        @Override
        public LongList matchesNoNulls(Vector[] values, int position)
        {
            long first = OperatorVectorSupport.longValue(values[0], position);
            long second = OperatorVectorSupport.longValue(values[1], position);
            long third = OperatorVectorSupport.longValue(values[2], position);
            int slot = probe(first, second, third, hash64(first, second, third));
            if (slot < 0) {
                return LongLists.emptyList();
            }
            LongArrayList rows = rowsBySlot[slot];
            if (rows != null) {
                return rows;
            }
            return singleMatch.withValue(entries[slot * 4 + 3]);
        }

        @Override
        public void matchRows(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.LongValues thirdValues = VectorAccess.longValues(values[2]);
            if (!hasNulls) {
                for (int index = 0; index < positionCount; index++) {
                    int position = positions[index];
                    long first = firstValues.value(position);
                    long second = secondValues.value(position);
                    long third = thirdValues.value(position);
                    int slot = probe(first, second, third, hash64(first, second, third));
                    if (slot < 0) {
                        matches[index] = LongLists.emptyList();
                        continue;
                    }
                    LongArrayList rows = rowsBySlot[slot];
                    if (rows != null) {
                        matches[index] = rows;
                    }
                    else {
                        matches[index] = singleMatches[index].withValue(entries[slot * 4 + 3]);
                    }
                }
                return;
            }
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
            VectorAccess.BooleanValues thirdNulls = VectorAccess.booleanValues(nulls[2]);
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position)) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                long first = firstValues.value(position);
                long second = secondValues.value(position);
                long third = thirdValues.value(position);
                int slot = probe(first, second, third, hash64(first, second, third));
                if (slot < 0) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                LongArrayList rows = rowsBySlot[slot];
                if (rows != null) {
                    matches[index] = rows;
                }
                else {
                    matches[index] = singleMatches[index].withValue(entries[slot * 4 + 3]);
                }
            }
        }

        @Override
        public boolean supportsSingleMatchRefs()
        {
            return !tripleHasDuplicates;
        }

        @Override
        public void matchSingleRows(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, long[] refs)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(valuesArray[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(valuesArray[1]);
            VectorAccess.LongValues thirdValues = VectorAccess.longValues(valuesArray[2]);
            if (!hasNulls) {
                if (NativeProbe.ENABLED) {
                    if (nativeFirst == null || nativeFirst.length < positionCount) {
                        long[] previousFirst = nativeFirst;
                        long[] previousSecond = nativeSecond;
                        long[] previousThird = nativeThird;
                        nativeFirst = arrayPool.borrowLongs(positionCount);
                        nativeSecond = arrayPool.borrowLongs(positionCount);
                        nativeThird = arrayPool.borrowLongs(positionCount);
                        arrayPool.release(previousFirst);
                        arrayPool.release(previousSecond);
                        arrayPool.release(previousThird);
                    }
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        nativeFirst[index] = firstValues.value(position);
                        nativeSecond[index] = secondValues.value(position);
                        nativeThird[index] = thirdValues.value(position);
                    }
                    NativeProbe.probeTriples(tags, entries, nativeFirst, nativeSecond, nativeThird, positionCount, refs, NativeProbe.DISTANCE);
                    return;
                }
                for (int index = 0; index < positionCount; index++) {
                    int position = positions[index];
                    refs[index] = singleRef(firstValues.value(position), secondValues.value(position), thirdValues.value(position));
                }
                return;
            }
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nullsArray[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nullsArray[1]);
            VectorAccess.BooleanValues thirdNulls = VectorAccess.booleanValues(nullsArray[2]);
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                refs[index] = firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position)
                        ? NO_MATCH_ROW_REFERENCE
                        : singleRef(firstValues.value(position), secondValues.value(position), thirdValues.value(position));
            }
        }

        private long singleRef(long first, long second, long third)
        {
            int slot = probe(first, second, third, hash64(first, second, third));
            return slot < 0 ? NO_MATCH_ROW_REFERENCE : entries[slot * 4 + 3];
        }

        // Returns the slot holding (first, second, third), or -1 if absent. Scans GROUP tags per step: one vector
        // load plus one tag compare filters the whole bucket; a key is only read on a tag match. A bucket with any
        // empty slot ends the search (open-addressing invariant; no deletions).
        private int probe(long first, long second, long third, long hash)
        {
            byte[] tagTable = tags;
            byte tag = (byte) ((hash >>> 56) | 0x80L);
            int group = ((int) hash) & mask & ~(GROUP - 1);
            while (true) {
                ByteVector groupTags = ByteVector.fromArray(SPECIES, tagTable, group);
                long matchBits = groupTags.compare(VectorOperators.EQ, tag).toLong();
                while (matchBits != 0) {
                    int slot = group + Long.numberOfTrailingZeros(matchBits);
                    int base = slot * 4;
                    if (entries[base] == first && entries[base + 1] == second && entries[base + 2] == third) {
                        return slot;
                    }
                    matchBits &= matchBits - 1;
                }
                if (groupTags.compare(VectorOperators.EQ, (byte) 0).toLong() != 0) {
                    return -1;
                }
                group = (group + GROUP) & mask;
            }
        }

        private void rehash()
        {
            byte[] oldTags = tags;
            long[] oldEntries = entries;
            LongArrayList[] oldLists = rowsBySlot;
            allocate(oldTags.length * 2);
            size = 0;
            for (int oldSlot = 0; oldSlot < oldTags.length; oldSlot++) {
                if (oldTags[oldSlot] == 0) {
                    continue;
                }
                int oldBase = oldSlot * 4;
                long first = oldEntries[oldBase];
                long second = oldEntries[oldBase + 1];
                long third = oldEntries[oldBase + 2];
                long hash = hash64(first, second, third);
                int slot = findEmpty(hash);
                int base = slot * 4;
                tags[slot] = (byte) ((hash >>> 56) | 0x80L);
                entries[base] = first;
                entries[base + 1] = second;
                entries[base + 2] = third;
                entries[base + 3] = oldEntries[oldBase + 3];
                rowsBySlot[slot] = oldLists[oldSlot];
                size++;
            }
            arrayPool.release(oldTags);
            arrayPool.release(oldEntries);
            releaseRowsBySlot(oldLists);
        }

        @Override
        public void releaseBuffers()
        {
            arrayPool.release(tags);
            tags = null;
            arrayPool.release(entries);
            entries = null;
            releaseRowsBySlot(rowsBySlot);
            rowsBySlot = null;
            arrayPool.release(nativeFirst);
            nativeFirst = null;
            arrayPool.release(nativeSecond);
            nativeSecond = null;
            arrayPool.release(nativeThird);
            nativeThird = null;
        }

        private void releaseRowsBySlot(LongArrayList[] rows)
        {
            if (rows == null) {
                return;
            }
            Arrays.fill(rows, null);
            arrayPool.retain(LongArrayList[].class, rows.length, (long) rows.length * Long.BYTES, rows);
        }

        // Distinct keys only (rehash): the first empty slot in the key's probe sequence.
        private int findEmpty(long hash)
        {
            int group = ((int) hash) & mask & ~(GROUP - 1);
            while (true) {
                long emptyBits = ByteVector.fromArray(SPECIES, tags, group).compare(VectorOperators.EQ, (byte) 0).toLong();
                if (emptyBits != 0) {
                    return group + Long.numberOfTrailingZeros(emptyBits);
                }
                group = (group + GROUP) & mask;
            }
        }

        private void addRow(long first, long second, long third, long rowReference)
        {
            long hash = hash64(first, second, third);
            byte tag = (byte) ((hash >>> 56) | 0x80L);
            int group = ((int) hash) & mask & ~(GROUP - 1);
            while (true) {
                ByteVector groupTags = ByteVector.fromArray(SPECIES, tags, group);
                long matchBits = groupTags.compare(VectorOperators.EQ, tag).toLong();
                while (matchBits != 0) {
                    int slot = group + Long.numberOfTrailingZeros(matchBits);
                    int base = slot * 4;
                    if (entries[base] == first && entries[base + 1] == second && entries[base + 2] == third) {
                        tripleHasDuplicates = true;
                        if (rowsBySlot[slot] == null) {
                            LongArrayList rows = new LongArrayList(DUPLICATE_LIST_INITIAL_CAPACITY);
                            rows.add(entries[base + 3]);
                            rows.add(rowReference);
                            rowsBySlot[slot] = rows;
                        }
                        else {
                            rowsBySlot[slot].add(rowReference);
                        }
                        return;
                    }
                    matchBits &= matchBits - 1;
                }
                long emptyBits = groupTags.compare(VectorOperators.EQ, (byte) 0).toLong();
                if (emptyBits != 0) {
                    int slot = group + Long.numberOfTrailingZeros(emptyBits);
                    int base = slot * 4;
                    tags[slot] = tag;
                    entries[base] = first;
                    entries[base + 1] = second;
                    entries[base + 2] = third;
                    entries[base + 3] = rowReference;
                    size++;
                    if (size >= maxFill) {
                        rehash();
                    }
                    return;
                }
                group = (group + GROUP) & mask;
            }
        }

        private static long hash64(long first, long second, long third)
        {
            long hash = first * 0x9E3779B97F4A7C15L + second * 0xC4CEB9FE1A85EC53L + third * 0x94D049BB133111EBL;
            hash ^= hash >>> 33;
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= hash >>> 33;
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            return hash;
        }
    }

    private static long[] emptyRows(int capacity)
    {
        long[] rows = new long[capacity];
        Arrays.fill(rows, NO_MATCH_ROW_REFERENCE);
        return rows;
    }

    private static SingleLongList[] createSingleLongLists(int size)
    {
        SingleLongList[] matches = new SingleLongList[size];
        for (int index = 0; index < size; index++) {
            matches[index] = new SingleLongList();
        }
        return matches;
    }

    /**
     * Reusable view over one key's build rows, threaded through a shared chain ({@code next}) starting at
     * {@code head}. Reading is cursor-cached so the sequential {@code getLong(0..size-1)} access the join
     * output loop performs is O(1) per element; out-of-order access falls back to a walk from the head.
     */
    private static final class ChainLongList
            extends AbstractLongList
    {
        private long[] rows;
        private int[] compactRows;
        private int[] next;
        private int head;
        private int length;
        private int cursorIndex;
        private int cursorOrdinal;
        private int compactBatchIndex = -1;
        // Range mode: the key's rows are a contiguous slice {@code rows[base .. base+length)} (compacted at finalize),
        // so a read is one sequential array index — no {@code next[]} pointer-chase. {@code base} reuses {@code head}.
        private boolean rangeMode;
        private boolean repeatedMode;
        private long repeatedValue;

        public ChainLongList reset(long[] rows, int[] next, int head, int length)
        {
            this.rows = rows;
            this.compactRows = null;
            this.next = next;
            this.head = head;
            this.length = length;
            this.cursorIndex = 0;
            this.cursorOrdinal = head;
            this.rangeMode = false;
            this.repeatedMode = false;
            return this;
        }

        public ChainLongList resetCompact(int[] rows, int[] next, int head, int length)
        {
            this.rows = null;
            this.compactRows = rows;
            this.next = next;
            this.head = head;
            this.length = length;
            this.cursorIndex = 0;
            this.cursorOrdinal = head;
            this.rangeMode = false;
            this.repeatedMode = false;
            this.compactBatchIndex = -1;
            return this;
        }

        public ChainLongList resetCompactSingleBatch(int[] rows, int[] next, int head, int length, int batchIndex)
        {
            resetCompact(rows, next, head, length);
            this.compactBatchIndex = batchIndex;
            return this;
        }

        public ChainLongList resetRange(long[] rows, int base, int length)
        {
            this.rows = rows;
            this.compactRows = null;
            this.head = base;
            this.length = length;
            this.rangeMode = true;
            this.repeatedMode = false;
            return this;
        }

        public ChainLongList resetRepeated(long value, int length)
        {
            this.repeatedValue = value;
            this.length = length;
            this.rangeMode = false;
            this.repeatedMode = true;
            return this;
        }

        @Override
        public long getLong(int index)
        {
            if (index < 0 || index >= length) {
                throw new IndexOutOfBoundsException("index " + index);
            }
            if (repeatedMode) {
                return repeatedValue;
            }
            if (rangeMode) {
                return rows[head + index];
            }
            if (index < cursorIndex) {
                cursorIndex = 0;
                cursorOrdinal = head;
            }
            while (cursorIndex < index) {
                cursorOrdinal = next[cursorOrdinal];
                cursorIndex++;
            }
            if (compactRows == null) {
                return rows[cursorOrdinal];
            }
            return compactBatchIndex >= 0
                    ? packRowReference(compactBatchIndex, compactRows[cursorOrdinal])
                    : LongJoinIndex.unpackRowReference32(compactRows[cursorOrdinal]);
        }

        @Override
        public int size()
        {
            return length;
        }

        public int storageIndex(int index)
        {
            if (repeatedMode) {
                return -1;
            }
            if (rangeMode) {
                return head + index;
            }
            if (index < cursorIndex) {
                cursorIndex = 0;
                cursorOrdinal = head;
            }
            while (cursorIndex < index) {
                cursorOrdinal = next[cursorOrdinal];
                cursorIndex++;
            }
            return cursorOrdinal;
        }
    }

    private static ChainLongList[] createChainLongLists(int size)
    {
        ChainLongList[] matches = new ChainLongList[size];
        for (int index = 0; index < size; index++) {
            matches[index] = new ChainLongList();
        }
        return matches;
    }

    private static final class SingleLongList
            extends AbstractLongList
    {
        private long value;

        public SingleLongList withValue(long value)
        {
            this.value = value;
            return this;
        }

        @Override
        public long getLong(int index)
        {
            if (index != 0) {
                throw new IndexOutOfBoundsException("index " + index);
            }
            return value;
        }

        @Override
        public int size()
        {
            return 1;
        }
    }

    private static final class ObjectJoinIndex
            implements JoinIndex
    {
        private final Map<OperatorKeySemantics.Key, LongArrayList> rowsByKey = new HashMap<>();
        private final OperatorKeySemantics.Key[] innerProbeKeys;
        private final OperatorKeySemantics.Key[] outerProbeKeys;
        private final OperatorKeySemantics.CompositeProbeKey innerCompositeProbeKey;
        private final OperatorKeySemantics.CompositeProbeKey outerCompositeProbeKey;

        private ObjectJoinIndex(int keyCount)
        {
            this.innerProbeKeys = new OperatorKeySemantics.Key[keyCount];
            this.outerProbeKeys = new OperatorKeySemantics.Key[keyCount];
            this.innerCompositeProbeKey = keyCount > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(keyCount) : null;
            this.outerCompositeProbeKey = keyCount > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(keyCount) : null;
        }

        @Override
        public boolean isEmpty()
        {
            return rowsByKey.isEmpty();
        }

        @Override
        public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
        {
            OperatorKeySemantics.Key key = keyForPosition(values, nulls, position, innerProbeKeys, innerCompositeProbeKey);
            if (key == null) {
                return;
            }
            LongArrayList rows = rowsByKey.get(key);
            if (rows == null) {
                rows = new LongArrayList();
                rowsByKey.put(OperatorKeySemantics.ownedKey(key), rows);
            }
            rows.add(rowReference);
        }

        @Override
        public LongList matches(Vector[] values, Vector[] nulls, int position)
        {
            OperatorKeySemantics.Key key = keyForPosition(values, nulls, position, outerProbeKeys, outerCompositeProbeKey);
            if (key == null) {
                return LongLists.emptyList();
            }
            LongArrayList rows = rowsByKey.get(key);
            return rows == null ? LongLists.emptyList() : rows;
        }

        private static OperatorKeySemantics.Key keyForPosition(Vector[] values, Vector[] nulls, int position, OperatorKeySemantics.Key[] reusableProbeKeys, OperatorKeySemantics.CompositeProbeKey reusableCompositeProbeKey)
        {
            for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
                if (reusableProbeKeys[keyIndex] == null) {
                    reusableProbeKeys[keyIndex] = OperatorKeySemantics.reusableProbeKey(values[keyIndex]);
                }
                OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values[keyIndex], nulls[keyIndex], position, reusableProbeKeys[keyIndex]);
                if (key == null) {
                    return null;
                }
                reusableProbeKeys[keyIndex] = key;
            }
            return OperatorKeySemantics.probeCompositeKey(reusableProbeKeys, reusableCompositeProbeKey);
        }
    }
}
