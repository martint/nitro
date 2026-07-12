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

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

final class DistinctKeySet
{
    private static final boolean ADAPTIVE_COMPACT_MULTI_LONG =
            Boolean.parseBoolean(System.getProperty("nitro.distinct.adaptiveCompactMultiLong", "true"));

    private final DistinctIndex index;

    private DistinctKeySet(DistinctIndex index)
    {
        this.index = index;
    }

    /**
     * Creates a distinct-key set that drops rows with any NULL key column. This matches the SQL semantics of
     * {@code count(distinct ...)} and distinct aggregation, where NULL keys are ignored.
     */
    public static DistinctKeySet create(Vector[] samples)
    {
        return create(samples, false);
    }

    /**
     * Creates an exact distinct set for the common grouped {@code count(distinct long)} shape. The first
     * vector is a dense non-null group id and the second is the nullable value. Partitioning by group avoids
     * repeating the group id in every hash slot and keeps each probe table smaller.
     */
    public static DistinctKeySet createGroupedLong(Vector[] samples)
    {
        if (samples.length != 2 || !(samples[0] instanceof I64Vector) || !isIntegerVector(samples[1])) {
            return create(samples);
        }
        return new DistinctKeySet(new GroupedLongDistinctIndex());
    }

    /**
     * Creates a distinct-key set.
     *
     * @param retainNulls when {@code true}, rows whose key contains NULLs are retained and de-duplicated with
     * SQL {@code DISTINCT}/{@code UNION} semantics (two NULLs in the same column are equal; a NULL is distinct
     * from any concrete value), so a single representative null-keyed row survives. When {@code false}, any row
     * with a NULL key column is dropped (the {@code count(distinct ...)} semantics).
     */
    public static DistinctKeySet create(Vector[] samples, boolean retainNulls)
    {
        DistinctIndex index = createIndex(samples);
        if (retainNulls) {
            index = new RetainNullsDistinctIndex(index, samples.length);
        }
        return new DistinctKeySet(index);
    }

    private static DistinctIndex createIndex(Vector[] samples)
    {
        if (samples.length == 1 && isIntegerVector(samples[0])) {
            return new LongDistinctIndex(Math.max(16, samples[0].length()));
        }
        if (samples.length == 2 && isIntegerVector(samples[0]) && isIntegerVector(samples[1])) {
            return new LongPairDistinctIndex(Math.max(16, samples[0].length()));
        }
        if (samples.length == 3 && isIntegerVector(samples[0]) && isIntegerVector(samples[1]) && isIntegerVector(samples[2])) {
            return new LongTripleDistinctIndex(Math.max(16, samples[0].length()));
        }
        if (samples.length == 4 && isIntegerVector(samples[0]) && isIntegerVector(samples[1]) && isIntegerVector(samples[2]) && isIntegerVector(samples[3])) {
            return new LongQuadDistinctIndex(Math.max(16, samples[0].length()));
        }
        if (samples.length >= 5 && samples.length <= AbstractMultiLongGroupingTable.MAX_ARITY && allIntegerVectors(samples)) {
            if (ADAPTIVE_COMPACT_MULTI_LONG) {
                return new AdaptiveMultiLongDistinctIndex(samples.length, Math.max(16, samples[0].length()));
            }
            return new MultiLongDistinctIndex(samples.length, Math.max(16, samples[0].length()));
        }
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(samples);
        if (layout != null) {
            return new FlatDistinctIndex(layout, Math.max(16, samples[0].length()));
        }
        return new ObjectDistinctIndex(samples.length);
    }

    public boolean add(Vector[] values, Vector[] nulls, int position)
    {
        return index.add(values, nulls, position);
    }

    public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
    {
        return index.addBatch(values, nulls, mask, distinctPositions);
    }

    /** Adds a grouped batch whose first key is a dense group id in {@code [0, groupCount)}. */
    public int addGroupedBatch(Vector[] values, Vector[] nulls, Mask mask, int groupCount, int[] distinctPositions)
    {
        return index.addGroupedBatch(values, nulls, mask, groupCount, distinctPositions);
    }

    public void reserveAdditional(int additionalEntries)
    {
        index.reserveAdditional(additionalEntries);
    }

    public void releaseBuffers()
    {
        index.releaseBuffers();
    }

    private interface DistinctIndex
    {
        boolean add(Vector[] values, Vector[] nulls, int position);

        default int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if (add(values, nulls, position)) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (add(values, nulls, position)) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }

        default int addGroupedBatch(Vector[] values, Vector[] nulls, Mask mask, int groupCount, int[] distinctPositions)
        {
            return addBatch(values, nulls, mask, distinctPositions);
        }

        /** Processes positions whose key columns have already been proven non-null by a wrapper. */
        default int addNonNullBatch(Vector[] values, Vector[] nulls, int[] positions, int positionCount, int[] distinctPositions)
        {
            int count = 0;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (add(values, nulls, position)) {
                    distinctPositions[count++] = position;
                }
            }
            return count;
        }

        default int addNonNullDenseBatch(Vector[] values, Vector[] nulls, int positionCount, int[] positions, int[] distinctPositions)
        {
            return addNonNullBatch(values, nulls, positions, positionCount, distinctPositions);
        }

        default void reserveAdditional(int additionalEntries) {}

        default void releaseBuffers() {}
    }

    private static final class LongDistinctIndex
            implements DistinctIndex
    {
        private static final boolean ADAPTIVE_PAGED_BITMAP =
                Boolean.parseBoolean(System.getProperty("nitro.distinct.adaptivePagedLongBitmap", "true"));
        private static final int PAGE_SHIFT = 16;
        private static final int PAGE_BITS = 1 << PAGE_SHIFT;
        private static final int PAGE_WORDS = PAGE_BITS / Long.SIZE;
        private static final int MIN_BITMAP_KEYS = 4_096;
        private static final long MAX_BITS_PER_KEY = 64;
        private final PrimitiveArrayPool arrayPool = PrimitiveArrayPool.shared();

        private PooledLongHashSet pooledKeys;
        private Long2ObjectOpenHashMap<long[]> bitmapPages;
        private long minimumKey = Long.MAX_VALUE;
        private long maximumKey = Long.MIN_VALUE;
        private int size;

        private LongDistinctIndex(int expectedSize)
        {
            createHash(expectedSize);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            if (hashPresent()) {
                ensureHashCapacity(hashSize() + Math.max(0, additionalEntries));
            }
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (OperatorVectorSupport.isNull(nulls[0], position)) {
                return false;
            }
            return addKey(OperatorVectorSupport.longValue(values[0], position));
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            VectorAccess.LongValues keyValues = VectorAccess.longValues(values[0]);
            VectorAccess.BooleanValues keyNulls = VectorAccess.booleanValues(nulls[0]);
            if (bitmapPages != null && mask.all()) {
                return addDenseBitmapBatch(keyValues, keyNulls, mask.size(), distinctPositions);
            }
            if (bitmapPages == null && (!ADAPTIVE_PAGED_BITMAP || size >= MIN_BITMAP_KEYS)) {
                return addFinalHashBatch(keyValues, keyNulls, mask, distinctPositions);
            }
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if (keyNulls.value(position)) {
                        continue;
                    }
                    if (addKey(keyValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (keyNulls.value(position)) {
                        continue;
                    }
                    if (addKey(keyValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }

        /** Dense bitmap loop with representation fallback handled only when a new page is encountered. */
        private int addDenseBitmapBatch(
                VectorAccess.LongValues keyValues,
                VectorAccess.BooleanValues keyNulls,
                int positionCount,
                int[] distinctPositions)
        {
            int count = 0;
            for (int position = 0; position < positionCount; position++) {
                if (keyNulls.value(position)) {
                    continue;
                }
                long key = keyValues.value(position);
                long pageId = key >> PAGE_SHIFT;
                long[] page = bitmapPages.get(pageId);
                if (page == null) {
                    page = arrayPool.borrowLongs(PAGE_WORDS);
                    Arrays.fill(page, 0);
                    bitmapPages.put(pageId, page);
                    if ((long) bitmapPages.size() * PAGE_BITS > Math.max(MIN_BITMAP_KEYS, size) * MAX_BITS_PER_KEY) {
                        convertToHash();
                        if (addHashKey(key)) {
                            size++;
                            distinctPositions[count++] = position;
                        }
                        return addFinalDenseHashRange(
                                keyValues,
                                keyNulls,
                                position + 1,
                                positionCount,
                                distinctPositions,
                                count);
                    }
                }
                int pagePosition = (int) key & (PAGE_BITS - 1);
                int wordIndex = pagePosition >>> 6;
                long bit = 1L << pagePosition;
                if ((page[wordIndex] & bit) == 0) {
                    page[wordIndex] |= bit;
                    size++;
                    distinctPositions[count++] = position;
                }
            }
            return count;
        }

        /** Plain steady-state hash loop selected once per batch after bitmap admission has closed. */
        private int addFinalHashBatch(
                VectorAccess.LongValues keyValues,
                VectorAccess.BooleanValues keyNulls,
                Mask mask,
                int[] distinctPositions)
        {
            int count = 0;
            if (mask.all()) {
                return addFinalDenseHashRange(keyValues, keyNulls, 0, mask.size(), distinctPositions, 0);
            }
            int currentSize = size;
            for (int position : mask) {
                if (!keyNulls.value(position) && addHashKey(keyValues.value(position))) {
                    distinctPositions[count++] = position;
                    currentSize++;
                }
            }
            size = currentSize;
            return count;
        }

        private int addFinalDenseHashRange(
                VectorAccess.LongValues keyValues,
                VectorAccess.BooleanValues keyNulls,
                int startPosition,
                int endPosition,
                int[] distinctPositions,
                int count)
        {
            int currentSize = size;
            for (int position = startPosition; position < endPosition; position++) {
                if (!keyNulls.value(position) && addHashKey(keyValues.value(position))) {
                    distinctPositions[count++] = position;
                    currentSize++;
                }
            }
            size = currentSize;
            return count;
        }

        private boolean addKey(long key)
        {
            if (bitmapPages != null) {
                return addBitmapKey(key);
            }
            if (!addHashKey(key)) {
                return false;
            }
            size++;
            // Representation selection is a bounded admission phase, not a permanent steady-state tax. A stream
            // whose first window is sparse remains a hash set; continuing to maintain its extrema and retest the
            // same predicate made wide 64-bit domains pay several operations for every later distinct key.
            if (ADAPTIVE_PAGED_BITMAP && size <= MIN_BITMAP_KEYS) {
                minimumKey = Math.min(minimumKey, key);
                maximumKey = Math.max(maximumKey, key);
                if (size == MIN_BITMAP_KEYS && denseEnoughForBitmap()) {
                    convertToBitmap();
                }
            }
            return true;
        }

        private boolean addBitmapKey(long key)
        {
            long pageId = key >> PAGE_SHIFT;
            long[] page = bitmapPages.get(pageId);
            if (page == null) {
                page = arrayPool.borrowLongs(PAGE_WORDS);
                Arrays.fill(page, 0);
                bitmapPages.put(pageId, page);
                if ((long) bitmapPages.size() * PAGE_BITS > Math.max(MIN_BITMAP_KEYS, size) * MAX_BITS_PER_KEY) {
                    convertToHash();
                    return addKey(key);
                }
            }
            int pagePosition = (int) key & (PAGE_BITS - 1);
            int wordIndex = pagePosition >>> 6;
            long bit = 1L << pagePosition;
            if ((page[wordIndex] & bit) != 0) {
                return false;
            }
            page[wordIndex] |= bit;
            size++;
            return true;
        }

        private boolean denseEnoughForBitmap()
        {
            long range = maximumKey - minimumKey;
            return range >= 0 && range / MAX_BITS_PER_KEY < size;
        }

        private void convertToBitmap()
        {
            bitmapPages = new Long2ObjectOpenHashMap<>();
            forEachHashKey(key -> {
                long pageId = key >> PAGE_SHIFT;
                long[] page = bitmapPages.get(pageId);
                if (page == null) {
                    page = arrayPool.borrowLongs(PAGE_WORDS);
                    Arrays.fill(page, 0);
                    bitmapPages.put(pageId, page);
                }
                int pagePosition = (int) key & (PAGE_BITS - 1);
                page[pagePosition >>> 6] |= 1L << pagePosition;
            });
            releaseHash();
        }

        private void convertToHash()
        {
            createHash(Math.max(MIN_BITMAP_KEYS, size));
            for (Long2ObjectMap.Entry<long[]> entry : bitmapPages.long2ObjectEntrySet()) {
                long pageBase = entry.getLongKey() << PAGE_SHIFT;
                long[] page = entry.getValue();
                for (int wordIndex = 0; wordIndex < page.length; wordIndex++) {
                    long word = page[wordIndex];
                    while (word != 0) {
                        int bit = Long.numberOfTrailingZeros(word);
                        addHashKey(pageBase | ((long) wordIndex << 6) | bit);
                        word &= word - 1;
                    }
                }
                arrayPool.release(page);
            }
            bitmapPages = null;
        }

        @Override
        public void releaseBuffers()
        {
            releaseHash();
            if (bitmapPages != null) {
                for (long[] page : bitmapPages.values()) {
                    arrayPool.release(page);
                }
                bitmapPages = null;
            }
            pooledKeys = null;
        }

        private void createHash(int expectedSize)
        {
            pooledKeys = new PooledLongHashSet(expectedSize);
        }

        private boolean hashPresent()
        {
            return pooledKeys != null;
        }

        private int hashSize()
        {
            return pooledKeys.size();
        }

        private void ensureHashCapacity(int expectedSize)
        {
            pooledKeys.ensureCapacity(expectedSize);
        }

        private boolean addHashKey(long key)
        {
            return pooledKeys.add(key);
        }

        private void forEachHashKey(java.util.function.LongConsumer consumer)
        {
            pooledKeys.forEach(consumer);
        }

        private void releaseHash()
        {
            if (pooledKeys != null) {
                pooledKeys.releaseBuffers();
                pooledKeys = null;
            }
        }
    }

    private static final class FlatDistinctIndex
            implements DistinctIndex
    {
        private static final boolean EMPTY_BINARY_FAST_PATH =
                Boolean.parseBoolean(System.getProperty("nitro.distinct.emptyBinaryFastPath", "true"));
        private static final boolean FILTER_SENTINEL_BEFORE_HASH =
                Boolean.parseBoolean(System.getProperty("nitro.distinct.filterSentinelBeforeHash", "true"));
        private final PrimitiveArrayPool arrayPool = PrimitiveArrayPool.shared();
        private final FlatKeyLayout layout;
        private final FlatGroupingTable table;
        private int[] probePositions;
        private boolean emptyBinarySeen;

        private FlatDistinctIndex(FlatKeyLayout layout, int expectedSize)
        {
            this.layout = layout;
            // A set assigns one monotonically increasing ordinal per retained record and never exposes or
            // reorders that ordinal. Record index is therefore the exact group id: let the general table's
            // identity mode avoid a redundant slot->group array and reverse group->record map.
            this.table = new FlatGroupingTable(layout, expectedSize, true);
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }
            if (isTrackedSentinel(values, position)) {
                if (emptyBinarySeen) {
                    return false;
                }
                emptyBinarySeen = true;
                return true;
            }
            int recordCount = table.recordCount();
            return table.assignGroup(values, position, recordCount) == recordCount;
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            boolean nullFree = true;
            for (Vector nullsVector : nulls) {
                if (!VectorAccess.isAllFalseNulls(nullsVector)) {
                    nullFree = false;
                    break;
                }
            }

            table.beginBatch(values, nulls);
            try {
                layout.admitFrequentDictionarySentinel(values, mask);
                if (FILTER_SENTINEL_BEFORE_HASH && hasTrackedSentinel(values)) {
                    return addFlatBinaryBatch(values, nulls, mask, distinctPositions, nullFree);
                }
                table.prepareBatchHashes(values, nulls, mask);
                int count = 0;
                for (int position : mask) {
                    if (!nullFree && hasNull(nulls, position)) {
                        continue;
                    }
                    if (isTrackedSentinel(values, position)) {
                        if (!emptyBinarySeen) {
                            emptyBinarySeen = true;
                            distinctPositions[count++] = position;
                        }
                        continue;
                    }
                    int recordCount = table.recordCount();
                    if (table.assignGroup(values, nulls, position, recordCount) == recordCount) {
                        distinctPositions[count++] = position;
                    }
                }
                return count;
            }
            finally {
                table.endBatch();
            }
        }

        private int addFlatBinaryBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions, boolean nullFree)
        {
            if (probePositions == null || probePositions.length < mask.count()) {
                int[] previous = probePositions;
                probePositions = arrayPool.borrowInts(mask.count());
                arrayPool.release(previous);
            }
            int positionCount = 0;
            int firstNewEmptyPosition = -1;
            for (int position : mask) {
                if (!nullFree && hasNull(nulls, position)) {
                    continue;
                }
                if (isTrackedSentinel(values, position)) {
                    if (!emptyBinarySeen && firstNewEmptyPosition < 0) {
                        firstNewEmptyPosition = position;
                    }
                    continue;
                }
                probePositions[positionCount++] = position;
            }

            table.prepareBatchHashes(values, nulls, probePositions, positionCount);
            int count = 0;
            boolean emptyEmitted = false;
            for (int index = 0; index < positionCount; index++) {
                int position = probePositions[index];
                if (!emptyEmitted && firstNewEmptyPosition >= 0 && firstNewEmptyPosition < position) {
                    emptyBinarySeen = true;
                    emptyEmitted = true;
                    distinctPositions[count++] = firstNewEmptyPosition;
                }
                int recordCount = table.recordCount();
                if (table.assignGroup(values, nulls, position, recordCount) == recordCount) {
                    distinctPositions[count++] = position;
                }
            }
            if (!emptyEmitted && firstNewEmptyPosition >= 0) {
                emptyBinarySeen = true;
                distinctPositions[count++] = firstNewEmptyPosition;
            }
            return count;
        }

        private boolean hasTrackedSentinel(Vector[] values)
        {
            return EMPTY_BINARY_FAST_PATH && layout.hasTrackedSentinel(values);
        }

        private boolean isTrackedSentinel(Vector[] values, int position)
        {
            return EMPTY_BINARY_FAST_PATH && layout.isTrackedSentinel(values, position);
        }

        private static boolean hasNull(Vector[] nulls, int position)
        {
            for (Vector nullsVector : nulls) {
                if (OperatorVectorSupport.isNull(nullsVector, position)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void releaseBuffers()
        {
            table.releaseBuffers();
            arrayPool.release(probePositions);
            probePositions = null;
        }
    }

    private static final class MultiLongDistinctIndex
            implements DistinctIndex
    {
        private static final VectorAccess.BooleanValues ALWAYS_FALSE = _ -> false;

        private final AbstractMultiLongGroupingTable table;
        private final VectorAccess.LongValues[] keyAccessors;
        private final VectorAccess.BooleanValues[] nullAccessors;
        private final int[] singlePosition = new int[1];
        private final int[] singleDistinctPosition = new int[1];

        private MultiLongDistinctIndex(int arity, int expectedSize)
        {
            table = MultiLongGroupingTableGenerator.createDistinct(arity, expectedSize);
            keyAccessors = new VectorAccess.LongValues[arity];
            nullAccessors = new VectorAccess.BooleanValues[arity];
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            table.ensureCapacity((long) table.size + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }
            prepareAccessors(values, nulls, true);
            singlePosition[0] = position;
            return table.assignDistinctBatch(keyAccessors, nullAccessors, singlePosition, 1, singleDistinctPosition, table.size) == 1;
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            boolean nullFree = true;
            for (Vector nullsVector : nulls) {
                if (!VectorAccess.isAllFalseNulls(nullsVector)) {
                    nullFree = false;
                    break;
                }
            }
            prepareAccessors(values, nulls, nullFree);

            int positionCount = 0;
            if (nullFree) {
                for (int position : mask) {
                    distinctPositions[positionCount++] = position;
                }
            }
            else {
                for (int position : mask) {
                    boolean hasNull = false;
                    for (VectorAccess.BooleanValues nullAccessor : nullAccessors) {
                        if (nullAccessor.value(position)) {
                            hasNull = true;
                            break;
                        }
                    }
                    if (!hasNull) {
                        distinctPositions[positionCount++] = position;
                    }
                }
            }
            if (nullFree) {
                return table.assignDistinctBatchNullFree(keyAccessors, nullAccessors, distinctPositions, positionCount, distinctPositions, table.size);
            }
            return table.assignDistinctBatch(keyAccessors, nullAccessors, distinctPositions, positionCount, distinctPositions, table.size);
        }

        @Override
        public int addNonNullBatch(Vector[] values, Vector[] nulls, int[] positions, int positionCount, int[] distinctPositions)
        {
            prepareAccessors(values, nulls, true);
            return table.assignDistinctBatchNullFree(keyAccessors, nullAccessors, positions, positionCount, distinctPositions, table.size);
        }

        private void prepareAccessors(Vector[] values, Vector[] nulls, boolean nullFree)
        {
            for (int index = 0; index < keyAccessors.length; index++) {
                keyAccessors[index] = VectorAccess.longValues(values[index]);
                nullAccessors[index] = nullFree ? ALWAYS_FALSE : VectorAccess.booleanValues(nulls[index]);
            }
        }

        @Override
        public void releaseBuffers()
        {
            table.releaseBuffers();
            Arrays.fill(keyAccessors, null);
            Arrays.fill(nullAccessors, null);
        }
    }

    /**
     * DISTINCT adapter over the same schema-generated compact table used by grouping. The table stores each key
     * once in packed reverse lanes, so growth rehashes only its compact slot index rather than copying a full
     * arity-wide record for every occupied slot. Values outside the compact domain promote exactly to the ordinary
     * full-width generated table.
     */
    private static final class AdaptiveMultiLongDistinctIndex
            implements DistinctIndex
    {
        private static final VectorAccess.BooleanValues ALWAYS_FALSE = _ -> false;
        private static final int[] EMPTY_POSITIONS = new int[0];
        private static final long[] EMPTY_GROUPS = new long[0];

        private final PrimitiveArrayPool arrayPool = PrimitiveArrayPool.shared();
        private final LongGroupingTable table;
        private final VectorAccess.LongValues[] keyAccessors;
        private final VectorAccess.BooleanValues[] nullAccessors;
        private final int[] singlePosition = new int[1];
        private int[] nonNullPositions = EMPTY_POSITIONS;
        private long[] assignedGroups = EMPTY_GROUPS;
        private long nextGroupId;

        private AdaptiveMultiLongDistinctIndex(int arity, int expectedSize)
        {
            table = AdaptiveLongGroupingTable.create(arity, expectedSize);
            keyAccessors = new VectorAccess.LongValues[arity];
            nullAccessors = new VectorAccess.BooleanValues[arity];
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            table.ensureCapacity(nextGroupId + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }
            prepareAccessors(values);
            ensureAssignedCapacity(values[0].length());
            singlePosition[0] = position;
            long startGroupId = nextGroupId;
            nextGroupId = table.assignBatch(keyAccessors, null, singlePosition, 1, assignedGroups, startGroupId);
            return nextGroupId != startGroupId;
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            prepareAccessors(values);
            ensureAssignedCapacity(values[0].length());
            ensurePositionCapacity(mask.selectedCount());
            int positionCount = 0;
            for (int position : mask) {
                if (!hasNull(nulls, position)) {
                    nonNullPositions[positionCount++] = position;
                }
            }
            boolean dense = positionCount == mask.size() && mask.all();
            return assignAndCollect(dense ? null : nonNullPositions, positionCount, distinctPositions);
        }

        @Override
        public int addNonNullBatch(Vector[] values, Vector[] nulls, int[] positions, int positionCount, int[] distinctPositions)
        {
            prepareAccessors(values);
            ensureAssignedCapacity(values[0].length());
            return assignAndCollect(positions, positionCount, distinctPositions);
        }

        @Override
        public int addNonNullDenseBatch(Vector[] values, Vector[] nulls, int positionCount, int[] positions, int[] distinctPositions)
        {
            prepareAccessors(values);
            ensureAssignedCapacity(values[0].length());
            return assignAndCollect(null, positionCount, distinctPositions);
        }

        private int assignAndCollect(int[] positions, int positionCount, int[] distinctPositions)
        {
            long startGroupId = nextGroupId;
            nextGroupId = table.assignBatch(keyAccessors, null, positions, positionCount, assignedGroups, startGroupId);
            long expectedNewGroup = startGroupId;
            int distinctCount = 0;
            for (int row = 0; row < positionCount; row++) {
                int position = positions == null ? row : positions[row];
                long groupId = assignedGroups[position];
                if (groupId == expectedNewGroup) {
                    distinctPositions[distinctCount++] = position;
                    expectedNewGroup++;
                }
            }
            if (expectedNewGroup != nextGroupId) {
                throw new IllegalStateException("Generated compact distinct table returned non-sequential group ids");
            }
            return distinctCount;
        }

        private void prepareAccessors(Vector[] values)
        {
            for (int index = 0; index < keyAccessors.length; index++) {
                keyAccessors[index] = VectorAccess.longValues(values[index]);
                nullAccessors[index] = ALWAYS_FALSE;
            }
        }

        private void ensurePositionCapacity(int size)
        {
            if (nonNullPositions.length >= size) {
                return;
            }
            arrayPool.release(nonNullPositions);
            nonNullPositions = arrayPool.borrowInts(size);
        }

        private void ensureAssignedCapacity(int size)
        {
            if (assignedGroups.length >= size) {
                return;
            }
            arrayPool.release(assignedGroups);
            assignedGroups = arrayPool.borrowLongs(size);
        }

        @Override
        public void releaseBuffers()
        {
            table.releaseBuffers();
            arrayPool.release(nonNullPositions);
            arrayPool.release(assignedGroups);
            nonNullPositions = EMPTY_POSITIONS;
            assignedGroups = EMPTY_GROUPS;
            Arrays.fill(keyAccessors, null);
            Arrays.fill(nullAccessors, null);
        }
    }

    private static final class LongPairDistinctIndex
            implements DistinctIndex
    {
        private static final float LOAD_FACTOR = 0.75f;
        private static final boolean TAGGED_HASH_TABLE =
                Boolean.parseBoolean(System.getProperty("nitro.distinct.taggedLongPairHash", "true"));
        private static final boolean NULL_FREE_BATCH =
                Boolean.parseBoolean(System.getProperty("nitro.distinct.longPairNullFreeBatch", "true"));
        private final PrimitiveArrayPool arrayPool = PrimitiveArrayPool.shared();

        private long[] firstKeys;
        private long[] secondKeys;
        private boolean[] occupied;
        private byte[] tags;
        private int mask;
        private int maxFill;
        private int size;

        private LongPairDistinctIndex(int expectedSize)
        {
            int capacity = DistinctKeySet.capacity(expectedSize);
            allocate(capacity);
        }

        private void allocate(int capacity)
        {
            firstKeys = arrayPool.borrowLongs(capacity);
            secondKeys = arrayPool.borrowLongs(capacity);
            if (TAGGED_HASH_TABLE) {
                tags = arrayPool.borrowBytes(capacity);
                Arrays.fill(tags, (byte) 0);
                occupied = null;
            }
            else {
                occupied = arrayPool.borrowBooleans(capacity);
                Arrays.fill(occupied, false);
                tags = null;
            }
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            ensureCapacity(size + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }

            long first = OperatorVectorSupport.longValue(values[0], position);
            long second = OperatorVectorSupport.longValue(values[1], position);
            int index = findSlot(first, second);
            if (isOccupied(index)) {
                return false;
            }

            firstKeys[index] = first;
            secondKeys[index] = second;
            markOccupied(index, first, second);
            size++;
            if (size >= maxFill) {
                rehash(firstKeys.length * 2);
            }
            return true;
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
            boolean nullFree = NULL_FREE_BATCH &&
                    VectorAccess.isAllFalseNulls(nulls[0]) &&
                    VectorAccess.isAllFalseNulls(nulls[1]);
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                if (nullFree) {
                    for (int position = 0; position < size; position++) {
                        if (addKey(firstValues.value(position), secondValues.value(position))) {
                            distinctPositions[count++] = position;
                        }
                    }
                    return count;
                }
                for (int position = 0; position < size; position++) {
                    if (firstNulls.value(position) || secondNulls.value(position)) {
                        continue;
                    }
                    if (addKey(firstValues.value(position), secondValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                if (nullFree) {
                    for (int position : mask) {
                        if (addKey(firstValues.value(position), secondValues.value(position))) {
                            distinctPositions[count++] = position;
                        }
                    }
                    return count;
                }
                for (int position : mask) {
                    if (firstNulls.value(position) || secondNulls.value(position)) {
                        continue;
                    }
                    if (addKey(firstValues.value(position), secondValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }

        private boolean addKey(long first, long second)
        {
            int index = findSlot(first, second);
            if (isOccupied(index)) {
                return false;
            }
            firstKeys[index] = first;
            secondKeys[index] = second;
            markOccupied(index, first, second);
            size++;
            if (size >= maxFill) {
                rehash(firstKeys.length * 2);
            }
            return true;
        }

        private int findSlot(long first, long second)
        {
            if (TAGGED_HASH_TABLE) {
                long hash = hash64(first, second);
                byte tag = hashTag(hash);
                int index = ((int) hash) & mask;
                while (tags[index] != 0) {
                    if (tags[index] == tag && firstKeys[index] == first && secondKeys[index] == second) {
                        return index;
                    }
                    index = (index + 1) & mask;
                }
                return index;
            }
            int index = mix(first, second) & mask;
            while (occupied[index] && (firstKeys[index] != first || secondKeys[index] != second)) {
                index = (index + 1) & mask;
            }
            return index;
        }

        private void ensureCapacity(int expectedSize)
        {
            if (expectedSize < maxFill) {
                return;
            }

            int capacity = firstKeys.length;
            while (expectedSize >= (int) (capacity * LOAD_FACTOR)) {
                capacity <<= 1;
            }
            rehash(capacity);
        }

        private void rehash(int capacity)
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            boolean[] previousOccupied = occupied;
            byte[] previousTags = tags;

            allocate(capacity);
            size = 0;

            for (int index = 0; index < previousFirstKeys.length; index++) {
                if (TAGGED_HASH_TABLE ? previousTags[index] == 0 : !previousOccupied[index]) {
                    continue;
                }
                int newIndex = findSlot(previousFirstKeys[index], previousSecondKeys[index]);
                firstKeys[newIndex] = previousFirstKeys[index];
                secondKeys[newIndex] = previousSecondKeys[index];
                markOccupied(newIndex, previousFirstKeys[index], previousSecondKeys[index]);
                size++;
            }
            arrayPool.release(previousFirstKeys);
            arrayPool.release(previousSecondKeys);
            arrayPool.release(previousOccupied);
            arrayPool.release(previousTags);
        }

        private boolean isOccupied(int index)
        {
            return TAGGED_HASH_TABLE ? tags[index] != 0 : occupied[index];
        }

        private void markOccupied(int index, long first, long second)
        {
            if (TAGGED_HASH_TABLE) {
                tags[index] = hashTag(hash64(first, second));
            }
            else {
                occupied[index] = true;
            }
        }

        @Override
        public void releaseBuffers()
        {
            arrayPool.release(firstKeys);
            arrayPool.release(secondKeys);
            arrayPool.release(occupied);
            arrayPool.release(tags);
            firstKeys = null;
            secondKeys = null;
            occupied = null;
            tags = null;
        }

        private static int mix(long first, long second)
        {
            return (int) hash64(first, second);
        }

        private static long hash64(long first, long second)
        {
            long hash = first * 0x9E3779B97F4A7C15L + second * 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= hash >>> 33;
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            return hash;
        }

        private static byte hashTag(long hash)
        {
            return (byte) ((hash >>> 56) | 0x80L);
        }
    }

    private static final class GroupedLongDistinctIndex
            implements DistinctIndex
    {
        private static final int MAX_RESERVED_BATCH_GROUPS = 1 << 16;
        private static final float LOAD_FACTOR = 0.75f;
        private static final int INITIAL_TABLE_SIZE = 16;
        private final PrimitiveArrayPool arrayPool = PrimitiveArrayPool.shared();

        private long[][] tables = new long[16][];
        private int[] sizes = new int[16];
        private boolean[] containsZero = new boolean[16];

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (OperatorVectorSupport.isNull(nulls[1], position)) {
                return false;
            }
            return addKey(toIntExact(OperatorVectorSupport.longValue(values[0], position)), OperatorVectorSupport.longValue(values[1], position));
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            VectorAccess.LongValues groupValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues keyValues = VectorAccess.longValues(values[1]);
            boolean nullFree = VectorAccess.isAllFalseNulls(nulls[1]);
            VectorAccess.BooleanValues keyNulls = nullFree ? null : VectorAccess.booleanValues(nulls[1]);
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if ((nullFree || !keyNulls.value(position)) && addKey(toIntExact(groupValues.value(position)), keyValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
                return count;
            }
            for (int position : mask) {
                if ((nullFree || !keyNulls.value(position)) && addKey(toIntExact(groupValues.value(position)), keyValues.value(position))) {
                    distinctPositions[count++] = position;
                }
            }
            return count;
        }

        @Override
        public int addGroupedBatch(Vector[] values, Vector[] nulls, Mask mask, int groupCount, int[] distinctPositions)
        {
            // With millions of groups, eagerly growing the three group-metadata arrays at every batch boundary
            // loses locality versus the ordinary incremental path. Small/stable group domains amortize the
            // reservation and let the hot row loop omit both conversion and capacity guards.
            if (groupCount > MAX_RESERVED_BATCH_GROUPS) {
                return addBatch(values, nulls, mask, distinctPositions);
            }
            ensureGroupCapacity(groupCount);
            VectorAccess.LongValues groupValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues keyValues = VectorAccess.longValues(values[1]);
            boolean nullFree = VectorAccess.isAllFalseNulls(nulls[1]);
            VectorAccess.BooleanValues keyNulls = nullFree ? null : VectorAccess.booleanValues(nulls[1]);
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if ((nullFree || !keyNulls.value(position)) && addReservedKey((int) groupValues.value(position), keyValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
                return count;
            }
            for (int position : mask) {
                if ((nullFree || !keyNulls.value(position)) && addReservedKey((int) groupValues.value(position), keyValues.value(position))) {
                    distinctPositions[count++] = position;
                }
            }
            return count;
        }

        private boolean addKey(int group, long key)
        {
            if (group < 0) {
                throw new IllegalArgumentException("group is negative: " + group);
            }
            ensureGroupCapacity(group + 1);
            return addReservedKey(group, key);
        }

        private boolean addReservedKey(int group, long key)
        {
            if (key == 0) {
                if (containsZero[group]) {
                    return false;
                }
                containsZero[group] = true;
                sizes[group]++;
                return true;
            }

            long[] table = tables[group];
            if (table == null) {
                table = allocateTable(INITIAL_TABLE_SIZE);
                tables[group] = table;
            }
            if (sizes[group] + 1 >= (int) (table.length * LOAD_FACTOR)) {
                table = growTable(table);
                tables[group] = table;
            }

            int hash = GroupingState.hashLong(key);
            int slot = hash & (table.length - 1);
            while (table[slot] != 0) {
                if (table[slot] == key) {
                    return false;
                }
                slot = (slot + 1) & (table.length - 1);
            }
            table[slot] = key;
            sizes[group]++;
            return true;
        }

        private void ensureGroupCapacity(int needed)
        {
            if (needed <= tables.length) {
                return;
            }
            int capacity = Integer.highestOneBit(needed - 1) << 1;
            tables = Arrays.copyOf(tables, capacity);
            sizes = Arrays.copyOf(sizes, capacity);
            containsZero = Arrays.copyOf(containsZero, capacity);
        }

        private long[] allocateTable(int capacity)
        {
            long[] table = arrayPool.borrowLongs(capacity);
            Arrays.fill(table, 0);
            return table;
        }

        private long[] growTable(long[] previous)
        {
            long[] table = allocateTable(previous.length * 2);
            int mask = table.length - 1;
            for (long key : previous) {
                if (key == 0) {
                    continue;
                }
                int hash = GroupingState.hashLong(key);
                int slot = hash & mask;
                while (table[slot] != 0) {
                    slot = (slot + 1) & mask;
                }
                table[slot] = key;
            }
            arrayPool.release(previous);
            return table;
        }

        @Override
        public void releaseBuffers()
        {
            for (long[] table : tables) {
                arrayPool.release(table);
            }
            tables = new long[0][];
            sizes = new int[0];
            containsZero = new boolean[0];
        }
    }

    private static final class LongTripleDistinctIndex
            implements DistinctIndex
    {
        private static final float LOAD_FACTOR = 0.75f;

        private long[] firstKeys;
        private long[] secondKeys;
        private long[] thirdKeys;
        private boolean[] occupied;
        private int mask;
        private int maxFill;
        private int size;

        private LongTripleDistinctIndex(int expectedSize)
        {
            int capacity = capacity(expectedSize);
            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            occupied = new boolean[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            ensureCapacity(size + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }

            long first = OperatorVectorSupport.longValue(values[0], position);
            long second = OperatorVectorSupport.longValue(values[1], position);
            long third = OperatorVectorSupport.longValue(values[2], position);
            return addKey(first, second, third);
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.LongValues thirdValues = VectorAccess.longValues(values[2]);
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
            VectorAccess.BooleanValues thirdNulls = VectorAccess.booleanValues(nulls[2]);
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if (firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position)) {
                        continue;
                    }
                    if (addKey(firstValues.value(position), secondValues.value(position), thirdValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position)) {
                        continue;
                    }
                    if (addKey(firstValues.value(position), secondValues.value(position), thirdValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }

        private boolean addKey(long first, long second, long third)
        {
            int index = findSlot(first, second, third);
            if (occupied[index]) {
                return false;
            }
            firstKeys[index] = first;
            secondKeys[index] = second;
            thirdKeys[index] = third;
            occupied[index] = true;
            size++;
            if (size >= maxFill) {
                rehash(occupied.length * 2);
            }
            return true;
        }

        private int findSlot(long first, long second, long third)
        {
            int index = mix(first, second, third) & mask;
            while (occupied[index] && (firstKeys[index] != first || secondKeys[index] != second || thirdKeys[index] != third)) {
                index = (index + 1) & mask;
            }
            return index;
        }

        private void ensureCapacity(int expectedSize)
        {
            if (expectedSize < maxFill) {
                return;
            }

            int capacity = occupied.length;
            while (expectedSize >= (int) (capacity * LOAD_FACTOR)) {
                capacity <<= 1;
            }
            rehash(capacity);
        }

        private void rehash(int capacity)
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            long[] previousThirdKeys = thirdKeys;
            boolean[] previousOccupied = occupied;

            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            occupied = new boolean[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int index = 0; index < previousOccupied.length; index++) {
                if (!previousOccupied[index]) {
                    continue;
                }
                int newIndex = findSlot(previousFirstKeys[index], previousSecondKeys[index], previousThirdKeys[index]);
                firstKeys[newIndex] = previousFirstKeys[index];
                secondKeys[newIndex] = previousSecondKeys[index];
                thirdKeys[newIndex] = previousThirdKeys[index];
                occupied[newIndex] = true;
                size++;
            }
        }

        private static int mix(long first, long second, long third)
        {
            long hash = first * 0x9E3779B97F4A7C15L + second * 0xC4CEB9FE1A85EC53L + third * 0x94D049BB133111EBL;
            hash ^= hash >>> 33;
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= hash >>> 33;
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            return (int) hash;
        }
    }

    private static final class LongQuadDistinctIndex
            implements DistinctIndex
    {
        private static final float LOAD_FACTOR = 0.75f;

        private long[] firstKeys;
        private long[] secondKeys;
        private long[] thirdKeys;
        private long[] fourthKeys;
        private boolean[] occupied;
        private int mask;
        private int maxFill;
        private int size;

        private LongQuadDistinctIndex(int expectedSize)
        {
            int capacity = capacity(expectedSize);
            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            fourthKeys = new long[capacity];
            occupied = new boolean[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            ensureCapacity(size + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }

            long first = OperatorVectorSupport.longValue(values[0], position);
            long second = OperatorVectorSupport.longValue(values[1], position);
            long third = OperatorVectorSupport.longValue(values[2], position);
            long fourth = OperatorVectorSupport.longValue(values[3], position);
            return addKey(first, second, third, fourth);
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.LongValues thirdValues = VectorAccess.longValues(values[2]);
            VectorAccess.LongValues fourthValues = VectorAccess.longValues(values[3]);
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
            VectorAccess.BooleanValues thirdNulls = VectorAccess.booleanValues(nulls[2]);
            VectorAccess.BooleanValues fourthNulls = VectorAccess.booleanValues(nulls[3]);
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if (firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position) || fourthNulls.value(position)) {
                        continue;
                    }
                    if (addKey(firstValues.value(position), secondValues.value(position), thirdValues.value(position), fourthValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position) || fourthNulls.value(position)) {
                        continue;
                    }
                    if (addKey(firstValues.value(position), secondValues.value(position), thirdValues.value(position), fourthValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }

        private boolean addKey(long first, long second, long third, long fourth)
        {
            int index = findSlot(first, second, third, fourth);
            if (occupied[index]) {
                return false;
            }
            firstKeys[index] = first;
            secondKeys[index] = second;
            thirdKeys[index] = third;
            fourthKeys[index] = fourth;
            occupied[index] = true;
            size++;
            if (size >= maxFill) {
                rehash(occupied.length * 2);
            }
            return true;
        }

        private int findSlot(long first, long second, long third, long fourth)
        {
            int index = mix(first, second, third, fourth) & mask;
            while (occupied[index] && (firstKeys[index] != first || secondKeys[index] != second || thirdKeys[index] != third || fourthKeys[index] != fourth)) {
                index = (index + 1) & mask;
            }
            return index;
        }

        private void ensureCapacity(int expectedSize)
        {
            if (expectedSize < maxFill) {
                return;
            }

            int capacity = occupied.length;
            while (expectedSize >= (int) (capacity * LOAD_FACTOR)) {
                capacity <<= 1;
            }
            rehash(capacity);
        }

        private void rehash(int capacity)
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            long[] previousThirdKeys = thirdKeys;
            long[] previousFourthKeys = fourthKeys;
            boolean[] previousOccupied = occupied;

            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            fourthKeys = new long[capacity];
            occupied = new boolean[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int index = 0; index < previousOccupied.length; index++) {
                if (!previousOccupied[index]) {
                    continue;
                }
                int newIndex = findSlot(previousFirstKeys[index], previousSecondKeys[index], previousThirdKeys[index], previousFourthKeys[index]);
                firstKeys[newIndex] = previousFirstKeys[index];
                secondKeys[newIndex] = previousSecondKeys[index];
                thirdKeys[newIndex] = previousThirdKeys[index];
                fourthKeys[newIndex] = previousFourthKeys[index];
                occupied[newIndex] = true;
                size++;
            }
        }

        private static int mix(long first, long second, long third, long fourth)
        {
            long hash = first * 0x9E3779B97F4A7C15L
                    + second * 0xC4CEB9FE1A85EC53L
                    + third * 0x94D049BB133111EBL
                    + fourth * 0xBF58476D1CE4E5B9L;
            hash ^= hash >>> 33;
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= hash >>> 33;
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            return (int) hash;
        }
    }

    private static final class ObjectDistinctIndex
            implements DistinctIndex
    {
        private final ObjectOpenHashSet<Object> keys = new ObjectOpenHashSet<>();
        private final OperatorKeySemantics.Key[] probeKeys;
        private final OperatorKeySemantics.CompositeProbeKey compositeProbeKey;

        private ObjectDistinctIndex(int keyCount)
        {
            this.probeKeys = new OperatorKeySemantics.Key[keyCount];
            this.compositeProbeKey = keyCount > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(keyCount) : null;
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            keys.ensureCapacity(keys.size() + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            OperatorKeySemantics.Key key = keyForPosition(values, nulls, position);
            if (key == null) {
                return false;
            }
            if (keys.contains(key)) {
                return false;
            }
            keys.add(OperatorKeySemantics.ownedKey(key));
            return true;
        }

        private OperatorKeySemantics.Key keyForPosition(Vector[] values, Vector[] nulls, int position)
        {
            for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
                if (probeKeys[keyIndex] == null) {
                    probeKeys[keyIndex] = OperatorKeySemantics.reusableProbeKey(values[keyIndex]);
                }
                OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values[keyIndex], nulls[keyIndex], position, probeKeys[keyIndex]);
                if (key == null) {
                    return null;
                }
                probeKeys[keyIndex] = key;
            }
            if (probeKeys.length == 1) {
                return probeKeys[0];
            }
            return OperatorKeySemantics.probeCompositeKey(Arrays.copyOf(probeKeys, probeKeys.length), compositeProbeKey);
        }
    }

    /**
     * Wraps a fast {@link DistinctIndex} to add SQL {@code DISTINCT}/{@code UNION} null handling. Fully non-null
     * batches are forwarded unchanged to the delegate (preserving its specialized batch path); a row with any
     * NULL key column is routed to a separate null-aware set keyed by a {@link OperatorKeySemantics.CompositeKey}
     * whose null columns are represented by {@code null} entries, so equal-null rows collapse to one survivor
     * while staying distinct from every concrete-valued row.
     */
    private static final class RetainNullsDistinctIndex
            implements DistinctIndex
    {
        private static final int[] EMPTY_POSITIONS = new int[0];

        private final DistinctIndex delegate;
        private final int keyCount;
        private final OperatorKeySemantics.Key[] probeKeys;
        private final VectorAccess.BooleanValues[] nullAccessors;
        private final ObjectOpenHashSet<Object> nullContainingKeys = new ObjectOpenHashSet<>();
        private int[] nonNullDistinctPositions = EMPTY_POSITIONS;

        private RetainNullsDistinctIndex(DistinctIndex delegate, int keyCount)
        {
            this.delegate = delegate;
            this.keyCount = keyCount;
            this.probeKeys = new OperatorKeySemantics.Key[keyCount];
            this.nullAccessors = new VectorAccess.BooleanValues[keyCount];
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            delegate.reserveAdditional(additionalEntries);
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            prepareNullAccessors(nulls);
            if (!hasPreparedNull(position)) {
                return delegate.add(values, nulls, position);
            }
            return nullContainingKeys.add(buildNullAwareKey(values, nulls, position));
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            if (!hasNullStream(nulls)) {
                return delegate.addBatch(values, nulls, mask, distinctPositions);
            }
            prepareNullAccessors(nulls);
            int selectedCount = mask.selectedCount();
            if (nonNullDistinctPositions.length < selectedCount) {
                int[] previous = nonNullDistinctPositions;
                nonNullDistinctPositions = PrimitiveArrayPool.shared().borrowInts(selectedCount);
                PrimitiveArrayPool.shared().release(previous);
            }

            // Partition once: keep the relatively rare null-containing keys in the generic SQL-null set,
            // and pass the proven-non-null position vector to the delegate's specialized batch kernel.
            int nullCount = 0;
            int nonNullPositionCount = 0;
            for (int position : mask) {
                if (hasPreparedNull(position)) {
                    if (nullContainingKeys.add(buildNullAwareKey(values, nulls, position))) {
                        distinctPositions[nullCount++] = position;
                    }
                }
                else {
                    nonNullDistinctPositions[nonNullPositionCount++] = position;
                }
            }
            int nonNullCount = nonNullPositionCount == selectedCount && mask.all()
                    ? delegate.addNonNullDenseBatch(values, nulls, nonNullPositionCount, nonNullDistinctPositions, nonNullDistinctPositions)
                    : delegate.addNonNullBatch(values, nulls, nonNullDistinctPositions, nonNullPositionCount, nonNullDistinctPositions);

            // Both lists preserve mask order. Merge backward so the null list can remain in the caller's
            // output buffer without another scratch allocation.
            int nullIndex = nullCount - 1;
            int nonNullIndex = nonNullCount - 1;
            int outputIndex = nullCount + nonNullCount - 1;
            while (nullIndex >= 0 && nonNullIndex >= 0) {
                if (distinctPositions[nullIndex] > nonNullDistinctPositions[nonNullIndex]) {
                    distinctPositions[outputIndex--] = distinctPositions[nullIndex--];
                }
                else {
                    distinctPositions[outputIndex--] = nonNullDistinctPositions[nonNullIndex--];
                }
            }
            while (nonNullIndex >= 0) {
                distinctPositions[outputIndex--] = nonNullDistinctPositions[nonNullIndex--];
            }
            return nullCount + nonNullCount;
        }

        private OperatorKeySemantics.Key buildNullAwareKey(Vector[] values, Vector[] nulls, int position)
        {
            OperatorKeySemantics.Key[] keys = new OperatorKeySemantics.Key[keyCount];
            for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
                VectorAccess.BooleanValues nullAccessor = nullAccessors[keyIndex];
                if (nullAccessor != null && nullAccessor.value(position)) {
                    keys[keyIndex] = null;
                    continue;
                }
                if (probeKeys[keyIndex] == null) {
                    probeKeys[keyIndex] = OperatorKeySemantics.reusableProbeKey(values[keyIndex]);
                }
                OperatorKeySemantics.Key probe = OperatorKeySemantics.probeKey(values[keyIndex], nulls[keyIndex], position, probeKeys[keyIndex]);
                probeKeys[keyIndex] = probe;
                keys[keyIndex] = OperatorKeySemantics.ownedKey(probe);
            }
            return new OperatorKeySemantics.CompositeKey(keys);
        }

        private void prepareNullAccessors(Vector[] nulls)
        {
            for (int index = 0; index < keyCount; index++) {
                Vector nullVector = nulls[index];
                nullAccessors[index] = VectorAccess.isAllFalseNulls(nullVector) ? null : VectorAccess.booleanValues(nullVector);
            }
        }

        private boolean hasPreparedNull(int position)
        {
            for (VectorAccess.BooleanValues nullAccessor : nullAccessors) {
                if (nullAccessor != null && nullAccessor.value(position)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void releaseBuffers()
        {
            delegate.releaseBuffers();
            PrimitiveArrayPool.shared().release(nonNullDistinctPositions);
            nonNullDistinctPositions = EMPTY_POSITIONS;
            Arrays.fill(nullAccessors, null);
        }

        private static boolean hasNullStream(Vector[] nulls)
        {
            for (Vector nullsVector : nulls) {
                if (!VectorAccess.isAllFalseNulls(nullsVector)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static boolean isIntegerVector(Vector vector)
    {
        return switch (OperatorVectorSupport.flatten(vector)) {
            case I32Vector _ -> true;
            case I64Vector _ -> true;
            default -> false;
        };
    }

    private static boolean allIntegerVectors(Vector[] vectors)
    {
        for (Vector vector : vectors) {
            if (!isIntegerVector(vector)) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasNull(Vector[] nulls, int position)
    {
        for (Vector nullsVector : nulls) {
            if (OperatorVectorSupport.isNull(nullsVector, position)) {
                return true;
            }
        }
        return false;
    }

    private static int capacity(int expectedSize)
    {
        int capacity = 16;
        while (capacity < expectedSize / LOAD_FACTOR) {
            capacity <<= 1;
        }
        return capacity;
    }

    private static final float LOAD_FACTOR = 0.75f;
}
