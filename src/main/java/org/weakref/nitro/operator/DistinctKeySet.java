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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;

import java.util.Arrays;

final class DistinctKeySet
{
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

    public void reserveAdditional(int additionalEntries)
    {
        index.reserveAdditional(additionalEntries);
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

        default void reserveAdditional(int additionalEntries) {}
    }

    private static final class LongDistinctIndex
            implements DistinctIndex
    {
        private final LongOpenHashSet keys;

        private LongDistinctIndex(int expectedSize)
        {
            this.keys = new LongOpenHashSet(expectedSize);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            keys.ensureCapacity(keys.size() + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (OperatorVectorSupport.isNull(nulls[0], position)) {
                return false;
            }
            return keys.add(OperatorVectorSupport.longValue(values[0], position));
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            VectorAccess.LongValues keyValues = VectorAccess.longValues(values[0]);
            VectorAccess.BooleanValues keyNulls = VectorAccess.booleanValues(nulls[0]);
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if (keyNulls.value(position)) {
                        continue;
                    }
                    if (keys.add(keyValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (keyNulls.value(position)) {
                        continue;
                    }
                    if (keys.add(keyValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }
    }

    private static final class FlatDistinctIndex
            implements DistinctIndex
    {
        private final FlatGroupingTable table;
        private long nextGroupId;

        private FlatDistinctIndex(FlatKeyLayout layout, int expectedSize)
        {
            this.table = new FlatGroupingTable(layout, expectedSize);
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }
            long newGroupId = nextGroupId;
            long assigned = table.assignGroup(values, position, newGroupId);
            if (assigned == newGroupId) {
                nextGroupId++;
                return true;
            }
            return false;
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
    }

    private static final class LongPairDistinctIndex
            implements DistinctIndex
    {
        private static final float LOAD_FACTOR = 0.75f;

        private long[] firstKeys;
        private long[] secondKeys;
        private boolean[] occupied;
        private int mask;
        private int maxFill;
        private int size;

        private LongPairDistinctIndex(int expectedSize)
        {
            int capacity = capacity(expectedSize);
            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
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
            int index = findSlot(first, second);
            if (occupied[index]) {
                return false;
            }

            firstKeys[index] = first;
            secondKeys[index] = second;
            occupied[index] = true;
            size++;
            if (size >= maxFill) {
                rehash(occupied.length * 2);
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
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
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
            if (occupied[index]) {
                return false;
            }
            firstKeys[index] = first;
            secondKeys[index] = second;
            occupied[index] = true;
            size++;
            if (size >= maxFill) {
                rehash(occupied.length * 2);
            }
            return true;
        }

        private int findSlot(long first, long second)
        {
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
            boolean[] previousOccupied = occupied;

            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            occupied = new boolean[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int index = 0; index < previousOccupied.length; index++) {
                if (!previousOccupied[index]) {
                    continue;
                }
                int newIndex = findSlot(previousFirstKeys[index], previousSecondKeys[index]);
                firstKeys[newIndex] = previousFirstKeys[index];
                secondKeys[newIndex] = previousSecondKeys[index];
                occupied[newIndex] = true;
                size++;
            }
        }

        private static int mix(long first, long second)
        {
            long hash = first * 0x9E3779B97F4A7C15L + second * 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= hash >>> 33;
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            return (int) hash;
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
        private final DistinctIndex delegate;
        private final int keyCount;
        private final OperatorKeySemantics.Key[] probeKeys;
        private final ObjectOpenHashSet<Object> nullContainingKeys = new ObjectOpenHashSet<>();

        private RetainNullsDistinctIndex(DistinctIndex delegate, int keyCount)
        {
            this.delegate = delegate;
            this.keyCount = keyCount;
            this.probeKeys = new OperatorKeySemantics.Key[keyCount];
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            delegate.reserveAdditional(additionalEntries);
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (!hasNull(nulls, position)) {
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

        private OperatorKeySemantics.Key buildNullAwareKey(Vector[] values, Vector[] nulls, int position)
        {
            OperatorKeySemantics.Key[] keys = new OperatorKeySemantics.Key[keyCount];
            for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
                if (OperatorVectorSupport.isNull(nulls[keyIndex], position)) {
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

        private static boolean hasNullStream(Vector[] nulls)
        {
            for (Vector nullsVector : nulls) {
                if (nullsVector != null) {
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
