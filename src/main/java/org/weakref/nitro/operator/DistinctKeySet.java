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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

final class DistinctKeySet
{
    private final DistinctIndex index;

    private DistinctKeySet(DistinctIndex index)
    {
        this.index = index;
    }

    public static DistinctKeySet create(Vector[] samples)
    {
        if (samples.length == 1 && isIntegerVector(samples[0])) {
            return new DistinctKeySet(new LongDistinctIndex(Math.max(16, samples[0].length())));
        }
        if (samples.length == 2 && isIntegerVector(samples[0]) && isIntegerVector(samples[1])) {
            return new DistinctKeySet(new LongPairDistinctIndex(Math.max(16, samples[0].length())));
        }
        if (samples.length == 3 && isIntegerVector(samples[0]) && isIntegerVector(samples[1]) && isIntegerVector(samples[2])) {
            return new DistinctKeySet(new LongTripleDistinctIndex(Math.max(16, samples[0].length())));
        }
        if (samples.length == 4 && isIntegerVector(samples[0]) && isIntegerVector(samples[1]) && isIntegerVector(samples[2]) && isIntegerVector(samples[3])) {
            return new DistinctKeySet(new LongQuadDistinctIndex(Math.max(16, samples[0].length())));
        }
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(samples);
        if (layout != null) {
            return new DistinctKeySet(new FlatDistinctIndex(layout, Math.max(16, samples[0].length())));
        }
        return new DistinctKeySet(new ObjectDistinctIndex(samples.length));
    }

    public boolean add(Vector[] values, BooleanVector[] nulls, int position)
    {
        return index.add(values, nulls, position);
    }

    public void reserveAdditional(int additionalEntries)
    {
        index.reserveAdditional(additionalEntries);
    }

    private interface DistinctIndex
    {
        boolean add(Vector[] values, BooleanVector[] nulls, int position);

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
        public boolean add(Vector[] values, BooleanVector[] nulls, int position)
        {
            if (OperatorVectorSupport.isNull(nulls[0], position)) {
                return false;
            }
            return keys.add(OperatorVectorSupport.longValue(values[0], position));
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
        public boolean add(Vector[] values, BooleanVector[] nulls, int position)
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

        private static boolean hasNull(BooleanVector[] nulls, int position)
        {
            for (BooleanVector nullsVector : nulls) {
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
        public boolean add(Vector[] values, BooleanVector[] nulls, int position)
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
            long hash = 31L * Long.hashCode(first) + Long.hashCode(second);
            hash ^= (hash >>> 16);
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
        public boolean add(Vector[] values, BooleanVector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }

            long first = OperatorVectorSupport.longValue(values[0], position);
            long second = OperatorVectorSupport.longValue(values[1], position);
            long third = OperatorVectorSupport.longValue(values[2], position);
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
            long hash = 31L * (31L * Long.hashCode(first) + Long.hashCode(second)) + Long.hashCode(third);
            hash ^= (hash >>> 16);
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
        public boolean add(Vector[] values, BooleanVector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }

            long first = OperatorVectorSupport.longValue(values[0], position);
            long second = OperatorVectorSupport.longValue(values[1], position);
            long third = OperatorVectorSupport.longValue(values[2], position);
            long fourth = OperatorVectorSupport.longValue(values[3], position);
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
            long hash = 31L * (31L * (31L * Long.hashCode(first) + Long.hashCode(second)) + Long.hashCode(third)) + Long.hashCode(fourth);
            hash ^= (hash >>> 16);
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
        public boolean add(Vector[] values, BooleanVector[] nulls, int position)
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

        private OperatorKeySemantics.Key keyForPosition(Vector[] values, BooleanVector[] nulls, int position)
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

    private static boolean isIntegerVector(Vector vector)
    {
        return switch (OperatorVectorSupport.flatten(vector)) {
            case I32Vector _ -> true;
            case I64Vector _ -> true;
            default -> false;
        };
    }

    private static boolean hasNull(BooleanVector[] nulls, int position)
    {
        for (BooleanVector nullsVector : nulls) {
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
