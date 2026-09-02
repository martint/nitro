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

import org.weakref.nitro.data.PrimitiveArrayPool;

import static java.util.Objects.requireNonNull;

final class NarrowRangeSort
{
    private NarrowRangeSort() {}

    static boolean trySort(
            PrimitiveArrayPool arrayPool,
            int maximumRange,
            int[] values,
            int[] scratch,
            int count,
            TopNState state)
    {
        requireNonNull(arrayPool, "arrayPool is null");
        requireNonNull(values, "values is null");
        requireNonNull(scratch, "scratch is null");
        requireNonNull(state, "state is null");
        if (count < 2 || maximumRange == 0) {
            return count < 2;
        }

        long minimum = state.singleFixedWidthSortKey(values[0]);
        long maximum = minimum;
        for (int index = 1; index < count; index++) {
            long key = state.singleFixedWidthSortKey(values[index]);
            if (Long.compareUnsigned(key, minimum) < 0) {
                minimum = key;
            }
            if (Long.compareUnsigned(key, maximum) > 0) {
                maximum = key;
            }
        }
        long width = maximum - minimum;
        if (Long.compareUnsigned(width, maximumRange - 1L) > 0) {
            return false;
        }

        int range = (int) width + 1;
        int[] offsets = arrayPool.borrowInts(range);
        try {
            java.util.Arrays.fill(offsets, 0, range, 0);
            for (int index = 0; index < count; index++) {
                offsets[(int) (state.singleFixedWidthSortKey(values[index]) - minimum)]++;
            }
            int offset = 0;
            if (state.singleOrderingDescending()) {
                for (int key = range - 1; key >= 0; key--) {
                    int size = offsets[key];
                    offsets[key] = offset;
                    offset += size;
                }
            }
            else {
                for (int key = 0; key < range; key++) {
                    int size = offsets[key];
                    offsets[key] = offset;
                    offset += size;
                }
            }
            for (int index = 0; index < count; index++) {
                int slot = values[index];
                int key = (int) (state.singleFixedWidthSortKey(slot) - minimum);
                scratch[offsets[key]++] = slot;
            }
            System.arraycopy(scratch, 0, values, 0, count);
            return true;
        }
        finally {
            arrayPool.release(offsets);
        }
    }
}
