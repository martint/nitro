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

import java.util.Arrays;

/** Sorts primitive references to exact encoded repeated-entry records without allocating entry objects. */
final class RepeatedKeyCanonicalizer
{
    private RepeatedKeyCanonicalizer() {}

    static void sort(byte[] data, int[] offsets, int[] order, int count)
    {
        for (int root = count / 2 - 1; root >= 0; root--) {
            siftDown(data, offsets, order, root, count);
        }
        for (int end = count - 1; end > 0; end--) {
            int value = order[0];
            order[0] = order[end];
            order[end] = value;
            siftDown(data, offsets, order, 0, end);
        }
    }

    private static void siftDown(byte[] data, int[] offsets, int[] order, int root, int size)
    {
        while (true) {
            int child = root * 2 + 1;
            if (child >= size) {
                return;
            }
            if (child + 1 < size && compare(data, offsets, order[child], order[child + 1]) < 0) {
                child++;
            }
            if (compare(data, offsets, order[root], order[child]) >= 0) {
                return;
            }
            int value = order[root];
            order[root] = order[child];
            order[child] = value;
            root = child;
        }
    }

    private static int compare(byte[] data, int[] offsets, int left, int right)
    {
        return Arrays.compareUnsigned(
                data,
                offsets[left],
                offsets[left + 1],
                data,
                offsets[right],
                offsets[right + 1]);
    }
}
