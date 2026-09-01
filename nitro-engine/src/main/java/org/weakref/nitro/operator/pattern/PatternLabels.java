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
package org.weakref.nitro.operator.pattern;

import java.util.Arrays;

final class PatternLabels
{
    private PatternLabels() {}

    public static int[] normalized(int[] ordinals)
    {
        int[] normalized = ordinals.clone();
        Arrays.sort(normalized);
        int unique = 0;
        for (int ordinal : normalized) {
            if (ordinal < 0) {
                throw new IllegalArgumentException("label ordinal is negative");
            }
            if (unique == 0 || normalized[unique - 1] != ordinal) {
                normalized[unique++] = ordinal;
            }
        }
        return Arrays.copyOf(normalized, unique);
    }

    public static boolean contains(int[] normalizedOrdinals, int labelOrdinal)
    {
        return normalizedOrdinals.length == 0 || Arrays.binarySearch(normalizedOrdinals, labelOrdinal) >= 0;
    }
}
