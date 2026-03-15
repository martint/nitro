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
package org.weakref.nitro.function.scalar.builtin;

import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import static com.google.common.base.Preconditions.checkArgument;

final class BinaryDispatchSupport
{
    private BinaryDispatchSupport() {}

    public static int requiredLength(Mask mask, int defaultLength)
    {
        if (mask.none()) {
            return defaultLength;
        }
        return Math.max(defaultLength, mask.maxPosition() + 1);
    }

    public static void validateLength(Vector vector, Mask mask)
    {
        if (mask.none()) {
            return;
        }

        int requiredLength = mask.maxPosition() + 1;
        checkArgument(vector.length() >= requiredLength, "Vector length %s is shorter than required length %s", vector.length(), requiredLength);
    }

    public static void mergeRuns(int[] leftCounts, int[] rightCounts, RunConsumer consumer)
    {
        int outputIndex = 0;
        int leftIndex = 0;
        int rightIndex = 0;
        int leftCount = 0;
        int rightCount = 0;

        while (leftIndex < leftCounts.length && rightIndex < rightCounts.length) {
            if (leftCount == 0) {
                leftCount = leftCounts[leftIndex];
            }
            if (rightCount == 0) {
                rightCount = rightCounts[rightIndex];
            }

            int count = Math.min(leftCount, rightCount);
            consumer.accept(outputIndex, leftIndex, rightIndex, count);
            outputIndex++;

            leftCount -= count;
            rightCount -= count;

            if (leftCount == 0) {
                leftIndex++;
            }
            if (rightCount == 0) {
                rightIndex++;
            }
        }
    }

    @FunctionalInterface
    public interface RunConsumer
    {
        void accept(int outputIndex, int leftIndex, int rightIndex, int count);
    }

    public static final class RlePositionCursor
    {
        private final int[] counts;
        private int runIndex;
        private int runEnd;

        public RlePositionCursor(int[] counts)
        {
            this.counts = counts;
            if (counts.length > 0) {
                runEnd = counts[0];
            }
        }

        public int runIndexAt(int position)
        {
            while (position >= runEnd) {
                runIndex++;
                runEnd += counts[runIndex];
            }
            return runIndex;
        }
    }

    public static int[] dictionaryIds(DictionaryVector vector)
    {
        return vector.ids();
    }
}
