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
package org.weakref.nitro.data;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public final class RleVector
        implements Vector
{
    private final int length;
    private final int[] counts;
    private final Vector values;

    public RleVector(int[] counts, Vector values)
    {
        checkArgument(counts.length == values.length(), "Run lengths counts (%s) must match the length of the underlying values vector (%s)", counts.length, values.length());

        this.values = values;
        this.counts = counts;
        length = Arrays.stream(counts).sum();
    }

    public static int computeTargetRleLength(RleVector left, RleVector right)
    {
        int result = 0;

        int leftIndex = 0;
        int rightIndex = 0;

        int leftCount = 0;
        int rightCount = 0;

        while (leftIndex < left.counts().length && rightIndex < right.counts().length) {
            result++;

            if (leftCount == 0) {
                leftCount = left.counts()[leftIndex];
            }
            if (rightCount == 0) {
                rightCount = right.counts()[rightIndex];
            }

            int count = Math.min(leftCount, rightCount);
            leftCount -= count;
            rightCount -= count;

            if (leftCount == 0) {
                leftIndex++;
            }
            if (rightCount == 0) {
                rightIndex++;
            }
        }

        return result;
    }

    public int[] counts()
    {
        return counts;
    }

    public Vector values()
    {
        return values;
    }

    @Override
    public int length()
    {
        return length;
    }

    @Override
    public String toString()
    {
        return "RLE {length: " + length + ", counts: " + Arrays.toString(counts) + ", values: " + values + "}";
    }
}
