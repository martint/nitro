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
package org.weakref.nitro.core.function.aggregation;

/** Physical window bounds in ordered partition position space. */
public interface AggregationWindowFrameBounds
{
    int partitionSize();

    int start(int position);

    int end(int position);

    record Resolved(int partitionSize, int rangeStart, int[] starts, int[] ends)
            implements AggregationWindowFrameBounds
    {
        public Resolved
        {
            if (partitionSize < 0 || rangeStart < 0 || starts.length != ends.length || rangeStart + starts.length > partitionSize) {
                throw new IllegalArgumentException("Invalid resolved window bounds");
            }
        }

        @Override
        public int start(int position)
        {
            return starts[position - rangeStart];
        }

        @Override
        public int end(int position)
        {
            return ends[position - rangeStart];
        }
    }

    /** Exact {@code [position - preceding, position + following]} ROWS bounds. */
    record AffineRows(int partitionSize, long preceding, long following)
            implements AggregationWindowFrameBounds
    {
        public AffineRows
        {
            if (partitionSize < 0 || preceding < 0 || following < 0) {
                throw new IllegalArgumentException("Invalid affine ROWS bounds");
            }
        }

        @Override
        public int start(int position)
        {
            return preceding >= position ? 0 : (int) (position - preceding);
        }

        @Override
        public int end(int position)
        {
            long end = following > Long.MAX_VALUE - position ? Long.MAX_VALUE : position + following;
            return end >= partitionSize - 1L ? partitionSize : (int) end + 1;
        }
    }
}
