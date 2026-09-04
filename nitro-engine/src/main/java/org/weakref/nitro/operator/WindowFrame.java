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

import org.weakref.nitro.core.function.aggregation.AggregationWindowFrameBounds;

/** Resolves one output row's physical frame within an ordered partition. */
@FunctionalInterface
public interface WindowFrame
{
    void resolve(WindowPositionIndex partition, int outputPosition, Bounds bounds);

    default Cursor bind(WindowPositionIndex partition)
    {
        return new Cursor()
        {
            @Override
            public int partitionSize()
            {
                return partition.size();
            }

            @Override
            public void resolve(int rangeStart, int positionCount, int[] starts, int[] ends, int offset)
            {
                Bounds bounds = new Bounds();
                for (int index = 0; index < positionCount; index++) {
                    bounds.clear();
                    WindowFrame.this.resolve(partition, rangeStart + index, bounds);
                    if (bounds.present()) {
                        starts[offset + index] = bounds.start();
                        ends[offset + index] = bounds.end();
                    }
                    else {
                        starts[offset + index] = -1;
                        ends[offset + index] = -1;
                    }
                }
            }
        };
    }

    /** Identity of a traversal whose resolved bounds may be shared by sibling functions. */
    default Object traversalIdentity()
    {
        return this;
    }

    static WindowFrame fullPartition()
    {
        return (partition, _, bounds) -> bounds.set(0, partition.size());
    }

    interface Cursor
    {
        int partitionSize();

        void resolve(int rangeStart, int positionCount, int[] starts, int[] ends, int offset);

        default AggregationWindowFrameBounds bind(int rangeStart, int positionCount)
        {
            int[] starts = new int[positionCount];
            int[] ends = new int[positionCount];
            resolve(rangeStart, positionCount, starts, ends, 0);
            return new AggregationWindowFrameBounds.Resolved(partitionSize(), rangeStart, starts, ends);
        }
    }

    final class Bounds
    {
        private int start;
        private int end;
        private boolean present;

        public void set(int start, int end)
        {
            if (start < 0 || start > end) {
                throw new IndexOutOfBoundsException("Invalid window frame [" + start + ", " + end + ")");
            }
            this.start = start;
            this.end = end;
            present = true;
        }

        public void clear()
        {
            present = false;
        }

        void setFrom(Bounds source)
        {
            if (source.present) {
                set(source.start, source.end);
            }
            else {
                clear();
            }
        }

        public boolean present()
        {
            return present;
        }

        public int start()
        {
            if (!present) {
                throw new IllegalStateException("Window frame is empty");
            }
            return start;
        }

        public int end()
        {
            if (!present) {
                throw new IllegalStateException("Window frame is empty");
            }
            return end;
        }
    }
}
