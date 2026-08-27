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

/** Resolves one output row's physical frame within an ordered partition. */
@FunctionalInterface
public interface WindowFrame
{
    void resolve(WindowPositionIndex partition, int outputPosition, Bounds bounds);

    static WindowFrame fullPartition()
    {
        return (partition, _, bounds) -> bounds.set(0, partition.size());
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
