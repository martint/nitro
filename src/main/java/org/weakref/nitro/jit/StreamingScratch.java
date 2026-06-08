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
package org.weakref.nitro.jit;

/**
 * Helpers for the buffers a generated streaming pipeline reuses across batches. A streamed query processes each
 * batch fully before the source advances to the next, so a transient per-batch buffer (the survivor selection index
 * and matched build-row indices) can be allocated once and reused every batch instead of allocated-and-discarded.
 * Generated code grows such a buffer with {@link #grow} when a batch needs more capacity than the buffer currently
 * holds; the growth is geometric so a stream over many same-sized batches reallocates a bounded number of times.
 */
public final class StreamingScratch
{
    private StreamingScratch() {}

    /**
     * The capacity to grow a reused scratch buffer to so it holds at least {@code required} elements, given its
     * {@code current} length. Doubles the current length (so repeated growth is amortized constant) but never
     * returns less than {@code required}.
     */
    public static int grow(int current, int required)
    {
        int doubled = current + current;
        return Math.max(doubled, required);
    }
}
