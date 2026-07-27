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

import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;

/** Reusable position-mapping frame for a sequence of synchronous consumers of the same mask slice. */
final class PositionBuffer
{
    private final PrimitiveArrayPool arrayPool;
    private int[] positions = new int[0];
    private Mask mask;
    private int maskStart;
    private int count;

    PositionBuffer(PrimitiveArrayPool arrayPool)
    {
        this.arrayPool = arrayPool;
    }

    /** Starts a new mapping generation. The first consumer fills the frame; later consumers reuse it. */
    void reset()
    {
        mask = null;
    }

    int[] positions(Mask mask, int maskStart, int count)
    {
        if (this.mask != null) {
            if (this.mask != mask || this.maskStart != maskStart || this.count != count) {
                throw new IllegalStateException("Position frame already contains a different mask slice");
            }
            return positions;
        }
        if (positions.length < count) {
            int[] previous = positions;
            positions = arrayPool.borrowInts(count);
            arrayPool.release(previous);
        }
        if (mask.all()) {
            for (int index = 0; index < count; index++) {
                positions[index] = maskStart + index;
            }
        }
        else {
            for (int index = 0; index < count; index++) {
                positions[index] = mask.position(maskStart + index);
            }
        }
        this.mask = mask;
        this.maskStart = maskStart;
        this.count = count;
        return positions;
    }

    void release()
    {
        arrayPool.release(positions);
        positions = new int[0];
        mask = null;
    }
}
