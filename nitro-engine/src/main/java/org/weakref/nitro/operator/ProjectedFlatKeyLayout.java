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
import org.weakref.nitro.data.Vector;

/**
 * Flat physical-key layout whose canonical fields are implemented by one exact generated subclass. The base owns
 * wrapper resolution and scratch lifetime; generated hot methods load the bound primitive arrays directly.
 */
abstract class ProjectedFlatKeyLayout
        extends FlatKeyLayout
{
    private final FixedWidthKeyBatchBindings projectedBindings;

    ProjectedFlatKeyLayout(
            Construction construction,
            ResolvedFixedWidthKeyLayout projectedLayout,
            PrimitiveArrayPool arrayPool)
    {
        super(construction);
        projectedBindings = new FixedWidthKeyBatchBindings(projectedLayout, arrayPool);
    }

    @Override
    public void beginBatch(Vector[] values, Vector[] nulls)
    {
        projectedBindings.bind(values, nulls);
        try {
            super.beginBatch(values, nulls);
        }
        catch (RuntimeException | Error e) {
            projectedBindings.release();
            throw e;
        }
    }

    @Override
    public void endBatch()
    {
        try {
            super.endBatch();
        }
        finally {
            projectedBindings.release();
        }
    }

    @Override
    void releaseBuffers()
    {
        projectedBindings.release();
        super.releaseBuffers();
    }

    final Object[] projectedKeyArrays()
    {
        return projectedBindings.keyArrays();
    }

    final int[][] projectedKeyMappings()
    {
        return projectedBindings.keyMappings();
    }

    final int[] projectedKeyMappingOffsets()
    {
        return projectedBindings.keyMappingOffsets();
    }

    final int[] projectedKeyBaseOffsets()
    {
        return projectedBindings.keyBaseOffsets();
    }
}
