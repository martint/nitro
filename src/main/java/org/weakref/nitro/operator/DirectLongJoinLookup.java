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

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Finalized direct lookup representation for a unique single-long join build.
 */
final class DirectLongJoinLookup
{
    private static final int NO_MATCH_COMPACT_REFERENCE = -1;

    private final PrimitiveArrayPool arrayPool;
    private final long noMatchReference;

    private boolean active;
    private long minKey;
    private long maxKey;
    private long[] references;
    private int[] compactReferences;

    DirectLongJoinLookup(PrimitiveArrayPool arrayPool, long noMatchReference)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        this.noMatchReference = noMatchReference;
    }

    boolean isActive()
    {
        return active;
    }

    void activateArithmetic(long minKey, long maxKey)
    {
        activate(minKey, maxKey);
    }

    void activateCompact(long minKey, long maxKey, int[] references)
    {
        activate(minKey, maxKey);
        compactReferences = requireNonNull(references, "references is null");
    }

    int[] activateCompact(long minKey, long maxKey, int size)
    {
        activate(minKey, maxKey);
        compactReferences = arrayPool.borrowInts(size);
        Arrays.fill(compactReferences, NO_MATCH_COMPACT_REFERENCE);
        return compactReferences;
    }

    void activateFull(long minKey, long maxKey, long[] references)
    {
        activate(minKey, maxKey);
        this.references = requireNonNull(references, "references is null");
    }

    long[] activateFull(long minKey, long maxKey, int size)
    {
        activate(minKey, maxKey);
        references = arrayPool.borrowLongs(size);
        Arrays.fill(references, noMatchReference);
        return references;
    }

    long reference(long key, DenseJoinSequence denseSequence)
    {
        if (!active || key < minKey || key > maxKey) {
            return noMatchReference;
        }
        int ordinal = (int) (key - minKey);
        if (denseSequence.referencesActive()) {
            return denseSequence.referenceAt(ordinal);
        }
        if (compactReferences != null) {
            int reference = compactReferences[ordinal];
            return reference == NO_MATCH_COMPACT_REFERENCE ? noMatchReference : JoinRowReference.unpackCompact(reference);
        }
        return references[ordinal];
    }

    void release()
    {
        arrayPool.release(references);
        references = null;
        arrayPool.release(compactReferences);
        compactReferences = null;
        active = false;
    }

    private void activate(long minKey, long maxKey)
    {
        active = true;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }
}
