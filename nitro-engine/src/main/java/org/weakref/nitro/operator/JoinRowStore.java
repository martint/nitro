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
 * Allocator-owned row references and insertion-ordered duplicate links for a single-long join index.
 */
final class JoinRowStore
{
    private final PrimitiveArrayPool arrayPool;
    private final boolean preferCompactReferences;
    private final int empty;

    private long[] references;
    private int[] compactReferences;
    private int[] next;
    private int capacity;
    private boolean implicitSequentialReferences;
    private long implicitReferenceBase;
    private boolean referencesFit32;

    JoinRowStore(
            PrimitiveArrayPool arrayPool,
            int initialCapacity,
            boolean preferCompactReferences,
            boolean implicitSequentialReferences,
            boolean referencesFit32,
            boolean eagerChainState,
            int empty)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        this.preferCompactReferences = preferCompactReferences;
        this.implicitSequentialReferences = implicitSequentialReferences;
        this.referencesFit32 = referencesFit32;
        this.empty = empty;
        capacity = initialCapacity;
        if (!implicitSequentialReferences) {
            if (preferCompactReferences) {
                compactReferences = arrayPool.borrowInts(initialCapacity);
            }
            else {
                references = arrayPool.borrowLongs(initialCapacity);
            }
        }
        if (eagerChainState) {
            next = arrayPool.borrowInts(initialCapacity);
        }
    }

    boolean implicitSequentialReferences()
    {
        return implicitSequentialReferences;
    }

    boolean referencesFit32()
    {
        return referencesFit32;
    }

    long retainedBytes()
    {
        long bytes = references == null ? 0 : (long) references.length * Long.BYTES;
        bytes += compactReferences == null ? 0 : (long) compactReferences.length * Integer.BYTES;
        bytes += next == null ? 0 : (long) next.length * Integer.BYTES;
        return bytes;
    }

    void observeReference(long reference)
    {
        if (!referencesFit32) {
            return;
        }
        if (JoinRowReference.batchIndex(reference) > JoinRowReference.MAX_COMPACT_BATCH_INDEX ||
                JoinRowReference.position(reference) > JoinRowReference.MAX_COMPACT_POSITION) {
            referencesFit32 = false;
        }
    }

    void observeReferenceRange(int batchIndex, int maximumPosition)
    {
        if (referencesFit32 &&
                (batchIndex > JoinRowReference.MAX_COMPACT_BATCH_INDEX || maximumPosition > JoinRowReference.MAX_COMPACT_POSITION)) {
            referencesFit32 = false;
        }
    }

    void append(int ordinal, long reference)
    {
        ensureCapacity(ordinal);
        storeReference(ordinal, reference);
        if (next != null) {
            next[ordinal] = empty;
        }
    }

    void ensureChainState(int rowCount)
    {
        if (next != null) {
            return;
        }
        next = arrayPool.borrowInts(capacity);
        Arrays.fill(next, 0, rowCount, empty);
    }

    void link(int previousOrdinal, int ordinal)
    {
        next[previousOrdinal] = ordinal;
    }

    int next(int ordinal)
    {
        return next[ordinal];
    }

    long referenceAt(int ordinal)
    {
        if (implicitSequentialReferences) {
            return implicitReferenceBase + ordinal;
        }
        return compactReferences != null ? JoinRowReference.unpackCompact(compactReferences[ordinal]) : references[ordinal];
    }

    ChainLongList resetChain(ChainLongList chain, int head, int count)
    {
        return compactReferences != null
                ? chain.resetCompact(compactReferences, next, head, count)
                : chain.reset(references, next, head, count);
    }

    int[] packReferences32(int rowCount)
    {
        int[] packed = arrayPool.borrowInts(rowCount);
        if (compactReferences != null) {
            System.arraycopy(compactReferences, 0, packed, 0, rowCount);
            return packed;
        }
        if (implicitSequentialReferences) {
            for (int index = 0; index < rowCount; index++) {
                packed[index] = JoinRowReference.packCompact(implicitReferenceBase + index);
            }
            return packed;
        }
        for (int index = 0; index < rowCount; index++) {
            packed[index] = JoinRowReference.packCompact(references[index]);
        }
        return packed;
    }

    long[] takeFullReferences()
    {
        long[] result = references;
        references = null;
        return result;
    }

    void release()
    {
        arrayPool.release(references);
        references = null;
        arrayPool.release(compactReferences);
        compactReferences = null;
        arrayPool.release(next);
        next = null;
    }

    private void ensureCapacity(int ordinal)
    {
        if (ordinal < capacity) {
            return;
        }
        int newCapacity = capacity * 2;
        if (compactReferences != null) {
            int[] previous = compactReferences;
            compactReferences = arrayPool.borrowInts(newCapacity);
            System.arraycopy(previous, 0, compactReferences, 0, ordinal);
            arrayPool.release(previous);
        }
        else if (!implicitSequentialReferences) {
            long[] previous = references;
            references = arrayPool.borrowLongs(newCapacity);
            System.arraycopy(previous, 0, references, 0, ordinal);
            arrayPool.release(previous);
        }
        if (next != null) {
            int[] previous = next;
            next = arrayPool.borrowInts(newCapacity);
            System.arraycopy(previous, 0, next, 0, ordinal);
            arrayPool.release(previous);
        }
        capacity = newCapacity;
    }

    private void storeReference(int ordinal, long reference)
    {
        if (implicitSequentialReferences) {
            if (ordinal == 0) {
                implicitReferenceBase = reference;
                return;
            }
            if (reference == implicitReferenceBase + ordinal) {
                return;
            }
            materializeImplicitReferences(ordinal);
        }
        if (compactReferences != null && referencesFit32) {
            compactReferences[ordinal] = JoinRowReference.packCompact(reference);
            return;
        }
        if (compactReferences != null) {
            references = arrayPool.borrowLongs(compactReferences.length);
            for (int index = 0; index < ordinal; index++) {
                references[index] = JoinRowReference.unpackCompact(compactReferences[index]);
            }
            arrayPool.release(compactReferences);
            compactReferences = null;
        }
        references[ordinal] = reference;
    }

    private void materializeImplicitReferences(int count)
    {
        implicitSequentialReferences = false;
        if (preferCompactReferences && referencesFit32) {
            compactReferences = arrayPool.borrowInts(capacity);
            for (int index = 0; index < count; index++) {
                compactReferences[index] = JoinRowReference.packCompact(implicitReferenceBase + index);
            }
            return;
        }
        references = arrayPool.borrowLongs(capacity);
        for (int index = 0; index < count; index++) {
            references[index] = implicitReferenceBase + index;
        }
    }
}
