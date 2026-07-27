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

import static java.util.Objects.requireNonNull;

/**
 * Observed consecutive-key and single-batch row-reference state for a single-long join build.
 */
final class DenseJoinSequence
{
    private static final int NO_MATCH = -1;

    private final PrimitiveArrayPool arrayPool;
    private boolean keyCandidate;
    private boolean referenceCandidate;

    private long firstKey;
    private long nextKey;
    private boolean referencesActive;
    private int referenceBatchIndex;
    private int firstReferencePosition;
    private long referenceBase;
    private int[] dictionaryPositionScratch;

    DenseJoinSequence(PrimitiveArrayPool arrayPool, boolean keyCandidate, boolean referenceCandidate)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        this.keyCandidate = keyCandidate;
        this.referenceCandidate = referenceCandidate;
    }

    boolean keyCandidate()
    {
        return keyCandidate;
    }

    boolean referenceCandidate()
    {
        return referenceCandidate;
    }

    boolean acceptsKey(long key, int rowCount)
    {
        return keyCandidate && (rowCount == 0 || key == nextKey);
    }

    void observeDenseRow(long key, long reference, int ordinal)
    {
        if (ordinal == 0) {
            firstKey = key;
        }
        nextKey = key + 1;
        observeReference(reference, ordinal);
    }

    void disableKeyCandidate()
    {
        keyCandidate = false;
    }

    void reject()
    {
        keyCandidate = false;
        referenceCandidate = false;
    }

    long firstKey()
    {
        return firstKey;
    }

    boolean referencesActive()
    {
        return referencesActive;
    }

    void activateObservedReferences()
    {
        if (!referenceCandidate) {
            throw new IllegalStateException("Dense row references were not observed");
        }
        referencesActive = true;
    }

    void activateReferences(int batchIndex, int firstPosition, long base)
    {
        referencesActive = true;
        referenceBatchIndex = batchIndex;
        firstReferencePosition = firstPosition;
        referenceBase = base;
    }

    int referenceBatchIndex()
    {
        return referenceBatchIndex;
    }

    int firstReferencePosition()
    {
        return firstReferencePosition;
    }

    long referenceBase()
    {
        return referenceBase;
    }

    long referenceAt(int ordinal)
    {
        return referenceBase + ordinal;
    }

    int[] dictionaryPositions(long[] dictionaryValues, long minimumKey, long maximumKey)
    {
        int[] positions = ensureDictionaryPositionScratch(dictionaryValues.length);
        for (int id = 0; id < dictionaryValues.length; id++) {
            positions[id] = rowPosition(dictionaryValues[id], minimumKey, maximumKey);
        }
        return positions;
    }

    int[] dictionaryPositions(int[] dictionaryValues, long minimumKey, long maximumKey)
    {
        int[] positions = ensureDictionaryPositionScratch(dictionaryValues.length);
        for (int id = 0; id < dictionaryValues.length; id++) {
            positions[id] = rowPosition(dictionaryValues[id], minimumKey, maximumKey);
        }
        return positions;
    }

    void release()
    {
        arrayPool.release(dictionaryPositionScratch);
        dictionaryPositionScratch = null;
    }

    private int[] ensureDictionaryPositionScratch(int dictionarySize)
    {
        if (dictionaryPositionScratch == null || dictionaryPositionScratch.length < dictionarySize) {
            arrayPool.release(dictionaryPositionScratch);
            dictionaryPositionScratch = arrayPool.borrowInts(dictionarySize);
        }
        return dictionaryPositionScratch;
    }

    private int rowPosition(long key, long minimumKey, long maximumKey)
    {
        if (key < minimumKey || key > maximumKey) {
            return NO_MATCH;
        }
        return (int) (firstReferencePosition + (key - minimumKey));
    }

    private void observeReference(long reference, int ordinal)
    {
        if (!referenceCandidate) {
            return;
        }
        int batchIndex = JoinRowReference.batchIndex(reference);
        int rowPosition = JoinRowReference.position(reference);
        if (ordinal == 0) {
            referenceBatchIndex = batchIndex;
            firstReferencePosition = rowPosition;
            referenceBase = reference;
            return;
        }
        if (batchIndex != referenceBatchIndex ||
                rowPosition != (long) firstReferencePosition + ordinal) {
            referenceCandidate = false;
        }
    }
}
