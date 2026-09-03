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

import org.weakref.nitro.core.function.aggregation.PrimitiveContributionCarrier;

import java.util.Arrays;

/**
 * Reusable physical bindings for the generated single-long grouping convention.
 *
 * <p>This data-layer adapter owns representation inspection. The grouping operator supplies logical value and null
 * streams, while the generated backend receives primitive bases, optional dictionary mappings, and a stable physical
 * shape. Adding another physical representation changes this adapter/backend boundary rather than the operator.
 */
public final class GeneratedLongGroupingBindings
{
    private final boolean dictionaryInput;
    private final Object[] inputs;
    private final int[][] inputIds;
    private final int[] inputOffsets;
    private final boolean[][] inputNulls;
    private final int[][] inputNullIds;
    private final int[] inputNullOffsets;
    private final boolean[] readsInput;
    private final boolean[] readsValue;
    private final boolean[] intInputs;
    private final PrimitiveContributionCarrier[] inputCarriers;
    private final boolean[] mappedInputs;
    private final boolean[] mappedInputNulls;
    private final boolean[] inputUsesKeyIds;
    private final boolean[] inputNullUsesKeyIds;
    private final boolean[] offsetInputs;
    private final boolean[] offsetInputNulls;

    private Object keyValues;
    private int[] keyIds;
    private int keyOffset;
    private int keyPhysicalLength;
    private boolean intKey;
    private boolean keyMapped;

    public GeneratedLongGroupingBindings(int inputCount, boolean dictionaryInput)
    {
        if (inputCount < 0) {
            throw new IllegalArgumentException("inputCount is negative");
        }
        this.dictionaryInput = dictionaryInput;
        inputs = new Object[inputCount];
        inputIds = new int[inputCount][];
        inputOffsets = new int[inputCount];
        inputNulls = new boolean[inputCount][];
        inputNullIds = new int[inputCount][];
        inputNullOffsets = new int[inputCount];
        readsInput = new boolean[inputCount];
        readsValue = new boolean[inputCount];
        intInputs = new boolean[inputCount];
        inputCarriers = new PrimitiveContributionCarrier[inputCount];
        mappedInputs = new boolean[inputCount];
        mappedInputNulls = new boolean[inputCount];
        inputUsesKeyIds = new boolean[inputCount];
        inputNullUsesKeyIds = new boolean[inputCount];
        offsetInputs = new boolean[inputCount];
        offsetInputNulls = new boolean[inputCount];
    }

    public boolean bindKey(Vector values, Vector nulls)
    {
        keyValues = null;
        keyIds = null;
        keyOffset = 0;
        keyPhysicalLength = 0;
        intKey = false;
        keyMapped = false;

        if (!VectorAccess.isAllFalseNulls(nulls)) {
            return false;
        }
        if (values instanceof DictionaryVector dictionary && dictionary.dictionaryDepth() == 1) {
            if (!dictionaryInput) {
                return false;
            }
            keyIds = dictionary.ids();
            keyMapped = true;
            values = dictionary.values();
        }
        if (values instanceof RegionVector region) {
            keyOffset = region.offset();
            keyPhysicalLength = region.length();
            values = region.values();
        }
        if (values instanceof I64Vector longs) {
            keyValues = longs.values();
            if (keyPhysicalLength == 0) {
                keyPhysicalLength = longs.length();
            }
            return true;
        }
        if (values instanceof I32Vector ints) {
            keyValues = ints.values();
            if (keyPhysicalLength == 0) {
                keyPhysicalLength = ints.length();
            }
            intKey = true;
            return true;
        }
        return false;
    }

    public boolean bindInput(
            int index,
            Vector values,
            Vector nulls,
            boolean valueRequired,
            PrimitiveContributionCarrier carrier)
    {
        clearInput(index);
        readsInput[index] = true;
        readsValue[index] = valueRequired;
        inputCarriers[index] = carrier;

        if (valueRequired) {
            if (values instanceof DictionaryVector dictionary && dictionary.dictionaryDepth() == 1) {
                if (!dictionaryInput) {
                    return false;
                }
                inputIds[index] = dictionary.ids();
                mappedInputs[index] = true;
                values = dictionary.values();
            }
            if (values instanceof RegionVector region) {
                inputOffsets[index] = region.offset();
                offsetInputs[index] = region.offset() != 0;
                values = region.values();
            }
            if (carrier == PrimitiveContributionCarrier.DOUBLE && values instanceof F64Vector doubles) {
                inputs[index] = doubles.values();
            }
            else if (carrier == PrimitiveContributionCarrier.BOOLEAN && values instanceof BooleanVector booleans) {
                inputs[index] = booleans.values();
            }
            else if (carrier == PrimitiveContributionCarrier.LONG && values instanceof I64Vector longs) {
                inputs[index] = longs.values();
            }
            else if (carrier == PrimitiveContributionCarrier.LONG && values instanceof I32Vector ints) {
                inputs[index] = ints.values();
                intInputs[index] = true;
            }
            else {
                return false;
            }
        }

        if (VectorAccess.isAllFalseNulls(nulls)) {
            return true;
        }
        if (nulls instanceof DictionaryVector dictionary && dictionary.dictionaryDepth() == 1) {
            if (!dictionaryInput) {
                return false;
            }
            inputNullIds[index] = dictionary.ids();
            mappedInputNulls[index] = true;
            nulls = dictionary.values();
        }
        if (nulls instanceof RegionVector region) {
            inputNullOffsets[index] = region.offset();
            offsetInputNulls[index] = region.offset() != 0;
            nulls = region.values();
        }
        if (nulls instanceof BooleanVector booleans) {
            inputNulls[index] = booleans.values();
            return true;
        }
        return false;
    }

    public void clearInput(int index)
    {
        inputs[index] = null;
        inputIds[index] = null;
        inputOffsets[index] = 0;
        inputNulls[index] = null;
        inputNullIds[index] = null;
        inputNullOffsets[index] = 0;
        readsInput[index] = false;
        readsValue[index] = false;
        intInputs[index] = false;
        inputCarriers[index] = null;
        mappedInputs[index] = false;
        mappedInputNulls[index] = false;
        inputUsesKeyIds[index] = false;
        inputNullUsesKeyIds[index] = false;
        offsetInputs[index] = false;
        offsetInputNulls[index] = false;
    }

    public void finish()
    {
        for (int index = 0; index < inputs.length; index++) {
            inputUsesKeyIds[index] = keyMapped && mappedInputs[index] && inputIds[index] == keyIds;
            inputNullUsesKeyIds[index] = keyMapped && mappedInputNulls[index] && inputNullIds[index] == keyIds;
        }
    }

    public int physicalShape()
    {
        int shape = intKey ? 1 : 0;
        shape = shape * 31 + (keyMapped ? 1 : 0);
        shape = shape * 31 + (keyOffset != 0 ? 1 : 0);
        for (int index = 0; index < inputs.length; index++) {
            if (readsValue[index]) {
                shape = shape * 31 + (intInputs[index] ? 1 : 0);
                shape = shape * 31 + inputCarriers[index].ordinal();
                shape = shape * 31 + (mappedInputs[index] ? 1 : 0);
                shape = shape * 31 + (inputUsesKeyIds[index] ? 1 : 0);
                shape = shape * 31 + (offsetInputs[index] ? 1 : 0);
            }
            if (readsInput[index]) {
                shape = shape * 31 + (mappedInputNulls[index] ? 1 : 0);
                shape = shape * 31 + (inputNullUsesKeyIds[index] ? 1 : 0);
                shape = shape * 31 + (offsetInputNulls[index] ? 1 : 0);
            }
        }
        return shape;
    }

    public long sampleKeyRuns(Mask mask)
    {
        int comparisons = 0;
        int hits = 0;
        boolean havePrevious = false;
        long previous = 0;
        for (int position : mask) {
            int keyPosition = (keyIds == null ? position : keyIds[position]) + keyOffset;
            long key = intKey ? ((int[]) keyValues)[keyPosition] : ((long[]) keyValues)[keyPosition];
            if (havePrevious) {
                comparisons++;
                if (key == previous) {
                    hits++;
                }
                if (comparisons == 64) {
                    break;
                }
            }
            havePrevious = true;
            previous = key;
        }
        return ((long) comparisons << 32) | (hits & 0xFFFF_FFFFL);
    }

    public int additionalGroupUpperBound(int selectedRows)
    {
        return keyMapped ? Math.min(selectedRows, keyPhysicalLength) : selectedRows;
    }

    public int keyDomainSize()
    {
        return keyMapped ? keyPhysicalLength : 0;
    }

    /**
     * Counts selected logical rows by physical key-domain position. Returns zero when the key is not mapped.
     * Representation inspection stays in this data-layer adapter; execution operators see only a compact domain.
     */
    public int countKeyDomain(Mask mask, int[] counts)
    {
        if (!keyMapped) {
            return 0;
        }
        if (counts.length < keyPhysicalLength) {
            throw new IllegalArgumentException("counts is smaller than key domain");
        }
        Arrays.fill(counts, 0, keyPhysicalLength, 0);
        for (int position : mask) {
            counts[keyIds[position]]++;
        }
        return keyPhysicalLength;
    }

    public long keyDomainValue(int position)
    {
        if (!keyMapped || position < 0 || position >= keyPhysicalLength) {
            throw new IllegalArgumentException("invalid key domain position");
        }
        return intKey ? ((int[]) keyValues)[keyOffset + position] : ((long[]) keyValues)[keyOffset + position];
    }

    public Object keyValues()
    {
        return keyValues;
    }

    public int[] keyIds()
    {
        return keyIds;
    }

    public int keyOffset()
    {
        return keyOffset;
    }

    public boolean intKey()
    {
        return intKey;
    }

    public boolean keyMapped()
    {
        return keyMapped;
    }

    public Object[] inputs()
    {
        return inputs;
    }

    public int[][] inputIds()
    {
        return inputIds;
    }

    public int[] inputOffsets()
    {
        return inputOffsets;
    }

    public boolean[][] inputNulls()
    {
        return inputNulls;
    }

    public int[][] inputNullIds()
    {
        return inputNullIds;
    }

    public int[] inputNullOffsets()
    {
        return inputNullOffsets;
    }

    public boolean keyOffsetInput()
    {
        return keyOffset != 0;
    }

    public boolean[] offsetInputs()
    {
        return offsetInputs;
    }

    public boolean[] offsetInputNulls()
    {
        return offsetInputNulls;
    }

    public boolean[] intInputs()
    {
        return intInputs;
    }

    public PrimitiveContributionCarrier[] inputCarriers()
    {
        return inputCarriers;
    }

    public boolean[] mappedInputs()
    {
        return mappedInputs;
    }

    public boolean[] mappedInputNulls()
    {
        return mappedInputNulls;
    }

    public boolean[] inputUsesKeyIds()
    {
        return inputUsesKeyIds;
    }

    public boolean[] inputNullUsesKeyIds()
    {
        return inputNullUsesKeyIds;
    }
}
