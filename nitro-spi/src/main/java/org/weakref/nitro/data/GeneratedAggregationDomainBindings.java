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

import org.weakref.nitro.core.function.aggregation.ContributionCarrier;

/**
 * Reusable physical bindings for generated updates over one shared dictionary domain.
 * The reference dictionary supplies mapping identity only; its logical value type is irrelevant.
 */
public final class GeneratedAggregationDomainBindings
{
    private final Object[] inputs;
    private final int[][] inputValueOffsets;
    private final int[] inputOffsets;
    private final boolean[][] inputNulls;
    private final int[] inputNullOffsets;
    private final boolean[] readsInput;
    private final boolean[] readsValue;
    private final boolean[] intInputs;
    private final boolean[] allNullInputs;
    private final ContributionCarrier[] inputCarriers;
    private final boolean[] offsetInputs;
    private final boolean[] offsetInputNulls;

    public GeneratedAggregationDomainBindings(int inputCount)
    {
        if (inputCount < 0) {
            throw new IllegalArgumentException("inputCount is negative");
        }
        inputs = new Object[inputCount];
        inputValueOffsets = new int[inputCount][];
        inputOffsets = new int[inputCount];
        inputNulls = new boolean[inputCount][];
        inputNullOffsets = new int[inputCount];
        readsInput = new boolean[inputCount];
        readsValue = new boolean[inputCount];
        intInputs = new boolean[inputCount];
        allNullInputs = new boolean[inputCount];
        inputCarriers = new ContributionCarrier[inputCount];
        offsetInputs = new boolean[inputCount];
        offsetInputNulls = new boolean[inputCount];
    }

    public boolean bindInput(
            int index,
            DictionaryVector rowMapping,
            Vector values,
            Vector nulls,
            boolean valueRequired,
            ContributionCarrier carrier)
    {
        clearInput(index);
        readsInput[index] = true;
        readsValue[index] = valueRequired;
        inputCarriers[index] = carrier;

        if (valueRequired) {
            if (!(values instanceof DictionaryVector dictionary) ||
                    !rowMapping.hasSameRowMapping(dictionary) ||
                    dictionary.values().length() != rowMapping.values().length()) {
                return false;
            }
            values = dictionary.values();
            if (values instanceof RegionVector region) {
                inputOffsets[index] = region.offset();
                offsetInputs[index] = region.offset() != 0;
                values = region.values();
            }
            if (carrier == ContributionCarrier.DOUBLE && values instanceof F64Vector doubles) {
                inputs[index] = doubles.values();
            }
            else if (carrier == ContributionCarrier.BOOLEAN && values instanceof BooleanVector booleans) {
                inputs[index] = booleans.values();
            }
            else if (carrier == ContributionCarrier.LONG && values instanceof I64Vector longs) {
                inputs[index] = longs.values();
            }
            else if (carrier == ContributionCarrier.LONG && values instanceof I32Vector ints) {
                inputs[index] = ints.values();
                intInputs[index] = true;
            }
            else if (carrier == ContributionCarrier.BINARY_REGION && values instanceof BinaryVector binary) {
                inputs[index] = binary.data();
                inputValueOffsets[index] = binary.offsets();
            }
            else {
                return false;
            }
        }

        if (VectorAccess.isAllFalseNulls(nulls)) {
            return true;
        }
        if (VectorAccess.isAllTrueNulls(nulls)) {
            allNullInputs[index] = true;
            return true;
        }
        if (!(nulls instanceof DictionaryVector dictionary) ||
                !rowMapping.hasSameRowMapping(dictionary) ||
                dictionary.values().length() != rowMapping.values().length()) {
            return false;
        }
        nulls = dictionary.values();
        if (nulls instanceof RegionVector region) {
            inputNullOffsets[index] = region.offset();
            offsetInputNulls[index] = region.offset() != 0;
            nulls = region.values();
        }
        if (!(nulls instanceof BooleanVector booleans)) {
            return false;
        }
        inputNulls[index] = booleans.values();
        return true;
    }

    public void clearInput(int index)
    {
        inputs[index] = null;
        inputValueOffsets[index] = null;
        inputOffsets[index] = 0;
        inputNulls[index] = null;
        inputNullOffsets[index] = 0;
        readsInput[index] = false;
        readsValue[index] = false;
        intInputs[index] = false;
        allNullInputs[index] = false;
        inputCarriers[index] = null;
        offsetInputs[index] = false;
        offsetInputNulls[index] = false;
    }

    public int physicalShape()
    {
        int shape = 1;
        for (int index = 0; index < inputs.length; index++) {
            if (readsValue[index]) {
                shape = shape * 31 + inputCarriers[index].ordinal();
                shape = shape * 31 + (intInputs[index] ? 1 : 0);
                shape = shape * 31 + (offsetInputs[index] ? 1 : 0);
            }
            if (readsInput[index]) {
                shape = shape * 31 + (inputNulls[index] == null ? 0 : 1);
                shape = shape * 31 + (allNullInputs[index] ? 1 : 0);
                shape = shape * 31 + (offsetInputNulls[index] ? 1 : 0);
            }
        }
        return shape;
    }

    public Object[] inputs()
    {
        return inputs;
    }

    public int[][] inputValueOffsets()
    {
        return inputValueOffsets;
    }

    public int[] inputOffsets()
    {
        return inputOffsets;
    }

    public boolean[][] inputNulls()
    {
        return inputNulls;
    }

    public int[] inputNullOffsets()
    {
        return inputNullOffsets;
    }

    public boolean[] intInputs()
    {
        return intInputs;
    }

    public ContributionCarrier[] inputCarriers()
    {
        return inputCarriers;
    }

    public boolean[] allNullInputs()
    {
        return allNullInputs;
    }

    public boolean[] offsetInputs()
    {
        return offsetInputs;
    }

    public boolean[] offsetInputNulls()
    {
        return offsetInputNulls;
    }
}
