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

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Reusable physical bindings for generated grouped updates after group ids have been resolved.
 *
 * <p>Every contribution is lowered to one primitive base array, an optional logical-to-physical mapping, and two
 * offsets. Region, dictionary, and RLE wrappers may occur in any order. Deeper mapping compositions use
 * allocator-owned pooled scratch rather than adding representation dispatch to the generated row loop.
 */
public final class GeneratedAggregationRowBindings
        implements AutoCloseable
{
    private record BoundVector(
            Object values,
            int[] valueOffsets,
            int[] mapping,
            int mappingOffset,
            int baseOffset,
            int[] ownedMapping) {}

    private final PrimitiveArrayPool arrayPool;
    private final Object[] inputs;
    private final int[][] inputValueOffsets;
    private final int[][] inputMappings;
    private final int[] inputMappingOffsets;
    private final int[] inputBaseOffsets;
    private final boolean[][] inputNulls;
    private final int[][] inputNullMappings;
    private final int[] inputNullMappingOffsets;
    private final int[] inputNullBaseOffsets;
    private final boolean[] readsInput;
    private final boolean[] readsValue;
    private final boolean[] intInputs;
    private final boolean[] allNullInputs;
    private final ContributionCarrier[] inputCarriers;
    private final int[][] ownedInputMappings;
    private final int[][] ownedInputNullMappings;

    public GeneratedAggregationRowBindings(int inputCount, PrimitiveArrayPool arrayPool)
    {
        if (inputCount < 0) {
            throw new IllegalArgumentException("inputCount is negative");
        }
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        inputs = new Object[inputCount];
        inputValueOffsets = new int[inputCount][];
        inputMappings = new int[inputCount][];
        inputMappingOffsets = new int[inputCount];
        inputBaseOffsets = new int[inputCount];
        inputNulls = new boolean[inputCount][];
        inputNullMappings = new int[inputCount][];
        inputNullMappingOffsets = new int[inputCount];
        inputNullBaseOffsets = new int[inputCount];
        readsInput = new boolean[inputCount];
        readsValue = new boolean[inputCount];
        intInputs = new boolean[inputCount];
        allNullInputs = new boolean[inputCount];
        inputCarriers = new ContributionCarrier[inputCount];
        ownedInputMappings = new int[inputCount][];
        ownedInputNullMappings = new int[inputCount][];
    }

    public boolean bindInput(
            int index,
            Vector values,
            Vector nulls,
            boolean valueRequired,
            ContributionCarrier carrier)
    {
        clearInput(index);
        readsInput[index] = true;
        readsValue[index] = valueRequired;
        inputCarriers[index] = requireNonNull(carrier, "carrier is null");
        try {
            if (valueRequired) {
                BoundVector bound = bind(values, carrier);
                inputs[index] = bound.values();
                inputValueOffsets[index] = bound.valueOffsets();
                inputMappings[index] = bound.mapping();
                inputMappingOffsets[index] = bound.mappingOffset();
                inputBaseOffsets[index] = bound.baseOffset();
                ownedInputMappings[index] = bound.ownedMapping();
                intInputs[index] = bound.values() instanceof int[];
            }

            if (VectorAccess.isAllFalseNulls(nulls)) {
                return true;
            }
            if (VectorAccess.isAllTrueNulls(nulls)) {
                allNullInputs[index] = true;
                return true;
            }
            BoundVector boundNulls = bind(nulls, ContributionCarrier.BOOLEAN);
            inputNulls[index] = (boolean[]) boundNulls.values();
            inputNullMappings[index] = boundNulls.mapping();
            inputNullMappingOffsets[index] = boundNulls.mappingOffset();
            inputNullBaseOffsets[index] = boundNulls.baseOffset();
            ownedInputNullMappings[index] = boundNulls.ownedMapping();
            return true;
        }
        catch (IllegalArgumentException e) {
            clearInput(index);
            return false;
        }
    }

    public void clearInput(int index)
    {
        arrayPool.release(ownedInputMappings[index]);
        arrayPool.release(ownedInputNullMappings[index]);
        ownedInputMappings[index] = null;
        ownedInputNullMappings[index] = null;
        inputs[index] = null;
        inputValueOffsets[index] = null;
        inputMappings[index] = null;
        inputMappingOffsets[index] = 0;
        inputBaseOffsets[index] = 0;
        inputNulls[index] = null;
        inputNullMappings[index] = null;
        inputNullMappingOffsets[index] = 0;
        inputNullBaseOffsets[index] = 0;
        readsInput[index] = false;
        readsValue[index] = false;
        intInputs[index] = false;
        allNullInputs[index] = false;
        inputCarriers[index] = null;
    }

    public PhysicalShape capturePhysicalShape()
    {
        short[] inputShapes = new short[inputs.length];
        for (int index = 0; index < inputs.length; index++) {
            inputShapes[index] = inputPhysicalShape(index);
        }
        return new PhysicalShape(inputShapes);
    }

    public boolean matchesPhysicalShape(PhysicalShape shape)
    {
        if (shape == null || shape.inputShapes.length != inputs.length) {
            return false;
        }
        for (int index = 0; index < inputs.length; index++) {
            if (shape.inputShapes[index] != inputPhysicalShape(index)) {
                return false;
            }
        }
        return true;
    }

    private short inputPhysicalShape(int index)
    {
        int carrier = readsValue[index] ? inputCarriers[index].ordinal() + 1 : 0;
        return (short) ((readsInput[index] ? 1 : 0) |
                (readsValue[index] ? 1 << 1 : 0) |
                (intInputs[index] ? 1 << 2 : 0) |
                (carrier << 3) |
                (inputMappings[index] != null ? 1 << 6 : 0) |
                (inputMappingOffsets[index] != 0 ? 1 << 7 : 0) |
                (inputBaseOffsets[index] != 0 ? 1 << 8 : 0) |
                (inputNulls[index] != null ? 1 << 9 : 0) |
                (inputNullMappings[index] != null ? 1 << 10 : 0) |
                (inputNullMappingOffsets[index] != 0 ? 1 << 11 : 0) |
                (inputNullBaseOffsets[index] != 0 ? 1 << 12 : 0) |
                (allNullInputs[index] ? 1 << 13 : 0));
    }

    public static final class PhysicalShape
    {
        private final short[] inputShapes;

        private PhysicalShape(short[] inputShapes)
        {
            this.inputShapes = inputShapes;
        }
    }

    public Object[] inputs()
    {
        return inputs;
    }

    public int[][] inputValueOffsets()
    {
        return inputValueOffsets;
    }

    public int[][] inputMappings()
    {
        return inputMappings;
    }

    public int[] inputMappingOffsets()
    {
        return inputMappingOffsets;
    }

    public int[] inputBaseOffsets()
    {
        return inputBaseOffsets;
    }

    public boolean[][] inputNulls()
    {
        return inputNulls;
    }

    public int[][] inputNullMappings()
    {
        return inputNullMappings;
    }

    public int[] inputNullMappingOffsets()
    {
        return inputNullMappingOffsets;
    }

    public int[] inputNullBaseOffsets()
    {
        return inputNullBaseOffsets;
    }

    public boolean[] intInputs()
    {
        return intInputs;
    }

    public boolean[] allNullInputs()
    {
        return allNullInputs;
    }

    public ContributionCarrier[] inputCarriers()
    {
        return inputCarriers;
    }

    public boolean[] mappedInputs()
    {
        boolean[] mapped = new boolean[inputMappings.length];
        for (int index = 0; index < mapped.length; index++) {
            mapped[index] = inputMappings[index] != null;
        }
        return mapped;
    }

    public boolean[] mappedInputNulls()
    {
        boolean[] mapped = new boolean[inputNullMappings.length];
        for (int index = 0; index < mapped.length; index++) {
            mapped[index] = inputNullMappings[index] != null;
        }
        return mapped;
    }

    public boolean[] inputMappingOffsetInputs()
    {
        boolean[] offset = new boolean[inputMappingOffsets.length];
        for (int index = 0; index < offset.length; index++) {
            offset[index] = inputMappingOffsets[index] != 0;
        }
        return offset;
    }

    public boolean[] inputBaseOffsetInputs()
    {
        boolean[] offset = new boolean[inputBaseOffsets.length];
        for (int index = 0; index < offset.length; index++) {
            offset[index] = inputBaseOffsets[index] != 0;
        }
        return offset;
    }

    public boolean[] nullableInputs()
    {
        boolean[] nullable = new boolean[inputNulls.length];
        for (int index = 0; index < nullable.length; index++) {
            nullable[index] = inputNulls[index] != null;
        }
        return nullable;
    }

    public boolean[] inputNullMappingOffsetInputs()
    {
        boolean[] offset = new boolean[inputNullMappingOffsets.length];
        for (int index = 0; index < offset.length; index++) {
            offset[index] = inputNullMappingOffsets[index] != 0;
        }
        return offset;
    }

    public boolean[] inputNullBaseOffsetInputs()
    {
        boolean[] offset = new boolean[inputNullBaseOffsets.length];
        for (int index = 0; index < offset.length; index++) {
            offset[index] = inputNullBaseOffsets[index] != 0;
        }
        return offset;
    }

    @Override
    public void close()
    {
        release();
    }

    public void release()
    {
        for (int index = 0; index < inputs.length; index++) {
            clearInput(index);
        }
    }

    private BoundVector bind(Vector vector, ContributionCarrier carrier)
    {
        if (vector == null) {
            throw new IllegalArgumentException("Required generated aggregation vector is null");
        }
        return switch (vector) {
            case I64Vector longs when carrier == ContributionCarrier.LONG ->
                    new BoundVector(longs.values(), null, null, 0, 0, null);
            case I32Vector integers when carrier == ContributionCarrier.LONG ->
                    new BoundVector(integers.values(), null, null, 0, 0, null);
            case F64Vector doubles when carrier == ContributionCarrier.DOUBLE ->
                    new BoundVector(doubles.values(), null, null, 0, 0, null);
            case BooleanVector booleans when carrier == ContributionCarrier.BOOLEAN ->
                    new BoundVector(booleans.values(), null, null, 0, 0, null);
            case BinaryVector binary when carrier == ContributionCarrier.BINARY_REGION ->
                    new BoundVector(binary.data(), binary.offsets(), null, 0, 0, null);
            case RegionVector region -> bindRegion(region, carrier);
            case DictionaryVector dictionary -> bindDictionary(dictionary, carrier);
            case RleVector rle -> bindRle(rle, carrier);
            default -> throw new IllegalArgumentException(
                    "No generated aggregation loader for " + vector.getClass().getSimpleName() + " as " + carrier);
        };
    }

    private BoundVector bindRegion(RegionVector region, ContributionCarrier carrier)
    {
        BoundVector child = bind(region.values(), carrier);
        if (child.mapping() == null) {
            return new BoundVector(
                    child.values(), child.valueOffsets(), null, 0, child.baseOffset() + region.offset(), child.ownedMapping());
        }
        return new BoundVector(
                child.values(),
                child.valueOffsets(),
                child.mapping(),
                child.mappingOffset() + region.offset(),
                child.baseOffset(),
                child.ownedMapping());
    }

    private BoundVector bindDictionary(DictionaryVector dictionary, ContributionCarrier carrier)
    {
        BoundVector child = bind(dictionary.values(), carrier);
        if (child.mapping() == null) {
            return new BoundVector(
                    child.values(), child.valueOffsets(), dictionary.ids(), 0, child.baseOffset(), child.ownedMapping());
        }
        int[] mapping = arrayPool.borrowInts(dictionary.length());
        int[] ids = dictionary.ids();
        for (int position = 0; position < dictionary.length(); position++) {
            mapping[position] = physicalPosition(child, ids[position]);
        }
        arrayPool.release(child.ownedMapping());
        return new BoundVector(child.values(), child.valueOffsets(), mapping, 0, 0, mapping);
    }

    private BoundVector bindRle(RleVector rle, ContributionCarrier carrier)
    {
        BoundVector child = bind(rle.values(), carrier);
        int[] mapping = arrayPool.borrowInts(rle.length());
        int output = 0;
        for (int run = 0; run < rle.counts().length; run++) {
            int physical = physicalPosition(child, run);
            Arrays.fill(mapping, output, output + rle.counts()[run], physical);
            output += rle.counts()[run];
        }
        arrayPool.release(child.ownedMapping());
        return new BoundVector(child.values(), child.valueOffsets(), mapping, 0, 0, mapping);
    }

    private static int physicalPosition(BoundVector binding, int logicalPosition)
    {
        int position = binding.mapping() == null
                ? logicalPosition
                : binding.mapping()[logicalPosition + binding.mappingOffset()];
        return position + binding.baseOffset();
    }
}
