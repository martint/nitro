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
package org.weakref.nitro.function.scalar.builtin;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "if_i64")
public final class IfI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("IfI64");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAndNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 3, "Unexpected argument count for if_i64");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector condition = inputs.get(0).values();
        Vector conditionNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector trueValues = inputs.get(1).values();
        Vector falseValues = inputs.get(2).values();
        Vector trueNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector falseNulls = inputs.get(2).getOrNull(Stream.NULLS);
        VectorAccess.BooleanValues conditionValues = VectorAccess.booleanValues(condition);
        VectorAccess.LongValues trueBranchValues = VectorAccess.longValues(trueValues);
        VectorAccess.LongValues falseBranchValues = VectorAccess.longValues(falseValues);
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(trueValues.length(), falseValues.length()));

        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.VALUES)) {
            Streams constantBranchOutput = tryApplyConstantBranchValues(condition, conditionValues, conditionNulls, trueValues, falseValues, trueNulls, falseNulls, mask, requiredLength, requestedStreams, context);
            if (constantBranchOutput != null) {
                return constantBranchOutput;
            }
        }

        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    requiredLength);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            I64Vector outputValues = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    requiredLength,
                    I64Vector::new);
            // The common conditional-aggregation shape if(cond, column, constant) leaves one branch a
            // non-null constant. Specializing it drops the per-row branch-object selection
            // (takeTrue ? trueValues : falseValues) that otherwise makes the value reads megamorphic, so
            // the surviving column access inlines to a flat-array load.
            Long falseConstant = constantNullValue(falseNulls) == Boolean.FALSE ? constantIntegerValue(falseValues) : null;
            if (falseConstant != null) {
                applyValuesConstantFalse(conditionValues, conditionNulls, trueBranchValues, trueNulls, falseConstant, mask, outputValues, outputNulls);
                return result.with(Stream.VALUES, outputValues);
            }
            Long trueConstant = constantNullValue(trueNulls) == Boolean.FALSE ? constantIntegerValue(trueValues) : null;
            if (trueConstant != null) {
                applyValuesConstantTrue(conditionValues, conditionNulls, trueConstant, falseBranchValues, falseNulls, mask, outputValues, outputNulls);
                return result.with(Stream.VALUES, outputValues);
            }
            applyValues(conditionValues, conditionNulls, trueBranchValues, falseBranchValues, trueNulls, falseNulls, mask, outputValues, outputNulls);
            return result.with(Stream.VALUES, outputValues);
        }

        applyNulls(conditionValues, conditionNulls, trueNulls, falseNulls, mask, outputNulls);
        return result;
    }

    private static Streams tryApplyConstantBranchValues(Vector condition, VectorAccess.BooleanValues conditionValues, Vector conditionNulls, Vector trueValues, Vector falseValues, Vector trueNulls, Vector falseNulls, Mask mask, int requiredLength, Set<Stream> requestedStreams, PrimitiveExecutionContext context)
    {
        if (constantNullValue(trueNulls) != Boolean.FALSE || constantNullValue(falseNulls) != Boolean.FALSE) {
            return null;
        }

        Long trueValue = constantIntegerValue(trueValues);
        Long falseValue = constantIntegerValue(falseValues);
        if (trueValue == null || falseValue == null) {
            return null;
        }

        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector nullValues = context.allocator().allocate(ALLOCATION_CONTEXT, BooleanVector.class, 1, BooleanVector::new);
            nullValues.values()[0] = false;
            result = result.with(Stream.NULLS, context.allocator().allocateRle(ALLOCATION_CONTEXT, new int[] {requiredLength}, nullValues));
        }
        if (trueValue.equals(falseValue)) {
            I64Vector values = context.allocator().allocate(ALLOCATION_CONTEXT, I64Vector.class, 1, I64Vector::new);
            values.values()[0] = trueValue;
            return requestedStreams.contains(Stream.VALUES) ? result.with(Stream.VALUES, context.allocator().allocateRle(ALLOCATION_CONTEXT, new int[] {requiredLength}, values)) : result;
        }

        int[] ids = new int[requiredLength];
        if (mask.all() && condition instanceof BooleanVector conditionFlat && VectorAccess.isAllFalseNulls(conditionNulls)) {
            // Monomorphic null-free fast path: read the flat boolean condition directly, no per-position accessor.
            boolean[] flags = conditionFlat.values();
            for (int position = 0; position < mask.size(); position++) {
                ids[position] = flags[position] ? 1 : 0;
            }
        }
        else if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                ids[position] = conditionValue(conditionValues, conditionNulls, position) ? 1 : 0;
            }
        }
        else {
            for (int position : mask) {
                ids[position] = conditionValue(conditionValues, conditionNulls, position) ? 1 : 0;
            }
        }

        I64Vector dictionaryValues = context.allocator().allocate(ALLOCATION_CONTEXT, I64Vector.class, 2, I64Vector::new);
        dictionaryValues.values()[0] = falseValue;
        dictionaryValues.values()[1] = trueValue;
        return requestedStreams.contains(Stream.VALUES) ? result.with(Stream.VALUES, context.allocator().allocateDictionary(ALLOCATION_CONTEXT, ids, dictionaryValues)) : result;
    }

    private static void applyValues(VectorAccess.BooleanValues conditionValues, Vector conditionNulls, VectorAccess.LongValues trueValues, VectorAccess.LongValues falseValues, Vector trueNulls, Vector falseNulls, Mask mask, I64Vector outputValues, BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues conditionNullValues = VectorAccess.booleanValues(conditionNulls);
        VectorAccess.BooleanValues trueNullValues = VectorAccess.booleanValues(trueNulls);
        VectorAccess.BooleanValues falseNullValues = VectorAccess.booleanValues(falseNulls);
        for (int position : mask) {
            boolean takeTrue = !conditionNullValues.value(position) && conditionValues.value(position);
            VectorAccess.LongValues selectedValues = takeTrue ? trueValues : falseValues;
            VectorAccess.BooleanValues selectedNullValues = takeTrue ? trueNullValues : falseNullValues;
            if (selectedNullValues.value(position)) {
                if (outputNulls != null) {
                    outputNulls.values()[position] = true;
                }
                continue;
            }

            outputValues.values()[position] = selectedValues.value(position);
            if (outputNulls != null) {
                outputNulls.values()[position] = false;
            }
        }
    }

    private static void applyValuesConstantFalse(VectorAccess.BooleanValues conditionValues, Vector conditionNulls, VectorAccess.LongValues trueValues, Vector trueNulls, long falseConstant, Mask mask, I64Vector outputValues, BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues conditionNullValues = VectorAccess.booleanValues(conditionNulls);
        VectorAccess.BooleanValues trueNullValues = VectorAccess.booleanValues(trueNulls);
        long[] values = outputValues.values();
        boolean[] nulls = outputNulls != null ? outputNulls.values() : null;
        for (int position : mask) {
            if (!conditionNullValues.value(position) && conditionValues.value(position)) {
                if (trueNullValues.value(position)) {
                    if (nulls != null) {
                        nulls[position] = true;
                    }
                    continue;
                }
                values[position] = trueValues.value(position);
            }
            else {
                values[position] = falseConstant;
            }
            if (nulls != null) {
                nulls[position] = false;
            }
        }
    }

    private static void applyValuesConstantTrue(VectorAccess.BooleanValues conditionValues, Vector conditionNulls, long trueConstant, VectorAccess.LongValues falseValues, Vector falseNulls, Mask mask, I64Vector outputValues, BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues conditionNullValues = VectorAccess.booleanValues(conditionNulls);
        VectorAccess.BooleanValues falseNullValues = VectorAccess.booleanValues(falseNulls);
        long[] values = outputValues.values();
        boolean[] nulls = outputNulls != null ? outputNulls.values() : null;
        for (int position : mask) {
            if (!conditionNullValues.value(position) && conditionValues.value(position)) {
                values[position] = trueConstant;
            }
            else {
                if (falseNullValues.value(position)) {
                    if (nulls != null) {
                        nulls[position] = true;
                    }
                    continue;
                }
                values[position] = falseValues.value(position);
            }
            if (nulls != null) {
                nulls[position] = false;
            }
        }
    }

    private static void applyNulls(VectorAccess.BooleanValues conditionValues, Vector conditionNulls, Vector trueNulls, Vector falseNulls, Mask mask, BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues conditionNullValues = VectorAccess.booleanValues(conditionNulls);
        VectorAccess.BooleanValues trueNullValues = VectorAccess.booleanValues(trueNulls);
        VectorAccess.BooleanValues falseNullValues = VectorAccess.booleanValues(falseNulls);
        boolean[] nulls = outputNulls.values();
        Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            boolean takeTrue = !conditionNullValues.value(position) && conditionValues.value(position);
            nulls[position] = takeTrue ? trueNullValues.value(position) : falseNullValues.value(position);
        }
    }

    private static boolean conditionValue(VectorAccess.BooleanValues values, Vector nulls, int position)
    {
        if (VectorAccess.isNull(nulls, position)) {
            return false;
        }
        return values.value(position);
    }

    private static Long constantIntegerValue(Vector values)
    {
        return switch (values) {
            case I32Vector vector when vector.length() == 1 -> (long) vector.values()[0];
            case I64Vector vector when vector.length() == 1 -> vector.values()[0];
            case DictionaryVector vector when vector.length() == 1 -> constantIntegerValue(vector.values(), vector.ids()[0]);
            case RleVector vector when vector.counts().length == 1 -> constantIntegerValue(vector.values(), vector.runIndex(0));
            default -> null;
        };
    }

    private static Long constantIntegerValue(Vector values, int position)
    {
        return switch (values) {
            case I32Vector vector -> (long) vector.values()[position];
            case I64Vector vector -> vector.values()[position];
            case DictionaryVector vector -> constantIntegerValue(vector.values(), vector.ids()[position]);
            case RleVector vector -> constantIntegerValue(vector.values(), vector.runIndex(position));
            default -> null;
        };
    }

    private static Boolean constantNullValue(Vector nulls)
    {
        if (nulls == null) {
            return false;
        }

        // A single-run RLE (the shape a literal branch's null stream takes, with length() == the batch size) is
        // constant by construction, so resolve it in O(1) instead of scanning every position.
        if (nulls instanceof RleVector rle && rle.counts().length == 1) {
            return VectorAccess.booleanValues(rle.values()).value(0);
        }

        if (nulls instanceof DictionaryVector dictionary) {
            int dictionarySize = dictionary.values().length();
            int[] ids = dictionary.ids();
            for (int position = 0; position < dictionary.length(); position++) {
                if (ids[position] < 0 || ids[position] >= dictionarySize) {
                    return null;
                }
            }
        }

        VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
        if (nulls.length() == 1) {
            return nullValues.value(0);
        }

        for (int position = 0; position < nulls.length(); position++) {
            if (nullValues.value(position)) {
                return null;
            }
        }
        return false;
    }

    private static Boolean constantNullValue(Vector nulls, int position)
    {
        if (nulls == null) {
            return false;
        }
        return VectorAccess.booleanValues(nulls).value(position);
    }
}
