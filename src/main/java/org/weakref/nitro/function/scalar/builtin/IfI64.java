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
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(trueValues.length(), falseValues.length()));

        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.VALUES)) {
            Streams constantBranchOutput = tryApplyConstantBranchValues(condition, conditionNulls, trueValues, falseValues, trueNulls, falseNulls, mask, requiredLength, requestedStreams, context);
            if (constantBranchOutput != null) {
                return constantBranchOutput;
            }
        }

        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            I64Vector outputValues = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    requiredLength,
                    I64Vector::new);
            applyValues(condition, conditionNulls, trueValues, falseValues, trueNulls, falseNulls, mask, outputValues, outputNulls);
            return result.with(Stream.VALUES, outputValues);
        }

        applyNulls(condition, conditionNulls, trueNulls, falseNulls, mask, outputNulls);
        return result;
    }

    private static Streams tryApplyConstantBranchValues(Vector condition, Vector conditionNulls, Vector trueValues, Vector falseValues, Vector trueNulls, Vector falseNulls, Mask mask, int requiredLength, Set<Stream> requestedStreams, PrimitiveExecutionContext context)
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
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                ids[position] = conditionValue(condition, conditionNulls, position) ? 1 : 0;
            }
        }
        else {
            for (int position : mask) {
                ids[position] = conditionValue(condition, conditionNulls, position) ? 1 : 0;
            }
        }

        I64Vector dictionaryValues = context.allocator().allocate(ALLOCATION_CONTEXT, I64Vector.class, 2, I64Vector::new);
        dictionaryValues.values()[0] = falseValue;
        dictionaryValues.values()[1] = trueValue;
        return requestedStreams.contains(Stream.VALUES) ? result.with(Stream.VALUES, context.allocator().allocateDictionary(ALLOCATION_CONTEXT, ids, dictionaryValues)) : result;
    }

    private static void applyValues(Vector condition, Vector conditionNulls, Vector trueValues, Vector falseValues, Vector trueNulls, Vector falseNulls, Mask mask, I64Vector outputValues, BooleanVector outputNulls)
    {
        for (int position : mask) {
            boolean takeTrue = conditionValue(condition, conditionNulls, position);
            Vector selectedValues = takeTrue ? trueValues : falseValues;
            Vector selectedNulls = takeTrue ? trueNulls : falseNulls;
            if (isNull(selectedNulls, position)) {
                if (outputNulls != null) {
                    outputNulls.values()[position] = true;
                }
                continue;
            }

            outputValues.values()[position] = integerValue(selectedValues, position);
            if (outputNulls != null) {
                outputNulls.values()[position] = false;
            }
        }
    }

    private static void applyNulls(Vector condition, Vector conditionNulls, Vector trueNulls, Vector falseNulls, Mask mask, BooleanVector outputNulls)
    {
        boolean[] nulls = outputNulls.values();
        Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            boolean takeTrue = conditionValue(condition, conditionNulls, position);
            Vector selectedNulls = takeTrue ? trueNulls : falseNulls;
            nulls[position] = isNull(selectedNulls, position);
        }
    }

    private static boolean conditionValue(Vector values, Vector nulls, int position)
    {
        if (isNull(nulls, position)) {
            return false;
        }
        return switch (values) {
            case BooleanVector vector -> vector.values()[position];
            case DictionaryVector vector -> conditionValue(vector.values(), null, vector.ids()[position]);
            case RleVector vector -> conditionValue(vector.values(), null, vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported if_i64 condition vector type: " + values.getClass().getSimpleName());
        };
    }

    private static long integerValue(Vector values, int position)
    {
        return switch (values) {
            case I32Vector vector -> vector.values()[position];
            case I64Vector vector -> vector.values()[position];
            case DictionaryVector vector -> integerValue(vector.values(), vector.ids()[position]);
            case RleVector vector -> integerValue(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported if_i64 branch vector type: " + values.getClass().getSimpleName());
        };
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
        return switch (nulls) {
            case null -> false;
            case BooleanVector vector when vector.length() == 1 -> vector.values()[0];
            case BooleanVector vector -> {
                for (boolean value : vector.values()) {
                    if (value) {
                        yield null;
                    }
                }
                yield false;
            }
            case DictionaryVector vector when vector.length() == 1 -> constantNullValue(vector.values(), vector.ids()[0]);
            case RleVector vector when vector.counts().length == 1 -> constantNullValue(vector.values(), vector.runIndex(0));
            case DictionaryVector _, RleVector _ -> null;
            default -> throw new IllegalArgumentException("Unsupported if_i64 null vector type: " + nulls.getClass().getSimpleName());
        };
    }

    private static Boolean constantNullValue(Vector nulls, int position)
    {
        return switch (nulls) {
            case null -> false;
            case BooleanVector vector -> vector.values()[position];
            case DictionaryVector vector -> constantNullValue(vector.values(), vector.ids()[position]);
            case RleVector vector -> constantNullValue(vector.values(), vector.runIndex(position));
            default -> null;
        };
    }

    private static boolean isNull(Vector nulls, int position)
    {
        return switch (nulls) {
            case null -> false;
            case BooleanVector vector -> vector.values()[position];
            case DictionaryVector vector -> isNull(vector.values(), vector.ids()[position]);
            case RleVector vector -> isNull(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported if_i64 null vector type: " + nulls.getClass().getSimpleName());
        };
    }
}
