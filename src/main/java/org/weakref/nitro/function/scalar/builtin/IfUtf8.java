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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
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

@ScalarFunction(name = "if_utf8")
public final class IfUtf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("IfUtf8");

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
        checkArgument(inputs.size() == 3, "Unexpected argument count for if_utf8");
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

        int totalBytes = 0;
        if (requestedStreams.contains(Stream.VALUES)) {
            for (int position : mask) {
                Vector selected = conditionValue(condition, conditionNulls, position) ? trueValues : falseValues;
                totalBytes += byteLength(selected, position);
            }
        }

        Streams result = Streams.empty();
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
            BinaryVector outputValues = BinaryVector.allocateOrGrow(
                    context.allocator(),
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof BinaryVector vector ? vector : null,
                    requiredLength,
                    totalBytes);
            outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
            applyValues(condition, conditionNulls, trueValues, falseValues, trueNulls, falseNulls, mask, outputValues, outputNulls);
            result = result.with(Stream.VALUES, outputValues);
            return result;
        }

        if (outputNulls != null) {
            applyNulls(condition, conditionNulls, trueNulls, falseNulls, mask, outputNulls);
        }
        return result;
    }

    private static void applyValues(Vector condition, Vector conditionNulls, Vector trueValues, Vector falseValues, Vector trueNulls, Vector falseNulls, Mask mask, BinaryVector outputValues, BooleanVector outputNulls)
    {
        for (int position : mask) {
            boolean takeTrue = conditionValue(condition, conditionNulls, position);
            Vector selectedValues = takeTrue ? trueValues : falseValues;
            Vector selectedNulls = takeTrue ? trueNulls : falseNulls;
            if (isNull(selectedNulls, position)) {
                outputValues.setNull(position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = true;
                }
            }
            else {
                copyBytes(selectedValues, position, outputValues, position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = false;
                }
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
            default -> throw new IllegalArgumentException("Unsupported if_utf8 condition vector type: " + values.getClass().getSimpleName());
        };
    }

    private static int byteLength(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector vector -> vector.length(position);
            case DictionaryVector vector -> byteLength(vector.values(), vector.ids()[position]);
            case RleVector vector -> byteLength(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported if_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static void copyBytes(Vector values, int inputPosition, BinaryVector output, int outputPosition)
    {
        switch (values) {
            case BinaryVector vector -> output.setBytes(outputPosition, vector.data(), vector.startOffset(inputPosition), vector.length(inputPosition));
            case DictionaryVector vector -> copyBytes(vector.values(), vector.ids()[inputPosition], output, outputPosition);
            case RleVector vector -> copyBytes(vector.values(), vector.runIndex(inputPosition), output, outputPosition);
            default -> throw new IllegalArgumentException("Unsupported if_utf8 vector type: " + values.getClass().getSimpleName());
        }
    }

    private static boolean isNull(Vector nulls, int position)
    {
        return switch (nulls) {
            case null -> false;
            case BooleanVector vector -> vector.values()[position];
            case DictionaryVector vector -> isNull(vector.values(), vector.ids()[position]);
            case RleVector vector -> isNull(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported if_utf8 null vector type: " + nulls.getClass().getSimpleName());
        };
    }
}
