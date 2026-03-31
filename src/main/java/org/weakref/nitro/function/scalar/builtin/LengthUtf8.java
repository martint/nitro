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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.invoke.MethodHandles.byteArrayViewVarHandle;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

@ScalarFunction(name = "length_utf8")
public final class LengthUtf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("LengthUtf8");
    private static final VarHandle INT_HANDLE = byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);
    private static final VarHandle LONG_HANDLE = byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);
    private static final int TOP_MASK32 = 0x8080_8080;
    private static final long TOP_MASK64 = 0x8080_8080_8080_8080L;

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAlwaysNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 1, "Unexpected argument count for length_utf8");

        Vector values = inputs.getFirst().values();
        BooleanVector inputNulls = (BooleanVector) inputs.getFirst().getOrNull(Stream.NULLS);
        int requiredLength = Math.max(mask.maxPosition() + 1, values.length());
        Streams result = Streams.empty();

        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            copyNulls(inputNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            I64Vector outputValues = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    requiredLength,
                    I64Vector::new);
            apply(values, inputNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void apply(Vector values, BooleanVector inputNulls, Mask mask, I64Vector output)
    {
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(inputNulls, position) ? 0 : utf8Length(values, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(inputNulls, position) ? 0 : utf8Length(values, position);
        }
    }

    private static long utf8Length(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector vector -> utf8Length(vector.data(), vector.startOffset(position), vector.endOffset(position));
            case DictionaryVector vector -> utf8Length(vector.values(), vector.ids()[position]);
            case RleVector vector -> utf8Length(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported length_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static int utf8Length(byte[] data, int offset, int end)
    {
        int length = end - offset;
        if (length == 0) {
            return 0;
        }

        int continuationBytesCount = 0;
        int index = offset;
        int lastLongStart = end - Long.BYTES;
        for (; index <= lastLongStart; index += Long.BYTES) {
            continuationBytesCount += countContinuationBytes((long) LONG_HANDLE.get(data, index));
        }
        if (index <= end - Integer.BYTES) {
            continuationBytesCount += countContinuationBytes((int) INT_HANDLE.get(data, index));
            index += Integer.BYTES;
        }
        for (; index < end; index++) {
            continuationBytesCount += countContinuationBytes(data[index]);
        }

        return length - continuationBytesCount;
    }

    private static int countContinuationBytes(byte value)
    {
        int unsigned = value & 0xff;
        return (unsigned >>> 7) & (~unsigned >>> 6);
    }

    private static int countContinuationBytes(int value)
    {
        value = ((value & TOP_MASK32) >>> 1) & (~value);
        return Integer.bitCount(value);
    }

    private static int countContinuationBytes(long value)
    {
        value = ((value & TOP_MASK64) >>> 1) & (~value);
        return Long.bitCount(value);
    }

    private static void copyNulls(BooleanVector inputNulls, Mask mask, BooleanVector output)
    {
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(inputNulls, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(inputNulls, position);
        }
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }
}
