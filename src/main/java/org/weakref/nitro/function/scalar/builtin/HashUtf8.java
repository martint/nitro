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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "hash_utf8")
public final class HashUtf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("HashUtf8");

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 1, "Unexpected argument count for hash_utf8");

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
            applyValues(values, inputNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void applyValues(Vector values, BooleanVector inputNulls, Mask mask, I64Vector output)
    {
        if (values instanceof BinaryVector binaryValues) {
            applyFlat(binaryValues, inputNulls, mask, output);
            return;
        }
        if (values instanceof DictionaryVector dictionaryValues) {
            applyDictionary(dictionaryValues, inputNulls, mask, output);
            return;
        }
        throw new IllegalArgumentException("Unsupported hash_utf8 vector type: " + values.getClass().getSimpleName());
    }

    private static void applyFlat(BinaryVector values, BooleanVector inputNulls, Mask mask, I64Vector output)
    {
        boolean ascii = requireUtf8AndCheckAscii(values);
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(inputNulls, position) ? 0 : hash(values, position, ascii);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(inputNulls, position) ? 0 : hash(values, position, ascii);
        }
    }

    private static void applyDictionary(DictionaryVector dictionaryValues, BooleanVector inputNulls, Mask mask, I64Vector output)
    {
        checkArgument(dictionaryValues.values() instanceof BinaryVector, "hash_utf8 requires BinaryVector dictionary values");
        BinaryVector values = (BinaryVector) dictionaryValues.values();
        boolean ascii = requireUtf8AndCheckAscii(values);
        int[] ids = dictionaryValues.ids();
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(inputNulls, position) ? 0 : hash(values, ids[position], ascii);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(inputNulls, position) ? 0 : hash(values, ids[position], ascii);
        }
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

    private static boolean requireUtf8AndCheckAscii(BinaryVector vector)
    {
        checkArgument(vector.hasTrait(BinaryVector.Trait.UTF8_STRING), "hash_utf8 requires UTF8_STRING inputs");
        return vector.hasTrait(BinaryVector.Trait.ASCII_ONLY);
    }

    private static long hash(BinaryVector vector, int position, boolean ascii)
    {
        if (ascii) {
            return asciiHash(vector, position);
        }
        return vector.utf8Value(position).hashCode();
    }

    private static long asciiHash(BinaryVector vector, int position)
    {
        int hash = 0;
        byte[] data = vector.data();
        int start = vector.startOffset(position);
        int end = vector.endOffset(position);
        for (int index = start; index < end; index++) {
            hash = 31 * hash + Byte.toUnsignedInt(data[index]);
        }
        return hash;
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }
}
