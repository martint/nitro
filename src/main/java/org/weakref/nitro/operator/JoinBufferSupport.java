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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;

final class JoinBufferSupport
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext;

    JoinBufferSupport(Allocator allocator, Allocator.Context allocationContext)
    {
        this.allocator = allocator;
        this.allocationContext = allocationContext;
    }

    public Streams borrowStreams(Output output)
    {
        Streams streams = Streams.empty();
        for (Stream stream : output.streams()) {
            streams = streams.with(stream, output.borrow(stream));
        }
        return streams;
    }

    public Streams replicate(Streams existing, Streams input, int size, int start, int length, int position)
    {
        Streams result = Streams.empty();
        for (Stream stream : input.asMap().keySet()) {
            Vector existingVector = existing != null ? existing.getOrNull(stream) : null;
            result = result.with(stream, replicateVector(existingVector, input.get(stream), size, start, length, position));
        }
        return result;
    }

    public Streams copyAndCompact(Output input, Mask mask, int maskStart, Streams existing, int outputStart, int copied, int size)
    {
        Streams result = Streams.empty();
        for (Stream stream : input.streams()) {
            Vector existingVector = existing != null ? existing.getOrNull(stream) : null;
            result = result.with(stream, compactVector(existingVector, input.borrow(stream), mask, maskStart, outputStart, copied, size));
        }
        return result;
    }

    public Streams emptyLike(Streams schema)
    {
        Streams empty = Streams.empty();
        for (Stream stream : schema.asMap().keySet()) {
            empty = empty.with(stream, emptyVector(schema.get(stream)));
        }
        return empty;
    }

    public Vector copyStreamVector(Streams streams, Stream stream)
    {
        return allocator.copyVector(allocationContext, streams.get(stream));
    }

    private Vector compactVector(Vector existing, Vector source, Mask mask, int maskStart, int outputStart, int copied, int size)
    {
        return switch (flatten(source)) {
            case I64Vector _ -> compactLongVector(existing, source, mask, maskStart, outputStart, copied, size);
            case BooleanVector _ -> compactBooleanVector(existing, source, mask, maskStart, outputStart, copied, size);
            case F64Vector _ -> compactDoubleVector(existing, source, mask, maskStart, outputStart, copied, size);
            case BinaryVector _ -> compactBinaryVector(existing, source, mask, maskStart, outputStart, copied, size);
            default -> throw new IllegalArgumentException("Unsupported nested-loop vector type: " + source.getClass().getSimpleName());
        };
    }

    private Vector replicateVector(Vector existing, Vector source, int size, int start, int length, int position)
    {
        return switch (flatten(source)) {
            case I64Vector _ -> replicateLongVector(existing, source, size, start, length, position);
            case BooleanVector _ -> replicateBooleanVector(existing, source, size, start, length, position);
            case F64Vector _ -> replicateDoubleVector(existing, source, size, start, length, position);
            case BinaryVector _ -> replicateBinaryVector(existing, source, size, start, length, position);
            default -> throw new IllegalArgumentException("Unsupported nested-loop vector type: " + source.getClass().getSimpleName());
        };
    }

    private I64Vector compactLongVector(Vector existing, Vector source, Mask mask, int maskStart, int outputStart, int copied, int size)
    {
        I64Vector output = allocator.allocateOrGrow(allocationContext, existing instanceof I64Vector vector ? vector : null, I64Vector.class, size, I64Vector::new);
        if (source instanceof I64Vector values && mask.all()) {
            System.arraycopy(values.values(), maskStart, output.values(), outputStart, copied);
            return output;
        }
        for (int index = 0; index < copied; index++) {
            int sourcePosition = mask.all() ? maskStart + index : mask.position(maskStart + index);
            output.values()[outputStart + index] = longValue(source, sourcePosition);
        }
        return output;
    }

    private BooleanVector compactBooleanVector(Vector existing, Vector source, Mask mask, int maskStart, int outputStart, int copied, int size)
    {
        BooleanVector output = allocator.allocateOrGrow(allocationContext, existing instanceof BooleanVector vector ? vector : null, BooleanVector.class, size, BooleanVector::new);
        if (source instanceof BooleanVector values && mask.all()) {
            System.arraycopy(values.values(), maskStart, output.values(), outputStart, copied);
            return output;
        }
        for (int index = 0; index < copied; index++) {
            int sourcePosition = mask.all() ? maskStart + index : mask.position(maskStart + index);
            output.values()[outputStart + index] = booleanValue(source, sourcePosition);
        }
        return output;
    }

    private F64Vector compactDoubleVector(Vector existing, Vector source, Mask mask, int maskStart, int outputStart, int copied, int size)
    {
        F64Vector output = allocator.allocateOrGrow(allocationContext, existing instanceof F64Vector vector ? vector : null, F64Vector.class, size, F64Vector::new);
        if (source instanceof F64Vector values && mask.all()) {
            System.arraycopy(values.values(), maskStart, output.values(), outputStart, copied);
            return output;
        }
        for (int index = 0; index < copied; index++) {
            int sourcePosition = mask.all() ? maskStart + index : mask.position(maskStart + index);
            output.values()[outputStart + index] = doubleValue(source, sourcePosition);
        }
        return output;
    }

    private BinaryVector compactBinaryVector(Vector existing, Vector source, Mask mask, int maskStart, int outputStart, int copied, int size)
    {
        int byteCapacity = binaryCapacity(existing, outputStart);
        for (int index = 0; index < copied; index++) {
            int sourcePosition = mask.all() ? maskStart + index : mask.position(maskStart + index);
            byteCapacity += binaryLength(source, sourcePosition);
        }

        BinaryVector output = allocator.allocateOrGrowBinary(allocationContext, existing instanceof BinaryVector vector ? vector : null, size, byteCapacity);
        if (outputStart == 0) {
            Arrays.fill(output.offsets(), 0);
            output.clearTraits();
            output.addTraits(binaryTraits(source));
        }
        else {
            output.addTraits(binaryTraits(source));
        }

        int currentOffset = output.offsets()[outputStart];
        for (int index = 0; index < copied; index++) {
            int targetPosition = outputStart + index;
            int sourcePosition = mask.all() ? maskStart + index : mask.position(maskStart + index);
            byte[] bytes = binaryBytes(source, sourcePosition);
            output.offsets()[targetPosition] = currentOffset;
            if (bytes.length == 0) {
                output.setNull(targetPosition);
            }
            else {
                output.setBytes(targetPosition, bytes);
                currentOffset = output.endOffset(targetPosition);
            }
        }
        return output;
    }

    private I64Vector replicateLongVector(Vector existing, Vector source, int size, int start, int length, int position)
    {
        I64Vector output = allocator.allocateOrGrow(allocationContext, existing instanceof I64Vector vector ? vector : null, I64Vector.class, size, I64Vector::new);
        Arrays.fill(output.values(), start, start + length, longValue(source, position));
        return output;
    }

    private BooleanVector replicateBooleanVector(Vector existing, Vector source, int size, int start, int length, int position)
    {
        BooleanVector output = allocator.allocateOrGrow(allocationContext, existing instanceof BooleanVector vector ? vector : null, BooleanVector.class, size, BooleanVector::new);
        Arrays.fill(output.values(), start, start + length, booleanValue(source, position));
        return output;
    }

    private F64Vector replicateDoubleVector(Vector existing, Vector source, int size, int start, int length, int position)
    {
        F64Vector output = allocator.allocateOrGrow(allocationContext, existing instanceof F64Vector vector ? vector : null, F64Vector.class, size, F64Vector::new);
        Arrays.fill(output.values(), start, start + length, doubleValue(source, position));
        return output;
    }

    private BinaryVector replicateBinaryVector(Vector existing, Vector source, int size, int start, int length, int position)
    {
        byte[] bytes = binaryBytes(source, position);
        int requiredCapacity = binaryCapacity(existing, start) + bytes.length * length;
        BinaryVector output = allocator.allocateOrGrowBinary(allocationContext, existing instanceof BinaryVector vector ? vector : null, size, requiredCapacity);
        if (start == 0) {
            Arrays.fill(output.offsets(), 0);
            output.clearTraits();
        }
        output.addTraits(binaryTraits(source));

        int currentOffset = output.offsets()[start];
        for (int index = 0; index < length; index++) {
            int targetPosition = start + index;
            output.offsets()[targetPosition] = currentOffset;
            if (bytes.length == 0) {
                output.setNull(targetPosition);
            }
            else {
                output.setBytes(targetPosition, bytes);
                currentOffset = output.endOffset(targetPosition);
            }
        }
        return output;
    }

    private Vector emptyVector(Vector source)
    {
        return switch (flatten(source)) {
            case I64Vector _ -> allocator.allocate(allocationContext, I64Vector.class, 0, I64Vector::new);
            case BooleanVector _ -> allocator.allocate(allocationContext, BooleanVector.class, 0, BooleanVector::new);
            case F64Vector _ -> allocator.allocate(allocationContext, F64Vector.class, 0, F64Vector::new);
            case BinaryVector binary -> {
                BinaryVector empty = allocator.allocateBinary(allocationContext, 0, 0);
                empty.addTraits(binary.traits());
                yield empty;
            }
            default -> throw new IllegalArgumentException("Unsupported nested-loop vector type: " + source.getClass().getSimpleName());
        };
    }

    private static Vector flatten(Vector vector)
    {
        return switch (vector) {
            case DictionaryVector values -> flatten(values.values());
            case RleVector values -> flatten(values.values());
            default -> vector;
        };
    }

    private static long longValue(Vector vector, int position)
    {
        return switch (vector) {
            case I64Vector values -> values.values()[position];
            case DictionaryVector values -> longValue(values.values(), values.ids()[position]);
            case RleVector values -> longValue(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected I64 vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static boolean booleanValue(Vector vector, int position)
    {
        return switch (vector) {
            case BooleanVector values -> values.values()[position];
            case DictionaryVector values -> booleanValue(values.values(), values.ids()[position]);
            case RleVector values -> booleanValue(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected boolean vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static double doubleValue(Vector vector, int position)
    {
        return switch (vector) {
            case F64Vector values -> values.values()[position];
            case DictionaryVector values -> doubleValue(values.values(), values.ids()[position]);
            case RleVector values -> doubleValue(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected F64 vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static int binaryLength(Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector values -> values.length(position);
            case DictionaryVector values -> binaryLength(values.values(), values.ids()[position]);
            case RleVector values -> binaryLength(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static byte[] binaryBytes(Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector values -> values.copyBytes(position);
            case DictionaryVector values -> binaryBytes(values.values(), values.ids()[position]);
            case RleVector values -> binaryBytes(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static java.util.Set<BinaryVector.Trait> binaryTraits(Vector vector)
    {
        return switch (flatten(vector)) {
            case BinaryVector values -> values.traits();
            default -> java.util.Set.of();
        };
    }

    private static int binaryCapacity(Vector existing, int positionCount)
    {
        if (existing instanceof BinaryVector values) {
            return values.offsets()[positionCount];
        }
        return 0;
    }

    private static int runIndex(RleVector values, int position)
    {
        int offset = 0;
        for (int index = 0; index < values.counts().length; index++) {
            offset += values.counts()[index];
            if (position < offset) {
                return index;
            }
        }
        throw new IndexOutOfBoundsException("Position " + position + " is out of bounds for RLE vector of length " + values.length());
    }
}
