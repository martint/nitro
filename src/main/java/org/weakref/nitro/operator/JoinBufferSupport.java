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
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.Map;

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
        return copyStreamsPositions(existing, input, repeatedPositions(position, length), start, size);
    }

    public Streams copyAndCompact(Output input, Mask mask, int maskStart, Streams existing, int outputStart, int copied, int size)
    {
        Streams result = Streams.empty();
        int[] positions = positions(mask, maskStart, copied);
        for (Stream stream : input.streams()) {
            Vector existingVector = existing != null ? existing.getOrNull(stream) : null;
            result = result.with(stream, copyVectorPositions(existingVector, input.borrow(stream), positions, outputStart, size));
        }
        return result;
    }

    public Streams copyPosition(Output input, Streams existing, int position)
    {
        Streams result = Streams.empty();
        int[] positions = new int[] {position};
        for (Stream stream : input.streams()) {
            Vector existingVector = existing != null ? existing.getOrNull(stream) : null;
            result = result.with(stream, copyVectorPositions(existingVector, input.borrow(stream), positions, 0, 1));
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

    public Vector materializeStream(Vector sample, Vector[] rows)
    {
        return switch (OperatorVectorSupport.flatten(sample)) {
            case I64Vector _ -> materializeLongs(rows);
            case BooleanVector _ -> materializeBooleans(rows);
            case F64Vector _ -> materializeDoubles(rows);
            case BinaryVector binary -> materializeBinary(binary, rows);
            case ArrayVector array -> materializeArrays(array, rows);
            case MapVector map -> materializeMaps(map, rows);
            case StructVector struct -> materializeStructs(struct, rows);
            default -> throw new IllegalArgumentException("Unsupported materialized vector type: " + sample.getClass().getSimpleName());
        };
    }

    private Streams copyStreamsPositions(Streams existing, Streams source, int[] sourcePositions, int outputStart, int size)
    {
        Streams result = Streams.empty();
        for (Map.Entry<Stream, Vector> entry : source.asMap().entrySet()) {
            Vector existingVector = existing != null ? existing.getOrNull(entry.getKey()) : null;
            result = result.with(entry.getKey(), copyVectorPositions(existingVector, entry.getValue(), sourcePositions, outputStart, size));
        }
        return result;
    }

    private Vector copyVectorPositions(Vector existing, Vector source, int[] sourcePositions, int outputStart, int size)
    {
        return switch (source) {
            case I64Vector values -> copyLongPositions(values, existing, sourcePositions, outputStart, size);
            case BooleanVector values -> copyBooleanPositions(values, existing, sourcePositions, outputStart, size);
            case F64Vector values -> copyDoublePositions(values, existing, sourcePositions, outputStart, size);
            case BinaryVector values -> copyBinaryPositions(values, existing, sourcePositions, outputStart, size);
            case ArrayVector values -> copyArrayPositions(values, existing, sourcePositions, outputStart, size);
            case MapVector values -> copyMapPositions(values, existing, sourcePositions, outputStart, size);
            case StructVector values -> copyStructPositions(values, existing, sourcePositions, outputStart, size);
            case DictionaryVector values -> copyVectorPositions(existing, values.values(), dictionaryPositions(values.ids(), sourcePositions), outputStart, size);
            case RleVector values -> copyVectorPositions(existing, values.values(), rlePositions(values, sourcePositions), outputStart, size);
            default -> throw new IllegalArgumentException("Unsupported join vector type: " + source.getClass().getSimpleName());
        };
    }

    private I64Vector copyLongPositions(I64Vector source, Vector existing, int[] sourcePositions, int outputStart, int size)
    {
        I64Vector output = ensureLongCapacity(existing instanceof I64Vector vector ? vector : null, size);
        for (int index = 0; index < sourcePositions.length; index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions[index]];
        }
        return output;
    }

    private BooleanVector copyBooleanPositions(BooleanVector source, Vector existing, int[] sourcePositions, int outputStart, int size)
    {
        BooleanVector output = ensureBooleanCapacity(existing instanceof BooleanVector vector ? vector : null, size);
        for (int index = 0; index < sourcePositions.length; index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions[index]];
        }
        return output;
    }

    private F64Vector copyDoublePositions(F64Vector source, Vector existing, int[] sourcePositions, int outputStart, int size)
    {
        F64Vector output = ensureDoubleCapacity(existing instanceof F64Vector vector ? vector : null, size);
        for (int index = 0; index < sourcePositions.length; index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions[index]];
        }
        return output;
    }

    private BinaryVector copyBinaryPositions(BinaryVector source, Vector existing, int[] sourcePositions, int outputStart, int size)
    {
        int byteCapacity = binaryCapacity(existing, outputStart);
        for (int sourcePosition : sourcePositions) {
            byteCapacity += source.length(sourcePosition);
        }

        BinaryVector output = allocator.allocateOrGrowBinary(allocationContext, existing instanceof BinaryVector vector ? vector : null, size, byteCapacity);
        if (outputStart == 0) {
            Arrays.fill(output.offsets(), 0);
            output.clearTraits();
            output.addTraits(source.traits());
        }
        else {
            output.addTraits(source.traits());
        }

        int currentOffset = output.offsets()[outputStart];
        for (int index = 0; index < sourcePositions.length; index++) {
            int targetPosition = outputStart + index;
            byte[] bytes = source.copyBytes(sourcePositions[index]);
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

    private ArrayVector copyArrayPositions(ArrayVector source, Vector existing, int[] sourcePositions, int outputStart, int size)
    {
        ArrayVector output = ensureArrayCapacity(existing instanceof ArrayVector vector ? vector : null, size);
        if (outputStart == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputStart];
        int currentOffset = childOutputStart;
        int totalElements = 0;
        for (int index = 0; index < sourcePositions.length; index++) {
            int sourcePosition = sourcePositions[index];
            output.offsets()[outputStart + index] = currentOffset;
            int length = source.length(sourcePosition);
            currentOffset += length;
            totalElements += length;
        }
        output.offsets()[outputStart + sourcePositions.length] = currentOffset;
        output.setElements(copyStreamsPositions(
                existing instanceof ArrayVector vector ? vector.elements() : null,
                source.elements(),
                nestedPositions(source.offsets(), sourcePositions, totalElements),
                childOutputStart,
                currentOffset));
        return output;
    }

    private MapVector copyMapPositions(MapVector source, Vector existing, int[] sourcePositions, int outputStart, int size)
    {
        MapVector output = ensureMapCapacity(existing instanceof MapVector vector ? vector : null, size);
        if (outputStart == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputStart];
        int currentOffset = childOutputStart;
        int totalEntries = 0;
        for (int index = 0; index < sourcePositions.length; index++) {
            int sourcePosition = sourcePositions[index];
            output.offsets()[outputStart + index] = currentOffset;
            int length = source.length(sourcePosition);
            currentOffset += length;
            totalEntries += length;
        }
        output.offsets()[outputStart + sourcePositions.length] = currentOffset;

        int[] entryPositions = nestedPositions(source.offsets(), sourcePositions, totalEntries);
        output.setEntries(
                copyStreamsPositions(
                        existing instanceof MapVector vector ? vector.keys() : null,
                        source.keys(),
                        entryPositions,
                        childOutputStart,
                        currentOffset),
                copyStreamsPositions(
                        existing instanceof MapVector vector ? vector.values() : null,
                        source.values(),
                        entryPositions,
                        childOutputStart,
                        currentOffset));
        return output;
    }

    private StructVector copyStructPositions(StructVector source, Vector existing, int[] sourcePositions, int outputStart, int size)
    {
        StructVector output = ensureStructCapacity(existing instanceof StructVector vector ? vector : null, size);
        if (outputStart == 0) {
            output.clearFields();
        }
        Map<String, Streams> existingFields = existing instanceof StructVector vector ? vector.fields() : Map.of();
        for (Map.Entry<String, Streams> entry : source.fields().entrySet()) {
            output.setField(entry.getKey(), copyStreamsPositions(existingFields.get(entry.getKey()), entry.getValue(), sourcePositions, outputStart, size));
        }
        return output;
    }

    private I64Vector materializeLongs(Vector[] rows)
    {
        I64Vector result = allocator.allocate(allocationContext, I64Vector.class, totalLength(rows), I64Vector::new);
        int outputPosition = 0;
        for (Vector row : rows) {
            for (int position = 0; position < row.length(); position++) {
                result.values()[outputPosition++] = OperatorVectorSupport.longValue(row, position);
            }
        }
        return result;
    }

    private BooleanVector materializeBooleans(Vector[] rows)
    {
        BooleanVector result = allocator.allocate(allocationContext, BooleanVector.class, totalLength(rows), BooleanVector::new);
        int outputPosition = 0;
        for (Vector row : rows) {
            for (int position = 0; position < row.length(); position++) {
                result.values()[outputPosition++] = OperatorVectorSupport.booleanValue(row, position);
            }
        }
        return result;
    }

    private F64Vector materializeDoubles(Vector[] rows)
    {
        F64Vector result = allocator.allocate(allocationContext, F64Vector.class, totalLength(rows), F64Vector::new);
        int outputPosition = 0;
        for (Vector row : rows) {
            for (int position = 0; position < row.length(); position++) {
                result.values()[outputPosition++] = OperatorVectorSupport.doubleValue(row, position);
            }
        }
        return result;
    }

    private BinaryVector materializeBinary(BinaryVector sample, Vector[] rows)
    {
        int totalBytes = 0;
        int totalPositions = 0;
        for (Vector row : rows) {
            totalPositions += row.length();
            for (int position = 0; position < row.length(); position++) {
                totalBytes += OperatorVectorSupport.binaryLength(row, position);
            }
        }

        BinaryVector result = allocator.allocateBinary(allocationContext, totalPositions, totalBytes);
        result.addTraits(sample.traits());
        int outputPosition = 0;
        for (Vector row : rows) {
            for (int position = 0; position < row.length(); position++) {
                int length = OperatorVectorSupport.binaryLength(row, position);
                if (length == 0) {
                    result.setNull(outputPosition++);
                    continue;
                }
                result.setBytes(outputPosition++, OperatorVectorSupport.binaryBytes(row, position));
            }
        }
        return result;
    }

    private ArrayVector materializeArrays(ArrayVector sample, Vector[] rows)
    {
        ArrayVector result = allocator.allocateArray(allocationContext, totalLength(rows));
        int outputPosition = 0;
        int elementCount = 0;
        for (Vector row : rows) {
            ArrayVector arrays = (ArrayVector) row;
            for (int position = 0; position < arrays.length(); position++) {
                result.offsets()[outputPosition++] = elementCount;
                elementCount += arrays.length(position);
            }
        }
        result.offsets()[outputPosition] = elementCount;

        Streams elements = Streams.empty();
        for (Map.Entry<Stream, Vector> entry : sample.elements().asMap().entrySet()) {
            Vector[] childRows = new Vector[rows.length];
            for (int index = 0; index < rows.length; index++) {
                childRows[index] = ((ArrayVector) rows[index]).elements().get(entry.getKey());
            }
            elements = elements.with(entry.getKey(), materializeStream(entry.getValue(), childRows));
        }
        result.setElements(elements);
        return result;
    }

    private MapVector materializeMaps(MapVector sample, Vector[] rows)
    {
        MapVector result = allocator.allocateMap(allocationContext, totalLength(rows));
        int outputPosition = 0;
        int entryCount = 0;
        for (Vector row : rows) {
            MapVector maps = (MapVector) row;
            for (int position = 0; position < maps.length(); position++) {
                result.offsets()[outputPosition++] = entryCount;
                entryCount += maps.length(position);
            }
        }
        result.offsets()[outputPosition] = entryCount;

        Streams keys = Streams.empty();
        for (Map.Entry<Stream, Vector> entry : sample.keys().asMap().entrySet()) {
            Vector[] childRows = new Vector[rows.length];
            for (int index = 0; index < rows.length; index++) {
                childRows[index] = ((MapVector) rows[index]).keys().get(entry.getKey());
            }
            keys = keys.with(entry.getKey(), materializeStream(entry.getValue(), childRows));
        }

        Streams values = Streams.empty();
        for (Map.Entry<Stream, Vector> entry : sample.values().asMap().entrySet()) {
            Vector[] childRows = new Vector[rows.length];
            for (int index = 0; index < rows.length; index++) {
                childRows[index] = ((MapVector) rows[index]).values().get(entry.getKey());
            }
            values = values.with(entry.getKey(), materializeStream(entry.getValue(), childRows));
        }
        result.setEntries(keys, values);
        return result;
    }

    private StructVector materializeStructs(StructVector sample, Vector[] rows)
    {
        StructVector result = allocator.allocate(allocationContext, StructVector.class, totalLength(rows), StructVector::new);
        for (Map.Entry<String, Streams> field : sample.fields().entrySet()) {
            Streams streams = Streams.empty();
            for (Map.Entry<Stream, Vector> entry : field.getValue().asMap().entrySet()) {
                Vector[] childRows = new Vector[rows.length];
                for (int index = 0; index < rows.length; index++) {
                    childRows[index] = ((StructVector) rows[index]).field(field.getKey()).get(entry.getKey());
                }
                streams = streams.with(entry.getKey(), materializeStream(entry.getValue(), childRows));
            }
            result.setField(field.getKey(), streams);
        }
        return result;
    }

    private Vector emptyVector(Vector source)
    {
        return switch (source) {
            case I64Vector _ -> allocator.allocate(allocationContext, I64Vector.class, 0, I64Vector::new);
            case BooleanVector _ -> allocator.allocate(allocationContext, BooleanVector.class, 0, BooleanVector::new);
            case F64Vector _ -> allocator.allocate(allocationContext, F64Vector.class, 0, F64Vector::new);
            case BinaryVector binary -> {
                BinaryVector empty = allocator.allocateBinary(allocationContext, 0, 0);
                empty.addTraits(binary.traits());
                yield empty;
            }
            case ArrayVector values -> {
                ArrayVector empty = allocator.allocateArray(allocationContext, 0);
                empty.setElements(emptyLike(values.elements()));
                yield empty;
            }
            case MapVector values -> {
                MapVector empty = allocator.allocateMap(allocationContext, 0);
                empty.setEntries(emptyLike(values.keys()), emptyLike(values.values()));
                yield empty;
            }
            case StructVector values -> {
                StructVector empty = allocator.allocate(allocationContext, StructVector.class, 0, StructVector::new);
                for (Map.Entry<String, Streams> entry : values.fields().entrySet()) {
                    empty.setField(entry.getKey(), emptyLike(entry.getValue()));
                }
                yield empty;
            }
            case DictionaryVector values -> emptyVector(values.values());
            case RleVector values -> emptyVector(values.values());
            default -> throw new IllegalArgumentException("Unsupported nested-loop vector type: " + source.getClass().getSimpleName());
        };
    }

    private static int[] positions(Mask mask, int maskStart, int copied)
    {
        int[] positions = new int[copied];
        if (mask.all()) {
            for (int index = 0; index < copied; index++) {
                positions[index] = maskStart + index;
            }
            return positions;
        }
        for (int index = 0; index < copied; index++) {
            positions[index] = mask.position(maskStart + index);
        }
        return positions;
    }

    private static int[] repeatedPositions(int position, int length)
    {
        int[] positions = new int[length];
        Arrays.fill(positions, position);
        return positions;
    }

    private static int[] nestedPositions(int[] offsets, int[] sourcePositions, int totalNestedPositions)
    {
        int[] positions = new int[totalNestedPositions];
        int outputIndex = 0;
        for (int sourcePosition : sourcePositions) {
            for (int nested = offsets[sourcePosition]; nested < offsets[sourcePosition + 1]; nested++) {
                positions[outputIndex++] = nested;
            }
        }
        return positions;
    }

    private static int[] dictionaryPositions(int[] ids, int[] sourcePositions)
    {
        int[] positions = new int[sourcePositions.length];
        for (int index = 0; index < sourcePositions.length; index++) {
            positions[index] = ids[sourcePositions[index]];
        }
        return positions;
    }

    private static int[] rlePositions(RleVector values, int[] sourcePositions)
    {
        int[] positions = new int[sourcePositions.length];
        for (int index = 0; index < sourcePositions.length; index++) {
            positions[index] = OperatorVectorSupport.runIndex(values, sourcePositions[index]);
        }
        return positions;
    }

    private static int totalLength(Vector[] rows)
    {
        int totalLength = 0;
        for (Vector row : rows) {
            totalLength += row.length();
        }
        return totalLength;
    }

    private I64Vector ensureLongCapacity(I64Vector existing, int size)
    {
        if (existing == null) {
            return allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new);
        }
        if (existing.length() >= size) {
            return existing;
        }

        I64Vector grown = allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new);
        System.arraycopy(existing.values(), 0, grown.values(), 0, existing.length());
        return grown;
    }

    private BooleanVector ensureBooleanCapacity(BooleanVector existing, int size)
    {
        if (existing == null) {
            return allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
        }
        if (existing.length() >= size) {
            return existing;
        }

        BooleanVector grown = allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
        System.arraycopy(existing.values(), 0, grown.values(), 0, existing.length());
        return grown;
    }

    private F64Vector ensureDoubleCapacity(F64Vector existing, int size)
    {
        if (existing == null) {
            return allocator.allocate(allocationContext, F64Vector.class, size, F64Vector::new);
        }
        if (existing.length() >= size) {
            return existing;
        }

        F64Vector grown = allocator.allocate(allocationContext, F64Vector.class, size, F64Vector::new);
        System.arraycopy(existing.values(), 0, grown.values(), 0, existing.length());
        return grown;
    }

    private ArrayVector ensureArrayCapacity(ArrayVector existing, int size)
    {
        if (existing == null) {
            return allocator.allocateArray(allocationContext, size);
        }
        if (existing.length() >= size) {
            return existing;
        }

        ArrayVector grown = allocator.allocateArray(allocationContext, size);
        System.arraycopy(existing.offsets(), 0, grown.offsets(), 0, existing.offsets().length);
        grown.setElements(allocator.copyStreams(allocationContext, existing.elements()));
        return grown;
    }

    private MapVector ensureMapCapacity(MapVector existing, int size)
    {
        if (existing == null) {
            return allocator.allocateMap(allocationContext, size);
        }
        if (existing.length() >= size) {
            return existing;
        }

        MapVector grown = allocator.allocateMap(allocationContext, size);
        System.arraycopy(existing.offsets(), 0, grown.offsets(), 0, existing.offsets().length);
        grown.setEntries(
                allocator.copyStreams(allocationContext, existing.keys()),
                allocator.copyStreams(allocationContext, existing.values()));
        return grown;
    }

    private StructVector ensureStructCapacity(StructVector existing, int size)
    {
        if (existing == null) {
            return allocator.allocate(allocationContext, StructVector.class, size, StructVector::new);
        }
        if (existing.length() >= size) {
            return existing;
        }

        StructVector grown = allocator.allocate(allocationContext, StructVector.class, size, StructVector::new);
        for (Map.Entry<String, Streams> entry : existing.fields().entrySet()) {
            grown.setField(entry.getKey(), allocator.copyStreams(allocationContext, entry.getValue()));
        }
        return grown;
    }

    private static int binaryCapacity(Vector existing, int positionCount)
    {
        if (existing instanceof BinaryVector values) {
            return values.offsets()[positionCount];
        }
        return 0;
    }
}
