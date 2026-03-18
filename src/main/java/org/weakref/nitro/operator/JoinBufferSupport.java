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
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;

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
        Streams.Builder streams = Streams.builder();
        for (Stream stream : output.streams()) {
            streams.put(stream, output.borrow(stream));
        }
        return streams.build();
    }

    public Streams replicate(Streams existing, Streams input, int size, int start, int length, int position)
    {
        if (length == 1) {
            return copySinglePosition(existing, input, size, start, position);
        }
        return copyStreamsPositions(existing, input, repeatedPositions(position, length), start, size);
    }

    public Streams copyAndCompact(Output input, Mask mask, int maskStart, Streams existing, int outputStart, int copied, int size)
    {
        Streams.Builder result = Streams.builder();
        int[] positions = positions(mask, maskStart, copied);
        for (Stream stream : input.streams()) {
            Vector existingVector = existing != null ? existing.getOrNull(stream) : null;
            result.put(stream, copyVectorPositions(existingVector, input.borrow(stream), positions, outputStart, size));
        }
        return result.build();
    }

    public Streams copyPosition(Output input, Streams existing, int position)
    {
        return copySinglePosition(input, existing, 1, 0, position);
    }

    public Streams copyPositions(Output input, Streams existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        if (sourceCount == 1) {
            return copySinglePosition(input, existing, size, outputStart, sourcePositions[0]);
        }

        Streams.Builder result = Streams.builder();
        for (Stream stream : input.streams()) {
            Vector existingVector = existing != null ? existing.getOrNull(stream) : null;
            result.put(stream, copyVectorPositions(existingVector, input.borrow(stream), sourcePositions, sourceCount, outputStart, size));
        }
        return result.build();
    }

    public Streams copyPositions(Streams existing, Streams input, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        if (sourceCount == 1) {
            return copySinglePosition(existing, input, size, outputStart, sourcePositions[0]);
        }

        Streams.Builder result = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : input.asMap().entrySet()) {
            Vector existingVector = existing != null ? existing.getOrNull(entry.getKey()) : null;
            result.put(entry.getKey(), copyVectorPositions(existingVector, entry.getValue(), sourcePositions, sourceCount, outputStart, size));
        }
        return result.build();
    }

    public Streams copySinglePosition(Output input, Streams existing, int size, int outputPosition, int sourcePosition)
    {
        if (isValuesOnly(input)) {
            Vector existingValues = existing != null ? existing.values() : null;
            Vector copied = copyVectorSinglePosition(existingValues, input.borrow(Stream.VALUES), sourcePosition, outputPosition, size);
            if (existing != null && copied == existingValues) {
                return existing;
            }
            return Streams.ofValues(copied);
        }

        if (existing != null) {
            Streams.Builder updated = null;
            boolean changed = false;
            for (Stream stream : input.streams()) {
                Vector existingVector = existing.getOrNull(stream);
                Vector copied = copyVectorSinglePosition(existingVector, input.borrow(stream), sourcePosition, outputPosition, size);
                if (copied != existingVector) {
                    if (updated == null) {
                        updated = Streams.builder().putAll(existing);
                    }
                    updated.put(stream, copied);
                    changed = true;
                }
            }
            return changed ? updated.build() : existing;
        }

        Streams.Builder result = Streams.builder();
        for (Stream stream : input.streams()) {
            result.put(stream, copyVectorSinglePosition(null, input.borrow(stream), sourcePosition, outputPosition, size));
        }
        return result.build();
    }

    public Streams copySinglePosition(Streams existing, Streams input, int size, int outputPosition, int sourcePosition)
    {
        if (isValuesOnly(input)) {
            Vector existingValues = existing != null ? existing.values() : null;
            Vector copied = copyVectorSinglePosition(existingValues, input.values(), sourcePosition, outputPosition, size);
            if (existing != null && copied == existingValues) {
                return existing;
            }
            return Streams.ofValues(copied);
        }

        if (existing != null) {
            Streams updated = existing;
            boolean changed = false;
            for (Map.Entry<Stream, Vector> entry : input.asMap().entrySet()) {
                Vector existingVector = updated.getOrNull(entry.getKey());
                Vector copied = copyVectorSinglePosition(existingVector, entry.getValue(), sourcePosition, outputPosition, size);
                if (copied != existingVector) {
                    updated = updated.with(entry.getKey(), copied);
                    changed = true;
                }
            }
            return changed ? updated : existing;
        }

        Streams result = Streams.empty();
        for (Map.Entry<Stream, Vector> entry : input.asMap().entrySet()) {
            result = result.with(entry.getKey(), copyVectorSinglePosition(null, entry.getValue(), sourcePosition, outputPosition, size));
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

    public Streams materializeColumn(Streams columnSchema, Streams[][] rowSlots, List<Integer> orderedSlots, int outputIndex)
    {
        Streams.Builder result = Streams.builder();
        for (Stream stream : columnSchema.asMap().keySet()) {
            Vector sample = columnSchema.get(stream);
            Vector materialized = switch (OperatorVectorSupport.flatten(sample)) {
                case I64Vector _ -> materializeLongs(rowSlots, orderedSlots, outputIndex, stream);
                case BooleanVector _ -> materializeBooleans(rowSlots, orderedSlots, outputIndex, stream);
                case F64Vector _ -> materializeDoubles(rowSlots, orderedSlots, outputIndex, stream);
                case BinaryVector binary -> materializeBinary(binary, rowSlots, orderedSlots, outputIndex, stream);
                default -> {
                    Vector[] rows = new Vector[orderedSlots.size()];
                    for (int rowIndex = 0; rowIndex < orderedSlots.size(); rowIndex++) {
                        rows[rowIndex] = rowSlots[orderedSlots.get(rowIndex)][outputIndex].get(stream);
                    }
                    yield materializeStream(sample, rows);
                }
            };
            result.put(stream, materialized);
        }
        return result.build();
    }

    private Streams copyStreamsPositions(Streams existing, Streams source, int[] sourcePositions, int outputStart, int size)
    {
        return copyPositions(existing, source, sourcePositions, sourcePositions.length, outputStart, size);
    }

    private Vector copyVectorPositions(Vector existing, Vector source, int[] sourcePositions, int outputStart, int size)
    {
        return copyVectorPositions(existing, source, sourcePositions, sourcePositions.length, outputStart, size);
    }

    private Vector copyVectorPositions(Vector existing, Vector source, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        return switch (source) {
            case I64Vector values -> copyLongPositions(values, existing, sourcePositions, sourceCount, outputStart, size);
            case BooleanVector values -> copyBooleanPositions(values, existing, sourcePositions, sourceCount, outputStart, size);
            case F64Vector values -> copyDoublePositions(values, existing, sourcePositions, sourceCount, outputStart, size);
            case BinaryVector values -> copyBinaryPositions(values, existing, sourcePositions, sourceCount, outputStart, size);
            case ArrayVector values -> copyArrayPositions(values, existing, sourcePositions, sourceCount, outputStart, size);
            case MapVector values -> copyMapPositions(values, existing, sourcePositions, sourceCount, outputStart, size);
            case StructVector values -> copyStructPositions(values, existing, sourcePositions, sourceCount, outputStart, size);
            case DictionaryVector values -> copyVectorPositions(existing, values.values(), dictionaryPositions(values.ids(), sourcePositions, sourceCount), outputStart, size);
            case RleVector values -> copyVectorPositions(existing, values.values(), rlePositions(values, sourcePositions, sourceCount), outputStart, size);
            default -> throw new IllegalArgumentException("Unsupported join vector type: " + source.getClass().getSimpleName());
        };
    }

    private Vector copyVectorSinglePosition(Vector existing, Vector source, int sourcePosition, int outputPosition, int size)
    {
        return switch (source) {
            case I64Vector values -> copyLongSinglePosition(values, existing, sourcePosition, outputPosition, size);
            case BooleanVector values -> copyBooleanSinglePosition(values, existing, sourcePosition, outputPosition, size);
            case F64Vector values -> copyDoubleSinglePosition(values, existing, sourcePosition, outputPosition, size);
            case BinaryVector values -> copyBinarySinglePosition(values, existing, sourcePosition, outputPosition, size);
            case ArrayVector values -> copyArraySinglePosition(values, existing, sourcePosition, outputPosition, size);
            case MapVector values -> copyMapSinglePosition(values, existing, sourcePosition, outputPosition, size);
            case StructVector values -> copyStructSinglePosition(values, existing, sourcePosition, outputPosition, size);
            case DictionaryVector values -> copyVectorSinglePosition(existing, values.values(), values.ids()[sourcePosition], outputPosition, size);
            case RleVector values -> copyVectorSinglePosition(existing, values.values(), rlePosition(values, sourcePosition), outputPosition, size);
            default -> throw new IllegalArgumentException("Unsupported join vector type: " + source.getClass().getSimpleName());
        };
    }

    private I64Vector copyLongPositions(I64Vector source, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        I64Vector output = ensureLongCapacity(existing instanceof I64Vector vector ? vector : null, size);
        for (int index = 0; index < sourceCount; index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions[index]];
        }
        return output;
    }

    private I64Vector copyLongSinglePosition(I64Vector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        I64Vector output = ensureLongCapacity(existing instanceof I64Vector vector ? vector : null, size);
        output.values()[outputPosition] = source.values()[sourcePosition];
        return output;
    }

    private BooleanVector copyBooleanPositions(BooleanVector source, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        BooleanVector output = ensureBooleanCapacity(existing instanceof BooleanVector vector ? vector : null, size);
        for (int index = 0; index < sourceCount; index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions[index]];
        }
        return output;
    }

    private BooleanVector copyBooleanSinglePosition(BooleanVector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        BooleanVector output = ensureBooleanCapacity(existing instanceof BooleanVector vector ? vector : null, size);
        output.values()[outputPosition] = source.values()[sourcePosition];
        return output;
    }

    private F64Vector copyDoublePositions(F64Vector source, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        F64Vector output = ensureDoubleCapacity(existing instanceof F64Vector vector ? vector : null, size);
        for (int index = 0; index < sourceCount; index++) {
            output.values()[outputStart + index] = source.values()[sourcePositions[index]];
        }
        return output;
    }

    private F64Vector copyDoubleSinglePosition(F64Vector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        F64Vector output = ensureDoubleCapacity(existing instanceof F64Vector vector ? vector : null, size);
        output.values()[outputPosition] = source.values()[sourcePosition];
        return output;
    }

    private BinaryVector copyBinaryPositions(BinaryVector source, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        int byteCapacity = binaryCapacity(existing, outputStart);
        for (int index = 0; index < sourceCount; index++) {
            byteCapacity += source.length(sourcePositions[index]);
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
        for (int index = 0; index < sourceCount; index++) {
            int targetPosition = outputStart + index;
            output.offsets()[targetPosition] = currentOffset;
            int sourcePosition = sourcePositions[index];
            int length = source.length(sourcePosition);
            if (length == 0) {
                output.setNull(targetPosition);
            }
            else {
                output.setBytes(targetPosition, source.data(), source.startOffset(sourcePosition), length);
                currentOffset = output.endOffset(targetPosition);
            }
        }
        return output;
    }

    private BinaryVector copyBinarySinglePosition(BinaryVector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        int requiredCapacity = binaryCapacity(existing, outputPosition) + source.length(sourcePosition);
        int requestedCapacity = requiredCapacity;
        if (existing == null && outputPosition == 0 && size > 1) {
            requestedCapacity = Math.max(requiredCapacity, estimatedBinaryCapacity(source, size));
        }
        BinaryVector output = allocator.allocateOrGrowBinary(allocationContext, existing instanceof BinaryVector vector ? vector : null, size, requestedCapacity);
        if (outputPosition == 0) {
            Arrays.fill(output.offsets(), 0);
            output.clearTraits();
            output.addTraits(source.traits());
        }
        else {
            output.addTraits(source.traits());
        }

        int currentOffset = output.offsets()[outputPosition];
        output.offsets()[outputPosition] = currentOffset;
        int length = source.length(sourcePosition);
        if (length == 0) {
            output.setNull(outputPosition);
            return output;
        }

        output.setBytes(outputPosition, source.data(), source.startOffset(sourcePosition), length);
        return output;
    }

    private ArrayVector copyArrayPositions(ArrayVector source, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        ArrayVector output = ensureArrayCapacity(existing instanceof ArrayVector vector ? vector : null, size);
        if (outputStart == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputStart];
        int currentOffset = childOutputStart;
        int totalElements = 0;
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = sourcePositions[index];
            output.offsets()[outputStart + index] = currentOffset;
            int length = source.length(sourcePosition);
            currentOffset += length;
            totalElements += length;
        }
        output.offsets()[outputStart + sourceCount] = currentOffset;
        output.setElements(copyStreamsPositions(
                existing instanceof ArrayVector vector ? vector.elements() : null,
                source.elements(),
                nestedPositions(source.offsets(), sourcePositions, sourceCount, totalElements),
                childOutputStart,
                currentOffset));
        return output;
    }

    private ArrayVector copyArraySinglePosition(ArrayVector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        ArrayVector output = ensureArrayCapacity(existing instanceof ArrayVector vector ? vector : null, size);
        if (outputPosition == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputPosition];
        int length = source.length(sourcePosition);
        output.offsets()[outputPosition] = childOutputStart;
        output.offsets()[outputPosition + 1] = childOutputStart + length;
        output.setElements(copyStreamsPositions(
                existing instanceof ArrayVector vector ? vector.elements() : null,
                source.elements(),
                nestedPositions(source.offsets(), new int[] {sourcePosition}, 1, length),
                childOutputStart,
                childOutputStart + length));
        return output;
    }

    private MapVector copyMapPositions(MapVector source, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        MapVector output = ensureMapCapacity(existing instanceof MapVector vector ? vector : null, size);
        if (outputStart == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputStart];
        int currentOffset = childOutputStart;
        int totalEntries = 0;
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = sourcePositions[index];
            output.offsets()[outputStart + index] = currentOffset;
            int length = source.length(sourcePosition);
            currentOffset += length;
            totalEntries += length;
        }
        output.offsets()[outputStart + sourceCount] = currentOffset;

        int[] entryPositions = nestedPositions(source.offsets(), sourcePositions, sourceCount, totalEntries);
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

    private MapVector copyMapSinglePosition(MapVector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        MapVector output = ensureMapCapacity(existing instanceof MapVector vector ? vector : null, size);
        if (outputPosition == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputPosition];
        int length = source.length(sourcePosition);
        output.offsets()[outputPosition] = childOutputStart;
        output.offsets()[outputPosition + 1] = childOutputStart + length;

        int[] entryPositions = nestedPositions(source.offsets(), new int[] {sourcePosition}, 1, length);
        output.setEntries(
                copyStreamsPositions(
                        existing instanceof MapVector vector ? vector.keys() : null,
                        source.keys(),
                        entryPositions,
                        childOutputStart,
                        childOutputStart + length),
                copyStreamsPositions(
                        existing instanceof MapVector vector ? vector.values() : null,
                        source.values(),
                        entryPositions,
                        childOutputStart,
                        childOutputStart + length));
        return output;
    }

    private StructVector copyStructPositions(StructVector source, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        StructVector output = ensureStructCapacity(existing instanceof StructVector vector ? vector : null, size);
        if (outputStart == 0) {
            output.clearFields();
        }
        Map<String, Streams> existingFields = existing instanceof StructVector vector ? vector.fields() : Map.of();
        for (Map.Entry<String, Streams> entry : source.fields().entrySet()) {
            output.setField(entry.getKey(), copyPositions(existingFields.get(entry.getKey()), entry.getValue(), sourcePositions, sourceCount, outputStart, size));
        }
        return output;
    }

    private StructVector copyStructSinglePosition(StructVector source, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        StructVector output = ensureStructCapacity(existing instanceof StructVector vector ? vector : null, size);
        if (outputPosition == 0) {
            output.clearFields();
        }
        Map<String, Streams> existingFields = existing instanceof StructVector vector ? vector.fields() : Map.of();
        for (Map.Entry<String, Streams> entry : source.fields().entrySet()) {
            output.setField(entry.getKey(), copySinglePosition(existingFields.get(entry.getKey()), entry.getValue(), size, outputPosition, sourcePosition));
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

    private I64Vector materializeLongs(Streams[][] rowSlots, List<Integer> orderedSlots, int outputIndex, Stream stream)
    {
        I64Vector result = allocator.allocate(allocationContext, I64Vector.class, orderedSlots.size(), I64Vector::new);
        for (int rowIndex = 0; rowIndex < orderedSlots.size(); rowIndex++) {
            Vector row = rowSlots[orderedSlots.get(rowIndex)][outputIndex].get(stream);
            result.values()[rowIndex] = OperatorVectorSupport.longValue(row, 0);
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

    private BooleanVector materializeBooleans(Streams[][] rowSlots, List<Integer> orderedSlots, int outputIndex, Stream stream)
    {
        BooleanVector result = allocator.allocate(allocationContext, BooleanVector.class, orderedSlots.size(), BooleanVector::new);
        for (int rowIndex = 0; rowIndex < orderedSlots.size(); rowIndex++) {
            Vector row = rowSlots[orderedSlots.get(rowIndex)][outputIndex].get(stream);
            result.values()[rowIndex] = OperatorVectorSupport.booleanValue(row, 0);
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

    private F64Vector materializeDoubles(Streams[][] rowSlots, List<Integer> orderedSlots, int outputIndex, Stream stream)
    {
        F64Vector result = allocator.allocate(allocationContext, F64Vector.class, orderedSlots.size(), F64Vector::new);
        for (int rowIndex = 0; rowIndex < orderedSlots.size(); rowIndex++) {
            Vector row = rowSlots[orderedSlots.get(rowIndex)][outputIndex].get(stream);
            result.values()[rowIndex] = OperatorVectorSupport.doubleValue(row, 0);
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
                copyBinaryValue(result, outputPosition++, row, position);
            }
        }
        return result;
    }

    private BinaryVector materializeBinary(BinaryVector sample, Streams[][] rowSlots, List<Integer> orderedSlots, int outputIndex, Stream stream)
    {
        int totalBytes = 0;
        for (int rowIndex = 0; rowIndex < orderedSlots.size(); rowIndex++) {
            Vector row = rowSlots[orderedSlots.get(rowIndex)][outputIndex].get(stream);
            totalBytes += OperatorVectorSupport.binaryLength(row, 0);
        }

        BinaryVector result = allocator.allocateBinary(allocationContext, orderedSlots.size(), totalBytes);
        result.addTraits(sample.traits());
        for (int rowIndex = 0; rowIndex < orderedSlots.size(); rowIndex++) {
            Vector row = rowSlots[orderedSlots.get(rowIndex)][outputIndex].get(stream);
            copyBinaryValue(result, rowIndex, row, 0);
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

    private static void copyBinaryValue(BinaryVector target, int targetPosition, Vector source, int sourcePosition)
    {
        switch (source) {
            case BinaryVector values -> {
                int length = values.length(sourcePosition);
                if (length == 0) {
                    target.setNull(targetPosition);
                }
                else {
                    target.setBytes(targetPosition, values.data(), values.startOffset(sourcePosition), length);
                }
            }
            case DictionaryVector values -> copyBinaryValue(target, targetPosition, values.values(), values.ids()[sourcePosition]);
            case RleVector values -> copyBinaryValue(target, targetPosition, values.values(), OperatorVectorSupport.runIndex(values, sourcePosition));
            default -> throw new IllegalArgumentException("Expected binary vector but found " + source.getClass().getSimpleName());
        }
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

    private static int[] nestedPositions(int[] offsets, int[] sourcePositions, int sourceCount, int totalNestedPositions)
    {
        int[] positions = new int[totalNestedPositions];
        int outputIndex = 0;
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = sourcePositions[index];
            for (int nested = offsets[sourcePosition]; nested < offsets[sourcePosition + 1]; nested++) {
                positions[outputIndex++] = nested;
            }
        }
        return positions;
    }

    private static int[] dictionaryPositions(int[] ids, int[] sourcePositions, int sourceCount)
    {
        int[] positions = new int[sourceCount];
        for (int index = 0; index < sourceCount; index++) {
            positions[index] = ids[sourcePositions[index]];
        }
        return positions;
    }

    private static int[] rlePositions(RleVector values, int[] sourcePositions, int sourceCount)
    {
        int[] positions = new int[sourceCount];
        for (int index = 0; index < sourceCount; index++) {
            positions[index] = rlePosition(values, sourcePositions[index]);
        }
        return positions;
    }

    private static int rlePosition(RleVector values, int sourcePosition)
    {
        return OperatorVectorSupport.runIndex(values, sourcePosition);
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

    private static int estimatedBinaryCapacity(BinaryVector source, int positionCount)
    {
        if (source.length() == 0 || positionCount <= 0) {
            return 0;
        }

        int usedBytes = source.offsets()[source.length()];
        long estimated = Math.max(1L, (usedBytes + (long) source.length() - 1) / source.length()) * positionCount;
        return toIntExact(Math.min(Integer.MAX_VALUE, estimated));
    }

    private static boolean isValuesOnly(Output output)
    {
        return output.streams().size() == 1 && output.streams().contains(Stream.VALUES);
    }

    private static boolean isValuesOnly(Streams streams)
    {
        return streams.asMap().size() == 1 && streams.has(Stream.VALUES);
    }
}
