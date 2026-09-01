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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

public final class StructVector
        implements FlatVector
{
    private final int positionCount;
    private final LinkedHashMap<String, Streams> fields = new LinkedHashMap<>();
    private final Map<String, Streams> fieldsView = unmodifiableMap(fields);
    private final Set<String> fieldNamesView = unmodifiableSet(fields.keySet());

    public StructVector(int positionCount)
    {
        this.positionCount = positionCount;
    }

    public void setField(String name, Streams streams)
    {
        fields.put(requireNonNull(name, "name is null"), requireNonNull(streams, "streams is null"));
    }

    public Streams field(String name)
    {
        Streams streams = fields.get(requireNonNull(name, "name is null"));
        if (streams == null) {
            throw new IllegalArgumentException("Unknown struct field: " + name);
        }
        return streams;
    }

    public Streams field(int index)
    {
        checkIndex(index, fields.size());
        for (Streams streams : fields.values()) {
            if (index == 0) {
                return streams;
            }
            index--;
        }
        throw new AssertionError();
    }

    public Vector fieldValues(String name)
    {
        return field(name).values();
    }

    public Vector fieldStreamOrNull(String name, Stream stream)
    {
        return field(name).getOrNull(stream);
    }

    public Set<String> fieldNames()
    {
        return fieldNamesView;
    }

    /** Returns an unmodifiable insertion-ordered view of the semantic struct fields. */
    public Map<String, Streams> fields()
    {
        return fieldsView;
    }

    public void clearFields()
    {
        fields.clear();
    }

    @Override
    public int length()
    {
        return positionCount;
    }

    @Override
    public long retainedBytes()
    {
        return 0;
    }

    @Override
    public boolean requiresMonotonicOutputWrites()
    {
        return fields.values().stream()
                .flatMap(streams -> streams.asMap().values().stream())
                .anyMatch(Vector::requiresMonotonicOutputWrites);
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        StructVector copy = allocator.allocate(allocationContext, StructVector.class, positionCount, StructVector::new);
        for (Map.Entry<String, Streams> entry : fields.entrySet()) {
            copy.setField(entry.getKey(), allocator.copyStreams(allocationContext, entry.getValue()));
        }
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        StructVector copy = allocator.allocate(allocationContext, StructVector.class, positions.length, StructVector::new);
        for (Map.Entry<String, Streams> entry : fields.entrySet()) {
            copy.setField(entry.getKey(), allocator.copyStreams(allocationContext, entry.getValue(), positions));
        }
        return copy;
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        Map<String, Streams> existingFields = existing instanceof StructVector vector ? vector.fields() : Map.of();
        StructVector output = allocator.reallocateIfNecessary(
                allocationContext,
                existing instanceof StructVector vector ? vector : null,
                StructVector.class,
                length(),
                StructVector::new);
        for (Map.Entry<String, Streams> entry : fields.entrySet()) {
            output.setField(
                    entry.getKey(),
                    copyMaskedStreams(entry.getValue(), existingFields.get(entry.getKey()), allocator, allocationContext, mask));
        }
        return output;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        StructVector output = allocator.reallocateIfNecessary(allocationContext, existing instanceof StructVector vector ? vector : null, StructVector.class, size, StructVector::new);
        Map<String, Streams> existingFields = existing instanceof StructVector vector ? vector.fields() : Map.of();
        if (outputStart == 0 && !hasSameFieldLayout(output, this)) {
            if (output == existing && !existingFields.isEmpty()) {
                existingFields = new LinkedHashMap<>(existingFields);
            }
            output.clearFields();
        }
        for (Map.Entry<String, Streams> entry : fields.entrySet()) {
            output.setField(entry.getKey(), copyStreams(entry.getValue(), existingFields.get(entry.getKey()), allocator, allocationContext, sourcePositions, sourceCount, outputStart, size));
        }
        return output;
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        StructVector output = allocator.reallocateIfNecessary(allocationContext, existing instanceof StructVector vector ? vector : null, StructVector.class, size, StructVector::new);
        Map<String, Streams> existingFields = existing instanceof StructVector vector ? vector.fields() : Map.of();
        if (outputPosition == 0 && !hasSameFieldLayout(output, this)) {
            if (output == existing && !existingFields.isEmpty()) {
                existingFields = new LinkedHashMap<>(existingFields);
            }
            output.clearFields();
        }
        for (Map.Entry<String, Streams> entry : fields.entrySet()) {
            output.setField(entry.getKey(), copySingleStreams(entry.getValue(), existingFields.get(entry.getKey()), allocator, allocationContext, sourcePosition, outputPosition, size));
        }
        return output;
    }

    @Override
    public Vector copySinglePositionRangeInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputStart, int outputEnd, int size)
    {
        StructVector output = allocator.reallocateIfNecessary(allocationContext, existing instanceof StructVector vector ? vector : null, StructVector.class, size, StructVector::new);
        Map<String, Streams> existingFields = existing instanceof StructVector vector ? vector.fields() : Map.of();
        if (outputStart == 0 && !hasSameFieldLayout(output, this)) {
            if (output == existing && !existingFields.isEmpty()) {
                existingFields = new LinkedHashMap<>(existingFields);
            }
            output.clearFields();
        }
        for (Map.Entry<String, Streams> entry : fields.entrySet()) {
            output.setField(entry.getKey(), copySingleRangeStreams(entry.getValue(), existingFields.get(entry.getKey()), allocator, allocationContext, sourcePosition, outputStart, outputEnd, size));
        }
        return output;
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        StructVector empty = allocator.allocate(allocationContext, StructVector.class, 0, StructVector::new);
        for (Map.Entry<String, Streams> entry : fields.entrySet()) {
            Streams.Builder streams = Streams.builder();
            for (Map.Entry<Stream, Vector> child : entry.getValue().asMap().entrySet()) {
                streams.put(child.getKey(), child.getValue().emptyLike(allocator, allocationContext));
            }
            empty.setField(entry.getKey(), streams.build());
        }
        return empty;
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        Vector[] normalizedRows = VectorSupport.normalizeRows(
                allocator, allocationContext, rows, StructVector.class);
        if (normalizedRows != null) {
            try {
                return materializeStructRows(allocator, allocationContext, normalizedRows);
            }
            finally {
                VectorSupport.releaseNormalizedRows(allocator, allocationContext, rows, normalizedRows);
            }
        }

        StructVector result = allocator.allocate(allocationContext, StructVector.class, VectorSupport.totalLength(rows), StructVector::new);
        int outputStart = 0;
        for (Vector row : rows) {
            int rowLength = row.length();
            if (rowLength == 1) {
                result = (StructVector) row.copySinglePositionInto(allocator, allocationContext, result, 0, outputStart, result.length());
            }
            else if (rowLength > 1) {
                result = (StructVector) row.copyPositionsInto(allocator, allocationContext, result, VectorSupport.densePositions(rowLength), rowLength, outputStart, result.length());
            }
            outputStart += rowLength;
        }
        return result;
    }

    private static StructVector materializeStructRows(
            Allocator allocator,
            Allocator.Context allocationContext,
            Vector[] rows)
    {
        StructVector result = allocator.allocate(
                allocationContext,
                StructVector.class,
                VectorSupport.totalLength(rows),
                StructVector::new);
        if (rows.length == 0) {
            return result;
        }

        StructVector first = (StructVector) rows[0];
        for (Map.Entry<String, Streams> field : first.fields().entrySet()) {
            Streams.Builder fieldStreams = Streams.builder();
            for (Map.Entry<Stream, Vector> stream : field.getValue().asMap().entrySet()) {
                Vector[] segments = new Vector[rows.length];
                for (int index = 0; index < rows.length; index++) {
                    StructVector struct = (StructVector) rows[index];
                    if (struct.fields().size() != first.fields().size()) {
                        throw new IllegalArgumentException("Struct segments have different field layouts");
                    }
                    Streams streams = struct.fields().get(field.getKey());
                    Vector segment = streams == null ? null : streams.getOrNull(stream.getKey());
                    if (segment == null) {
                        throw new IllegalArgumentException("Struct segments have different stream shapes");
                    }
                    segments[index] = segment;
                }
                fieldStreams.put(
                        stream.getKey(),
                        segments[0].materializeRows(allocator, allocationContext, segments));
            }
            result.setField(field.getKey(), fieldStreams.build());
        }
        return result;
    }

    @Override
    public void copyInto(Vector target)
    {
        StructVector structTarget = (StructVector) target;
        for (Map.Entry<String, Streams> entry : fields.entrySet()) {
            structTarget.setField(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clearForReuse()
    {
        clearFields();
    }

    @Override
    public Object poolFamily()
    {
        return StructVector.class;
    }

    @Override
    public int poolCapacity()
    {
        return length();
    }

    @Override
    public int poolMaxRetained()
    {
        return 16;
    }

    @Override
    public int childVectorCount()
    {
        int count = 0;
        for (Streams streams : fields.values()) {
            count += streams.vectorCount();
        }
        return count;
    }

    @Override
    public Vector childVector(int index)
    {
        if (index < 0) {
            throw new IndexOutOfBoundsException(index);
        }
        for (Streams streams : fields.values()) {
            int count = streams.vectorCount();
            if (index < count) {
                return streams.vectorAt(index);
            }
            index -= count;
        }
        throw new IndexOutOfBoundsException(index);
    }

    @Override
    public void forEachChildVector(Consumer<Vector> consumer)
    {
        fields.values().forEach(streams -> streams.asMap().values().forEach(consumer));
    }

    private static boolean hasSameFieldLayout(StructVector left, StructVector right)
    {
        if (left.fields.size() != right.fields.size()) {
            return false;
        }
        var leftNames = left.fields.keySet().iterator();
        var rightNames = right.fields.keySet().iterator();
        while (leftNames.hasNext()) {
            if (!leftNames.next().equals(rightNames.next())) {
                return false;
            }
        }
        return true;
    }

    private static Streams copyStreams(Streams source, Streams existing, Allocator allocator, Allocator.Context allocationContext, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        Streams.Builder result = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : source.asMap().entrySet()) {
            Vector existingVector = existing != null ? existing.getOrNull(entry.getKey()) : null;
            result.put(entry.getKey(), entry.getValue().copyPositionsInto(allocator, allocationContext, existingVector, sourcePositions, sourceCount, outputStart, size));
        }
        return result.build();
    }

    private static Streams copySingleStreams(Streams source, Streams existing, Allocator allocator, Allocator.Context allocationContext, int sourcePosition, int outputPosition, int size)
    {
        Streams.Builder result = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : source.asMap().entrySet()) {
            Vector existingVector = existing != null ? existing.getOrNull(entry.getKey()) : null;
            result.put(entry.getKey(), entry.getValue().copySinglePositionInto(allocator, allocationContext, existingVector, sourcePosition, outputPosition, size));
        }
        return result.build();
    }

    private static Streams copySingleRangeStreams(Streams source, Streams existing, Allocator allocator, Allocator.Context allocationContext, int sourcePosition, int outputStart, int outputEnd, int size)
    {
        Streams.Builder result = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : source.asMap().entrySet()) {
            Vector existingVector = existing != null ? existing.getOrNull(entry.getKey()) : null;
            result.put(entry.getKey(), entry.getValue().copySinglePositionRangeInto(allocator, allocationContext, existingVector, sourcePosition, outputStart, outputEnd, size));
        }
        return result.build();
    }

    private static Streams copyMaskedStreams(Streams source, Streams existing, Allocator allocator, Allocator.Context allocationContext, Mask mask)
    {
        Streams.Builder result = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : source.asMap().entrySet()) {
            Vector existingVector = existing != null ? existing.getOrNull(entry.getKey()) : null;
            result.put(entry.getKey(), entry.getValue().copyMasked(allocator, allocationContext, existingVector, mask));
        }
        return result.build();
    }
}
