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

import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class StructVector
        implements FlatVector
{
    private final int positionCount;
    private final LinkedHashMap<String, Streams> fields = new LinkedHashMap<>();

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
        return Set.copyOf(fields.keySet());
    }

    public Map<String, Streams> fields()
    {
        return Map.copyOf(fields);
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
        for (int position : mask) {
            existing = copySinglePositionInto(allocator, allocationContext, existing, position, position, length());
        }
        return existing;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        StructVector output = allocator.reallocateIfNecessary(allocationContext, existing instanceof StructVector vector ? vector : null, StructVector.class, size, StructVector::new);
        if (outputStart == 0) {
            output.clearFields();
        }
        Map<String, Streams> existingFields = existing instanceof StructVector vector ? vector.fields() : Map.of();
        for (Map.Entry<String, Streams> entry : fields.entrySet()) {
            output.setField(entry.getKey(), copyStreams(entry.getValue(), existingFields.get(entry.getKey()), allocator, allocationContext, sourcePositions, sourceCount, outputStart, size));
        }
        return output;
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        StructVector output = allocator.reallocateIfNecessary(allocationContext, existing instanceof StructVector vector ? vector : null, StructVector.class, size, StructVector::new);
        if (outputPosition == 0) {
            output.clearFields();
        }
        Map<String, Streams> existingFields = existing instanceof StructVector vector ? vector.fields() : Map.of();
        for (Map.Entry<String, Streams> entry : fields.entrySet()) {
            output.setField(entry.getKey(), copySingleStreams(entry.getValue(), existingFields.get(entry.getKey()), allocator, allocationContext, sourcePosition, outputPosition, size));
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
        return 2;
    }

    @Override
    public void forEachChildVector(Consumer<Vector> consumer)
    {
        fields.values().forEach(streams -> streams.asMap().values().forEach(consumer));
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
}
