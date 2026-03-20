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
    public PoolingMode poolingMode()
    {
        return PoolingMode.STANDARD;
    }

    @Override
    public void forEachChildVector(Consumer<Vector> consumer)
    {
        fields.values().forEach(streams -> streams.asMap().values().forEach(consumer));
    }
}
