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

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.EnumMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class Streams
{
    private final EnumMap<Stream, Vector> vectors;

    private Streams(EnumMap<Stream, Vector> vectors)
    {
        this.vectors = new EnumMap<>(vectors);
    }

    public static Streams empty()
    {
        return new Streams(new EnumMap<>(Stream.class));
    }

    public static Streams of(Stream stream, Vector vector)
    {
        return empty().with(stream, vector);
    }

    public static Streams ofValues(Vector values)
    {
        return of(Stream.VALUES, values);
    }

    public static Streams ofValuesAndNulls(Vector values, BooleanVector nulls)
    {
        return of(Stream.VALUES, values).with(Stream.NULLS, nulls);
    }

    public Streams with(Stream stream, Vector vector)
    {
        requireNonNull(stream, "stream is null");
        requireNonNull(vector, "vector is null");

        EnumMap<Stream, Vector> updated = new EnumMap<>(vectors);
        updated.put(stream, vector);
        return new Streams(updated);
    }

    public boolean has(Stream stream)
    {
        return vectors.containsKey(stream);
    }

    public Vector get(Stream stream)
    {
        requireNonNull(stream, "stream is null");
        Vector vector = vectors.get(stream);
        if (vector == null) {
            throw new IllegalArgumentException("Stream not present: " + stream);
        }
        return vector;
    }

    public Vector values()
    {
        return get(Stream.VALUES);
    }

    public Map<Stream, Vector> asMap()
    {
        return Map.copyOf(vectors);
    }
}
