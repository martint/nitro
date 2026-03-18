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

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class Streams
{
    private final EnumMap<Stream, Vector> vectors;
    private final Map<Stream, Vector> view;

    private Streams(EnumMap<Stream, Vector> vectors)
    {
        this(vectors, true);
    }

    private Streams(EnumMap<Stream, Vector> vectors, boolean copy)
    {
        this.vectors = copy ? new EnumMap<>(vectors) : vectors;
        this.view = Collections.unmodifiableMap(this.vectors);
    }

    public static Streams empty()
    {
        return new Streams(new EnumMap<>(Stream.class));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Streams of(Stream stream, Vector vector)
    {
        return builder()
                .put(stream, vector)
                .build();
    }

    public static Streams ofValues(Vector values)
    {
        return of(Stream.VALUES, values);
    }

    public static Streams ofValuesAndNulls(Vector values, BooleanVector nulls)
    {
        return builder()
                .put(Stream.VALUES, values)
                .put(Stream.NULLS, nulls)
                .build();
    }

    public Streams with(Stream stream, Vector vector)
    {
        requireNonNull(stream, "stream is null");
        requireNonNull(vector, "vector is null");

        EnumMap<Stream, Vector> updated = new EnumMap<>(vectors);
        updated.put(stream, vector);
        return new Streams(updated, false);
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

    public Vector getOrNull(Stream stream)
    {
        requireNonNull(stream, "stream is null");
        return vectors.get(stream);
    }

    public Vector values()
    {
        return get(Stream.VALUES);
    }

    public Map<Stream, Vector> asMap()
    {
        return view;
    }

    public static final class Builder
    {
        private final EnumMap<Stream, Vector> vectors = new EnumMap<>(Stream.class);

        private Builder() {}

        public Builder put(Stream stream, Vector vector)
        {
            requireNonNull(stream, "stream is null");
            requireNonNull(vector, "vector is null");
            vectors.put(stream, vector);
            return this;
        }

        public Builder putAll(Streams streams)
        {
            vectors.putAll(streams.vectors);
            return this;
        }

        public Streams build()
        {
            if (vectors.isEmpty()) {
                return Streams.empty();
            }
            return new Streams(vectors);
        }
    }
}
