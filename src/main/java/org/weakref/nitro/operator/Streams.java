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
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Immutable tuple of logical streams for one column or state slot.
 * <p>
 * Nitro commonly models a value as a VALUES stream plus optional NULLS and ERRORS side streams.
 * This class is the compact transport container for that trio.
 */
public final class Streams
{
    private static final boolean REUSE_TRANSPORT_TUPLES = !Boolean.getBoolean("nitro.streams.disableTransportTupleReuse");

    private static final int VALUES_FLAG = 1;
    private static final int NULLS_FLAG = 1 << 1;
    private static final int ERRORS_FLAG = 1 << 2;

    @SuppressWarnings("unchecked")
    private static final Set<Stream>[] STREAM_SETS = new Set[8];

    static {
        for (int flags = 0; flags < STREAM_SETS.length; flags++) {
            EnumSet<Stream> streams = EnumSet.noneOf(Stream.class);
            if ((flags & VALUES_FLAG) != 0) {
                streams.add(Stream.VALUES);
            }
            if ((flags & NULLS_FLAG) != 0) {
                streams.add(Stream.NULLS);
            }
            if ((flags & ERRORS_FLAG) != 0) {
                streams.add(Stream.ERRORS);
            }
            STREAM_SETS[flags] = Collections.unmodifiableSet(streams);
        }
    }

    private static final Streams EMPTY = new Streams(null, null, null);

    private final Vector values;
    private final Vector nulls;
    private final Vector errors;
    private final int flags;
    private Map<Stream, Vector> view;

    private Streams(Vector values, Vector nulls, Vector errors)
    {
        this.values = values;
        this.nulls = nulls;
        this.errors = errors;
        this.flags = flags(values, nulls, errors);
    }

    public static Streams empty()
    {
        return EMPTY;
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
        return new Streams(values, null, null);
    }

    public static Streams ofValuesAndNulls(Vector values, BooleanVector nulls)
    {
        return new Streams(values, nulls, null);
    }

    /**
     * Returns {@code existing} when it already contains exactly the supplied backing vectors,
     * otherwise creates a new stream tuple.
     *
     * This is intended for hot copy paths whose vectors are caller-owned and grow only
     * occasionally. It preserves the immutable {@link Streams} contract while avoiding a new
     * transport object for every copied position once the backing storage has reached its
     * steady-state capacity.
     */
    public static Streams reuseOrCreate(Streams existing, Vector values, Vector nulls, Vector errors)
    {
        if (REUSE_TRANSPORT_TUPLES &&
                existing != null &&
                existing.values == values &&
                existing.nulls == nulls &&
                existing.errors == errors) {
            return existing;
        }
        return of(values, nulls, errors);
    }

    public static Streams reuseValuesAndNulls(Streams existing, Vector values, BooleanVector nulls)
    {
        return reuseOrCreate(existing, values, nulls, null);
    }

    public static Streams of(Vector values, Vector nulls, Vector errors)
    {
        if (values == null && nulls == null && errors == null) {
            return EMPTY;
        }
        return new Streams(values, nulls, errors);
    }

    public Streams with(Stream stream, Vector vector)
    {
        requireNonNull(stream, "stream is null");
        requireNonNull(vector, "vector is null");

        return switch (stream) {
            case VALUES -> values == vector ? this : new Streams(vector, nulls, errors);
            case NULLS -> nulls == vector ? this : new Streams(values, vector, errors);
            case ERRORS -> errors == vector ? this : new Streams(values, nulls, vector);
        };
    }

    public boolean has(Stream stream)
    {
        requireNonNull(stream, "stream is null");
        return switch (stream) {
            case VALUES -> values != null;
            case NULLS -> nulls != null;
            case ERRORS -> errors != null;
        };
    }

    public boolean hasValues()
    {
        return values != null;
    }

    public boolean hasNulls()
    {
        return nulls != null;
    }

    public boolean hasErrors()
    {
        return errors != null;
    }

    public boolean isValuesOnly()
    {
        return flags == VALUES_FLAG;
    }

    public Vector get(Stream stream)
    {
        Vector vector = getOrNull(stream);
        if (vector == null) {
            throw new IllegalArgumentException("Stream not present: " + stream);
        }
        return vector;
    }

    public Vector getOrNull(Stream stream)
    {
        requireNonNull(stream, "stream is null");
        return switch (stream) {
            case VALUES -> values;
            case NULLS -> nulls;
            case ERRORS -> errors;
        };
    }

    public Vector values()
    {
        return get(Stream.VALUES);
    }

    public Set<Stream> streams()
    {
        return STREAM_SETS[flags];
    }

    static Set<Stream> streamSet(int flags)
    {
        return STREAM_SETS[flags];
    }

    int flags()
    {
        return flags;
    }

    public Map<Stream, Vector> asMap()
    {
        Map<Stream, Vector> existing = view;
        if (existing != null) {
            return existing;
        }

        if (flags == 0) {
            view = Map.of();
            return view;
        }

        EnumMap<Stream, Vector> vectors = new EnumMap<>(Stream.class);
        if (values != null) {
            vectors.put(Stream.VALUES, values);
        }
        if (nulls != null) {
            vectors.put(Stream.NULLS, nulls);
        }
        if (errors != null) {
            vectors.put(Stream.ERRORS, errors);
        }
        view = Collections.unmodifiableMap(vectors);
        return view;
    }

    public static final class Builder
    {
        private Vector values;
        private Vector nulls;
        private Vector errors;

        private Builder() {}

        public Builder put(Stream stream, Vector vector)
        {
            requireNonNull(stream, "stream is null");
            requireNonNull(vector, "vector is null");
            switch (stream) {
                case VALUES -> values = vector;
                case NULLS -> nulls = vector;
                case ERRORS -> errors = vector;
            }
            return this;
        }

        public Builder putAll(Streams streams)
        {
            if (streams.values != null) {
                values = streams.values;
            }
            if (streams.nulls != null) {
                nulls = streams.nulls;
            }
            if (streams.errors != null) {
                errors = streams.errors;
            }
            return this;
        }

        public Streams build()
        {
            if (values == null && nulls == null && errors == null) {
                return EMPTY;
            }
            return new Streams(values, nulls, errors);
        }
    }

    private static int flags(Vector values, Vector nulls, Vector errors)
    {
        int flags = 0;
        if (values != null) {
            flags |= VALUES_FLAG;
        }
        if (nulls != null) {
            flags |= NULLS_FLAG;
        }
        if (errors != null) {
            flags |= ERRORS_FLAG;
        }
        return flags;
    }
}
