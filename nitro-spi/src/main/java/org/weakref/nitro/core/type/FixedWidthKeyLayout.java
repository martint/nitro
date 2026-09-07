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
package org.weakref.nitro.core.type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Provider proof that logical key identity is exactly the ordered tuple of fixed-width primitive lanes described
 * here. Field paths describe physical access only; neither their names nor their carriers assign logical meaning.
 *
 * <p>A lane with an empty field path reads the logical value itself. A non-empty path selects nested structural
 * fields in order. Selected structural components must be non-null whenever their enclosing logical value is
 * non-null. Logical nullness remains attached to the enclosing value and applies to all of its lanes. {@link
 * Carrier#F64} identity is equality of the raw IEEE-754 bits; providers whose logical identity normalizes zeros,
 * NaNs, or other encodings must not declare that carrier layout.
 */
public record FixedWidthKeyLayout(List<Lane> lanes)
{
    public enum Carrier
    {
        I32,
        I64,
        F64,
        BOOLEAN
    }

    public record Source(List<String> fieldPath, Carrier carrier)
    {
        public Source
        {
            fieldPath = List.copyOf(requireNonNull(fieldPath, "fieldPath is null"));
            if (fieldPath.stream().anyMatch(field -> field == null || field.isEmpty())) {
                throw new IllegalArgumentException("fieldPath contains a null or empty field");
            }
            carrier = requireNonNull(carrier, "carrier is null");
        }
    }

    /**
     * One canonical 64-bit key lane computed from one or more physical primitive sources.
     *
     * <p>An absent projection is the raw identity conversion of exactly one source. A present projection is an exact
     * provider-owned target whose primitive arguments match the sources and whose return type is {@code long}.
     * Generated consumers constant-link the target into their row loops; it is not an interpreted or virtual
     * callback.
     */
    public record Lane(List<Source> sources, Optional<MethodHandle> projection)
    {
        public Lane(List<String> fieldPath, Carrier carrier)
        {
            this(List.of(new Source(fieldPath, carrier)), Optional.empty());
        }

        public Lane
        {
            sources = List.copyOf(requireNonNull(sources, "sources is null"));
            if (sources.isEmpty()) {
                throw new IllegalArgumentException("sources is empty");
            }
            sources.forEach(source -> requireNonNull(source, "source is null"));
            projection = requireNonNull(projection, "projection is null");
            if (projection.isEmpty() && sources.size() != 1) {
                throw new IllegalArgumentException("Raw key lane requires exactly one source");
            }
            if (projection.isPresent()) {
                MethodHandle target = projection.orElseThrow();
                MethodType expected = MethodType.methodType(
                        long.class,
                        sources.stream().map(source -> carrierType(source.carrier())).toArray(Class<?>[]::new));
                if (!target.type().equals(expected)) {
                    throw new IllegalArgumentException("Key projection target has type %s; expected %s"
                            .formatted(target.type(), expected));
                }
            }
        }

        public static Lane projected(List<Source> sources, MethodHandle projection)
        {
            return new Lane(sources, Optional.of(requireNonNull(projection, "projection is null")));
        }

        /** Compatibility accessor for raw single-source lanes. */
        public List<String> fieldPath()
        {
            return requireRawSource().fieldPath();
        }

        /** Compatibility accessor for raw single-source lanes. */
        public Carrier carrier()
        {
            return requireRawSource().carrier();
        }

        private Source requireRawSource()
        {
            if (projection.isPresent() || sources.size() != 1) {
                throw new IllegalStateException("Projected key lane has no single raw source");
            }
            return sources.getFirst();
        }

        public static Lane i64(List<String> fieldPath)
        {
            return new Lane(fieldPath, Carrier.I64);
        }

        public static Lane i32(List<String> fieldPath)
        {
            return new Lane(fieldPath, Carrier.I32);
        }

        public static Lane f64(List<String> fieldPath)
        {
            return new Lane(fieldPath, Carrier.F64);
        }

        public static Lane bool(List<String> fieldPath)
        {
            return new Lane(fieldPath, Carrier.BOOLEAN);
        }
    }

    private static Class<?> carrierType(Carrier carrier)
    {
        return switch (carrier) {
            case I32 -> int.class;
            case I64 -> long.class;
            case F64 -> double.class;
            case BOOLEAN -> boolean.class;
        };
    }

    public FixedWidthKeyLayout
    {
        lanes = List.copyOf(requireNonNull(lanes, "lanes is null"));
        if (lanes.isEmpty()) {
            throw new IllegalArgumentException("lanes is empty");
        }
        lanes.forEach(lane -> requireNonNull(lane, "lane is null"));
    }
}
