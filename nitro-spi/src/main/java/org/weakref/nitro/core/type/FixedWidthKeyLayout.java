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

import java.util.List;

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

    public record Lane(List<String> fieldPath, Carrier carrier)
    {
        public Lane
        {
            fieldPath = List.copyOf(requireNonNull(fieldPath, "fieldPath is null"));
            if (fieldPath.stream().anyMatch(field -> field == null || field.isEmpty())) {
                throw new IllegalArgumentException("fieldPath contains a null or empty field");
            }
            carrier = requireNonNull(carrier, "carrier is null");
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

    public FixedWidthKeyLayout
    {
        lanes = List.copyOf(requireNonNull(lanes, "lanes is null"));
        if (lanes.isEmpty()) {
            throw new IllegalArgumentException("lanes is empty");
        }
        lanes.forEach(lane -> requireNonNull(lane, "lane is null"));
    }
}
