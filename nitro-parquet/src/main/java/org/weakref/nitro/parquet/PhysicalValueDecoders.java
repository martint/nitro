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
package org.weakref.nitro.parquet;

import org.weakref.nitro.data.PrimitiveArrayPool;

import static java.util.Objects.requireNonNull;

/** Schema-driven construction is isolated here; page/level decoding never switches on physical type. */
final class PhysicalValueDecoders
{
    private PhysicalValueDecoders() {}

    static PhysicalValueDecoder create(ParquetSchema.Primitive leaf, PrimitiveArrayPool arrayPool)
    {
        requireNonNull(arrayPool, "arrayPool is null");
        return switch (leaf.type()) {
            case BOOLEAN -> new BooleanPhysicalValueDecoder(arrayPool);
            case DOUBLE -> new DoublePhysicalValueDecoder(arrayPool);
            case INT32, INT64 -> new LongPhysicalValueDecoder(leaf.type(), arrayPool);
            case BYTE_ARRAY -> new BinaryPhysicalValueDecoder(arrayPool);
            case FIXED_LEN_BYTE_ARRAY -> createFixedDecimal(leaf, arrayPool);
            default -> throw new UnsupportedParquetFeatureException(
                    "Native nested Parquet reader does not support physical type " + leaf.type() + " at '" + String.join(".", leaf.path()) + "'");
        };
    }

    private static PhysicalValueDecoder createFixedDecimal(ParquetSchema.Primitive leaf, PrimitiveArrayPool arrayPool)
    {
        if (!leaf.decimal() || leaf.typeLength() > Long.BYTES) {
            throw new UnsupportedParquetFeatureException(
                    "Native nested Parquet reader does not support fixed-width field '" + String.join(".", leaf.path()) + "'");
        }
        return new FixedDecimalPhysicalValueDecoder(leaf.typeLength(), arrayPool);
    }
}
