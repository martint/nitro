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

import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

final class OperatorKeySemantics
{
    private OperatorKeySemantics() {}

    public static Key key(Vector values, BooleanVector nulls, int position)
    {
        return ownedKey(probeKey(values, nulls, position));
    }

    public static Key compositeKey(Key[] keys)
    {
        return keys.length == 1 ? keys[0] : new CompositeKey(keys);
    }

    public static Key probeKey(Vector values, BooleanVector nulls, int position)
    {
        if (OperatorVectorSupport.isNull(nulls, position)) {
            return null;
        }

        return switch (OperatorVectorSupport.flatten(values)) {
            case I64Vector _ -> new LongKey(OperatorVectorSupport.longValue(values, position));
            case BooleanVector _ -> new BooleanKey(OperatorVectorSupport.booleanValue(values, position));
            case F64Vector _ -> new DoubleKey(Double.doubleToLongBits(OperatorVectorSupport.doubleValue(values, position)));
            case BinaryVector _ -> new BinaryProbeKey(values, position);
            default -> throw new IllegalArgumentException("Unsupported key vector: " + values.getClass().getSimpleName());
        };
    }

    public static Key probeCompositeKey(Key[] keys)
    {
        return keys.length == 1 ? keys[0] : new CompositeProbeKey(keys);
    }

    public static Key ownedKey(Key key)
    {
        return switch (key) {
            case null -> null;
            case LongKey _, BooleanKey _, DoubleKey _, BinaryKey _, CompositeKey _ -> key;
            case BinaryProbeKey probe -> new BinaryKey(OperatorVectorSupport.binaryBytes(probe.values(), probe.position()));
            case CompositeProbeKey probe -> {
                Key[] owned = new Key[probe.keys().length];
                for (int index = 0; index < owned.length; index++) {
                    owned[index] = ownedKey(probe.keys()[index]);
                }
                yield new CompositeKey(owned);
            }
        };
    }

    public static BinaryKey binaryKey(byte[] bytes)
    {
        return new BinaryKey(bytes);
    }

    sealed interface Key
            permits LongKey, BooleanKey, DoubleKey, BinaryKey, BinaryProbeKey, CompositeKey, CompositeProbeKey
    {
    }

    public record LongKey(long value)
            implements Key
    {
    }

    public record BooleanKey(boolean value)
            implements Key
    {
    }

    public record DoubleKey(long bits)
            implements Key
    {
    }

    public record BinaryKey(byte[] bytes)
            implements Key
    {
        @Override
        public boolean equals(Object object)
        {
            return switch (object) {
                case BinaryKey other -> Arrays.equals(bytes, other.bytes);
                case BinaryProbeKey other -> OperatorVectorSupport.binaryEquals(other.values(), other.position(), bytes);
                default -> false;
            };
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(bytes);
        }
    }

    public record BinaryProbeKey(Vector values, int position)
            implements Key
    {
        @Override
        public boolean equals(Object object)
        {
            return switch (object) {
                case BinaryKey other -> OperatorVectorSupport.binaryEquals(values, position, other.bytes());
                case BinaryProbeKey other -> OperatorVectorSupport.binaryEquals(values, position, other.values(), other.position());
                default -> false;
            };
        }

        @Override
        public int hashCode()
        {
            return OperatorVectorSupport.binaryHash(values, position);
        }
    }

    public static final class CompositeKey
            implements Key
    {
        private final Key[] keys;

        public CompositeKey(Key[] keys)
        {
            this.keys = keys.clone();
        }

        @Override
        public boolean equals(Object object)
        {
            return switch (object) {
                case CompositeKey other -> Arrays.equals(keys, other.keys);
                case CompositeProbeKey other -> Arrays.equals(keys, other.keys());
                default -> false;
            };
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(keys);
        }
    }

    public record CompositeProbeKey(Key[] keys)
            implements Key
    {
        @Override
        public boolean equals(Object object)
        {
            return switch (object) {
                case CompositeKey other -> Arrays.equals(keys, other.keys);
                case CompositeProbeKey other -> Arrays.equals(keys, other.keys);
                default -> false;
            };
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(keys);
        }
    }
}
