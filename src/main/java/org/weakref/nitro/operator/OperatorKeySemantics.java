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
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

final class OperatorKeySemantics
{
    private OperatorKeySemantics() {}

    public static Key reusableProbeKey(Vector values)
    {
        return switch (OperatorVectorSupport.flatten(values)) {
            case I32Vector _, I64Vector _ -> new LongProbeKey(0);
            case BooleanVector _ -> new BooleanProbeKey(false);
            case F64Vector _ -> new DoubleProbeKey(0);
            case BinaryVector _ -> new BinaryProbeKey(null, 0);
            default -> throw new IllegalArgumentException("Unsupported key vector: " + values.getClass().getSimpleName());
        };
    }

    public static Key probeKey(Vector values, BooleanVector nulls, int position, Key reusable)
    {
        if (OperatorVectorSupport.isNull(nulls, position)) {
            return null;
        }

        return switch (OperatorVectorSupport.flatten(values)) {
            case I32Vector _, I64Vector _ -> {
                LongProbeKey key = (LongProbeKey) reusable;
                key.setValue(OperatorVectorSupport.longValue(values, position));
                yield key;
            }
            case BooleanVector _ -> {
                BooleanProbeKey key = (BooleanProbeKey) reusable;
                key.setValue(OperatorVectorSupport.booleanValue(values, position));
                yield key;
            }
            case F64Vector _ -> {
                DoubleProbeKey key = (DoubleProbeKey) reusable;
                key.setBits(Double.doubleToLongBits(OperatorVectorSupport.doubleValue(values, position)));
                yield key;
            }
            case BinaryVector _ -> {
                BinaryProbeKey key = (BinaryProbeKey) reusable;
                key.set(values, position);
                yield key;
            }
            default -> throw new IllegalArgumentException("Unsupported key vector: " + values.getClass().getSimpleName());
        };
    }

    public static CompositeProbeKey reusableCompositeProbeKey(int keyCount)
    {
        return new CompositeProbeKey(new Key[keyCount], false);
    }

    public static Key probeCompositeKey(Key[] keys, CompositeProbeKey reusable)
    {
        if (keys.length == 1) {
            return keys[0];
        }
        reusable.setKeys(keys);
        return reusable;
    }

    public static Key ownedKey(Key key)
    {
        return switch (key) {
            case null -> null;
            case LongKey _, BooleanKey _, DoubleKey _, BinaryKey _, CompositeKey _ -> key;
            case LongProbeKey probe -> new LongKey(probe.value());
            case BooleanProbeKey probe -> new BooleanKey(probe.value());
            case DoubleProbeKey probe -> new DoubleKey(probe.bits());
            case BinaryProbeKey probe -> new BinaryKey(copyBinaryBytes(probe.values(), probe.position()));
            case CompositeProbeKey probe -> {
                Key[] owned = new Key[probe.keys().length];
                for (int index = 0; index < owned.length; index++) {
                    owned[index] = ownedKey(probe.keys()[index]);
                }
                yield new CompositeKey(owned);
            }
        };
    }

    private static byte[] copyBinaryBytes(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector binary -> binary.copyBytes(position);
            case org.weakref.nitro.data.DictionaryVector dictionary -> copyBinaryBytes(dictionary.values(), dictionary.ids()[position]);
            case org.weakref.nitro.data.RleVector rle -> copyBinaryBytes(rle.values(), OperatorVectorSupport.runIndex(rle, position));
            default -> throw new IllegalArgumentException("Expected binary vector but found " + values.getClass().getSimpleName());
        };
    }

    sealed interface Key
            permits LongKey, LongProbeKey, BooleanKey, BooleanProbeKey, DoubleKey, DoubleProbeKey, BinaryKey, BinaryProbeKey, CompositeKey, CompositeProbeKey
    {
    }

    public record LongKey(long value)
            implements Key
    {
    }

    public static final class LongProbeKey
            implements Key
    {
        private long value;

        public LongProbeKey(long value)
        {
            this.value = value;
        }

        public long value()
        {
            return value;
        }

        public void setValue(long value)
        {
            this.value = value;
        }

        @Override
        public boolean equals(Object object)
        {
            return switch (object) {
                case LongKey other -> value == other.value();
                case LongProbeKey other -> value == other.value;
                default -> false;
            };
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(value);
        }
    }

    public record BooleanKey(boolean value)
            implements Key
    {
    }

    public static final class BooleanProbeKey
            implements Key
    {
        private boolean value;

        public BooleanProbeKey(boolean value)
        {
            this.value = value;
        }

        public boolean value()
        {
            return value;
        }

        public void setValue(boolean value)
        {
            this.value = value;
        }

        @Override
        public boolean equals(Object object)
        {
            return switch (object) {
                case BooleanKey other -> value == other.value();
                case BooleanProbeKey other -> value == other.value;
                default -> false;
            };
        }

        @Override
        public int hashCode()
        {
            return Boolean.hashCode(value);
        }
    }

    public record DoubleKey(long bits)
            implements Key
    {
    }

    public static final class DoubleProbeKey
            implements Key
    {
        private long bits;

        public DoubleProbeKey(long bits)
        {
            this.bits = bits;
        }

        public long bits()
        {
            return bits;
        }

        public void setBits(long bits)
        {
            this.bits = bits;
        }

        @Override
        public boolean equals(Object object)
        {
            return switch (object) {
                case DoubleKey other -> bits == other.bits();
                case DoubleProbeKey other -> bits == other.bits;
                default -> false;
            };
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(bits);
        }
    }

    public static final class BinaryKey
            implements Key
    {
        private final byte[] bytes;
        private final int hash;

        public BinaryKey(byte[] bytes)
        {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        public byte[] bytes()
        {
            return bytes;
        }

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
            return hash;
        }
    }

    public static final class BinaryProbeKey
            implements Key
    {
        private Vector values;
        private int position;

        public BinaryProbeKey(Vector values, int position)
        {
            this.values = values;
            this.position = position;
        }

        public Vector values()
        {
            return values;
        }

        public int position()
        {
            return position;
        }

        public void set(Vector values, int position)
        {
            this.values = values;
            this.position = position;
        }

        @Override
        public boolean equals(Object object)
        {
            return switch (object) {
                case BinaryKey other -> OperatorVectorSupport.binaryEquals(values, position, other.bytes());
                case BinaryProbeKey other -> OperatorVectorSupport.binaryEquals(values, position, other.values, other.position);
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

    public static final class CompositeProbeKey
            implements Key
    {
        private Key[] keys;
        private final boolean copyKeys;

        public CompositeProbeKey(Key[] keys)
        {
            this(keys, true);
        }

        private CompositeProbeKey(Key[] keys, boolean copyKeys)
        {
            this.copyKeys = copyKeys;
            this.keys = copyKeys ? keys.clone() : keys;
        }

        public void setKeys(Key[] keys)
        {
            this.keys = copyKeys ? keys.clone() : keys;
        }

        public Key[] keys()
        {
            return keys;
        }

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
