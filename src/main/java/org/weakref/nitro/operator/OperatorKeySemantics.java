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
        if (OperatorVectorSupport.isNull(nulls, position)) {
            return null;
        }

        return switch (OperatorVectorSupport.flatten(values)) {
            case I64Vector _ -> new LongKey(OperatorVectorSupport.longValue(values, position));
            case BooleanVector _ -> new BooleanKey(OperatorVectorSupport.booleanValue(values, position));
            case F64Vector _ -> new DoubleKey(Double.doubleToLongBits(OperatorVectorSupport.doubleValue(values, position)));
            case BinaryVector _ -> new BinaryKey(OperatorVectorSupport.binaryBytes(values, position));
            default -> throw new IllegalArgumentException("Unsupported key vector: " + values.getClass().getSimpleName());
        };
    }

    public static Key compositeKey(Key[] keys)
    {
        return keys.length == 1 ? keys[0] : new CompositeKey(keys);
    }

    public static BinaryKey binaryKey(byte[] bytes)
    {
        return new BinaryKey(bytes);
    }

    sealed interface Key
            permits LongKey, BooleanKey, DoubleKey, BinaryKey, CompositeKey
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
            return object instanceof BinaryKey other && Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(bytes);
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
            return object instanceof CompositeKey other && Arrays.equals(keys, other.keys);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(keys);
        }
    }
}
