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
package org.weakref.nitro.jit;

import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Utf8Traits;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * Registry of {@link Type}s. Built-in types are registered here, and new types are added with
 * {@link #register}; the compiler resolves a type by {@link Type#name() name} via {@link #get}. Generated code
 * references a type as {@code Types.get("<name>")}, so a custom type flows through results without any change to
 * the compiler.
 */
public final class Types
{
    /** Signed 64-bit value, stored directly. */
    public static final Type LONG = new SimpleType(
            "long",
            (a, b) -> "Long.compare(" + a + ", " + b + ")",
            slot -> slot,
            (slots, count, dictionary) -> new I64Vector(Arrays.copyOf(slots, count)));

    /** Double, stored as {@link Double#doubleToRawLongBits}. */
    public static final Type DOUBLE = new SimpleType(
            "double",
            (a, b) -> "Double.compare(Double.longBitsToDouble(" + a + "), Double.longBitsToDouble(" + b + "))",
            slot -> "Double.longBitsToDouble(" + slot + ")",
            (slots, count, dictionary) -> {
                double[] values = new double[count];
                for (int i = 0; i < count; i++) {
                    values[i] = Double.longBitsToDouble(slots[i]);
                }
                return new F64Vector(values);
            });

    /** Dictionary string id; the slot holds the id and the consumer reconstructs the string from the dictionary. */
    // NOTE: comparison is by id, not lexicographic -- a sorted-dictionary or boundary string compare is the follow-up.
    public static final Type STRING = new SimpleType(
            "string",
            (a, b) -> "Long.compare(" + a + ", " + b + ")",
            slot -> slot,
            Types::reconstructString);

    private static final Map<String, Type> REGISTRY = new ConcurrentHashMap<>();

    static {
        register(LONG);
        register(DOUBLE);
        register(STRING);
    }

    private Types() {}

    public static void register(Type type)
    {
        REGISTRY.put(type.name(), type);
    }

    public static Type get(String name)
    {
        Type type = REGISTRY.get(name);
        if (type == null) {
            throw new IllegalArgumentException("unknown type: " + name);
        }
        return type;
    }

    /** Reconstructs a result column into an engine {@link Vector}; the runtime counterpart of {@link Type#decode}. */
    @FunctionalInterface
    private interface Reconstructor
    {
        Vector toVector(long[] slots, int count, byte[][] dictionary);
    }

    private static BinaryVector reconstructString(long[] slots, int count, byte[][] dictionary)
    {
        int bytes = 0;
        for (int i = 0; i < count; i++) {
            bytes += dictionary[(int) slots[i]].length;
        }
        BinaryVector vector = new BinaryVector(count, bytes);
        for (int i = 0; i < count; i++) {
            vector.setBytes(i, dictionary[(int) slots[i]]);
        }
        vector.addTrait(Utf8Traits.UTF8_STRING);
        return vector;
    }

    private record SimpleType(String name, BinaryOperator<String> compareFn, UnaryOperator<String> decodeFn, Reconstructor reconstructFn)
            implements Type
    {
        @Override
        public String compare(String a, String b)
        {
            return compareFn.apply(a, b);
        }

        @Override
        public String decode(String slot)
        {
            return decodeFn.apply(slot);
        }

        @Override
        public Vector toVector(long[] slots, int count, byte[][] dictionary)
        {
            return reconstructFn.toVector(slots, count, dictionary);
        }
    }
}
