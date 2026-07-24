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
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Explicitly owned registry of {@link Type}s. Built-in types are registered for every owner, and new types are added
 * with {@link #register}; generated pipelines receive their owner's resolver during construction.
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

    /**
     * Dictionary string id; the slot holds the id and the consumer reconstructs the string from the dictionary.
     * Comparison is by id, which equals value (lexicographic) order under the engine's <em>ordered dictionary</em>
     * contract: a string column's dictionary is sorted (UTF-8 byte / code-point order), so ORDER BY on the string
     * is a fast integer compare.
     */
    public static final Type STRING = new SimpleType(
            "string",
            (a, b) -> "Long.compare(" + a + ", " + b + ")",
            slot -> slot,
            Types::reconstructString);

    private final Map<String, Type> registry = new ConcurrentHashMap<>();

    public Types()
    {
        register(LONG);
        register(DOUBLE);
        register(STRING);
    }

    public void register(Type type)
    {
        registry.put(type.name(), type);
    }

    public Type get(String name)
    {
        Type type = registry.get(name);
        if (type == null) {
            throw new IllegalArgumentException("unknown type: " + name);
        }
        return type;
    }

    /**
     * Freezes the current bindings for one compiler artifact. Later registry changes cannot alter the meaning of an
     * already compiled pipeline or extend its connector-classloader reachability.
     */
    public Function<String, Type> snapshotResolver()
    {
        Map<String, Type> snapshot = Map.copyOf(registry);
        return name -> {
            Type type = snapshot.get(name);
            if (type == null) {
                throw new IllegalArgumentException("unknown type: " + name);
            }
            return type;
        };
    }

    /** Reconstructs a result column into an engine {@link Vector}; the runtime counterpart of {@link Type#decode}. */
    @FunctionalInterface
    private interface Reconstructor
    {
        Vector toVector(long[] slots, int count, byte[][] dictionary);
    }

    private static final byte[] NO_BYTES = new byte[0];

    private static BinaryVector reconstructString(long[] slots, int count, byte[][] dictionary)
    {
        // An out-of-range id emits empty bytes: a null entry canonicalizes to id 0 (out of range when the
        // dictionary is empty, masked null by the caller), and a string CASE's else-'' branch emits the -1
        // sentinel, whose value IS the empty string.
        int bytes = 0;
        for (int i = 0; i < count; i++) {
            int id = (int) slots[i];
            if (id >= 0 && id < dictionary.length) {
                bytes += dictionary[id].length;
            }
        }
        BinaryVector vector = new BinaryVector(count, bytes);
        for (int i = 0; i < count; i++) {
            int id = (int) slots[i];
            vector.setBytes(i, id >= 0 && id < dictionary.length ? dictionary[id] : NO_BYTES);
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
