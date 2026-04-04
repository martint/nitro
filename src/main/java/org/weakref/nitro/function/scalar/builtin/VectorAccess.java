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
package org.weakref.nitro.function.scalar.builtin;

import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;

final class VectorAccess
{
    private VectorAccess() {}

    public static LongValues longValues(Vector vector)
    {
        return switch (vector) {
            case I64Vector values -> position -> values.values()[position];
            case I32Vector values -> position -> values.values()[position];
            case DictionaryVector values -> {
                LongValues dictionaryValues = longValues(values.values());
                int[] ids = values.ids();
                yield position -> dictionaryValues.value(ids[position]);
            }
            case RleVector values -> {
                LongValues runValues = longValues(values.values());
                yield position -> runValues.value(values.runIndex(position));
            }
            default -> throw new IllegalArgumentException("Expected integer vector but found " + vector.getClass().getSimpleName());
        };
    }

    public static BooleanValues booleanValues(Vector vector)
    {
        if (vector == null) {
            return position -> false;
        }
        return switch (vector) {
            case BooleanVector values -> position -> values.values()[position];
            case DictionaryVector values -> {
                BooleanValues dictionaryValues = booleanValues(values.values());
                int[] ids = values.ids();
                yield position -> dictionaryValues.value(ids[position]);
            }
            case RleVector values -> {
                BooleanValues runValues = booleanValues(values.values());
                yield position -> runValues.value(values.runIndex(position));
            }
            default -> throw new IllegalArgumentException("Expected boolean-backed null vector but found " + vector.getClass().getSimpleName());
        };
    }

    public static DoubleValues doubleValues(Vector vector)
    {
        return switch (vector) {
            case F64Vector values -> position -> values.values()[position];
            case DictionaryVector values -> {
                DoubleValues dictionaryValues = doubleValues(values.values());
                int[] ids = values.ids();
                yield position -> dictionaryValues.value(ids[position]);
            }
            case RleVector values -> {
                DoubleValues runValues = doubleValues(values.values());
                yield position -> runValues.value(values.runIndex(position));
            }
            default -> throw new IllegalArgumentException("Expected double vector but found " + vector.getClass().getSimpleName());
        };
    }

    public static BinaryValues binaryValues(Vector vector)
    {
        return switch (vector) {
            case BinaryVector values -> position -> new BinarySlice(values.data(), values.startOffset(position), values.length(position));
            case DictionaryVector values -> {
                BinaryValues dictionaryValues = binaryValues(values.values());
                int[] ids = values.ids();
                yield position -> dictionaryValues.value(ids[position]);
            }
            case RleVector values -> {
                BinaryValues runValues = binaryValues(values.values());
                yield position -> runValues.value(values.runIndex(position));
            }
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
        };
    }

    @FunctionalInterface
    interface LongValues
    {
        long value(int position);
    }

    @FunctionalInterface
    interface BooleanValues
    {
        boolean value(int position);
    }

    @FunctionalInterface
    interface DoubleValues
    {
        double value(int position);
    }

    @FunctionalInterface
    interface BinaryValues
    {
        BinarySlice value(int position);
    }

    record BinarySlice(byte[] data, int offset, int length) {}
}
