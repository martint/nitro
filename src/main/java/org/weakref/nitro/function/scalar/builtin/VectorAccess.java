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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.ConcatenatedBooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.SelectedPositions;
import org.weakref.nitro.data.SelectionVector;
import org.weakref.nitro.data.Vector;

public final class VectorAccess
{
    private VectorAccess() {}

    public static LongValues longValues(Vector vector)
    {
        return switch (vector) {
            case I64Vector values -> position -> values.values()[position];
            case I32Vector values -> position -> values.values()[position];
            case DictionaryVector values -> {
                int[] ids = values.ids();
                yield switch (values.values()) {
                    case I64Vector dictionaryValues -> position -> dictionaryValues.values()[ids[position]];
                    case I32Vector dictionaryValues -> position -> dictionaryValues.values()[ids[position]];
                    default -> {
                        LongValues dictionaryValues = longValues(values.values());
                        yield position -> dictionaryValues.value(ids[position]);
                    }
                };
            }
            case SelectionVector values -> {
                yield switch (values.values()) {
                    case I64Vector selectedValues -> longSelectionValues(values.positions(), selectedValues.values());
                    case I32Vector selectedValues -> intSelectionValues(values.positions(), selectedValues.values());
                    default -> {
                        LongValues selectedValues = longValues(values.values());
                        yield position -> selectedValues.value(values.positions().position(position));
                    }
                };
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
            case ConcatenatedBooleanVector values -> values::value;
            case DictionaryVector values -> {
                int[] ids = values.ids();
                yield switch (values.values()) {
                    case BooleanVector dictionaryValues -> position -> dictionaryValues.values()[ids[position]];
                    case ConcatenatedBooleanVector dictionaryValues -> position -> dictionaryValues.value(ids[position]);
                    default -> {
                        BooleanValues dictionaryValues = booleanValues(values.values());
                        yield position -> dictionaryValues.value(ids[position]);
                    }
                };
            }
            case SelectionVector values -> {
                yield switch (values.values()) {
                    case BooleanVector selectedValues -> booleanSelectionValues(values.positions(), selectedValues.values());
                    case ConcatenatedBooleanVector selectedValues -> concatenatedBooleanSelectionValues(values.positions(), selectedValues);
                    default -> {
                        BooleanValues selectedValues = booleanValues(values.values());
                        yield position -> selectedValues.value(values.positions().position(position));
                    }
                };
            }
            case RleVector values -> {
                BooleanValues runValues = booleanValues(values.values());
                yield position -> runValues.value(values.runIndex(position));
            }
            default -> throw new IllegalArgumentException("Expected boolean-backed null vector but found " + vector.getClass().getSimpleName());
        };
    }

    public static boolean isNull(Vector nulls, int position)
    {
        return booleanValues(nulls).value(position);
    }

    public static BooleanVector writableBooleanVector(Allocator allocator, Allocator.Context allocationContext, Vector existing, int size)
    {
        if (existing instanceof BooleanVector vector) {
            return allocator.allocateOrGrow(allocationContext, vector, BooleanVector.class, size, BooleanVector::new);
        }

        BooleanVector result = allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
        if (existing != null) {
            int copied = Math.min(existing.length(), size);
            int[] positions = new int[copied];
            for (int position = 0; position < copied; position++) {
                positions[position] = position;
            }
            try {
                existing.copyPositionsInto(allocator, allocationContext, result, positions, copied, 0, size);
            }
            catch (UnsupportedOperationException ignored) {
                BooleanValues existingValues = booleanValues(existing);
                for (int position = 0; position < copied; position++) {
                    result.values()[position] = existingValues.value(position);
                }
            }
        }
        return result;
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
            case SelectionVector values -> {
                DoubleValues selectedValues = doubleValues(values.values());
                yield position -> selectedValues.value(values.positions().position(position));
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
            case SelectionVector values -> {
                BinaryValues selectedValues = binaryValues(values.values());
                yield position -> selectedValues.value(values.positions().position(position));
            }
            case RleVector values -> {
                BinaryValues runValues = binaryValues(values.values());
                yield position -> runValues.value(values.runIndex(position));
            }
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
        };
    }

    @FunctionalInterface
    public interface LongValues
    {
        long value(int position);
    }

    @FunctionalInterface
    public interface BooleanValues
    {
        boolean value(int position);
    }

    @FunctionalInterface
    public interface DoubleValues
    {
        double value(int position);
    }

    @FunctionalInterface
    public interface BinaryValues
    {
        BinarySlice value(int position);
    }

    public record BinarySlice(byte[] data, int offset, int length) {}

    private static LongValues longSelectionValues(SelectedPositions positions, long[] values)
    {
        int[] array = positions.backingArrayOrNull();
        if (array != null) {
            int offset = positions.backingArrayOffset();
            return position -> values[array[offset + position]];
        }
        return position -> values[positions.position(position)];
    }

    private static LongValues intSelectionValues(SelectedPositions positions, int[] values)
    {
        int[] array = positions.backingArrayOrNull();
        if (array != null) {
            int offset = positions.backingArrayOffset();
            return position -> values[array[offset + position]];
        }
        return position -> values[positions.position(position)];
    }

    private static BooleanValues booleanSelectionValues(SelectedPositions positions, boolean[] values)
    {
        int[] array = positions.backingArrayOrNull();
        if (array != null) {
            int offset = positions.backingArrayOffset();
            return position -> values[array[offset + position]];
        }
        return position -> values[positions.position(position)];
    }

    private static BooleanValues concatenatedBooleanSelectionValues(SelectedPositions positions, ConcatenatedBooleanVector values)
    {
        int[] array = positions.backingArrayOrNull();
        if (array != null) {
            int offset = positions.backingArrayOffset();
            return position -> values.value(array[offset + position]);
        }
        return position -> values.value(positions.position(position));
    }
}
