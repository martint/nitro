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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
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
            case RleVector values -> {
                LongValues runValues = longValues(values.values());
                int[] hint = {0};
                yield position -> {
                    int runIndex = values.runIndexFromHint(position, hint[0]);
                    hint[0] = runIndex;
                    return runValues.value(runIndex);
                };
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
            case DictionaryVector values -> dictionaryBooleanValues(values);
            case RleVector values -> {
                BooleanValues runValues = booleanValues(values.values());
                int[] hint = {0};
                yield position -> {
                    int runIndex = values.runIndexFromHint(position, hint[0]);
                    hint[0] = runIndex;
                    return runValues.value(runIndex);
                };
            }
            default -> throw new IllegalArgumentException("Expected boolean-backed null vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static BooleanValues dictionaryBooleanValues(DictionaryVector vector)
    {
        int depth = 0;
        Vector current = vector;
        while (current instanceof DictionaryVector dictionary) {
            depth++;
            current = dictionary.values();
        }

        int[][] ids = new int[depth][];
        current = vector;
        for (int index = 0; index < depth; index++) {
            DictionaryVector dictionary = (DictionaryVector) current;
            ids[index] = dictionary.ids();
            current = dictionary.values();
        }

        BooleanValues values = booleanValues(current);
        return position -> {
            for (int[] mapping : ids) {
                position = mapping[position];
            }
            return values.value(position);
        };
    }

    public static boolean isNull(Vector nulls, int position)
    {
        return booleanValues(nulls).value(position);
    }

    /**
     * Returns true when {@code nulls} represents "no rows are null" — i.e. the reference is null,
     * or is a run-length-encoded vector whose single run is false, or is a flat boolean vector of
     * length one with value false. Scalar function implementations can use this to short-circuit
     * null-propagation loops when both of their inputs are known to be null-free (the common case
     * for literals, column outputs from scans of non-nullable columns, and outputs of aggregates
     * that never produce nulls). When both inputs test true, the scalar should omit NULLS from
     * its output; {@code PlanEvaluator.completeRequestedStreams} will fill the caller's requested
     * NULLS with an all-false stream if needed.
     */
    public static boolean isAllFalseNulls(Vector nulls)
    {
        if (nulls == null) {
            return true;
        }
        if (nulls instanceof RleVector rle && rle.values() instanceof BooleanVector runValues && runValues.length() == 1) {
            return !runValues.values()[0];
        }
        if (nulls instanceof BooleanVector flat) {
            // O(n) on first call, O(1) thereafter. See BooleanVector.isAllFalse() for the caching
            // contract. This lets upstream operators (aggregations, scans) declare "no rows are null"
            // without allocating a length-1 representation: a plain flat vector of zeros is detected
            // and downstream scalar null-propagation loops short-circuit.
            return flat.isAllFalse();
        }
        if (nulls instanceof DictionaryVector dictionary) {
            // Any ids over an all-false dictionary select only false, so the stream is all-false.
            // (Conservative: a false here may still be effectively all-false, but never the reverse.)
            return isAllFalseNulls(dictionary.values());
        }
        if (nulls instanceof ConcatenatedBooleanVector concatenated) {
            for (int index = 0; index < concatenated.segmentCount(); index++) {
                if (!isAllFalseNulls(concatenated.segment(index))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Computes {@code outputNulls[p] = isNull(leftNulls, p) || isNull(rightNulls, p)} for each selected
     * position (others false) — the standard null propagation for a binary scalar function. Specializes
     * the common cases: a side known to be null-free is skipped entirely, and a lone flat null vector is
     * copied in bulk rather than read through a per-position accessor, avoiding the megamorphic
     * {@link #booleanValues} lambda when only one operand can actually be null.
     */
    public static void combineNullsOr(Vector leftNulls, Vector rightNulls, Mask mask, BooleanVector outputNulls)
    {
        boolean[] target = outputNulls.values();
        int length = outputNulls.length();
        boolean leftFree = isAllFalseNulls(leftNulls);
        boolean rightFree = isAllFalseNulls(rightNulls);
        if (leftFree) {
            copyNulls(rightNulls, rightFree, mask, target, length);
            return;
        }
        if (rightFree) {
            copyNulls(leftNulls, false, mask, target, length);
            return;
        }
        java.util.Arrays.fill(target, 0, length, false);
        BooleanValues left = booleanValues(leftNulls);
        BooleanValues right = booleanValues(rightNulls);
        for (int position : mask) {
            target[position] = left.value(position) || right.value(position);
        }
    }

    private static void copyNulls(Vector nulls, boolean allFalse, Mask mask, boolean[] target, int length)
    {
        if (allFalse) {
            java.util.Arrays.fill(target, 0, length, false);
            return;
        }
        if (nulls instanceof BooleanVector flat && mask.all() && flat.values().length >= length) {
            System.arraycopy(flat.values(), 0, target, 0, length);
            return;
        }
        java.util.Arrays.fill(target, 0, length, false);
        BooleanValues values = booleanValues(nulls);
        for (int position : mask) {
            target[position] = values.value(position);
        }
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
            case RleVector values -> {
                DoubleValues runValues = doubleValues(values.values());
                int[] hint = {0};
                yield position -> {
                    int runIndex = values.runIndexFromHint(position, hint[0]);
                    hint[0] = runIndex;
                    return runValues.value(runIndex);
                };
            }
            default -> throw new IllegalArgumentException("Expected double vector but found " + vector.getClass().getSimpleName());
        };
    }

    /** The backing {@code double[]} when {@code vector} is a plain flat {@link F64Vector}, else {@code null}. */
    public static double[] flatDoubles(Vector vector)
    {
        return vector instanceof F64Vector values ? values.values() : null;
    }

    /** The raw backing array of a flat boolean vector (e.g. a null/error stream), or null for any other encoding. */
    public static boolean[] flatBooleans(Vector vector)
    {
        return vector instanceof BooleanVector values ? values.values() : null;
    }

    /**
     * True when {@code vector} is a single-value broadcast of a double — a literal materializes as an
     * {@link RleVector} with one run over a length-one {@link F64Vector}. Lets a binary kernel hoist a
     * constant operand out of the row loop instead of reading it through the megamorphic
     * {@link #doubleValues} accessor per element.
     */
    public static boolean isConstantDouble(Vector vector)
    {
        return vector instanceof RleVector rle
                && rle.counts().length == 1
                && rle.values() instanceof F64Vector flat
                && flat.length() == 1;
    }

    public static double constantDouble(Vector vector)
    {
        return ((F64Vector) ((RleVector) vector).values()).values()[0];
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
                int[] hint = {0};
                yield position -> {
                    int runIndex = values.runIndexFromHint(position, hint[0]);
                    hint[0] = runIndex;
                    return runValues.value(runIndex);
                };
            }
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
        };
    }

    /**
     * Allocation-free binary access for hot batch kernels. Unlike {@link BinaryValues#value(int)}, this exposes the
     * three slice components independently so a caller does not create one {@link BinarySlice} object per row. Vector
     * representation is resolved once when the accessor is created.
     */
    public static BinaryRegions binaryRegions(Vector vector)
    {
        return switch (vector) {
            case BinaryVector values -> new BinaryRegions()
            {
                @Override
                public byte[] data(int position)
                {
                    return values.data();
                }

                @Override
                public int offset(int position)
                {
                    return values.startOffset(position);
                }

                @Override
                public int length(int position)
                {
                    return values.length(position);
                }
            };
            case DictionaryVector values -> {
                BinaryRegions dictionaryValues = binaryRegions(values.values());
                int[] ids = values.ids();
                yield new BinaryRegions()
                {
                    @Override
                    public byte[] data(int position)
                    {
                        return dictionaryValues.data(ids[position]);
                    }

                    @Override
                    public int offset(int position)
                    {
                        return dictionaryValues.offset(ids[position]);
                    }

                    @Override
                    public int length(int position)
                    {
                        return dictionaryValues.length(ids[position]);
                    }
                };
            }
            case RleVector values -> {
                BinaryRegions runValues = binaryRegions(values.values());
                int[] hint = {0};
                yield new BinaryRegions()
                {
                    private int run(int position)
                    {
                        int run = values.runIndexFromHint(position, hint[0]);
                        hint[0] = run;
                        return run;
                    }

                    @Override
                    public byte[] data(int position)
                    {
                        return runValues.data(run(position));
                    }

                    @Override
                    public int offset(int position)
                    {
                        return runValues.offset(run(position));
                    }

                    @Override
                    public int length(int position)
                    {
                        return runValues.length(run(position));
                    }
                };
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

    public interface BinaryRegions
    {
        byte[] data(int position);

        int offset(int position);

        int length(int position);
    }

    public record BinarySlice(byte[] data, int offset, int length) {}
}
