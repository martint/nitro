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
package org.weakref.nitro.data;

import java.util.Arrays;

public class BooleanVector
        implements FlatVector
{
    private final boolean[] values;
    // Lazily computed "are all entries false" flag. null = unknown (caller must scan), TRUE/FALSE = known.
    // Reset to null on clearForReuse since the producer may write new values after reuse. Producers that
    // directly mutate {@link #values()} after publication invalidate this cache; in the Nitro codebase
    // BooleanVector instances are treated as immutable after they're exposed via {@link Streams}, so this
    // is safe. See {@link org.weakref.nitro.function.scalar.builtin.VectorAccess#isAllFalseNulls}.
    private Boolean isAllFalseCache;

    public BooleanVector(int size)
    {
        this(new boolean[size]);
    }

    public BooleanVector(boolean[] values)
    {
        this.values = values;
    }

    public boolean[] values()
    {
        return values;
    }

    /**
     * Returns true when every entry in the backing array is {@code false}. First call scans the array
     * in O(n); subsequent calls return the cached result in O(1). Intended for use as a pre-flight check
     * by scalar null-propagation code paths that want to skip their per-position loop when both inputs'
     * NULLS vectors carry no nulls.
     *
     * <p>Callers that mutate {@link #values()} after publication must not rely on this cache; the Nitro
     * convention is that vectors are immutable once returned via {@link Streams}.
     */
    public boolean isAllFalse()
    {
        Boolean cached = isAllFalseCache;
        if (cached != null) {
            return cached;
        }
        for (boolean value : values) {
            if (value) {
                isAllFalseCache = Boolean.FALSE;
                return false;
            }
        }
        isAllFalseCache = Boolean.TRUE;
        return true;
    }

    /**
     * Records that every element is false without scanning. The caller guarantees the backing array is
     * all-false and will not be mutated afterward — Nitro treats a {@link BooleanVector} as immutable
     * once it has been published through {@link org.weakref.nitro.operator.Streams}. Lets producers of a
     * known all-false stream make {@link #isAllFalse()} (and hence
     * {@link org.weakref.nitro.function.scalar.builtin.VectorAccess#isAllFalseNulls}) O(1).
     */
    public void markAllFalse()
    {
        isAllFalseCache = Boolean.TRUE;
    }

    @Override
    public int length()
    {
        return values.length;
    }

    @Override
    public long retainedBytes()
    {
        return values.length;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        BooleanVector copy = allocator.allocate(allocationContext, BooleanVector.class, values.length, BooleanVector::new);
        copyInto(copy);
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        BooleanVector copy = allocator.allocate(allocationContext, BooleanVector.class, positions.length, BooleanVector::new);
        for (int index = 0; index < positions.length; index++) {
            copy.values()[index] = values[positions[index]];
        }
        return copy;
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        BooleanVector target = allocator.allocateOrGrow(allocationContext, (BooleanVector) existing, BooleanVector.class, values.length, BooleanVector::new);
        for (int position : mask) {
            target.values()[position] = values[position];
        }
        return target;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        BooleanVector target = allocator.allocateOrGrow(allocationContext, (BooleanVector) existing, BooleanVector.class, size, BooleanVector::new);
        for (int index = 0; index < sourceCount; index++) {
            target.values()[outputStart + index] = values[sourcePositions[index]];
        }
        return target;
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        BooleanVector target = allocator.allocateOrGrow(allocationContext, (BooleanVector) existing, BooleanVector.class, size, BooleanVector::new);
        target.values()[outputPosition] = values[sourcePosition];
        return target;
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        return allocator.allocate(allocationContext, BooleanVector.class, 0, BooleanVector::new);
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        BooleanVector result = allocator.allocate(allocationContext, BooleanVector.class, VectorSupport.totalLength(rows), BooleanVector::new);
        int outputStart = 0;
        for (Vector row : rows) {
            int rowLength = row.length();
            if (rowLength == 1) {
                row.copySinglePositionInto(allocator, allocationContext, result, 0, outputStart, result.length());
            }
            else if (rowLength > 1) {
                row.copyPositionsInto(allocator, allocationContext, result, VectorSupport.densePositions(rowLength), rowLength, outputStart, result.length());
            }
            outputStart += rowLength;
        }
        return result;
    }

    @Override
    public void copyInto(Vector target)
    {
        System.arraycopy(values, 0, ((BooleanVector) target).values(), 0, values.length);
    }

    @Override
    public void clearForReuse()
    {
        Arrays.fill(values, false);
        // The next producer will write new values; force callers to re-scan rather than reuse our stale
        // "all false" observation from a previous lifecycle.
        isAllFalseCache = null;
    }

    @Override
    public Object poolFamily()
    {
        return BooleanVector.class;
    }

    @Override
    public int poolCapacity()
    {
        return length();
    }

    @Override
    public int poolMaxRetained()
    {
        return 2;
    }

    @Override
    public String toString()
    {
        return "BooleanVector" + Arrays.toString(values);
    }
}
