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

import static java.util.Objects.requireNonNull;

/**
 * Boolean-compatible error-presence vector with optional provider-owned
 * diagnostics for failing positions.
 */
public final class ErrorVector
        extends BooleanVector
{
    private final ErrorValue[] errors;

    public ErrorVector(int size)
    {
        super(size);
        errors = new ErrorValue[size];
    }

    public ErrorValue error(int position)
    {
        return errors[position];
    }

    public void setError(int position, ErrorValue error)
    {
        errors[position] = requireNonNull(error, "error is null");
        values()[position] = true;
    }

    public void clearError(int position)
    {
        errors[position] = null;
        values()[position] = false;
    }

    @Override
    public void markAllFalse()
    {
        super.markAllFalse();
        Arrays.fill(errors, null);
    }

    @Override
    public void markAllTrue()
    {
        super.markAllTrue();
        Arrays.fill(errors, null);
    }

    @Override
    public long retainedBytes()
    {
        return super.retainedBytes() + (long) errors.length * Long.BYTES;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        ErrorVector copy = allocator.allocate(allocationContext, ErrorVector.class, length(), ErrorVector::new);
        copyInto(copy);
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        ErrorVector copy = allocator.allocate(allocationContext, ErrorVector.class, positions.length, ErrorVector::new);
        for (int index = 0; index < positions.length; index++) {
            copyPosition(copy, positions[index], index);
        }
        return copy;
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        ErrorVector target = writable(allocator, allocationContext, existing, length());
        for (int position : mask) {
            copyPosition(target, position, position);
        }
        return target;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        ErrorVector target = writable(allocator, allocationContext, existing, size);
        for (int index = 0; index < sourceCount; index++) {
            copyPosition(target, sourcePositions[index], outputStart + index);
        }
        return target;
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        ErrorVector target = writable(allocator, allocationContext, existing, size);
        copyPosition(target, sourcePosition, outputPosition);
        return target;
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        return allocator.allocate(allocationContext, ErrorVector.class, 0, ErrorVector::new);
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        ErrorVector result = allocator.allocate(allocationContext, ErrorVector.class, VectorSupport.totalLength(rows), ErrorVector::new);
        int outputStart = 0;
        for (Vector row : rows) {
            if (!(row instanceof ErrorVector errors)) {
                throw new IllegalArgumentException("ErrorVector rows must have the same representation");
            }
            for (int position = 0; position < errors.length(); position++) {
                errors.copyPosition(result, position, outputStart++);
            }
        }
        return result;
    }

    @Override
    public void copyInto(Vector target)
    {
        if (!(target instanceof ErrorVector errorTarget)) {
            throw new IllegalArgumentException("ErrorVector target must have the same representation");
        }
        super.copyInto(errorTarget);
        System.arraycopy(errors, 0, errorTarget.errors, 0, errors.length);
    }

    @Override
    public void clearForReuse()
    {
        super.clearForReuse();
        Arrays.fill(errors, null);
    }

    @Override
    public Object poolFamily()
    {
        return ErrorVector.class;
    }

    private void copyPosition(ErrorVector target, int sourcePosition, int targetPosition)
    {
        target.values()[targetPosition] = values()[sourcePosition];
        target.errors[targetPosition] = errors[sourcePosition];
    }

    private static ErrorVector writable(Allocator allocator, Allocator.Context allocationContext, Vector existing, int size)
    {
        ErrorVector target = existing instanceof ErrorVector errors ? errors : null;
        return allocator.allocateOrGrow(allocationContext, target, ErrorVector.class, size, ErrorVector::new);
    }
}
