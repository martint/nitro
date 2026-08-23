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
        invalidateContentSummary();
        errors[position] = requireNonNull(error, "error is null");
        values()[position] = true;
    }

    public void clearError(int position)
    {
        invalidateContentSummary();
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
    public long contentFingerprint()
    {
        long hash = super.contentFingerprint();
        for (ErrorValue error : errors) {
            hash = (hash ^ (error == null ? 0 : error.hashCode())) * 0x100000001b3L;
        }
        return hash == NO_CONTENT_FINGERPRINT ? hash + 1 : hash;
    }

    @Override
    public boolean hasSameContent(Vector other)
    {
        return other instanceof ErrorVector errorsVector &&
                super.hasSameContent(errorsVector) &&
                Arrays.equals(errors, errorsVector.errors);
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
    public Vector copySinglePositionRangeInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputStart, int outputEnd, int size)
    {
        ErrorVector target = writable(allocator, allocationContext, existing, size);
        for (int outputPosition = outputStart; outputPosition < outputEnd; outputPosition++) {
            copyPosition(target, sourcePosition, outputPosition);
        }
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
            for (int position = 0; position < row.length(); position++) {
                boolean failed = booleanValue(row, position);
                ErrorValue error = ErrorVectors.errorAt(row, position);
                if (failed && error != null) {
                    result.setError(outputStart, error);
                }
                else {
                    result.values()[outputStart] = failed;
                }
                outputStart++;
            }
        }
        result.invalidateContentSummary();
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
        target.invalidateContentSummary();
        target.values()[targetPosition] = values()[sourcePosition];
        target.errors[targetPosition] = errors[sourcePosition];
    }

    private static ErrorVector writable(Allocator allocator, Allocator.Context allocationContext, Vector existing, int size)
    {
        ErrorVector target = existing instanceof ErrorVector errors ? errors : null;
        return allocator.allocateOrGrow(allocationContext, target, ErrorVector.class, size, ErrorVector::new);
    }

    private static boolean booleanValue(Vector vector, int position)
    {
        return switch (vector) {
            case BooleanVector values -> values.values()[position];
            case DictionaryVector values -> booleanValue(values.values(), values.ids()[position]);
            case RleVector values -> booleanValue(values.values(), values.runIndex(position));
            case ConcatenatedBooleanVector values -> values.value(position);
            default -> throw new IllegalArgumentException("ErrorVector row is not boolean-backed: " + vector.getClass().getSimpleName());
        };
    }
}
