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
import java.util.PrimitiveIterator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Boolean-compatible error-presence vector with optional provider-owned
 * diagnostics for failing positions.
 */
public final class ErrorVector
        extends BooleanVector
        implements DynamicRetainedBytesVector
{
    private ErrorValue[] errors;
    private Allocator retainedBytesAllocator;
    private Allocator.Context retainedBytesContext;

    public ErrorVector(int size)
    {
        super(size);
    }

    public ErrorValue error(int position)
    {
        return errors == null ? null : errors[position];
    }

    public void setError(int position, ErrorValue error)
    {
        invalidateContentSummary();
        ensureDiagnosticStorage()[position] = requireNonNull(error, "error is null");
        values()[position] = true;
    }

    /// Records error presence without a provider diagnostic, replacing any previous diagnostic at this position.
    public void setError(int position)
    {
        clearError(position);
        values()[position] = true;
    }

    public void clearError(int position)
    {
        invalidateContentSummary();
        if (errors != null) {
            errors[position] = null;
        }
        values()[position] = false;
    }

    @Override
    public void markAllFalse()
    {
        super.markAllFalse();
        if (errors != null) {
            Arrays.fill(errors, null);
        }
    }

    /// Clears error presence and diagnostics only at selected positions. A dense mask may cover a prefix of an
    /// oversized vector; errors outside that prefix remain intact for independently evaluated positions.
    public void clearErrors(Mask mask)
    {
        requireNonNull(mask, "mask is null");
        checkArgument(mask.size() <= length(), "Mask exceeds error vector length");
        if (mask.none()) {
            return;
        }
        if (mask.all() && mask.size() == length()) {
            markAllFalse();
            return;
        }
        invalidateContentSummary();
        if (mask.all()) {
            Arrays.fill(values(), 0, mask.size(), false);
            if (errors != null) {
                Arrays.fill(errors, 0, mask.size(), null);
            }
            return;
        }
        for (PrimitiveIterator.OfInt positions = mask.iterator(); positions.hasNext(); ) {
            int position = positions.nextInt();
            values()[position] = false;
            if (errors != null) {
                errors[position] = null;
            }
        }
    }

    @Override
    public void markAllTrue()
    {
        super.markAllTrue();
        if (errors != null) {
            Arrays.fill(errors, null);
        }
    }

    @Override
    public long retainedBytes()
    {
        return super.retainedBytes() + (errors == null ? 0 : (long) errors.length * Long.BYTES);
    }

    @Override
    public void bindRetainedBytesAccounting(Allocator allocator, Allocator.Context context)
    {
        retainedBytesAllocator = requireNonNull(allocator, "allocator is null");
        retainedBytesContext = requireNonNull(context, "context is null");
    }

    @Override
    public long contentFingerprint()
    {
        long hash = super.contentFingerprint();
        for (int position = 0; position < length(); position++) {
            ErrorValue error = errors == null ? null : errors[position];
            hash = (hash ^ (error == null ? 0 : error.hashCode())) * 0x100000001b3L;
        }
        return hash == NO_CONTENT_FINGERPRINT ? hash + 1 : hash;
    }

    @Override
    public boolean hasSameContent(Vector other)
    {
        return other instanceof ErrorVector errorsVector &&
                super.hasSameContent(errorsVector) &&
                diagnosticsEqual(errorsVector);
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
        if (errors == null) {
            if (errorTarget.errors != null) {
                Arrays.fill(errorTarget.errors, null);
            }
        }
        else {
            System.arraycopy(errors, 0, errorTarget.ensureDiagnosticStorage(), 0, errors.length);
        }
    }

    @Override
    public void clearForReuse()
    {
        super.clearForReuse();
        if (errors != null) {
            Arrays.fill(errors, null);
        }
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
        ErrorValue error = errors == null ? null : errors[sourcePosition];
        if (error != null) {
            target.ensureDiagnosticStorage()[targetPosition] = error;
        }
        else if (target.errors != null) {
            target.errors[targetPosition] = null;
        }
    }

    private ErrorValue[] ensureDiagnosticStorage()
    {
        if (errors == null) {
            long previousRetainedBytes = retainedBytes();
            errors = new ErrorValue[length()];
            if (retainedBytesAllocator != null) {
                retainedBytesAllocator.retainedBytesChanged(retainedBytesContext, this, previousRetainedBytes);
            }
        }
        return errors;
    }

    private boolean diagnosticsEqual(ErrorVector other)
    {
        if (errors == null && other.errors == null) {
            return true;
        }
        for (int position = 0; position < length(); position++) {
            if (!java.util.Objects.equals(error(position), other.error(position))) {
                return false;
            }
        }
        return true;
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
