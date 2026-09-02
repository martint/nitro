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

import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.ErrorValue;
import org.weakref.nitro.data.ErrorVector;
import org.weakref.nitro.data.ErrorVectors;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

final class WindowValueCopySupport
{
    private WindowValueCopySupport() {}

    static Streams emptyOutput(
            TypeVectorFactory vectorFactory,
            Allocator allocator,
            Allocator.Context allocationContext,
            int size,
            boolean initiallyNull)
    {
        Vector values = vectorFactory.nullValues(allocator.vectorAllocator(allocationContext), size);
        BooleanVector nulls = allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
        Arrays.fill(nulls.values(), initiallyNull);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    static Streams mutableNullOutput(
            TypeVectorFactory vectorFactory,
            Allocator allocator,
            Allocator.Context allocationContext,
            int size)
    {
        Vector placeholder = vectorFactory.nullValues(allocator.vectorAllocator(allocationContext), size);
        Vector values = null;
        try {
            values = placeholder.copySinglePositionRangeInto(
                    allocator,
                    allocationContext,
                    null,
                    0,
                    0,
                    size,
                    size);
        }
        finally {
            if (values != placeholder) {
                allocator.release(allocationContext, placeholder);
            }
        }
        BooleanVector nulls = allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
        Arrays.fill(nulls.values(), true);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    static Streams copyRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams source,
            Streams output,
            boolean outputValuesInitialized,
            int sourcePosition,
            int outputStart,
            int outputEnd,
            int outputSize)
    {
        Vector placeholder = output.values();
        Vector values = source.values().copySinglePositionRangeInto(
                allocator,
                allocationContext,
                outputValuesInitialized ? placeholder : null,
                sourcePosition,
                outputStart,
                outputEnd,
                outputSize);
        if (!outputValuesInitialized) {
            allocator.release(allocationContext, placeholder);
        }
        Vector nulls = copyOptionalBooleanRange(
                allocator,
                allocationContext,
                source.getOrNull(Stream.NULLS),
                output.getOrNull(Stream.NULLS),
                sourcePosition,
                outputStart,
                outputEnd,
                outputSize,
                false);
        Vector errors = copyOptionalBooleanRange(
                allocator,
                allocationContext,
                source.getOrNull(Stream.ERRORS),
                output.getOrNull(Stream.ERRORS),
                sourcePosition,
                outputStart,
                outputEnd,
                outputSize,
                true);
        return allocator.reuseOrCreateStreams(output, values, nulls, errors);
    }

    private static Vector copyOptionalBooleanRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            Vector source,
            Vector existing,
            int sourcePosition,
            int outputStart,
            int outputEnd,
            int outputSize,
            boolean preserveDiagnostics)
    {
        if (preserveDiagnostics && (ErrorVectors.hasDiagnostics(source) || existing instanceof ErrorVector)) {
            ErrorVector target = existing instanceof ErrorVector errors
                    ? allocator.allocateOrGrow(allocationContext, errors, ErrorVector.class, outputSize, ErrorVector::new)
                    : allocator.allocate(allocationContext, ErrorVector.class, outputSize, ErrorVector::new);
            if (existing != null && !(existing instanceof ErrorVector)) {
                VectorAccess.BooleanValues previous = VectorAccess.booleanValues(existing);
                for (int position = 0; position < outputStart; position++) {
                    target.values()[position] = previous.value(position);
                }
                allocator.release(allocationContext, existing);
            }
            copyErrorRange(source, sourcePosition, target, outputStart, outputEnd);
            return target;
        }

        BooleanVector target = VectorAccess.writableBooleanVector(allocator, allocationContext, existing, outputSize);
        if (source == null) {
            Arrays.fill(target.values(), outputStart, outputEnd, false);
            return target;
        }
        boolean value = VectorAccess.booleanValues(source).value(sourcePosition);
        Arrays.fill(target.values(), outputStart, outputEnd, value);
        return target;
    }

    private static void copyErrorRange(Vector source, int sourcePosition, ErrorVector target, int outputStart, int outputEnd)
    {
        boolean failed = source != null && VectorAccess.booleanValues(source).value(sourcePosition);
        ErrorValue error = failed ? ErrorVectors.errorAt(source, sourcePosition) : null;
        for (int position = outputStart; position < outputEnd; position++) {
            if (error != null) {
                target.setError(position, error);
            }
            else {
                target.clearError(position);
                target.values()[position] = failed;
            }
        }
    }
}
