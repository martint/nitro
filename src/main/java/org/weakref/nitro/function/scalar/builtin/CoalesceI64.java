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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "coalesce_i64")
public final class CoalesceI64
        implements PrimitiveFunction
{
    // COALESCE can be null only when both operands are null. If either input proves a null-free physical stream,
    // omit the output NULLS vector instead of walking the other (potentially nested dictionary) null mapping solely
    // to AND it with false. The evaluator's ordinary stream-completion contract supplies shared all-false metadata
    // when a downstream consumer explicitly requests NULLS.
    private static final boolean OMIT_NULLS_FOR_NON_NULL_INPUT =
            Boolean.parseBoolean(System.getProperty("nitro.coalesce.omitNullsForNonNullInput", "true"));
    private static final boolean DEBUG_OMITTED_NULLS = Boolean.getBoolean("nitro.debug.coalesceOmittedNulls");
    private boolean omittedNullsReported;

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAndNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for coalesce_i64");
        boolean requestValues = requestedStreams.contains(Stream.VALUES);
        boolean requestNulls = requestedStreams.contains(Stream.NULLS);
        if (!requestValues && !requestNulls) {
            return Streams.empty();
        }

        Allocator.Context allocationContext = context.allocationContext("CoalesceI64");
        Vector primaryNullVector = inputs.get(0).getOrNull(Stream.NULLS);
        Vector fallbackNullVector = inputs.get(1).getOrNull(Stream.NULLS);
        VectorAccess.LongValues primaryValues = VectorAccess.longValues(inputs.get(0).values());
        VectorAccess.LongValues fallbackValues = VectorAccess.longValues(inputs.get(1).values());
        VectorAccess.BooleanValues primaryNulls = VectorAccess.booleanValues(primaryNullVector);
        VectorAccess.BooleanValues fallbackNulls = VectorAccess.booleanValues(fallbackNullVector);
        Vector primaryValueVector = inputs.get(0).values();
        Vector fallbackValueVector = inputs.get(1).values();
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(primaryValueVector.length(), fallbackValueVector.length()));

        Streams result = Streams.empty();
        boolean resultCannotBeNull = OMIT_NULLS_FOR_NON_NULL_INPUT &&
                (VectorAccess.isAllFalseNulls(primaryNullVector) || VectorAccess.isAllFalseNulls(fallbackNullVector));
        if (resultCannotBeNull && DEBUG_OMITTED_NULLS && !omittedNullsReported) {
            omittedNullsReported = true;
            System.err.printf(
                    "[coalesce-omitted-nulls] primary=%s fallback=%s rows=%d%n",
                    primaryNullVector == null ? "none" : primaryNullVector.getClass().getSimpleName(),
                    fallbackNullVector == null ? "none" : fallbackNullVector.getClass().getSimpleName(),
                    mask.count());
        }
        if (requestNulls && !resultCannotBeNull) {
            BooleanVector nulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null ? output.getOrNull(Stream.NULLS) : null,
                    requiredLength);
            boolean[] nullValues = nulls.values();
            for (int position : mask) {
                nullValues[position] = primaryNulls.value(position) && fallbackNulls.value(position);
            }
            result = result.with(Stream.NULLS, nulls);
        }
        if (!requestValues) {
            return result;
        }

        I64Vector values = context.allocator().allocateOrGrow(
                allocationContext,
                output != null && output.getOrNull(Stream.VALUES) instanceof I64Vector vector ? vector : null,
                I64Vector.class,
                requiredLength,
                I64Vector::new);
        long[] outputValues = values.values();
        for (int position : mask) {
            outputValues[position] = primaryNulls.value(position) ? fallbackValues.value(position) : primaryValues.value(position);
        }
        return result.with(Stream.VALUES, values);
    }
}
