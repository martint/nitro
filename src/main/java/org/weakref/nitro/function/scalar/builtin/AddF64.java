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
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "add_f64", capabilities = AddF64Optimization.class)
public final class AddF64
        implements PrimitiveFunction
{
    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAlwaysNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for add_f64");
        boolean requestValues = requestedStreams.contains(Stream.VALUES);
        boolean requestNulls = requestedStreams.contains(Stream.NULLS);
        if (!requestValues && !requestNulls) {
            return Streams.empty();
        }

        Allocator.Context allocationContext = context.allocationContext("AddF64");
        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();

        if (requestValues) {
            F64Vector fast = NullFreeScalarKernels.arithmeticDouble(NullFreeScalarKernels.ADD, left, right, inputs.get(0).getOrNull(Stream.NULLS), inputs.get(1).getOrNull(Stream.NULLS), mask, output != null ? output.getOrNull(Stream.VALUES) : null, context.allocator(), allocationContext);
            if (fast != null) {
                return Streams.ofValues(fast);
            }
        }
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(left.length(), right.length()));

        Streams result = Streams.empty();
        if (requestNulls) {
            BooleanVector nulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null ? output.getOrNull(Stream.NULLS) : null,
                    requiredLength);
            VectorAccess.combineNullsOr(inputs.get(0).getOrNull(Stream.NULLS), inputs.get(1).getOrNull(Stream.NULLS), mask, nulls);
            result = result.with(Stream.NULLS, nulls);
        }
        if (!requestValues) {
            return result;
        }

        F64Vector values = context.allocator().allocateOrGrow(
                allocationContext,
                output != null && output.getOrNull(Stream.VALUES) instanceof F64Vector vector ? vector : null,
                F64Vector.class,
                requiredLength,
                F64Vector::new);
        double[] out = values.values();

        // Monomorphic, encoding-specialized kernels: decide operand shapes once, then run a tight typed loop
        // (the all-selected case auto-vectorizes). Only the rare general case uses the per-element accessor.
        double[] l = VectorAccess.flatDoubles(left);
        double[] r = VectorAccess.flatDoubles(right);
        int[] selected = mask.selectedPositions();
        if (l != null && r != null) {
            if (selected == null) {
                int n = mask.size();
                for (int i = 0; i < n; i++) {
                    out[i] = l[i] + r[i];
                }
            }
            else {
                int n = mask.selectedCount();
                for (int k = 0; k < n; k++) {
                    int p = selected[k];
                    out[p] = l[p] + r[p];
                }
            }
        }
        else if (l != null && VectorAccess.isConstantDouble(right)) {
            double c = VectorAccess.constantDouble(right);
            if (selected == null) {
                int n = mask.size();
                for (int i = 0; i < n; i++) {
                    out[i] = l[i] + c;
                }
            }
            else {
                int n = mask.selectedCount();
                for (int k = 0; k < n; k++) {
                    int p = selected[k];
                    out[p] = l[p] + c;
                }
            }
        }
        else if (r != null && VectorAccess.isConstantDouble(left)) {
            double c = VectorAccess.constantDouble(left);
            if (selected == null) {
                int n = mask.size();
                for (int i = 0; i < n; i++) {
                    out[i] = c + r[i];
                }
            }
            else {
                int n = mask.selectedCount();
                for (int k = 0; k < n; k++) {
                    int p = selected[k];
                    out[p] = c + r[p];
                }
            }
        }
        else {
            VectorAccess.DoubleValues leftValues = VectorAccess.doubleValues(left);
            VectorAccess.DoubleValues rightValues = VectorAccess.doubleValues(right);
            for (int position : mask) {
                out[position] = leftValues.value(position) + rightValues.value(position);
            }
        }
        return result.with(Stream.VALUES, values);
    }
}
