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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;

/**
 * Monomorphic null-free fast paths for the binary numeric scalar kernels. When both operands are flat arrays (or a
 * single-run RLE constant) and neither carries nulls -- the common projection/filter shape -- each operation runs a
 * tight per-element loop over the backing {@code long[]}/{@code double[]} that C2 auto-vectorizes. The operator is
 * switched <em>outside</em> the loop so every loop body has a single visible operator (no megamorphic per-position
 * {@code VectorAccess} accessor dispatch, which the shared interpreter path pays and which defeats vectorization).
 * <p>
 * Returns {@code null} when the fast path does not apply (mask not full, an operand encoded some other way, or any
 * null present), so the caller falls back to its general path. A null-free result omits its NULLS stream, matching the
 * interpreter; the evaluator synthesizes an all-false companion downstream if one is requested.
 */
final class NullFreeScalarKernels
{
    private NullFreeScalarKernels() {}

    static final int LESS_THAN = 0;
    static final int GREATER_THAN = 1;
    static final int LESS_THAN_OR_EQUAL = 2;
    static final int GREATER_THAN_OR_EQUAL = 3;
    static final int EQUAL = 4;

    static final int ADD = 0;
    static final int SUBTRACT = 1;
    static final int MULTIPLY = 2;

    private static boolean applicable(Vector leftNulls, Vector rightNulls, Mask mask, Vector existing)
    {
        return mask.all() && existing == null
                && VectorAccess.isAllFalseNulls(leftNulls) && VectorAccess.isAllFalseNulls(rightNulls);
    }

    private static int length(Mask mask, Vector left, Vector right)
    {
        return I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length()));
    }

    private static Long constantLong(Vector vector)
    {
        if (vector instanceof RleVector rle && rle.counts().length == 1) {
            return VectorAccess.longValues(rle.values()).value(0);
        }
        return null;
    }

    private static Double constantDouble(Vector vector)
    {
        if (vector instanceof RleVector rle && rle.counts().length == 1) {
            return VectorAccess.doubleValues(rle.values()).value(0);
        }
        return null;
    }

    static BooleanVector compareLong(int op, Vector left, Vector right, Vector leftNulls, Vector rightNulls, Mask mask, Vector existing, Allocator allocator, Allocator.Context context)
    {
        if (!applicable(leftNulls, rightNulls, mask, existing)) {
            return null;
        }
        long[] leftFlat = left instanceof I64Vector v ? v.values() : null;
        long[] rightFlat = right instanceof I64Vector v ? v.values() : null;
        if (leftFlat == null && rightFlat == null) {
            return null;
        }
        long leftConstant = 0;
        long rightConstant = 0;
        if (leftFlat == null) {
            Long constant = constantLong(left);
            if (constant == null) {
                return null;
            }
            leftConstant = constant;
        }
        if (rightFlat == null) {
            Long constant = constantLong(right);
            if (constant == null) {
                return null;
            }
            rightConstant = constant;
        }
        int allocated = length(mask, left, right);
        int length = mask.maxPosition() + 1;
        BooleanVector output = allocator.allocate(context, BooleanVector.class, allocated, BooleanVector::new);
        boolean[] out = output.values();
        if (leftFlat != null && rightFlat != null) {
            compareLongFlatFlat(op, leftFlat, rightFlat, out, length);
        }
        else if (leftFlat != null) {
            compareLongFlatConstant(op, leftFlat, rightConstant, out, length);
        }
        else {
            compareLongConstantFlat(op, leftConstant, rightFlat, out, length);
        }
        return output;
    }

    static BooleanVector compareDouble(int op, Vector left, Vector right, Vector leftNulls, Vector rightNulls, Mask mask, Vector existing, Allocator allocator, Allocator.Context context)
    {
        if (!applicable(leftNulls, rightNulls, mask, existing)) {
            return null;
        }
        double[] leftFlat = left instanceof F64Vector v ? v.values() : null;
        double[] rightFlat = right instanceof F64Vector v ? v.values() : null;
        if (leftFlat == null && rightFlat == null) {
            return null;
        }
        double leftConstant = 0;
        double rightConstant = 0;
        if (leftFlat == null) {
            Double constant = constantDouble(left);
            if (constant == null) {
                return null;
            }
            leftConstant = constant;
        }
        if (rightFlat == null) {
            Double constant = constantDouble(right);
            if (constant == null) {
                return null;
            }
            rightConstant = constant;
        }
        int allocated = length(mask, left, right);
        int length = mask.maxPosition() + 1;
        BooleanVector output = allocator.allocate(context, BooleanVector.class, allocated, BooleanVector::new);
        boolean[] out = output.values();
        if (leftFlat != null && rightFlat != null) {
            compareDoubleFlatFlat(op, leftFlat, rightFlat, out, length);
        }
        else if (leftFlat != null) {
            compareDoubleFlatConstant(op, leftFlat, rightConstant, out, length);
        }
        else {
            compareDoubleConstantFlat(op, leftConstant, rightFlat, out, length);
        }
        return output;
    }

    static I64Vector arithmeticLong(int op, Vector left, Vector right, Vector leftNulls, Vector rightNulls, Mask mask, Vector existing, Allocator allocator, Allocator.Context context)
    {
        if (!applicable(leftNulls, rightNulls, mask, existing)) {
            return null;
        }
        long[] leftFlat = left instanceof I64Vector v ? v.values() : null;
        long[] rightFlat = right instanceof I64Vector v ? v.values() : null;
        if (leftFlat == null && rightFlat == null) {
            return null;
        }
        long leftConstant = 0;
        long rightConstant = 0;
        if (leftFlat == null) {
            Long constant = constantLong(left);
            if (constant == null) {
                return null;
            }
            leftConstant = constant;
        }
        if (rightFlat == null) {
            Long constant = constantLong(right);
            if (constant == null) {
                return null;
            }
            rightConstant = constant;
        }
        int allocated = length(mask, left, right);
        int length = mask.maxPosition() + 1;
        I64Vector output = allocator.allocate(context, I64Vector.class, allocated, I64Vector::new);
        long[] out = output.values();
        if (leftFlat != null && rightFlat != null) {
            arithmeticLongFlatFlat(op, leftFlat, rightFlat, out, length);
        }
        else if (leftFlat != null) {
            arithmeticLongFlatConstant(op, leftFlat, rightConstant, out, length);
        }
        else {
            arithmeticLongConstantFlat(op, leftConstant, rightFlat, out, length);
        }
        return output;
    }

    static F64Vector arithmeticDouble(int op, Vector left, Vector right, Vector leftNulls, Vector rightNulls, Mask mask, Vector existing, Allocator allocator, Allocator.Context context)
    {
        if (!applicable(leftNulls, rightNulls, mask, existing)) {
            return null;
        }
        double[] leftFlat = left instanceof F64Vector v ? v.values() : null;
        double[] rightFlat = right instanceof F64Vector v ? v.values() : null;
        if (leftFlat == null && rightFlat == null) {
            return null;
        }
        double leftConstant = 0;
        double rightConstant = 0;
        if (leftFlat == null) {
            Double constant = constantDouble(left);
            if (constant == null) {
                return null;
            }
            leftConstant = constant;
        }
        if (rightFlat == null) {
            Double constant = constantDouble(right);
            if (constant == null) {
                return null;
            }
            rightConstant = constant;
        }
        int allocated = length(mask, left, right);
        int length = mask.maxPosition() + 1;
        F64Vector output = allocator.allocate(context, F64Vector.class, allocated, F64Vector::new);
        double[] out = output.values();
        if (leftFlat != null && rightFlat != null) {
            arithmeticDoubleFlatFlat(op, leftFlat, rightFlat, out, length);
        }
        else if (leftFlat != null) {
            arithmeticDoubleFlatConstant(op, leftFlat, rightConstant, out, length);
        }
        else {
            arithmeticDoubleConstantFlat(op, leftConstant, rightFlat, out, length);
        }
        return output;
    }

    // ---- monomorphic loops (operator switched outside the loop) ---------------

    private static void compareLongFlatFlat(int op, long[] left, long[] right, boolean[] out, int n)
    {
        switch (op) {
            case LESS_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] < right[i];
                }
            }
            case GREATER_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] > right[i];
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] <= right[i];
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] >= right[i];
                }
            }
            case EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] == right[i];
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }

    private static void compareLongFlatConstant(int op, long[] left, long right, boolean[] out, int n)
    {
        switch (op) {
            case LESS_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] < right;
                }
            }
            case GREATER_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] > right;
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] <= right;
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] >= right;
                }
            }
            case EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] == right;
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }

    private static void compareLongConstantFlat(int op, long left, long[] right, boolean[] out, int n)
    {
        switch (op) {
            case LESS_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left < right[i];
                }
            }
            case GREATER_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left > right[i];
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left <= right[i];
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left >= right[i];
                }
            }
            case EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left == right[i];
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }

    private static void compareDoubleFlatFlat(int op, double[] left, double[] right, boolean[] out, int n)
    {
        switch (op) {
            case LESS_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] < right[i];
                }
            }
            case GREATER_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] > right[i];
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] <= right[i];
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] >= right[i];
                }
            }
            case EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] == right[i];
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }

    private static void compareDoubleFlatConstant(int op, double[] left, double right, boolean[] out, int n)
    {
        switch (op) {
            case LESS_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] < right;
                }
            }
            case GREATER_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] > right;
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] <= right;
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] >= right;
                }
            }
            case EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] == right;
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }

    private static void compareDoubleConstantFlat(int op, double left, double[] right, boolean[] out, int n)
    {
        switch (op) {
            case LESS_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left < right[i];
                }
            }
            case GREATER_THAN -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left > right[i];
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left <= right[i];
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left >= right[i];
                }
            }
            case EQUAL -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left == right[i];
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }

    private static void arithmeticLongFlatFlat(int op, long[] left, long[] right, long[] out, int n)
    {
        switch (op) {
            case ADD -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] + right[i];
                }
            }
            case SUBTRACT -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] - right[i];
                }
            }
            case MULTIPLY -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] * right[i];
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }

    private static void arithmeticLongFlatConstant(int op, long[] left, long right, long[] out, int n)
    {
        switch (op) {
            case ADD -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] + right;
                }
            }
            case SUBTRACT -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] - right;
                }
            }
            case MULTIPLY -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] * right;
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }

    private static void arithmeticLongConstantFlat(int op, long left, long[] right, long[] out, int n)
    {
        switch (op) {
            case ADD -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left + right[i];
                }
            }
            case SUBTRACT -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left - right[i];
                }
            }
            case MULTIPLY -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left * right[i];
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }

    private static void arithmeticDoubleFlatFlat(int op, double[] left, double[] right, double[] out, int n)
    {
        switch (op) {
            case ADD -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] + right[i];
                }
            }
            case SUBTRACT -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] - right[i];
                }
            }
            case MULTIPLY -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] * right[i];
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }

    private static void arithmeticDoubleFlatConstant(int op, double[] left, double right, double[] out, int n)
    {
        switch (op) {
            case ADD -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] + right;
                }
            }
            case SUBTRACT -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] - right;
                }
            }
            case MULTIPLY -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left[i] * right;
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }

    private static void arithmeticDoubleConstantFlat(int op, double left, double[] right, double[] out, int n)
    {
        switch (op) {
            case ADD -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left + right[i];
                }
            }
            case SUBTRACT -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left - right[i];
                }
            }
            case MULTIPLY -> {
                for (int i = 0; i < n; i++) {
                    out[i] = left * right[i];
                }
            }
            default -> throw new IllegalArgumentException("op " + op);
        }
    }
}
