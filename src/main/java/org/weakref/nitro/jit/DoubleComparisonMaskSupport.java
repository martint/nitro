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
package org.weakref.nitro.jit;

import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.List;

/**
 * In-place mask evaluation for {@code double} comparisons, the F64 analog of {@link LongComparisonMaskSupport}.
 * A conjunctive filter narrows the active mask term by term; letting a double comparison narrow it directly
 * avoids materializing a full boolean vector per term and classifying it back into a mask (the dominant filter
 * cost of TPC-H q06's five-way range filter).
 *
 * <p>Three shapes, fastest first: a dictionary column vs a constant (compare once per distinct entry, then a
 * per-row {@code keep[id]} lookup — predicate-over-dictionary, the win for q06's low-cardinality decimals); a
 * flat column vs a constant (monomorphic array scan); and a per-row fallback for everything else.
 */
final class DoubleComparisonMaskSupport
{
    private DoubleComparisonMaskSupport() {}

    @FunctionalInterface
    interface ComparisonKernel
    {
        boolean test(double left, double right);
    }

    static boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, ComparisonKernel kernel, Mask.ComparisonOperator operator)
    {
        if (inputs.size() != 2) {
            return false;
        }
        return tryEvaluateTrueMaskInPlace(inputs.get(0), inputs.get(1), mask, kernel, operator);
    }

    static boolean tryEvaluateTrueMaskInPlace(Streams left, Streams right, Mask mask, ComparisonKernel kernel, Mask.ComparisonOperator operator)
    {
        if (tryColumnConstantInPlace(left, right, mask, kernel, operator, true)) {
            return true;
        }
        return evaluatePerRow(left, right, mask, kernel, true);
    }

    static boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, ComparisonKernel kernel, Mask.ComparisonOperator operator)
    {
        if (inputs.size() != 2) {
            return false;
        }
        return tryEvaluateFalseMaskInPlace(inputs.get(0), inputs.get(1), mask, kernel, operator);
    }

    static boolean tryEvaluateFalseMaskInPlace(Streams left, Streams right, Mask mask, ComparisonKernel kernel, Mask.ComparisonOperator operator)
    {
        if (tryColumnConstantInPlace(left, right, mask, kernel, operator, false)) {
            return true;
        }
        return evaluatePerRow(left, right, mask, kernel, false);
    }

    private static boolean tryColumnConstantInPlace(Streams leftInput, Streams rightInput, Mask mask, ComparisonKernel kernel, Mask.ComparisonOperator baseOperator, boolean wantTrue)
    {
        if (baseOperator == null) {
            return false;
        }
        Vector left = leftInput.values();
        Vector right = rightInput.values();

        Vector column;
        double literal;
        boolean columnOnLeft;
        if (VectorAccess.isConstantDouble(right) && isDoubleColumn(left)) {
            column = left;
            literal = VectorAccess.constantDouble(right);
            columnOnLeft = true;
        }
        else if (VectorAccess.isConstantDouble(left) && isDoubleColumn(right)) {
            column = right;
            literal = VectorAccess.constantDouble(left);
            columnOnLeft = false;
        }
        else {
            return false;
        }

        if (!isNullAndErrorFree(leftInput) || !isNullAndErrorFree(rightInput)) {
            return false;
        }

        if (column instanceof DictionaryVector dictionary && dictionary.values() instanceof F64Vector entries) {
            int entryCount = entries.length();
            // Pre-evaluating every distinct entry only pays off when the dictionary is smaller than the rows it
            // describes (a join-output dictionary wraps the whole build column -- far more entries than referenced).
            if (entryCount > mask.count()) {
                return false;
            }
            double[] entryValues = entries.values();
            boolean[] keep = new boolean[entryCount];
            for (int entry = 0; entry < entryCount; entry++) {
                boolean result = columnOnLeft ? kernel.test(entryValues[entry], literal) : kernel.test(literal, entryValues[entry]);
                keep[entry] = wantTrue == result;
            }
            mask.retainDictionaryComparison(dictionary.ids(), keep);
            return true;
        }

        double[] values = VectorAccess.flatDoubles(column);
        if (values == null) {
            return false;
        }
        Mask.ComparisonOperator operator = columnOnLeft ? baseOperator : swapOperands(baseOperator);
        mask.retainConstantComparison(values, literal, wantTrue ? operator : negate(operator));
        return true;
    }

    private static boolean evaluatePerRow(Streams leftInput, Streams rightInput, Mask mask, ComparisonKernel kernel, boolean wantTrue)
    {
        VectorAccess.DoubleValues leftValues = VectorAccess.doubleValues(leftInput.values());
        VectorAccess.DoubleValues rightValues = VectorAccess.doubleValues(rightInput.values());
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(leftInput.getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(rightInput.getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(leftInput.getOrNull(Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(rightInput.getOrNull(Stream.ERRORS));
        mask.retainIf(position -> {
            if (leftErrors.value(position) || rightErrors.value(position) || leftNulls.value(position) || rightNulls.value(position)) {
                return false;
            }
            return wantTrue == kernel.test(leftValues.value(position), rightValues.value(position));
        });
        return true;
    }

    private static boolean isDoubleColumn(Vector vector)
    {
        return vector instanceof F64Vector
                || (vector instanceof DictionaryVector dictionary && dictionary.values() instanceof F64Vector);
    }

    private static boolean isNullAndErrorFree(Streams input)
    {
        return VectorAccess.isAllFalseNulls(input.getOrNull(Stream.NULLS))
                && VectorAccess.isAllFalseNulls(input.getOrNull(Stream.ERRORS));
    }

    private static Mask.ComparisonOperator negate(Mask.ComparisonOperator operator)
    {
        return switch (operator) {
            case EQUAL -> Mask.ComparisonOperator.NOT_EQUAL;
            case NOT_EQUAL -> Mask.ComparisonOperator.EQUAL;
            case LESS_THAN -> Mask.ComparisonOperator.GREATER_THAN_OR_EQUAL;
            case LESS_THAN_OR_EQUAL -> Mask.ComparisonOperator.GREATER_THAN;
            case GREATER_THAN -> Mask.ComparisonOperator.LESS_THAN_OR_EQUAL;
            case GREATER_THAN_OR_EQUAL -> Mask.ComparisonOperator.LESS_THAN;
        };
    }

    private static Mask.ComparisonOperator swapOperands(Mask.ComparisonOperator operator)
    {
        return switch (operator) {
            case EQUAL -> Mask.ComparisonOperator.EQUAL;
            case NOT_EQUAL -> Mask.ComparisonOperator.NOT_EQUAL;
            case LESS_THAN -> Mask.ComparisonOperator.GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> Mask.ComparisonOperator.GREATER_THAN_OR_EQUAL;
            case GREATER_THAN -> Mask.ComparisonOperator.LESS_THAN;
            case GREATER_THAN_OR_EQUAL -> Mask.ComparisonOperator.LESS_THAN_OR_EQUAL;
        };
    }
}
