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

import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

final class OperatorEqualitySemantics
{
    private OperatorEqualitySemantics() {}

    public static boolean equal(Vector leftValues, Vector leftNulls, int leftPosition, Vector rightValues, Vector rightNulls, int rightPosition)
    {
        if (OperatorVectorSupport.isNull(leftNulls, leftPosition) || OperatorVectorSupport.isNull(rightNulls, rightPosition)) {
            return false;
        }

        Vector left = OperatorVectorSupport.flatten(leftValues);
        Vector right = OperatorVectorSupport.flatten(rightValues);
        if ((left instanceof I64Vector || left instanceof I32Vector) && (right instanceof I64Vector || right instanceof I32Vector)) {
            return OperatorVectorSupport.longValue(leftValues, leftPosition) == OperatorVectorSupport.longValue(rightValues, rightPosition);
        }
        if (left instanceof BooleanVector && right instanceof BooleanVector) {
            return OperatorVectorSupport.booleanValue(leftValues, leftPosition) == OperatorVectorSupport.booleanValue(rightValues, rightPosition);
        }
        if (left instanceof F64Vector && right instanceof F64Vector) {
            return Double.compare(
                    OperatorVectorSupport.doubleValue(leftValues, leftPosition),
                    OperatorVectorSupport.doubleValue(rightValues, rightPosition)) == 0;
        }
        if (left instanceof BinaryVector && right instanceof BinaryVector) {
            return OperatorVectorSupport.binaryEquals(leftValues, leftPosition, rightValues, rightPosition);
        }
        throw new IllegalArgumentException("Unsupported equality comparison between %s and %s".formatted(
                left.getClass().getSimpleName(),
                right.getClass().getSimpleName()));
    }

    public static StructuralComparisonKernel.PositionEquality bindPartitionEquality(
            Vector leftValues,
            Vector leftNulls,
            Vector rightValues,
            Vector rightNulls)
    {
        Vector left = OperatorVectorSupport.flatten(leftValues);
        Vector right = OperatorVectorSupport.flatten(rightValues);
        if (left == leftValues && right == rightValues) {
            if (left instanceof I64Vector leftLongs && right instanceof I64Vector rightLongs) {
                return (leftPosition, rightPosition) -> partitionEqual(
                        leftNulls, leftPosition, rightNulls, rightPosition,
                        leftLongs.values()[leftPosition] == rightLongs.values()[rightPosition]);
            }
            if (left instanceof I32Vector leftIntegers && right instanceof I32Vector rightIntegers) {
                return (leftPosition, rightPosition) -> partitionEqual(
                        leftNulls, leftPosition, rightNulls, rightPosition,
                        leftIntegers.values()[leftPosition] == rightIntegers.values()[rightPosition]);
            }
            if (left instanceof BinaryVector leftBinary && right instanceof BinaryVector rightBinary) {
                return (leftPosition, rightPosition) -> partitionEqual(
                        leftNulls, leftPosition, rightNulls, rightPosition,
                        Arrays.equals(
                                leftBinary.data(), leftBinary.offsets()[leftPosition], leftBinary.offsets()[leftPosition + 1],
                                rightBinary.data(), rightBinary.offsets()[rightPosition], rightBinary.offsets()[rightPosition + 1]));
            }
        }
        return (leftPosition, rightPosition) -> {
            boolean leftNull = OperatorVectorSupport.isNull(leftNulls, leftPosition);
            boolean rightNull = OperatorVectorSupport.isNull(rightNulls, rightPosition);
            if (leftNull || rightNull) {
                return leftNull == rightNull;
            }
            return equal(leftValues, leftNulls, leftPosition, rightValues, rightNulls, rightPosition);
        };
    }

    private static boolean partitionEqual(
            Vector leftNulls,
            int leftPosition,
            Vector rightNulls,
            int rightPosition,
            boolean valuesEqual)
    {
        boolean leftNull = OperatorVectorSupport.isNull(leftNulls, leftPosition);
        boolean rightNull = OperatorVectorSupport.isNull(rightNulls, rightPosition);
        if (leftNull || rightNull) {
            return leftNull == rightNull;
        }
        return valuesEqual;
    }
}
