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

import org.weakref.nitro.core.type.UnorderedPlacement;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;

final class OperatorOrderingSemantics
{
    private OperatorOrderingSemantics() {}

    public static int compare(Vector leftValues, Vector leftNulls, int leftPosition, Vector rightValues, Vector rightNulls, int rightPosition)
    {
        return compare(leftValues, leftNulls, leftPosition, rightValues, rightNulls, rightPosition, UnorderedPlacement.LAST);
    }

    public static int compare(
            Vector leftValues,
            Vector leftNulls,
            int leftPosition,
            Vector rightValues,
            Vector rightNulls,
            int rightPosition,
            UnorderedPlacement placement)
    {
        boolean leftNull = OperatorVectorSupport.isNull(leftNulls, leftPosition);
        boolean rightNull = OperatorVectorSupport.isNull(rightNulls, rightPosition);
        if (leftNull || rightNull) {
            if (leftNull == rightNull) {
                return 0;
            }
            return leftNull == (placement == UnorderedPlacement.LAST) ? 1 : -1;
        }

        Vector left = OperatorVectorSupport.flatten(leftValues);
        Vector right = OperatorVectorSupport.flatten(rightValues);
        if ((left instanceof I64Vector || left instanceof I32Vector) && (right instanceof I64Vector || right instanceof I32Vector)) {
            return Long.compare(
                    OperatorVectorSupport.longValue(leftValues, leftPosition),
                    OperatorVectorSupport.longValue(rightValues, rightPosition));
        }
        if (left instanceof F64Vector && right instanceof F64Vector) {
            double leftValue = OperatorVectorSupport.doubleValue(leftValues, leftPosition);
            double rightValue = OperatorVectorSupport.doubleValue(rightValues, rightPosition);
            if (placement == UnorderedPlacement.FIRST) {
                if (Double.isNaN(leftValue)) {
                    return Double.isNaN(rightValue) ? 0 : -1;
                }
                if (Double.isNaN(rightValue)) {
                    return 1;
                }
            }
            return Double.compare(leftValue, rightValue);
        }
        if (left instanceof BinaryVector && right instanceof BinaryVector) {
            return OperatorVectorSupport.binaryCompare(leftValues, leftPosition, rightValues, rightPosition);
        }
        if (left instanceof BooleanVector && right instanceof BooleanVector) {
            return Boolean.compare(
                    OperatorVectorSupport.booleanValue(leftValues, leftPosition),
                    OperatorVectorSupport.booleanValue(rightValues, rightPosition));
        }
        throw new IllegalArgumentException("Unsupported ordering comparison between %s and %s".formatted(
                left.getClass().getSimpleName(),
                right.getClass().getSimpleName()));
    }
}
