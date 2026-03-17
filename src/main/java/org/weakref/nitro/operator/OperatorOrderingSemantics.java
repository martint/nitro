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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;

final class OperatorOrderingSemantics
{
    private OperatorOrderingSemantics() {}

    public static int compare(Vector leftValues, BooleanVector leftNulls, int leftPosition, Vector rightValues, BooleanVector rightNulls, int rightPosition)
    {
        boolean leftNull = OperatorVectorSupport.isNull(leftNulls, leftPosition);
        boolean rightNull = OperatorVectorSupport.isNull(rightNulls, rightPosition);
        if (leftNull || rightNull) {
            if (leftNull == rightNull) {
                return 0;
            }
            return leftNull ? -1 : 1;
        }

        Vector left = OperatorVectorSupport.flatten(leftValues);
        Vector right = OperatorVectorSupport.flatten(rightValues);
        if (left instanceof I64Vector && right instanceof I64Vector) {
            return Long.compare(
                    OperatorVectorSupport.longValue(leftValues, leftPosition),
                    OperatorVectorSupport.longValue(rightValues, rightPosition));
        }
        if (left instanceof F64Vector && right instanceof F64Vector) {
            return Double.compare(
                    OperatorVectorSupport.doubleValue(leftValues, leftPosition),
                    OperatorVectorSupport.doubleValue(rightValues, rightPosition));
        }
        if (left instanceof BinaryVector && right instanceof BinaryVector) {
            return OperatorVectorSupport.binaryCompare(leftValues, leftPosition, rightValues, rightPosition);
        }
        throw new IllegalArgumentException("Unsupported ordering comparison between %s and %s".formatted(
                left.getClass().getSimpleName(),
                right.getClass().getSimpleName()));
    }
}
