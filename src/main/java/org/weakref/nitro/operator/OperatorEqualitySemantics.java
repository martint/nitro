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

import java.util.Arrays;

final class OperatorEqualitySemantics
{
    private OperatorEqualitySemantics() {}

    public static boolean equal(Vector leftValues, BooleanVector leftNulls, int leftPosition, Vector rightValues, BooleanVector rightNulls, int rightPosition)
    {
        if (OperatorVectorSupport.isNull(leftNulls, leftPosition) || OperatorVectorSupport.isNull(rightNulls, rightPosition)) {
            return false;
        }

        Vector left = OperatorVectorSupport.flatten(leftValues);
        Vector right = OperatorVectorSupport.flatten(rightValues);
        if (left instanceof I64Vector && right instanceof I64Vector) {
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
            return Arrays.equals(
                    OperatorVectorSupport.binaryBytes(leftValues, leftPosition),
                    OperatorVectorSupport.binaryBytes(rightValues, rightPosition));
        }
        throw new IllegalArgumentException("Unsupported equality comparison between %s and %s".formatted(
                left.getClass().getSimpleName(),
                right.getClass().getSimpleName()));
    }
}
