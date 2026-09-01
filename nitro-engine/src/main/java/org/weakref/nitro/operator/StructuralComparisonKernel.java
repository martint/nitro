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

import org.weakref.nitro.core.type.BoundTypeComparison;
import org.weakref.nitro.data.Vector;

interface StructuralComparisonKernel
        extends StructuralIdentityKernel, BoundTypeComparison
{
    default PositionEquality bindPartitionEquality(
            Vector leftValues,
            Vector leftNulls,
            Vector rightValues,
            Vector rightNulls)
    {
        return (leftPosition, rightPosition) -> {
            boolean leftNull = OperatorVectorSupport.isNull(leftNulls, leftPosition);
            boolean rightNull = OperatorVectorSupport.isNull(rightNulls, rightPosition);
            if (leftNull || rightNull) {
                return leftNull == rightNull;
            }
            return identical(leftValues, leftNulls, leftPosition, rightValues, rightNulls, rightPosition);
        };
    }

    @FunctionalInterface
    interface PositionEquality
    {
        boolean identical(int leftPosition, int rightPosition);
    }

    default boolean allowsLegacyPhysicalShortcuts()
    {
        return false;
    }

    int compare(
            Vector leftValues,
            Vector leftNulls,
            int leftPosition,
            Vector rightValues,
            Vector rightNulls,
            int rightPosition);
}
