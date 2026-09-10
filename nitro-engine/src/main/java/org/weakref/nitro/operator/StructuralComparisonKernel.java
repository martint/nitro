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

import static java.util.Objects.requireNonNull;

interface StructuralComparisonKernel
        extends StructuralIdentityKernel, BoundTypeComparison
{
    default PositionComparison bindComparison(
            Vector leftValues,
            Vector leftNulls,
            Vector rightValues,
            Vector rightNulls)
    {
        return new NullablePositionComparison(this, leftValues, leftNulls, rightValues, rightNulls);
    }

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

    @FunctionalInterface
    interface PositionComparison
    {
        int compare(int leftPosition, int rightPosition);
    }

    final class NullablePositionComparison
            implements PositionComparison
    {
        private final StructuralComparisonKernel kernel;
        private final Vector leftValues;
        private final Vector leftNulls;
        private final Vector rightValues;
        private final Vector rightNulls;

        private NullablePositionComparison(
                StructuralComparisonKernel kernel,
                Vector leftValues,
                Vector leftNulls,
                Vector rightValues,
                Vector rightNulls)
        {
            this.kernel = requireNonNull(kernel, "kernel is null");
            this.leftValues = requireNonNull(leftValues, "leftValues is null");
            this.leftNulls = leftNulls;
            this.rightValues = requireNonNull(rightValues, "rightValues is null");
            this.rightNulls = rightNulls;
        }

        @Override
        public int compare(int leftPosition, int rightPosition)
        {
            return kernel.compare(
                    leftValues, leftNulls, leftPosition,
                    rightValues, rightNulls, rightPosition);
        }
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
