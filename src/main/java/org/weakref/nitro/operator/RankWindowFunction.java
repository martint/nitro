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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

public final class RankWindowFunction
        implements RunningWindowFunction
{
    private final int[] orderingColumns;
    private final boolean[] descendingByColumn;

    private Streams[] previousColumns;
    private int previousPosition;
    private long rowNumber;
    private long rank;

    public RankWindowFunction(int[] orderingColumns, boolean[] descendingByColumn)
    {
        if (orderingColumns.length != descendingByColumn.length) {
            throw new IllegalArgumentException("Ordering columns and directions must have the same length");
        }
        this.orderingColumns = orderingColumns.clone();
        this.descendingByColumn = descendingByColumn.clone();
    }

    @Override
    public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValues(
                allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new));
    }

    @Override
    public void reset()
    {
        previousColumns = null;
        previousPosition = -1;
        rowNumber = 0;
        rank = 0;
    }

    @Override
    public Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize)
    {
        rowNumber++;
        if (previousColumns == null || orderingChanged(previousColumns, previousPosition, sourceColumns, inputPosition)) {
            rank = rowNumber;
        }
        ((I64Vector) output.values()).values()[outputPosition] = rank;
        previousColumns = sourceColumns;
        previousPosition = inputPosition;
        return output;
    }

    private boolean orderingChanged(Streams[] leftColumns, int leftPosition, Streams[] rightColumns, int rightPosition)
    {
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            Streams left = leftColumns[orderingColumns[orderingIndex]];
            Streams right = rightColumns[orderingColumns[orderingIndex]];
            int comparison = OperatorOrderingSemantics.compare(
                    left.values(),
                    left.getOrNull(Stream.NULLS),
                    leftPosition,
                    right.values(),
                    right.getOrNull(Stream.NULLS),
                    rightPosition);
            if (descendingByColumn[orderingIndex]) {
                comparison = -comparison;
            }
            if (comparison != 0) {
                return true;
            }
        }
        return false;
    }
}
