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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;

/** Registry-selectable peer distribution over an ordered window partition. */
public final class PeerDistributionWindowFunction
        implements RunningWindowFunction
{
    public enum Distribution
    {
        CUMULATIVE,
        PERCENT_RANK
    }

    private final int[] orderingColumns;
    private final StructuralComparisonKernel[] orderingKernels;
    private final Distribution distribution;

    private Streams[] previousColumns;
    private int previousPosition;
    private int peerStartOutputPosition;
    private long rowNumber;
    private double peerRankNumerator;

    public PeerDistributionWindowFunction(Schema inputSchema, int[] orderingColumns, Distribution distribution)
    {
        requireNonNull(inputSchema, "inputSchema is null");
        this.orderingColumns = requireNonNull(orderingColumns, "orderingColumns is null").clone();
        this.distribution = requireNonNull(distribution, "distribution is null");
        StructuralTypeKernelFactory structuralTypes = new StructuralTypeKernelFactory();
        orderingKernels = new StructuralComparisonKernel[orderingColumns.length];
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int column = orderingColumns[orderingIndex];
            if (column < 0 || column >= inputSchema.size()) {
                throw new IllegalArgumentException("Ordering column is outside the input schema: " + column);
            }
            orderingKernels[orderingIndex] = structuralTypes.comparison(inputSchema.field(column).type());
        }
    }

    @Override
    public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValues(allocator.allocate(allocationContext, F64Vector.class, size, F64Vector::new));
    }

    @Override
    public void reset()
    {
        previousColumns = null;
        previousPosition = -1;
        peerStartOutputPosition = -1;
        rowNumber = 0;
        peerRankNumerator = 0;
    }

    @Override
    public Streams append(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            Streams[] sourceColumns,
            int inputPosition,
            int outputPosition,
            int outputSize)
    {
        rowNumber++;
        boolean newPeer = previousColumns == null || orderingChanged(previousColumns, previousPosition, sourceColumns, inputPosition);
        if (newPeer) {
            if (distribution == Distribution.CUMULATIVE && peerStartOutputPosition >= 0) {
                fill(output, peerStartOutputPosition, outputPosition, rowNumber - 1);
            }
            peerStartOutputPosition = outputPosition;
            peerRankNumerator = rowNumber - 1;
        }
        if (distribution == Distribution.PERCENT_RANK) {
            ((F64Vector) output.values()).values()[outputPosition] = peerRankNumerator;
        }
        previousColumns = sourceColumns;
        previousPosition = inputPosition;
        return output;
    }

    @Override
    public Streams finishPartition(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            int partitionStart,
            int partitionEnd,
            int outputSize)
    {
        if (partitionStart == partitionEnd) {
            return output;
        }
        if (distribution == Distribution.CUMULATIVE) {
            fill(output, peerStartOutputPosition, partitionEnd, rowNumber);
        }
        double denominator = distribution == Distribution.CUMULATIVE ? rowNumber : Math.max(1, rowNumber - 1);
        double[] values = ((F64Vector) output.values()).values();
        for (int position = partitionStart; position < partitionEnd; position++) {
            values[position] /= denominator;
        }
        return output;
    }

    private static void fill(Streams output, int start, int end, double value)
    {
        java.util.Arrays.fill(((F64Vector) output.values()).values(), start, end, value);
    }

    private boolean orderingChanged(Streams[] leftColumns, int leftPosition, Streams[] rightColumns, int rightPosition)
    {
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int column = orderingColumns[orderingIndex];
            Streams left = leftColumns[column];
            Streams right = rightColumns[column];
            if (orderingKernels[orderingIndex].compare(
                    left.values(),
                    left.getOrNull(Stream.NULLS),
                    leftPosition,
                    right.values(),
                    right.getOrNull(Stream.NULLS),
                    rightPosition) != 0) {
                return true;
            }
        }
        return false;
    }
}
