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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

/** Registry-selectable assignment of partition positions to a requested number of buckets. */
public final class PartitionBucketWindowFunction
        implements RunningWindowFunction
{
    private final int bucketColumn;

    public PartitionBucketWindowFunction(int bucketColumn)
    {
        if (bucketColumn < 0) {
            throw new IllegalArgumentException("bucketColumn is negative");
        }
        this.bucketColumn = bucketColumn;
    }

    @Override
    public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValuesAndNulls(
                allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new),
                allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
    }

    @Override
    public void reset() {}

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
        Streams buckets = sourceColumns[bucketColumn];
        boolean isNull = OperatorVectorSupport.isNull(buckets.getOrNull(Stream.NULLS), inputPosition);
        ((BooleanVector) output.get(Stream.NULLS)).values()[outputPosition] = isNull;
        if (!isNull) {
            long bucketCount = OperatorVectorSupport.longValue(buckets.values(), inputPosition);
            if (bucketCount <= 0) {
                throw new IllegalArgumentException("Buckets must be greater than 0");
            }
            ((I64Vector) output.values()).values()[outputPosition] = bucketCount;
        }
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
        int rowCount = partitionEnd - partitionStart;
        long[] values = ((I64Vector) output.values()).values();
        Vector nulls = output.get(Stream.NULLS);
        for (int outputPosition = partitionStart; outputPosition < partitionEnd; outputPosition++) {
            if (!OperatorVectorSupport.isNull(nulls, outputPosition)) {
                long bucketCount = values[outputPosition];
                values[outputPosition] = bucket(bucketCount, rowCount, outputPosition - partitionStart) + 1;
            }
        }
        return output;
    }

    private static long bucket(long bucketCount, int rowCount, int currentRow)
    {
        if (rowCount < bucketCount) {
            return currentRow;
        }

        long remainderRows = rowCount % bucketCount;
        long rowsPerBucket = rowCount / bucketCount;
        if (currentRow < ((rowsPerBucket + 1) * remainderRows)) {
            return currentRow / (rowsPerBucket + 1);
        }
        return (currentRow - remainderRows) / rowsPerBucket;
    }
}
