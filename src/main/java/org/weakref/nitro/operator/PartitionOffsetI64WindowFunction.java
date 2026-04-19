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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;

public final class PartitionOffsetI64WindowFunction
        implements RunningWindowFunction
{
    private final int inputColumn;
    private final int offset;

    private long[] partitionValues = new long[0];
    private boolean[] partitionNulls = new boolean[0];
    private int partitionCount;

    public PartitionOffsetI64WindowFunction(int inputColumn, int offset)
    {
        this.inputColumn = inputColumn;
        this.offset = offset;
    }

    @Override
    public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValuesAndNulls(
                allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new),
                allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
    }

    @Override
    public void reset()
    {
        partitionCount = 0;
    }

    @Override
    public Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize)
    {
        ensureCapacity(partitionCount + 1);
        Streams input = sourceColumns[inputColumn];
        Vector values = input.values();
        Vector nulls = input.getOrNull(Stream.NULLS);
        boolean isNull = OperatorVectorSupport.isNull(nulls, inputPosition);
        partitionNulls[partitionCount] = isNull;
        if (!isNull) {
            partitionValues[partitionCount] = OperatorVectorSupport.longValue(values, inputPosition);
        }
        partitionCount++;
        return output;
    }

    @Override
    public Streams finishPartition(Allocator allocator, Allocator.Context allocationContext, Streams output, int partitionStart, int partitionEnd)
    {
        I64Vector outputValues = (I64Vector) output.values();
        WindowFunctionSupport.WritableNulls writableNulls = WindowFunctionSupport.writableOutputNulls(allocator, allocationContext, output);
        output = writableNulls.output();
        BooleanVector outputNulls = writableNulls.nulls();
        for (int partitionPosition = 0; partitionPosition < partitionCount; partitionPosition++) {
            int sourcePosition = partitionPosition + offset;
            int outputPosition = partitionStart + partitionPosition;
            boolean isNull = sourcePosition < 0 || sourcePosition >= partitionCount || partitionNulls[sourcePosition];
            outputNulls.values()[outputPosition] = isNull;
            if (!isNull) {
                outputValues.values()[outputPosition] = partitionValues[sourcePosition];
            }
        }
        return output;
    }

    private void ensureCapacity(int requiredSize)
    {
        if (partitionValues.length >= requiredSize) {
            return;
        }
        int newSize = Math.max(requiredSize, Math.max(8, partitionValues.length * 2));
        partitionValues = Arrays.copyOf(partitionValues, newSize);
        partitionNulls = Arrays.copyOf(partitionNulls, newSize);
    }
}
