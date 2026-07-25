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

public final class PartitionAverageI64WindowFunction
        implements RunningWindowFunction
{
    private final int inputColumn;
    private long runningSum;
    private long runningCount;

    public PartitionAverageI64WindowFunction(int inputColumn)
    {
        this.inputColumn = inputColumn;
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
        runningSum = 0;
        runningCount = 0;
    }

    @Override
    public Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize)
    {
        Streams input = sourceColumns[inputColumn];
        Vector values = input.values();
        Vector nulls = input.getOrNull(Stream.NULLS);
        if (!OperatorVectorSupport.isNull(nulls, inputPosition)) {
            runningSum += OperatorVectorSupport.longValue(values, inputPosition);
            runningCount++;
        }
        return output;
    }

    @Override
    public Streams finishPartition(Allocator allocator, Allocator.Context allocationContext, Streams output, int partitionStart, int partitionEnd)
    {
        I64Vector outputValues = (I64Vector) output.values();
        WindowFunctionSupport.WritableNulls writableNulls = WindowFunctionSupport.writableOutputNulls(allocator, allocationContext, output);
        output = writableNulls.output();
        BooleanVector outputNulls = writableNulls.nulls();
        boolean hasValue = runningCount > 0;
        long average = hasValue ? roundDivide(runningSum, runningCount) : 0;
        for (int outputPosition = partitionStart; outputPosition < partitionEnd; outputPosition++) {
            outputNulls.values()[outputPosition] = !hasValue;
            if (hasValue) {
                outputValues.values()[outputPosition] = average;
            }
        }
        return output;
    }

    private static long roundDivide(long numerator, long denominator)
    {
        long positiveNumerator = numerator >= 0 ? numerator : -numerator;
        long positiveDenominator = denominator >= 0 ? denominator : -denominator;
        long rounded = (positiveNumerator + (positiveDenominator / 2)) / positiveDenominator;
        return (numerator < 0) ^ (denominator < 0) ? -rounded : rounded;
    }
}
