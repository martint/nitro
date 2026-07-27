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

public final class RunningSumI64WindowFunction
        implements RunningWindowFunction
{
    private final int inputColumn;
    private long runningSum;
    private boolean hasValue;

    public RunningSumI64WindowFunction(int inputColumn)
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
        hasValue = false;
    }

    @Override
    public Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize)
    {
        Streams input = sourceColumns[inputColumn];
        Vector values = input.values();
        Vector nulls = input.getOrNull(Stream.NULLS);
        if (!OperatorVectorSupport.isNull(nulls, inputPosition)) {
            runningSum += OperatorVectorSupport.longValue(values, inputPosition);
            hasValue = true;
        }

        I64Vector outputValues = (I64Vector) output.values();
        WindowFunctionSupport.WritableNulls writableNulls = WindowFunctionSupport.writableOutputNulls(allocator, allocationContext, output);
        output = writableNulls.output();
        BooleanVector outputNulls = writableNulls.nulls();
        outputNulls.values()[outputPosition] = !hasValue;
        if (hasValue) {
            outputValues.values()[outputPosition] = runningSum;
        }
        return output;
    }
}
