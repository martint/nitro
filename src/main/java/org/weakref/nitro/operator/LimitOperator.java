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

import org.weakref.nitro.data.Mask;

import static java.lang.Math.toIntExact;

public class LimitOperator
        implements BatchOperator
{
    private final long limit;
    private final BatchOperator source;

    private long count;
    private Batch currentBatch;
    private Mask currentMask;

    public LimitOperator(long limit, BatchOperator source)
    {
        this.limit = limit;
        this.source = source;
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
    }

    @Override
    public Batch nextBatch()
    {
        currentBatch = source.nextBatch();
        currentMask = currentBatch.borrowMask();

        int remaining = toIntExact(Math.min(limit - count, currentMask.count()));
        currentMask = currentMask.first(remaining);
        source.constrain(currentMask);
        count += remaining;

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            Output sourceOutput = currentBatch.output(outputIndex);
            outputs[outputIndex] = new Output(sourceOutput.streams(), sourceOutput::borrow);
        }
        return new Batch(currentMask, outputs);
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext() && count < limit;
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
    }

    @Override
    public void close()
    {
        source.close();
    }
}
