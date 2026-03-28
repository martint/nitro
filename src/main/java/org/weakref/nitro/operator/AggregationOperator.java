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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.Max;
import org.weakref.nitro.operator.aggregation.Min;
import org.weakref.nitro.operator.aggregation.StreamAccessors;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;

public class AggregationOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("AggregationOperator");
    private final Allocator allocator;

    private final Operator source;
    private final List<Accumulator> aggregations;

    private final Streams[] reusableResults;
    private BatchState currentBatchState;
    private boolean done;

    public AggregationOperator(Allocator allocator, List<Accumulator> aggregations, Operator source)
    {
        this.allocator = allocator;
        this.source = source;
        this.aggregations = aggregations;

        reusableResults = new Streams[aggregations.size()];
    }

    @Override
    public int outputCount()
    {
        return aggregations.size();
    }

    @Override
    public Batch next()
    {
        done = true;
        BatchState batchState = new BatchState(allocator.allocateAllMask(ALLOCATION_CONTEXT, 1));
        currentBatchState = batchState;
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int output = outputIndex;
            outputs[outputIndex] = resultOutput(batchState, output);
        }
        return new Batch(batchState.mask, batchState::constrain, takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask), outputs);
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public void constrain(Mask mask)
    {
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    private Output resultOutput(BatchState batchState, int output)
    {
        return new Output(
                java.util.Set.of(org.weakref.nitro.operator.evaluator.ir.Stream.VALUES, org.weakref.nitro.operator.evaluator.ir.Stream.NULLS),
                stream -> {
                    doAggregationIfNeeded(batchState);
                    return batchState.results[output].get(stream);
                },
                (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
    }

    private void doAggregationIfNeeded(BatchState batchState)
    {
        if (!batchState.filled) {
            batchState.filled = true;

            Streams[] state = new Streams[aggregations.size()];
            for (int i = 0; i < state.length; i++) {
                state[i] = aggregations.get(i).allocate(allocator, ALLOCATION_CONTEXT, 1);
                aggregations.get(i).initialize(state[i], 0, 1);
                reusableResults[i] = aggregations.get(i).result(1, state[i], reusableResults[i], allocator, ALLOCATION_CONTEXT);
                batchState.results[i] = reusableResults[i];
            }

            if (batchState.mask.none()) {
                return;
            }

            while (source.hasNext()) {
                try (Batch batch = source.next()) {
                    Mask mask = batch.borrowMask();
                    if (mask.none()) {
                        continue;
                    }
                    if (!tryAccumulateFusedMinMax(state, batch, mask, batchState)) {
                        for (int aggregation = 0; aggregation < aggregations.size(); aggregation++) {
                            Accumulator accumulator = aggregations.get(aggregation);
                            accumulator.accumulate(state[aggregation], 0, mask, StreamAccessors.forBatch(batch));
                            reusableResults[aggregation] = accumulator.result(1, state[aggregation], reusableResults[aggregation], allocator, ALLOCATION_CONTEXT);
                            batchState.results[aggregation] = reusableResults[aggregation];
                        }
                    }
                }
            }
        }
    }

    private boolean tryAccumulateFusedMinMax(Streams[] state, Batch batch, Mask mask, BatchState batchState)
    {
        if (aggregations.size() != 2) {
            return false;
        }

        int minIndex;
        int maxIndex;
        if (aggregations.get(0) instanceof Min && aggregations.get(1) instanceof Max) {
            minIndex = 0;
            maxIndex = 1;
        }
        else if (aggregations.get(0) instanceof Max && aggregations.get(1) instanceof Min) {
            minIndex = 1;
            maxIndex = 0;
        }
        else {
            return false;
        }

        Min min = (Min) aggregations.get(minIndex);
        Max max = (Max) aggregations.get(maxIndex);
        if (min.inputColumn() != max.inputColumn()) {
            return false;
        }

        Output output = batch.output(min.inputColumn());
        Vector inputValues = output.borrow(Stream.VALUES);
        BooleanVector inputNulls = (BooleanVector) output.borrowOrNull(Stream.NULLS);

        I64Vector minValues = (I64Vector) state[minIndex].values();
        BooleanVector minNulls = (BooleanVector) state[minIndex].get(Stream.NULLS);
        I64Vector maxValues = (I64Vector) state[maxIndex].values();
        BooleanVector maxNulls = (BooleanVector) state[maxIndex].get(Stream.NULLS);

        long currentMin = minValues.values()[0];
        boolean currentMinNull = minNulls.values()[0];
        long currentMax = maxValues.values()[0];
        boolean currentMaxNull = maxNulls.values()[0];

        for (int position : mask) {
            if (OperatorVectorSupport.isNull(inputNulls, position)) {
                continue;
            }

            long value = OperatorVectorSupport.longValue(inputValues, position);
            if (currentMinNull) {
                currentMin = value;
                currentMinNull = false;
            }
            else {
                currentMin = Math.min(currentMin, value);
            }

            if (currentMaxNull) {
                currentMax = value;
                currentMaxNull = false;
            }
            else {
                currentMax = Math.max(currentMax, value);
            }
        }

        minValues.values()[0] = currentMin;
        minNulls.values()[0] = currentMinNull;
        maxValues.values()[0] = currentMax;
        maxNulls.values()[0] = currentMaxNull;

        reusableResults[minIndex] = aggregations.get(minIndex).result(1, state[minIndex], reusableResults[minIndex], allocator, ALLOCATION_CONTEXT);
        batchState.results[minIndex] = reusableResults[minIndex];
        reusableResults[maxIndex] = aggregations.get(maxIndex).result(1, state[maxIndex], reusableResults[maxIndex], allocator, ALLOCATION_CONTEXT);
        batchState.results[maxIndex] = reusableResults[maxIndex];
        return true;
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private final class BatchState
    {
        private final Streams[] results = new Streams[aggregations.size()];
        private Mask mask;
        private boolean filled;

        private BatchState(Mask mask)
        {
            this.mask = mask;
        }

        private void constrain(Mask mask)
        {
            this.mask = mask;
        }
    }
}
