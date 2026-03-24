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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.SumStateVector;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.StreamAccessors;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;

public class AggregationOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("AggregationOperator");
    private final Allocator allocator;

    private final Operator source;
    private final List<Accumulator> aggregations;
    private final Sum[] fusedSums;

    private final Streams[] reusableResults;
    private BatchState currentBatchState;
    private boolean done;

    public AggregationOperator(Allocator allocator, List<Accumulator> aggregations, Operator source)
    {
        this.allocator = allocator;
        this.source = source;
        this.aggregations = aggregations;
        this.fusedSums = allSums(aggregations);

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
                    var streamAccessor = StreamAccessors.forBatch(batch);
                    if (fusedSums != null) {
                        accumulateSums(state, mask, streamAccessor);
                    }
                    else {
                        for (int aggregation = 0; aggregation < aggregations.size(); aggregation++) {
                            Accumulator accumulator = aggregations.get(aggregation);
                            accumulator.accumulate(state[aggregation], 0, mask, streamAccessor);
                            reusableResults[aggregation] = accumulator.result(1, state[aggregation], reusableResults[aggregation], allocator, ALLOCATION_CONTEXT);
                            batchState.results[aggregation] = reusableResults[aggregation];
                        }
                    }
                    if (fusedSums != null) {
                        for (int aggregation = 0; aggregation < aggregations.size(); aggregation++) {
                            Accumulator accumulator = aggregations.get(aggregation);
                            reusableResults[aggregation] = accumulator.result(1, state[aggregation], reusableResults[aggregation], allocator, ALLOCATION_CONTEXT);
                            batchState.results[aggregation] = reusableResults[aggregation];
                        }
                    }
                }
            }
        }
    }

    private void accumulateSums(Streams[] state, Mask mask, org.weakref.nitro.operator.aggregation.StreamAccessor streamAccessor)
    {
        long[] sums = new long[fusedSums.length];
        org.weakref.nitro.data.Vector[] inputValues = new org.weakref.nitro.data.Vector[fusedSums.length];
        boolean[][] inputNulls = new boolean[fusedSums.length][];
        for (int index = 0; index < fusedSums.length; index++) {
            int inputColumn = fusedSums[index].inputColumn();
            inputValues[index] = streamAccessor.values(inputColumn);
            inputNulls[index] = Sum.nulls(streamAccessor.stream(inputColumn, Stream.NULLS));
        }

        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                for (int aggregation = 0; aggregation < fusedSums.length; aggregation++) {
                    sums[aggregation] += Sum.isNull(inputNulls[aggregation], position) ? 0 : Sum.valueAt(inputValues[aggregation], position);
                }
            }
        }
        else {
            for (int position : mask) {
                for (int aggregation = 0; aggregation < fusedSums.length; aggregation++) {
                    sums[aggregation] += Sum.isNull(inputNulls[aggregation], position) ? 0 : Sum.valueAt(inputValues[aggregation], position);
                }
            }
        }

        for (int aggregation = 0; aggregation < fusedSums.length; aggregation++) {
            ((SumStateVector) state[aggregation].values()).increment(0, sums[aggregation]);
        }
    }

    private static Sum[] allSums(List<Accumulator> aggregations)
    {
        if (aggregations.isEmpty()) {
            return null;
        }
        Sum[] sums = new Sum[aggregations.size()];
        for (int index = 0; index < aggregations.size(); index++) {
            if (!(aggregations.get(index) instanceof Sum sum)) {
                return null;
            }
            sums[index] = sum;
        }
        return sums;
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
