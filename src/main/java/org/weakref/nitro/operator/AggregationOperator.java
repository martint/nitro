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
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.AccumulatorFusion;
import org.weakref.nitro.operator.aggregation.StreamAccessors;

import java.util.List;

import static org.weakref.nitro.operator.evaluator.ir.Stream.VALUES;

public class AggregationOperator
        implements Operator
{
    private static final boolean DEFER_RESULT_MATERIALIZATION =
            Boolean.parseBoolean(System.getProperty("nitro.aggregate.deferResultMaterialization", "true"));
    private final Allocator allocator;
    // Every live operator owns an independent lease scope. Instances under the same engine resource owner still
    // share a compatibility domain so a closed aggregate's buffers can be recycled by a later aggregate, but closing
    // a nested aggregate must never release the state or result vectors of an outer aggregate that is still consuming
    // its source.
    private final Allocator.Context allocationContext;

    private final Operator source;
    private final List<Accumulator> aggregations;

    private final Streams[] reusableResults;
    private BatchState currentBatchState;
    private boolean done;

    public AggregationOperator(Allocator allocator, List<Accumulator> aggregations, Operator source)
    {
        this.allocator = allocator;
        this.allocationContext = new Allocator.Context(
                "AggregationOperator",
                allocator.engineResources().aggregationOperator().bufferPoolGroup());
        this.source = source;
        // AccumulatorFusion rewrites recognized pairs (e.g. Min + Max on the same column) into
        // cooperating accumulators that share a single input scan per batch. The operator's main
        // loop remains fully generic; any operation-specific fast path lives in the aggregation
        // package.
        this.aggregations = AccumulatorFusion.fuse(aggregations);

        reusableResults = new Streams[this.aggregations.size()];
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
        BatchState batchState = new BatchState(allocator.allocateAllMask(allocationContext, 1));
        currentBatchState = batchState;
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int output = outputIndex;
            outputs[outputIndex] = resultOutput(batchState, output);
        }
        return new Batch(batchState.mask, batchState::constrain, takenMask -> allocator.transfer(allocationContext, takenMask), outputs);
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
                (stream, vector) -> allocator.transfer(allocationContext, vector));
    }

    private void doAggregationIfNeeded(BatchState batchState)
    {
        if (!batchState.filled) {
            batchState.filled = true;

            Streams[] state = new Streams[aggregations.size()];
            for (int i = 0; i < state.length; i++) {
                state[i] = aggregations.get(i).allocate(allocator, allocationContext, 1);
                aggregations.get(i).initialize(state[i], 0, 1);
            }
            if (!DEFER_RESULT_MATERIALIZATION) {
                materializeResults(state, batchState);
            }

            if (batchState.mask.none()) {
                if (DEFER_RESULT_MATERIALIZATION) {
                    materializeResults(state, batchState);
                }
                return;
            }

            while (source.hasNext()) {
                try (Batch batch = source.next()) {
                    Mask mask = batch.borrowMask();
                    if (mask.none()) {
                        continue;
                    }
                    for (int aggregation = 0; aggregation < aggregations.size(); aggregation++) {
                        Accumulator accumulator = aggregations.get(aggregation);
                        int filterColumn = accumulator.filterInputColumn();
                        Mask aggregationMask = filterColumn < 0 ? mask : filterMask(batch, filterColumn, mask);
                        try {
                            accumulator.accumulate(state[aggregation], 0, aggregationMask, StreamAccessors.forBatch(batch));
                        }
                        finally {
                            if (aggregationMask != mask) {
                                allocator.release(allocationContext, aggregationMask);
                            }
                        }
                        if (!DEFER_RESULT_MATERIALIZATION) {
                            reusableResults[aggregation] = accumulator.result(0, state[aggregation], reusableResults[aggregation], allocator, allocationContext);
                            batchState.results[aggregation] = reusableResults[aggregation];
                        }
                    }
                }
            }
            if (DEFER_RESULT_MATERIALIZATION) {
                materializeResults(state, batchState);
            }
        }
    }

    private void materializeResults(Streams[] state, BatchState batchState)
    {
        for (int aggregation = 0; aggregation < aggregations.size(); aggregation++) {
            reusableResults[aggregation] = aggregations.get(aggregation).result(
                    0,
                    state[aggregation],
                    reusableResults[aggregation],
                    allocator,
                    allocationContext);
            batchState.results[aggregation] = reusableResults[aggregation];
        }
    }

    private Mask filterMask(Batch batch, int filterColumn, Mask mask)
    {
        Output output = batch.output(filterColumn);
        Mask direct = output.tryBorrowMask(VALUES, mask, true, allocator, allocationContext);
        if (direct != null) {
            return direct;
        }
        Mask selected = allocator.copyMask(allocationContext, mask);
        var values = VectorAccess.booleanValues(output.borrow(VALUES));
        selected.retainIf(position -> values.value(position));
        return selected;
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(allocationContext);
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
