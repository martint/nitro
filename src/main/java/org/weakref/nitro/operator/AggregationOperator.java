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
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.AggregationExecutionContext;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationUnit;
import org.weakref.nitro.operator.aggregation.StreamAccessors;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.data.Stream.VALUES;

public class AggregationOperator
        implements Operator
{
    private final Allocator allocator;
    // Every live operator owns an independent lease scope. Instances under the same engine resource owner still
    // share a compatibility domain so a closed aggregate's buffers can be recycled by a later aggregate, but closing
    // a nested aggregate must never release the state or result vectors of an outer aggregate that is still consuming
    // its source.
    private final Allocator.Context allocationContext;
    private final AggregationExecutionContext aggregationExecutionContext;
    private final boolean deferResultMaterialization;

    private final Operator source;
    private final PhysicalAggregationProgram program;
    private final List<PhysicalAggregationUnit> units;
    private final int[][] outputsByUnit;

    private final Streams[] reusableResults;
    private BatchState currentBatchState;
    private boolean done;

    public AggregationOperator(Allocator allocator, List<Accumulator> aggregations, Operator source)
    {
        this(allocator, PhysicalAggregationProgram.independent(aggregations), source, allocator.engineResources().operatorResources());
    }

    public AggregationOperator(Allocator allocator, List<Accumulator> aggregations, Operator source, OperatorResources operatorResources)
    {
        this(allocator, PhysicalAggregationProgram.independent(aggregations), source, operatorResources);
    }

    public AggregationOperator(Allocator allocator, PhysicalAggregationProgram program, Operator source)
    {
        this(allocator, program, source, allocator.engineResources().operatorResources());
    }

    public AggregationOperator(Allocator allocator, PhysicalAggregationProgram program, Operator source, OperatorResources operatorResources)
    {
        this.allocator = allocator;
        operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        AggregationOperatorResources aggregationResources = operatorResources.aggregation();
        this.allocationContext = new Allocator.Context(
                "AggregationOperator",
                aggregationResources.bufferPoolGroup());
        this.aggregationExecutionContext = new AggregationExecutionContext(
                allocator,
                allocationContext,
                operatorResources.codeGeneration());
        this.deferResultMaterialization = aggregationResources.policy().deferResultMaterialization();
        this.source = source;
        this.program = requireNonNull(program, "program is null");
        this.units = program.units();
        this.outputsByUnit = outputsByUnit(program);

        reusableResults = new Streams[program.outputs().size()];
    }

    @Override
    public int outputCount()
    {
        return program.outputs().size();
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
                java.util.Set.of(org.weakref.nitro.data.Stream.VALUES, org.weakref.nitro.data.Stream.NULLS),
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

            Streams[] state = new Streams[units.size()];
            for (int i = 0; i < state.length; i++) {
                state[i] = units.get(i).allocate(aggregationExecutionContext, 1);
                units.get(i).initialize(state[i], 0, 1);
            }
            if (!deferResultMaterialization) {
                materializeResults(state, batchState);
            }

            if (batchState.mask.none()) {
                if (deferResultMaterialization) {
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
                    for (int unit = 0; unit < units.size(); unit++) {
                        PhysicalAggregationUnit aggregationUnit = units.get(unit);
                        int filterColumn = aggregationUnit.filterInputColumn();
                        Mask aggregationMask = filterColumn < 0 ? mask : filterMask(batch, filterColumn, mask);
                        try {
                            aggregationUnit.accumulate(state[unit], 0, aggregationMask, StreamAccessors.forBatch(batch));
                        }
                        finally {
                            if (aggregationMask != mask) {
                                allocator.release(allocationContext, aggregationMask);
                            }
                        }
                        if (!deferResultMaterialization) {
                            materializeUnitResults(unit, state[unit], batchState);
                        }
                    }
                }
            }
            if (deferResultMaterialization) {
                materializeResults(state, batchState);
            }
        }
    }

    private void materializeResults(Streams[] state, BatchState batchState)
    {
        for (int unit = 0; unit < units.size(); unit++) {
            materializeUnitResults(unit, state[unit], batchState);
        }
    }

    private void materializeUnitResults(int unit, Streams state, BatchState batchState)
    {
        PhysicalAggregationUnit aggregationUnit = units.get(unit);
        for (int output : outputsByUnit[unit]) {
            PhysicalAggregationProgram.Output binding = program.outputs().get(output);
            reusableResults[output] = aggregationUnit.result(
                    binding.result(),
                    0,
                    state,
                    reusableResults[output],
                    allocator,
                    allocationContext);
            batchState.results[output] = reusableResults[output];
        }
    }

    private static int[][] outputsByUnit(PhysicalAggregationProgram program)
    {
        int[] counts = new int[program.units().size()];
        for (PhysicalAggregationProgram.Output output : program.outputs()) {
            counts[output.unit()]++;
        }
        int[][] outputsByUnit = new int[counts.length][];
        for (int unit = 0; unit < counts.length; unit++) {
            outputsByUnit[unit] = new int[counts[unit]];
        }
        int[] indexes = new int[counts.length];
        for (int output = 0; output < program.outputs().size(); output++) {
            int unit = program.outputs().get(output).unit();
            outputsByUnit[unit][indexes[unit]++] = output;
        }
        return outputsByUnit;
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
        private final Streams[] results = new Streams[program.outputs().size()];
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
