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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.aggregation.AggregationExecutionContext;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationUnit;
import org.weakref.nitro.operator.aggregation.StreamAccessors;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.data.Stream.VALUES;

/**
 * Long-lived global aggregation state that accepts independently scheduled input batches.
 *
 * <p>The host or cooperative source driver owns input availability and blocking. This session owns
 * only Nitro aggregation state and result materialization, so a source may pause between batches
 * without being mistaken for end of input.
 */
public final class AggregationSession
        implements BatchAggregationSession
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final AggregationExecutionContext aggregationExecutionContext;
    private final OperatorResources operatorResources;
    private final boolean deferResultMaterialization;
    private final PhysicalAggregationProgram program;
    private final List<PhysicalAggregationUnit> units;
    private final PhysicalAggregationUnit[] unitArray;
    private final DistinctAggregationPlan distinctAggregationPlan;
    private final int[][] outputsByUnit;
    private final Streams[] reusableResults;

    private Object[] state;
    private boolean materialized;
    private boolean finished;
    private boolean closed;

    public AggregationSession(
            Allocator allocator,
            Schema inputSchema,
            PhysicalAggregationProgram program,
            OperatorResources operatorResources)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.program = requireNonNull(program, "program is null");
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        AggregationOperatorResources aggregationResources = operatorResources.aggregation();
        allocationContext = new Allocator.Context(
                "AggregationSession",
                aggregationResources.bufferPoolGroup());
        aggregationExecutionContext = new AggregationExecutionContext(
                allocator,
                allocationContext,
                operatorResources.codeGeneration(),
                operatorResources.distinctKeySetPolicy(),
                operatorResources.adaptiveLongGroupingPolicy(),
                operatorResources.flatKeyTablePolicy(),
                requireNonNull(inputSchema, "inputSchema is null"));
        deferResultMaterialization = aggregationResources.policy().deferResultMaterialization();
        units = program.units();
        unitArray = units.toArray(PhysicalAggregationUnit[]::new);
        distinctAggregationPlan = DistinctAggregationPlan.plan(unitArray, false, false, inputSchema);
        outputsByUnit = outputsByUnit(program);
        reusableResults = new Streams[program.outputs().size()];
    }

    public Schema outputSchema()
    {
        return program.outputSchema();
    }

    @Override
    public long retainedBytes()
    {
        return allocator.scopeCurrentBytes(allocationContext);
    }

    public void addInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkAcceptingInput();
        ensureState();
        Mask mask = batch.borrowMask();
        if (mask.none()) {
            return;
        }
        var streamAccessor = StreamAccessors.forBatch(batch);
        for (int unit : distinctAggregationPlan.plainAggregationIndexes()) {
            PhysicalAggregationUnit aggregationUnit = unitArray[unit];
            aggregationUnit.accumulate(state[unit], 0, mask, streamAccessor);
        }
        for (int unit : distinctAggregationPlan.filteredAggregationIndexes()) {
            PhysicalAggregationUnit aggregationUnit = unitArray[unit];
            int filterColumn = aggregationUnit.filterInputColumn();
            Mask aggregationMask = filterMask(batch, filterColumn, mask);
            try {
                aggregationUnit.accumulate(state[unit], 0, aggregationMask, streamAccessor);
            }
            finally {
                if (aggregationMask != mask) {
                    allocator.release(allocationContext, aggregationMask);
                }
            }
        }
        if (distinctAggregationPlan.distinctAggregationGroups().length > 0) {
            for (DistinctAggregationPlan.Group distinctGroup : distinctAggregationPlan.distinctAggregationGroups()) {
                Mask filteredMask = distinctGroup.filterInputColumn() < 0
                        ? mask
                        : filterMask(batch, distinctGroup.filterInputColumn(), mask);
                try {
                    Mask distinctMask = distinctGroup.select(
                            null,
                            filteredMask,
                            streamAccessor,
                            1,
                            allocator,
                            allocationContext,
                            operatorResources.codeGeneration(),
                            operatorResources.distinctKeySetPolicy(),
                            operatorResources.adaptiveLongGroupingPolicy(),
                            operatorResources.flatKeyTablePolicy());
                    try {
                        for (int unit : distinctGroup.aggregationIndexes()) {
                            unitArray[unit].accumulateDistinctSelected(state[unit], 0, distinctMask, streamAccessor);
                        }
                    }
                    finally {
                        allocator.release(allocationContext, distinctMask);
                    }
                }
                finally {
                    if (filteredMask != mask) {
                        allocator.release(allocationContext, filteredMask);
                    }
                }
            }
        }
        if (!deferResultMaterialization) {
            materializeResults();
        }
    }

    public Batch finish()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("aggregation session is already finished");
        }
        finished = true;
        ensureState();
        if (deferResultMaterialization || !materialized) {
            materializeResults();
        }

        Mask mask = allocator.allocateAllMask(allocationContext, 1);
        Output[] outputs = new Output[program.outputs().size()];
        for (int output = 0; output < outputs.length; output++) {
            int result = output;
            outputs[output] = new Output(
                    reusableResults[result].streams(),
                    reusableResults[result]::get,
                    (_, vector) -> allocator.transfer(allocationContext, vector));
        }
        return new Batch(
                mask,
                _ -> {},
                takenMask -> allocator.transfer(allocationContext, takenMask),
                outputs);
    }

    private void ensureState()
    {
        if (state != null) {
            return;
        }
        state = new Object[units.size()];
        for (int unit = 0; unit < state.length; unit++) {
            state[unit] = units.get(unit).allocate(aggregationExecutionContext, 1);
            units.get(unit).initialize(state[unit], 0, 1);
        }
    }

    private void materializeResults()
    {
        for (int unit = 0; unit < units.size(); unit++) {
            materializeUnitResults(unit);
        }
    }

    private void materializeUnitResults(int unit)
    {
        PhysicalAggregationUnit aggregationUnit = units.get(unit);
        for (int output : outputsByUnit[unit]) {
            PhysicalAggregationProgram.Output binding = program.outputs().get(output);
            reusableResults[output] = aggregationUnit.result(
                    binding.result(),
                    0,
                    state[unit],
                    reusableResults[output],
                    allocator,
                    allocationContext);
        }
        materialized = true;
    }

    private Mask filterMask(Batch batch, int filterColumn, Mask mask)
    {
        Output output = batch.output(filterColumn);
        Mask direct = output.tryBorrowMask(VALUES, mask, true, allocator, allocationContext);
        if (direct != null) {
            return direct;
        }
        Mask selected = allocator.copyMask(allocationContext, mask);
        Vector values = output.borrow(VALUES);
        if (!selected.tryRetainBooleanVector(values, true)) {
            var booleanValues = VectorAccess.booleanValues(values);
            selected.retainIf(position -> booleanValues.value(position));
        }
        return selected;
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

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("aggregation session is finished");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("aggregation session is closed");
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        distinctAggregationPlan.releaseBuffers();
        allocator.release(allocationContext);
    }
}
