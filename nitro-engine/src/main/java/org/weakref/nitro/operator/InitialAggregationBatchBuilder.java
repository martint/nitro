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

import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.aggregation.AggregationExecutionContext;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationUnit;
import org.weakref.nitro.operator.aggregation.StreamAccessors;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Lowers every selected raw input row to one initial aggregation row without hash grouping.
 *
 * <p>The supplied physical program determines the state representation and result form. A partial
 * aggregation program therefore emits initial intermediate rows that a downstream aggregation can
 * merge. The builder neither recognizes aggregate functions nor depends on a host engine's
 * aggregation controller.
 */
final class InitialAggregationBatchBuilder
{
    private final Allocator allocator;
    private final Schema inputSchema;
    private final int[] groupedColumns;
    private final PhysicalAggregationProgram program;
    private final OperatorResources operatorResources;
    private final Schema outputSchema;

    InitialAggregationBatchBuilder(
            Allocator allocator,
            Schema inputSchema,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            OperatorResources operatorResources)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
        this.groupedColumns = requireNonNull(groupedColumns, "groupedColumns is null").stream()
                .mapToInt(Integer::intValue)
                .toArray();
        this.program = requireNonNull(program, "program is null");
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        this.outputSchema = outputSchema(inputSchema, this.groupedColumns, program.outputSchema());
    }

    Schema outputSchema()
    {
        return outputSchema;
    }

    Batch build(Batch input)
    {
        requireNonNull(input, "input is null");
        Allocator.Context context = new Allocator.Context(
                "InitialAggregationBatch",
                operatorResources.aggregation().bufferPoolGroup());
        try {
            Mask inputMask = input.borrowMask();
            int groupCount = inputMask.count();
            int maxGroup = groupCount - 1;
            Mask outputMask = allocator.allocateAllMask(context, groupCount);

            I64Vector groups = allocator.allocate(
                    context,
                    I64Vector.class,
                    inputMask.none() ? 0 : inputMask.maxPosition() + 1,
                    I64Vector::new);
            int group = 0;
            for (int position : inputMask) {
                groups.values()[position] = group++;
            }

            AggregationExecutionContext executionContext = new AggregationExecutionContext(
                    allocator,
                    context,
                    operatorResources.codeGeneration(),
                    operatorResources.distinctKeySetPolicy(),
                    operatorResources.adaptiveLongGroupingPolicy(),
                    operatorResources.flatKeyTablePolicy(),
                    inputSchema);
            Object[] states = new Object[program.units().size()];
            for (int unit = 0; unit < states.length; unit++) {
                PhysicalAggregationUnit aggregation = program.units().get(unit);
                states[unit] = aggregation.allocate(executionContext, groupCount);
                aggregation.initialize(states[unit], 0, groupCount);

                int filterColumn = aggregation.filterInputColumn();
                Mask aggregationMask = filterColumn < 0 ? inputMask : filterMask(input, filterColumn, inputMask, context);
                try {
                    if (aggregation.distinctInputColumns() == null) {
                        aggregation.accumulate(states[unit], groups, aggregationMask, StreamAccessors.forBatch(input));
                    }
                    else {
                        // Every input row has a unique group, so no two rows can be duplicates within a group.
                        aggregation.accumulateDistinctSelected(states[unit], groups, aggregationMask, StreamAccessors.forBatch(input));
                    }
                }
                finally {
                    if (aggregationMask != inputMask) {
                        allocator.release(context, aggregationMask);
                    }
                }
            }

            Output[] outputs = new Output[groupedColumns.length + program.outputs().size()];
            for (int output = 0; output < groupedColumns.length; output++) {
                Streams copied = allocator.copyStreams(context, borrowedStreams(input.output(groupedColumns[output])), inputMask);
                outputs[output] = ownedOutput(copied, context);
            }
            for (int output = 0; output < program.outputs().size(); output++) {
                PhysicalAggregationProgram.Output binding = program.outputs().get(output);
                Streams result = program.units().get(binding.unit()).result(
                        binding.result(),
                        maxGroup,
                        states[binding.unit()],
                        outputMask,
                        null,
                        allocator,
                        context);
                outputs[groupedColumns.length + output] = ownedOutput(result, context);
            }
            return new Batch(
                    outputMask,
                    _ -> {},
                    mask -> allocator.transfer(context, mask),
                    _ -> {},
                    () -> allocator.release(context),
                    outputs);
        }
        catch (Throwable throwable) {
            allocator.release(context);
            throw throwable;
        }
    }

    private Mask filterMask(Batch batch, int filterColumn, Mask mask, Allocator.Context context)
    {
        Output output = batch.output(filterColumn);
        Mask direct = output.tryBorrowMask(Stream.VALUES, mask, true, allocator, context);
        if (direct != null) {
            return direct;
        }
        Mask selected = allocator.copyMask(context, mask);
        var values = VectorAccess.booleanValues(output.borrow(Stream.VALUES));
        selected.retainIf(position -> values.value(position));
        return selected;
    }

    private Output ownedOutput(Streams streams, Allocator.Context context)
    {
        return new Output(
                streams.streams(),
                streams::get,
                (_, vector) -> allocator.transfer(context, vector));
    }

    private static Streams borrowedStreams(Output output)
    {
        Streams.Builder streams = Streams.builder();
        for (Stream stream : output.streams()) {
            streams.put(stream, output.borrow(stream));
        }
        return streams.build();
    }

    private static Schema outputSchema(Schema inputSchema, int[] groupedColumns, Schema aggregationSchema)
    {
        List<Field> fields = new ArrayList<>(groupedColumns.length + aggregationSchema.size());
        for (int groupedColumn : groupedColumns) {
            fields.add(inputSchema.field(groupedColumn));
        }
        fields.addAll(aggregationSchema.fields());
        return new Schema(fields);
    }
}
