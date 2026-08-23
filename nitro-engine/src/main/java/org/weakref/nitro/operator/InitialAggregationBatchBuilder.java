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
import org.weakref.nitro.data.BooleanVector;
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
    private final MutableAggregationPhaseMetrics phaseMetrics;
    private final Schema outputSchema;

    InitialAggregationBatchBuilder(
            Allocator allocator,
            Schema inputSchema,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            OperatorResources operatorResources)
    {
        this(allocator, inputSchema, groupedColumns, program, operatorResources, new MutableAggregationPhaseMetrics());
    }

    InitialAggregationBatchBuilder(
            Allocator allocator,
            Schema inputSchema,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            OperatorResources operatorResources,
            MutableAggregationPhaseMetrics phaseMetrics)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
        this.groupedColumns = requireNonNull(groupedColumns, "groupedColumns is null").stream()
                .mapToInt(Integer::intValue)
                .toArray();
        this.program = requireNonNull(program, "program is null");
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        this.phaseMetrics = requireNonNull(phaseMetrics, "phaseMetrics is null");
        this.outputSchema = outputSchema(inputSchema, this.groupedColumns, program.outputSchema());
    }

    Schema outputSchema()
    {
        return outputSchema;
    }

    Batch build(Batch input)
    {
        requireNonNull(input, "input is null");
        return build(input, input.borrowMask());
    }

    Batch build(Batch input, Mask inputMask)
    {
        return build(input, inputMask, false);
    }

    private Batch build(Batch input, Mask inputMask, boolean retainInput)
    {
        requireNonNull(input, "input is null");
        requireNonNull(inputMask, "inputMask is null");
        Allocator.Context context = new Allocator.Context(
                "InitialAggregationBatch",
                operatorResources.aggregation().bufferPoolGroup());
        try {
            Batch direct = buildDirect(input, inputMask, context, retainInput);
            if (direct != null) {
                return direct;
            }
            if (retainInput && !inputMask.all()) {
                allocator.release(context);
                return null;
            }
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
                outputs[output] = retainInput
                        ? input.output(groupedColumns[output])
                        : ownedOutput(copyGroupedStreams(input, output, inputMask, context), context);
            }
            long start = System.nanoTime();
            try {
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
            }
            finally {
                phaseMetrics.recordInitialAggregation(System.nanoTime() - start);
            }
            return new Batch(
                    outputMask,
                    _ -> {},
                    mask -> allocator.transfer(context, mask),
                    _ -> {},
                    () -> close(context, retainInput ? input : null),
                    outputs);
        }
        catch (Throwable throwable) {
            allocator.release(context);
            throw throwable;
        }
    }

    /**
     * Builds direct initial aggregation output while transferring the input batch lifetime to the
     * output. Sparse input is retained only when every aggregation can preserve physical row
     * positions. Returning {@code null} means retention would require compaction or materialization.
     */
    Batch buildRetaining(Batch input)
    {
        requireNonNull(input, "input is null");
        Mask inputMask = input.borrowMask();
        return build(input, inputMask, true);
    }

    /**
     * Builds position-preserving initial output for a selected subset while transferring the complete input batch
     * lifetime to the result. Retaining the input is useful when grouped streams can be borrowed directly: the
     * output mask constrains every consumer to {@code inputMask}, and closing the result closes the retained input.
     * Returning {@code null} means at least one aggregation result would require compaction.
     */
    Batch buildRetaining(Batch input, Mask inputMask)
    {
        requireNonNull(input, "input is null");
        requireNonNull(inputMask, "inputMask is null");
        return build(input, inputMask, true);
    }

    Batch empty()
    {
        Allocator.Context context = new Allocator.Context(
                "InitialAggregationBatch",
                operatorResources.aggregation().bufferPoolGroup());
        try {
            Output[] outputs = new Output[outputSchema.size()];
            for (int output = 0; output < outputs.length; output++) {
                var type = outputSchema.field(output).type();
                org.weakref.nitro.data.Vector values = !type.isSpecified()
                        ? allocator.allocate(context, I64Vector.class, 0, I64Vector::new)
                        : type.vectorFactory()
                                .orElseThrow(() -> new IllegalStateException("Output type does not provide a vector factory"))
                                .nullValues(allocator.vectorAllocator(context), 0);
                BooleanVector nulls = allocator.allocate(context, BooleanVector.class, 0, BooleanVector::new);
                outputs[output] = ownedOutput(Streams.ofValuesAndNulls(values, nulls), context);
            }
            Mask outputMask = allocator.allocateAllMask(context, 0);
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

    private Batch buildDirect(Batch input, Mask inputMask, Allocator.Context context)
    {
        return buildDirect(input, inputMask, context, false);
    }

    private Batch buildDirect(Batch input, Mask inputMask, Allocator.Context context, boolean retainInput)
    {
        for (PhysicalAggregationUnit unit : program.units()) {
            if (!unit.supportsInitialInput()) {
                return null;
            }
            if (retainInput && !inputMask.all() && !unit.supportsPositionPreservingInitialInput()) {
                return null;
            }
        }

        boolean preservePositions = retainInput && !inputMask.all();
        int groupCount = inputMask.count();
        Mask outputMask = preservePositions
                ? allocator.copyMask(context, inputMask)
                : allocator.allocateAllMask(context, groupCount);
        Output[] outputs = new Output[groupedColumns.length + program.outputs().size()];
        for (int output = 0; output < groupedColumns.length; output++) {
            outputs[output] = retainInput
                    ? input.output(groupedColumns[output])
                    : ownedOutput(copyGroupedStreams(input, output, inputMask, context), context);
        }
        long start = System.nanoTime();
        try {
            for (int output = 0; output < program.outputs().size(); output++) {
                PhysicalAggregationProgram.Output binding = program.outputs().get(output);
                PhysicalAggregationUnit unit = program.units().get(binding.unit());
                Streams result = requireNonNull(
                        preservePositions
                                ? unit.positionPreservingInitialInput(
                                        binding.result(),
                                        inputMask,
                                        StreamAccessors.forBatch(input),
                                        allocator,
                                        context)
                                : unit.initialInput(
                                        binding.result(),
                                        inputMask,
                                        StreamAccessors.forBatch(input),
                                        allocator,
                                        context),
                        "direct initial aggregation result is null");
                outputs[groupedColumns.length + output] = ownedOutput(result, context);
            }
        }
        finally {
            phaseMetrics.recordInitialAggregation(System.nanoTime() - start);
        }
        return new Batch(
                outputMask,
                _ -> {},
                mask -> allocator.transfer(context, mask),
                _ -> {},
                () -> close(context, retainInput ? input : null),
                outputs);
    }

    private void close(Allocator.Context context, Batch retainedInput)
    {
        try {
            allocator.release(context);
        }
        finally {
            if (retainedInput != null) {
                retainedInput.close();
            }
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

    private Streams copyGroupedStreams(Batch input, int outputIndex, Mask inputMask, Allocator.Context context)
    {
        long start = System.nanoTime();
        Streams borrowed = borrowedStreams(input.output(groupedColumns[outputIndex]));
        boolean encoded = borrowed.values().childVectorCount() != 0;
        try {
            return allocator.copyStreams(context, borrowed, inputMask);
        }
        finally {
            long nanos = System.nanoTime() - start;
            phaseMetrics.recordInitialKey(nanos);
            if (encoded) {
                phaseMetrics.recordInitialEncodedKey(nanos);
            }
            else {
                phaseMetrics.recordInitialFlatKey(nanos);
            }
        }
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
