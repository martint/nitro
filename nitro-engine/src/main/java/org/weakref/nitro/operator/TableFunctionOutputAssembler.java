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

import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.function.table.TableFunctionOutputBatch;
import org.weakref.nitro.core.function.table.TableFunctionPassThroughColumn;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.MaskSelection;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorBatchScope;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Transfers proper table-function outputs and lazily gathers requested pass-through columns from buffered input
 * partitions. Row-reference columns are engine metadata and are not exposed downstream.
 */
final class TableFunctionOutputAssembler
{
    record Argument(Schema schema, RowPositionIndex rows)
    {
        Argument
        {
            requireNonNull(schema, "schema is null");
            requireNonNull(rows, "rows is null");
        }
    }

    private final Allocator allocator;
    private final Schema outputSchema;
    private final int properOutputCount;
    private final List<TableFunctionPassThroughColumn> passThroughColumns;
    private final List<Argument> arguments;
    private final int[] partitionStarts;
    private final int[] partitionEnds;

    TableFunctionOutputAssembler(
            Allocator allocator,
            Schema outputSchema,
            int properOutputCount,
            List<TableFunctionPassThroughColumn> passThroughColumns,
            List<Argument> arguments,
            int[] partitionStarts,
            int[] partitionEnds)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        if (properOutputCount < 0 || properOutputCount > outputSchema.size()) {
            throw new IllegalArgumentException("proper output count is invalid");
        }
        this.properOutputCount = properOutputCount;
        this.passThroughColumns = List.copyOf(passThroughColumns);
        this.arguments = List.copyOf(arguments);
        this.partitionStarts = requireNonNull(partitionStarts, "partitionStarts is null").clone();
        this.partitionEnds = requireNonNull(partitionEnds, "partitionEnds is null").clone();
        if (properOutputCount + this.passThroughColumns.size() != outputSchema.size()) {
            throw new IllegalArgumentException("output schema does not match pass-through columns");
        }
        if (this.partitionStarts.length != this.arguments.size() || this.partitionEnds.length != this.arguments.size()) {
            throw new IllegalArgumentException("partition bounds do not match arguments");
        }
        for (int argument = 0; argument < this.arguments.size(); argument++) {
            if (this.partitionStarts[argument] < 0 ||
                    this.partitionEnds[argument] < this.partitionStarts[argument] ||
                    this.partitionEnds[argument] > this.arguments.get(argument).rows().size()) {
                throw new IllegalArgumentException("partition bounds are invalid");
            }
        }
        for (int index = 0; index < this.passThroughColumns.size(); index++) {
            TableFunctionPassThroughColumn column = this.passThroughColumns.get(index);
            if (column.argument() >= this.arguments.size()) {
                throw new IllegalArgumentException("pass-through argument is out of bounds");
            }
            Argument argument = this.arguments.get(column.argument());
            if (column.inputColumn() >= argument.schema().size()) {
                throw new IllegalArgumentException("pass-through input column is out of bounds");
            }
            int outputColumn = properOutputCount + index;
            if (!argument.schema().field(column.inputColumn()).type().identity()
                    .equals(outputSchema.field(outputColumn).type().identity())) {
                throw new IllegalArgumentException("pass-through input type does not match output schema");
            }
        }
    }

    Batch assemble(TableFunctionOutputBatch output)
    {
        requireNonNull(output, "output is null");
        VectorBatchScope buffers = new VectorBatchScope(allocator, "TableFunctionOutputAssembler");
        buffers.begin(null);
        boolean transferred = false;
        try (output) {
            validate(output);
            Selection selection = output.selection();
            Mask mask = copySelection(buffers.context(), selection);
            Map<Integer, Streams> references = references(output, buffers);
            Output[] columns = new Output[outputSchema.size()];
            for (int column = 0; column < properOutputCount; column++) {
                columns[column] = transfer(output.column(column), buffers);
            }
            for (int index = 0; index < passThroughColumns.size(); index++) {
                TableFunctionPassThroughColumn passThrough = passThroughColumns.get(index);
                Streams reference = references.get(passThrough.argument());
                int outputColumn = properOutputCount + index;
                LazyGather gather = new LazyGather(
                        arguments.get(passThrough.argument()).rows(),
                        partitionStarts[passThrough.argument()],
                        partitionEnds[passThrough.argument()],
                        passThrough.inputColumn(),
                        outputSchema,
                        outputColumn,
                        reference,
                        selection,
                        buffers.context());
                columns[outputColumn] = new Output(Set.of(Stream.VALUES, Stream.NULLS, Stream.ERRORS), gather::stream, buffers);
            }
            Batch result = new Batch(
                    mask,
                    _ -> {},
                    buffers::take,
                    buffers::release,
                    buffers::endBatch,
                    columns);
            transferred = true;
            return result;
        }
        finally {
            if (!transferred) {
                buffers.close();
            }
        }
    }

    private void validate(TableFunctionOutputBatch output)
    {
        if (output.properOutputCount() != properOutputCount) {
            throw new IllegalArgumentException("proper output count does not match plan");
        }
        if (output.schema().size() != properOutputCount) {
            throw new IllegalArgumentException("table-function output schema does not match proper outputs");
        }
        if (output.selection().count() == 0) {
            throw new IllegalArgumentException("table-function output is empty");
        }
        for (int column = 0; column < properOutputCount; column++) {
            if (!output.column(column).type().identity().equals(outputSchema.field(column).type().identity())) {
                throw new IllegalArgumentException("proper output type does not match output schema");
            }
        }
    }

    private Map<Integer, Streams> references(TableFunctionOutputBatch output, VectorBatchScope buffers)
    {
        Map<Integer, Streams> references = new HashMap<>();
        for (TableFunctionOutputBatch.PassThroughReference reference : output.passThroughReferences()) {
            int argument = reference.argument();
            if (argument >= arguments.size()) {
                throw new IllegalArgumentException("pass-through reference argument is out of bounds");
            }
            if (reference.positions().positionCount() != output.selection().positionCount()) {
                throw new IllegalArgumentException("pass-through reference domain does not match output");
            }
            if (references.put(argument, transferStreams(reference.positions(), buffers)) != null) {
                throw new IllegalArgumentException("duplicate pass-through reference argument");
            }
        }
        for (TableFunctionPassThroughColumn column : passThroughColumns) {
            if (!references.containsKey(column.argument())) {
                throw new IllegalArgumentException("requested pass-through argument has no row reference");
            }
        }
        return Map.copyOf(references);
    }

    private Output transfer(ColumnView input, VectorBatchScope buffers)
    {
        Streams transferred = transferStreams(input, buffers);
        return new Output(transferred.streams(), transferred::get, buffers);
    }

    private Streams transferStreams(ColumnView input, VectorBatchScope buffers)
    {
        Streams.Builder streams = Streams.builder();
        for (Stream stream : input.streams()) {
            Vector vector = input.take(stream);
            streams.put(stream, allocator.transferOwned(buffers.context(), vector));
        }
        return streams.build();
    }

    private Mask copySelection(Allocator.Context context, Selection selection)
    {
        if (selection instanceof MaskSelection maskSelection) {
            return allocator.copyMask(context, maskSelection.mask());
        }
        int[] positions = new int[selection.count()];
        for (int index = 0; index < positions.length; index++) {
            positions[index] = selection.position(index);
        }
        return allocator.allocateSparseMask(context, positions, selection.positionCount());
    }

    private final class LazyGather
    {
        private final RowPositionIndex argument;
        private final int partitionStart;
        private final int partitionEnd;
        private final int inputColumn;
        private final Schema schema;
        private final int outputColumn;
        private final Streams reference;
        private final Selection selection;
        private final Allocator.Context context;

        private Streams streams;

        private LazyGather(
                RowPositionIndex argument,
                int partitionStart,
                int partitionEnd,
                int inputColumn,
                Schema schema,
                int outputColumn,
                Streams reference,
                Selection selection,
                Allocator.Context context)
        {
            this.argument = argument;
            this.partitionStart = partitionStart;
            this.partitionEnd = partitionEnd;
            this.inputColumn = inputColumn;
            this.schema = schema;
            this.outputColumn = outputColumn;
            this.reference = reference;
            this.selection = selection;
            this.context = context;
        }

        private Vector stream(Stream stream)
        {
            if (streams == null) {
                streams = gather();
            }
            return streams.get(stream);
        }

        private Streams gather()
        {
            int size = selection.positionCount();
            Streams result = WindowValueCopySupport.mutableNullOutput(
                    schema.field(outputColumn).type().vectorFactory()
                            .orElseThrow(() -> new IllegalArgumentException("pass-through type has no vector factory")),
                    allocator,
                    context,
                    size);
            BooleanVector errors = allocator.allocate(context, BooleanVector.class, size, BooleanVector::new);
            result = allocator.reuseOrCreateStreams(result, result.values(), result.get(Stream.NULLS), errors);

            for (int index = 0; index < selection.count(); index++) {
                int outputPosition = selection.position(index);
                if (OperatorVectorSupport.isNull(reference.getOrNull(Stream.NULLS), outputPosition)) {
                    continue;
                }
                Vector referenceErrors = reference.getOrNull(Stream.ERRORS);
                if (referenceErrors != null && OperatorVectorSupport.booleanValue(referenceErrors, outputPosition)) {
                    throw new IllegalArgumentException("pass-through row reference is an error");
                }
                long relativePosition = OperatorVectorSupport.longValue(reference.values(), outputPosition);
                long sourcePosition = (long) partitionStart + relativePosition;
                if (relativePosition < 0 || sourcePosition >= partitionEnd) {
                    throw new IllegalArgumentException("pass-through row reference is outside its partition");
                }
                int position = toIntExact(sourcePosition);
                result = allocator.copySinglePositionInto(
                        context,
                        argument.column(inputColumn, position),
                        result,
                        argument.sourcePosition(position),
                        outputPosition,
                        size);
            }
            return result;
        }
    }
}
