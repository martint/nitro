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
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Appends a one-based row number within each logical key partition.
 *
 * <p>Partition identity is delegated to the general grouping implementation, so this operator does not know key
 * types, arity, or physical vector encodings. Dense internal group ids address allocator-owned counters. An optional
 * per-partition bound constrains the same source batch before any payload is materialized.
 */
public final class PartitionedRowNumberOperator
        implements Operator
{
    private static final long[] EMPTY_COUNTS = new long[0];
    private static final int[] EMPTY_POSITIONS = new int[0];

    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final PrimitiveArrayPool arrayPool;
    private final int[] partitionColumns;
    private final Operator source;
    private final Schema outputSchema;
    private final OptionalLong maxRowsPerPartition;
    private final Vector[] partitionValues;
    private final Vector[] partitionNulls;
    private final GroupingState groupingState;

    private long[] counts = EMPTY_COUNTS;
    private int[] selectedPositions = EMPTY_POSITIONS;
    private BatchState currentBatchState;

    public PartitionedRowNumberOperator(
            Allocator allocator,
            int[] partitionColumns,
            Operator source,
            Field rowNumberField,
            OptionalLong maxRowsPerPartition,
            OperatorResources operatorResources)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.allocationContext = new Allocator.Context("PartitionedRowNumberOperator");
        this.arrayPool = allocator.primitiveArrays();
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null").clone();
        this.source = requireNonNull(source, "source is null");
        operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        this.partitionValues = new Vector[partitionColumns.length];
        this.partitionNulls = new Vector[partitionColumns.length];
        this.groupingState = new GroupingState(
                arrayPool,
                operatorResources.codeGeneration(),
                operatorResources.grouping(),
                operatorResources.adaptiveLongGroupingPolicy(),
                operatorResources.flatKeyTablePolicy(),
                partitionTypes(source.outputSchema(), partitionColumns),
                allocator,
                allocationContext);
        this.outputSchema = outputSchema(source.outputSchema(), requireNonNull(rowNumberField, "rowNumberField is null"));
        this.maxRowsPerPartition = requireNonNull(maxRowsPerPartition, "maxRowsPerPartition is null");
        maxRowsPerPartition.ifPresent(maxRows -> {
            if (maxRows < 0) {
                throw new IllegalArgumentException("maxRowsPerPartition is negative");
            }
        });
    }

    private static List<TypeBinding> partitionTypes(Schema sourceSchema, int[] partitionColumns)
    {
        for (int column : partitionColumns) {
            if (column < 0 || column >= sourceSchema.size()) {
                throw new IllegalArgumentException("partition column is out of bounds: " + column);
            }
        }
        return Arrays.stream(partitionColumns)
                .mapToObj(column -> sourceSchema.field(column).type())
                .toList();
    }

    private static Schema outputSchema(Schema sourceSchema, Field rowNumberField)
    {
        List<Field> fields = new ArrayList<>(sourceSchema.fields());
        fields.add(rowNumberField);
        return new Schema(fields);
    }

    @Override
    public int outputCount()
    {
        return outputSchema.size();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext();
    }

    @Override
    public Batch next()
    {
        Batch sourceBatch = source.next();
        Mask sourceMask = sourceBatch.borrowMask();
        I64Vector rowNumbers = allocator.allocate(allocationContext, I64Vector.class, sourceMask.size(), I64Vector::new);
        boolean moreInputExpected = !source.supportsOpenBatchHasNext() || source.hasNext();
        assignGroups(sourceBatch, sourceMask, rowNumbers, moreInputExpected);

        int selectedCount = assignRowNumbers(sourceMask, rowNumbers.values());
        Mask outputMask = sourceMask;
        boolean ownsOutputMask = false;
        if (selectedCount != sourceMask.selectedCount()) {
            outputMask = allocator.allocateSparseMask(allocationContext, selectedPositions, selectedCount, sourceMask.size());
            ownsOutputMask = true;
            sourceBatch.constrain(outputMask);
        }

        BatchState batchState = new BatchState(sourceBatch, ownsOutputMask ? outputMask : null, rowNumbers);
        currentBatchState = batchState;
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputCount() - 1; outputIndex++) {
            Output sourceOutput = sourceBatch.output(outputIndex);
            outputs[outputIndex] = new Output(
                    sourceOutput.streams(),
                    sourceOutput::borrow,
                    (stream, vector) -> sourceOutput.take(stream),
                    (_, _) -> {},
                    (_, existing, sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange) ->
                            sourceOutput.copyPositions(existing, sourcePositions, sourceStart, sourceCount, outputStart, size, assumeClearOutputRange),
                    sourceOutput::copySinglePosition);
        }
        outputs[outputs.length - 1] = new Output(
                Set.of(Stream.VALUES),
                _ -> rowNumbers,
                (_, vector) -> {
                    batchState.rowNumbers = null;
                    return allocator.transfer(allocationContext, vector);
                },
                (_, vector) -> {
                    batchState.rowNumbers = null;
                    allocator.release(allocationContext, vector);
                });

        return new Batch(
                outputMask,
                batchState::constrain,
                batchState::takeMask,
                batchState::releaseMask,
                batchState::close,
                outputs);
    }

    private void assignGroups(Batch sourceBatch, Mask mask, I64Vector groups, boolean moreInputExpected)
    {
        if (partitionColumns.length == 1) {
            Output output = sourceBatch.output(partitionColumns[0]);
            groupingState.assignGroups(
                    output.borrow(Stream.VALUES),
                    output.borrowOrNull(Stream.NULLS),
                    mask,
                    groups,
                    moreInputExpected);
            return;
        }
        try {
            for (int index = 0; index < partitionColumns.length; index++) {
                Output output = sourceBatch.output(partitionColumns[index]);
                partitionValues[index] = output.borrow(Stream.VALUES);
                partitionNulls[index] = output.borrowOrNull(Stream.NULLS);
            }
            groupingState.assignGroups(partitionValues, partitionNulls, mask, groups, moreInputExpected);
        }
        finally {
            Arrays.fill(partitionValues, null);
            Arrays.fill(partitionNulls, null);
        }
    }

    private int assignRowNumbers(Mask mask, long[] rowNumbers)
    {
        if (selectedPositions.length < mask.selectedCount()) {
            int[] previous = selectedPositions;
            selectedPositions = arrayPool.borrowInts(mask.selectedCount());
            arrayPool.release(previous);
        }

        int selectedCount = 0;
        for (int index = 0; index < mask.selectedCount(); index++) {
            int position = mask.position(index);
            int groupId = toIntExact(rowNumbers[position]);
            ensureGroupCapacity(groupId + 1);
            long rowNumber = ++counts[groupId];
            rowNumbers[position] = rowNumber;
            if (maxRowsPerPartition.isEmpty() || rowNumber <= maxRowsPerPartition.getAsLong()) {
                selectedPositions[selectedCount++] = position;
            }
        }
        return selectedCount;
    }

    private void ensureGroupCapacity(int required)
    {
        if (counts.length >= required) {
            return;
        }
        int capacity = Math.max(required, Math.max(16, counts.length * 2));
        long[] expanded = arrayPool.borrowLongs(capacity);
        Arrays.fill(expanded, 0);
        System.arraycopy(counts, 0, expanded, 0, counts.length);
        arrayPool.release(counts);
        counts = expanded;
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return source.supportsRetainedBatches();
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        return source.supportsOpenBatchHasNext();
    }

    @Override
    public void close()
    {
        if (currentBatchState != null) {
            currentBatchState.close();
        }
        source.close();
        Arrays.fill(partitionValues, null);
        Arrays.fill(partitionNulls, null);
        groupingState.releaseBuffers();
        arrayPool.release(counts);
        arrayPool.release(selectedPositions);
        counts = EMPTY_COUNTS;
        selectedPositions = EMPTY_POSITIONS;
        allocator.release(allocationContext);
    }

    private final class BatchState
    {
        private final Batch sourceBatch;
        private final Mask ownedMask;
        private I64Vector rowNumbers;

        private BatchState(Batch sourceBatch, Mask ownedMask, I64Vector rowNumbers)
        {
            this.sourceBatch = sourceBatch;
            this.ownedMask = ownedMask;
            this.rowNumbers = rowNumbers;
        }

        private void constrain(Mask mask)
        {
            sourceBatch.constrain(mask);
        }

        private Mask takeMask(Mask mask)
        {
            if (mask == ownedMask) {
                return allocator.transfer(allocationContext, mask);
            }
            return sourceBatch.takeMask();
        }

        private void releaseMask(Mask mask)
        {
            if (mask == ownedMask) {
                allocator.release(allocationContext, mask);
            }
        }

        private void close()
        {
            if (currentBatchState == this) {
                currentBatchState = null;
            }
            if (rowNumbers != null) {
                allocator.release(allocationContext, rowNumbers);
                rowNumbers = null;
            }
            sourceBatch.close();
        }
    }
}
