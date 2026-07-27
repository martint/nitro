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
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class GroupOperator
        implements Operator, GroupedKeySource
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext;

    private final int[] groupByColumns;
    private final Operator source;
    private final Schema outputSchema;
    private final Vector[] groupValues;
    private final Vector[] groupNulls;
    private final GroupingState groupingState;
    private BatchState currentBatchState;
    private I64Vector reusableResult;

    public GroupOperator(Allocator allocator, int groupByColumn, Operator source)
    {
        this(allocator, new int[] {groupByColumn}, source);
    }

    public GroupOperator(Allocator allocator, int[] groupByColumns, Operator source)
    {
        this(allocator, groupByColumns, source, allocator.engineResources().operatorResources());
    }

    public GroupOperator(Allocator allocator, int groupByColumn, Operator source, OperatorResources operatorResources)
    {
        this(allocator, new int[] {groupByColumn}, source, operatorResources);
    }

    public GroupOperator(Allocator allocator, int[] groupByColumns, Operator source, OperatorResources operatorResources)
    {
        this(
                allocator,
                groupByColumns,
                source,
                new Field(Schema.unspecified(1).field(0).type(), false),
                operatorResources);
    }

    public GroupOperator(
            Allocator allocator,
            int[] groupByColumns,
            Operator source,
            Field groupIdField,
            OperatorResources operatorResources)
    {
        this.allocator = allocator;
        operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        this.allocationContext = new Allocator.Context(
                "GroupOperator",
                operatorResources.grouping().groupOperatorBufferPool());
        this.groupingState = new GroupingState(
                allocator.primitiveArrays(),
                operatorResources.codeGeneration(),
                operatorResources.grouping(),
                operatorResources.adaptiveLongGroupingPolicy(),
                operatorResources.flatKeyTablePolicy(),
                groupingTypes(source.outputSchema(), groupByColumns));
        this.groupByColumns = groupByColumns.clone();
        this.source = source;
        this.outputSchema = outputSchema(source.outputSchema(), groupIdField);
        this.groupValues = new Vector[groupByColumns.length];
        this.groupNulls = new Vector[groupByColumns.length];
    }

    private static List<TypeBinding> groupingTypes(Schema sourceSchema, int[] groupByColumns)
    {
        for (int column : groupByColumns) {
            if (column < 0 || column >= sourceSchema.size()) {
                return List.of();
            }
        }
        return Arrays.stream(groupByColumns)
                .mapToObj(column -> sourceSchema.field(column).type())
                .toList();
    }

    @Override
    public int outputCount()
    {
        return source.outputCount() + 1;
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    private static Schema outputSchema(Schema sourceSchema, Field groupIdField)
    {
        List<Field> fields = new ArrayList<>(sourceSchema.size() + 1);
        fields.add(requireNonNull(groupIdField, "groupIdField is null"));
        fields.addAll(sourceSchema.fields());
        return new Schema(fields);
    }

    @Override
    public Batch next()
    {
        Batch sourceBatch = source.next();
        BatchState batchState = new BatchState(sourceBatch, sourceBatch.borrowMask(), source.hasNext());
        currentBatchState = batchState;

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            if (outputIndex == 0) {
                outputs[outputIndex] = new Output(
                        java.util.Set.of(Stream.VALUES),
                        stream -> groupIds(batchState),
                        (stream, vector) -> {
                            if (vector == reusableResult) {
                                reusableResult = null;
                            }
                            batchState.result = null;
                            return allocator.transfer(allocationContext, vector);
                        });
            }
            else {
                Output sourceOutput = sourceBatch.output(outputIndex - 1);
                outputs[outputIndex] = new Output(
                        sourceOutput.streams(),
                        sourceOutput::borrow,
                        (stream, vector) -> sourceOutput.take(stream),
                        (_, _) -> {},
                        sourceOutput::copySinglePosition);
            }
        }
        return new Batch(
                batchState.mask,
                batchState::constrain,
                ignored -> sourceBatch.takeMask(),
                _ -> {},
                () -> {
                    if (currentBatchState == batchState) {
                        currentBatchState = null;
                    }
                    sourceBatch.close();
                },
                outputs);
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext();
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    private Vector groupIds(BatchState batchState)
    {
        doGroupingIfNeeded(batchState);
        return batchState.result;
    }

    private void doGroupingIfNeeded(BatchState batchState)
    {
        if (!batchState.filled && !batchState.mask.none()) {
            batchState.filled = true;
            reusableResult = allocator.reallocateIfNecessary(allocationContext, reusableResult, I64Vector.class, batchState.mask.maxPosition() + 1, I64Vector::new);
            batchState.result = reusableResult;
            if (groupByColumns.length == 1) {
                int groupByColumn = groupByColumns[0];
                groupingState.assignGroups(
                        batchState.sourceBatch.output(groupByColumn).borrow(Stream.VALUES),
                        batchState.sourceBatch.output(groupByColumn).borrowOrNull(Stream.NULLS),
                        batchState.mask,
                        batchState.result,
                        batchState.moreInputExpected);
            }
            else {
                try {
                    for (int index = 0; index < groupByColumns.length; index++) {
                        Output output = batchState.sourceBatch.output(groupByColumns[index]);
                        groupValues[index] = output.borrow(Stream.VALUES);
                        groupNulls[index] = output.borrowOrNull(Stream.NULLS);
                    }
                    groupingState.assignGroups(groupValues, groupNulls, batchState.mask, batchState.result, batchState.moreInputExpected);
                }
                finally {
                    Arrays.fill(groupValues, null);
                    Arrays.fill(groupNulls, null);
                }
            }
        }
    }

    @Override
    public void close()
    {
        if (currentBatchState != null) {
            currentBatchState.sourceBatch.close();
            currentBatchState = null;
        }
        source.close();
        Arrays.fill(groupValues, null);
        Arrays.fill(groupNulls, null);
        groupingState.releaseBuffers();
        allocator.release(allocationContext);
    }

    @Override
    public Streams groupedKeyOutput(int outputIndex, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        return groupingState.groupedValues(groupedKeyIndex(outputIndex), mask, output, allocator, allocationContext);
    }

    @Override
    public Streams copyGroupedKeyPosition(int outputIndex, Streams output, int sourcePosition, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        return groupingState.copyGroupedValuePosition(groupedKeyIndex(outputIndex), output, sourcePosition, outputPosition, size, allocator, allocationContext);
    }

    private int groupedKeyIndex(int outputIndex)
    {
        for (int index = 0; index < groupByColumns.length; index++) {
            if (outputIndex == groupByColumns[index] + 1) {
                return index;
            }
        }
        throw new IllegalArgumentException("Output " + outputIndex + " is not a grouping key output");
    }

    private static final class BatchState
    {
        private final Batch sourceBatch;
        private final boolean moreInputExpected;
        private Mask mask;
        private boolean filled;
        private I64Vector result;

        private BatchState(Batch sourceBatch, Mask mask, boolean moreInputExpected)
        {
            this.sourceBatch = sourceBatch;
            this.mask = mask;
            this.moreInputExpected = moreInputExpected;
        }

        private void constrain(Mask mask)
        {
            this.mask = mask;
            sourceBatch.constrain(mask);
        }
    }
}
