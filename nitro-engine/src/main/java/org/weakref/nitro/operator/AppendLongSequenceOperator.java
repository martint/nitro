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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Appends a non-null long sequence while preserving every source row.
 *
 * <p>The allocator is supplied by the composition boundary, so this operator does not define
 * identity scope or encoding. A caller may share one allocator among parallel operator instances
 * when sequence ranges must be disjoint.
 */
public final class AppendLongSequenceOperator
        implements Operator
{
    @FunctionalInterface
    public interface RangeAllocator
    {
        long allocate(int length);
    }

    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context(
            "AppendLongSequenceOperator",
            AppendLongSequenceOperator.class);
    private final Operator source;
    private final RangeAllocator rangeAllocator;
    private final Schema outputSchema;

    private Batch currentBatch;

    public AppendLongSequenceOperator(
            Allocator allocator,
            Operator source,
            Field sequenceField,
            RangeAllocator rangeAllocator)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.source = requireNonNull(source, "source is null");
        this.rangeAllocator = requireNonNull(rangeAllocator, "rangeAllocator is null");
        List<Field> fields = new ArrayList<>(source.outputSchema().fields());
        fields.add(requireNonNull(sequenceField, "sequenceField is null"));
        outputSchema = new Schema(fields);
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

    @Override
    public boolean hasNext()
    {
        return source.hasNext();
    }

    @Override
    public Batch next()
    {
        Batch sourceBatch = source.next();
        currentBatch = sourceBatch;
        Mask mask = sourceBatch.borrowMask();
        I64Vector sequence = allocator.allocate(allocationContext, I64Vector.class, mask.size(), I64Vector::new);
        long first = rangeAllocator.allocate(mask.selectedCount());
        for (int index = 0; index < mask.selectedCount(); index++) {
            sequence.values()[mask.position(index)] = first + index;
        }

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < source.outputCount(); outputIndex++) {
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
                _ -> sequence,
                (_, vector) -> allocator.transfer(allocationContext, vector),
                (_, vector) -> allocator.release(allocationContext, vector));

        return new Batch(
                mask,
                sourceBatch::constrain,
                ignored -> sourceBatch.takeMask(),
                _ -> {},
                () -> {
                    sourceBatch.close();
                    if (currentBatch == sourceBatch) {
                        currentBatch = null;
                    }
                },
                outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        if (currentBatch != null) {
            currentBatch.constrain(mask);
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
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }
        source.close();
        allocator.release(allocationContext);
    }
}
