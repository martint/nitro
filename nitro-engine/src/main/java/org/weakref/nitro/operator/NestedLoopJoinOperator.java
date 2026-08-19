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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.source.ExternallyScheduledSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class NestedLoopJoinOperator
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("NestedLoopJoinOperator", NestedLoopJoinOperator.class);

    private final NestedLoopJoinPolicy policy;
    private final Allocator allocator;
    private final Operator outer;
    private final Operator inner;
    private final JoinMatcher matcher;
    private final JoinBufferSupport buffers;
    private final BufferedJoinInput bufferedInner;
    private final JoinOutputBuffer outputBuffer;
    private final int[] outputOuterPositions;
    private final long[] outputInnerRows;
    private final int[] innerPositionsScratch;
    private final int[] retainedInnerPositionsScratch;
    private final int[] retainedInnerMaskPositionsScratch;
    private final Streams[] currentOutputs;
    private final Schema fullOutputSchema;
    private int[] outputChannels;
    private Schema outputSchema;

    private int currentInnerBatch;
    private int currentInnerPosition;

    private Mask currentOuterMask;
    private Batch currentOuterBatch;
    private OuterBatchLease currentOuterLease;
    private int outerRemaining;
    private Iterator<Integer> outerPositionIterator;
    private int currentOuterPosition;
    private boolean currentOuterPositionReady;
    private int currentOutputCount;
    private Mask currentOutputMask;
    private boolean outerConstrained;

    private boolean done;
    private boolean started;
    private boolean waitingForOuterInput;

    public NestedLoopJoinOperator(Allocator allocator, Operator outer, Operator inner)
    {
        this(allocator, outer, inner, new CrossJoinMatcher());
    }

    public NestedLoopJoinOperator(OperatorResources resources, Allocator allocator, Operator outer, Operator inner)
    {
        this(resources, allocator, outer, inner, new CrossJoinMatcher());
    }

    public NestedLoopJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(
                EngineResources.from(allocator).operatorResources(),
                allocator,
                outer,
                outerJoinColumn,
                inner,
                innerJoinColumn);
    }

    public NestedLoopJoinOperator(OperatorResources resources, Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(
                resources,
                allocator,
                outer,
                inner,
                new EquiJoinMatcher(
                        outerJoinColumn,
                        innerJoinColumn,
                        outer.outputSchema(),
                        inner.outputSchema(),
                        requireNonNull(resources, "resources is null").codeGeneration().structuralTypes()));
    }

    public NestedLoopJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns)
    {
        this(
                EngineResources.from(allocator).operatorResources(),
                allocator,
                outer,
                outerJoinColumns,
                inner,
                innerJoinColumns);
    }

    public NestedLoopJoinOperator(OperatorResources resources, Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns)
    {
        this(
                resources,
                allocator,
                outer,
                inner,
                new EquiJoinMatcher(
                        outerJoinColumns,
                        innerJoinColumns,
                        outer.outputSchema(),
                        inner.outputSchema(),
                        requireNonNull(resources, "resources is null").codeGeneration().structuralTypes()));
    }

    private NestedLoopJoinOperator(OperatorResources resources, Allocator allocator, Operator outer, Operator inner, JoinMatcher matcher)
    {
        this(
                requireNonNull(resources, "resources is null").nestedLoopJoinPolicy(),
                resources.bufferedJoinInputPolicy(),
                resources.joinBufferPolicy(),
                allocator,
                outer,
                inner,
                matcher);
    }

    private NestedLoopJoinOperator(Allocator allocator, Operator outer, Operator inner, JoinMatcher matcher)
    {
        this(
                EngineResources.from(allocator).operatorResources().nestedLoopJoinPolicy(),
                EngineResources.from(allocator).operatorResources().bufferedJoinInputPolicy(),
                EngineResources.from(allocator).operatorResources().joinBufferPolicy(),
                allocator,
                outer,
                inner,
                matcher);
    }

    private NestedLoopJoinOperator(
            NestedLoopJoinPolicy policy,
            BufferedJoinInputPolicy bufferedJoinInputPolicy,
            JoinBufferPolicy joinBufferPolicy,
            Allocator allocator,
            Operator outer,
            Operator inner,
            JoinMatcher matcher)
    {
        this.policy = requireNonNull(policy, "policy is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.outer = requireNonNull(outer, "outer is null");
        this.inner = requireNonNull(inner, "inner is null");
        this.matcher = requireNonNull(matcher, "matcher is null");
        int maxBatchRows = policy.maxBatchRows();
        this.outputOuterPositions = new int[maxBatchRows];
        this.outputInnerRows = new long[maxBatchRows];
        this.innerPositionsScratch = new int[maxBatchRows];
        this.retainedInnerPositionsScratch = new int[maxBatchRows];
        this.retainedInnerMaskPositionsScratch = new int[maxBatchRows];
        this.buffers = new JoinBufferSupport(joinBufferPolicy, allocator, allocationContext);
        this.bufferedInner = new BufferedJoinInput(bufferedJoinInputPolicy, buffers, inner.outputCount());
        this.outputBuffer = new JoinOutputBuffer(buffers, maxBatchRows, outer.outputCount(), inner.outputCount());
        int totalOutputCount = outer.outputCount() + inner.outputCount();
        this.currentOutputs = new Streams[totalOutputCount];
        this.fullOutputSchema = outputSchema(outer.outputSchema(), inner.outputSchema());
        this.outputChannels = java.util.stream.IntStream.range(0, totalOutputCount).toArray();
        this.outputSchema = fullOutputSchema;
    }

    @Override
    public int outputCount()
    {
        return outputChannels.length;
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    private static Schema outputSchema(Schema outerSchema, Schema innerSchema)
    {
        List<Field> fields = new ArrayList<>(outerSchema.size() + innerSchema.size());
        fields.addAll(outerSchema.fields());
        fields.addAll(innerSchema.fields());
        return new Schema(fields);
    }

    /**
     * Selects and orders the join's public outputs using physical concatenated column ordinals
     * ({@code outer columns} followed by {@code inner columns}).
     */
    public NestedLoopJoinOperator withOutputs(int... outputChannels)
    {
        requireNonNull(outputChannels, "outputChannels is null");
        if (started) {
            throw new IllegalStateException("nested loop join has started");
        }
        int[] selected = outputChannels.clone();
        List<Field> fields = new ArrayList<>(selected.length);
        for (int outputChannel : selected) {
            if (outputChannel < 0 || outputChannel >= currentOutputs.length) {
                throw new IllegalArgumentException("Join output column is out of bounds: " + outputChannel);
            }
            fields.add(fullOutputSchema.field(outputChannel));
        }
        this.outputChannels = selected;
        this.outputSchema = new Schema(fields);
        return this;
    }

    @Override
    public boolean hasNext()
    {
        if (!done && started && currentOuterBatch == null && outerRemaining == 0 && !outer.hasNext()) {
            if (outer instanceof ExternallyScheduledSource source && !source.isFinished()) {
                waitingForOuterInput = true;
                return false;
            }
            done = true;
        }
        waitingForOuterInput = false;
        return !done;
    }

    boolean isWaitingForOuterInput()
    {
        return waitingForOuterInput;
    }

    private Mask produceBatch()
    {
        if (!matcher.producesFullCrossProduct()) {
            return produceEquiJoinBatch();
        }

        loadInnerIfNecessary();
        if (bufferedInner.rowCount() == 0) {
            captureOuterSchemaIfAvailable();
            done = true;
            currentOutputCount = 0;
            return allocator.allocateAllMask(allocationContext, 0);
        }

        if (outerRemaining == 0) {
            if (!loadNextOuterBatch()) {
                markDoneOrWaitingForOuterInput();
                return allocator.allocateAllMask(allocationContext, 0);
            }
        }

        if (currentInnerBatch == 0 && currentInnerPosition == 0 && outerPositionIterator.hasNext()) {
            currentOuterPosition = outerPositionIterator.next();
        }

        int innerRemaining = bufferedInner.batches().get(currentInnerBatch).length() - currentInnerPosition;

        int innerProcessed;
        int outerProcessed;

        Mask mask;
        if (outerRemaining < innerRemaining) {
            int batchSize = joinWithOuterRow();

            mask = allocator.allocateAllMask(allocationContext, batchSize);
            innerProcessed = batchSize;
            outerProcessed = 1;
        }
        else {
            joinWithInnerRow();

            mask = allocator.lastMask(allocationContext, currentOuterMask, outerRemaining);
            innerProcessed = 1;
            outerProcessed = outerRemaining;

            // TODO: should we compact outer if mask != all?
            //    tradeoff: if we don't compact, inner will have to be replicated to cover all outer rows
            //              if we do, extra copy for outer and inability to just transfer ownership of outer columns
            //    maybe, we need a way to represent an RLE vector with holes?
        }

        currentInnerPosition += innerProcessed;
        if (currentInnerPosition == bufferedInner.batches().get(currentInnerBatch).length()) {
            currentInnerBatch++;
            currentInnerPosition = 0;
        }

        if (currentInnerBatch == bufferedInner.batches().size()) {
            currentInnerBatch = 0;
            outerRemaining -= outerProcessed;
        }

        return mask;
    }

    private Mask produceEquiJoinBatch()
    {
        loadInnerIfNecessary();
        if (bufferedInner.rowCount() == 0) {
            captureOuterSchemaIfAvailable();
            done = true;
            currentOutputCount = 0;
            return allocator.allocateAllMask(allocationContext, 0);
        }

        int outputPosition = 0;
        while (outputPosition < policy.maxBatchRows()) {
            if (outerRemaining == 0 && !loadNextOuterBatch()) {
                markDoneOrWaitingForOuterInput();
                break;
            }

            if (!currentOuterPositionReady) {
                if (!outerPositionIterator.hasNext()) {
                    outerRemaining = 0;
                    continue;
                }
                currentOuterPosition = outerPositionIterator.next();
                currentOuterPositionReady = true;
                currentInnerBatch = 0;
                currentInnerPosition = 0;
            }

            while (currentInnerBatch < bufferedInner.batches().size() && outputPosition < policy.maxBatchRows()) {
                BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(currentInnerBatch);
                while (currentInnerPosition < innerBatch.length() && outputPosition < policy.maxBatchRows()) {
                    if (matcher.matches(currentOuterBatch, currentOuterPosition, innerBatch, currentInnerPosition)) {
                        outputOuterPositions[outputPosition] = currentOuterPosition;
                        outputInnerRows[outputPosition] = packRowReference(currentInnerBatch, currentInnerPosition);
                        outputPosition++;
                    }
                    currentInnerPosition++;
                }
                if (currentInnerPosition == innerBatch.length()) {
                    currentInnerBatch++;
                    currentInnerPosition = 0;
                }
            }

            if (currentInnerBatch == bufferedInner.batches().size()) {
                currentInnerBatch = 0;
                currentInnerPosition = 0;
                outerRemaining--;
                currentOuterPositionReady = false;
            }
        }

        if (outputPosition == 0) {
            currentOutputCount = 0;
            return allocator.allocateAllMask(allocationContext, 0);
        }
        currentOutputCount = outputPosition;
        return allocator.allocateRangeMask(allocationContext, 0, outputPosition);
    }

    private boolean loadNextOuterBatch()
    {
        while (outer.hasNext()) {
            currentOuterBatch = outer.next();
            currentOuterLease = new OuterBatchLease(currentOuterBatch);
            currentOuterMask = currentOuterBatch.borrowMask();
            if (!currentOuterMask.none()) {
                outerPositionIterator = currentOuterMask.iterator();
                outerRemaining = currentOuterMask.count();
                currentOuterPositionReady = false;
                return true;
            }
            currentOuterLease.release();
            currentOuterBatch = null;
            currentOuterLease = null;
        }
        return false;
    }

    private void markDoneOrWaitingForOuterInput()
    {
        if (outer instanceof ExternallyScheduledSource source && !source.isFinished()) {
            waitingForOuterInput = true;
            return;
        }
        done = true;
    }

    @Override
    public Batch next()
    {
        Mask batchMask = produceBatch();
        started = true;
        currentOutputMask = batchMask;
        outerConstrained = false;
        java.util.Arrays.fill(currentOutputs, null);
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int physicalOutput = outputChannels[outputIndex];
            outputs[outputIndex] = matcher.producesFullCrossProduct()
                    ? outputBuffer.resultOutputForNestedLoop(physicalOutput, currentOuterBatch, allocator, allocationContext)
                    : resultOutput(physicalOutput);
        }
        Batch outerBatch = batchMask.none() ? null : currentOuterBatch;
        Mask outerMask = batchMask.none() ? null : currentOuterMask;
        OuterBatchLease outerLease = batchMask.none() ? null : currentOuterLease;
        if (outerLease != null) {
            outerLease.retain();
        }
        boolean outerBatchConsumed = outerRemaining == 0;
        if (outerBatchConsumed && outerLease != null) {
            outerLease.release();
        }
        return new Batch(
                batchMask,
                _ -> {},
                takenMask -> outerBatch != null && takenMask == outerMask ? outerBatch.takeMask() : allocator.transfer(allocationContext, takenMask),
                _ -> {},
                () -> {
                    if (outerLease != null) {
                        outerLease.release();
                    }
                    if (outerBatchConsumed && currentOuterBatch == outerBatch) {
                        currentOuterBatch = null;
                        currentOuterLease = null;
                    }
                },
                outputs);
    }

    private void joinWithInnerRow()
    {
        outputBuffer.joinWithInnerRow(currentOuterBatch, currentOuterMask, bufferedInner.batches().get(currentInnerBatch), currentInnerPosition);
    }

    private int joinWithOuterRow()
    {
        return outputBuffer.joinWithOuterRow(currentOuterBatch, currentOuterPosition, bufferedInner.batches().get(currentInnerBatch));
    }

    private void loadInnerIfNecessary()
    {
        bufferedInner.loadAll(inner, policy.maxBatchRows(), new int[0], matcher.supportsPerPositionEmission() && inner.supportsRetainedBatches(), matcher.supportsPerPositionEmission() && inner.supportsConstrainedReborrow());
        outputBuffer.captureInnerSchema(bufferedInner.schema());
    }

    private void captureOuterSchemaIfAvailable()
    {
        while (outer.hasNext()) {
            outputBuffer.captureOuterSchema(outer.next());
        }
    }

    @Override
    public void constrain(Mask mask)
    {
        if (matcher.supportsPerPositionEmission()) {
            currentOutputMask = mask;
        }
    }

    @Override
    public void close()
    {
        outer.close();
        inner.close();
        allocator.release(allocationContext);
    }

    private Output resultOutput(int outputIndex)
    {
        if (currentOutputCount == 0) {
            Streams schema = outputSchema(outputIndex);
            if (schema == null) {
                return new Output(Set.of(), stream -> {
                    throw new IllegalArgumentException("Output does not expose stream: " + stream);
                });
            }
            Streams empty = buffers.emptyLike(schema);
            return new Output(empty.asMap().keySet(), empty::get, (stream, vector) -> allocator.transfer(allocationContext, vector));
        }

        Set<Stream> streams = outputIndex < outer.outputCount()
                ? currentOuterBatch.output(outputIndex).streams()
                : bufferedInner.outputStreams(outputIndex - outer.outputCount());
        return new Output(
                streams,
                stream -> materializeOutput(outputIndex).get(stream),
                (stream, vector) -> allocator.transfer(allocationContext, vector));
    }

    private Streams outputSchema(int outputIndex)
    {
        if (outputIndex < outer.outputCount()) {
            Streams schema = outputBuffer.outerSchema()[outputIndex];
            if (schema != null) {
                return schema;
            }
            if (currentOuterBatch != null) {
                Streams.Builder streams = Streams.builder();
                Output output = currentOuterBatch.output(outputIndex);
                for (Stream stream : output.streams()) {
                    streams.put(stream, output.borrow(stream));
                }
                return streams.build();
            }
            return null;
        }
        Streams schema = outputBuffer.innerSchema()[outputIndex - outer.outputCount()];
        if (schema != null) {
            return schema;
        }
        return bufferedInner.outputSchema(outputIndex - outer.outputCount());
    }

    private Streams materializeOutput(int outputIndex)
    {
        Streams existing = currentOutputs[outputIndex];
        if (existing != null) {
            return existing;
        }

        Streams materialized = outputIndex < outer.outputCount()
                ? materializeOuterOutput(outputIndex)
                : materializeInnerOutput(outputIndex - outer.outputCount());
        currentOutputs[outputIndex] = materialized;
        return materialized;
    }

    private Streams materializeOuterOutput(int outputIndex)
    {
        constrainOuterIfNecessary();
        Output sourceOutput = currentOuterBatch.output(outputIndex);
        if (currentOutputMask.all()) {
            return buffers.copyPositions(sourceOutput, null, outputOuterPositions, currentOutputCount, 0, currentOutputCount);
        }

        Streams result = null;
        for (int index = 0; index < currentOutputMask.count(); index++) {
            int outputPosition = currentOutputMask.position(index);
            result = buffers.copySinglePosition(sourceOutput, result, currentOutputCount, outputPosition, outputOuterPositions[outputPosition]);
        }
        return result == null ? buffers.emptyLike(outputSchema(outputIndex)) : result;
    }

    private Streams materializeInnerOutput(int innerOutputIndex)
    {
        if (currentOutputMask.all()) {
            Streams result = null;
            int outputStart = 0;
            int next = 0;
            while (next < currentOutputCount) {
                int batchIndex = batchIndex(outputInnerRows[next]);
                int runLength = 0;
                while (next + runLength < currentOutputCount && batchIndex(outputInnerRows[next + runLength]) == batchIndex) {
                    innerPositionsScratch[runLength] = rowPosition(outputInnerRows[next + runLength]);
                    runLength++;
                }
                BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex);
                result = copyInnerPositions(result, innerBatch, innerOutputIndex, innerPositionsScratch, runLength, outputStart, currentOutputCount);
                outputStart += runLength;
                next += runLength;
            }
            return result;
        }

        Streams result = null;
        for (int index = 0; index < currentOutputMask.count(); index++) {
            int outputPosition = currentOutputMask.position(index);
            long rowReference = outputInnerRows[outputPosition];
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex(rowReference));
            result = copyInnerSinglePosition(result, innerBatch, innerOutputIndex, currentOutputCount, outputPosition, rowPosition(rowReference));
        }
        return result == null ? buffers.emptyLike(outputSchema(innerOutputIndex + outer.outputCount())) : result;
    }

    private Streams copyInnerPositions(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int innerOutputIndex, int[] positions, int positionCount, int outputStart, int size)
    {
        if (!innerBatch.retained()) {
            return buffers.copyPositions(existing, innerBatch.columns()[innerOutputIndex], positions, positionCount, outputStart, size);
        }

        constrainRetainedInnerBatch(innerBatch, positions, positionCount);
        Output output = innerBatch.retainedBatch().output(innerOutputIndex);
        for (int index = 0; index < positionCount; index++) {
            retainedInnerPositionsScratch[index] = innerBatch.sourcePosition(positions[index]);
        }
        return buffers.copyPositions(output, existing, retainedInnerPositionsScratch, positionCount, outputStart, size);
    }

    private Streams copyInnerSinglePosition(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int innerOutputIndex, int size, int outputPosition, int logicalPosition)
    {
        if (!innerBatch.retained()) {
            return buffers.copySinglePosition(existing, innerBatch.columns()[innerOutputIndex], size, outputPosition, logicalPosition);
        }

        int sourcePosition = innerBatch.sourcePosition(logicalPosition);
        constrainRetainedInnerBatch(innerBatch, new int[] {logicalPosition}, 1);
        return buffers.copySinglePosition(innerBatch.retainedBatch().output(innerOutputIndex), existing, size, outputPosition, sourcePosition);
    }

    private void constrainRetainedInnerBatch(BufferedJoinInput.InnerBatch innerBatch, int[] logicalPositions, int positionCount)
    {
        if (!innerBatch.retained()) {
            return;
        }
        for (int index = 0; index < positionCount; index++) {
            retainedInnerMaskPositionsScratch[index] = innerBatch.sourcePosition(logicalPositions[index]);
        }
        Arrays.sort(retainedInnerMaskPositionsScratch, 0, positionCount);
        int uniqueCount = 0;
        int previous = -1;
        for (int index = 0; index < positionCount; index++) {
            int position = retainedInnerMaskPositionsScratch[index];
            if (position != previous) {
                retainedInnerMaskPositionsScratch[uniqueCount++] = position;
                previous = position;
            }
        }
        innerBatch.retainedBatch().constrain(allocator.allocateSparseMask(
                allocationContext,
                Arrays.copyOf(retainedInnerMaskPositionsScratch, uniqueCount),
                innerBatch.retainedBatch().borrowMask().size()));
    }

    private void constrainOuterIfNecessary()
    {
        if (outerConstrained || currentOuterBatch == null) {
            return;
        }
        outerConstrained = true;
        outer.constrain(matchedOuterMask());
    }

    private Mask matchedOuterMask()
    {
        if (currentOutputMask.none()) {
            return allocator.allocateEmptyMask(allocationContext, currentOuterMask.size());
        }

        int[] positions = new int[Math.min(currentOutputMask.count(), currentOuterMask.count())];
        int selectedCount = 0;
        int previous = -1;
        for (int index = 0; index < currentOutputMask.count(); index++) {
            int outputPosition = currentOutputMask.position(index);
            int outerPosition = outputOuterPositions[outputPosition];
            if (outerPosition != previous) {
                positions[selectedCount++] = outerPosition;
                previous = outerPosition;
            }
        }
        return allocator.allocateSparseMask(allocationContext, java.util.Arrays.copyOf(positions, selectedCount), currentOuterMask.size());
    }

    private static long packRowReference(int batchIndex, int position)
    {
        return ((long) batchIndex << Integer.SIZE) | (position & 0xFFFF_FFFFL);
    }

    private static int batchIndex(long rowReference)
    {
        return (int) (rowReference >>> Integer.SIZE);
    }

    private static int rowPosition(long rowReference)
    {
        return (int) rowReference;
    }

    private static final class OuterBatchLease
    {
        private final Batch batch;
        private int references = 1;

        private OuterBatchLease(Batch batch)
        {
            this.batch = requireNonNull(batch, "batch is null");
        }

        private void retain()
        {
            if (references <= 0) {
                throw new IllegalStateException("Outer batch already released");
            }
            references++;
        }

        private void release()
        {
            if (references <= 0) {
                throw new IllegalStateException("Outer batch already released");
            }
            references--;
            if (references == 0) {
                batch.close();
            }
        }
    }
}
