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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NestedLoopJoinOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("NestedLoopJoinOperator");
    private static final int BATCH_SIZE = 1024;

    private final Allocator allocator;
    private final Operator outer;
    private final Operator inner;
    private final JoinMatcher matcher;
    private final JoinBufferSupport buffers;

    private boolean innerLoaded;
    private final List<InnerBatch> innerBatches = new ArrayList<>();
    private long innerRowCount;

    private int currentInnerBatch;
    private int currentInnerPosition;

    private Mask currentOuterMask;
    private Batch currentOuterBatch;
    private int outerRemaining;
    private Iterator<Integer> outerPositionIterator;
    private int currentOuterPosition;
    private boolean currentOuterPositionReady;

    private final Streams[] result;
    private final Streams[] outerBuffer; // buffer to hold output from outer columns when replicating the same outer row for multiple inner rows
    private final Streams[] innerBuffer; // buffer to hold output from inner columns when replicating the same inner row for multiple outer rows
    private final Streams[] outerSchema;
    private final Streams[] innerSchema;

    private boolean done;

    public NestedLoopJoinOperator(Allocator allocator, Operator outer, Operator inner)
    {
        this(allocator, outer, inner, new CrossJoinMatcher());
    }

    public NestedLoopJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(allocator, outer, inner, new EquiJoinMatcher(outerJoinColumn, innerJoinColumn));
    }

    private NestedLoopJoinOperator(Allocator allocator, Operator outer, Operator inner, JoinMatcher matcher)
    {
        this.allocator = allocator;
        this.outer = outer;
        this.inner = inner;
        this.matcher = matcher;
        this.buffers = new JoinBufferSupport(allocator, ALLOCATION_CONTEXT);
        result = new Streams[outer.outputCount() + inner.outputCount()];
        outerBuffer = new Streams[outer.outputCount()];
        innerBuffer = new Streams[inner.outputCount()];
        outerSchema = new Streams[outer.outputCount()];
        innerSchema = new Streams[inner.outputCount()];
    }

    @Override
    public int outputCount()
    {
        return outer.outputCount() + inner.outputCount();
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    private Mask produceBatch()
    {
        if (!matcher.isCrossJoin()) {
            return produceEquiJoinBatch();
        }

        loadInnerIfNecessary();
        if (innerRowCount == 0) {
            done = true;
            return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
        }

        if (outerRemaining == 0) {
            if (!loadNextOuterBatch()) {
                done = true;
                return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
            }
        }

        if (currentInnerBatch == 0 && currentInnerPosition == 0 && outerPositionIterator.hasNext()) {
            currentOuterPosition = outerPositionIterator.next();
        }

        int innerRemaining = innerBatches.get(currentInnerBatch).length() - currentInnerPosition;

        int innerProcessed;
        int outerProcessed;

        Mask mask;
        if (outerRemaining < innerRemaining) {
            int batchSize = joinWithOuterRow();

            mask = allocator.allocateAllMask(ALLOCATION_CONTEXT, batchSize);
            innerProcessed = batchSize;
            outerProcessed = 1;
        }
        else {
            joinWithInnerRow();

            mask = allocator.lastMask(ALLOCATION_CONTEXT, currentOuterMask, outerRemaining);
            innerProcessed = 1;
            outerProcessed = outerRemaining;

            // TODO: should we compact outer if mask != all?
            //    tradeoff: if we don't compact, inner will have to be replicated to cover all outer rows
            //              if we do, extra copy for outer and inability to just transfer ownership of outer columns
            //    maybe, we need a way to represent an RLE vector with holes?
        }

        currentInnerPosition += innerProcessed;
        if (currentInnerPosition == innerBatches.get(currentInnerBatch).length()) {
            currentInnerBatch++;
            currentInnerPosition = 0;
        }

        if (currentInnerBatch == innerBatches.size()) {
            currentInnerBatch = 0;
            outerRemaining -= outerProcessed;
        }

        if (outerRemaining == 0 && !outer.hasNext()) {
            done = true;
        }

        return mask;
    }

    private Mask produceEquiJoinBatch()
    {
        loadInnerIfNecessary();
        if (innerRowCount == 0) {
            done = true;
            clearResults();
            return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
        }

        int outputPosition = 0;
        while (outputPosition < BATCH_SIZE) {
            if (outerRemaining == 0 && !loadNextOuterBatch()) {
                done = true;
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

            while (currentInnerBatch < innerBatches.size() && outputPosition < BATCH_SIZE) {
                InnerBatch innerBatch = innerBatches.get(currentInnerBatch);
                while (currentInnerPosition < innerBatch.length() && outputPosition < BATCH_SIZE) {
                    if (matcher.matches(currentOuterBatch, currentOuterPosition, innerBatch.columns(), currentInnerPosition)) {
                        appendJoinMatch(outputPosition, innerBatch, currentInnerPosition);
                        outputPosition++;
                    }
                    currentInnerPosition++;
                }
                if (currentInnerPosition == innerBatch.length()) {
                    currentInnerBatch++;
                    currentInnerPosition = 0;
                }
            }

            if (currentInnerBatch == innerBatches.size()) {
                currentInnerBatch = 0;
                currentInnerPosition = 0;
                outerRemaining--;
                currentOuterPositionReady = false;
            }
        }

        if (outputPosition == 0) {
            clearResults();
            return allocator.allocateAllMask(ALLOCATION_CONTEXT, 0);
        }
        return allocator.allocateRangeMask(ALLOCATION_CONTEXT, 0, outputPosition);
    }

    private boolean loadNextOuterBatch()
    {
        while (outer.hasNext()) {
            currentOuterBatch = outer.next();
            captureSchema(currentOuterBatch, outerSchema);
            currentOuterMask = currentOuterBatch.borrowMask();
            if (!currentOuterMask.none()) {
                outerPositionIterator = currentOuterMask.iterator();
                outerRemaining = currentOuterMask.count();
                currentOuterPositionReady = false;
                return true;
            }
        }
        return false;
    }

    private void appendJoinMatch(int outputPosition, InnerBatch innerBatch, int innerPosition)
    {
        int outerColumnCount = outer.outputCount();
        for (int i = 0; i < outerColumnCount; i++) {
            outerBuffer[i] = buffers.replicate(
                    outerBuffer[i],
                    buffers.borrowStreams(currentOuterBatch.output(i)),
                    BATCH_SIZE,
                    outputPosition,
                    1,
                    currentOuterPosition);
            result[i] = outerBuffer[i];
        }
        for (int i = 0; i < inner.outputCount(); i++) {
            innerBuffer[i] = buffers.replicate(
                    innerBuffer[i],
                    innerBatch.columns()[i],
                    BATCH_SIZE,
                    outputPosition,
                    1,
                    innerPosition);
            result[outerColumnCount + i] = innerBuffer[i];
        }
    }

    @Override
    public Batch next()
    {
        Mask batchMask = produceBatch();
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            outputs[outputIndex] = resultOutput(outputIndex);
        }
        return new Batch(
                batchMask,
                takenMask -> takenMask == currentOuterMask ? currentOuterBatch.takeMask() : allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                outputs);
    }

    private Output resultOutput(int outputIndex)
    {
        Streams streams = result[outputIndex];
        if (streams == null) {
            Streams schema = outputIndex < outer.outputCount() ? outerSchema[outputIndex] : innerSchema[outputIndex - outer.outputCount()];
            if (schema != null) {
                Streams empty = buffers.emptyLike(schema);
                return new Output(empty.asMap().keySet(), empty::get, (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
            }
            I64Vector empty = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, 0, I64Vector::new);
            return new Output(java.util.Set.of(Stream.VALUES), stream -> empty, (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
        }

        int outerColumnCount = outer.outputCount();
        if (outputIndex < outerColumnCount) {
            if (streams == outerBuffer[outputIndex]) {
                return new Output(
                        streams.asMap().keySet(),
                        streams::get,
                        (stream, vector) -> {
                            result[outputIndex] = null;
                            outerBuffer[outputIndex] = null;
                            return allocator.transfer(ALLOCATION_CONTEXT, vector);
                        });
            }

            Output sourceOutput = currentOuterBatch.output(outputIndex);
            return new Output(sourceOutput.streams(), sourceOutput::borrow, (stream, vector) -> sourceOutput.take(stream));
        }

        int innerIndex = outputIndex - outerColumnCount;
        if (streams == innerBuffer[innerIndex]) {
            return new Output(
                    streams.asMap().keySet(),
                    streams::get,
                    (stream, vector) -> {
                        result[outputIndex] = null;
                        innerBuffer[innerIndex] = null;
                        return allocator.transfer(ALLOCATION_CONTEXT, vector);
                    });
        }

        return new Output(
                streams.asMap().keySet(),
                streams::get,
                (stream, vector) -> buffers.copyStreamVector(streams, stream));
    }

    private void joinWithInnerRow()
    {
        int outerColumnCount = outer.outputCount();
        for (int i = 0; i < outerColumnCount; i++) {
            result[i] = buffers.borrowStreams(currentOuterBatch.output(i));
        }
        for (int i = 0; i < inner.outputCount(); i++) {
            innerBuffer[i] = buffers.replicate(
                    innerBuffer[i],
                    innerBatches.get(currentInnerBatch).columns()[i],
                    currentOuterMask.maxPosition() + 1,
                    0,
                    currentOuterMask.maxPosition() + 1,
                    currentInnerPosition);

            result[i + outerColumnCount] = innerBuffer[i];
        }
    }

    private int joinWithOuterRow()
    {
        int batchSize = innerBatches.get(currentInnerBatch).length();

        int outerColumnCount = outer.outputCount();
        for (int i = 0; i < outerColumnCount; i++) {
            outerBuffer[i] = buffers.replicate(
                    outerBuffer[i],
                    buffers.borrowStreams(currentOuterBatch.output(i)),
                    batchSize,
                    0,
                    batchSize,
                    currentOuterPosition);

            result[i] = outerBuffer[i];
        }
        System.arraycopy(innerBatches.get(currentInnerBatch).columns(), 0, result, outerColumnCount, inner.outputCount());
        return batchSize;
    }

    private void loadInnerIfNecessary()
    {
        if (!innerLoaded) {
            innerLoaded = true;

            Streams[] columns = allocateNewBatch(inner.outputCount());
            int outputPosition = 0;
            innerRowCount = 0;

            while (inner.hasNext()) {
                Batch batch = inner.next();
                captureSchema(batch, innerSchema);
                Mask mask = batch.borrowMask();
                int maskOffset = 0;
                while (maskOffset < mask.count()) {
                    int copied = Math.min(mask.count() - maskOffset, BATCH_SIZE - outputPosition);
                    for (int i = 0; i < columns.length; i++) {
                        // TODO: allow transferring ownership from underlying operator in case we don't need to copy+compact
                        columns[i] = buffers.copyAndCompact(batch.output(i), mask, maskOffset, columns[i], outputPosition, copied, BATCH_SIZE);
                    }
                    outputPosition += copied;
                    maskOffset += copied;
                    innerRowCount += copied;

                    if (outputPosition == BATCH_SIZE) {
                        outputPosition = 0;
                        innerBatches.add(new InnerBatch(columns, BATCH_SIZE));
                        columns = allocateNewBatch(inner.outputCount());
                    }
                }
            }

            if (outputPosition > 0) {
                innerBatches.add(new InnerBatch(columns, outputPosition));
            }
        }
    }

    private void clearResults()
    {
        java.util.Arrays.fill(result, null);
    }

    private Streams[] allocateNewBatch(int columnCount)
    {
        return new Streams[columnCount];
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public void close()
    {
        outer.close();
        inner.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private static void captureSchema(Batch batch, Streams[] schema)
    {
        for (int outputIndex = 0; outputIndex < schema.length; outputIndex++) {
            if (schema[outputIndex] != null) {
                continue;
            }
            Output output = batch.output(outputIndex);
            Streams streams = Streams.empty();
            for (Stream stream : output.streams()) {
                streams = streams.with(stream, output.borrow(stream));
            }
            schema[outputIndex] = streams;
        }
    }

    record InnerBatch(Streams[] columns, int length) {}
}
