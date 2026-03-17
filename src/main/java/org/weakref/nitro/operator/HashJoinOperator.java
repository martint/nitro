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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HashJoinOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("HashJoinOperator");
    private static final int BATCH_SIZE = 1024;

    private final Allocator allocator;
    private final Operator outer;
    private final Operator inner;
    private final int[] outerJoinColumns;
    private final int[] innerJoinColumns;
    private final JoinBufferSupport buffers;

    private boolean innerLoaded;
    private final List<InnerBatch> innerBatches = new ArrayList<>();
    private final Map<OperatorKeySemantics.Key, List<InnerRowReference>> innerIndex = new HashMap<>();

    private Mask currentOuterMask;
    private Batch currentOuterBatch;
    private Iterator<Integer> outerPositionIterator;
    private int outerRemaining;
    private int currentOuterPosition;
    private boolean currentOuterPositionReady;
    private List<InnerRowReference> currentMatches = List.of();
    private int currentMatchIndex;

    private final Streams[] result;
    private final Streams[] outerBuffer;
    private final Streams[] innerBuffer;
    private final Streams[] outerSchema;
    private final Streams[] innerSchema;

    private boolean done;

    public HashJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn});
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns)
    {
        if (outerJoinColumns.length != innerJoinColumns.length) {
            throw new IllegalArgumentException("Join key counts must match");
        }
        if (outerJoinColumns.length == 0) {
            throw new IllegalArgumentException("Hash join requires at least one join key");
        }

        this.allocator = allocator;
        this.outer = outer;
        this.inner = inner;
        this.outerJoinColumns = outerJoinColumns.clone();
        this.innerJoinColumns = innerJoinColumns.clone();
        this.buffers = new JoinBufferSupport(allocator, ALLOCATION_CONTEXT);
        this.result = new Streams[outer.outputCount() + inner.outputCount()];
        this.outerBuffer = new Streams[outer.outputCount()];
        this.innerBuffer = new Streams[inner.outputCount()];
        this.outerSchema = new Streams[outer.outputCount()];
        this.innerSchema = new Streams[inner.outputCount()];
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
                takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                outputs);
    }

    private Mask produceBatch()
    {
        loadInnerIfNecessary();
        if (innerIndex.isEmpty()) {
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
                currentMatches = matchesForOuterPosition();
                currentMatchIndex = 0;
            }

            while (currentMatchIndex < currentMatches.size() && outputPosition < BATCH_SIZE) {
                InnerRowReference match = currentMatches.get(currentMatchIndex++);
                appendJoinMatch(outputPosition, innerBatches.get(match.batchIndex()), match.position());
                outputPosition++;
            }

            if (currentMatchIndex == currentMatches.size()) {
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatches = List.of();
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

    private List<InnerRowReference> matchesForOuterPosition()
    {
        OperatorKeySemantics.Key key = keyForOuterPosition();
        if (key == null) {
            return List.of();
        }
        return innerIndex.getOrDefault(key, List.of());
    }

    private OperatorKeySemantics.Key keyForOuterPosition()
    {
        OperatorKeySemantics.Key[] keys = new OperatorKeySemantics.Key[outerJoinColumns.length];
        for (int keyIndex = 0; keyIndex < outerJoinColumns.length; keyIndex++) {
            Output output = currentOuterBatch.output(outerJoinColumns[keyIndex]);
            OperatorKeySemantics.Key key = OperatorKeySemantics.key(
                    output.borrow(Stream.VALUES),
                    (BooleanVector) output.borrowOrNull(Stream.NULLS),
                    currentOuterPosition);
            if (key == null) {
                return null;
            }
            keys[keyIndex] = key;
        }
        return OperatorKeySemantics.compositeKey(keys);
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
            return new Output(
                    streams.asMap().keySet(),
                    streams::get,
                    (stream, vector) -> {
                        result[outputIndex] = null;
                        outerBuffer[outputIndex] = null;
                        return allocator.transfer(ALLOCATION_CONTEXT, vector);
                    });
        }

        int innerIndex = outputIndex - outerColumnCount;
        return new Output(
                streams.asMap().keySet(),
                streams::get,
                (stream, vector) -> {
                    result[outputIndex] = null;
                    innerBuffer[innerIndex] = null;
                    return allocator.transfer(ALLOCATION_CONTEXT, vector);
                });
    }

    private void loadInnerIfNecessary()
    {
        if (innerLoaded) {
            return;
        }
        innerLoaded = true;

        Streams[] columns = new Streams[inner.outputCount()];
        int outputPosition = 0;

        while (inner.hasNext()) {
            Batch batch = inner.next();
            captureSchema(batch, innerSchema);
            Mask mask = batch.borrowMask();
            int maskOffset = 0;
            while (maskOffset < mask.count()) {
                int copied = Math.min(mask.count() - maskOffset, BATCH_SIZE - outputPosition);
                for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
                    columns[columnIndex] = buffers.copyAndCompact(batch.output(columnIndex), mask, maskOffset, columns[columnIndex], outputPosition, copied, BATCH_SIZE);
                }
                indexInnerRows(columns, outputPosition, copied, innerBatches.size());
                outputPosition += copied;
                maskOffset += copied;

                if (outputPosition == BATCH_SIZE) {
                    innerBatches.add(new InnerBatch(columns, BATCH_SIZE));
                    columns = new Streams[inner.outputCount()];
                    outputPosition = 0;
                }
            }
        }

        if (outputPosition > 0) {
            innerBatches.add(new InnerBatch(columns, outputPosition));
        }
    }

    private void indexInnerRows(Streams[] columns, int startPosition, int length, int batchIndex)
    {
        for (int position = startPosition; position < startPosition + length; position++) {
            OperatorKeySemantics.Key[] keys = new OperatorKeySemantics.Key[innerJoinColumns.length];
            boolean hasNull = false;
            for (int keyIndex = 0; keyIndex < innerJoinColumns.length; keyIndex++) {
                Streams streams = columns[innerJoinColumns[keyIndex]];
                OperatorKeySemantics.Key key = OperatorKeySemantics.key(
                        streams.values(),
                        (BooleanVector) streams.getOrNull(Stream.NULLS),
                        position);
                if (key == null) {
                    hasNull = true;
                    break;
                }
                keys[keyIndex] = key;
            }
            if (hasNull) {
                continue;
            }
            OperatorKeySemantics.Key compositeKey = OperatorKeySemantics.compositeKey(keys);
            innerIndex.computeIfAbsent(compositeKey, ignored -> new ArrayList<>())
                    .add(new InnerRowReference(batchIndex, position));
        }
    }

    private void clearResults()
    {
        java.util.Arrays.fill(result, null);
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

    private record InnerBatch(Streams[] columns, int length) {}

    private record InnerRowReference(int batchIndex, int position) {}
}
