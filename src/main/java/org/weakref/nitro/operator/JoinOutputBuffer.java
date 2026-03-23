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
import org.weakref.nitro.data.Mask;

import java.util.Set;

final class JoinOutputBuffer
{
    private final JoinBufferSupport buffers;
    private final int batchSize;
    private final int outerColumnCount;
    private final int innerColumnCount;

    private final Streams[] result;
    private final Streams[] outerBuffer;
    private final Streams[] innerBuffer;
    private final Streams[] outerSchema;
    private final Streams[] innerSchema;
    private final int[] innerPositionsScratch;

    JoinOutputBuffer(JoinBufferSupport buffers, int batchSize, int outerColumnCount, int innerColumnCount)
    {
        this.buffers = buffers;
        this.batchSize = batchSize;
        this.outerColumnCount = outerColumnCount;
        this.innerColumnCount = innerColumnCount;
        this.result = new Streams[outerColumnCount + innerColumnCount];
        this.outerBuffer = new Streams[outerColumnCount];
        this.innerBuffer = new Streams[innerColumnCount];
        this.outerSchema = new Streams[outerColumnCount];
        this.innerSchema = new Streams[innerColumnCount];
        this.innerPositionsScratch = new int[batchSize];
    }

    public int outputCount()
    {
        return result.length;
    }

    public Streams[] outerSchema()
    {
        return outerSchema;
    }

    public Streams[] innerSchema()
    {
        return innerSchema;
    }

    public void captureOuterSchema(Batch batch)
    {
        BufferedJoinInput.captureSchema(batch, outerSchema);
    }

    public void captureInnerSchema(Batch batch)
    {
        BufferedJoinInput.captureSchema(batch, innerSchema);
    }

    public void captureInnerSchema(Streams[] schema)
    {
        copySchema(schema, innerSchema);
    }

    public void appendMatchAt(Batch outerBatch, int outerPosition, BufferedJoinInput.InnerBatch innerBatch, int innerPosition, int outputPosition, int batchSize)
    {
        for (int i = 0; i < outerColumnCount; i++) {
            outerBuffer[i] = buffers.copySinglePosition(
                    outerBatch.output(i),
                    outerBuffer[i],
                    batchSize,
                    outputPosition,
                    outerPosition);
            result[i] = outerBuffer[i];
        }
        for (int i = 0; i < innerColumnCount; i++) {
            innerBuffer[i] = buffers.copySinglePosition(
                    innerBuffer[i],
                    innerBatch.columns()[i],
                    batchSize,
                    outputPosition,
                    innerPosition);
            result[outerColumnCount + i] = innerBuffer[i];
        }
    }

    public void materializeHashJoinMatches(Batch outerBatch, BufferedJoinInput bufferedInner, int[] outerPositions, long[] innerRows, int matchCount)
    {
        for (int i = 0; i < outerColumnCount; i++) {
            outerBuffer[i] = buffers.copyPositions(
                    outerBatch.output(i),
                    outerBuffer[i],
                    outerPositions,
                    matchCount,
                    0,
                    matchCount);
            result[i] = outerBuffer[i];
        }

        for (int i = 0; i < innerColumnCount; i++) {
            Streams existing = innerBuffer[i];
            int outputStart = 0;
            int next = 0;
            while (next < matchCount) {
                int batchIndex = batchIndex(innerRows[next]);
                int runLength = 0;
                while (next + runLength < matchCount && batchIndex(innerRows[next + runLength]) == batchIndex) {
                    innerPositionsScratch[runLength] = rowPosition(innerRows[next + runLength]);
                    runLength++;
                }

                existing = buffers.copyPositions(
                        existing,
                        bufferedInner.batches().get(batchIndex).columns()[i],
                        innerPositionsScratch,
                        runLength,
                        outputStart,
                        matchCount);
                outputStart += runLength;
                next += runLength;
            }
            innerBuffer[i] = existing;
            result[outerColumnCount + i] = existing;
        }
    }

    public int joinWithOuterRow(Batch outerBatch, int outerPosition, BufferedJoinInput.InnerBatch innerBatch)
    {
        int batchSize = innerBatch.length();
        for (int i = 0; i < outerColumnCount; i++) {
            outerBuffer[i] = buffers.replicate(
                    outerBuffer[i],
                    buffers.borrowStreams(outerBatch.output(i)),
                    batchSize,
                    0,
                    batchSize,
                    outerPosition);
            result[i] = outerBuffer[i];
        }
        System.arraycopy(innerBatch.columns(), 0, result, outerColumnCount, innerColumnCount);
        return batchSize;
    }

    public void joinWithInnerRow(Batch outerBatch, Mask outerMask, BufferedJoinInput.InnerBatch innerBatch, int innerPosition)
    {
        for (int i = 0; i < outerColumnCount; i++) {
            result[i] = buffers.borrowStreams(outerBatch.output(i));
        }
        for (int i = 0; i < innerColumnCount; i++) {
            innerBuffer[i] = buffers.replicate(
                    innerBuffer[i],
                    innerBatch.columns()[i],
                    outerMask.maxPosition() + 1,
                    0,
                    outerMask.maxPosition() + 1,
                    innerPosition);
            result[outerColumnCount + i] = innerBuffer[i];
        }
    }

    public Output resultOutputForHashJoin(int outputIndex, Allocator allocator, Allocator.Context allocationContext)
    {
        Streams streams = result[outputIndex];
        if (streams == null) {
            return emptyOutput(outputIndex, allocator, allocationContext);
        }

        int innerIndex = outputIndex - outerColumnCount;
        return new Output(
                streams.asMap().keySet(),
                streams::get,
                (stream, vector) -> {
                    result[outputIndex] = null;
                    if (outputIndex < outerColumnCount) {
                        outerBuffer[outputIndex] = null;
                    }
                    else {
                        innerBuffer[innerIndex] = null;
                    }
                    return allocator.transfer(allocationContext, vector);
                });
    }

    public Output resultOutputForNestedLoop(int outputIndex, Batch currentOuterBatch, Allocator allocator, Allocator.Context allocationContext)
    {
        Streams streams = result[outputIndex];
        if (streams == null) {
            return emptyOutput(outputIndex, allocator, allocationContext);
        }

        if (outputIndex < outerColumnCount) {
            if (streams == outerBuffer[outputIndex]) {
                return new Output(
                        streams.asMap().keySet(),
                        streams::get,
                        (stream, vector) -> {
                            result[outputIndex] = null;
                            outerBuffer[outputIndex] = null;
                            return allocator.transfer(allocationContext, vector);
                        });
            }

            Output sourceOutput = currentOuterBatch.output(outputIndex);
            return new Output(
                    sourceOutput.streams(),
                    sourceOutput::borrow,
                    (stream, vector) -> sourceOutput.take(stream),
                    (_, _) -> {},
                    sourceOutput::copySinglePosition);
        }

        int innerIndex = outputIndex - outerColumnCount;
        if (streams == innerBuffer[innerIndex]) {
            return new Output(
                    streams.asMap().keySet(),
                    streams::get,
                    (stream, vector) -> {
                        result[outputIndex] = null;
                        innerBuffer[innerIndex] = null;
                        return allocator.transfer(allocationContext, vector);
                    });
        }

        return new Output(
                streams.asMap().keySet(),
                streams::get,
                (stream, vector) -> buffers.copyStreamVector(streams, stream));
    }

    public void clearResults()
    {
        java.util.Arrays.fill(result, null);
    }

    private Output emptyOutput(int outputIndex, Allocator allocator, Allocator.Context allocationContext)
    {
        Streams schema = outputIndex < outerColumnCount ? outerSchema[outputIndex] : innerSchema[outputIndex - outerColumnCount];
        if (schema != null) {
            Streams empty = buffers.emptyLike(schema);
            return new Output(empty.asMap().keySet(), empty::get, (stream, vector) -> allocator.transfer(allocationContext, vector));
        }
        return new Output(Set.of(), stream -> {
            throw new IllegalArgumentException("Output does not expose stream: " + stream);
        });
    }

    private static int batchIndex(long rowReference)
    {
        return (int) (rowReference >>> Integer.SIZE);
    }

    private static int rowPosition(long rowReference)
    {
        return (int) rowReference;
    }

    private static void copySchema(Streams[] source, Streams[] target)
    {
        for (int index = 0; index < target.length; index++) {
            if (target[index] == null && source[index] != null) {
                target[index] = source[index];
            }
        }
    }
}
