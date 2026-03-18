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

import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.List;

final class BufferedJoinInput
{
    private final JoinBufferSupport buffers;
    private final int columnCount;
    private final Streams[] schema;
    private final List<InnerBatch> batches = new ArrayList<>();

    private boolean loaded;
    private long rowCount;

    BufferedJoinInput(JoinBufferSupport buffers, int columnCount)
    {
        this.buffers = buffers;
        this.columnCount = columnCount;
        this.schema = new Streams[columnCount];
    }

    public void loadAll(Operator source, int batchSize)
    {
        if (loaded) {
            return;
        }
        loaded = true;

        Streams[] columns = new Streams[columnCount];
        int outputPosition = 0;

        while (source.hasNext()) {
            Batch batch = source.next();
            captureSchema(batch, schema);
            Mask mask = batch.borrowMask();
            int maskOffset = 0;
            while (maskOffset < mask.count()) {
                int copied = Math.min(mask.count() - maskOffset, batchSize - outputPosition);
                for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
                    columns[columnIndex] = buffers.copyAndCompact(batch.output(columnIndex), mask, maskOffset, columns[columnIndex], outputPosition, copied, batchSize);
                }
                outputPosition += copied;
                maskOffset += copied;
                rowCount += copied;

                if (outputPosition == batchSize) {
                    batches.add(new InnerBatch(columns, batchSize));
                    columns = new Streams[columnCount];
                    outputPosition = 0;
                }
            }
        }

        if (outputPosition > 0) {
            batches.add(new InnerBatch(columns, outputPosition));
        }
    }

    public List<InnerBatch> batches()
    {
        return batches;
    }

    public Streams[] schema()
    {
        return schema;
    }

    public long rowCount()
    {
        return rowCount;
    }

    public static void captureSchema(Batch batch, Streams[] schema)
    {
        for (int outputIndex = 0; outputIndex < schema.length; outputIndex++) {
            if (schema[outputIndex] != null) {
                continue;
            }
            Output output = batch.output(outputIndex);
            Streams.Builder streams = Streams.builder();
            for (Stream stream : output.streams()) {
                streams.put(stream, output.borrow(stream));
            }
            schema[outputIndex] = streams.build();
        }
    }

    record InnerBatch(Streams[] columns, int length) {}
}
