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

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.List;

final class BufferedJoinInput
{
    private static final int MAX_COALESCED_ROWS = Integer.getInteger("nitro.hash.join.maxCoalescedInnerRows", 500_000);
    private static final int VALUES_FLAG = 1;
    private static final int NULLS_FLAG = 1 << 1;
    private static final int ERRORS_FLAG = 1 << 2;

    private final JoinBufferSupport buffers;
    private final int columnCount;
    private final Streams[] schema;
    private final java.util.Set<Stream>[] outputStreams;
    private final int[] outputKnownAllFalseFlags;
    private final boolean[] outputKnownAllFalseInitialized;
    private final List<InnerBatch> batches = new ArrayList<>();
    private Batch firstRetainedBatch;

    private boolean loaded;
    private long rowCount;

    @SuppressWarnings("unchecked")
    BufferedJoinInput(JoinBufferSupport buffers, int columnCount)
    {
        this.buffers = buffers;
        this.columnCount = columnCount;
        this.schema = new Streams[columnCount];
        this.outputStreams = (java.util.Set<Stream>[]) new java.util.Set<?>[columnCount];
        this.outputKnownAllFalseFlags = new int[columnCount];
        this.outputKnownAllFalseInitialized = new boolean[columnCount];
    }

    public void loadAll(Operator source, int batchSize)
    {
        loadAll(source, batchSize, new int[0], false);
    }

    public void loadAll(Operator source, int batchSize, int[] eagerColumns, boolean retainBatches)
    {
        if (loaded) {
            return;
        }
        loaded = true;

        if (retainBatches) {
            loadRetained(source);
            coalesceSmallBatches();
            return;
        }

        Streams[] columns = new Streams[columnCount];
        int outputPosition = 0;

        while (source.hasNext()) {
            Batch batch = source.next();
            captureSchema(batch, schema);
            captureStreams(batch, outputStreams);
            captureKnownAllFalse(batch, outputKnownAllFalseFlags, outputKnownAllFalseInitialized);
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

        coalesceSmallBatches();
    }

    private void loadRetained(Operator source)
    {
        while (source.hasNext()) {
            Batch batch = source.next();
            if (firstRetainedBatch == null) {
                firstRetainedBatch = batch;
            }
            captureSchema(batch, schema);
            captureStreams(batch, outputStreams);
            captureKnownAllFalse(batch, outputKnownAllFalseFlags, outputKnownAllFalseInitialized);
            Mask mask = batch.borrowMask();
            if (mask.none()) {
                continue;
            }
            if (!mask.all()) {
                Streams[] columns = new Streams[columnCount];
                for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
                    columns[columnIndex] = buffers.copyAndCompact(batch.output(columnIndex), mask, 0, null, 0, mask.count(), mask.count());
                }
                rowCount += mask.count();
                batches.add(new InnerBatch(columns, mask.count()));
                continue;
            }
            int[] positions = positions(mask);
            rowCount += positions.length;
            batches.add(InnerBatch.retained(batch, positions));
        }
    }

    private void coalesceSmallBatches()
    {
        if (batches.size() <= 1 || rowCount == 0 || rowCount > MAX_COALESCED_ROWS) {
            return;
        }

        int size = Math.toIntExact(rowCount);
        Streams[] columns = new Streams[columnCount];
        int outputStart = 0;
        for (InnerBatch batch : batches) {
            int[] sourcePositions = densePositions(batch.length());
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                columns[columnIndex] = batch.retained()
                        ? buffers.copyPositions(batch.retainedBatch().output(columnIndex), columns[columnIndex], batch.positions(), batch.length(), outputStart, size)
                        : buffers.copyPositions(columns[columnIndex], batch.columns()[columnIndex], sourcePositions, batch.length(), outputStart, size);
            }
            outputStart += batch.length();
        }
        batches.clear();
        batches.add(new InnerBatch(columns, size));
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

    public java.util.Set<Stream> outputStreams(int outputIndex)
    {
        return outputStreams[outputIndex];
    }

    public boolean outputKnownAllFalse(int outputIndex, Stream stream)
    {
        return (outputKnownAllFalseFlags[outputIndex] & streamFlag(stream)) != 0;
    }

    public Streams outputSchema(int outputIndex)
    {
        Streams outputSchema = schema[outputIndex];
        if (outputSchema != null) {
            return outputSchema;
        }
        for (InnerBatch batch : batches) {
            if (!batch.retained() && batch.columns()[outputIndex] != null) {
                schema[outputIndex] = batch.columns()[outputIndex];
                return schema[outputIndex];
            }
        }
        if (firstRetainedBatch == null) {
            return null;
        }
        Output output = firstRetainedBatch.output(outputIndex);
        Streams.Builder streams = Streams.builder();
        if (output.hasValues()) {
            streams.put(Stream.VALUES, output.borrow(Stream.VALUES));
        }
        if (output.hasNulls()) {
            streams.put(Stream.NULLS, new BooleanVector(0));
        }
        if (output.hasErrors()) {
            streams.put(Stream.ERRORS, new BooleanVector(0));
        }
        schema[outputIndex] = streams.build();
        return schema[outputIndex];
    }

    public static void captureSchema(Batch batch, Streams[] schema)
    {
        for (int outputIndex = 0; outputIndex < schema.length; outputIndex++) {
            if (schema[outputIndex] != null) {
                continue;
            }
            Output output = batch.output(outputIndex);
            Streams.Builder streams = Streams.builder();
            try {
                if (output.hasValues()) {
                    streams.put(Stream.VALUES, output.borrow(Stream.VALUES));
                }
                if (output.hasNulls()) {
                    streams.put(Stream.NULLS, new BooleanVector(0));
                }
                if (output.hasErrors()) {
                    streams.put(Stream.ERRORS, new BooleanVector(0));
                }
                schema[outputIndex] = streams.build();
            }
            catch (IllegalArgumentException ignored) {
                // Some projected outputs cannot yield a representative VALUES stream at
                // schema-capture time. Later buffered rows or retained batches can still
                // establish the schema when the join needs it.
            }
        }
    }

    private static void captureStreams(Batch batch, java.util.Set<Stream>[] outputStreams)
    {
        for (int outputIndex = 0; outputIndex < outputStreams.length; outputIndex++) {
            if (outputStreams[outputIndex] == null) {
                outputStreams[outputIndex] = batch.output(outputIndex).streams();
            }
        }
    }

    private static void captureKnownAllFalse(Batch batch, int[] outputKnownAllFalseFlags, boolean[] outputKnownAllFalseInitialized)
    {
        for (int outputIndex = 0; outputIndex < outputKnownAllFalseFlags.length; outputIndex++) {
            Output output = batch.output(outputIndex);
            int flags = streamFlags(output.knownAllFalseStreams());
            if (!outputKnownAllFalseInitialized[outputIndex]) {
                outputKnownAllFalseFlags[outputIndex] = flags;
                outputKnownAllFalseInitialized[outputIndex] = true;
            }
            else {
                outputKnownAllFalseFlags[outputIndex] &= flags;
            }
        }
    }

    private static int[] positions(Mask mask)
    {
        int[] positions = new int[mask.count()];
        if (mask.all()) {
            for (int index = 0; index < positions.length; index++) {
                positions[index] = index;
            }
            return positions;
        }
        for (int index = 0; index < positions.length; index++) {
            positions[index] = mask.position(index);
        }
        return positions;
    }

    private static int[] densePositions(int length)
    {
        int[] positions = new int[length];
        for (int index = 0; index < length; index++) {
            positions[index] = index;
        }
        return positions;
    }

    private static int streamFlags(java.util.Set<Stream> streams)
    {
        int flags = 0;
        for (Stream stream : streams) {
            flags |= streamFlag(stream);
        }
        return flags;
    }

    private static int streamFlag(Stream stream)
    {
        return switch (stream) {
            case VALUES -> VALUES_FLAG;
            case NULLS -> NULLS_FLAG;
            case ERRORS -> ERRORS_FLAG;
        };
    }

    static final class InnerBatch
    {
        private final Streams[] columns;
        private final int length;
        private final Batch retainedBatch;
        private final int[] positions;

        InnerBatch(Streams[] columns, int length)
        {
            this(columns, length, null, null);
        }

        private InnerBatch(Streams[] columns, int length, Batch retainedBatch, int[] positions)
        {
            this.columns = columns;
            this.length = length;
            this.retainedBatch = retainedBatch;
            this.positions = positions;
        }

        public static InnerBatch retained(Batch batch, int[] positions)
        {
            return new InnerBatch(null, positions.length, batch, positions);
        }

        public Streams[] columns()
        {
            return columns;
        }

        public int length()
        {
            return length;
        }

        public boolean retained()
        {
            return retainedBatch != null;
        }

        public Batch retainedBatch()
        {
            return retainedBatch;
        }

        public int sourcePosition(int position)
        {
            return positions == null ? position : positions[position];
        }

        public int[] positions()
        {
            return positions;
        }

    }
}
