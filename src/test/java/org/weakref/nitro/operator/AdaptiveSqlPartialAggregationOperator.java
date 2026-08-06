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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/** Streaming test-harness model of a Trino adaptive partial aggregation plan node. */
public final class AdaptiveSqlPartialAggregationOperator
        implements Operator
{
    private final Operator source;
    private final GroupedAggregationSession[] sessions;
    private final AdaptiveControl control;
    private final long maxRetainedBytes;
    private final Allocator allocator;
    private final int[] hashChannels;
    private final SqlStageAggregationOperator.PartitionTransform partitionTransform;
    private final ArrayDeque<PartitionInput> partitionInputs = new ArrayDeque<>();
    private final Schema outputSchema;

    private Batch pendingOutput;
    private int nextInputPartition;
    private int finishingPartition;
    private boolean sourceFinished;
    private boolean closed;
    private long exchangeBatchId;

    public AdaptiveSqlPartialAggregationOperator(
            Allocator allocator,
            Operator source,
            int partitionCount,
            List<Integer> groupByColumns,
            Supplier<List<Accumulator>> accumulators,
            long maxRetainedBytes,
            double uniqueRowsRatioThreshold)
    {
        this(
                allocator,
                source,
                partitionCount,
                new int[0],
                null,
                groupByColumns,
                accumulators,
                maxRetainedBytes,
                uniqueRowsRatioThreshold,
                false);
    }

    public AdaptiveSqlPartialAggregationOperator(
            Allocator allocator,
            Operator source,
            int partitionCount,
            int[] hashChannels,
            SqlStageAggregationOperator.PartitionTransform partitionTransform,
            List<Integer> groupByColumns,
            Supplier<List<Accumulator>> accumulators,
            long maxRetainedBytes,
            double uniqueRowsRatioThreshold,
            boolean aggregationRequired)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.source = requireNonNull(source, "source is null");
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        if (maxRetainedBytes <= 0) {
            throw new IllegalArgumentException("maxRetainedBytes must be positive");
        }
        this.maxRetainedBytes = maxRetainedBytes;
        this.hashChannels = requireNonNull(hashChannels, "hashChannels is null").clone();
        this.partitionTransform = partitionTransform;
        control = new AdaptiveControl(maxRetainedBytes, uniqueRowsRatioThreshold, aggregationRequired);
        OperatorResources resources = EngineResources.from(allocator).operatorResources();
        requireNonNull(accumulators, "accumulators is null");
        Schema inputSchema = partitionTransform == null ? source.outputSchema() : partitionTransform.outputSchema();
        sessions = new GroupedAggregationSession[partitionCount];
        for (int partition = 0; partition < partitionCount; partition++) {
            sessions[partition] = new GroupedAggregationSession(
                    allocator,
                    inputSchema,
                    groupByColumns,
                    groupByColumns,
                    PhysicalAggregationProgram.independent(accumulators.get()),
                    resources,
                    control);
        }
        outputSchema = sessions[0].outputSchema();
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
        if (pendingOutput != null) {
            return true;
        }
        while (!sourceFinished && source.hasNext()) {
            if (processPartitionInput()) {
                return true;
            }
            try (Batch batch = source.next()) {
                preparePartitionInputs(batch);
            }
        }
        while (processPartitionInput()) {
            return true;
        }
        if (!sourceFinished) {
            sourceFinished = true;
            source.close();
        }
        while (finishingPartition < sessions.length) {
            GroupedAggregationSession session = sessions[finishingPartition++];
            pendingOutput = session.finish();
            if (pendingOutput.borrowMask().count() > 0) {
                return true;
            }
            pendingOutput.close();
            pendingOutput = null;
        }
        return false;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more batches");
        }
        Batch output = pendingOutput;
        pendingOutput = null;
        return output;
    }

    @Override
    public void constrain(Mask mask) {}

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (pendingOutput != null) {
            pendingOutput.close();
            pendingOutput = null;
        }
        source.close();
        while (!partitionInputs.isEmpty()) {
            partitionInputs.removeFirst().operator().close();
        }
        for (GroupedAggregationSession session : sessions) {
            session.close();
        }
    }

    private long estimatedInputBytes(Batch batch, int outputCount)
    {
        Mask mask = batch.borrowMask();
        Set<Vector> vectors = Collections.newSetFromMap(new IdentityHashMap<>());
        long bytes = 0;
        for (int channel = 0; channel < outputCount; channel++) {
            Output output = batch.output(channel);
            for (Stream stream : output.streams()) {
                Vector vector = output.borrow(stream, mask);
                if (vectors.add(vector)) {
                    bytes += vector.retainedBytes();
                }
            }
        }
        return bytes;
    }

    private boolean processPartitionInput()
    {
        while (!partitionInputs.isEmpty()) {
            PartitionInput input = partitionInputs.getFirst();
            if (!input.operator().hasNext()) {
                input.operator().close();
                partitionInputs.removeFirst();
                continue;
            }
            try (Batch batch = input.operator().next()) {
                GroupedAggregationSession session = sessions[input.partition()];
                session.addInput(batch, estimatedInputBytes(batch, input.operator().outputCount()));
                if (session.hasOutput()) {
                    pendingOutput = session.getOutput();
                    return true;
                }
                if (session.retainedBytes() >= maxRetainedBytes) {
                    session.flush();
                    pendingOutput = session.getOutput();
                    return true;
                }
            }
        }
        return false;
    }

    private void preparePartitionInputs(Batch batch)
    {
        Mask inputMask = batch.borrowMask();
        if (inputMask.none()) {
            return;
        }
        if (hashChannels.length == 0) {
            int partition = nextInputPartition;
            nextInputPartition = (nextInputPartition + 1) % sessions.length;
            partitionInputs.addLast(new PartitionInput(partition, partitionTransform == null
                    ? new BatchOperator(source.outputSchema(), copyBatch(batch, positions(inputMask)))
                    : partitionTransform.apply(source.outputSchema(), copyBatch(batch, positions(inputMask)))));
            return;
        }

        Streams[] streams = resolveStreams(batch, inputMask);
        int[][] partitionPositions = new int[sessions.length][Math.max(1, inputMask.count())];
        int[] counts = new int[sessions.length];
        Vector[] values = new Vector[hashChannels.length];
        Vector[] nulls = new Vector[hashChannels.length];
        for (int index = 0; index < hashChannels.length; index++) {
            Streams key = streams[hashChannels[index]];
            values[index] = key.get(Stream.VALUES);
            nulls[index] = key.getOrNull(Stream.NULLS);
        }
        for (int position : inputMask) {
            long hash = 1;
            for (int key = 0; key < values.length; key++) {
                hash = 31 * hash + OperatorVectorSupport.hash(values[key], nulls[key], position);
            }
            int partition = Math.floorMod(hash, sessions.length);
            partitionPositions[partition][counts[partition]++] = position;
        }
        for (int partition = 0; partition < sessions.length; partition++) {
            if (counts[partition] == 0) {
                continue;
            }
            Batch partitionBatch = copyBatch(batch, Arrays.copyOf(partitionPositions[partition], counts[partition]));
            Operator operator = partitionTransform == null
                    ? new BatchOperator(source.outputSchema(), partitionBatch)
                    : partitionTransform.apply(source.outputSchema(), partitionBatch);
            partitionInputs.addLast(new PartitionInput(partition, operator));
        }
    }

    private Batch copyBatch(Batch batch, int[] selectedPositions)
    {
        Allocator.Context context = new Allocator.Context("adaptive-sql-exchange-" + exchangeBatchId++);
        Streams[] streams = resolveStreams(batch, batch.borrowMask());
        Output[] outputs = new Output[source.outputCount()];
        for (int channel = 0; channel < outputs.length; channel++) {
            Streams copied = allocator.copyStreams(context, streams[channel], selectedPositions);
            outputs[channel] = Output.of(copied);
        }
        return new Batch(
                Mask.all(selectedPositions.length),
                _ -> {},
                java.util.function.Function.identity(),
                _ -> {},
                () -> allocator.releaseIfPresent(context),
                outputs);
    }

    private Streams[] resolveStreams(Batch batch, Mask mask)
    {
        Streams[] streams = new Streams[source.outputCount()];
        for (int channel = 0; channel < streams.length; channel++) {
            Output output = batch.output(channel);
            Streams.Builder builder = Streams.builder();
            for (Stream stream : output.streams()) {
                builder.put(stream, output.borrow(stream, mask));
            }
            streams[channel] = builder.build();
        }
        return streams;
    }

    private static int[] positions(Mask mask)
    {
        int[] positions = new int[mask.count()];
        int index = 0;
        for (int position : mask) {
            positions[index++] = position;
        }
        return positions;
    }

    private static final class AdaptiveControl
            implements PartialAggregationControl
    {
        private static final double DISABLE_SAMPLE_FACTOR = 1.5;
        private static final double ENABLE_SAMPLE_FACTOR = DISABLE_SAMPLE_FACTOR * 200;

        private final long maxRetainedBytes;
        private final double uniqueRowsRatioThreshold;
        private final boolean aggregationRequired;
        private volatile boolean disabled;
        private long inputBytes;
        private long inputRows;
        private long outputRows;

        private AdaptiveControl(long maxRetainedBytes, double uniqueRowsRatioThreshold, boolean aggregationRequired)
        {
            this.maxRetainedBytes = maxRetainedBytes;
            this.uniqueRowsRatioThreshold = uniqueRowsRatioThreshold;
            this.aggregationRequired = aggregationRequired;
        }

        @Override
        public boolean aggregationEnabled()
        {
            return aggregationRequired || !disabled;
        }

        @Override
        public synchronized void onAggregatedFlush(long inputBytes, long inputRows, long outputRows)
        {
            this.inputBytes += inputBytes;
            this.inputRows += inputRows;
            this.outputRows += outputRows;
            if (!disabled && this.inputBytes >= maxRetainedBytes * DISABLE_SAMPLE_FACTOR &&
                    (double) this.outputRows / this.inputRows > uniqueRowsRatioThreshold) {
                disabled = true;
            }
        }

        @Override
        public synchronized void onPassthroughFlush(long inputBytes, long inputRows)
        {
            this.inputBytes += inputBytes;
            this.inputRows += inputRows;
            if (disabled && this.inputBytes >= maxRetainedBytes * ENABLE_SAMPLE_FACTOR) {
                this.inputBytes = 0;
                this.inputRows = 0;
                outputRows = 0;
                disabled = false;
            }
        }
    }

    private record PartitionInput(int partition, Operator operator) {}

    private static final class BatchOperator
            implements Operator
    {
        private final Schema outputSchema;
        private Batch batch;

        private BatchOperator(Schema outputSchema, Batch batch)
        {
            this.outputSchema = outputSchema;
            this.batch = batch;
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
            return batch != null;
        }

        @Override
        public Batch next()
        {
            Batch result = requireNonNull(batch, "No more batches");
            batch = null;
            return result;
        }

        @Override
        public void constrain(Mask mask)
        {
            if (batch != null) {
                batch.constrain(mask);
            }
        }

        @Override
        public void close()
        {
            if (batch != null) {
                batch.close();
                batch = null;
            }
        }
    }
}
