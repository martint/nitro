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

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Test-harness model of a distributed SQL aggregation fragment.
 *
 * <p>Each partition owns independent aggregation state. Input batches are either assigned to
 * source-driver partitions or hash-partitioned like an exchange. Multiple stages execute in the
 * same partition, modeling operator pipelines such as final DISTINCT followed by partial count.
 * This intentionally lives in test code: it describes SQL execution topology without adding a
 * scheduler or exchange policy to the Nitro engine.
 */
public final class SqlStageAggregationOperator
        implements Operator
{
    private final Operator source;
    private final Allocator allocator;
    private final Allocator.Context exchangeMaterializationContext = new Allocator.Context("sql-stage-exchange-materialization");
    private final int partitionCount;
    private final int[] hashChannels;
    private final PartitionTransform partitionTransform;
    private final PartitionTransform outputTransform;
    private final GroupedAggregationSession[][] sessions;
    private final Batch[] firstOutputs;
    private final Operator[] outputOperators;
    private final Schema outputSchema;
    private final boolean materializeExchange;

    private Batch pendingOutput;
    private int outputPartition;
    private int nextSourcePartition;
    private boolean prepared;
    private boolean closed;

    public SqlStageAggregationOperator(
            Allocator allocator,
            Operator source,
            int partitionCount,
            int[] hashChannels,
            List<Stage> stages)
    {
        this(allocator, source, partitionCount, hashChannels, stages, true, null, null);
    }

    public SqlStageAggregationOperator(
            Allocator allocator,
            Operator source,
            int partitionCount,
            int[] hashChannels,
            List<Stage> stages,
            boolean materializeExchange)
    {
        this(allocator, source, partitionCount, hashChannels, stages, materializeExchange, null, null);
    }

    public SqlStageAggregationOperator(
            Allocator allocator,
            Operator source,
            int partitionCount,
            int[] hashChannels,
            List<Stage> stages,
            boolean materializeExchange,
            PartitionTransform partitionTransform)
    {
        this(allocator, source, partitionCount, hashChannels, stages, materializeExchange, partitionTransform, null);
    }

    public SqlStageAggregationOperator(
            Allocator allocator,
            Operator source,
            int partitionCount,
            int[] hashChannels,
            List<Stage> stages,
            boolean materializeExchange,
            PartitionTransform partitionTransform,
            PartitionTransform outputTransform)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.source = requireNonNull(source, "source is null");
        checkArgument(partitionCount > 0, "partitionCount must be positive");
        this.partitionCount = partitionCount;
        this.hashChannels = requireNonNull(hashChannels, "hashChannels is null").clone();
        this.materializeExchange = materializeExchange;
        this.partitionTransform = partitionTransform;
        this.outputTransform = outputTransform;
        checkArgument(!stages.isEmpty(), "stages is empty");

        OperatorResources resources = EngineResources.from(allocator).operatorResources();
        sessions = new GroupedAggregationSession[stages.size()][partitionCount];
        firstOutputs = new Batch[partitionCount];
        outputOperators = outputTransform == null ? null : new Operator[partitionCount];
        Schema inputSchema = partitionTransform == null ? source.outputSchema() : partitionTransform.outputSchema();
        for (int stageIndex = 0; stageIndex < stages.size(); stageIndex++) {
            Stage stage = stages.get(stageIndex);
            for (int partition = 0; partition < partitionCount; partition++) {
                sessions[stageIndex][partition] = new GroupedAggregationSession(
                        allocator,
                        inputSchema,
                        stage.groupByColumns(),
                        stage.groupedColumns(),
                        stage.programFactory().get(),
                        resources);
            }
            inputSchema = sessions[stageIndex][0].outputSchema();
        }
        outputSchema = outputTransform == null ? inputSchema : outputTransform.outputSchema();
    }

    public static Stage distinct(List<Integer> groupByColumns)
    {
        return new Stage(groupByColumns, groupByColumns, () -> PhysicalAggregationProgram.independent(List.of()));
    }

    public static Stage aggregate(List<Integer> groupByColumns, Supplier<List<Accumulator>> accumulators)
    {
        return new Stage(
                groupByColumns,
                groupByColumns,
                () -> PhysicalAggregationProgram.independent(accumulators.get()));
    }

    public static PartitionTransform groupIdTransform(
            Allocator allocator,
            Schema inputSchema,
            int[][] groupingSetInputs,
            GroupIdOperatorPolicy policy)
    {
        GroupIdOperator schemaOperator = new GroupIdOperator(
                allocator,
                new EmptyOperator(inputSchema),
                groupingSetInputs,
                policy);
        Schema outputSchema = schemaOperator.outputSchema();
        schemaOperator.close();
        return new PartitionTransform(
                outputSchema,
                source -> new GroupIdOperator(allocator, source, groupingSetInputs, policy));
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
        prepare();
        advanceOutput();
        return pendingOutput != null;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more batches");
        }
        Batch result = pendingOutput;
        pendingOutput = null;
        return result;
    }

    @Override
    public void constrain(Mask mask)
    {
    }

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
        for (int partition = 0; partition < firstOutputs.length; partition++) {
            if (outputOperators != null && outputOperators[partition] != null) {
                outputOperators[partition].close();
                outputOperators[partition] = null;
            }
            if (firstOutputs[partition] != null) {
                firstOutputs[partition].close();
                firstOutputs[partition] = null;
            }
        }
        source.close();
        for (GroupedAggregationSession[] stage : sessions) {
            for (GroupedAggregationSession session : stage) {
                session.close();
            }
        }
    }

    private void prepare()
    {
        if (prepared) {
            return;
        }
        prepared = true;
        try {
            while (source.hasNext()) {
                try (Batch batch = source.next()) {
                    if (hashChannels.length == 0) {
                        sessions[0][nextSourcePartition].addInput(batch);
                        nextSourcePartition = (nextSourcePartition + 1) % partitionCount;
                    }
                    else {
                        addHashPartitioned(batch);
                    }
                }
            }
            source.close();

            for (int stageIndex = 0; stageIndex < sessions.length - 1; stageIndex++) {
                for (int partition = 0; partition < partitionCount; partition++) {
                    drain(sessions[stageIndex][partition], sessions[stageIndex + 1][partition]);
                }
            }
            for (int partition = 0; partition < partitionCount; partition++) {
                GroupedAggregationSession finalSession = sessions[sessions.length - 1][partition];
                if (outputTransform == null) {
                    firstOutputs[partition] = finalSession.finish();
                }
                else {
                    outputOperators[partition] = outputTransform.factory().apply(new SessionOutputOperator(finalSession));
                }
            }
        }
        catch (RuntimeException | Error failure) {
            close();
            throw failure;
        }
    }

    private void addHashPartitioned(Batch batch)
    {
        Mask inputMask = batch.borrowMask();
        Streams[] streams = resolveStreams(batch, inputMask, source.outputCount());
        int[][] positions = new int[partitionCount][Math.max(1, inputMask.count())];
        int[] counts = new int[partitionCount];
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
            int partition = Math.floorMod(hash, partitionCount);
            positions[partition][counts[partition]++] = position;
        }

        for (int partition = 0; partition < partitionCount; partition++) {
            if (counts[partition] == 0) {
                continue;
            }
            int[] selectedPositions = Arrays.copyOf(positions[partition], counts[partition]);
            Output[] outputs = materializeExchange
                    ? copyOutputs(batch, streams, selectedPositions)
                    : Arrays.stream(streams)
                            .map(Output::of)
                            .toArray(Output[]::new);
            try (Batch partitionBatch = new Batch(
                    materializeExchange ? Mask.all(counts[partition]) : Mask.sparse(selectedPositions, inputMask.size()),
                    _ -> {},
                    java.util.function.Function.identity(),
                    _ -> {},
                    materializeExchange ? () -> allocator.releaseIfPresent(exchangeMaterializationContext) : () -> {},
                    outputs)) {
                addPartitionInput(partition, partitionBatch);
            }
        }
    }

    private void addPartitionInput(int partition, Batch partitionBatch)
    {
        if (partitionTransform == null) {
            sessions[0][partition].addInput(partitionBatch);
            return;
        }

        Operator transformed = partitionTransform.factory().apply(new SingleBatchOperator(source.outputSchema(), partitionBatch));
        try {
            while (transformed.hasNext()) {
                try (Batch transformedBatch = transformed.next()) {
                    sessions[0][partition].addInput(transformedBatch);
                }
            }
        }
        finally {
            transformed.close();
        }
    }

    private Output[] copyOutputs(Batch batch, Streams[] streams, int[] positions)
    {
        Output[] outputs = new Output[source.outputCount()];
        for (int channel = 0; channel < outputs.length; channel++) {
            Streams copied = batch.output(channel).copyPositions(
                    null,
                    positions,
                    0,
                    positions.length,
                    0,
                    positions.length,
                    true);
            if (copied == null) {
                copied = allocator.copyStreams(exchangeMaterializationContext, streams[channel], positions);
            }
            outputs[channel] = Output.of(copied);
        }
        return outputs;
    }

    private static Streams[] resolveStreams(Batch batch, Mask mask, int outputCount)
    {
        Streams[] streams = new Streams[outputCount];
        for (int channel = 0; channel < outputCount; channel++) {
            Output output = batch.output(channel);
            Streams.Builder builder = Streams.builder();
            for (Stream stream : output.streams()) {
                builder.put(stream, output.borrow(stream, mask));
            }
            streams[channel] = builder.build();
        }
        return streams;
    }

    private static void drain(GroupedAggregationSession source, GroupedAggregationSession target)
    {
        try (Batch first = source.finish()) {
            target.addInput(first);
        }
        while (source.hasOutput()) {
            try (Batch output = source.getOutput()) {
                target.addInput(output);
            }
        }
        source.close();
    }

    private void advanceOutput()
    {
        if (pendingOutput != null) {
            return;
        }
        GroupedAggregationSession[] finalStage = sessions[sessions.length - 1];
        while (outputPartition < partitionCount) {
            if (outputOperators != null) {
                Operator output = outputOperators[outputPartition];
                if (output.hasNext()) {
                    pendingOutput = output.next();
                    return;
                }
                output.close();
                outputOperators[outputPartition] = null;
                outputPartition++;
                continue;
            }
            GroupedAggregationSession session = finalStage[outputPartition];
            if (firstOutputs[outputPartition] != null) {
                pendingOutput = firstOutputs[outputPartition];
                firstOutputs[outputPartition] = null;
                return;
            }
            if (session.hasOutput()) {
                pendingOutput = session.getOutput();
                return;
            }
            session.close();
            outputPartition++;
        }
    }

    public record Stage(
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            Supplier<PhysicalAggregationProgram> programFactory)
    {
        public Stage
        {
            groupByColumns = List.copyOf(groupByColumns);
            groupedColumns = List.copyOf(groupedColumns);
            requireNonNull(programFactory, "programFactory is null");
        }
    }

    public record PartitionTransform(Schema outputSchema, Function<Operator, Operator> factory)
    {
        public PartitionTransform
        {
            requireNonNull(outputSchema, "outputSchema is null");
            requireNonNull(factory, "factory is null");
        }

        Operator apply(Schema inputSchema, Batch batch)
        {
            return factory.apply(new SingleBatchOperator(inputSchema, batch));
        }
    }

    private static final class EmptyOperator
            implements Operator
    {
        private final Schema outputSchema;

        private EmptyOperator(Schema outputSchema)
        {
            this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
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
            return false;
        }

        @Override
        public Batch next()
        {
            throw new IllegalStateException("No batches");
        }

        @Override
        public void constrain(Mask mask)
        {
        }

        @Override
        public void close()
        {
        }
    }

    private static final class SingleBatchOperator
            implements Operator
    {
        private final Schema outputSchema;
        private Batch batch;

        private SingleBatchOperator(Schema outputSchema, Batch batch)
        {
            this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
            this.batch = requireNonNull(batch, "batch is null");
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
            if (batch == null) {
                throw new IllegalStateException("No more batches");
            }
            Batch result = batch;
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

    private static final class SessionOutputOperator
            implements Operator
    {
        private final GroupedAggregationSession session;
        private Batch first;
        private boolean started;

        private SessionOutputOperator(GroupedAggregationSession session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public int outputCount()
        {
            return session.outputSchema().size();
        }

        @Override
        public Schema outputSchema()
        {
            return session.outputSchema();
        }

        @Override
        public boolean hasNext()
        {
            if (!started) {
                started = true;
                first = session.finish();
            }
            return first != null || session.hasOutput();
        }

        @Override
        public Batch next()
        {
            if (!hasNext()) {
                throw new IllegalStateException("No more batches");
            }
            if (first != null) {
                Batch result = first;
                first = null;
                return result;
            }
            return session.getOutput();
        }

        @Override
        public void constrain(Mask mask)
        {
            if (first != null) {
                first.constrain(mask);
            }
        }

        @Override
        public void close()
        {
            if (first != null) {
                first.close();
                first = null;
            }
            session.close();
        }
    }
}
