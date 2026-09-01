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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLongArray;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Immutable build payload and task-wide match state shared by parallel probe sessions of a RIGHT or FULL join.
 *
 * <p>Probe sessions emit matched rows independently and atomically mark the corresponding build identities. After
 * every probe session has closed, the embedding scheduler creates the single unmatched-build operator. Keeping that
 * lifecycle decision outside this class lets an embedding engine coordinate its own driver graph without coupling
 * Nitro to that scheduler.
 */
public final class SharedBuildOuterJoin
        implements AutoCloseable
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("SharedBuildOuterJoin", SharedBuildOuterJoin.class);
    private final Schema probeSchema;
    private final Schema buildSchema;
    private final Schema augmentedBuildSchema;
    private final Schema outputSchema;
    private final int[] probeJoinColumns;
    private final int[] buildJoinColumns;
    private final int[] outputChannels;
    private final int[] joinOutputChannels;
    private final HashJoinOperator.JoinFilter[] joinFilters;
    private final Streams[] observedProbeStreams;
    private final boolean probeOuterJoin;
    private final int maxOutputRows;
    private final List<TableOperator.Page> buildPages;
    private final List<TableOperator.Page> augmentedBuildPages;
    private final int[] buildPageStarts;
    private final AtomicLongArray matchedBuildRows;
    private final boolean nestedLoop;
    private final HashJoinBuild preparedBuild;
    private boolean closed;

    public static Optional<SharedBuildOuterJoin> prepare(
            OperatorResources resources,
            Allocator allocator,
            Schema probeSchema,
            int[] probeJoinColumns,
            Operator build,
            int[] buildJoinColumns,
            boolean probeOuterJoin,
            int[] outputChannels,
            HashJoinOperator.JoinFilter... joinFilters)
    {
        SharedBuildOuterJoin shared = new SharedBuildOuterJoin(
                resources,
                allocator,
                probeSchema,
                probeJoinColumns,
                build,
                buildJoinColumns,
                probeOuterJoin,
                outputChannels,
                joinFilters);
        if (!shared.nestedLoop && shared.preparedBuild == null) {
            shared.close();
            return Optional.empty();
        }
        return Optional.of(shared);
    }

    private SharedBuildOuterJoin(
            OperatorResources resources,
            Allocator allocator,
            Schema probeSchema,
            int[] probeJoinColumns,
            Operator build,
            int[] buildJoinColumns,
            boolean probeOuterJoin,
            int[] outputChannels,
            HashJoinOperator.JoinFilter[] joinFilters)
    {
        requireNonNull(resources, "resources is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.probeSchema = requireNonNull(probeSchema, "probeSchema is null");
        this.probeJoinColumns = requireNonNull(probeJoinColumns, "probeJoinColumns is null").clone();
        requireNonNull(build, "build is null");
        buildSchema = build.outputSchema();
        this.buildJoinColumns = requireNonNull(buildJoinColumns, "buildJoinColumns is null").clone();
        if ((this.probeJoinColumns.length == 0) != (this.buildJoinColumns.length == 0)) {
            throw new IllegalArgumentException("Probe and build join columns must both be empty or both be non-empty");
        }
        nestedLoop = this.probeJoinColumns.length == 0;
        this.probeOuterJoin = probeOuterJoin;
        maxOutputRows = resources.hashJoin().executionPolicy().maxBatchRows();
        this.outputChannels = requireNonNull(outputChannels, "outputChannels is null").clone();
        this.joinFilters = requireNonNull(joinFilters, "joinFilters is null").clone();
        observedProbeStreams = new Streams[probeSchema.size()];
        outputSchema = selectOutputSchema(probeSchema, buildSchema, this.outputChannels);

        MaterializedBuild materialized = materializeBuild(build);
        buildPages = materialized.pages();
        buildPageStarts = materialized.pageStarts();
        matchedBuildRows = new AtomicLongArray((materialized.rowCount() + Long.SIZE - 1) / Long.SIZE);
        augmentedBuildSchema = appendIdentity(buildSchema);
        augmentedBuildPages = appendBuildIdentities(buildPages, buildPageStarts);
        int hiddenOutputChannel = probeSchema.size() + buildSchema.size();
        joinOutputChannels = Arrays.copyOf(this.outputChannels, this.outputChannels.length + 1);
        joinOutputChannels[joinOutputChannels.length - 1] = hiddenOutputChannel;
        preparedBuild = nestedLoop
                ? null
                : HashJoinSession.prepareBuild(
                                resources,
                                allocator,
                                probeSchema,
                                this.probeJoinColumns,
                                TableOperator.retained(augmentedBuildSchema, augmentedBuildPages),
                                this.buildJoinColumns,
                                probeOuterJoin,
                                joinOutputChannels,
                                this.joinFilters)
                        .orElse(null);
    }

    public JoinSession newProbeSession(OperatorResources resources, Allocator probeAllocator)
    {
        checkOpen();
        JoinSession join = nestedLoop
                ? new NestedLoopJoinSession(
                                requireNonNull(resources, "resources is null"),
                                requireNonNull(probeAllocator, "probeAllocator is null"),
                                probeSchema,
                                TableOperator.retained(augmentedBuildSchema, augmentedBuildPages),
                                probeOuterJoin,
                                joinFilters)
                        .withOutputs(joinOutputChannels)
                : new HashJoinSession(
                                requireNonNull(resources, "resources is null"),
                                requireNonNull(probeAllocator, "probeAllocator is null"),
                                probeSchema,
                                probeJoinColumns,
                                TableOperator.retained(augmentedBuildSchema, augmentedBuildPages),
                                buildJoinColumns,
                                probeOuterJoin,
                                preparedBuild,
                                joinFilters)
                        .withOutputs(joinOutputChannels);
        return new ProbeSession(join);
    }

    /** Creates the sole post-probe stream of unmatched build rows. */
    public Operator newUnmatchedBuildOperator(Allocator outputAllocator)
    {
        checkOpen();
        return new UnmatchedBuildOperator(requireNonNull(outputAllocator, "outputAllocator is null"));
    }

    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (preparedBuild != null) {
            preparedBuild.close();
        }
        allocator.release(allocationContext);
    }

    private void markMatched(Batch output)
    {
        Output identityOutput = output.output(outputChannels.length);
        VectorAccess.LongValues identities = VectorAccess.longValues(identityOutput.borrow(Stream.VALUES));
        VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(identityOutput.borrowOrNull(Stream.NULLS));
        Mask mask = output.borrowMask();
        for (int position : mask) {
            if (nulls.value(position)) {
                continue;
            }
            int identity = toIntExact(identities.value(position));
            int word = identity / Long.SIZE;
            long bit = 1L << (identity % Long.SIZE);
            matchedBuildRows.getAndUpdate(word, value -> value | bit);
        }
    }

    private boolean matched(int identity)
    {
        return (matchedBuildRows.get(identity / Long.SIZE) & (1L << (identity % Long.SIZE))) != 0;
    }

    private final class ProbeSession
            implements JoinSession
    {
        private final JoinSession join;

        private ProbeSession(JoinSession join)
        {
            this.join = join;
        }

        @Override
        public Schema outputSchema()
        {
            return outputSchema;
        }

        @Override
        public void addInput(Batch batch)
        {
            captureProbeStreams(batch);
            join.addInput(batch);
        }

        @Override
        public boolean hasOutput()
        {
            return join.hasOutput();
        }

        @Override
        public Batch getOutput()
        {
            Batch source = join.getOutput();
            markMatched(source);
            Output[] outputs = new Output[outputChannels.length];
            for (int index = 0; index < outputs.length; index++) {
                Output sourceOutput = source.output(index);
                outputs[index] = sourceOutput.forward(
                        (stream, _) -> sourceOutput.take(stream),
                        (_, _) -> {});
            }
            return new Batch(
                    source.borrowMask(),
                    source::constrain,
                    _ -> source.takeMask(),
                    _ -> {},
                    source::close,
                    outputs);
        }

        @Override
        public void finish()
        {
            join.finish();
        }

        @Override
        public boolean isFinished()
        {
            return join.isFinished();
        }

        @Override
        public void close()
        {
            join.close();
        }
    }

    private final class UnmatchedBuildOperator
            implements Operator
    {
        private final Allocator outputAllocator;
        private final Allocator.Context outputContext = new Allocator.Context("SharedBuildOuterJoin.output", SharedBuildOuterJoin.class);
        private int pageIndex;
        private int position;
        private Batch output;
        private boolean closed;

        private UnmatchedBuildOperator(Allocator outputAllocator)
        {
            this.outputAllocator = outputAllocator;
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
            checkOperatorOpen();
            if (output != null) {
                return true;
            }
            while (pageIndex < buildPages.size()) {
                TableOperator.Page page = buildPages.get(pageIndex);
                int[] positions = new int[Math.min(maxOutputRows, page.rows() - position)];
                int count = 0;
                while (position < page.rows() && count < positions.length) {
                    int physicalPosition = position++;
                    if (!matched(buildPageStarts[pageIndex] + physicalPosition)) {
                        positions[count++] = physicalPosition;
                    }
                }
                if (position == page.rows()) {
                    pageIndex++;
                    position = 0;
                }
                if (count != 0) {
                    output = unmatchedBuildOutput(page, Mask.sparse(Arrays.copyOf(positions, count), page.rows()), count);
                    return true;
                }
            }
            return false;
        }

        @Override
        public Batch next()
        {
            if (!hasNext()) {
                throw new IllegalStateException("No more unmatched build rows");
            }
            Batch result = output;
            output = null;
            return result;
        }

        @Override
        public void constrain(Mask mask)
        {
            if (output != null) {
                output.constrain(mask);
            }
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (output != null) {
                output.close();
                output = null;
            }
            outputAllocator.release(outputContext);
        }

        private Batch unmatchedBuildOutput(TableOperator.Page page, Mask selection, int rowCount)
        {
            Output[] outputs = new Output[outputChannels.length];
            for (int outputIndex = 0; outputIndex < outputChannels.length; outputIndex++) {
                int channel = outputChannels[outputIndex];
                Streams streams = channel < probeSchema.size()
                        ? nullProbeStreams(probeSchema.field(channel).type(), channel, rowCount)
                        : outputAllocator.copyStreams(outputContext, page.columns()[channel - probeSchema.size()], selection);
                outputs[outputIndex] = ownedOutput(streams);
            }
            Mask outputMask = outputAllocator.allocateAllMask(outputContext, rowCount);
            return new Batch(
                    outputMask,
                    _ -> {},
                    mask -> outputAllocator.transfer(outputContext, mask),
                    mask -> outputAllocator.release(outputContext, mask),
                    () -> {},
                    outputs);
        }

        private Streams nullProbeStreams(TypeBinding type, int probeColumn, int rowCount)
        {
            Vector values = type.vectorFactory()
                    .map(factory -> factory.nullValues(outputAllocator.vectorAllocator(outputContext), rowCount))
                    .orElseGet(() -> legacyNullValues(type, probeColumn, rowCount));
            BooleanVector nulls = outputAllocator.allocate(outputContext, BooleanVector.class, rowCount, BooleanVector::new);
            Arrays.fill(nulls.values(), true);
            return Streams.of(values, nulls, null);
        }

        private Vector legacyNullValues(TypeBinding type, int probeColumn, int rowCount)
        {
            Streams streams = observedProbeStreams[probeColumn];
            if (streams == null) {
                throw new IllegalArgumentException("Build-outer join requires a vector factory or observed probe input for type " + type.identity());
            }
            return switch (streams.values()) {
                case I64Vector _ -> outputAllocator.allocate(outputContext, I64Vector.class, rowCount, I64Vector::new);
                case I32Vector _ -> outputAllocator.allocate(outputContext, I32Vector.class, rowCount, I32Vector::new);
                case F64Vector _ -> outputAllocator.allocate(outputContext, F64Vector.class, rowCount, F64Vector::new);
                case BooleanVector _ -> outputAllocator.allocate(outputContext, BooleanVector.class, rowCount, BooleanVector::new);
                case BinaryVector _ -> BinaryVector.allocate(outputAllocator, outputContext, rowCount, 0);
                default -> throw new IllegalArgumentException("Unsupported build-outer probe vector: " + streams.values().getClass().getName());
            };
        }

        private Output ownedOutput(Streams streams)
        {
            return new Output(
                    streams.streams(),
                    streams::get,
                    (_, vector) -> outputAllocator.transfer(outputContext, vector),
                    (_, vector) -> outputAllocator.release(outputContext, vector));
        }

        private void checkOperatorOpen()
        {
            if (closed) {
                throw new IllegalStateException("unmatched build operator is closed");
            }
        }
    }

    private synchronized void captureProbeStreams(Batch batch)
    {
        for (int outputIndex = 0; outputIndex < observedProbeStreams.length; outputIndex++) {
            if (observedProbeStreams[outputIndex] != null) {
                continue;
            }
            Output output = batch.output(outputIndex);
            Streams.Builder streams = Streams.builder();
            for (Stream stream : output.streams()) {
                streams.put(stream, output.borrow(stream).emptyLike(allocator, allocationContext));
            }
            observedProbeStreams[outputIndex] = streams.build();
        }
    }

    private MaterializedBuild materializeBuild(Operator build)
    {
        List<TableOperator.Page> pages = new ArrayList<>();
        List<Integer> starts = new ArrayList<>();
        int rowCount = 0;
        try {
            while (build.hasNext()) {
                try (Batch batch = build.next()) {
                    Mask mask = batch.borrowMask();
                    if (mask.none()) {
                        continue;
                    }
                    Streams[] columns = new Streams[build.outputCount()];
                    for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                        Output output = batch.output(outputIndex);
                        columns[outputIndex] = allocator.copyStreams(allocationContext, borrowedStreams(output), mask);
                    }
                    starts.add(rowCount);
                    pages.add(new TableOperator.Page(mask.count(), columns, Mask.all(mask.count())));
                    rowCount = Math.addExact(rowCount, mask.count());
                }
            }
        }
        finally {
            build.close();
        }
        return new MaterializedBuild(List.copyOf(pages), starts.stream().mapToInt(Integer::intValue).toArray(), rowCount);
    }

    private List<TableOperator.Page> appendBuildIdentities(List<TableOperator.Page> pages, int[] pageStarts)
    {
        List<TableOperator.Page> augmented = new ArrayList<>(pages.size());
        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
            TableOperator.Page page = pages.get(pageIndex);
            Streams[] columns = Arrays.copyOf(page.columns(), page.columns().length + 1);
            I64Vector identities = allocator.allocate(allocationContext, I64Vector.class, page.rows(), I64Vector::new);
            int start = pageStarts[pageIndex];
            Arrays.setAll(identities.values(), position -> start + position);
            columns[columns.length - 1] = Streams.ofValues(identities);
            augmented.add(new TableOperator.Page(page.rows(), columns, page.mask()));
        }
        return List.copyOf(augmented);
    }

    private static Streams borrowedStreams(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, output.borrow(stream));
        }
        return builder.build();
    }

    private static Schema appendIdentity(Schema schema)
    {
        List<Field> fields = new ArrayList<>(schema.fields());
        fields.add(new Field(Schema.unspecified(1).field(0).type(), false));
        return new Schema(fields);
    }

    private static Schema selectOutputSchema(Schema probeSchema, Schema buildSchema, int[] outputChannels)
    {
        List<Field> fields = new ArrayList<>(outputChannels.length);
        for (int channel : outputChannels) {
            if (channel < 0 || channel >= probeSchema.size() + buildSchema.size()) {
                throw new IllegalArgumentException("Join output column is out of bounds: " + channel);
            }
            Field field = channel < probeSchema.size()
                    ? probeSchema.field(channel)
                    : buildSchema.field(channel - probeSchema.size());
            fields.add(channel < probeSchema.size() && !field.nullable()
                    ? new Field(field.name(), field.type(), true)
                    : field);
        }
        return new Schema(fields);
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("shared build-outer join is closed");
        }
    }

    private record MaterializedBuild(List<TableOperator.Page> pages, int[] pageStarts, int rowCount) {}
}
