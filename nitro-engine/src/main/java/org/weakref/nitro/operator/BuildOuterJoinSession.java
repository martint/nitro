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

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Streaming hash join that preserves every build row. Matched rows are emitted as the probe is scheduled; after the
 * probe finishes, build rows that never matched are emitted with a null probe side. A hidden build-row identity makes
 * the match accounting exact for duplicate keys and residual join filters without exposing representation details to
 * the embedding engine.
 */
public final class BuildOuterJoinSession
        implements JoinSession
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("BuildOuterJoinSession", BuildOuterJoinSession.class);
    private final Schema probeSchema;
    private final Schema buildSchema;
    private final Schema outputSchema;
    private final int[] outputChannels;
    private final int maxOutputRows;
    private final Streams[] probeStreams;
    private final List<TableOperator.Page> buildPages;
    private final int[] buildPageStarts;
    private final boolean[] matchedBuildRows;
    private final HashJoinSession join;

    private Batch output;
    private boolean finishing;
    private boolean emittingUnmatchedBuild;
    private int unmatchedPageIndex;
    private int unmatchedPosition;
    private boolean closed;

    public BuildOuterJoinSession(
            OperatorResources resources,
            Allocator allocator,
            Schema probeSchema,
            int[] probeJoinColumns,
            Operator build,
            int[] buildJoinColumns,
            int[] outputChannels,
            HashJoinOperator.JoinFilter... joinFilters)
    {
        requireNonNull(resources, "resources is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.probeSchema = requireNonNull(probeSchema, "probeSchema is null");
        probeStreams = new Streams[probeSchema.size()];
        requireNonNull(build, "build is null");
        buildSchema = build.outputSchema();
        this.outputChannels = requireNonNull(outputChannels, "outputChannels is null").clone();
        maxOutputRows = resources.hashJoin().executionPolicy().maxBatchRows();

        MaterializedBuild materialized = materializeBuild(build);
        buildPages = materialized.pages();
        buildPageStarts = materialized.pageStarts();
        matchedBuildRows = new boolean[materialized.rowCount()];

        Schema augmentedBuildSchema = appendIdentity(buildSchema);
        int hiddenOutputChannel = probeSchema.size() + buildSchema.size();
        int[] joinOutputs = Arrays.copyOf(this.outputChannels, this.outputChannels.length + 1);
        joinOutputs[joinOutputs.length - 1] = hiddenOutputChannel;
        join = new HashJoinSession(
                resources,
                allocator,
                probeSchema,
                requireNonNull(probeJoinColumns, "probeJoinColumns is null"),
                TableOperator.retained(augmentedBuildSchema, appendBuildIdentities(buildPages, buildPageStarts)),
                requireNonNull(buildJoinColumns, "buildJoinColumns is null"),
                false,
                requireNonNull(joinFilters, "joinFilters is null"))
                .withOutputs(joinOutputs);
        outputSchema = selectOutputSchema(probeSchema, buildSchema, this.outputChannels);
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public void addInput(Batch batch)
    {
        checkAcceptingInput();
        requireNonNull(batch, "batch is null");
        captureProbeStreams(batch);
        join.addInput(batch);
    }

    @Override
    public boolean hasOutput()
    {
        checkOpen();
        if (output != null) {
            return true;
        }
        if (!emittingUnmatchedBuild && join.hasOutput()) {
            output = matchedOutput(join.getOutput());
            return true;
        }
        if (finishing && !emittingUnmatchedBuild && join.isFinished()) {
            emittingUnmatchedBuild = true;
        }
        if (emittingUnmatchedBuild) {
            output = nextUnmatchedBuildOutput();
        }
        return output != null;
    }

    @Override
    public Batch getOutput()
    {
        checkOpen();
        if (output == null) {
            throw new IllegalStateException("no join output is ready");
        }
        Batch result = output;
        output = null;
        return result;
    }

    @Override
    public void finish()
    {
        checkOpen();
        if (finishing) {
            return;
        }
        finishing = true;
        join.finish();
    }

    @Override
    public boolean isFinished()
    {
        checkOpen();
        if (!finishing || output != null) {
            return false;
        }
        hasOutput();
        return output == null && emittingUnmatchedBuild && unmatchedPageIndex == buildPages.size();
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
        try {
            join.close();
        }
        finally {
            allocator.release(allocationContext);
        }
    }

    private Batch matchedOutput(Batch source)
    {
        int hiddenOutput = outputChannels.length;
        VectorAccess.LongValues identities = VectorAccess.longValues(source.output(hiddenOutput).borrow(Stream.VALUES));
        Mask mask = source.borrowMask();
        for (int index = 0; index < mask.count(); index++) {
            matchedBuildRows[toIntExact(identities.value(mask.position(index)))] = true;
        }

        Output[] outputs = new Output[outputChannels.length];
        for (int index = 0; index < outputs.length; index++) {
            Output sourceOutput = source.output(index);
            outputs[index] = sourceOutput.forward(
                    (stream, _) -> sourceOutput.take(stream),
                    (_, _) -> {});
        }
        return new Batch(
                mask,
                source::constrain,
                _ -> source.takeMask(),
                _ -> {},
                source::close,
                outputs);
    }

    private Batch nextUnmatchedBuildOutput()
    {
        while (unmatchedPageIndex < buildPages.size()) {
            TableOperator.Page page = buildPages.get(unmatchedPageIndex);
            int[] positions = new int[Math.min(maxOutputRows, page.rows() - unmatchedPosition)];
            int count = 0;
            while (unmatchedPosition < page.rows() && count < positions.length) {
                int position = unmatchedPosition++;
                if (!matchedBuildRows[buildPageStarts[unmatchedPageIndex] + position]) {
                    positions[count++] = position;
                }
            }
            if (unmatchedPosition == page.rows()) {
                unmatchedPageIndex++;
                unmatchedPosition = 0;
            }
            if (count == 0) {
                continue;
            }
            Mask selection = Mask.sparse(Arrays.copyOf(positions, count), page.rows());
            return unmatchedBuildOutput(page, selection, count);
        }
        return null;
    }

    private Batch unmatchedBuildOutput(TableOperator.Page page, Mask selection, int rowCount)
    {
        Output[] outputs = new Output[outputChannels.length];
        for (int outputIndex = 0; outputIndex < outputChannels.length; outputIndex++) {
            int channel = outputChannels[outputIndex];
            Streams streams;
            if (channel < probeSchema.size()) {
                streams = nullProbeStreams(probeSchema.field(channel).type(), channel, rowCount);
            }
            else {
                streams = allocator.copyStreams(
                        allocationContext,
                        page.columns()[channel - probeSchema.size()],
                        selection);
            }
            outputs[outputIndex] = ownedOutput(streams);
        }
        Mask outputMask = allocator.allocateAllMask(allocationContext, rowCount);
        return new Batch(
                outputMask,
                _ -> {},
                mask -> allocator.transfer(allocationContext, mask),
                mask -> allocator.release(allocationContext, mask),
                () -> {},
                outputs);
    }

    private Streams nullProbeStreams(TypeBinding type, int probeColumn, int rowCount)
    {
        Vector values = type.vectorFactory()
                .map(factory -> factory.nullValues(allocator.vectorAllocator(allocationContext), rowCount))
                .orElseGet(() -> legacyNullValuesLike(probeStreams[probeColumn], type, rowCount));
        BooleanVector nulls = allocator.allocate(allocationContext, BooleanVector.class, rowCount, BooleanVector::new);
        Arrays.fill(nulls.values(), true);
        return Streams.of(values, nulls, null);
    }

    private Vector legacyNullValuesLike(Streams streams, TypeBinding type, int rowCount)
    {
        if (streams == null) {
            throw new IllegalArgumentException("Build-outer join requires a vector factory or observed probe input for type " + type.identity());
        }
        return switch (streams.values()) {
            case I64Vector _ -> allocator.allocate(allocationContext, I64Vector.class, rowCount, I64Vector::new);
            case I32Vector _ -> allocator.allocate(allocationContext, I32Vector.class, rowCount, I32Vector::new);
            case F64Vector _ -> allocator.allocate(allocationContext, F64Vector.class, rowCount, F64Vector::new);
            case BooleanVector _ -> allocator.allocate(allocationContext, BooleanVector.class, rowCount, BooleanVector::new);
            case BinaryVector _ -> BinaryVector.allocate(allocator, allocationContext, rowCount, 0);
            default -> throw new IllegalArgumentException("Unsupported build-outer probe vector: " + streams.values().getClass().getName());
        };
    }

    private void captureProbeStreams(Batch batch)
    {
        for (int outputIndex = 0; outputIndex < probeStreams.length; outputIndex++) {
            if (probeStreams[outputIndex] != null) {
                continue;
            }
            Output output = batch.output(outputIndex);
            Streams.Builder streams = Streams.builder();
            for (Stream stream : output.streams()) {
                streams.put(stream, output.borrow(stream).emptyLike(allocator, allocationContext));
            }
            probeStreams[outputIndex] = streams.build();
        }
    }

    private Output ownedOutput(Streams streams)
    {
        return new Output(
                streams.streams(),
                streams::get,
                (_, vector) -> allocator.transfer(allocationContext, vector),
                (_, vector) -> allocator.release(allocationContext, vector));
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
                        columns[outputIndex] = build.supportsRetainedBatches() && mask.all()
                                ? takenStreams(output)
                                : allocator.copyStreams(allocationContext, borrowedStreams(output), mask);
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
        int[] pageStarts = starts.stream().mapToInt(Integer::intValue).toArray();
        return new MaterializedBuild(List.copyOf(pages), pageStarts, rowCount);
    }

    private List<TableOperator.Page> appendBuildIdentities(List<TableOperator.Page> pages, int[] pageStarts)
    {
        List<TableOperator.Page> augmented = new ArrayList<>(pages.size());
        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
            TableOperator.Page page = pages.get(pageIndex);
            Streams[] columns = Arrays.copyOf(page.columns(), page.columns().length + 1);
            I64Vector identities = allocator.allocate(allocationContext, I64Vector.class, page.rows(), I64Vector::new);
            int pageStart = pageStarts[pageIndex];
            Arrays.setAll(identities.values(), position -> pageStart + position);
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

    private Streams takenStreams(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, allocator.transfer(allocationContext, output.take(stream)));
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

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finishing) {
            throw new IllegalStateException("build-outer join session is finishing");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("build-outer join session is closed");
        }
    }

    private record MaterializedBuild(List<TableOperator.Page> pages, int[] pageStarts, int rowCount) {}
}
