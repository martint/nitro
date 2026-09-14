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

import java.util.Optional;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/**
 * Long-lived hash-join state for probe batches scheduled by an external host.
 *
 * <p>The build operator is consumed once, on the first probe. Probe-batch ownership transfers to
 * the session and each batch is closed as soon as all of its join output has been drained. The
 * hash table, retained build payload, generated kernels, and reusable buffers survive across every
 * offered probe batch.
 */
public final class HashJoinSession
        implements JoinSession
{
    private final Allocator allocator;
    private final ExternallyScheduledBatchFeed probe;
    private final HashJoinOperator join;
    private Operator outputRoot;

    private Batch output;
    private boolean finishing;
    private boolean closed;

    public HashJoinSession(
            OperatorResources operatorResources,
            Allocator allocator,
            Schema probeSchema,
            int[] probeJoinColumns,
            Operator build,
            int[] buildJoinColumns,
            boolean probeOuterJoin,
            HashJoinOperator.JoinFilter... joinFilters)
    {
        this(
                operatorResources,
                allocator,
                probeSchema,
                probeJoinColumns,
                build,
                buildJoinColumns,
                probeOuterJoin,
                null,
                joinFilters);
    }

    public HashJoinSession(
            OperatorResources operatorResources,
            Allocator allocator,
            Schema probeSchema,
            int[] probeJoinColumns,
            Operator build,
            int[] buildJoinColumns,
            boolean probeOuterJoin,
            HashJoinBuild preparedBuild,
            HashJoinOperator.JoinFilter... joinFilters)
    {
        this(
                operatorResources,
                allocator,
                probeSchema,
                probeJoinColumns,
                build,
                buildJoinColumns,
                probeOuterJoin,
                preparedBuild,
                UnaryOperator.identity(),
                joinFilters);
    }

    public HashJoinSession(
            OperatorResources operatorResources,
            Allocator allocator,
            Schema probeSchema,
            int[] probeJoinColumns,
            Operator build,
            int[] buildJoinColumns,
            boolean probeOuterJoin,
            HashJoinBuild preparedBuild,
            UnaryOperator<Operator> probePipeline,
            HashJoinOperator.JoinFilter... joinFilters)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        probe = new ExternallyScheduledBatchFeed(requireNonNull(probeSchema, "probeSchema is null"));
        Operator transformedProbe;
        try {
            transformedProbe = requireNonNull(
                    requireNonNull(probePipeline, "probePipeline is null").apply(probe),
                    "probePipeline returned null");
        }
        catch (RuntimeException | Error failure) {
            probe.close();
            throw failure;
        }
        Operator scheduledProbe = transformedProbe == probe
                ? probe
                : new ExternallyScheduledOperator(transformedProbe, probe::isFinished);
        join = new HashJoinOperator(
                requireNonNull(operatorResources, "operatorResources is null"),
                allocator,
                scheduledProbe,
                probeJoinColumns.clone(),
                requireNonNull(build, "build is null"),
                buildJoinColumns.clone(),
                probeOuterJoin,
                preparedBuild,
                joinFilters.clone());
        outputRoot = join;
    }

    public static Optional<HashJoinBuild> prepareBuild(
            OperatorResources operatorResources,
            Allocator allocator,
            Schema probeSchema,
            int[] probeJoinColumns,
            Operator build,
            int[] buildJoinColumns,
            boolean probeOuterJoin,
            int[] outputChannels,
            HashJoinOperator.JoinFilter... joinFilters)
    {
        ExternallyScheduledBatchFeed probe = new ExternallyScheduledBatchFeed(
                requireNonNull(probeSchema, "probeSchema is null"));
        probe.finish();
        HashJoinOperator join = new HashJoinOperator(
                requireNonNull(operatorResources, "operatorResources is null"),
                requireNonNull(allocator, "allocator is null"),
                probe,
                probeJoinColumns.clone(),
                requireNonNull(build, "build is null"),
                buildJoinColumns.clone(),
                probeOuterJoin,
                null,
                false,
                joinFilters.clone())
                .withOutputs(outputChannels.clone());
        HashJoinBuild prepared = join.prepareBuild();
        if (prepared == null) {
            join.close();
        }
        return Optional.ofNullable(prepared);
    }

    @Override
    public Schema outputSchema()
    {
        return outputRoot.outputSchema();
    }

    /// Immutable observation of this session's probe strategy; prepared-build siblings have separate counters.
    public HashJoinProbeStatistics probeStatistics()
    {
        return join.probeStatistics();
    }

    public HashJoinSession withOutputs(int... outputChannels)
    {
        checkAcceptingInput();
        if (outputRoot != join) {
            throw new IllegalStateException("hash join output pipeline is already configured");
        }
        join.withOutputs(outputChannels);
        return this;
    }

    /**
     * Composes a stateless output pipeline directly over native join batches.
     *
     * <p>The pipeline must preserve the externally scheduled input boundary: while a probe batch
     * is active, its {@link Operator#hasNext()} must remain true until the wrapped join has been
     * drained. Filter and projection pipelines satisfy this contract. This lets host integrations
     * evaluate residual predicates before materializing join payload or crossing an engine
     * boundary.
     */
    public HashJoinSession withOutputPipeline(UnaryOperator<Operator> outputPipeline)
    {
        checkAcceptingInput();
        if (outputRoot != join) {
            throw new IllegalStateException("hash join output pipeline is already configured");
        }
        outputRoot = requireNonNull(
                requireNonNull(outputPipeline, "outputPipeline is null").apply(join),
                "outputPipeline returned null");
        return this;
    }

    public HashJoinSession withOutputSingleMatch()
    {
        checkAcceptingInput();
        join.withOutputSingleMatch();
        return this;
    }

    /**
     * Transfers ownership of the next probe batch to this session.
     *
     * <p>The previous input must be fully drained first.
     */
    @Override
    public void addInput(Batch batch)
    {
        checkAcceptingInput();
        if (output != null || probe.hasInput()) {
            throw new IllegalStateException("previous probe input is not fully drained");
        }
        probe.addInput(requireNonNull(batch, "batch is null"));
    }

    /**
     * Returns whether a joined output batch is ready for the current probe input.
     */
    @Override
    public boolean hasOutput()
    {
        checkOpen();
        if (output != null) {
            return true;
        }
        if (!probe.hasInput() && !finishing) {
            return false;
        }

        while (outputRoot.hasNext()) {
            Batch candidate = outputRoot.next();
            if (!candidate.borrowMask().none()) {
                output = candidate;
                return true;
            }
            candidate.close();
            if (join.isWaitingForProbeInput()) {
                probe.releaseInput();
                return false;
            }
        }
        probe.releaseInput();
        return false;
    }

    /**
     * Returns the next joined batch after {@link #hasOutput()} reports one.
     *
     * <p>Ownership transfers to the caller.
     */
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
    public Batch getRetainedOutput()
    {
        return RetainedBatch.retain(allocator, getOutput(), outputRoot.outputCount());
    }

    /**
     * Signals that no more probe batches will be offered.
     */
    @Override
    public void finish()
    {
        checkOpen();
        finishing = true;
        probe.finish();
    }

    @Override
    public boolean isFinished()
    {
        checkOpen();
        if (!finishing || output != null || probe.hasInput()) {
            return false;
        }
        hasOutput();
        return !outputRoot.hasNext();
    }

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finishing) {
            throw new IllegalStateException("hash join session is finishing");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("hash join session is closed");
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
        outputRoot.close();
    }
}
