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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.SemiJoinOperator.MatchOutputSemantics;

import java.util.Optional;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/**
 * Long-lived marking semi-join state for probe batches scheduled by an external host.
 *
 * <p>The build operator is consumed once, on the first probe. Probe-batch ownership transfers to
 * the session and each batch is closed as soon as its marked output is closed. Membership state and
 * reusable buffers survive across every offered probe batch.
 */
public final class SemiJoinSession
        implements JoinSession
{
    private final ExternallyScheduledBatchFeed probe;
    private final Operator root;

    private Batch output;
    private boolean finishing;
    private boolean closed;

    public SemiJoinSession(
            OperatorResources operatorResources,
            Allocator allocator,
            Schema probeSchema,
            int probeJoinColumn,
            Operator build,
            int buildJoinColumn,
            boolean includeMatches,
            Field matchField,
            MatchOutputSemantics matchOutputSemantics)
    {
        this(
                operatorResources,
                allocator,
                probeSchema,
                probeJoinColumn,
                build,
                buildJoinColumn,
                includeMatches,
                matchField,
                matchOutputSemantics,
                null,
                UnaryOperator.identity());
    }

    public SemiJoinSession(
            OperatorResources operatorResources,
            Allocator allocator,
            Schema probeSchema,
            int probeJoinColumn,
            Operator build,
            int buildJoinColumn,
            boolean includeMatches,
            Field matchField,
            MatchOutputSemantics matchOutputSemantics,
            UnaryOperator<Operator> outputPipeline)
    {
        this(
                operatorResources,
                allocator,
                probeSchema,
                probeJoinColumn,
                build,
                buildJoinColumn,
                includeMatches,
                matchField,
                matchOutputSemantics,
                null,
                outputPipeline);
    }

    public SemiJoinSession(
            OperatorResources operatorResources,
            Allocator allocator,
            Schema probeSchema,
            int probeJoinColumn,
            Operator build,
            int buildJoinColumn,
            boolean includeMatches,
            Field matchField,
            MatchOutputSemantics matchOutputSemantics,
            SemiJoinBuild preparedBuild)
    {
        this(
                operatorResources,
                allocator,
                probeSchema,
                probeJoinColumn,
                build,
                buildJoinColumn,
                includeMatches,
                matchField,
                matchOutputSemantics,
                requireNonNull(preparedBuild, "preparedBuild is null"),
                UnaryOperator.identity());
    }

    public SemiJoinSession(
            OperatorResources operatorResources,
            Allocator allocator,
            Schema probeSchema,
            int probeJoinColumn,
            Operator build,
            int buildJoinColumn,
            boolean includeMatches,
            Field matchField,
            MatchOutputSemantics matchOutputSemantics,
            SemiJoinBuild preparedBuild,
            UnaryOperator<Operator> outputPipeline)
    {
        probe = new ExternallyScheduledBatchFeed(requireNonNull(probeSchema, "probeSchema is null"));
        SemiJoinOperator semiJoin = preparedBuild == null
                ? new SemiJoinOperator(
                        requireNonNull(allocator, "allocator is null"),
                        probe,
                        probeJoinColumn,
                        requireNonNull(build, "build is null"),
                        buildJoinColumn,
                        includeMatches,
                        requireNonNull(matchField, "matchField is null"),
                        requireNonNull(matchOutputSemantics, "matchOutputSemantics is null"),
                        requireNonNull(operatorResources, "operatorResources is null"))
                : new SemiJoinOperator(
                        requireNonNull(allocator, "allocator is null"),
                        probe,
                        probeJoinColumn,
                        requireNonNull(build, "build is null"),
                        buildJoinColumn,
                        includeMatches,
                        requireNonNull(matchField, "matchField is null"),
                        requireNonNull(matchOutputSemantics, "matchOutputSemantics is null"),
                        requireNonNull(operatorResources, "operatorResources is null"),
                        preparedBuild);
        try {
            root = requireNonNull(
                    requireNonNull(outputPipeline, "outputPipeline is null").apply(semiJoin),
                    "outputPipeline returned null");
        }
        catch (RuntimeException | Error failure) {
            semiJoin.close();
            throw failure;
        }
    }

    public static Optional<SemiJoinBuild> prepareBuild(
            OperatorResources operatorResources,
            Allocator allocator,
            Schema probeSchema,
            int probeJoinColumn,
            Operator build,
            int buildJoinColumn,
            boolean includeMatches,
            Field matchField,
            MatchOutputSemantics matchOutputSemantics)
    {
        ExternallyScheduledBatchFeed probe = new ExternallyScheduledBatchFeed(
                requireNonNull(probeSchema, "probeSchema is null"));
        probe.finish();
        SemiJoinOperator semiJoin = new SemiJoinOperator(
                requireNonNull(allocator, "allocator is null"),
                probe,
                probeJoinColumn,
                requireNonNull(build, "build is null"),
                buildJoinColumn,
                includeMatches,
                requireNonNull(matchField, "matchField is null"),
                requireNonNull(matchOutputSemantics, "matchOutputSemantics is null"),
                requireNonNull(operatorResources, "operatorResources is null"));
        SemiJoinBuild prepared = semiJoin.prepareBuild();
        if (prepared == null) {
            semiJoin.close();
        }
        return Optional.ofNullable(prepared);
    }

    @Override
    public Schema outputSchema()
    {
        return root.outputSchema();
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

    @Override
    public boolean hasOutput()
    {
        checkOpen();
        if (output != null) {
            return true;
        }
        if (!probe.hasInput()) {
            return false;
        }
        if (root.hasNext()) {
            Batch candidate = root.next();
            if (!candidate.borrowMask().none()) {
                output = candidate;
                return true;
            }
            candidate.close();
        }
        probe.releaseInput();
        return false;
    }

    /**
     * Returns the marked batch after {@link #hasOutput()} reports one.
     *
     * <p>Ownership transfers to the caller.
     */
    @Override
    public Batch getOutput()
    {
        checkOpen();
        if (output == null) {
            throw new IllegalStateException("no semi-join output is ready");
        }
        Batch result = output;
        output = null;
        return result;
    }

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
        return !hasOutput();
    }

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finishing) {
            throw new IllegalStateException("semi-join session is finishing");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("semi-join session is closed");
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
        root.close();
    }
}
