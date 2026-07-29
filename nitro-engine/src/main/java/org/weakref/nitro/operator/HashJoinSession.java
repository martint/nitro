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
import org.weakref.nitro.operator.source.ExternallyScheduledSource;

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
        implements AutoCloseable
{
    private final ProbeFeed probe;
    private final HashJoinOperator join;

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
        probe = new ProbeFeed(requireNonNull(probeSchema, "probeSchema is null"));
        join = new HashJoinOperator(
                requireNonNull(operatorResources, "operatorResources is null"),
                requireNonNull(allocator, "allocator is null"),
                probe,
                probeJoinColumns.clone(),
                requireNonNull(build, "build is null"),
                buildJoinColumns.clone(),
                probeOuterJoin,
                joinFilters.clone());
    }

    public Schema outputSchema()
    {
        return join.outputSchema();
    }

    public HashJoinSession withOutputs(int... outputChannels)
    {
        checkAcceptingInput();
        join.withOutputs(outputChannels);
        return this;
    }

    /**
     * Transfers ownership of the next probe batch to this session.
     *
     * <p>The previous input must be fully drained first.
     */
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
    public boolean hasOutput()
    {
        checkOpen();
        if (output != null) {
            return true;
        }
        if (!probe.hasInput() && !finishing) {
            return false;
        }

        while (join.hasNext()) {
            Batch candidate = join.next();
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

    /**
     * Signals that no more probe batches will be offered.
     */
    public void finish()
    {
        checkOpen();
        finishing = true;
        probe.finish();
    }

    public boolean isFinished()
    {
        checkOpen();
        if (!finishing || output != null || probe.hasInput()) {
            return false;
        }
        hasOutput();
        return !join.hasNext();
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
        join.close();
    }

    static final class ProbeFeed
            implements ExternallyScheduledSource
    {
        private final Schema schema;

        private Batch input;
        private boolean emitted;
        private boolean finished;
        private boolean closed;

        private ProbeFeed(Schema schema)
        {
            this.schema = schema;
        }

        private void addInput(Batch batch)
        {
            if (closed || finished) {
                throw new IllegalStateException("probe feed is closed");
            }
            if (input != null) {
                throw new IllegalStateException("probe feed already has input");
            }
            input = batch;
            emitted = false;
        }

        private boolean hasInput()
        {
            return input != null;
        }

        private void releaseInput()
        {
            input = null;
            emitted = false;
        }

        private void finish()
        {
            finished = true;
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public int outputCount()
        {
            return schema.size();
        }

        @Override
        public Schema outputSchema()
        {
            return schema;
        }

        @Override
        public boolean hasNext()
        {
            return !closed && input != null && !emitted;
        }

        @Override
        public Batch next()
        {
            if (!hasNext()) {
                throw new IllegalStateException("no probe input");
            }
            emitted = true;
            return input;
        }

        @Override
        public void constrain(Mask mask)
        {
            if (input == null) {
                throw new IllegalStateException("no probe input");
            }
            input.constrain(requireNonNull(mask, "mask is null"));
        }

        @Override
        public boolean supportsStableBatchBorrow()
        {
            return true;
        }

        @Override
        public boolean supportsOpenBatchHasNext()
        {
            return true;
        }

        @Override
        public boolean supportsConstrainedReborrow()
        {
            return true;
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (!emitted && input != null) {
                input.close();
            }
            input = null;
        }
    }
}
