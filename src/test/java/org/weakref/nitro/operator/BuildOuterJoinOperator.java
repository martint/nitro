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
import org.weakref.nitro.execution.EngineResources;

import static java.util.Objects.requireNonNull;

/**
 * Pull-operator adapter for a streaming join that preserves every build row.
 */
public final class BuildOuterJoinOperator
        implements Operator
{
    private final Operator probe;
    private final BuildOuterJoinSession session;

    private Batch output;
    private boolean probeFinished;
    private boolean closed;

    public BuildOuterJoinOperator(
            Allocator allocator,
            Operator probe,
            int probeJoinColumn,
            Operator build,
            int buildJoinColumn,
            int... outputChannels)
    {
        this(
                EngineResources.from(allocator).operatorResources(),
                allocator,
                probe,
                new int[] {probeJoinColumn},
                build,
                new int[] {buildJoinColumn},
                outputChannels);
    }

    public BuildOuterJoinOperator(
            OperatorResources resources,
            Allocator allocator,
            Operator probe,
            int[] probeJoinColumns,
            Operator build,
            int[] buildJoinColumns,
            int[] outputChannels,
            HashJoinOperator.JoinFilter... joinFilters)
    {
        this.probe = requireNonNull(probe, "probe is null");
        session = new BuildOuterJoinSession(
                requireNonNull(resources, "resources is null"),
                requireNonNull(allocator, "allocator is null"),
                probe.outputSchema(),
                requireNonNull(probeJoinColumns, "probeJoinColumns is null"),
                requireNonNull(build, "build is null"),
                requireNonNull(buildJoinColumns, "buildJoinColumns is null"),
                requireNonNull(outputChannels, "outputChannels is null"),
                requireNonNull(joinFilters, "joinFilters is null"));
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
        checkOpen();
        if (output != null) {
            return true;
        }
        while (true) {
            if (session.hasOutput()) {
                output = session.getOutput();
                return true;
            }
            if (probeFinished) {
                if (session.isFinished()) {
                    return false;
                }
                continue;
            }
            if (probe.hasNext()) {
                session.addInput(probe.next());
            }
            else {
                probeFinished = true;
                session.finish();
            }
        }
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more batches");
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
        try {
            session.close();
        }
        finally {
            probe.close();
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("build outer join operator is closed");
        }
    }
}
