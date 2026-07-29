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

import static java.util.Objects.requireNonNull;

/**
 * Long-lived cross-join state for outer batches scheduled by an external host.
 *
 * <p>The inner operator is buffered once. Outer-batch ownership transfers to the
 * session and each batch is closed after all of its cross-product output is drained.
 */
public final class NestedLoopJoinSession
        implements AutoCloseable
{
    private final ExternallyScheduledBatchFeed outer;
    private final NestedLoopJoinOperator join;

    private Batch output;
    private boolean finishing;
    private boolean closed;

    public NestedLoopJoinSession(
            OperatorResources operatorResources,
            Allocator allocator,
            Schema outerSchema,
            Operator inner)
    {
        outer = new ExternallyScheduledBatchFeed(requireNonNull(outerSchema, "outerSchema is null"));
        join = new NestedLoopJoinOperator(
                requireNonNull(operatorResources, "operatorResources is null"),
                requireNonNull(allocator, "allocator is null"),
                outer,
                requireNonNull(inner, "inner is null"));
    }

    public Schema outputSchema()
    {
        return join.outputSchema();
    }

    public void addInput(Batch batch)
    {
        checkAcceptingInput();
        if (output != null || outer.hasInput()) {
            throw new IllegalStateException("previous outer input is not fully drained");
        }
        outer.addInput(requireNonNull(batch, "batch is null"));
    }

    public boolean hasOutput()
    {
        checkOpen();
        if (output != null) {
            return true;
        }
        if (!outer.hasInput() && !finishing) {
            return false;
        }

        while (join.hasNext()) {
            Batch candidate = join.next();
            if (!candidate.borrowMask().none()) {
                output = candidate;
                return true;
            }
            candidate.close();
            if (join.isWaitingForOuterInput()) {
                outer.releaseInput();
                return false;
            }
        }
        outer.releaseInput();
        return false;
    }

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

    public void finish()
    {
        checkOpen();
        finishing = true;
        outer.finish();
    }

    public boolean isFinished()
    {
        checkOpen();
        if (!finishing || output != null || outer.hasInput()) {
            return false;
        }
        hasOutput();
        return !join.hasNext();
    }

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finishing) {
            throw new IllegalStateException("nested loop join session is finishing");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("nested loop join session is closed");
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
}
