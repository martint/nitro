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
package org.weakref.nitro.execution;

import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/// Lifecycle owner for a legacy pull-operator island.
///
/// The driver freezes allocator admission only after the complete plan has registered its
/// allocation domains. It also establishes the cooperative yield/cancel seam needed by a host
/// scheduler while preserving the existing pull data plane.
public final class OperatorExecutionDriver
        implements AutoCloseable
{
    private final Operator root;
    private final Allocator allocator;
    private final ExecutionContext context;
    private boolean begun;
    private boolean finished;
    private boolean rootClosed;
    private boolean closed;

    public OperatorExecutionDriver(Operator root, Allocator allocator, ExecutionContext context)
    {
        this.root = requireNonNull(root, "root is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.context = requireNonNull(context, "context is null");
    }

    public DriverResult processNext(Consumer<Batch> outputConsumer)
    {
        requireNonNull(outputConsumer, "outputConsumer is null");
        checkOpen();
        if (finished) {
            return DriverResult.FINISHED;
        }
        if (context.isCancelled()) {
            close();
            throw new IllegalStateException("execution is cancelled");
        }
        if (allocator.memoryBlocked().isPresent()) {
            return DriverResult.BLOCKED;
        }
        if (context.isYieldRequested()) {
            return DriverResult.YIELDED;
        }
        if (!begun) {
            allocator.beginExecution();
            begun = true;
        }
        if (!root.hasNext()) {
            finished = true;
            closeRoot();
            return DriverResult.FINISHED;
        }
        try (Batch batch = root.next()) {
            outputConsumer.accept(batch);
        }
        return DriverResult.OUTPUT;
    }

    public boolean isFinished()
    {
        return finished;
    }

    /// Host continuation for a [DriverResult#BLOCKED] result.
    public Optional<CompletionStage<Void>> blocked()
    {
        return allocator.memoryBlocked();
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            closeRoot();
        }
    }

    private void closeRoot()
    {
        if (!rootClosed) {
            rootClosed = true;
            root.close();
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("driver is closed");
        }
    }
}
