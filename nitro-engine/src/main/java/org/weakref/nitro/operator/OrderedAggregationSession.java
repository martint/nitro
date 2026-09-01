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

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.operator.BatchAggregationSession.InputOwnership.CALLER;

/// Blocking input-order adapter for an aggregation session.
///
/// Selected rows from independently scheduled host batches are copied into allocator-owned sort
/// state. At end of input, one globally ordered Nitro batch is synchronously consumed by the
/// delegate. The adapter therefore never retains a caller-owned input batch.
public final class OrderedAggregationSession
        implements BatchAggregationSession
{
    private final BatchAggregationSession delegate;
    private final SortSession sort;

    private long inputBytes;
    private boolean finished;
    private boolean closed;

    public OrderedAggregationSession(
            Allocator allocator,
            Schema inputSchema,
            PhysicalOrdering ordering,
            OperatorResources resources,
            BatchAggregationSession delegate)
    {
        requireNonNull(ordering, "ordering is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        sort = new SortSession(
                requireNonNull(allocator, "allocator is null"),
                ordering.columns(),
                ordering.descending(),
                ordering.nullsFirst(),
                requireNonNull(inputSchema, "inputSchema is null"),
                requireNonNull(resources, "resources is null"));
    }

    @Override
    public Schema outputSchema()
    {
        return delegate.outputSchema();
    }

    @Override
    public void addInput(Batch batch)
    {
        addInput(batch, 0);
    }

    @Override
    public void addInput(Batch batch, long inputBytes)
    {
        checkAcceptingInput();
        if (inputBytes < 0) {
            throw new IllegalArgumentException("inputBytes is negative");
        }
        sort.addInput(requireNonNull(batch, "batch is null"));
        this.inputBytes = Math.addExact(this.inputBytes, inputBytes);
    }

    @Override
    public InputOwnership addInputWithOwnership(Batch batch, long inputBytes)
    {
        addInput(batch, inputBytes);
        return CALLER;
    }

    @Override
    public long retainedBytes()
    {
        return Math.addExact(sort.retainedBytes(), delegate.retainedBytes());
    }

    @Override
    public AggregationPhaseMetrics phaseMetrics()
    {
        return delegate.phaseMetrics();
    }

    @Override
    public Batch finish()
    {
        return finishOutput().orElseThrow(() -> new IllegalStateException("ordered aggregation produced no output"));
    }

    @Override
    public Optional<Batch> finishOutput()
    {
        checkAcceptingInput();
        finished = true;
        Optional<Batch> sorted = sort.finish();
        if (sorted.isPresent()) {
            Batch batch = sorted.orElseThrow();
            boolean callerOwned = true;
            try {
                callerOwned = delegate.addInputWithOwnership(batch, inputBytes) == CALLER;
                if (!callerOwned) {
                    throw new IllegalStateException("Ordered aggregation delegate retained its sorted input");
                }
            }
            finally {
                if (callerOwned) {
                    batch.close();
                }
            }
        }
        return requireNonNull(delegate.finishOutput(), "aggregation returned null final output");
    }

    private void checkAcceptingInput()
    {
        if (closed) {
            throw new IllegalStateException("ordered aggregation session is closed");
        }
        if (finished) {
            throw new IllegalStateException("ordered aggregation session is finished");
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            delegate.close();
        }
        finally {
            sort.close();
        }
    }
}
