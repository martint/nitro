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

import static java.util.Objects.requireNonNull;

/** Test-harness model of a single-destination native exchange boundary. */
public final class SqlExchangeOperator
        implements Operator
{
    private final Allocator allocator;
    private final Operator source;
    private Batch pending;
    private boolean closed;

    public SqlExchangeOperator(Allocator allocator, Operator source)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.source = requireNonNull(source, "source is null");
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return source.outputSchema();
    }

    @Override
    public boolean hasNext()
    {
        if (pending == null && source.hasNext()) {
            try (Batch input = source.next()) {
                pending = copy(input);
            }
        }
        return pending != null;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more batches");
        }
        Batch result = pending;
        pending = null;
        return result;
    }

    @Override
    public void constrain(Mask mask)
    {
        if (pending != null) {
            pending.constrain(mask);
        }
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        source.pushDynamicFilter(filter);
    }

    @Override
    public boolean supportsDynamicFilterPushdown(int column)
    {
        return source.supportsDynamicFilterPushdown(column);
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public boolean supportsStableBatchBorrow()
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
        if (pending != null) {
            pending.close();
            pending = null;
        }
        source.close();
    }

    private Batch copy(Batch input)
    {
        Allocator.Context allocationContext = new Allocator.Context("sql-exchange");
        try {
            Mask inputMask = input.borrowMask();
            int[] positions = new int[inputMask.count()];
            int index = 0;
            for (int position : inputMask) {
                positions[index++] = position;
            }

            Output[] outputs = new Output[source.outputCount()];
            for (int channel = 0; channel < outputs.length; channel++) {
                Output inputOutput = input.output(channel);
                Streams.Builder streams = Streams.builder();
                for (Stream stream : inputOutput.streams()) {
                    streams.put(stream, inputOutput.borrow(stream, inputMask));
                }
                Streams copied = allocator.copyStreams(allocationContext, streams.build(), positions);
                outputs[channel] = Output.of(copied);
            }
            return new Batch(
                    Mask.all(positions.length),
                    _ -> {},
                    java.util.function.Function.identity(),
                    _ -> {},
                    () -> allocator.releaseIfPresent(allocationContext),
                    outputs);
        }
        catch (RuntimeException | Error failure) {
            allocator.releaseIfPresent(allocationContext);
            throw failure;
        }
    }
}
