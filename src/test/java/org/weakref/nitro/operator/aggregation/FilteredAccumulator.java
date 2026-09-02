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
package org.weakref.nitro.operator.aggregation;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import static java.util.Objects.requireNonNull;

/**
 * Declarative SQL aggregate filter. The aggregation operator resolves {@link #filterInputColumn()}
 * into a batch mask before invoking this wrapper, while all state and result behavior remains owned
 * by the wrapped accumulator.
 */
public final class FilteredAccumulator
        implements Accumulator
{
    private final Accumulator delegate;
    private final int filterInputColumn;

    public FilteredAccumulator(Accumulator delegate, int filterInputColumn)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        if (filterInputColumn < 0) {
            throw new IllegalArgumentException("filterInputColumn is negative");
        }
        this.filterInputColumn = filterInputColumn;
    }

    @Override
    public int[] distinctInputColumns()
    {
        return delegate.distinctInputColumns();
    }

    @Override
    public int filterInputColumn()
    {
        return filterInputColumn;
    }

    @Override
    public Streams allocate(AggregationExecutionContext context, int size)
    {
        return delegate.allocate(context, size);
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        return delegate.grow(allocator, allocationContext, state, size);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        delegate.initialize(state, offset, length);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        delegate.accumulate(state, group, mask, streams);
    }

    @Override
    public void accumulateDistinctSelected(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        delegate.accumulateDistinctSelected(state, group, mask, streams);
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        delegate.accumulate(state, groups, mask, streams);
    }

    @Override
    public void accumulateDistinctSelected(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        delegate.accumulateDistinctSelected(state, groups, mask, streams);
    }

    @Override
    public Streams result(int maxGroup, Streams state, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        return delegate.result(maxGroup, state, mask, output, allocator, allocationContext);
    }

    @Override
    public Streams copyResultPosition(int group, int maxGroup, Streams state, Streams output, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        return delegate.copyResultPosition(group, maxGroup, state, output, outputPosition, size, allocator, allocationContext);
    }

    @Override
    public Streams copyResultRange(int groupStart, int groupCount, int maxGroup, Streams state, Streams output, int outputStart, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        return delegate.copyResultRange(groupStart, groupCount, maxGroup, state, output, outputStart, size, allocator, allocationContext);
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        return delegate.result(maxGroup, state, output, allocator, allocationContext);
    }
}
