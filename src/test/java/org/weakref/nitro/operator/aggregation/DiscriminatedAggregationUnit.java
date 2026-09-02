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
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Routes mutually exclusive discriminator values to independent physical aggregation units after
 * one encoding-aware discriminator traversal. A planner can use this for families such as
 * {@code sum(CASE WHEN key = literal THEN value END)} without materializing sparse conditional
 * vectors or teaching the execution operator about a particular aggregate.
 */
public final class DiscriminatedAggregationUnit
        implements PhysicalAggregationUnit
{
    private final int discriminatorColumn;
    private final List<ConditionalSum.Literal> literals;
    private final List<PhysicalAggregationUnit> delegates;

    public static DiscriminatedAggregationUnit equalUtf8(
            int discriminatorColumn,
            List<String> literals,
            List<? extends PhysicalAggregationUnit> delegates)
    {
        requireNonNull(literals, "literals is null");
        return new DiscriminatedAggregationUnit(
                discriminatorColumn,
                literals.stream()
                        .map(value -> (ConditionalSum.Literal) new ConditionalSum.BinaryLiteral(value.getBytes(StandardCharsets.UTF_8)))
                        .toList(),
                delegates);
    }

    public static DiscriminatedAggregationUnit equalLong(
            int discriminatorColumn,
            List<Long> literals,
            List<? extends PhysicalAggregationUnit> delegates)
    {
        requireNonNull(literals, "literals is null");
        return new DiscriminatedAggregationUnit(
                discriminatorColumn,
                literals.stream()
                        .map(value -> (ConditionalSum.Literal) new ConditionalSum.LongLiteral(value))
                        .toList(),
                delegates);
    }

    private DiscriminatedAggregationUnit(
            int discriminatorColumn,
            List<ConditionalSum.Literal> literals,
            List<? extends PhysicalAggregationUnit> delegates)
    {
        if (discriminatorColumn < 0) {
            throw new IllegalArgumentException("discriminatorColumn is negative");
        }
        this.discriminatorColumn = discriminatorColumn;
        this.literals = List.copyOf(requireNonNull(literals, "literals is null"));
        this.delegates = List.copyOf(requireNonNull(delegates, "delegates is null"));
        if (this.literals.isEmpty()) {
            throw new IllegalArgumentException("literals is empty");
        }
        if (this.literals.size() != this.delegates.size()) {
            throw new IllegalArgumentException("literals size does not match delegates size");
        }
        if (this.delegates.stream().anyMatch(delegate -> delegate.outputCount() != 1)) {
            throw new IllegalArgumentException("each delegate must expose exactly one result");
        }
        if (this.delegates.stream().anyMatch(delegate -> delegate.distinctInputColumns() != null || delegate.filterInputColumn() >= 0)) {
            throw new IllegalArgumentException("filtered or distinct delegates are not supported");
        }
    }

    @Override
    public int outputCount()
    {
        return delegates.size();
    }

    @Override
    public int stateCapacity(int requiredGroups, int defaultCapacity)
    {
        int capacity = defaultCapacity;
        for (PhysicalAggregationUnit delegate : delegates) {
            capacity = Math.max(capacity, delegate.stateCapacity(requiredGroups, defaultCapacity));
        }
        return capacity;
    }

    @Override
    public Object allocate(AggregationExecutionContext context, int size)
    {
        Object[] states = new Object[delegates.size()];
        Mask[] routes = new Mask[delegates.size()];
        for (int index = 0; index < delegates.size(); index++) {
            states[index] = delegates.get(index).allocate(context, size);
            routes[index] = Mask.all(0);
        }
        return new State(context.allocator().primitiveArrays(), states, routes);
    }

    @Override
    public Object grow(Allocator allocator, Allocator.Context allocationContext, Object stateObject, int size)
    {
        State state = (State) stateObject;
        for (int index = 0; index < delegates.size(); index++) {
            state.states()[index] = delegates.get(index).grow(allocator, allocationContext, state.states()[index], size);
        }
        return state;
    }

    @Override
    public void initialize(Object stateObject, int offset, int length)
    {
        State state = (State) stateObject;
        for (int index = 0; index < delegates.size(); index++) {
            delegates.get(index).initialize(state.states()[index], offset, length);
        }
    }

    @Override
    public void accumulate(Object stateObject, int group, Mask mask, StreamAccessor streams)
    {
        State state = (State) stateObject;
        route(state, mask, streams);
        for (int index = 0; index < delegates.size(); index++) {
            Mask route = state.routes()[index];
            if (!route.none()) {
                delegates.get(index).accumulate(state.states()[index], group, route, streams);
            }
        }
    }

    @Override
    public void accumulate(Object stateObject, Vector groups, Mask mask, StreamAccessor streams)
    {
        State state = (State) stateObject;
        route(state, mask, streams);
        for (int index = 0; index < delegates.size(); index++) {
            Mask route = state.routes()[index];
            if (!route.none()) {
                delegates.get(index).accumulate(state.states()[index], groups, route, streams);
            }
        }
    }

    private void route(State state, Mask mask, StreamAccessor streams)
    {
        int size = mask.size();
        int capacity = mask.selectedCount();
        int[] counts = state.arrayPool().borrowInts(delegates.size());
        Arrays.fill(counts, 0, delegates.size(), 0);
        try (ConditionalSumsAggregationUnit.OrdinalResolver resolver = ConditionalSumsAggregationUnit.OrdinalResolver.create(
                streams.values(discriminatorColumn),
                literals,
                state.arrayPool())) {
            Vector nullVector = streams.stream(discriminatorColumn, Stream.NULLS);
            boolean nullFree = VectorAccess.isAllFalseNulls(nullVector);
            VectorAccess.BooleanValues nulls = nullFree ? null : VectorAccess.booleanValues(nullVector);
            int[][] routePositions = new int[state.routes().length][];
            for (int index = 0; index < state.routes().length; index++) {
                Mask route = state.routes()[index];
                route.clear(size);
                routePositions[index] = route.positionsArrayForOverwrite(capacity);
            }
            for (int position : mask) {
                if (!nullFree && nulls.value(position)) {
                    continue;
                }
                int ordinal = resolver.ordinal(position);
                if (ordinal >= 0) {
                    routePositions[ordinal][counts[ordinal]++] = position;
                }
            }
            for (int index = 0; index < state.routes().length; index++) {
                state.routes()[index].finishRetain(counts[index]);
            }
        }
        finally {
            state.arrayPool().release(counts);
        }
    }

    @Override
    public Streams result(int output, int maxGroup, Object stateObject, Streams existing, Allocator allocator, Allocator.Context allocationContext)
    {
        State state = (State) stateObject;
        if (output < 0 || output >= delegates.size()) {
            throw new IndexOutOfBoundsException("discriminated aggregation result: " + output);
        }
        return delegates.get(output).result(0, maxGroup, state.states()[output], existing, allocator, allocationContext);
    }

    private record State(org.weakref.nitro.data.PrimitiveArrayPool arrayPool, Object[] states, Mask[] routes) {}
}
