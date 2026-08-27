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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * A planner-selected physical aggregation unit that computes compatible conditional sums in one
 * discriminator traversal. Result slots have the same order as the supplied literals.
 */
public final class ConditionalSumsAggregationUnit
        implements PhysicalAggregationUnit
{
    private final int discriminatorColumn;
    private final int valueColumn;
    private final List<ConditionalSum.Literal> literals;

    public static ConditionalSumsAggregationUnit equalUtf8(int discriminatorColumn, int valueColumn, List<String> literals)
    {
        requireNonNull(literals, "literals is null");
        return new ConditionalSumsAggregationUnit(
                discriminatorColumn,
                valueColumn,
                literals.stream()
                        .map(value -> (ConditionalSum.Literal) new ConditionalSum.BinaryLiteral(value.getBytes(StandardCharsets.UTF_8)))
                        .toList());
    }

    public static ConditionalSumsAggregationUnit equalLong(int discriminatorColumn, int valueColumn, List<Long> literals)
    {
        requireNonNull(literals, "literals is null");
        return new ConditionalSumsAggregationUnit(
                discriminatorColumn,
                valueColumn,
                literals.stream()
                        .map(value -> (ConditionalSum.Literal) new ConditionalSum.LongLiteral(value))
                        .toList());
    }

    private ConditionalSumsAggregationUnit(int discriminatorColumn, int valueColumn, List<ConditionalSum.Literal> literals)
    {
        if (literals.isEmpty()) {
            throw new IllegalArgumentException("literals is empty");
        }
        this.discriminatorColumn = discriminatorColumn;
        this.valueColumn = valueColumn;
        this.literals = List.copyOf(literals);
    }

    @Override
    public int outputCount()
    {
        return literals.size();
    }

    @Override
    public Object allocate(AggregationExecutionContext context, int size)
    {
        SumStateVector[] sums = new SumStateVector[literals.size()];
        for (int index = 0; index < sums.length; index++) {
            sums[index] = context.allocator().allocate(
                    context.allocationContext(),
                    SumStateVector.class,
                    size,
                    SumStateVector::new);
        }
        return new State(context.allocator().primitiveArrays(), sums, new boolean[size]);
    }

    @Override
    public Object grow(Allocator allocator, Allocator.Context allocationContext, Object stateObject, int size)
    {
        State state = (State) stateObject;
        if (state.sums()[0].length() >= size) {
            return state;
        }
        SumStateVector[] sums = state.sums();
        for (int index = 0; index < sums.length; index++) {
            SumStateVector current = sums[index];
            sums[index] = allocator.adopt(allocationContext, SumStateVector.grow(current, size));
            allocator.discard(allocationContext, current);
        }
        return new State(state.arrayPool(), sums, Arrays.copyOf(state.fullyInitialized(), size));
    }

    @Override
    public void initialize(Object stateObject, int offset, int length)
    {
        State state = (State) stateObject;
        for (SumStateVector sum : state.sums()) {
            sum.initialize(offset, length);
        }
        Arrays.fill(state.fullyInitialized(), offset, offset + length, false);
    }

    @Override
    public void accumulate(Object stateObject, int group, Mask mask, StreamAccessor streams)
    {
        State state = (State) stateObject;
        try (OrdinalResolver resolver = OrdinalResolver.create(streams.values(discriminatorColumn), literals, state.arrayPool())) {
            Inputs inputs = inputs(streams);
            for (int position : mask) {
                int matched = inputs.discriminatorNullFree() || !inputs.discriminatorNulls().value(position) ? resolver.ordinal(position) : -1;
                accumulate(state, group, matched, inputs.values(), inputs.valueNulls(), position);
            }
        }
    }

    @Override
    public void accumulate(Object stateObject, Vector groups, Mask mask, StreamAccessor streams)
    {
        State state = (State) stateObject;
        long[] groupIds = ((I64Vector) groups).values();
        try (OrdinalResolver resolver = OrdinalResolver.create(streams.values(discriminatorColumn), literals, state.arrayPool())) {
            Inputs inputs = inputs(streams);
            for (int position : mask) {
                int matched = inputs.discriminatorNullFree() || !inputs.discriminatorNulls().value(position) ? resolver.ordinal(position) : -1;
                accumulate(state, toIntExact(groupIds[position]), matched, inputs.values(), inputs.valueNulls(), position);
            }
        }
    }

    private Inputs inputs(StreamAccessor streams)
    {
        Vector discriminatorNullVector = streams.stream(discriminatorColumn, Stream.NULLS);
        boolean discriminatorNullFree = VectorAccess.isAllFalseNulls(discriminatorNullVector);
        return new Inputs(
                discriminatorNullFree,
                discriminatorNullFree ? null : VectorAccess.booleanValues(discriminatorNullVector),
                VectorAccess.longValues(streams.values(valueColumn)),
                VectorAccess.booleanValues(streams.stream(valueColumn, Stream.NULLS)));
    }

    private static void accumulate(
            State state,
            int group,
            int matched,
            VectorAccess.LongValues values,
            VectorAccess.BooleanValues nulls,
            int position)
    {
        SumStateVector[] sums = state.sums();
        if (!state.fullyInitialized()[group]) {
            boolean allInitialized = true;
            for (int bucket = 0; bucket < sums.length; bucket++) {
                if (bucket != matched && sums[bucket].isNull(group)) {
                    sums[bucket].increment(group, 0);
                }
                allInitialized &= !sums[bucket].isNull(group);
            }
            state.fullyInitialized()[group] = allInitialized;
        }
        if (matched >= 0 && !nulls.value(position)) {
            sums[matched].increment(group, values.value(position));
        }
    }

    @Override
    public Streams result(int output, int maxGroup, Object stateObject, Streams existing, Allocator allocator, Allocator.Context allocationContext)
    {
        State state = (State) stateObject;
        if (output < 0 || output >= state.sums().length) {
            throw new IndexOutOfBoundsException("conditional sum result: " + output);
        }
        SumStateVector sum = state.sums()[output];
        int visibleCount = Math.max(maxGroup + 1, 0);
        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                existing == null ? null : (I64Vector) existing.values(),
                I64Vector.class,
                visibleCount,
                I64Vector::new);
        sum.copySumsTo(values, visibleCount);
        if (!sum.hasAnyNull()) {
            BooleanVector sentinel = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
            return Streams.ofValues(values).with(Stream.NULLS, allocator.allocateSingleRunRle(allocationContext, visibleCount, sentinel));
        }
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                existing == null ? null : existing.getOrNull(Stream.NULLS),
                visibleCount);
        sum.copyNullsTo(nulls, visibleCount);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private record State(PrimitiveArrayPool arrayPool, SumStateVector[] sums, boolean[] fullyInitialized) {}

    private record Inputs(
            boolean discriminatorNullFree,
            VectorAccess.BooleanValues discriminatorNulls,
            VectorAccess.LongValues values,
            VectorAccess.BooleanValues valueNulls) {}

    interface OrdinalResolver
            extends AutoCloseable
    {
        int ordinal(int position);

        @Override
        default void close() {}

        static OrdinalResolver create(Vector values, List<ConditionalSum.Literal> literals, PrimitiveArrayPool arrayPool)
        {
            return switch (values) {
                case DictionaryVector dictionary -> {
                    OrdinalResolver dictionaryResolver = create(dictionary.values(), literals, arrayPool);
                    int[] ids = dictionary.ids();
                    yield new OrdinalResolver()
                    {
                        @Override
                        public int ordinal(int position)
                        {
                            return dictionaryResolver.ordinal(ids[position]);
                        }

                        @Override
                        public void close()
                        {
                            dictionaryResolver.close();
                        }
                    };
                }
                case RleVector rle -> {
                    OrdinalResolver runResolver = create(rle.values(), literals, arrayPool);
                    yield new OrdinalResolver()
                    {
                        @Override
                        public int ordinal(int position)
                        {
                            return runResolver.ordinal(rle.runIndex(position));
                        }

                        @Override
                        public void close()
                        {
                            runResolver.close();
                        }
                    };
                }
                case BinaryVector binary -> lookup(binary, literals, arrayPool);
                case I64Vector longs -> lookup(longs.values(), literals, arrayPool);
                case I32Vector ints -> lookup(ints.values(), literals, arrayPool);
                default -> throw new IllegalArgumentException("Unsupported conditional discriminator: " + values.getClass().getSimpleName());
            };
        }

        private static OrdinalResolver lookup(BinaryVector values, List<ConditionalSum.Literal> literals, PrimitiveArrayPool arrayPool)
        {
            int[] ordinals = arrayPool.borrowInts(values.length());
            for (int position = 0; position < values.length(); position++) {
                int ordinal = -1;
                for (int candidate = 0; candidate < literals.size(); candidate++) {
                    if (literals.get(candidate) instanceof ConditionalSum.BinaryLiteral binary &&
                            ConditionalSum.binaryEquals(values, position, binary.value())) {
                        ordinal = candidate;
                        break;
                    }
                }
                ordinals[position] = ordinal;
            }
            return pooledLookup(ordinals, arrayPool);
        }

        private static OrdinalResolver lookup(long[] values, List<ConditionalSum.Literal> literals, PrimitiveArrayPool arrayPool)
        {
            int[] ordinals = arrayPool.borrowInts(values.length);
            for (int position = 0; position < values.length; position++) {
                ordinals[position] = longOrdinal(values[position], literals);
            }
            return pooledLookup(ordinals, arrayPool);
        }

        private static OrdinalResolver lookup(int[] values, List<ConditionalSum.Literal> literals, PrimitiveArrayPool arrayPool)
        {
            int[] ordinals = arrayPool.borrowInts(values.length);
            for (int position = 0; position < values.length; position++) {
                ordinals[position] = longOrdinal(values[position], literals);
            }
            return pooledLookup(ordinals, arrayPool);
        }

        private static int longOrdinal(long value, List<ConditionalSum.Literal> literals)
        {
            for (int candidate = 0; candidate < literals.size(); candidate++) {
                if (literals.get(candidate) instanceof ConditionalSum.LongLiteral literal && literal.value() == value) {
                    return candidate;
                }
            }
            return -1;
        }

        private static OrdinalResolver pooledLookup(int[] ordinals, PrimitiveArrayPool arrayPool)
        {
            return new OrdinalResolver()
            {
                @Override
                public int ordinal(int position)
                {
                    return ordinals[position];
                }

                @Override
                public void close()
                {
                    arrayPool.release(ordinals);
                }
            };
        }
    }
}
