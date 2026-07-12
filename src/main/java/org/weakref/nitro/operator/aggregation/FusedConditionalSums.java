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
import org.weakref.nitro.data.SumStateVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.List;

import static java.lang.Math.toIntExact;

/** One discriminator scan for a set of compatible {@link ConditionalSum} declarations. */
final class FusedConditionalSums
{
    private static final boolean FULLY_INITIALIZED_FAST_PATH =
            Boolean.parseBoolean(System.getProperty("nitro.aggregate.conditionalSumsInitializedFastPath", "true"));

    private FusedConditionalSums() {}

    static void fuse(List<Accumulator> output, List<Integer> indexes)
    {
        ConditionalSum first = (ConditionalSum) output.get(indexes.getFirst());
        SharedState shared = new SharedState(first.discriminatorColumn(), first.valueColumn(), indexes.size());
        for (int ordinal = 0; ordinal < indexes.size(); ordinal++) {
            int index = indexes.get(ordinal);
            ConditionalSum declaration = (ConditionalSum) output.get(index);
            shared.literals[ordinal] = declaration.literal();
            output.set(index, ordinal == 0 ? new Scanner(shared, ordinal) : new Follower(shared, ordinal));
        }
    }

    private static final class SharedState
    {
        private final int discriminatorColumn;
        private final int valueColumn;
        private final ConditionalSum.Literal[] literals;
        private final SumStateVector[] sums;
        private boolean[] fullyInitialized;

        private SharedState(int discriminatorColumn, int valueColumn, int count)
        {
            this.discriminatorColumn = discriminatorColumn;
            this.valueColumn = valueColumn;
            this.literals = new ConditionalSum.Literal[count];
            this.sums = new SumStateVector[count];
        }
    }

    private abstract static class Base
            implements Accumulator
    {
        final SharedState shared;
        final int ordinal;

        Base(SharedState shared, int ordinal)
        {
            this.shared = shared;
            this.ordinal = ordinal;
        }

        @Override
        public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
        {
            Streams state = Streams.ofValues(allocator.allocate(allocationContext, SumStateVector.class, size, SumStateVector::new));
            shared.sums[ordinal] = (SumStateVector) state.values();
            if (shared.fullyInitialized == null) {
                shared.fullyInitialized = new boolean[size];
            }
            return state;
        }

        @Override
        public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
        {
            SumStateVector current = (SumStateVector) state.values();
            if (current.length() >= size) {
                return state;
            }
            SumStateVector grown = allocator.adopt(allocationContext, SumStateVector.grow(current, size));
            allocator.discard(allocationContext, current);
            Streams result = Streams.ofValues(grown);
            shared.sums[ordinal] = grown;
            if (shared.fullyInitialized.length < size) {
                shared.fullyInitialized = Arrays.copyOf(shared.fullyInitialized, size);
            }
            return result;
        }

        @Override
        public void initialize(Streams state, int offset, int length)
        {
            ((SumStateVector) state.values()).initialize(offset, length);
            if (ordinal == 0) {
                Arrays.fill(shared.fullyInitialized, offset, offset + length, false);
            }
        }

        @Override
        public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
        {
            SumStateVector stateVector = (SumStateVector) state.values();
            int visibleCount = Math.max(maxGroup + 1, 0);
            I64Vector values = allocator.allocateOrGrow(
                    allocationContext,
                    output == null ? null : (I64Vector) output.values(),
                    I64Vector.class,
                    visibleCount,
                    I64Vector::new);
            stateVector.copySumsTo(values, visibleCount);
            if (!stateVector.hasAnyNull()) {
                BooleanVector sentinel = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
                return Streams.ofValues(values).with(Stream.NULLS, allocator.allocateSingleRunRle(allocationContext, visibleCount, sentinel));
            }
            BooleanVector nulls = VectorAccess.writableBooleanVector(
                    allocator,
                    allocationContext,
                    output == null ? null : output.getOrNull(Stream.NULLS),
                    visibleCount);
            stateVector.copyNullsTo(nulls, visibleCount);
            return Streams.ofValuesAndNulls(values, nulls);
        }
    }

    private static final class Scanner
            extends Base
    {
        Scanner(SharedState shared, int ordinal)
        {
            super(shared, ordinal);
        }

        @Override
        public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
        {
            try (OrdinalResolver resolver = OrdinalResolver.create(streams.values(shared.discriminatorColumn), shared.literals)) {
                Vector discriminatorNullVector = streams.stream(shared.discriminatorColumn, Stream.NULLS);
                boolean discriminatorNullFree = VectorAccess.isAllFalseNulls(discriminatorNullVector);
                VectorAccess.BooleanValues discriminatorNulls = discriminatorNullFree ? null : VectorAccess.booleanValues(discriminatorNullVector);
                VectorAccess.LongValues values = VectorAccess.longValues(streams.values(shared.valueColumn));
                VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(streams.stream(shared.valueColumn, Stream.NULLS));
                SumStateVector[] sums = shared.sums;
                for (int position : mask) {
                    int matched = discriminatorNullFree || !discriminatorNulls.value(position) ? resolver.ordinal(position) : -1;
                    accumulate(shared, sums, group, matched, values, nulls, position);
                }
            }
        }

        @Override
        public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
        {
            try (OrdinalResolver resolver = OrdinalResolver.create(streams.values(shared.discriminatorColumn), shared.literals)) {
                Vector discriminatorNullVector = streams.stream(shared.discriminatorColumn, Stream.NULLS);
                boolean discriminatorNullFree = VectorAccess.isAllFalseNulls(discriminatorNullVector);
                VectorAccess.BooleanValues discriminatorNulls = discriminatorNullFree ? null : VectorAccess.booleanValues(discriminatorNullVector);
                VectorAccess.LongValues values = VectorAccess.longValues(streams.values(shared.valueColumn));
                VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(streams.stream(shared.valueColumn, Stream.NULLS));
                long[] groupIds = ((I64Vector) groups).values();
                SumStateVector[] sums = shared.sums;
                for (int position : mask) {
                    int matched = discriminatorNullFree || !discriminatorNulls.value(position) ? resolver.ordinal(position) : -1;
                    accumulate(shared, sums, toIntExact(groupIds[position]), matched, values, nulls, position);
                }
            }
        }

        private static void accumulate(SharedState shared, SumStateVector[] sums, int group, int matched, VectorAccess.LongValues values, VectorAccess.BooleanValues nulls, int position)
        {
            // A false CASE branch contributes non-null zero. Initialize only still-null buckets; after each group has
            // observed two or more discriminator values this loop becomes checks only, while the matched state gets
            // the sole arithmetic update. A matched NULL value remains NULL until a false or non-null row occurs.
            if (!FULLY_INITIALIZED_FAST_PATH || !shared.fullyInitialized[group]) {
                boolean allInitialized = true;
                for (int bucket = 0; bucket < sums.length; bucket++) {
                    if (bucket != matched && sums[bucket].isNull(group)) {
                        sums[bucket].increment(group, 0);
                    }
                    allInitialized &= !sums[bucket].isNull(group);
                }
                shared.fullyInitialized[group] = allInitialized;
            }
            if (matched >= 0 && !nulls.value(position)) {
                sums[matched].increment(group, values.value(position));
            }
        }
    }

    private static final class Follower
            extends Base
    {
        Follower(SharedState shared, int ordinal)
        {
            super(shared, ordinal);
        }

        @Override
        public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams) {}

        @Override
        public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams) {}
    }

    private interface OrdinalResolver
            extends AutoCloseable
    {
        int ordinal(int position);

        @Override
        default void close() {}

        static OrdinalResolver create(Vector values, ConditionalSum.Literal[] literals)
        {
            return switch (values) {
                case DictionaryVector dictionary -> {
                    OrdinalResolver dictionaryResolver = create(dictionary.values(), literals);
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
                    OrdinalResolver runResolver = create(rle.values(), literals);
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
                case BinaryVector binary -> lookup(binary, literals);
                case I64Vector longs -> lookup(longs.values(), literals);
                case I32Vector ints -> lookup(ints.values(), literals);
                default -> throw new IllegalArgumentException("Unsupported conditional discriminator: " + values.getClass().getSimpleName());
            };
        }

        private static OrdinalResolver lookup(BinaryVector values, ConditionalSum.Literal[] literals)
        {
            int[] ordinals = PrimitiveArrayPool.shared().borrowInts(values.length());
            for (int position = 0; position < values.length(); position++) {
                int ordinal = -1;
                for (int candidate = 0; candidate < literals.length; candidate++) {
                    if (literals[candidate] instanceof ConditionalSum.BinaryLiteral binary && ConditionalSum.binaryEquals(values, position, binary.value())) {
                        ordinal = candidate;
                        break;
                    }
                }
                ordinals[position] = ordinal;
            }
            return pooledLookup(ordinals);
        }

        private static OrdinalResolver lookup(long[] values, ConditionalSum.Literal[] literals)
        {
            int[] ordinals = PrimitiveArrayPool.shared().borrowInts(values.length);
            for (int position = 0; position < values.length; position++) {
                ordinals[position] = longOrdinal(values[position], literals);
            }
            return pooledLookup(ordinals);
        }

        private static OrdinalResolver lookup(int[] values, ConditionalSum.Literal[] literals)
        {
            int[] ordinals = PrimitiveArrayPool.shared().borrowInts(values.length);
            for (int position = 0; position < values.length; position++) {
                ordinals[position] = longOrdinal(values[position], literals);
            }
            return pooledLookup(ordinals);
        }

        private static int longOrdinal(long value, ConditionalSum.Literal[] literals)
        {
            for (int candidate = 0; candidate < literals.length; candidate++) {
                if (literals[candidate] instanceof ConditionalSum.LongLiteral literal && literal.value() == value) {
                    return candidate;
                }
            }
            return -1;
        }

        private static OrdinalResolver pooledLookup(int[] ordinals)
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
                    PrimitiveArrayPool.shared().release(ordinals);
                }
            };
        }
    }
}
