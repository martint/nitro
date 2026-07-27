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
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.SumStateVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.nio.charset.StandardCharsets;

import static java.lang.Math.toIntExact;

/**
 * SQL {@code sum(if(discriminator = literal, value, 0))} without requiring the conditional value vector to be
 * materialized. The declaration is representation-neutral and resolves the concrete vector shape once per batch.
 */
public final class ConditionalSum
        implements Accumulator
{
    public sealed interface Literal
            permits LongLiteral, BinaryLiteral {}

    public record LongLiteral(long value)
            implements Literal {}

    public record BinaryLiteral(byte[] value)
            implements Literal
    {
        public BinaryLiteral
        {
            value = value.clone();
        }
    }

    private final int discriminatorColumn;
    private final Literal literal;
    private final int valueColumn;

    public static ConditionalSum equalLong(int discriminatorColumn, long literal, int valueColumn)
    {
        return new ConditionalSum(discriminatorColumn, new LongLiteral(literal), valueColumn);
    }

    public static ConditionalSum equalUtf8(int discriminatorColumn, String literal, int valueColumn)
    {
        return new ConditionalSum(discriminatorColumn, new BinaryLiteral(literal.getBytes(StandardCharsets.UTF_8)), valueColumn);
    }

    private ConditionalSum(int discriminatorColumn, Literal literal, int valueColumn)
    {
        this.discriminatorColumn = discriminatorColumn;
        this.literal = literal;
        this.valueColumn = valueColumn;
    }

    int discriminatorColumn()
    {
        return discriminatorColumn;
    }

    Literal literal()
    {
        return literal;
    }

    int valueColumn()
    {
        return valueColumn;
    }

    @Override
    public Streams allocate(AggregationExecutionContext context, int size)
    {
        Allocator allocator = context.allocator();
        Allocator.Context allocationContext = context.allocationContext();
        return Streams.ofValues(allocator.allocate(allocationContext, SumStateVector.class, size, SumStateVector::new));
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
        return Streams.ofValues(grown);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        ((SumStateVector) state.values()).initialize(offset, length);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        SumStateVector sums = (SumStateVector) state.values();
        Inputs inputs = inputs(streams);
        for (int position : mask) {
            if (inputs.discriminatorNulls().value(position) || !matches(inputs.discriminator(), position)) {
                sums.increment(group, 0);
            }
            else if (!inputs.valueNulls().value(position)) {
                sums.increment(group, inputs.values().value(position));
            }
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        SumStateVector sums = (SumStateVector) state.values();
        long[] groupIds = ((I64Vector) groups).values();
        Inputs inputs = inputs(streams);
        for (int position : mask) {
            int group = toIntExact(groupIds[position]);
            if (inputs.discriminatorNulls().value(position) || !matches(inputs.discriminator(), position)) {
                sums.increment(group, 0);
            }
            else if (!inputs.valueNulls().value(position)) {
                sums.increment(group, inputs.values().value(position));
            }
        }
    }

    private Inputs inputs(StreamAccessor streams)
    {
        return new Inputs(
                streams.values(discriminatorColumn),
                VectorAccess.booleanValues(streams.stream(discriminatorColumn, Stream.NULLS)),
                VectorAccess.longValues(streams.values(valueColumn)),
                VectorAccess.booleanValues(streams.stream(valueColumn, Stream.NULLS)));
    }

    private boolean matches(Vector vector, int position)
    {
        return switch (vector) {
            case DictionaryVector dictionary -> matches(dictionary.values(), dictionary.ids()[position]);
            case RleVector rle -> matches(rle.values(), rle.runIndex(position));
            case I64Vector values when literal instanceof LongLiteral expected -> values.values()[position] == expected.value();
            case I32Vector values when literal instanceof LongLiteral expected -> values.values()[position] == expected.value();
            case BinaryVector values when literal instanceof BinaryLiteral expected -> binaryEquals(values, position, expected.value());
            default -> throw new IllegalArgumentException("Unsupported conditional discriminator/literal: " + vector.getClass().getSimpleName() + " / " + literal.getClass().getSimpleName());
        };
    }

    static boolean binaryEquals(BinaryVector vector, int position, byte[] expected)
    {
        int start = vector.startOffset(position);
        if (vector.length(position) != expected.length) {
            return false;
        }
        byte[] data = vector.data();
        for (int index = 0; index < expected.length; index++) {
            if (data[start + index] != expected[index]) {
                return false;
            }
        }
        return true;
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

    private record Inputs(Vector discriminator, VectorAccess.BooleanValues discriminatorNulls, VectorAccess.LongValues values, VectorAccess.BooleanValues valueNulls) {}
}
