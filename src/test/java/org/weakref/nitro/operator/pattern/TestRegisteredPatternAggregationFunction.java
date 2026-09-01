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
package org.weakref.nitro.operator.pattern;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.function.aggregation.AggregationExecution;
import org.weakref.nitro.core.function.aggregation.AggregationImplementation;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.function.aggregation.AggregationPositionAccumulator;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.RowPositionIndex;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.operator.pattern.PatternAggregationSet.Scope.FINAL;

final class TestRegisteredPatternAggregationFunction
{
    @Test
    void testDirectAndMaskFallbackPositionUpdates()
    {
        PatternEvaluationContext context = context();
        PatternAggregationSet allRows = new PatternAggregationSet(new int[0], FINAL);
        PatternAggregationValueEvaluator direct = new PatternAggregationValueEvaluator(
                allRows,
                new RegisteredPatternAggregationFunction(new SumImplementation(true), new LongInput()));
        PatternAggregationValueEvaluator fallback = new PatternAggregationValueEvaluator(
                allRows,
                new RegisteredPatternAggregationFunction(new SumImplementation(false), new LongInput()));

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context allocationContext = new Allocator.Context("registered-pattern-aggregation");
            Streams output = direct.append(context, allocator, allocationContext, Streams.empty(), 0, 2);
            output = fallback.append(context, allocator, allocationContext, output, 1, 2);

            assertThat(((I64Vector) output.values()).values()).containsExactly(100, 100);
        }
    }

    private static PatternEvaluationContext context()
    {
        PatternEvaluationContext context = new PatternEvaluationContext(new LongRows());
        context.resetMatch(0, 4, 0, 1);
        context.resetRow(3, labels(0, 1, 0, 1));
        return context;
    }

    private static PatternLabelEvaluator.LabelHistory labels(int... labels)
    {
        return new PatternLabelEvaluator.LabelHistory()
        {
            @Override
            public int size()
            {
                return labels.length;
            }

            @Override
            public int labelAt(int position)
            {
                return labels[position];
            }
        };
    }

    private static final class LongInput
            implements PatternAggregationInput
    {
        private final Schema schema = Schema.unspecified(1);
        private final Streams values = Streams.ofValues(new I64Vector(new long[] {10, 20, 30, 40}));
        private int position;

        @Override
        public Schema schema()
        {
            return schema;
        }

        @Override
        public void reset(PatternEvaluationContext context, int position, int labelOrdinal)
        {
            this.position = position;
        }

        @Override
        public int physicalPosition()
        {
            return position;
        }

        @Override
        public int physicalSize()
        {
            return values.values().length();
        }

        @Override
        public Vector stream(int input, Stream stream)
        {
            return values.getOrNull(stream);
        }
    }

    private static final class SumImplementation
            implements AggregationImplementation
    {
        private final boolean direct;

        private SumImplementation(boolean direct)
        {
            this.direct = direct;
        }

        @Override
        public Object allocate(AggregationExecution execution, int groups)
        {
            return new long[groups];
        }

        @Override
        public Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initialize(Object state, int offset, int length)
        {
            ((long[]) state)[0] = 0;
        }

        @Override
        public void addRawInput(Object state, int group, Mask mask, AggregationInput input)
        {
            VectorAccess.LongValues values = VectorAccess.longValues(input.stream(0, Stream.VALUES));
            for (int position : mask) {
                ((long[]) state)[group] += values.value(position);
            }
        }

        @Override
        public void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggregationPositionAccumulator bindRawInputPosition(Object state, int group, AggregationInput input)
        {
            if (!direct) {
                return null;
            }
            VectorAccess.LongValues values = VectorAccess.longValues(input.stream(0, Stream.VALUES));
            return position -> ((long[]) state)[group] += values.value(position);
        }

        @Override
        public void addIntermediate(Object state, int group, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Streams intermediate(int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context allocationContext)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Streams result(int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context allocationContext)
        {
            I64Vector output = allocator.allocate(allocationContext, I64Vector.class, 1, I64Vector::new);
            output.values()[0] = ((long[]) state)[0];
            return Streams.ofValues(output);
        }
    }

    private static final class LongRows
            implements RowPositionIndex
    {
        @Override
        public int size()
        {
            return 4;
        }

        @Override
        public Streams column(int column, int position)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int sourcePosition(int position)
        {
            return position;
        }

        @Override
        public boolean sharesSource(int leftPosition, int rightPosition)
        {
            return true;
        }
    }
}
