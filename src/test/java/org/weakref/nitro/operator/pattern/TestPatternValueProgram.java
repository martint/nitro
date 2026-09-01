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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.RowPositionIndex;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestPatternValueProgram
{
    @Test
    void testAppendsIntoReusableCallerOwnedColumns()
    {
        PatternEvaluationContext context = new PatternEvaluationContext(new LongRows());
        context.resetMatch(0, 1, 0, 7);
        context.resetRow(0, labels(3));
        PatternValueProgram program = new PatternValueProgram(List.of(
                (match, _, _, output, position, size) -> append(output, position, size, match.matchNumber()),
                (match, _, _, output, position, size) -> append(output, position, size, match.classifier(navigation()))));

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context allocationContext = new Allocator.Context("pattern-values");
            Streams[] output = program.append(context, allocator, allocationContext, null, 1, 3);
            Streams[] reused = program.append(context, allocator, allocationContext, output, 2, 3);

            assertThat(reused).isSameAs(output);
            assertThat(((I64Vector) output[0].values()).values()).containsExactly(0, 7, 7);
            assertThat(((I64Vector) output[1].values()).values()).containsExactly(0, 3, 3);
        }
    }

    @Test
    void testClosesEveryValueEvaluator()
    {
        AtomicInteger closes = new AtomicInteger();
        PatternValueProgram program = new PatternValueProgram(List.of(
                closingEvaluator(closes, false),
                closingEvaluator(closes, true),
                closingEvaluator(closes, false)));

        assertThatThrownBy(program::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("close failure");
        assertThat(closes).hasValue(3);
    }

    private static PatternValueEvaluator closingEvaluator(AtomicInteger closes, boolean fail)
    {
        return new PatternValueEvaluator()
        {
            @Override
            public Streams append(
                    PatternEvaluationContext context,
                    Allocator allocator,
                    Allocator.Context allocationContext,
                    Streams output,
                    int outputPosition,
                    int outputSize)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close()
            {
                closes.incrementAndGet();
                if (fail) {
                    throw new IllegalStateException("close failure");
                }
            }
        };
    }

    private static Streams append(Streams output, int position, int size, long value)
    {
        I64Vector vector = output.getOrNull(Stream.VALUES) instanceof I64Vector existing && existing.length() == size
                ? existing
                : new I64Vector(new long[size]);
        vector.values()[position] = value;
        return Streams.ofValues(vector);
    }

    private static PatternNavigation navigation()
    {
        return new PatternNavigation(
                new int[0],
                PatternNavigation.Origin.LAST,
                PatternNavigation.Scope.FINAL,
                0,
                0);
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

    private static final class LongRows
            implements RowPositionIndex
    {
        private final Streams values = Streams.ofValues(new I64Vector(new long[1]));

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public Streams column(int column, int position)
        {
            return values;
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
