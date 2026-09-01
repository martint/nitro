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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.RowPositionIndex;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.operator.pattern.PatternNavigation.Origin.FIRST;
import static org.weakref.nitro.operator.pattern.PatternNavigation.Scope.FINAL;

final class TestPatternScalarValueEvaluator
{
    @Test
    void testAppendsNavigatedAndMissingValues()
    {
        PatternEvaluationContext context = new PatternEvaluationContext(new LongRows());
        context.resetMatch(0, 3, 0, 7);
        context.resetRow(2, labels(0, 1, 0));
        PatternScalarValueEvaluator selected = evaluator(1);
        PatternScalarValueEvaluator missing = evaluator(2);

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context allocationContext = new Allocator.Context("pattern-scalar");
            Streams output = selected.append(context, allocator, allocationContext, Streams.empty(), 0, 2);
            output = missing.append(context, allocator, allocationContext, output, 1, 2);

            assertThat(((I64Vector) output.values()).values()).containsExactly(20, 30);
            assertThat(((BooleanVector) output.get(Stream.NULLS)).values()).containsExactly(false, true);
        }
    }

    private static PatternScalarValueEvaluator evaluator(int label)
    {
        PatternNavigation navigation = new PatternNavigation(new int[] {label}, FIRST, FINAL, 0, 0);
        return new PatternScalarValueEvaluator(new PatternValuePointer.Scalar(0, navigation));
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
        private final Streams values = Streams.ofValues(new I64Vector(new long[] {10, 20, 30}));

        @Override
        public int size()
        {
            return 3;
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
