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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.operator.RowPositionIndex;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.operator.pattern.PatternNavigation.Origin.LAST;
import static org.weakref.nitro.operator.pattern.PatternNavigation.Scope.RUNNING;

final class TestPatternDefinitionEvaluator
{
    @Test
    void testDefinitionsReadRowsNavigationClassifierAndMatchNumber()
    {
        RowPositionIndex rows = new LongRows(3, 7, 8, 12);
        PatternNavigation previousFirstLabel = new PatternNavigation(new int[] {0}, LAST, RUNNING, 0, 0);
        PatternDefinition first = context -> context.rows().longValue(0, context.currentRow()) < 10;
        PatternDefinition second = context -> {
            int previous = context.resolvePosition(previousFirstLabel);
            return context.matchNumber() == 2 &&
                    previous == 1 &&
                    context.classifier(previousFirstLabel) == 0 &&
                    context.rows().longValue(0, previous) + context.rows().longValue(0, context.currentRow()) == 15;
        };
        PatternDefinitionEvaluator evaluator = new PatternDefinitionEvaluator(rows, List.of(first, second));
        evaluator.reset(1, 4, 1, 2);

        assertThat(evaluator.evaluate(0, 0, labels(0))).isTrue();
        assertThat(evaluator.evaluate(1, 1, labels(0, 1))).isTrue();
    }

    @Test
    void testRequiresInitializationAndKnownLabel()
    {
        PatternDefinitionEvaluator evaluator = new PatternDefinitionEvaluator(new LongRows(1), List.of(_ -> true));

        assertThatThrownBy(() -> evaluator.evaluate(0, 0, labels(0)))
                .isInstanceOf(IllegalStateException.class);
        evaluator.reset(0, 1, 0, 1);
        assertThatThrownBy(() -> evaluator.evaluate(1, 0, labels(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPositionsAcceptedMatchForMeasureEvaluation()
    {
        PatternDefinitionEvaluator evaluator = new PatternDefinitionEvaluator(new LongRows(3, 7, 8), List.of(_ -> true));

        PatternEvaluationContext context = evaluator.positionMatch(0, 3, 1, 4, 2, labels(0, 1));

        assertThat(context.partitionStart()).isZero();
        assertThat(context.partitionEnd()).isEqualTo(3);
        assertThat(context.patternStart()).isEqualTo(1);
        assertThat(context.matchNumber()).isEqualTo(4);
        assertThat(context.currentRow()).isEqualTo(2);
        assertThat(context.labels().labelAt(1)).isEqualTo(1);
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
        private final Streams values;

        private LongRows(long... values)
        {
            this.values = Streams.ofValues(new I64Vector(values));
        }

        @Override
        public int size()
        {
            return values.values().length();
        }

        @Override
        public Streams column(int column, int position)
        {
            if (column != 0 || position < 0 || position >= size()) {
                throw new IndexOutOfBoundsException();
            }
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
