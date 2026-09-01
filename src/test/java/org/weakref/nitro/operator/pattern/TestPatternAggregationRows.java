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

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.operator.pattern.PatternAggregationSet.Scope.FINAL;
import static org.weakref.nitro.operator.pattern.PatternAggregationSet.Scope.RUNNING;

final class TestPatternAggregationRows
{
    @Test
    void testRunningAndFinalLabelDomains()
    {
        PatternEvaluationContext context = context(labels(0, 1, 0, 2));
        PatternAggregationRows rows = new PatternAggregationRows();

        rows.reset(context, new PatternAggregationSet(new int[] {0}, RUNNING));
        assertThat(positions(rows)).containsExactly(10, 12);

        rows.reset(context, new PatternAggregationSet(new int[] {2}, FINAL));
        assertThat(positions(rows)).containsExactly(13);

        rows.reset(context, new PatternAggregationSet(new int[0], RUNNING));
        assertThat(positions(rows)).containsExactly(10, 11, 12);
    }

    @Test
    void testNormalizesLabelSet()
    {
        int[] ordinals = {2, 0, 2};
        PatternAggregationSet set = new PatternAggregationSet(ordinals, FINAL);
        ordinals[0] = 9;
        assertThat(set.labelOrdinals()).containsExactly(0, 2);
    }

    private static PatternEvaluationContext context(PatternLabelEvaluator.LabelHistory labels)
    {
        PatternEvaluationContext context = new PatternEvaluationContext(new LongRows(14));
        context.resetMatch(0, 14, 10, 1);
        context.resetRow(12, labels);
        return context;
    }

    private static int[] positions(PatternAggregationRows rows)
    {
        int[] positions = new int[4];
        int count = 0;
        while (rows.advance()) {
            positions[count++] = rows.position();
        }
        return Arrays.copyOf(positions, count);
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

        private LongRows(int size)
        {
            values = Streams.ofValues(new I64Vector(new long[size]));
        }

        @Override
        public int size()
        {
            return values.values().length();
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
