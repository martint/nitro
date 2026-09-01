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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.operator.pattern.PatternNavigation.Origin.FIRST;
import static org.weakref.nitro.operator.pattern.PatternNavigation.Origin.LAST;
import static org.weakref.nitro.operator.pattern.PatternNavigation.Scope.FINAL;
import static org.weakref.nitro.operator.pattern.PatternSkipPolicy.Fixed.NEXT;
import static org.weakref.nitro.operator.pattern.PatternSkipPolicy.Fixed.PAST_LAST;

final class TestPatternSkipPolicy
{
    private static final PatternLabelEvaluator.LabelHistory LABELS = labels(0, 1, 0, 2);

    @Test
    void testFixedPositionsPreserveSearchStartDistinction()
    {
        assertThat(NEXT.nextStart(10, 8, 30, 12, LABELS)).isEqualTo(11);
        assertThat(PAST_LAST.nextStart(10, 8, 30, 12, LABELS)).isEqualTo(16);
    }

    @Test
    void testLabelPosition()
    {
        assertThat(new PatternSkipPolicy.ToLabel(new PatternNavigation(new int[] {0}, LAST, FINAL, 0, 0))
                .nextStart(10, 8, 30, 12, LABELS))
                .isEqualTo(14);
        assertThat(new PatternSkipPolicy.ToLabel(new PatternNavigation(new int[] {2}, FIRST, FINAL, 0, 0))
                .nextStart(10, 8, 30, 12, LABELS))
                .isEqualTo(15);
    }

    @Test
    void testRejectsInvalidLabelTargets()
    {
        assertThatThrownBy(() -> new PatternSkipPolicy.ToLabel(new PatternNavigation(new int[] {3}, FIRST, FINAL, 0, 0))
                .nextStart(10, 8, 30, 12, LABELS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("AFTER MATCH SKIP target is absent");
        assertThatThrownBy(() -> new PatternSkipPolicy.ToLabel(new PatternNavigation(new int[] {0}, FIRST, FINAL, 0, 0))
                .nextStart(10, 8, 30, 12, LABELS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("AFTER MATCH SKIP cannot target the first row of the match");
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
}
