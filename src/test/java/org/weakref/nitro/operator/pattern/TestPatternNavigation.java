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
import static org.weakref.nitro.operator.pattern.PatternNavigation.Scope.RUNNING;

final class TestPatternNavigation
{
    private static final PatternLabelEvaluator.LabelHistory LABELS = labels(0, 1, 0, 2, 1);

    @Test
    void testLogicalLabelNavigation()
    {
        assertThat(new PatternNavigation(new int[] {0}, FIRST, FINAL, 1, 0)
                .resolvePosition(14, LABELS, 0, 20, 10))
                .isEqualTo(12);
        assertThat(new PatternNavigation(new int[] {1}, LAST, FINAL, 1, 0)
                .resolvePosition(14, LABELS, 0, 20, 10))
                .isEqualTo(11);
        assertThat(new PatternNavigation(new int[] {2, 0, 2}, LAST, FINAL, 0, 0)
                .resolvePosition(14, LABELS, 0, 20, 10))
                .isEqualTo(13);
    }

    @Test
    void testUniversalAndPhysicalNavigation()
    {
        assertThat(new PatternNavigation(new int[0], FIRST, FINAL, 2, -2)
                .resolvePosition(14, LABELS, 0, 20, 10))
                .isEqualTo(10);
        assertThat(new PatternNavigation(new int[] {2}, LAST, FINAL, 0, 7)
                .resolvePosition(14, LABELS, 0, 20, 10))
                .isEqualTo(-1);
    }

    @Test
    void testRunningScopeDoesNotSeeFutureLabels()
    {
        PatternNavigation running = new PatternNavigation(new int[] {1}, LAST, RUNNING, 0, 0);
        PatternNavigation complete = new PatternNavigation(new int[] {1}, LAST, FINAL, 0, 0);

        assertThat(running.resolvePosition(12, LABELS, 0, 20, 10)).isEqualTo(11);
        assertThat(complete.resolvePosition(12, LABELS, 0, 20, 10)).isEqualTo(14);
    }

    @Test
    void testValidationAndImmutability()
    {
        int[] ordinals = {2, 1, 2};
        PatternNavigation navigation = new PatternNavigation(ordinals, FIRST, FINAL, 0, 0);
        ordinals[0] = 9;
        assertThat(navigation.labelOrdinals()).containsExactly(1, 2);

        assertThatThrownBy(() -> new PatternNavigation(new int[] {-1}, FIRST, FINAL, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("label ordinal is negative");
        assertThatThrownBy(() -> navigation.resolvePosition(9, LABELS, 0, 20, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("currentRow is outside the match");
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
