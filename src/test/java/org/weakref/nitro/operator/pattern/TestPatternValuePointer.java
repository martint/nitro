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
import static org.weakref.nitro.operator.pattern.PatternNavigation.Scope.RUNNING;

final class TestPatternValuePointer
{
    @Test
    void testValidatesPhysicalInputColumn()
    {
        PatternNavigation navigation = new PatternNavigation(new int[0], FIRST, RUNNING, 0, 0);

        assertThat(new PatternValuePointer.Scalar(3, navigation).inputColumn()).isEqualTo(3);
        assertThat(new PatternValuePointer.Classifier(navigation).navigation()).isSameAs(navigation);
        assertThatThrownBy(() -> new PatternValuePointer.Scalar(-1, navigation))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
