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
import static org.weakref.nitro.operator.pattern.PatternOutputMode.ALL_OMIT_EMPTY;
import static org.weakref.nitro.operator.pattern.PatternOutputMode.ALL_SHOW_EMPTY;
import static org.weakref.nitro.operator.pattern.PatternOutputMode.ALL_WITH_UNMATCHED;
import static org.weakref.nitro.operator.pattern.PatternOutputMode.ONE;
import static org.weakref.nitro.operator.pattern.PatternOutputMode.WINDOW;

final class TestPatternOutputMode
{
    @Test
    void testOutputSemantics()
    {
        assertThat(ONE.isOneRow()).isTrue();
        assertThat(ONE.outputsEmptyMatches()).isTrue();
        assertThat(ONE.outputsUnmatchedRows()).isFalse();

        assertThat(ALL_SHOW_EMPTY.isOneRow()).isFalse();
        assertThat(ALL_SHOW_EMPTY.outputsEmptyMatches()).isTrue();
        assertThat(ALL_OMIT_EMPTY.outputsEmptyMatches()).isFalse();

        assertThat(ALL_WITH_UNMATCHED.outputsUnmatchedRows()).isTrue();
        assertThat(WINDOW.isOneRow()).isTrue();
        assertThat(WINDOW.outputsEmptyMatches()).isTrue();
        assertThat(WINDOW.outputsUnmatchedRows()).isTrue();
    }
}
