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

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.DONE;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.JUMP;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.MATCH_END;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.MATCH_LABEL;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.MATCH_START;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.SAVE;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.SPLIT;

final class TestPatternCompiler
{
    @Test
    void testCompilesStructuralPattern()
    {
        PatternProgram program = PatternCompiler.compile(new PatternExpression.Concatenation(List.of(
                new PatternExpression.Anchor(PatternExpression.Anchor.Type.PARTITION_START),
                new PatternExpression.Label(0),
                new PatternExpression.Exclusion(new PatternExpression.Label(1)),
                new PatternExpression.Anchor(PatternExpression.Anchor.Type.PARTITION_END))));

        assertThat(instructions(program))
                .containsExactly(MATCH_START, MATCH_LABEL, SAVE, MATCH_LABEL, SAVE, MATCH_END, DONE);
        assertThat(program.firstOperand(1)).isZero();
        assertThat(program.firstOperand(3)).isOne();
        assertThat(program.minimumSlotCount()).isEqualTo(2);
        assertThat(program.minimumLabelCount()).isEqualTo(2);
    }

    @Test
    void testCompilesPreferredAlternation()
    {
        PatternProgram program = PatternCompiler.compile(new PatternExpression.Alternation(List.of(
                new PatternExpression.Label(0),
                new PatternExpression.Label(1))));

        assertThat(instructions(program)).containsExactly(SPLIT, MATCH_LABEL, JUMP, MATCH_LABEL, DONE);
        assertThat(program.firstOperand(0)).isEqualTo(1);
        assertThat(program.secondOperand(0)).isEqualTo(3);
        assertThat(program.firstOperand(2)).isEqualTo(4);
    }

    @Test
    void testCompilesGreedyAndReluctantLoops()
    {
        PatternExpression label = new PatternExpression.Label(0);
        PatternProgram greedy = PatternCompiler.compile(new PatternExpression.Quantified(
                label,
                0,
                OptionalInt.empty(),
                true));
        PatternProgram reluctant = PatternCompiler.compile(new PatternExpression.Quantified(
                label,
                0,
                OptionalInt.empty(),
                false));

        assertThat(instructions(greedy)).containsExactly(SPLIT, MATCH_LABEL, SPLIT, DONE);
        assertThat(greedy.firstOperand(0)).isEqualTo(1);
        assertThat(greedy.secondOperand(0)).isEqualTo(3);
        assertThat(greedy.firstOperand(2)).isEqualTo(1);
        assertThat(greedy.secondOperand(2)).isEqualTo(3);

        assertThat(instructions(reluctant)).containsExactly(SPLIT, MATCH_LABEL, SPLIT, DONE);
        assertThat(reluctant.firstOperand(0)).isEqualTo(3);
        assertThat(reluctant.secondOperand(0)).isEqualTo(1);
        assertThat(reluctant.firstOperand(2)).isEqualTo(3);
        assertThat(reluctant.secondOperand(2)).isEqualTo(1);
    }

    @Test
    void testCompilesBoundedRangeAndPermutation()
    {
        PatternProgram range = PatternCompiler.compile(new PatternExpression.Quantified(
                new PatternExpression.Label(0),
                1,
                OptionalInt.of(3),
                true));
        assertThat(instructions(range)).containsExactly(MATCH_LABEL, SPLIT, MATCH_LABEL, SPLIT, MATCH_LABEL, DONE);
        assertThat(range.firstOperand(1)).isEqualTo(2);
        assertThat(range.secondOperand(1)).isEqualTo(5);
        assertThat(range.firstOperand(3)).isEqualTo(4);
        assertThat(range.secondOperand(3)).isEqualTo(5);

        PatternProgram permutation = PatternCompiler.compile(new PatternExpression.Permutation(List.of(
                new PatternExpression.Label(0),
                new PatternExpression.Label(1),
                new PatternExpression.Label(2))));
        assertThat(permutation.size()).isEqualTo(29);
        assertThat(permutation.minimumLabelCount()).isEqualTo(3);
    }

    @Test
    void testExpressionsAreImmutableAndValidated()
    {
        List<PatternExpression> elements = new ArrayList<>(List.of(
                new PatternExpression.Label(0),
                new PatternExpression.Label(1)));
        PatternExpression.Concatenation concatenation = new PatternExpression.Concatenation(elements);
        elements.clear();
        assertThat(concatenation.elements()).hasSize(2);

        assertThatThrownBy(() -> new PatternExpression.Label(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ordinal is negative");
        assertThatThrownBy(() -> new PatternExpression.Alternation(List.of(new PatternExpression.Label(0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("alternation requires at least two alternatives");
        assertThatThrownBy(() -> new PatternExpression.Quantified(
                new PatternExpression.Label(0),
                2,
                OptionalInt.of(1),
                true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maximum is less than minimum");
    }

    private static List<PatternProgram.Instruction> instructions(PatternProgram program)
    {
        List<PatternProgram.Instruction> instructions = new ArrayList<>(program.size());
        for (int pointer = 0; pointer < program.size(); pointer++) {
            instructions.add(program.instruction(pointer));
        }
        return instructions;
    }
}
