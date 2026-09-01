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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.DONE;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.JUMP;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.MATCH_END;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.MATCH_LABEL;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.MATCH_START;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.SAVE;
import static org.weakref.nitro.operator.pattern.PatternProgram.Instruction.SPLIT;

public final class PatternCompiler
{
    private final List<PatternProgram.Instruction> instructions = new ArrayList<>();
    private final List<Integer> firstOperands = new ArrayList<>();
    private final List<Integer> secondOperands = new ArrayList<>();

    private PatternCompiler() {}

    public static PatternProgram compile(PatternExpression pattern)
    {
        PatternCompiler compiler = new PatternCompiler();
        compiler.emit(requireNonNull(pattern, "pattern is null"));
        compiler.add(DONE);
        return new PatternProgram(compiler.instructions, compiler.firstOperands, compiler.secondOperands);
    }

    private void emit(PatternExpression pattern)
    {
        switch (pattern) {
            case PatternExpression.Label label -> add(MATCH_LABEL, label.ordinal());
            case PatternExpression.Empty _ -> {}
            case PatternExpression.Anchor anchor -> add(switch (anchor.type()) {
                case PARTITION_START -> MATCH_START;
                case PARTITION_END -> MATCH_END;
            });
            case PatternExpression.Exclusion exclusion -> {
                add(SAVE);
                emit(exclusion.pattern());
                add(SAVE);
            }
            case PatternExpression.Alternation alternation -> emitAlternation(alternation.alternatives());
            case PatternExpression.Concatenation concatenation -> concatenation.elements().forEach(this::emit);
            case PatternExpression.Permutation permutation -> emitPermutations(permutation.elements());
            case PatternExpression.Quantified quantified -> emitQuantified(quantified);
        }
    }

    private void emitAlternation(List<PatternExpression> alternatives)
    {
        List<Integer> jumps = new ArrayList<>(alternatives.size() - 1);
        for (int index = 0; index < alternatives.size() - 1; index++) {
            int split = addPlaceholder();
            int preferred = size();
            emit(alternatives.get(index));
            jumps.add(addPlaceholder());
            set(split, SPLIT, preferred, size());
        }
        emit(alternatives.getLast());
        jumps.forEach(jump -> set(jump, JUMP, size()));
    }

    private void emitPermutations(List<PatternExpression> elements)
    {
        int[] indexes = new int[elements.size()];
        Arrays.setAll(indexes, index -> index);
        List<PatternExpression> alternatives = new ArrayList<>();
        do {
            List<PatternExpression> permutation = new ArrayList<>(elements.size());
            for (int index : indexes) {
                permutation.add(elements.get(index));
            }
            alternatives.add(new PatternExpression.Concatenation(permutation));
        }
        while (nextPermutation(indexes));
        emitAlternation(alternatives);
    }

    private static boolean nextPermutation(int[] values)
    {
        int left = values.length - 2;
        while (left >= 0 && values[left] >= values[left + 1]) {
            left--;
        }
        if (left < 0) {
            return false;
        }
        int right = values.length - 1;
        while (values[right] <= values[left]) {
            right--;
        }
        swap(values, left, right);
        for (int start = left + 1, end = values.length - 1; start < end; start++, end--) {
            swap(values, start, end);
        }
        return true;
    }

    private static void swap(int[] values, int left, int right)
    {
        int value = values[left];
        values[left] = values[right];
        values[right] = value;
    }

    private void emitQuantified(PatternExpression.Quantified quantified)
    {
        if (quantified.maximum().isPresent()) {
            emitRange(quantified.pattern(), quantified.minimum(), quantified.maximum().getAsInt(), quantified.greedy());
            return;
        }
        emitLoop(quantified.pattern(), quantified.minimum(), quantified.greedy());
    }

    private void emitLoop(PatternExpression pattern, int minimum, boolean greedy)
    {
        if (minimum == 0) {
            int entry = addPlaceholder();
            int body = size();
            emit(pattern);
            add(SPLIT, greedy ? body : size() + 1, greedy ? size() + 1 : body);
            set(entry, SPLIT, greedy ? body : size(), greedy ? size() : body);
            return;
        }

        int loop = size();
        for (int repetition = 0; repetition < minimum; repetition++) {
            loop = size();
            emit(pattern);
        }
        add(SPLIT, greedy ? loop : size() + 1, greedy ? size() + 1 : loop);
    }

    private void emitRange(PatternExpression pattern, int minimum, int maximum, boolean greedy)
    {
        for (int repetition = 0; repetition < minimum; repetition++) {
            emit(pattern);
        }
        if (minimum == maximum) {
            return;
        }

        List<Integer> splits = new ArrayList<>(maximum - minimum);
        List<Integer> bodies = new ArrayList<>(maximum - minimum);
        for (int repetition = minimum; repetition < maximum; repetition++) {
            splits.add(addPlaceholder());
            bodies.add(size());
            emit(pattern);
        }
        for (int index = 0; index < splits.size(); index++) {
            set(splits.get(index), SPLIT, greedy ? bodies.get(index) : size(), greedy ? size() : bodies.get(index));
        }
    }

    private int addPlaceholder()
    {
        instructions.add(null);
        firstOperands.add(0);
        secondOperands.add(0);
        return size() - 1;
    }

    private void add(PatternProgram.Instruction instruction)
    {
        add(instruction, 0, 0);
    }

    private void add(PatternProgram.Instruction instruction, int first)
    {
        add(instruction, first, 0);
    }

    private void add(PatternProgram.Instruction instruction, int first, int second)
    {
        instructions.add(instruction);
        firstOperands.add(first);
        secondOperands.add(second);
    }

    private void set(int pointer, PatternProgram.Instruction instruction, int first)
    {
        set(pointer, instruction, first, 0);
    }

    private void set(int pointer, PatternProgram.Instruction instruction, int first, int second)
    {
        instructions.set(pointer, instruction);
        firstOperands.set(pointer, first);
        secondOperands.set(pointer, second);
    }

    private int size()
    {
        return instructions.size();
    }
}
