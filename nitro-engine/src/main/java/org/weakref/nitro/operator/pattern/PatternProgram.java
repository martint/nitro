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

import java.util.List;

import static java.util.Objects.requireNonNull;

/// Compact immutable instruction program for row-pattern matching.
///
/// The packed plan arrays are construction-time metadata, not execution buffers. Match-thread and capture storage is
/// allocated separately by the execution operator from its instance-owned allocator resources.
public final class PatternProgram
{
    private final byte[] instructions;
    private final int[] firstOperands;
    private final int[] secondOperands;
    private final int minimumSlotCount;
    private final int minimumLabelCount;

    PatternProgram(List<Instruction> instructions, List<Integer> firstOperands, List<Integer> secondOperands)
    {
        requireNonNull(instructions, "instructions is null");
        requireNonNull(firstOperands, "firstOperands is null");
        requireNonNull(secondOperands, "secondOperands is null");
        if (instructions.isEmpty() || instructions.size() != firstOperands.size() || instructions.size() != secondOperands.size()) {
            throw new IllegalArgumentException("instruction arrays have inconsistent sizes");
        }

        this.instructions = new byte[instructions.size()];
        this.firstOperands = new int[instructions.size()];
        this.secondOperands = new int[instructions.size()];
        int slots = 0;
        int labels = 0;
        for (int pointer = 0; pointer < instructions.size(); pointer++) {
            Instruction instruction = requireNonNull(instructions.get(pointer), "instruction is null");
            int first = firstOperands.get(pointer);
            int second = secondOperands.get(pointer);
            this.instructions[pointer] = (byte) instruction.ordinal();
            this.firstOperands[pointer] = first;
            this.secondOperands[pointer] = second;
            switch (instruction) {
                case JUMP -> checkTarget(first, instructions.size());
                case SPLIT -> {
                    checkTarget(first, instructions.size());
                    checkTarget(second, instructions.size());
                }
                case MATCH_LABEL -> {
                    if (first < 0) {
                        throw new IllegalArgumentException("label ordinal is negative");
                    }
                    labels = Math.max(labels, first + 1);
                }
                case SAVE -> slots++;
                case MATCH_START, MATCH_END, DONE -> {}
            }
        }
        if (instruction(instructions.size() - 1) != Instruction.DONE) {
            throw new IllegalArgumentException("program does not end with DONE");
        }
        this.minimumSlotCount = slots;
        this.minimumLabelCount = labels;
    }

    public int size()
    {
        return instructions.length;
    }

    public Instruction instruction(int pointer)
    {
        return switch (instructions[pointer]) {
            case 0 -> Instruction.JUMP;
            case 1 -> Instruction.MATCH_LABEL;
            case 2 -> Instruction.MATCH_START;
            case 3 -> Instruction.MATCH_END;
            case 4 -> Instruction.SAVE;
            case 5 -> Instruction.SPLIT;
            case 6 -> Instruction.DONE;
            default -> throw new IllegalStateException("unknown pattern instruction");
        };
    }

    public int firstOperand(int pointer)
    {
        return firstOperands[pointer];
    }

    public int secondOperand(int pointer)
    {
        return secondOperands[pointer];
    }

    public int minimumSlotCount()
    {
        return minimumSlotCount;
    }

    public int minimumLabelCount()
    {
        return minimumLabelCount;
    }

    private static void checkTarget(int target, int size)
    {
        if (target < 0 || target >= size) {
            throw new IllegalArgumentException("instruction target is out of bounds: " + target);
        }
    }

    public enum Instruction
    {
        JUMP,
        MATCH_LABEL,
        MATCH_START,
        MATCH_END,
        SAVE,
        SPLIT,
        DONE,
    }
}
