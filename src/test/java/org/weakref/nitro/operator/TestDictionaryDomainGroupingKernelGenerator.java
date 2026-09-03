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
package org.weakref.nitro.operator;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.function.aggregation.ContributionCarrier;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdateTarget;

import java.util.List;
import java.util.Optional;

import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.BINARY_REGION;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.LONG;

class TestDictionaryDomainGroupingKernelGenerator
{
    @Test
    void testGeneratedKernelUsesPhysicalValuesAndExactFrequencies()
            throws ReflectiveOperationException
    {
        GroupedAggregationUpdateTarget target = new GroupedAggregationUpdateTarget(
                lookup().findVirtual(State.class, "update", methodType(void.class, int.class, long.class, byte[].class, int.class, int.class)),
                Optional.of(lookup().findVirtual(State.class, "updateRepeated", methodType(void.class, int.class, long.class, byte[].class, int.class, int.class, int.class))));
        GroupedAggregationUpdate update = GroupedAggregationUpdate.inputs(
                List.of(
                        new GroupedAggregationUpdate.InputValue(0, LONG),
                        new GroupedAggregationUpdate.InputValue(1, BINARY_REGION)),
                target);

        try (DictionaryDomainGroupingKernelGenerator generator = new DictionaryDomainGroupingKernelGenerator()) {
            DictionaryDomainGroupingKernel kernel = generator.create(
                    List.of(update),
                    new boolean[] {true, false},
                    new ContributionCarrier[] {LONG, BINARY_REGION},
                    new boolean[] {false, false},
                    new boolean[] {false, false},
                    new boolean[] {false, false});
            State state = new State(3);
            kernel.accumulate(
                    3,
                    new int[] {4, 0, 2},
                    new int[] {2, 0, 1},
                    new Object[] {new int[] {5, 100, 7}, new byte[] {10, 11, 20, 30, 31, 32}},
                    new int[][] {null, new int[] {0, 2, 3, 6}},
                    new int[2],
                    new boolean[2][],
                    new int[2],
                    new Object[] {state});

            assertThat(state.values).containsExactly(0, (7 + 30 + 3) * 2, (5 + 10 + 2) * 4);
            assertThat(state.calls).isEqualTo(2);
        }
    }

    @Test
    void testGeneratedKernelElidesNullDomainValues()
            throws ReflectiveOperationException
    {
        GroupedAggregationUpdateTarget target = new GroupedAggregationUpdateTarget(
                lookup().findVirtual(LongState.class, "update", methodType(void.class, int.class, long.class)),
                Optional.of(lookup().findVirtual(LongState.class, "updateRepeated", methodType(void.class, int.class, long.class, int.class))));
        GroupedAggregationUpdate update = GroupedAggregationUpdate.inputValue(0, target);

        try (DictionaryDomainGroupingKernelGenerator generator = new DictionaryDomainGroupingKernelGenerator()) {
            DictionaryDomainGroupingKernel kernel = generator.create(
                    List.of(update),
                    new boolean[] {false},
                    new ContributionCarrier[] {LONG},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false});
            LongState state = new LongState(2);
            kernel.accumulate(
                    2,
                    new int[] {3, 5},
                    new int[] {0, 1},
                    new Object[] {new long[] {9, 11}},
                    new int[1][],
                    new int[1],
                    new boolean[][] {new boolean[] {false, true}},
                    new int[1],
                    new Object[] {state});

            assertThat(state.values).containsExactly(27, 0);
        }
    }

    public static final class State
    {
        private final long[] values;
        private int calls;

        public State(int groups)
        {
            values = new long[groups];
        }

        public void update(int group, long value, byte[] data, int offset, int length)
        {
            values[group] += value + data[offset] + length;
        }

        public void updateRepeated(int group, long value, byte[] data, int offset, int length, int frequency)
        {
            calls++;
            values[group] += (value + data[offset] + length) * frequency;
        }
    }

    public static final class LongState
    {
        private final long[] values;

        public LongState(int groups)
        {
            values = new long[groups];
        }

        public void update(int group, long value)
        {
            values[group] += value;
        }

        public void updateRepeated(int group, long value, int frequency)
        {
            values[group] += value * frequency;
        }
    }
}
