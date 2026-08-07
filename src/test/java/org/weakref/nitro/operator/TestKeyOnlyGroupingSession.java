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
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestKeyOnlyGroupingSession
{
    @Test
    void testStreamsDistinctKeysAcrossInputBatchesAndRetainsNull()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        resources.operatorResources())) {
            allocator.beginExecution();

            try (Batch input = batch(new long[] {1, 2, 2, 0}, new boolean[] {false, false, false, true})) {
                session.addInput(input);
            }
            assertThat(session.hasOutput()).isTrue();
            try (Batch output = session.getOutput()) {
                assertThat(selectedValues(output)).containsExactly(1L, 2L, 0L);
                assertThat(selectedNulls(output)).containsExactly(false, false, true);
            }

            try (Batch input = batch(new long[] {2, 3, 0}, new boolean[] {false, false, true})) {
                session.addInput(input);
            }
            try (Batch output = session.getOutput()) {
                assertThat(selectedValues(output)).containsExactly(3L);
                assertThat(selectedNulls(output)).containsExactly(false);
            }

            try (Batch terminal = session.finish()) {
                assertThat(terminal.borrowMask().none()).isTrue();
                assertThat(terminal.output(0).borrow(Stream.VALUES).length()).isZero();
            }
        }
    }

    @Test
    void testProducesNoPendingOutputForDuplicateOnlyBatch()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        resources.operatorResources())) {
            allocator.beginExecution();
            try (Batch input = batch(new long[] {7}, new boolean[] {false})) {
                session.addInput(input);
            }
            try (Batch output = session.getOutput()) {
                assertThat(selectedValues(output)).containsExactly(7L);
            }
            try (Batch input = batch(new long[] {7, 7}, new boolean[] {false, false})) {
                session.addInput(input);
            }
            assertThat(session.hasOutput()).isFalse();
        }
    }

    private static Batch batch(long[] values, boolean[] nulls)
    {
        return new Batch(
                Mask.all(values.length),
                Output.of(Streams.ofValuesAndNulls(new I64Vector(values), new BooleanVector(nulls))));
    }

    private static List<Long> selectedValues(Batch batch)
    {
        long[] values = ((I64Vector) batch.output(0).borrow(Stream.VALUES)).values();
        List<Long> selected = new ArrayList<>();
        for (int position : batch.borrowMask()) {
            selected.add(values[position]);
        }
        return selected;
    }

    private static List<Boolean> selectedNulls(Batch batch)
    {
        boolean[] nulls = ((BooleanVector) batch.output(0).borrow(Stream.NULLS)).values();
        List<Boolean> selected = new ArrayList<>();
        for (int position : batch.borrowMask()) {
            selected.add(nulls[position]);
        }
        return selected;
    }
}
