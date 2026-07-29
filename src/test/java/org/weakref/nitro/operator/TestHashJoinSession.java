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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestHashJoinSession
{
    @Test
    void testPreservesBuildIndexAcrossIndependentlyScheduledProbeBatches()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        Schema.unspecified(1),
                        new int[] {0},
                        table(2, 3, 2),
                        new int[] {0},
                        false)) {
            allocator.beginExecution();

            List<Long> probeValues = new ArrayList<>();
            List<Long> buildValues = new ArrayList<>();
            session.addInput(batch(1, 2));
            drain(session, probeValues, buildValues);
            session.addInput(batch(3, 2));
            drain(session, probeValues, buildValues);
            session.finish();
            drain(session, probeValues, buildValues);

            assertThat(probeValues).containsExactly(2L, 2L, 3L, 2L, 2L);
            assertThat(buildValues).containsExactly(2L, 2L, 3L, 2L, 2L);
            assertThat(session.isFinished()).isTrue();
            try (Batch late = batch(4)) {
                assertThatIllegalStateException()
                        .isThrownBy(() -> session.addInput(late))
                        .withMessage("hash join session is finishing");
            }
        }
    }

    private static void drain(HashJoinSession session, List<Long> probeValues, List<Long> buildValues)
    {
        while (session.hasOutput()) {
            try (Batch output = session.getOutput()) {
                Mask mask = output.borrowMask();
                VectorAccess.LongValues probe = VectorAccess.longValues(output.output(0).borrow(Stream.VALUES));
                VectorAccess.LongValues build = VectorAccess.longValues(output.output(1).borrow(Stream.VALUES));
                for (int position : mask) {
                    probeValues.add(probe.value(position));
                    buildValues.add(build.value(position));
                }
            }
        }
    }

    private static Batch batch(long... values)
    {
        return new Batch(
                Mask.all(values.length),
                Output.of(org.weakref.nitro.data.Streams.ofValues(new I64Vector(values))));
    }

    private static Operator table(long... values)
    {
        return new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(
                        values.length,
                        new org.weakref.nitro.data.Vector[] {new I64Vector(values)},
                        Mask.all(values.length))));
    }
}
