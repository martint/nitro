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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.data.Row.row;

class TestBuildOuterJoinSession
{
    @Test
    void testStreamsMatchesAndEmitsEveryUnmatchedDuplicateBuildRow()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            ConstantTableOperator probe = new ConstantTableOperator(allocator, 1, List.of(row(2L)));
            try (BuildOuterJoinSession session = new BuildOuterJoinSession(
                    resources.operatorResources(),
                    allocator,
                    probe.outputSchema(),
                    new int[] {0},
                    new ConstantTableOperator(allocator, 1, List.of(row(2L), row(2L), row(3L), row(3L))),
                    new int[] {0},
                    new int[] {0, 1})) {
                List<Long> probeValues = new ArrayList<>();
                List<Boolean> probeNulls = new ArrayList<>();
                List<Long> buildValues = new ArrayList<>();

                session.addInput(probe.next());
                drain(session, probeValues, probeNulls, buildValues);
                session.finish();
                drain(session, probeValues, probeNulls, buildValues);

                assertThat(probeValues).containsExactly(2L, 2L, 0L, 0L);
                assertThat(probeNulls).containsExactly(false, false, true, true);
                assertThat(buildValues).containsExactly(2L, 2L, 3L, 3L);
                assertThat(session.isFinished()).isTrue();
            }
            probe.close();
        }
    }

    private static void drain(
            BuildOuterJoinSession session,
            List<Long> probeValues,
            List<Boolean> probeNulls,
            List<Long> buildValues)
    {
        while (session.hasOutput()) {
            try (Batch output = session.getOutput()) {
                var mask = output.borrowMask();
                var probes = VectorAccess.longValues(output.output(0).borrow(Stream.VALUES));
                var nulls = VectorAccess.booleanValues(output.output(0).borrowOrNull(Stream.NULLS));
                var builds = VectorAccess.longValues(output.output(1).borrow(Stream.VALUES));
                for (int index = 0; index < mask.count(); index++) {
                    int position = mask.position(index);
                    probeValues.add(probes.value(position));
                    probeNulls.add(nulls.value(position));
                    buildValues.add(builds.value(position));
                }
            }
        }
    }
}
