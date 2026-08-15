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

class TestSharedBuildOuterJoin
{
    @Test
    void testSharesMatchesAcrossParallelProbeSessionsAndEmitsUnmatchedOnce()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator buildAllocator = new Allocator(resources);
                Allocator firstProbeAllocator = new Allocator(resources);
                Allocator secondProbeAllocator = new Allocator(resources);
                Allocator outputAllocator = new Allocator(resources)) {
            buildAllocator.beginExecution();
            firstProbeAllocator.beginExecution();
            secondProbeAllocator.beginExecution();
            outputAllocator.beginExecution();

            ConstantTableOperator firstProbe = new ConstantTableOperator(firstProbeAllocator, 1, List.of(row(2L)));
            ConstantTableOperator secondProbe = new ConstantTableOperator(secondProbeAllocator, 1, List.of(row(3L)));
            try (SharedBuildOuterJoin shared = SharedBuildOuterJoin.prepare(
                            resources.operatorResources(),
                            buildAllocator,
                            firstProbe.outputSchema(),
                            new int[] {0},
                            new ConstantTableOperator(buildAllocator, 1, List.of(row(2L), row(2L), row(3L), row(4L))),
                            new int[] {0},
                            false,
                            new int[] {0, 1})
                    .orElseThrow();
                    JoinSession first = shared.newProbeSession(resources.operatorResources(), firstProbeAllocator);
                    JoinSession second = shared.newProbeSession(resources.operatorResources(), secondProbeAllocator);
                    Operator unmatched = shared.newUnmatchedBuildOperator(outputAllocator)) {
                List<Long> matchedBuild = new ArrayList<>();
                first.addInput(firstProbe.next());
                first.finish();
                drainMatches(first, matchedBuild);
                second.addInput(secondProbe.next());
                second.finish();
                drainMatches(second, matchedBuild);

                assertThat(matchedBuild).containsExactly(2L, 2L, 3L);
                assertThat(readBuildValues(unmatched)).containsExactly(4L);
            }
            firstProbe.close();
            secondProbe.close();
        }
    }

    private static void drainMatches(JoinSession session, List<Long> buildValues)
    {
        while (session.hasOutput()) {
            try (Batch output = session.getOutput()) {
                VectorAccess.LongValues values = VectorAccess.longValues(output.output(1).borrow(Stream.VALUES));
                for (int position : output.borrowMask()) {
                    buildValues.add(values.value(position));
                }
            }
        }
        assertThat(session.isFinished()).isTrue();
    }

    private static List<Long> readBuildValues(Operator operator)
    {
        List<Long> values = new ArrayList<>();
        while (operator.hasNext()) {
            try (Batch output = operator.next()) {
                VectorAccess.LongValues build = VectorAccess.longValues(output.output(1).borrow(Stream.VALUES));
                for (int position : output.borrowMask()) {
                    values.add(build.value(position));
                }
            }
        }
        return values;
    }
}
