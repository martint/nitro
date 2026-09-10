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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RegionVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.data.Row.row;
import static org.weakref.nitro.function.scalar.builtin.JoinFilterFunctions.longLessThan;

class TestSharedBuildOuterJoin
{
    @Test
    void testMarksDenseDictionaryIdentities()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            AtomicLongArray matched = new AtomicLongArray(3);
            DictionaryVector identities = new DictionaryVector(
                    new int[] {2, 0, 1, 2},
                    new I64Vector(new long[] {65, 1, 129}));
            try (BuildOuterMatchMarker marker = new BuildOuterMatchMarker(
                    resources.operatorResources().codeGeneration().buildOuterMatchMarker(),
                    allocator.primitiveArrays(),
                    matched)) {
                marker.mark(identities, null, Mask.all(4));
            }

            assertThat(matched.get(0)).isEqualTo(1L << 1);
            assertThat(matched.get(1)).isEqualTo(1L << 1);
            assertThat(matched.get(2)).isEqualTo(1L << 1);
        }
    }

    @Test
    void testMarksSparseDictionaryIdentities()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            AtomicLongArray matched = new AtomicLongArray(3);
            DictionaryVector identities = new DictionaryVector(
                    new int[] {2, 0, 1, 2},
                    new I32Vector(new int[] {65, 1, 129}));
            try (BuildOuterMatchMarker marker = new BuildOuterMatchMarker(
                    resources.operatorResources().codeGeneration().buildOuterMatchMarker(),
                    allocator.primitiveArrays(),
                    matched)) {
                marker.mark(identities, null, Mask.sparse(new int[] {1, 3}, 4));
            }

            assertThat(matched.get(0)).isZero();
            assertThat(matched.get(1)).isEqualTo(1L << 1);
            assertThat(matched.get(2)).isEqualTo(1L << 1);
        }
    }

    @Test
    void testMarksNestedEncodedIdentitiesWithIndependentNulls()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            AtomicLongArray matched = new AtomicLongArray(4);
            RleVector identities = new RleVector(
                    new int[] {2, 1, 1, 2},
                    DictionaryVector.wrapNested(
                            new int[] {2, 0, 1, 2},
                            4,
                            new RegionVector(new I64Vector(new long[] {1, 65, 129, 193}), 1, 3)));
            RleVector nulls = new RleVector(
                    new int[] {2, 4},
                    new BooleanVector(new boolean[] {true, false}));
            try (BuildOuterMatchMarker marker = new BuildOuterMatchMarker(
                    resources.operatorResources().codeGeneration().buildOuterMatchMarker(),
                    allocator.primitiveArrays(),
                    matched)) {
                marker.mark(identities, nulls, Mask.all(6));
            }

            assertThat(matched.get(0)).isZero();
            assertThat(matched.get(1)).isEqualTo(1L << 1);
            assertThat(matched.get(2)).isEqualTo(1L << 1);
            assertThat(matched.get(3)).isEqualTo(1L << 1);
        }
    }

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

    @Test
    void testUnmatchedFullJoinProbeDoesNotMarkFirstBuildRow()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator buildAllocator = new Allocator(resources);
                Allocator probeAllocator = new Allocator(resources);
                Allocator outputAllocator = new Allocator(resources)) {
            buildAllocator.beginExecution();
            probeAllocator.beginExecution();
            outputAllocator.beginExecution();

            ConstantTableOperator probe = new ConstantTableOperator(probeAllocator, 1, List.of(row(3L)));
            try (SharedBuildOuterJoin shared = SharedBuildOuterJoin.prepare(
                            resources.operatorResources(),
                            buildAllocator,
                            probe.outputSchema(),
                            new int[] {0},
                            new ConstantTableOperator(buildAllocator, 1, List.of(row(1L), row(2L))),
                            new int[] {0},
                            true,
                            new int[] {0, 1})
                    .orElseThrow();
                    JoinSession session = shared.newProbeSession(resources.operatorResources(), probeAllocator);
                    Operator unmatched = shared.newUnmatchedBuildOperator(outputAllocator)) {
                session.addInput(probe.next());
                session.finish();
                while (session.hasOutput()) {
                    session.getOutput().close();
                }
                assertThat(session.isFinished()).isTrue();

                assertThat(readBuildValues(unmatched)).containsExactly(1L, 2L);
            }
            probe.close();
        }
    }

    @Test
    void testSharesNestedLoopMatchesAcrossProbeSessions()
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

            ConstantTableOperator firstProbe = new ConstantTableOperator(firstProbeAllocator, 1, List.of(row(1L)));
            ConstantTableOperator secondProbe = new ConstantTableOperator(secondProbeAllocator, 1, List.of(row(3L)));
            try (SharedBuildOuterJoin shared = SharedBuildOuterJoin.prepare(
                            resources.operatorResources(),
                            buildAllocator,
                            firstProbe.outputSchema(),
                            new int[0],
                            new ConstantTableOperator(buildAllocator, 1, List.of(row(0L), row(2L), row(4L))),
                            new int[0],
                            false,
                            new int[] {0, 1},
                            longLessThan(0, 0))
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

                assertThat(matchedBuild).containsExactly(2L, 4L, 4L);
                assertThat(readBuildValues(unmatched)).containsExactly(0L);
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
