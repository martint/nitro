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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestHashJoinSession
{
    @Test
    void testPublishedOutputSurvivesFollowingProbeBatch()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        Schema.unspecified(1),
                        new int[] {0},
                        table(1),
                        new int[] {0},
                        false)) {
            allocator.beginExecution();
            session.addInput(batch(1));
            assertThat(session.hasOutput()).isTrue();
            try (Batch first = session.getRetainedOutput()) {
                assertThat(session.hasOutput()).isFalse();

                session.addInput(batch(1));
                assertThat(session.hasOutput()).isTrue();
                session.getOutput().close();

                assertThat(VectorAccess.longValues(first.output(0).borrow(Stream.VALUES)).value(0)).isOne();
                assertThat(VectorAccess.longValues(first.output(1).borrow(Stream.VALUES)).value(0)).isOne();
            }
        }
    }

    @Test
    void testIdentityProbeOutputDoesNotTransferBorrowedInputVector()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        Schema.unspecified(1),
                        new int[] {0},
                        table(1, 2),
                        new int[] {0},
                        false)) {
            allocator.beginExecution();
            Allocator.Context probeContext = new Allocator.Context("identity-probe");
            I64Vector probeValues = allocator.allocate(probeContext, I64Vector.class, 2, I64Vector::new);
            probeValues.values()[0] = 1;
            probeValues.values()[1] = 2;
            long probeBytes = allocator.currentBytes(probeContext);
            int[] constraints = new int[1];
            Batch probe = new Batch(
                    Mask.all(2),
                    _ -> constraints[0]++,
                    mask -> mask,
                    new Output(
                            Set.of(Stream.VALUES),
                            _ -> probeValues,
                            (_, vector) -> allocator.transfer(probeContext, vector),
                            (_, vector) -> allocator.release(probeContext, vector)));
            session.addInput(probe);

            assertThat(session.hasOutput()).isTrue();
            try (Batch output = session.getOutput()) {
                Vector taken = output.output(0).take(Stream.VALUES);
                assertThat(taken).isNotSameAs(probeValues);
                assertThat(allocator.currentBytes(probeContext)).isEqualTo(probeBytes);
                assertThat(constraints[0]).isOne();
                Output.of(Streams.ofValues(taken)).close();
            }
            allocator.release(probeContext, probeValues);
        }
    }

    @Test
    void testReleasesSupersededRetainedBuildConstraints()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            Allocator.Context context = new Allocator.Context("retained-constraint-test");
            try (Batch retained = batch(1, 2, 3)) {
                Mask current = null;
                for (int iteration = 0; iteration < 1_000; iteration++) {
                    Mask replacement = allocator.allocateSparseMask(context, new int[] {iteration % 3}, 3);
                    current = HashJoinOperator.replaceRetainedConstraint(allocator, context, retained, current, replacement);
                }
                assertThat(allocator.currentBytes(context)).isLessThan(128);
                allocator.release(context, current);
            }
        }
    }

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

    @Test
    void testDirectRangeBuildPreservesPayload()
    {
        long[] keys = new long[300];
        long[] payload = new long[300];
        for (int position = 0; position < keys.length; position++) {
            keys[position] = position * 2L;
            payload[position] = 10_000L + position;
        }

        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        Schema.unspecified(1),
                        new int[] {0},
                        pairTable(keys, payload),
                        new int[] {0},
                        false)) {
            allocator.beginExecution();
            session.addInput(batch(598));

            assertThat(session.hasOutput()).isTrue();
            try (Batch output = session.getOutput()) {
                assertThat(output.borrowMask().selectedCount()).isOne();
                assertThat(VectorAccess.longValues(output.output(2).borrow(Stream.VALUES)).value(0))
                        .isEqualTo(10_299);
            }
        }
    }

    @Test
    void testPreservesExternalSchedulingAcrossProbePipeline()
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
                        false,
                        null,
                        CountingNextOperator::new)) {
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
        }
    }

    @Test
    void testComposesOutputPipelineOverNativeJoinBatches()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        Schema.unspecified(1),
                        new int[] {0},
                        table(2, 2, 2),
                        new int[] {0},
                        false)
                        .withOutputPipeline(source -> new LimitOperator(allocator, 2, source))) {
            allocator.beginExecution();

            List<Long> probeValues = new ArrayList<>();
            List<Long> buildValues = new ArrayList<>();
            session.addInput(batch(2));
            drain(session, probeValues, buildValues);
            session.finish();
            drain(session, probeValues, buildValues);

            assertThat(probeValues).containsExactly(2L, 2L);
            assertThat(buildValues).containsExactly(2L, 2L);
            assertThat(session.isFinished()).isTrue();
        }
    }

    @Test
    void testSharesPreparedBuildAcrossProbeSessions()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            HashJoinBuild build = HashJoinSession.prepareBuild(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            table(2, 3, 2),
                            new int[] {0},
                            false,
                            new int[] {0, 1})
                    .orElseThrow();
            try (build;
                    HashJoinSession first = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            table(2, 3, 2),
                            new int[] {0},
                            false,
                            build);
                    HashJoinSession second = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            table(2, 3, 2),
                            new int[] {0},
                            false,
                            build)) {
                assertSessionOutput(first, 2);
                assertSessionOutput(second, 3);
            }
        }
    }

    @Test
    void testPreparedBuildExposesExactMembershipWithoutCopyingKeys()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            long[] buildKeys = new long[256];
            for (int index = 0; index < buildKeys.length; index++) {
                buildKeys[index] = 100_000_000L + index * 10L;
            }
            try (HashJoinBuild build = HashJoinSession.prepareBuild(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            table(buildKeys),
                            new int[] {0},
                            false,
                            new int[0])
                    .orElseThrow()) {
                DynamicFilter filter = build.exactDynamicFilter(3);

                assertThat(filter).isNotNull();
                assertThat(filter.column()).isEqualTo(3);
                assertThat(filter.accepts(100_000_000)).isTrue();
                assertThat(filter.accepts(100_000_010)).isTrue();
                assertThat(filter.accepts(100_002_550)).isTrue();
                assertThat(filter.accepts(100_000_011)).isFalse();
                List<Long> visitedKeys = new ArrayList<>();
                assertThat(build.visitExactLongKeys(visitedKeys::add)).isTrue();
                assertThat(visitedKeys).hasSize(buildKeys.length);
                for (long key : buildKeys) {
                    assertThat(visitedKeys).contains(key);
                }
                assertThat(build.separateDynamicFilterCollectionActivated()).isFalse();
            }
        }
    }

    @Test
    void testOrdinaryJoinStillCollectsBuildMembershipForProbePushdown()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                HashJoinOperator join = new HashJoinOperator(
                        resources.operatorResources(),
                        allocator,
                        table(2),
                        0,
                        table(2, 3),
                        0)) {
            allocator.beginExecution();
            while (join.hasNext()) {
                join.next().close();
            }
            assertThat(join.separateDynamicFilterCollectionActivated()).isTrue();
        }
    }

    @Test
    void testPreparedBuildVisitsDenseMembership()
    {
        long[] buildKeys = new long[512];
        for (int index = 0; index < buildKeys.length; index++) {
            buildKeys[index] = 50_000L + index;
        }
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            try (HashJoinBuild build = HashJoinSession.prepareBuild(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            table(buildKeys),
                            new int[] {0},
                            false,
                            new int[0])
                    .orElseThrow()) {
                List<Long> visitedKeys = new ArrayList<>();
                assertThat(build.visitExactLongKeys(visitedKeys::add)).isTrue();
                assertThat(visitedKeys).containsExactlyElementsOf(
                        java.util.Arrays.stream(buildKeys).boxed().toList());
            }
        }
    }

    @Test
    void testTinyHighOffsetStreamingBuildDoesNotAllocateSparseDirectRange()
    {
        long[] buildKeys = new long[12];
        for (int position = 0; position < buildKeys.length; position++) {
            buildKeys[position] = 2_451_577L + position * 2L;
        }

        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        Schema.unspecified(1),
                        new int[] {0},
                        table(buildKeys),
                        new int[] {0},
                        false)
                        .withOutputs(0)) {
            allocator.beginExecution();
            session.addInput(batch(buildKeys[buildKeys.length - 1]));

            assertThat(session.hasOutput()).isTrue();
            try (Batch output = session.getOutput()) {
                assertThat(output.borrowMask().selectedCount()).isOne();
            }
            assertThat(allocator.allocatedBytes()).isLessThan(1L << 20);
        }
    }

    @Test
    void testPreparedProbeReusesCompactedBuildPayload()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            HashJoinBuild build = HashJoinSession.prepareBuild(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            table(2, 3, 2),
                            new int[] {0},
                            false,
                            new int[] {0, 1})
                    .orElseThrow();
            Operator unconsumableBuild = new Operator()
            {
                @Override
                public int outputCount()
                {
                    return 1;
                }

                @Override
                public boolean hasNext()
                {
                    throw new AssertionError("prepared probe reloaded the raw build");
                }

                @Override
                public Batch next()
                {
                    throw new AssertionError("prepared probe reloaded the raw build");
                }

                @Override
                public void constrain(Mask mask) {}

                @Override
                public void close() {}
            };
            try (build;
                    HashJoinSession session = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            unconsumableBuild,
                            new int[] {0},
                            false,
                            build)) {
                assertSessionOutput(session, 2);
            }
        }
    }

    @Test
    void testPreparedKeyOnlyProbeDoesNotRebuildIndex()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            HashJoinBuild build = HashJoinSession.prepareBuild(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            table(2, 3, 2),
                            new int[] {0},
                            false,
                            new int[] {0})
                    .orElseThrow();
            Operator unconsumableBuild = new Operator()
            {
                @Override
                public int outputCount()
                {
                    return 1;
                }

                @Override
                public boolean hasNext()
                {
                    throw new AssertionError("prepared key-only probe rebuilt the raw build");
                }

                @Override
                public Batch next()
                {
                    throw new AssertionError("prepared key-only probe rebuilt the raw build");
                }

                @Override
                public void constrain(Mask mask) {}

                @Override
                public void close() {}
            };
            try (build;
                    HashJoinSession session = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            unconsumableBuild,
                            new int[] {0},
                            false,
                            build)
                            .withOutputs(0)) {
                session.addInput(batch(2));
                assertThat(session.hasOutput()).isTrue();
                try (Batch output = session.getOutput()) {
                    assertThat(output.borrowMask().selectedCount()).isEqualTo(2);
                    assertThat(VectorAccess.longValues(output.output(0).borrow(Stream.VALUES)).value(0)).isEqualTo(2);
                    assertThat(VectorAccess.longValues(output.output(0).borrow(Stream.VALUES)).value(1)).isEqualTo(2);
                }
            }
        }
    }

    @Test
    void testSharesPreparedPairBuildAcrossProbeSessions()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            HashJoinBuild build = HashJoinSession.prepareBuild(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(2),
                            new int[] {0, 1},
                            pairTable(new long[] {2, 3, 2}, new long[] {20, 30, 21}),
                            new int[] {0, 1},
                            false,
                            new int[] {0, 1, 2, 3})
                    .orElseThrow();
            try (build;
                    HashJoinSession first = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(2),
                            new int[] {0, 1},
                            pairTable(new long[] {2, 3, 2}, new long[] {20, 30, 21}),
                            new int[] {0, 1},
                            false,
                            build);
                    HashJoinSession second = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(2),
                            new int[] {0, 1},
                            pairTable(new long[] {2, 3, 2}, new long[] {20, 30, 21}),
                            new int[] {0, 1},
                            false,
                            build)) {
                assertPairSessionOutput(first, 2, 21);
                assertPairSessionOutput(second, 3, 30);
            }
        }
    }

    @Test
    void testSharesCompactedPairDuplicateRangesAcrossProbeSessions()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            HashJoinBuild build = HashJoinSession.prepareBuild(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(3),
                            new int[] {0, 1},
                            tripleTable(
                                    new long[] {2, 2, 2},
                                    new long[] {20, 20, 20},
                                    new long[] {200, 201, 202}),
                            new int[] {0, 1},
                            false,
                            new int[] {0, 1, 2, 3, 4, 5})
                    .orElseThrow();
            try (build;
                    HashJoinSession first = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(3),
                            new int[] {0, 1},
                            tripleTable(
                                    new long[] {2, 2, 2},
                                    new long[] {20, 20, 20},
                                    new long[] {200, 201, 202}),
                            new int[] {0, 1},
                            false,
                            build);
                    HashJoinSession second = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(3),
                            new int[] {0, 1},
                            tripleTable(
                                    new long[] {2, 2, 2},
                                    new long[] {20, 20, 20},
                                    new long[] {200, 201, 202}),
                            new int[] {0, 1},
                            false,
                            build)) {
                assertPairDuplicateSessionOutput(first);
                assertPairDuplicateSessionOutput(second);
            }
        }
    }

    @Test
    void testSharesPreparedTripleBuildAcrossProbeSessions()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            HashJoinBuild build = HashJoinSession.prepareBuild(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(3),
                            new int[] {0, 1, 2},
                            tripleTable(
                                    new long[] {2, 3, 2},
                                    new long[] {20, 30, 21},
                                    new long[] {200, 300, 201}),
                            new int[] {0, 1, 2},
                            false,
                            new int[] {0, 1, 2, 3, 4, 5})
                    .orElseThrow();
            try (build;
                    HashJoinSession first = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(3),
                            new int[] {0, 1, 2},
                            tripleTable(
                                    new long[] {2, 3, 2},
                                    new long[] {20, 30, 21},
                                    new long[] {200, 300, 201}),
                            new int[] {0, 1, 2},
                            false,
                            build);
                    HashJoinSession second = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(3),
                            new int[] {0, 1, 2},
                            tripleTable(
                                    new long[] {2, 3, 2},
                                    new long[] {20, 30, 21},
                                    new long[] {200, 300, 201}),
                            new int[] {0, 1, 2},
                            false,
                            build)) {
                assertTripleSessionOutput(first, 2, 21, 201);
                assertTripleSessionOutput(second, 3, 30, 300);
            }
        }
    }

    @Test
    void testSharesPreparedFlatBuildAcrossProbeSessions()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            HashJoinBuild build = HashJoinSession.prepareBuild(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            binaryTable("alpha", "beta", "alpha"),
                            new int[] {0},
                            false,
                            new int[] {0, 1})
                    .orElseThrow();
            try (build;
                    HashJoinSession first = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            binaryTable("alpha", "beta", "alpha"),
                            new int[] {0},
                            false,
                            build);
                    HashJoinSession second = new HashJoinSession(
                            resources.operatorResources(),
                            allocator,
                            Schema.unspecified(1),
                            new int[] {0},
                            binaryTable("alpha", "beta", "alpha"),
                            new int[] {0},
                            false,
                            build)) {
                assertBinarySessionOutput(first, "alpha", 2);
                assertBinarySessionOutput(second, "beta", 1);
            }
        }
    }

    private static void assertSessionOutput(HashJoinSession session, long probeValue)
    {
        List<Long> probeValues = new ArrayList<>();
        List<Long> buildValues = new ArrayList<>();
        session.addInput(batch(probeValue));
        drain(session, probeValues, buildValues);
        session.finish();
        drain(session, probeValues, buildValues);
        assertThat(probeValues).allMatch(value -> value == probeValue);
        assertThat(buildValues).allMatch(value -> value == probeValue);
    }

    private static void assertPairSessionOutput(HashJoinSession session, long first, long second)
    {
        List<Long> probeValues = new ArrayList<>();
        List<Long> buildValues = new ArrayList<>();
        session.addInput(pairBatch(new long[] {first}, new long[] {second}));
        drain(session, probeValues, buildValues);
        session.finish();
        drain(session, probeValues, buildValues);
        assertThat(probeValues).containsExactly(first);
        assertThat(buildValues).containsExactly(second);
    }

    private static void assertTripleSessionOutput(HashJoinSession session, long first, long second, long third)
    {
        session.addInput(tripleBatch(
                new long[] {first},
                new long[] {second},
                new long[] {third}));
        assertThat(session.hasOutput()).isTrue();
        try (Batch output = session.getOutput()) {
            assertThat(VectorAccess.longValues(output.output(0).borrow(Stream.VALUES)).value(0)).isEqualTo(first);
            assertThat(VectorAccess.longValues(output.output(5).borrow(Stream.VALUES)).value(0)).isEqualTo(third);
        }
        session.finish();
        while (session.hasOutput()) {
            session.getOutput().close();
        }
    }

    private static void assertPairDuplicateSessionOutput(HashJoinSession session)
    {
        session.addInput(tripleBatch(new long[] {2}, new long[] {20}, new long[] {100}));
        List<Long> buildPayloads = new ArrayList<>();
        while (session.hasOutput()) {
            try (Batch output = session.getOutput()) {
                Mask mask = output.borrowMask();
                VectorAccess.LongValues buildPayload = VectorAccess.longValues(output.output(5).borrow(Stream.VALUES));
                for (int position : mask) {
                    buildPayloads.add(buildPayload.value(position));
                }
            }
        }
        session.finish();
        while (session.hasOutput()) {
            try (Batch output = session.getOutput()) {
                Mask mask = output.borrowMask();
                VectorAccess.LongValues buildPayload = VectorAccess.longValues(output.output(5).borrow(Stream.VALUES));
                for (int position : mask) {
                    buildPayloads.add(buildPayload.value(position));
                }
            }
        }
        assertThat(buildPayloads).containsExactly(200L, 201L, 202L);
    }

    private static void assertBinarySessionOutput(HashJoinSession session, String value, int expectedRows)
    {
        session.addInput(binaryBatch(value));
        int rows = 0;
        while (session.hasOutput()) {
            try (Batch output = session.getOutput()) {
                rows += output.borrowMask().size();
            }
        }
        session.finish();
        while (session.hasOutput()) {
            try (Batch output = session.getOutput()) {
                rows += output.borrowMask().size();
            }
        }
        assertThat(rows).isEqualTo(expectedRows);
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

    private static Batch pairBatch(long[] first, long[] second)
    {
        return new Batch(
                Mask.all(first.length),
                Output.of(org.weakref.nitro.data.Streams.ofValues(new I64Vector(first))),
                Output.of(org.weakref.nitro.data.Streams.ofValues(new I64Vector(second))));
    }

    private static Operator pairTable(long[] first, long[] second)
    {
        return new TableOperator(
                Schema.unspecified(2),
                List.of(TableOperator.Page.values(
                        first.length,
                        new org.weakref.nitro.data.Vector[] {new I64Vector(first), new I64Vector(second)},
                        Mask.all(first.length))));
    }

    private static Batch tripleBatch(long[] first, long[] second, long[] third)
    {
        return new Batch(
                Mask.all(first.length),
                Output.of(org.weakref.nitro.data.Streams.ofValues(new I64Vector(first))),
                Output.of(org.weakref.nitro.data.Streams.ofValues(new I64Vector(second))),
                Output.of(org.weakref.nitro.data.Streams.ofValues(new I64Vector(third))));
    }

    private static Operator tripleTable(long[] first, long[] second, long[] third)
    {
        return new TableOperator(
                Schema.unspecified(3),
                List.of(TableOperator.Page.values(
                        first.length,
                        new org.weakref.nitro.data.Vector[] {
                                new I64Vector(first),
                                new I64Vector(second),
                                new I64Vector(third)},
                        Mask.all(first.length))));
    }

    private static Batch binaryBatch(String... values)
    {
        return new Batch(
                Mask.all(values.length),
                Output.of(org.weakref.nitro.data.Streams.ofValues(binaryVector(values))));
    }

    private static Operator binaryTable(String... values)
    {
        return new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(
                        values.length,
                        new org.weakref.nitro.data.Vector[] {binaryVector(values)},
                        Mask.all(values.length))));
    }

    private static BinaryVector binaryVector(String... values)
    {
        byte[][] bytes = java.util.Arrays.stream(values)
                .map(value -> value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .toArray(byte[][]::new);
        int[] offsets = new int[values.length + 1];
        int totalBytes = 0;
        for (int position = 0; position < values.length; position++) {
            totalBytes += bytes[position].length;
            offsets[position + 1] = totalBytes;
        }
        byte[] data = new byte[totalBytes];
        int offset = 0;
        for (byte[] value : bytes) {
            System.arraycopy(value, 0, data, offset, value.length);
            offset += value.length;
        }
        return new BinaryVector(values.length, offsets, data);
    }
}
