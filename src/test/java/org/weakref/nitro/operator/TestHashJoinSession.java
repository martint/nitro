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
