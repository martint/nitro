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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestGenericJoinIndexFactory
{
    @Test
    void testBoundedPayloadAdmissionAroundRowCountBoundaries()
    {
        try (EngineResources resources = EngineResources.createDefault()) {
            GenericJoinIndexFactory indexes = resources.operatorResources().genericJoinIndexes();
            HashJoinBuildPolicy buildPolicy = resources.operatorResources().hashJoin().buildPolicy();
            int minimumRows = buildPolicy.payloadHashCapBoundedExpectedRows();
            long maximumKey = resources.operatorResources().hashJoin().indexPolicy().maxDirectBuildKey();
            long[] keys = sequence(maximumKey - 32, 1, 32);

            assertThat(indexes.initialHashBuildAdmission(batch(keys), vectors(keys), new Vector[1], minimumRows - 1, false))
                    .isEqualTo(new GenericJoinIndexFactory.InitialHashBuildAdmission(false, false));
            for (int expectedRows : new int[] {minimumRows, minimumRows + 1, 4_999_999, 5_000_000, 5_000_001}) {
                assertThat(indexes.initialHashBuildAdmission(batch(keys), vectors(keys), new Vector[1], expectedRows, false))
                        .as("bounded unique payload build with %s expected rows", expectedRows)
                        .isEqualTo(new GenericJoinIndexFactory.InitialHashBuildAdmission(true, true));
                assertThat(indexes.initialHashBuildAdmission(batch(keys), vectors(keys), new Vector[1], expectedRows, true))
                        .as("key-only build with %s expected rows", expectedRows)
                        .isEqualTo(new GenericJoinIndexFactory.InitialHashBuildAdmission(false, false));
            }

            for (long invalidKey : new long[] {-1, Long.MIN_VALUE, maximumKey, Long.MAX_VALUE}) {
                keys[keys.length - 1] = invalidKey;
                assertThat(indexes.initialHashBuildAdmission(batch(keys), vectors(keys), new Vector[1], minimumRows, false))
                        .as("unique payload sample containing %s", invalidKey)
                        .isEqualTo(new GenericJoinIndexFactory.InitialHashBuildAdmission(false, false));
            }
        }
    }

    @Test
    void testCappedAdmissionRejectsOnlyProvenImpossibleKeys()
    {
        try (EngineResources resources = EngineResources.createDefault()) {
            GenericJoinIndexFactory indexes = resources.operatorResources().genericJoinIndexes();
            long maximumKey = resources.operatorResources().hashJoin().indexPolicy().maxDirectBuildKey();
            long[] validKeys = {0, 0, 0, 0, 0, 0, 0, maximumKey - 1};
            assertThat(indexes.initialHashBuildAdmission(batch(validKeys), vectors(validKeys), new Vector[1], 8, false))
                    .isEqualTo(new GenericJoinIndexFactory.InitialHashBuildAdmission(true, true));

            for (long invalidKey : new long[] {-1, Long.MIN_VALUE, maximumKey, Long.MAX_VALUE}) {
                long[] keys = {0, 0, 0, 0, 0, 0, 0, invalidKey};
                Vector[] values = vectors(keys);
                assertThat(indexes.initialHashBuildAdmission(batch(keys), values, new Vector[1], 8, false))
                        .isEqualTo(new GenericJoinIndexFactory.InitialHashBuildAdmission(true, false));
                Vector[] nulls = {new BooleanVector(new boolean[] {false, false, false, false, false, false, false, true})};
                assertThat(indexes.initialHashBuildAdmission(batch(keys), values, nulls, 8, false))
                        .isEqualTo(new GenericJoinIndexFactory.InitialHashBuildAdmission(true, true));
                try (Batch source = new Batch(Mask.all(keys.length), Output.of(Streams.ofValues(values[0])))) {
                    BufferedJoinInput.InnerBatch selected = BufferedJoinInput.InnerBatch.retained(source, new int[] {0, 1, 2, 3, 4, 5, 6});
                    assertThat(indexes.initialHashBuildAdmission(selected, values, new Vector[1], 7, false))
                            .isEqualTo(new GenericJoinIndexFactory.InitialHashBuildAdmission(true, true));
                }
            }
        }
    }

    @Test
    void testFlatJoinUsesProviderCanonicalLongStorage()
    {
        try (EngineResources resources = EngineResources.createDefault()) {
            GenericJoinIndexFactory indexes = resources.operatorResources().genericJoinIndexes();
            BinaryVector labels = binary("a", "b");
            Vector[] values = {new I64Vector(new long[] {1, 2}), labels};
            JoinIndex index = indexes.create(
                    values,
                    List.of(CanonicalFlatKeyTestType.signedInteger(), Schema.unspecified(2).field(1).type()),
                    resources.primitiveArrays(),
                    2,
                    false,
                    false);
            try {
                index.addNoNulls(values, 0, 11);
                index.addNoNulls(values, 1, 22);
                assertThat(index.matchesNoNulls(values, 0).toLongArray()).containsExactly(11);
                assertThat(index.matchesNoNulls(values, 1).toLongArray()).containsExactly(22);

                assertThatThrownBy(() -> index.addNoNulls(
                        new Vector[] {new I64Vector(new long[] {1L << 40}), binary("c")},
                        0,
                        33))
                        .isInstanceOf(ArithmeticException.class);
            }
            finally {
                index.releaseBuffers();
            }
        }
    }

    @Test
    void testDirectRangeBuildAdmissionUsesBoundedAbsoluteDomain()
    {
        try (EngineResources resources = EngineResources.createDefault()) {
            GenericJoinIndexFactory indexes = resources.operatorResources().genericJoinIndexes();

            assertThat(indexes.shouldUseDirectRangeBuild(batch(sequence(0, 2, 4096)), vectors(sequence(0, 2, 4096)), 4096))
                    .isTrue();
            assertThat(indexes.shouldUseDirectRangeBuild(batch(sequence(1_000_000, 1, 4096)), vectors(sequence(1_000_000, 1, 4096)), 4096))
                    .isFalse();
            assertThat(indexes.shouldUseDirectRangeBuild(batch(sequence(0, 1, 255)), vectors(sequence(0, 1, 255)), 255))
                    .isFalse();
        }
    }

    private static BufferedJoinInput.InnerBatch batch(long[] values)
    {
        return new BufferedJoinInput.InnerBatch(new Streams[] {Streams.ofValues(new I64Vector(values))}, values.length);
    }

    private static Vector[] vectors(long[] values)
    {
        return new Vector[] {new I64Vector(values)};
    }

    private static long[] sequence(long start, long stride, int size)
    {
        long[] values = new long[size];
        for (int index = 0; index < size; index++) {
            values[index] = start + stride * index;
        }
        return values;
    }

    private static BinaryVector binary(String... values)
    {
        int bytes = java.util.Arrays.stream(values).mapToInt(String::length).sum();
        BinaryVector vector = new BinaryVector(values.length, bytes);
        for (int index = 0; index < values.length; index++) {
            vector.setBytes(index, values[index].getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return vector;
    }
}
