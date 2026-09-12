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
import org.weakref.nitro.execution.EngineResources;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class TestBufferedJoinInputPolicy
{
    @Test
    void testDeferredReborrowDoesNotPermitOpenBatchPolling()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("advancing-join-input");
            BufferedJoinInput input = new BufferedJoinInput(
                    BufferedJoinInputPolicy.defaults(),
                    new JoinBufferSupport(JoinBufferPolicy.defaults(), allocator, context),
                    1);
            AtomicBoolean closed = new AtomicBoolean();
            Batch batch = new Batch(
                    Mask.all(2),
                    _ -> {},
                    Function.identity(),
                    _ -> {},
                    () -> closed.set(true),
                    Output.of(Streams.ofValues(new I64Vector(new long[] {11, 22}))));
            try (Operator source = new Operator()
            {
                private boolean emitted;

                @Override
                public int outputCount()
                {
                    return 1;
                }

                @Override
                public boolean hasNext()
                {
                    if (emitted) {
                        assertThat(closed.get()).as("close the build batch before polling upstream").isTrue();
                    }
                    return !emitted;
                }

                @Override
                public Batch next()
                {
                    emitted = true;
                    return batch;
                }

                @Override
                public void constrain(Mask mask)
                {
                    batch.constrain(mask);
                }

                @Override
                public boolean supportsConstrainedReborrow()
                {
                    return true;
                }

                @Override
                public void close()
                {
                    batch.close();
                }
            }) {
                input.loadAll(source, 1024, new int[] {0}, false, true);
                assertThat(input.batches()).hasSize(1);
                I64Vector values = (I64Vector) input.batches().getFirst().columns()[0].values();
                assertThat(values.values()).startsWith(11, 22);
            }
            finally {
                input.releaseBuffers();
                allocator.release(context);
            }
        }
    }

    @Test
    void testOwnedBuildColumnsArePublishedImmutable()
    {
        BinaryVector values = binary("alpha", "beta");

        BufferedJoinInput.InnerBatch batch = new BufferedJoinInput.InnerBatch(
                new Streams[] {Streams.ofValues(values)},
                values.length());

        assertThat(batch.columns()[0].values()).isSameAs(values);
        assertThat(values.contentImmutable()).isTrue();
    }

    @Test
    void testDenseRetainedBinaryBatchesCoalesceByRange()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("retained-binary-range-test");
            BufferedJoinInput input = new BufferedJoinInput(
                    BufferedJoinInputPolicy.defaults(),
                    new JoinBufferSupport(JoinBufferPolicy.defaults(), allocator, context),
                    1);
            TableOperator source = TableOperator.retained(
                    Schema.unspecified(1),
                    List.of(
                            TableOperator.Page.values(2, new BinaryVector[] {binary("a", "bb")}, Mask.all(2)),
                            TableOperator.Page.values(2, new BinaryVector[] {binary("ccc", "d")}, Mask.all(2))));

            input.loadAll(source, 1024, new int[] {0}, true);

            assertThat(input.batches()).hasSize(1);
            BinaryVector values = (BinaryVector) input.batches().getFirst().columns()[0].get(Stream.VALUES);
            assertThat(values.offsets()).startsWith(0, 1, 3, 6, 7);
            assertThat(values.data()).startsWith((byte) 'a', (byte) 'b', (byte) 'b', (byte) 'c', (byte) 'c', (byte) 'c', (byte) 'd');

            input.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testStandaloneDefaultsAreOwnedByOperatorResources()
    {
        try (OperatorResources resources = OperatorResources.createDefault()) {
            BufferedJoinInputPolicy policy = resources.bufferedJoinInputPolicy();

            assertThat(policy).isEqualTo(BufferedJoinInputPolicy.defaults());
            assertThat(policy.maxCoalescedRows()).isEqualTo(4_000_000);
            assertThat(policy.maxPostLoadCoalescedRows()).isEqualTo(1 << 20);
            assertThat(policy.minAutomaticDirectExactRows()).isEqualTo(1 << 18);
            assertThat(policy.directExactCoalesce()).isFalse();
        }
    }

    @Test
    void testRetainedDenseBatchUsesImplicitPositions()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("retained-dense-input-test");
            BufferedJoinInput input = new BufferedJoinInput(
                    BufferedJoinInputPolicy.defaults(),
                    new JoinBufferSupport(JoinBufferPolicy.defaults(), allocator, context),
                    1);
            TableOperator source = TableOperator.retained(
                    Schema.unspecified(1),
                    List.of(TableOperator.Page.values(
                            3,
                            new I64Vector[] {new I64Vector(new long[] {11, 12, 13})},
                            Mask.all(3))));

            input.loadAll(source, 1024, new int[] {0}, true);

            assertThat(input.batches()).hasSize(1);
            BufferedJoinInput.InnerBatch batch = input.batches().getFirst();
            assertThat(batch.positions()).isNull();
            assertThat(batch.sourcePosition(0)).isZero();
            assertThat(batch.sourcePosition(2)).isEqualTo(2);

            input.releaseBuffers();
            allocator.release(context);
        }
    }

    private static BinaryVector binary(String... values)
    {
        int[] offsets = new int[values.length + 1];
        int byteCount = 0;
        for (int index = 0; index < values.length; index++) {
            byteCount += values[index].length();
            offsets[index + 1] = byteCount;
        }
        byte[] data = new byte[byteCount];
        int offset = 0;
        for (String value : values) {
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            System.arraycopy(bytes, 0, data, offset, bytes.length);
            offset += bytes.length;
        }
        return new BinaryVector(values.length, offsets, data);
    }
}
