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
import org.weakref.nitro.execution.EngineResources;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestBufferedJoinInputPolicy
{
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
}
