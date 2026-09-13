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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;

import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.data.Row.row;

class TestBatchConstraintPropagation
{
    @Test
    void testSelectionNotifiesTheSourceBatchOnce()
    {
        for (String kind : List.of("filter", "limit", "distinct", "distinct-pass", "semi", "row-number")) {
            try (EngineResources resources = EngineResources.createDefault();
                    Allocator allocator = new Allocator(resources)) {
                boolean unchangedDistinct = kind.equals("distinct-pass");
                TrackedSource source = new TrackedSource(new ConstantTableOperator(allocator, 1,
                        unchangedDistinct ? List.of(row(1L), row(2L), row(3L), row(4L)) : List.of(row(1L), row(1L), row(2L), row(2L))));
                try (Operator operator = switch (kind) {
                    case "filter" -> new FilterOperator(source, new EvaluationPlan(List.of(), List.of()),
                            new PrimitiveRegistry(), AllMask.ALL, allocator, resources.operatorResources().filter());
                    case "limit" -> new LimitOperator(allocator, 2, source);
                    case "distinct", "distinct-pass" -> new MarkDistinctOperator(allocator, new int[] {0}, source, true, resources.operatorResources());
                    case "semi" -> new SemiJoinOperator(allocator, source, 0,
                            new ConstantTableOperator(allocator, 1, List.of(row(1L))), 0, true, false, resources.operatorResources());
                    case "row-number" -> new PartitionedRowNumberOperator(allocator, new int[] {0}, source,
                            Schema.unspecified(1).field(0), OptionalLong.of(1), resources.operatorResources());
                    default -> throw new IllegalArgumentException(kind);
                };
                        Batch batch = operator.next()) {
                    assertThat(source.operatorConstraints).as(kind + " operator notifications").isZero();
                    int initialNotifications = unchangedDistinct ? 0 : 1;
                    assertThat(source.batchConstraints).as(kind + " batch notifications").isEqualTo(initialNotifications);
                    assertThat(batch.borrowMask().count()).as(kind + " selected rows").isEqualTo(kind.equals("filter") || unchangedDistinct ? 4 : 2);
                    int first = batch.borrowMask().iterator().nextInt();
                    batch.constrain(Mask.sparse(new int[] {first}, 4));
                    assertThat(source.operatorConstraints).as(kind + " forwarded operator notifications").isZero();
                    assertThat(source.batchConstraints).as(kind + " forwarded batch notifications").isEqualTo(initialNotifications + 1);
                    assertThat(batch.borrowMask().count()).isEqualTo(1);
                    assertThat(VectorAccess.longValues(batch.output(0).borrow(Stream.VALUES)).value(first)).isEqualTo(1);
                }
            }
        }
    }

    private static final class TrackedSource
            implements Operator
    {
        private final Operator delegate;
        private int operatorConstraints;
        private int batchConstraints;

        private TrackedSource(Operator delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public int outputCount()
        {
            return delegate.outputCount();
        }

        @Override
        public Schema outputSchema()
        {
            return delegate.outputSchema();
        }

        @Override
        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        @Override
        public Batch next()
        {
            Batch batch = delegate.next();
            return Batch.forwarding(batch.borrowMask(), new Batch.Lifecycle()
            {
                @Override
                public void constrain(Mask mask)
                {
                    batchConstraints++;
                    batch.constrain(mask);
                }

                @Override
                public Mask takeMask(Mask mask)
                {
                    return batch.takeMask();
                }

                @Override
                public void releaseMask(Mask mask) {}

                @Override
                public void close()
                {
                    batch.close();
                }
            }, batch);
        }

        @Override
        public void constrain(Mask mask)
        {
            operatorConstraints++;
            delegate.constrain(mask);
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
