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
package org.weakref.nitro.operator.source;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.batch.ColumnCapability;
import org.weakref.nitro.core.batch.ColumnEncoding;
import org.weakref.nitro.core.batch.ColumnTraits;
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.OrdinalSourceColumnHandle;
import org.weakref.nitro.core.source.RuntimeFilter;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.ValueDemand;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestColumnViewSourceOperatorIngress
{
    private static final Schema SCHEMA = Schema.unspecified(List.of("value"));

    @Test
    void testPreservesKnownEmptySourceOutputDemand()
    {
        try (Operator operator = new BatchSourceOperator(
                new SingleBatchSource(null),
                new SourceOperatorIngress()
                {
                    @Override
                    public Batch adapt(SourceBatch batch)
                    {
                        throw new UnsupportedOperationException();
                    }
                })) {
            assertThat(operator.sourceOutputDemand(Map.of())).contains(Map.of());
            assertThat(operator.sourceOutputDemand(Map.of(0, ValueDemand.FULL)))
                    .contains(Map.of(0, ValueDemand.FULL));
            assertThatThrownBy(() -> operator.sourceOutputDemand(Map.of(1, ValueDemand.FULL)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("demanded output is outside source schema: 1");
        }
    }

    @Test
    void testConstructedColumnBindingPreservesLazinessSelectionAndLifetime()
    {
        AtomicInteger columnRequests = new AtomicInteger();
        AtomicReference<Selection> selected = new AtomicReference<>();
        AtomicBoolean sourceBatchClosed = new AtomicBoolean();
        SourceBatch sourceBatch = new SourceBatch()
        {
            private final ColumnView column = new TestingColumnView(SCHEMA.field(0).type(), new long[] {11, 22, 33});

            @Override
            public Schema schema()
            {
                return SCHEMA;
            }

            @Override
            public Selection selection()
            {
                return new PositionsSelection(new int[] {0, 1, 2}, 3);
            }

            @Override
            public ColumnView column(int index)
            {
                assertThat(index).isZero();
                columnRequests.incrementAndGet();
                return column;
            }

            @Override
            public void select(Selection selection)
            {
                selected.set(selection);
            }

            @Override
            public void close()
            {
                sourceBatchClosed.set(true);
            }
        };
        BatchSource source = new SingleBatchSource(sourceBatch);
        SelectionOperatorIngress selectionIngress = new SelectionOperatorIngress()
        {
            @Override
            public Mask toMask(Selection selection)
            {
                int[] positions = new int[selection.count()];
                for (int index = 0; index < positions.length; index++) {
                    positions[index] = selection.position(index);
                }
                return Mask.sparse(positions, selection.maxPosition() + 1);
            }

            @Override
            public Selection toSelection(Mask mask)
            {
                int[] positions = new int[mask.count()];
                for (int index = 0; index < positions.length; index++) {
                    positions[index] = mask.position(index);
                }
                return new PositionsSelection(positions, mask.size());
            }
        };
        ColumnViewOperatorIngress columnIngress = new ColumnViewOperatorIngress()
        {
            @Override
            public TypeBinding type()
            {
                return SCHEMA.field(0).type();
            }

            @Override
            public Output output(java.util.function.Supplier<ColumnView> column)
            {
                return new Output(
                        Set.of(Stream.VALUES),
                        _ -> new I64Vector(column.get().capability(LongArrayCapability.LONG_ARRAY).orElseThrow()));
            }
        };
        Operator operator = new BatchSourceOperator(
                source,
                new ColumnViewSourceOperatorIngress(
                        SCHEMA,
                        selectionIngress,
                        new RuntimeFilterSourceIngress()
                        {
                            @Override
                            public boolean supports(BatchSource source, SourceColumnHandle column)
                            {
                                return false;
                            }

                            @Override
                            public RuntimeFilter runtimeFilter(SourceColumnHandle column, DynamicFilter filter)
                            {
                                throw new UnsupportedOperationException();
                            }
                        },
                        _ -> columnIngress));

        assertThat(operator.hasNext()).isTrue();
        try (Batch batch = operator.next()) {
            assertThat(columnRequests).hasValue(0);
            batch.constrain(Mask.sparse(new int[] {1}, 3));
            assertThat(selected.get().count()).isOne();
            assertThat(selected.get().position(0)).isEqualTo(1);
            assertThat(columnRequests).hasValue(0);

            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            assertThat(values.values()).containsExactly(11, 22, 33);
            assertThat(columnRequests).hasValue(1);

            batch.constrain(Mask.sparse(new int[] {2}, 3));
            assertThat(selected.get().position(0)).isEqualTo(2);
            I64Vector rebound = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            assertThat(rebound.values()).containsExactly(11, 22, 33);
            assertThat(columnRequests).hasValue(2);
            assertThat(sourceBatchClosed).isFalse();
        }
        assertThat(sourceBatchClosed).isTrue();
        assertThat(operator.hasNext()).isFalse();
    }

    private enum LongArrayCapability
            implements ColumnCapability<long[]>
    {
        LONG_ARRAY;

        @Override
        public Class<long[]> valueType()
        {
            return long[].class;
        }
    }

    private record TestingColumnView(TypeBinding type, long[] values)
            implements ColumnView
    {
        @Override
        public int positionCount()
        {
            return values.length;
        }

        @Override
        public ColumnTraits traits()
        {
            return new ColumnTraits(ColumnEncoding.FLAT, false, true, true);
        }

        @Override
        public Set<Stream> streams()
        {
            return Set.of(Stream.VALUES);
        }

        @Override
        public org.weakref.nitro.data.Vector borrow(Stream stream)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.weakref.nitro.data.Vector take(Stream stream)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Optional<T> capability(ColumnCapability<T> capability)
        {
            if (capability == LongArrayCapability.LONG_ARRAY) {
                return Optional.of(capability.valueType().cast(values));
            }
            return Optional.empty();
        }
    }

    private record PositionsSelection(int[] positions, int size)
            implements Selection
    {
        @Override
        public int positionCount()
        {
            return size;
        }

        @Override
        public int count()
        {
            return positions.length;
        }

        @Override
        public int maxPosition()
        {
            return positions.length == 0 ? -1 : positions[positions.length - 1];
        }

        @Override
        public boolean isDense()
        {
            return positions.length == size;
        }

        @Override
        public int position(int index)
        {
            return positions[index];
        }
    }

    private static final class SingleBatchSource
            implements BatchSource
    {
        private final SourceBatch batch;
        private final SourceColumnHandle column = new OrdinalSourceColumnHandle(0, SCHEMA.field(0).type());
        private boolean emitted;

        private SingleBatchSource(SourceBatch batch)
        {
            this.batch = batch;
        }

        @Override
        public Schema schema()
        {
            return SCHEMA;
        }

        @Override
        public SourceColumnHandle column(int outputIndex)
        {
            if (outputIndex != 0) {
                throw new IndexOutOfBoundsException(outputIndex);
            }
            return column;
        }

        @Override
        public Set<SourceCapability> capabilities()
        {
            return Set.of(SourceCapability.LAZY_COLUMNS, SourceCapability.SELECTION_PUSHDOWN);
        }

        @Override
        public SourcePoll poll()
        {
            if (emitted) {
                return SourcePoll.Finished.FINISHED;
            }
            emitted = true;
            return new SourcePoll.Ready(batch);
        }

        @Override
        public void close() {}
    }
}
