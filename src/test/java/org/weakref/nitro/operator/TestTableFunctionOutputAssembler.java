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
import org.weakref.nitro.core.batch.BatchCapability;
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.function.table.TableFunctionOutputBatch;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorBatchScope;
import org.weakref.nitro.data.VectorColumnGeneration;
import org.weakref.nitro.data.VectorSourceBatch;
import org.weakref.nitro.execution.EngineResources;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

final class TestTableFunctionOutputAssembler
{
    private static final TypeBinding BIGINT = bigintType();
    private static final Schema INPUT_SCHEMA = new Schema(List.of(new Field("input", BIGINT, true)));
    private static final Schema FUNCTION_OUTPUT_SCHEMA = new Schema(List.of(
            new Field("proper", BIGINT, true),
            new Field("reference", BIGINT, true)));
    private static final Schema FINAL_OUTPUT_SCHEMA = new Schema(List.of(
            new Field("proper", BIGINT, true),
            new Field("pass_through", BIGINT, true)));

    @Test
    void testLazilyGathersPartitionRelativeRows()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                EncodedRowBuffer rows = new EncodedRowBuffer(allocator, new Allocator.Context("rows"), 1)) {
            rows.load(TableOperator.retained(
                    INPUT_SCHEMA,
                    List.of(TableOperator.Page.values(
                            4,
                            new I64Vector[] {new I64Vector(new long[] {10, 20, 30, 40})},
                            Mask.all(4)))));
            AtomicInteger inputReads = new AtomicInteger();
            RowPositionIndex observedRows = observing(rows, inputReads);

            TableFunctionOutputBatch output = output(
                    allocator,
                    new long[] {100, 200, 300},
                    new long[] {2, 0, 1},
                    new boolean[] {false, false, true},
                    Mask.all(3));
            TableFunctionOutputAssembler assembler = new TableFunctionOutputAssembler(
                    allocator,
                    FINAL_OUTPUT_SCHEMA,
                    1,
                    List.of(new TableFunctionOutputAssembler.PassThroughColumn(0, 0)),
                    List.of(new TableFunctionOutputAssembler.Argument(INPUT_SCHEMA, observedRows)),
                    new int[] {1},
                    new int[] {4});

            try (Batch result = assembler.assemble(output)) {
                assertThat(inputReads).hasValue(0);
                assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values())
                        .containsExactly(100, 200, 300);
                assertThat(inputReads).hasValue(0);

                assertThat(((I64Vector) result.output(1).borrow(Stream.VALUES)).values())
                        .containsExactly(40, 20, 0);
                assertThat(((BooleanVector) result.output(1).borrow(Stream.NULLS)).values())
                        .containsExactly(false, false, true);
                assertThat(inputReads).hasValue(2);
            }
        }
    }

    @Test
    void testPreservesSparseOutputPositions()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                EncodedRowBuffer rows = new EncodedRowBuffer(allocator, new Allocator.Context("rows"), 1)) {
            rows.load(TableOperator.retained(
                    INPUT_SCHEMA,
                    List.of(TableOperator.Page.values(
                            3,
                            new I64Vector[] {new I64Vector(new long[] {11, 22, 33})},
                            Mask.all(3)))));
            TableFunctionOutputBatch output = output(
                    allocator,
                    new long[] {100, 999, 300},
                    new long[] {2, 99, 0},
                    new boolean[] {false, false, false},
                    Mask.sparse(new int[] {0, 2}, 3));
            TableFunctionOutputAssembler assembler = new TableFunctionOutputAssembler(
                    allocator,
                    FINAL_OUTPUT_SCHEMA,
                    1,
                    List.of(new TableFunctionOutputAssembler.PassThroughColumn(0, 0)),
                    List.of(new TableFunctionOutputAssembler.Argument(INPUT_SCHEMA, rows)),
                    new int[] {0},
                    new int[] {3});

            try (Batch result = assembler.assemble(output)) {
                assertThat(result.borrowMask().selectedPositions()).containsExactly(0, 2);
                assertThat(((I64Vector) result.output(1).borrow(Stream.VALUES)).values())
                        .containsExactly(33, 0, 11);
            }
        }
    }

    private static TableFunctionOutputBatch output(
            Allocator allocator,
            long[] proper,
            long[] references,
            boolean[] referenceNulls,
            Mask selection)
    {
        VectorBatchScope buffers = new VectorBatchScope(allocator, "function-output");
        I64Vector properVector = allocator.allocate(buffers.context(), I64Vector.class, proper.length, I64Vector::new);
        System.arraycopy(proper, 0, properVector.values(), 0, proper.length);
        I64Vector referenceVector = allocator.allocate(buffers.context(), I64Vector.class, references.length, I64Vector::new);
        System.arraycopy(references, 0, referenceVector.values(), 0, references.length);
        BooleanVector nulls = allocator.allocate(buffers.context(), BooleanVector.class, referenceNulls.length, BooleanVector::new);
        System.arraycopy(referenceNulls, 0, nulls.values(), 0, referenceNulls.length);
        Mask ownedSelection = selection.all()
                ? allocator.allocateRangeMask(buffers.context(), 0, selection.size())
                : allocator.allocateSparseMask(buffers.context(), selection.selectedPositions(), selection.size());
        SourceBatch delegate = new VectorSourceBatch(
                FUNCTION_OUTPUT_SCHEMA,
                ownedSelection,
                new VectorColumnGeneration[] {
                        new VectorColumnGeneration(Set.of(Stream.VALUES), _ -> properVector, buffers),
                        new VectorColumnGeneration(
                                Set.of(Stream.VALUES, Stream.NULLS),
                                stream -> stream == Stream.VALUES ? referenceVector : nulls,
                                buffers),
                },
                buffers,
                _ -> {},
                () -> {});
        return new OutputBatch(delegate);
    }

    private static RowPositionIndex observing(RowPositionIndex delegate, AtomicInteger reads)
    {
        return new RowPositionIndex()
        {
            @Override
            public int size()
            {
                return delegate.size();
            }

            @Override
            public Streams column(int column, int position)
            {
                reads.incrementAndGet();
                return delegate.column(column, position);
            }

            @Override
            public int sourcePosition(int position)
            {
                return delegate.sourcePosition(position);
            }

            @Override
            public boolean sharesSource(int leftPosition, int rightPosition)
            {
                return delegate.sharesSource(leftPosition, rightPosition);
            }
        };
    }

    private static TypeBinding bigintType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:bigint");
            }

            @Override
            public Class<?> carrierType()
            {
                return long.class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Optional<TypeVectorFactory> vectorFactory()
            {
                return Optional.of(new TypeVectorFactory()
                {
                    @Override
                    public Vector constant(org.weakref.nitro.data.VectorAllocator allocator, Object value, int length)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Vector nullValues(org.weakref.nitro.data.VectorAllocator allocator, int length)
                    {
                        return allocator.allocate(I64Vector.class, length, I64Vector::new);
                    }
                });
            }
        };
    }

    private record OutputBatch(SourceBatch delegate)
            implements TableFunctionOutputBatch
    {
        @Override
        public int properOutputCount()
        {
            return 1;
        }

        @Override
        public List<PassThroughReference> passThroughReferences()
        {
            return List.of(new PassThroughReference(0));
        }

        @Override
        public Schema schema()
        {
            return delegate.schema();
        }

        @Override
        public Selection selection()
        {
            return delegate.selection();
        }

        @Override
        public ColumnView column(int index)
        {
            return delegate.column(index);
        }

        @Override
        public void select(Selection selection)
        {
            delegate.select(selection);
        }

        @Override
        public <T> Optional<T> capability(BatchCapability<T> capability)
        {
            return delegate.capability(capability);
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
