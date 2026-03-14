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
package org.weakref.nitro;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.List;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.data.Row.row;

public class TestBatchRuntime
{
    @Test
    void testSimpleOutputAllowsRepeatedBorrowButInvalidatesAfterTake()
    {
        I64Vector values = new I64Vector(new long[] {11, 12, 13});
        BooleanVector nulls = new BooleanVector(new boolean[] {false, true, false});
        Output output = Output.of(Streams.of(Stream.VALUES, values).with(Stream.NULLS, nulls));

        assertThat(output.borrow(Stream.VALUES)).isSameAs(values);
        assertThat(output.borrow(Stream.VALUES)).isSameAs(values);
        assertThat(output.take(Stream.NULLS)).isSameAs(nulls);
        assertThat(output.borrow(Stream.VALUES)).isSameAs(values);

        assertThatThrownBy(() -> output.borrow(Stream.NULLS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already taken");
    }

    @Test
    void testSimpleBatchInvalidatesMaskAfterTake()
    {
        Mask mask = Mask.range(5, 3);
        Batch batch = new Batch(mask, Output.values(new I64Vector(8)));

        assertThat(batch.borrowMask()).isSameAs(mask);
        assertThat(batch.takeMask()).isSameAs(mask);

        assertThatThrownBy(batch::borrowMask)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already taken");
    }

    @Test
    void testAllMaskTracksRowDomainWithoutMaterializedPositions()
    {
        Mask mask = Mask.all(4);

        assertThat(mask.size()).isEqualTo(4);
        assertThat(mask.count()).isEqualTo(4);
        assertThat(mask.all()).isTrue();
        assertThat(mask.none()).isFalse();
        assertThat(mask.maxPosition()).isEqualTo(3);
        assertThat(positions(mask)).containsExactly(0, 1, 2, 3);
        assertThat(mask.complement().none()).isTrue();
    }

    @Test
    void testRangeMaskKeepsOriginalRowDomain()
    {
        Mask mask = Mask.range(5, 3);

        assertThat(mask.size()).isEqualTo(8);
        assertThat(mask.count()).isEqualTo(3);
        assertThat(mask.all()).isFalse();
        assertThat(mask.position(0)).isEqualTo(5);
        assertThat(mask.position(2)).isEqualTo(7);
        assertThat(mask.contains(6)).isTrue();
        assertThat(mask.contains(4)).isFalse();
    }

    @Test
    void testOwnedMasksSupportInPlaceRefinement()
    {
        Mask mask = Mask.all(6);
        mask.intersectInPlace(Mask.sparse(new int[] {1, 2, 4}, 6));

        assertThat(mask.size()).isEqualTo(6);
        assertThat(mask.count()).isEqualTo(3);
        assertThat(positions(mask)).containsExactly(1, 2, 4);

        mask.differenceInPlace(Mask.sparse(new int[] {2}, 6));
        assertThat(positions(mask)).containsExactly(1, 4);
    }

    @Test
    void testAllocatorTracksDerivedMaskAllocations()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("MaskTest");

        Mask all = allocator.allocateAllMask(context, 6);
        BooleanVector predicate = new BooleanVector(new boolean[] {false, true, false, true, false, false});
        Mask filtered = allocator.intersectMask(context, all, predicate);
        Mask tail = allocator.allocateRangeMask(context, 4, 2);
        Mask combined = allocator.unionMask(context, filtered, tail);

        assertThat(positions(filtered)).containsExactly(1, 3);
        assertThat(positions(combined)).containsExactly(1, 3, 4, 5);
        assertThat(allocator.currentBytes(context)).isEqualTo((2L + 2L + 4L) * Integer.BYTES);
    }

    @Test
    void testAllocatorReusesVectorInstancesAfterRelease()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("VectorPool");

        I64Vector first = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
        long totalBytes = allocator.totalBytes(context);

        allocator.release(context);

        I64Vector second = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);

        assertThat(second).isSameAs(first);
        assertThat(allocator.totalBytes(context)).isEqualTo(totalBytes);
    }

    @Test
    void testAllocatorReusesMaskInstancesAfterRelease()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("MaskPool");

        Mask first = allocator.allocateRangeMask(context, 4, 3);
        long totalBytes = allocator.totalBytes(context);

        allocator.release(context);

        Mask second = allocator.allocateSparseMask(context, new int[] {1, 5}, 6);

        assertThat(second).isSameAs(first);
        assertThat(positions(second)).containsExactly(1, 5);
        assertThat(allocator.totalBytes(context)).isEqualTo(totalBytes);
    }

    @Test
    void testTransferredVectorIsNotReturnedToAllocatorPool()
    {
        Allocator allocator = new Allocator();
        Operator operator = new GeneratorOperator(allocator, 4, 4, List.of(new SequenceGenerator(0)));

        Batch batch = operator.next();
        I64Vector taken = (I64Vector) batch.output(0).take(Stream.VALUES);
        operator.close();

        I64Vector allocated = allocator.allocate(new Allocator.Context("GeneratorOperator"), I64Vector.class, 4, I64Vector::new);

        assertThat(allocated).isNotSameAs(taken);
    }

    @Test
    void testTransferredMaskIsNotReturnedToAllocatorPool()
    {
        Allocator allocator = new Allocator();
        Operator operator = new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L)));

        Batch batch = operator.next();
        Mask taken = batch.takeMask();
        operator.close();

        Mask allocated = allocator.allocateAllMask(new Allocator.Context("ConstantTableOperator"), 2);

        assertThat(allocated).isNotSameAs(taken);
    }

    @Test
    void testOperatorOutputsRespectBorrowAndTakeSemantics()
    {
        Allocator allocator = new Allocator();
        Operator operator = new ConstantTableOperator(
                allocator,
                2,
                List.of(
                        row(1L, 10L),
                        row(2L, 20L),
                        row(3L, 30L)));

        assertThat(operator.hasNext()).isTrue();

        Batch batch = operator.next();
        Output firstOutput = batch.output(0);
        Output secondOutput = batch.output(1);

        assertThat(((I64Vector) firstOutput.borrow(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
        assertThat(((I64Vector) firstOutput.borrow(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
        assertThat(((I64Vector) secondOutput.take(Stream.VALUES)).values()).containsExactly(10L, 20L, 30L);

        assertThat(((BooleanVector) firstOutput.borrow(Stream.NULLS)).values()).containsExactly(false, false, false);
        assertThat(((BooleanVector) secondOutput.borrow(Stream.NULLS)).values()).containsExactly(false, false, false);

        assertThatThrownBy(() -> secondOutput.borrow(Stream.VALUES))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already taken");

        operator.close();
    }

    private static List<Integer> positions(Mask mask)
    {
        return StreamSupport.stream(mask.spliterator(), false)
                .toList();
    }
}
