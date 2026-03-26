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
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.AvgStateVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.DistinctCountStateVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.MinUtf8StateVector;
import org.weakref.nitro.data.SumStateVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
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
    void testOutputCloseReleasesBorrowedButNotTakenStreams()
    {
        I64Vector values = new I64Vector(new long[] {11, 12, 13});
        BooleanVector nulls = new BooleanVector(new boolean[] {false, true, false});
        List<Vector> released = new ArrayList<>();
        Output output = new Output(
                Set.of(Stream.VALUES, Stream.NULLS),
                stream -> stream == Stream.VALUES ? values : nulls,
                (_, vector) -> vector,
                (_, vector) -> released.add(vector));

        assertThat(output.borrow(Stream.VALUES)).isSameAs(values);
        assertThat(output.take(Stream.NULLS)).isSameAs(nulls);

        output.close();

        assertThat(released).containsExactly(values);
    }

    @Test
    void testOptionalStreamAccessReturnsNullWhenStreamIsAbsent()
    {
        I64Vector values = new I64Vector(new long[] {11, 12, 13});
        Streams streams = Streams.ofValues(values);
        Output output = Output.of(streams);

        assertThat(streams.getOrNull(Stream.NULLS)).isNull();
        assertThat(output.borrowOrNull(Stream.NULLS)).isNull();
    }

    @Test
    void testSimpleBatchInvalidatesMaskAfterTake()
    {
        Mask mask = Mask.range(5, 3);
        Batch batch = new Batch(mask, Output.of(Streams.ofValues(new I64Vector(8))));

        assertThat(batch.borrowMask()).isSameAs(mask);
        assertThat(batch.takeMask()).isSameAs(mask);

        assertThatThrownBy(batch::borrowMask)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already taken");
    }

    @Test
    void testBatchCloseReleasesBorrowedMask()
    {
        Mask mask = Mask.range(5, 3);
        List<Mask> released = new ArrayList<>();
        Batch batch = new Batch(mask, _ -> {}, Function.identity(), released::add, () -> {}, Output.of(Streams.ofValues(new I64Vector(8))));

        assertThat(batch.borrowMask()).isSameAs(mask);

        batch.close();

        assertThat(released).containsExactly(mask);
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
    void testDictionaryVectorDereferencesBaseValues()
    {
        DictionaryVector dictionary = new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30}));

        assertThat(dictionary.length()).isEqualTo(4);
        assertThat(((I64Vector) dictionary.values()).values()[dictionary.ids()[0]]).isEqualTo(30L);
        assertThat(((I64Vector) dictionary.values()).values()[dictionary.ids()[1]]).isEqualTo(10L);
        assertThat(new DictionaryVector(Arrays.copyOf(dictionary.ids(), 3), dictionary.values()).ids()).containsExactly(2, 0, 1);
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
    void testAllocatorDoesNotReuseDifferentLogicalVectorLength()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("ExactVectorPool");

        I64Vector first = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
        allocator.release(context);

        I64Vector second = allocator.allocate(context, I64Vector.class, 4, I64Vector::new);

        assertThat(second).isNotSameAs(first);
        assertThat(second.length()).isEqualTo(4);
    }

    @Test
    void testAllocatorDoesNotReuseBinaryVectorWithDifferentLogicalLength()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("ExactBinaryVectorPool");

        BinaryVector first = BinaryVector.allocate(allocator, context, 8, 32);
        allocator.release(context);

        BinaryVector second = BinaryVector.allocate(allocator, context, 4, 16);

        assertThat(second).isNotSameAs(first);
        assertThat(second.length()).isEqualTo(4);
    }

    @Test
    void testBinaryVectorCopyMaskedPreservesSparseSelectedPositions()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("BinaryCopyMasked");

        BinaryVector values = new BinaryVector(5, 64);
        values.addTrait(BinaryVector.Trait.UTF8_STRING);
        values.addTrait(BinaryVector.Trait.ASCII_ONLY);
        values.setBytes(0, "zero".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(1, "one".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(2, "two".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(3, "three".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(4, "four".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BinaryVector copy = (BinaryVector) values.copyMasked(allocator, context, null, Mask.sparse(new int[] {1, 3, 4}, 5));

        assertThat(copy.utf8Value(1)).isEqualTo("one");
        assertThat(copy.utf8Value(3)).isEqualTo("three");
        assertThat(copy.utf8Value(4)).isEqualTo("four");
    }

    @Test
    void testMinUtf8StateVectorRetainedBytesAreCachedIncrementally()
    {
        MinUtf8StateVector state = new MinUtf8StateVector(4);

        assertThat(state.retainedBytes()).isEqualTo(4);

        state.setValue(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        state.setValue(1, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThat(state.retainedBytes()).isEqualTo(4 + "alpha".length() + "beta".length());

        state.setValue(0, "z".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThat(state.retainedBytes()).isEqualTo(4 + 1 + "beta".length());

        state.initialize(0, 2);
        assertThat(state.retainedBytes()).isEqualTo(4);

        MinUtf8StateVector grown = MinUtf8StateVector.grow(state, 8);
        assertThat(grown.retainedBytes()).isEqualTo(8);
    }

    @Test
    void testAvgStateVectorGrowPreservesValuesWithoutFlatCopy()
    {
        AvgStateVector state = new AvgStateVector(4);
        state.increment(0, 10, 1);
        state.increment(3, 40, 2);

        AvgStateVector grown = AvgStateVector.grow(state, 8);

        assertThat(grown.length()).isEqualTo(8);
        assertThat(grown.sum(0)).isEqualTo(10);
        assertThat(grown.count(0)).isEqualTo(1);
        assertThat(grown.sum(3)).isEqualTo(40);
        assertThat(grown.count(3)).isEqualTo(2);
        assertThat(grown.sum(7)).isZero();
        assertThat(grown.count(7)).isZero();
    }

    @Test
    void testSumStateVectorGrowPreservesValuesWithoutFlatCopy()
    {
        SumStateVector state = new SumStateVector(4);
        state.increment(0, 10);
        state.increment(3, 40);

        SumStateVector grown = SumStateVector.grow(state, 8);

        assertThat(grown.length()).isEqualTo(8);
        assertThat(grown.sum(0)).isEqualTo(10);
        assertThat(grown.isNull(0)).isFalse();
        assertThat(grown.sum(3)).isEqualTo(40);
        assertThat(grown.isNull(3)).isFalse();
        assertThat(grown.sum(7)).isZero();
        assertThat(grown.isNull(7)).isTrue();
    }

    @Test
    void testDistinctCountStateVectorGrowPreservesValuesWithoutFlatCopy()
    {
        DistinctCountStateVector state = new DistinctCountStateVector();
        state.incrementDistinctCount(0);
        state.incrementDistinctCount(3);
        state.incrementDistinctCount(3);

        state.ensureGroupCapacity(8);

        assertThat(state.length()).isEqualTo(8);
        assertThat(state.distinctCount(0)).isEqualTo(1);
        assertThat(state.distinctCount(3)).isEqualTo(2);
        assertThat(state.distinctCount(7)).isZero();
    }

    @Test
    void testAllocatorCapsFlatVectorPoolBucketSize()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("CappedVectorPool");

        I64Vector first = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
        I64Vector second = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
        I64Vector third = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);

        allocator.release(context);

        I64Vector reusedOne = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
        I64Vector reusedTwo = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
        I64Vector fresh = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);

        assertThat(reusedCount(List.of(first, second, third), List.of(reusedOne, reusedTwo, fresh))).isEqualTo(2);
    }

    @Test
    void testAllocatorCapsBinaryVectorPoolPerPositionCount()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("CappedBinaryVectorPool");

        BinaryVector first = BinaryVector.allocate(allocator, context, 8, 16);
        BinaryVector second = BinaryVector.allocate(allocator, context, 8, 32);
        BinaryVector third = BinaryVector.allocate(allocator, context, 8, 64);

        allocator.release(context);

        BinaryVector reusedOne = BinaryVector.allocate(allocator, context, 8, 8);
        BinaryVector reusedTwo = BinaryVector.allocate(allocator, context, 8, 24);
        BinaryVector fresh = BinaryVector.allocate(allocator, context, 8, 48);

        assertThat(reusedCount(List.of(first, second, third), List.of(reusedOne, reusedTwo, fresh))).isEqualTo(2);
    }

    @Test
    void testAllocatorDoesNotPoolSupersededGrowthVectors()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("DiscardGrowthVectors");

        I64Vector first = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
        I64Vector second = allocator.allocateOrGrow(context, first, I64Vector.class, 16, I64Vector::new);
        I64Vector third = allocator.allocateOrGrow(context, second, I64Vector.class, 32, I64Vector::new);

        allocator.release(context);

        I64Vector reusedFinal = allocator.allocate(context, I64Vector.class, 32, I64Vector::new);
        I64Vector freshSmaller = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
        I64Vector freshMiddle = allocator.allocate(context, I64Vector.class, 16, I64Vector::new);

        assertThat(reusedFinal).isSameAs(third);
        assertThat(freshSmaller).isNotSameAs(first);
        assertThat(freshMiddle).isNotSameAs(second);
    }

    @Test
    void testAllocatorDoesNotPoolOversizedSumStateVector()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("LargeSumStatePool");

        SumStateVector first = allocator.allocate(context, SumStateVector.class, 1_000_000, SumStateVector::new);
        allocator.release(context);

        SumStateVector second = allocator.allocate(context, SumStateVector.class, 1_000_000, SumStateVector::new);

        assertThat(second).isNotSameAs(first);
    }

    @Test
    void testAllocatorDoesNotPoolOversizedAvgStateVector()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("LargeAvgStatePool");

        AvgStateVector first = allocator.allocate(context, AvgStateVector.class, 600_000, AvgStateVector::new);
        allocator.release(context);

        AvgStateVector second = allocator.allocate(context, AvgStateVector.class, 600_000, AvgStateVector::new);

        assertThat(second).isNotSameAs(first);
    }

    @Test
    void testAllocatorComputedCapacityNeverDropsBelowRequestedSize()
    {
        assertThat(Allocator.computeCapacity(0)).isEqualTo(0);
        assertThat(Allocator.computeCapacity(1)).isGreaterThanOrEqualTo(1);
        assertThat(Allocator.computeCapacity(16)).isGreaterThanOrEqualTo(16);
        assertThat(Allocator.computeCapacity(156)).isGreaterThanOrEqualTo(156);
        assertThat(Allocator.computeCapacity(512)).isGreaterThanOrEqualTo(512);
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
    void testTransferDetachesMaskFromItsOwningContext()
    {
        Allocator allocator = new Allocator();
        Allocator.Context owner = new Allocator.Context("Owner");
        Allocator.Context borrower = new Allocator.Context("Borrower");

        Mask mask = allocator.allocateRangeMask(owner, 3, 2);
        allocator.transfer(borrower, mask);
        allocator.release(owner);

        Mask allocated = allocator.allocateRangeMask(owner, 0, 2);

        assertThat(allocated).isNotSameAs(mask);
    }

    @Test
    void testTransferDetachesVectorFromItsOwningContext()
    {
        Allocator allocator = new Allocator();
        Allocator.Context owner = new Allocator.Context("Owner");
        Allocator.Context borrower = new Allocator.Context("Borrower");

        I64Vector vector = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);
        allocator.transfer(borrower, vector);
        allocator.release(owner);

        I64Vector allocated = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);

        assertThat(allocated).isNotSameAs(vector);
    }

    @Test
    void testTransferDetachesNestedChildVectorsFromTheirOwningContext()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("Nested");

        ArrayVector array = allocator.allocateArray(context, 2);
        array.offsets()[1] = 2;
        array.offsets()[2] = 3;

        I64Vector childValues = allocator.allocate(context, I64Vector.class, 3, I64Vector::new);
        childValues.values()[0] = 10;
        childValues.values()[1] = 20;
        childValues.values()[2] = 30;

        BooleanVector childNulls = allocator.allocate(context, BooleanVector.class, 3, BooleanVector::new);
        childNulls.values()[1] = true;
        array.setElements(Streams.ofValues(childValues).with(Stream.NULLS, childNulls));

        Output output = new Output(Set.of(Stream.VALUES), stream -> array, (stream, vector) -> allocator.transfer(context, vector));
        ArrayVector taken = (ArrayVector) output.take(Stream.VALUES);
        allocator.release(context);

        I64Vector allocatedValues = allocator.allocate(context, I64Vector.class, 3, I64Vector::new);
        BooleanVector allocatedNulls = allocator.allocate(context, BooleanVector.class, 3, BooleanVector::new);

        assertThat(allocatedValues).isNotSameAs(taken.elementValues());
        assertThat(allocatedNulls).isNotSameAs(taken.elementNulls());
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

    private static int reusedCount(List<?> pooled, List<?> allocated)
    {
        int count = 0;
        for (Object candidate : allocated) {
            if (pooled.stream().anyMatch(vector -> vector == candidate)) {
                count++;
            }
        }
        return count;
    }
}
