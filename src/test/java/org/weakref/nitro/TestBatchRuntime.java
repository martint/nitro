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
import org.weakref.nitro.data.DoubleStateVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.MinUtf8StateVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.SumStateVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.data.Row.row;

public class TestBatchRuntime
{
    @Test
    void testBooleanVectorConstantMetadata()
    {
        BooleanVector values = new BooleanVector(4);
        values.markAllTrue();
        assertThat(values.values()).containsExactly(true, true, true, true);
        assertThat(values.isAllTrue()).isTrue();
        assertThat(values.isAllFalse()).isFalse();
        assertThat(VectorAccess.isAllTrueNulls(values)).isTrue();

        values.clearForReuse();
        assertThat(values.isAllTrue()).isFalse();
        assertThat(values.isAllFalse()).isTrue();

        values.markAllFalse();
        assertThat(values.isAllTrue()).isFalse();
        assertThat(values.isAllFalse()).isTrue();
    }

    @Test
    void testStreamsReuseTransportTupleWhenBackingVectorsAreUnchanged()
    {
        I64Vector values = new I64Vector(8);
        BooleanVector nulls = new BooleanVector(8);
        Streams existing = Streams.ofValuesAndNulls(values, nulls);

        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            assertThat(allocator.reuseValuesAndNulls(existing, values, nulls)).isSameAs(existing);

            I64Vector grownValues = new I64Vector(16);
            Streams grown = allocator.reuseValuesAndNulls(existing, grownValues, nulls);
            assertThat(grown).isNotSameAs(existing);
            assertThat(grown.values()).isSameAs(grownValues);
            assertThat(grown.get(Stream.NULLS)).isSameAs(nulls);
        }
    }

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
    void testConstraintInvalidatesResolvedOutputBeforeReborrow()
    {
        List<Vector> resolved = new ArrayList<>();
        List<Vector> released = new ArrayList<>();
        Output output = new Output(
                Set.of(Stream.VALUES),
                _ -> {
                    I64Vector vector = new I64Vector(new long[] {resolved.size()});
                    resolved.add(vector);
                    return vector;
                },
                (_, vector) -> vector,
                (_, vector) -> released.add(vector))
                .withConstraintSensitiveResolution();
        Batch batch = new Batch(Mask.all(1), _ -> {}, Function.identity(), _ -> {}, () -> {}, output);

        Vector first = output.borrow(Stream.VALUES);
        batch.constrain(Mask.all(1));
        Vector second = output.borrow(Stream.VALUES);

        assertThat(second).isNotSameAs(first);
        assertThat(released).containsExactly(first);
        batch.close();
        assertThat(released).containsExactly(first, second);
    }

    @Test
    void testConstraintRejectsTakenOutputWithoutInvalidatingOtherOutputs()
    {
        I64Vector firstValues = new I64Vector(new long[] {1});
        I64Vector secondValues = new I64Vector(new long[] {2});
        Output first = Output.of(Streams.ofValues(firstValues)).withConstraintSensitiveResolution();
        Output second = Output.of(Streams.ofValues(secondValues)).withConstraintSensitiveResolution();
        Batch batch = new Batch(Mask.all(1), first, second);

        assertThat(first.borrow(Stream.VALUES)).isSameAs(firstValues);
        assertThat(second.take(Stream.VALUES)).isSameAs(secondValues);
        assertThatThrownBy(() -> batch.constrain(Mask.all(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("output stream was taken");
        assertThat(first.borrow(Stream.VALUES)).isSameAs(firstValues);
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
    void testAllocatorRetainsObservedConcurrentVectorWorkingSet()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("wide-project-working-set");
        List<I64Vector> firstGeneration = new ArrayList<>();
        for (int index = 0; index < 90; index++) {
            firstGeneration.add(allocator.allocate(context, I64Vector.class, 1_024, I64Vector::new));
        }
        firstGeneration.forEach(vector -> allocator.release(context, vector));

        List<I64Vector> secondGeneration = new ArrayList<>();
        for (int index = 0; index < 90; index++) {
            secondGeneration.add(allocator.allocate(context, I64Vector.class, 1_024, I64Vector::new));
        }

        assertThat(secondGeneration).containsExactlyInAnyOrderElementsOf(firstGeneration);
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
    void testComplementMaskSupportsRandomPositionLookup()
    {
        Mask mask = Mask.sparse(new int[] {0, 2, 3, 7, 12, 19}, 20).complement();

        assertThat(mask.size()).isEqualTo(20);
        assertThat(mask.count()).isEqualTo(14);
        assertThat(mask.position(5)).isEqualTo(9);
        assertThat(mask.position(0)).isEqualTo(1);
        assertThat(mask.position(13)).isEqualTo(18);
        assertThat(positions(mask)).containsExactly(1, 4, 5, 6, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18);
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
    void testDictionaryVectorSupportsLogicalLengthOverSharedIds()
    {
        int[] ids = {2, 0, 1, 2, 99};
        DictionaryVector dictionary = DictionaryVector.wrap(ids, 4, new I64Vector(new long[] {10, 20, 30}));

        assertThat(dictionary.length()).isEqualTo(4);
        assertThat(dictionary.ids()).isSameAs(ids);
        assertThat(((I64Vector) dictionary.values()).values()[dictionary.ids()[0]]).isEqualTo(30L);

        DictionaryVector nested = DictionaryVector.wrap(new int[] {3, 1}, dictionary);
        assertThat(nested.length()).isEqualTo(2);
        assertThat(nested.ids()).containsExactly(2, 0);
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
    void testAllocatorDifferenceFromAllKeepsLogicalComplement()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("MaskComplement");

        Mask remaining = allocator.differenceMask(context, Mask.all(8), Mask.sparse(new int[] {1, 3, 6}, 8));

        assertThat(remaining.size()).isEqualTo(8);
        assertThat(remaining.selectedCount()).isEqualTo(5);
        assertThat(remaining.all()).isFalse();
        assertThat(remaining.contains(3)).isFalse();
        assertThat(remaining.contains(4)).isTrue();
        assertThat(remaining.position(0)).isEqualTo(0);
        assertThat(remaining.position(4)).isEqualTo(7);
        assertThat(positions(remaining)).containsExactly(0, 2, 4, 5, 7);
        assertThat(positions(remaining.complement())).containsExactly(1, 3, 6);
        assertThat(allocator.currentBytes(context)).isEqualTo(5L * Integer.BYTES);
    }

    @Test
    void testAllocatorSparseMaskTakesOneOwnedCopy()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("SparseMaskCopy");
        int[] scratch = {1, 3, 6, 99};

        Mask mask = allocator.allocateSparseMask(context, scratch, 3, 8);
        scratch[0] = 7;

        assertThat(positions(mask)).containsExactly(1, 3, 6);
        assertThat(mask.positionsArrayForOverwrite(3)).hasSize(3);
    }

    @Test
    void testAllocatorReleasesOnlyUnreferencedEncodedChildren()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("ReplacementTree");
        I32Vector ids = allocator.allocate(context, I32Vector.class, 8, I32Vector::new);
        I64Vector values = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
        DictionaryVector source = allocator.adopt(context, DictionaryVector.wrapOwnedIds(ids, 8, values));

        allocator.releaseUnreferenced(context, source, Set.of(values));

        assertThat(allocator.allocate(context, I32Vector.class, 8, I32Vector::new)).isSameAs(ids);
        assertThat(allocator.allocate(context, I64Vector.class, 8, I64Vector::new)).isNotSameAs(values);
    }

    @Test
    void testAllocatorTracksDerivedMaskAllocations()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("BinaryCopyMasked");

        BinaryVector values = new BinaryVector(5, 64);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        values.setBytes(0, "zero".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(1, "one".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(2, "two".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(3, "three".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(4, "four".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BinaryVector copy = (BinaryVector) values.copyMasked(allocator, context, null, Mask.sparse(new int[] {1, 3, 4}, 5));

        assertThat(copy.copyBytes(1)).isEqualTo("one".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThat(copy.copyBytes(3)).isEqualTo("three".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThat(copy.copyBytes(4)).isEqualTo("four".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    void testVariableWidthMaskedCopiesPreserveInterleavedBranches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("VariableWidthMaskedMerge");

        BinaryVector trueValues = new BinaryVector(5, 32);
        trueValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        trueValues.setBytes(0, "zero".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        trueValues.setBytes(1, "one".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        trueValues.setBytes(2, "two".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        trueValues.setBytes(3, "three".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        trueValues.setBytes(4, "four".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        BinaryVector empty = new BinaryVector(1, 0);
        empty.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        RleVector falseValues = new RleVector(new int[] {4}, empty);

        Vector merged = trueValues.copyMasked(
                allocator,
                context,
                null,
                Mask.sparse(new int[] {1, 3, 4}, 5));
        merged = falseValues.copyMasked(
                allocator,
                context,
                merged,
                Mask.sparse(new int[] {0, 2}, 4));

        BinaryVector result = (BinaryVector) merged;
        assertThat(new String(result.copyBytes(0), java.nio.charset.StandardCharsets.UTF_8)).isEmpty();
        assertThat(new String(result.copyBytes(1), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("one");
        assertThat(new String(result.copyBytes(2), java.nio.charset.StandardCharsets.UTF_8)).isEmpty();
        assertThat(new String(result.copyBytes(3), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("three");
        assertThat(new String(result.copyBytes(4), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("four");

        BinaryVector dictionaryValues = new BinaryVector(2, 8);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        dictionaryValues.setBytes(0, "unused".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(1, new byte[0]);
        DictionaryVector dictionary = new DictionaryVector(new int[] {1, 0, 1, 0}, dictionaryValues);

        merged = trueValues.copyMasked(
                allocator,
                context,
                null,
                Mask.sparse(new int[] {1, 3, 4}, 5));
        merged = dictionary.copyMasked(
                allocator,
                context,
                merged,
                Mask.sparse(new int[] {0, 2}, 4));

        result = (BinaryVector) merged;
        assertThat(new String(result.copyBytes(0), java.nio.charset.StandardCharsets.UTF_8)).isEmpty();
        assertThat(new String(result.copyBytes(1), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("one");
        assertThat(new String(result.copyBytes(2), java.nio.charset.StandardCharsets.UTF_8)).isEmpty();
        assertThat(new String(result.copyBytes(3), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("three");
        assertThat(new String(result.copyBytes(4), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("four");
    }

    @Test
    void testStructMaskedCopiesPreserveInterleavedBranches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("StructMaskedMerge");

        StructVector trueValues = new StructVector(5);
        trueValues.setField("high", Streams.ofValues(new I64Vector(new long[] {10, 11, 12, 13, 14})));
        trueValues.setField("low", Streams.ofValues(new I64Vector(new long[] {20, 21, 22, 23, 24})));

        StructVector falseValues = new StructVector(5);
        falseValues.setField("high", Streams.ofValues(new I64Vector(new long[5])));
        falseValues.setField("low", Streams.ofValues(new I64Vector(new long[5])));

        Vector merged = trueValues.copyMasked(
                allocator,
                context,
                null,
                Mask.sparse(new int[] {1, 3, 4}, 5));
        merged = falseValues.copyMasked(
                allocator,
                context,
                merged,
                Mask.sparse(new int[] {0, 2}, 5));

        StructVector result = (StructVector) merged;
        assertThat(((I64Vector) result.fieldValues("high")).values()).containsExactly(0, 11, 0, 13, 14);
        assertThat(((I64Vector) result.fieldValues("low")).values()).containsExactly(0, 21, 0, 23, 24);
    }

    @Test
    void testStructFieldAccessPreservesOuterEncodings()
    {
        StructVector values = new StructVector(2);
        values.setField("high", Streams.ofValues(new I64Vector(new long[] {10, 20})));
        values.setField("low", Streams.ofValues(new I64Vector(new long[] {100, 200})));

        Vector dictionary = new DictionaryVector(new int[] {1, 0, 1}, values);
        Vector rle = new RleVector(new int[] {2, 1}, values);

        VectorAccess.LongValues dictionaryHigh = VectorAccess.longValues(VectorAccess.structFieldValues(dictionary, "high"));
        VectorAccess.LongValues dictionaryLow = VectorAccess.longValues(VectorAccess.structFieldValues(dictionary, "low"));
        assertThat(new long[] {
                dictionaryHigh.value(0),
                dictionaryHigh.value(1),
                dictionaryHigh.value(2)})
                .containsExactly(20, 10, 20);
        assertThat(new long[] {
                dictionaryLow.value(0),
                dictionaryLow.value(1),
                dictionaryLow.value(2)})
                .containsExactly(200, 100, 200);

        VectorAccess.LongValues rleHigh = VectorAccess.longValues(VectorAccess.structFieldValues(rle, "high"));
        VectorAccess.LongValues rleLow = VectorAccess.longValues(VectorAccess.structFieldValues(rle, "low"));
        assertThat(new long[] {
                rleHigh.value(0),
                rleHigh.value(1),
                rleHigh.value(2)})
                .containsExactly(10, 10, 20);
        assertThat(new long[] {
                rleLow.value(0),
                rleLow.value(1),
                rleLow.value(2)})
                .containsExactly(100, 100, 200);
    }

    @Test
    void testBinaryVectorCopySinglePositionPreservesSparseOutputOffsets()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("BinaryCopySingleSparse");

        BinaryVector values = new BinaryVector(3, 32);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        values.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(1, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(2, "gamma".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BinaryVector copy = null;
        copy = (BinaryVector) values.copySinglePositionInto(allocator, context, copy, 0, 2, 8);
        copy = (BinaryVector) values.copySinglePositionInto(allocator, context, copy, 1, 5, 8);
        copy = (BinaryVector) values.copySinglePositionInto(allocator, context, copy, 2, 7, 8);

        assertThat(new String(copy.copyBytes(2), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("alpha");
        assertThat(new String(copy.copyBytes(5), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("beta");
        assertThat(new String(copy.copyBytes(7), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("gamma");
    }

    @Test
    void testBinaryVectorCopyPositionsIntoAppendsFromMultipleSources()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("BinaryCopyAppend");

        BinaryVector first = new BinaryVector(4, 32);
        first.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        first.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        first.setBytes(1, "bravo".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        first.setBytes(2, "charlie".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        first.setBytes(3, "delta".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BinaryVector second = new BinaryVector(4, 32);
        second.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        second.setBytes(0, "echo".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        second.setBytes(1, "foxtrot".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        second.setBytes(2, "golf".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        second.setBytes(3, "hotel".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BinaryVector copy = null;
        copy = (BinaryVector) first.copyPositionsInto(allocator, context, copy, new int[] {1, 3}, 2, 0, 4);
        copy = (BinaryVector) second.copyPositionsInto(allocator, context, copy, new int[] {0, 2}, 2, 2, 4);

        assertThat(new String(copy.copyBytes(0), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("bravo");
        assertThat(new String(copy.copyBytes(1), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("delta");
        assertThat(new String(copy.copyBytes(2), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("echo");
        assertThat(new String(copy.copyBytes(3), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("golf");
    }

    @Test
    void testBinaryVectorCopyPositionsIntoAppendsLargeChunks()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("BinaryCopyLargeAppend");

        BinaryVector first = new BinaryVector(10_000, 200_000);
        first.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        for (int position = 0; position < 10_000; position++) {
            first.setBytes(position, ("left-" + position).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        BinaryVector second = new BinaryVector(10_000, 200_000);
        second.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        for (int position = 0; position < 10_000; position++) {
            second.setBytes(position, ("right-" + position).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        int[] firstPositions = java.util.stream.IntStream.range(4_096, 5_904).toArray();
        int[] secondPositions = java.util.stream.IntStream.range(0, 2_288).toArray();

        BinaryVector copy = null;
        copy = (BinaryVector) first.copyPositionsInto(allocator, context, copy, firstPositions, firstPositions.length, 0, 4_096);
        copy = (BinaryVector) second.copyPositionsInto(allocator, context, copy, secondPositions, secondPositions.length, 1_808, 4_096);

        assertThat(new String(copy.copyBytes(0), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("left-4096");
        assertThat(new String(copy.copyBytes(1_807), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("left-5903");
        assertThat(new String(copy.copyBytes(1_808), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("right-0");
        assertThat(new String(copy.copyBytes(4_095), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("right-2287");
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
        SumStateVector state = new SumStateVector(1024);
        state.increment(0, 10);
        state.increment(1023, 40);

        SumStateVector grown = SumStateVector.grow(state, 1025);

        assertThat(grown.length()).isEqualTo(1025);
        assertThat(grown.retainedBytes()).isEqualTo(state.retainedBytes() * 2);
        assertThat(grown.sum(0)).isEqualTo(10);
        assertThat(grown.isNull(0)).isFalse();
        assertThat(grown.sum(1023)).isEqualTo(40);
        assertThat(grown.isNull(1023)).isFalse();
        assertThat(grown.sum(1024)).isZero();
        assertThat(grown.isNull(1024)).isTrue();

        state.increment(0, 1);
        assertThat(grown.sum(0)).isEqualTo(11);
    }

    @Test
    void testDoubleStateVectorGrowsBySharingExistingChunks()
    {
        DoubleStateVector state = new DoubleStateVector(1024);
        state.set(0, 1.5);
        state.set(1023, 4.5);

        DoubleStateVector grown = DoubleStateVector.grow(state, 1025);

        assertThat(grown.length()).isEqualTo(1025);
        assertThat(grown.retainedBytes()).isEqualTo(state.retainedBytes() * 2);
        assertThat(grown.get(0)).isEqualTo(1.5);
        assertThat(grown.get(1023)).isEqualTo(4.5);
        assertThat(grown.get(1024)).isZero();

        state.add(0, 0.5);
        assertThat(grown.get(0)).isEqualTo(2.0);
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
    void testAllocatorRetainsFlatVectorWorkingSetBeyondStaticFamilyDefault()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("CappedVectorPool");

        int maxRetained = new I64Vector(8).poolMaxRetained();

        List<I64Vector> pooled = new ArrayList<>();
        for (int index = 0; index <= maxRetained; index++) {
            pooled.add(allocator.allocate(context, I64Vector.class, 8, I64Vector::new));
        }

        allocator.release(context);

        List<I64Vector> reallocated = new ArrayList<>();
        for (int index = 0; index <= maxRetained; index++) {
            reallocated.add(allocator.allocate(context, I64Vector.class, 8, I64Vector::new));
        }

        // The static family default is a floor. Once this context observes maxRetained + 1 vectors in concurrent
        // use, it retains that actual working set (subject to the independent byte cap).
        assertThat(reusedCount(pooled, reallocated)).isEqualTo(maxRetained + 1);
    }

    @Test
    void testAllocatorRetainsBinaryVectorWorkingSetBeyondStaticFamilyDefault()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("CappedBinaryVectorPool");

        BinaryVector first = BinaryVector.allocate(allocator, context, 8, 16);
        int maxRetained = first.poolMaxRetained();

        List<BinaryVector> pooled = new ArrayList<>();
        pooled.add(first);
        for (int index = 1; index <= maxRetained; index++) {
            pooled.add(BinaryVector.allocate(allocator, context, 8, 16));
        }

        allocator.release(context);

        List<BinaryVector> reallocated = new ArrayList<>();
        for (int index = 0; index <= maxRetained; index++) {
            reallocated.add(BinaryVector.allocate(allocator, context, 8, 16));
        }

        // All vectors share one position-count pool family. The observed maxRetained + 1 concurrent instances form
        // its working set and remain reusable because they fit under the independent local byte budget.
        assertThat(reusedCount(pooled, reallocated)).isEqualTo(maxRetained + 1);
    }

    @Test
    void testAllocatorReusesLargeVariableWidthVectorWithCeilingCapacity()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("VariableWidthCeilingCapacity");

        // This is large enough to qualify for the process-wide pool. It must nevertheless remain in the
        // active allocator's local working set, whose ceiling lookup can satisfy a smaller next batch.
        BinaryVector larger = BinaryVector.allocate(allocator, context, 12_347, 400_000);
        allocator.release(context);

        BinaryVector smaller = BinaryVector.allocate(allocator, context, 12_347, 350_000);

        assertThat(smaller).isSameAs(larger);
    }

    @Test
    void testAllocatorBoundsLocalVariableWidthPoolByBytes()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("BoundedVariableWidthPool");

        List<BinaryVector> released = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
            released.add(BinaryVector.allocate(allocator, context, 12_349, 30_000_000));
        }
        allocator.release(context);

        List<BinaryVector> borrowed = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
            borrowed.add(BinaryVector.allocate(allocator, context, 12_349, 29_000_000));
        }

        // The default 64 MiB local budget can retain at most two of these buffers. The evicted exact-capacity
        // vector may enter the shared pool, but cannot satisfy this smaller ceiling-capacity request there.
        assertThat(reusedCount(released, borrowed)).isLessThanOrEqualTo(2);
    }

    @Test
    void testAllocatorDoesNotPoolSupersededGrowthVectors()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("LargeSumStatePool");

        SumStateVector first = allocator.allocate(context, SumStateVector.class, 1_000_000, SumStateVector::new);
        allocator.release(context);

        SumStateVector second = allocator.allocate(context, SumStateVector.class, 1_000_000, SumStateVector::new);

        assertThat(second).isNotSameAs(first);
    }

    @Test
    void testAllocatorDoesNotPoolOversizedAvgStateVector()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
    void testAllocatorReusesEmptyMaskAndPreservesZeroSizeAllInvariant()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("EmptyMaskPool");

        Mask first = allocator.allocateEmptyMask(context, 7);
        assertThat(first.none()).isTrue();
        assertThat(first.all()).isFalse();

        allocator.release(context);

        Mask second = allocator.allocateEmptyMask(context, 0);
        assertThat(second).isSameAs(first);
        assertThat(second.none()).isTrue();
        assertThat(second.all()).isTrue();
    }

    @Test
    void testTransferredVectorIsNotReturnedToAllocatorPool()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
    void testVectorTreeLeasePinsBorrowedChildrenUntilConsumerCloses()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context owner = new Allocator.Context("Owner");
        I64Vector values = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);
        DictionaryVector dictionary = DictionaryVector.wrap(new int[] {3, 1}, values);

        Allocator.VectorTreeLease lease = allocator.leaseVectorTree(List.of(dictionary));
        allocator.release(owner);
        I64Vector whilePinned = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);

        assertThat(whilePinned).isNotSameAs(values);

        lease.close();
        I64Vector afterClose = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);

        assertThat(afterClose).isSameAs(values);
    }

    @Test
    void testVectorTreeLeaseIsReferenceCountedByVectorIdentity()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context owner = new Allocator.Context("Owner");
        I64Vector values = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);
        DictionaryVector first = DictionaryVector.wrap(new int[] {0}, values);
        DictionaryVector second = DictionaryVector.wrap(new int[] {1}, values);

        Allocator.VectorTreeLease firstLease = allocator.leaseVectorTree(List.of(first));
        Allocator.VectorTreeLease secondLease = allocator.leaseVectorTree(List.of(second));
        allocator.release(owner);
        firstLease.close();

        assertThat(allocator.allocate(owner, I64Vector.class, 4, I64Vector::new)).isNotSameAs(values);

        secondLease.close();

        assertThat(allocator.allocate(owner, I64Vector.class, 4, I64Vector::new)).isSameAs(values);
    }

    @Test
    void testVectorTreeLeaseRestoresLiveOwnerWithoutPooling()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context owner = new Allocator.Context("Owner");
        I64Vector values = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);

        allocator.leaseVectorTree(List.of(values)).close();

        I64Vector other = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);
        assertThat(other).isNotSameAs(values);
        allocator.release(owner);
        I64Vector first = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);
        I64Vector second = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);
        assertThat(first == values || second == values).isTrue();
    }

    @Test
    void testVectorTreeLeasePoolsAfterExplicitProducerRelease()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context owner = new Allocator.Context("Owner");
        I64Vector values = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);

        Allocator.VectorTreeLease lease = allocator.leaseVectorTree(List.of(values));
        allocator.release(owner, values);
        assertThat(allocator.allocate(owner, I64Vector.class, 4, I64Vector::new)).isNotSameAs(values);

        lease.close();

        assertThat(allocator.allocate(owner, I64Vector.class, 4, I64Vector::new)).isSameAs(values);
    }

    @Test
    void testDiscardedOwnerDoesNotPoolLeasedVector()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context owner = new Allocator.Context("Owner");
        I64Vector values = allocator.allocate(owner, I64Vector.class, 4, I64Vector::new);

        Allocator.VectorTreeLease lease = allocator.leaseVectorTree(List.of(values));
        allocator.discardAllIfPresent(owner);
        lease.close();

        assertThat(allocator.allocate(owner, I64Vector.class, 4, I64Vector::new)).isNotSameAs(values);
    }

    @Test
    void testAsyncVectorTreeDetachRecyclesWithoutProducerThreadAffinity()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator producer = new Allocator(resources)) {
            Allocator.Context owner = new Allocator.Context("Owner");
            I64Vector values = producer.allocate(owner, I64Vector.class, 32_768, I64Vector::new);

            Allocator.AsyncVectorTreeLease lease = producer.detachVectorTreeForAsyncRelease(List.of(values));
            assertThat(producer.residentBytes()).isZero();
            producer.close();

            CompletableFuture.runAsync(lease::close).join();

            try (Allocator consumer = new Allocator(resources)) {
                I64Vector reused = consumer.allocate(owner, I64Vector.class, 32_768, I64Vector::new);
                assertThat(reused).isSameAs(values);
            }
        }
    }

    @Test
    void testAsyncVectorTreeDetachReferenceCountsSharedChildren()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context owner = new Allocator.Context("Owner");
            I64Vector values = allocator.allocate(owner, I64Vector.class, 32_768, I64Vector::new);
            DictionaryVector first = DictionaryVector.wrap(new int[] {0}, values);
            DictionaryVector second = DictionaryVector.wrap(new int[] {1}, values);

            Allocator.AsyncVectorTreeLease firstLease = allocator.detachVectorTreeForAsyncRelease(List.of(first));
            Allocator.AsyncVectorTreeLease secondLease = allocator.detachVectorTreeForAsyncRelease(List.of(second));
            firstLease.close();

            I64Vector whilePinned = allocator.allocate(owner, I64Vector.class, 32_768, I64Vector::new);
            assertThat(whilePinned).isNotSameAs(values);
            allocator.discard(owner, whilePinned);

            CompletableFuture.runAsync(secondLease::close).join();

            assertThat(allocator.allocate(owner, I64Vector.class, 32_768, I64Vector::new)).isSameAs(values);
        }
    }

    @Test
    void testOperatorOutputsRespectBorrowAndTakeSemantics()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
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
