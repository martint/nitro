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
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.AllocatorPolicy;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.ConcatenatedBooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.MinUtf8StateVector;
import org.weakref.nitro.data.NativeBufferAdvice;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VariableWidthStorageReusePolicy;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.aggregation.CountStateVector;
import org.weakref.nitro.operator.aggregation.SumStateVector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAllocator
{
    @Test
    void testExactBinaryOutputDoesNotAddIncrementalGrowthHeadroom()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context growingContext = new Allocator.Context("growing-binary-output");
            Allocator.Context exactContext = new Allocator.Context("exact-binary-output");

            BinaryVector growing = BinaryVector.allocateOrGrow(allocator, growingContext, null, 10, 1_000);
            BinaryVector exact = BinaryVector.allocateOrGrowExact(allocator, exactContext, null, 10, 1_000);

            assertThat(growing.byteCapacity()).isGreaterThan(1_000);
            assertThat(exact.byteCapacity()).isEqualTo(1_000);
            allocator.release(growingContext, growing);
            allocator.release(exactContext, exact);
        }
    }

    @Test
    void testBinaryOutputDoesNotReuseStorageAfterOwnershipIsDetached()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("detached-binary-output");
            BinaryVector first = BinaryVector.allocateOrGrowExact(allocator, context, null, 2, 8);
            first.setBytes(0, new byte[] {1, 2});
            first.setBytes(1, new byte[] {3, 4});

            try (Allocator.AsyncVectorTreeLease ignored = allocator.detachVectorTreeForAsyncRelease(List.of(first))) {
                BinaryVector replacement = BinaryVector.allocateOrGrowExact(allocator, context, first, 2, 8);
                assertThat(replacement).isNotSameAs(first);
                replacement.setBytes(0, new byte[] {9});
                replacement.setBytes(1, new byte[] {8});

                assertThat(first.copyBytes(0)).containsExactly(1, 2);
                assertThat(first.copyBytes(1)).containsExactly(3, 4);
                allocator.release(context, replacement);
            }
        }
    }

    @Test
    void testExactBinaryOutputUsesRequestedPositionCountWhenGrowingPayload()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("exact-binary-output");
            BinaryVector existing = BinaryVector.allocate(allocator, context, 10, 10);
            for (int position = 0; position <= existing.length(); position++) {
                existing.offsets()[position] = position;
            }
            for (int index = 0; index < existing.data().length; index++) {
                existing.data()[index] = (byte) index;
            }

            BinaryVector grown = BinaryVector.allocateOrGrowExact(allocator, context, existing, 3, 20);

            assertThat(grown.length()).isEqualTo(3);
            assertThat(grown.offsets()).containsExactly(0, 1, 2, 3);
            assertThat(grown.data()).startsWith((byte) 0, (byte) 1, (byte) 2);
            allocator.release(context, grown);
        }
    }

    @Test
    void testOverwritesCheckedOutSparseMaskWithoutChangingOwnership()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("retained-mask");
            Mask mask = allocator.allocateSparseMask(context, new int[] {1, 3}, 4);
            long bytes = allocator.currentBytes(context);

            allocator.overwriteSparseMask(context, mask, new int[] {0, 2, 4}, 3, 5);

            assertThat(mask).containsExactly(0, 2, 4);
            assertThat(allocator.currentBytes(context)).isEqualTo(bytes);
            allocator.release(context, mask);
        }
    }

    @Test
    void testComplementMaskPreservesCompactDictionaryDomainAcrossPoolReuse()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("dictionary-complement");
            int[] ids = {3, 1, 2, 0, 3, 2, 1};
            DictionaryVector dictionary = DictionaryVector.wrap(ids, ids.length, new I64Vector(new long[] {10, 20, 30, 40}));
            Mask selected = allocator.allocateAllMask(context, ids.length);
            selected.retainDictionaryComparison(ids, new boolean[] {false, true, false, true});

            Mask first = allocator.complementMask(context, selected);
            assertThat(first.dictionaryDomainSelection(dictionary)).isNotNull();
            assertThat(first).containsExactly(2, 3, 5);
            allocator.release(context, first);

            Mask reused = allocator.complementMask(context, selected);
            assertThat(reused.dictionaryDomainSelection(dictionary)).isNotNull();
            assertThat(reused).containsExactly(2, 3, 5);
        }
    }

    @Test
    void testCopyMaskPreservesCompactDictionaryDomainAcrossPoolReuse()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("dictionary-copy");
            int[] ids = {3, 1, 2, 0, 3, 2, 1};
            DictionaryVector dictionary = DictionaryVector.wrap(ids, ids.length, new I64Vector(new long[] {10, 20, 30, 40}));
            Mask selected = allocator.allocateAllMask(context, ids.length);
            selected.retainDictionaryComparison(ids, new boolean[] {false, true, false, true});

            Mask first = allocator.copyMask(context, selected);
            assertThat(first.dictionaryDomainSelection(dictionary)).isNotNull();
            assertThat(first).containsExactly(0, 1, 4, 6);
            allocator.release(context, first);

            Mask reused = allocator.copyMask(context, selected);
            assertThat(reused.dictionaryDomainSelection(dictionary)).isNotNull();
            assertThat(reused).containsExactly(0, 1, 4, 6);
        }
    }

    @Test
    void testLateVectorReleaseAfterAllocatorCloseIsSatisfied()
    {
        try (EngineResources resources = EngineResources.createDefault()) {
            Allocator allocator = new Allocator(resources);
            Allocator.Context context = new Allocator.Context("late-generation-release");
            I64Vector vector = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            Allocator.AsyncVectorTreeLease lease = allocator.detachVectorTreeForAsyncRelease(List.of(vector));

            allocator.close();
            allocator.release(context, vector);
            lease.close();
        }
    }

    @Test
    void testIndexedVectorOwnershipFollowsLeaseAndDetachLifecycles()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            for (int index = 0; index < 1_000; index++) {
                Allocator.Context context = new Allocator.Context("completed-" + index);
                I64Vector completed = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
                allocator.release(context, completed);
            }

            Allocator.Context context = new Allocator.Context("active");
            I64Vector leasedVector = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            assertThat(allocator.ownsVectorTree(leasedVector)).isTrue();
            assertThat(allocator.canDetachVectorTreeForAsyncRelease(leasedVector)).isTrue();

            try (Allocator.VectorTreeLease ignored = allocator.leaseVectorTree(List.of(leasedVector))) {
                assertThat(allocator.ownsVectorTree(leasedVector)).isTrue();
                assertThat(allocator.canDetachVectorTreeForAsyncRelease(leasedVector)).isFalse();
            }
            assertThat(allocator.ownsVectorTree(leasedVector)).isTrue();
            assertThat(allocator.canDetachVectorTreeForAsyncRelease(leasedVector)).isTrue();

            I64Vector detachedVector = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            try (Allocator.AsyncVectorTreeLease ignored = allocator.detachVectorTreeForAsyncRelease(List.of(detachedVector))) {
                assertThat(allocator.ownsVectorTree(detachedVector)).isTrue();
                assertThat(allocator.canDetachVectorTreeForAsyncRelease(detachedVector)).isTrue();
            }
            assertThat(allocator.ownsVectorTree(detachedVector)).isFalse();
        }
    }

    @Test
    void testRecognizesForeignVectorTrees()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator first = new Allocator(resources);
                Allocator second = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("ownership");
            I64Vector vector = first.allocate(context, I64Vector.class, 8, I64Vector::new);

            assertThat(first.ownsVectorTree(vector)).isTrue();
            assertThat(second.ownsVectorTree(vector)).isFalse();
        }
    }

    @Test
    void testStorageFreeWrapperOwnershipFollowsItsChildren()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator first = new Allocator(resources, new TestingMemoryReservation());
                Allocator second = new Allocator(resources, new TestingMemoryReservation())) {
            Allocator.Context context = new Allocator.Context("ownership");
            I64Vector ownedValues = first.allocate(context, I64Vector.class, 2, I64Vector::new);
            I64Vector foreignValues = second.allocate(context, I64Vector.class, 2, I64Vector::new);
            int[] borrowedIds = {0, 1, 0};

            DictionaryVector ownedTree = first.allocateDictionarySharedIds(context, borrowedIds, borrowedIds.length, ownedValues);
            DictionaryVector foreignTree = first.allocateDictionarySharedIds(context, borrowedIds, borrowedIds.length, foreignValues);

            assertThat(ownedTree.retainedBytes()).isZero();
            assertThat(first.ownsVectorTree(ownedTree)).isTrue();
            assertThat(first.ownsVectorTree(foreignTree)).isFalse();
        }
    }

    @Test
    void testDictionaryDomainReplacementRetainsOwnedMapping()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("dictionary-replacement");
            I32Vector ids = allocator.allocate(context, I32Vector.class, 4, I32Vector::new);
            System.arraycopy(new int[] {0, 1, 0, 1}, 0, ids.values(), 0, 4);
            I32Vector frequencies = allocator.allocate(context, I32Vector.class, 2, I32Vector::new);
            frequencies.values()[0] = 2;
            frequencies.values()[1] = 2;
            I64Vector originalDomain = allocator.allocate(context, I64Vector.class, 2, I64Vector::new);
            DictionaryVector original = DictionaryVector.wrapOwnedIdsWithDomainFrequencies(ids, 4, originalDomain, frequencies);
            I64Vector replacementDomain = allocator.allocate(context, I64Vector.class, 2, I64Vector::new);

            DictionaryVector replacement = original.ownedMappingWithValues(replacementDomain);

            assertThat(replacement.hasOwnedMapping()).isTrue();
            assertThat(replacement.ids()).isSameAs(ids.values());
            assertThat(replacement.values()).isSameAs(replacementDomain);
            assertThat(replacement.domainFrequency(0)).isEqualTo(2);
            assertThat(replacement.domainFrequency(1)).isEqualTo(2);
            assertThat(allocator.ownsVectorTree(replacement)).isTrue();
        }
    }

    @Test
    void testSparseVariableWidthCopyIsCompactAndIndependent()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("sparse-copy");
            BinaryVector source = new BinaryVector(5, 32);
            source.setBytes(0, "ignored-0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            source.setBytes(1, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            source.setBytes(2, "ignored-2".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            source.setBytes(3, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));

            BinaryVector copy = (BinaryVector) allocator.copyVector(context, source, Mask.sparse(new int[] {1, 3}, 5));

            assertThat(copy.length()).isEqualTo(2);
            assertThat(copy.offsets()).containsExactly(0, 5, 9);
            assertThat(copy.data()).containsExactly("alphabeta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            source.data()[source.startOffset(1)] = 'x';
            assertThat(copy.data()).containsExactly("alphabeta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test
    void testEngineResourcesAreIsolatedAndExplicitlyClosed()
    {
        EngineResources first = EngineResources.createDefault();
        EngineResources second = EngineResources.createDefault();
        PrimitiveArrayPool firstArrays = first.primitiveArrays();

        int[] retained = new int[1 << 16];
        assertThat(firstArrays.retain(int[].class, retained.length, (long) retained.length * Integer.BYTES, retained)).isTrue();
        assertThat(firstArrays.retainedBytes()).isEqualTo((long) retained.length * Integer.BYTES);
        assertThat(second.primitiveArrays().retainedBytes()).isZero();

        first.close();
        assertThatThrownBy(first::primitiveArrays)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Engine resources are closed");
        assertThatThrownBy(() -> firstArrays.release(new int[1]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Primitive array pool is closed");
        assertThat(second.primitiveArrays().retainedBytes()).isZero();
        second.close();
    }

    @Test
    void testResidentMemoryIncludesRetainedPool()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("test");
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            I64Vector vector = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            long bytes = vector.retainedBytes();

            assertThat(allocator.residentBytes()).isEqualTo(bytes);
            assertThat(allocator.peakResidentBytes()).isEqualTo(bytes);
            assertThat(memory.reservedBytes()).isEqualTo(bytes);

            allocator.release(context, vector);
            assertThat(allocator.residentBytes()).isEqualTo(bytes);
            assertThat(memory.reservedBytes()).isEqualTo(bytes);

            I64Vector reused = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            assertThat(reused).isSameAs(vector);
            assertThat(memory.reservedBytes()).isEqualTo(bytes);

            allocator.discard(context, reused);
            assertThat(allocator.residentBytes()).isZero();
            assertThat(allocator.peakResidentBytes()).isEqualTo(bytes);
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testReleasesPooledMemoryWithoutDiscardingInUseVectors()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("test");
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            I64Vector idle = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            I64Vector inUse = allocator.allocate(context, I64Vector.class, 16, I64Vector::new);
            long inUseBytes = inUse.retainedBytes();
            allocator.release(context, idle);

            allocator.releasePooledMemory();

            assertThat(allocator.residentBytes()).isEqualTo(inUseBytes);
            assertThat(memory.reservedBytes()).isEqualTo(inUseBytes);
            allocator.discard(context, inUse);
            assertThat(allocator.residentBytes()).isZero();
        }
    }

    @Test
    void testReportsAllocatedBytesWithoutCountingPoolReuse()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("allocated-bytes");
            I64Vector vector = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            long allocatedBytes = vector.retainedBytes();
            allocator.release(context, vector);

            I64Vector reused = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            assertThat(reused).isSameAs(vector);
            assertThat(allocator.allocatedBytes()).isEqualTo(allocatedBytes);
            allocator.release(context, reused);
        }
    }

    @Test
    void testReusesVectorAcrossLongLivedContextOwnershipLog()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("long-lived-reuse");
        I64Vector first;
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            first = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            allocator.release(context, first);
            for (int iteration = 0; iteration < 2_048; iteration++) {
                I64Vector reused = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
                assertThat(reused).isSameAs(first);
                allocator.release(context, reused);
            }

            I64Vector retained = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            assertThat(retained).isSameAs(first);
            allocator.release(context);
            assertThat(allocator.allocatedBytes()).isEqualTo(first.retainedBytes());
        }
        assertThat(memory.reservedBytes()).isZero();
    }

    @Test
    void testReportsAllocatedBytesByContextName()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context first = new Allocator.Context("first");
            Allocator.Context firstSibling = new Allocator.Context("first");
            Allocator.Context second = new Allocator.Context("second");
            I64Vector firstVector = allocator.allocate(first, I64Vector.class, 8, I64Vector::new);
            I32Vector siblingVector = allocator.allocate(firstSibling, I32Vector.class, 4, I32Vector::new);
            I64Vector secondVector = allocator.allocate(second, I64Vector.class, 2, I64Vector::new);

            assertThat(allocator.allocatedBytesByContext()).isEqualTo(Map.of(
                    "first", Math.addExact(firstVector.retainedBytes(), siblingVector.retainedBytes()),
                    "second", secondVector.retainedBytes()));
        }
    }

    @Test
    void testReportsPeakBytesByContextName()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context first = new Allocator.Context("first");
            Allocator.Context firstSibling = new Allocator.Context("first");
            Allocator.Context second = new Allocator.Context("second");
            I64Vector firstVector = allocator.allocate(first, I64Vector.class, 8, I64Vector::new);
            I32Vector siblingVector = allocator.allocate(firstSibling, I32Vector.class, 4, I32Vector::new);
            I64Vector secondVector = allocator.allocate(second, I64Vector.class, 2, I64Vector::new);
            allocator.release(first, firstVector);

            assertThat(allocator.peakBytesByContext()).isEqualTo(Map.of(
                    "first", Math.addExact(firstVector.retainedBytes(), siblingVector.retainedBytes()),
                    "second", secondVector.retainedBytes()));
        }
    }

    @Test
    void testReportsAllocatedVectorBytesByTypeWithoutCountingPoolReuse()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context first = new Allocator.Context("first");
            Allocator.Context second = new Allocator.Context("second");
            I64Vector firstVector = allocator.allocate(first, I64Vector.class, 8, I64Vector::new);
            I32Vector secondVector = allocator.allocate(second, I32Vector.class, 4, I32Vector::new);
            allocator.release(first, firstVector);
            I64Vector reused = allocator.allocate(first, I64Vector.class, 8, I64Vector::new);

            assertThat(reused).isSameAs(firstVector);
            assertThat(allocator.allocatedVectorBytesByType()).isEqualTo(Map.of(
                    "I32Vector", secondVector.retainedBytes(),
                    "I64Vector", firstVector.retainedBytes()));
        }
    }

    @Test
    void testSharedChunkGrowthCountsOnlyNewStorage()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("shared-growth");
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            CountStateVector state = allocator.allocate(
                    context,
                    CountStateVector.class,
                    4_096,
                    CountStateVector::new);
            state.increment(7, 11);
            long initialBytes = state.retainedBytes();

            CountStateVector grown = allocator.replaceSharedGrowth(
                    context,
                    state,
                    CountStateVector.grow(state, 4_097));
            long growthBytes = grown.retainedBytes() - initialBytes;

            assertThat(grown.value(7)).isEqualTo(11);
            assertThat(allocator.allocatedBytes()).isEqualTo(initialBytes + growthBytes);
            assertThat(allocator.allocatedVectorBytesByType()).isEqualTo(Map.of(
                    "CountStateVector", initialBytes + growthBytes));
            assertThat(allocator.residentBytes()).isEqualTo(grown.retainedBytes());
            assertThat(memory.reservedBytes()).isEqualTo(grown.retainedBytes());
            allocator.discard(context, grown);
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testTracksInPlaceVectorRetainedSizeChanges()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("variable-state");
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            MinUtf8StateVector state = allocator.allocate(
                    context,
                    MinUtf8StateVector.class,
                    2,
                    MinUtf8StateVector::new);
            long initialBytes = state.retainedBytes();

            long previousBytes = state.retainedBytes();
            state.setValue(0, new byte[17]);
            allocator.retainedBytesChanged(context, state, previousBytes);
            assertThat(allocator.residentBytes()).isEqualTo(initialBytes + 17);
            assertThat(memory.reservedBytes()).isEqualTo(initialBytes + 17);

            previousBytes = state.retainedBytes();
            state.setValue(0, new byte[5]);
            allocator.retainedBytesChanged(context, state, previousBytes);
            assertThat(allocator.residentBytes()).isEqualTo(initialBytes + 5);
            assertThat(memory.reservedBytes()).isEqualTo(initialBytes + 5);

            allocator.discard(context, state);
            assertThat(allocator.residentBytes()).isZero();
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testTracksAdaptiveCountChunkPromotion()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("adaptive-count-state");
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            CountStateVector state = allocator.allocate(
                    context,
                    CountStateVector.class,
                    4_096,
                    CountStateVector::new);
            long compactBytes = state.retainedBytes();

            state.increment(0, 256);

            assertThat(state.retainedBytes()).isEqualTo(compactBytes + 4_096L * 7);
            assertThat(allocator.residentBytes()).isEqualTo(state.retainedBytes());
            assertThat(memory.reservedBytes()).isEqualTo(state.retainedBytes());
            allocator.discard(context, state);
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testReleasesRetainedSparseStorageAfterMaskBecomesAll()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("test");
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            Mask sparse = allocator.copyMask(context, Mask.sparse(new int[] {1, 3}, 8));
            long retainedBytes = allocator.residentBytes();
            allocator.release(context, sparse);

            Mask all = allocator.allocateAllMask(context, 8);
            assertThat(all).isSameAs(sparse);
            assertThat(allocator.residentBytes()).isEqualTo(retainedBytes);
            allocator.release(context, all);
            allocator.releasePooledMemory();

            assertThat(allocator.residentBytes()).isZero();
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testReleasesOnlySelectedContextPool()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context firstContext = new Allocator.Context("first");
        Allocator.Context secondContext = new Allocator.Context("second");
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            I64Vector first = allocator.allocate(firstContext, I64Vector.class, 8, I64Vector::new);
            I64Vector second = allocator.allocate(secondContext, I64Vector.class, 16, I64Vector::new);
            allocator.release(firstContext, first);
            allocator.release(secondContext, second);
            long retainedBytes = allocator.residentBytes();

            allocator.releasePooledMemory(firstContext);
            assertThat(allocator.residentBytes()).isEqualTo(second.retainedBytes());
            assertThat(allocator.residentBytes()).isLessThan(retainedBytes);
            assertThat(memory.reservedBytes()).isEqualTo(second.retainedBytes());

            allocator.releasePooledMemory(secondContext);
            assertThat(allocator.residentBytes()).isZero();
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testTracksNonVectorRetainedStateByOwner()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("index-state");
        Object firstIndex = new Object();
        Object secondIndex = new Object();
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            allocator.setRetainedBytes(context, firstIndex, 100);
            allocator.setRetainedBytes(context, secondIndex, 25);
            assertThat(allocator.residentBytes()).isEqualTo(125);
            assertThat(allocator.currentBytes(context)).isEqualTo(125);
            assertThat(memory.reservedBytes()).isEqualTo(125);

            allocator.setRetainedBytes(context, firstIndex, 60);
            allocator.setRetainedBytes(context, secondIndex, 0);
            assertThat(allocator.residentBytes()).isEqualTo(60);
            assertThat(allocator.currentBytes(context)).isEqualTo(60);
            assertThat(memory.reservedBytes()).isEqualTo(60);

            allocator.release(context);
            assertThat(allocator.residentBytes()).isZero();
            assertThat(allocator.currentBytes(context)).isZero();
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testExactScopeBytesDoNotIncludeSameNamedContexts()
    {
        Allocator.Context first = new Allocator.Context("shared-name");
        Allocator.Context second = new Allocator.Context("shared-name");
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            allocator.setRetainedBytes(first, new Object(), 100);
            allocator.setRetainedBytes(second, new Object(), 25);

            assertThat(allocator.currentBytes(first)).isEqualTo(125);
            assertThat(allocator.scopeCurrentBytes(first)).isEqualTo(100);
            assertThat(allocator.scopeCurrentBytes(second)).isEqualTo(25);
        }
    }

    @Test
    void testRejectedRetainedStateReservationDoesNotCorruptTeardownAccounting()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("rejected-index-state");
        Object index = new Object();
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            allocator.setRetainedBytes(context, index, 100);
            memory.failure = new IllegalStateException("memory limit exceeded");

            assertThatThrownBy(() -> allocator.setRetainedBytes(context, index, 250))
                    .isSameAs(memory.failure);

            assertThat(allocator.residentBytes()).isEqualTo(100);
            assertThat(allocator.currentBytes(context)).isEqualTo(100);
            assertThat(memory.reservedBytes()).isEqualTo(100);
            memory.failure = null;
            allocator.release(context);
            assertThat(allocator.residentBytes()).isZero();
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testClosingAllocatorReleasesResidentConstantsAndPools()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator allocator = new Allocator(EngineResources.createDefault(), memory);
        Allocator.Context context = new Allocator.Context("test");

        allocator.borrowAllFalseBoolean(context, 32);
        I64Vector vector = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
        allocator.release(context, vector);
        assertThat(memory.reservedBytes()).isPositive();

        allocator.close();
        assertThat(memory.reservedBytes()).isZero();
        assertThat(allocator.residentBytes()).isZero();
        allocator.close();
    }

    @Test
    void testBlockedMemoryContinuationIsExposed()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        memory.blocked = new CompletableFuture<>();
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            allocator.allocate(new Allocator.Context("test"), I64Vector.class, 8, I64Vector::new);

            assertThat(allocator.memoryBlocked()).contains(memory.blocked);
            memory.blocked.complete(null);
            assertThat(allocator.memoryBlocked()).isEmpty();
        }
    }

    @Test
    void testCompatibleMaskPoolCanSpillEntireLocalBucket()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Object compatibilityGroup = new Object();
        Allocator.Context producer = new Allocator.Context("producer", new Object(), compatibilityGroup);
        Allocator.Context consumer = new Allocator.Context("consumer", new Object(), compatibilityGroup);
        Allocator.Context third = new Allocator.Context("third", new Object(), compatibilityGroup);
        allocator.register(producer);
        allocator.register(consumer);
        allocator.register(third);
        allocator.beginExecution();

        Mask produced = allocator.allocateRangeMask(producer, 1, 4);
        allocator.release(producer, produced);

        Mask borrowed = allocator.allocateRangeMask(consumer, 2, 4);
        assertThat(borrowed.size()).isEqualTo(6);
        allocator.release(consumer, borrowed);
    }

    @Test
    void testMaskCannotBeReleasedThroughAnotherContext()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context owner = new Allocator.Context("owner");
            Allocator.Context other = new Allocator.Context("other");
            Mask mask = allocator.allocateRangeMask(owner, 1, 10_000);
            long residentBytes = allocator.residentBytes();

            allocator.release(other, mask);
            assertThat(allocator.residentBytes()).isEqualTo(residentBytes);

            allocator.release(owner, mask);
            assertThat(allocator.allocateRangeMask(owner, 2, 10_000)).isSameAs(mask);
        }
    }

    @Test
    void testResidentMemoryTracksInPlaceMaskCapacityGrowth()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            Allocator.Context context = new Allocator.Context("test");
            Mask mask = allocator.allocateAllMask(context, 10_000);
            assertThat(allocator.residentBytes()).isZero();

            mask.retainIf(position -> (position & 1) == 0);
            assertThat(allocator.residentBytes()).isEqualTo(40_000);
            assertThat(memory.reservedBytes()).isEqualTo(40_000);

            allocator.release(context, mask);
            assertThat(allocator.residentBytes()).isEqualTo(40_000);
        }
        assertThat(memory.reservedBytes()).isZero();
    }

    @Test
    void testUninitializedSparseMaskSupportsAllRowsSelected()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("test");
            Mask mask = allocator.allocateUninitializedSparseMask(context, 4, 4);
            int[] positions = mask.positionsArrayForOverwrite(4);
            for (int position = 0; position < 4; position++) {
                positions[position] = position;
            }

            assertThat(mask.all()).isTrue();
            assertThat(mask).containsExactly(0, 1, 2, 3);

            allocator.release(context, mask);
            Mask reused = allocator.allocateUninitializedSparseMask(context, 4, 4);
            assertThat(reused).isSameAs(mask);
            assertThat(reused.all()).isTrue();
        }
    }

    @Test
    void testCompatibleVectorPoolRequiresThreeRegisteredGroups()
    {
        Object compatibilityGroup = new Object();
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context first = new Allocator.Context("first", new Object(), compatibilityGroup);
        Allocator.Context second = new Allocator.Context("second", new Object(), compatibilityGroup);
        allocator.register(first);
        allocator.register(second);
        allocator.beginExecution();

        I64Vector isolated = allocator.allocate(first, I64Vector.class, 8, I64Vector::new);
        allocator.release(first, isolated);
        I64Vector secondVector = allocator.allocate(second, I64Vector.class, 8, I64Vector::new);
        assertThat(secondVector).isNotSameAs(isolated);
        allocator.release(second, secondVector);

        allocator = new Allocator(EngineResources.createDefault());
        first = new Allocator.Context("first", new Object(), compatibilityGroup);
        second = new Allocator.Context("second", new Object(), compatibilityGroup);
        Allocator.Context third = new Allocator.Context("third", new Object(), compatibilityGroup);
        allocator.register(first);
        allocator.register(second);
        allocator.register(third);
        allocator.beginExecution();
        I64Vector shared = allocator.allocate(first, I64Vector.class, 8, I64Vector::new);
        allocator.release(first, shared);
        assertThat(allocator.allocate(third, I64Vector.class, 8, I64Vector::new)).isSameAs(shared);
    }

    @Test
    void testCompatibilityAdmissionDoesNotAccumulateAcrossExecutions()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Object compatibilityGroup = new Object();
        Allocator.Context first = new Allocator.Context("first", new Object(), compatibilityGroup);
        Allocator.Context second = new Allocator.Context("second", new Object(), compatibilityGroup);
        Allocator.Context third = new Allocator.Context("third", new Object(), compatibilityGroup);
        allocator.register(first);
        allocator.register(second);
        allocator.register(third);
        allocator.beginExecution();

        I64Vector shared = allocator.allocate(first, I64Vector.class, 8, I64Vector::new);
        allocator.release(first, shared);
        assertThat(allocator.allocate(third, I64Vector.class, 8, I64Vector::new)).isSameAs(shared);

        Allocator.Context laterFirst = new Allocator.Context("laterFirst", new Object(), compatibilityGroup);
        Allocator.Context laterSecond = new Allocator.Context("laterSecond", new Object(), compatibilityGroup);
        allocator.register(laterFirst);
        allocator.register(laterSecond);
        allocator.beginExecution();

        I64Vector laterIsolated = allocator.allocate(laterFirst, I64Vector.class, 8, I64Vector::new);
        allocator.release(laterFirst, laterIsolated);
        assertThat(allocator.allocate(laterSecond, I64Vector.class, 8, I64Vector::new)).isNotSameAs(laterIsolated);
    }

    @Test
    void testSharedResourceClosesAfterLastLease()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Object key = new Object();
        AtomicInteger closes = new AtomicInteger();

        Allocator.SharedResource<TestResource> first = allocator.acquireSharedResource(key, () -> new TestResource(closes));
        Allocator.SharedResource<TestResource> second = allocator.acquireSharedResource(key, () -> {
            throw new AssertionError("resource constructed twice");
        });

        assertThat(second.value()).isSameAs(first.value());
        first.close();
        assertThat(closes).hasValue(0);
        first.close();
        assertThat(closes).hasValue(0);
        second.close();
        assertThat(closes).hasValue(1);
    }

    @Test
    void testSharedResourceLeaseCanCloseAfterAllocator()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        AtomicInteger closes = new AtomicInteger();
        Allocator.SharedResource<TestResource> lease =
                allocator.acquireSharedResource(new Object(), () -> new TestResource(closes));

        allocator.close();
        assertThat(closes).hasValue(1);

        lease.close();
        lease.close();
        assertThat(closes).hasValue(1);
    }

    @Test
    void testAllFalseBooleanConstantIsSharedAndUnowned()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("test");

        Vector first = allocator.borrowAllFalseBoolean(context, 5);
        Vector sameLength = allocator.borrowAllFalseBoolean(context, 5);
        Vector differentLength = allocator.borrowAllFalseBoolean(context, 3);

        assertThat(first).isInstanceOf(BooleanVector.class);
        assertThat(sameLength).isSameAs(first);
        assertThat(differentLength).isNotSameAs(first);
        assertThat(first.length()).isEqualTo(5);
        assertThat(differentLength.length()).isEqualTo(3);
        assertThat(allocator.isSharedAllFalseBoolean(first)).isTrue();
        assertThat(allocator.isSharedAllFalseBoolean(new BooleanVector(5))).isFalse();
        for (int position = 0; position < first.length(); position++) {
            assertThat(VectorAccess.booleanValues(first).value(position)).isFalse();
        }

        allocator.release(context, first);
        allocator.release(context, sameLength);
        assertThat(allocator.borrowAllFalseBoolean(context, 5)).isSameAs(first);
        assertThat(allocator.totalBytes(context)).isZero();
    }

    @Test
    void testSharedAllFalseBooleanIsCopiedBeforeWrite()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("test");
        BooleanVector constant = (BooleanVector) allocator.borrowAllFalseBoolean(context, 5);

        BooleanVector writable = allocator.allocateOrGrow(context, constant, BooleanVector.class, 5, BooleanVector::new);
        assertThat(writable).isNotSameAs(constant);
        writable.values()[0] = true;
        assertThat(VectorAccess.booleanValues(constant).value(0)).isFalse();
    }

    @Test
    void testSharedAllFalseBooleanIsOwnedByItsAllocator()
    {
        EngineResources resources = EngineResources.createDefault();
        Allocator firstAllocator = new Allocator(resources);
        Allocator secondAllocator = new Allocator(resources);
        Allocator.Context context = new Allocator.Context("test");

        BooleanVector constant = (BooleanVector) firstAllocator.borrowAllFalseBoolean(context, 5);

        assertThat(firstAllocator.isSharedAllFalseBoolean(constant)).isTrue();
        assertThat(secondAllocator.isSharedAllFalseBoolean(constant)).isFalse();
    }

    @Test
    void testDictionaryRetainedBytesDistinguishOwnedAndBorrowedMappings()
    {
        int[] ids = {0, 1, 0};
        I64Vector values = new I64Vector(new long[] {11, 29});

        DictionaryVector copied = new DictionaryVector(ids, values);
        assertThat(copied.retainedBytes()).isEqualTo(3L * Integer.BYTES);
        assertThat(copied.idStorageOwnerOrNull()).isSameAs(copied);

        DictionaryVector trusted = DictionaryVector.ofTrustedIds(ids, values);
        assertThat(trusted.retainedBytes()).isEqualTo(3L * Integer.BYTES);
        assertThat(trusted.idStorageOwnerOrNull()).isSameAs(trusted);

        DictionaryVector borrowed = DictionaryVector.wrap(ids, values);
        assertThat(borrowed.retainedBytes()).isZero();
        assertThat(borrowed.idStorageOwnerOrNull()).isNull();

        DictionaryVector nested = DictionaryVector.ofTrustedIds(new int[] {1, 0}, values);
        assertThat(DictionaryVector.wrap(ids, nested).retainedBytes()).isEqualTo(3L * Integer.BYTES);

        I32Vector ownedIds = new I32Vector(ids.clone());
        DictionaryVector explicitlyOwned = DictionaryVector.wrapOwnedIds(ownedIds, ids.length, values);
        assertThat(explicitlyOwned.retainedBytes()).isZero();
        assertThat(explicitlyOwned.idStorageOwnerOrNull()).isSameAs(ownedIds);
    }

    @Test
    void testDictionaryCanCopyLogicalRowsIntoFlatRepresentation()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            BinaryVector values = new BinaryVector(2, 6);
            values.setBytes(0, new byte[] {1, 2});
            values.setBytes(1, new byte[] {3, 4, 5, 6});
            DictionaryVector dictionary = DictionaryVector.wrap(new int[] {1, 0, 1}, values);
            Allocator.Context context = new Allocator.Context("flat-dictionary-copy");

            BinaryVector flat = (BinaryVector) dictionary.copyFlat(allocator, context);

            assertThat(flat.length()).isEqualTo(3);
            assertThat(flat.copyBytes(0)).containsExactly(3, 4, 5, 6);
            assertThat(flat.copyBytes(1)).containsExactly(1, 2);
            assertThat(flat.copyBytes(2)).containsExactly(3, 4, 5, 6);
        }
    }

    @Test
    void testDictionaryTracksAllocatorOwnedIdsAndDomainFrequencies()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("dictionary-metadata");
            I32Vector ids = allocator.allocate(context, I32Vector.class, 4, I32Vector::new);
            I32Vector frequencies = allocator.allocate(context, I32Vector.class, 2, I32Vector::new);
            System.arraycopy(new int[] {0, 1, 0, 1}, 0, ids.values(), 0, 4);
            System.arraycopy(new int[] {2, 2}, 0, frequencies.values(), 0, 2);

            DictionaryVector dictionary = DictionaryVector.wrapOwnedIdsWithDomainFrequencies(
                    ids,
                    4,
                    new I64Vector(new long[] {11, 29}),
                    frequencies);

            assertThat(dictionary.childVectorCount()).isEqualTo(3);
            assertThat(dictionary.childVector(0)).isSameAs(ids);
            assertThat(dictionary.childVector(1)).isSameAs(frequencies);
            assertThat(dictionary.domainFrequency(0)).isEqualTo(2);
            assertThat(dictionary.domainFrequency(1)).isEqualTo(2);

            dictionary.prepareBufferTransfer(allocator, context);
            allocator.release(context, ids);
            allocator.release(context, frequencies);
            assertThat(allocator.allocate(context, I32Vector.class, 4, I32Vector::new)).isNotSameAs(ids);
            assertThat(allocator.allocate(context, I32Vector.class, 2, I32Vector::new)).isNotSameAs(frequencies);

            dictionary.releaseTransferredBuffers();
            assertThat(allocator.allocate(context, I32Vector.class, 4, I32Vector::new)).isSameAs(ids);
            assertThat(allocator.allocate(context, I32Vector.class, 2, I32Vector::new)).isSameAs(frequencies);
        }
    }

    @Test
    void testSingleRunRleSharesAllocatorOwnedCountMetadata()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("single-run");
            I64Vector values = new I64Vector(new long[] {37});

            RleVector first = allocator.allocateSingleRunRle(context, 11, values);
            RleVector second = allocator.allocateRle(context, new int[] {11}, values);

            assertThat(first.counts()).isSameAs(second.counts());
            assertThat(first.retainedBytes()).isZero();
            assertThat(second.retainedBytes()).isZero();
        }
    }

    @Test
    void testVariableWidthStorageRejectsGrosslyOversizedPooledArray()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            byte[] oversized = new byte[1 << 20];
            allocator.primitiveArrays().release(oversized);
            Allocator.Context context = new Allocator.Context("variable-width");

            BinaryVector vector = BinaryVector.allocate(allocator, context, 10, 64);

            assertThat(vector.byteCapacity()).isBetween(64, 512);
            assertThat(allocator.primitiveArrays().borrowBytes(1 << 20)).isSameAs(oversized);
        }
    }

    @Test
    void testVariableWidthStorageUsesAllocationLifetimePolicy()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            byte[] moderatelyOversized = new byte[512 << 10];
            allocator.primitiveArrays().release(moderatelyOversized);
            Object poolGroup = new Object();
            Allocator.Context producerContext = new Allocator.Context("producer", poolGroup);
            BinaryVector oversized = BinaryVector.allocate(allocator, producerContext, 10, 64 << 10);
            assertThat(oversized.byteCapacity()).isEqualTo(512 << 10);
            allocator.release(producerContext, oversized);
            Allocator.Context longLivedContext = new Allocator.Context(
                    "long-lived",
                    poolGroup,
                    VariableWidthStorageReusePolicy.maximumOversizeRatio(2));

            BinaryVector longLived = BinaryVector.allocate(allocator, longLivedContext, 10, 64 << 10);

            assertThat(longLived.byteCapacity()).isBetween(64 << 10, 128 << 10);

            Allocator.Context ordinaryContext = new Allocator.Context("ordinary", poolGroup);
            BinaryVector ordinary = BinaryVector.allocate(allocator, ordinaryContext, 10, 64 << 10);
            assertThat(ordinary).isSameAs(oversized);
        }
    }

    @Test
    void testAllocatorUsesOwnerSuppliedPolicy()
    {
        AllocatorPolicy policy = new AllocatorPolicy(
                true,
                false,
                false,
                new AllocatorPolicy.BooleanCopies(false, false, false),
                new AllocatorPolicy.MaskFiltering(false, false, 0),
                4,
                false,
                true,
                true,
                true,
                64L << 20,
                64L << 20,
                AllocatorPolicy.AggregateStateVectorRetention.defaults(),
                1,
                0,
                3,
                0,
                true,
                false,
                true,
                true,
                2);
        NativeBufferAdvice nativeBufferAdvice = NativeBufferAdvice.disabled();
        try (AllocationResources resources = new AllocationResources(
                new PrimitiveArrayPool(1 << 20, 0),
                new PrimitiveArrayPool(1 << 20, 0),
                policy,
                nativeBufferAdvice);
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("test");

            assertThat(allocator.nativeBufferAdvice()).isSameAs(nativeBufferAdvice);

            Vector first = allocator.borrowAllFalseBoolean(context, 5);
            Vector second = allocator.borrowAllFalseBoolean(context, 5);

            assertThat(first).isNotSameAs(second);

            I64Vector values = new I64Vector(5);
            BooleanVector nulls = new BooleanVector(5);
            Streams existing = Streams.ofValuesAndNulls(values, nulls);
            assertThat(allocator.reuseValuesAndNulls(existing, values, nulls)).isNotSameAs(existing);

            I32Vector ids = allocator.allocate(context, I32Vector.class, 5, I32Vector::new);
            DictionaryVector dictionary = DictionaryVector.wrapOwnedIds(ids, 5, values);
            dictionary.prepareBufferTransfer(allocator, context);
            allocator.release(context, ids);
            assertThat(allocator.allocate(context, I32Vector.class, 5, I32Vector::new)).isSameAs(ids);

            boolean[] copiedFlags = new boolean[3];
            allocator.copyBooleanValues(
                    new BooleanVector(new boolean[] {true, false, true}),
                    Mask.all(3),
                    copiedFlags);
            assertThat(copiedFlags).containsExactly(true, false, true);

            Mask integerRange = allocator.allocateAllMask(context, 4);
            integerRange.retainConstantRange(new int[] {-5, 0, 5, 10}, 0, 10, null);
            assertThat(integerRange).containsExactly(2);

            Mask doubleComparison = allocator.allocateAllMask(context, 4);
            doubleComparison.retainConstantComparison(
                    new double[] {3.0, -1.0, 2.0, 1.5},
                    2.0,
                    Mask.ComparisonOperator.LESS_THAN);
            assertThat(doubleComparison).containsExactly(1, 3);

            ConcatenatedBooleanVector concatenated = new ConcatenatedBooleanVector(new Vector[] {
                    new BooleanVector(new boolean[] {false, true}),
                    new BooleanVector(new boolean[] {true, false})});
            BooleanVector copiedPositions = (BooleanVector) concatenated.copyPositionsInto(
                    allocator,
                    context,
                    null,
                    new int[] {3, 0, 2},
                    3,
                    0,
                    3);
            assertThat(copiedPositions.values()).containsExactly(false, false, true);
        }
    }

    @Test
    void testAllocatorUsesOwnerSuppliedAggregateStateRetention()
    {
        AllocatorPolicy policy = AllocatorPolicy.defaults()
                .withAggregateStateVectorRetention(new AllocatorPolicy.AggregateStateVectorRetention(1, 18, 18));
        try (AllocationResources resources = new AllocationResources(
                new PrimitiveArrayPool(1 << 20, 0),
                new PrimitiveArrayPool(1 << 20, 0),
                policy);
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("aggregate-state-retention");

            SumStateVector retained = allocator.allocate(context, SumStateVector.class, 2, SumStateVector::new);
            allocator.release(context, retained);
            assertThat(allocator.allocate(context, SumStateVector.class, 2, SumStateVector::new)).isSameAs(retained);

            SumStateVector discarded = allocator.allocate(context, SumStateVector.class, 3, SumStateVector::new);
            allocator.release(context, discarded);
            assertThat(allocator.allocate(context, SumStateVector.class, 3, SumStateVector::new)).isNotSameAs(discarded);
        }
    }

    private record TestResource(AtomicInteger closes)
            implements AutoCloseable
    {
        @Override
        public void close()
        {
            closes.incrementAndGet();
        }
    }

    private static final class TestingMemoryReservation
            implements org.weakref.nitro.core.execution.MemoryReservation
    {
        private long reserved;
        private CompletableFuture<Void> blocked = CompletableFuture.completedFuture(null);
        private RuntimeException failure;

        @Override
        public CompletionStage<Void> reserve(long bytes)
        {
            if (failure != null) {
                throw failure;
            }
            reserved += bytes;
            return blocked;
        }

        @Override
        public void release(long bytes)
        {
            reserved -= bytes;
        }

        @Override
        public long reservedBytes()
        {
            return reserved;
        }
    }
}
