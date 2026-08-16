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
package org.weakref.nitro.data;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestScopedVectorAllocator
{
    @Test
    void testSparseBinaryCopyClearsRecycledPrefixOffsets()
    {
        Allocator.Context context = new Allocator.Context("sparse-binary-copy");
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            BinaryVector recycled = BinaryVector.allocate(allocator, context, 5, 32);
            for (int position = 0; position < recycled.length(); position++) {
                recycled.setBytes(position, new byte[] {(byte) ('a' + position)});
            }
            allocator.release(context, recycled);

            BinaryVector source = new BinaryVector(1, 1);
            source.setBytes(0, new byte[] {'x'});
            BinaryVector copied = (BinaryVector) source.copySinglePositionInto(
                    allocator,
                    context,
                    null,
                    0,
                    3,
                    5);

            assertThat(copied).isSameAs(recycled);
            assertThat(copied.offsets()).containsExactly(0, 0, 0, 0, 1, 0);
            assertThat(copied.copyBytes(3)).containsExactly((byte) 'x');
            allocator.release(context, copied);
        }
    }

    @Test
    void testInterleavesStreamBundlesInRowMajorOrder()
    {
        Allocator.Context context = new Allocator.Context("connector");
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources);
                VectorAllocator vectors = allocator.vectorAllocator(context)) {
            Streams interleaved = vectors.interleave(
                    List.of(
                            Streams.of(
                                    new I64Vector(new long[] {1, 2}),
                                    new BooleanVector(new boolean[] {false, true}),
                                    null),
                            Streams.of(
                                    new I64Vector(new long[] {10, 20}),
                                    null,
                                    new BooleanVector(new boolean[] {true, false}))),
                    2);

            assertThat(((I64Vector) interleaved.values()).values()).containsExactly(1, 10, 2, 20);
            assertThat(((BooleanVector) interleaved.get(Stream.NULLS)).values())
                    .containsExactly(false, false, true, false);
            assertThat(((BooleanVector) interleaved.get(Stream.ERRORS)).values())
                    .containsExactly(false, true, false, false);
        }
    }

    @Test
    void testAllocatesReleasesAndTransfersWithoutExposingEngineResources()
    {
        Allocator.Context context = new Allocator.Context("connector");
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            VectorAllocator vectors = allocator.vectorAllocator(context);
            assertThat(allocator.resourcesOwner()).isSameAs(resources);
            assertThat(allocator.nativeBuffers()).isSameAs(resources.nativeBuffers());

            I64Vector released = vectors.allocate(I64Vector.class, 4, I64Vector::new);
            assertThat(allocator.currentBytes(context)).isPositive();
            vectors.release(released);
            assertThat(allocator.currentBytes(context)).isZero();

            BinaryVector binary = BinaryVector.allocate(vectors, 3, 32);
            assertThat(binary.length()).isEqualTo(3);
            assertThat(binary.byteCapacity()).isGreaterThanOrEqualTo(32);
            assertThat(vectors.transfer(binary)).isSameAs(binary);
            assertThat(allocator.currentBytes(context)).isZero();

            vectors.allocate(I32Vector.class, 8, I32Vector::new);
            assertThat(allocator.currentBytes(context)).isPositive();
            vectors.close();
            assertThat(allocator.currentBytes(context)).isZero();
            assertThatIllegalStateException()
                    .isThrownBy(() -> vectors.allocate(I32Vector.class, 1, I32Vector::new))
                    .withMessageContaining("closed");
        }
    }
}
