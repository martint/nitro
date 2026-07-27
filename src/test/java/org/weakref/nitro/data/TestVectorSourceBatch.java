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
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.type.Schema;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestVectorSourceBatch
{
    @Test
    void testOwnsLazyColumnsAndPropagatesSelection()
    {
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources);
                VectorBatchScope scope = new VectorBatchScope(allocator, "source")) {
            I64Vector first = allocator.allocate(scope.context(), I64Vector.class, 4, I64Vector::new);
            I64Vector second = allocator.allocate(scope.context(), I64Vector.class, 4, I64Vector::new);
            int[] resolutions = new int[1];
            Mask[] constrained = new Mask[1];
            VectorColumnGeneration column = new VectorColumnGeneration(
                    Set.of(Stream.VALUES),
                    _ -> resolutions[0]++ == 0 ? first : second,
                    scope)
                    .withConstraintSensitiveResolution();
            VectorSourceBatch batch = new VectorSourceBatch(
                    Schema.unspecified(1),
                    allocator.allocateAllMask(scope.context(), 4),
                    new VectorColumnGeneration[] {column},
                    scope,
                    mask -> constrained[0] = mask,
                    () -> {});

            assertThat(batch.selection().isDense()).isTrue();
            assertThat(batch.column(0).borrow(Stream.VALUES)).isSameAs(first);
            batch.select(sparseSelection(4, 1, 3));
            assertThat(constrained[0].selectedPositions()).containsExactly(1, 3);
            assertThat(batch.column(0).borrow(Stream.VALUES)).isSameAs(second);
            batch.close();

            assertThatIllegalStateException().isThrownBy(batch::selection);
            I64Vector reused = allocator.allocate(scope.context(), I64Vector.class, 4, I64Vector::new);
            assertThat(reused).isIn(first, second);
        }
    }

    @Test
    void testRejectsSelectionAfterTransferWithoutPartiallyInvalidatingColumns()
    {
        VectorColumnGeneration first = VectorColumnGeneration.of(Streams.ofValues(new I64Vector(2)))
                .withConstraintSensitiveResolution();
        VectorColumnGeneration second = VectorColumnGeneration.of(Streams.ofValues(new I64Vector(2)))
                .withConstraintSensitiveResolution();
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources);
                VectorBatchScope scope = new VectorBatchScope(allocator, "source")) {
            VectorSourceBatch batch = new VectorSourceBatch(
                    Schema.unspecified(2),
                    allocator.allocateAllMask(scope.context(), 2),
                    new VectorColumnGeneration[] {first, second},
                    scope,
                    _ -> {},
                    () -> {});
            first.take(Stream.VALUES);

            assertThatIllegalStateException()
                    .isThrownBy(() -> batch.select(sparseSelection(2, 1)))
                    .withMessageContaining("taken");
            assertThat(second.borrow(Stream.VALUES)).isNotNull();
            batch.close();
        }
    }

    private static Selection sparseSelection(int positionCount, int... positions)
    {
        return new MaskSelection(Mask.sparse(positions, positionCount));
    }
}
