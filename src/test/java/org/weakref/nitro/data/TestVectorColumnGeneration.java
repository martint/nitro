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

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestVectorColumnGeneration
{
    @Test
    void testCachesBorrowAndTransfersThroughGenerationOwner()
    {
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources);
                VectorBatchScope scope = new VectorBatchScope(allocator, "source")) {
            scope.begin(null);
            I64Vector vector = allocator.allocate(scope.context(), I64Vector.class, 8, I64Vector::new);
            int[] resolutions = new int[1];
            VectorColumnGeneration column = new VectorColumnGeneration(
                    Set.of(Stream.VALUES),
                    _ -> {
                        resolutions[0]++;
                        return vector;
                    },
                    scope);

            assertThat(column.borrow(Stream.VALUES)).isSameAs(vector);
            assertThat(column.borrow(Stream.VALUES)).isSameAs(vector);
            assertThat(resolutions[0]).isOne();
            assertThat(column.take(Stream.VALUES)).isSameAs(vector);
            column.close();
            scope.endBatch();

            scope.begin(null);
            I64Vector replacement = allocator.allocate(scope.context(), I64Vector.class, 8, I64Vector::new);
            assertThat(replacement).isNotSameAs(vector);
            scope.endBatch();
        }
    }

    @Test
    void testSelectionSensitiveCacheCanBeInvalidatedUntilTaken()
    {
        I64Vector first = new I64Vector(1);
        I64Vector second = new I64Vector(1);
        int[] resolutions = new int[1];
        VectorColumnGeneration column = new VectorColumnGeneration(
                Set.of(Stream.VALUES),
                _ -> resolutions[0]++ == 0 ? first : second)
                .withConstraintSensitiveResolution();

        assertThat(column.borrow(Stream.VALUES)).isSameAs(first);
        column.invalidateResolvedForConstraint();
        assertThat(column.borrow(Stream.VALUES)).isSameAs(second);
        column.take(Stream.VALUES);

        assertThatIllegalStateException()
                .isThrownBy(column::invalidateResolvedForConstraint)
                .withMessageContaining("taken");
        column.close();
    }
}
