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
import org.weakref.nitro.execution.EngineResources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestErrorVector
{
    @Test
    void testClearsOnlySelectedPositionsAndInvalidatesSummary()
    {
        ErrorVector errors = new ErrorVector(512);
        ErrorValue failure = new ErrorValue("test", 1, "FAILURE", "USER_ERROR", "failure");
        errors.setError(129, failure);
        errors.setError(255, failure);
        errors.setError(400, failure);
        assertThat(errors.isAllFalse()).isFalse();
        long generation = errors.contentGeneration();
        errors.clearErrors(Mask.none(512));
        assertThat(errors.contentGeneration()).isEqualTo(generation);
        assertThat(errors.error(129)).isEqualTo(failure);

        errors.clearErrors(Mask.all(256));
        assertThat(errors.error(129)).isNull();
        assertThat(errors.error(255)).isNull();
        assertThat(errors.values()[129]).isFalse();
        assertThat(errors.values()[255]).isFalse();
        assertThat(errors.error(400)).isEqualTo(failure);
        assertThat(errors.isAllFalse()).isFalse();

        errors.clearErrors(Mask.sparse(new int[] {400}, 512));
        assertThat(errors.error(400)).isNull();
        assertThat(errors.isAllFalse()).isTrue();
        errors.setError(255, failure);
        errors.clearErrors(Mask.all(512));
        assertThat(errors.error(255)).isNull();
        assertThat(errors.isAllFalse()).isTrue();
        errors.setError(255);
        assertThat(errors.error(255)).isNull();
        assertThat(errors.isAllFalse()).isFalse();
        errors.setError(255, failure);
        errors.setError(255);
        assertThat(errors.error(255)).isNull();
        assertThat(errors.values()[255]).isTrue();
    }

    @Test
    void testClearingDoesNotAllocateDiagnosticsAndRejectsOversizedMask()
    {
        ErrorVector errors = new ErrorVector(512);
        long retainedBytes = errors.retainedBytes();
        errors.markAllTrue();
        errors.clearErrors(Mask.all(256));
        assertThat(errors.values()[129]).isFalse();
        assertThat(errors.values()[400]).isTrue();
        errors.clearErrors(Mask.sparse(new int[] {400}, 512));
        assertThat(errors.values()[400]).isFalse();
        assertThat(errors.retainedBytes()).isEqualTo(retainedBytes);
        assertThatThrownBy(() -> errors.clearErrors(Mask.all(513)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Mask exceeds error vector length");
    }

    @Test
    void testInvalidatesAllFalseSummaryWhenErrorIsSet()
    {
        ErrorVector errors = new ErrorVector(2);
        errors.markAllFalse();
        assertThat(errors.isAllFalse()).isTrue();

        errors.setError(1, new ErrorValue("test", 1, "FAILURE", "USER_ERROR", "failure"));

        assertThat(errors.isAllFalse()).isFalse();
    }

    @Test
    void testAllocatesDiagnosticStorageOnlyWhenAnErrorIsRecorded()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("test");
            ErrorVector errors = allocator.allocate(context, ErrorVector.class, 16, ErrorVector::new);
            long booleanBytes = errors.retainedBytes();

            assertThat(errors.error(0)).isNull();
            assertThat(errors.retainedBytes()).isEqualTo(booleanBytes);
            assertThat(allocator.residentBytes()).isEqualTo(booleanBytes);

            errors.setError(3, new ErrorValue("test", 1, "FAILURE", "USER_ERROR", "failure"));

            assertThat(errors.retainedBytes()).isEqualTo(booleanBytes + 16L * Long.BYTES);
            assertThat(allocator.residentBytes()).isEqualTo(errors.retainedBytes());
        }
    }

    @Test
    void testLazyAndAllocatedEmptyDiagnosticsHaveTheSameContent()
    {
        ErrorVector lazy = new ErrorVector(2);
        ErrorVector allocated = new ErrorVector(2);
        allocated.setError(0, new ErrorValue("test", 1, "FAILURE", "USER_ERROR", "failure"));
        allocated.clearError(0);

        assertThat(lazy.hasSameContent(allocated)).isTrue();
        assertThat(lazy.contentFingerprint()).isEqualTo(allocated.contentFingerprint());
    }

    @Test
    void testCopiesDiagnosticsThroughGenericVectorOperations()
    {
        ErrorValue first = new ErrorValue("test", 1, "FIRST", "USER_ERROR", "first");
        ErrorValue second = new ErrorValue("test", 2, "SECOND", "USER_ERROR", "second");
        ErrorVector source = new ErrorVector(4);
        source.setError(1, first);
        source.setError(3, second);

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("test");
            ErrorVector selected = (ErrorVector) source.copy(allocator, context, new int[] {3, 0, 1});
            ErrorVector masked = (ErrorVector) source.copyMasked(
                    allocator,
                    context,
                    null,
                    Mask.sparse(new int[] {1, 3}, 4));

            assertThat(selected.values()).containsExactly(true, false, true);
            assertThat(selected.error(0)).isEqualTo(second);
            assertThat(selected.error(2)).isEqualTo(first);
            assertThat(masked.values()).containsExactly(false, true, false, true);
            assertThat(masked.error(1)).isEqualTo(first);
            assertThat(masked.error(3)).isEqualTo(second);
        }
    }

    @Test
    void testMaterializesMixedBooleanRepresentationsWithoutLosingDiagnostics()
    {
        ErrorValue failure = new ErrorValue("test", 1, "FAILURE", "USER_ERROR", "failure");
        ErrorVector diagnostic = new ErrorVector(2);
        diagnostic.setError(1, failure);
        BooleanVector legacy = new BooleanVector(new boolean[] {true, false});

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("test");
            ErrorVector materialized = (ErrorVector) diagnostic.materializeRows(
                    allocator,
                    context,
                    new Vector[] {diagnostic, new DictionaryVector(new int[] {0, 1}, legacy)});

            assertThat(materialized.values()).containsExactly(false, true, true, false);
            assertThat(materialized.error(1)).isEqualTo(failure);
            assertThat(materialized.error(2)).isNull();
        }
    }
}
