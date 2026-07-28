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

class TestErrorVector
{
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
}
