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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;

import static org.assertj.core.api.Assertions.assertThat;

class TestAllocator
{
    @Test
    void testAllFalseBooleanConstantIsSharedAndUnowned()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("test");

        Vector first = allocator.borrowAllFalseBoolean(context, 5);
        Vector sameLength = allocator.borrowAllFalseBoolean(context, 5);
        Vector differentLength = allocator.borrowAllFalseBoolean(context, 3);

        assertThat(first).isInstanceOf(BooleanVector.class);
        assertThat(sameLength).isSameAs(first);
        assertThat(differentLength).isNotSameAs(first);
        assertThat(first.length()).isEqualTo(5);
        assertThat(differentLength.length()).isEqualTo(3);
        for (int position = 0; position < first.length(); position++) {
            assertThat(VectorAccess.booleanValues(first).value(position)).isFalse();
        }

        allocator.release(context, first);
        allocator.release(context, sameLength);
        assertThat(allocator.borrowAllFalseBoolean(context, 5)).isSameAs(first);
        assertThat(allocator.totalBytes(context)).isZero();
    }
}
