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
package org.weakref.nitro.operator;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import static org.assertj.core.api.Assertions.assertThat;

class TestStructuralHashJoinIndex
{
    @Test
    void testProbeViewsShareImmutableRowsWithIndependentProbeKeys()
    {
        StructuralKeyKernel kernel = new StructuralKeyKernel()
        {
            @Override
            public boolean identical(
                    Vector leftValues,
                    Vector leftNulls,
                    int leftPosition,
                    Vector rightValues,
                    Vector rightNulls,
                    int rightPosition)
            {
                return VectorAccess.longValues(leftValues).value(leftPosition) ==
                        VectorAccess.longValues(rightValues).value(rightPosition);
            }

            @Override
            public long hash(Vector values, Vector nulls, int position)
            {
                return Long.hashCode(VectorAccess.longValues(values).value(position));
            }
        };
        StructuralHashJoinIndex owner = new StructuralHashJoinIndex(new StructuralKeyKernel[] {kernel});
        Vector[] build = {new I64Vector(new long[] {11, 22, 11})};
        owner.add(build, new Vector[0], 0, 100);
        owner.add(build, new Vector[0], 1, 200);
        owner.add(build, new Vector[0], 2, 300);

        StructuralHashJoinIndex first = owner.newProbeView();
        StructuralHashJoinIndex second = owner.newProbeView();
        Vector[] firstProbe = {new I64Vector(new long[] {11})};
        Vector[] secondProbe = {new I64Vector(new long[] {22})};

        assertThat(first.matches(firstProbe, new Vector[0], 0).toLongArray()).containsExactly(100, 300);
        assertThat(second.matches(secondProbe, new Vector[0], 0).toLongArray()).containsExactly(200);
        assertThat(first.matches(firstProbe, new Vector[0], 0).toLongArray()).containsExactly(100, 300);
        assertThat(first.retainedBytes()).isZero();
        assertThat(second.retainedBytes()).isZero();
        assertThat(owner.retainedBytes()).isPositive();

        first.releaseBuffers();
        second.releaseBuffers();
        assertThat(owner.matches(firstProbe, new Vector[0], 0).toLongArray()).containsExactly(100, 300);
        owner.releaseBuffers();
        assertThat(owner.isEmpty()).isTrue();
    }
}
