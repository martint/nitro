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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestPrimitiveMaskedCopies
{
    @Test
    void testEmptySparseAndDenseCopiesPreserveInactivePositionsAndPhysicalBits()
    {
        int size = 512;
        I32Vector integers = new I32Vector(size);
        I64Vector longs = new I64Vector(size);
        F64Vector doubles = new F64Vector(size);
        BooleanVector booleans = new BooleanVector(size);
        for (int position = 0; position < size; position++) {
            integers.values()[position] = position + 1;
            longs.values()[position] = (1L << 40) + position;
            doubles.values()[position] = position % 2 == 0 ? -0.0 : Double.longBitsToDouble(0x7ff8000000001234L);
            booleans.values()[position] = true;
        }
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("masked-copies");
            I32Vector integerTarget = allocator.allocate(context, I32Vector.class, size, I32Vector::new);
            I64Vector longTarget = allocator.allocate(context, I64Vector.class, size, I64Vector::new);
            F64Vector doubleTarget = allocator.allocate(context, F64Vector.class, size, F64Vector::new);
            BooleanVector booleanTarget = allocator.allocate(context, BooleanVector.class, size, BooleanVector::new);
            boolean[] copiedPositions = new boolean[size];
            for (Mask mask : List.of(Mask.sparse(new int[0], size), Mask.sparse(new int[] {129, 300, 511}, size), Mask.all(256), Mask.all(size))) {
                assertThat(integers.copyMasked(allocator, context, integerTarget, mask)).isSameAs(integerTarget);
                assertThat(longs.copyMasked(allocator, context, longTarget, mask)).isSameAs(longTarget);
                assertThat(doubles.copyMasked(allocator, context, doubleTarget, mask)).isSameAs(doubleTarget);
                assertThat(booleans.copyMasked(allocator, context, booleanTarget, mask)).isSameAs(booleanTarget);
                for (int position : mask) {
                    copiedPositions[position] = true;
                }
                for (int position = 0; position < size; position++) {
                    boolean selected = copiedPositions[position];
                    assertThat(integerTarget.values()[position]).isEqualTo(selected ? integers.values()[position] : 0);
                    assertThat(longTarget.values()[position]).isEqualTo(selected ? longs.values()[position] : 0);
                    assertThat(Double.doubleToRawLongBits(doubleTarget.values()[position]))
                            .isEqualTo(selected ? Double.doubleToRawLongBits(doubles.values()[position]) : 0);
                    assertThat(booleanTarget.values()[position]).isEqualTo(selected);
                }
            }
        }
    }
}
