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

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class TestRegionVector
{
    @Test
    void testPrimitiveAccessorsUseLogicalRegion()
    {
        RegionVector longs = new RegionVector(new I64Vector(new long[] {10, 20, 30, 40}), 1, 2);
        RegionVector integers = new RegionVector(new I32Vector(new int[] {1, 2, 3, 4}), 1, 2);
        RegionVector doubles = new RegionVector(new F64Vector(new double[] {1.5, 2.5, 3.5}), 1, 2);
        RegionVector booleans = new RegionVector(new BooleanVector(new boolean[] {true, false, true}), 1, 2);
        BinaryVector binary = new BinaryVector(
                4,
                new int[] {0, 4, 7, 10, 15},
                "zeroonetwothree".getBytes(StandardCharsets.UTF_8));
        RegionVector strings = new RegionVector(binary, 1, 2);

        assertThat(longs.length()).isEqualTo(2);
        assertThat(longs.retainedBytes()).isZero();
        assertThat(longs.childVectorCount()).isZero();
        assertThat(VectorAccess.longValues(longs).value(0)).isEqualTo(20);
        assertThat(VectorAccess.longValues(longs).value(1)).isEqualTo(30);
        assertThat(VectorAccess.longValues(integers).value(0)).isEqualTo(2);
        assertThat(VectorAccess.doubleValues(doubles).value(1)).isEqualTo(3.5);
        assertThat(VectorAccess.booleanValues(booleans).value(0)).isFalse();
        assertThat(new String(
                VectorAccess.binaryRegions(strings).data(1),
                VectorAccess.binaryRegions(strings).offset(1),
                VectorAccess.binaryRegions(strings).length(1),
                StandardCharsets.UTF_8)).isEqualTo("two");
    }

    @Test
    void testCopyOperationsTranslatePositions()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("region-copy");
        RegionVector region = new RegionVector(new I64Vector(new long[] {10, 20, 30, 40, 50}), 1, 3);
        try {
            I64Vector copy = (I64Vector) region.copy(allocator, context);
            assertThat(copy.values()).containsExactly(20, 30, 40);

            I64Vector selected = (I64Vector) region.copy(allocator, context, new int[] {2, 0});
            assertThat(selected.values()).containsExactly(40, 20);
        }
        finally {
            allocator.release(context);
        }
    }

    @Test
    void testMaterializesRowsThroughComposedRleRegions()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("region-materialize");
        I64Vector values = new I64Vector(new long[] {10, 20, 30, 40});
        Vector[] rows = {
                new RleVector(new int[] {2}, new RegionVector(values, 1, 1)),
                new RleVector(new int[] {1}, new RegionVector(values, 3, 1)),
        };
        try {
            I64Vector materialized = (I64Vector) rows[0].materializeRows(allocator, context, rows);
            assertThat(materialized.values()).containsExactly(20, 20, 40);
        }
        finally {
            allocator.release(context);
        }
    }
}
