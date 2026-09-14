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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestBinaryRegions
{
    @Test
    void sharesBackingArrayThroughPhysicalMappings()
    {
        BinaryVector values = new BinaryVector(3, 16);
        values.setBytes(0, "alpha".getBytes(UTF_8));
        values.setBytes(1, "é".getBytes(UTF_8));
        values.setBytes(2, new byte[0]);
        for (Vector vector : List.of(
                values,
                new RegionVector(values, 1, 2),
                new DictionaryVector(new int[] {2, 0, 1, 0}, values),
                new RleVector(new int[] {2, 3, 1}, values),
                new DictionaryVector(new int[] {1, 0, 1}, new RegionVector(values, 1, 2)),
                new RegionVector(new RleVector(new int[] {2, 3, 1}, values), 1, 4))) {
            VectorAccess.BinaryRegions regions = VectorAccess.binaryRegions(vector);
            assertThat(regions.sharedData()).containsSame(values.data());
            for (int position = 0; position < vector.length(); position++) {
                assertThat(regions.data(position)).isSameAs(regions.sharedData().orElseThrow());
            }
        }
    }

    @Test
    void discoversStorageWithoutReadingAPosition()
    {
        BinaryVector empty = new BinaryVector(0, 0);
        assertThat(VectorAccess.binaryRegions(empty).sharedData()).containsSame(empty.data());
        assertThat(VectorAccess.binaryRegions(new DictionaryVector(new int[0], empty)).sharedData())
                .containsSame(empty.data());
    }

    @Test
    void doesNotAssumeCustomAccessorsShareStorage()
    {
        byte[][] buffers = {{1}, {2}};
        VectorAccess.BinaryRegions regions = new VectorAccess.BinaryRegions()
        {
            @Override
            public byte[] data(int position)
            {
                return buffers[position];
            }

            @Override
            public int offset(int position)
            {
                return 0;
            }

            @Override
            public int length(int position)
            {
                return 1;
            }
        };
        assertThat(regions.sharedData()).isEmpty();
    }
}
