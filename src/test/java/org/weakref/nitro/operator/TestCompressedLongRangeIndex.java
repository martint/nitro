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
import org.weakref.nitro.data.PrimitiveArrayPool;

import static org.assertj.core.api.Assertions.assertThat;

class TestCompressedLongRangeIndex
{
    @Test
    void ownsAdmissionPackedLookupAndRelease()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        CompressedLongRangeIndex index = new CompressedLongRangeIndex(arrayPool, true, 2, 4, 2, false);
        long[] keys = {0x10, 0x12, 0, 0};

        assertThat(index.prepare(keys, 2, 5, 3, 0x10, 0x12)).isTrue();
        index.build(keys, new int[] {2, 3, 0, 0}, new int[] {0, 1, 0, 2}, 2, 5, 0, 1);

        int first = index.entry(0x10);
        assertThat(CompressedLongRangeIndex.start(first)).isEqualTo(0);
        assertThat(CompressedLongRangeIndex.count(first)).isEqualTo(2);
        int second = index.entry(0x12);
        assertThat(CompressedLongRangeIndex.start(second)).isEqualTo(2);
        assertThat(CompressedLongRangeIndex.count(second)).isEqualTo(3);
        assertThat(index.entry(0x11)).isZero();
        assertThat(index.entry(0x00)).isZero();

        index.release();
        assertThat(arrayPool.retainedBytes()).isEqualTo(2L * Integer.BYTES);
    }

    @Test
    void rejectsBuildShapesOutsideItsImmutablePolicy()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        long[] keys = {0x10, 0x12, 0, 0};

        assertThat(new CompressedLongRangeIndex(arrayPool, false, 2, 4, 2, false)
                .prepare(keys, 2, 5, 3, 0x10, 0x12)).isFalse();
        assertThat(new CompressedLongRangeIndex(arrayPool, true, 3, 4, 2, false)
                .prepare(keys, 2, 5, 3, 0x10, 0x12)).isFalse();
        assertThat(new CompressedLongRangeIndex(arrayPool, true, 2, 1, 2, false)
                .prepare(keys, 2, 5, 3, 0x10, 0x12)).isFalse();
        assertThat(new CompressedLongRangeIndex(arrayPool, true, 2, 4, 2, false)
                .prepare(keys, 2, 5, 256, 0x10, 0x12)).isFalse();
    }
}
