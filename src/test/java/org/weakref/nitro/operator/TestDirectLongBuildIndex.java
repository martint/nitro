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

class TestDirectLongBuildIndex
{
    @Test
    void ownsHeadMapDuplicateRepresentationsGrowthAndRelease()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        DirectLongBuildIndex index = new DirectLongBuildIndex(arrayPool, true, 4, 2, 1, -1);
        index.initialize(8);
        index.recordRow();
        index.recordRow();
        assertThat(index.rowCount()).isEqualTo(2);

        index.initializeKey(3, 11);
        assertThat(index.entry(3)).isEqualTo(11);
        assertThat(index.entryHead(index.entry(3))).isEqualTo(11);
        assertThat(index.entryCount(3, index.entry(3))).isEqualTo(1);

        assertThat(index.admitSparseDuplicates(4)).isTrue();
        int sparseEntry = index.promoteSparseDuplicate(3, index.entry(3));
        index.incrementSparseDuplicate(sparseEntry);
        assertThat(index.entryHead(sparseEntry)).isEqualTo(11);
        assertThat(index.entryCount(3, sparseEntry)).isEqualTo(2);
        assertThat(index.appendSparseDuplicate(sparseEntry, 17)).isEqualTo(11);
        assertThat(index.entryTail(3, sparseEntry)).isEqualTo(17);

        index.ensureCapacity(9);
        assertThat(index.capacity()).isEqualTo(16);
        assertThat(index.entry(3)).isEqualTo(sparseEntry);
        assertThat(index.entry(12)).isEqualTo(-1);

        index.release();
        assertThat(index.isActive()).isFalse();
        assertThat(index.rowCount()).isZero();
        assertThat(arrayPool.retainedBytes()).isGreaterThan(0);
    }
}
