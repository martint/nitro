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

class TestJoinRowStore
{
    private static final int EMPTY = -1;

    @Test
    void ownsCompactReferencesDuplicateLinksGrowthAndRelease()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        JoinRowStore rows = new JoinRowStore(arrayPool, 2, true, false, true, false, EMPTY);

        long first = JoinRowReference.pack(3, 7);
        long second = JoinRowReference.pack(3, 9);
        long third = JoinRowReference.pack(4, 11);
        rows.append(0, first);
        rows.append(1, second);
        rows.ensureChainState(2);
        rows.link(0, 1);
        rows.append(2, third);
        rows.link(1, 2);

        assertThat(rows.referenceAt(2)).isEqualTo(third);
        assertThat(rows.next(0)).isEqualTo(1);
        ChainLongList chain = rows.resetChain(new ChainLongList(), 0, 3);
        assertThat(chain.getLong(0)).isEqualTo(first);
        assertThat(chain.getLong(1)).isEqualTo(second);
        assertThat(chain.getLong(2)).isEqualTo(third);
        int[] packed = rows.packReferences32(3);
        assertThat(packed).containsExactly(
                JoinRowReference.packCompact(first),
                JoinRowReference.packCompact(second),
                JoinRowReference.packCompact(third));
        arrayPool.release(packed);

        rows.release();
        assertThat(arrayPool.retainedBytes()).isGreaterThan(0);
    }

    @Test
    void materializesImplicitReferencesWhenSequenceDiverges()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        JoinRowStore rows = new JoinRowStore(arrayPool, 4, true, true, true, true, EMPTY);

        long base = JoinRowReference.pack(2, 10);
        rows.append(0, base);
        rows.append(1, base + 1);
        rows.append(2, base + 4);

        assertThat(rows.implicitSequentialReferences()).isFalse();
        assertThat(rows.referenceAt(0)).isEqualTo(base);
        assertThat(rows.referenceAt(1)).isEqualTo(base + 1);
        assertThat(rows.referenceAt(2)).isEqualTo(base + 4);

        rows.release();
    }

    @Test
    void widensCompactStorageWhenReferenceDoesNotFit()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        JoinRowStore rows = new JoinRowStore(arrayPool, 4, true, false, true, false, EMPTY);

        long compact = JoinRowReference.pack(1, 2);
        long wide = JoinRowReference.pack(JoinRowReference.MAX_COMPACT_BATCH_INDEX + 1, 3);
        rows.observeReference(compact);
        rows.append(0, compact);
        rows.observeReference(wide);
        rows.append(1, wide);

        assertThat(rows.referencesFit32()).isFalse();
        assertThat(rows.referenceAt(0)).isEqualTo(compact);
        assertThat(rows.referenceAt(1)).isEqualTo(wide);

        rows.release();
    }
}
