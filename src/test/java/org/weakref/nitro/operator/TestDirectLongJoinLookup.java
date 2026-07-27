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

class TestDirectLongJoinLookup
{
    private static final long NO_MATCH = -1;

    @Test
    void ownsCompactLookupAndRelease()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        DirectLongJoinLookup lookup = new DirectLongJoinLookup(arrayPool, NO_MATCH);
        int[] references = arrayPool.borrowInts(3);
        references[0] = JoinRowReference.packCompact(JoinRowReference.pack(2, 7));
        references[1] = -1;
        references[2] = JoinRowReference.packCompact(JoinRowReference.pack(2, 9));

        lookup.activateCompact(11, 13, references);

        assertThat(lookup.reference(10, inactiveSequence())).isEqualTo(NO_MATCH);
        assertThat(lookup.reference(11, inactiveSequence())).isEqualTo(JoinRowReference.pack(2, 7));
        assertThat(lookup.reference(12, inactiveSequence())).isEqualTo(NO_MATCH);
        assertThat(lookup.reference(13, inactiveSequence())).isEqualTo(JoinRowReference.pack(2, 9));
        lookup.release();
        assertThat(lookup.isActive()).isFalse();
        assertThat(arrayPool.retainedBytes()).isGreaterThan(0);
    }

    @Test
    void supportsFullAndArithmeticRepresentations()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        DirectLongJoinLookup fullLookup = new DirectLongJoinLookup(arrayPool, NO_MATCH);
        long[] references = arrayPool.borrowLongs(2);
        references[0] = JoinRowReference.pack(7, 3);
        references[1] = JoinRowReference.pack(7, 5);
        fullLookup.activateFull(20, 21, references);

        assertThat(fullLookup.reference(20, inactiveSequence())).isEqualTo(references[0]);
        assertThat(fullLookup.reference(21, inactiveSequence())).isEqualTo(references[1]);
        fullLookup.release();

        DirectLongJoinLookup arithmeticLookup = new DirectLongJoinLookup(arrayPool, NO_MATCH);
        DenseJoinSequence sequence = new DenseJoinSequence(arrayPool, true, true);
        long base = JoinRowReference.pack(4, 8);
        sequence.activateReferences(4, 8, base);
        arithmeticLookup.activateArithmetic(30, 31);

        assertThat(arithmeticLookup.reference(30, sequence)).isEqualTo(base);
        assertThat(arithmeticLookup.reference(31, sequence)).isEqualTo(base + 1);
        arithmeticLookup.release();
    }

    private static DenseJoinSequence inactiveSequence()
    {
        return new DenseJoinSequence(new PrimitiveArrayPool(1024, 0), false, false);
    }
}
