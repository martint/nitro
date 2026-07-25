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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;

import static org.assertj.core.api.Assertions.assertThat;

class TestMembershipSet
{
    @Test
    void exactLongMembershipSurvivesRangeExpansionAndEncodedProbe()
    {
        EngineResources engineResources = EngineResources.createDefault();
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("TestMembershipSet");
        MembershipSet set = new MembershipSet(allocator, allocationContext, engineResources.operatorResources());
        try {
            long[] initial = new long[1_024];
            for (int index = 0; index < initial.length; index++) {
                initial[index] = 100_000L + index * 100L;
            }
            set.addBatch(new I64Vector(initial), null, Mask.all(initial.length));
            set.addBatch(new I64Vector(new long[] {-10_000, 240_000}), null, Mask.all(2));

            I32Vector dictionary = new I32Vector(new int[] {100_000, -10_000, 240_000, 7});
            DictionaryVector probe = DictionaryVector.wrap(new int[] {0, 1, 2, 3, 0}, dictionary);
            BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, false, true});
            set.beginProbeBatch(probe, nulls);
            try {
                assertThat(set.contains(0)).isTrue();
                assertThat(set.contains(1)).isTrue();
                assertThat(set.contains(2)).isTrue();
                assertThat(set.contains(3)).isFalse();
                assertThat(set.contains(4)).isFalse();
            }
            finally {
                set.endProbeBatch();
            }
        }
        finally {
            set.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void sparseExtremeLongDomainFallsBackWithoutLosingExactness()
    {
        EngineResources engineResources = EngineResources.createDefault();
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("TestMembershipSet");
        MembershipSet set = new MembershipSet(allocator, allocationContext, engineResources.operatorResources());
        try {
            set.addBatch(
                    new I64Vector(new long[] {Long.MIN_VALUE, 0, Long.MAX_VALUE}),
                    null,
                    Mask.all(3));
            I64Vector probe = new I64Vector(new long[] {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE});
            set.beginProbeBatch(probe, null);
            try {
                assertThat(set.contains(0)).isTrue();
                assertThat(set.contains(1)).isFalse();
                assertThat(set.contains(2)).isTrue();
                assertThat(set.contains(3)).isFalse();
                assertThat(set.contains(4)).isTrue();
            }
            finally {
                set.endProbeBatch();
            }
        }
        finally {
            set.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void denseLongMembershipConvertsToHashWhenLaterKeysEscapeTheBoundedDomain()
    {
        EngineResources engineResources = EngineResources.createDefault();
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("TestMembershipSet");
        MembershipSet set = new MembershipSet(allocator, allocationContext, engineResources.operatorResources());
        try {
            set.addBatch(new I64Vector(new long[] {1, 2, 2, 3}), null, Mask.all(4));
            set.addBatch(new I64Vector(new long[] {100_000_000}), null, Mask.all(1));

            I64Vector probe = new I64Vector(new long[] {0, 1, 2, 3, 4, 100_000_000});
            set.beginProbeBatch(probe, null);
            try {
                assertThat(set.contains(0)).isFalse();
                assertThat(set.contains(1)).isTrue();
                assertThat(set.contains(2)).isTrue();
                assertThat(set.contains(3)).isTrue();
                assertThat(set.contains(4)).isFalse();
                assertThat(set.contains(5)).isTrue();
            }
            finally {
                set.endProbeBatch();
            }
        }
        finally {
            set.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void emptyBuildRejectsEveryProbe()
    {
        EngineResources engineResources = EngineResources.createDefault();
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("TestMembershipSet");
        MembershipSet set = new MembershipSet(allocator, allocationContext, engineResources.operatorResources());
        try {
            set.beginProbeBatch(new I64Vector(new long[] {1, 2}), null);
            try {
                assertThat(set.contains(0)).isFalse();
                assertThat(set.contains(1)).isFalse();
            }
            finally {
                set.endProbeBatch();
            }
        }
        finally {
            set.releaseBuffers();
            allocator.release(allocationContext);
        }
    }
}
