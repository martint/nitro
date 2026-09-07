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

import it.unimi.dsi.fastutil.longs.LongList;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.type.FixedWidthKeyLayout;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestFixedWidthJoinIndex
{
    @Test
    void testGeneratedBuildAndProbeComposeNestedMappingsAndSkipNulls()
    {
        try (EngineResources resources = EngineResources.createDefault()) {
            FixedWidthJoinIndex index = new FixedWidthJoinIndex(
                    pairLayout(),
                    16,
                    resources.primitiveArrays(),
                    resources.operatorCodeGeneration(),
                    resources.operatorResources().adaptiveLongGroupingPolicy());

            StructVector domain = pairs(
                    new long[] {10, 20, 30},
                    new long[] {100, 200, 300});
            int[] innerIds = {2, 0, 1, 2};
            int[] outerIds = {1, 3, 0, 2, 3, 1};
            DictionaryVector build = DictionaryVector.wrapNested(
                    outerIds,
                    outerIds.length,
                    DictionaryVector.wrapNested(innerIds, innerIds.length, domain));
            DictionaryVector buildNulls = nestedBooleans(
                    outerIds,
                    innerIds,
                    new boolean[] {false, false, true});

            assertThat(index.addBuildRows(
                    new Vector[] {build},
                    new Vector[] {buildNulls},
                    true,
                    Mask.all(build.length()),
                    0)).isTrue();
            assertThat(index.supportsSingleMatchRefs()).isFalse();

            StructVector probe = pairs(
                    new long[] {10, 20, 30, 40, 10},
                    new long[] {100, 200, 300, 400, 100});
            int[] positions = {0, 1, 2, 3, 4};
            LongList[] matches = new LongList[positions.length];
            SingleLongList[] singles = singleLists(positions.length);
            index.matchRows(
                    new Vector[] {probe},
                    new Vector[] {new BooleanVector(new boolean[] {false, false, false, false, true})},
                    true,
                    positions,
                    positions.length,
                    matches,
                    singles);

            assertThat(matches).extracting(LongList::toLongArray).containsExactly(
                    new long[] {JoinRowReference.pack(0, 0), JoinRowReference.pack(0, 5)},
                    new long[] {JoinRowReference.pack(0, 3)},
                    new long[0],
                    new long[0],
                    new long[0]);
            index.releaseBuffers();
        }
    }

    @Test
    void testUniquePreparedProbeViewUsesGeneratedBatchFind()
    {
        try (EngineResources resources = EngineResources.createDefault()) {
            FixedWidthJoinIndex owner = new FixedWidthJoinIndex(
                    pairLayout(),
                    16,
                    resources.primitiveArrays(),
                    resources.operatorCodeGeneration(),
                    resources.operatorResources().adaptiveLongGroupingPolicy());
            StructVector build = pairs(new long[] {1, 2}, new long[] {10, 20});
            owner.addBuildRows(
                    new Vector[] {build},
                    new Vector[0],
                    false,
                    Mask.all(build.length()),
                    7);
            assertThat(owner.supportsSingleMatchRefs()).isTrue();

            FixedWidthJoinIndex probeView = owner.newProbeView();
            DictionaryVector probe = DictionaryVector.wrap(
                    new int[] {1, 0, 2, 1},
                    pairs(new long[] {1, 2, 3}, new long[] {10, 20, 30}));
            long[] refs = new long[probe.length()];
            probeView.matchSingleRows(
                    new Vector[] {probe},
                    new Vector[0],
                    false,
                    new int[] {0, 1, 2, 3},
                    probe.length(),
                    refs);
            assertThat(refs).containsExactly(
                    JoinRowReference.pack(7, 1),
                    JoinRowReference.pack(7, 0),
                    -1,
                    JoinRowReference.pack(7, 1));
            assertThat(probeView.retainedBytes()).isPositive();

            probeView.releaseProbeBuffers();
            owner.releaseBuffers();
        }
    }

    private static ResolvedFixedWidthKeyLayout pairLayout()
    {
        return new ResolvedFixedWidthKeyLayout(
                new ResolvedFixedWidthKeyLayout.Lane[] {
                        new ResolvedFixedWidthKeyLayout.Lane(0, List.of("high"), FixedWidthKeyLayout.Carrier.I64),
                        new ResolvedFixedWidthKeyLayout.Lane(0, List.of("low"), FixedWidthKeyLayout.Carrier.I64)},
                1);
    }

    private static StructVector pairs(long[] high, long[] low)
    {
        StructVector pairs = new StructVector(high.length);
        pairs.setField("high", Streams.ofValues(new I64Vector(high)));
        pairs.setField("low", Streams.ofValues(new I64Vector(low)));
        return pairs;
    }

    private static DictionaryVector nestedBooleans(int[] outerIds, int[] innerIds, boolean[] values)
    {
        return DictionaryVector.wrapNested(
                outerIds,
                outerIds.length,
                DictionaryVector.wrapNested(innerIds, innerIds.length, new BooleanVector(values)));
    }

    private static SingleLongList[] singleLists(int size)
    {
        SingleLongList[] lists = new SingleLongList[size];
        java.util.Arrays.setAll(lists, _ -> new SingleLongList());
        return lists;
    }
}
