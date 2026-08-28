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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TestStructuralHashJoinIndex
{
    @Test
    void testAlignedDictionaryKeysAreProbedOncePerPhysicalTuple()
    {
        AtomicInteger firstHashCalls = new AtomicInteger();
        AtomicInteger secondHashCalls = new AtomicInteger();
        StructuralHashJoinIndex index = new StructuralHashJoinIndex(new StructuralKeyKernel[] {
                countingLongKernel(firstHashCalls),
                countingLongKernel(secondHashCalls)});
        Vector[] build = {
                new I64Vector(new long[] {11, 22}),
                new I64Vector(new long[] {110, 220})};
        index.add(build, new Vector[0], 0, 100);
        index.add(build, new Vector[0], 1, 200);
        firstHashCalls.set(0);
        secondHashCalls.set(0);

        DictionaryVector first = new DictionaryVector(
                new int[] {0, 1, 0, 0, 1},
                new I64Vector(new long[] {11, 22}));
        DictionaryVector second = first.sharedMappingWithValues(new I64Vector(new long[] {110, 220}));
        LongList[] matches = new LongList[5];
        index.matchRows(
                new Vector[] {first, second},
                new Vector[0],
                false,
                new int[] {0, 1, 2, 3, 4},
                5,
                matches,
                new SingleLongList[5]);

        assertThat(matches).extracting(LongList::toLongArray).containsExactly(
                new long[] {100},
                new long[] {200},
                new long[] {100},
                new long[] {100},
                new long[] {200});
        assertThat(firstHashCalls).hasValue(2);
        assertThat(secondHashCalls).hasValue(2);
    }

    @Test
    void testAlignedSingleRunKeysAreProbedOnce()
    {
        AtomicInteger firstHashCalls = new AtomicInteger();
        AtomicInteger secondHashCalls = new AtomicInteger();
        StructuralHashJoinIndex index = new StructuralHashJoinIndex(new StructuralKeyKernel[] {
                countingLongKernel(firstHashCalls),
                countingLongKernel(secondHashCalls)});
        Vector[] build = {
                new I64Vector(new long[] {11}),
                new I64Vector(new long[] {110})};
        index.add(build, new Vector[0], 0, 100);
        firstHashCalls.set(0);
        secondHashCalls.set(0);

        RleVector first = new RleVector(new int[] {5}, new I64Vector(new long[] {11}));
        RleVector second = new RleVector(new int[] {5}, new I64Vector(new long[] {110}));
        LongList[] matches = new LongList[5];
        index.matchRows(
                new Vector[] {first, second},
                new Vector[0],
                false,
                new int[] {0, 1, 2, 3, 4},
                5,
                matches,
                new SingleLongList[5]);

        assertThat(matches).extracting(LongList::toLongArray).containsOnly(new long[] {100});
        assertThat(firstHashCalls).hasValue(1);
        assertThat(secondHashCalls).hasValue(1);
    }

    @Test
    void testUniqueStructuralDomainSupportsSingleMatchProbe()
    {
        AtomicInteger hashCalls = new AtomicInteger();
        StructuralHashJoinIndex index = new StructuralHashJoinIndex(new StructuralKeyKernel[] {countingLongKernel(hashCalls)});
        Vector[] build = {new I64Vector(new long[] {11, 22})};
        index.add(build, new Vector[0], 0, 100);
        index.add(build, new Vector[0], 1, 200);
        assertThat(index.supportsSingleMatchRefs()).isTrue();

        hashCalls.set(0);
        DictionaryVector probe = new DictionaryVector(
                new int[] {0, 1, 0, 2},
                new I64Vector(new long[] {11, 22, 33}));
        long[] refs = new long[4];
        index.matchSingleRows(
                new Vector[] {probe},
                new Vector[0],
                false,
                new int[] {0, 1, 2, 3},
                4,
                refs);

        assertThat(refs).containsExactly(100, 200, 100, -1);
        assertThat(hashCalls).hasValue(3);
        assertThat(index.newProbeView().supportsSingleMatchRefs()).isTrue();
    }

    @Test
    void testDuplicateStructuralDomainRejectsSingleMatchProbe()
    {
        StructuralHashJoinIndex index = new StructuralHashJoinIndex(new StructuralKeyKernel[] {countingLongKernel(new AtomicInteger())});
        Vector[] build = {new I64Vector(new long[] {11, 11})};
        index.add(build, new Vector[0], 0, 100);
        index.add(build, new Vector[0], 1, 200);

        assertThat(index.supportsSingleMatchRefs()).isFalse();
        assertThat(index.newProbeView().supportsSingleMatchRefs()).isFalse();
    }

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

    private static StructuralKeyKernel countingLongKernel(AtomicInteger hashCalls)
    {
        return new StructuralKeyKernel()
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
                hashCalls.incrementAndGet();
                return Long.hashCode(VectorAccess.longValues(values).value(position));
            }
        };
    }
}
