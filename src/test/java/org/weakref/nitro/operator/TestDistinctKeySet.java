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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class TestDistinctKeySet
{
    @Test
    void testDictionarySingleBinaryFastPathUsesActiveMaskFrequency()
    {
        int[] ids = new int[128];
        int[] nonEmptyPositions = new int[64];
        for (int position = 0; position < ids.length; position++) {
            ids[position] = position % 2;
            if (ids[position] != 0) {
                nonEmptyPositions[position / 2] = position;
            }
        }
        DictionaryVector dictionary = dictionary(new String[] {"", "value"}, ids);
        Vector[] values = {dictionary};
        Vector[] nulls = {new BooleanVector(new boolean[ids.length])};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values);
        assertThat(layout).isNotNull();

        layout.beginBatch(values, nulls);
        try {
            assertThat(layout.admitFrequentDictionarySentinel(values, Mask.all(ids.length))).isTrue();
            assertThat(layout.hasTrackedSentinel(values)).isTrue();
        }
        finally {
            layout.endBatch();
        }

        layout.releaseBuffers();

        layout = FlatKeyLayout.tryCreate(values);
        assertThat(layout).isNotNull();
        layout.beginBatch(values, nulls);
        try {
            Mask filtered = Mask.sparse(nonEmptyPositions, ids.length);
            assertThat(layout.admitFrequentDictionarySentinel(values, filtered)).isFalse();
            assertThat(layout.hasTrackedSentinel(values)).isFalse();
        }
        finally {
            layout.endBatch();
            layout.releaseBuffers();
        }
    }

    @Test
    void testNullFreeSingleBinaryDictionaryUsesExactStableValueEquality()
    {
        DictionaryVector first = dictionary(
                new String[] {"", "alpha", "beta", "gamma"},
                new int[] {0, 1, 1, 2, 0, 3});
        DistinctKeySet keys = DistinctKeySet.create(new Vector[] {first});
        try {
            int[] positions = new int[first.length()];
            int distinct = keys.addBatch(
                    new Vector[] {first},
                    new Vector[] {new BooleanVector(new boolean[first.length()])},
                    Mask.all(first.length()),
                    positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1, 3, 5);

            // A new dictionary identity may assign different local ids. Equality must remain value-based across
            // batches while the physical fast path stores and probes query-stable ids internally.
            DictionaryVector second = dictionary(
                    new String[] {"beta", "delta", "", "alpha"},
                    new int[] {0, 1, 2, 3, 1});
            positions = new int[second.length()];
            distinct = keys.addBatch(
                    new Vector[] {second},
                    new Vector[] {new BooleanVector(new boolean[second.length()])},
                    Mask.all(second.length()),
                    positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(1);
        }
        finally {
            keys.releaseBuffers();
        }
    }

    private static DictionaryVector dictionary(String[] entries, int[] ids)
    {
        int bytes = Arrays.stream(entries).mapToInt(String::length).sum();
        BinaryVector values = new BinaryVector(entries.length, bytes);
        for (int index = 0; index < entries.length; index++) {
            values.setBytes(index, entries[index].getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return DictionaryVector.wrap(ids, values);
    }

    @Test
    void testDenseBitmapBatchFallsBackExactlyWhenDomainBecomesSparse()
    {
        long[] denseKeys = new long[4_096];
        for (int index = 0; index < denseKeys.length; index++) {
            denseKeys[index] = index;
        }

        DistinctKeySet keys = DistinctKeySet.create(new Vector[] {new I64Vector(denseKeys)});
        try {
            int[] positions = new int[denseKeys.length];
            assertThat(keys.addBatch(
                    new Vector[] {new I64Vector(denseKeys)},
                    new Vector[] {new BooleanVector(new boolean[denseKeys.length])},
                    Mask.all(denseKeys.length),
                    positions)).isEqualTo(denseKeys.length);

            // The first four distant pages make the paged representation too sparse. Conversion can happen in
            // the middle of this dense batch; the suffix must continue in the hash representation without losing
            // or duplicating the conversion-triggering key.
            long[] sparseSuffix = {4_096, 1L << 20, 2L << 20, 3L << 20, 4L << 20, 5L << 20, 1, 9_999};
            int[] suffixPositions = new int[sparseSuffix.length];
            int distinct = keys.addBatch(
                    new Vector[] {new I64Vector(sparseSuffix)},
                    new Vector[] {new BooleanVector(new boolean[sparseSuffix.length])},
                    Mask.all(sparseSuffix.length),
                    suffixPositions);
            assertThat(Arrays.copyOf(suffixPositions, distinct)).containsExactly(0, 1, 2, 3, 4, 5, 7);

            assertThat(keys.addBatch(
                    new Vector[] {new I64Vector(sparseSuffix)},
                    new Vector[] {new BooleanVector(new boolean[sparseSuffix.length])},
                    Mask.all(sparseSuffix.length),
                    suffixPositions)).isZero();
        }
        finally {
            keys.releaseBuffers();
        }
    }
}
