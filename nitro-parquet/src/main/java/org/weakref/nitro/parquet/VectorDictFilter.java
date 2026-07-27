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
package org.weakref.nitro.parquet;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD predicate-over-dictionary tile compaction — the Java-Vector-API equivalent of Velox's
 * {@code DictionaryColumnVisitor::processRun} hot loop (which the q20 asm profile showed spends ~80% in AVX2
 * {@code vpgatherdd}). Given a tile of decoded dictionary ids, it gathers {@code accept[id]} and {@code dict[id]}
 * eight lanes at a time ({@code vpgatherqq}/{@code vpgatherdd}), builds a survivor mask, and compacts the surviving
 * positions and values with {@code compress} ({@code vpcompressd}/{@code vpcompressq}) — replacing the scalar
 * per-id {@code laload}/{@code movzbl}/{@code iastore} loop.
 */
final class VectorDictFilter
{
    private static final VectorSpecies<Integer> I_SPECIES = IntVector.SPECIES_256; // 8 int lanes
    private static final VectorSpecies<Long> L_SPECIES = LongVector.SPECIES_512;   // 8 long lanes
    private static final int[] LANE_INDEX = {0, 1, 2, 3, 4, 5, 6, 7};

    private VectorDictFilter() {}

    /** True when the pinned 8-lane int/long species map to real hardware vectors (AVX-512 => the gather is 8-wide). */
    static boolean supported()
    {
        return I_SPECIES.length() == 8 && L_SPECIES.length() == 8;
    }

    /**
     * For each {@code id = tile[i]} in {@code [0, tileRows)} where {@code accept[id] != 0}, emit survivor position
     * {@code positionBase + i} into {@code survivorsOut} and {@code dict[id]} into {@code valuesOut}, starting at
     * {@code sc}; returns the advanced survivor cursor. Output order matches the scalar path (byte-identical).
     */
    static int compactTile(int[] tile, int tileRows, int positionBase, int[] accept, long[] dict,
            int[] survivorsOut, long[] valuesOut, int sc)
    {
        IntVector lanes = IntVector.fromArray(I_SPECIES, LANE_INDEX, 0);
        int i = 0;
        int bound = I_SPECIES.loopBound(tileRows);
        for (; i < bound; i += 8) {
            // Gather accept[id] for 8 ids and select the survivors.
            IntVector accepted = IntVector.fromArray(I_SPECIES, accept, 0, tile, i);
            VectorMask<Integer> keep = accepted.compare(VectorOperators.NE, 0);
            int n = keep.trueCount();
            if (n != 0) {
                // Compact the 8 positions (base + lane) to the low n lanes and store exactly n.
                IntVector positions = lanes.add(positionBase + i);
                positions.compress(keep).intoArray(survivorsOut, sc, I_SPECIES.indexInRange(0, n));
                // Gather dict[id] (8 longs) and compact likewise.
                LongVector values = LongVector.fromArray(L_SPECIES, dict, 0, tile, i);
                values.compress(keep.cast(L_SPECIES)).intoArray(valuesOut, sc, L_SPECIES.indexInRange(0, n));
                sc += n;
            }
        }
        for (; i < tileRows; i++) {
            int id = tile[i];
            if (accept[id] != 0) {
                survivorsOut[sc] = positionBase + i;
                valuesOut[sc] = dict[id];
                sc++;
            }
        }
        return sc;
    }
}
