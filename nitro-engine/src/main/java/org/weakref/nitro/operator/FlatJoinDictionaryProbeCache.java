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

import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;

import static java.util.Objects.requireNonNull;

/**
 * Reusable dictionary-to-group probe state for a flat join index.
 */
final class FlatJoinDictionaryProbeCache
{
    private final PrimitiveArrayPool arrayPool;
    private final boolean enabled;
    private final int maxCardinality;
    private final int minRowsPerEntry;
    private final boolean debug;
    private final Vector[] probeValues = new Vector[1];

    private Vector identity;
    private long generation = -1;
    private int[] groups;
    private boolean debugPrinted;

    FlatJoinDictionaryProbeCache(PrimitiveArrayPool arrayPool, HashJoinIndexPolicy policy)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        requireNonNull(policy, "policy is null");
        this.enabled = policy.flatDictionaryProbeCache();
        this.maxCardinality = policy.flatDictionaryProbeCacheMaxCardinality();
        this.minRowsPerEntry = policy.flatDictionaryProbeCacheMinRowsPerEntry();
        this.debug = policy.debugJoinIndex();
    }

    /**
     * Resolves a small encoded domain once, then maps probe rows through dictionary ids. The exact flat table
     * remains authoritative for each base entry's first lookup. Identity alone is insufficient because pooled
     * binary vectors are reused; a cache generation is valid only while both identity and content generation
     * match. Large or weakly reused dictionaries retain the ordinary row-at-a-time probe path.
     */
    int[] prepare(FlatGroupingTable table, Vector[] values, int positionCount, long groupCount)
    {
        requireNonNull(table, "table is null");
        if (!enabled ||
                values.length != 1 ||
                !(values[0] instanceof DictionaryVector dictionary)) {
            return null;
        }
        Vector base = dictionary.baseValues();
        if (!(base instanceof BinaryVector binary) || !binary.contentImmutable()) {
            return null;
        }
        int cardinality = base.length();
        long contentGeneration = base.contentGeneration();
        if (contentGeneration < 0 ||
                cardinality == 0 ||
                cardinality > maxCardinality ||
                (long) cardinality * minRowsPerEntry > positionCount) {
            return null;
        }
        if (groups == null || groups.length < cardinality) {
            arrayPool.release(groups);
            groups = arrayPool.borrowInts(cardinality);
            identity = null;
            generation = -1;
        }
        if (base != identity || contentGeneration != generation) {
            probeValues[0] = base;
            table.beginBatch(probeValues, null);
            try {
                for (int dictionaryId = 0; dictionaryId < cardinality; dictionaryId++) {
                    groups[dictionaryId] = (int) table.findGroup(probeValues, dictionaryId);
                }
            }
            finally {
                table.endBatch();
            }
            identity = base;
            generation = contentGeneration;
        }
        if (debug && !debugPrinted) {
            debugPrinted = true;
            System.err.printf("[flat-join-dictionary-cache] groups=%d rows=%d cardinality=%d depth=%d%n",
                    groupCount,
                    positionCount,
                    cardinality,
                    dictionary.dictionaryDepth());
        }
        return groups;
    }

    long retainedBytes()
    {
        return groups == null ? 0 : (long) groups.length * Integer.BYTES;
    }

    void release()
    {
        arrayPool.release(groups);
        groups = null;
        identity = null;
        generation = -1;
    }
}
