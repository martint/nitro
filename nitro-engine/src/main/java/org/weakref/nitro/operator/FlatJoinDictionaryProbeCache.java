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

import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

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
    private Vector[] probeValues = new Vector[0];

    private Vector[] identities = new Vector[0];
    private long[] generations = new long[0];
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
     * remains authoritative for each base entry's first lookup. Any number of fields may participate when they
     * share the exact row mapping; the bound layout still owns their complete recursive identity and null policy.
     * Identity alone is insufficient because pooled vectors are reused, so cross-batch reuse requires immutable
     * content plus a matching generation. Other structural domains are recomputed once per batch. Large or weakly
     * reused dictionaries retain the ordinary row-at-a-time probe path.
     */
    int[] prepare(FlatGroupingTable table, Vector[] values, Vector[] nulls, int positionCount, long groupCount)
    {
        requireNonNull(table, "table is null");
        if (!enabled ||
                values.length == 0 ||
                !(values[0] instanceof DictionaryVector dictionary)) {
            return null;
        }
        if (nulls != null) {
            for (Vector nullVector : nulls) {
                if (!VectorAccess.isAllFalseNulls(nullVector)) {
                    return null;
                }
            }
        }
        if (probeValues.length != values.length) {
            probeValues = new Vector[values.length];
            identities = new Vector[values.length];
            generations = new long[values.length];
            Arrays.fill(generations, -1);
        }
        probeValues[0] = dictionary.baseValues();
        for (int field = 1; field < values.length; field++) {
            if (!(values[field] instanceof DictionaryVector fieldDictionary) ||
                    !dictionary.hasSameRowMapping(fieldDictionary)) {
                return null;
            }
            probeValues[field] = fieldDictionary.baseValues();
        }
        int cardinality = probeValues[0].length();
        if (cardinality == 0 ||
                cardinality > maxCardinality ||
                (long) cardinality * minRowsPerEntry > positionCount) {
            return null;
        }
        for (int field = 1; field < probeValues.length; field++) {
            if (probeValues[field].length() != cardinality) {
                return null;
            }
        }
        if (groups == null || groups.length < cardinality) {
            arrayPool.release(groups);
            groups = arrayPool.borrowInts(cardinality);
            Arrays.fill(identities, null);
            Arrays.fill(generations, -1);
        }
        boolean cacheable = true;
        boolean current = true;
        for (int field = 0; field < probeValues.length; field++) {
            Vector base = probeValues[field];
            long generation = base.contentGeneration();
            cacheable &= base.contentImmutable() && generation >= 0;
            current &= base == identities[field] && generation == generations[field];
        }
        if (!cacheable || !current) {
            table.beginBatch(probeValues, null);
            try {
                for (int dictionaryId = 0; dictionaryId < cardinality; dictionaryId++) {
                    groups[dictionaryId] = table.boundInputHasAnyNull(dictionaryId)
                            ? -1
                            : (int) table.findGroup(probeValues, dictionaryId);
                }
            }
            finally {
                table.endBatch();
            }
            for (int field = 0; field < probeValues.length; field++) {
                identities[field] = cacheable ? probeValues[field] : null;
                generations[field] = cacheable ? probeValues[field].contentGeneration() : -1;
            }
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
        Arrays.fill(identities, null);
        Arrays.fill(generations, -1);
    }
}
