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
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Reusable encoded-domain-to-group probe state for a flat join index.
 */
final class FlatJoinEncodedProbeCache
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

    FlatJoinEncodedProbeCache(PrimitiveArrayPool arrayPool, HashJoinIndexPolicy policy)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        requireNonNull(policy, "policy is null");
        this.enabled = policy.flatDictionaryProbeCache();
        this.maxCardinality = policy.flatDictionaryProbeCacheMaxCardinality();
        this.minRowsPerEntry = policy.flatDictionaryProbeCacheMinRowsPerEntry();
        this.debug = policy.debugJoinIndex();
    }

    /**
     * Resolves a small aligned encoded domain once, then maps probe rows through its physical mapping. Dictionary
     * fields may share one exact row mapping and may be combined with single-run RLE constants; all-RLE keys form a
     * one-entry domain. The exact flat table remains authoritative for every domain entry's first lookup, and the
     * bound layout still owns complete recursive identity and null policy. Identity alone is insufficient because
     * pooled vectors are reused, so cross-batch reuse requires immutable content plus a matching generation. Other
     * structural domains are recomputed once per batch. Large or weakly reused domains retain the ordinary probe.
     */
    Prepared prepare(FlatGroupingTable table, Vector[] values, Vector[] nulls, int positionCount, long groupCount)
    {
        requireNonNull(table, "table is null");
        if (!enabled || values.length == 0) {
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

        DictionaryVector dictionary = null;
        for (Vector value : values) {
            if (value instanceof DictionaryVector candidate) {
                dictionary = candidate;
                break;
            }
        }
        int[] rowIds = null;
        int cardinality;
        if (dictionary != null) {
            cardinality = dictionary.values().length();
            rowIds = dictionary.ids();
            for (int field = 0; field < values.length; field++) {
                Vector value = values[field];
                if (value instanceof DictionaryVector fieldDictionary) {
                    if (!dictionary.hasSameRowMapping(fieldDictionary) ||
                            fieldDictionary.values().length() != cardinality) {
                        return null;
                    }
                    // Bind the immediate aligned domain. Nested mappings remain part of the generated field binding.
                    probeValues[field] = fieldDictionary.values();
                }
                else if (value instanceof RleVector rle &&
                        rle.counts().length == 1 &&
                        rle.length() >= cardinality) {
                    // A single-run value is constant over every position in the dictionary domain.
                    probeValues[field] = rle;
                }
                else {
                    return null;
                }
            }
        }
        else {
            cardinality = 1;
            for (int field = 0; field < values.length; field++) {
                if (!(values[field] instanceof RleVector rle) || rle.counts().length != 1) {
                    return null;
                }
                probeValues[field] = rle.values();
            }
        }
        if (cardinality == 0 ||
                cardinality > maxCardinality ||
                (long) cardinality * minRowsPerEntry > positionCount) {
            return null;
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
                for (int domainPosition = 0; domainPosition < cardinality; domainPosition++) {
                    groups[domainPosition] = table.boundInputHasAnyNull(domainPosition)
                            ? -1
                            : (int) table.findGroup(probeValues, domainPosition);
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
            System.err.printf("[flat-join-encoded-cache] groups=%d rows=%d cardinality=%d mapping=%s%n",
                    groupCount,
                    positionCount,
                    cardinality,
                    dictionary == null ? "single-run-rle" : "dictionary");
        }
        return new Prepared(groups, rowIds);
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

    record Prepared(int[] groups, int[] rowIds) {}
}
