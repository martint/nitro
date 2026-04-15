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
package org.weakref.nitro.data;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

public final class ProjectedRowsDebug
{
    private static final boolean ENABLED = Boolean.getBoolean("nitro.projected.rows.debug");

    private static final LongAdder projectedRowsCreated = new LongAdder();
    private static final LongAdder projectedRowsCreatedPositions = new LongAdder();

    private static final LongAdder composeCalls = new LongAdder();
    private static final LongAdder composeRows = new LongAdder();
    private static final LongAdder composeIdentityHits = new LongAdder();
    private static final LongAdder composeLastHits = new LongAdder();
    private static final LongAdder composeMapHits = new LongAdder();
    private static final LongAdder composeMisses = new LongAdder();

    private static final LongAdder projectCalls = new LongAdder();
    private static final LongAdder projectRows = new LongAdder();
    private static final LongAdder projectIdentityHits = new LongAdder();
    private static final LongAdder projectLastHits = new LongAdder();
    private static final LongAdder projectMapHits = new LongAdder();
    private static final LongAdder projectMisses = new LongAdder();

    private static final LongAdder materializeCalls = new LongAdder();
    private static final LongAdder materializeRows = new LongAdder();
    private static final LongAdder materializeWithTargetCalls = new LongAdder();
    private static final LongAdder materializeCacheHits = new LongAdder();
    private static final LongAdder materializeCacheMisses = new LongAdder();

    private static final LongAdder wrapCalls = new LongAdder();
    private static final LongAdder wrapRows = new LongAdder();
    private static final LongAdder wrapSelectionInputs = new LongAdder();
    private static final LongAdder wrapSelectionIdentityInputs = new LongAdder();
    private static final LongAdder wrapDictionaryInputs = new LongAdder();
    private static final LongAdder wrapRleInputs = new LongAdder();
    private static final LongAdder wrapBaseInputs = new LongAdder();
    private static final LongAdder wrapSelectionProjectPairUnique = new LongAdder();
    private static final LongAdder wrapSelectionProjectPairRepeat = new LongAdder();
    private static final Map<ProjectedRows, IdentityHashMap<SelectedPositions, Boolean>> wrapSelectionProjectPairs = ENABLED ? new IdentityHashMap<>() : null;
    private static final LongAdder wrapSelectionPositionsPairUnique = new LongAdder();
    private static final LongAdder wrapSelectionPositionsPairRepeat = new LongAdder();
    private static final Map<SelectedPositions, IdentityHashMap<SelectedPositions, Boolean>> wrapSelectionPositionsPairs = ENABLED ? new IdentityHashMap<>() : null;
    private static final LongAdder wrapSelectionFingerprintPairUnique = new LongAdder();
    private static final LongAdder wrapSelectionFingerprintPairRepeat = new LongAdder();
    private static final Set<Long> wrapSelectionFingerprintPairs = ENABLED ? new HashSet<>() : null;

    private static final LongAdder outerWrapSelectedStreamCalls = new LongAdder();
    private static final LongAdder outerWrapSelectedStreamRows = new LongAdder();
    private static final LongAdder outerBorrowValuesCalls = new LongAdder();
    private static final LongAdder outerBorrowNullsCalls = new LongAdder();
    private static final LongAdder outerBorrowErrorsCalls = new LongAdder();
    private static final LongAdder outerBorrowCacheHits = new LongAdder();
    private static final LongAdder outerMaterializeValuesWrapped = new LongAdder();
    private static final LongAdder outerMaterializeNullsWrapped = new LongAdder();
    private static final LongAdder outerMaterializeErrorsWrapped = new LongAdder();
    private static final LongAdder outerProjectOutputCalls = new LongAdder();
    private static final LongAdder outerProjectOutputRows = new LongAdder();
    private static final LongAdder outerProjectedOutputFastHits = new LongAdder();
    private static final LongAdder outerProjectedOutputFallbacks = new LongAdder();
    private static final LongAdder outerSelectionComposeCacheHits = new LongAdder();
    private static final LongAdder outerSelectionComposeCacheMisses = new LongAdder();
    private static final LongAdder outerSelectionComposePairUnique = new LongAdder();
    private static final LongAdder outerSelectionComposePairRepeat = new LongAdder();
    private static final Set<Long> outerSelectionComposePairs = ENABLED ? new HashSet<>() : null;
    private static final LongAdder outerSelectionComposeMissPairUnique = new LongAdder();
    private static final LongAdder outerSelectionComposeMissPairRepeat = new LongAdder();
    private static final LongAdder outerSelectionComposeMissPairDistanceTotal = new LongAdder();
    private static final LongAccumulator outerSelectionComposeMissPairDistanceMax = new LongAccumulator(Long::max, 0);
    private static final LongAdder outerSelectionComposeMissPairDistanceLe1k = new LongAdder();
    private static final LongAdder outerSelectionComposeMissPairDistanceLe8k = new LongAdder();
    private static final LongAdder outerSelectionComposeMissPairDistanceLe64k = new LongAdder();
    private static final LongAdder outerSelectionComposeMissPairDistanceGt64k = new LongAdder();
    private static long outerSelectionComposeMissOrdinal;
    private static final Map<Long, Long> outerSelectionComposeMissLastSeen = ENABLED ? new java.util.HashMap<>() : null;
    private static final int OUTER_SELECTION_EXACT_DEBUG_CACHE_SIZE = Integer.getInteger("nitro.hash.join.outerSelectionComposeCacheSize", 8_192);
    private static final LongAdder outerSelectionComposeExactCacheHits = new LongAdder();
    private static final LongAdder outerSelectionComposeExactCacheMisses = new LongAdder();
    private static final Set<Long> outerSelectionComposeExactCache = ENABLED ? new HashSet<>() : null;
    private static final ArrayDeque<Long> outerSelectionComposeExactCacheOrder = ENABLED ? new ArrayDeque<>() : null;
    private static final LongAdder outerSelectionComposeGlobalCacheHits = new LongAdder();
    private static final LongAdder outerSelectionComposeGlobalCacheMisses = new LongAdder();
    private static final LongAdder outerSelectionComposeMissResultCurrentHits = new LongAdder();
    private static final LongAdder outerSelectionComposeMissResultCurrentMisses = new LongAdder();
    private static final Map<String, OperatorSelectionComposeStats> outerSelectionComposeByOperator = ENABLED ? new HashMap<>() : null;

    private static final LongAdder hashJoinNextCalls = new LongAdder();
    private static final LongAdder hashJoinNextRows = new LongAdder();
    private static final LongAdder hashJoinNextOutputs = new LongAdder();
    private static final LongAdder hashJoinNextProduceBatchNanos = new LongAdder();
    private static final LongAdder hashJoinNextBuildOutputsNanos = new LongAdder();

    private static final LongAdder hashJoinCacheOuterCalls = new LongAdder();
    private static final LongAdder hashJoinCacheOuterNanos = new LongAdder();
    private static final LongAdder hashJoinCacheOuterValueSelectionVectors = new LongAdder();
    private static final LongAdder hashJoinCacheOuterValueOtherVectors = new LongAdder();
    private static final LongAdder hashJoinCacheOuterNullSelectionVectors = new LongAdder();
    private static final LongAdder hashJoinCacheOuterNullOtherVectors = new LongAdder();

    private static final LongAdder hashJoinPrepareChunkCalls = new LongAdder();
    private static final LongAdder hashJoinPrepareChunkRows = new LongAdder();
    private static final LongAdder hashJoinPrepareChunkNanos = new LongAdder();
    private static final LongAdder hashJoinLongProbeSelectionCalls = new LongAdder();
    private static final LongAdder hashJoinLongProbeSelectionRows = new LongAdder();
    private static final LongAdder hashJoinLongProbeSelectionNanos = new LongAdder();
    private static final LongAdder hashJoinLongProbeOtherCalls = new LongAdder();
    private static final LongAdder hashJoinLongProbeOtherRows = new LongAdder();
    private static final LongAdder hashJoinLongProbeOtherNanos = new LongAdder();
    private static final LongAdder hashJoinGenericProbeCalls = new LongAdder();
    private static final LongAdder hashJoinGenericProbeRows = new LongAdder();
    private static final LongAdder hashJoinGenericProbeNanos = new LongAdder();
    private static final LongAdder hashJoinGenericProbeFlatCalls = new LongAdder();
    private static final LongAdder hashJoinGenericProbePairCalls = new LongAdder();
    private static final LongAdder hashJoinGenericProbeTripleCalls = new LongAdder();
    private static final LongAdder hashJoinGenericProbeObjectCalls = new LongAdder();
    private static final LongAdder hashJoinGenericProbeSelectionColumns = new LongAdder();
    private static final LongAdder hashJoinGenericProbeTotalColumns = new LongAdder();
    private static final Map<String, HashJoinProbePathStats> hashJoinProbePathByOperator = ENABLED ? new HashMap<>() : null;
    private static final ThreadLocal<ProbeContext> currentHashJoinProbeContext = ENABLED ? new ThreadLocal<>() : null;

    private static final LongAdder joinCopySelectedCalls = new LongAdder();
    private static final LongAdder joinCopySelectedRows = new LongAdder();
    private static final LongAdder joinCopySelectedSelectionInputs = new LongAdder();
    private static final LongAdder joinCopySelectedDictionaryArrayInputs = new LongAdder();
    private static final LongAdder joinCopySelectedDictionaryMappedInputs = new LongAdder();
    private static final LongAdder joinCopySelectedRleArrayInputs = new LongAdder();
    private static final LongAdder joinCopySelectedRleMappedInputs = new LongAdder();
    private static final LongAdder joinCopySelectedI64Inputs = new LongAdder();
    private static final LongAdder joinCopySelectedI32Inputs = new LongAdder();
    private static final LongAdder joinCopySelectedBooleanInputs = new LongAdder();
    private static final LongAdder joinCopySelectedF64Inputs = new LongAdder();
    private static final LongAdder joinCopySelectedBinaryInputs = new LongAdder();
    private static final LongAdder joinCopySelectedFallbackInputs = new LongAdder();

    private ProjectedRowsDebug() {}

    public static boolean enabled()
    {
        return ENABLED;
    }

    public static void reset()
    {
        if (!ENABLED) {
            return;
        }
        projectedRowsCreated.reset();
        projectedRowsCreatedPositions.reset();
        composeCalls.reset();
        composeRows.reset();
        composeIdentityHits.reset();
        composeLastHits.reset();
        composeMapHits.reset();
        composeMisses.reset();
        projectCalls.reset();
        projectRows.reset();
        projectIdentityHits.reset();
        projectLastHits.reset();
        projectMapHits.reset();
        projectMisses.reset();
        materializeCalls.reset();
        materializeRows.reset();
        materializeWithTargetCalls.reset();
        materializeCacheHits.reset();
        materializeCacheMisses.reset();
        wrapCalls.reset();
        wrapRows.reset();
        wrapSelectionInputs.reset();
        wrapSelectionIdentityInputs.reset();
        wrapDictionaryInputs.reset();
        wrapRleInputs.reset();
        wrapBaseInputs.reset();
        wrapSelectionProjectPairUnique.reset();
        wrapSelectionProjectPairRepeat.reset();
        wrapSelectionProjectPairs.clear();
        wrapSelectionPositionsPairUnique.reset();
        wrapSelectionPositionsPairRepeat.reset();
        wrapSelectionPositionsPairs.clear();
        wrapSelectionFingerprintPairUnique.reset();
        wrapSelectionFingerprintPairRepeat.reset();
        wrapSelectionFingerprintPairs.clear();
        outerWrapSelectedStreamCalls.reset();
        outerWrapSelectedStreamRows.reset();
        outerBorrowValuesCalls.reset();
        outerBorrowNullsCalls.reset();
        outerBorrowErrorsCalls.reset();
        outerBorrowCacheHits.reset();
        outerMaterializeValuesWrapped.reset();
        outerMaterializeNullsWrapped.reset();
        outerMaterializeErrorsWrapped.reset();
        outerProjectOutputCalls.reset();
        outerProjectOutputRows.reset();
        outerProjectedOutputFastHits.reset();
        outerProjectedOutputFallbacks.reset();
        outerSelectionComposeCacheHits.reset();
        outerSelectionComposeCacheMisses.reset();
        outerSelectionComposePairUnique.reset();
        outerSelectionComposePairRepeat.reset();
        outerSelectionComposePairs.clear();
        outerSelectionComposeMissPairUnique.reset();
        outerSelectionComposeMissPairRepeat.reset();
        outerSelectionComposeMissPairDistanceTotal.reset();
        outerSelectionComposeMissPairDistanceMax.reset();
        outerSelectionComposeMissPairDistanceLe1k.reset();
        outerSelectionComposeMissPairDistanceLe8k.reset();
        outerSelectionComposeMissPairDistanceLe64k.reset();
        outerSelectionComposeMissPairDistanceGt64k.reset();
        outerSelectionComposeMissOrdinal = 0;
        outerSelectionComposeMissLastSeen.clear();
        outerSelectionComposeExactCacheHits.reset();
        outerSelectionComposeExactCacheMisses.reset();
        outerSelectionComposeExactCache.clear();
        outerSelectionComposeExactCacheOrder.clear();
        outerSelectionComposeGlobalCacheHits.reset();
        outerSelectionComposeGlobalCacheMisses.reset();
        outerSelectionComposeMissResultCurrentHits.reset();
        outerSelectionComposeMissResultCurrentMisses.reset();
        outerSelectionComposeByOperator.clear();
        hashJoinNextCalls.reset();
        hashJoinNextRows.reset();
        hashJoinNextOutputs.reset();
        hashJoinNextProduceBatchNanos.reset();
        hashJoinNextBuildOutputsNanos.reset();
        hashJoinCacheOuterCalls.reset();
        hashJoinCacheOuterNanos.reset();
        hashJoinCacheOuterValueSelectionVectors.reset();
        hashJoinCacheOuterValueOtherVectors.reset();
        hashJoinCacheOuterNullSelectionVectors.reset();
        hashJoinCacheOuterNullOtherVectors.reset();
        hashJoinPrepareChunkCalls.reset();
        hashJoinPrepareChunkRows.reset();
        hashJoinPrepareChunkNanos.reset();
        hashJoinLongProbeSelectionCalls.reset();
        hashJoinLongProbeSelectionRows.reset();
        hashJoinLongProbeSelectionNanos.reset();
        hashJoinLongProbeOtherCalls.reset();
        hashJoinLongProbeOtherRows.reset();
        hashJoinLongProbeOtherNanos.reset();
        hashJoinGenericProbeCalls.reset();
        hashJoinGenericProbeRows.reset();
        hashJoinGenericProbeNanos.reset();
        hashJoinGenericProbeFlatCalls.reset();
        hashJoinGenericProbePairCalls.reset();
        hashJoinGenericProbeTripleCalls.reset();
        hashJoinGenericProbeObjectCalls.reset();
        hashJoinGenericProbeSelectionColumns.reset();
        hashJoinGenericProbeTotalColumns.reset();
        hashJoinProbePathByOperator.clear();
        currentHashJoinProbeContext.remove();
        joinCopySelectedCalls.reset();
        joinCopySelectedRows.reset();
        joinCopySelectedSelectionInputs.reset();
        joinCopySelectedDictionaryArrayInputs.reset();
        joinCopySelectedDictionaryMappedInputs.reset();
        joinCopySelectedRleArrayInputs.reset();
        joinCopySelectedRleMappedInputs.reset();
        joinCopySelectedI64Inputs.reset();
        joinCopySelectedI32Inputs.reset();
        joinCopySelectedBooleanInputs.reset();
        joinCopySelectedF64Inputs.reset();
        joinCopySelectedBinaryInputs.reset();
        joinCopySelectedFallbackInputs.reset();
    }

    public static void recordProjectedRowsCreated(int count)
    {
        if (!ENABLED) {
            return;
        }
        projectedRowsCreated.increment();
        projectedRowsCreatedPositions.add(count);
    }

    public static void recordCompose(int count, String outcome)
    {
        if (!ENABLED) {
            return;
        }
        composeCalls.increment();
        composeRows.add(count);
        switch (outcome) {
            case "identity" -> composeIdentityHits.increment();
            case "last" -> composeLastHits.increment();
            case "map" -> composeMapHits.increment();
            case "miss" -> composeMisses.increment();
            default -> throw new IllegalArgumentException("Unknown compose outcome: " + outcome);
        }
    }

    public static void recordProject(int count, String outcome)
    {
        if (!ENABLED) {
            return;
        }
        projectCalls.increment();
        projectRows.add(count);
        switch (outcome) {
            case "identity" -> projectIdentityHits.increment();
            case "last" -> projectLastHits.increment();
            case "map" -> projectMapHits.increment();
            case "miss" -> projectMisses.increment();
            default -> throw new IllegalArgumentException("Unknown project outcome: " + outcome);
        }
    }

    public static void recordMaterialize(int count, boolean targetProvided, boolean cacheHit)
    {
        if (!ENABLED) {
            return;
        }
        materializeCalls.increment();
        materializeRows.add(count);
        if (targetProvided) {
            materializeWithTargetCalls.increment();
        }
        else if (cacheHit) {
            materializeCacheHits.increment();
        }
        else {
            materializeCacheMisses.increment();
        }
    }

    public static void recordWrap(int count, String inputKind)
    {
        if (!ENABLED) {
            return;
        }
        wrapCalls.increment();
        wrapRows.add(count);
        switch (inputKind) {
            case "selection" -> wrapSelectionInputs.increment();
            case "selection.identity" -> wrapSelectionIdentityInputs.increment();
            case "dictionary" -> wrapDictionaryInputs.increment();
            case "rle" -> wrapRleInputs.increment();
            case "base" -> wrapBaseInputs.increment();
            default -> throw new IllegalArgumentException("Unknown wrap input kind: " + inputKind);
        }
    }

    public static synchronized void recordWrapSelectionProjectPair(ProjectedRows sourceProjectedRows, SelectedPositions selectedRows)
    {
        if (!ENABLED) {
            return;
        }
        IdentityHashMap<SelectedPositions, Boolean> selectedRowsBySource = wrapSelectionProjectPairs.computeIfAbsent(sourceProjectedRows, _ -> new IdentityHashMap<>());
        if (selectedRowsBySource.putIfAbsent(selectedRows, Boolean.TRUE) == null) {
            wrapSelectionProjectPairUnique.increment();
        }
        else {
            wrapSelectionProjectPairRepeat.increment();
        }
    }

    public static synchronized void recordWrapSelectionPositionsPair(SelectedPositions sourcePositions, SelectedPositions selectedRows)
    {
        if (!ENABLED) {
            return;
        }
        IdentityHashMap<SelectedPositions, Boolean> selectedRowsBySource = wrapSelectionPositionsPairs.computeIfAbsent(sourcePositions, _ -> new IdentityHashMap<>());
        if (selectedRowsBySource.putIfAbsent(selectedRows, Boolean.TRUE) == null) {
            wrapSelectionPositionsPairUnique.increment();
        }
        else {
            wrapSelectionPositionsPairRepeat.increment();
        }
    }

    public static synchronized void recordWrapSelectionFingerprintPair(SelectedPositions sourcePositions, SelectedPositions selectedRows)
    {
        if (!ENABLED) {
            return;
        }
        long pairFingerprint = mixFingerprints(fingerprint(sourcePositions), fingerprint(selectedRows));
        if (wrapSelectionFingerprintPairs.add(pairFingerprint)) {
            wrapSelectionFingerprintPairUnique.increment();
        }
        else {
            wrapSelectionFingerprintPairRepeat.increment();
        }
    }

    public static void recordOuterWrapSelectedStream(int count)
    {
        if (!ENABLED) {
            return;
        }
        outerWrapSelectedStreamCalls.increment();
        outerWrapSelectedStreamRows.add(count);
    }

    public static void recordOuterBorrow(org.weakref.nitro.operator.evaluator.ir.Stream stream, boolean cacheHit)
    {
        if (!ENABLED) {
            return;
        }
        switch (stream) {
            case VALUES -> outerBorrowValuesCalls.increment();
            case NULLS -> outerBorrowNullsCalls.increment();
            case ERRORS -> outerBorrowErrorsCalls.increment();
        }
        if (cacheHit) {
            outerBorrowCacheHits.increment();
        }
    }

    public static void recordOuterMaterializeWrapped(boolean values, boolean nulls, boolean errors)
    {
        if (!ENABLED) {
            return;
        }
        if (values) {
            outerMaterializeValuesWrapped.increment();
        }
        if (nulls) {
            outerMaterializeNullsWrapped.increment();
        }
        if (errors) {
            outerMaterializeErrorsWrapped.increment();
        }
    }

    public static void recordOuterProjectOutput(int count, boolean fastHit)
    {
        if (!ENABLED) {
            return;
        }
        outerProjectOutputCalls.increment();
        outerProjectOutputRows.add(count);
        if (fastHit) {
            outerProjectedOutputFastHits.increment();
        }
        else {
            outerProjectedOutputFallbacks.increment();
        }
    }

    public static void recordOuterSelectionComposeCache(boolean hit)
    {
        if (!ENABLED) {
            return;
        }
        if (hit) {
            outerSelectionComposeCacheHits.increment();
        }
        else {
            outerSelectionComposeCacheMisses.increment();
        }
    }

    public static synchronized void recordOuterSelectionComposeOperator(String operatorName, boolean hit, int sourceCount, int currentCount)
    {
        if (!ENABLED) {
            return;
        }
        String name = operatorName == null ? "hash_join" : operatorName;
        OperatorSelectionComposeStats stats = outerSelectionComposeByOperator.computeIfAbsent(name, _ -> new OperatorSelectionComposeStats());
        if (hit) {
            stats.hits.increment();
        }
        else {
            stats.misses.increment();
        }
        stats.sourceRows.add(sourceCount);
        stats.currentRows.add(currentCount);
    }

    public static void recordOuterSelectionComposeMissResult(boolean equalsCurrent)
    {
        if (!ENABLED) {
            return;
        }
        if (equalsCurrent) {
            outerSelectionComposeMissResultCurrentHits.increment();
        }
        else {
            outerSelectionComposeMissResultCurrentMisses.increment();
        }
    }

    public static void recordOuterSelectionComposePair(SelectedPositions sourcePositions, SelectedPositions projectedPositions)
    {
        if (!ENABLED) {
            return;
        }
        long pairFingerprint = mixFingerprints(fingerprint(sourcePositions), fingerprint(projectedPositions));
        if (outerSelectionComposePairs.add(pairFingerprint)) {
            outerSelectionComposePairUnique.increment();
        }
        else {
            outerSelectionComposePairRepeat.increment();
        }
    }

    public static void recordOuterSelectionComposeMissPair(SelectedPositions sourcePositions, SelectedPositions projectedPositions)
    {
        if (!ENABLED) {
            return;
        }
        long pairFingerprint = mixFingerprints(fingerprintAll(sourcePositions), fingerprintAll(projectedPositions));
        long ordinal = outerSelectionComposeMissOrdinal++;
        Long lastSeen = outerSelectionComposeMissLastSeen.put(pairFingerprint, ordinal);
        if (outerSelectionComposeExactCache.add(pairFingerprint)) {
            outerSelectionComposeExactCacheMisses.increment();
            outerSelectionComposeExactCacheOrder.addLast(pairFingerprint);
            while (outerSelectionComposeExactCache.size() > OUTER_SELECTION_EXACT_DEBUG_CACHE_SIZE) {
                outerSelectionComposeExactCache.remove(outerSelectionComposeExactCacheOrder.removeFirst());
            }
        }
        else {
            outerSelectionComposeExactCacheHits.increment();
        }
        if (lastSeen == null) {
            outerSelectionComposeMissPairUnique.increment();
            return;
        }

        outerSelectionComposeMissPairRepeat.increment();
        long distance = ordinal - lastSeen;
        outerSelectionComposeMissPairDistanceTotal.add(distance);
        outerSelectionComposeMissPairDistanceMax.accumulate(distance);
        if (distance <= 1_024) {
            outerSelectionComposeMissPairDistanceLe1k.increment();
        }
        else if (distance <= 8_192) {
            outerSelectionComposeMissPairDistanceLe8k.increment();
        }
        else if (distance <= 65_536) {
            outerSelectionComposeMissPairDistanceLe64k.increment();
        }
        else {
            outerSelectionComposeMissPairDistanceGt64k.increment();
        }
    }

    public static void recordOuterSelectionComposeGlobalCache(boolean hit)
    {
        if (!ENABLED) {
            return;
        }
        if (hit) {
            outerSelectionComposeGlobalCacheHits.increment();
        }
        else {
            outerSelectionComposeGlobalCacheMisses.increment();
        }
    }

    public static void recordHashJoinNext(int rowCount, int outputCount, long produceBatchNanos, long buildOutputsNanos)
    {
        if (!ENABLED) {
            return;
        }
        hashJoinNextCalls.increment();
        hashJoinNextRows.add(rowCount);
        hashJoinNextOutputs.add(outputCount);
        hashJoinNextProduceBatchNanos.add(produceBatchNanos);
        hashJoinNextBuildOutputsNanos.add(buildOutputsNanos);
    }

    public static void recordHashJoinCacheOuter(long nanos, boolean valueSelectionVector, boolean nullSelectionVector, boolean hasNulls)
    {
        if (!ENABLED) {
            return;
        }
        hashJoinCacheOuterCalls.increment();
        hashJoinCacheOuterNanos.add(nanos);
        if (valueSelectionVector) {
            hashJoinCacheOuterValueSelectionVectors.increment();
        }
        else {
            hashJoinCacheOuterValueOtherVectors.increment();
        }
        if (hasNulls) {
            if (nullSelectionVector) {
                hashJoinCacheOuterNullSelectionVectors.increment();
            }
            else {
                hashJoinCacheOuterNullOtherVectors.increment();
            }
        }
    }

    public static void recordHashJoinPrepareChunk(int rowCount, long nanos)
    {
        if (!ENABLED) {
            return;
        }
        hashJoinPrepareChunkCalls.increment();
        hashJoinPrepareChunkRows.add(rowCount);
        hashJoinPrepareChunkNanos.add(nanos);
    }

    public static void recordHashJoinLongProbe(int rowCount, long nanos, boolean selectionVector)
    {
        if (!ENABLED) {
            return;
        }
        if (selectionVector) {
            hashJoinLongProbeSelectionCalls.increment();
            hashJoinLongProbeSelectionRows.add(rowCount);
            hashJoinLongProbeSelectionNanos.add(nanos);
        }
        else {
            hashJoinLongProbeOtherCalls.increment();
            hashJoinLongProbeOtherRows.add(rowCount);
            hashJoinLongProbeOtherNanos.add(nanos);
        }
    }

    public static void recordHashJoinGenericProbe(int rowCount, long nanos, String kind, int selectionColumns, int totalColumns)
    {
        if (!ENABLED) {
            return;
        }
        hashJoinGenericProbeCalls.increment();
        hashJoinGenericProbeRows.add(rowCount);
        hashJoinGenericProbeNanos.add(nanos);
        hashJoinGenericProbeSelectionColumns.add(selectionColumns);
        hashJoinGenericProbeTotalColumns.add(totalColumns);
        switch (kind) {
            case "flat" -> hashJoinGenericProbeFlatCalls.increment();
            case "pair" -> hashJoinGenericProbePairCalls.increment();
            case "triple" -> hashJoinGenericProbeTripleCalls.increment();
            case "object" -> hashJoinGenericProbeObjectCalls.increment();
            default -> throw new IllegalArgumentException("Unknown generic probe kind: " + kind);
        }
    }

    public static void recordHashJoinDirectNoNullProbe(String operatorName, String kind)
    {
        if (!ENABLED) {
            return;
        }
        probeStats(operatorName, kind).directNoNullCalls.increment();
    }

    public static void enterHashJoinNullableProbe(String operatorName, String kind)
    {
        if (!ENABLED) {
            return;
        }
        probeStats(operatorName, kind).nullableCalls.increment();
        currentHashJoinProbeContext.set(new ProbeContext(normalizeOperatorName(operatorName), kind));
    }

    public static void exitHashJoinNullableProbe()
    {
        if (!ENABLED) {
            return;
        }
        currentHashJoinProbeContext.remove();
    }

    public static void recordHashJoinNullableProbeNullCheck(boolean hasNull)
    {
        if (!ENABLED) {
            return;
        }
        ProbeContext probeContext = currentHashJoinProbeContext.get();
        if (probeContext == null) {
            return;
        }
        HashJoinProbePathStats stats = probeStats(probeContext.operatorName(), probeContext.kind());
        if (hasNull) {
            stats.nullHits.increment();
        }
        else {
            stats.nullFallthroughCalls.increment();
        }
    }

    public static void recordJoinCopySelected(int rowCount, String inputKind)
    {
        if (!ENABLED) {
            return;
        }
        joinCopySelectedCalls.increment();
        joinCopySelectedRows.add(rowCount);
        switch (inputKind) {
            case "selection" -> joinCopySelectedSelectionInputs.increment();
            case "dictionary.array" -> joinCopySelectedDictionaryArrayInputs.increment();
            case "dictionary.mapped" -> joinCopySelectedDictionaryMappedInputs.increment();
            case "rle.array" -> joinCopySelectedRleArrayInputs.increment();
            case "rle.mapped" -> joinCopySelectedRleMappedInputs.increment();
            case "i64" -> joinCopySelectedI64Inputs.increment();
            case "i32" -> joinCopySelectedI32Inputs.increment();
            case "boolean" -> joinCopySelectedBooleanInputs.increment();
            case "f64" -> joinCopySelectedF64Inputs.increment();
            case "binary" -> joinCopySelectedBinaryInputs.increment();
            case "fallback" -> joinCopySelectedFallbackInputs.increment();
            default -> throw new IllegalArgumentException("Unknown join copy selected input kind: " + inputKind);
        }
    }

    public static String snapshot()
    {
        if (!ENABLED) {
            return "ProjectedRows debug disabled";
        }
        return """
                ProjectedRows debug
                projectedRows.created=%d rows=%d
                compose.calls=%d rows=%d identity=%d last=%d map=%d miss=%d
                project.calls=%d rows=%d identity=%d last=%d map=%d miss=%d
                materialize.calls=%d rows=%d with_target=%d cache_hit=%d cache_miss=%d
                wrap.calls=%d rows=%d selection=%d selection_identity=%d dictionary=%d rle=%d base=%d selection_project_pair_unique=%d selection_project_pair_repeat=%d selection_positions_pair_unique=%d selection_positions_pair_repeat=%d selection_fingerprint_pair_unique=%d selection_fingerprint_pair_repeat=%d
                outer.wrapSelectedStream.calls=%d rows=%d
                outer.borrow.values=%d nulls=%d errors=%d cache_hits=%d
                outer.materialize.values=%d nulls=%d errors=%d
                outer.projectOutput.calls=%d rows=%d fast=%d fallback=%d selection_cache_hits=%d selection_cache_misses=%d selection_pair_unique=%d selection_pair_repeat=%d selection_miss_unique=%d selection_miss_repeat=%d selection_miss_le1k=%d selection_miss_le8k=%d selection_miss_le64k=%d selection_miss_gt64k=%d selection_miss_distance_avg=%.1f selection_miss_distance_max=%d selection_exact_cache_hits=%d selection_exact_cache_misses=%d selection_global_hits=%d selection_global_misses=%d selection_miss_result_current_hits=%d selection_miss_result_current_misses=%d
                hashJoin.next.calls=%d rows=%d outputs=%d produce_batch_ms=%.3f build_outputs_ms=%.3f
                hashJoin.cacheOuter.calls=%d cpu_ms=%.3f value_selection=%d value_other=%d null_selection=%d null_other=%d
                hashJoin.prepareChunk.calls=%d rows=%d cpu_ms=%.3f
                hashJoin.longProbe.selection.calls=%d rows=%d cpu_ms=%.3f
                hashJoin.longProbe.other.calls=%d rows=%d cpu_ms=%.3f
                hashJoin.genericProbe.calls=%d rows=%d cpu_ms=%.3f flat=%d pair=%d triple=%d object=%d selection_columns=%d total_columns=%d
                hashJoin.probePath.by_operator:
                %s
                join.copySelected.calls=%d rows=%d selection=%d dictionary_array=%d dictionary_mapped=%d rle_array=%d rle_mapped=%d i64=%d i32=%d boolean=%d f64=%d binary=%d fallback=%d
                outer.selection.by_operator:
                %s
                """.formatted(
                projectedRowsCreated.sum(),
                projectedRowsCreatedPositions.sum(),
                composeCalls.sum(),
                composeRows.sum(),
                composeIdentityHits.sum(),
                composeLastHits.sum(),
                composeMapHits.sum(),
                composeMisses.sum(),
                projectCalls.sum(),
                projectRows.sum(),
                projectIdentityHits.sum(),
                projectLastHits.sum(),
                projectMapHits.sum(),
                projectMisses.sum(),
                materializeCalls.sum(),
                materializeRows.sum(),
                materializeWithTargetCalls.sum(),
                materializeCacheHits.sum(),
                materializeCacheMisses.sum(),
                wrapCalls.sum(),
                wrapRows.sum(),
                wrapSelectionInputs.sum(),
                wrapSelectionIdentityInputs.sum(),
                wrapDictionaryInputs.sum(),
                wrapRleInputs.sum(),
                wrapBaseInputs.sum(),
                wrapSelectionProjectPairUnique.sum(),
                wrapSelectionProjectPairRepeat.sum(),
                wrapSelectionPositionsPairUnique.sum(),
                wrapSelectionPositionsPairRepeat.sum(),
                wrapSelectionFingerprintPairUnique.sum(),
                wrapSelectionFingerprintPairRepeat.sum(),
                outerWrapSelectedStreamCalls.sum(),
                outerWrapSelectedStreamRows.sum(),
                outerBorrowValuesCalls.sum(),
                outerBorrowNullsCalls.sum(),
                outerBorrowErrorsCalls.sum(),
                outerBorrowCacheHits.sum(),
                outerMaterializeValuesWrapped.sum(),
                outerMaterializeNullsWrapped.sum(),
                outerMaterializeErrorsWrapped.sum(),
                outerProjectOutputCalls.sum(),
                outerProjectOutputRows.sum(),
                outerProjectedOutputFastHits.sum(),
                outerProjectedOutputFallbacks.sum(),
                outerSelectionComposeCacheHits.sum(),
                outerSelectionComposeCacheMisses.sum(),
                outerSelectionComposePairUnique.sum(),
                outerSelectionComposePairRepeat.sum(),
                outerSelectionComposeMissPairUnique.sum(),
                outerSelectionComposeMissPairRepeat.sum(),
                outerSelectionComposeMissPairDistanceLe1k.sum(),
                outerSelectionComposeMissPairDistanceLe8k.sum(),
                outerSelectionComposeMissPairDistanceLe64k.sum(),
                outerSelectionComposeMissPairDistanceGt64k.sum(),
                outerSelectionComposeMissPairRepeat.sum() == 0 ? 0.0 : ((double) outerSelectionComposeMissPairDistanceTotal.sum() / outerSelectionComposeMissPairRepeat.sum()),
                outerSelectionComposeMissPairDistanceMax.get(),
                outerSelectionComposeExactCacheHits.sum(),
                outerSelectionComposeExactCacheMisses.sum(),
                outerSelectionComposeGlobalCacheHits.sum(),
                outerSelectionComposeGlobalCacheMisses.sum(),
                outerSelectionComposeMissResultCurrentHits.sum(),
                outerSelectionComposeMissResultCurrentMisses.sum(),
                hashJoinNextCalls.sum(),
                hashJoinNextRows.sum(),
                hashJoinNextOutputs.sum(),
                hashJoinNextProduceBatchNanos.sum() / 1_000_000.0,
                hashJoinNextBuildOutputsNanos.sum() / 1_000_000.0,
                hashJoinCacheOuterCalls.sum(),
                hashJoinCacheOuterNanos.sum() / 1_000_000.0,
                hashJoinCacheOuterValueSelectionVectors.sum(),
                hashJoinCacheOuterValueOtherVectors.sum(),
                hashJoinCacheOuterNullSelectionVectors.sum(),
                hashJoinCacheOuterNullOtherVectors.sum(),
                hashJoinPrepareChunkCalls.sum(),
                hashJoinPrepareChunkRows.sum(),
                hashJoinPrepareChunkNanos.sum() / 1_000_000.0,
                hashJoinLongProbeSelectionCalls.sum(),
                hashJoinLongProbeSelectionRows.sum(),
                hashJoinLongProbeSelectionNanos.sum() / 1_000_000.0,
                hashJoinLongProbeOtherCalls.sum(),
                hashJoinLongProbeOtherRows.sum(),
                hashJoinLongProbeOtherNanos.sum() / 1_000_000.0,
                hashJoinGenericProbeCalls.sum(),
                hashJoinGenericProbeRows.sum(),
                hashJoinGenericProbeNanos.sum() / 1_000_000.0,
                hashJoinGenericProbeFlatCalls.sum(),
                hashJoinGenericProbePairCalls.sum(),
                hashJoinGenericProbeTripleCalls.sum(),
                hashJoinGenericProbeObjectCalls.sum(),
                hashJoinGenericProbeSelectionColumns.sum(),
                hashJoinGenericProbeTotalColumns.sum(),
                formatHashJoinProbePathByOperator(),
                joinCopySelectedCalls.sum(),
                joinCopySelectedRows.sum(),
                joinCopySelectedSelectionInputs.sum(),
                joinCopySelectedDictionaryArrayInputs.sum(),
                joinCopySelectedDictionaryMappedInputs.sum(),
                joinCopySelectedRleArrayInputs.sum(),
                joinCopySelectedRleMappedInputs.sum(),
                joinCopySelectedI64Inputs.sum(),
                joinCopySelectedI32Inputs.sum(),
                joinCopySelectedBooleanInputs.sum(),
                joinCopySelectedF64Inputs.sum(),
                joinCopySelectedBinaryInputs.sum(),
                joinCopySelectedFallbackInputs.sum(),
                formatTopOuterSelectionComposeOperators());
    }

    private static synchronized String formatHashJoinProbePathByOperator()
    {
        if (hashJoinProbePathByOperator.isEmpty()) {
            return "  <none>";
        }
        return hashJoinProbePathByOperator.entrySet().stream()
                .sorted((left, right) -> Long.compare(right.getValue().totalCalls(), left.getValue().totalCalls()))
                .limit(16)
                .map(entry -> "  %s direct_no_null=%d nullable=%d null_hits=%d nullable_fallthrough=%d".formatted(
                        entry.getKey(),
                        entry.getValue().directNoNullCalls.sum(),
                        entry.getValue().nullableCalls.sum(),
                        entry.getValue().nullHits.sum(),
                        entry.getValue().nullFallthroughCalls.sum()))
                .reduce((left, right) -> left + "\n" + right)
                .orElse("  <none>");
    }

    private static synchronized String formatTopOuterSelectionComposeOperators()
    {
        if (outerSelectionComposeByOperator.isEmpty()) {
            return "  <none>";
        }
        return outerSelectionComposeByOperator.entrySet().stream()
                .sorted((left, right) -> Long.compare(right.getValue().misses.sum(), left.getValue().misses.sum()))
                .limit(12)
                .map(entry -> "  %s misses=%d hits=%d source_rows=%d current_rows=%d".formatted(
                        entry.getKey(),
                        entry.getValue().misses.sum(),
                        entry.getValue().hits.sum(),
                        entry.getValue().sourceRows.sum(),
                        entry.getValue().currentRows.sum()))
                .reduce((left, right) -> left + "\n" + right)
                .orElse("  <none>");
    }

    private static final class OperatorSelectionComposeStats
    {
        private final LongAdder hits = new LongAdder();
        private final LongAdder misses = new LongAdder();
        private final LongAdder sourceRows = new LongAdder();
        private final LongAdder currentRows = new LongAdder();
    }

    private static final class HashJoinProbePathStats
    {
        private final LongAdder directNoNullCalls = new LongAdder();
        private final LongAdder nullableCalls = new LongAdder();
        private final LongAdder nullHits = new LongAdder();
        private final LongAdder nullFallthroughCalls = new LongAdder();

        private long totalCalls()
        {
            return directNoNullCalls.sum() + nullableCalls.sum();
        }
    }

    private record ProbeContext(String operatorName, String kind) {}

    private static synchronized HashJoinProbePathStats probeStats(String operatorName, String kind)
    {
        return hashJoinProbePathByOperator.computeIfAbsent("%s [%s]".formatted(normalizeOperatorName(operatorName), kind), _ -> new HashJoinProbePathStats());
    }

    private static String normalizeOperatorName(String operatorName)
    {
        return operatorName == null ? "hash_join" : operatorName;
    }

    private static long fingerprint(SelectedPositions positions)
    {
        int count = positions.count();
        if (count == 0) {
            return 0x9E3779B97F4A7C15L;
        }
        long hash = 0x9E3779B97F4A7C15L ^ count;
        hash = mix(hash, positions.position(0));
        hash = mix(hash, positions.position(count >>> 1));
        hash = mix(hash, positions.position(count - 1));
        hash = mix(hash, positions.position(count >>> 2));
        hash = mix(hash, positions.position((count * 3) >>> 2));
        return hash;
    }

    private static long mixFingerprints(long left, long right)
    {
        long hash = 0x517CC1B727220A95L;
        hash = mix(hash, left);
        hash = mix(hash, right);
        return hash;
    }

    private static long fingerprintAll(SelectedPositions positions)
    {
        long hash = 0x9E3779B97F4A7C15L ^ positions.count();
        for (int index = 0; index < positions.count(); index++) {
            hash = mix(hash, positions.position(index));
        }
        return hash;
    }

    private static long mix(long hash, long value)
    {
        hash ^= value + 0x9E3779B97F4A7C15L + (hash << 6) + (hash >>> 2);
        return hash;
    }
}
