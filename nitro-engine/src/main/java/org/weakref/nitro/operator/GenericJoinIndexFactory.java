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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;

import java.util.List;
import java.util.function.IntToLongFunction;

import static java.util.Objects.requireNonNull;

/**
 * Engine-owned composition of the general hash-join index implementations.
 */
final class GenericJoinIndexFactory
{
    private final OperatorCodeGenerationResources codeGeneration;
    private final FlatKeyTablePolicy flatKeyTablePolicy;
    private final HashJoinBuildPolicy buildPolicy;
    private final HashJoinIndexPolicy joinIndexPolicy;
    private final HashJoinOutputPolicy outputPolicy;
    private final HashJoinExecutionPolicy executionPolicy;

    GenericJoinIndexFactory(
            OperatorCodeGenerationResources codeGeneration,
            FlatKeyTablePolicy flatKeyTablePolicy,
            HashJoinBuildPolicy buildPolicy,
            HashJoinIndexPolicy joinIndexPolicy,
            HashJoinOutputPolicy outputPolicy,
            HashJoinExecutionPolicy executionPolicy)
    {
        this.codeGeneration = requireNonNull(codeGeneration, "codeGeneration is null");
        this.flatKeyTablePolicy = requireNonNull(flatKeyTablePolicy, "flatKeyTablePolicy is null");
        this.buildPolicy = requireNonNull(buildPolicy, "buildPolicy is null");
        this.joinIndexPolicy = requireNonNull(joinIndexPolicy, "joinIndexPolicy is null");
        this.outputPolicy = requireNonNull(outputPolicy, "outputPolicy is null");
        this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
    }

    JoinIndex structural(StructuralKeyKernel[] kernels)
    {
        return new StructuralHashJoinIndex(kernels);
    }

    JoinIndex create(
            Vector[] values,
            List<TypeBinding> keyTypes,
            PrimitiveArrayPool arrayPool,
            int expectedSize,
            boolean pairKeyOnlyBuild,
            boolean capInitialHash)
    {
        if (values.length == 2 &&
                isLong(values[0]) &&
                isLong(values[1])) {
            return new LongPairJoinIndex(
                    joinIndexPolicy,
                    outputPolicy,
                    executionPolicy,
                    arrayPool,
                    expectedSize,
                    pairKeyOnlyBuild,
                    capInitialHash,
                    buildPolicy.batchLongPairBuild());
        }
        if (values.length == 3 &&
                isLong(values[0]) &&
                isLong(values[1]) &&
                isLong(values[2])) {
            return new LongTripleJoinIndex(executionPolicy, arrayPool, expectedSize);
        }
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(
                values,
                false,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy,
                keyTypes);
        if (layout != null) {
            return new FlatJoinIndex(joinIndexPolicy, layout, expectedSize);
        }
        return new ObjectJoinIndex(values.length);
    }

    boolean shouldCapInitialHash(
            BufferedJoinInput.InnerBatch batch,
            Vector[] values,
            int expectedRows,
            boolean keyOnlyBuild)
    {
        if (buildPolicy.capDuplicatePairHash() &&
                values.length == 2 &&
                isLong(values[0]) &&
                isLong(values[1]) &&
                expectedRows >= buildPolicy.pairHashCapMinExpectedRows()) {
            long capacity = 16;
            while (capacity < expectedRows / 0.75) {
                capacity <<= 1;
            }
            if (capacity * (2L * Long.BYTES + Byte.BYTES) <= buildPolicy.maxInitialPairHashBytes()) {
                return false;
            }
            int sampleSize = Math.min(batch.length(), buildPolicy.initialHashAdmissionSampleRows());
            LongOpenHashSet distinct = new LongOpenHashSet(sampleSize);
            long firstMin = Long.MAX_VALUE;
            long firstMax = Long.MIN_VALUE;
            long secondMin = Long.MAX_VALUE;
            long secondMax = Long.MIN_VALUE;
            for (int position = 0; position < sampleSize; position++) {
                int sourcePosition = batch.sourcePosition(position);
                long first = OperatorVectorSupport.longValue(values[0], sourcePosition);
                long second = OperatorVectorSupport.longValue(values[1], sourcePosition);
                distinct.add(LongPairJoinIndex.hash64(first, second));
                firstMin = Math.min(firstMin, first);
                firstMax = Math.max(firstMax, first);
                secondMin = Math.min(secondMin, second);
                secondMax = Math.max(secondMax, second);
            }
            // The sample controls initial capacity only. Exact key equality and ordinary rehash growth preserve
            // correctness even in the vanishingly unlikely event of a sampled hash collision.
            if (sampleSize < buildPolicy.initialHashAdmissionMinSampleRows()) {
                return false;
            }
            if ((long) distinct.size() * 100 <=
                    (long) sampleSize * buildPolicy.pairHashCapMaxDistinctPercent()) {
                return true;
            }
            long firstRange = firstMax - firstMin + 1;
            long secondRange = secondMax - secondMin + 1;
            long boundedDomain =
                    (long) expectedRows * buildPolicy.pairHashCapMaxDomainPercent() / 100;
            // A large ordered prefix can look unique despite a bounded duplicate domain. Admit that case only when
            // the sampled Cartesian range is small; a wide unique pair build keeps the one-allocation presized path.
            return firstRange > 0 && secondRange > 0 &&
                    firstRange <= boundedDomain / secondRange;
        }
        if (values.length != 1 || keyOnlyBuild || !isLong(values[0])) {
            return false;
        }
        if (expectedRows >= buildPolicy.payloadHashCapAlwaysExpectedRows()) {
            return true;
        }
        int sampleSize = Math.min(batch.length(), buildPolicy.initialHashAdmissionSampleRows());
        LongOpenHashSet distinct = new LongOpenHashSet(sampleSize);
        long sampleMin = Long.MAX_VALUE;
        long sampleMax = Long.MIN_VALUE;
        for (int position = 0; position < sampleSize; position++) {
            long key = OperatorVectorSupport.longValue(values[0], batch.sourcePosition(position));
            distinct.add(key);
            sampleMin = Math.min(sampleMin, key);
            sampleMax = Math.max(sampleMax, key);
        }
        if ((long) distinct.size() * 100 <=
                (long) sampleSize * buildPolicy.payloadHashCapMaxDistinctPercent()) {
            return true;
        }
        // A random prefix of a bounded duplicate domain can look entirely unique. For a very large build, admit
        // bounded range state when the observed domain itself fits; exact fallback remains available if later keys
        // escape the ceiling. Wide-domain samples retain the ordinary pre-sized hash path.
        return expectedRows >= buildPolicy.payloadHashCapBoundedExpectedRows() &&
                sampleMin >= 0 &&
                sampleMax < joinIndexPolicy.maxDirectBuildKey();
    }

    boolean shouldUseGroupedLongHash(BufferedJoinInput.InnerBatch batch, Vector[] values, int expectedRows)
    {
        if (!joinIndexPolicy.groupedLongHashTable() || values.length != 1 || !isLong(values[0])) {
            return false;
        }
        if (!joinIndexPolicy.sparseAwareLongHashLayout()) {
            return true;
        }
        if (expectedRows < joinIndexPolicy.sparseAwareScalarMinRows()) {
            return true;
        }
        int sampleSize = Math.min(batch.length(), joinIndexPolicy.rangeAdmissionSampleRows());
        if (sampleSize < joinIndexPolicy.rangeAdmissionMinSampleRows()) {
            return true;
        }
        long sampleMin = Long.MAX_VALUE;
        long sampleMax = Long.MIN_VALUE;
        for (int position = 0; position < sampleSize; position++) {
            long key = OperatorVectorSupport.longValue(values[0], batch.sourcePosition(position));
            sampleMin = Math.min(sampleMin, key);
            sampleMax = Math.max(sampleMax, key);
        }
        long range = sampleMax - sampleMin + 1;
        // A bounded sparse domain receives an exact membership filter before probing. Surviving hash lookups are
        // consequently hits, where scalar linear probing is cheaper than loading and comparing a SIMD tag group.
        // Compare with the known full build cardinality, not sample cardinality: a random prefix of a dense table
        // spans most of its domain and would otherwise be misclassified as sparse (TPC-H q7 customer/orders).
        return range <= 0 ||
                range > joinIndexPolicy.maxArrayRange() ||
                range < expectedRows * joinIndexPolicy.sparseLongRangeMinRatio();
    }

    boolean shouldUseDirectRangeBuild(
            BufferedJoinInput.InnerBatch batch,
            Vector[] values,
            int expectedRows)
    {
        return shouldUseDirectRangeBuild(
                values,
                expectedRows,
                batch.length(),
                position -> OperatorVectorSupport.longValue(values[0], batch.sourcePosition(position)));
    }

    boolean shouldUseDirectRangeBuild(
            Mask mask,
            Vector[] values,
            int expectedRows)
    {
        return shouldUseDirectRangeBuild(
                values,
                expectedRows,
                mask.count(),
                position -> OperatorVectorSupport.longValue(values[0], mask.position(position)));
    }

    private boolean shouldUseDirectRangeBuild(
            Vector[] values,
            int expectedRows,
            int rowCount,
            IntToLongFunction keyAt)
    {
        if (!joinIndexPolicy.directRangeBuild() ||
                values.length != 1 ||
                !isLong(values[0])) {
            return false;
        }
        if (expectedRows < joinIndexPolicy.directRangeBuildMinRows()) {
            return false;
        }
        int sampleSize = Math.min(rowCount, joinIndexPolicy.rangeAdmissionSampleRows());
        if (sampleSize < joinIndexPolicy.rangeAdmissionMinSampleRows()) {
            return false;
        }
        long sampleMin = Long.MAX_VALUE;
        long sampleMax = Long.MIN_VALUE;
        for (int position = 0; position < sampleSize; position++) {
            long key = keyAt.applyAsLong(position);
            sampleMin = Math.min(sampleMin, key);
            sampleMax = Math.max(sampleMax, key);
        }
        // The streaming builder addresses non-negative keys directly until the build shape is complete. Bound the
        // absolute observed domain, rather than only its width, so a high-offset narrow range cannot reserve a large
        // mostly-empty array. The builder remains exact and falls back if a later key escapes the admitted ceiling.
        return sampleMin >= 0 &&
                sampleMax < joinIndexPolicy.maxDirectBuildKey() &&
                sampleMax + 1 <= (long) joinIndexPolicy.directRangeMaxCardinalityRatio() * expectedRows +
                        joinIndexPolicy.directRangeBuildInitialCapacity();
    }

    private static boolean isLong(Vector values)
    {
        FlatTypeHandler handler = FlatTypeHandlers.forVector(values);
        return handler != null && handler.kind() == FlatTypeHandler.Kind.LONG;
    }
}
