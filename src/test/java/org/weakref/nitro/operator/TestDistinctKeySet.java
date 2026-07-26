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
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class TestDistinctKeySet
{
    private final EngineResources engineResources = EngineResources.createDefault();
    private final PrimitiveArrayPool arrayPool = engineResources.primitiveArrays();
    private final OperatorCodeGenerationResources codeGeneration = engineResources.operatorCodeGeneration();
    private final FlatKeyTablePolicy flatKeyTablePolicy = engineResources.operatorResources().flatKeyTablePolicy();

    @Test
    void testAdaptiveLongPairMigratesAndPromotesExactly()
    {
        Vector[] initialValues = longPair(new long[] {1, 2, 1, 99}, new long[] {10, 20, 10, 990});
        DistinctKeySet keys = DistinctKeySet.create(initialValues, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), flatKeyTablePolicy);
        try {
            assertDistinctPositions(keys, initialValues, new boolean[] {false, false, false, true}, 0, 1);
            assertDistinctPositions(keys, longPair(new long[] {2, 3, 3}, new long[] {20, 30, 30}), null, 1);
            assertDistinctPositions(keys, longPair(new long[] {4, 1}, new long[] {40, 10}), null, 0);

            // The fourth batch migrates the retained pair table into the generated compact representation.
            assertDistinctPositions(keys, longPair(new long[] {3, 5, 4}, new long[] {30, 50, 40}), null, 1);

            // A later value outside the signed-int compact domain promotes the complete retained set back to
            // the generated full-width representation without losing either old or new keys.
            long wide = 1L << 40;
            assertDistinctPositions(keys, longPair(new long[] {wide, 5, wide}, new long[] {60, 50, 60}), null, 0);
            assertDistinctPositions(keys, longPair(new long[] {1, 2, 3, 4, 5, wide}, new long[] {10, 20, 30, 40, 50, 60}), null);
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveMultiLongDistinctSharesDictionaryPositionOnlyForIdenticalMappings()
    {
        int[] innerIds = {2, 0, 1, 2};
        int[] outerIds = {0, 1, 2, 3, 0, 2};
        Vector[] shared = {
                nestedLongDictionary(outerIds, innerIds, new long[] {10, 20, 30}),
                nestedLongDictionary(outerIds, innerIds, new long[] {100, 200, 300}),
                nestedLongDictionary(outerIds, innerIds, new long[] {1_000, 2_000, 3_000})};
        DistinctKeySet keys = DistinctKeySet.create(shared, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), flatKeyTablePolicy);
        try {
            assertDistinctPositions(keys, shared, null, 0, 1, 2);

            // A content-equivalent but separately owned mapping must use the exact generic fallback. Changing only
            // the second lane's first mapping then creates one genuinely new tuple.
            Vector[] mismatched = {
                    nestedLongDictionary(outerIds, innerIds, new long[] {10, 20, 30}),
                    nestedLongDictionary(new int[] {1, 1, 2, 3, 0, 2}, innerIds, new long[] {100, 200, 300}),
                    nestedLongDictionary(outerIds, innerIds, new long[] {1_000, 2_000, 3_000})};
            assertDistinctPositions(keys, mismatched, null, 0);

            // A wide value can force compact-to-full promotion after the shared resolver was admitted. The replay
            // still consumes the ordered key accessors exactly and preserves all previously inserted tuples.
            int[] identity = {0, 1, 2};
            Vector[] promoted = {
                    nestedLongDictionary(identity, identity, new long[] {10, 1L << 40, 30}),
                    nestedLongDictionary(identity, identity, new long[] {100, 200, 300}),
                    nestedLongDictionary(identity, identity, new long[] {1_000, 2_000, 3_000})};
            assertDistinctPositions(keys, promoted, null, 1);
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveMultiLongDistinctSharesDictionaryPositionAcrossNullsExactly()
    {
        int[] identity = {0, 1, 2, 3};
        Vector[] values = {
                nestedLongDictionary(identity, identity, new long[] {1, 2, 3, 4}),
                nestedLongDictionary(identity, identity, new long[] {10, 20, 30, 40}),
                nestedLongDictionary(identity, identity, new long[] {100, 200, 300, 400})};

        DistinctKeySet nullFreeKeys = DistinctKeySet.create(values, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), flatKeyTablePolicy);
        try {
            Vector[] nulls = {
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length]),
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length]),
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length])};
            int[] positions = new int[identity.length];
            int distinct = nullFreeKeys.addBatch(values, nulls, Mask.all(identity.length), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1, 2, 3);
        }
        finally {
            nullFreeKeys.releaseBuffers();
        }

        DistinctKeySet nullableKeys = DistinctKeySet.create(values, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), flatKeyTablePolicy);
        try {
            Vector[] nulls = {
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length]),
                    nestedBooleanDictionary(identity, identity, new boolean[] {false, true, false, false}),
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length])};
            int[] positions = new int[identity.length];
            int distinct = nullableKeys.addBatch(values, nulls, Mask.all(identity.length), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 2, 3);
        }
        finally {
            nullableKeys.releaseBuffers();
        }

        DistinctKeySet sparseKeys = DistinctKeySet.create(values, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), flatKeyTablePolicy);
        try {
            Vector[] nulls = {
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length]),
                    nestedBooleanDictionary(identity, identity, new boolean[] {false, true, false, false}),
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length])};
            int[] positions = new int[identity.length];
            int distinct = sparseKeys.addBatch(values, nulls, Mask.sparse(new int[] {1, 3}, identity.length), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(3);
        }
        finally {
            sparseKeys.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveMultiLongDistinctRetainsNullsThroughCompactAndPromotedBatches()
    {
        Vector[] firstValues = {
                new I64Vector(new long[] {1, 1, 1, 1, 1, 2}),
                new I64Vector(new long[] {10, 99, 99, 10, 10, 20}),
                new I64Vector(new long[] {100, 100, 100, 100, 100, 200})};
        Vector[] firstNulls = {
                new BooleanVector(new boolean[] {false, false, false, true, true, false}),
                new BooleanVector(new boolean[] {false, true, true, false, false, false}),
                new BooleanVector(new boolean[6])};
        DistinctKeySet keys = DistinctKeySet.create(firstValues, true, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), flatKeyTablePolicy);
        try {
            int[] positions = new int[6];
            int distinct = keys.addBatch(firstValues, firstNulls, Mask.all(6), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1, 3, 5);

            long wide = 1L << 40;
            Vector[] promotedValues = {
                    new I64Vector(new long[] {1, 2, wide, wide}),
                    new I64Vector(new long[] {99, 99, 99, 99}),
                    new I64Vector(new long[] {100, 200, 300, 300})};
            Vector[] promotedNulls = {
                    new BooleanVector(new boolean[4]),
                    new BooleanVector(new boolean[] {true, true, true, true}),
                    new BooleanVector(new boolean[4])};
            positions = new int[4];
            distinct = keys.addBatch(promotedValues, promotedNulls, Mask.all(4), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(1, 2);
            assertThat(keys.addBatch(promotedValues, promotedNulls, Mask.all(4), positions)).isZero();
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveMultiLongDistinctCachesStableSharedBasePositionsAcrossBatches()
    {
        int[] innerIds = {0, 1, 2};
        I64Vector[] valueBases = {
                new I64Vector(new long[] {1, 2, 3}),
                new I64Vector(new long[] {10, 20, 30}),
                new I64Vector(new long[] {100, 200, 300})};
        BooleanVector[] nullBases = {
                new BooleanVector(new boolean[] {false, false, false}),
                new BooleanVector(new boolean[] {false, false, false}),
                new BooleanVector(new boolean[] {false, false, false})};

        Vector[] firstValues = nestedLongDictionaries(new int[] {0, 1, 0}, innerIds, valueBases);
        Vector[] firstNulls = nestedBooleanDictionaries(new int[] {0, 1, 0}, innerIds, nullBases);
        DistinctKeySet keys = DistinctKeySet.create(firstValues, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), flatKeyTablePolicy);
        try {
            int[] positions = new int[3];
            int distinct = keys.addBatch(firstValues, firstNulls, Mask.all(3), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1);

            // Base position 1 was already submitted through a different logical row. Base position 2 is new and
            // must retain its first logical position from this batch.
            Vector[] secondValues = nestedLongDictionaries(new int[] {1, 2, 2}, innerIds, valueBases);
            Vector[] secondNulls = nestedBooleanDictionaries(new int[] {1, 2, 2}, innerIds, nullBases);
            distinct = keys.addBatch(secondValues, secondNulls, Mask.all(3), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(1);

            // Reusing a pooled base object under a new content generation invalidates the bitmap. Exact hashing
            // then observes the changed tuple rather than treating its physical position as already processed.
            valueBases[0].clearForReuse();
            valueBases[0].values()[0] = 99;
            Vector[] changedValues = nestedLongDictionaries(new int[] {0}, innerIds, valueBases);
            Vector[] changedNulls = nestedBooleanDictionaries(new int[] {0}, innerIds, nullBases);
            distinct = keys.addBatch(changedValues, changedNulls, Mask.all(1), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0);
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveMultiLongDistinctInvalidatesCachedNullBaseGeneration()
    {
        int[] identity = {0};
        I64Vector[] valueBases = {
                new I64Vector(new long[] {1}),
                new I64Vector(new long[] {10}),
                new I64Vector(new long[] {100})};
        BooleanVector changedNullBase = new BooleanVector(new boolean[] {true});
        BooleanVector[] nullBases = {
                new BooleanVector(new boolean[] {false}),
                changedNullBase,
                new BooleanVector(new boolean[] {false})};
        Vector[] values = nestedLongDictionaries(identity, identity, valueBases);
        Vector[] nulls = nestedBooleanDictionaries(identity, identity, nullBases);

        DistinctKeySet keys = DistinctKeySet.create(values, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), flatKeyTablePolicy);
        try {
            int[] positions = new int[1];
            assertThat(keys.addBatch(values, nulls, Mask.all(1), positions)).isZero();

            // clearForReuse advances the content generation and resets the physical null bit. The position that
            // was previously cached as null must therefore be submitted to the exact table in the new lifetime.
            changedNullBase.clearForReuse();
            values = nestedLongDictionaries(identity, identity, valueBases);
            nulls = nestedBooleanDictionaries(identity, identity, nullBases);
            int distinct = keys.addBatch(values, nulls, Mask.all(1), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0);
        }
        finally {
            keys.releaseBuffers();
        }
    }

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
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, arrayPool, codeGeneration, flatKeyTablePolicy);
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

        layout = FlatKeyLayout.tryCreate(values, arrayPool, codeGeneration, flatKeyTablePolicy);
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
        DistinctKeySet keys = DistinctKeySet.create(new Vector[] {first}, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), flatKeyTablePolicy);
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

    private static Vector[] longPair(long[] first, long[] second)
    {
        return new Vector[] {new I64Vector(first), new I64Vector(second)};
    }

    private static DictionaryVector nestedLongDictionary(int[] outerIds, int[] innerIds, long[] values)
    {
        return DictionaryVector.wrapNested(
                outerIds,
                outerIds.length,
                DictionaryVector.wrapNested(innerIds, innerIds.length, new I64Vector(values)));
    }

    private static DictionaryVector nestedBooleanDictionary(int[] outerIds, int[] innerIds, boolean[] values)
    {
        return DictionaryVector.wrapNested(
                outerIds,
                outerIds.length,
                DictionaryVector.wrapNested(innerIds, innerIds.length, new BooleanVector(values)));
    }

    private static Vector[] nestedLongDictionaries(int[] outerIds, int[] innerIds, I64Vector[] bases)
    {
        Vector[] vectors = new Vector[bases.length];
        for (int index = 0; index < vectors.length; index++) {
            vectors[index] = DictionaryVector.wrapNested(
                    outerIds,
                    outerIds.length,
                    DictionaryVector.wrapNested(innerIds, innerIds.length, bases[index]));
        }
        return vectors;
    }

    private static Vector[] nestedBooleanDictionaries(int[] outerIds, int[] innerIds, BooleanVector[] bases)
    {
        Vector[] vectors = new Vector[bases.length];
        for (int index = 0; index < vectors.length; index++) {
            vectors[index] = DictionaryVector.wrapNested(
                    outerIds,
                    outerIds.length,
                    DictionaryVector.wrapNested(innerIds, innerIds.length, bases[index]));
        }
        return vectors;
    }

    private static void assertDistinctPositions(
            DistinctKeySet keys,
            Vector[] values,
            boolean[] secondNulls,
            int... expectedPositions)
    {
        int length = values[0].length();
        Vector[] nulls = {
                new BooleanVector(new boolean[length]),
                new BooleanVector(secondNulls == null ? new boolean[length] : secondNulls)};
        int[] positions = new int[length];
        int distinct = keys.addBatch(values, nulls, Mask.all(length), positions);
        assertThat(Arrays.copyOf(positions, distinct)).containsExactly(expectedPositions);
    }

    @Test
    void testDenseBitmapBatchFallsBackExactlyWhenDomainBecomesSparse()
    {
        long[] denseKeys = new long[4_096];
        for (int index = 0; index < denseKeys.length; index++) {
            denseKeys[index] = index;
        }

        DistinctKeySet keys = DistinctKeySet.create(new Vector[] {new I64Vector(denseKeys)}, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), flatKeyTablePolicy);
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

    @Test
    void testLargeGroupedLongDomainPromotesInlineKeysExactly()
    {
        long[] groups = {0, 0, 0, 0, 0, 0, 0, 0};
        long[] values = {0, 1, 2, 3, 4, 5, 5, 99};
        boolean[] valueNulls = {false, false, false, false, false, false, false, true};
        DistinctKeySet keys = DistinctKeySet.createGroupedLong(new Vector[] {
                new I64Vector(groups),
                new I64Vector(values)}, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), flatKeyTablePolicy);
        try {
            int[] positions = new int[groups.length];
            int distinct = keys.addGroupedBatch(
                    new Vector[] {new I64Vector(groups), new I64Vector(values)},
                    new Vector[] {new BooleanVector(new boolean[groups.length]), new BooleanVector(valueNulls)},
                    Mask.all(groups.length),
                    (1 << 16) + 1,
                    positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1, 2, 3, 4, 5);

            long[] laterValues = {0, 1, 5, 6};
            positions = new int[laterValues.length];
            distinct = keys.addGroupedBatch(
                    new Vector[] {new I64Vector(new long[laterValues.length]), new I64Vector(laterValues)},
                    new Vector[] {
                            new BooleanVector(new boolean[laterValues.length]),
                            new BooleanVector(new boolean[laterValues.length])},
                    Mask.all(laterValues.length),
                    (1 << 16) + 1,
                    positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(3);
        }
        finally {
            keys.releaseBuffers();
        }
    }
}
