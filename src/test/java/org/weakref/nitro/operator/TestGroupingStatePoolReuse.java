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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGroupingStatePoolReuse
{
    @Test
    public void testPackedIntTripleGroupingPreservesKeysAndFirstSeenIds()
    {
        GroupingState state = new GroupingState();
        I64Vector groups = new I64Vector(5);
        state.assignGroups(
                new Vector[] {
                        new I64Vector(new long[] {1, -1, 1, Integer.MAX_VALUE, -1}),
                        new I64Vector(new long[] {2, 3, 2, Integer.MIN_VALUE, 3}),
                        new I64Vector(new long[] {4, 5, 4, -6, 7})},
                new Vector[] {null, null, null},
                Mask.all(5),
                groups);

        assertThat(groups.values()).containsExactly(0, 1, 0, 2, 3);
        assertThat(state.groupCount()).isEqualTo(4);

        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("packedIntTripleTest");
        assertThat(((I64Vector) state.groupedValues(0, Mask.all(4), null, allocator, context).values()).values())
                .containsExactly(1, -1, Integer.MAX_VALUE, -1);
        assertThat(((I64Vector) state.groupedValues(1, Mask.all(4), null, allocator, context).values()).values())
                .containsExactly(2, 3, Integer.MIN_VALUE, 3);
        assertThat(((I64Vector) state.groupedValues(2, Mask.all(4), null, allocator, context).values()).values())
                .containsExactly(4, 5, -6, 7);

        allocator.release(context);
        state.releaseBuffers();
    }

    @Test
    public void testPackedIntTripleGroupingPromotesOnNullAndWideValue()
    {
        GroupingState state = new GroupingState();
        I64Vector firstGroups = new I64Vector(2);
        state.assignGroups(
                new Vector[] {
                        new I64Vector(new long[] {1, 4}),
                        new I64Vector(new long[] {2, 5}),
                        new I64Vector(new long[] {3, 6})},
                new Vector[] {null, null, null},
                Mask.all(2),
                firstGroups);
        assertThat(firstGroups.values()).containsExactly(0, 1);

        // The compact tail reserves three high bits for nullness. A non-null third key outside the signed
        // 29-bit domain must promote without losing the groups already assigned by the compact table.
        long wide = 1L << 28;
        I64Vector secondGroups = new I64Vector(3);
        state.assignGroups(
                new Vector[] {
                        new I64Vector(new long[] {1, 4, 7}),
                        new I64Vector(new long[] {2, 5, 8}),
                        new I64Vector(new long[] {9, 6, wide})},
                new Vector[] {new BooleanVector(new boolean[] {true, false, false}), null, null},
                Mask.all(3),
                secondGroups);
        assertThat(secondGroups.values()).containsExactly(2, 1, 3);
        assertThat(state.groupCount()).isEqualTo(4);

        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("packedIntTriplePromotionTest");
        Streams first = state.groupedValues(0, Mask.all(4), null, allocator, context);
        Streams third = state.groupedValues(2, Mask.all(4), null, allocator, context);
        assertThat(((I64Vector) first.values()).values()).containsExactly(1, 4, 0, 7);
        assertThat(((BooleanVector) first.get(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS)).values())
                .containsExactly(false, false, true, false);
        assertThat(((I64Vector) third.values()).values()).containsExactly(3, 6, 9, wide);

        allocator.release(context);
        state.releaseBuffers();
    }

    @Test
    public void testPackedIntPairGroupingPreservesSignedKeysAndFirstSeenIds()
    {
        GroupingState state = new GroupingState();
        I64Vector groups = new I64Vector(4);
        state.assignGroups(
                new Vector[] {
                        new I64Vector(new long[] {1, -1, 1, Integer.MAX_VALUE}),
                        new I64Vector(new long[] {2, 3, 2, Integer.MIN_VALUE})},
                new Vector[] {null, null},
                Mask.all(4),
                groups);

        assertThat(groups.values()).containsExactly(0, 1, 0, 2);
        assertThat(state.groupCount()).isEqualTo(3);

        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("packedIntPairTest");
        Streams first = state.groupedValues(0, Mask.all(3), null, allocator, context);
        Streams second = state.groupedValues(1, Mask.all(3), null, allocator, context);
        assertThat(((I64Vector) first.values()).values()).containsExactly(1, -1, Integer.MAX_VALUE);
        assertThat(((I64Vector) second.values()).values()).containsExactly(2, 3, Integer.MIN_VALUE);
        assertThat(((BooleanVector) first.get(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS)).values()).containsExactly(false, false, false);

        allocator.release(context);
        state.releaseBuffers();
    }

    @Test
    public void testPackedIntPairGroupingPromotesOnNullWithoutChangingIds()
    {
        GroupingState state = new GroupingState();
        I64Vector firstGroups = new I64Vector(2);
        state.assignGroups(
                new Vector[] {new I64Vector(new long[] {1, 3}), new I64Vector(new long[] {2, 4})},
                new Vector[] {null, null},
                Mask.all(2),
                firstGroups);
        assertThat(firstGroups.values()).containsExactly(0, 1);

        I64Vector secondGroups = new I64Vector(3);
        state.assignGroups(
                new Vector[] {new I64Vector(new long[] {1, 3, 6}), new I64Vector(new long[] {5, 4, 7})},
                new Vector[] {new BooleanVector(new boolean[] {true, false, false}), null},
                Mask.all(3),
                secondGroups);
        assertThat(secondGroups.values()).containsExactly(2, 1, 3);
        assertThat(state.groupCount()).isEqualTo(4);

        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("packedIntPairPromotionTest");
        Streams first = state.groupedValues(0, Mask.all(4), null, allocator, context);
        Streams second = state.groupedValues(1, Mask.all(4), null, allocator, context);
        assertThat(((I64Vector) first.values()).values()).containsExactly(1, 3, 0, 6);
        assertThat(((BooleanVector) first.get(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS)).values()).containsExactly(false, false, true, false);
        assertThat(((I64Vector) second.values()).values()).containsExactly(2, 4, 5, 7);

        allocator.release(context);
        state.releaseBuffers();
    }

    @Test
    public void testPackedIntPairGroupingPromotesOnWideValue()
    {
        GroupingState state = new GroupingState();
        I64Vector firstGroups = new I64Vector(1);
        state.assignGroups(
                new Vector[] {new I64Vector(new long[] {1}), new I64Vector(new long[] {2})},
                new Vector[] {null, null},
                Mask.all(1),
                firstGroups);

        long wide = (long) Integer.MAX_VALUE + 1;
        I64Vector secondGroups = new I64Vector(2);
        state.assignGroups(
                new Vector[] {new I64Vector(new long[] {1, wide}), new I64Vector(new long[] {2, 3})},
                new Vector[] {null, null},
                Mask.all(2),
                secondGroups);
        assertThat(secondGroups.values()).containsExactly(0, 1);

        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("packedIntPairWidePromotionTest");
        Streams first = state.groupedValues(0, Mask.all(2), null, allocator, context);
        assertThat(((I64Vector) first.values()).values()).containsExactly(1, wide);

        allocator.release(context);
        state.releaseBuffers();
    }

    @Test
    public void testNestedDictionaryKeysResolveToLeafValues()
    {
        BinaryVector binaryLeaf = new BinaryVector(3, 3);
        binaryLeaf.setBytes(0, "A".getBytes(StandardCharsets.UTF_8));
        binaryLeaf.setBytes(1, "B".getBytes(StandardCharsets.UTF_8));
        binaryLeaf.setBytes(2, "C".getBytes(StandardCharsets.UTF_8));
        DictionaryVector binaryInner = DictionaryVector.wrapNested(new int[] {2, 0, 1, 2}, 4, binaryLeaf);

        I64Vector longLeaf = new I64Vector(new long[] {10, 20, 30});
        DictionaryVector longInner = DictionaryVector.wrapNested(new int[] {1, 2, 0, 1}, 4, longLeaf);

        int[] outerIds = {1, 0, 3, 2, 1};
        GroupingState state = new GroupingState();
        I64Vector groups = new I64Vector(outerIds.length);
        state.assignGroups(
                new Vector[] {
                        DictionaryVector.wrapNested(outerIds, outerIds.length, binaryInner),
                        DictionaryVector.wrapNested(outerIds, outerIds.length, longInner)},
                new Vector[] {null, null},
                Mask.all(outerIds.length),
                groups);

        // Logical keys are (A,30), (C,20), (C,20), (B,10), (A,30).
        assertThat(groups.values()).containsExactly(0, 1, 1, 2, 0);
        assertThat(state.groupCount()).isEqualTo(3);
        state.releaseBuffers();
    }

    @Test
    public void testSharedDictionaryCacheIsEmptyAfterPoolReuse()
    {
        int dictionarySize = 32_768; // long[] is large enough to enter PrimitiveArrayPool
        BinaryVector firstValues = dictionaryValues(dictionarySize, "a");
        BinaryVector secondValues = dictionaryValues(dictionarySize, "b");
        int[] firstIds = {0, 1, 2, 3, 4};

        GroupingState first = new GroupingState();
        I64Vector firstGroups = new I64Vector(firstIds.length);
        first.assignGroups(
                new Vector[] {DictionaryVector.wrap(firstIds, firstValues), DictionaryVector.wrap(firstIds, secondValues)},
                new Vector[] {null, null},
                Mask.all(firstIds.length),
                firstGroups);
        assertThat(firstGroups.values()).containsExactly(0, 1, 2, 3, 4);
        first.releaseBuffers();

        GroupingState second = new GroupingState();
        I64Vector secondGroups = new I64Vector(1);
        int[] secondIds = {4};
        second.assignGroups(
                new Vector[] {DictionaryVector.wrap(secondIds, firstValues), DictionaryVector.wrap(secondIds, secondValues)},
                new Vector[] {null, null},
                Mask.all(1),
                secondGroups);

        assertThat(secondGroups.values()[0]).isZero();
        assertThat(second.groupCount()).isEqualTo(1);
        second.releaseBuffers();
    }

    @Test
    public void testPooledFlatRecordDoesNotReadNullVariableWidthPayload()
    {
        GroupingState first = new GroupingState();
        I64Vector firstGroups = new I64Vector(1);
        first.assignGroups(
                new Vector[] {binaryValue("prefix"), binaryValue("poison"), new I64Vector(new long[] {1})},
                new Vector[] {null, null, null},
                Mask.all(1),
                firstGroups);
        first.releaseBuffers();

        GroupingState second = new GroupingState();
        I64Vector secondGroups = new I64Vector(1);
        second.assignGroups(
                new Vector[] {binaryValue("x"), binaryValue("ignored"), new I64Vector(new long[] {2})},
                new Vector[] {null, new BooleanVector(new boolean[] {true}), null},
                Mask.all(1),
                secondGroups);

        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("pooledFlatRecordTest");
        Streams grouped = second.groupedValues(1, Mask.all(1), null, allocator, context);
        assertThat(((BooleanVector) grouped.get(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS)).values()[0]).isTrue();
        assertThat(((BinaryVector) grouped.values()).length(0)).isZero();

        allocator.release(context);
        second.releaseBuffers();
    }

    private static BinaryVector dictionaryValues(int size, String prefix)
    {
        BinaryVector values = new BinaryVector(size, size * 8);
        for (int index = 0; index < size; index++) {
            values.setBytes(index, (prefix + index).getBytes(StandardCharsets.UTF_8));
        }
        return values;
    }

    private static BinaryVector binaryValue(String value)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        BinaryVector vector = new BinaryVector(1, bytes.length);
        vector.setBytes(0, bytes);
        return vector;
    }
}
