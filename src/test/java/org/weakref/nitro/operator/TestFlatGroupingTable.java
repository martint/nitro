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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class TestFlatGroupingTable
{
    @Test
    void testDictionaryValueIdsObservePooledBinaryGeneration()
    {
        Allocator allocator = new Allocator();
        Allocator.Context context = new Allocator.Context("pooled-dictionary-generation");
        GroupingState state = new GroupingState();
        int[] dictionaryIds = {0, 1};

        BinaryVector first = binary(allocator, context, "alpha", "beta");
        I64Vector firstGroups = new I64Vector(2);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(dictionaryIds, 2, first)},
                new Vector[] {null},
                Mask.all(2),
                firstGroups);
        assertThat(firstGroups.values()).containsExactly(0, 1);
        allocator.release(context, first);

        BinaryVector second = binary(allocator, context, "gamma", "zeta");
        assertThat(second).isSameAs(first);
        I64Vector secondGroups = new I64Vector(2);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(dictionaryIds, 2, second)},
                new Vector[] {null},
                Mask.all(2),
                secondGroups);
        assertThat(secondGroups.values()).containsExactly(2, 3);
        allocator.release(context, second);

        BinaryVector third = binary(allocator, context, "gamma", "zeta");
        assertThat(third).isSameAs(first);
        I64Vector thirdGroups = new I64Vector(2);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(dictionaryIds, 2, third)},
                new Vector[] {null},
                Mask.all(2),
                thirdGroups);
        assertThat(thirdGroups.values()).containsExactly(2, 3);
        assertThat(state.groupCount()).isEqualTo(4);

        state.releaseBuffers();
        allocator.release(context);
    }

    @Test
    void testNormalizedScratchRequiresProportionalAddressSpace()
    {
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(1024, 1024)).isTrue();
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(1024, 256)).isTrue();
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(1024, 255)).isFalse();
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(127, 127)).isFalse();
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(1 << 26, 2)).isFalse();
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(0, 0)).isFalse();
    }

    @Test
    void testEnsureCapacityPreservesExistingAndLaterGroups()
    {
        Vector[] values = {utf8("alpha", "beta", "alpha")};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(values), 2);
        try {
            table.beginBatch(values, new Vector[] {null});
            assertThat(table.assignGroup(values, null, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(values, null, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(values, null, 2, 2)).isEqualTo(0);
            table.endBatch();

            table.ensureCapacity(1 << 16);

            Vector[] later = {utf8("beta", "gamma", "alpha")};
            table.beginBatch(later, new Vector[] {null});
            assertThat(table.findGroup(later, null, 0)).isEqualTo(1);
            assertThat(table.assignGroup(later, null, 1, 2)).isEqualTo(2);
            assertThat(table.findGroup(later, null, 2)).isEqualTo(0);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testDiscriminatingFieldHashStillComparesCompleteCompositeKey()
    {
        int size = 128;
        String[] first = new String[size];
        String[] second = new String[size];
        String[] third = new String[size];
        String[] fourth = new String[size];
        for (int position = 0; position < size; position++) {
            first[position] = "customer-" + position;
            second[position] = "first";
            third[position] = "last";
            fourth[position] = "country";
        }
        int[] sharedIds = new int[size];
        for (int position = 0; position < size; position++) {
            sharedIds[position] = position;
        }

        GroupingState state = new GroupingState();
        I64Vector initialGroups = new I64Vector(size);
        state.assignGroups(
                new Vector[] {
                        nestedDictionary(sharedIds, first),
                        nestedDictionary(sharedIds, second),
                        nestedDictionary(sharedIds, third),
                        nestedDictionary(sharedIds, fourth)},
                new Vector[] {null, null, null, null},
                Mask.all(size),
                initialGroups);
        assertThat(state.groupCount()).isEqualTo(size);

        // The high-cardinality first field is sufficient for hash placement, but never for equality. A later row
        // with the same discriminator and a different trailing field must remain a distinct composite group.
        fourth[0] = "different-country";
        I64Vector laterGroups = new I64Vector(size);
        state.assignGroups(
                new Vector[] {
                        nestedDictionary(sharedIds, first),
                        nestedDictionary(sharedIds, second),
                        nestedDictionary(sharedIds, third),
                        nestedDictionary(sharedIds, fourth)},
                new Vector[] {null, null, null, null},
                Mask.all(size),
                laterGroups);
        assertThat(state.groupCount()).isEqualTo(size + 1L);
        assertThat(laterGroups.values()[0]).isEqualTo(size);
        assertThat(laterGroups.values()[1]).isEqualTo(initialGroups.values()[1]);
        state.releaseBuffers();
    }

    private static DictionaryVector nestedDictionary(int[] ids, String[] values)
    {
        DictionaryVector inner = DictionaryVector.wrapNested(ids, ids.length, utf8(values));
        return DictionaryVector.wrapNested(ids, ids.length, inner);
    }

    private static BinaryVector binary(Allocator allocator, Allocator.Context context, String... values)
    {
        int byteCount = 0;
        for (String value : values) {
            byteCount += value.getBytes(StandardCharsets.UTF_8).length;
        }
        BinaryVector result = BinaryVector.allocate(allocator, context, values.length, byteCount);
        for (int position = 0; position < values.length; position++) {
            result.setBytes(position, values[position].getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }

    @Test
    void testRecordIdentityAdmissionRequiresLargeHighCardinalityBatch()
    {
        int size = 1 << 12;
        String[] distinctValues = new String[size];
        String[] categoricalValues = new String[size];
        for (int position = 0; position < size; position++) {
            distinctValues[position] = "key-" + position;
            categoricalValues[position] = "category-" + (position & 15);
        }

        GroupingState distinct = new GroupingState();
        I64Vector distinctGroups = new I64Vector(size);
        distinct.assignGroups(new Vector[] {utf8(distinctValues)}, new Vector[] {null}, Mask.all(size), distinctGroups);
        assertThat(distinct.usesFlatSingleRecordIdentity()).isTrue();
        assertThat(distinct.groupCount()).isEqualTo(size);
        assertThat(distinctGroups.values()[0]).isZero();
        assertThat(distinctGroups.values()[size - 1]).isEqualTo(size - 1L);
        distinct.releaseBuffers();

        GroupingState categorical = new GroupingState();
        I64Vector categoricalGroups = new I64Vector(size);
        categorical.assignGroups(new Vector[] {utf8(categoricalValues)}, new Vector[] {null}, Mask.all(size), categoricalGroups);
        assertThat(categorical.usesFlatSingleRecordIdentity()).isFalse();
        assertThat(categorical.groupCount()).isEqualTo(16);
        assertThat(categoricalGroups.values()[0]).isEqualTo(categoricalGroups.values()[16]);
        categorical.releaseBuffers();
    }

    @Test
    void testNullableRecordIdentityPreservesNullOrder()
    {
        org.weakref.nitro.data.BooleanVector firstNulls = new org.weakref.nitro.data.BooleanVector(4);
        firstNulls.values()[1] = true;
        Vector[] nulls = {firstNulls};
        BinaryVector first = utf8("alpha", "ignored", "beta", "alpha");
        Vector[] firstValues = {first};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(firstValues, true), 16, true);
        try {
            table.beginBatch(firstValues, nulls);
            assertThat(table.assignGroup(firstValues, nulls, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(firstValues, nulls, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(firstValues, nulls, 2, 2)).isEqualTo(2);
            assertThat(table.assignGroup(firstValues, nulls, 3, 3)).isEqualTo(0);
            table.endBatch();

            org.weakref.nitro.data.BooleanVector secondNulls = new org.weakref.nitro.data.BooleanVector(3);
            secondNulls.values()[1] = true;
            Vector[] secondNullVectors = {secondNulls};
            BinaryVector second = utf8("gamma", "ignored", "beta");
            Vector[] secondValues = {second};
            table.beginBatch(secondValues, secondNullVectors);
            assertThat(table.assignGroup(secondValues, secondNullVectors, 0, 3)).isEqualTo(3);
            assertThat(table.assignGroup(secondValues, secondNullVectors, 1, 4)).isEqualTo(1);
            assertThat(table.assignGroup(secondValues, secondNullVectors, 2, 4)).isEqualTo(2);
            table.endBatch();

            assertThat(table.recordIndex(0)).isEqualTo(0);
            assertThat(table.recordIndex(1)).isEqualTo(1);
            assertThat(table.recordIndex(2)).isEqualTo(2);
            assertThat(table.recordIndex(3)).isEqualTo(3);
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testNormalizedIntKeyPreservesCompleteEqualityAcrossBinaryEncodings()
    {
        Vector[] firstValues = {
                DictionaryVector.wrapNested(new int[] {0, 0, 1, 0}, 4, utf8("alpha", "beta")),
                new I64Vector(new long[] {1, 1, 2, 1}),
                new I64Vector(new long[] {10, 10, 20, 10}),
                new I64Vector(new long[] {1, 1, 1, 2})};
        Vector[] nulls = {null, null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(firstValues, true);
        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
        try {
            table.beginBatch(firstValues, nulls);
            table.prepareBatchHashes(firstValues, nulls, Mask.all(4));
            assertThat(table.assignGroup(firstValues, nulls, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(firstValues, nulls, 1, 1)).isEqualTo(0);
            assertThat(table.assignGroup(firstValues, nulls, 2, 1)).isEqualTo(1);
            assertThat(table.assignGroup(firstValues, nulls, 3, 2)).isEqualTo(2);
            table.endBatch();

            // The same binary value arrives flat instead of dictionary-encoded. Query-stable interning must keep
            // its normalized key identical; the negative long declines normalization and exercises exact fallback.
            Vector[] laterValues = {
                    utf8("beta", "alpha"),
                    new I64Vector(new long[] {2, 1}),
                    new I64Vector(new long[] {20, -10}),
                    new I64Vector(new long[] {1, 1})};
            table.beginBatch(laterValues, nulls);
            table.prepareBatchHashes(laterValues, nulls, Mask.all(2));
            assertThat(table.assignGroup(laterValues, nulls, 0, 3)).isEqualTo(1);
            assertThat(table.assignGroup(laterValues, nulls, 1, 3)).isEqualTo(3);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    private static BinaryVector utf8(String... values)
    {
        int bytes = 0;
        for (String value : values) {
            bytes += value.getBytes(StandardCharsets.UTF_8).length;
        }
        BinaryVector vector = new BinaryVector(values.length, bytes);
        for (int index = 0; index < values.length; index++) {
            vector.setBytes(index, values[index].getBytes(StandardCharsets.UTF_8));
        }
        return vector;
    }
}
