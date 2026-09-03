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
package org.weakref.nitro;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class TestMask
{
    private static final int DENSE_DOUBLE_TEST_SIZE = 131;

    @Test
    void primitiveIteratorPreservesDenseSparseAndExcludedPositions()
    {
        assertPrimitivePositions(Mask.all(4), 0, 1, 2, 3);
        assertPrimitivePositions(Mask.sparse(new int[] {1, 4, 7}, 9), 1, 4, 7);

        Mask excluded = Mask.sparse(new int[] {0, 2, 5, 8}, 9).complement();
        assertPrimitivePositions(excluded, 1, 3, 4, 6, 7);
    }

    @Test
    void maxPositionPreservesUnorderedSparseIteration()
    {
        Mask mask = Mask.sparse(new int[] {7, 1, 4}, 9);

        assertThat(mask.maxPosition()).isEqualTo(7);
        assertPrimitivePositions(mask, 7, 1, 4);
    }

    @Test
    void denseIntegerConstantRangePreservesStrictBounds()
    {
        int[] values = new int[257];
        for (int position = 0; position < values.length; position++) {
            values[position] = position - 128;
        }

        Mask mask = Mask.all(values.length);
        mask.retainConstantRange(values, -17, 43, null);

        assertThat(mask).containsExactly(java.util.stream.IntStream.rangeClosed(112, 170).boxed().toArray(Integer[]::new));
    }

    @Test
    void integerConstantRangePreservesSparseNullAndWideBoundSemantics()
    {
        int[] values = {Integer.MIN_VALUE, -10, 0, 10, Integer.MAX_VALUE};
        boolean[] nulls = {false, false, true, false, false};

        Mask sparse = Mask.sparse(new int[] {0, 1, 2, 4}, values.length);
        sparse.retainConstantRange(values, (long) Integer.MIN_VALUE - 1, (long) Integer.MAX_VALUE + 1, nulls);

        assertThat(sparse).containsExactly(0, 1, 4);
    }

    @Test
    void denseIntegerConstantRangeKeepsAllSelectedAndHandlesLateFailure()
    {
        int[] values = new int[257];
        java.util.Arrays.fill(values, 7);

        Mask all = Mask.all(values.length);
        all.retainConstantRange(values, 0, 10, null);
        assertThat(all.all()).isTrue();
        assertThat(all).hasSize(values.length);

        values[250] = 10;
        Mask lateFailure = Mask.all(values.length);
        lateFailure.retainConstantRange(values, 0, 10, null);
        assertThat(lateFailure).hasSize(values.length - 1).doesNotContain(250);
    }

    @Test
    void denseDoubleLessThanPreservesTailAndNanSemantics()
    {
        double[] values = new double[DENSE_DOUBLE_TEST_SIZE];
        for (int position = 0; position < values.length; position++) {
            values[position] = position - 64.5;
        }
        values[3] = Double.NaN;
        values[17] = Double.NEGATIVE_INFINITY;
        values[values.length - 1] = Double.POSITIVE_INFINITY;

        Mask mask = Mask.all(values.length);
        mask.retainConstantComparison(values, 0.0, Mask.ComparisonOperator.LESS_THAN);

        Integer[] expected = java.util.stream.IntStream.range(0, values.length)
                .filter(position -> values[position] < 0.0)
                .boxed()
                .toArray(Integer[]::new);
        assertThat(mask).containsExactly(expected);
    }

    @Test
    void directDictionaryIdComparisonPreservesMatchComplementAndNullSemantics()
    {
        int[] ids = {2, 1, 2, 0, 2, 1};
        boolean[] nulls = {false, false, true, false, false, true};

        Mask matches = Mask.all(ids.length);
        matches.retainDictionaryIdComparison(ids, 2, nulls, true);
        assertThat(matches).containsExactly(0, 4);

        Mask complement = Mask.all(ids.length);
        complement.retainDictionaryIdComparison(ids, 2, nulls, false);
        assertThat(complement).containsExactly(1, 3);

        Mask sparse = Mask.sparse(new int[] {1, 2, 3, 4}, ids.length);
        sparse.retainDictionaryIdComparison(ids, 2, null, false);
        assertThat(sparse).containsExactly(1, 3);
    }

    @Test
    void compactDictionaryComparisonPreservesDenseSparseComplementAndNullSemantics()
    {
        int[] ids = {3, 1, 2, 0, 3, 2, 1};
        boolean[] keep = {false, true, false, true};
        boolean[] nulls = {false, false, false, false, true, false, true};

        Mask dense = Mask.all(ids.length);
        dense.retainDictionaryComparison(ids, keep);
        assertThat(dense).containsExactly(0, 1, 4, 6);

        Mask matches = Mask.all(ids.length);
        matches.retainDictionaryComparison(ids, keep, nulls, true);
        assertThat(matches).containsExactly(0, 1);

        Mask complement = Mask.all(ids.length);
        complement.retainDictionaryComparison(ids, keep, nulls, false);
        assertThat(complement).containsExactly(2, 3, 5);

        Mask sparse = Mask.sparse(new int[] {0, 2, 3, 6}, ids.length);
        sparse.retainDictionaryComparison(ids, keep, null, true);
        assertThat(sparse).containsExactly(0, 6);

        Mask denseDomain = Mask.all(ids.length);
        denseDomain.retainDictionaryComparison(ids, new boolean[] {true, true, true, false}, null, true);
        assertThat(denseDomain).containsExactly(1, 2, 3, 5, 6);
    }

    @Test
    void compactDictionaryComparisonRetainsAlignedDomainUntilPositionsAreRequired()
    {
        int[] ids = {3, 1, 2, 0, 3, 2, 1};
        DictionaryVector dictionary = DictionaryVector.wrap(ids, ids.length, new I64Vector(new long[] {10, 20, 30, 40}));
        Mask mask = Mask.all(ids.length);

        mask.retainDictionaryComparison(ids, new boolean[] {false, true, false, true});

        Mask.DictionaryDomainSelection selection = mask.dictionaryDomainSelection(dictionary);
        assertThat(selection).isNotNull();
        assertThat(selection.selectedDomainBits()).isEqualTo(0b1010);
        assertThat(mask.count()).isEqualTo(4);
        assertThat(mask.maxPosition()).isEqualTo(6);

        assertThat(mask.selectedPositions()).startsWith(0, 1, 4, 6);
        assertThat(mask.dictionaryDomainSelection(dictionary)).isNull();
    }

    @Test
    void compactDictionaryComparisonConsumesExactDomainFrequencies()
    {
        int[] ids = {3, 1, 2, 0, 3, 2, 1};
        DictionaryVector dictionary = DictionaryVector.wrapWithDomainFrequencies(
                ids,
                ids.length,
                new I64Vector(new long[] {10, 20, 30, 40}),
                new int[] {1, 2, 2, 2});
        Mask mask = Mask.all(ids.length);

        mask.retainDictionaryComparison(dictionary, new boolean[] {false, true, false, true});

        Mask.DictionaryDomainSelection selection = mask.dictionaryDomainSelection(dictionary);
        assertThat(selection).isNotNull();
        assertThat(selection.selectedDomainBits()).isEqualTo(0b1010);
        assertThat(mask.count()).isEqualTo(4);
        assertThat(mask).containsExactly(0, 1, 4, 6);
    }

    @Test
    void compactDictionaryComplementConsumesExactDomainFrequencies()
    {
        int[] ids = {3, 1, 2, 0, 3, 2, 1};
        DictionaryVector dictionary = DictionaryVector.wrapWithDomainFrequencies(
                ids,
                ids.length,
                new I64Vector(new long[] {10, 20, 30, 40}),
                new int[] {1, 2, 2, 2});
        Mask mask = Mask.all(ids.length);

        mask.retainDictionaryComparison(dictionary, new boolean[] {false, true, false, true}, false);

        Mask.DictionaryDomainSelection selection = mask.dictionaryDomainSelection(dictionary);
        assertThat(selection).isNotNull();
        assertThat(selection.selectedDomainBits()).isEqualTo(0b0101);
        assertThat(mask.count()).isEqualTo(3);
        assertThat(mask.copyDictionaryDomainFrequencies(selection)).containsExactly(1, 2, 2, 2);
        assertThat(mask).containsExactly(2, 3, 5);
    }

    @Test
    void visitsSelectedDictionaryDomainWithoutLogicalExpansion()
    {
        int[] ids = {3, 1, 2, 0, 3, 2, 1};
        DictionaryVector dictionary = DictionaryVector.wrapWithDomainFrequencies(
                ids,
                ids.length,
                new I64Vector(new long[] {10, 20, 30, 40}),
                new int[] {1, 2, 2, 2});
        Mask mask = Mask.all(ids.length);
        mask.retainDictionaryComparison(dictionary, new boolean[] {false, true, false, true});

        ArrayList<Integer> visited = new ArrayList<>();
        assertThat(dictionary.visitSelectedDomain(mask, value -> visited.add(value))).isTrue();
        assertThat(visited).containsExactly(1, 3);
    }

    @Test
    void visitsArbitraryCardinalityDomainFromExactFrequencies()
    {
        long[] values = new long[70];
        int[] frequencies = new int[70];
        frequencies[1] = 2;
        frequencies[69] = 1;
        DictionaryVector dictionary = DictionaryVector.wrapWithDomainFrequencies(
                new int[] {1, 69, 1},
                3,
                new I64Vector(values),
                frequencies);

        ArrayList<Integer> visited = new ArrayList<>();
        assertThat(dictionary.visitSelectedDomain(Mask.all(3), value -> visited.add(value))).isTrue();
        assertThat(visited).containsExactly(1, 69);
    }

    @Test
    void countsIndependentDictionaryDomainUnderArbitrarySelection()
    {
        DictionaryVector dictionary = DictionaryVector.wrapWithDomainFrequencies(
                new int[] {0, 1, 0, 2, 1, 2, 0},
                7,
                new I64Vector(new long[] {10, 20, 30}),
                new int[] {3, 2, 2});
        int[] frequencies = {99, 99, 99, 99};

        int populated = dictionary.populateSelectedDomainFrequencies(
                Mask.sparse(new int[] {1, 2, 5, 6}, 7),
                frequencies);

        assertThat(populated).isEqualTo(3);
        assertThat(frequencies).containsExactly(2, 1, 1, 99);
    }

    @Test
    void reusesAlignedDictionaryDomainCountsWithoutLogicalExpansion()
    {
        int[] ids = {0, 1, 0, 2, 1, 2};
        DictionaryVector dictionary = DictionaryVector.wrapWithDomainFrequencies(
                ids,
                ids.length,
                new I64Vector(new long[] {10, 20, 30}),
                new int[] {2, 2, 2});
        Mask mask = Mask.all(ids.length);
        mask.retainDictionaryComparison(dictionary, new boolean[] {false, true, true});
        int[] frequencies = new int[3];

        int populated = dictionary.populateSelectedDomainFrequencies(mask, frequencies);

        assertThat(populated).isEqualTo(2);
        assertThat(frequencies).containsExactly(0, 2, 2);
    }

    @Test
    void declinesDomainVisitWithoutExactMetadata()
    {
        DictionaryVector dictionary = DictionaryVector.wrap(
                new int[] {1, 2, 1},
                3,
                new I64Vector(new long[] {10, 20, 30}));

        ArrayList<Integer> visited = new ArrayList<>();
        assertThat(dictionary.visitSelectedDomain(Mask.all(3), value -> visited.add(value))).isFalse();
        assertThat(visited).isEmpty();
    }

    @Test
    void compactDictionaryComparisonRetainsFrequenciesForDegenerateSelections()
    {
        int[] ids = {0, 1, 0, 2, 1, 2};
        DictionaryVector dictionary = DictionaryVector.wrap(ids, ids.length, new I64Vector(new long[] {10, 20, 30}));

        Mask all = Mask.all(ids.length);
        all.retainDictionaryComparison(ids, new boolean[] {true, true, true});
        Mask.DictionaryDomainSelection allSelection = all.dictionaryDomainSelection(dictionary);
        assertThat(all.all()).isTrue();
        assertThat(allSelection).isNotNull();
        assertThat(all.copyDictionaryDomainFrequencies(allSelection)).containsExactly(2, 2, 2);
        all.retainDictionaryComparison(ids, new boolean[] {false, true, true});
        assertThat(all.all()).isFalse();
        assertThat(all).containsExactly(1, 3, 4, 5);

        Mask none = Mask.all(ids.length);
        none.retainDictionaryComparison(ids, new boolean[] {false, false, false});
        Mask.DictionaryDomainSelection noSelection = none.dictionaryDomainSelection(dictionary);
        assertThat(none.none()).isTrue();
        assertThat(noSelection).isNotNull();
        assertThat(none.copyDictionaryDomainFrequencies(noSelection)).containsExactly(2, 2, 2);
    }

    @Test
    void compactDictionaryComparisonComposesAlignedDomainsWithoutMaterializingPositions()
    {
        int[] ids = {3, 1, 2, 0, 3, 2, 1};
        DictionaryVector dictionary = DictionaryVector.wrap(ids, ids.length, new I64Vector(new long[] {10, 20, 30, 40}));
        Mask mask = Mask.all(ids.length);

        mask.retainDictionaryComparison(ids, new boolean[] {true, true, true, false}, null, true);
        mask.retainDictionaryComparison(ids, new boolean[] {false, true, true, true}, null, true);

        Mask.DictionaryDomainSelection selection = mask.dictionaryDomainSelection(dictionary);
        assertThat(selection).isNotNull();
        assertThat(selection.selectedDomainBits()).isEqualTo(0b0110);
        assertThat(mask.count()).isEqualTo(4);
        assertThat(mask.maxPosition()).isEqualTo(6);
        assertThat(mask).containsExactly(1, 2, 5, 6);
    }

    @Test
    void compactDictionaryDomainSurvivesCopyAndComplement()
    {
        int[] ids = {3, 1, 2, 0, 3, 2, 1};
        DictionaryVector dictionary = DictionaryVector.wrap(ids, ids.length, new I64Vector(new long[] {10, 20, 30, 40}));
        Mask selected = Mask.all(ids.length);
        selected.retainDictionaryComparison(ids, new boolean[] {false, true, false, true});

        Mask copy = Mask.none(ids.length);
        copy.copyFrom(selected);
        Mask complement = selected.complement();

        assertThat(copy.dictionaryDomainSelection(dictionary)).isNotNull();
        assertThat(copy.dictionaryDomainSelection(dictionary).selectedDomainBits()).isEqualTo(0b1010);
        assertThat(copy).containsExactly(0, 1, 4, 6);
        assertThat(complement.dictionaryDomainSelection(dictionary)).isNotNull();
        assertThat(complement.dictionaryDomainSelection(dictionary).selectedDomainBits()).isEqualTo(0b0101);
        assertThat(complement).containsExactly(2, 3, 5);
    }

    @Test
    void unionPreservesAlignedDictionaryDomain()
    {
        int[] ids = {3, 1, 2, 0, 3, 2, 1};
        DictionaryVector dictionary = DictionaryVector.wrapWithDomainFrequencies(
                ids,
                ids.length,
                new I64Vector(new long[] {10, 20, 30, 40}),
                new int[] {1, 2, 2, 2});
        Mask left = Mask.all(ids.length);
        left.retainDictionaryComparison(dictionary, new boolean[] {false, true, false, false});
        Mask right = Mask.all(ids.length);
        right.retainDictionaryComparison(dictionary, new boolean[] {false, false, false, true});

        Mask union = left.union(right);

        Mask.DictionaryDomainSelection selection = union.dictionaryDomainSelection(dictionary);
        assertThat(selection).isNotNull();
        assertThat(selection.selectedDomainBits()).isEqualTo(0b1010);
        assertThat(union.copyDictionaryDomainFrequencies(selection)).containsExactly(1, 2, 2, 2);
        assertThat(union).containsExactly(0, 1, 4, 6);
    }

    @Test
    void differenceInPlacePreservesAlignedDictionaryDomain()
    {
        int[] ids = {3, 1, 2, 0, 3, 2, 1};
        DictionaryVector dictionary = DictionaryVector.wrapWithDomainFrequencies(
                ids,
                ids.length,
                new I64Vector(new long[] {10, 20, 30, 40}),
                new int[] {1, 2, 2, 2});
        Mask selected = Mask.all(ids.length);
        selected.retainDictionaryComparison(dictionary, new boolean[] {false, true, false, true});
        Mask remaining = Mask.all(ids.length);

        remaining.differenceInPlace(selected);

        Mask.DictionaryDomainSelection selection = remaining.dictionaryDomainSelection(dictionary);
        assertThat(selection).isNotNull();
        assertThat(selection.selectedDomainBits()).isEqualTo(0b0101);
        assertThat(remaining).containsExactly(2, 3, 5);
    }

    @Test
    void retainsDictionaryBooleanVectorWithoutFlattening()
    {
        int[] ids = {0, 1, 1, 0, 1, 0};
        DictionaryVector dictionary = DictionaryVector.wrapWithDomainFrequencies(
                ids,
                ids.length,
                new BooleanVector(new boolean[] {false, true}),
                new int[] {3, 3});
        Mask mask = Mask.all(ids.length);

        assertThat(mask.tryRetainBooleanVector(dictionary, true)).isTrue();
        assertThat(mask.dictionaryDomainSelection(dictionary)).isNotNull();
        assertThat(mask).containsExactly(1, 2, 4);
    }

    @Test
    void retainsRleBooleanVectorWithMonotonicRunTraversal()
    {
        RleVector values = new RleVector(
                new int[] {3, 2, 4},
                new BooleanVector(new boolean[] {false, true, false}));
        Mask mask = Mask.sparse(new int[] {1, 3, 4, 6, 8}, values.length());

        assertThat(mask.tryRetainBooleanVector(values, true)).isTrue();
        assertThat(mask).containsExactly(3, 4);
    }

    private static void assertPrimitivePositions(Mask mask, int... expected)
    {
        int[] actual = new int[mask.selectedCount()];
        int index = 0;
        for (var positions = mask.iterator(); positions.hasNext(); ) {
            actual[index++] = positions.nextInt();
        }
        assertThat(actual).containsExactly(expected);
    }
}
