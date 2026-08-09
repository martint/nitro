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
import org.weakref.nitro.data.Mask;

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
