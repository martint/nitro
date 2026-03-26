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
package org.weakref.nitro.tpcds;

import io.trino.testing.MaterializedResult;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;

public final class TpcdsResultAssertions
{
    // Keep in sync with Trino product tests.
    private static final org.assertj.core.data.Percentage DOUBLE_COMPARISON_ACCURACY = withPercentage(1.1E-10);

    private TpcdsResultAssertions() {}

    public static void assertMatches(List<String> expected, MaterializedResult result)
    {
        List<? extends List<?>> actualRows = result.getMaterializedRows().stream()
                .map(row -> row.getFields())
                .toList();

        assertThat(actualRows).hasSize(expected.size());
        for (int rowIndex = 0; rowIndex < expected.size(); rowIndex++) {
            String[] expectedValues = expected.get(rowIndex).split("\\|");
            List<?> actual = actualRows.get(rowIndex);
            assertThat(actual).hasSize(expectedValues.length);
            for (int fieldIndex = 0; fieldIndex < expectedValues.length; fieldIndex++) {
                assertValueMatches(expectedValues[fieldIndex], actual.get(fieldIndex));
            }
        }
    }

    private static void assertValueMatches(String expectedValue, Object actualValue)
    {
        if (actualValue instanceof Double doubleValue) {
            BigDecimal expectedDecimal = new BigDecimal(trimIfNeeded(expectedValue));
            BigDecimal actualDecimal = BigDecimal.valueOf(doubleValue).setScale(expectedDecimal.scale(), RoundingMode.HALF_DOWN);
            assertThat(expectedDecimal).isCloseTo(actualDecimal, DOUBLE_COMPARISON_ACCURACY);
            return;
        }

        if (actualValue instanceof BigDecimal) {
            assertThat(trimIfNeeded(Objects.toString(actualValue))).isEqualTo(trimIfNeeded(expectedValue));
            return;
        }

        assertThat(Objects.toString(actualValue)).isEqualTo(expectedValue);
    }

    private static String trimIfNeeded(String value)
    {
        if (!value.contains(".")) {
            return value;
        }

        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '0') {
            end--;
        }
        if (end > 0 && value.charAt(end - 1) == '.') {
            end--;
        }
        return value.substring(0, end);
    }
}
