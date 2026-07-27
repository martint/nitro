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
package org.weakref.nitro.parquet;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestNitroParquetBatchSource
{
    @Test
    void testFragmentedNumericSkipAdmissionBoundaries()
    {
        assertTrue(admits(600, 10_000, 6, 2));
        assertTrue(admits(600, 10_000, 6, 8));

        assertFalse(admits(601, 10_000, 6, 2));
        assertFalse(admits(600, 10_000, 7, 2));
        assertFalse(admits(600, 10_000, 6, 1));
        assertFalse(admits(600, 10_000, 6, 9));
        assertFalse(admits(0, 0, 6, 2));
        assertFalse(new ParquetLateMaterializationPolicy.FragmentedNumeric(false, 6, 6, 2, 8)
                .admits(600, 10_000, 6, 2));
    }

    private static boolean admits(int selected, int total, int scanColumns, int payloadColumns)
    {
        return ParquetLateMaterializationPolicy.defaults()
                .skipDecode()
                .fragmentedNumeric()
                .admits(selected, total, scanColumns, payloadColumns);
    }
}
