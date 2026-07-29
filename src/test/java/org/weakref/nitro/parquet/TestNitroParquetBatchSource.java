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

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

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

    @Test
    void testSplitOwnsRowGroupByFirstPhysicalColumnStart()
    {
        RowGroup plain = rowGroup(100, 0);
        assertTrue(ParquetFile.splitContainsRowGroup(plain, 100, 200));
        assertFalse(ParquetFile.splitContainsRowGroup(plain, 0, 100));
        assertFalse(ParquetFile.splitContainsRowGroup(plain, 101, 200));

        RowGroup dictionary = rowGroup(150, 90);
        assertTrue(ParquetFile.splitContainsRowGroup(dictionary, 0, 100));
        assertFalse(ParquetFile.splitContainsRowGroup(dictionary, 100, 200));
    }

    private static boolean admits(int selected, int total, int scanColumns, int payloadColumns)
    {
        return ParquetLateMaterializationPolicy.defaults()
                .skipDecode()
                .fragmentedNumeric()
                .admits(selected, total, scanColumns, payloadColumns);
    }

    private static RowGroup rowGroup(long dataOffset, long dictionaryOffset)
    {
        ColumnMetaData metadata = new ColumnMetaData(
                Type.INT64,
                List.of(Encoding.PLAIN),
                List.of("value"),
                CompressionCodec.UNCOMPRESSED,
                1,
                8,
                8,
                dataOffset);
        metadata.setDictionary_page_offset(dictionaryOffset);
        ColumnChunk column = new ColumnChunk();
        column.setMeta_data(metadata);
        return new RowGroup(List.of(column), 8, 1);
    }
}
