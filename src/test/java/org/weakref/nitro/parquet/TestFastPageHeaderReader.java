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

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFastPageHeaderReader
{
    @Test
    public void testDataPageWithOptionalFields()
            throws IOException
    {
        Statistics statistics = new Statistics()
                .setMax(new byte[] {9, 8, 7})
                .setMin(new byte[] {1, 2, 3})
                .setNull_count(37)
                .setDistinct_count(911)
                .setMax_value(new byte[] {6, 5, 4})
                .setMin_value(new byte[] {3, 2, 1})
                .setIs_max_value_exact(true)
                .setIs_min_value_exact(false);
        PageHeader header = new PageHeader(PageType.DATA_PAGE, 1_234_567, 765_432)
                .setCrc(0x1234_5678)
                .setData_page_header(new DataPageHeader(345_678, Encoding.RLE_DICTIONARY, Encoding.RLE, Encoding.RLE)
                        .setStatistics(statistics));

        assertHeader(header, 345_678, Encoding.RLE_DICTIONARY);
    }

    @Test
    public void testDictionaryPageWithBooleanField()
            throws IOException
    {
        PageHeader header = new PageHeader(PageType.DICTIONARY_PAGE, 98_765, 54_321)
                .setDictionary_page_header(new DictionaryPageHeader(12_345, Encoding.PLAIN).setIs_sorted(true));

        assertHeader(header, 12_345, Encoding.PLAIN);
    }

    private static void assertHeader(PageHeader expected, int valueCount, Encoding encoding)
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Util.writePageHeader(expected, output);
        byte[] bytes = output.toByteArray();

        FastPageHeaderReader reader = new FastPageHeaderReader();
        long consumed = reader.read(MemorySegment.ofArray(bytes), 0, bytes.length);

        assertThat(consumed).isEqualTo(bytes.length);
        assertThat(reader.type()).isEqualTo(expected.type.getValue());
        assertThat(reader.uncompressedSize()).isEqualTo(expected.uncompressed_page_size);
        assertThat(reader.compressedSize()).isEqualTo(expected.compressed_page_size);
        assertThat(reader.valueCount()).isEqualTo(valueCount);
        assertThat(reader.encoding()).isEqualTo(encoding.getValue());
    }
}
