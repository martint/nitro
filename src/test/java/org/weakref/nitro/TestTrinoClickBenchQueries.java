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

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.weakref.nitro.trino.TrinoClickBenchSupport;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoClickBenchQueries
{
    @TempDir
    Path tempDirectory;

    private Path writeHitsFixture()
            throws IOException
    {
        return ClickBenchHitsSupport.writeHitsFixture(tempDirectory.resolve("clickbench-hits.parquet"), ClickBenchHitsSupport.DEFAULT_ROW_COUNT);
    }

    @Test
    void testTrinoQuery1CountAll()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query1CountAll(writeHitsFixture());
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getOnlyValue()).isEqualTo(8L);
        }
    }

    @Test
    void testTrinoQuery3SumAdvEngineAndAvgResolutionWidth()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query3SumAdvEngineAndAvgResolutionWidth(writeHitsFixture());
            assertThat(result.getMaterializedRows()).singleElement().satisfies(row -> {
                assertThat(row.getFields()).containsExactly(80L, 8L, 792.5);
            });
        }
    }

    @Test
    void testTrinoQuery7MinAndMaxEventDate()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query7MinAndMaxEventDate(writeHitsFixture());
            assertThat(result.getMaterializedRows()).singleElement().satisfies(row -> {
                assertThat(row.getFields()).containsExactly(20130701, 20130801);
            });
        }
    }

    @Test
    void testTrinoQuery8GroupByAdvEngineId()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query8GroupByAdvEngineId(writeHitsFixture());
            assertThat(result.getMaterializedRows()).hasSize(2);
            assertThat(result.getMaterializedRows().getFirst().getFields()).isEqualTo(List.of(20, 3L));
            assertThat(result.getMaterializedRows().get(1).getFields()).isEqualTo(List.of(10, 2L));
        }
    }

    @Test
    void testTrinoQuery13TopSearchPhrases()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query13TopSearchPhrases(writeHitsFixture());
            assertThat(result.getRowCount()).isEqualTo(4);
            assertThat(result.getMaterializedRows().getFirst().getFields()).isEqualTo(List.of("news", 2L));
        }
    }

    @Test
    void testTrinoQuery34TopUrls()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query34TopUrls(writeHitsFixture());
            assertThat(result.getRowCount()).isEqualTo(7);
            assertThat(result.getMaterializedRows().getFirst().getFields()).isEqualTo(List.of("https://google.com/search", 2L));
        }
    }

    @Test
    void testTrinoQuery29RefererHosts()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query29RefererHosts(writeHitsFixture());
            assertThat(result.getRowCount()).isZero();
        }
    }
}
