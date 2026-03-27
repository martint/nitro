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
package org.weakref.trino.clickbench;

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.weakref.nitro.clickbench.ClickBenchHitsSupport;
import org.weakref.nitro.trino.TrinoClickBenchSupport;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestQueries
{
    @TempDir
    Path tempDirectory;

    private Path writeHitsFixture()
            throws IOException
    {
        return ClickBenchHitsSupport.writeHitsFixture(tempDirectory.resolve("clickbench-hits.parquet"), ClickBenchHitsSupport.DEFAULT_ROW_COUNT);
    }

    @Test
    void testQuery01()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query01(writeHitsFixture());
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getOnlyValue()).isEqualTo(8L);
        }
    }

    @Test
    void testQuery03()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query03(writeHitsFixture());
            assertThat(result.getMaterializedRows()).singleElement().satisfies(row -> {
                assertThat(row.getFields()).containsExactly(80L, 8L, 792.5);
            });
        }
    }

    @Test
    void testQuery07()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query07(writeHitsFixture());
            assertThat(result.getMaterializedRows()).singleElement().satisfies(row -> {
                assertThat(row.getFields()).containsExactly(20130701, 20130801);
            });
        }
    }

    @Test
    void testQuery08()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query08(writeHitsFixture());
            assertThat(result.getMaterializedRows()).hasSize(2);
            assertThat(result.getMaterializedRows().getFirst().getFields()).isEqualTo(List.of(20, 3L));
            assertThat(result.getMaterializedRows().get(1).getFields()).isEqualTo(List.of(10, 2L));
        }
    }

    @Test
    void testQuery13()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query13(writeHitsFixture());
            assertThat(result.getRowCount()).isEqualTo(4);
            assertThat(result.getMaterializedRows().getFirst().getFields()).isEqualTo(List.of("news", 2L));
        }
    }

    @Test
    void testQuery34()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query34(writeHitsFixture());
            assertThat(result.getRowCount()).isEqualTo(7);
            assertThat(result.getMaterializedRows().getFirst().getFields()).isEqualTo(List.of("https://google.com/search", 2L));
        }
    }

    @Test
    void testQuery21()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query21(writeHitsFixture());
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getOnlyValue()).isEqualTo(4L);
        }
    }

    @Test
    void testQuery22()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query22(writeHitsFixture());
            assertThat(result.getMaterializedRows()).isNotEmpty();
            assertThat(result.getMaterializedRows().getFirst().getFields()).hasSize(3);
        }
    }

    @Test
    void testQuery23()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query23(writeHitsFixture());
            assertThat(result.getMaterializedRows()).isNotEmpty();
            assertThat(result.getMaterializedRows().getFirst().getFields()).hasSize(5);
        }
    }

    @Test
    void testQuery24()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query24(writeHitsFixture());
            assertThat(result.getMaterializedRows()).isNotEmpty();
            assertThat(result.getMaterializedRows().getFirst().getFields().size()).isGreaterThan(20);
        }
    }

    @Test
    void testQuery29()
            throws IOException
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query29(writeHitsFixture());
            assertThat(result.getRowCount()).isZero();
        }
    }
}
