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
package org.weakref.nitro.clickbench;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

public class TestQueries
{
    @TempDir
    Path tempDirectory;

    private Path writeHitsFixture()
            throws IOException
    {
        return ClickBenchHitsSupport.writeHitsFixture(tempDirectory.resolve("clickbench-hits.parquet"), ClickBenchHitsSupport.DEFAULT_ROW_COUNT);
    }

    private Path writeHitsFixture(int rowCount)
            throws IOException
    {
        return ClickBenchHitsSupport.writeHitsFixture(tempDirectory.resolve("clickbench-hits-" + rowCount + ".parquet"), rowCount);
    }

    private Path writeSplitHitsFixture()
            throws IOException
    {
        Path splitDirectory = tempDirectory.resolve("clickbench-hits-split");
        java.nio.file.Files.createDirectories(splitDirectory);
        ClickBenchHitsSupport.writeHitsFixture(splitDirectory.resolve("hits-part-000.parquet"), ClickBenchHitsSupport.DEFAULT_ROW_COUNT);
        ClickBenchHitsSupport.writeHitsFixture(splitDirectory.resolve("hits-part-001.parquet"), ClickBenchHitsSupport.DEFAULT_ROW_COUNT);
        return splitDirectory;
    }

    @Test
    void testQuery00()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query00(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(0, 1000, 1L, 20130701, "https://google.com", ""),
                    row(10, 1200, 2L, 20130702, "https://example.com", ""),
                    row(10, 900, 2L, 20130703, "https://example.com/page", "phone"),
                    row(20, 800, 1L, 20130701, "https://google.com/maps", "map"),
                    row(20, 700, 2L, 20130731, "https://yandex.ru", "weather"),
                    row(0, 640, 4L, 20130801, "", ""),
                    row(20, 600, 5L, 20130715, "https://google.com/search", "news"),
                    row(0, 500, ClickBenchHitsSupport.QUERY20_USER_ID, 20130716, "https://google.com/search", "news")));
        }
    }

    @Test
    void testQuery01()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query01(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(8L)));
        }
    }

    @Test
    void testQuery02()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query02(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(5L)));
        }
    }

    @Test
    void testQuery07()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query07(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(20130701L, 20130801L)));
        }
    }

    @Test
    void testQuery03()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query03(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(80L, 8L, 792.5)));
        }
    }

    @Test
    void testQuery04()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query04(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(54_386_366_612_455_058.0)));
        }
    }

    @Test
    void testQuery05()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query05(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(5L)));
        }
    }

    @Test
    void testQuery06()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query06(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(5L)));
        }
    }

    @Test
    void testQuery08()
            throws IOException
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        try (Operator query = ClickBenchHitsSupport.query08(allocator, primitiveRegistry, writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(20L, 3L),
                    row(10L, 2L)));
        }
    }

    @Test
    void testQuery09()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query09(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(9L, 4L),
                    row(7L, 2L),
                    row(8L, 1L)));
        }
    }

    @Test
    void testQuery10()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query10(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(9L, 40L, 4L, 610.0, 4L),
                    row(7L, 30L, 3L, 1000.0, 2L),
                    row(8L, 10L, 1L, 900.0, 1L)));
        }
    }

    @Test
    void testQuery11()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query11(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row("pixel", 3L),
                    row("iphone", 2L)));
        }
    }

    @Test
    void testQuery12()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query12(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(3L, "pixel", 3L),
                    row(1L, "iphone", 2L)));
        }
    }

    @Test
    void testQuery13()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query13(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(4);
            assertThat(rows.getFirst()).isEqualTo(row("news", 2L));
        }
    }

    @Test
    void testQuery13ManyGroups()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query13(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture(160))) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery14()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query14(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row("news", 2L),
                    row("phone", 1L),
                    row("map", 1L),
                    row("weather", 1L)));
        }
    }

    @Test
    void testQuery15()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query15(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(3L, "news", 2L),
                    row(1L, "phone", 1L),
                    row(1L, "map", 1L),
                    row(2L, "weather", 1L)));
        }
    }

    @Test
    void testQuery16()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query16(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(5);
            assertThat(rows.getFirst()).isEqualTo(row(2L, 3L));
            assertThat(rows.get(1)).isEqualTo(row(1L, 2L));
        }
    }

    @Test
    void testQuery17()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query17(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(8);
            assertThat(rows.getFirst().values()).hasSize(3);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery18()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query18(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(1L, "", 1L),
                    row(2L, "", 1L),
                    row(2L, "phone", 1L),
                    row(1L, "map", 1L),
                    row(2L, "weather", 1L),
                    row(4L, "", 1L),
                    row(5L, "news", 1L),
                    row(ClickBenchHitsSupport.QUERY20_USER_ID, "news", 1L)));
        }
    }

    @Test
    void testQuery19()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query19(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(8);
            assertThat(rows.getFirst().values()).hasSize(4);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[3]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery20()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query20(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(ClickBenchHitsSupport.QUERY20_USER_ID)));
        }
    }

    @Test
    void testQuery25()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query25(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row("map"),
                    row("phone"),
                    row("news"),
                    row("news"),
                    row("weather")));
        }
    }

    @Test
    void testQuery26()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query26(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row("map"),
                    row("news"),
                    row("news"),
                    row("phone"),
                    row("weather")));
        }
    }

    @Test
    void testQuery27()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query27(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row("map"),
                    row("phone"),
                    row("news"),
                    row("news"),
                    row("weather")));
        }
    }

    @Test
    void testQuery30()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query30(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            Object[] sums = new Object[90];
            for (int offset = 0; offset < sums.length; offset++) {
                sums[offset] = 6_340L + (8L * offset);
            }
            assertThat(operator(query)).matchesExactly(List.of(row(sums)));
        }
    }

    @Test
    void testQuery34()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query34(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(7);
            assertThat(rows.getFirst()).isEqualTo(row("https://google.com/search", 2L));
        }
    }

    @Test
    void testQuery35()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query35(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(7);
            assertThat(rows.getFirst()).isEqualTo(row(1L, "https://google.com/search", 2L));
        }
    }

    @Test
    void testQuery21()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query21(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(4L)));
        }
    }

    @Test
    void testQuery22()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query22(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
        }
    }

    @Test
    void testQuery23()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query23(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testQuery24()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query24(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values().length).isGreaterThan(20);
        }
    }

    @Test
    void testQuery28()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query28(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testQuery29()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query29(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testQuery31()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query31(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testQuery32()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query32(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testQuery33()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query33(new Allocator(EngineResources.createDefault()), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testQuery36()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query36(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testQuery37()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query37(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
        }
    }

    @Test
    void testQuery38()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query38(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
        }
    }

    @Test
    void testQuery39()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query39(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testQuery40()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query40(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testQuery41()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query41(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testQuery42()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query42(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testQuery43()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query43(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testQuery01SplitDirectory()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query01(new Allocator(EngineResources.createDefault()), writeSplitHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(16L)));
        }
    }

    @Test
    void testQuery34SplitDirectory()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query34(new Allocator(EngineResources.createDefault()), writeSplitHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(7);
            assertThat(rows.getFirst()).isEqualTo(row("https://google.com/search", 4L));
        }
    }
}
