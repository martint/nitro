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
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;

import java.nio.file.Path;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

public class TestRealData
{
    private static final String RUN_ACTUAL_TESTS_PROPERTY = "nitro.clickbench.runActualDataTests";

    private Path actualHitsDirectory()
    {
        assumeTrue(actualDataTestsEnabled(), "Set -D" + RUN_ACTUAL_TESTS_PROPERTY + "=false to skip ClickBench tests against the split real hits parquet data");
        var directory = ClickBenchHitsSupport.actualHitsDirectoryIfPresent();
        assumeTrue(directory.isPresent(), "Set -D" + ClickBenchHitsSupport.CLICKBENCH_HITS_PATH_PROPERTY + "=/path/to/clickbench or place the split files at ~/tmp/clickbench");
        return directory.orElseThrow();
    }

    private static boolean actualDataTestsEnabled()
    {
        return !"false".equalsIgnoreCase(System.getProperty(RUN_ACTUAL_TESTS_PROPERTY));
    }

    @Test
    void testQuery01()
    {
        try (Operator query = ClickBenchHitsSupport.query01(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            assertThat(operator(query)).matchesExactly(List.of(row(99_997_497L)));
        }
    }

    @Test
    void testQuery00()
    {
        try (Operator query = ClickBenchHitsSupport.query00(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            assertThat(query.outputCount()).isEqualTo(6);
            boolean sawBatch = false;
            long totalRows = 0;
            while (query.hasNext()) {
                Batch batch = query.next();
                var mask = batch.borrowMask();
                assertThat(mask.none()).isFalse();

                totalRows += mask.selectedCount();
                if (!sawBatch) {
                    int firstPosition = mask.position(0);
                    assertThat(baseVector(batch.output(0).borrow(Stream.VALUES))).isInstanceOf(I32Vector.class);
                    assertThat(baseVector(batch.output(1).borrow(Stream.VALUES))).isInstanceOf(I32Vector.class);
                    assertThat(baseVector(batch.output(2).borrow(Stream.VALUES))).isInstanceOf(I64Vector.class);
                    assertThat(baseVector(batch.output(3).borrow(Stream.VALUES))).isInstanceOf(I32Vector.class);
                    assertThat(baseVector(batch.output(4).borrow(Stream.VALUES))).isInstanceOf(BinaryVector.class);
                    assertThat(baseVector(batch.output(5).borrow(Stream.VALUES))).isInstanceOf(BinaryVector.class);

                    integerValue(batch.output(0).borrow(Stream.VALUES), firstPosition);
                    integerValue(batch.output(1).borrow(Stream.VALUES), firstPosition);
                    integerValue(batch.output(2).borrow(Stream.VALUES), firstPosition);
                    assertThat(integerValue(batch.output(3).borrow(Stream.VALUES), firstPosition)).isPositive();
                    assertThat(binaryValue(batch.output(4).borrow(Stream.VALUES), firstPosition)).isNotNull();
                    assertThat(binaryValue(batch.output(5).borrow(Stream.VALUES), firstPosition)).isNotNull();
                    sawBatch = true;
                }

                for (int column = 0; column < query.outputCount(); column++) {
                    batch.output(column).borrow(Stream.VALUES);
                }
            }
            assertThat(sawBatch).isTrue();
            assertThat(totalRows).isEqualTo(99_997_497L);
        }
    }

    @Test
    void testQuery02()
    {
        try (Operator query = ClickBenchHitsSupport.query02(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            assertThat(operator(query)).matchesExactly(List.of(row(630_500L)));
        }
    }

    @Test
    void testQuery07()
    {
        try (Operator query = ClickBenchHitsSupport.query07(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            assertThat(operator(query)).matchesExactly(List.of(row(15_888L, 15_917L)));
        }
    }

    @Test
    void testQuery03()
    {
        try (Operator query = ClickBenchHitsSupport.query03(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).matches(rowsList -> rowsList.size() == 1);
            assertThat(rows.getFirst().values()).hasSize(3);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isEqualTo(99_997_497L);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(Double.class);
        }
    }

    @Test
    void testQuery04()
    {
        try (Operator query = ClickBenchHitsSupport.query04(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Double.class);
        }
    }

    @Test
    void testQuery05()
    {
        try (Operator query = ClickBenchHitsSupport.query05(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat((Long) rows.getFirst().values()[0]).isPositive();
        }
    }

    @Test
    void testQuery06()
    {
        try (Operator query = ClickBenchHitsSupport.query06(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat((Long) rows.getFirst().values()[0]).isPositive();
        }
    }

    @Test
    void testQuery08()
    {
        try (Operator query = ClickBenchHitsSupport.query08(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(2L, 404_602L),
                    row(27L, 113_167L),
                    row(13L, 45_631L),
                    row(45L, 38_960L),
                    row(44L, 9_730L),
                    row(3L, 6_896L),
                    row(62L, 5_266L),
                    row(52L, 3_554L),
                    row(50L, 938L),
                    row(28L, 836L)));
        }
    }

    @Test
    void testQuery09()
    {
        try (Operator query = ClickBenchHitsSupport.query09(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery10()
    {
        try (Operator query = ClickBenchHitsSupport.query10(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[3]).isInstanceOf(Double.class);
            assertThat(rows.getFirst().values()[4]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery11()
    {
        try (Operator query = ClickBenchHitsSupport.query11(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery12()
    {
        try (Operator query = ClickBenchHitsSupport.query12(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery13()
    {
        try (Operator query = ClickBenchHitsSupport.query13(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery14()
    {
        try (Operator query = ClickBenchHitsSupport.query14(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery15()
    {
        try (Operator query = ClickBenchHitsSupport.query15(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery16()
    {
        try (Operator query = ClickBenchHitsSupport.query16(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery17()
    {
        try (Operator query = ClickBenchHitsSupport.query17(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery18()
    {
        try (Operator query = ClickBenchHitsSupport.query18(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery19()
    {
        try (Operator query = ClickBenchHitsSupport.query19(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(4);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[3]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery20()
    {
        try (Operator query = ClickBenchHitsSupport.query20(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[0]).isEqualTo(ClickBenchHitsSupport.QUERY20_USER_ID);
        }
    }

    @Test
    void testQuery25()
    {
        try (Operator query = ClickBenchHitsSupport.query25(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
        }
    }

    @Test
    void testQuery26()
    {
        try (Operator query = ClickBenchHitsSupport.query26(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
        }
    }

    @Test
    void testQuery27()
    {
        try (Operator query = ClickBenchHitsSupport.query27(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
        }
    }

    @Test
    void testQuery30()
    {
        try (Operator query = ClickBenchHitsSupport.query30(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()).hasSize(90);
            for (Object value : rows.getFirst().values()) {
                assertThat(value).isInstanceOf(Long.class);
            }
        }
    }

    @Test
    void testQuery34()
    {
        try (Operator query = ClickBenchHitsSupport.query34(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery35()
    {
        try (Operator query = ClickBenchHitsSupport.query35(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
            assertThat(rows.getFirst().values()[0]).isEqualTo(1L);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery21()
    {
        try (Operator query = ClickBenchHitsSupport.query21(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat((Long) rows.getFirst().values()[0]).isPositive();
        }
    }

    @Test
    void testQuery22()
    {
        try (Operator query = ClickBenchHitsSupport.query22(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
        }
    }

    @Test
    void testQuery23()
    {
        try (Operator query = ClickBenchHitsSupport.query23(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testQuery24()
    {
        try (Operator query = ClickBenchHitsSupport.query24(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(10);
            assertThat(rows.getFirst().values().length).isGreaterThan(20);
        }
    }

    @Test
    void testQuery28()
    {
        try (Operator query = ClickBenchHitsSupport.query28(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
        }
    }

    @Test
    void testQuery29()
    {
        try (Operator query = ClickBenchHitsSupport.query29(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(4);
        }
    }

    @Test
    void testQuery31()
    {
        try (Operator query = ClickBenchHitsSupport.query31(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testQuery32()
    {
        try (Operator query = ClickBenchHitsSupport.query32(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testQuery33()
    {
        try (Operator query = ClickBenchHitsSupport.query33(new Allocator(EngineResources.createDefault()), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testQuery36()
    {
        try (Operator query = ClickBenchHitsSupport.query36(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testQuery37()
    {
        try (Operator query = ClickBenchHitsSupport.query37(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
        }
    }

    @Test
    void testQuery38()
    {
        try (Operator query = ClickBenchHitsSupport.query38(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
        }
    }

    @Test
    void testQuery39()
    {
        try (Operator query = ClickBenchHitsSupport.query39(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
        }
    }

    @Test
    void testQuery40()
    {
        try (Operator query = ClickBenchHitsSupport.query40(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(6);
        }
    }

    @Test
    void testQuery41()
    {
        try (Operator query = ClickBenchHitsSupport.query41(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
        }
    }

    @Test
    void testQuery42()
    {
        try (Operator query = ClickBenchHitsSupport.query42(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
        }
    }

    @Test
    void testQuery43()
    {
        try (Operator query = ClickBenchHitsSupport.query43(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
        }
    }

    private static long integerValue(Object vector, int position)
    {
        return switch (vector) {
            case I32Vector typed -> typed.values()[position];
            case I64Vector typed -> typed.values()[position];
            case DictionaryVector typed -> integerValue(typed.values(), typed.ids()[position]);
            case RleVector typed -> integerValue(typed.values(), typed.runIndex(position));
            default -> throw new AssertionError("Expected integer vector but got " + vector.getClass().getSimpleName());
        };
    }

    private static String binaryValue(Object vector, int position)
    {
        return switch (vector) {
            case BinaryVector typed -> new String(typed.copyBytes(position), UTF_8);
            case DictionaryVector typed -> binaryValue(typed.values(), typed.ids()[position]);
            case RleVector typed -> binaryValue(typed.values(), typed.runIndex(position));
            default -> throw new AssertionError("Expected binary vector but got " + vector.getClass().getSimpleName());
        };
    }

    private static Vector baseVector(Object vector)
    {
        return switch ((Vector) vector) {
            case DictionaryVector typed -> baseVector(typed.values());
            case RleVector typed -> baseVector(typed.values());
            case Vector typed -> typed;
        };
    }
}
