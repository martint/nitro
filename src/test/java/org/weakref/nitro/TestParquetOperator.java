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

import org.apache.parquet.column.Encoding;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.ParquetScanOperator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.io.IOException;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;

public class TestParquetOperator
{
    @TempDir
    java.nio.file.Path tempDirectory;

    @Test
    void testParquetScanReadsPlainColumns()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("plain.parquet", false, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, false, null),
                new ParquetRow(13, true, 103L)));

        try (ParquetScanOperator operator = new ParquetScanOperator(new Allocator(), file, List.of("x", "flag", "maybe"))) {
            assertThat(operator(operator))
                    .matchesExactly(List.of(
                            Row.row(11L, 1L, 101L),
                            Row.row(12L, 0L, null),
                            Row.row(13L, 1L, 103L)));
        }
    }

    @Test
    void testParquetScanFeedsFilterAndProjectOnDictionaryPages()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("dictionary.parquet", true, List.of(
                new ParquetRow(10, true, 100L),
                new ParquetRow(20, false, 200L),
                new ParquetRow(10, true, null),
                new ParquetRow(20, true, 400L)));

        assertDictionaryEncoding(file, "x");

        try (ParquetScanOperator scan = new ParquetScanOperator(new Allocator(), file, List.of("x", "flag", "maybe"))) {
            Batch batch = scan.next();
            assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(DictionaryVector.class);
        }

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable doubled = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        doubled,
                        new Call("multiply", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(doubled, Stream.VALUES),
                        new Reference(new Input(2), Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new FilterOperator(
                        new ParquetScanOperator(new Allocator(), file, List.of("x", "flag", "maybe")),
                        new EvaluationPlan(List.of(), List.of()),
                        primitiveRegistry,
                        new Reference(new Input(1), Stream.VALUES),
                        new Allocator()))) {
            Batch batch = operator.next();
            assertThat(batch.borrowMask()).containsExactly(0, 2, 3);
            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);
            assertThat(values.values()[0]).isEqualTo(100);
            assertThat(values.values()[2]).isEqualTo(100);
            assertThat(values.values()[3]).isEqualTo(400);
            assertThat(nulls.values()[0]).isFalse();
            assertThat(nulls.values()[2]).isTrue();
            assertThat(nulls.values()[3]).isFalse();
        }
    }

    private java.nio.file.Path writeParquetFile(String name, boolean dictionaryEnabled, List<ParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .required(INT64).named("x")
                .required(BOOLEAN).named("flag")
                .optional(INT64).named("maybe")
                .named("nitro_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withDictionaryEncoding(dictionaryEnabled)
                .build()) {
            for (ParquetRow row : rows) {
                Group group = groups.newGroup()
                        .append("x", row.x())
                        .append("flag", row.flag());
                if (row.maybe() != null) {
                    group.append("maybe", row.maybe());
                }
                writer.write(group);
            }
        }
        return file;
    }

    private static void assertDictionaryEncoding(java.nio.file.Path file, String columnName)
            throws IOException
    {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            ParquetMetadata metadata = reader.getFooter();
            assertThat(metadata.getBlocks()).isNotEmpty();
            assertThat(metadata.getBlocks().getFirst().getColumns().stream()
                    .filter(column -> column.getPath().toDotString().equals(columnName))
                    .flatMap(column -> column.getEncodings().stream()))
                    .anyMatch(Encoding::usesDictionary);
        }
    }

    private record ParquetRow(long x, boolean flag, Long maybe) {}
}
