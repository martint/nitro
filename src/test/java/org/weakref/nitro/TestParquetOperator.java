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
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.ParquetScanOperator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.First;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
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

    @Test
    void testParquetScanReadsVariableWidthColumns()
            throws IOException
    {
        java.nio.file.Path file = writeBinaryParquetFile("binary.parquet", false, List.of(
                new BinaryParquetRow("alice", bytes(1, 2, 3)),
                new BinaryParquetRow("bob", null),
                new BinaryParquetRow("charlie", bytes(4, 5))));

        try (ParquetScanOperator operator = new ParquetScanOperator(new Allocator(), file, List.of("name", "payload"))) {
            Batch batch = operator.next();
            BinaryVector names = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
            BinaryVector payloads = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
            BooleanVector payloadNulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(names.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
            assertThat(names.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isTrue();
            assertThat(payloads.traits()).isEmpty();
            assertThat(names.utf8Value(0)).isEqualTo("alice");
            assertThat(names.utf8Value(1)).isEqualTo("bob");
            assertThat(names.utf8Value(2)).isEqualTo("charlie");

            assertThat(payloadNulls.values()[0]).isFalse();
            assertThat(payloadNulls.values()[1]).isTrue();
            assertThat(payloadNulls.values()[2]).isFalse();
            assertThat(payloads.copyBytes(0)).containsExactly((byte) 1, (byte) 2, (byte) 3);
            assertThat(payloads.copyBytes(2)).containsExactly((byte) 4, (byte) 5);
        }
    }

    @Test
    void testParquetScanReadsRepeatedI64Columns()
            throws IOException
    {
        java.nio.file.Path file = writeRepeatedI64ParquetFile("arrays.parquet", List.of(
                new ArrayParquetRow(List.of(10L, 20L)),
                new ArrayParquetRow(List.of()),
                new ArrayParquetRow(List.of(30L)),
                new ArrayParquetRow(List.of(40L, 50L, 60L))));

        try (ParquetScanOperator operator = new ParquetScanOperator(new Allocator(), file, List.of("items"))) {
            Batch batch = operator.next();
            ArrayVector arrays = (ArrayVector) batch.output(0).borrow(Stream.VALUES);
            I64Vector elements = (I64Vector) arrays.elementValues();

            assertThat(arrays.length(0)).isEqualTo(2);
            assertThat(arrays.length(1)).isEqualTo(0);
            assertThat(arrays.length(2)).isEqualTo(1);
            assertThat(arrays.length(3)).isEqualTo(3);
            assertThat(elements.values()).startsWith(10L, 20L, 30L, 40L, 50L, 60L);
        }
    }

    @Test
    void testParquetScanPreservesDictionaryEncodingForStrings()
            throws IOException
    {
        java.nio.file.Path file = writeBinaryParquetFile("dictionary-strings.parquet", true, List.of(
                new BinaryParquetRow("alpha", bytes(9)),
                new BinaryParquetRow("beta", bytes(8)),
                new BinaryParquetRow("alpha", null),
                new BinaryParquetRow("beta", bytes(7))));

        assertDictionaryEncoding(file, "name");

        try (ParquetScanOperator operator = new ParquetScanOperator(new Allocator(), file, List.of("name", "payload"))) {
            Batch batch = operator.next();
            assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(DictionaryVector.class);
            DictionaryVector names = (DictionaryVector) batch.output(0).borrow(Stream.VALUES);
            assertThat(names.values()).isInstanceOf(BinaryVector.class);
            BinaryVector dictionaryValues = (BinaryVector) names.values();
            assertThat(dictionaryValues.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
            assertThat(dictionaryValues.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isTrue();
            assertThat(dictionaryValues.utf8Value(names.ids()[0])).isEqualTo("alpha");
            assertThat(dictionaryValues.utf8Value(names.ids()[1])).isEqualTo("beta");
        }
    }

    @Test
    void testEqualUtf8PropagatesNullsOnAsciiStrings()
            throws IOException
    {
        java.nio.file.Path file = writeUtf8PairParquetFile("ascii-pairs.parquet", true, List.of(
                new Utf8PairRow("alpha", "alpha"),
                new Utf8PairRow("beta", null),
                new Utf8PairRow("gamma", "delta")));

        try (ParquetScanOperator operator = new ParquetScanOperator(new Allocator(), file, List.of("left_name", "right_name"))) {
            Batch batch = operator.next();
            BinaryVector left = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
            BinaryVector right = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
            BooleanVector rightNulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(left.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
            assertThat(left.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isTrue();
            assertThat(right.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
            assertThat(right.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isTrue();

            Streams result = eqUtf8().apply(
                    List.of(
                            Streams.ofValues(left),
                            Streams.ofValues(right).with(Stream.NULLS, rightNulls)),
                    batch.borrowMask(),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(new Allocator()));
            BooleanVector values = (BooleanVector) result.get(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);

            assertThat(values.values()[0]).isTrue();
            assertThat(values.values()[1]).isFalse();
            assertThat(values.values()[2]).isFalse();
            assertThat(nulls.values()[0]).isFalse();
            assertThat(nulls.values()[1]).isTrue();
            assertThat(nulls.values()[2]).isFalse();
        }
    }

    @Test
    void testEqualUtf8SupportsNonAsciiStrings()
            throws IOException
    {
        java.nio.file.Path file = writeUtf8PairParquetFile("unicode-pairs.parquet", true, List.of(
                new Utf8PairRow("élan", "élan"),
                new Utf8PairRow("élan", "été")));

        try (ParquetScanOperator operator = new ParquetScanOperator(new Allocator(), file, List.of("left_name", "right_name"))) {
            Batch batch = operator.next();
            var leftValues = batch.output(0).borrow(Stream.VALUES);
            var rightValues = batch.output(1).borrow(Stream.VALUES);
            BinaryVector left = binaryValues(leftValues);
            BinaryVector right = binaryValues(rightValues);

            assertThat(left.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
            assertThat(left.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isFalse();
            assertThat(right.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
            assertThat(right.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isFalse();

            Streams result = eqUtf8().apply(
                    List.of(Streams.ofValues((org.weakref.nitro.data.Vector) leftValues), Streams.ofValues((org.weakref.nitro.data.Vector) rightValues)),
                    batch.borrowMask(),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(new Allocator()));
            BooleanVector values = (BooleanVector) result.get(Stream.VALUES);

            assertThat(values.values()[0]).isTrue();
            assertThat(values.values()[1]).isFalse();
        }
    }

    @Test
    void testLessThanUtf8FiltersAsciiParquetStrings()
            throws IOException
    {
        java.nio.file.Path file = writeUtf8PairParquetFile("ascii-less-than.parquet", true, List.of(
                new Utf8PairRow("apple", "banana"),
                new Utf8PairRow("mango", "banana"),
                new Utf8PairRow("banana", null),
                new Utf8PairRow("alpha", "beta")));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable predicate = new Variable(0);
        EvaluationPlan filterPlan = new EvaluationPlan(
                List.of(new Assignment(
                        predicate,
                        new Call("lt_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of());

        try (FilterOperator operator = new FilterOperator(
                new ParquetScanOperator(new Allocator(), file, List.of("left_name", "right_name")),
                filterPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                new Allocator())) {
            Batch batch = operator.next();
            assertThat(batch.borrowMask()).containsExactly(0, 3);
            var leftValues = batch.output(0).borrow(Stream.VALUES);
            assertThat(utf8Value(leftValues, 0)).isEqualTo("apple");
            assertThat(utf8Value(leftValues, 3)).isEqualTo("alpha");
        }
    }

    @Test
    void testStartsWithUtf8ProjectsValuesAndNulls()
            throws IOException
    {
        java.nio.file.Path file = writeUtf8PairParquetFile("unicode-prefix.parquet", true, List.of(
                new Utf8PairRow("élan", "é"),
                new Utf8PairRow("été", "él"),
                new Utf8PairRow("ascii", "as"),
                new Utf8PairRow("beta", null)));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable startsWith = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        startsWith,
                        new Call("starts_with_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(startsWith, Stream.VALUES),
                        new Reference(startsWith, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("left_name", "right_name")))) {
            Batch batch = operator.next();
            BooleanVector values = (BooleanVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()[0]).isTrue();
            assertThat(values.values()[1]).isFalse();
            assertThat(values.values()[2]).isTrue();
            assertThat(values.values()[3]).isFalse();
            assertThat(nulls.values()[0]).isFalse();
            assertThat(nulls.values()[1]).isFalse();
            assertThat(nulls.values()[2]).isFalse();
            assertThat(nulls.values()[3]).isTrue();
        }
    }

    @Test
    void testHashUtf8ProjectsValuesAndNulls()
            throws IOException
    {
        java.nio.file.Path file = writeUtf8PairParquetFile("unicode-hash.parquet", true, List.of(
                new Utf8PairRow("ignored", "alpha"),
                new Utf8PairRow("ignored", null),
                new Utf8PairRow("ignored", "élan")));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable hash = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        hash,
                        new Call("hash_utf8", List.of(new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(hash, Stream.VALUES),
                        new Reference(hash, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("left_name", "right_name")))) {
            Batch batch = operator.next();
            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()[0]).isEqualTo(expectedUtf8Hash("alpha"));
            assertThat(values.values()[1]).isEqualTo(0);
            assertThat(values.values()[2]).isEqualTo(expectedUtf8Hash("élan"));
            assertThat(nulls.values()[0]).isFalse();
            assertThat(nulls.values()[1]).isTrue();
            assertThat(nulls.values()[2]).isFalse();
        }
    }

    @Test
    void testHashUtf8FeedsGrouping()
            throws IOException
    {
        java.nio.file.Path file = writeBinaryParquetFile("grouped-hash.parquet", true, List.of(
                new BinaryParquetRow("alpha", bytes(1)),
                new BinaryParquetRow("beta", bytes(2)),
                new BinaryParquetRow("alpha", bytes(3)),
                new BinaryParquetRow("gamma", bytes(4)),
                new BinaryParquetRow("beta", bytes(5))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable hash = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        hash,
                        new Call("hash_utf8", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(hash, Stream.VALUES),
                        new Reference(hash, Stream.VALUES)));

        assertThat(operator(
                new GroupedAggregationOperator(
                        new Allocator(),
                        0,
                        List.of(
                                new First(1),
                                new CountAll()),
                        new GroupOperator(
                                new Allocator(),
                                0,
                                new ProjectOperator(
                                        new Allocator(),
                                        projectionPlan,
                                        primitiveRegistry,
                                        new ParquetScanOperator(new Allocator(), file, List.of("name")))))))
                .matchesExactly(List.of(
                        Row.row(expectedUtf8Hash("alpha"), 2L),
                        Row.row(expectedUtf8Hash("beta"), 2L),
                        Row.row(expectedUtf8Hash("gamma"), 1L)));
    }

    @Test
    void testCardinalityProjectsRepeatedI64Columns()
            throws IOException
    {
        java.nio.file.Path file = writeRepeatedI64ParquetFile("array-cardinality.parquet", List.of(
                new ArrayParquetRow(List.of(10L, 20L)),
                new ArrayParquetRow(List.of()),
                new ArrayParquetRow(List.of(30L)),
                new ArrayParquetRow(List.of(40L, 50L, 60L))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable cardinality = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        cardinality,
                        new Call("cardinality", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(cardinality, Stream.VALUES)));

        assertThat(operator(
                new ProjectOperator(
                        new Allocator(),
                        projectionPlan,
                        primitiveRegistry,
                        new ParquetScanOperator(new Allocator(), file, List.of("items")))))
                .matchesExactly(List.of(
                        Row.row(2L),
                        Row.row(0L),
                        Row.row(1L),
                        Row.row(3L)));
    }

    @Test
    void testCardinalityFeedsGrouping()
            throws IOException
    {
        java.nio.file.Path file = writeRepeatedI64ParquetFile("array-grouping.parquet", List.of(
                new ArrayParquetRow(List.of(10L)),
                new ArrayParquetRow(List.of()),
                new ArrayParquetRow(List.of(20L, 30L)),
                new ArrayParquetRow(List.of(40L)),
                new ArrayParquetRow(List.of())));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable cardinality = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        cardinality,
                        new Call("cardinality", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(cardinality, Stream.VALUES),
                        new Reference(cardinality, Stream.VALUES)));

        assertThat(operator(
                new GroupedAggregationOperator(
                        new Allocator(),
                        0,
                        List.of(
                                new First(1),
                                new CountAll()),
                        new GroupOperator(
                                new Allocator(),
                                0,
                                new ProjectOperator(
                                        new Allocator(),
                                        projectionPlan,
                                        primitiveRegistry,
                                        new ParquetScanOperator(new Allocator(), file, List.of("items")))))))
                .matchesExactly(List.of(
                        Row.row(1L, 2L),
                        Row.row(0L, 2L),
                        Row.row(2L, 1L)));
    }

    @Test
    void testParquetScanReadsRepeatedNullableI64Elements()
            throws IOException
    {
        java.nio.file.Path file = writeRepeatedNullableI64ParquetFile("nullable-arrays.parquet", List.of(
                new NullableArrayParquetRow(Arrays.asList(10L, null, 20L)),
                new NullableArrayParquetRow(List.of()),
                new NullableArrayParquetRow(Arrays.asList((Long) null)),
                new NullableArrayParquetRow(List.of(30L))));

        try (ParquetScanOperator operator = new ParquetScanOperator(new Allocator(), file, List.of("items"))) {
            Batch batch = operator.next();
            ArrayVector arrays = (ArrayVector) batch.output(0).borrow(Stream.VALUES);
            I64Vector elements = (I64Vector) arrays.elementValues();
            BooleanVector elementNulls = arrays.elementNulls();

            assertThat(arrays.length(0)).isEqualTo(3);
            assertThat(arrays.length(1)).isEqualTo(0);
            assertThat(arrays.length(2)).isEqualTo(1);
            assertThat(arrays.length(3)).isEqualTo(1);
            assertThat(elements.values()).startsWith(10L, 0L, 20L, 0L, 30L);
            assertThat(elementNulls.values()).startsWith(false, true, false, true, false);
        }
    }

    @Test
    void testArraySumProjectsNullableElements()
            throws IOException
    {
        java.nio.file.Path file = writeRepeatedNullableI64ParquetFile("array-sum.parquet", List.of(
                new NullableArrayParquetRow(Arrays.asList(10L, null, 20L)),
                new NullableArrayParquetRow(List.of()),
                new NullableArrayParquetRow(Arrays.asList((Long) null)),
                new NullableArrayParquetRow(List.of(30L, 5L))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable sum = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        sum,
                        new Call("array_sum_i64", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(sum, Stream.VALUES)));

        assertThat(operator(
                new ProjectOperator(
                        new Allocator(),
                        projectionPlan,
                        primitiveRegistry,
                        new ParquetScanOperator(new Allocator(), file, List.of("items")))))
                .matchesExactly(List.of(
                        Row.row(30L),
                        Row.row(0L),
                        Row.row(0L),
                        Row.row(35L)));
    }

    @Test
    void testArraySumFeedsGrouping()
            throws IOException
    {
        java.nio.file.Path file = writeRepeatedNullableI64ParquetFile("array-sum-grouping.parquet", List.of(
                new NullableArrayParquetRow(Arrays.asList(10L, null, 20L)),
                new NullableArrayParquetRow(List.of()),
                new NullableArrayParquetRow(Arrays.asList((Long) null)),
                new NullableArrayParquetRow(List.of(30L)),
                new NullableArrayParquetRow(List.of(5L, 25L))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable sum = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        sum,
                        new Call("array_sum_i64", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(sum, Stream.VALUES),
                        new Reference(sum, Stream.VALUES)));

        assertThat(operator(
                new GroupedAggregationOperator(
                        new Allocator(),
                        0,
                        List.of(
                                new First(1),
                                new CountAll()),
                        new GroupOperator(
                                new Allocator(),
                                0,
                                new ProjectOperator(
                                        new Allocator(),
                                        projectionPlan,
                                        primitiveRegistry,
                                        new ParquetScanOperator(new Allocator(), file, List.of("items")))))))
                .matchesExactly(List.of(
                        Row.row(30L, 3L),
                        Row.row(0L, 2L)));
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

    private java.nio.file.Path writeBinaryParquetFile(String name, boolean dictionaryEnabled, List<BinaryParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .required(BINARY).as(stringType()).named("name")
                .optional(BINARY).named("payload")
                .named("nitro_binary_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withDictionaryEncoding(dictionaryEnabled)
                .build()) {
            for (BinaryParquetRow row : rows) {
                Group group = groups.newGroup()
                        .append("name", row.name());
                if (row.payload() != null) {
                    group.append("payload", org.apache.parquet.io.api.Binary.fromConstantByteArray(row.payload()));
                }
                writer.write(group);
            }
        }
        return file;
    }

    private java.nio.file.Path writeUtf8PairParquetFile(String name, boolean dictionaryEnabled, List<Utf8PairRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .required(BINARY).as(stringType()).named("left_name")
                .optional(BINARY).as(stringType()).named("right_name")
                .named("nitro_utf8_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withDictionaryEncoding(dictionaryEnabled)
                .build()) {
            for (Utf8PairRow row : rows) {
                Group group = groups.newGroup()
                        .append("left_name", row.left());
                if (row.right() != null) {
                    group.append("right_name", row.right());
                }
                writer.write(group);
            }
        }
        return file;
    }

    private java.nio.file.Path writeRepeatedI64ParquetFile(String name, List<ArrayParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .repeated(INT64).named("items")
                .named("nitro_array_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .build()) {
            for (ArrayParquetRow row : rows) {
                Group group = groups.newGroup();
                for (long item : row.items()) {
                    group.append("items", item);
                }
                writer.write(group);
            }
        }
        return file;
    }

    private java.nio.file.Path writeRepeatedNullableI64ParquetFile(String name, List<NullableArrayParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .repeatedGroup()
                    .optional(INT64).named("element")
                .named("items")
                .named("nitro_nullable_array_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .build()) {
            for (NullableArrayParquetRow row : rows) {
                Group group = groups.newGroup();
                for (Long item : row.items()) {
                    Group elementGroup = group.addGroup("items");
                    if (item != null) {
                        elementGroup.append("element", item);
                    }
                }
                writer.write(group);
            }
        }
        return file;
    }

    private static byte[] bytes(int... values)
    {
        byte[] bytes = new byte[values.length];
        for (int index = 0; index < values.length; index++) {
            bytes[index] = (byte) values[index];
        }
        return bytes;
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

    private static PrimitiveFunction eqUtf8()
    {
        return TestPrimitiveFunctions.primitiveRegistry().get("eq_utf8");
    }

    private static long expectedUtf8Hash(String value)
    {
        return value.hashCode();
    }

    private static String utf8Value(org.weakref.nitro.data.Vector values, int position)
    {
        return switch (values) {
            case BinaryVector vector -> vector.utf8Value(position);
            case DictionaryVector vector -> ((BinaryVector) vector.values()).utf8Value(vector.ids()[position]);
            default -> throw new IllegalArgumentException("Expected binary-backed vector but got " + values.getClass().getSimpleName());
        };
    }

    private static BinaryVector binaryValues(org.weakref.nitro.data.Vector values)
    {
        return switch (values) {
            case BinaryVector vector -> vector;
            case DictionaryVector vector -> (BinaryVector) vector.values();
            default -> throw new IllegalArgumentException("Expected binary-backed vector but got " + values.getClass().getSimpleName());
        };
    }

    private record ParquetRow(long x, boolean flag, Long maybe) {}

    private record BinaryParquetRow(String name, byte[] payload) {}

    private record Utf8PairRow(String left, String right) {}

    private record ArrayParquetRow(List<Long> items) {}

    private record NullableArrayParquetRow(List<Long> items) {}
}
