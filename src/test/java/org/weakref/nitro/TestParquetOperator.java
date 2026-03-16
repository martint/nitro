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
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.StructVector;
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
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.StructField;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
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
    void testArrayElementProjectsValuesAndNulls()
            throws IOException
    {
        java.nio.file.Path file = writeNullableArrayLookupParquetFile("array-element.parquet", List.of(
                new NullableArrayLookupParquetRow(Arrays.asList(10L, null, 20L), 0L),
                new NullableArrayLookupParquetRow(List.of(), 0L),
                new NullableArrayLookupParquetRow(Arrays.asList((Long) null), 0L),
                new NullableArrayLookupParquetRow(List.of(30L, 5L), 5L),
                new NullableArrayLookupParquetRow(List.of(40L), null),
                new NullableArrayLookupParquetRow(List.of(50L, 60L), 1L)));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable element = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        element,
                        new Call("array_element_i64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(element, Stream.VALUES),
                        new Reference(element, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items", "index")))) {
            Batch batch = operator.next();
            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()).startsWith(10L, 0L, 0L, 0L, 0L, 60L);
            assertThat(nulls.values()).startsWith(false, true, true, true, true, false);
        }
    }

    @Test
    void testArrayContainsI64ProjectsValuesAndNulls()
            throws IOException
    {
        java.nio.file.Path file = writeNullableArrayContainsParquetFile("array-contains.parquet", List.of(
                new NullableArrayContainsParquetRow(Arrays.asList(10L, null, 20L), 20L),
                new NullableArrayContainsParquetRow(List.of(), 0L),
                new NullableArrayContainsParquetRow(Arrays.asList((Long) null), 5L),
                new NullableArrayContainsParquetRow(List.of(30L, 5L), null),
                new NullableArrayContainsParquetRow(List.of(50L, 60L), 7L)));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable contains = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        contains,
                        new Call("array_contains_i64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(contains, Stream.VALUES),
                        new Reference(contains, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items", "needle")))) {
            Batch batch = operator.next();
            BooleanVector values = (BooleanVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()).startsWith(true, false, false, false, false);
            assertThat(nulls.values()).startsWith(false, false, false, true, false);
        }
    }

    @Test
    void testArrayContainsI64FiltersParquetArrays()
            throws IOException
    {
        java.nio.file.Path file = writeNullableArrayContainsParquetFile("array-contains-filter.parquet", List.of(
                new NullableArrayContainsParquetRow(Arrays.asList(10L, null, 20L), 20L),
                new NullableArrayContainsParquetRow(List.of(), 0L),
                new NullableArrayContainsParquetRow(Arrays.asList((Long) null), 5L),
                new NullableArrayContainsParquetRow(List.of(30L, 5L), null),
                new NullableArrayContainsParquetRow(List.of(50L, 60L), 60L)));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable predicate = new Variable(0);
        EvaluationPlan filterPlan = new EvaluationPlan(
                List.of(new Assignment(
                        predicate,
                        new Call("array_contains_i64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of());

        try (FilterOperator operator = new FilterOperator(
                new ParquetScanOperator(new Allocator(), file, List.of("items", "needle")),
                filterPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                new Allocator())) {
            Batch batch = operator.next();
            assertThat(batch.borrowMask()).containsExactly(0, 4);

            I64Vector needles = (I64Vector) batch.output(1).borrow(Stream.VALUES);
            assertThat(needles.values()[0]).isEqualTo(20L);
            assertThat(needles.values()[4]).isEqualTo(60L);
        }
    }

    @Test
    void testParquetScanReadsStructColumns()
            throws IOException
    {
        java.nio.file.Path file = writeStructParquetFile("structs.parquet", List.of(
                new StructParquetRow(11, "alice", true),
                new StructParquetRow(12, null, false),
                new StructParquetRow(13, "carol", null)));

        try (ParquetScanOperator operator = new ParquetScanOperator(new Allocator(), file, List.of("person"))) {
            Batch batch = operator.next();
            StructVector struct = (StructVector) batch.output(0).borrow(Stream.VALUES);

            I64Vector ids = (I64Vector) struct.fieldValues("id");
            BinaryVector names = (BinaryVector) struct.fieldValues("name");
            BooleanVector nameNulls = (BooleanVector) struct.fieldStreamOrNull("name", Stream.NULLS);
            BooleanVector active = (BooleanVector) struct.fieldValues("active");
            BooleanVector activeNulls = (BooleanVector) struct.fieldStreamOrNull("active", Stream.NULLS);

            assertThat(struct.length()).isEqualTo(3);
            assertThat(struct.fieldNames()).containsExactlyInAnyOrder("id", "name", "active");
            assertThat(ids.values()).startsWith(11L, 12L, 13L);

            assertThat(names.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
            assertThat(names.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isTrue();
            assertThat(names.utf8Value(0)).isEqualTo("alice");
            assertThat(names.utf8Value(2)).isEqualTo("carol");
            assertThat(nameNulls.values()).startsWith(false, true, false);

            assertThat(active.values()).startsWith(true, false, false);
            assertThat(activeNulls.values()).startsWith(false, false, true);
        }
    }

    @Test
    void testParquetScanReadsOptionalStructColumns()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalStructParquetFile("optional-structs.parquet", List.of(
                new OptionalStructParquetRow(new StructParquetRow(21, "alpha", true)),
                new OptionalStructParquetRow(null),
                new OptionalStructParquetRow(new StructParquetRow(22, null, false))));

        try (ParquetScanOperator operator = new ParquetScanOperator(new Allocator(), file, List.of("person"))) {
            Batch batch = operator.next();
            StructVector struct = (StructVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector structNulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);

            assertThat(struct.length()).isEqualTo(3);
            assertThat(structNulls.values()).startsWith(false, true, false);

            I64Vector ids = (I64Vector) struct.fieldValues("id");
            BinaryVector names = (BinaryVector) struct.fieldValues("name");
            BooleanVector nameNulls = (BooleanVector) struct.fieldStreamOrNull("name", Stream.NULLS);

            assertThat(ids.values()).startsWith(21L, 0L, 22L);
            assertThat(names.utf8Value(0)).isEqualTo("alpha");
            assertThat(nameNulls.values()).startsWith(false, false, true);
        }
    }

    @Test
    void testParquetScanReadsOptionalMapColumns()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalMapParquetFile("optional-maps.parquet", List.of(
                new MapParquetRow(orderedMap("alpha", 10L, "beta", null)),
                new MapParquetRow(null),
                new MapParquetRow(Map.of()),
                new MapParquetRow(orderedMap("gamma", 30L))));

        try (ParquetScanOperator operator = new ParquetScanOperator(new Allocator(), file, List.of("items"))) {
            Batch batch = operator.next();
            MapVector maps = (MapVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector mapNulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
            BinaryVector keys = (BinaryVector) maps.keyValues();
            I64Vector values = (I64Vector) maps.valueValues();
            BooleanVector valueNulls = (BooleanVector) maps.valueStreamOrNull(Stream.NULLS);

            assertThat(mapNulls.values()).startsWith(false, true, false, false);
            assertThat(maps.length(0)).isEqualTo(2);
            assertThat(maps.length(1)).isEqualTo(0);
            assertThat(maps.length(2)).isEqualTo(0);
            assertThat(maps.length(3)).isEqualTo(1);

            assertThat(keys.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
            assertThat(keys.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isTrue();
            assertThat(keys.utf8Value(0)).isEqualTo("alpha");
            assertThat(keys.utf8Value(1)).isEqualTo("beta");
            assertThat(keys.utf8Value(2)).isEqualTo("gamma");

            assertThat(values.values()).startsWith(10L, 0L, 30L);
            assertThat(valueNulls.values()).startsWith(false, true, false);
        }
    }

    @Test
    void testProjectExtractsOptionalStructField()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalStructParquetFile("project-structs.parquet", List.of(
                new OptionalStructParquetRow(new StructParquetRow(31, "alice", true)),
                new OptionalStructParquetRow(null),
                new OptionalStructParquetRow(new StructParquetRow(32, null, false)),
                new OptionalStructParquetRow(new StructParquetRow(33, "carol", null))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable name = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        name,
                        new StructField(new Reference(new Input(0), Stream.VALUES), "name"),
                        AllMask.ALL)),
                List.of(
                        new Reference(name, Stream.VALUES),
                        new Reference(name, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("person")))) {
            Batch batch = operator.next();
            BinaryVector names = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(names.utf8Value(0)).isEqualTo("alice");
            assertThat(names.utf8Value(3)).isEqualTo("carol");
            assertThat(nulls.values()).startsWith(false, true, true, false);
        }
    }

    @Test
    void testFilterUsesStructBooleanField()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalStructParquetFile("filter-structs.parquet", List.of(
                new OptionalStructParquetRow(new StructParquetRow(41, "alpha", true)),
                new OptionalStructParquetRow(null),
                new OptionalStructParquetRow(new StructParquetRow(42, "beta", false)),
                new OptionalStructParquetRow(new StructParquetRow(43, "gamma", true)),
                new OptionalStructParquetRow(new StructParquetRow(44, "delta", null))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable active = new Variable(0);
        EvaluationPlan filterPlan = new EvaluationPlan(
                List.of(new Assignment(
                        active,
                        new StructField(new Reference(new Input(0), Stream.VALUES), "active"),
                        AllMask.ALL)),
                List.of());

        try (FilterOperator operator = new FilterOperator(
                new ParquetScanOperator(new Allocator(), file, List.of("person")),
                filterPlan,
                primitiveRegistry,
                new Reference(active, Stream.VALUES),
                new Allocator())) {
            Batch batch = operator.next();
            assertThat(batch.borrowMask()).containsExactly(0, 3);

            StructVector people = (StructVector) batch.output(0).borrow(Stream.VALUES);
            I64Vector ids = (I64Vector) people.fieldValues("id");
            assertThat(ids.values()[0]).isEqualTo(41L);
            assertThat(ids.values()[3]).isEqualTo(43L);
        }
    }

    @Test
    void testStructFieldFeedsGrouping()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalStructParquetFile("group-structs.parquet", List.of(
                new OptionalStructParquetRow(new StructParquetRow(51, "a", true)),
                new OptionalStructParquetRow(new StructParquetRow(52, "b", false)),
                new OptionalStructParquetRow(new StructParquetRow(51, "c", true)),
                new OptionalStructParquetRow(null),
                new OptionalStructParquetRow(new StructParquetRow(52, "d", false))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable id = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        id,
                        new StructField(new Reference(new Input(0), Stream.VALUES), "id"),
                        AllMask.ALL)),
                List.of(
                        new Reference(id, Stream.VALUES),
                        new Reference(id, Stream.VALUES)));

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
                                        new FilterOperator(
                                                new ParquetScanOperator(new Allocator(), file, List.of("person")),
                                                new EvaluationPlan(List.of(), List.of()),
                                                primitiveRegistry,
                                                new NotMask(new ReferenceMask(new Reference(new Input(0), Stream.NULLS))),
                                                new Allocator()))))))
                .matchesExactly(List.of(
                        Row.row(51L, 2L),
                        Row.row(52L, 2L)));
    }

    @Test
    void testCardinalityProjectsOptionalMapColumns()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalMapParquetFile("map-cardinality.parquet", List.of(
                new MapParquetRow(orderedMap("alpha", 10L, "beta", null)),
                new MapParquetRow(null),
                new MapParquetRow(Map.of()),
                new MapParquetRow(orderedMap("gamma", 30L))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable cardinality = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        cardinality,
                        new Call("cardinality", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(cardinality, Stream.VALUES),
                        new Reference(cardinality, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items")))) {
            Batch batch = operator.next();
            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()).startsWith(2L, 0L, 0L, 1L);
            assertThat(nulls.values()).startsWith(false, true, false, false);
        }
    }

    @Test
    void testMapKeysProjectsNestedKeyArrays()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalMapParquetFile("map-keys.parquet", List.of(
                new MapParquetRow(orderedMap("alpha", 10L, "beta", null)),
                new MapParquetRow(null),
                new MapParquetRow(Map.of()),
                new MapParquetRow(orderedMap("gamma", 30L))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable keys = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        keys,
                        new Call("map_keys", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(keys, Stream.VALUES),
                        new Reference(keys, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items")))) {
            Batch batch = operator.next();
            ArrayVector arrays = (ArrayVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);
            BinaryVector keysVector = (BinaryVector) arrays.elementValues();

            assertThat(arrays.length(0)).isEqualTo(2);
            assertThat(arrays.length(1)).isEqualTo(0);
            assertThat(arrays.length(2)).isEqualTo(0);
            assertThat(arrays.length(3)).isEqualTo(1);
            assertThat(keysVector.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
            assertThat(keysVector.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isTrue();
            assertThat(keysVector.utf8Value(0)).isEqualTo("alpha");
            assertThat(keysVector.utf8Value(1)).isEqualTo("beta");
            assertThat(keysVector.utf8Value(2)).isEqualTo("gamma");
            assertThat(nulls.values()).startsWith(false, true, false, false);
        }
    }

    @Test
    void testCardinalityConsumesMapKeys()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalMapParquetFile("map-keys-cardinality.parquet", List.of(
                new MapParquetRow(orderedMap("alpha", 10L, "beta", null)),
                new MapParquetRow(null),
                new MapParquetRow(Map.of()),
                new MapParquetRow(orderedMap("gamma", 30L))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable keys = new Variable(0);
        Variable cardinality = new Variable(1);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(
                        new Assignment(
                                keys,
                                new Call("map_keys", List.of(new Reference(new Input(0), Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(
                                cardinality,
                                new Call("cardinality", List.of(new Reference(keys, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(
                        new Reference(cardinality, Stream.VALUES),
                        new Reference(cardinality, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items")))) {
            Batch batch = operator.next();
            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()).startsWith(2L, 0L, 0L, 1L);
            assertThat(nulls.values()).startsWith(false, true, false, false);
        }
    }

    @Test
    void testMapValuesProjectsNestedValueArrays()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalMapParquetFile("map-values.parquet", List.of(
                new MapParquetRow(orderedMap("alpha", 10L, "beta", null)),
                new MapParquetRow(null),
                new MapParquetRow(Map.of()),
                new MapParquetRow(orderedMap("gamma", 30L))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable valuesArray = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        valuesArray,
                        new Call("map_values", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(valuesArray, Stream.VALUES),
                        new Reference(valuesArray, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items")))) {
            Batch batch = operator.next();
            ArrayVector arrays = (ArrayVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);
            I64Vector values = (I64Vector) arrays.elementValues();
            BooleanVector elementNulls = arrays.elementNulls();

            assertThat(arrays.length(0)).isEqualTo(2);
            assertThat(arrays.length(1)).isEqualTo(0);
            assertThat(arrays.length(2)).isEqualTo(0);
            assertThat(arrays.length(3)).isEqualTo(1);
            assertThat(values.values()).startsWith(10L, 0L, 30L);
            assertThat(elementNulls.values()).startsWith(false, true, false);
            assertThat(nulls.values()).startsWith(false, true, false, false);
        }
    }

    @Test
    void testCardinalityConsumesMapValues()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalMapParquetFile("map-values-cardinality.parquet", List.of(
                new MapParquetRow(orderedMap("alpha", 10L, "beta", null)),
                new MapParquetRow(null),
                new MapParquetRow(Map.of()),
                new MapParquetRow(orderedMap("gamma", 30L))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable valuesArray = new Variable(0);
        Variable cardinality = new Variable(1);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(
                        new Assignment(
                                valuesArray,
                                new Call("map_values", List.of(new Reference(new Input(0), Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(
                                cardinality,
                                new Call("cardinality", List.of(new Reference(valuesArray, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(
                        new Reference(cardinality, Stream.VALUES),
                        new Reference(cardinality, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items")))) {
            Batch batch = operator.next();
            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()).startsWith(2L, 0L, 0L, 1L);
            assertThat(nulls.values()).startsWith(false, true, false, false);
        }
    }

    @Test
    void testMapValuesProjectsNestedUtf8ValueArrays()
            throws IOException
    {
        java.nio.file.Path file = writeUtf8MapLookupParquetFile("map-values-utf8.parquet", List.of(
                new MapLookupUtf8ParquetRow(orderedUtf8Map("alpha", "one", "beta", null), null),
                new MapLookupUtf8ParquetRow(null, null),
                new MapLookupUtf8ParquetRow(Map.of(), null),
                new MapLookupUtf8ParquetRow(orderedUtf8Map("gamma", "three"), null)));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable valuesArray = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        valuesArray,
                        new Call("map_values", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(valuesArray, Stream.VALUES),
                        new Reference(valuesArray, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items")))) {
            Batch batch = operator.next();
            ArrayVector arrays = (ArrayVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);
            BinaryVector values = (BinaryVector) arrays.elementValues();
            BooleanVector elementNulls = arrays.elementNulls();

            assertThat(arrays.length(0)).isEqualTo(2);
            assertThat(arrays.length(1)).isEqualTo(0);
            assertThat(arrays.length(2)).isEqualTo(0);
            assertThat(arrays.length(3)).isEqualTo(1);
            assertThat(values.hasTrait(BinaryVector.Trait.UTF8_STRING)).isTrue();
            assertThat(values.hasTrait(BinaryVector.Trait.ASCII_ONLY)).isTrue();
            assertThat(values.utf8Value(0)).isEqualTo("one");
            assertThat(values.utf8Value(2)).isEqualTo("three");
            assertThat(elementNulls.values()).startsWith(false, true, false);
            assertThat(nulls.values()).startsWith(false, true, false, false);
        }
    }

    @Test
    void testCardinalityConsumesUtf8MapValues()
            throws IOException
    {
        java.nio.file.Path file = writeUtf8MapLookupParquetFile("map-values-utf8-cardinality.parquet", List.of(
                new MapLookupUtf8ParquetRow(orderedUtf8Map("alpha", "one", "beta", null), null),
                new MapLookupUtf8ParquetRow(null, null),
                new MapLookupUtf8ParquetRow(Map.of(), null),
                new MapLookupUtf8ParquetRow(orderedUtf8Map("gamma", "three"), null)));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable valuesArray = new Variable(0);
        Variable cardinality = new Variable(1);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(
                        new Assignment(
                                valuesArray,
                                new Call("map_values", List.of(new Reference(new Input(0), Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(
                                cardinality,
                                new Call("cardinality", List.of(new Reference(valuesArray, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(
                        new Reference(cardinality, Stream.VALUES),
                        new Reference(cardinality, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items")))) {
            Batch batch = operator.next();
            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()).startsWith(2L, 0L, 0L, 1L);
            assertThat(nulls.values()).startsWith(false, true, false, false);
        }
    }

    @Test
    void testCardinalityFeedsGroupingForRequiredMaps()
            throws IOException
    {
        java.nio.file.Path file = writeRequiredMapParquetFile("required-map-grouping.parquet", List.of(
                new MapParquetRow(orderedMap("a", 1L, "b", 2L)),
                new MapParquetRow(Map.of()),
                new MapParquetRow(orderedMap("c", 3L)),
                new MapParquetRow(orderedMap("d", 4L, "e", 5L)),
                new MapParquetRow(Map.of())));

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
                        Row.row(2L, 2L),
                        Row.row(0L, 2L),
                        Row.row(1L, 1L)));
    }

    @Test
    void testMapContainsKeyUtf8ProjectsValuesAndNulls()
            throws IOException
    {
        java.nio.file.Path file = writeMapLookupParquetFile("map-lookup.parquet", List.of(
                new MapLookupParquetRow(orderedMap("alpha", 10L, "beta", null), "alpha"),
                new MapLookupParquetRow(null, "alpha"),
                new MapLookupParquetRow(Map.of(), "missing"),
                new MapLookupParquetRow(orderedMap("gamma", 30L), null),
                new MapLookupParquetRow(orderedMap("delta", 40L), "missing")));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable contains = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        contains,
                        new Call("map_contains_key_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(contains, Stream.VALUES),
                        new Reference(contains, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items", "needle")))) {
            Batch batch = operator.next();
            BooleanVector values = (BooleanVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()).startsWith(true, false, false, false, false);
            assertThat(nulls.values()).startsWith(false, true, false, true, false);
        }
    }

    @Test
    void testMapContainsKeyUtf8FiltersParquetMaps()
            throws IOException
    {
        java.nio.file.Path file = writeMapLookupParquetFile("map-filter.parquet", List.of(
                new MapLookupParquetRow(orderedMap("alpha", 10L, "beta", null), "alpha"),
                new MapLookupParquetRow(null, "alpha"),
                new MapLookupParquetRow(Map.of(), "missing"),
                new MapLookupParquetRow(orderedMap("gamma", 30L), null),
                new MapLookupParquetRow(orderedMap("delta", 40L), "delta")));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable predicate = new Variable(0);
        EvaluationPlan filterPlan = new EvaluationPlan(
                List.of(new Assignment(
                        predicate,
                        new Call("map_contains_key_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of());

        try (FilterOperator operator = new FilterOperator(
                new ParquetScanOperator(new Allocator(), file, List.of("items", "needle")),
                filterPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                new Allocator())) {
            Batch batch = operator.next();
            assertThat(batch.borrowMask()).containsExactly(0, 4);

            org.weakref.nitro.data.Vector needles = batch.output(1).borrow(Stream.VALUES);
            assertThat(utf8Value(needles, 0)).isEqualTo("alpha");
            assertThat(utf8Value(needles, 4)).isEqualTo("delta");
        }
    }

    @Test
    void testElementAtI64Utf8ProjectsValuesAndNulls()
            throws IOException
    {
        java.nio.file.Path file = writeMapLookupParquetFile("map-element-at.parquet", List.of(
                new MapLookupParquetRow(orderedMap("alpha", 10L, "beta", null), "alpha"),
                new MapLookupParquetRow(null, "alpha"),
                new MapLookupParquetRow(Map.of(), "missing"),
                new MapLookupParquetRow(orderedMap("gamma", 30L), null),
                new MapLookupParquetRow(orderedMap("delta", 40L), "missing"),
                new MapLookupParquetRow(orderedMap("epsilon", 50L), "epsilon"),
                new MapLookupParquetRow(orderedMap("zeta", null), "zeta")));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable element = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        element,
                        new Call("element_at_i64_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(element, Stream.VALUES),
                        new Reference(element, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items", "needle")))) {
            Batch batch = operator.next();
            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()).startsWith(10L, 0L, 0L, 0L, 0L, 50L, 0L);
            assertThat(nulls.values()).startsWith(false, true, true, true, true, false, true);
        }
    }

    @Test
    void testElementAtUtf8Utf8ProjectsValuesAndNulls()
            throws IOException
    {
        java.nio.file.Path file = writeUtf8MapLookupParquetFile("map-element-at-utf8.parquet", List.of(
                new MapLookupUtf8ParquetRow(orderedUtf8Map("alpha", "one", "beta", null), "alpha"),
                new MapLookupUtf8ParquetRow(null, "alpha"),
                new MapLookupUtf8ParquetRow(Map.of(), "missing"),
                new MapLookupUtf8ParquetRow(orderedUtf8Map("gamma", "three"), null),
                new MapLookupUtf8ParquetRow(orderedUtf8Map("delta", "four"), "missing"),
                new MapLookupUtf8ParquetRow(orderedUtf8Map("epsilon", "five"), "epsilon"),
                new MapLookupUtf8ParquetRow(orderedUtf8Map("zeta", null), "zeta")));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable element = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        element,
                        new Call("element_at_utf8_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(element, Stream.VALUES),
                        new Reference(element, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items", "needle")))) {
            Batch batch = operator.next();
            org.weakref.nitro.data.Vector values = batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(utf8Value(values, 0)).isEqualTo("one");
            assertThat(utf8Value(values, 5)).isEqualTo("five");
            assertThat(nulls.values()).startsWith(false, true, true, true, true, false, true);
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
    void testArrayMinProjectsValuesAndNulls()
            throws IOException
    {
        java.nio.file.Path file = writeRepeatedNullableI64ParquetFile("array-min.parquet", List.of(
                new NullableArrayParquetRow(Arrays.asList(10L, null, 20L)),
                new NullableArrayParquetRow(List.of()),
                new NullableArrayParquetRow(Arrays.asList((Long) null)),
                new NullableArrayParquetRow(List.of(30L, 5L)),
                new NullableArrayParquetRow(Arrays.asList(7L, -3L, 12L))));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable minimum = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        minimum,
                        new Call("array_min_i64", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(minimum, Stream.VALUES),
                        new Reference(minimum, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(new Allocator(), file, List.of("items")))) {
            Batch batch = operator.next();
            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()).startsWith(10L, 0L, 0L, 5L, -3L);
            assertThat(nulls.values()).startsWith(false, true, true, false, false);
        }
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

    private java.nio.file.Path writeNullableArrayLookupParquetFile(String name, List<NullableArrayLookupParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .repeatedGroup()
                    .optional(INT64).named("element")
                .named("items")
                .optional(INT64).named("index")
                .named("nitro_nullable_array_lookup_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .build()) {
            for (NullableArrayLookupParquetRow row : rows) {
                Group group = groups.newGroup();
                for (Long item : row.items()) {
                    Group elementGroup = group.addGroup("items");
                    if (item != null) {
                        elementGroup.append("element", item);
                    }
                }
                if (row.index() != null) {
                    group.append("index", row.index());
                }
                writer.write(group);
            }
        }
        return file;
    }

    private java.nio.file.Path writeNullableArrayContainsParquetFile(String name, List<NullableArrayContainsParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .repeatedGroup()
                    .optional(INT64).named("element")
                .named("items")
                .optional(INT64).named("needle")
                .named("nitro_nullable_array_contains_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .build()) {
            for (NullableArrayContainsParquetRow row : rows) {
                Group group = groups.newGroup();
                for (Long item : row.items()) {
                    Group elementGroup = group.addGroup("items");
                    if (item != null) {
                        elementGroup.append("element", item);
                    }
                }
                if (row.needle() != null) {
                    group.append("needle", row.needle());
                }
                writer.write(group);
            }
        }
        return file;
    }

    private java.nio.file.Path writeStructParquetFile(String name, List<StructParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .requiredGroup()
                    .required(INT64).named("id")
                    .optional(BINARY).as(stringType()).named("name")
                    .optional(BOOLEAN).named("active")
                .named("person")
                .named("nitro_struct_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withDictionaryEncoding(true)
                .build()) {
            for (StructParquetRow row : rows) {
                Group group = groups.newGroup();
                Group person = group.addGroup("person")
                        .append("id", row.id());
                if (row.name() != null) {
                    person.append("name", row.name());
                }
                if (row.active() != null) {
                    person.append("active", row.active());
                }
                writer.write(group);
            }
        }
        return file;
    }

    private java.nio.file.Path writeOptionalMapParquetFile(String name, List<MapParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .optionalGroup().as(mapType())
                    .repeatedGroup()
                        .required(BINARY).as(stringType()).named("key")
                        .optional(INT64).named("value")
                    .named("key_value")
                .named("items")
                .named("nitro_optional_map_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withDictionaryEncoding(true)
                .build()) {
            for (MapParquetRow row : rows) {
                Group group = groups.newGroup();
                appendMap(group, "items", row.items());
                writer.write(group);
            }
        }
        return file;
    }

    private java.nio.file.Path writeRequiredMapParquetFile(String name, List<MapParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .requiredGroup().as(mapType())
                    .repeatedGroup()
                        .required(BINARY).as(stringType()).named("key")
                        .optional(INT64).named("value")
                    .named("key_value")
                .named("items")
                .named("nitro_required_map_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withDictionaryEncoding(true)
                .build()) {
            for (MapParquetRow row : rows) {
                Group group = groups.newGroup();
                appendMap(group, "items", row.items());
                writer.write(group);
            }
        }
        return file;
    }

    private java.nio.file.Path writeMapLookupParquetFile(String name, List<MapLookupParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .optionalGroup().as(mapType())
                    .repeatedGroup()
                        .required(BINARY).as(stringType()).named("key")
                        .optional(INT64).named("value")
                    .named("key_value")
                .named("items")
                .optional(BINARY).as(stringType()).named("needle")
                .named("nitro_map_lookup_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withDictionaryEncoding(true)
                .build()) {
            for (MapLookupParquetRow row : rows) {
                Group group = groups.newGroup();
                appendMap(group, "items", row.items());
                if (row.needle() != null) {
                    group.append("needle", row.needle());
                }
                writer.write(group);
            }
        }
        return file;
    }

    private java.nio.file.Path writeUtf8MapLookupParquetFile(String name, List<MapLookupUtf8ParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .optionalGroup().as(mapType())
                    .repeatedGroup()
                        .required(BINARY).as(stringType()).named("key")
                        .optional(BINARY).as(stringType()).named("value")
                    .named("key_value")
                .named("items")
                .optional(BINARY).as(stringType()).named("needle")
                .named("nitro_utf8_map_lookup_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withDictionaryEncoding(true)
                .build()) {
            for (MapLookupUtf8ParquetRow row : rows) {
                Group group = groups.newGroup();
                appendUtf8Map(group, "items", row.items());
                if (row.needle() != null) {
                    group.append("needle", row.needle());
                }
                writer.write(group);
            }
        }
        return file;
    }

    private java.nio.file.Path writeOptionalStructParquetFile(String name, List<OptionalStructParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .optionalGroup()
                    .required(INT64).named("id")
                    .optional(BINARY).as(stringType()).named("name")
                    .optional(BOOLEAN).named("active")
                .named("person")
                .named("nitro_optional_struct_test");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withDictionaryEncoding(true)
                .build()) {
            for (OptionalStructParquetRow row : rows) {
                Group group = groups.newGroup();
                if (row.person() != null) {
                    Group person = group.addGroup("person")
                            .append("id", row.person().id());
                    if (row.person().name() != null) {
                        person.append("name", row.person().name());
                    }
                    if (row.person().active() != null) {
                        person.append("active", row.person().active());
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

    private static void appendMap(Group group, String fieldName, Map<String, Long> entries)
    {
        if (entries == null) {
            return;
        }
        Group mapGroup = group.addGroup(fieldName);
        for (Map.Entry<String, Long> entry : entries.entrySet()) {
            Group keyValue = mapGroup.addGroup("key_value")
                    .append("key", entry.getKey());
            if (entry.getValue() != null) {
                keyValue.append("value", entry.getValue());
            }
        }
    }

    private static Map<String, Long> orderedMap(Object... entries)
    {
        LinkedHashMap<String, Long> map = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            map.put((String) entries[index], (Long) entries[index + 1]);
        }
        return map;
    }

    private static void appendUtf8Map(Group group, String fieldName, Map<String, String> entries)
    {
        if (entries == null) {
            return;
        }
        Group mapGroup = group.addGroup(fieldName);
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            Group keyValue = mapGroup.addGroup("key_value")
                    .append("key", entry.getKey());
            if (entry.getValue() != null) {
                keyValue.append("value", entry.getValue());
            }
        }
    }

    private static Map<String, String> orderedUtf8Map(Object... entries)
    {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            map.put((String) entries[index], (String) entries[index + 1]);
        }
        return map;
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

    private record NullableArrayLookupParquetRow(List<Long> items, Long index) {}

    private record NullableArrayContainsParquetRow(List<Long> items, Long needle) {}

    private record StructParquetRow(long id, String name, Boolean active) {}

    private record OptionalStructParquetRow(StructParquetRow person) {}

    private record MapParquetRow(Map<String, Long> items) {}

    private record MapLookupParquetRow(Map<String, Long> items, String needle) {}

    private record MapLookupUtf8ParquetRow(Map<String, String> items, String needle) {}
}
