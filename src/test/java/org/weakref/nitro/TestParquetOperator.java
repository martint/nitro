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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.weakref.nitro.clickbench.ClickBenchHitsSupport;
import org.weakref.nitro.core.function.VersionedLongPredicate;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.First;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.StructField;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.operator.source.BatchSourceOperator;
import org.weakref.nitro.operator.source.compatibility.NativeSourceOperatorIngress;
import org.weakref.nitro.operator.source.compatibility.OperatorBatchSource;
import org.weakref.nitro.operator.source.compatibility.parquet.HardwoodParquetScanOperator;
import org.weakref.nitro.operator.source.compatibility.parquet.HardwoodParquetScanPolicy;
import org.weakref.nitro.operator.source.compatibility.parquet.NitroParquetScanOperator;
import org.weakref.nitro.operator.source.compatibility.parquet.ParquetScanOperator;
import org.weakref.nitro.operator.source.compatibility.parquet.SkipDecodeScanOperator;
import org.weakref.nitro.operator.source.compatibility.parquet.SkipDecodeScanPolicy;
import org.weakref.nitro.operator.source.compatibility.parquet.TrinoParquetScanOperator;
import org.weakref.nitro.operator.source.compatibility.parquet.TrinoParquetScanPolicy;
import org.weakref.nitro.parquet.ColumnReader;
import org.weakref.nitro.parquet.DecompressedPageCachePolicy;
import org.weakref.nitro.parquet.NitroParquetBatchSource;
import org.weakref.nitro.parquet.NitroParquetScanResources;
import org.weakref.nitro.parquet.ParquetDictionaryFilterPolicy;
import org.weakref.nitro.parquet.ParquetFile;
import org.weakref.nitro.parquet.ParquetFilterEvaluationPolicy;
import org.weakref.nitro.parquet.ParquetFilterWindowPolicy;
import org.weakref.nitro.parquet.ParquetFilteredPayloadPolicy;
import org.weakref.nitro.parquet.ParquetLateMaterializationPolicy;
import org.weakref.nitro.parquet.ParquetMaterializationPolicy;
import org.weakref.nitro.parquet.ParquetNumericDecodeAdmissionPolicy;
import org.weakref.nitro.parquet.ParquetNumericDecodePolicy;
import org.weakref.nitro.parquet.ParquetPageNavigationPolicy;
import org.weakref.nitro.parquet.ParquetProgressiveFilterCompactionPolicy;
import org.weakref.nitro.parquet.ParquetReaderDiagnostics;
import org.weakref.nitro.parquet.ParquetReaderPolicy;
import org.weakref.nitro.parquet.ParquetScanBatchPolicy;
import org.weakref.nitro.parquet.ParquetScanDiagnostics;
import org.weakref.nitro.parquet.RleReaderPolicy;

import java.io.IOException;
import java.util.ArrayList;
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
    private static final TypeBinding BIGINT = new TestingTypeBinding(new TypeIdentity("testing:bigint"), long.class);
    private static final ParquetScanBatchPolicy LEGACY_PARQUET_SCAN_BATCH_POLICY = new ParquetScanBatchPolicy(512);
    private static final ParquetPageNavigationPolicy GENERIC_PAGE_NAVIGATION =
            new ParquetPageNavigationPolicy(false, 0, 101, Integer.MAX_VALUE, false, 0, Integer.MAX_VALUE, false);
    private static final ParquetMaterializationPolicy GENERIC_MATERIALIZATION =
            new ParquetMaterializationPolicy(
                    false, false, false, false, false, false, Long.MAX_VALUE, false, 0, false, false, false);
    private static final ParquetDictionaryFilterPolicy GENERIC_DICTIONARY_FILTER =
            new ParquetDictionaryFilterPolicy(
                    false,
                    false,
                    new ParquetDictionaryFilterPolicy.Compaction(0, false, 0, 0, Integer.MAX_VALUE, false, Long.MAX_VALUE),
                    new ParquetDictionaryFilterPolicy.NullableFilter(false, false, Integer.MAX_VALUE, Integer.MAX_VALUE),
                    new ParquetDictionaryFilterPolicy.ZeroAcceptedPageSkip(false, Long.MAX_VALUE),
                    new ParquetDictionaryFilterPolicy.VersionedPredicate(false, Integer.MAX_VALUE, 12));
    private static final ParquetLateMaterializationPolicy GENERIC_LATE_MATERIALIZATION =
            new ParquetLateMaterializationPolicy(
                    false,
                    new ParquetLateMaterializationPolicy.SkipDecode(
                            Integer.MAX_VALUE,
                            Integer.MAX_VALUE,
                            false,
                            new ParquetLateMaterializationPolicy.FragmentedNumeric(
                                    false,
                                    Integer.MAX_VALUE,
                                    Integer.MAX_VALUE,
                                    Integer.MAX_VALUE,
                                    Integer.MAX_VALUE),
                            Integer.MAX_VALUE,
                            false),
                    false);
    private static final ParquetProgressiveFilterCompactionPolicy GENERIC_PROGRESSIVE_FILTER_COMPACTION =
            new ParquetProgressiveFilterCompactionPolicy(
                    false,
                    0,
                    Long.MAX_VALUE,
                    0,
                    new ParquetProgressiveFilterCompactionPolicy.Prospective(
                            false, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE),
                    new ParquetProgressiveFilterCompactionPolicy.Fused(
                            false, 0, Integer.MAX_VALUE, Integer.MAX_VALUE),
                    false);
    private static final ParquetFilteredPayloadPolicy GENERIC_FILTERED_PAYLOAD =
            new ParquetFilteredPayloadPolicy(
                    0,
                    new ParquetFilteredPayloadPolicy.Deferred(
                            false,
                            0,
                            Integer.MAX_VALUE,
                            new ParquetFilteredPayloadPolicy.BoundedWindow(
                                    false, Integer.MAX_VALUE, 0, false)));
    private static final ParquetFilterWindowPolicy GENERIC_FILTER_WINDOW =
            new ParquetFilterWindowPolicy(
                    1 << 19,
                    new ParquetFilterWindowPolicy.AdaptiveNarrow(
                            false, 0, 1 << 19, 0, false),
                    false);
    private static final ParquetFilterEvaluationPolicy GENERIC_FILTER_EVALUATION =
            new ParquetFilterEvaluationPolicy(
                    new ParquetFilterEvaluationPolicy.Ordering(false, false),
                    new ParquetFilterEvaluationPolicy.NonSelectiveElision(false, false),
                    new ParquetFilterEvaluationPolicy.DirectNullMask(false, false));
    private final PrimitiveArrayPool arrayPool = EngineResources.createDefault().primitiveArrays();

    @TempDir
    java.nio.file.Path tempDirectory;

    @Test
    void testNitroParquetScanUsesExplicitConnectorResourcesWithAllocationOnlyAllocator()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("nitro-explicit-source-resources.parquet", true, List.of(
                new ParquetRow(10, true, 100L),
                new ParquetRow(20, true, 200L)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                Operator scan = new NitroParquetScanOperator(
                        new NitroParquetScanResources(
                                DecompressedPageCachePolicy.defaults(),
                                new ParquetReaderPolicy(
                                        RleReaderPolicy.defaults(),
                                        ParquetPageNavigationPolicy.defaults(),
                                        ParquetReaderDiagnostics.disabled(),
                                        GENERIC_MATERIALIZATION,
                                        new ParquetNumericDecodePolicy(false, false, false),
                                        GENERIC_DICTIONARY_FILTER),
                                new ParquetNumericDecodeAdmissionPolicy(
                                        Integer.MAX_VALUE,
                                        Long.MAX_VALUE,
                                        false,
                                        false,
                                        Integer.MAX_VALUE,
                                        Integer.MAX_VALUE,
                                        Long.MAX_VALUE,
                                        false),
                                GENERIC_LATE_MATERIALIZATION,
                                GENERIC_PROGRESSIVE_FILTER_COMPACTION,
                                GENERIC_FILTERED_PAYLOAD,
                                GENERIC_FILTER_WINDOW,
                                GENERIC_FILTER_EVALUATION,
                                ParquetScanDiagnostics.disabled(),
                                ParquetScanBatchPolicy.defaults()),
                        allocator,
                        List.of(file),
                        List.of("x"))) {
            assertThat(operator(scan)).matchesExactly(List.of(Row.row(10L), Row.row(20L)));
        }
    }

    @Test
    void testNitroParquetSourceEmitsNativeSourceBatch()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("nitro-native-source.parquet", true, List.of(
                new ParquetRow(10, true, 100L),
                new ParquetRow(20, false, null),
                new ParquetRow(30, true, 300L)));
        Schema schema = new Schema(List.of(new Field("maybe", BIGINT, true)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        NitroParquetScanResources.createDefault(),
                        allocator,
                        List.of(file),
                        schema)) {
            assertThat(source.schema()).isSameAs(schema);
            assertThat(source.column(0)).isSameAs(source.column(0));

            SourcePoll.Ready ready = (SourcePoll.Ready) source.poll();
            var batch = ready.batch();
            batch.select(new org.weakref.nitro.data.MaskSelection(Mask.sparse(new int[] {0, 2}, 3)));
            assertThat(((I64Vector) batch.column(0).borrow(Stream.VALUES)).values())
                    .startsWith(100L, 0L, 300L);
            assertThat(((BooleanVector) batch.column(0).borrow(Stream.NULLS)).values())
                    .startsWith(false, true, false);
            batch.close();

            assertThat(source.poll()).isSameAs(SourcePoll.Finished.FINISHED);
        }
    }

    @Test
    void testCompatibilityParquetScansExposeProjectedColumnNames()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("compatibility-scan-schema.parquet", true, List.of(
                new ParquetRow(10, true, 100L)));
        List<String> columns = List.of("x", "maybe");

        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                Operator hardwood = new HardwoodParquetScanOperator(HardwoodParquetScanPolicy.defaults(), allocator, file, columns);
                Operator nitro = new NitroParquetScanOperator(NitroParquetScanResources.createDefault(), allocator, List.of(file), columns);
                Operator parquet = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, allocator, file, columns);
                Operator skip = new SkipDecodeScanOperator(SkipDecodeScanPolicy.defaults(), allocator, List.of(file), columns);
                Operator trino = new TrinoParquetScanOperator(TrinoParquetScanPolicy.defaults(), allocator, file, columns)) {
            for (Operator scan : List.of(hardwood, nitro, parquet, skip, trino)) {
                assertThat(scan.outputSchema().fields())
                        .extracting(field -> field.name().orElseThrow())
                        .containsExactlyElementsOf(columns);
                assertThat(scan.outputSchema().fields())
                        .allSatisfy(field -> assertThat(field.type().isSpecified()).isFalse());
            }
        }
    }

    @Test
    void testNitroParquetScanFilterProjectCrossesSourcePort()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("nitro-source-port.parquet", true, List.of(
                new ParquetRow(10, true, 100L),
                new ParquetRow(20, true, 200L),
                new ParquetRow(10, false, 300L),
                new ParquetRow(10, true, 400L)));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        Schema inputSchema = new Schema(List.of(
                new Field("x", BIGINT, false),
                new Field("maybe", BIGINT, true)));
        Operator ingress = new BatchSourceOperator(new OperatorBatchSource(
                new NitroParquetScanOperator(NitroParquetScanResources.createDefault(), allocator, List.of(file), List.of("x", "maybe")),
                inputSchema), new NativeSourceOperatorIngress());
        assertThat(ingress.outputSchema()).isEqualTo(inputSchema);
        assertThat(ingress.supportsDynamicFilterPushdown(0)).isTrue();
        ingress.pushDynamicFilter(DynamicFilter.fromRange(0, 10, 10));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable squared = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        squared,
                        new Call("multiply", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(squared, Stream.VALUES)));
        Schema outputSchema = new Schema(List.of(new Field("squared", BIGINT, false)));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                projectionPlan,
                primitiveRegistry,
                new FilterOperator(
                        ingress,
                        new EvaluationPlan(List.of(), List.of()),
                        primitiveRegistry,
                        AllMask.ALL,
                        allocator,
                        EngineResources.from(allocator).operatorResources().filter()),
                outputSchema)) {
            assertThat(operator.outputSchema()).isEqualTo(outputSchema);
            assertThat(operator(operator))
                    .matchesExactly(List.of(
                            Row.row(100L),
                            Row.row(100L),
                            Row.row(100L)));
        }
    }

    @Test
    void testNitroFilteredWindowDefersWidePayloadUntilConstrain()
            throws IOException
    {
        java.nio.file.Path file = writeWideNumericParquetFile("nitro-deferred-filtered-payload.parquet", List.of(
                new WideNumericRow(1, 11, 12, 13, 14),
                new WideNumericRow(2, 21, 22, 23, 24),
                new WideNumericRow(3, 31, 32, 33, 34),
                new WideNumericRow(4, 41, 42, 43, 44),
                new WideNumericRow(5, 51, 52, 53, 54),
                new WideNumericRow(6, 61, 62, 63, 64),
                new WideNumericRow(7, 71, 72, 73, 74),
                new WideNumericRow(8, 81, 82, 83, 84),
                new WideNumericRow(9, 91, 92, 93, 94),
                new WideNumericRow(10, 101, 102, 103, 104)));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(
                NitroParquetScanResources.createDefault(),
                allocator,
                List.of(file),
                List.of("key", "p1", "p2", "p3", "p4"))) {
            // Keep the synthetic window sparse enough to exercise the selective skip/defer path rather than
            // the deliberately eager bulk path used when more than half of a window survives.
            scan.pushDynamicFilter(DynamicFilter.fromRange(0, 2, 6));
            Batch batch = scan.next();
            assertThat(batch.borrowMask()).hasSize(5);
            // Re-borrow remains available during the current batch, while availability polling stays forbidden
            // until that batch closes.
            assertThat(scan.supportsConstrainedReborrow()).isTrue();
            assertThat(scan.supportsOpenBatchHasNext()).isFalse();

            scan.constrain(Mask.sparse(new int[] {2}, 5));
            I64Vector payload = (I64Vector) batch.output(4).borrow(Stream.VALUES);
            assertThat(payload.values()[2]).isEqualTo(44L);
            batch.close();
            assertThat(scan.hasNext()).isFalse();
        }
    }

    @Test
    void testNitroDirectNullMaskKeepsValueReaderIndependent()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("nitro-direct-null-mask.parquet", true, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, false, null),
                new ParquetRow(13, true, 103L),
                new ParquetRow(14, false, null)));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("direct-null-mask-test");
        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(NitroParquetScanResources.createDefault(), allocator, List.of(file), List.of("maybe"))) {
            Batch batch = scan.next();
            Mask nullMask = batch.output(0).tryBorrowMask(
                    Stream.NULLS,
                    batch.borrowMask(),
                    true,
                    allocator,
                    context);
            assertThat(nullMask).isNotNull().containsExactly(1, 3);

            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
            assertThat(values.values()).startsWith(101L, 0L, 103L, 0L);
            assertThat(nulls.values()).startsWith(false, true, false, true);
            allocator.release(context, nullMask);
        }
    }

    @Test
    void testNitroDirectNullMaskAdvancesAcrossUnresolvedBatches()
            throws IOException
    {
        List<ParquetRow> firstRows = new ArrayList<>();
        for (int position = 0; position < 10_000; position++) {
            firstRows.add(new ParquetRow(position, false, (long) position));
        }
        java.nio.file.Path first = writeParquetFile("nitro-direct-null-mask-skipped-batch-first.parquet", true, firstRows);
        java.nio.file.Path second = writeParquetFile("nitro-direct-null-mask-skipped-batch-second.parquet", true, List.of(
                new ParquetRow(10_000, false, null),
                new ParquetRow(10_001, false, 10_001L),
                new ParquetRow(10_002, false, null)));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("direct-null-mask-skipped-batch-test");
        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(NitroParquetScanResources.createDefault(), allocator, List.of(first, second), List.of("maybe"))) {
            Batch skipped = scan.next();
            assertThat(skipped.borrowMask()).hasSize(10_000);

            Batch batch = scan.next();
            Mask nullMask = batch.output(0).tryBorrowMask(
                    Stream.NULLS,
                    batch.borrowMask(),
                    true,
                    allocator,
                    context);
            assertThat(nullMask).isNotNull().containsExactly(0, 2);

            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
            assertThat(values.values()).startsWith(0L, 10_001L, 0L);
            assertThat(nulls.values()).startsWith(true, false, true);
            allocator.release(context, nullMask);
        }
    }

    @Test
    void testNitroConstrainedEmptyBorrowDefersDecodeAndPreservesNextChunkAlignment()
            throws IOException
    {
        List<ParquetRow> firstRows = new ArrayList<>();
        for (int position = 0; position < 10_000; position++) {
            firstRows.add(new ParquetRow(position % 3, false, 100L + (position % 5)));
        }
        java.nio.file.Path first = writeParquetFile("nitro-empty-constrained-first.parquet", true, firstRows);
        java.nio.file.Path second = writeParquetFile("nitro-empty-constrained-second.parquet", true, List.of(
                new ParquetRow(21, false, 201L),
                new ParquetRow(22, false, 202L),
                new ParquetRow(23, false, null)));
        assertDictionaryEncoding(first, "x");
        assertDictionaryEncoding(first, "maybe");

        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(
                NitroParquetScanResources.createDefault(),
                new Allocator(EngineResources.createDefault()),
                List.of(first, second),
                List.of("x", "maybe"))) {
            Batch empty = scan.next();
            scan.constrain(Mask.none(10_000));
            // Schema-only borrows must not decode the empty batch or commit either reader to a page path.
            assertThat(empty.output(0).borrow(Stream.VALUES)).isInstanceOf(I64Vector.class);
            assertThat(empty.output(1).borrow(Stream.NULLS)).isInstanceOf(BooleanVector.class);

            Batch selected = scan.next();
            scan.constrain(Mask.sparse(new int[] {1}, 3));
            I64Vector x = (I64Vector) selected.output(0).borrow(Stream.VALUES);
            I64Vector maybe = (I64Vector) selected.output(1).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) selected.output(1).borrow(Stream.NULLS);
            assertThat(x.values()[1]).isEqualTo(22L);
            assertThat(maybe.values()[1]).isEqualTo(202L);
            assertThat(nulls.values()[1]).isFalse();
            assertThat(scan.hasNext()).isFalse();
        }
    }

    @Test
    void testParquetScanReadsPlainColumns()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("plain.parquet", false, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, false, null),
                new ParquetRow(13, true, 103L)));

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("x", "flag", "maybe"))) {
            assertThat(operator(operator))
                    .matchesExactly(List.of(
                            Row.row(11L, 1L, 101L),
                            Row.row(12L, 0L, null),
                            Row.row(13L, 1L, 103L)));
        }
    }

    @Test
    void testParquetScanUsesSuppliedBatchPolicy()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("configured-batches.parquet", false, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, false, null),
                new ParquetRow(13, true, 103L)));

        try (ParquetScanOperator operator = new ParquetScanOperator(
                new ParquetScanBatchPolicy(2),
                new Allocator(EngineResources.createDefault()),
                file,
                List.of("x"))) {
            Batch first = operator.next();
            assertThat(first.borrowMask()).hasSize(2);
            assertThat(((I64Vector) first.output(0).borrow(Stream.VALUES)).values())
                    .startsWith(11, 12);

            Batch second = operator.next();
            assertThat(second.borrowMask()).hasSize(1);
            assertThat(((I64Vector) second.output(0).borrow(Stream.VALUES)).values()[0])
                    .isEqualTo(13);
            assertThat(operator.hasNext()).isFalse();
        }
    }

    @Test
    void testTrinoParquetScanReadsPlainColumns()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("trino-plain.parquet", false, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, false, null),
                new ParquetRow(13, true, 103L)));

        try (TrinoParquetScanOperator operator = new TrinoParquetScanOperator(TrinoParquetScanPolicy.fromSystemProperties(), new Allocator(EngineResources.createDefault()), file, List.of("x", "flag", "maybe"))) {
            assertThat(operator(operator))
                    .matchesExactly(List.of(
                            Row.row(11L, 1L, 101L),
                            Row.row(12L, 0L, null),
                            Row.row(13L, 1L, 103L)));
        }
    }

    @Test
    void testTrinoParquetScanUsesExplicitDynamicFilterPolicy()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("trino-dynamic-filter-policy.parquet", false, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, true, 102L),
                new ParquetRow(13, true, 103L)));

        TrinoParquetScanPolicy policy = new TrinoParquetScanPolicy(1, 1, 0.30);
        try (TrinoParquetScanOperator operator = new TrinoParquetScanOperator(
                policy,
                new Allocator(EngineResources.createDefault()),
                file,
                List.of("x"))) {
            operator.pushDynamicFilter(DynamicFilter.fromRange(0, 11, 11));

            assertThat(operator(operator))
                    .matchesExactly(List.of(
                            Row.row(11L),
                            Row.row(12L),
                            Row.row(13L)));
        }
    }

    @Test
    void testTrinoParquetScanReadsClickBenchI32Columns()
            throws IOException
    {
        java.nio.file.Path file = ClickBenchHitsSupport.writeHitsFixture(tempDirectory.resolve("trino-clickbench.parquet"), 3);

        try (TrinoParquetScanOperator operator = new TrinoParquetScanOperator(TrinoParquetScanPolicy.fromSystemProperties(), new Allocator(EngineResources.createDefault()), file, List.of("AdvEngineID", "ResolutionWidth", "UserID"))) {
            Batch batch = operator.next();

            assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
            assertThat(batch.output(1).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
            assertThat(batch.output(2).borrow(Stream.VALUES)).isInstanceOf(I64Vector.class);
            assertThat(((I32Vector) batch.output(0).borrow(Stream.VALUES)).values()[0]).isEqualTo(0);
            assertThat(((I32Vector) batch.output(1).borrow(Stream.VALUES)).values()[0]).isEqualTo(1000);
            assertThat(((I64Vector) batch.output(2).borrow(Stream.VALUES)).values()[0]).isEqualTo(1L);
        }

        try (TrinoParquetScanOperator operator = new TrinoParquetScanOperator(TrinoParquetScanPolicy.fromSystemProperties(), new Allocator(EngineResources.createDefault()), file, List.of("AdvEngineID", "ResolutionWidth", "UserID"))) {
            assertThat(operator(operator))
                    .matchesExactly(List.of(
                            Row.row(0, 1000, 1L),
                            Row.row(10, 1200, 2L),
                            Row.row(10, 900, 2L)));
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

        try (ParquetScanOperator scan = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("x", "flag", "maybe"))) {
            Batch batch = scan.next();
            assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(DictionaryVector.class);
        }

        Allocator allocator = new Allocator(EngineResources.createDefault());
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
                allocator,
                projectionPlan,
                primitiveRegistry,
                new FilterOperator(
                        new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, allocator, file, List.of("x", "flag", "maybe")),
                        new EvaluationPlan(List.of(), List.of()),
                        primitiveRegistry,
                        new Reference(new Input(1), Stream.VALUES),
                        allocator,
                        EngineResources.from(allocator).operatorResources().filter()))) {
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
    void testExactDictionaryCoverageAcrossChunksDoesNotConsumeReader()
            throws IOException
    {
        List<ParquetRow> firstRows = new ArrayList<>();
        List<ParquetRow> secondRows = new ArrayList<>();
        for (int position = 0; position < 2_000; position++) {
            firstRows.add(new ParquetRow((position & 1) == 0 ? 10 : 20, true, null));
            secondRows.add(new ParquetRow((position & 1) == 0 ? 30 : 40, true, null));
        }
        java.nio.file.Path first = writeParquetFile("dictionary-coverage-first.parquet", true, firstRows);
        java.nio.file.Path second = writeParquetFile("dictionary-coverage-second.parquet", true, secondRows);
        assertDictionaryEncoding(first, "x");
        assertDictionaryEncoding(second, "x");

        try (ParquetFile firstFile = ParquetFile.open(first);
                ParquetFile secondFile = ParquetFile.open(second);
                ColumnReader reader = columnReader(List.of(firstFile, secondFile), "x")) {
            // Cardinality alone cannot decide this: the accepted range is much larger than either local dictionary,
            // but it covers every physical value and therefore cannot prune the scan.
            assertThat(reader.dictionaryValuesCovered(value -> value >= 10 && value <= 40)).isTrue();

            // Conversely, matching a local dictionary's cardinality says nothing about value equality.
            assertThat(reader.dictionaryValuesCovered(value -> value == 10 || value == 30)).isFalse();

            // Admission reads dictionary pages only and must leave the ordinary data cursor untouched.
            long[] values = new long[4_000];
            reader.readLongs(values, null, values.length);
            assertThat(Arrays.copyOfRange(values, 0, 4)).containsExactly(10, 20, 10, 20);
            assertThat(Arrays.copyOfRange(values, 2_000, 2_004)).containsExactly(30, 40, 30, 40);
        }
    }

    @Test
    void testNitroDynamicFilterStreamsNullableDictionaryPages()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        Long[] values = {null, 100L, 200L, 400L, 300L};
        for (int position = 0; position < 2_000; position++) {
            rows.add(new ParquetRow(position, (position & 1) == 0, values[position % values.length]));
        }
        java.nio.file.Path file = writeParquetFile("nullable-dictionary-filter.parquet", true, rows);

        assertDictionaryEncoding(file, "maybe");
        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(NitroParquetScanResources.createDefault(), new Allocator(EngineResources.createDefault()), List.of(file), List.of("maybe"))) {
            scan.pushDynamicFilter(DynamicFilter.fromRange(0, 100, 300));
            try (Batch batch = scan.next()) {
                assertThat(batch.borrowMask()).hasSize(1_200);
                I64Vector filtered = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                BooleanVector nulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
                for (int position = 0; position < 1_200; position++) {
                    assertThat(filtered.values()[position]).isIn(100L, 200L, 300L);
                    assertThat(nulls.values()[position]).isFalse();
                }
            }
            assertThat(scan.hasNext()).isFalse();
        }
    }

    @Test
    void testNitroProgressiveDynamicFiltersPreserveAlignedValuesAndNulls()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        int expected = 0;
        for (int position = 0; position < 4_096; position++) {
            long x = position & 15;
            Long maybe = position % 23 == 0 ? null : (long) ((position >>> 4) & 15);
            rows.add(new ParquetRow(x, true, maybe));
            if (x == 0 && maybe != null && maybe == 0) {
                expected++;
            }
        }
        java.nio.file.Path file = writeParquetFile("progressive-dynamic-filters.parquet", true, rows);

        int actual = 0;
        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(
                NitroParquetScanResources.createDefault(),
                new Allocator(EngineResources.createDefault()),
                List.of(file),
                List.of("x", "maybe"))) {
            // Equal-cardinality x/maybe filters retain column order: x leads and nullable maybe narrows its survivors
            // to about one sixteenth, exercising in-place compaction of both the earlier and current value buffers.
            scan.pushDynamicFilter(DynamicFilter.fromRange(0, 0, 0));
            scan.pushDynamicFilter(DynamicFilter.fromRange(1, 0, 0));
            while (scan.hasNext()) {
                try (Batch batch = scan.next()) {
                    int count = batch.borrowMask().size();
                    I64Vector x = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                    I64Vector maybe = (I64Vector) batch.output(1).borrow(Stream.VALUES);
                    BooleanVector maybeNulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);
                    for (int position = 0; position < count; position++) {
                        assertThat(x.values()[position]).isZero();
                        assertThat(maybe.values()[position]).isZero();
                        assertThat(maybeNulls.values()[position]).isFalse();
                    }
                    actual += count;
                }
            }
        }
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testRejectedDictionaryPageSkipPreservesNullableAndRequiredCursorsAcrossChunks()
            throws IOException
    {
        List<ParquetRow> rejected = new ArrayList<>();
        List<ParquetRow> accepted = new ArrayList<>();
        for (int position = 0; position < 2_000; position++) {
            rejected.add(new ParquetRow(400, true, (position & 1) == 0 ? null : 400L));
            accepted.add(new ParquetRow((position & 1) == 0 ? 100 : 200, true, (position & 1) == 0 ? 100L : 200L));
        }
        java.nio.file.Path first = writeParquetFile("rejected-dictionary-page-first.parquet", true, rejected);
        java.nio.file.Path second = writeParquetFile("rejected-dictionary-page-second.parquet", true, accepted);
        assertDictionaryEncoding(first, "x");
        assertDictionaryEncoding(first, "maybe");
        assertDictionaryEncoding(second, "x");
        assertDictionaryEncoding(second, "maybe");

        try (ParquetFile firstFile = ParquetFile.open(first);
                ParquetFile secondFile = ParquetFile.open(second);
                ColumnReader required = columnReader(List.of(firstFile, secondFile), "x");
                ColumnReader nullable = columnReader(List.of(firstFile, secondFile), "maybe")) {
            int[] survivors = new int[2_000];
            long[] values = new long[2_000];
            java.util.function.LongPredicate predicate = value -> value <= 200;

            assertThat(required.filterDictLongs(predicate, 2_000, survivors, values, null)).isZero();
            assertThat(required.filterDictLongs(predicate, 2_000, survivors, values, null)).isEqualTo(2_000);
            assertThat(values).containsOnly(100L, 200L);

            Arrays.fill(values, 0);
            assertThat(nullable.filterDictLongs(predicate, 2_000, survivors, values, null)).isZero();
            assertThat(nullable.filterDictLongs(predicate, 2_000, survivors, values, null)).isEqualTo(2_000);
            assertThat(values).containsOnly(100L, 200L);
        }
    }

    @Test
    void testVersionedPredicateReusesDictionaryAcceptanceUntilGenerationChanges()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        long[] dictionary = {100, 200, 300, 400};
        for (int position = 0; position < 24_000; position++) {
            rows.add(new ParquetRow(position, true, dictionary[position & 3]));
        }
        java.nio.file.Path file = writeParquetFile("versioned-dictionary-predicate.parquet", true, rows);
        assertDictionaryEncoding(file, "maybe");

        class CountingPredicate
                implements VersionedLongPredicate
        {
            private long generation;
            private int calls;

            @Override
            public boolean test(long value)
            {
                calls++;
                return value <= 300;
            }

            @Override
            public long contentGeneration()
            {
                return generation;
            }
        }

        try (ParquetFile parquetFile = ParquetFile.open(file);
                ColumnReader reader = columnReader(List.of(parquetFile), "maybe")) {
            CountingPredicate predicate = new CountingPredicate();
            int[] survivors = new int[8_000];
            long[] values = new long[8_000];

            assertThat(reader.filterDictLongs(predicate, predicate, 8_000, survivors, values, null)).isEqualTo(6_000);
            int firstGenerationCalls = predicate.calls;
            assertThat(firstGenerationCalls).isPositive();

            assertThat(reader.filterDictLongs(predicate, predicate, 8_000, survivors, values, null)).isEqualTo(6_000);
            assertThat(predicate.calls).isEqualTo(firstGenerationCalls);

            predicate.generation++;
            assertThat(reader.filterDictLongs(predicate, predicate, 8_000, survivors, values, null)).isEqualTo(6_000);
            assertThat(predicate.calls).isEqualTo(firstGenerationCalls * 2);
        }
    }

    @Test
    void testNitroDynamicFilterStreamsHomogeneousNullableDictionaryRuns()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        Long[] values = {100L, 400L, 200L, 300L};
        for (int position = 0; position < 8_000; position++) {
            Long value = position < 2_000 ? null : values[position & 3];
            rows.add(new ParquetRow(position, true, value));
        }
        java.nio.file.Path file = writeParquetFile("nullable-dictionary-homogeneous-runs.parquet", true, rows);

        assertDictionaryEncoding(file, "maybe");
        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(NitroParquetScanResources.createDefault(), new Allocator(EngineResources.createDefault()), List.of(file), List.of("maybe"))) {
            scan.pushDynamicFilter(DynamicFilter.fromRange(0, 100, 300));
            try (Batch batch = scan.next()) {
                assertThat(batch.borrowMask()).hasSize(4_500);
                I64Vector filtered = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                BooleanVector nulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
                for (int position = 0; position < 4_500; position++) {
                    assertThat(filtered.values()[position]).isIn(100L, 200L, 300L);
                    assertThat(nulls.values()[position]).isFalse();
                }
            }
            assertThat(scan.hasNext()).isFalse();
        }
    }

    @Test
    void testTrinoParquetScanPreservesDictionaryEncodingForUtf8Columns()
            throws IOException
    {
        java.nio.file.Path file = writeBinaryParquetFile("trino-dictionary-strings.parquet", true, List.of(
                new BinaryParquetRow("alice", bytes(1, 2, 3)),
                new BinaryParquetRow("bob", null),
                new BinaryParquetRow("alice", bytes(4, 5))));

        assertDictionaryEncoding(file, "name");

        try (TrinoParquetScanOperator operator = new TrinoParquetScanOperator(TrinoParquetScanPolicy.fromSystemProperties(), new Allocator(EngineResources.createDefault()), file, List.of("name", "payload"))) {
            Batch batch = operator.next();
            BinaryVector names = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
            assertThat(names.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(utf8(names, 0)).isEqualTo("alice");
        }
    }

    @Test
    void testParquetScanHonorsConstrainBeforeBorrowingPlainColumn()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("plain-constrained.parquet", false, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, false, 102L),
                new ParquetRow(13, true, 103L)));

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("x", "maybe"))) {
            Batch batch = operator.next();
            Mask constrainedMask = Mask.sparse(new int[] {2}, 3);

            operator.constrain(constrainedMask);

            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()[0]).isEqualTo(0L);
            assertThat(values.values()[1]).isEqualTo(0L);
            assertThat(values.values()[2]).isEqualTo(13L);
            assertThat(nulls.values()[0]).isFalse();
            assertThat(nulls.values()[1]).isFalse();
            assertThat(nulls.values()[2]).isFalse();
        }
    }

    @Test
    void testColumnReaderSkipsCompleteChunksWithoutDecodingThem()
            throws IOException
    {
        java.nio.file.Path first = writeParquetFile("skip-chunk-first.parquet", false, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, false, 102L),
                new ParquetRow(13, true, 103L)));
        java.nio.file.Path second = writeParquetFile("skip-chunk-second.parquet", false, List.of(
                new ParquetRow(21, true, 201L),
                new ParquetRow(22, false, 202L),
                new ParquetRow(23, true, 203L)));

        try (org.weakref.nitro.parquet.ParquetFile firstFile = org.weakref.nitro.parquet.ParquetFile.open(first);
                org.weakref.nitro.parquet.ParquetFile secondFile = org.weakref.nitro.parquet.ParquetFile.open(second)) {
            org.weakref.nitro.parquet.ParquetFile.Column column = firstFile.column("x");
            try (org.weakref.nitro.parquet.ColumnReader reader = new org.weakref.nitro.parquet.ColumnReader(
                    column.type(), column.optional(), column.typeLength(), column.decimal(), null, arrayPool, ParquetReaderPolicy.defaults())) {
                for (org.apache.parquet.format.RowGroup rowGroup : firstFile.rowGroups()) {
                    reader.addChunk(firstFile.data(), firstFile.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
                }
                for (org.apache.parquet.format.RowGroup rowGroup : secondFile.rowGroups()) {
                    reader.addChunk(secondFile.data(), secondFile.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
                }

                reader.skip(3);
                long[] values = new long[3];
                reader.readLongs(values, null, values.length);
                assertThat(values).containsExactly(21L, 22L, 23L);
            }
        }
    }

    @Test
    void testColumnReaderReusesNumericDictionaryScratchAcrossChunks()
            throws IOException
    {
        java.nio.file.Path first = writeParquetFile("dictionary-scratch-first.parquet", true, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, false, 102L),
                new ParquetRow(11, true, 101L)));
        java.nio.file.Path second = writeParquetFile("dictionary-scratch-second.parquet", true, List.of(
                new ParquetRow(21, true, 201L),
                new ParquetRow(22, false, 202L),
                new ParquetRow(23, true, 203L),
                new ParquetRow(21, false, 204L)));
        assertDictionaryEncoding(first, "x");
        assertDictionaryEncoding(second, "x");

        try (org.weakref.nitro.parquet.ParquetFile firstFile = org.weakref.nitro.parquet.ParquetFile.open(first);
                org.weakref.nitro.parquet.ParquetFile secondFile = org.weakref.nitro.parquet.ParquetFile.open(second)) {
            org.weakref.nitro.parquet.ParquetFile.Column column = firstFile.column("x");
            try (org.weakref.nitro.parquet.ColumnReader reader = new org.weakref.nitro.parquet.ColumnReader(
                    column.type(), column.optional(), column.typeLength(), column.decimal(), null, arrayPool, ParquetReaderPolicy.defaults())) {
                for (org.apache.parquet.format.RowGroup rowGroup : firstFile.rowGroups()) {
                    reader.addChunk(firstFile.data(), firstFile.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
                }
                for (org.apache.parquet.format.RowGroup rowGroup : secondFile.rowGroups()) {
                    reader.addChunk(secondFile.data(), secondFile.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
                }

                long[] values = new long[7];
                reader.readLongs(values, null, values.length);
                assertThat(values).containsExactly(11L, 12L, 11L, 21L, 22L, 23L, 21L);
            }
        }
    }

    @Test
    void testFullNumericDictionaryReadRemainsAlignedAcrossPartialBatches()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int position = 0; position < 257; position++) {
            rows.add(new ParquetRow(
                    100 + (position % 17),
                    true,
                    position % 7 == 0 ? null : 1_000L + (position % 23)));
        }
        java.nio.file.Path file = writeParquetFile("numeric-dictionary-partial-batches.parquet", true, rows);
        assertDictionaryEncoding(file, "x");
        assertDictionaryEncoding(file, "maybe");

        try (ParquetFile parquetFile = ParquetFile.open(file);
                ColumnReader required = columnReader(List.of(parquetFile), "x");
                ColumnReader optional = columnReader(List.of(parquetFile), "maybe")) {
            int consumed = 0;
            for (int batchSize : new int[] {31, 73, 153}) {
                long[] requiredValues = new long[batchSize];
                long[] optionalValues = new long[batchSize];
                boolean[] optionalNulls = new boolean[batchSize];
                required.readLongs(requiredValues, null, batchSize);
                optional.readLongs(optionalValues, optionalNulls, batchSize);

                for (int index = 0; index < batchSize; index++) {
                    int position = consumed + index;
                    assertThat(requiredValues[index]).isEqualTo(100L + (position % 17));
                    assertThat(optionalNulls[index]).isEqualTo(position % 7 == 0);
                    assertThat(optionalValues[index]).isEqualTo(position % 7 == 0 ? 0 : 1_000L + (position % 23));
                }
                consumed += batchSize;
            }
            assertThat(consumed).isEqualTo(rows.size());
        }
    }

    @Test
    void testDenseSelectedNullablePageResetsDefinitionReaderAfterAllOnesProbe()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int position = 0; position < 200; position++) {
            rows.add(new ParquetRow(position, false, position == 50 ? null : 1_000L + position));
        }
        java.nio.file.Path file = writeParquetFile("dense-selected-nullable.parquet", true, rows);

        try (org.weakref.nitro.parquet.ParquetFile parquet = org.weakref.nitro.parquet.ParquetFile.open(file)) {
            org.weakref.nitro.parquet.ParquetFile.Column column = parquet.column("maybe");
            try (org.weakref.nitro.parquet.ColumnReader reader = new org.weakref.nitro.parquet.ColumnReader(
                    column.type(), column.optional(), column.typeLength(), column.decimal(), null, arrayPool,
                    new ParquetReaderPolicy(
                            RleReaderPolicy.defaults(),
                            GENERIC_PAGE_NAVIGATION,
                            ParquetReaderDiagnostics.disabled(),
                            ParquetMaterializationPolicy.defaults(),
                            ParquetNumericDecodePolicy.defaults(),
                            ParquetDictionaryFilterPolicy.defaults()))) {
                for (org.apache.parquet.format.RowGroup rowGroup : parquet.rowGroups()) {
                    reader.addChunk(parquet.data(), parquet.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
                }

                int[] survivors = new int[100];
                for (int index = 0; index < survivors.length; index++) {
                    survivors[index] = index * 2;
                }
                long[] values = new long[survivors.length];
                boolean[] nulls = new boolean[survivors.length];
                reader.readSelectedLongs(survivors, survivors.length, rows.size(), values, nulls);

                for (int index = 0; index < survivors.length; index++) {
                    int position = survivors[index];
                    assertThat(nulls[index]).isEqualTo(position == 50);
                    if (position != 50) {
                        assertThat(values[index]).isEqualTo(1_000L + position);
                    }
                }
            }
        }
    }

    @Test
    void testSelectedNumericDictionaryIdsRemainPageRelativeAcrossOutputBatches()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int position = 0; position < 1_000; position++) {
            Long value = position % 97 == 0 ? null : 10_000L + (position % 211);
            rows.add(new ParquetRow(position % 37, false, value));
        }
        java.nio.file.Path file = writeParquetFile("selected-dictionary-multiple-batches.parquet", true, rows);
        assertDictionaryEncoding(file, "maybe");

        try (org.weakref.nitro.parquet.ParquetFile parquet = org.weakref.nitro.parquet.ParquetFile.open(file)) {
            org.weakref.nitro.parquet.ParquetFile.Column column = parquet.column("maybe");
            try (org.weakref.nitro.parquet.ColumnReader reader = new org.weakref.nitro.parquet.ColumnReader(
                    column.type(), column.optional(), column.typeLength(), column.decimal(), null, arrayPool, ParquetReaderPolicy.defaults())) {
                for (org.apache.parquet.format.RowGroup rowGroup : parquet.rowGroups()) {
                    reader.addChunk(parquet.data(), parquet.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
                }

                int[] survivors = {1, 2, 17, 63, 96, 97, 151, 199};
                long[] values = new long[survivors.length];
                boolean[] nulls = new boolean[survivors.length];
                for (int batch = 0; batch < 5; batch++) {
                    reader.readSelectedLongs(survivors, survivors.length, 200, values, nulls);
                    for (int index = 0; index < survivors.length; index++) {
                        int position = batch * 200 + survivors[index];
                        assertThat(nulls[index]).isEqualTo(position % 97 == 0);
                        if (position % 97 != 0) {
                            assertThat(values[index]).isEqualTo(10_000L + (position % 211));
                        }
                    }
                }
            }
        }
    }

    @Test
    void testTrinoParquetScanHonorsConstrainBeforeBorrowingPlainColumn()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("trino-plain-constrained.parquet", false, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, false, 102L),
                new ParquetRow(13, true, 103L)));

        try (TrinoParquetScanOperator operator = new TrinoParquetScanOperator(TrinoParquetScanPolicy.fromSystemProperties(), new Allocator(EngineResources.createDefault()), file, List.of("x", "maybe"))) {
            operator.next();
            Batch batch = operator.next();
            operator.constrain(Mask.sparse(new int[] {1}, 2));

            I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.values()[0]).isEqualTo(0L);
            assertThat(values.values()[1]).isEqualTo(13L);
            assertThat(nulls.values()[0]).isFalse();
            assertThat(nulls.values()[1]).isFalse();
        }
    }

    @Test
    void testTrinoParquetScanCanBorrowValuesAfterNullsAcrossConstrain()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("trino-nulls-then-values-constrained.parquet", false, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, false, null),
                new ParquetRow(13, true, 103L)));

        try (TrinoParquetScanOperator operator = new TrinoParquetScanOperator(TrinoParquetScanPolicy.fromSystemProperties(), new Allocator(EngineResources.createDefault()), file, List.of("x", "maybe"))) {
            operator.next();
            Batch batch = operator.next();

            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);
            assertThat(nulls.values()[0]).isTrue();
            assertThat(nulls.values()[1]).isFalse();

            Mask constrainedMask = Mask.sparse(new int[] {1}, 2);
            operator.constrain(constrainedMask);
            batch.constrain(constrainedMask);

            I64Vector values = (I64Vector) batch.output(1).borrow(Stream.VALUES);
            BooleanVector constrainedNulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(values.length()).isGreaterThan(0);
            assertThat(constrainedNulls.length()).isEqualTo(values.length());
            assertThat(values.values()[values.length() - 1]).isEqualTo(103L);
            assertThat(constrainedNulls.values()[constrainedNulls.length() - 1]).isFalse();
        }
    }

    @Test
    void testTrinoParquetScanReadsMultipleFiles()
            throws IOException
    {
        java.nio.file.Path first = writeParquetFile("trino-multi-1.parquet", false, List.of(
                new ParquetRow(11, true, 101L),
                new ParquetRow(12, false, null)));
        java.nio.file.Path second = writeParquetFile("trino-multi-2.parquet", false, List.of(
                new ParquetRow(13, true, 103L),
                new ParquetRow(14, false, 104L)));

        try (TrinoParquetScanOperator operator = new TrinoParquetScanOperator(TrinoParquetScanPolicy.fromSystemProperties(), new Allocator(EngineResources.createDefault()), List.of(first, second), List.of("x", "flag", "maybe"))) {
            assertThat(operator(operator))
                    .matchesExactly(List.of(
                            Row.row(11L, 1L, 101L),
                            Row.row(12L, 0L, null),
                            Row.row(13L, 1L, 103L),
                            Row.row(14L, 0L, 104L)));
        }
    }

    @Test
    void testHashJoinLateMaterializesProjectedInnerPayloadOverMultiBatchParquet()
            throws IOException
    {
        // Regression guard for deferred payload materialization over an irreversible source.
        // The inner side is a projection over a multi-file (irreversibly advancing) Trino Parquet
        // scan. Such a source reports supportsConstrainedReborrow() == false, so the join must NOT
        // defer the projected payload past the scan's advance - it must materialize eagerly. This
        // exercises the exact shape that previously crashed real Parquet joins with an
        // ArrayIndexOutOfBoundsException and confirms it now materializes correctly without crashing.
        java.nio.file.Path first = writeParquetFile("join-inner-1.parquet", false, List.of(
                new ParquetRow(1, true, 10L),
                new ParquetRow(2, true, 20L)));
        java.nio.file.Path second = writeParquetFile("join-inner-2.parquet", false, List.of(
                new ParquetRow(3, true, 30L),
                new ParquetRow(4, true, 40L)));
        java.nio.file.Path third = writeParquetFile("join-inner-3.parquet", false, List.of(
                new ParquetRow(5, true, 50L),
                new ParquetRow(6, true, 60L)));

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Allocator allocator = new Allocator(EngineResources.createDefault());

        // Project a squared payload that is only computed when the projected output is borrowed.
        Variable squaredPayload = new Variable(0);
        EvaluationPlan projectPlan = new EvaluationPlan(
                List.of(new Assignment(
                        squaredPayload,
                        new Call("multiply", List.of(
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(squaredPayload, Stream.VALUES)));

        Operator inner = new ProjectOperator(
                allocator,
                projectPlan,
                primitiveRegistry,
                new TrinoParquetScanOperator(TrinoParquetScanPolicy.fromSystemProperties(), allocator, List.of(first, second, third), List.of("x", "flag", "maybe")));

        try (Operator join = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(Row.row(2L), Row.row(5L))),
                0,
                inner,
                0)) {
            // outer key, inner key, inner squared payload
            assertThat(operator(join))
                    .matchesExactly(List.of(
                            Row.row(2L, 2L, 400L),
                            Row.row(5L, 5L, 2_500L)));
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

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("name", "payload"))) {
            Batch batch = operator.next();
            BinaryVector names = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
            BinaryVector payloads = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
            BooleanVector payloadNulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(names.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(payloads.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(utf8(names, 0)).isEqualTo("alice");
            assertThat(utf8(names, 1)).isEqualTo("bob");
            assertThat(utf8(names, 2)).isEqualTo("charlie");

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

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items"))) {
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
    void testParquetScanHonorsConstrainBeforeBorrowingArrayColumn()
            throws IOException
    {
        java.nio.file.Path file = writeRepeatedI64ParquetFile("arrays-constrained.parquet", List.of(
                new ArrayParquetRow(List.of(10L, 20L)),
                new ArrayParquetRow(List.of()),
                new ArrayParquetRow(List.of(30L)),
                new ArrayParquetRow(List.of(40L, 50L, 60L))));

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items"))) {
            Batch batch = operator.next();
            operator.constrain(Mask.sparse(new int[] {3}, 4));

            ArrayVector arrays = (ArrayVector) batch.output(0).borrow(Stream.VALUES);
            I64Vector elements = (I64Vector) arrays.elementValues();

            assertThat(arrays.length(0)).isEqualTo(0);
            assertThat(arrays.length(1)).isEqualTo(0);
            assertThat(arrays.length(2)).isEqualTo(0);
            assertThat(arrays.length(3)).isEqualTo(3);
            assertThat(elements.values()).startsWith(40L, 50L, 60L);
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

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("name", "payload"))) {
            Batch batch = operator.next();
            assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(DictionaryVector.class);
            DictionaryVector names = (DictionaryVector) batch.output(0).borrow(Stream.VALUES);
            assertThat(names.values()).isInstanceOf(BinaryVector.class);
            BinaryVector dictionaryValues = (BinaryVector) names.values();
            assertThat(dictionaryValues.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(utf8(dictionaryValues, names.ids()[0])).isEqualTo("alpha");
            assertThat(utf8(dictionaryValues, names.ids()[1])).isEqualTo("beta");
        }
    }

    @Test
    void testParquetScanFiltersCompressedDictionaryStrings()
            throws IOException
    {
        java.nio.file.Path file = writeBinaryParquetFile("compressed-dictionary-strings.parquet", true, CompressionCodecName.SNAPPY, List.of(
                new BinaryParquetRow("alpha", bytes(1)),
                new BinaryParquetRow("beta", bytes(2)),
                new BinaryParquetRow("alphabet", bytes(3)),
                new BinaryParquetRow("gamma", bytes(4))));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable literal = new Variable(0);
        Variable contains = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new org.weakref.nitro.operator.evaluator.ir.Literal("alp"), AllMask.ALL),
                new Assignment(contains, new Call("contains_utf8", List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());

        try (FilterOperator operator = new FilterOperator(
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, allocator, file, List.of("name")),
                plan,
                primitiveRegistry,
                new ReferenceMask(new Reference(contains, Stream.VALUES)),
                allocator,
                EngineResources.from(allocator).operatorResources().filter())) {
            assertThat(operator(operator))
                    .matchesExactly(List.of(
                            Row.row("alpha"),
                            Row.row("alphabet")));
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

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("left_name", "right_name"))) {
            Batch batch = operator.next();
            BinaryVector left = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
            BinaryVector right = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
            BooleanVector rightNulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(left.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(right.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();

            Streams result = eqUtf8().apply(
                    List.of(
                            Streams.ofValues(left),
                            Streams.ofValues(right).with(Stream.NULLS, rightNulls)),
                    batch.borrowMask(),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));
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

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("left_name", "right_name"))) {
            Batch batch = operator.next();
            var leftValues = batch.output(0).borrow(Stream.VALUES);
            var rightValues = batch.output(1).borrow(Stream.VALUES);
            BinaryVector left = binaryValues(leftValues);
            BinaryVector right = binaryValues(rightValues);

            assertThat(left.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(left.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY)).isFalse();
            assertThat(right.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(right.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY)).isFalse();

            Streams result = eqUtf8().apply(
                    List.of(Streams.ofValues((org.weakref.nitro.data.Vector) leftValues), Streams.ofValues((org.weakref.nitro.data.Vector) rightValues)),
                    batch.borrowMask(),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));
            BooleanVector values = (BooleanVector) result.get(Stream.VALUES);

            assertThat(values.values()[0]).isTrue();
            assertThat(values.values()[1]).isFalse();
        }
    }

    @Test
    void testTakenBinaryDictionarySurvivesLaterChunkScratchReuse()
            throws IOException
    {
        List<BinaryParquetRow> firstRows = new ArrayList<>();
        List<BinaryParquetRow> secondRows = new ArrayList<>();
        for (int position = 0; position < 12_000; position++) {
            firstRows.add(new BinaryParquetRow((position & 1) == 0 ? "alpha" : "beta", bytes(position & 0xFF)));
            secondRows.add(new BinaryParquetRow((position & 1) == 0 ? "gamma" : "delta", bytes(position & 0xFF)));
        }
        java.nio.file.Path first = writeBinaryParquetFile("taken-dictionary-first.parquet", true, firstRows);
        java.nio.file.Path second = writeBinaryParquetFile("taken-dictionary-second.parquet", true, secondRows);
        assertDictionaryEncoding(first, "name");
        assertDictionaryEncoding(second, "name");

        org.weakref.nitro.data.Vector retained;
        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(NitroParquetScanResources.createDefault(), new Allocator(EngineResources.createDefault()), List.of(first, second), List.of("name"))) {
            try (Batch firstBatch = scan.next()) {
                retained = firstBatch.output(0).take(Stream.VALUES);
            }
            try (Batch secondBatch = scan.next()) {
                secondBatch.output(0).borrow(Stream.VALUES);
            }
            try (Batch thirdBatch = scan.next()) {
                thirdBatch.output(0).borrow(Stream.VALUES);
            }

            DictionaryVector dictionary = (DictionaryVector) retained;
            BinaryVector values = (BinaryVector) dictionary.values();
            assertThat(utf8(values, dictionary.ids()[0])).isEqualTo("alpha");
            assertThat(utf8(values, dictionary.ids()[1])).isEqualTo("beta");
        }
        retained.releaseTransferredBuffers();
    }

    @Test
    void testEqualUtf8SupportsDictionaryAgainstSingleLiteral()
    {
        BinaryVector dictionaryValues = new BinaryVector(3, 32);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionaryValues.setBytes(0, "".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(1, "iphone".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(2, "nokia".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector left = DictionaryVector.wrap(new int[] {0, 1, 2, 1, 0}, dictionaryValues);
        BooleanVector leftNulls = new BooleanVector(new boolean[] {false, false, true, false, false});

        BinaryVector literal = new BinaryVector(1, 1);
        literal.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        literal.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        literal.setBytes(0, "".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        Streams result = eqUtf8().apply(
                List.of(
                        Streams.ofValues(left).with(Stream.NULLS, leftNulls),
                        Streams.ofValues(new RleVector(new int[] {5}, literal))),
                Mask.all(5),
                EnumSet.of(Stream.VALUES, Stream.NULLS),
                Streams.empty(),
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        org.weakref.nitro.data.Vector values = result.get(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);

        assertThat(values).isInstanceOf(DictionaryVector.class);
        assertThat(booleanValue(values, 0)).isTrue();
        assertThat(booleanValue(values, 1)).isFalse();
        assertThat(booleanValue(values, 2)).isFalse();
        assertThat(booleanValue(values, 3)).isFalse();
        assertThat(booleanValue(values, 4)).isTrue();

        assertThat(nulls.values()[0]).isFalse();
        assertThat(nulls.values()[1]).isFalse();
        assertThat(nulls.values()[2]).isTrue();
        assertThat(nulls.values()[3]).isFalse();
        assertThat(nulls.values()[4]).isFalse();
    }

    @Test
    void testEqualUtf8PreservesExistingValuesWhenLiteralIsNull()
    {
        BinaryVector dictionaryValues = new BinaryVector(2, 16);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionaryValues.setBytes(0, "".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(1, "iphone".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector left = DictionaryVector.wrap(new int[] {0, 1, 1, 0}, dictionaryValues);
        BooleanVector existingValues = new BooleanVector(new boolean[] {true, false, true, false});

        BinaryVector literal = new BinaryVector(1, 1);
        literal.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        literal.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        literal.setBytes(0, "".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        Streams result = eqUtf8().apply(
                List.of(
                        Streams.ofValues(left),
                        Streams.ofValues(new RleVector(new int[] {4}, literal)).with(Stream.NULLS, new BooleanVector(new boolean[] {true, true, true, true}))),
                Mask.all(4),
                EnumSet.of(Stream.VALUES, Stream.NULLS),
                Streams.ofValues(existingValues),
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(result.get(Stream.VALUES)).isSameAs(existingValues);
        assertThat(existingValues.values()).containsExactly(true, false, true, false);

        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);
        assertThat(nulls.values()).containsExactly(true, true, true, true);
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

        Allocator allocator = new Allocator(EngineResources.createDefault());
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
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, allocator, file, List.of("left_name", "right_name")),
                filterPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                allocator,
                EngineResources.from(allocator).operatorResources().filter())) {
            Batch batch = operator.next();
            assertThat(batch.borrowMask()).containsExactly(0, 3);
            var leftValues = batch.output(0).borrow(Stream.VALUES);
            assertThat(utf8(leftValues, 0)).isEqualTo("apple");
            assertThat(utf8(leftValues, 3)).isEqualTo("alpha");
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("left_name", "right_name")))) {
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("left_name", "right_name")))) {
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
                        new Allocator(EngineResources.createDefault()),
                        0,
                        List.of(
                                new First(1),
                                new CountAll()),
                        new GroupOperator(
                                new Allocator(EngineResources.createDefault()),
                                0,
                                new ProjectOperator(
                                        new Allocator(EngineResources.createDefault()),
                                        projectionPlan,
                                        primitiveRegistry,
                                        new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("name")))))))
                .matchesExactly(List.of(
                        Row.row(expectedUtf8Hash("alpha"), 2L),
                        Row.row(expectedUtf8Hash("beta"), 2L),
                        Row.row(expectedUtf8Hash("gamma"), 1L)));
    }

    @Test
    void testGroupOperatorGroupsDictionaryBackedUtf8StringsDirectly()
            throws IOException
    {
        java.nio.file.Path file = writeBinaryParquetFile("grouped-utf8-ascii-direct.parquet", true, List.of(
                new BinaryParquetRow("alpha", bytes(1)),
                new BinaryParquetRow("beta", bytes(2)),
                new BinaryParquetRow("alpha", bytes(3)),
                new BinaryParquetRow("gamma", bytes(4)),
                new BinaryParquetRow("beta", bytes(5))));

        try (GroupOperator operator = new GroupOperator(
                new Allocator(EngineResources.createDefault()),
                0,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("name")))) {
            Batch batch = operator.next();
            I64Vector groups = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            org.weakref.nitro.data.Vector names = batch.output(1).borrow(Stream.VALUES);

            assertThat(groups.values()).containsExactly(0L, 1L, 0L, 2L, 1L);
            assertThat(names).isInstanceOf(DictionaryVector.class);
            DictionaryVector dictionary = (DictionaryVector) names;
            BinaryVector values = (BinaryVector) dictionary.values();
            assertThat(values.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
        }
    }

    @Test
    void testGroupOperatorGroupsUtf8StringsDirectly()
            throws IOException
    {
        java.nio.file.Path file = writeBinaryParquetFile("grouped-utf8-direct.parquet", false, List.of(
                new BinaryParquetRow("élan", bytes(1)),
                new BinaryParquetRow("beta", bytes(2)),
                new BinaryParquetRow("élan", bytes(3)),
                new BinaryParquetRow("ångstrom", bytes(4)),
                new BinaryParquetRow("beta", bytes(5))));

        try (GroupOperator operator = new GroupOperator(
                new Allocator(EngineResources.createDefault()),
                0,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("name")))) {
            Batch batch = operator.next();
            I64Vector groups = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            BinaryVector names = binaryValues(batch.output(1).borrow(Stream.VALUES));

            assertThat(groups.values()).containsExactly(0L, 1L, 0L, 2L, 1L);
            assertThat(names.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(names.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY)).isFalse();
        }
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
                        new Allocator(EngineResources.createDefault()),
                        projectionPlan,
                        primitiveRegistry,
                        new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))))
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
                        new Allocator(EngineResources.createDefault()),
                        0,
                        List.of(
                                new First(1),
                                new CountAll()),
                        new GroupOperator(
                                new Allocator(EngineResources.createDefault()),
                                0,
                                new ProjectOperator(
                                        new Allocator(EngineResources.createDefault()),
                                        projectionPlan,
                                        primitiveRegistry,
                                        new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))))))
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

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items"))) {
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items", "index")))) {
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items", "needle")))) {
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

        Allocator allocator = new Allocator(EngineResources.createDefault());
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
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, allocator, file, List.of("items", "needle")),
                filterPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                allocator,
                EngineResources.from(allocator).operatorResources().filter())) {
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

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("person"))) {
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

            assertThat(names.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(utf8(names, 0)).isEqualTo("alice");
            assertThat(utf8(names, 2)).isEqualTo("carol");
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

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("person"))) {
            Batch batch = operator.next();
            StructVector struct = (StructVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector structNulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);

            assertThat(struct.length()).isEqualTo(3);
            assertThat(structNulls.values()).startsWith(false, true, false);

            I64Vector ids = (I64Vector) struct.fieldValues("id");
            BinaryVector names = (BinaryVector) struct.fieldValues("name");
            BooleanVector nameNulls = (BooleanVector) struct.fieldStreamOrNull("name", Stream.NULLS);

            assertThat(ids.values()).startsWith(21L, 0L, 22L);
            assertThat(utf8(names, 0)).isEqualTo("alpha");
            assertThat(nameNulls.values()).startsWith(false, false, true);
        }
    }

    @Test
    void testParquetScanHonorsConstrainBeforeBorrowingStructColumn()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalStructParquetFile("optional-structs-constrained.parquet", List.of(
                new OptionalStructParquetRow(new StructParquetRow(21, "alpha", true)),
                new OptionalStructParquetRow(null),
                new OptionalStructParquetRow(new StructParquetRow(22, null, false))));

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("person"))) {
            Batch batch = operator.next();
            operator.constrain(Mask.sparse(new int[] {2}, 3));

            StructVector struct = (StructVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector structNulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
            I64Vector ids = (I64Vector) struct.fieldValues("id");
            BinaryVector names = (BinaryVector) struct.fieldValues("name");
            BooleanVector nameNulls = (BooleanVector) struct.fieldStreamOrNull("name", Stream.NULLS);
            BooleanVector active = (BooleanVector) struct.fieldValues("active");

            assertThat(ids.values()[0]).isEqualTo(0L);
            assertThat(ids.values()[1]).isEqualTo(0L);
            assertThat(ids.values()[2]).isEqualTo(22L);
            assertThat(names.length(2)).isEqualTo(0);
            assertThat(nameNulls.values()[2]).isTrue();
            assertThat(active.values()[2]).isFalse();
            assertThat(structNulls.values()[2]).isFalse();
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

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items"))) {
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

            assertThat(keys.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(utf8(keys, 0)).isEqualTo("alpha");
            assertThat(utf8(keys, 1)).isEqualTo("beta");
            assertThat(utf8(keys, 2)).isEqualTo("gamma");

            assertThat(values.values()).startsWith(10L, 0L, 30L);
            assertThat(valueNulls.values()).startsWith(false, true, false);
        }
    }

    @Test
    void testParquetScanHonorsConstrainBeforeBorrowingMapColumn()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalMapParquetFile("optional-maps-constrained.parquet", List.of(
                new MapParquetRow(orderedMap("alpha", 10L, "beta", null)),
                new MapParquetRow(null),
                new MapParquetRow(Map.of()),
                new MapParquetRow(orderedMap("gamma", 30L))));

        try (ParquetScanOperator operator = new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items"))) {
            Batch batch = operator.next();
            operator.constrain(Mask.sparse(new int[] {3}, 4));

            MapVector maps = (MapVector) batch.output(0).borrow(Stream.VALUES);
            BinaryVector keys = (BinaryVector) maps.keyValues();
            I64Vector values = (I64Vector) maps.valueValues();
            BooleanVector mapNulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);

            assertThat(maps.length(0)).isEqualTo(0);
            assertThat(maps.length(1)).isEqualTo(0);
            assertThat(maps.length(2)).isEqualTo(0);
            assertThat(maps.length(3)).isEqualTo(1);
            assertThat(utf8(keys, 0)).isEqualTo("gamma");
            assertThat(values.values()[0]).isEqualTo(30L);
            assertThat(mapNulls.values()[3]).isFalse();
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
                        new StructField(new Reference(new Input(0), Stream.VALUES), 1),
                        AllMask.ALL)),
                List.of(
                        new Reference(name, Stream.VALUES),
                        new Reference(name, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("person")))) {
            Batch batch = operator.next();
            BinaryVector names = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(utf8(names, 0)).isEqualTo("alice");
            assertThat(utf8(names, 3)).isEqualTo("carol");
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

        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable active = new Variable(0);
        EvaluationPlan filterPlan = new EvaluationPlan(
                List.of(new Assignment(
                        active,
                        new StructField(new Reference(new Input(0), Stream.VALUES), 2),
                        AllMask.ALL)),
                List.of());

        try (FilterOperator operator = new FilterOperator(
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, allocator, file, List.of("person")),
                filterPlan,
                primitiveRegistry,
                new Reference(active, Stream.VALUES),
                allocator,
                EngineResources.from(allocator).operatorResources().filter())) {
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

        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable id = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        id,
                        new StructField(new Reference(new Input(0), Stream.VALUES), 0),
                        AllMask.ALL)),
                List.of(
                        new Reference(id, Stream.VALUES),
                        new Reference(id, Stream.VALUES)));

        assertThat(operator(
                new GroupedAggregationOperator(
                        allocator,
                        0,
                        List.of(
                                new First(1),
                                new CountAll()),
                        new GroupOperator(
                                allocator,
                                0,
                                new ProjectOperator(
                                        allocator,
                                        projectionPlan,
                                        primitiveRegistry,
                                        new FilterOperator(
                                                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, allocator, file, List.of("person")),
                                                new EvaluationPlan(List.of(), List.of()),
                                                primitiveRegistry,
                                                new NotMask(new ReferenceMask(new Reference(new Input(0), Stream.NULLS))),
                                                allocator,
                                                EngineResources.from(allocator).operatorResources().filter()))))))
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))) {
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))) {
            Batch batch = operator.next();
            ArrayVector arrays = (ArrayVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);
            BinaryVector keysVector = (BinaryVector) arrays.elementValues();

            assertThat(arrays.length(0)).isEqualTo(2);
            assertThat(arrays.length(1)).isEqualTo(0);
            assertThat(arrays.length(2)).isEqualTo(0);
            assertThat(arrays.length(3)).isEqualTo(1);
            assertThat(keysVector.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(utf8(keysVector, 0)).isEqualTo("alpha");
            assertThat(utf8(keysVector, 1)).isEqualTo("beta");
            assertThat(utf8(keysVector, 2)).isEqualTo("gamma");
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))) {
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))) {
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))) {
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))) {
            Batch batch = operator.next();
            ArrayVector arrays = (ArrayVector) batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);
            BinaryVector values = (BinaryVector) arrays.elementValues();
            BooleanVector elementNulls = arrays.elementNulls();

            assertThat(arrays.length(0)).isEqualTo(2);
            assertThat(arrays.length(1)).isEqualTo(0);
            assertThat(arrays.length(2)).isEqualTo(0);
            assertThat(arrays.length(3)).isEqualTo(1);
            assertThat(values.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
            assertThat(utf8(values, 0)).isEqualTo("one");
            assertThat(utf8(values, 2)).isEqualTo("three");
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))) {
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
                        new Allocator(EngineResources.createDefault()),
                        0,
                        List.of(
                                new First(1),
                                new CountAll()),
                        new GroupOperator(
                                new Allocator(EngineResources.createDefault()),
                                0,
                                new ProjectOperator(
                                        new Allocator(EngineResources.createDefault()),
                                        projectionPlan,
                                        primitiveRegistry,
                                        new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))))))
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items", "needle")))) {
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

        Allocator allocator = new Allocator(EngineResources.createDefault());
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
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, allocator, file, List.of("items", "needle")),
                filterPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                allocator,
                EngineResources.from(allocator).operatorResources().filter())) {
            Batch batch = operator.next();
            assertThat(batch.borrowMask()).containsExactly(0, 4);

            org.weakref.nitro.data.Vector needles = batch.output(1).borrow(Stream.VALUES);
            assertThat(utf8(needles, 0)).isEqualTo("alpha");
            assertThat(utf8(needles, 4)).isEqualTo("delta");
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items", "needle")))) {
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items", "needle")))) {
            Batch batch = operator.next();
            org.weakref.nitro.data.Vector values = batch.output(0).borrow(Stream.VALUES);
            BooleanVector nulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);

            assertThat(utf8(values, 0)).isEqualTo("one");
            assertThat(utf8(values, 5)).isEqualTo("five");
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
                        new Allocator(EngineResources.createDefault()),
                        projectionPlan,
                        primitiveRegistry,
                        new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))))
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
                new Allocator(EngineResources.createDefault()),
                projectionPlan,
                primitiveRegistry,
                new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))) {
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
                        new Allocator(EngineResources.createDefault()),
                        0,
                        List.of(
                                new First(1),
                                new CountAll()),
                        new GroupOperator(
                                new Allocator(EngineResources.createDefault()),
                                0,
                                new ProjectOperator(
                                        new Allocator(EngineResources.createDefault()),
                                        projectionPlan,
                                        primitiveRegistry,
                                        new ParquetScanOperator(LEGACY_PARQUET_SCAN_BATCH_POLICY, new Allocator(EngineResources.createDefault()), file, List.of("items")))))))
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

    private record TestingTypeBinding(TypeIdentity identity, Class<?> carrierType)
            implements TypeBinding
    {
        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }
    }

    private ColumnReader columnReader(List<ParquetFile> files, String columnName)
    {
        ParquetFile.Column first = files.getFirst().column(columnName);
        ColumnReader reader = new ColumnReader(
                first.type(),
                first.optional(),
                first.typeLength(),
                first.decimal(),
                null,
                arrayPool,
                ParquetReaderPolicy.defaults());
        for (ParquetFile file : files) {
            ParquetFile.Column column = file.column(columnName);
            for (org.apache.parquet.format.RowGroup rowGroup : file.rowGroups()) {
                reader.addChunk(file.data(), file.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
            }
        }
        return reader;
    }

    private java.nio.file.Path writeWideNumericParquetFile(String name, List<WideNumericRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        MessageType schema = Types.buildMessage()
                .required(INT64).named("key")
                .required(INT64).named("p1")
                .required(INT64).named("p2")
                .required(INT64).named("p3")
                .required(INT64).named("p4")
                .named("nitro_wide_numeric_test");
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withDictionaryEncoding(true)
                .build()) {
            for (WideNumericRow row : rows) {
                writer.write(groups.newGroup()
                        .append("key", row.key())
                        .append("p1", row.p1())
                        .append("p2", row.p2())
                        .append("p3", row.p3())
                        .append("p4", row.p4()));
            }
        }
        return file;
    }

    private java.nio.file.Path writeBinaryParquetFile(String name, boolean dictionaryEnabled, List<BinaryParquetRow> rows)
            throws IOException
    {
        return writeBinaryParquetFile(name, dictionaryEnabled, CompressionCodecName.UNCOMPRESSED, rows);
    }

    private java.nio.file.Path writeBinaryParquetFile(String name, boolean dictionaryEnabled, CompressionCodecName compressionCodecName, List<BinaryParquetRow> rows)
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
                .withCompressionCodec(compressionCodecName)
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

    private static boolean booleanValue(org.weakref.nitro.data.Vector values, int position)
    {
        return switch (values) {
            case BooleanVector vector -> vector.values()[position];
            case DictionaryVector vector -> ((BooleanVector) vector.values()).values()[vector.ids()[position]];
            case RleVector vector -> ((BooleanVector) vector.values()).values()[vector.runIndex(position)];
            default -> throw new IllegalArgumentException("Expected boolean-backed vector but got " + values.getClass().getSimpleName());
        };
    }

    private static String utf8(org.weakref.nitro.data.Vector values, int position)
    {
        return switch (values) {
            case BinaryVector vector -> new String(vector.copyBytes(position), java.nio.charset.StandardCharsets.UTF_8);
            case DictionaryVector vector -> utf8(vector.values(), vector.ids()[position]);
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

    private record WideNumericRow(long key, long p1, long p2, long p3, long p4) {}

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
