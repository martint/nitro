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

import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.weakref.nitro.clickbench.ClickBenchHitsSupport;
import org.weakref.nitro.core.function.VersionedLongPredicate;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.DomainCapability;
import org.weakref.nitro.core.source.LongDomainCapability;
import org.weakref.nitro.core.source.RuntimeFilter;
import org.weakref.nitro.core.source.RuntimeFilterAcceptance;
import org.weakref.nitro.core.source.SourceMetrics;
import org.weakref.nitro.core.source.SourceMetricsProtocol;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.source.TypedDomain;
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
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.operator.source.BatchSourceOperator;
import org.weakref.nitro.operator.source.compatibility.NativeSourceOperatorIngress;
import org.weakref.nitro.operator.source.compatibility.OperatorBatchSource;
import org.weakref.nitro.operator.source.compatibility.parquet.NitroParquetScanOperator;
import org.weakref.nitro.parquet.ColumnReader;
import org.weakref.nitro.parquet.DecompressedPageCachePolicy;
import org.weakref.nitro.parquet.NativeParquetTestFileWriter.LongStringStruct;
import org.weakref.nitro.parquet.NitroParquetBatchSource;
import org.weakref.nitro.parquet.NitroParquetScanResources;
import org.weakref.nitro.parquet.ParquetColumnNameMatching;
import org.weakref.nitro.parquet.ParquetDecodeScratchPolicy;
import org.weakref.nitro.parquet.ParquetDictionaryFilterPolicy;
import org.weakref.nitro.parquet.ParquetFile;
import org.weakref.nitro.parquet.ParquetFilterEvaluationPolicy;
import org.weakref.nitro.parquet.ParquetFilterWindowPolicy;
import org.weakref.nitro.parquet.ParquetFilteredPayloadPolicy;
import org.weakref.nitro.parquet.ParquetInput;
import org.weakref.nitro.parquet.ParquetInputRange;
import org.weakref.nitro.parquet.ParquetLateMaterializationPolicy;
import org.weakref.nitro.parquet.ParquetMaterializationPolicy;
import org.weakref.nitro.parquet.ParquetNumericDecodeAdmissionPolicy;
import org.weakref.nitro.parquet.ParquetNumericDecodePolicy;
import org.weakref.nitro.parquet.ParquetPageNavigationPolicy;
import org.weakref.nitro.parquet.ParquetProgressiveFilterCompactionPolicy;
import org.weakref.nitro.parquet.ParquetReaderDiagnostics;
import org.weakref.nitro.parquet.ParquetReaderPolicy;
import org.weakref.nitro.parquet.ParquetRuntimeFilterPolicy;
import org.weakref.nitro.parquet.ParquetScanBatchPolicy;
import org.weakref.nitro.parquet.ParquetScanDiagnostics;
import org.weakref.nitro.parquet.RleReaderPolicy;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.Column.optionalBinary;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.Column.optionalInt32;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.Column.optionalInt64;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.Column.requiredBinary;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.Column.requiredBoolean;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.Column.requiredInt32;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.Column.requiredInt64;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.write;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.writeOptionalLongUtf8Struct;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.writeOptionalUtf8LongMap;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.writeRepeatedOptionalInt64;

public class TestParquetOperator
{
    private record Range(long offset, int length) {}

    private static NitroParquetScanResources executableRuntimeFilterResources()
    {
        return NitroParquetScanResources.createDefault(
                org.weakref.nitro.parquet.ParquetArenaPolicy.confined(),
                new ParquetRuntimeFilterPolicy(true, true, true, 8_096));
    }

    private static final TypeBinding BIGINT = new TestingTypeBinding(new TypeIdentity("testing:bigint"), long.class);
    private static final TypeBinding VARCHAR = new TestingTypeBinding(new TypeIdentity("testing:varchar"), byte[].class);
    private static final TypeBinding BIGINT_ARRAY = new TypeBinding()
    {
        @Override
        public TypeIdentity identity()
        {
            return new TypeIdentity("testing:array(bigint)");
        }

        @Override
        public Class<?> carrierType()
        {
            return List.class;
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }

        @Override
        public Set<Class<? extends org.weakref.nitro.data.Vector>> supportedVectorTypes()
        {
            return Set.of(ArrayVector.class);
        }
    };
    private static final TypeBinding BIGINT_VARCHAR_ROW = new TypeBinding()
    {
        @Override
        public TypeIdentity identity()
        {
            return new TypeIdentity("testing:row(id bigint,name varchar)");
        }

        @Override
        public Class<?> carrierType()
        {
            return Row.class;
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }

        @Override
        public Set<Class<? extends org.weakref.nitro.data.Vector>> supportedVectorTypes()
        {
            return Set.of(StructVector.class);
        }
    };
    private static final TypeBinding VARCHAR_BIGINT_MAP = new TypeBinding()
    {
        @Override
        public TypeIdentity identity()
        {
            return new TypeIdentity("testing:map(varchar,bigint)");
        }

        @Override
        public Class<?> carrierType()
        {
            return Map.class;
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }

        @Override
        public Set<Class<? extends org.weakref.nitro.data.Vector>> supportedVectorTypes()
        {
            return Set.of(MapVector.class);
        }
    };
    private static final ParquetPageNavigationPolicy GENERIC_PAGE_NAVIGATION =
            new ParquetPageNavigationPolicy(false, 0, 101, Integer.MAX_VALUE, false, 0, Integer.MAX_VALUE, false);
    private static final ParquetMaterializationPolicy GENERIC_MATERIALIZATION =
            new ParquetMaterializationPolicy(
                    false, false, false, false, false, false, Long.MAX_VALUE, false, 0, false, false, false, false);
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
                    0,
                    1,
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
                            false, 0, 1 << 19, false),
                    false);

    private static final ParquetFilterEvaluationPolicy GENERIC_FILTER_EVALUATION =
            new ParquetFilterEvaluationPolicy(
                    new ParquetFilterEvaluationPolicy.Ordering(false, false, 0),
                    new ParquetFilterEvaluationPolicy.NonSelectiveElision(false, false),
                    new ParquetFilterEvaluationPolicy.DirectNullMask(false, false));
    private final PrimitiveArrayPool arrayPool = EngineResources.createDefault().primitiveArrays();

    @TempDir
    java.nio.file.Path tempDirectory;

    @Test
    void testFilteredPayloadDecodeAdmissionAccountsForSurvivorRuns()
    {
        ParquetFilteredPayloadPolicy policy = ParquetFilteredPayloadPolicy.defaults();

        assertThat(policy.useBulkDecode(sequence(0, 2, 30), 30, 100)).isTrue();
        assertThat(policy.useBulkDecode(sequence(20, 1, 30), 30, 100)).isFalse();
        assertThat(policy.useBulkDecode(sequence(0, 10, 10), 10, 100)).isFalse();
        assertThat(policy.useBulkDecode(sequence(0, 1, 60), 60, 100)).isTrue();
    }

    @Test
    void testDefaultNarrowFilterWindowDoesNotWidenScratch()
    {
        ParquetFilterWindowPolicy policy = ParquetFilterWindowPolicy.defaults();

        assertThat(policy.adaptiveNarrow().rows()).isEqualTo(policy.rows());
    }

    private static int[] sequence(int start, int stride, int count)
    {
        int[] values = new int[count];
        for (int index = 0; index < count; index++) {
            values[index] = start + index * stride;
        }
        return values;
    }

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
                                        GENERIC_DICTIONARY_FILTER,
                                        ParquetDecodeScratchPolicy.defaults()),
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
            SourceMetrics metrics = source.protocol(SourceMetricsProtocol.METRICS).orElseThrow();
            assertThat(metrics.completedBytes()).isEmpty();
            assertThat(metrics.completedPositions()).hasValue(0);
            assertThat(metrics.readTimeNanos()).isEmpty();

            SourcePoll.Ready ready = (SourcePoll.Ready) source.poll();
            var batch = ready.batch();
            batch.select(new org.weakref.nitro.data.MaskSelection(Mask.sparse(new int[] {0, 2}, 3)));
            assertThat(((I64Vector) batch.column(0).borrow(Stream.VALUES)).values())
                    .startsWith(100L, 0L, 300L);
            assertThat(((BooleanVector) batch.column(0).borrow(Stream.NULLS)).values())
                    .startsWith(false, true, false);
            batch.close();

            assertThat(metrics.completedBytes().orElseThrow()).isPositive();
            assertThat(metrics.completedPositions()).hasValue(3);
            assertThat(source.poll()).isSameAs(SourcePoll.Finished.FINISHED);
        }
    }

    @Test
    void testNitroParquetSourceReadsConnectorSuppliedRanges()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("nitro-range-input.parquet", true, List.of(
                new ParquetRow(10, true, 100L),
                new ParquetRow(20, true, 200L)));
        byte[] bytes = Files.readAllBytes(file);
        Range valuesRange;
        Range deferredRange;
        try (ParquetFile parquetFile = ParquetFile.open(file)) {
            var rowGroup = parquetFile.rowGroups().getFirst();
            var valuesMetadata = parquetFile.columnChunk(rowGroup, parquetFile.column("x")).meta_data;
            var deferredMetadata = parquetFile.columnChunk(rowGroup, parquetFile.column("maybe")).meta_data;
            valuesRange = new Range(columnChunkStart(valuesMetadata), toIntExact(valuesMetadata.total_compressed_size));
            deferredRange = new Range(columnChunkStart(deferredMetadata), toIntExact(deferredMetadata.total_compressed_size));
        }
        List<Range> reads = new ArrayList<>();
        List<Range> released = new ArrayList<>();
        AtomicBoolean closed = new AtomicBoolean();
        ParquetInput input = new ParquetInput()
        {
            @Override
            public String id()
            {
                return "test://nitro-range-input.parquet";
            }

            @Override
            public long size()
            {
                return bytes.length;
            }

            @Override
            public ParquetInputRange readRange(long offset, int length)
            {
                Range range = new Range(offset, length);
                reads.add(range);
                return new ParquetInputRange(
                        MemorySegment.ofArray(bytes).asSlice(offset, length),
                        () -> released.add(range));
            }

            @Override
            public void close()
            {
                closed.set(true);
            }
        };
        Schema schema = new Schema(List.of(
                new Field("x", BIGINT, false),
                new Field("maybe", BIGINT, true)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                BatchSource source = NitroParquetBatchSource.forInputs(
                        NitroParquetScanResources.createDefault(),
                        allocator,
                        List.of(new NitroParquetBatchSource.InputSplit(input, 0, bytes.length)),
                        schema)) {
            assertThat(released).containsAll(reads);
            SourcePoll.Ready ready = (SourcePoll.Ready) source.poll();
            VectorAccess.LongValues values = VectorAccess.longValues(ready.batch().column(0).borrow(Stream.VALUES));
            assertThat(new long[] {values.value(0), values.value(1)})
                    .containsExactly(10L, 20L);
            ready.batch().close();
            assertThat(reads).noneMatch(range -> range.offset() == 0 && range.length() == bytes.length);
            assertThat(reads).anyMatch(range -> range.offset() == bytes.length - 8 && range.length() == 8);
            assertThat(reads).contains(valuesRange);
            assertThat(reads).doesNotContain(deferredRange);
        }
        assertThat(closed).isTrue();
        assertThat(released).containsExactlyInAnyOrderElementsOf(reads);
    }

    @Test
    void testNativeNestedMapSourceHonorsSelectionBeforeMaterialization()
            throws IOException
    {
        java.nio.file.Path file = writeOptionalMapParquetFile("native-map.parquet", List.of(
                new MapParquetRow(null),
                new MapParquetRow(Map.of()),
                new MapParquetRow(orderedMap("alpha", 1L, "beta", null)),
                new MapParquetRow(orderedMap("gamma", 3L))));
        byte[] bytes = Files.readAllBytes(file);
        ParquetInput input = new ParquetInput()
        {
            @Override
            public String id()
            {
                return "native-map";
            }

            @Override
            public long size()
            {
                return bytes.length;
            }

            @Override
            public ParquetInputRange readRange(long offset, int length)
            {
                return ParquetInputRange.retained(MemorySegment.ofArray(bytes).asSlice(offset, length));
            }

            @Override
            public void close() {}
        };
        Schema schema = new Schema(List.of(new Field("items", VARCHAR_BIGINT_MAP, true)));

        try (NitroParquetScanResources resources = NitroParquetScanResources.createDefault();
                AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                BatchSource source = NitroParquetBatchSource.forInputs(
                        resources,
                        allocator,
                        List.of(new NitroParquetBatchSource.InputSplit(input, 0, bytes.length)),
                        schema)) {
            try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                batch.select(new org.weakref.nitro.data.MaskSelection(Mask.sparse(new int[] {2}, 4)));
                MapVector maps = (MapVector) batch.column(0).borrow(Stream.VALUES);
                BinaryVector keys = (BinaryVector) maps.keyValues();
                I64Vector values = (I64Vector) maps.valueValues();
                BooleanVector valueNulls = (BooleanVector) maps.valueStreamOrNull(Stream.NULLS);

                assertThat(maps.offsets()).containsExactly(0, 0, 0, 2, 2);
                assertThat(utf8(keys, 0)).isEqualTo("alpha");
                assertThat(utf8(keys, 1)).isEqualTo("beta");
                assertThat(values.values()).containsExactly(1L, 0L);
                assertThat(valueNulls.values()).containsExactly(false, true);
            }
            assertThat(source.poll()).isSameAs(SourcePoll.Finished.FINISHED);
        }
    }

    private static long columnChunkStart(ColumnMetaData metadata)
    {
        return metadata.dictionary_page_offset > 0 ? metadata.dictionary_page_offset : metadata.data_page_offset;
    }

    @Test
    void testNitroParquetSourceCarriesPhysicalNullFreeProofIntoNullableSchema()
            throws IOException
    {
        List<BinaryParquetRow> rows = new ArrayList<>();
        for (int position = 0; position < 1_024; position++) {
            rows.add(new BinaryParquetRow("name-" + (position & 3), bytes(1 + (position & 1), 2)));
        }
        java.nio.file.Path file = writeBinaryParquetFile("nitro-optional-binary-null-proof.parquet", true, rows);
        assertDictionaryEncoding(file, "payload");
        Schema schema = new Schema(List.of(new Field("payload", VARCHAR, true)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        NitroParquetScanResources.createDefault(),
                        allocator,
                        List.of(file),
                        schema);
                var batch = ((SourcePoll.Ready) source.poll()).batch()) {
            assertThat(batch.column(0).borrow(Stream.VALUES)).isInstanceOf(DictionaryVector.class);
            BooleanVector nulls = (BooleanVector) batch.column(0).borrow(Stream.NULLS);
            assertThat(nulls.isAllFalse()).isTrue();
        }
    }

    @Test
    void testNitroParquetSourceReadsLegacyRepeatedListNatively()
            throws IOException
    {
        java.nio.file.Path file = writeRepeatedNullableI64ParquetFile("native-array.parquet", List.of(
                new NullableArrayParquetRow(java.util.Arrays.asList(10L, null, 20L)),
                new NullableArrayParquetRow(List.of()),
                new NullableArrayParquetRow(List.of(30L))));
        byte[] bytes = Files.readAllBytes(file);
        ParquetInput input = new ParquetInput()
        {
            @Override
            public String id()
            {
                return "native-array";
            }

            @Override
            public long size()
            {
                return bytes.length;
            }

            @Override
            public ParquetInputRange readRange(long offset, int length)
            {
                return ParquetInputRange.retained(MemorySegment.ofArray(bytes).asSlice(offset, length));
            }

            @Override
            public void close() {}
        };
        Schema schema = new Schema(List.of(new Field("items", BIGINT_ARRAY, false)));

        try (NitroParquetScanResources resources = NitroParquetScanResources.createDefault();
                AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                BatchSource source = NitroParquetBatchSource.forInputs(
                        resources,
                        allocator,
                        List.of(new NitroParquetBatchSource.InputSplit(input, 0, bytes.length)),
                        schema);
                var batch = ((SourcePoll.Ready) source.poll()).batch()) {
            ArrayVector arrays = (ArrayVector) batch.column(0).borrow(Stream.VALUES);
            I64Vector values = (I64Vector) arrays.elementValues();
            BooleanVector nulls = arrays.elementNulls();

            assertThat(arrays.offsets()).containsExactly(0, 3, 3, 4);
            assertThat(values.values()).containsExactly(10, 0, 20, 30);
            assertThat(nulls.values()).containsExactly(false, true, false, false);
        }
    }

    @Test
    void testNitroParquetSourceReadsOptionalStructNatively()
            throws Exception
    {
        java.nio.file.Path file = writeOptionalSimpleStructParquetFile("native-struct.parquet");
        byte[] bytes = Files.readAllBytes(file);
        ParquetInput input = new ParquetInput()
        {
            @Override
            public String id()
            {
                return "native-struct";
            }

            @Override
            public long size()
            {
                return bytes.length;
            }

            @Override
            public ParquetInputRange readRange(long offset, int length)
            {
                return ParquetInputRange.retained(MemorySegment.ofArray(bytes).asSlice(offset, length));
            }

            @Override
            public void close() {}
        };
        Schema schema = new Schema(List.of(new Field("person", BIGINT_VARCHAR_ROW, true)));

        try (NitroParquetScanResources resources = NitroParquetScanResources.createDefault();
                AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                BatchSource source = NitroParquetBatchSource.forInputs(
                        resources,
                        allocator,
                        List.of(new NitroParquetBatchSource.InputSplit(input, 0, bytes.length)),
                        schema);
                var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
                var batch = executor.submit(() -> ((SourcePoll.Ready) source.poll()).batch()).get()) {
            StructVector rows = (StructVector) batch.column(0).borrow(Stream.VALUES);
            BooleanVector rowNulls = (BooleanVector) batch.column(0).borrow(Stream.NULLS);
            I64Vector ids = (I64Vector) rows.fieldValues("id");
            BinaryVector names = (BinaryVector) rows.fieldValues("name");

            assertThat(rowNulls.values()).containsExactly(false, true, false);
            assertThat(ids.values()).containsExactly(11, 0, 12);
            assertThat(utf8(names, 0)).isEqualTo("alice");
            assertThat(((BooleanVector) rows.field("name").get(Stream.NULLS)).values())
                    .containsExactly(false, true, true);
        }
    }

    @Test
    void testNitroParquetSourcePreservesNumericDictionaryEncoding()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int position = 0; position < 2_048; position++) {
            rows.add(new ParquetRow((position % 3) * 10, true, (long) (position % 5) * 100));
        }
        java.nio.file.Path file = writeParquetFile("nitro-native-numeric-dictionary.parquet", true, rows);
        assertDictionaryEncoding(file, "x");
        assertDictionaryEncoding(file, "maybe");
        Schema schema = new Schema(List.of(
                new Field("x", BIGINT, false),
                new Field("maybe", BIGINT, true)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        NitroParquetScanResources.createDefault(),
                        allocator,
                        List.of(file),
                        schema)) {
            SourcePoll.Ready ready = (SourcePoll.Ready) source.poll();
            var batch = ready.batch();
            DictionaryVector required = (DictionaryVector) batch.column(0).borrow(Stream.VALUES);
            DictionaryVector optional = (DictionaryVector) batch.column(1).borrow(Stream.VALUES);

            assertThat(required.values()).isInstanceOf(I64Vector.class);
            assertThat(optional.values()).isInstanceOf(I64Vector.class);
            long[] requiredValues = ((I64Vector) required.values()).values();
            long[] optionalValues = ((I64Vector) optional.values()).values();
            for (int position = 0; position < required.length(); position++) {
                assertThat(requiredValues[required.ids()[position]]).isEqualTo((position % 3) * 10L);
                assertThat(optionalValues[optional.ids()[position]]).isEqualTo((position % 5) * 100L);
            }
            batch.close();
        }
    }

    @Test
    void testNitroParquetSourceReportsOnlyConsumedLazyColumnPages()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int value = 0; value < 1_000; value++) {
            rows.add(new ParquetRow(value, true, (long) value * 17));
        }
        java.nio.file.Path file = writeParquetFile("nitro-consumed-page-metrics.parquet", false, rows);
        Schema schema = new Schema(List.of(
                new Field("x", BIGINT, false),
                new Field("maybe", BIGINT, true)));

        long keyOnlyBytes;
        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        NitroParquetScanResources.createDefault(),
                        allocator,
                        List.of(file),
                        schema)) {
            SourceMetrics metrics = source.protocol(SourceMetricsProtocol.METRICS).orElseThrow();
            SourcePoll.Ready ready = (SourcePoll.Ready) source.poll();
            ready.batch().column(0).borrow(Stream.VALUES);
            ready.batch().close();
            keyOnlyBytes = metrics.completedBytes().orElseThrow();
        }

        long allColumnBytes;
        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        NitroParquetScanResources.createDefault(),
                        allocator,
                        List.of(file),
                        schema)) {
            SourceMetrics metrics = source.protocol(SourceMetricsProtocol.METRICS).orElseThrow();
            SourcePoll.Ready ready = (SourcePoll.Ready) source.poll();
            ready.batch().column(0).borrow(Stream.VALUES);
            ready.batch().column(1).borrow(Stream.VALUES);
            ready.batch().close();
            allColumnBytes = metrics.completedBytes().orElseThrow();
        }

        assertThat(keyOnlyBytes).isPositive();
        assertThat(allColumnBytes).isGreaterThan(keyOnlyBytes);
    }

    @Test
    void testNitroParquetSourceGrowsBatchAfterSustainedSparseDownstreamSelection()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int value = 0; value < 16; value++) {
            rows.add(new ParquetRow(value, true, (long) value));
        }
        java.nio.file.Path file = writeParquetFile("nitro-adaptive-source-batches.parquet", true, rows);
        Schema schema = new Schema(List.of(new Field("maybe", BIGINT, true)));
        NitroParquetScanResources resources = NitroParquetScanResources.createDefault(
                org.weakref.nitro.parquet.ParquetArenaPolicy.confined(),
                ParquetRuntimeFilterPolicy.defaults(),
                new ParquetScanBatchPolicy(4, 8, 8, 0.25));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(resources, allocator, List.of(file), schema)) {
            for (int batchIndex = 0; batchIndex < 2; batchIndex++) {
                try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                    assertThat(batch.selection().positionCount()).isEqualTo(4);
                    batch.select(new org.weakref.nitro.data.MaskSelection(Mask.sparse(new int[] {0}, 4)));
                }
            }
            try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                assertThat(batch.selection().positionCount()).isEqualTo(8);
            }
            assertThat(source.poll()).isSameAs(SourcePoll.Finished.FINISHED);
        }
    }

    @Test
    void testNitroParquetSourceGrowsInternalBatchOnlyWhileSelectionRemainsDense()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int value = 0; value < 16; value++) {
            rows.add(new ParquetRow(value, true, (long) value));
        }
        java.nio.file.Path file = writeParquetFile("nitro-adaptive-internal-batches.parquet", true, rows);
        Schema schema = new Schema(List.of(new Field("maybe", BIGINT, true)));
        NitroParquetScanResources resources = NitroParquetScanResources.createDefault(
                org.weakref.nitro.parquet.ParquetArenaPolicy.confined(),
                ParquetRuntimeFilterPolicy.defaults(),
                new ParquetScanBatchPolicy(4, 8, 8, ParquetScanBatchPolicy.AdaptiveGrowth.DENSE, 1.0, Long.MAX_VALUE));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(resources, allocator, List.of(file), schema)) {
            for (int batchIndex = 0; batchIndex < 2; batchIndex++) {
                try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                    assertThat(batch.selection().positionCount()).isEqualTo(4);
                }
            }
            try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                assertThat(batch.selection().positionCount()).isEqualTo(8);
            }
            assertThat(source.poll()).isSameAs(SourcePoll.Finished.FINISHED);
        }

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(resources, allocator, List.of(file), schema)) {
            for (int batchIndex = 0; batchIndex < 2; batchIndex++) {
                try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                    batch.select(new org.weakref.nitro.data.MaskSelection(Mask.sparse(new int[] {0, 1, 2}, 4)));
                }
            }
            try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                assertThat(batch.selection().positionCount()).isEqualTo(4);
            }
        }
    }

    @Test
    void testNitroParquetSourceBoundsAdaptiveBatchByProjectedWidth()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int value = 0; value < 16; value++) {
            rows.add(new ParquetRow(value, true, (long) value));
        }
        java.nio.file.Path file = writeParquetFile("nitro-width-bounded-adaptive-source-batches.parquet", true, rows);
        Schema schema = new Schema(List.of(new Field("maybe", BIGINT, true)));
        NitroParquetScanResources resources = NitroParquetScanResources.createDefault(
                org.weakref.nitro.parquet.ParquetArenaPolicy.confined(),
                ParquetRuntimeFilterPolicy.defaults(),
                new ParquetScanBatchPolicy(4, 12, 8, 0.25, 16));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(resources, allocator, List.of(file), schema)) {
            for (int batchIndex = 0; batchIndex < 2; batchIndex++) {
                try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                    assertThat(batch.selection().positionCount()).isEqualTo(4);
                    batch.select(new org.weakref.nitro.data.MaskSelection(Mask.sparse(new int[] {0}, 4)));
                }
            }
            try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                // The value and nullable streams consume two vectors, so a sixteen-cell budget caps growth at eight rows.
                assertThat(batch.selection().positionCount()).isEqualTo(8);
            }
            assertThat(source.poll()).isSameAs(SourcePoll.Finished.FINISHED);
        }
    }

    @Test
    void testNitroParquetSourceLeavesNullInclusiveLongDomainAsResidual()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("nitro-null-inclusive-domain.parquet", true, List.of(
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
            assertThat(source.addRuntimeFilter(new RuntimeFilter(
                    source.column(0),
                    new TestingTypedLongDomain(source.column(0).type(), DynamicFilter.fromRange(0, 1, 0), true),
                    false)))
                    .isEqualTo(RuntimeFilterAcceptance.ACCEPTED_WITH_RESIDUAL);

            try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                assertThat(batch.selection().count()).isEqualTo(3);
                assertThat(((BooleanVector) batch.column(0).borrow(Stream.NULLS)).values())
                        .startsWith(false, true, false);
            }
            assertThat(source.poll()).isSameAs(SourcePoll.Finished.FINISHED);
        }
    }

    @Test
    void testNitroParquetSourceEnforcesExactNonNullLongDomain()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("nitro-enforced-domain.parquet", true, List.of(
                new ParquetRow(10, true, 100L),
                new ParquetRow(20, false, null),
                new ParquetRow(30, true, 300L)));
        Schema schema = new Schema(List.of(new Field("maybe", BIGINT, true)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        executableRuntimeFilterResources(),
                        allocator,
                        List.of(file),
                        schema)) {
            assertThat(source.addRuntimeFilter(new RuntimeFilter(
                    source.column(0),
                    new TestingTypedLongDomain(source.column(0).type(), DynamicFilter.fromRange(0, 300, 300)),
                    false).withoutResidual()))
                    .isEqualTo(RuntimeFilterAcceptance.ENFORCED);

            try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                assertThat(batch.selection().count()).isEqualTo(1);
                assertThat(((I64Vector) batch.column(0).borrow(Stream.VALUES)).values()).startsWith(300L);
            }
            assertThat(source.poll()).isSameAs(SourcePoll.Finished.FINISHED);
        }
    }

    @Test
    void testLateResidualRuntimeFilterNarrowsUnreadRowsMidPage()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int value = 0; value < 20_001; value++) {
            rows.add(new ParquetRow(value, true, (long) value));
        }
        java.nio.file.Path file = writeParquetFile("late-runtime-filter.parquet", true, rows);
        Schema schema = new Schema(List.of(new Field("maybe", BIGINT, true)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        executableRuntimeFilterResources(),
                        allocator,
                        List.of(file),
                        schema)) {
            int positions = 0;
            try (var first = ((SourcePoll.Ready) source.poll()).batch()) {
                positions += first.selection().count();
            }
            assertThat(positions).isLessThan(rows.size());

            assertThat(source.addRuntimeFilter(new RuntimeFilter(
                    source.column(0),
                    new TestingTypedLongDomain(source.column(0).type(), DynamicFilter.fromRange(0, 100_000, 100_001)),
                    false)))
                    .isEqualTo(RuntimeFilterAcceptance.ACCEPTED_WITH_RESIDUAL);

            SourcePoll poll = source.poll();
            while (poll instanceof SourcePoll.Ready ready) {
                try (var batch = ready.batch()) {
                    positions += batch.selection().count();
                }
                poll = source.poll();
            }
            assertThat(poll).isSameAs(SourcePoll.Finished.FINISHED);
            assertThat(positions).isEqualTo(10_000);
        }
    }

    @Test
    void testLateEnforcedRuntimeFilterRemainsResidualMidPage()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int value = 0; value < 9; value++) {
            rows.add(new ParquetRow(value, true, (long) value));
        }
        java.nio.file.Path file = writeParquetFile("late-enforced-runtime-filter.parquet", true, rows);
        Schema schema = new Schema(List.of(new Field("maybe", BIGINT, true)));
        NitroParquetScanResources resources = NitroParquetScanResources.createDefault(
                org.weakref.nitro.parquet.ParquetArenaPolicy.confined(),
                new ParquetRuntimeFilterPolicy(true, true, true, 8_096),
                new ParquetScanBatchPolicy(4));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(resources, allocator, List.of(file), schema)) {
            int positions;
            try (var first = ((SourcePoll.Ready) source.poll()).batch()) {
                positions = first.selection().count();
            }
            assertThat(source.addRuntimeFilter(new RuntimeFilter(
                    source.column(0),
                    new TestingTypedLongDomain(source.column(0).type(), DynamicFilter.fromRange(0, 100, 100)),
                    false).withoutResidual()))
                    .isEqualTo(RuntimeFilterAcceptance.ACCEPTED_WITH_RESIDUAL);

            SourcePoll poll = source.poll();
            while (poll instanceof SourcePoll.Ready ready) {
                try (var batch = ready.batch()) {
                    positions += batch.selection().count();
                }
                poll = source.poll();
            }
            assertThat(poll).isSameAs(SourcePoll.Finished.FINISHED);
            assertThat(positions).isEqualTo(rows.size());
        }
    }

    @Test
    void testNitroParquetSourceCanMatchConnectorColumnNamesIgnoringCase()
            throws IOException
    {
        java.nio.file.Path file = ClickBenchHitsSupport.writeHitsFixture(tempDirectory.resolve("mixed-case-column.parquet"), 3);
        Schema schema = new Schema(List.of(new Field("advengineid", BIGINT, false)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = NitroParquetBatchSource.forSplits(
                        NitroParquetScanResources.createDefault(),
                        allocator,
                        List.of(NitroParquetBatchSource.Split.wholeFile(file)),
                        schema,
                        ParquetColumnNameMatching.CASE_INSENSITIVE)) {
            SourcePoll.Ready ready = (SourcePoll.Ready) source.poll();
            try (var batch = ready.batch()) {
                var values = org.weakref.nitro.data.VectorAccess.longValues(batch.column(0).borrow(Stream.VALUES));
                assertThat(new long[] {values.value(0), values.value(1), values.value(2)})
                        .containsExactly(0, 10, 10);
            }
        }
    }

    @Test
    void testNitroParquetSourceCanProjectConnectorColumnsByOrdinal()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("ordinal-projection.parquet", true, List.of(
                new ParquetRow(10, true, 100L),
                new ParquetRow(20, false, null),
                new ParquetRow(30, true, 300L)));
        Schema schema = new Schema(List.of(
                new Field("logical_x", BIGINT, false),
                new Field("logical_maybe", BIGINT, true)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = NitroParquetBatchSource.forSplitsByOrdinal(
                        NitroParquetScanResources.createDefault(),
                        allocator,
                        List.of(NitroParquetBatchSource.Split.wholeFile(file)),
                        schema,
                        List.of(0, 2))) {
            SourcePoll.Ready ready = (SourcePoll.Ready) source.poll();
            try (var batch = ready.batch()) {
                VectorAccess.LongValues x = VectorAccess.longValues(batch.column(0).borrow(Stream.VALUES));
                VectorAccess.LongValues maybe = VectorAccess.longValues(batch.column(1).borrow(Stream.VALUES));
                assertThat(new long[] {x.value(0), x.value(1), x.value(2)})
                        .containsExactly(10, 20, 30);
                assertThat(new long[] {maybe.value(0), maybe.value(1), maybe.value(2)})
                        .containsExactly(100, 0, 300);
                assertThat(((BooleanVector) batch.column(1).borrow(Stream.NULLS)).values())
                        .startsWith(false, true, false);
            }
        }
    }

    @Test
    void testNitroParquetSourcePrunesMixedPayloadRowGroups()
            throws IOException
    {
        java.nio.file.Path first = writeIntStringParquetFile("mixed-row-group-first.parquet", List.of(1, 2, 3));
        java.nio.file.Path second = writeIntStringParquetFile("mixed-row-group-second.parquet", List.of(100, 101, 102));

        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(
                executableRuntimeFilterResources(),
                new Allocator(EngineResources.createDefault()),
                List.of(first, second),
                List.of("value", "payload"))) {
            assertThat(scan.supportsDynamicFilterPushdown(0)).isTrue();
            scan.pushDynamicFilter(DynamicFilter.fromRange(0, 100, 102));

            List<Integer> values = new ArrayList<>();
            while (scan.hasNext()) {
                try (Batch batch = scan.next()) {
                    I32Vector vector = (I32Vector) batch.output(0).borrow(Stream.VALUES);
                    batch.borrowMask().forEach(position -> values.add(vector.values()[position]));
                }
            }
            assertThat(values).containsExactly(100, 101, 102);
        }
    }

    @Test
    void testNitroParquetSourceAppliesRowLevelRuntimeFilterWithBinaryPayload()
            throws IOException
    {
        List<Integer> rows = new ArrayList<>();
        for (int value = 0; value < 20_003; value++) {
            rows.add(value);
        }
        java.nio.file.Path file = writeIntStringParquetFile("mixed-row-level-runtime-filter.parquet", rows);
        Schema schema = Schema.unspecified(List.of("value", "payload"));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        executableRuntimeFilterResources(),
                        allocator,
                        List.of(file),
                        schema)) {
            RuntimeFilterAcceptance acceptance = source.addRuntimeFilter(new RuntimeFilter(
                    source.column(0),
                    new TestingTypedLongDomain(source.column(0).type(), DynamicFilter.fromRange(0, 12_345, 12_346)),
                    false).withoutResidual());
            assertThat(acceptance).isEqualTo(RuntimeFilterAcceptance.ENFORCED);

            List<Integer> values = new ArrayList<>();
            List<String> payloads = new ArrayList<>();
            SourcePoll poll = source.poll();
            while (poll instanceof SourcePoll.Ready ready) {
                try (var batch = ready.batch()) {
                    var keys = VectorAccess.longValues(batch.column(0).borrow(Stream.VALUES));
                    org.weakref.nitro.data.Vector payload = batch.column(1).borrow(Stream.VALUES);
                    BinaryVector binary = binaryValues(payload);
                    int[] ids = payload instanceof DictionaryVector dictionary ? dictionary.ids() : null;
                    for (int index = 0; index < batch.selection().count(); index++) {
                        int position = batch.selection().position(index);
                        values.add((int) keys.value(position));
                        payloads.add(utf8(binary, ids == null ? position : ids[position]));
                    }
                }
                poll = source.poll();
            }

            assertThat(poll).isSameAs(SourcePoll.Finished.FINISHED);
            assertThat(values).containsExactly(12_345, 12_346);
            assertThat(payloads).containsExactly("payload-12345", "payload-12346");
        }
    }

    @Test
    void testNitroParquetSourceSlicesRowLevelFilteredBinaryPayload()
            throws IOException
    {
        List<Integer> rows = new ArrayList<>();
        for (int value = 0; value < 25_003; value++) {
            rows.add(value);
        }
        java.nio.file.Path file = writeIntStringParquetFile("mixed-row-level-runtime-filter-slices.parquet", rows);
        Schema schema = Schema.unspecified(List.of("value", "payload"));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        executableRuntimeFilterResources(),
                        allocator,
                        List.of(file),
                        schema)) {
            assertThat(source.addRuntimeFilter(new RuntimeFilter(
                    source.column(0),
                    new TestingTypedLongDomain(source.column(0).type(), DynamicFilter.fromRange(0, 1_000, 24_000)),
                    false).withoutResidual()))
                    .isEqualTo(RuntimeFilterAcceptance.ENFORCED);

            int batches = 0;
            int positions = 0;
            SourcePoll poll = source.poll();
            while (poll instanceof SourcePoll.Ready ready) {
                try (var batch = ready.batch()) {
                    var keys = VectorAccess.longValues(batch.column(0).borrow(Stream.VALUES));
                    org.weakref.nitro.data.Vector payload = batch.column(1).borrow(Stream.VALUES);
                    BinaryVector binary = binaryValues(payload);
                    int[] ids = payload instanceof DictionaryVector dictionary ? dictionary.ids() : null;
                    for (int index = 0; index < batch.selection().count(); index++) {
                        int position = batch.selection().position(index);
                        long expected = 1_000L + positions + index;
                        assertThat(keys.value(position)).isEqualTo(expected);
                        assertThat(utf8(binary, ids == null ? position : ids[position])).isEqualTo("payload-" + expected);
                    }
                    positions += batch.selection().count();
                    batches++;
                }
                poll = source.poll();
            }

            assertThat(poll).isSameAs(SourcePoll.Finished.FINISHED);
            assertThat(batches).isGreaterThan(1);
            assertThat(positions).isEqualTo(23_001);
        }
    }

    @Test
    void testNitroParquetSourcePreservesDictionaryPayloadAcrossRowFilter()
            throws IOException
    {
        List<Integer> rows = new ArrayList<>();
        for (int position = 0; position < 100_003; position++) {
            rows.add(position % 31);
        }
        java.nio.file.Path file = writeIntStringParquetFile("mixed-dictionary-payload-runtime-filter.parquet", rows);
        assertDictionaryEncoding(file, "payload");
        Schema schema = Schema.unspecified(List.of("value", "payload"));
        NitroParquetScanResources resources = NitroParquetScanResources.createDefault(
                org.weakref.nitro.parquet.ParquetArenaPolicy.confined(),
                new ParquetRuntimeFilterPolicy(true, true, true, 8_096),
                new ParquetScanBatchPolicy(4_000));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        resources,
                        allocator,
                        List.of(file),
                        schema)) {
            assertThat(source.addRuntimeFilter(new RuntimeFilter(
                    source.column(0),
                    new TestingTypedLongDomain(source.column(0).type(), DynamicFilter.fromRange(0, 5, 7)),
                    false).withoutResidual()))
                    .isEqualTo(RuntimeFilterAcceptance.ENFORCED);

            int positions = 0;
            int batches = 0;
            SourcePoll poll = source.poll();
            while (poll instanceof SourcePoll.Ready ready) {
                try (var batch = ready.batch()) {
                    org.weakref.nitro.data.Vector payload = batch.column(1).borrow(Stream.VALUES);
                    assertThat(payload).isInstanceOf(DictionaryVector.class);
                    DictionaryVector dictionary = (DictionaryVector) payload;
                    assertThat(dictionary.values().length()).isEqualTo(31);
                    var keys = VectorAccess.longValues(batch.column(0).borrow(Stream.VALUES));
                    for (int index = 0; index < batch.selection().count(); index++) {
                        int position = batch.selection().position(index);
                        long value = keys.value(position);
                        assertThat(value).isBetween(5L, 7L);
                        assertThat(utf8(dictionary.values(), dictionary.ids()[position]))
                                .isEqualTo("payload-" + value);
                    }
                    positions += batch.selection().count();
                    batches++;
                }
                poll = source.poll();
            }
            assertThat(poll).isSameAs(SourcePoll.Finished.FINISHED);
            assertThat(batches).isGreaterThan(1);
            assertThat(positions).isEqualTo(9_678);
        }
    }

    @Test
    void testNitroParquetSourcePrunesRowGroupsBeforeNumericFilterWindows()
            throws IOException
    {
        java.nio.file.Path first = writeIntStringParquetFile("numeric-filter-row-group-first.parquet", List.of(1, 2, 3));
        java.nio.file.Path second = writeIntStringParquetFile("numeric-filter-row-group-second.parquet", List.of(100, 101, 102));
        Schema schema = Schema.unspecified(List.of("value"));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        executableRuntimeFilterResources(),
                        allocator,
                        List.of(first, second),
                        schema)) {
            DynamicFilter filter = DynamicFilter.fromRange(0, 100, 102);
            source.addRuntimeFilter(new RuntimeFilter(
                    source.column(0),
                    new TestingTypedLongDomain(source.column(0).type(), filter),
                    false));
            SourceMetrics metrics = source.protocol(SourceMetricsProtocol.METRICS).orElseThrow();

            SourcePoll poll = source.poll();
            while (poll instanceof SourcePoll.Ready ready) {
                ready.batch().close();
                poll = source.poll();
            }

            assertThat(poll).isSameAs(SourcePoll.Finished.FINISHED);
            assertThat(metrics.completedPositions()).hasValue(3);
        }
    }

    @Test
    void testNitroParquetSourcePrunesDictionaryDisjointRowGroupWithOverlappingStatistics()
            throws IOException
    {
        List<Integer> disjointValues = new ArrayList<>();
        List<Integer> matchingValues = new ArrayList<>();
        for (int position = 0; position < 1_000; position++) {
            disjointValues.add(position % 2 == 0 ? 1 : 100);
            matchingValues.add(50);
        }
        java.nio.file.Path disjoint = writeIntStringParquetFile("dictionary-disjoint-row-group.parquet", disjointValues);
        java.nio.file.Path matching = writeIntStringParquetFile("dictionary-matching-row-group.parquet", matchingValues);
        Schema schema = Schema.unspecified(List.of("value"));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        executableRuntimeFilterResources(),
                        allocator,
                        List.of(disjoint, matching),
                        schema)) {
            DynamicFilter filter = DynamicFilter.fromRange(0, 50, 50);
            source.addRuntimeFilter(new RuntimeFilter(
                    source.column(0),
                    new TestingTypedLongDomain(source.column(0).type(), filter),
                    false));
            SourceMetrics metrics = source.protocol(SourceMetricsProtocol.METRICS).orElseThrow();

            List<Integer> values = new ArrayList<>();
            SourcePoll poll = source.poll();
            while (poll instanceof SourcePoll.Ready ready) {
                try (var batch = ready.batch()) {
                    I32Vector vector = (I32Vector) batch.column(0).borrow(Stream.VALUES);
                    for (int index = 0; index < batch.selection().count(); index++) {
                        values.add(vector.values()[batch.selection().position(index)]);
                    }
                }
                poll = source.poll();
            }

            assertThat(poll).isSameAs(SourcePoll.Finished.FINISHED);
            assertThat(metrics.completedPositions()).hasValue(1_000);
            assertThat(values).hasSize(1_000).containsOnly(50);
        }
    }

    @Test
    void testNitroParquetSourceKeepsRuntimeFilterAsResidualWhenPushdownDisabled()
            throws IOException
    {
        java.nio.file.Path file = writeParquetFile("runtime-filter-residual.parquet", true, List.of(
                new ParquetRow(1, true, 1L),
                new ParquetRow(2, true, 100L)));
        Schema schema = new Schema(List.of(new Field("maybe", BIGINT, true)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        NitroParquetScanResources.createDefault(
                                org.weakref.nitro.parquet.ParquetArenaPolicy.confined(),
                                new ParquetRuntimeFilterPolicy(false, false, false, 0)),
                        allocator,
                        List.of(file),
                        schema)) {
            RuntimeFilterAcceptance acceptance = source.addRuntimeFilter(new RuntimeFilter(
                    source.column(0),
                    new TestingTypedLongDomain(source.column(0).type(), DynamicFilter.fromRange(0, 100, 100)),
                    false).withoutResidual());

            assertThat(acceptance).isEqualTo(RuntimeFilterAcceptance.ACCEPTED_WITH_RESIDUAL);
            try (var batch = ((SourcePoll.Ready) source.poll()).batch()) {
                assertThat(batch.selection().count()).isEqualTo(2);
            }
        }
    }

    @Test
    void testNitroParquetSourceGrowsNullablePayloadScratchAcrossFilterWindows()
            throws IOException
    {
        java.nio.file.Path first = writeParquetFile("numeric-filter-small-row-group.parquet", true, List.of(
                new ParquetRow(1, true, null),
                new ParquetRow(2, true, 20L),
                new ParquetRow(3, true, 30L)));
        List<ParquetRow> largerRows = new ArrayList<>();
        for (int position = 0; position < 100; position++) {
            largerRows.add(new ParquetRow(100 + position, true, position % 7 == 0 ? null : 1_000L + position));
        }
        java.nio.file.Path second = writeParquetFile("numeric-filter-large-row-group.parquet", true, largerRows);
        Schema schema = new Schema(List.of(
                new Field("x", BIGINT, false),
                new Field("maybe", BIGINT, true)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        NitroParquetScanResources.createDefault(),
                        allocator,
                        List.of(first, second),
                        schema)) {
            DynamicFilter filter = DynamicFilter.fromRange(0, 1, 199);
            source.addRuntimeFilter(new RuntimeFilter(
                    source.column(0),
                    new TestingTypedLongDomain(source.column(0).type(), filter),
                    false));

            int positions = 0;
            SourcePoll poll = source.poll();
            while (poll instanceof SourcePoll.Ready ready) {
                try (var batch = ready.batch()) {
                    I64Vector payload = (I64Vector) batch.column(1).borrow(Stream.VALUES);
                    BooleanVector nulls = (BooleanVector) batch.column(1).borrow(Stream.NULLS);
                    for (int index = 0; index < batch.selection().count(); index++) {
                        int position = batch.selection().position(index);
                        if (nulls.values()[position]) {
                            assertThat(payload.values()[position]).isZero();
                        }
                    }
                    positions += batch.selection().count();
                }
                poll = source.poll();
            }

            assertThat(poll).isSameAs(SourcePoll.Finished.FINISHED);
            assertThat(positions).isEqualTo(103);
        }
    }

    @Test
    void testNitroParquetSourceWidensInt32ForLongLogicalBinding()
            throws IOException
    {
        java.nio.file.Path file = writeInt32ParquetFile("nitro-int32-as-long.parquet", List.of(1, -20, 300));
        TypeBinding longBinding = new VectorTypeBinding(
                new TypeIdentity("testing:int32-as-long"),
                long.class,
                Set.of(I64Vector.class));
        Schema schema = new Schema(List.of(new Field("value", longBinding, false)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        NitroParquetScanResources.createDefault(),
                        allocator,
                        List.of(file),
                        schema)) {
            SourcePoll.Ready ready = (SourcePoll.Ready) source.poll();
            try (var batch = ready.batch()) {
                assertThat(batch.column(0).borrow(Stream.VALUES))
                        .isInstanceOf(I64Vector.class);
                assertThat(((I64Vector) batch.column(0).borrow(Stream.VALUES)).values())
                        .startsWith(1L, -20L, 300L);
            }
            assertThat(source.poll()).isSameAs(SourcePoll.Finished.FINISHED);
        }
    }

    @Test
    void testNitroParquetSourceWithSharedArenasCanCloseConcurrentlyAcrossThreads()
            throws Exception
    {
        java.nio.file.Path file = writeInt32ParquetFile("nitro-shared-arena.parquet", List.of(1, 2, 3));
        TypeBinding intBinding = new VectorTypeBinding(
                new TypeIdentity("testing:int32"),
                int.class,
                Set.of(I32Vector.class));
        Schema schema = new Schema(List.of(new Field("value", intBinding, false)));

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                var executor = Executors.newFixedThreadPool(2)) {
            NitroParquetBatchSource source = new NitroParquetBatchSource(
                    NitroParquetScanResources.createDefault(org.weakref.nitro.parquet.ParquetArenaPolicy.shared()),
                    allocator,
                    List.of(file),
                    schema);
            CountDownLatch start = new CountDownLatch(1);
            var first = executor.submit(() -> {
                start.await();
                source.close();
                return null;
            });
            var second = executor.submit(() -> {
                start.await();
                source.close();
                return null;
            });
            start.countDown();
            first.get();
            second.get();
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
                new NitroParquetScanOperator(executableRuntimeFilterResources(), allocator, List.of(file), List.of("x", "maybe")),
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
                executableRuntimeFilterResources(),
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
    void testNitroFilteredWindowDoesNotDeferAcrossInitialAdaptiveBatches()
            throws IOException
    {
        java.nio.file.Path file = writeWideNumericParquetFile("nitro-adaptive-filtered-payload.parquet", List.of(
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
        NitroParquetScanResources resources = NitroParquetScanResources.createDefault(
                org.weakref.nitro.parquet.ParquetArenaPolicy.confined(),
                new ParquetRuntimeFilterPolicy(true, true, true, 8_096),
                new ParquetScanBatchPolicy(4, 8, 8, 0.25));

        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(
                resources,
                new Allocator(EngineResources.createDefault()),
                List.of(file),
                List.of("key", "p1", "p2", "p3", "p4"))) {
            scan.pushDynamicFilter(DynamicFilter.fromRange(0, 2, 6));
            try (Batch first = scan.next()) {
                assertThat(first.borrowMask()).hasSize(4);
                assertThat(((I64Vector) first.output(4).borrow(Stream.VALUES)).values())
                        .startsWith(24L, 34L, 44L, 54L);
            }
            try (Batch second = scan.next()) {
                assertThat(second.borrowMask()).hasSize(1);
                assertThat(((I64Vector) second.output(4).borrow(Stream.VALUES)).values()[0]).isEqualTo(64L);
            }
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
            VectorAccess.LongValues x = VectorAccess.longValues(selected.output(0).borrow(Stream.VALUES));
            VectorAccess.LongValues maybe = VectorAccess.longValues(selected.output(1).borrow(Stream.VALUES));
            BooleanVector nulls = (BooleanVector) selected.output(1).borrow(Stream.NULLS);
            assertThat(x.value(1)).isEqualTo(22L);
            assertThat(maybe.value(1)).isEqualTo(202L);
            assertThat(nulls.values()[1]).isFalse();
            assertThat(scan.hasNext()).isFalse();
        }
    }

    @Test
    void testNitroLateBinaryPayloadRemainsAlignedAcrossSparseBatches()
            throws IOException
    {
        List<Integer> rows = new ArrayList<>();
        for (int position = 0; position < 25_003; position++) {
            rows.add(position % 31);
        }
        java.nio.file.Path file = writeIntStringParquetFile("nitro-late-binary-sparse-batches.parquet", rows);
        assertDictionaryEncoding(file, "payload");

        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                ParquetFile parquetFile = ParquetFile.open(file);
                ColumnReader reader = columnReader(List.of(parquetFile), "payload")) {
            Allocator.Context context = new Allocator.Context("late-binary-direct-skip");
            reader.skip(10_000);
            org.weakref.nitro.data.Vector payload = reader.readBinary(allocator, context, null, 3);
            BinaryVector values = binaryValues(payload);
            int[] ids = payload instanceof DictionaryVector dictionary ? dictionary.ids() : null;
            for (int position = 0; position < 3; position++) {
                assertThat(utf8(values, ids == null ? position : ids[position]))
                        .isEqualTo("payload-" + ((10_000 + position) % 31));
            }
            allocator.release(context, payload);
        }

        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(
                NitroParquetScanResources.createDefault(),
                new Allocator(EngineResources.createDefault()),
                List.of(file),
                List.of("value", "payload"))) {
            int batchStart = 0;
            while (scan.hasNext()) {
                try (Batch batch = scan.next()) {
                    var keys = org.weakref.nitro.data.VectorAccess.longValues(batch.output(0).borrow(Stream.VALUES));
                    int count = batch.borrowMask().selectedCount();
                    if (batchStart == 0) {
                        scan.constrain(Mask.none(count));
                        batchStart += count;
                        continue;
                    }
                    int[] selected = count > 2
                            ? new int[] {1, count / 2, count - 1}
                            : new int[] {count - 1};
                    scan.constrain(Mask.sparse(selected, count));

                    org.weakref.nitro.data.Vector payload = batch.output(1).borrow(Stream.VALUES);
                    BinaryVector values = binaryValues(payload);
                    int[] ids = payload instanceof DictionaryVector dictionary ? dictionary.ids() : null;
                    for (int position : selected) {
                        int row = batchStart + position;
                        assertThat(keys.value(position)).isEqualTo(row % 31);
                        assertThat(utf8(values, ids == null ? position : ids[position]))
                                .isEqualTo("payload-" + (row % 31));
                    }
                    batchStart += count;
                }
            }
            assertThat(batchStart).isEqualTo(rows.size());
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
    void testBoundedDictionaryMatchFractionAcrossChunksDoesNotConsumeReader()
            throws IOException
    {
        List<ParquetRow> firstRows = new ArrayList<>();
        List<ParquetRow> secondRows = new ArrayList<>();
        for (int position = 0; position < 2_000; position++) {
            firstRows.add(new ParquetRow((position & 1) == 0 ? 10 : 20, true, null));
            secondRows.add(new ParquetRow((position & 1) == 0 ? 30 : 40, true, null));
        }
        java.nio.file.Path first = writeParquetFile("dictionary-match-fraction-first.parquet", true, firstRows);
        java.nio.file.Path second = writeParquetFile("dictionary-match-fraction-second.parquet", true, secondRows);
        assertDictionaryEncoding(first, "x");
        assertDictionaryEncoding(second, "x");

        try (ParquetFile firstFile = ParquetFile.open(first);
                ParquetFile secondFile = ParquetFile.open(second);
                ColumnReader reader = columnReader(List.of(firstFile, secondFile), "x")) {
            long[] prefix = new long[2];
            reader.readLongs(prefix, null, prefix.length);
            assertThat(prefix).containsExactly(10, 20);

            assertThat(reader.estimateDictionaryMatchFraction(value -> value == 10 || value == 30, 4))
                    .isEqualTo(0.5);
            assertThat(reader.estimateDictionaryMatchFraction(value -> true, 3)).isNaN();
            assertThat(reader.estimateDictionaryMatchFraction(value -> true, 0)).isNaN();

            long[] values = new long[3_998];
            reader.readLongs(values, null, values.length);
            assertThat(Arrays.copyOfRange(values, 0, 4)).containsExactly(10, 20, 10, 20);
            assertThat(Arrays.copyOfRange(values, 1_998, 2_002)).containsExactly(30, 40, 30, 40);
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
        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(executableRuntimeFilterResources(), new Allocator(EngineResources.createDefault()), List.of(file), List.of("maybe"))) {
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
                executableRuntimeFilterResources(),
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
    void testGrowingAliasedFilterScratchDoesNotReturnAnActiveLeaseToThePool()
            throws Exception
    {
        List<ParquetRow> rows = new ArrayList<>();
        int windowRows = 100_000;
        for (int position = 0; position < windowRows * 2; position++) {
            boolean firstWindow = position < windowRows;
            long x = !firstWindow || position < 10 ? 0 : 1;
            long maybe = firstWindow ? (position == 0 ? 0 : 1) : position & 1;
            // The first window narrows 10 -> 1 and compacts, making maybe's ten-cell filter scratch the published
            // window buffer. The second narrows 100,000 -> 50,000 without compaction, forcing both scratch buffers to
            // grow and making a stale alias observable to a concurrent pool borrower.
            rows.add(new ParquetRow(x, true, maybe));
        }
        java.nio.file.Path file = writeParquetFile("growing-aliased-filter-scratch.parquet", true, rows);
        ParquetProgressiveFilterCompactionPolicy compaction = new ParquetProgressiveFilterCompactionPolicy(
                true,
                12,
                0,
                4,
                new ParquetProgressiveFilterCompactionPolicy.Prospective(false, Long.MAX_VALUE, 100, Long.MAX_VALUE),
                new ParquetProgressiveFilterCompactionPolicy.Fused(false, 100, Integer.MAX_VALUE, Integer.MAX_VALUE),
                false);
        NitroParquetScanResources scanResources = new NitroParquetScanResources(
                DecompressedPageCachePolicy.defaults(),
                ParquetReaderPolicy.defaults(),
                ParquetNumericDecodeAdmissionPolicy.defaults(),
                GENERIC_LATE_MATERIALIZATION,
                compaction,
                GENERIC_FILTERED_PAYLOAD,
                new ParquetFilterWindowPolicy(
                        windowRows,
                        new ParquetFilterWindowPolicy.AdaptiveNarrow(false, 0, windowRows, false),
                        false),
                GENERIC_FILTER_EVALUATION,
                ParquetScanDiagnostics.disabled(),
                new ParquetScanBatchPolicy(windowRows));
        PrimitiveArrayPool arrays = new PrimitiveArrayPool(1 << 20, 0);
        try (scanResources;
                AllocationResources allocationResources = new AllocationResources(arrays, new PrimitiveArrayPool(1 << 20, 0));
                Allocator allocator = new Allocator(allocationResources);
                NitroParquetBatchSource source = new NitroParquetBatchSource(
                        scanResources,
                        allocator,
                        List.of(file),
                        new Schema(List.of(new Field("x", BIGINT, false), new Field("maybe", BIGINT, true))))) {
            addExactRuntimeFilter(source, 0, 0, 0);
            addExactRuntimeFilter(source, 1, 0, 0);
            try (var first = ((SourcePoll.Ready) source.poll()).batch()) {
                assertThat(first.selection().count()).isOne();
            }
            AtomicBoolean stop = new AtomicBoolean();
            CountDownLatch borrowerStarted = new CountDownLatch(1);
            try (var executor = Executors.newSingleThreadExecutor()) {
                var duplicateLease = executor.submit(() -> {
                    Set<long[]> activeLeases = Collections.newSetFromMap(new IdentityHashMap<>());
                    borrowerStarted.countDown();
                    while (!stop.get()) {
                        long[] lease = arrays.borrow(long[].class, 10, long[].class);
                        if (lease == null) {
                            Thread.onSpinWait();
                        }
                        else if (!activeLeases.add(lease)) {
                            return true;
                        }
                    }
                    return false;
                });
                borrowerStarted.await();
                try {
                    try (var second = ((SourcePoll.Ready) source.poll()).batch()) {
                        assertThat(second.selection().count()).isEqualTo(windowRows / 2);
                    }
                }
                finally {
                    stop.set(true);
                }
                assertThat(duplicateLease.get()).isFalse();
            }
        }
    }

    private static void addExactRuntimeFilter(NitroParquetBatchSource source, int column, long min, long max)
    {
        source.addRuntimeFilter(new RuntimeFilter(
                source.column(column),
                new TestingTypedLongDomain(source.column(column).type(), DynamicFilter.fromRange(column, min, max)),
                false).withoutResidual());
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
    void testSelectionOnlyDictionaryFilterMatchesMaterializedValues()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int position = 0; position < 24_000; position++) {
            Long value = position < 1_024 && position % 7 == 0 ? null : (long) (position & 7);
            rows.add(new ParquetRow(position & 7, true, value));
        }
        java.nio.file.Path file = writeParquetFile("selection-only-dictionary-filter.parquet", true, rows);
        assertDictionaryEncoding(file, "maybe");

        try (ParquetFile parquetFile = ParquetFile.open(file);
                ColumnReader materialized = columnReader(List.of(parquetFile), "maybe");
                ColumnReader selectionOnly = columnReader(List.of(parquetFile), "maybe")) {
            int[] materializedSurvivors = new int[24_000];
            int[] selectionOnlySurvivors = new int[24_000];
            long[] values = new long[24_000];
            java.util.function.LongPredicate predicate = value -> (value & 1) != 0;

            int materializedCount = materialized.filterDictLongs(
                    predicate, 24_000, materializedSurvivors, values, null);
            int selectionOnlyCount = selectionOnly.filterDictLongs(
                    predicate, 24_000, selectionOnlySurvivors, null, null);

            assertThat(selectionOnlyCount).isEqualTo(materializedCount);
            assertThat(Arrays.copyOf(selectionOnlySurvivors, selectionOnlyCount))
                    .containsExactly(Arrays.copyOf(materializedSurvivors, materializedCount));
            for (int position = 0; position < materializedCount; position++) {
                assertThat(predicate.test(values[position])).isTrue();
            }
        }
    }

    @Test
    void testSelectionOnlyIntDictionaryFilterMatchesMaterializedValues()
            throws IOException
    {
        List<Integer> rows = java.util.stream.IntStream.range(0, 24_000)
                .map(position -> position & 7)
                .boxed()
                .toList();
        java.nio.file.Path file = writeInt32ParquetFile("selection-only-int-dictionary-filter.parquet", rows);

        try (ParquetFile parquetFile = ParquetFile.open(file);
                ColumnReader materialized = columnReader(List.of(parquetFile), "value");
                ColumnReader selectionOnly = columnReader(List.of(parquetFile), "value")) {
            int[] materializedSurvivors = new int[24_000];
            int[] selectionOnlySurvivors = new int[24_000];
            int[] values = new int[24_000];
            java.util.function.LongPredicate predicate = value -> (value & 1) == 0;

            int materializedCount = materialized.filterDictInts(
                    predicate, 24_000, materializedSurvivors, values, null);
            int selectionOnlyCount = selectionOnly.filterDictInts(
                    predicate, 24_000, selectionOnlySurvivors, null, null);

            assertThat(selectionOnlyCount).isEqualTo(materializedCount);
            assertThat(Arrays.copyOf(selectionOnlySurvivors, selectionOnlyCount))
                    .containsExactly(Arrays.copyOf(materializedSurvivors, materializedCount));
        }
    }

    @Test
    void testSelectionOnlyNullableIntDictionaryFilterMatchesMaterializedValues()
            throws IOException
    {
        List<Integer> rows = new ArrayList<>();
        for (int position = 0; position < 24_000; position++) {
            rows.add(position < 1_024 && position % 7 == 0 ? null : position & 7);
        }
        java.nio.file.Path file = writeNullableInt32ParquetFile("selection-only-nullable-int-dictionary-filter.parquet", rows);
        assertDictionaryEncoding(file, "value");

        try (ParquetFile parquetFile = ParquetFile.open(file);
                ColumnReader materialized = columnReader(List.of(parquetFile), "value");
                ColumnReader selectionOnly = columnReader(List.of(parquetFile), "value")) {
            int[] materializedSurvivors = new int[24_000];
            int[] selectionOnlySurvivors = new int[24_000];
            int[] values = new int[24_000];
            java.util.function.LongPredicate predicate = value -> (value & 1) == 0;

            int materializedCount = materialized.filterDictInts(
                    predicate, 24_000, materializedSurvivors, values, null);
            int selectionOnlyCount = selectionOnly.filterDictInts(
                    predicate, 24_000, selectionOnlySurvivors, null, null);

            assertThat(selectionOnlyCount).isEqualTo(materializedCount);
            assertThat(Arrays.copyOf(selectionOnlySurvivors, selectionOnlyCount))
                    .containsExactly(Arrays.copyOf(materializedSurvivors, materializedCount));
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
        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(executableRuntimeFilterResources(), new Allocator(EngineResources.createDefault()), List.of(file), List.of("maybe"))) {
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
    void testSequentialReadStreamsHomogeneousNullableRunsWithoutReusingDefinitionScratch()
            throws IOException
    {
        for (boolean dictionaryEnabled : List.of(false, true)) {
            List<ParquetRow> rows = new ArrayList<>();
            for (int position = 0; position < 8_000; position++) {
                rows.add(new ParquetRow(position, true, position < 2_000 ? null : 100L + (position & 3)));
            }
            java.nio.file.Path file = writeParquetFile(
                    "nullable-homogeneous-sequential-" + dictionaryEnabled + ".parquet",
                    dictionaryEnabled,
                    rows);

            try (ParquetFile parquetFile = ParquetFile.open(file);
                    ColumnReader reader = columnReader(List.of(parquetFile), "maybe")) {
                long[] values = new long[2_000];
                boolean[] nulls = new boolean[2_000];

                reader.readLongs(values, nulls, values.length);
                assertThat(nulls).containsOnly(true);

                for (int batch = 0; batch < 3; batch++) {
                    reader.readLongs(values, nulls, values.length);
                    assertThat(nulls).containsOnly(false);
                    for (int position = 0; position < values.length; position++) {
                        assertThat(values[position]).isEqualTo(100L + (position & 3));
                    }
                }
            }
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
                    reader.addChunk(firstFile.readRange(0, firstFile.size()), firstFile.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
                }
                for (org.apache.parquet.format.RowGroup rowGroup : secondFile.rowGroups()) {
                    reader.addChunk(secondFile.readRange(0, secondFile.size()), secondFile.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
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
                    reader.addChunk(firstFile.readRange(0, firstFile.size()), firstFile.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
                }
                for (org.apache.parquet.format.RowGroup rowGroup : secondFile.rowGroups()) {
                    reader.addChunk(secondFile.readRange(0, secondFile.size()), secondFile.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
                }

                long[] values = new long[7];
                reader.readLongs(values, null, values.length);
                assertThat(values).containsExactly(11L, 12L, 11L, 21L, 22L, 23L, 21L);
            }
        }
    }

    @Test
    void testFullNullableNumericReadRemainsAlignedAcrossPartialBatches()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int position = 0; position < 257; position++) {
            rows.add(new ParquetRow(
                    100 + (position % 17),
                    true,
                    position % 7 == 0 ? null : 1_000L + (position % 23)));
        }
        for (boolean dictionaryEnabled : new boolean[] {false, true}) {
            java.nio.file.Path file = writeParquetFile(
                    "numeric-" + (dictionaryEnabled ? "dictionary" : "plain") + "-partial-batches.parquet",
                    dictionaryEnabled,
                    rows);
            if (dictionaryEnabled) {
                assertDictionaryEncoding(file, "x");
                assertDictionaryEncoding(file, "maybe");
            }

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
    }

    @Test
    void testSelectedLongReadContinuesWithinPartiallyConsumedNumericPage()
            throws IOException
    {
        List<ParquetRow> rows = new ArrayList<>();
        for (int position = 0; position < 257; position++) {
            rows.add(new ParquetRow(10_000 + (position % 17), true, null));
        }

        for (boolean directDecode : new boolean[] {false, true}) {
            for (boolean dictionaryEnabled : new boolean[] {false, true}) {
                java.nio.file.Path file = writeParquetFile(
                        (directDecode ? "direct" : "materialized") + "-then-selected-" +
                                (dictionaryEnabled ? "dictionary" : "plain") + ".parquet",
                        dictionaryEnabled,
                        rows);
                if (dictionaryEnabled) {
                    assertDictionaryEncoding(file, "x");
                }
                try (ParquetFile parquetFile = ParquetFile.open(file);
                        ColumnReader reader = columnReader(List.of(parquetFile), "x")) {
                    if (directDecode) {
                        reader.enableDirectNumericBatchDecode();
                    }

                    long[] prefix = new long[31];
                    reader.readLongs(prefix, null, prefix.length);
                    for (int index = 0; index < prefix.length; index++) {
                        assertThat(prefix[index]).isEqualTo(10_000L + (index % 17));
                    }

                    int[] survivors = {0, 1, 17, 63, 96, 151, 225};
                    long[] values = new long[survivors.length];
                    boolean[] nulls = new boolean[survivors.length];
                    reader.readSelectedLongs(survivors, survivors.length, rows.size() - prefix.length, values, nulls);

                    for (int index = 0; index < survivors.length; index++) {
                        assertThat(values[index]).isEqualTo(10_000L + ((prefix.length + survivors[index]) % 17));
                        assertThat(nulls[index]).isFalse();
                    }
                }
            }
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
                            ParquetDictionaryFilterPolicy.defaults(),
                            ParquetDecodeScratchPolicy.defaults()))) {
                for (org.apache.parquet.format.RowGroup rowGroup : parquet.rowGroups()) {
                    reader.addChunk(parquet.readRange(0, parquet.size()), parquet.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
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
                    reader.addChunk(parquet.readRange(0, parquet.size()), parquet.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
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
    void testFullBinaryDictionaryReadRemainsAlignedAcrossPartialBatches()
            throws IOException
    {
        List<BinaryParquetRow> rows = new ArrayList<>();
        for (int position = 0; position < 257; position++) {
            rows.add(new BinaryParquetRow(
                    (position & 1) == 0 ? "alpha" : "beta",
                    position % 7 == 0 ? null : bytes(10 + (position % 3), 20 + (position % 5))));
        }
        java.nio.file.Path file = writeBinaryParquetFile("binary-dictionary-partial-batches.parquet", true, rows);
        assertDictionaryEncoding(file, "name");
        assertDictionaryEncoding(file, "payload");

        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                ParquetFile parquetFile = ParquetFile.open(file);
                ColumnReader required = columnReader(List.of(parquetFile), "name");
                ColumnReader optional = columnReader(List.of(parquetFile), "payload")) {
            Allocator.Context context = new Allocator.Context("binary-dictionary-partial-batches");
            int consumed = 0;
            for (int batchSize : new int[] {31, 73, 153}) {
                boolean[] optionalNulls = new boolean[batchSize];
                org.weakref.nitro.data.Vector requiredValues = required.readBinary(allocator, context, null, batchSize);
                org.weakref.nitro.data.Vector optionalValues = optional.readBinary(allocator, context, optionalNulls, batchSize);
                assertThat(required.lastReadNullsProvenAbsent()).isTrue();
                assertThat(optional.lastReadNullsProvenAbsent()).isFalse();

                try {
                    BinaryVector requiredDictionary = binaryValues(requiredValues);
                    BinaryVector optionalDictionary = binaryValues(optionalValues);
                    for (int index = 0; index < batchSize; index++) {
                        int position = consumed + index;
                        int requiredPosition = requiredValues instanceof DictionaryVector dictionary ? dictionary.ids()[index] : index;
                        assertThat(utf8(requiredDictionary, requiredPosition))
                                .isEqualTo((position & 1) == 0 ? "alpha" : "beta");

                        assertThat(optionalNulls[index]).isEqualTo(position % 7 == 0);
                        if (position % 7 != 0) {
                            int optionalPosition = optionalValues instanceof DictionaryVector dictionary ? dictionary.ids()[index] : index;
                            assertThat(optionalDictionary.copyBytes(optionalPosition))
                                    .containsExactly((byte) (10 + (position % 3)), (byte) (20 + (position % 5)));
                        }
                    }
                }
                finally {
                    allocator.release(context, requiredValues);
                    allocator.release(context, optionalValues);
                }
                consumed += batchSize;
            }
            assertThat(consumed).isEqualTo(rows.size());
        }
    }

    @Test
    void testCompactedBinaryReadProducesDenseSelectedValues()
            throws IOException
    {
        List<BinaryParquetRow> rows = List.of(
                new BinaryParquetRow("zero", bytes(0)),
                new BinaryParquetRow("one", null),
                new BinaryParquetRow("two", bytes(2, 12)),
                new BinaryParquetRow("three", bytes(3, 13)),
                new BinaryParquetRow("four", null),
                new BinaryParquetRow("five", bytes(5, 15)));
        java.nio.file.Path file = writeBinaryParquetFile("compacted-selected-binary.parquet", false, CompressionCodec.SNAPPY, rows);

        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                ParquetFile parquetFile = ParquetFile.open(file);
                ColumnReader reader = columnReader(List.of(parquetFile), "payload")) {
            Allocator.Context context = new Allocator.Context("compacted-selected-binary");
            boolean[] nulls = new boolean[3];
            BinaryVector values = reader.readCompactedBinary(allocator, context, new int[] {1, 2, 5}, 3, rows.size(), nulls);
            try {
                assertThat(values.length()).isEqualTo(3);
                assertThat(nulls).containsExactly(true, false, false);
                assertThat(values.copyBytes(0)).isEmpty();
                assertThat(values.copyBytes(1)).containsExactly((byte) 2, (byte) 12);
                assertThat(values.copyBytes(2)).containsExactly((byte) 5, (byte) 15);
            }
            finally {
                allocator.release(context, values);
            }
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
    void testDerivedBinaryDictionarySurvivesLaterChunkScratchReuse()
            throws IOException
    {
        List<BinaryParquetRow> firstRows = new ArrayList<>();
        List<BinaryParquetRow> secondRows = new ArrayList<>();
        for (int position = 0; position < 12_000; position++) {
            firstRows.add(new BinaryParquetRow((position & 1) == 0 ? "alpha" : "beta", bytes(position & 0xFF)));
            secondRows.add(new BinaryParquetRow((position & 1) == 0 ? "gamma" : "delta", bytes(position & 0xFF)));
        }
        java.nio.file.Path first = writeBinaryParquetFile("derived-dictionary-first.parquet", true, firstRows);
        java.nio.file.Path second = writeBinaryParquetFile("derived-dictionary-second.parquet", true, secondRows);
        assertDictionaryEncoding(first, "name");
        assertDictionaryEncoding(second, "name");

        DictionaryVector retained;
        try (NitroParquetScanOperator scan = new NitroParquetScanOperator(NitroParquetScanResources.createDefault(), new Allocator(EngineResources.createDefault()), List.of(first, second), List.of("name"))) {
            try (Batch firstBatch = scan.next()) {
                // Join output can own a new mapping over a borrowed source dictionary. Closing the source batch must
                // not make that immutable value domain available as scratch for a later row group.
                retained = DictionaryVector.wrap(new int[] {0, 1}, firstBatch.output(0).borrow(Stream.VALUES));
            }
            while (scan.hasNext()) {
                try (Batch batch = scan.next()) {
                    batch.output(0).borrow(Stream.VALUES);
                }
            }

            BinaryVector values = (BinaryVector) retained.values();
            assertThat(utf8(values, retained.ids()[0])).isEqualTo("alpha");
            assertThat(utf8(values, retained.ids()[1])).isEqualTo("beta");
        }
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

    private java.nio.file.Path writeParquetFile(String name, boolean dictionaryEnabled, List<ParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        write(file, "nitro_test", List.of(
                requiredInt64("x", rows.stream().map(ParquetRow::x).toList()),
                requiredBoolean("flag", rows.stream().map(ParquetRow::flag).toList()),
                optionalInt64("maybe", rows.stream().map(ParquetRow::maybe).toList())), dictionaryEnabled);
        return file;
    }

    private java.nio.file.Path writeInt32ParquetFile(String name, List<Integer> values)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        write(file, "nitro_int32_test", List.of(requiredInt32("value", values)), values.size() >= 128);
        return file;
    }

    private java.nio.file.Path writeNullableInt32ParquetFile(String name, List<Integer> values)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        write(file, "nitro_nullable_int32_test", List.of(optionalInt32("value", values)), true);
        return file;
    }

    private java.nio.file.Path writeIntStringParquetFile(String name, List<Integer> values)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        write(file, "nitro_int_string_test", List.of(
                requiredInt32("value", values),
                requiredBinary("payload", values.stream().map(value -> "payload-" + value).toList()).asUtf8()), true);
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

    private record TestingTypedLongDomain(TypeBinding type, DynamicFilter filter, boolean includesNull)
            implements TypedDomain
    {
        private TestingTypedLongDomain(TypeBinding type, DynamicFilter filter)
        {
            this(type, filter, false);
        }

        @Override
        public boolean isAll()
        {
            return false;
        }

        @Override
        public boolean isNone()
        {
            return filter.isEmpty();
        }

        @Override
        public <T> java.util.Optional<T> capability(DomainCapability<T> capability)
        {
            if (capability == LongDomainCapability.LONG_DOMAIN) {
                return java.util.Optional.of(capability.valueType().cast(filter));
            }
            return java.util.Optional.empty();
        }
    }

    private record VectorTypeBinding(
            TypeIdentity identity,
            Class<?> carrierType,
            Set<Class<? extends org.weakref.nitro.data.Vector>> supportedVectorTypes)
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
                reader.addChunk(file.readRange(0, file.size()), file.columnChunk(rowGroup, column).meta_data, rowGroup.num_rows);
            }
        }
        return reader;
    }

    private java.nio.file.Path writeWideNumericParquetFile(String name, List<WideNumericRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        write(file, "nitro_wide_numeric_test", List.of(
                requiredInt64("key", rows.stream().map(WideNumericRow::key).toList()),
                requiredInt64("p1", rows.stream().map(WideNumericRow::p1).toList()),
                requiredInt64("p2", rows.stream().map(WideNumericRow::p2).toList()),
                requiredInt64("p3", rows.stream().map(WideNumericRow::p3).toList()),
                requiredInt64("p4", rows.stream().map(WideNumericRow::p4).toList())), true);
        return file;
    }

    private java.nio.file.Path writeBinaryParquetFile(String name, boolean dictionaryEnabled, List<BinaryParquetRow> rows)
            throws IOException
    {
        return writeBinaryParquetFile(name, dictionaryEnabled, CompressionCodec.UNCOMPRESSED, rows);
    }

    private java.nio.file.Path writeBinaryParquetFile(String name, boolean dictionaryEnabled, CompressionCodec compressionCodec, List<BinaryParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        write(file, "nitro_binary_test", List.of(
                requiredBinary("name", rows.stream().map(BinaryParquetRow::name).toList()).asUtf8(),
                optionalBinary("payload", rows.stream().map(BinaryParquetRow::payload).toList())), dictionaryEnabled, compressionCodec);
        return file;
    }

    private java.nio.file.Path writeRepeatedNullableI64ParquetFile(String name, List<NullableArrayParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        writeRepeatedOptionalInt64(file, "nitro_nullable_array_test", "items", rows.stream().map(NullableArrayParquetRow::items).toList());
        return file;
    }

    private java.nio.file.Path writeOptionalMapParquetFile(String name, List<MapParquetRow> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        writeOptionalUtf8LongMap(file, "nitro_optional_map_test", "items", rows.stream().map(MapParquetRow::items).toList());
        return file;
    }

    private java.nio.file.Path writeOptionalSimpleStructParquetFile(String name)
            throws IOException
    {
        java.nio.file.Path file = tempDirectory.resolve(name);
        List<LongStringStruct> rows = new ArrayList<>();
        rows.add(new LongStringStruct(11, "alice"));
        rows.add(null);
        rows.add(new LongStringStruct(12, null));
        writeOptionalLongUtf8Struct(file, "nitro_optional_simple_struct_test", "person", rows);
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

    private static Map<String, Long> orderedMap(Object... entries)
    {
        LinkedHashMap<String, Long> map = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            map.put((String) entries[index], (Long) entries[index + 1]);
        }
        return map;
    }

    private static void assertDictionaryEncoding(java.nio.file.Path file, String columnName)
    {
        try (ParquetFile parquet = ParquetFile.open(file)) {
            assertThat(parquet.rowGroups()).isNotEmpty();
            assertThat(parquet.rowGroups().getFirst().columns.stream()
                    .map(column -> column.meta_data)
                    .filter(metadata -> metadata.path_in_schema.equals(List.of(columnName)))
                    .flatMap(metadata -> metadata.encodings.stream()))
                    .anyMatch(encoding -> encoding == Encoding.RLE_DICTIONARY || encoding == Encoding.PLAIN_DICTIONARY);
        }
    }

    private static PrimitiveFunction eqUtf8()
    {
        return TestPrimitiveFunctions.primitiveRegistry().get("eq_utf8");
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

    private record NullableArrayParquetRow(List<Long> items) {}

    private record MapParquetRow(Map<String, Long> items) {}
}
