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

import org.apache.parquet.format.RowGroup;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.LongDomain;
import org.weakref.nitro.core.source.LongDomainCapability;
import org.weakref.nitro.core.source.OrdinalSourceColumnHandle;
import org.weakref.nitro.core.source.RuntimeFilter;
import org.weakref.nitro.core.source.RuntimeFilterAcceptance;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.core.source.SourceMetrics;
import org.weakref.nitro.core.source.SourceMetricsProtocol;
import org.weakref.nitro.core.source.SourceOutputDemand;
import org.weakref.nitro.core.source.SourceOutputDemandProtocol;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.source.SourceProtocol;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.ValueDemand;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorBatchScope;
import org.weakref.nitro.data.VectorColumnGeneration;
import org.weakref.nitro.data.VectorSourceBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.lang.Math.addExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/** Native mixed flat/nested Parquet source. Unsupported nested layouts fail during construction. */
final class NestedNitroParquetBatchSource
        implements BatchSource
{
    private interface ProjectedReader
            extends AutoCloseable
    {
        void addRowGroup(ParquetFile file, RowGroup rowGroup);

        Streams read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask);

        default void setValueDemand(ValueDemand demand)
        {
            requireNonNull(demand, "demand is null");
            // ValueDemand is an optimization request. Readers which cannot expose the requested physical
            // representation conservatively retain their ordinary full-value output.
        }

        default boolean supportsLongDomain()
        {
            return false;
        }

        default boolean chunkMayMatch(int index, LongDomain domain)
        {
            return true;
        }

        default boolean dictionaryMayMatch(int index, LongDomain domain, int maxDictionaryValues)
        {
            return true;
        }

        default boolean supportsIndependentNulls()
        {
            return false;
        }

        default BooleanVector readNulls(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
        {
            throw new UnsupportedOperationException("Reader does not support independent null resolution");
        }

        default void skipNulls(long rowCount)
        {
            throw new UnsupportedOperationException("Reader does not support independent null resolution");
        }

        void skip(long rowCount);

        long consumedPageBytes();

        Set<Stream> streams();

        @Override
        void close();
    }

    private final class PrimitiveProjectedReader
            implements ProjectedReader
    {
        private static final long[] EMPTY_LONGS = new long[0];

        private final ParquetSchema.Primitive leaf;
        private final ColumnReader reader;
        private final TypeBinding outputType;
        private final ParquetPrimitiveValueBinding.Bound logicalValueBinding;
        private final boolean intOutputAsLong;
        private long[] doubleScratch = EMPTY_LONGS;

        private PrimitiveProjectedReader(
                ParquetSchema.Primitive leaf,
                TypeBinding outputType,
                ParquetPrimitiveValueBinding logicalValueBinding)
        {
            this.leaf = leaf;
            this.outputType = outputType;
            this.logicalValueBinding = logicalValueBinding == null
                    ? null
                    : requireNonNull(logicalValueBinding.bind(leaf.descriptor(), outputType), "logical value binding returned null");
            if (leaf.maximumRepetitionLevel() != 0) {
                throw new UnsupportedParquetFeatureException(
                        "Direct primitive projection does not support repeated path '" + String.join(".", leaf.path()) + "'");
            }
            if (leaf.maximumDefinitionLevel() > 1) {
                throw new UnsupportedParquetFeatureException(
                        "Direct primitive projection does not support multiple nullable path components '" +
                                String.join(".", leaf.path()) + "'");
            }
            this.reader = new ColumnReader(
                    leaf.type(),
                    leaf.maximumDefinitionLevel() != 0,
                    leaf.typeLength(),
                    leaf.decimal(),
                    leaf.string(),
                    null,
                    allocator.primitiveArrays(),
                    resources.readerPolicy(),
                    resources.decodeScratchPool());
            Set<Class<? extends Vector>> vectors = outputType.supportedVectorTypes();
            this.intOutputAsLong = reader.kind() == ColumnReader.Kind.INT &&
                    ((this.logicalValueBinding != null && this.logicalValueBinding.decodedVectorType() == I64Vector.class) ||
                            (!vectors.contains(I32Vector.class) && vectors.contains(I64Vector.class)));
        }

        @Override
        public void addRowGroup(ParquetFile file, RowGroup rowGroup)
        {
            reader.addChunk(file, file.columnChunk(rowGroup, leaf).meta_data, rowGroup.num_rows, null);
        }

        @Override
        public void setValueDemand(ValueDemand demand)
        {
            reader.setDictionaryDomainMetadataDemand(requireNonNull(demand, "demand is null"));
        }

        @Override
        public boolean supportsLongDomain()
        {
            return (logicalValueBinding == null || logicalValueBinding.preservesLongDomain()) &&
                    reader.kind() != ColumnReader.Kind.BINARY &&
                    !reader.isDouble();
        }

        @Override
        public boolean chunkMayMatch(int index, LongDomain domain)
        {
            return reader.chunkMayMatch(index, domain);
        }

        @Override
        public boolean dictionaryMayMatch(int index, LongDomain domain, int maxDictionaryValues)
        {
            return reader.dictionaryMayMatch(index, domain, maxDictionaryValues);
        }

        @Override
        public Streams read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
        {
            BooleanVector nulls = nullable()
                    ? allocator.allocate(context, BooleanVector.class, rowCount, BooleanVector::new)
                    : null;
            boolean[] nullValues = nulls == null ? null : nulls.values();
            Vector values = switch (reader.kind()) {
                case INT -> reader.readNumeric(allocator, context, nullValues, rowCount, intOutputAsLong);
                case LONG -> readLong(allocator, context, nullValues, rowCount);
                case BINARY -> reader.readBinary(allocator, context, nullValues, rowCount);
            };
            if (nulls != null && reader.lastReadNullsProvenAbsent()) {
                nulls.declareAllFalse();
            }
            if (logicalValueBinding != null) {
                values = logicalValueBinding.convert(allocator, context, values);
            }
            if (!outputType.supportsVector(values)) {
                throw new UnsupportedParquetFeatureException(
                        "Native Parquet representation " + values.getClass().getSimpleName() +
                                " is not supported by output type " + outputType.identity());
            }
            return nulls == null ? Streams.ofValues(values) : Streams.ofValuesAndNulls(values, nulls);
        }

        private Vector readLong(Allocator allocator, Allocator.Context context, boolean[] nulls, int rowCount)
        {
            if (!reader.isDouble()) {
                return reader.readNumeric(allocator, context, nulls, rowCount, false);
            }
            F64Vector values = F64Vector.allocate(allocator, context, rowCount);
            if (doubleScratch.length < rowCount) {
                long[] replacement = allocator.primitiveArrays().borrowLongs(rowCount);
                allocator.primitiveArrays().release(doubleScratch);
                doubleScratch = replacement;
            }
            reader.readLongs(doubleScratch, nulls, rowCount);
            for (int position = 0; position < rowCount; position++) {
                values.values()[position] = Double.longBitsToDouble(doubleScratch[position]);
            }
            return values;
        }

        private boolean nullable()
        {
            return leaf.maximumDefinitionLevel() != 0;
        }

        @Override
        public void skip(long rowCount)
        {
            reader.skip(rowCount);
        }

        @Override
        public long consumedPageBytes()
        {
            return reader.consumedPageBytes();
        }

        @Override
        public Set<Stream> streams()
        {
            return nullable() ? Set.of(Stream.VALUES, Stream.NULLS) : Set.of(Stream.VALUES);
        }

        @Override
        public void close()
        {
            reader.close();
            allocator.primitiveArrays().release(doubleScratch);
            doubleScratch = EMPTY_LONGS;
        }
    }

    private static final class NestedPrimitiveProjectedReader
            implements ProjectedReader
    {
        private final ParquetSchema.Primitive leaf;
        private final NestedLeafReader reader;
        private final NestedValueAccumulator values;
        private final TypeBinding outputType;
        private final ParquetPrimitiveValueBinding.Bound logicalValueBinding;
        private final boolean nullable;

        private NestedPrimitiveProjectedReader(
                ParquetSchema.Primitive leaf,
                TypeBinding outputType,
                ParquetPrimitiveValueBinding logicalValueBinding,
                RleReaderPolicy rlePolicy,
                ParquetMaterializationPolicy materializationPolicy,
                PrimitiveArrayPool arrayPool)
        {
            this.leaf = requireNonNull(leaf, "leaf is null");
            if (leaf.maximumRepetitionLevel() != 0) {
                throw new UnsupportedParquetFeatureException(
                        "Direct primitive projection does not support repeated path '" + String.join(".", leaf.path()) + "'");
            }
            this.outputType = requireNonNull(outputType, "outputType is null");
            this.logicalValueBinding = logicalValueBinding == null
                    ? null
                    : requireNonNull(logicalValueBinding.bind(leaf.descriptor(), outputType), "logical value binding returned null");
            this.nullable = leaf.maximumDefinitionLevel() != 0;
            this.reader = new NestedLeafReader(leaf, rlePolicy, arrayPool);
            this.values = NestedValueAccumulators.create(leaf, nullable, materializationPolicy);
        }

        @Override
        public void addRowGroup(ParquetFile file, RowGroup rowGroup)
        {
            reader.addChunk(file, file.columnChunk(rowGroup, leaf).meta_data);
        }

        @Override
        public Streams read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
        {
            if (rowCount < 0 || mask.size() != rowCount) {
                throw new IllegalArgumentException("Nested primitive row count and mask length differ: " + rowCount + " != " + mask.size());
            }
            values.reset(allocator, context, rowCount);
            if (mask.all()) {
                reader.appendFlatRows(values, rowCount);
                return materialize(allocator, context);
            }
            int selectedIndex = 0;
            int nextSelected = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
            int runOrdinal = -1;
            int runLength = 0;
            for (int row = 0; row < rowCount; row++) {
                if (!reader.next()) {
                    throw new IllegalArgumentException("Nested primitive event stream ended before requested rows");
                }
                if (reader.repetitionLevel() != 0) {
                    throw new IllegalArgumentException("Projected primitive row starts with nonzero repetition level");
                }
                boolean selected = mask.all() || row == nextSelected;
                if ((selected || !nullable) && reader.hasValue()) {
                    int ordinal = reader.valueOrdinal();
                    int dictionaryId = reader.dictionaryId();
                    if (dictionaryId < 0 && (runLength == 0 || ordinal == runOrdinal + runLength)) {
                        if (runLength == 0) {
                            runOrdinal = ordinal;
                        }
                        runLength++;
                    }
                    else {
                        if (runLength != 0) {
                            values.appendPlainRun(reader.valueDecoder(), runOrdinal, runLength);
                            runLength = 0;
                        }
                        values.append(reader.valueDecoder(), ordinal, dictionaryId);
                    }
                }
                else {
                    if (runLength != 0) {
                        values.appendPlainRun(reader.valueDecoder(), runOrdinal, runLength);
                        runLength = 0;
                    }
                    values.appendNull();
                }
                if (selected && !mask.all()) {
                    selectedIndex++;
                    nextSelected = selectedIndex < mask.count() ? mask.position(selectedIndex) : rowCount;
                }
                if (runLength != 0 && reader.pageExhausted()) {
                    values.appendPlainRun(reader.valueDecoder(), runOrdinal, runLength);
                    runLength = 0;
                }
            }
            if (runLength != 0) {
                values.appendPlainRun(reader.valueDecoder(), runOrdinal, runLength);
            }
            return materialize(allocator, context);
        }

        private Streams materialize(Allocator allocator, Allocator.Context context)
        {
            Streams streams = values.materialize(allocator, context);
            if (logicalValueBinding != null) {
                streams = streams.with(Stream.VALUES, logicalValueBinding.convert(allocator, context, streams.values()));
            }
            if (!outputType.supportsVector(streams.values())) {
                throw new UnsupportedParquetFeatureException(
                        "Native nested primitive representation does not match output type " + outputType.identity());
            }
            return streams;
        }

        @Override
        public void skip(long rowCount)
        {
            if (rowCount < 0) {
                throw new IllegalArgumentException("rowCount is negative");
            }
            for (long row = 0; row < rowCount; row++) {
                if (!reader.next()) {
                    throw new IllegalArgumentException("Nested primitive event stream ended before requested rows");
                }
            }
        }

        @Override
        public long consumedPageBytes()
        {
            return reader.consumedPageBytes();
        }

        @Override
        public Set<Stream> streams()
        {
            return nullable ? Set.of(Stream.VALUES, Stream.NULLS) : Set.of(Stream.VALUES);
        }

        @Override
        public void close()
        {
            RuntimeException failure = null;
            try {
                reader.close();
            }
            catch (RuntimeException e) {
                failure = e;
            }
            try {
                values.close();
            }
            catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class MapProjectedReader
            implements ProjectedReader
    {
        private final NestedMapReader reader;
        private final NestedRepeatedShapeReader shapeReader;
        private final TypeBinding outputType;
        private final boolean nullable;
        private ValueDemand valueDemand = ValueDemand.FULL;

        private MapProjectedReader(
                ParquetSchema.Group map,
                TypeBinding outputType,
                ParquetValueBinding.Group logicalBinding,
                RleReaderPolicy rlePolicy,
                ParquetMaterializationPolicy materializationPolicy,
                PrimitiveArrayPool arrayPool)
        {
            this.reader = logicalBinding == null
                    ? new NestedMapReader(map, rlePolicy, materializationPolicy, arrayPool)
                    : new NestedMapReader(map, rlePolicy, materializationPolicy, arrayPool, outputType, logicalBinding);
            ParquetSchema.Group entries = (ParquetSchema.Group) map.children().getFirst();
            this.shapeReader = new NestedRepeatedShapeReader(
                    map,
                    entries,
                    (ParquetSchema.Primitive) entries.children().getFirst(),
                    rlePolicy,
                    arrayPool);
            this.outputType = requireNonNull(outputType, "outputType is null");
            this.nullable = map.repetition() != org.apache.parquet.format.FieldRepetitionType.REQUIRED;
        }

        @Override
        public void addRowGroup(ParquetFile file, RowGroup rowGroup)
        {
            reader.addRowGroup(file, rowGroup);
            shapeReader.addRowGroup(file, rowGroup);
        }

        @Override
        public void setValueDemand(ValueDemand demand)
        {
            this.valueDemand = requireNonNull(demand, "demand is null") == ValueDemand.STRUCTURE
                    ? ValueDemand.STRUCTURE
                    : ValueDemand.FULL;
        }

        @Override
        public Streams read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
        {
            Streams streams;
            if (valueDemand == ValueDemand.STRUCTURE) {
                MapVector maps = allocator.allocateMap(context, rowCount);
                BooleanVector nulls = nullable
                        ? allocator.allocate(context, BooleanVector.class, rowCount, BooleanVector::new)
                        : null;
                shapeReader.read(maps.offsets(), nulls == null ? null : nulls.values(), rowCount, mask);
                streams = nulls == null ? Streams.ofValues(maps) : Streams.ofValuesAndNulls(maps, nulls);
            }
            else {
                streams = reader.read(allocator, context, rowCount, mask);
            }
            if (valueDemand == ValueDemand.FULL && !outputType.supportsVector(streams.values())) {
                throw new UnsupportedParquetFeatureException(
                        "Native Parquet MAP representation does not match output type " + outputType.identity());
            }
            return streams;
        }

        @Override
        public void skip(long rowCount)
        {
            if (valueDemand == ValueDemand.STRUCTURE) {
                shapeReader.skip(rowCount);
            }
            else {
                reader.skip(rowCount);
            }
        }

        @Override
        public long consumedPageBytes()
        {
            return valueDemand == ValueDemand.STRUCTURE ? shapeReader.consumedPageBytes() : reader.consumedPageBytes();
        }

        @Override
        public Set<Stream> streams()
        {
            return nullable ? Set.of(Stream.VALUES, Stream.NULLS) : Set.of(Stream.VALUES);
        }

        @Override
        public void close()
        {
            try (reader; shapeReader) {
                // Closing releases the selected and dormant reader resources.
            }
        }
    }

    private static final class ArrayProjectedReader
            implements ProjectedReader
    {
        private final NestedArrayReader reader;
        private final NestedRepeatedShapeReader shapeReader;
        private final TypeBinding outputType;
        private final boolean nullable;
        private ValueDemand valueDemand = ValueDemand.FULL;

        private ArrayProjectedReader(
                ParquetSchema.Group list,
                TypeBinding outputType,
                ParquetValueBinding.Group logicalBinding,
                RleReaderPolicy rlePolicy,
                ParquetMaterializationPolicy materializationPolicy,
                PrimitiveArrayPool arrayPool)
        {
            this.reader = logicalBinding == null
                    ? new NestedArrayReader(list, rlePolicy, materializationPolicy, arrayPool)
                    : new NestedArrayReader(list, rlePolicy, materializationPolicy, arrayPool, outputType, logicalBinding);
            ParquetSchema.Group repeatedValues = list.isList()
                    ? (ParquetSchema.Group) list.children().getFirst()
                    : list;
            this.shapeReader = new NestedRepeatedShapeReader(
                    list,
                    repeatedValues,
                    firstPrimitive(repeatedValues.children().getFirst()),
                    rlePolicy,
                    arrayPool);
            this.outputType = requireNonNull(outputType, "outputType is null");
            this.nullable = list.repetition() != org.apache.parquet.format.FieldRepetitionType.REQUIRED;
        }

        @Override
        public void addRowGroup(ParquetFile file, RowGroup rowGroup)
        {
            reader.addRowGroup(file, rowGroup);
            shapeReader.addRowGroup(file, rowGroup);
        }

        @Override
        public void setValueDemand(ValueDemand demand)
        {
            this.valueDemand = requireNonNull(demand, "demand is null") == ValueDemand.STRUCTURE
                    ? ValueDemand.STRUCTURE
                    : ValueDemand.FULL;
        }

        @Override
        public Streams read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
        {
            Streams streams;
            if (valueDemand == ValueDemand.STRUCTURE) {
                ArrayVector arrays = allocator.allocateArray(context, rowCount);
                BooleanVector nulls = nullable
                        ? allocator.allocate(context, BooleanVector.class, rowCount, BooleanVector::new)
                        : null;
                shapeReader.read(arrays.offsets(), nulls == null ? null : nulls.values(), rowCount, mask);
                streams = nulls == null ? Streams.ofValues(arrays) : Streams.ofValuesAndNulls(arrays, nulls);
            }
            else {
                streams = reader.read(allocator, context, rowCount, mask);
            }
            if (valueDemand == ValueDemand.FULL && !outputType.supportsVector(streams.values())) {
                throw new UnsupportedParquetFeatureException(
                        "Native Parquet LIST representation does not match output type " + outputType.identity());
            }
            return streams;
        }

        @Override
        public void skip(long rowCount)
        {
            if (valueDemand == ValueDemand.STRUCTURE) {
                shapeReader.skip(rowCount);
            }
            else {
                reader.skip(rowCount);
            }
        }

        @Override
        public long consumedPageBytes()
        {
            return valueDemand == ValueDemand.STRUCTURE ? shapeReader.consumedPageBytes() : reader.consumedPageBytes();
        }

        @Override
        public Set<Stream> streams()
        {
            return nullable ? Set.of(Stream.VALUES, Stream.NULLS) : Set.of(Stream.VALUES);
        }

        @Override
        public void close()
        {
            try (reader; shapeReader) {
                // Closing releases the selected and dormant reader resources.
            }
        }
    }

    private static final class StructProjectedReader
            implements ProjectedReader
    {
        private final NestedStructReader reader;
        private final NestedStructNullReader nullReader;
        private final TypeBinding outputType;
        private final boolean nullable;

        private StructProjectedReader(
                ParquetSchema.Group struct,
                TypeBinding outputType,
                ParquetValueBinding.Group logicalBinding,
                RleReaderPolicy rlePolicy,
                ParquetMaterializationPolicy materializationPolicy,
                PrimitiveArrayPool arrayPool)
        {
            this.reader = logicalBinding == null
                    ? new NestedStructReader(struct, rlePolicy, materializationPolicy, arrayPool)
                    : new NestedStructReader(struct, rlePolicy, materializationPolicy, arrayPool, outputType, logicalBinding);
            this.outputType = requireNonNull(outputType, "outputType is null");
            this.nullable = struct.repetition() != org.apache.parquet.format.FieldRepetitionType.REQUIRED;
            this.nullReader = nullable ? new NestedStructNullReader(struct, rlePolicy, arrayPool) : null;
        }

        @Override
        public void addRowGroup(ParquetFile file, RowGroup rowGroup)
        {
            reader.addRowGroup(file, rowGroup);
            if (nullReader != null) {
                nullReader.addRowGroup(file, rowGroup);
            }
        }

        @Override
        public Streams read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
        {
            Streams streams = reader.read(allocator, context, rowCount, mask);
            if (!outputType.supportsVector(streams.values())) {
                throw new UnsupportedParquetFeatureException(
                        "Native Parquet struct representation does not match output type " + outputType.identity());
            }
            return streams;
        }

        @Override
        public void skip(long rowCount)
        {
            reader.skip(rowCount);
        }

        @Override
        public boolean supportsIndependentNulls()
        {
            return nullReader != null;
        }

        @Override
        public BooleanVector readNulls(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
        {
            return requireNonNull(nullReader, "nullReader is null").read(allocator, context, rowCount, mask);
        }

        @Override
        public void skipNulls(long rowCount)
        {
            requireNonNull(nullReader, "nullReader is null").skip(rowCount);
        }

        @Override
        public long consumedPageBytes()
        {
            return Math.addExact(reader.consumedPageBytes(), nullReader == null ? 0 : nullReader.consumedPageBytes());
        }

        @Override
        public Set<Stream> streams()
        {
            return nullable ? Set.of(Stream.VALUES, Stream.NULLS) : Set.of(Stream.VALUES);
        }

        @Override
        public void close()
        {
            RuntimeException failure = null;
            try {
                reader.close();
            }
            catch (RuntimeException e) {
                failure = e;
            }
            try {
                if (nullReader != null) {
                    nullReader.close();
                }
            }
            catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private final NitroParquetScanResources resources;
    private final Allocator allocator;
    private final Schema schema;
    private final ParquetFile[] files;
    private final ProjectedReader[] readers;
    private final SourceColumnHandle[] sourceColumns;
    private final ValueDemand[] outputDemands;
    private final LongDomain[] rowGroupFilters;
    private final long[] rowGroupRows;
    private final long[] pendingRows;
    private final long[] pendingNullRows;
    private final VectorBatchScope batchScope;
    private final Allocator.Context allocationContext;
    private final int batchRows;
    private final long totalRows;

    private long nextRow;
    private long prunedRows;
    private int rowGroupIndex;
    private long rowGroupRemaining;
    private boolean rowGroupTracking;
    private boolean hasRowGroupFilters;
    private BatchState currentBatch;
    private boolean closed;

    NestedNitroParquetBatchSource(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<NitroParquetBatchSource.InputSplit> splits,
            Schema schema,
            ParquetColumnNameMatching columnNameMatching,
            List<Integer> sourceOrdinals)
    {
        this(resources, allocator, splits, schema, columnNameMatching, sourceOrdinals, null, false, Map.of());
    }

    NestedNitroParquetBatchSource(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<NitroParquetBatchSource.InputSplit> splits,
            Schema schema,
            ParquetColumnNameMatching columnNameMatching,
            List<Integer> sourceOrdinals,
            Map<Integer, ParquetValueBinding> logicalValueBindings)
    {
        this(resources, allocator, splits, schema, columnNameMatching, sourceOrdinals, null, false, logicalValueBindings);
    }

    NestedNitroParquetBatchSource(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<NitroParquetBatchSource.InputSplit> splits,
            Schema schema,
            ParquetColumnNameMatching columnNameMatching,
            List<NitroParquetBatchSource.ColumnProjection> projections,
            boolean projectionsByName)
    {
        this(resources, allocator, splits, schema, columnNameMatching, null, projections, projectionsByName, Map.of());
    }

    NestedNitroParquetBatchSource(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<NitroParquetBatchSource.InputSplit> splits,
            Schema schema,
            ParquetColumnNameMatching columnNameMatching,
            List<NitroParquetBatchSource.ColumnProjection> projections,
            boolean projectionsByName,
            Map<Integer, ParquetValueBinding> logicalValueBindings)
    {
        this(resources, allocator, splits, schema, columnNameMatching, null, projections, projectionsByName, logicalValueBindings);
    }

    private NestedNitroParquetBatchSource(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<NitroParquetBatchSource.InputSplit> splits,
            Schema schema,
            ParquetColumnNameMatching columnNameMatching,
            List<Integer> sourceOrdinals,
            List<NitroParquetBatchSource.ColumnProjection> projections,
            boolean projectionsByName,
            Map<Integer, ParquetValueBinding> logicalValueBindings)
    {
        this.resources = requireNonNull(resources, "resources is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.schema = requireNonNull(schema, "schema is null");
        requireNonNull(columnNameMatching, "columnNameMatching is null");
        logicalValueBindings = Map.copyOf(requireNonNull(logicalValueBindings, "logicalValueBindings is null"));
        splits = List.copyOf(splits);
        if (splits.isEmpty()) {
            throw new IllegalArgumentException("splits is empty");
        }
        if (sourceOrdinals != null && sourceOrdinals.size() != schema.size()) {
            throw new IllegalArgumentException("source ordinals size does not match projected columns");
        }
        if (projections != null && projections.size() != schema.size()) {
            throw new IllegalArgumentException("source projections size does not match projected columns");
        }
        if (sourceOrdinals != null && projections != null) {
            throw new IllegalArgumentException("source ordinals and projections are mutually exclusive");
        }

        this.files = new ParquetFile[splits.size()];
        for (int index = 0; index < files.length; index++) {
            files[index] = ParquetFile.open(splits.get(index).input(), resources.metadataCache());
        }
        this.readers = new ProjectedReader[schema.size()];
        this.sourceColumns = new SourceColumnHandle[schema.size()];
        this.outputDemands = new ValueDemand[schema.size()];
        java.util.Arrays.fill(outputDemands, ValueDemand.FULL);
        this.rowGroupFilters = new LongDomain[schema.size()];
        this.pendingRows = new long[schema.size()];
        this.pendingNullRows = new long[schema.size()];
        for (int column = 0; column < schema.size(); column++) {
            String name = sourceOrdinals == null && projections == null
                    ? schema.field(column).name().orElseThrow(
                            () -> new IllegalArgumentException("nested Parquet source requires named output fields"))
                    : "";
            ParquetSchema.Node node = projections == null
                    ? resolveNode(files[0].schema(), name, columnNameMatching, sourceOrdinals, column)
                    : resolveProjection(files[0].schema(), columnNameMatching, projections.get(column), projectionsByName);
            readers[column] = createReader(node, schema.field(column).type(), logicalValueBindings.get(column));
            sourceColumns[column] = new OrdinalSourceColumnHandle(column, schema.field(column).type());
        }

        long rows = 0;
        List<Long> rowCounts = new ArrayList<>();
        for (int fileIndex = 0; fileIndex < files.length; fileIndex++) {
            ParquetFile file = files[fileIndex];
            NitroParquetBatchSource.InputSplit split = splits.get(fileIndex);
            List<RowGroup> rowGroups = file.rowGroups(split.start(), split.length());
            for (RowGroup rowGroup : rowGroups) {
                rows = addExact(rows, rowGroup.num_rows);
                rowCounts.add(rowGroup.num_rows);
                for (ProjectedReader reader : readers) {
                    reader.addRowGroup(file, rowGroup);
                }
            }
        }
        this.totalRows = rows;
        this.rowGroupRows = rowCounts.stream().mapToLong(Long::longValue).toArray();
        this.batchRows = resources.batchPolicy().initialRows();
        this.batchScope = new VectorBatchScope(allocator, "NestedNitroParquetBatchSource", resources.batchBufferPool());
        this.allocationContext = batchScope.context();
    }

    private ProjectedReader createReader(
            ParquetSchema.Node node,
            TypeBinding outputType,
            ParquetValueBinding logicalValueBinding)
    {
        return switch (node) {
            case ParquetSchema.Primitive primitive when primitive.maximumDefinitionLevel() > 1 ->
                    new NestedPrimitiveProjectedReader(
                            primitive,
                            outputType,
                            primitiveBinding(primitive, logicalValueBinding),
                            resources.readerPolicy().rle(),
                            resources.readerPolicy().materialization(),
                            allocator.primitiveArrays());
            case ParquetSchema.Primitive primitive -> new PrimitiveProjectedReader(
                    primitive, outputType, primitiveBinding(primitive, logicalValueBinding));
            case ParquetSchema.Group group when group.isMap() -> {
                ParquetValueBinding.Group groupBinding = groupBinding(group, logicalValueBinding);
                if (!outputType.supportedVectorTypes().contains(MapVector.class)) {
                    throw new UnsupportedParquetFeatureException(
                            "Parquet MAP field '" + group.name() + "' has no MapVector output representation");
                }
                yield new MapProjectedReader(
                        group,
                        outputType,
                        groupBinding,
                        resources.readerPolicy().rle(),
                        resources.readerPolicy().materialization(),
                        allocator.primitiveArrays());
            }
            case ParquetSchema.Group group when group.isList() ||
                    (group.repetition() == org.apache.parquet.format.FieldRepetitionType.REPEATED &&
                            outputType.supportedVectorTypes().contains(ArrayVector.class)) -> {
                ParquetValueBinding.Group groupBinding = groupBinding(group, logicalValueBinding);
                if (!outputType.supportedVectorTypes().contains(ArrayVector.class)) {
                    throw new UnsupportedParquetFeatureException(
                            "Parquet LIST field '" + group.name() + "' has no ArrayVector output representation");
                }
                yield new ArrayProjectedReader(
                        group,
                        outputType,
                        groupBinding,
                        resources.readerPolicy().rle(),
                        resources.readerPolicy().materialization(),
                        allocator.primitiveArrays());
            }
            case ParquetSchema.Group group when outputType.supportedVectorTypes().contains(StructVector.class) -> {
                ParquetValueBinding.Group groupBinding = groupBinding(group, logicalValueBinding);
                yield new StructProjectedReader(
                        group,
                        outputType,
                        groupBinding,
                        resources.readerPolicy().rle(),
                        resources.readerPolicy().materialization(),
                        allocator.primitiveArrays());
            }
            case ParquetSchema.Group group -> throw new UnsupportedParquetFeatureException(
                    "Native Nitro Parquet reader does not support nested field '" + group.name() + "' with this logical layout");
        };
    }

    private static ParquetPrimitiveValueBinding primitiveBinding(
            ParquetSchema.Primitive primitive,
            ParquetValueBinding binding)
    {
        return switch (binding) {
            case null -> null;
            case ParquetValueBinding.Direct ignored -> null;
            case ParquetPrimitiveValueBinding primitiveBinding -> primitiveBinding;
            case ParquetValueBinding.Group ignored -> throw new UnsupportedParquetFeatureException(
                    "Logical group binding cannot be applied to primitive Parquet field '" +
                            String.join(".", primitive.path()) + "'");
        };
    }

    private static ParquetValueBinding.Group groupBinding(ParquetSchema.Group group, ParquetValueBinding binding)
    {
        return switch (binding) {
            case null -> null;
            case ParquetValueBinding.Group nested -> nested;
            case ParquetPrimitiveValueBinding ignored -> throw mismatchedGroupBinding(group);
            case ParquetValueBinding.Direct ignored -> throw mismatchedGroupBinding(group);
        };
    }

    private static UnsupportedParquetFeatureException mismatchedGroupBinding(ParquetSchema.Group group)
    {
        return new UnsupportedParquetFeatureException(
                "Primitive logical value binding cannot be applied to nested field '" + group.name() + "'");
    }

    private static ParquetSchema.Primitive firstPrimitive(ParquetSchema.Node node)
    {
        return switch (node) {
            case ParquetSchema.Primitive primitive -> primitive;
            case ParquetSchema.Group group when !group.children().isEmpty() -> firstPrimitive(group.children().getFirst());
            case ParquetSchema.Group group -> throw new UnsupportedParquetFeatureException(
                    "Native nested Parquet field '" + group.name() + "' has no physical leaf");
        };
    }

    private static ParquetSchema.Node resolveNode(
            ParquetSchema parquetSchema,
            String name,
            ParquetColumnNameMatching matching,
            List<Integer> sourceOrdinals,
            int outputColumn)
    {
        if (sourceOrdinals != null) {
            return parquetSchema.fields().get(sourceOrdinals.get(outputColumn));
        }
        if (matching == ParquetColumnNameMatching.EXACT) {
            return parquetSchema.field(name);
        }
        String normalized = name.toLowerCase(Locale.ROOT);
        ParquetSchema.Node result = null;
        for (ParquetSchema.Node field : parquetSchema.fields()) {
            if (field.name().toLowerCase(Locale.ROOT).equals(normalized)) {
                if (result != null) {
                    throw new IllegalArgumentException("Ambiguous case-insensitive column: " + name);
                }
                result = field;
            }
        }
        if (result == null) {
            throw new IllegalArgumentException("No such Parquet field: " + name);
        }
        return result;
    }

    private static ParquetSchema.Node resolveProjection(
            ParquetSchema parquetSchema,
            ParquetColumnNameMatching matching,
            NitroParquetBatchSource.ColumnProjection projection,
            boolean byName)
    {
        ParquetSchema.Node node = byName
                ? resolveField(parquetSchema.fields(), projection.baseName(), matching)
                : parquetSchema.fields().get(projection.baseOrdinal());
        for (int depth = 0; depth < projection.fieldOrdinals().size(); depth++) {
            if (!(node instanceof ParquetSchema.Group group)) {
                throw new UnsupportedParquetFeatureException(
                        "Parquet projection descends through primitive field '" + node.name() + "'");
            }
            node = byName
                    ? resolveField(group.children(), projection.fieldNames().get(depth), matching)
                    : group.children().get(projection.fieldOrdinals().get(depth));
        }
        return node;
    }

    private static ParquetSchema.Node resolveField(
            List<ParquetSchema.Node> fields,
            String name,
            ParquetColumnNameMatching matching)
    {
        if (matching == ParquetColumnNameMatching.EXACT) {
            return fields.stream()
                    .filter(field -> field.name().equals(name))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("No such Parquet field: " + name));
        }
        String normalized = name.toLowerCase(Locale.ROOT);
        ParquetSchema.Node result = null;
        for (ParquetSchema.Node field : fields) {
            if (field.name().toLowerCase(Locale.ROOT).equals(normalized)) {
                if (result != null) {
                    throw new IllegalArgumentException("Ambiguous case-insensitive column: " + name);
                }
                result = field;
            }
        }
        if (result == null) {
            throw new IllegalArgumentException("No such Parquet field: " + name);
        }
        return result;
    }

    @Override
    public Schema schema()
    {
        return schema;
    }

    @Override
    public SourceColumnHandle column(int outputIndex)
    {
        return sourceColumns[outputIndex];
    }

    @Override
    public Set<SourceCapability> capabilities()
    {
        return Set.of(SourceCapability.LAZY_COLUMNS, SourceCapability.SELECTION_PUSHDOWN, SourceCapability.RUNTIME_FILTER);
    }

    @Override
    public OptionalLong exactRows()
    {
        return OptionalLong.of(totalRows);
    }

    @Override
    public <T> Optional<T> protocol(SourceProtocol<T> protocol)
    {
        if (protocol == SourceOutputDemandProtocol.OUTPUT_DEMAND) {
            SourceOutputDemand demand = this::retainOutputs;
            return Optional.of(protocol.valueType().cast(demand));
        }
        if (protocol == SourceMetricsProtocol.METRICS) {
            SourceMetrics metrics = new SourceMetrics()
            {
                @Override
                public OptionalLong completedBytes()
                {
                    return nextRow == totalRows ? OptionalLong.of(consumedPageBytes()) : OptionalLong.empty();
                }

                @Override
                public OptionalLong completedPositions()
                {
                    return OptionalLong.of(nextRow - prunedRows);
                }

                @Override
                public OptionalLong readTimeNanos()
                {
                    return OptionalLong.empty();
                }
            };
            return Optional.of(protocol.valueType().cast(metrics));
        }
        return Optional.empty();
    }

    @Override
    public RuntimeFilterAcceptance addRuntimeFilter(RuntimeFilter filter)
    {
        checkOpen();
        requireNonNull(filter, "filter is null");
        int column = columnIndex(filter.column());
        if (column < 0 || !readers[column].supportsLongDomain()) {
            return RuntimeFilterAcceptance.REJECTED;
        }
        LongDomain domain = filter.domain().capability(LongDomainCapability.LONG_DOMAIN).orElse(null);
        if (domain == null || filter.domain().includesNull()) {
            return RuntimeFilterAcceptance.REJECTED;
        }
        LongDomain existing = rowGroupFilters[column];
        if (existing == null || domain.size() < existing.size()) {
            rowGroupFilters[column] = domain;
            hasRowGroupFilters = true;
        }
        return RuntimeFilterAcceptance.ACCEPTED_WITH_RESIDUAL;
    }

    @Override
    public boolean supportsRuntimeFilter(SourceColumnHandle column)
    {
        int index = columnIndex(requireNonNull(column, "column is null"));
        return index >= 0 && readers[index].supportsLongDomain();
    }

    private void retainOutputs(Map<SourceColumnHandle, ValueDemand> outputs)
    {
        checkOpen();
        if (nextRow != 0 || currentBatch != null) {
            throw new IllegalStateException("output demand must be declared before polling");
        }
        requireNonNull(outputs, "outputs is null");
        java.util.Arrays.fill(outputDemands, null);
        for (Map.Entry<SourceColumnHandle, ValueDemand> entry : outputs.entrySet()) {
            int column = columnIndex(requireNonNull(entry.getKey(), "output is null"));
            if (column < 0) {
                throw new IllegalArgumentException("output belongs to another source");
            }
            ValueDemand demand = requireNonNull(entry.getValue(), "value demand is null");
            readers[column].setValueDemand(demand);
            outputDemands[column] = demand;
        }
    }

    private int columnIndex(SourceColumnHandle output)
    {
        if (output instanceof OrdinalSourceColumnHandle ordinal &&
                ordinal.ordinal() >= 0 && ordinal.ordinal() < sourceColumns.length &&
                sourceColumns[ordinal.ordinal()] == output) {
            return ordinal.ordinal();
        }
        for (int column = 0; column < sourceColumns.length; column++) {
            if (sourceColumns[column].equals(output)) {
                return column;
            }
        }
        return -1;
    }

    private long consumedPageBytes()
    {
        long bytes = 0;
        for (ProjectedReader reader : readers) {
            bytes = Math.addExact(bytes, reader.consumedPageBytes());
        }
        return bytes;
    }

    @Override
    public SourcePoll poll()
    {
        checkOpen();
        if (currentBatch != null) {
            currentBatch.close();
        }
        advancePastRejectedRowGroups();
        if (nextRow == totalRows) {
            return SourcePoll.Finished.FINISHED;
        }
        int count = toIntExact(Math.min(batchRows, totalRows - nextRow));
        if (rowGroupTracking) {
            count = toIntExact(Math.min(count, rowGroupRemaining));
            consumeRowGroupRows(count);
        }
        nextRow += count;
        currentBatch = new BatchState(count);
        return new SourcePoll.Ready(currentBatch.batch());
    }

    private void advancePastRejectedRowGroups()
    {
        if (!hasRowGroupFilters || nextRow >= totalRows) {
            return;
        }
        initializeRowGroupTracking();
        while (rowGroupIndex < rowGroupRows.length && rowGroupRemaining == rowGroupRows[rowGroupIndex]) {
            if (rowGroupMayMatch(rowGroupIndex)) {
                return;
            }
            long rows = rowGroupRemaining;
            for (int column = 0; column < readers.length; column++) {
                pendingRows[column] = addExact(pendingRows[column], rows);
                if (readers[column].supportsIndependentNulls()) {
                    pendingNullRows[column] = addExact(pendingNullRows[column], rows);
                }
            }
            nextRow = addExact(nextRow, rows);
            prunedRows = addExact(prunedRows, rows);
            rowGroupIndex++;
            rowGroupRemaining = rowGroupIndex < rowGroupRows.length ? rowGroupRows[rowGroupIndex] : 0;
        }
    }

    private void initializeRowGroupTracking()
    {
        if (rowGroupTracking) {
            return;
        }
        long position = nextRow;
        while (rowGroupIndex < rowGroupRows.length && position >= rowGroupRows[rowGroupIndex]) {
            position -= rowGroupRows[rowGroupIndex++];
        }
        rowGroupRemaining = rowGroupIndex < rowGroupRows.length ? rowGroupRows[rowGroupIndex] - position : 0;
        rowGroupTracking = true;
    }

    private boolean rowGroupMayMatch(int index)
    {
        for (int column = 0; column < readers.length; column++) {
            LongDomain filter = rowGroupFilters[column];
            if (filter != null &&
                    (!readers[column].chunkMayMatch(index, filter) ||
                            !readers[column].dictionaryMayMatch(
                                    index,
                                    filter,
                                    resources.runtimeFilterPolicy().maxDictionaryPruningValues()))) {
                return false;
            }
        }
        return true;
    }

    private void consumeRowGroupRows(int rows)
    {
        rowGroupRemaining -= rows;
        if (rowGroupRemaining == 0) {
            rowGroupIndex++;
            rowGroupRemaining = rowGroupIndex < rowGroupRows.length ? rowGroupRows[rowGroupIndex] : 0;
        }
    }

    private final class BatchState
    {
        private final int rowCount;
        private final Streams[] resolved = new Streams[readers.length];
        private final boolean[] independentNullResolved = new boolean[readers.length];
        private final VectorSourceBatch batch;
        private Mask mask;
        private boolean batchClosed;

        private BatchState(int rowCount)
        {
            this.rowCount = rowCount;
            this.mask = allocator.allocateAllMask(allocationContext, rowCount);
            VectorColumnGeneration[] columns = new VectorColumnGeneration[readers.length];
            for (int column = 0; column < columns.length; column++) {
                int outputColumn = column;
                columns[column] = new VectorColumnGeneration(
                        readers[column].streams(),
                        stream -> resolve(outputColumn, stream),
                        batchScope);
            }
            this.batch = new VectorSourceBatch(schema, mask, columns, batchScope, this::constrain, this::closed);
        }

        private VectorSourceBatch batch()
        {
            return batch;
        }

        private Vector resolve(int column, Stream stream)
        {
            if (outputDemands[column] == null) {
                throw new IllegalStateException("source output was not declared before polling: " + column);
            }
            if (resolved[column] != null && resolved[column].has(stream)) {
                return resolved[column].get(stream);
            }
            if (stream == Stream.NULLS && readers[column].supportsIndependentNulls()) {
                long pending = pendingNullRows[column];
                if (pending > 0) {
                    readers[column].skipNulls(pending);
                    pendingNullRows[column] = 0;
                }
                BooleanVector nulls = readers[column].readNulls(allocator, allocationContext, rowCount, mask);
                independentNullResolved[column] = true;
                resolved[column] = resolved[column] == null ? Streams.of(Stream.NULLS, nulls) : resolved[column].with(Stream.NULLS, nulls);
                return nulls;
            }
            long pending = pendingRows[column];
            if (pending > 0) {
                readers[column].skip(pending);
                pendingRows[column] = 0;
            }
            resolved[column] = readers[column].read(allocator, allocationContext, rowCount, mask);
            return resolved[column].get(stream);
        }

        private void constrain(Mask mask)
        {
            this.mask = requireNonNull(mask, "mask is null");
        }

        private void close()
        {
            batch.close();
        }

        private void closed()
        {
            if (batchClosed) {
                return;
            }
            batchClosed = true;
            for (int column = 0; column < readers.length; column++) {
                if (resolved[column] == null) {
                    pendingRows[column] = addExact(pendingRows[column], rowCount);
                }
                else if (!resolved[column].has(Stream.VALUES)) {
                    pendingRows[column] = addExact(pendingRows[column], rowCount);
                }
                if (readers[column].supportsIndependentNulls() && !independentNullResolved[column]) {
                    pendingNullRows[column] = addExact(pendingNullRows[column], rowCount);
                }
            }
            if (currentBatch == this) {
                currentBatch = null;
            }
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("source is closed");
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException failure = null;
        if (currentBatch != null) {
            currentBatch.close();
        }
        for (ProjectedReader reader : readers) {
            try {
                reader.close();
            }
            catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
        }
        batchScope.close();
        for (ParquetFile file : files) {
            file.close();
        }
        if (failure != null) {
            throw failure;
        }
    }
}
