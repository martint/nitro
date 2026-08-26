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
import org.weakref.nitro.core.source.OrdinalSourceColumnHandle;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.core.source.SourceMetrics;
import org.weakref.nitro.core.source.SourceMetricsProtocol;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.source.SourceProtocol;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorBatchScope;
import org.weakref.nitro.data.VectorColumnGeneration;
import org.weakref.nitro.data.VectorSourceBatch;

import java.util.List;
import java.util.Locale;
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

        void skip(long rowCount);

        Set<Stream> streams();

        @Override
        void close();
    }

    private final class PrimitiveProjectedReader
            implements ProjectedReader
    {
        private final ParquetSchema.Primitive leaf;
        private final ColumnReader reader;
        private final TypeBinding outputType;
        private final boolean intOutputAsLong;
        private long[] doubleScratch = new long[0];

        private PrimitiveProjectedReader(ParquetSchema.Primitive leaf, TypeBinding outputType)
        {
            this.leaf = leaf;
            this.outputType = outputType;
            this.reader = new ColumnReader(
                    leaf.type(),
                    leaf.repetition() != org.apache.parquet.format.FieldRepetitionType.REQUIRED,
                    leaf.typeLength(),
                    leaf.decimal(),
                    null,
                    allocator.primitiveArrays(),
                    resources.readerPolicy(),
                    resources.decodeScratchPool());
            Set<Class<? extends Vector>> vectors = outputType.supportedVectorTypes();
            this.intOutputAsLong = reader.kind() == ColumnReader.Kind.INT &&
                    !vectors.contains(I32Vector.class) && vectors.contains(I64Vector.class);
        }

        @Override
        public void addRowGroup(ParquetFile file, RowGroup rowGroup)
        {
            reader.addChunk(file, file.columnChunk(rowGroup, leaf).meta_data, rowGroup.num_rows, null);
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
                doubleScratch = new long[rowCount];
            }
            reader.readLongs(doubleScratch, nulls, rowCount);
            for (int position = 0; position < rowCount; position++) {
                values.values()[position] = Double.longBitsToDouble(doubleScratch[position]);
            }
            return values;
        }

        private boolean nullable()
        {
            return leaf.repetition() != org.apache.parquet.format.FieldRepetitionType.REQUIRED;
        }

        @Override
        public void skip(long rowCount)
        {
            reader.skip(rowCount);
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
        }
    }

    private static final class MapProjectedReader
            implements ProjectedReader
    {
        private final NestedMapReader reader;
        private final TypeBinding outputType;
        private final boolean nullable;

        private MapProjectedReader(ParquetSchema.Group map, TypeBinding outputType, RleReaderPolicy rlePolicy)
        {
            this.reader = new NestedMapReader(map, rlePolicy);
            this.outputType = requireNonNull(outputType, "outputType is null");
            this.nullable = map.repetition() != org.apache.parquet.format.FieldRepetitionType.REQUIRED;
        }

        @Override
        public void addRowGroup(ParquetFile file, RowGroup rowGroup)
        {
            reader.addRowGroup(file, rowGroup);
        }

        @Override
        public Streams read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
        {
            Streams streams = reader.read(allocator, context, rowCount, mask);
            if (!outputType.supportsVector(streams.values())) {
                throw new UnsupportedParquetFeatureException(
                        "Native Parquet MAP representation does not match output type " + outputType.identity());
            }
            return streams;
        }

        @Override
        public void skip(long rowCount)
        {
            reader.skip(rowCount);
        }

        @Override
        public Set<Stream> streams()
        {
            return nullable ? Set.of(Stream.VALUES, Stream.NULLS) : Set.of(Stream.VALUES);
        }

        @Override
        public void close()
        {
            reader.close();
        }
    }

    private final NitroParquetScanResources resources;
    private final Allocator allocator;
    private final Schema schema;
    private final ParquetFile[] files;
    private final ProjectedReader[] readers;
    private final SourceColumnHandle[] sourceColumns;
    private final long[] pendingRows;
    private final VectorBatchScope batchScope;
    private final Allocator.Context allocationContext;
    private final int batchRows;
    private final long totalRows;

    private long nextRow;
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
        this.resources = requireNonNull(resources, "resources is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.schema = requireNonNull(schema, "schema is null");
        requireNonNull(columnNameMatching, "columnNameMatching is null");
        splits = List.copyOf(splits);
        if (splits.isEmpty()) {
            throw new IllegalArgumentException("splits is empty");
        }
        if (sourceOrdinals != null && sourceOrdinals.size() != schema.size()) {
            throw new IllegalArgumentException("source ordinals size does not match projected columns");
        }

        this.files = new ParquetFile[splits.size()];
        for (int index = 0; index < files.length; index++) {
            files[index] = ParquetFile.open(splits.get(index).input());
        }
        this.readers = new ProjectedReader[schema.size()];
        this.sourceColumns = new SourceColumnHandle[schema.size()];
        this.pendingRows = new long[schema.size()];
        for (int column = 0; column < schema.size(); column++) {
            String name = sourceOrdinals == null
                    ? schema.field(column).name().orElseThrow(
                            () -> new IllegalArgumentException("nested Parquet source requires named output fields"))
                    : "";
            ParquetSchema.Node node = resolveNode(files[0].schema(), name, columnNameMatching, sourceOrdinals, column);
            readers[column] = createReader(node, schema.field(column).type());
            sourceColumns[column] = new OrdinalSourceColumnHandle(column, schema.field(column).type());
        }

        long rows = 0;
        for (int fileIndex = 0; fileIndex < files.length; fileIndex++) {
            ParquetFile file = files[fileIndex];
            NitroParquetBatchSource.InputSplit split = splits.get(fileIndex);
            List<RowGroup> rowGroups = file.rowGroups(split.start(), split.length());
            for (RowGroup rowGroup : rowGroups) {
                rows = addExact(rows, rowGroup.num_rows);
                for (ProjectedReader reader : readers) {
                    reader.addRowGroup(file, rowGroup);
                }
            }
        }
        this.totalRows = rows;
        this.batchRows = resources.batchPolicy().initialRows();
        this.batchScope = new VectorBatchScope(allocator, "NestedNitroParquetBatchSource", resources.batchBufferPool());
        this.allocationContext = batchScope.context();
    }

    private ProjectedReader createReader(ParquetSchema.Node node, TypeBinding outputType)
    {
        return switch (node) {
            case ParquetSchema.Primitive primitive -> new PrimitiveProjectedReader(primitive, outputType);
            case ParquetSchema.Group group when group.isMap() -> {
                if (!outputType.supportedVectorTypes().contains(MapVector.class)) {
                    throw new UnsupportedParquetFeatureException(
                            "Parquet MAP field '" + group.name() + "' has no MapVector output representation");
                }
                yield new MapProjectedReader(group, outputType, resources.readerPolicy().rle());
            }
            case ParquetSchema.Group group -> throw new UnsupportedParquetFeatureException(
                    "Native Nitro Parquet reader does not support nested field '" + group.name() + "' with this logical layout");
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
        return Set.of(SourceCapability.LAZY_COLUMNS, SourceCapability.SELECTION_PUSHDOWN);
    }

    @Override
    public OptionalLong exactRows()
    {
        return OptionalLong.of(totalRows);
    }

    @Override
    public <T> Optional<T> protocol(SourceProtocol<T> protocol)
    {
        if (protocol == SourceMetricsProtocol.METRICS) {
            SourceMetrics metrics = new SourceMetrics()
            {
                @Override
                public OptionalLong completedBytes()
                {
                    return OptionalLong.empty();
                }

                @Override
                public OptionalLong completedPositions()
                {
                    return OptionalLong.of(nextRow);
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
    public SourcePoll poll()
    {
        checkOpen();
        if (currentBatch != null) {
            currentBatch.close();
        }
        if (nextRow == totalRows) {
            return SourcePoll.Finished.FINISHED;
        }
        int count = toIntExact(Math.min(batchRows, totalRows - nextRow));
        nextRow += count;
        currentBatch = new BatchState(count);
        return new SourcePoll.Ready(currentBatch.batch());
    }

    private final class BatchState
    {
        private final int rowCount;
        private final Streams[] resolved = new Streams[readers.length];
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
                        stream -> resolve(outputColumn).get(stream),
                        batchScope);
            }
            this.batch = new VectorSourceBatch(schema, mask, columns, batchScope, this::constrain, this::closed);
        }

        private VectorSourceBatch batch()
        {
            return batch;
        }

        private Streams resolve(int column)
        {
            if (resolved[column] != null) {
                return resolved[column];
            }
            long pending = pendingRows[column];
            if (pending > 0) {
                readers[column].skip(pending);
                pendingRows[column] = 0;
            }
            resolved[column] = readers[column].read(allocator, allocationContext, rowCount, mask);
            return resolved[column];
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
