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
package org.weakref.nitro.operator;

import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.Column;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetTypeUtils;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.connector.SourcePage;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public final class TrinoParquetScanOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("TrinoParquetScanOperator");
    private static final String MAX_BATCH_ROWS_PROPERTY = "nitro.trino.scan.maxBatchRows";
    private static final int DEFAULT_MAX_BATCH_ROWS = 10_000;
    private static final int MAX_BATCH_ROWS = Integer.getInteger(MAX_BATCH_ROWS_PROPERTY, DEFAULT_MAX_BATCH_ROWS);
    private static final DataSize MAX_READ_BLOCK_SIZE = DataSize.of(2, MEGABYTE);
    private static final DataSize MAX_MERGE_DISTANCE = DataSize.of(1, MEGABYTE);
    private static final DataSize MAX_BUFFER_SIZE = DataSize.of(2, MEGABYTE);
    private static final DataSize MAX_PAGE_READ_SIZE = DataSize.of(2, MEGABYTE);
    private static final Method LONG_ARRAY_RAW_VALUES = declaredMethod(LongArrayBlock.class, "getRawValues");
    private static final Method LONG_ARRAY_RAW_VALUES_OFFSET = declaredMethod(LongArrayBlock.class, "getRawValuesOffset");
    private static final Method LONG_ARRAY_RAW_NULLS = declaredMethod(LongArrayBlock.class, "getRawValueIsNull");
    private static final Method INT_ARRAY_RAW_NULLS = declaredMethod(IntArrayBlock.class, "getRawValueIsNull");
    private static final Method VARIABLE_WIDTH_RAW_OFFSETS = declaredMethod(VariableWidthBlock.class, "getRawOffsets");
    private static final Method VARIABLE_WIDTH_RAW_ARRAY_BASE = declaredMethod(VariableWidthBlock.class, "getRawArrayBase");
    private static final Field PARQUET_SOURCE_PAGE_BLOCKS = declaredField("io.trino.parquet.reader.ParquetReader$ParquetSourcePage", "blocks");

    private final Allocator allocator;
    // Converted dictionary vectors, cached by the source dictionary Block's identity: the reader hands the same
    // dictionary Block to every batch of a column chunk, so converting it once keeps the vector's identity stable
    // across those batches (identity-keyed consumers -- per-batch mask caches, global intern page remaps -- rely
    // on it) and skips the per-batch conversion.
    private final java.util.IdentityHashMap<Block, Vector> convertedDictionaries = new java.util.IdentityHashMap<>();
    private final List<Path> files;
    private final List<String> columnNames;

    private int fileIndex;
    private SingleFileScan currentFile;
    private Batch currentBatch;

    public TrinoParquetScanOperator(Allocator allocator, Path file, List<String> columns)
    {
        this(allocator, List.of(file), columns);
    }

    public TrinoParquetScanOperator(Allocator allocator, List<Path> files, List<String> columns)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        requireNonNull(files, "files is null");
        requireNonNull(columns, "columns is null");
        checkArgument(!files.isEmpty(), "files is empty");
        this.files = List.copyOf(files);
        this.columnNames = List.copyOf(columns);
    }

    @Override
    public int outputCount()
    {
        return columnNames.size();
    }

    @Override
    public boolean hasNext()
    {
        advanceIfNecessary();
        return currentFile != null && currentFile.hasNext();
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more Parquet rows");
        }
        closeCurrentBatch();
        return currentFile.next();
    }

    @Override
    public void constrain(Mask mask)
    {
        if (currentFile != null) {
            currentFile.constrain(mask);
        }
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return false;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // The underlying Parquet reader advances irreversibly: an earlier batch's columns cannot be
        // re-borrowed after the reader moves on, so downstream operators must materialize eagerly.
        return false;
    }

    @Override
    public void close()
    {
        closeCurrentBatch();
        closeCurrent();
    }

    private void advanceIfNecessary()
    {
        while ((currentFile == null || !currentFile.hasNext()) && fileIndex < files.size()) {
            closeCurrent();
            currentFile = new SingleFileScan(files.get(fileIndex++));
        }
    }

    private void closeCurrent()
    {
        if (currentFile == null) {
            return;
        }
        currentFile.close();
        currentFile = null;
    }

    private void closeCurrentBatch()
    {
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }
    }

    private final class SingleFileScan
            implements AutoCloseable
    {
        private final ParquetReader reader;
        private final List<ColumnSpec> columns;

        private SourcePage nextPage;
        private boolean exhausted;
        private BatchState currentBatchState;

        private SingleFileScan(Path file)
        {
            requireNonNull(file, "file is null");

            try {
                ParquetReaderOptions options = ParquetReaderOptions.builder()
                        .withMaxReadBlockSize(MAX_READ_BLOCK_SIZE)
                        .withMaxReadBlockRowCount(MAX_BATCH_ROWS)
                        .withMaxMergeDistance(MAX_MERGE_DISTANCE)
                        .withMaxBufferSize(MAX_BUFFER_SIZE)
                        .withMaxPageReadSize(MAX_PAGE_READ_SIZE)
                        .withVectorizedDecodingEnabled(false)
                        .build();

                FileParquetDataSource dataSource = new FileParquetDataSource(file.toFile(), options);
                ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
                MessageType schema = metadata.getFileMetaData().getSchema();
                MessageType requestedSchema = new MessageType(schema.getName(), columnNames.stream()
                        .map(name -> findField(schema, name))
                        .toList());
                Map<List<String>, ColumnDescriptor> descriptorsByPath = ParquetTypeUtils.getDescriptors(schema, requestedSchema);
                columns = resolveColumns(schema, descriptorsByPath);
                List<RowGroupInfo> rowGroups = metadata.getBlocks().stream()
                        .map(block -> createRowGroupInfo(block, dataSource.getId(), descriptorsByPath))
                        .toList();

                reader = new ParquetReader(
                        Optional.ofNullable(metadata.getFileMetaData().getCreatedBy()),
                        columns.stream().map(ColumnSpec::column).toList(),
                        false,
                        rowGroups,
                        dataSource,
                        DateTimeZone.UTC,
                        AggregatedMemoryContext.newSimpleAggregatedMemoryContext(),
                        options,
                        exception -> new RuntimeException("Unable to read Trino Parquet page from " + file, exception),
                        Optional.empty(),
                        Optional.empty(),
                        metadata.getDecryptionContext());
            }
            catch (IOException exception) {
                throw new UncheckedIOException("Unable to open Trino Parquet file: " + file, exception);
            }
        }

        private boolean hasNext()
        {
            if (!exhausted && nextPage == null) {
                nextPage = loadNextPage();
                exhausted = nextPage == null;
            }
            return nextPage != null;
        }

        private Batch next()
        {
            if (!hasNext()) {
                throw new IllegalStateException("No more Trino Parquet rows");
            }

            SourcePage page = nextPage;
            nextPage = null;
            currentBatchState = null;
            BatchState batchState = new BatchState(page, allocator.allocateAllMask(ALLOCATION_CONTEXT, page.getPositionCount()), columns.size());
            currentBatchState = batchState;

            Output[] outputs = new Output[columns.size()];
            for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                int outputIndex = columnIndex;
                ColumnSpec column = columns.get(columnIndex);
                outputs[columnIndex] = new Output(
                        column.nullable() ? Set.of(Stream.VALUES, Stream.NULLS) : Set.of(Stream.VALUES),
                        stream -> {
                            ColumnBuffer buffer = resolveColumn(outputIndex, batchState);
                            return switch (stream) {
                                case VALUES -> buffer.values();
                                case NULLS -> requireNonNull(buffer.nulls(), "NULLS stream is absent");
                                default -> throw new IllegalArgumentException("Output does not expose stream: " + stream);
                            };
                        },
                        (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector),
                        (stream, vector) -> allocator.release(ALLOCATION_CONTEXT, vector));
            }
            Batch batch = new Batch(
                    batchState.mask(),
                    batchState::constrain,
                    takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                    mask -> allocator.release(ALLOCATION_CONTEXT, mask),
                    () -> {},
                    outputs);
            currentBatch = batch;
            return batch;
        }

        private void constrain(Mask mask)
        {
            if (currentBatchState != null) {
                currentBatchState.constrain(mask);
            }
        }

        @Override
        public void close()
        {
            try {
                reader.close();
            }
            catch (IOException exception) {
                throw new UncheckedIOException("Unable to close Trino Parquet reader", exception);
            }
            finally {
                nextPage = null;
                currentBatchState = null;
                exhausted = true;
                allocator.release(ALLOCATION_CONTEXT);
            }
        }

        private SourcePage loadNextPage()
        {
            try {
                while (true) {
                    SourcePage page = reader.nextPage();
                    if (page == null || page.getPositionCount() > 0) {
                        return page;
                    }
                }
            }
            catch (IOException exception) {
                throw new UncheckedIOException("Unable to read Trino Parquet page", exception);
            }
        }

        private List<ColumnSpec> resolveColumns(MessageType schema, Map<List<String>, ColumnDescriptor> descriptorsByPath)
        {
            List<ColumnSpec> resolvedColumns = new ArrayList<>(columnNames.size());
            for (int columnIndex = 0; columnIndex < columnNames.size(); columnIndex++) {
                String name = columnNames.get(columnIndex);
                Type field = findField(schema, name);
                checkArgument(field.isPrimitive(), "Only flat primitive Parquet columns are supported by TrinoParquetScanOperator: %s", name);

                PrimitiveType primitive = field.asPrimitiveType();
                ColumnDescriptor descriptor = requireNonNull(descriptorsByPath.get(List.of(field.getName())), "descriptor is null for " + name);
                ColumnKind kind;
                io.trino.spi.type.Type trinoType;
                if (primitive.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation decimal) {
                    checkArgument(decimal.getPrecision() <= 18, "Only short decimals are supported by TrinoParquetScanOperator: %s", primitive);
                    kind = ColumnKind.SHORT_DECIMAL;
                    trinoType = createDecimalType(decimal.getPrecision(), decimal.getScale());
                }
                else {
                    kind = switch (primitive.getPrimitiveTypeName()) {
                        case INT32 -> primitive.getLogicalTypeAnnotation() instanceof DateLogicalTypeAnnotation ? ColumnKind.DATE : ColumnKind.I32;
                        case INT64 -> ColumnKind.I64;
                        case BOOLEAN -> ColumnKind.BOOLEAN;
                        case BINARY, FIXED_LEN_BYTE_ARRAY -> ColumnKind.BINARY;
                        default -> throw new IllegalArgumentException("Unsupported Trino Parquet primitive type for column %s: %s".formatted(name, primitive.getPrimitiveTypeName()));
                    };
                    trinoType = kind.trinoType();
                }

                PrimitiveField primitiveField = new PrimitiveField(trinoType, field.getRepetition() == REQUIRED, descriptor, columnIndex);
                resolvedColumns.add(new ColumnSpec(
                        name,
                        kind,
                        trinoType,
                        field.getRepetition() != REQUIRED,
                        binaryTraits(primitive),
                        new Column(name, primitiveField)));
            }
            return resolvedColumns;
        }

        private RowGroupInfo createRowGroupInfo(io.trino.parquet.metadata.BlockMetadata block, ParquetDataSourceId dataSourceId, Map<List<String>, ColumnDescriptor> descriptorsByPath)
        {
            try {
                return new RowGroupInfo(
                        PrunedBlockMetadata.createPrunedColumnsMetadata(block, dataSourceId, descriptorsByPath),
                        block.fileRowCountOffset(),
                        Optional.empty());
            }
            catch (IOException exception) {
                throw new UncheckedIOException("Unable to prune Trino row-group metadata", exception);
            }
        }

        private ColumnBuffer resolveColumn(int columnIndex, BatchState batchState)
        {
            ColumnBuffer buffer = batchState.buffers()[columnIndex];
            if (buffer != null) {
                return buffer;
            }

            ColumnSpec column = columns.get(columnIndex);
            Block block = batchState.page().getBlock(columnIndex);
            buffer = batchState.mask().all()
                    ? readFullColumn(column, block)
                    : readMaskedColumn(column, block, batchState.mask());
            batchState.releaseBlock(columnIndex);
            batchState.buffers()[columnIndex] = buffer;
            return buffer;
        }

        private ColumnBuffer readFullColumn(ColumnSpec column, Block block)
        {
            Mask fullMask = allocator.allocateAllMask(ALLOCATION_CONTEXT, block.getPositionCount());
            BooleanVector nulls = null;
            try {
                if (column.nullable()) {
                    nulls = copyNulls(block, fullMask);
                }
            }
            finally {
                allocator.release(ALLOCATION_CONTEXT, fullMask);
            }
            return new ColumnBuffer(
                    convertValues(column, block, true),
                    nulls);
        }

        private ColumnBuffer readMaskedColumn(ColumnSpec column, Block block, Mask mask)
        {
            if (block.getPositionCount() == 0) {
                return new ColumnBuffer(
                        emptyMaskedValues(column, mask.size()),
                        column.nullable() ? selectedNulls(mask) : null);
            }
            return new ColumnBuffer(
                    convertMaskedValues(column, block, mask),
                    column.nullable() ? copyNulls(block, mask) : null);
        }
    }

    private static Type findField(MessageType schema, String name)
    {
        for (Type field : schema.getFields()) {
            if (field.getName().equalsIgnoreCase(name)) {
                return field;
            }
        }
        throw new IllegalArgumentException("Unknown Parquet column: " + name);
    }

    private Vector convertValues(ColumnSpec column, Block block, boolean preserveEncodings)
    {
        if (preserveEncodings) {
            if (block instanceof DictionaryBlock dictionaryBlock) {
                int[] ids = dictionaryIds(dictionaryBlock);
                Vector dictionaryValues = convertedDictionaries.computeIfAbsent(dictionaryBlock.getDictionary(),
                        dictionary -> convertValues(column, dictionary, true));
                return allocator.adopt(ALLOCATION_CONTEXT, DictionaryVector.wrap(ids, dictionaryValues));
            }
            if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
                Vector values = convertValues(column, runLengthEncodedBlock.getValue(), true);
                return allocator.allocateRle(ALLOCATION_CONTEXT, new int[] {runLengthEncodedBlock.getPositionCount()}, values);
            }
        }

        return switch (column.kind()) {
            case I32 -> copyI32(block);
            case DATE -> copyI32(block);
            case I64 -> copyI64(block);
            case SHORT_DECIMAL -> copyI64(block);
            case BOOLEAN -> copyBoolean(block);
            case BINARY -> copyBinary(column, block);
        };
    }

    private Vector convertMaskedValues(ColumnSpec column, Block block, Mask mask)
    {
        return switch (column.kind()) {
            case I32 -> copyMaskedI32(block, mask);
            case DATE -> copyMaskedI32(block, mask);
            case I64 -> copyMaskedI64(block, mask);
            case SHORT_DECIMAL -> copyMaskedI64(block, mask);
            case BOOLEAN -> copyMaskedBoolean(block, mask);
            case BINARY -> copyMaskedBinary(column, block, mask);
        };
    }

    private Vector emptyMaskedValues(ColumnSpec column, int size)
    {
        return switch (column.kind()) {
            case I32 -> allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, size, I32Vector::new);
            case DATE -> allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, size, I32Vector::new);
            case I64, SHORT_DECIMAL -> allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, size, I64Vector::new);
            case BOOLEAN -> allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, size, BooleanVector::new);
            case BINARY -> {
                BinaryVector values = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, size, 0);
                values.addTraits(column.binaryTraits());
                yield values;
            }
        };
    }

    private I32Vector copyI32(Block block)
    {
        if (block instanceof IntArrayBlock intArrayBlock) {
            int[] rawValues = intArrayBlock.getRawValues();
            if (intArrayBlock.getRawValuesOffset() == 0 && rawValues.length == block.getPositionCount()) {
                return allocator.adopt(ALLOCATION_CONTEXT, new I32Vector(rawValues));
            }
        }

        I32Vector values = allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, block.getPositionCount(), I32Vector::new);
        int[] output = values.values();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                output[position] = readInt(block, position);
            }
        }
        return values;
    }

    private I64Vector copyI64(Block block)
    {
        if (block instanceof LongArrayBlock longArrayBlock) {
            long[] rawValues = rawValues(longArrayBlock);
            if (rawValuesOffset(longArrayBlock) == 0 && rawValues.length == block.getPositionCount()) {
                return allocator.adopt(ALLOCATION_CONTEXT, new I64Vector(rawValues));
            }
        }

        I64Vector values = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, block.getPositionCount(), I64Vector::new);
        long[] output = values.values();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                output[position] = readLong(block, position);
            }
        }
        return values;
    }

    private BooleanVector copyBoolean(Block block)
    {
        BooleanVector values = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, block.getPositionCount(), BooleanVector::new);
        boolean[] output = values.values();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                output[position] = readBoolean(block, position);
            }
        }
        return values;
    }

    private BinaryVector copyBinary(ColumnSpec column, Block block)
    {
        if (block instanceof VariableWidthBlock variableWidthBlock) {
            return materializeVariableWidthBinary(column, variableWidthBlock);
        }
        if (block instanceof DictionaryBlock dictionaryBlock && dictionaryBlock.getDictionary() instanceof VariableWidthBlock dictionaryValues) {
            return copyDictionaryBinary(column, dictionaryBlock, dictionaryValues);
        }

        throw new IllegalArgumentException("Unsupported Trino binary block type for full-batch fast path: " + block.getClass().getSimpleName());
    }

    private BinaryVector copyDictionaryBinary(ColumnSpec column, DictionaryBlock block, VariableWidthBlock dictionaryValues)
    {
        int[] ids = block.getRawIds();
        int idsOffset = block.getRawIdsOffset();
        int[] rawOffsets = rawOffsets(dictionaryValues);
        int rawArrayBase = rawArrayBase(dictionaryValues);
        int positionCount = block.getPositionCount();
        int totalBytes = 0;
        for (int position = 0; position < positionCount; position++) {
            if (!block.isNull(position)) {
                int dictionaryPosition = ids[idsOffset + position];
                totalBytes += rawOffsets[rawArrayBase + dictionaryPosition + 1] - rawOffsets[rawArrayBase + dictionaryPosition];
            }
        }

        BinaryVector values = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, positionCount, totalBytes);
        values.addTraits(column.binaryTraits());

        Slice rawSlice = dictionaryValues.getRawSlice();
        byte[] source = rawSlice.byteArray();
        int sourceOffset = rawSlice.byteArrayOffset();
        int[] offsets = values.offsets();
        int outputOffset = 0;
        for (int position = 0; position < positionCount; position++) {
            offsets[position] = outputOffset;
            if (block.isNull(position)) {
                offsets[position + 1] = outputOffset;
                continue;
            }

            int dictionaryPosition = ids[idsOffset + position];
            int start = rawOffsets[rawArrayBase + dictionaryPosition];
            int end = rawOffsets[rawArrayBase + dictionaryPosition + 1];
            int length = end - start;
            System.arraycopy(source, sourceOffset + start, values.data(), outputOffset, length);
            outputOffset += length;
            offsets[position + 1] = outputOffset;
        }
        return values;
    }

    private I32Vector copyMaskedI32(Block block, Mask mask)
    {
        I32Vector values = allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, block.getPositionCount(), I32Vector::new);
        int[] output = values.values();
        forEachSelected(mask, position -> {
            if (!block.isNull(position)) {
                output[position] = readInt(block, position);
            }
        });
        return values;
    }

    private BinaryVector materializeVariableWidthBinary(ColumnSpec column, VariableWidthBlock block)
    {
        BinaryVector adopted = adoptVariableWidthBinary(column, block);
        if (adopted != null) {
            return adopted;
        }
        return copyVariableWidthBinary(column, block);
    }

    private BinaryVector adoptVariableWidthBinary(ColumnSpec column, VariableWidthBlock block)
    {
        int positionCount = block.getPositionCount();
        int[] rawOffsets = rawOffsets(block);
        int rawArrayBase = rawArrayBase(block);
        int firstOffset = rawOffsets[rawArrayBase];
        int lastEnd = rawOffsets[rawArrayBase + positionCount];
        int totalBytes = Math.max(0, lastEnd - firstOffset);

        Slice rawSlice = block.getRawSlice();
        if (rawArrayBase != 0 || rawOffsets.length != positionCount + 1 || firstOffset != 0 || rawSlice.byteArrayOffset() != 0 || rawSlice.byteArray().length != totalBytes) {
            return null;
        }

        BinaryVector values = allocator.adopt(ALLOCATION_CONTEXT, new BinaryVector(positionCount, rawOffsets, rawSlice.byteArray()));
        values.addTraits(column.binaryTraits());
        return values;
    }

    private BinaryVector copyVariableWidthBinary(ColumnSpec column, VariableWidthBlock block)
    {
        int positionCount = block.getPositionCount();
        int[] rawOffsets = rawOffsets(block);
        int rawArrayBase = rawArrayBase(block);
        int firstOffset = rawOffsets[rawArrayBase];
        int lastEnd = rawOffsets[rawArrayBase + positionCount];
        int totalBytes = Math.max(0, lastEnd - firstOffset);
        Slice rawSlice = block.getRawSlice();

        BinaryVector values = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, positionCount, totalBytes);
        values.addTraits(column.binaryTraits());

        if (totalBytes > 0) {
            System.arraycopy(rawSlice.byteArray(), rawSlice.byteArrayOffset() + firstOffset, values.data(), 0, totalBytes);
        }

        int[] offsets = values.offsets();
        System.arraycopy(rawOffsets, rawArrayBase, offsets, 0, positionCount + 1);
        if (firstOffset != 0) {
            for (int index = 0; index < offsets.length; index++) {
                offsets[index] -= firstOffset;
            }
        }
        return values;
    }

    private I64Vector copyMaskedI64(Block block, Mask mask)
    {
        I64Vector values = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, block.getPositionCount(), I64Vector::new);
        long[] output = values.values();
        forEachSelected(mask, position -> {
            if (!block.isNull(position)) {
                output[position] = readLong(block, position);
            }
        });
        return values;
    }

    private BooleanVector copyMaskedBoolean(Block block, Mask mask)
    {
        BooleanVector values = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, block.getPositionCount(), BooleanVector::new);
        boolean[] output = values.values();
        forEachSelected(mask, position -> {
            if (!block.isNull(position)) {
                output[position] = readBoolean(block, position);
            }
        });
        return values;
    }

    private BinaryVector copyMaskedBinary(ColumnSpec column, Block block, Mask mask)
    {
        int totalBytes = selectedBinaryBytes(block, mask);
        BinaryVector values = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, block.getPositionCount(), totalBytes);
        values.addTraits(column.binaryTraits());
        int[] currentOffset = {0};
        forEachSelected(mask, position -> {
            values.offsets()[position] = currentOffset[0];
            if (block.isNull(position)) {
                values.setNull(position);
                return;
            }
            Slice slice = readSlice(block, position);
            values.setBytes(position, slice.byteArray(), slice.byteArrayOffset(), slice.length());
            currentOffset[0] = values.endOffset(position);
        });
        return values;
    }

    private static int selectedBinaryBytes(Block block, Mask mask)
    {
        final int[] totalBytes = {0};
        forEachSelected(mask, position -> {
            if (!block.isNull(position)) {
                totalBytes[0] += readSlice(block, position).length();
            }
        });
        return totalBytes[0];
    }

    private BooleanVector copyNulls(Block block, Mask mask)
    {
        int positionCount = block.getPositionCount();
        BooleanVector nulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, positionCount, BooleanVector::new);
        if (!block.mayHaveNull()) {
            return nulls;
        }
        boolean[] output = nulls.values();
        // Fixed-width blocks expose their null flags as a boolean[] with the same true==null convention
        // as BooleanVector, so copy them in bulk instead of calling isNull() per position via a lambda.
        boolean[] rawNulls = rawValueIsNull(block);
        if (rawNulls != null) {
            int offset = rawValueIsNullOffset(block);
            if (mask.all()) {
                System.arraycopy(rawNulls, offset, output, 0, positionCount);
            }
            else {
                for (int index = 0; index < mask.selectedCount(); index++) {
                    int position = mask.position(index);
                    output[position] = rawNulls[offset + position];
                }
            }
            return nulls;
        }
        forEachSelected(mask, position -> output[position] = block.isNull(position));
        return nulls;
    }

    // Raw null flags (true == null) for fixed-width blocks that expose them, indexed by
    // rawValueIsNullOffset(block) + position; null for blocks with a different layout.
    private static boolean[] rawValueIsNull(Block block)
    {
        try {
            if (block instanceof LongArrayBlock longBlock) {
                return (boolean[]) LONG_ARRAY_RAW_NULLS.invoke(longBlock);
            }
            if (block instanceof IntArrayBlock intBlock) {
                return (boolean[]) INT_ARRAY_RAW_NULLS.invoke(intBlock);
            }
        }
        catch (IllegalAccessException | InvocationTargetException exception) {
            throw new IllegalStateException("Unable to access Trino block null flags", exception);
        }
        return null;
    }

    private static int rawValueIsNullOffset(Block block)
    {
        if (block instanceof LongArrayBlock longBlock) {
            return rawValuesOffset(longBlock);
        }
        if (block instanceof IntArrayBlock intBlock) {
            return intBlock.getRawValuesOffset();
        }
        return 0;
    }

    private BooleanVector selectedNulls(Mask mask)
    {
        BooleanVector nulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, mask.size(), BooleanVector::new);
        boolean[] output = nulls.values();
        forEachSelected(mask, position -> output[position] = true);
        return nulls;
    }

    private static int readInt(Block block, int position)
    {
        return switch (block) {
            case IntArrayBlock intArrayBlock -> intArrayBlock.getInt(position);
            case DictionaryBlock dictionaryBlock -> readInt(dictionaryBlock.getUnderlyingValueBlock(), dictionaryBlock.getUnderlyingValuePosition(position));
            case RunLengthEncodedBlock runLengthEncodedBlock -> readInt(runLengthEncodedBlock.getValue(), 0);
            default -> throw unsupported(block);
        };
    }

    private static long readLong(Block block, int position)
    {
        return switch (block) {
            case IntArrayBlock intArrayBlock -> intArrayBlock.getInt(position);
            case LongArrayBlock longArrayBlock -> longArrayBlock.getLong(position);
            case DictionaryBlock dictionaryBlock -> readLong(dictionaryBlock.getUnderlyingValueBlock(), dictionaryBlock.getUnderlyingValuePosition(position));
            case RunLengthEncodedBlock runLengthEncodedBlock -> readLong(runLengthEncodedBlock.getValue(), 0);
            default -> throw unsupported(block);
        };
    }

    private static boolean readBoolean(Block block, int position)
    {
        return switch (block) {
            case ByteArrayBlock byteArrayBlock -> byteArrayBlock.getByte(position) != 0;
            case DictionaryBlock dictionaryBlock -> readBoolean(dictionaryBlock.getUnderlyingValueBlock(), dictionaryBlock.getUnderlyingValuePosition(position));
            case RunLengthEncodedBlock runLengthEncodedBlock -> readBoolean(runLengthEncodedBlock.getValue(), 0);
            default -> throw unsupported(block);
        };
    }

    private static Slice readSlice(Block block, int position)
    {
        return switch (block) {
            case VariableWidthBlock variableWidthBlock -> variableWidthBlock.getSlice(position);
            case DictionaryBlock dictionaryBlock -> readSlice(dictionaryBlock.getUnderlyingValueBlock(), dictionaryBlock.getUnderlyingValuePosition(position));
            case RunLengthEncodedBlock runLengthEncodedBlock -> readSlice(runLengthEncodedBlock.getValue(), 0);
            default -> throw unsupported(block);
        };
    }

    private static IllegalArgumentException unsupported(Block block)
    {
        return new IllegalArgumentException("Unsupported Trino block type: " + block.getClass().getSimpleName());
    }

    private static int[] dictionaryIds(DictionaryBlock block)
    {
        int[] rawIds = block.getRawIds();
        if (block.getRawIdsOffset() == 0 && rawIds.length == block.getPositionCount()) {
            return rawIds;
        }
        return Arrays.copyOfRange(rawIds, block.getRawIdsOffset(), block.getRawIdsOffset() + block.getPositionCount());
    }

    private static Method declaredMethod(Class<?> type, String name)
    {
        try {
            Method method = type.getDeclaredMethod(name);
            method.setAccessible(true);
            return method;
        }
        catch (NoSuchMethodException exception) {
            throw new IllegalStateException("Missing Trino block method " + type.getSimpleName() + "." + name, exception);
        }
    }

    private static Field declaredField(String className, String fieldName)
    {
        try {
            Field field = Class.forName(className).getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        }
        catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Missing Trino field " + className + "." + fieldName, exception);
        }
    }

    private static int rawValuesOffset(LongArrayBlock block)
    {
        try {
            return (int) LONG_ARRAY_RAW_VALUES_OFFSET.invoke(block);
        }
        catch (IllegalAccessException | InvocationTargetException exception) {
            throw new IllegalStateException("Unable to access Trino LongArrayBlock values offset", exception);
        }
    }

    private static long[] rawValues(LongArrayBlock block)
    {
        try {
            return (long[]) LONG_ARRAY_RAW_VALUES.invoke(block);
        }
        catch (IllegalAccessException | InvocationTargetException exception) {
            throw new IllegalStateException("Unable to access Trino LongArrayBlock values", exception);
        }
    }

    private static int[] rawOffsets(VariableWidthBlock block)
    {
        try {
            return (int[]) VARIABLE_WIDTH_RAW_OFFSETS.invoke(block);
        }
        catch (IllegalAccessException | InvocationTargetException exception) {
            throw new IllegalStateException("Unable to access Trino VariableWidthBlock offsets", exception);
        }
    }

    private static int rawArrayBase(VariableWidthBlock block)
    {
        try {
            return (int) VARIABLE_WIDTH_RAW_ARRAY_BASE.invoke(block);
        }
        catch (IllegalAccessException | InvocationTargetException exception) {
            throw new IllegalStateException("Unable to access Trino VariableWidthBlock array base", exception);
        }
    }

    private static void forEachSelected(Mask mask, java.util.function.IntConsumer consumer)
    {
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                consumer.accept(position);
            }
            return;
        }
        for (int index = 0; index < mask.selectedCount(); index++) {
            consumer.accept(mask.position(index));
        }
    }

    private static Set<BinaryVector.Trait> binaryTraits(PrimitiveType primitive)
    {
        // A BINARY column with the String annotation is UTF-8 by declaration; an UNANNOTATED one is treated as
        // UTF-8 too -- the ClickBench athena-partitioned hits files carry their string columns as plain BINARY
        // with no logical type, and refusing the trait makes every UTF-8 primitive reject them.
        if (primitive.getLogicalTypeAnnotation() == null || stringType().equals(primitive.getLogicalTypeAnnotation())) {
            return Set.of(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        }
        return Set.of();
    }

    private record BatchState(SourcePage[] pageHolder, Mask[] maskHolder, ColumnBuffer[] buffers)
    {
        private BatchState(SourcePage page, Mask mask, int columnCount)
        {
            this(new SourcePage[] {page}, new Mask[] {mask}, new ColumnBuffer[columnCount]);
        }

        private SourcePage page()
        {
            return pageHolder[0];
        }

        private Mask mask()
        {
            return maskHolder[0];
        }

        private void constrain(Mask mask)
        {
            maskHolder[0] = mask;
        }

        private void releaseBlock(int columnIndex)
        {
            SourcePage page = page();
            if (page == null || !PARQUET_SOURCE_PAGE_BLOCKS.getDeclaringClass().isInstance(page)) {
                return;
            }
            try {
                Block[] blocks = (Block[]) PARQUET_SOURCE_PAGE_BLOCKS.get(page);
                blocks[columnIndex] = null;
            }
            catch (IllegalAccessException exception) {
                throw new IllegalStateException("Unable to clear Trino source page block cache", exception);
            }
        }
    }

    private enum ColumnKind
    {
        I32(io.trino.spi.type.IntegerType.INTEGER),
        DATE(io.trino.spi.type.DateType.DATE),
        I64(io.trino.spi.type.BigintType.BIGINT),
        SHORT_DECIMAL(io.trino.spi.type.BigintType.BIGINT),
        BOOLEAN(io.trino.spi.type.BooleanType.BOOLEAN),
        BINARY(io.trino.spi.type.VarbinaryType.VARBINARY);

        private final io.trino.spi.type.Type trinoType;

        ColumnKind(io.trino.spi.type.Type trinoType)
        {
            this.trinoType = trinoType;
        }

        public io.trino.spi.type.Type trinoType()
        {
            return trinoType;
        }
    }

    private record ColumnSpec(String name, ColumnKind kind, io.trino.spi.type.Type trinoType, boolean nullable, Set<BinaryVector.Trait> binaryTraits, Column column)
    {
        private ColumnSpec
        {
            binaryTraits = binaryTraits.isEmpty() ? Set.of() : Set.copyOf(binaryTraits);
        }
    }

    private record ColumnBuffer(Vector values, BooleanVector nulls) {}

    private static final class FileParquetDataSource
            extends AbstractParquetDataSource
    {
        private final RandomAccessFile input;

        private FileParquetDataSource(File file, ParquetReaderOptions options)
                throws FileNotFoundException
        {
            super(new ParquetDataSourceId(file.toString()), file.length(), options);
            this.input = new RandomAccessFile(file, "r");
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            input.seek(position);
            input.readFully(buffer, bufferOffset, bufferLength);
        }

        @Override
        public void close()
                throws IOException
        {
            input.close();
        }
    }
}
