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

import dev.hardwood.Hardwood;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.MultiFileColumnReaders;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.SchemaNode;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * A Hardwood-backed scan operator for flat top-level Parquet columns.
 * <p>
 * This is intended for the ClickBench path where we want a faster Parquet
 * reader layer without disturbing the existing parquet-java-based nested scan
 * path yet.
 */
public final class HardwoodParquetScanOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("HardwoodParquetScanOperator");
    private static final int MAX_BATCH_ROWS = 512;

    private final Allocator allocator;
    private final AutoCloseable[] closeables;
    private final List<ColumnSpec> columns;
    private final ColumnCursor[] cursors;
    private final int totalRows;

    private int nextBatchStart;
    private BatchState currentBatchState;
    private Batch currentBatch;

    public HardwoodParquetScanOperator(Allocator allocator, Path file, List<String> columns)
    {
        this(allocator, List.of(file), columns);
    }

    public HardwoodParquetScanOperator(Allocator allocator, List<Path> files, List<String> columns)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        requireNonNull(files, "files is null");
        requireNonNull(columns, "columns is null");
        checkArgument(!files.isEmpty(), "files is empty");

        try {
            if (files.size() == 1 && Files.isRegularFile(files.getFirst())) {
                ParquetFileReader reader = ParquetFileReader.open(files.getFirst());
                this.columns = columns.stream()
                        .map(name -> resolveColumn(reader.getFileSchema().getField(name), name))
                        .toList();
                cursors = this.columns.stream()
                        .map(spec -> new ColumnCursor(spec, reader.createColumnReader(spec.name())))
                        .toArray(ColumnCursor[]::new);
                totalRows = toIntExact(reader.getFileMetaData().numRows());
                closeables = new AutoCloseable[] {reader};
                return;
            }

            Hardwood hardwood = Hardwood.create();
            MultiFileParquetReader parquet = hardwood.openAll(files);
            this.columns = columns.stream()
                    .map(name -> resolveColumn(parquet.getFileSchema().getField(name), name))
                    .toList();
            totalRows = totalRows(hardwood, files);
            if (this.columns.isEmpty()) {
                cursors = new ColumnCursor[0];
                closeables = new AutoCloseable[] {parquet, hardwood};
                return;
            }

            MultiFileColumnReaders columnReaders = parquet.createColumnReaders(ColumnProjection.columns(columns.toArray(String[]::new)));
            cursors = this.columns.stream()
                    .map(spec -> new ColumnCursor(spec, columnReaders.getColumnReader(spec.name())))
                    .toArray(ColumnCursor[]::new);
            closeables = new AutoCloseable[] {columnReaders, parquet, hardwood};
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to open Parquet files: " + files, exception);
        }
    }

    @Override
    public int outputCount()
    {
        return columns.size();
    }

    @Override
    public boolean hasNext()
    {
        return nextBatchStart < totalRows;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more Parquet rows");
        }
        closeCurrentBatch();

        int batchStart = nextBatchStart;
        int rowCount = Math.min(MAX_BATCH_ROWS, totalRows - batchStart);
        nextBatchStart += rowCount;

        BatchState batchState = new BatchState(batchStart, rowCount, allocator.allocateAllMask(ALLOCATION_CONTEXT, rowCount));
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

    @Override
    public void constrain(Mask mask)
    {
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return false;
    }

    @Override
    public void close()
    {
        closeCurrentBatch();
        try {
            for (ColumnCursor cursor : cursors) {
                cursor.close();
            }
            for (AutoCloseable closeable : closeables) {
                closeable.close();
            }
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to close Hardwood Parquet reader", exception);
        }
        catch (Exception exception) {
            throw new RuntimeException("Unable to close Hardwood Parquet reader", exception);
        }
        finally {
            allocator.release(ALLOCATION_CONTEXT);
        }
    }

    private void closeCurrentBatch()
    {
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }
    }

    private ColumnBuffer resolveColumn(int columnIndex, BatchState batchState)
    {
        ColumnBuffer buffer = batchState.buffers()[columnIndex];
        if (buffer != null) {
            return buffer;
        }

        buffer = cursors[columnIndex].readBatch(batchState.startRow(), batchState.rowCount(), batchState.mask());
        batchState.buffers()[columnIndex] = buffer;
        return buffer;
    }

    private ColumnSpec resolveColumn(SchemaNode field, String name)
    {
        checkArgument(field != null, "Unknown Parquet column: %s", name);
        checkArgument(field instanceof SchemaNode.PrimitiveNode, "Only flat primitive Parquet columns are supported by HardwoodParquetScanOperator: %s", name);

        SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) field;
        return new ColumnSpec(
                name,
                switch (primitive.type()) {
                    case INT32 -> ColumnKind.I32;
                    case INT64 -> ColumnKind.I64;
                    case BOOLEAN -> ColumnKind.BOOLEAN;
                    case BYTE_ARRAY -> ColumnKind.BINARY;
                    default -> throw new IllegalArgumentException("Unsupported Hardwood Parquet primitive type for column %s: %s".formatted(name, primitive.type()));
                },
                primitive.repetitionType() != RepetitionType.REQUIRED,
                binaryTraits(primitive.logicalType()));
    }

    private static int totalRows(Hardwood hardwood, List<Path> files)
            throws IOException
    {
        long totalRows = 0;
        for (Path file : files) {
            try (ParquetFileReader reader = hardwood.open(file)) {
                totalRows += reader.getFileMetaData().numRows();
            }
        }
        return toIntExact(totalRows);
    }

    private static Set<BinaryVector.Trait> binaryTraits(LogicalType logicalType)
    {
        if (logicalType instanceof LogicalType.StringType) {
            return Set.of(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        }
        return Set.of();
    }

    private final class ColumnCursor
            implements AutoCloseable
    {
        private final ColumnSpec column;
        private final ColumnReader reader;

        private int globalPosition;
        private int batchRecordCount;
        private int batchRecordPosition;
        private int batchValuePosition;
        private int[] ints;
        private long[] longs;
        private boolean[] booleans;
        private byte[][] binaries;
        private BitSet nulls;

        private ColumnCursor(ColumnSpec column, ColumnReader reader)
        {
            this.column = column;
            this.reader = reader;
        }

        private ColumnBuffer readBatch(int startRow, int rowCount, Mask mask)
        {
            skipTo(startRow);
            return switch (column.kind()) {
                case I32 -> readI32Batch(rowCount, mask);
                case I64 -> readI64Batch(rowCount, mask);
                case BOOLEAN -> readBooleanBatch(rowCount, mask);
                case BINARY -> readBinaryBatch(rowCount, mask);
            };
        }

        private ColumnBuffer readI32Batch(int rowCount, Mask mask)
        {
            I32Vector values = allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, rowCount, I32Vector::new);
            BooleanVector nullVector = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
            boolean[] nulls = nullVector == null ? null : nullVector.values();
            int[] result = values.values();
            int selectedIndex = 0;
            for (int position = 0; position < rowCount; position++) {
                loadBatchIfNecessary();
                boolean selected = mask.all() || (selectedIndex < mask.selectedCount() && mask.position(selectedIndex) == position);
                if (selected && !mask.all()) {
                    selectedIndex++;
                }
                if (isNull()) {
                    if (nulls != null && selected) {
                        nulls[position] = true;
                    }
                    advance(1);
                    continue;
                }
                if (selected) {
                    result[position] = ints[batchValuePosition];
                }
                advance(1);
            }
            return new ColumnBuffer(values, nullVector);
        }

        private ColumnBuffer readI64Batch(int rowCount, Mask mask)
        {
            I64Vector values = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, rowCount, I64Vector::new);
            BooleanVector nullVector = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
            boolean[] nulls = nullVector == null ? null : nullVector.values();
            long[] result = values.values();
            int selectedIndex = 0;
            for (int position = 0; position < rowCount; position++) {
                loadBatchIfNecessary();
                boolean selected = mask.all() || (selectedIndex < mask.selectedCount() && mask.position(selectedIndex) == position);
                if (selected && !mask.all()) {
                    selectedIndex++;
                }
                if (isNull()) {
                    if (nulls != null && selected) {
                        nulls[position] = true;
                    }
                    advance(1);
                    continue;
                }
                if (selected) {
                    result[position] = longs[batchValuePosition];
                }
                advance(1);
            }
            return new ColumnBuffer(values, nullVector);
        }

        private ColumnBuffer readBooleanBatch(int rowCount, Mask mask)
        {
            BooleanVector values = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new);
            BooleanVector nullVector = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
            boolean[] nulls = nullVector == null ? null : nullVector.values();
            boolean[] result = values.values();
            int selectedIndex = 0;
            for (int position = 0; position < rowCount; position++) {
                loadBatchIfNecessary();
                boolean selected = mask.all() || (selectedIndex < mask.selectedCount() && mask.position(selectedIndex) == position);
                if (selected && !mask.all()) {
                    selectedIndex++;
                }
                if (isNull()) {
                    if (nulls != null && selected) {
                        nulls[position] = true;
                    }
                    advance(1);
                    continue;
                }
                if (selected) {
                    result[position] = booleans[batchValuePosition];
                }
                advance(1);
            }
            return new ColumnBuffer(values, nullVector);
        }

        private ColumnBuffer readBinaryBatch(int rowCount, Mask mask)
        {
            BinaryVector values = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, rowCount, 0);
            values.addTraits(column.binaryTraits());
            BooleanVector nullVector = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
            boolean[] nulls = nullVector == null ? null : nullVector.values();
            int selectedIndex = 0;
            int totalBytes = 0;
            for (int position = 0; position < rowCount; position++) {
                loadBatchIfNecessary();
                boolean selected = mask.all() || (selectedIndex < mask.selectedCount() && mask.position(selectedIndex) == position);
                if (selected && !mask.all()) {
                    selectedIndex++;
                }
                if (isNull()) {
                    if (nulls != null && selected) {
                        nulls[position] = true;
                    }
                    values.setNull(position);
                    advance(1);
                    continue;
                }
                byte[] value = binaries[batchValuePosition];
                if (selected) {
                    totalBytes += value.length;
                    values = BinaryVector.allocateOrGrow(allocator, ALLOCATION_CONTEXT, values, rowCount, totalBytes);
                    values.setBytes(position, value);
                }
                else {
                    values.setNull(position);
                }
                advance(1);
            }
            return new ColumnBuffer(values, nullVector);
        }

        private void skipTo(int targetRow)
        {
            while (globalPosition < targetRow) {
                loadBatchIfNecessary();
                int skipped = Math.min(targetRow - globalPosition, batchRecordCount - batchRecordPosition);
                advance(skipped);
            }
        }

        private boolean isNull()
        {
            return nulls != null && nulls.get(batchRecordPosition);
        }

        private void advance(int recordCount)
        {
            for (int index = 0; index < recordCount; index++) {
                if (!isNull()) {
                    batchValuePosition++;
                }
                batchRecordPosition++;
                globalPosition++;
                if (batchRecordPosition == batchRecordCount) {
                    clearBatch();
                    break;
                }
            }
        }

        private void loadBatchIfNecessary()
        {
            if (batchRecordPosition < batchRecordCount) {
                return;
            }
            if (!reader.nextBatch()) {
                throw new IllegalStateException("Hardwood column reader ran out of rows for column " + column.name());
            }
            batchRecordCount = reader.getRecordCount();
            batchRecordPosition = 0;
            batchValuePosition = 0;
            nulls = column.nullable() ? reader.getElementNulls() : null;
            ints = column.kind() == ColumnKind.I32 ? reader.getInts() : null;
            longs = column.kind() == ColumnKind.I64 ? reader.getLongs() : null;
            booleans = column.kind() == ColumnKind.BOOLEAN ? reader.getBooleans() : null;
            binaries = column.kind() == ColumnKind.BINARY ? reader.getBinaries() : null;
        }

        private void clearBatch()
        {
            batchRecordCount = 0;
            batchRecordPosition = 0;
            batchValuePosition = 0;
            ints = null;
            longs = null;
            booleans = null;
            binaries = null;
            nulls = null;
        }

        @Override
        public void close()
        {
            reader.close();
        }
    }

    private final class BatchState
    {
        private final int startRow;
        private final int rowCount;
        private final Mask[] maskHolder;
        private final ColumnBuffer[] buffers;

        private BatchState(int startRow, int rowCount, Mask mask)
        {
            this.startRow = startRow;
            this.rowCount = rowCount;
            this.maskHolder = new Mask[] {mask};
            this.buffers = new ColumnBuffer[columns.size()];
        }

        private int startRow()
        {
            return startRow;
        }

        private int rowCount()
        {
            return rowCount;
        }

        private Mask mask()
        {
            return maskHolder[0];
        }

        private ColumnBuffer[] buffers()
        {
            return buffers;
        }

        private void constrain(Mask mask)
        {
            maskHolder[0] = mask;
            Arrays.fill(buffers, null);
        }
    }

    private enum ColumnKind
    {
        I32,
        I64,
        BOOLEAN,
        BINARY,
    }

    private record ColumnSpec(String name, ColumnKind kind, boolean nullable, Set<BinaryVector.Trait> binaryTraits)
    {
        private ColumnSpec
        {
            binaryTraits = binaryTraits.isEmpty() ? Set.of() : Set.copyOf(binaryTraits);
        }
    }

    private record ColumnBuffer(Vector values, BooleanVector nulls) {}
}
