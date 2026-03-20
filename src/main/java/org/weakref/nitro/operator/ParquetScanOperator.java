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

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public final class ParquetScanOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ParquetScanOperator");
    private static final int MAX_BATCH_ROWS = 512;

    private final Allocator allocator;
    private final ParquetFileReader reader;
    private final MessageType schema;
    private final String createdBy;
    private final GroupConverter recordConverter;
    private final List<ColumnSpec> columns;

    private PageReadStore currentRowGroup;
    private ColumnPages[] currentRowGroupColumnPages;
    private List<ColumnPages>[] currentRowGroupNestedColumnPages;
    private int currentRowGroupPosition;
    private PageReadStore nextRowGroup;
    private RowGroupBatchState currentBatchState;

    public ParquetScanOperator(Allocator allocator, java.nio.file.Path file, List<String> columns)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        requireNonNull(file, "file is null");
        requireNonNull(columns, "columns is null");

        try {
            reader = ParquetFileReader.open(new LocalInputFile(file));
            schema = reader.getFooter().getFileMetaData().getSchema();
            createdBy = reader.getFooter().getFileMetaData().getCreatedBy();
            recordConverter = new NoOpGroupConverter(schema);
            this.columns = columns.stream()
                    .map(this::resolveColumn)
                    .toList();
            reader.setRequestedSchema(new MessageType(schema.getName(), this.columns.stream()
                    .map(ColumnSpec::projectedType)
                    .toList()));
            nextRowGroup = reader.readNextRowGroup();
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to open Parquet file: " + file, exception);
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
        return (currentRowGroup != null && currentRowGroupPosition < currentRowGroup.getRowCount()) || nextRowGroup != null;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more Parquet row groups");
        }

        advanceRowGroupIfNecessary();
        int rowGroupRowCount = toIntExact(currentRowGroup.getRowCount());
        int batchStart = currentRowGroupPosition;
        int batchRowCount = Math.min(MAX_BATCH_ROWS, rowGroupRowCount - batchStart);
        currentRowGroupPosition += batchRowCount;

        RowGroupBatchState batchState = new RowGroupBatchState(
                currentRowGroup,
                rowGroupRowCount,
                batchStart,
                batchRowCount,
                currentRowGroupColumnPages,
                currentRowGroupNestedColumnPages,
                allocator.allocateAllMask(ALLOCATION_CONTEXT, batchRowCount));
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
                    (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
        }
        return new Batch(batchState.mask(), batchState::constrain, takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask), outputs);
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
        return true;
    }

    @Override
    public void close()
    {
        try {
            reader.close();
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to close Parquet file reader", exception);
        }
        finally {
            allocator.release(ALLOCATION_CONTEXT);
        }
    }

    private void advanceRowGroupIfNecessary()
    {
        if (currentRowGroup != null && currentRowGroupPosition < currentRowGroup.getRowCount()) {
            return;
        }
        currentRowGroup = nextRowGroup;
        if (currentRowGroup != null) {
            currentRowGroupColumnPages = new ColumnPages[columns.size()];
            @SuppressWarnings("unchecked")
            List<ColumnPages>[] nestedColumnPages = (List<ColumnPages>[]) new List<?>[columns.size()];
            currentRowGroupNestedColumnPages = nestedColumnPages;
        }
        else {
            currentRowGroupColumnPages = null;
            currentRowGroupNestedColumnPages = null;
        }
        currentRowGroupPosition = 0;
        loadNextRowGroup();
    }

    private void loadNextRowGroup()
    {
        try {
            nextRowGroup = reader.readNextRowGroup();
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to read next Parquet row group", exception);
        }
    }

    private ColumnBuffer resolveColumn(int columnIndex, RowGroupBatchState batchState)
    {
        ColumnBuffer buffer = batchState.buffers()[columnIndex];
        if (buffer != null) {
            return buffer;
        }

        ColumnSpec column = columns.get(columnIndex);
        buffer = switch (column.kind()) {
            case I32 -> {
                ColumnPages columnPages = columnPages(columnIndex, batchState);
                yield readI32Column(column, columnPages, batchState.startRow(), batchState.rowCount(), createColumnReader(columnPages), batchState.mask());
            }
            case I64 -> {
                ColumnPages columnPages = columnPages(columnIndex, batchState);
                yield readI64Column(column, columnPages, batchState.startRow(), batchState.rowCount(), createColumnReader(columnPages), batchState.mask());
            }
            case BOOLEAN -> {
                ColumnPages columnPages = columnPages(columnIndex, batchState);
                yield readBooleanColumn(column, batchState.startRow(), batchState.rowCount(), createColumnReader(columnPages), batchState.mask());
            }
            case BINARY -> {
                ColumnPages columnPages = columnPages(columnIndex, batchState);
                yield readBinaryColumn(column, columnPages, batchState.startRow(), batchState.rowCount(), batchState.mask());
            }
            case ARRAY_I64 -> readArrayI64Column(column, columnPages(columnIndex, batchState), batchState.rowGroupRowCount(), batchState.startRow(), batchState.rowCount(), batchState.mask());
            case MAP -> readMapColumn(column, nestedColumnPages(columnIndex, batchState), batchState.rowGroupRowCount(), batchState.startRow(), batchState.rowCount(), batchState.mask());
            case STRUCT -> readStructColumn(column, nestedColumnPages(columnIndex, batchState), batchState.rowGroupRowCount(), batchState.startRow(), batchState.rowCount(), batchState.mask());
        };
        batchState.buffers()[columnIndex] = buffer;
        return buffer;
    }

    private ColumnPages columnPages(int columnIndex, RowGroupBatchState batchState)
    {
        ColumnPages columnPages = batchState.columnPages()[columnIndex];
        if (columnPages == null) {
            columnPages = captureColumnPages(batchState.rowGroup(), columns.get(columnIndex).descriptor());
            batchState.columnPages()[columnIndex] = columnPages;
        }
        return columnPages;
    }

    private List<ColumnPages> nestedColumnPages(int columnIndex, RowGroupBatchState batchState)
    {
        List<ColumnPages> nestedColumnPages = batchState.nestedColumnPages()[columnIndex];
        if (nestedColumnPages == null) {
            ColumnSpec column = columns.get(columnIndex);
            nestedColumnPages = switch (column.kind()) {
                case MAP -> captureMapColumnPages(batchState.rowGroup(), column);
                case STRUCT -> captureStructColumnPages(batchState.rowGroup(), column);
                default -> throw new IllegalArgumentException("Unsupported nested Parquet column kind: " + column.kind());
            };
            batchState.nestedColumnPages()[columnIndex] = nestedColumnPages;
        }
        return nestedColumnPages;
    }

    private final class RowGroupBatchState
    {
        private final PageReadStore rowGroup;
        private final int rowGroupRowCount;
        private final int startRow;
        private final int rowCount;
        private final ColumnPages[] columnPages;
        private final List<ColumnPages>[] nestedColumnPages;
        private final Mask[] maskHolder;
        private final ColumnBuffer[] buffers;

        private RowGroupBatchState(PageReadStore rowGroup, int rowGroupRowCount, int startRow, int rowCount, ColumnPages[] columnPages, List<ColumnPages>[] nestedColumnPages, Mask mask)
        {
            this.rowGroup = rowGroup;
            this.rowGroupRowCount = rowGroupRowCount;
            this.startRow = startRow;
            this.rowCount = rowCount;
            this.columnPages = columnPages;
            this.nestedColumnPages = nestedColumnPages;
            this.maskHolder = new Mask[] {mask};
            this.buffers = new ColumnBuffer[columns.size()];
        }

        private PageReadStore rowGroup()
        {
            return rowGroup;
        }

        private int rowCount()
        {
            return rowCount;
        }

        private int rowGroupRowCount()
        {
            return rowGroupRowCount;
        }

        private int startRow()
        {
            return startRow;
        }

        private Mask mask()
        {
            return maskHolder[0];
        }

        private ColumnPages[] columnPages()
        {
            return columnPages;
        }

        private ColumnBuffer[] buffers()
        {
            return buffers;
        }

        private List<ColumnPages>[] nestedColumnPages()
        {
            return nestedColumnPages;
        }

        private void constrain(Mask mask)
        {
            maskHolder[0] = mask;
            Arrays.fill(buffers, null);
        }
    }

    private ColumnSpec resolveColumn(String name)
    {
        checkArgument(schema.containsField(name), "Unknown Parquet column: %s", name);
        Type type = schema.getType(name);
        if (type.isPrimitive()) {
            PrimitiveType primitiveType = type.asPrimitiveType();
            return new ColumnSpec(
                    name,
                    switch (primitiveType.getRepetition()) {
                        case REPEATED -> switch (primitiveType.getPrimitiveTypeName()) {
                            case INT64 -> ColumnKind.ARRAY_I64;
                            default -> throw new IllegalArgumentException("Unsupported repeated Parquet primitive type for column %s: %s".formatted(name, primitiveType.getPrimitiveTypeName()));
                        };
                        case OPTIONAL, REQUIRED -> switch (primitiveType.getPrimitiveTypeName()) {
                            case INT32 -> ColumnKind.I32;
                            case INT64 -> ColumnKind.I64;
                            case BOOLEAN -> ColumnKind.BOOLEAN;
                            case BINARY -> ColumnKind.BINARY;
                            default -> throw new IllegalArgumentException("Unsupported Parquet primitive type for column %s: %s".formatted(name, primitiveType.getPrimitiveTypeName()));
                        };
                    },
                    primitiveType.getRepetition() != REQUIRED && primitiveType.getRepetition() != Type.Repetition.REPEATED,
                    schema.getColumnDescription(new String[] {name}),
                    binaryTraits(primitiveType),
                    type,
                    null,
                    null,
                    false,
                    List.of(),
                    null);
        }

        GroupType groupType = type.asGroupType();
        if (mapType().equals(groupType.getLogicalTypeAnnotation())) {
            checkArgument(groupType.getFieldCount() == 1, "Parquet map column must contain one repeated entries field: %s", name);
            GroupType entriesType = groupType.getType(0).asGroupType();
            checkArgument(entriesType.getRepetition() == Type.Repetition.REPEATED, "Parquet map entries field must be repeated: %s", name);
            checkArgument(entriesType.getFieldCount() == 2, "Parquet map entries field must contain key and value: %s", name);

            Type keyType = entriesType.getType(0);
            checkArgument(keyType.isPrimitive(), "Parquet map keys must be primitive: %s", name);
            PrimitiveType primitiveKeyType = keyType.asPrimitiveType();
            checkArgument(primitiveKeyType.getRepetition() == REQUIRED, "Parquet map keys must be required: %s", name);
            ColumnKind keyKind = switch (primitiveKeyType.getPrimitiveTypeName()) {
                case INT32 -> ColumnKind.I32;
                case INT64 -> ColumnKind.I64;
                case BINARY -> ColumnKind.BINARY;
                default -> throw new IllegalArgumentException("Unsupported Parquet map key type for column %s: %s".formatted(name, primitiveKeyType.getPrimitiveTypeName()));
            };

            Type valueType = entriesType.getType(1);
            checkArgument(valueType.isPrimitive(), "Parquet map values must be primitive: %s", name);
            PrimitiveType primitiveValueType = valueType.asPrimitiveType();
            ColumnKind valueKind = switch (primitiveValueType.getPrimitiveTypeName()) {
                case INT32 -> ColumnKind.I32;
                case INT64 -> ColumnKind.I64;
                case BINARY -> ColumnKind.BINARY;
                default -> throw new IllegalArgumentException("Unsupported Parquet map value type for column %s: %s".formatted(name, primitiveValueType.getPrimitiveTypeName()));
            };

            return new ColumnSpec(
                    name,
                    ColumnKind.MAP,
                    groupType.getRepetition() != REQUIRED,
                    null,
                    Set.of(),
                    type,
                    null,
                    null,
                    false,
                    List.of(),
                    new MapSpec(
                            entriesType.getName(),
                            new MapComponentSpec(keyType.getName(), keyKind, false, schema.getColumnDescription(new String[] {name, entriesType.getName(), keyType.getName()}), binaryTraits(primitiveKeyType)),
                            new MapComponentSpec(valueType.getName(), valueKind, primitiveValueType.getRepetition() != REQUIRED, schema.getColumnDescription(new String[] {name, entriesType.getName(), valueType.getName()}), binaryTraits(primitiveValueType))));
        }
        if (groupType.getRepetition() == Type.Repetition.REPEATED) {
            checkArgument(groupType.getFieldCount() == 1, "Repeated Parquet groups must have one field: %s", name);
            Type elementType = groupType.getType(0);
            checkArgument(elementType.isPrimitive(), "Repeated Parquet group elements must be primitive: %s", name);
            PrimitiveType primitiveElementType = elementType.asPrimitiveType();
            checkArgument(primitiveElementType.getPrimitiveTypeName() == INT64, "Unsupported repeated Parquet group element type for column %s: %s", name, primitiveElementType.getPrimitiveTypeName());

            return new ColumnSpec(
                    name,
                    ColumnKind.ARRAY_I64,
                    false,
                    schema.getColumnDescription(new String[] {name, elementType.getName()}),
                    Set.of(),
                    type,
                    null,
                    elementType.getName(),
                    primitiveElementType.getRepetition() != REQUIRED,
                    List.of(),
                    null);
        }

        checkArgument(groupType.getRepetition() != Type.Repetition.REPEATED, "Unsupported repeated Parquet group column: %s", name);
        List<StructFieldSpec> structFields = groupType.getFields().stream()
                .map(field -> {
                    checkArgument(field.isPrimitive(), "Struct field must be primitive: %s.%s", name, field.getName());
                    PrimitiveType primitiveField = field.asPrimitiveType();
                    return new StructFieldSpec(
                            field.getName(),
                            switch (primitiveField.getPrimitiveTypeName()) {
                                case INT32 -> ColumnKind.I32;
                                case INT64 -> ColumnKind.I64;
                                case BOOLEAN -> ColumnKind.BOOLEAN;
                                case BINARY -> ColumnKind.BINARY;
                                default -> throw new IllegalArgumentException("Unsupported Parquet struct field type for column %s.%s: %s".formatted(name, field.getName(), primitiveField.getPrimitiveTypeName()));
                            },
                            primitiveField.getRepetition() != REQUIRED,
                            schema.getColumnDescription(new String[] {name, field.getName()}),
                            binaryTraits(primitiveField));
                })
                .toList();

        return new ColumnSpec(
                name,
                ColumnKind.STRUCT,
                groupType.getRepetition() != REQUIRED,
                null,
                Set.of(),
                type,
                groupType,
                null,
                false,
                structFields,
                null);
    }

    private ColumnBuffer readArrayI64Column(ColumnSpec column, ColumnPages columnPages, int rowGroupRowCount, int startRow, int rowCount, Mask mask)
    {
        ArrayVector values = allocator.allocateArray(ALLOCATION_CONTEXT, rowCount);
        String columnName = column.name();
        MessageType projectedSchema = new MessageType(schema.getName(), column.projectedType());
        MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(projectedSchema);
        RecordReader<Group> recordReader = columnIo.getRecordReader(
                new ReplayPageReadStore(rowGroupRowCount, List.of(columnPages)),
                new GroupRecordConverter(projectedSchema));
        skipRecords(recordReader, startRow);
        int[] offsets = values.offsets();

        int elementCount = 0;
        int maskIndex = 0;
        int nextMaskPosition = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        for (int position = 0; position < rowCount; position++) {
            Group row = recordReader.read();
            if (mask.all() || position == nextMaskPosition) {
                int valueCount = row.getFieldRepetitionCount(columnName);
                elementCount += valueCount;
                if (!mask.all()) {
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
            }
            offsets[position + 1] = elementCount;
        }

        I64Vector elementValues = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, elementCount, I64Vector::new);
        BooleanVector elementNulls = column.elementNullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, elementCount, BooleanVector::new) : null;
        recordReader = columnIo.getRecordReader(
                new ReplayPageReadStore(rowGroupRowCount, List.of(columnPages)),
                new GroupRecordConverter(projectedSchema));
        skipRecords(recordReader, startRow);
        int elementIndex = 0;
        maskIndex = 0;
        nextMaskPosition = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        for (int position = 0; position < rowCount; position++) {
            Group row = recordReader.read();
            if (!mask.all() && position != nextMaskPosition) {
                continue;
            }
            int valueCount = row.getFieldRepetitionCount(columnName);
            for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                if (column.elementNullable()) {
                    Group elementGroup = row.getGroup(columnName, valueIndex);
                    if (elementGroup.getFieldRepetitionCount(column.elementName()) == 0) {
                        elementNulls.values()[elementIndex++] = true;
                    }
                    else {
                        elementValues.values()[elementIndex] = elementGroup.getLong(column.elementName(), 0);
                        elementIndex++;
                    }
                }
                else {
                    elementValues.values()[elementIndex++] = row.getLong(columnName, valueIndex);
                }
            }
            if (!mask.all()) {
                maskIndex++;
                nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
            }
        }
        values.setElements(elementNulls == null ? Streams.ofValues(elementValues) : Streams.ofValuesAndNulls(elementValues, elementNulls));
        return new ColumnBuffer(values, null);
    }

    private ColumnBuffer readMapColumn(ColumnSpec column, List<ColumnPages> mapPages, int rowGroupRowCount, int startRow, int rowCount, Mask mask)
    {
        checkArgument(column.mapSpec() != null, "Map column is missing map metadata: %s", column.name());

        MapVector values = allocator.allocateMap(ALLOCATION_CONTEXT, rowCount);
        BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
        MessageType projectedSchema = new MessageType(schema.getName(), column.projectedType());
        MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(projectedSchema);

        int entryCount = 0;
        int keyBinaryCapacity = 0;
        int valueBinaryCapacity = 0;
        RecordReader<Group> recordReader = columnIo.getRecordReader(new ReplayPageReadStore(rowGroupRowCount, mapPages), new GroupRecordConverter(projectedSchema));
        skipRecords(recordReader, startRow);
        int maskIndex = 0;
        int nextMaskPosition = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        for (int position = 0; position < rowCount; position++) {
            Group row = recordReader.read();
            if (!mask.all() && position != nextMaskPosition) {
                values.offsets()[position + 1] = entryCount;
                continue;
            }
            if (column.nullable() && row.getFieldRepetitionCount(column.name()) == 0) {
                values.offsets()[position + 1] = entryCount;
                if (!mask.all()) {
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                continue;
            }
            Group mapGroup = row.getGroup(column.name(), 0);
            int valueCount = mapGroup.getFieldRepetitionCount(column.mapSpec().entriesName());
            entryCount += valueCount;
            values.offsets()[position + 1] = entryCount;
            if (column.mapSpec().key().kind() == ColumnKind.BINARY) {
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    keyBinaryCapacity += mapGroup.getGroup(column.mapSpec().entriesName(), valueIndex).getBinary(column.mapSpec().key().name(), 0).length();
                }
            }
            if (column.mapSpec().value().kind() == ColumnKind.BINARY) {
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    Group entry = mapGroup.getGroup(column.mapSpec().entriesName(), valueIndex);
                    if (!column.mapSpec().value().nullable() || entry.getFieldRepetitionCount(column.mapSpec().value().name()) > 0) {
                        valueBinaryCapacity += entry.getBinary(column.mapSpec().value().name(), 0).length();
                    }
                }
            }
            if (!mask.all()) {
                maskIndex++;
                nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
            }
        }

        Vector keyValues = switch (column.mapSpec().key().kind()) {
            case I32 -> allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, entryCount, I32Vector::new);
            case I64 -> allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, entryCount, I64Vector::new);
            case BINARY -> allocator.allocateBinary(ALLOCATION_CONTEXT, entryCount, keyBinaryCapacity);
            default -> throw new IllegalArgumentException("Unsupported map key kind: " + column.mapSpec().key().kind());
        };
        Vector valueValues = switch (column.mapSpec().value().kind()) {
            case I32 -> allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, entryCount, I32Vector::new);
            case I64 -> allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, entryCount, I64Vector::new);
            case BINARY -> allocator.allocateBinary(ALLOCATION_CONTEXT, entryCount, valueBinaryCapacity);
            default -> throw new IllegalArgumentException("Unsupported map value kind: " + column.mapSpec().value().kind());
        };
        BooleanVector valueNulls = column.mapSpec().value().nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, entryCount, BooleanVector::new) : null;

        recordReader = columnIo.getRecordReader(new ReplayPageReadStore(rowGroupRowCount, mapPages), new GroupRecordConverter(projectedSchema));
        skipRecords(recordReader, startRow);
        int entryIndex = 0;
        maskIndex = 0;
        nextMaskPosition = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        for (int position = 0; position < rowCount; position++) {
            Group row = recordReader.read();
            if (!mask.all() && position != nextMaskPosition) {
                continue;
            }
            if (column.nullable() && row.getFieldRepetitionCount(column.name()) == 0) {
                nulls.values()[position] = true;
                if (!mask.all()) {
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                continue;
            }
            Group mapGroup = row.getGroup(column.name(), 0);
            int valueCount = mapGroup.getFieldRepetitionCount(column.mapSpec().entriesName());
            for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                Group entry = mapGroup.getGroup(column.mapSpec().entriesName(), valueIndex);
                switch (column.mapSpec().key().kind()) {
                    case I32 -> ((I32Vector) keyValues).values()[entryIndex] = entry.getInteger(column.mapSpec().key().name(), 0);
                    case I64 -> ((I64Vector) keyValues).values()[entryIndex] = entry.getLong(column.mapSpec().key().name(), 0);
                    case BINARY -> ((BinaryVector) keyValues).setBytes(entryIndex, entry.getBinary(column.mapSpec().key().name(), 0).getBytesUnsafe());
                    default -> throw new IllegalArgumentException("Unsupported map key kind: " + column.mapSpec().key().kind());
                }

                if (column.mapSpec().value().nullable() && entry.getFieldRepetitionCount(column.mapSpec().value().name()) == 0) {
                    valueNulls.values()[entryIndex] = true;
                    if (valueValues instanceof BinaryVector binaryValues) {
                        binaryValues.setNull(entryIndex);
                    }
                }
                else {
                    switch (column.mapSpec().value().kind()) {
                        case I32 -> ((I32Vector) valueValues).values()[entryIndex] = entry.getInteger(column.mapSpec().value().name(), 0);
                        case I64 -> ((I64Vector) valueValues).values()[entryIndex] = entry.getLong(column.mapSpec().value().name(), 0);
                        case BINARY -> ((BinaryVector) valueValues).setBytes(entryIndex, entry.getBinary(column.mapSpec().value().name(), 0).getBytesUnsafe());
                        default -> throw new IllegalArgumentException("Unsupported map value kind: " + column.mapSpec().value().kind());
                    }
                }
                entryIndex++;
            }
            if (!mask.all()) {
                maskIndex++;
                nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
            }
        }

        if (keyValues instanceof BinaryVector binaryKeyValues) {
            applyBinaryTraits(binaryKeyValues, column.mapSpec().key().binaryTraits(), entryCount);
        }
        if (valueValues instanceof BinaryVector binaryValueValues) {
            applyBinaryTraits(binaryValueValues, column.mapSpec().value().binaryTraits(), entryCount);
        }
        values.setEntries(
                Streams.ofValues(keyValues),
                valueNulls == null ? Streams.ofValues(valueValues) : Streams.ofValuesAndNulls(valueValues, valueNulls));
        return new ColumnBuffer(values, nulls);
    }

    private ColumnBuffer readStructColumn(ColumnSpec column, List<ColumnPages> fieldPages, int rowGroupRowCount, int startRow, int rowCount, Mask mask)
    {
        StructVector values = allocator.allocate(ALLOCATION_CONTEXT, StructVector.class, rowCount, StructVector::new);
        BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
        MessageType projectedSchema = new MessageType(schema.getName(), column.projectedType());
        MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(projectedSchema);
        ReplayPageReadStore replayPageStore = new ReplayPageReadStore(rowGroupRowCount, fieldPages);

        int[] binaryCapacities = new int[column.structFields().size()];
        RecordReader<Group> recordReader = columnIo.getRecordReader(replayPageStore, new GroupRecordConverter(projectedSchema));
        skipRecords(recordReader, startRow);
        int maskIndex = 0;
        int nextMaskPosition = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        for (int position = 0; position < rowCount; position++) {
            Group row = recordReader.read();
            if (!mask.all() && position != nextMaskPosition) {
                continue;
            }
            if (column.nullable() && row.getFieldRepetitionCount(column.name()) == 0) {
                if (!mask.all()) {
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                continue;
            }
            Group struct = row.getGroup(column.name(), 0);
            for (int fieldIndex = 0; fieldIndex < column.structFields().size(); fieldIndex++) {
                StructFieldSpec field = column.structFields().get(fieldIndex);
                if (field.kind() == ColumnKind.BINARY && struct.getFieldRepetitionCount(field.name()) > 0) {
                    binaryCapacities[fieldIndex] += struct.getBinary(field.name(), 0).length();
                }
            }
            if (!mask.all()) {
                maskIndex++;
                nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
            }
        }

        Vector[] fieldValues = new Vector[column.structFields().size()];
        BooleanVector[] fieldNulls = new BooleanVector[column.structFields().size()];
        for (int fieldIndex = 0; fieldIndex < column.structFields().size(); fieldIndex++) {
            StructFieldSpec field = column.structFields().get(fieldIndex);
            fieldValues[fieldIndex] = switch (field.kind()) {
                case I32 -> allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, rowCount, I32Vector::new);
                case I64 -> allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, rowCount, I64Vector::new);
                case BOOLEAN -> allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new);
                case BINARY -> allocator.allocateBinary(ALLOCATION_CONTEXT, rowCount, binaryCapacities[fieldIndex]);
                default -> throw new IllegalArgumentException("Unsupported struct field kind: " + field.kind());
            };
            fieldNulls[fieldIndex] = field.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
        }

        recordReader = columnIo.getRecordReader(new ReplayPageReadStore(rowGroupRowCount, fieldPages), new GroupRecordConverter(projectedSchema));
        skipRecords(recordReader, startRow);
        maskIndex = 0;
        nextMaskPosition = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        for (int position = 0; position < rowCount; position++) {
            Group row = recordReader.read();
            if (!mask.all() && position != nextMaskPosition) {
                continue;
            }
            if (column.nullable() && row.getFieldRepetitionCount(column.name()) == 0) {
                nulls.values()[position] = true;
                for (Vector fieldValue : fieldValues) {
                    if (fieldValue instanceof BinaryVector binaryValues) {
                        binaryValues.setNull(position);
                    }
                }
                if (!mask.all()) {
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                continue;
            }
            Group struct = row.getGroup(column.name(), 0);
            for (int fieldIndex = 0; fieldIndex < column.structFields().size(); fieldIndex++) {
                StructFieldSpec field = column.structFields().get(fieldIndex);
                BooleanVector fieldNullVector = fieldNulls[fieldIndex];
                if (struct.getFieldRepetitionCount(field.name()) == 0) {
                    if (fieldNullVector != null) {
                        fieldNullVector.values()[position] = true;
                    }
                    if (fieldValues[fieldIndex] instanceof BinaryVector binaryValues) {
                        binaryValues.setNull(position);
                    }
                    continue;
                }

                switch (field.kind()) {
                    case I32 -> ((I32Vector) fieldValues[fieldIndex]).values()[position] = struct.getInteger(field.name(), 0);
                    case I64 -> ((I64Vector) fieldValues[fieldIndex]).values()[position] = struct.getLong(field.name(), 0);
                    case BOOLEAN -> ((BooleanVector) fieldValues[fieldIndex]).values()[position] = struct.getBoolean(field.name(), 0);
                    case BINARY -> ((BinaryVector) fieldValues[fieldIndex]).setBytes(position, struct.getBinary(field.name(), 0).getBytesUnsafe());
                    default -> throw new IllegalArgumentException("Unsupported struct field kind: " + field.kind());
                }
            }
            if (!mask.all()) {
                maskIndex++;
                nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
            }
        }

        for (int fieldIndex = 0; fieldIndex < column.structFields().size(); fieldIndex++) {
            StructFieldSpec field = column.structFields().get(fieldIndex);
            if (fieldValues[fieldIndex] instanceof BinaryVector binaryValues) {
                applyBinaryTraits(binaryValues, field.binaryTraits(), rowCount);
            }
            Streams fieldStreams = Streams.ofValues(fieldValues[fieldIndex]);
            if (fieldNulls[fieldIndex] != null) {
                fieldStreams = fieldStreams.with(Stream.NULLS, fieldNulls[fieldIndex]);
            }
            values.setField(field.name(), fieldStreams);
        }
        return new ColumnBuffer(values, nulls);
    }

    private ColumnBuffer readI32Column(ColumnSpec column, ColumnPages columnPages, int startRow, int rowCount, ColumnReader columnReader, Mask mask)
    {
        skipColumnEntries(columnReader, startRow);
        if (columnPages.dictionaryEncoded()) {
            return readDictionaryI32Column(column, columnPages, rowCount, columnReader, mask);
        }
        return readFlatI32Column(column, rowCount, columnReader, mask);
    }

    private ColumnBuffer readDictionaryI32Column(ColumnSpec column, ColumnPages columnPages, int rowCount, ColumnReader columnReader, Mask mask)
    {
        if (columnPages.dictionaryPage() == null || columnPages.dictionaryPage().getDictionarySize() == 0) {
            return readFlatI32Column(column, rowCount, columnReader, mask);
        }

        try {
            Dictionary dictionary = columnPages.dictionaryPage().getEncoding().initDictionary(column.descriptor(), columnPages.dictionaryPage());
            I32Vector dictionaryValues = allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, columnPages.dictionaryPage().getDictionarySize(), I32Vector::new);
            int[] dictionaryEntries = dictionaryValues.values();
            for (int index = 0; index < dictionaryEntries.length; index++) {
                dictionaryEntries[index] = dictionary.decodeToInt(index);
            }

            int[] ids = new int[rowCount];
            BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
            boolean[] outputNulls = nulls == null ? null : nulls.values();
            int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
            if (mask.all()) {
                if (nulls == null) {
                    for (int position = 0; position < rowCount; position++) {
                        ids[position] = columnReader.getCurrentValueDictionaryID();
                        columnReader.consume();
                    }
                }
                else {
                    for (int position = 0; position < rowCount; position++) {
                        if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                            ids[position] = columnReader.getCurrentValueDictionaryID();
                        }
                        else {
                            outputNulls[position] = true;
                        }
                        columnReader.consume();
                    }
                }
            }
            else if (nulls == null) {
                int maskIndex = 0;
                int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
                for (int position = 0; position < rowCount; position++) {
                    if (position == nextMaskPosition) {
                        ids[position] = columnReader.getCurrentValueDictionaryID();
                        maskIndex++;
                        nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                    }
                    else {
                        columnReader.getCurrentValueDictionaryID();
                    }
                    columnReader.consume();
                }
            }
            else {
                int maskIndex = 0;
                int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
                for (int position = 0; position < rowCount; position++) {
                    boolean present = columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel;
                    if (position == nextMaskPosition) {
                        if (present) {
                            ids[position] = columnReader.getCurrentValueDictionaryID();
                        }
                        else {
                            outputNulls[position] = true;
                        }
                        maskIndex++;
                        nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                    }
                    else if (present) {
                        columnReader.getCurrentValueDictionaryID();
                    }
                    columnReader.consume();
                }
            }
            return new ColumnBuffer(allocator.allocateDictionary(ALLOCATION_CONTEXT, ids, dictionaryValues), nulls);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to decode Parquet dictionary", exception);
        }
    }

    private ColumnBuffer readFlatI32Column(ColumnSpec column, int rowCount, ColumnReader columnReader, Mask mask)
    {
        I32Vector values = allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, rowCount, I32Vector::new);
        BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
        int[] outputValues = values.values();
        boolean[] outputNulls = nulls == null ? null : nulls.values();
        int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
        if (mask.all()) {
            if (nulls == null) {
                for (int position = 0; position < rowCount; position++) {
                    outputValues[position] = columnReader.getInteger();
                    columnReader.consume();
                }
            }
            else {
                for (int position = 0; position < rowCount; position++) {
                    if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                        outputValues[position] = columnReader.getInteger();
                    }
                    else {
                        outputNulls[position] = true;
                    }
                    columnReader.consume();
                }
            }
        }
        else if (nulls == null) {
            int maskIndex = 0;
            int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
            for (int position = 0; position < rowCount; position++) {
                if (position == nextMaskPosition) {
                    outputValues[position] = columnReader.getInteger();
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                else {
                    columnReader.getInteger();
                }
                columnReader.consume();
            }
        }
        else {
            int maskIndex = 0;
            int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
            for (int position = 0; position < rowCount; position++) {
                boolean present = columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel;
                if (position == nextMaskPosition) {
                    if (present) {
                        outputValues[position] = columnReader.getInteger();
                    }
                    else {
                        outputNulls[position] = true;
                    }
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                else if (present) {
                    columnReader.getInteger();
                }
                columnReader.consume();
            }
        }
        return new ColumnBuffer(values, nulls);
    }

    private static List<ColumnPages> captureMapColumnPages(PageReadStore rowGroup, ColumnSpec column)
    {
        return List.of(
                captureColumnPages(rowGroup, column.mapSpec().key().descriptor()),
                captureColumnPages(rowGroup, column.mapSpec().value().descriptor()));
    }

    private static List<ColumnPages> captureStructColumnPages(PageReadStore rowGroup, ColumnSpec column)
    {
        return column.structFields().stream()
                .map(field -> captureColumnPages(rowGroup, field.descriptor()))
                .toList();
    }

    private ColumnBuffer readI64Column(ColumnSpec column, ColumnPages columnPages, int startRow, int rowCount, ColumnReader columnReader, Mask mask)
    {
        skipColumnEntries(columnReader, startRow);
        if (columnPages.dictionaryEncoded()) {
            return readDictionaryI64Column(column, columnPages, rowCount, columnReader, mask);
        }
        return readFlatI64Column(column, rowCount, columnReader, mask);
    }

    private ColumnBuffer readDictionaryI64Column(ColumnSpec column, ColumnPages columnPages, int rowCount, ColumnReader columnReader, Mask mask)
    {
        if (columnPages.dictionaryPage() == null || columnPages.dictionaryPage().getDictionarySize() == 0) {
            return readFlatI64Column(column, rowCount, columnReader, mask);
        }

        try {
            Dictionary dictionary = columnPages.dictionaryPage().getEncoding().initDictionary(column.descriptor(), columnPages.dictionaryPage());
            I64Vector dictionaryValues = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, columnPages.dictionaryPage().getDictionarySize(), I64Vector::new);
            long[] dictionaryEntries = dictionaryValues.values();
            for (int index = 0; index < dictionaryEntries.length; index++) {
                dictionaryEntries[index] = dictionary.decodeToLong(index);
            }

            int[] ids = new int[rowCount];
            BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
            boolean[] outputNulls = nulls == null ? null : nulls.values();
            int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
            if (mask.all()) {
                if (nulls == null) {
                    for (int position = 0; position < rowCount; position++) {
                        ids[position] = columnReader.getCurrentValueDictionaryID();
                        columnReader.consume();
                    }
                }
                else {
                    for (int position = 0; position < rowCount; position++) {
                        if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                            ids[position] = columnReader.getCurrentValueDictionaryID();
                        }
                        else {
                            outputNulls[position] = true;
                        }
                        columnReader.consume();
                    }
                }
            }
            else if (nulls == null) {
                int maskIndex = 0;
                int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
                for (int position = 0; position < rowCount; position++) {
                    if (position == nextMaskPosition) {
                        ids[position] = columnReader.getCurrentValueDictionaryID();
                        maskIndex++;
                        nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                    }
                    else {
                        columnReader.getCurrentValueDictionaryID();
                    }
                    columnReader.consume();
                }
            }
            else {
                int maskIndex = 0;
                int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
                for (int position = 0; position < rowCount; position++) {
                    boolean present = columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel;
                    if (position == nextMaskPosition) {
                        if (present) {
                            ids[position] = columnReader.getCurrentValueDictionaryID();
                        }
                        else {
                            outputNulls[position] = true;
                        }
                        maskIndex++;
                        nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                    }
                    else if (present) {
                        columnReader.getCurrentValueDictionaryID();
                    }
                    columnReader.consume();
                }
            }
            return new ColumnBuffer(allocator.allocateDictionary(ALLOCATION_CONTEXT, ids, dictionaryValues), nulls);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to decode Parquet dictionary", exception);
        }
    }

    private ColumnBuffer readFlatI64Column(ColumnSpec column, int rowCount, ColumnReader columnReader, Mask mask)
    {
        I64Vector values = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, rowCount, I64Vector::new);
        BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
        long[] outputValues = values.values();
        boolean[] outputNulls = nulls == null ? null : nulls.values();
        int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
        if (mask.all()) {
            if (nulls == null) {
                for (int position = 0; position < rowCount; position++) {
                    outputValues[position] = columnReader.getLong();
                    columnReader.consume();
                }
            }
            else {
                for (int position = 0; position < rowCount; position++) {
                    if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                        outputValues[position] = columnReader.getLong();
                    }
                    else {
                        outputNulls[position] = true;
                    }
                    columnReader.consume();
                }
            }
        }
        else if (nulls == null) {
            int maskIndex = 0;
            int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
            for (int position = 0; position < rowCount; position++) {
                if (position == nextMaskPosition) {
                    outputValues[position] = columnReader.getLong();
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                else {
                    columnReader.getLong();
                }
                columnReader.consume();
            }
        }
        else {
            int maskIndex = 0;
            int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
            for (int position = 0; position < rowCount; position++) {
                boolean present = columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel;
                if (position == nextMaskPosition) {
                    if (present) {
                        outputValues[position] = columnReader.getLong();
                    }
                    else {
                        outputNulls[position] = true;
                    }
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                else if (present) {
                    columnReader.getLong();
                }
                columnReader.consume();
            }
        }
        return new ColumnBuffer(values, nulls);
    }

    private ColumnBuffer readBooleanColumn(ColumnSpec column, int startRow, int rowCount, ColumnReader columnReader, Mask mask)
    {
        skipColumnEntries(columnReader, startRow);
        BooleanVector values = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new);
        BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
        boolean[] outputValues = values.values();
        boolean[] outputNulls = nulls == null ? null : nulls.values();
        int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
        if (mask.all()) {
            if (nulls == null) {
                for (int position = 0; position < rowCount; position++) {
                    outputValues[position] = columnReader.getBoolean();
                    columnReader.consume();
                }
            }
            else {
                for (int position = 0; position < rowCount; position++) {
                    if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                        outputValues[position] = columnReader.getBoolean();
                    }
                    else {
                        outputNulls[position] = true;
                    }
                    columnReader.consume();
                }
            }
        }
        else if (nulls == null) {
            int maskIndex = 0;
            int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
            for (int position = 0; position < rowCount; position++) {
                if (position == nextMaskPosition) {
                    outputValues[position] = columnReader.getBoolean();
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                else {
                    columnReader.getBoolean();
                }
                columnReader.consume();
            }
        }
        else {
            int maskIndex = 0;
            int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
            for (int position = 0; position < rowCount; position++) {
                boolean present = columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel;
                if (position == nextMaskPosition) {
                    if (present) {
                        outputValues[position] = columnReader.getBoolean();
                    }
                    else {
                        outputNulls[position] = true;
                    }
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                else if (present) {
                    columnReader.getBoolean();
                }
                columnReader.consume();
            }
        }
        return new ColumnBuffer(values, nulls);
    }

    private ColumnBuffer readBinaryColumn(ColumnSpec column, ColumnPages columnPages, int startRow, int rowCount, Mask mask)
    {
        if (columnPages.dictionaryEncoded()) {
            return readDictionaryBinaryColumn(column, columnPages, startRow, rowCount, mask);
        }
        return readFlatBinaryColumn(column, rowCount, requiredBinaryByteCapacity(column, startRow, rowCount, columnPages, mask), createColumnReader(columnPages, startRow), mask);
    }

    private ColumnBuffer readDictionaryBinaryColumn(ColumnSpec column, ColumnPages columnPages, int startRow, int rowCount, Mask mask)
    {
        if (columnPages.dictionaryPage() == null || columnPages.dictionaryPage().getDictionarySize() == 0) {
            return readFlatBinaryColumn(column, rowCount, requiredBinaryByteCapacity(column, startRow, rowCount, columnPages, mask), createColumnReader(columnPages, startRow), mask);
        }

        try {
            Dictionary dictionary = columnPages.dictionaryPage().getEncoding().initDictionary(column.descriptor(), columnPages.dictionaryPage());
            int dictionaryByteCapacity = 0;
            for (int index = 0; index < columnPages.dictionaryPage().getDictionarySize(); index++) {
                dictionaryByteCapacity += dictionary.decodeToBinary(index).length();
            }
            BinaryVector dictionaryValues = allocator.allocateBinary(ALLOCATION_CONTEXT, columnPages.dictionaryPage().getDictionarySize(), dictionaryByteCapacity);
            for (int index = 0; index < columnPages.dictionaryPage().getDictionarySize(); index++) {
                dictionaryValues.setBytes(index, dictionary.decodeToBinary(index).getBytesUnsafe());
            }
            applyBinaryTraits(dictionaryValues, column.binaryTraits(), dictionaryValues.length());

            ColumnReader columnReader = createColumnReader(columnPages, startRow);
            int[] ids = new int[rowCount];
            BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
            boolean[] outputNulls = nulls == null ? null : nulls.values();
            int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
            if (mask.all()) {
                if (nulls == null) {
                    for (int position = 0; position < rowCount; position++) {
                        ids[position] = columnReader.getCurrentValueDictionaryID();
                        columnReader.consume();
                    }
                }
                else {
                    for (int position = 0; position < rowCount; position++) {
                        if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                            ids[position] = columnReader.getCurrentValueDictionaryID();
                        }
                        else {
                            outputNulls[position] = true;
                        }
                        columnReader.consume();
                    }
                }
            }
            else if (nulls == null) {
                int maskIndex = 0;
                int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
                for (int position = 0; position < rowCount; position++) {
                    if (position == nextMaskPosition) {
                        ids[position] = columnReader.getCurrentValueDictionaryID();
                        maskIndex++;
                        nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                    }
                    else {
                        columnReader.getCurrentValueDictionaryID();
                    }
                    columnReader.consume();
                }
            }
            else {
                int maskIndex = 0;
                int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
                for (int position = 0; position < rowCount; position++) {
                    boolean present = columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel;
                    if (position == nextMaskPosition) {
                        if (present) {
                            ids[position] = columnReader.getCurrentValueDictionaryID();
                        }
                        else {
                            outputNulls[position] = true;
                        }
                        maskIndex++;
                        nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                    }
                    else if (present) {
                        columnReader.getCurrentValueDictionaryID();
                    }
                    columnReader.consume();
                }
            }
            return new ColumnBuffer(allocator.allocateDictionary(ALLOCATION_CONTEXT, ids, dictionaryValues), nulls);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to decode Parquet dictionary", exception);
        }
    }

    private ColumnBuffer readFlatBinaryColumn(ColumnSpec column, int rowCount, int byteCapacity, ColumnReader columnReader, Mask mask)
    {
        BinaryVector values = allocator.allocateBinary(ALLOCATION_CONTEXT, rowCount, byteCapacity);
        BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
        boolean[] outputNulls = nulls == null ? null : nulls.values();
        int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
        if (mask.all()) {
            if (nulls == null) {
                for (int position = 0; position < rowCount; position++) {
                    values.setBytes(position, columnReader.getBinary().getBytesUnsafe());
                    columnReader.consume();
                }
            }
            else {
                for (int position = 0; position < rowCount; position++) {
                    if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                        values.setBytes(position, columnReader.getBinary().getBytesUnsafe());
                    }
                    else {
                        values.setNull(position);
                        outputNulls[position] = true;
                    }
                    columnReader.consume();
                }
            }
        }
        else if (nulls == null) {
            int maskIndex = 0;
            int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
            for (int position = 0; position < rowCount; position++) {
                if (position == nextMaskPosition) {
                    values.setBytes(position, columnReader.getBinary().getBytesUnsafe());
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                else {
                    columnReader.getBinary();
                    values.setNull(position);
                }
                columnReader.consume();
            }
        }
        else {
            int maskIndex = 0;
            int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
            for (int position = 0; position < rowCount; position++) {
                boolean present = columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel;
                if (position == nextMaskPosition) {
                    if (present) {
                        values.setBytes(position, columnReader.getBinary().getBytesUnsafe());
                    }
                    else {
                        values.setNull(position);
                        outputNulls[position] = true;
                    }
                    maskIndex++;
                    nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
                }
                else {
                    if (present) {
                        columnReader.getBinary();
                    }
                    values.setNull(position);
                }
                columnReader.consume();
            }
        }
        applyBinaryTraits(values, column.binaryTraits(), rowCount);
        return new ColumnBuffer(values, nulls);
    }

    private static void skipRecords(RecordReader<Group> recordReader, int count)
    {
        for (int skipped = 0; skipped < count; skipped++) {
            recordReader.read();
        }
    }

    private static void skipColumnEntries(ColumnReader columnReader, int count)
    {
        for (int skipped = 0; skipped < count; skipped++) {
            columnReader.consume();
        }
    }

    private static Set<BinaryVector.Trait> binaryTraits(PrimitiveType primitiveType)
    {
        if (primitiveType.getPrimitiveTypeName() != BINARY) {
            return Set.of();
        }
        if (stringType().equals(primitiveType.getLogicalTypeAnnotation())) {
            return Set.of(BinaryVector.Trait.UTF8_STRING);
        }
        return Set.of();
    }

    private static void applyBinaryTraits(BinaryVector values, Set<BinaryVector.Trait> declaredTraits, int positionCount)
    {
        values.addTraits(declaredTraits);
        if (values.hasTrait(BinaryVector.Trait.UTF8_STRING) && isAsciiOnly(values, positionCount)) {
            values.addTrait(BinaryVector.Trait.ASCII_ONLY);
        }
    }

    private static boolean isAsciiOnly(BinaryVector values, int positionCount)
    {
        byte[] data = values.data();
        for (int position = 0; position < positionCount; position++) {
            for (int index = values.startOffset(position); index < values.endOffset(position); index++) {
                if ((data[index] & 0x80) != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    private int requiredBinaryByteCapacity(ColumnSpec column, int startRow, int rowCount, ColumnPages columnPages, Mask mask)
    {
        ColumnReader columnReader = createColumnReader(columnPages, startRow);
        int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
        int byteCapacity = 0;
        if (mask.all()) {
            for (int position = 0; position < rowCount; position++) {
                if (!column.nullable() || columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                    byteCapacity += columnReader.getBinary().length();
                }
                columnReader.consume();
            }
            return byteCapacity;
        }
        int maskIndex = 0;
        int nextMaskPosition = mask.count() == 0 ? rowCount : mask.position(0);
        for (int position = 0; position < rowCount; position++) {
            if (position == nextMaskPosition) {
                if (!column.nullable() || columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                    byteCapacity += columnReader.getBinary().length();
                }
                maskIndex++;
                nextMaskPosition = maskIndex < mask.count() ? mask.position(maskIndex) : rowCount;
            }
            else if (!column.nullable() || columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                columnReader.getBinary();
            }
            columnReader.consume();
        }
        return byteCapacity;
    }

    private ColumnReader createColumnReader(ColumnPages columnPages)
    {
        return createColumnReader(columnPages, 0);
    }

    private ColumnReader createColumnReader(ColumnPages columnPages, int startRow)
    {
        ColumnReader columnReader = new ColumnReadStoreImpl(
                new ReplayPageReadStore(columnPages.totalValueCount(), List.of(columnPages)),
                recordConverter,
                schema,
                createdBy)
                .getColumnReader(columnPages.descriptor());
        skipColumnEntries(columnReader, startRow);
        return columnReader;
    }

    private static ColumnPages captureColumnPages(PageReadStore rowGroup, ColumnDescriptor descriptor)
    {
        PageReader pageReader = rowGroup.getPageReader(descriptor);
        DictionaryPage dictionaryPage = copyDictionaryPage(pageReader.readDictionaryPage());
        List<DataPage> dataPages = new ArrayList<>();
        boolean dictionaryEncoded = dictionaryPage != null;
        for (DataPage dataPage = pageReader.readPage(); dataPage != null; dataPage = pageReader.readPage()) {
            dictionaryEncoded &= isDictionaryEncoded(dataPage);
            dataPages.add(copyDataPage(dataPage));
        }
        return new ColumnPages(descriptor, dictionaryPage, dataPages, pageReader.getTotalValueCount(), dictionaryEncoded);
    }

    private static DictionaryPage copyDictionaryPage(DictionaryPage dictionaryPage)
    {
        if (dictionaryPage == null) {
            return null;
        }
        try {
            return dictionaryPage.copy();
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to copy Parquet dictionary page", exception);
        }
    }

    private static DataPage copyDataPage(DataPage dataPage)
    {
        return dataPage.accept(new DataPage.Visitor<>()
        {
            @Override
            public DataPage visit(DataPageV1 dataPageV1)
            {
                try {
                    BytesInput bytes = BytesInput.copy(dataPageV1.getBytes());
                    if (dataPageV1.getFirstRowIndex().isPresent() && dataPageV1.getIndexRowCount().isPresent()) {
                        return new DataPageV1(
                                bytes,
                                dataPageV1.getValueCount(),
                                dataPageV1.getUncompressedSize(),
                                dataPageV1.getFirstRowIndex().orElseThrow(),
                                dataPageV1.getIndexRowCount().orElseThrow(),
                                dataPageV1.getStatistics(),
                                dataPageV1.getRlEncoding(),
                                dataPageV1.getDlEncoding(),
                                dataPageV1.getValueEncoding());
                    }
                    return new DataPageV1(
                            bytes,
                            dataPageV1.getValueCount(),
                            dataPageV1.getUncompressedSize(),
                            dataPageV1.getStatistics(),
                            dataPageV1.getRlEncoding(),
                            dataPageV1.getDlEncoding(),
                            dataPageV1.getValueEncoding());
                }
                catch (IOException exception) {
                    throw new UncheckedIOException("Unable to copy Parquet data page", exception);
                }
            }

            @Override
            public DataPage visit(DataPageV2 dataPageV2)
            {
                try {
                    BytesInput repetitionLevels = BytesInput.copy(dataPageV2.getRepetitionLevels());
                    BytesInput definitionLevels = BytesInput.copy(dataPageV2.getDefinitionLevels());
                    BytesInput data = BytesInput.copy(dataPageV2.getData());
                    if (dataPageV2.isCompressed()) {
                        return DataPageV2.compressed(
                                dataPageV2.getRowCount(),
                                dataPageV2.getNullCount(),
                                dataPageV2.getValueCount(),
                                repetitionLevels,
                                definitionLevels,
                                dataPageV2.getDataEncoding(),
                                data,
                                dataPageV2.getUncompressedSize(),
                                dataPageV2.getStatistics());
                    }
                    if (dataPageV2.getFirstRowIndex().isPresent()) {
                        return DataPageV2.uncompressed(
                                dataPageV2.getRowCount(),
                                dataPageV2.getNullCount(),
                                dataPageV2.getValueCount(),
                                dataPageV2.getFirstRowIndex().orElseThrow(),
                                repetitionLevels,
                                definitionLevels,
                                dataPageV2.getDataEncoding(),
                                data,
                                dataPageV2.getStatistics());
                    }
                    return DataPageV2.uncompressed(
                            dataPageV2.getRowCount(),
                            dataPageV2.getNullCount(),
                            dataPageV2.getValueCount(),
                            repetitionLevels,
                            definitionLevels,
                            dataPageV2.getDataEncoding(),
                            data,
                            dataPageV2.getStatistics());
                }
                catch (IOException exception) {
                    throw new UncheckedIOException("Unable to copy Parquet data page", exception);
                }
            }
        });
    }

    private static boolean isDictionaryEncoded(DataPage page)
    {
        return page.accept(new DataPage.Visitor<>() {
            @Override
            public Boolean visit(org.apache.parquet.column.page.DataPageV1 dataPageV1)
            {
                return dataPageV1.getValueEncoding().usesDictionary();
            }

            @Override
            public Boolean visit(org.apache.parquet.column.page.DataPageV2 dataPageV2)
            {
                return dataPageV2.getDataEncoding().usesDictionary();
            }
        });
    }

    private enum ColumnKind
    {
        I32,
        I64,
        BOOLEAN,
        BINARY,
        ARRAY_I64,
        MAP,
        STRUCT,
    }

    private record ColumnSpec(String name, ColumnKind kind, boolean nullable, ColumnDescriptor descriptor, Set<BinaryVector.Trait> binaryTraits, Type projectedType, GroupType structType, String elementName, boolean elementNullable, List<StructFieldSpec> structFields, MapSpec mapSpec)
    {
        private ColumnSpec
        {
            binaryTraits = binaryTraits.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(binaryTraits));
            structFields = List.copyOf(structFields);
        }
    }

    private record MapSpec(String entriesName, MapComponentSpec key, MapComponentSpec value) {}

    private record MapComponentSpec(String name, ColumnKind kind, boolean nullable, ColumnDescriptor descriptor, Set<BinaryVector.Trait> binaryTraits)
    {
        private MapComponentSpec
        {
            binaryTraits = binaryTraits.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(binaryTraits));
        }
    }

    private record StructFieldSpec(String name, ColumnKind kind, boolean nullable, ColumnDescriptor descriptor, Set<BinaryVector.Trait> binaryTraits)
    {
        private StructFieldSpec
        {
            binaryTraits = binaryTraits.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(binaryTraits));
        }
    }

    private record ColumnBuffer(Vector values, BooleanVector nulls) {}

    private record ColumnPages(ColumnDescriptor descriptor, DictionaryPage dictionaryPage, List<DataPage> dataPages, long totalValueCount, boolean dictionaryEncoded) {}

    private static final class NoOpGroupConverter
            extends GroupConverter
    {
        private final Converter[] converters;

        private NoOpGroupConverter(GroupType type)
        {
            converters = new Converter[type.getFieldCount()];
            for (int fieldIndex = 0; fieldIndex < type.getFieldCount(); fieldIndex++) {
                Type field = type.getType(fieldIndex);
                converters[fieldIndex] = field.isPrimitive() ? new DictionaryAwarePrimitiveConverter() : new NoOpGroupConverter(field.asGroupType());
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return converters[fieldIndex];
        }

        @Override
        public void start()
        {
        }

        @Override
        public void end()
        {
        }
    }

    private static final class DictionaryAwarePrimitiveConverter
            extends PrimitiveConverter
    {
        @Override
        public boolean hasDictionarySupport()
        {
            return true;
        }

        @Override
        public void setDictionary(Dictionary dictionary)
        {
        }

        @Override
        public void addValueFromDictionary(int dictionaryId)
        {
        }
    }

    private static final class ReplayPageReadStore
            implements PageReadStore
    {
        private final long rowCount;
        private final Map<ColumnDescriptor, ColumnPages> columns = new HashMap<>();

        private ReplayPageReadStore(long rowCount, ColumnPages[] columns)
        {
            this.rowCount = rowCount;
            for (ColumnPages column : columns) {
                this.columns.put(column.descriptor(), column);
            }
        }

        private ReplayPageReadStore(long rowCount, List<ColumnPages> columns)
        {
            this.rowCount = rowCount;
            for (ColumnPages column : columns) {
                this.columns.put(column.descriptor(), column);
            }
        }

        @Override
        public PageReader getPageReader(ColumnDescriptor descriptor)
        {
            ColumnPages column = columns.get(descriptor);
            if (column == null) {
                throw new IllegalArgumentException("Unknown Parquet column: " + descriptor);
            }
            return new ReplayPageReader(column);
        }

        @Override
        public long getRowCount()
        {
            return rowCount;
        }

        @Override
        public Optional<Long> getRowIndexOffset()
        {
            return Optional.empty();
        }

        @Override
        public Optional<PrimitiveIterator.OfLong> getRowIndexes()
        {
            return Optional.empty();
        }
    }

    private static final class ReplayPageReader
            implements PageReader
    {
        private final ColumnPages column;
        private boolean dictionaryRead;
        private int pageIndex;

        private ReplayPageReader(ColumnPages column)
        {
            this.column = column;
        }

        @Override
        public DictionaryPage readDictionaryPage()
        {
            if (dictionaryRead || column.dictionaryPage() == null) {
                return null;
            }
            dictionaryRead = true;
            return column.dictionaryPage();
        }

        @Override
        public long getTotalValueCount()
        {
            return column.totalValueCount();
        }

        @Override
        public DataPage readPage()
        {
            if (pageIndex >= column.dataPages().size()) {
                return null;
            }
            return column.dataPages().get(pageIndex++);
        }
    }
}
