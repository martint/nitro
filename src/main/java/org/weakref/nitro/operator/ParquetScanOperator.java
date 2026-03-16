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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DataPage;
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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
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
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public final class ParquetScanOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ParquetScanOperator");

    private final Allocator allocator;
    private final ParquetFileReader reader;
    private final MessageType schema;
    private final String createdBy;
    private final GroupConverter recordConverter;
    private final List<ColumnSpec> columns;

    private PageReadStore nextRowGroup;

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
        return nextRowGroup != null;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more Parquet row groups");
        }

        PageReadStore rowGroup = nextRowGroup;
        loadNextRowGroup();

        int rowCount = toIntExact(rowGroup.getRowCount());
        ColumnPages[] columnPages = columns.stream()
                .map(column -> captureColumnPages(rowGroup, column.descriptor()))
                .toArray(ColumnPages[]::new);
        ColumnReadStoreImpl columnReadStore = new ColumnReadStoreImpl(new ReplayPageReadStore(rowCount, columnPages), recordConverter, schema, createdBy);
        ColumnBuffer[] buffers = new ColumnBuffer[columns.size()];
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            ColumnSpec column = columns.get(columnIndex);
            buffers[columnIndex] = switch (column.kind()) {
                case I64 -> readI64Column(column, columnPages[columnIndex], rowCount, columnReadStore.getColumnReader(column.descriptor()));
                case BOOLEAN -> readBooleanColumn(column, rowCount, columnReadStore.getColumnReader(column.descriptor()));
                case BINARY -> readBinaryColumn(column, columnPages[columnIndex], rowCount);
                case ARRAY_I64 -> readArrayI64Column(column, columnPages[columnIndex], rowCount);
            };
        }

        Output[] outputs = new Output[columns.size()];
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            ColumnBuffer buffer = buffers[columnIndex];
            Streams streams = Streams.of(Stream.VALUES, buffer.values());
            if (buffer.nulls() != null) {
                streams = streams.with(Stream.NULLS, buffer.nulls());
            }
            outputs[columnIndex] = new Output(streams.asMap().keySet(), streams::get, (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
        }
        return new Batch(allocator.allocateAllMask(ALLOCATION_CONTEXT, rowCount), takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask), outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
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

    private void loadNextRowGroup()
    {
        try {
            nextRowGroup = reader.readNextRowGroup();
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to read next Parquet row group", exception);
        }
    }

    private ColumnSpec resolveColumn(String name)
    {
        checkArgument(schema.containsField(name), "Unknown Parquet column: %s", name);
        Type type = schema.getType(name);
        checkArgument(type.isPrimitive(), "Only primitive top-level Parquet columns are supported: %s", name);
        PrimitiveType primitiveType = type.asPrimitiveType();

        return new ColumnSpec(
                switch (primitiveType.getRepetition()) {
                    case REPEATED -> switch (primitiveType.getPrimitiveTypeName()) {
                        case INT64 -> ColumnKind.ARRAY_I64;
                        default -> throw new IllegalArgumentException("Unsupported repeated Parquet primitive type for column %s: %s".formatted(name, primitiveType.getPrimitiveTypeName()));
                    };
                    case OPTIONAL, REQUIRED -> switch (primitiveType.getPrimitiveTypeName()) {
                        case INT64 -> ColumnKind.I64;
                        case BOOLEAN -> ColumnKind.BOOLEAN;
                        case BINARY -> ColumnKind.BINARY;
                        default -> throw new IllegalArgumentException("Unsupported Parquet primitive type for column %s: %s".formatted(name, primitiveType.getPrimitiveTypeName()));
                    };
                },
                primitiveType.getRepetition() != REQUIRED && primitiveType.getRepetition() != Type.Repetition.REPEATED,
                schema.getColumnDescription(new String[] {name}),
                binaryTraits(primitiveType));
    }

    private ColumnBuffer readArrayI64Column(ColumnSpec column, ColumnPages columnPages, int rowCount)
    {
        ArrayVector values = allocator.allocateArray(ALLOCATION_CONTEXT, rowCount);
        String columnName = column.descriptor().getPath()[0];
        MessageType projectedSchema = new MessageType(schema.getName(), schema.getType(columnName));
        MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(projectedSchema);
        RecordReader<Group> recordReader = columnIo.getRecordReader(
                new ReplayPageReadStore(rowCount, List.of(columnPages)),
                new GroupRecordConverter(projectedSchema));
        int[] offsets = values.offsets();

        int elementCount = 0;
        for (int position = 0; position < rowCount; position++) {
            Group row = recordReader.read();
            int valueCount = row.getFieldRepetitionCount(columnName);
            elementCount += valueCount;
            offsets[position + 1] = elementCount;
        }

        I64Vector elementValues = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, elementCount, I64Vector::new);
        recordReader = columnIo.getRecordReader(
                new ReplayPageReadStore(rowCount, List.of(columnPages)),
                new GroupRecordConverter(projectedSchema));
        int elementIndex = 0;
        for (int position = 0; position < rowCount; position++) {
            Group row = recordReader.read();
            int valueCount = row.getFieldRepetitionCount(columnName);
            for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                elementValues.values()[elementIndex++] = row.getLong(columnName, valueIndex);
            }
        }
        values.setElements(Streams.ofValues(elementValues));
        return new ColumnBuffer(values, null);
    }

    private ColumnBuffer readI64Column(ColumnSpec column, ColumnPages columnPages, int rowCount, ColumnReader columnReader)
    {
        if (columnPages.dictionaryEncoded()) {
            return readDictionaryI64Column(column, columnPages, rowCount, columnReader);
        }
        return readFlatI64Column(column, rowCount, columnReader);
    }

    private ColumnBuffer readDictionaryI64Column(ColumnSpec column, ColumnPages columnPages, int rowCount, ColumnReader columnReader)
    {
        if (columnPages.dictionaryPage() == null || columnPages.dictionaryPage().getDictionarySize() == 0) {
            return readFlatI64Column(column, rowCount, columnReader);
        }

        try {
            Dictionary dictionary = columnPages.dictionaryPage().getEncoding().initDictionary(column.descriptor(), columnPages.dictionaryPage().copy());
            I64Vector dictionaryValues = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, columnPages.dictionaryPage().getDictionarySize(), I64Vector::new);
            long[] dictionaryEntries = dictionaryValues.values();
            for (int index = 0; index < dictionaryEntries.length; index++) {
                dictionaryEntries[index] = dictionary.decodeToLong(index);
            }

            int[] ids = new int[rowCount];
            BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
            boolean[] outputNulls = nulls == null ? null : nulls.values();
            int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
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
            return new ColumnBuffer(new DictionaryVector(ids, dictionaryValues), nulls);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to decode Parquet dictionary", exception);
        }
    }

    private ColumnBuffer readFlatI64Column(ColumnSpec column, int rowCount, ColumnReader columnReader)
    {
        I64Vector values = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, rowCount, I64Vector::new);
        BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
        long[] outputValues = values.values();
        boolean[] outputNulls = nulls == null ? null : nulls.values();
        int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
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
        return new ColumnBuffer(values, nulls);
    }

    private ColumnBuffer readBooleanColumn(ColumnSpec column, int rowCount, ColumnReader columnReader)
    {
        BooleanVector values = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new);
        BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
        boolean[] outputValues = values.values();
        boolean[] outputNulls = nulls == null ? null : nulls.values();
        int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
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
        return new ColumnBuffer(values, nulls);
    }

    private ColumnBuffer readBinaryColumn(ColumnSpec column, ColumnPages columnPages, int rowCount)
    {
        if (columnPages.dictionaryEncoded()) {
            return readDictionaryBinaryColumn(column, columnPages, rowCount);
        }
        return readFlatBinaryColumn(column, rowCount, requiredBinaryByteCapacity(column, rowCount, columnPages), createColumnReader(columnPages));
    }

    private ColumnBuffer readDictionaryBinaryColumn(ColumnSpec column, ColumnPages columnPages, int rowCount)
    {
        if (columnPages.dictionaryPage() == null || columnPages.dictionaryPage().getDictionarySize() == 0) {
            return readFlatBinaryColumn(column, rowCount, requiredBinaryByteCapacity(column, rowCount, columnPages), createColumnReader(columnPages));
        }

        try {
            Dictionary dictionary = columnPages.dictionaryPage().getEncoding().initDictionary(column.descriptor(), columnPages.dictionaryPage().copy());
            int dictionaryByteCapacity = 0;
            for (int index = 0; index < columnPages.dictionaryPage().getDictionarySize(); index++) {
                dictionaryByteCapacity += dictionary.decodeToBinary(index).length();
            }
            BinaryVector dictionaryValues = allocator.allocateBinary(ALLOCATION_CONTEXT, columnPages.dictionaryPage().getDictionarySize(), dictionaryByteCapacity);
            for (int index = 0; index < columnPages.dictionaryPage().getDictionarySize(); index++) {
                dictionaryValues.setBytes(index, dictionary.decodeToBinary(index).getBytesUnsafe());
            }
            applyBinaryTraits(dictionaryValues, column.binaryTraits(), dictionaryValues.length());

            ColumnReader columnReader = createColumnReader(columnPages);
            int[] ids = new int[rowCount];
            BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
            boolean[] outputNulls = nulls == null ? null : nulls.values();
            int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
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
            return new ColumnBuffer(new DictionaryVector(ids, dictionaryValues), nulls);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to decode Parquet dictionary", exception);
        }
    }

    private ColumnBuffer readFlatBinaryColumn(ColumnSpec column, int rowCount, int byteCapacity, ColumnReader columnReader)
    {
        BinaryVector values = allocator.allocateBinary(ALLOCATION_CONTEXT, rowCount, byteCapacity);
        BooleanVector nulls = column.nullable() ? allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rowCount, BooleanVector::new) : null;
        boolean[] outputNulls = nulls == null ? null : nulls.values();
        int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
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
        applyBinaryTraits(values, column.binaryTraits(), rowCount);
        return new ColumnBuffer(values, nulls);
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

    private int requiredBinaryByteCapacity(ColumnSpec column, int rowCount, ColumnPages columnPages)
    {
        ColumnReader columnReader = createColumnReader(columnPages);
        int maxDefinitionLevel = column.descriptor().getMaxDefinitionLevel();
        int byteCapacity = 0;
        for (int position = 0; position < rowCount; position++) {
            if (!column.nullable() || columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                byteCapacity += columnReader.getBinary().length();
            }
            columnReader.consume();
        }
        return byteCapacity;
    }

    private ColumnReader createColumnReader(ColumnPages columnPages)
    {
        return new ColumnReadStoreImpl(
                new ReplayPageReadStore(columnPages.totalValueCount(), List.of(columnPages)),
                recordConverter,
                schema,
                createdBy)
                .getColumnReader(columnPages.descriptor());
    }

    private static ColumnPages captureColumnPages(PageReadStore rowGroup, ColumnDescriptor descriptor)
    {
        PageReader pageReader = rowGroup.getPageReader(descriptor);
        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        List<DataPage> dataPages = new ArrayList<>();
        boolean dictionaryEncoded = dictionaryPage != null;
        for (DataPage dataPage = pageReader.readPage(); dataPage != null; dataPage = pageReader.readPage()) {
            dictionaryEncoded &= isDictionaryEncoded(dataPage);
            dataPages.add(dataPage);
        }
        return new ColumnPages(descriptor, dictionaryPage, dataPages, pageReader.getTotalValueCount(), dictionaryEncoded);
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
        I64,
        BOOLEAN,
        BINARY,
        ARRAY_I64,
    }

    private record ColumnSpec(ColumnKind kind, boolean nullable, ColumnDescriptor descriptor, Set<BinaryVector.Trait> binaryTraits)
    {
        private ColumnSpec
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
