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
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
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
        ColumnReadStoreImpl columnReadStore = new ColumnReadStoreImpl(rowGroup, recordConverter, schema, createdBy);
        ColumnBuffer[] buffers = new ColumnBuffer[columns.size()];
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            ColumnSpec column = columns.get(columnIndex);
            ColumnReader columnReader = columnReadStore.getColumnReader(column.descriptor());
            buffers[columnIndex] = switch (column.kind()) {
                case I64 -> readI64Column(column, rowCount, columnReader);
                case BOOLEAN -> readBooleanColumn(column, rowCount, columnReader);
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
        checkArgument(primitiveType.getRepetition() != Type.Repetition.REPEATED, "Repeated Parquet columns are not supported: %s", name);

        return new ColumnSpec(
                switch (primitiveType.getPrimitiveTypeName()) {
                    case INT64 -> ColumnKind.I64;
                    case BOOLEAN -> ColumnKind.BOOLEAN;
                    default -> throw new IllegalArgumentException("Unsupported Parquet primitive type for column %s: %s".formatted(name, primitiveType.getPrimitiveTypeName()));
                },
                primitiveType.getRepetition() != REQUIRED,
                schema.getColumnDescription(new String[] {name}));
    }

    private ColumnBuffer readI64Column(ColumnSpec column, int rowCount, ColumnReader columnReader)
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

    private enum ColumnKind
    {
        I64,
        BOOLEAN,
    }

    private record ColumnSpec(ColumnKind kind, boolean nullable, ColumnDescriptor descriptor) {}

    private record ColumnBuffer(Vector values, BooleanVector nulls) {}

    private static final class NoOpGroupConverter
            extends GroupConverter
    {
        private final Converter[] converters;

        private NoOpGroupConverter(GroupType type)
        {
            converters = new Converter[type.getFieldCount()];
            for (int fieldIndex = 0; fieldIndex < type.getFieldCount(); fieldIndex++) {
                Type field = type.getType(fieldIndex);
                converters[fieldIndex] = field.isPrimitive() ? new PrimitiveConverter() {} : new NoOpGroupConverter(field.asGroupType());
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
}
