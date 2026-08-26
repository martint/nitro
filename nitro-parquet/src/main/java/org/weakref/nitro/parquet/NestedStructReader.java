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

import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.RowGroup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Reconstructs a non-repeated Parquet group from synchronized primitive-leaf event streams. */
final class NestedStructReader
        implements AutoCloseable
{
    private final ParquetSchema.Group struct;
    private final List<ParquetSchema.Primitive> fields;
    private final NestedLeafCursor[] readers;
    private final NestedValueAccumulator[] values;
    private boolean exhausted;

    NestedStructReader(ParquetSchema.Group struct, RleReaderPolicy rlePolicy)
    {
        this(struct, rlePolicy, null);
    }

    NestedStructReader(ParquetSchema.Group struct, RleReaderPolicy rlePolicy, NestedLeafCursor[] cursors)
    {
        this.struct = requireNonNull(struct, "struct is null");
        if (struct.isMap() || struct.isList() || struct.repetition() == FieldRepetitionType.REPEATED) {
            throw unsupported("field is not a non-repeated struct");
        }
        if (struct.children().stream().anyMatch(child -> !(child instanceof ParquetSchema.Primitive))) {
            throw unsupported("nested struct fields are not implemented yet");
        }
        this.fields = struct.children().stream().map(ParquetSchema.Primitive.class::cast).toList();
        if (fields.isEmpty()) {
            throw unsupported("struct has no fields");
        }
        if (cursors != null && cursors.length != fields.size()) {
            throw new IllegalArgumentException("Injected struct cursor count does not match field count");
        }
        this.readers = new NestedLeafCursor[fields.size()];
        this.values = new NestedValueAccumulator[fields.size()];
        for (int field = 0; field < fields.size(); field++) {
            ParquetSchema.Primitive leaf = fields.get(field);
            readers[field] = cursors == null ? new NestedLeafReader(leaf, rlePolicy) : requireNonNull(cursors[field], "cursor is null");
            values[field] = NestedValueAccumulators.create(leaf, true);
        }
    }

    void addRowGroup(ParquetFile file, RowGroup rowGroup)
    {
        requireNonNull(file, "file is null");
        requireNonNull(rowGroup, "rowGroup is null");
        for (int field = 0; field < readers.length; field++) {
            if (!(readers[field] instanceof NestedLeafReader physicalReader)) {
                throw new IllegalStateException("Cannot add Parquet row groups to injected nested cursors");
            }
            physicalReader.addChunk(file, file.columnChunk(rowGroup, fields.get(field)).meta_data);
        }
    }

    Streams read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
    {
        requireNonNull(allocator, "allocator is null");
        requireNonNull(context, "context is null");
        requireNonNull(mask, "mask is null");
        if (rowCount < 0 || mask.size() != rowCount) {
            throw new IllegalArgumentException("Nested struct row count and mask length differ: " + rowCount + " != " + mask.size());
        }
        for (NestedValueAccumulator accumulator : values) {
            accumulator.reset();
        }
        StructVector result = allocator.allocate(context, StructVector.class, rowCount, StructVector::new);
        BooleanVector structNulls = struct.repetition() == FieldRepetitionType.OPTIONAL
                ? allocator.allocate(context, BooleanVector.class, rowCount, BooleanVector::new)
                : null;

        int selectedIndex = 0;
        int nextSelected = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        for (int row = 0; row < rowCount; row++) {
            advanceTogether();
            boolean selected = mask.all() || row == nextSelected;
            boolean structIsNull = readers[0].definitionLevel() < struct.maximumDefinitionLevel();
            if (selected && structNulls != null) {
                structNulls.values()[row] = structIsNull;
            }
            for (int field = 0; field < readers.length; field++) {
                NestedLeafCursor reader = readers[field];
                if (reader.repetitionLevel() != 0) {
                    throw new IllegalArgumentException("Nested struct field starts with nonzero repetition level");
                }
                if ((reader.definitionLevel() < struct.maximumDefinitionLevel()) != structIsNull) {
                    throw new IllegalArgumentException("Nested struct leaves disagree about struct presence");
                }
                if (selected && reader.hasValue()) {
                    values[field].append(reader.valueDecoder(), reader.valueOrdinal(), reader.dictionaryId());
                }
                else {
                    values[field].appendNull();
                }
            }
            if (selected && !mask.all()) {
                selectedIndex++;
                nextSelected = selectedIndex < mask.count() ? mask.position(selectedIndex) : rowCount;
            }
        }
        for (int field = 0; field < fields.size(); field++) {
            result.setField(fields.get(field).name(), values[field].materialize(allocator, context));
        }
        return structNulls == null ? Streams.ofValues(result) : Streams.ofValuesAndNulls(result, structNulls);
    }

    void skip(long rowCount)
    {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount is negative");
        }
        for (long row = 0; row < rowCount; row++) {
            advanceTogether();
        }
    }

    long consumedPageBytes()
    {
        long bytes = 0;
        for (NestedLeafCursor reader : readers) {
            if (reader instanceof NestedLeafReader physicalReader) {
                bytes = Math.addExact(bytes, physicalReader.consumedPageBytes());
            }
        }
        return bytes;
    }

    private void advanceTogether()
    {
        if (exhausted) {
            throw new IllegalArgumentException("Nested struct event streams ended before requested rows");
        }
        boolean available = readers[0].next();
        for (int field = 1; field < readers.length; field++) {
            if (readers[field].next() != available) {
                throw new IllegalArgumentException("Nested struct leaf event streams have different lengths");
            }
        }
        exhausted = !available;
        if (!available) {
            throw new IllegalArgumentException("Nested struct event streams ended before requested rows");
        }
    }

    private UnsupportedParquetFeatureException unsupported(String reason)
    {
        return new UnsupportedParquetFeatureException(
                "Unsupported native Parquet struct layout at '" + struct.name() + "': " + reason);
    }

    @Override
    public void close()
    {
        RuntimeException failure = null;
        for (NestedLeafCursor reader : readers) {
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
        if (failure != null) {
            throw failure;
        }
    }
}
