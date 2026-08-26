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
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;

/**
 * Reconstructs a standard three-level Parquet MAP from two synchronized physical-leaf event streams.
 * Physical type decoding and Nitro child-vector construction are delegated to schema-selected components.
 */
final class NestedMapReader
        implements AutoCloseable
{
    private final ParquetSchema.Group map;
    private final ParquetSchema.Group entries;
    private final ParquetSchema.Primitive key;
    private final ParquetSchema.Primitive value;
    private final NestedLeafCursor keyReader;
    private final NestedLeafCursor valueReader;
    private final NestedValueAccumulator keyValues;
    private final NestedValueAccumulator values;

    private boolean positioned;
    private boolean exhausted;

    NestedMapReader(ParquetSchema.Group map, RleReaderPolicy rlePolicy)
    {
        this(map, rlePolicy, null, null);
    }

    NestedMapReader(
            ParquetSchema.Group map,
            RleReaderPolicy rlePolicy,
            NestedLeafCursor keyCursor,
            NestedLeafCursor valueCursor)
    {
        this.map = requireNonNull(map, "map is null");
        if (!map.isMap()) {
            throw unsupported("field is not annotated as MAP");
        }
        if (map.children().size() != 1 || !(map.children().getFirst() instanceof ParquetSchema.Group entryGroup)) {
            throw unsupported("MAP must contain one repeated key-value group");
        }
        this.entries = entryGroup;
        if (entries.repetition() != FieldRepetitionType.REPEATED || entries.children().size() != 2) {
            throw unsupported("MAP key-value group must be repeated and contain exactly two fields");
        }
        if (!(entries.children().get(0) instanceof ParquetSchema.Primitive keyLeaf) ||
                !(entries.children().get(1) instanceof ParquetSchema.Primitive valueLeaf)) {
            throw unsupported("nested MAP keys or values are not implemented yet");
        }
        this.key = keyLeaf;
        this.value = valueLeaf;
        if (key.repetition() != FieldRepetitionType.REQUIRED) {
            throw unsupported("MAP key must be required");
        }
        if (value.repetition() == FieldRepetitionType.REPEATED) {
            throw unsupported("MAP value cannot be repeated");
        }
        this.keyReader = keyCursor == null ? new NestedLeafReader(key, rlePolicy) : keyCursor;
        this.valueReader = valueCursor == null ? new NestedLeafReader(value, rlePolicy) : valueCursor;
        this.keyValues = NestedValueAccumulators.create(key);
        this.values = NestedValueAccumulators.create(value);
    }

    void addRowGroup(ParquetFile file, RowGroup rowGroup)
    {
        requireNonNull(file, "file is null");
        requireNonNull(rowGroup, "rowGroup is null");
        if (!(keyReader instanceof NestedLeafReader physicalKeyReader) ||
                !(valueReader instanceof NestedLeafReader physicalValueReader)) {
            throw new IllegalStateException("Cannot add Parquet row groups to injected nested cursors");
        }
        physicalKeyReader.addChunk(file, file.columnChunk(rowGroup, key).meta_data);
        physicalValueReader.addChunk(file, file.columnChunk(rowGroup, value).meta_data);
    }

    Streams read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
    {
        requireNonNull(allocator, "allocator is null");
        requireNonNull(context, "context is null");
        requireNonNull(mask, "mask is null");
        if (rowCount < 0 || mask.size() != rowCount) {
            throw new IllegalArgumentException("Nested MAP row count and mask length differ: " + rowCount + " != " + mask.size());
        }

        keyValues.reset();
        values.reset();
        MapVector maps = allocator.allocateMap(context, rowCount);
        BooleanVector mapNulls = map.repetition() == FieldRepetitionType.OPTIONAL
                ? allocator.allocate(context, BooleanVector.class, rowCount, BooleanVector::new)
                : null;

        int selectedIndex = 0;
        int nextSelected = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        ensurePositioned();
        for (int row = 0; row < rowCount; row++) {
            if (exhausted) {
                throw new IllegalArgumentException("Nested MAP event stream ended before row " + row);
            }
            requireAlignedEvents();
            if (keyReader.repetitionLevel() != 0) {
                throw new IllegalArgumentException("Nested MAP row starts with nonzero repetition level");
            }

            boolean selected = mask.all() || row == nextSelected;
            boolean mapIsNull = keyReader.definitionLevel() < map.maximumDefinitionLevel();
            if (selected && mapNulls != null) {
                mapNulls.values()[row] = mapIsNull;
            }

            do {
                requireAlignedEvents();
                boolean hasEntry = keyReader.hasValue();
                if (selected && hasEntry) {
                    append(keyReader, keyValues);
                    if (valueReader.hasValue()) {
                        append(valueReader, values);
                    }
                    else {
                        values.appendNull();
                    }
                }
                advanceTogether();
            }
            while (!exhausted && keyReader.repetitionLevel() != 0);

            maps.offsets()[row + 1] = keyValues.size();
            if (selected && !mask.all()) {
                selectedIndex++;
                nextSelected = selectedIndex < mask.count() ? mask.position(selectedIndex) : rowCount;
            }
        }

        if (keyValues.size() != values.size()) {
            throw new IllegalArgumentException("Nested MAP key/value cardinalities differ");
        }
        maps.setEntries(keyValues.materialize(allocator, context), values.materialize(allocator, context));
        return mapNulls == null ? Streams.ofValues(maps) : Streams.ofValuesAndNulls(maps, mapNulls);
    }

    void skip(long rowCount)
    {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount is negative");
        }
        ensurePositioned();
        for (long row = 0; row < rowCount; row++) {
            if (exhausted) {
                throw new IllegalArgumentException("Nested MAP event stream ended before skipped row " + row);
            }
            requireAlignedEvents();
            if (keyReader.repetitionLevel() != 0) {
                throw new IllegalArgumentException("Nested MAP row starts with nonzero repetition level");
            }
            do {
                requireAlignedEvents();
                advanceTogether();
            }
            while (!exhausted && keyReader.repetitionLevel() != 0);
        }
    }

    private void ensurePositioned()
    {
        if (!positioned) {
            positioned = true;
            advanceTogether();
        }
    }

    private void advanceTogether()
    {
        boolean keyAvailable = keyReader.next();
        boolean valueAvailable = valueReader.next();
        if (keyAvailable != valueAvailable) {
            throw new IllegalArgumentException("Nested MAP key/value event streams have different lengths");
        }
        exhausted = !keyAvailable;
    }

    private void requireAlignedEvents()
    {
        if (keyReader.repetitionLevel() != valueReader.repetitionLevel()) {
            throw new IllegalArgumentException("Nested MAP key/value repetition levels differ");
        }
        boolean keyEntry = keyReader.definitionLevel() >= entries.maximumDefinitionLevel();
        boolean valueEntry = valueReader.definitionLevel() >= entries.maximumDefinitionLevel();
        if (keyEntry != valueEntry) {
            throw new IllegalArgumentException("Nested MAP key/value definition levels describe different entries");
        }
    }

    private static void append(NestedLeafCursor reader, NestedValueAccumulator accumulator)
    {
        accumulator.append(reader.valueDecoder(), reader.valueOrdinal(), reader.dictionaryId());
    }

    private UnsupportedParquetFeatureException unsupported(String reason)
    {
        return new UnsupportedParquetFeatureException(
                "Unsupported native Parquet MAP layout at '" + map.name() + "': " + reason);
    }

    @Override
    public void close()
    {
        try (keyReader; valueReader) {
            // Closing the resources in the try header releases both leaf ranges and scratch arenas.
        }
    }
}
