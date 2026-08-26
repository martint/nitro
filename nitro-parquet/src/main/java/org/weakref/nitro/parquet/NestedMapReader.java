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

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.RowGroup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Reconstructs a standard three-level Parquet MAP from two synchronized physical-leaf event streams.
 * Physical type decoding and Nitro child-vector construction are delegated to schema-selected components.
 */
final class NestedMapReader
        implements AutoCloseable
{
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;

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

    NestedMapReader(ParquetSchema.Group map, RleReaderPolicy rlePolicy, PrimitiveArrayPool arrayPool)
    {
        this(map, rlePolicy, arrayPool, null, null);
    }

    NestedMapReader(
            ParquetSchema.Group map,
            RleReaderPolicy rlePolicy,
            NestedLeafCursor keyCursor,
            NestedLeafCursor valueCursor)
    {
        this(map, rlePolicy, null, keyCursor, valueCursor);
    }

    private NestedMapReader(
            ParquetSchema.Group map,
            RleReaderPolicy rlePolicy,
            PrimitiveArrayPool arrayPool,
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
        this.keyReader = keyCursor == null
                ? new NestedLeafReader(key, rlePolicy, requireNonNull(arrayPool, "arrayPool is null"))
                : keyCursor;
        this.valueReader = valueCursor == null
                ? new NestedLeafReader(value, rlePolicy, requireNonNull(arrayPool, "arrayPool is null"), true, false)
                : valueCursor;
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

        keyValues.reset(allocator);
        values.reset(allocator);
        MapVector maps = allocator.allocateMap(context, rowCount);
        BooleanVector mapNulls = map.repetition() == FieldRepetitionType.OPTIONAL
                ? allocator.allocate(context, BooleanVector.class, rowCount, BooleanVector::new)
                : null;

        if (rowCount == 0) {
            maps.setEntries(keyValues.materialize(allocator, context), values.materialize(allocator, context));
            return mapNulls == null ? Streams.ofValues(maps) : Streams.ofValuesAndNulls(maps, mapNulls);
        }

        if (!positioned &&
                keyReader instanceof NestedLeafEventSource keySource &&
                valueReader instanceof NestedLeafEventSource valueSource) {
            readEventWindows(keySource, valueSource, maps, mapNulls, rowCount, mask);
            maps.setEntries(keyValues.materialize(allocator, context), values.materialize(allocator, context));
            return mapNulls == null ? Streams.ofValues(maps) : Streams.ofValuesAndNulls(maps, mapNulls);
        }

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

    private void readEventWindows(
            NestedLeafEventSource keySource,
            NestedLeafEventSource valueSource,
            MapVector maps,
            BooleanVector mapNulls,
            int rowCount,
            Mask mask)
    {
        if (mask.all()) {
            readAllEventWindows(keySource, valueSource, maps, mapNulls, rowCount);
            return;
        }

        int row = -1;
        int selectedIndex = 0;
        int nextSelected = mask.count() == 0 ? rowCount : mask.position(0);
        boolean selected = false;
        int outputEntryCount = 0;
        int entryDefinitionLevel = entries.maximumDefinitionLevel();
        int mapDefinitionLevel = map.maximumDefinitionLevel();

        while (true) {
            NestedEventWindow keyWindow = keySource.eventWindow();
            NestedEventWindow valueWindow = valueSource.eventWindow();
            if (keyWindow == null || valueWindow == null) {
                if (keyWindow == null && valueWindow == null && row == rowCount - 1) {
                    maps.offsets()[rowCount] = outputEntryCount;
                    return;
                }
                throw new IllegalArgumentException("Nested MAP key/value event streams have different lengths");
            }

            int windowLength = Math.min(keyWindow.length(), valueWindow.length());
            int keyOffset = keyWindow.offset();
            int valueOffset = valueWindow.offset();
            int[] keyRepetitionLevels = keyWindow.repetitionLevels();
            int[] valueRepetitionLevels = valueWindow.repetitionLevels();
            if (valueSource.hasRepetitionLevels() && Arrays.mismatch(
                    keyRepetitionLevels,
                    keyOffset,
                    keyOffset + windowLength,
                    valueRepetitionLevels,
                    valueOffset,
                    valueOffset + windowLength) >= 0) {
                throw new IllegalArgumentException("Nested MAP key/value repetition levels differ");
            }
            int[] keyDefinitionLevels = keyWindow.definitionLevels();
            int[] valueDefinitionLevels = valueWindow.definitionLevels();
            int consumed = 0;
            int entryRunStart = -1;
            int entryRunCount = 0;
            while (consumed < windowLength) {
                int repetitionLevel = keyRepetitionLevels[keyOffset + consumed];
                boolean keyEntry = keyDefinitionLevels[keyOffset + consumed] >= entryDefinitionLevel;
                boolean valueEntry = valueDefinitionLevels[valueOffset + consumed] >= entryDefinitionLevel;
                if (keyEntry != valueEntry) {
                    throw new IllegalArgumentException("Nested MAP key/value definition levels describe different entries");
                }

                if (repetitionLevel == 0) {
                    if (row >= 0) {
                        maps.offsets()[row + 1] = outputEntryCount;
                        if (row + 1 == rowCount) {
                            if (entryRunCount != 0) {
                                appendWindowRun(keyWindow, valueWindow, entryRunStart, entryRunCount);
                            }
                            keySource.advanceEvents(consumed);
                            valueSource.advanceEvents(consumed);
                            return;
                        }
                    }
                    row++;
                    selected = row == nextSelected;
                    if (selected) {
                        selectedIndex++;
                        nextSelected = selectedIndex < mask.count() ? mask.position(selectedIndex) : rowCount;
                    }
                    if (selected && mapNulls != null) {
                        mapNulls.values()[row] = keyDefinitionLevels[keyOffset + consumed] < mapDefinitionLevel;
                    }
                }
                else if (row < 0) {
                    throw new IllegalArgumentException("Nested MAP row starts with nonzero repetition level");
                }

                if (selected && keyEntry) {
                    if (entryRunCount == 0) {
                        entryRunStart = consumed;
                    }
                    entryRunCount++;
                    outputEntryCount++;
                }
                else if (entryRunCount != 0) {
                    appendWindowRun(keyWindow, valueWindow, entryRunStart, entryRunCount);
                    entryRunCount = 0;
                }
                consumed++;
            }
            if (entryRunCount != 0) {
                appendWindowRun(keyWindow, valueWindow, entryRunStart, entryRunCount);
            }
            keySource.advanceEvents(consumed);
            valueSource.advanceEvents(consumed);
        }
    }

    private void readAllEventWindows(
            NestedLeafEventSource keySource,
            NestedLeafEventSource valueSource,
            MapVector maps,
            BooleanVector mapNulls,
            int rowCount)
    {
        int row = -1;
        int outputEntryCount = 0;
        int entryDefinitionLevel = entries.maximumDefinitionLevel();
        int mapDefinitionLevel = map.maximumDefinitionLevel();

        while (true) {
            NestedEventWindow keyWindow = keySource.eventWindow();
            NestedEventWindow valueWindow = valueSource.eventWindow();
            if (keyWindow == null || valueWindow == null) {
                if (keyWindow == null && valueWindow == null && row == rowCount - 1) {
                    maps.offsets()[rowCount] = outputEntryCount;
                    return;
                }
                throw new IllegalArgumentException("Nested MAP key/value event streams have different lengths");
            }

            int windowLength = Math.min(keyWindow.length(), valueWindow.length());
            int keyOffset = keyWindow.offset();
            int valueOffset = valueWindow.offset();
            int[] keyRepetitionLevels = keyWindow.repetitionLevels();
            int[] valueRepetitionLevels = valueWindow.repetitionLevels();
            if (valueSource.hasRepetitionLevels() && Arrays.mismatch(
                    keyRepetitionLevels,
                    keyOffset,
                    keyOffset + windowLength,
                    valueRepetitionLevels,
                    valueOffset,
                    valueOffset + windowLength) >= 0) {
                throw new IllegalArgumentException("Nested MAP key/value repetition levels differ");
            }
            int[] keyDefinitionLevels = keyWindow.definitionLevels();
            int[] valueDefinitionLevels = valueWindow.definitionLevels();
            if (allAtLeast(keyDefinitionLevels, keyOffset, windowLength, entryDefinitionLevel) &&
                    allAtLeast(valueDefinitionLevels, valueOffset, windowLength, entryDefinitionLevel)) {
                int consumed = 0;
                while (consumed < windowLength) {
                    if (keyRepetitionLevels[keyOffset + consumed] == 0) {
                        if (row >= 0) {
                            maps.offsets()[row + 1] = outputEntryCount + consumed;
                            if (row + 1 == rowCount) {
                                keyWindow.appendTo(keyValues, 0, consumed);
                                valueWindow.appendTo(values, 0, consumed);
                                keySource.advanceEvents(consumed);
                                valueSource.advanceEvents(consumed);
                                return;
                            }
                        }
                        row++;
                    }
                    else if (row < 0) {
                        throw new IllegalArgumentException("Nested MAP row starts with nonzero repetition level");
                    }
                    consumed++;
                }
                keyWindow.appendTo(keyValues, 0, consumed);
                valueWindow.appendTo(values, 0, consumed);
                outputEntryCount += consumed;
                keySource.advanceEvents(consumed);
                valueSource.advanceEvents(consumed);
                continue;
            }
            int consumed = 0;
            int entryRunStart = -1;
            int entryRunCount = 0;
            while (consumed < windowLength) {
                int repetitionLevel = keyRepetitionLevels[keyOffset + consumed];
                boolean keyEntry = keyDefinitionLevels[keyOffset + consumed] >= entryDefinitionLevel;
                boolean valueEntry = valueDefinitionLevels[valueOffset + consumed] >= entryDefinitionLevel;
                if (keyEntry != valueEntry) {
                    throw new IllegalArgumentException("Nested MAP key/value definition levels describe different entries");
                }

                if (repetitionLevel == 0) {
                    if (row >= 0) {
                        maps.offsets()[row + 1] = outputEntryCount;
                        if (row + 1 == rowCount) {
                            if (entryRunCount != 0) {
                                appendWindowRun(keyWindow, valueWindow, entryRunStart, entryRunCount);
                            }
                            keySource.advanceEvents(consumed);
                            valueSource.advanceEvents(consumed);
                            return;
                        }
                    }
                    row++;
                    if (mapNulls != null) {
                        mapNulls.values()[row] = keyDefinitionLevels[keyOffset + consumed] < mapDefinitionLevel;
                    }
                }
                else if (row < 0) {
                    throw new IllegalArgumentException("Nested MAP row starts with nonzero repetition level");
                }

                if (keyEntry) {
                    if (entryRunCount == 0) {
                        entryRunStart = consumed;
                    }
                    entryRunCount++;
                    outputEntryCount++;
                }
                else if (entryRunCount != 0) {
                    appendWindowRun(keyWindow, valueWindow, entryRunStart, entryRunCount);
                    entryRunCount = 0;
                }
                consumed++;
            }
            if (entryRunCount != 0) {
                appendWindowRun(keyWindow, valueWindow, entryRunStart, entryRunCount);
            }
            keySource.advanceEvents(consumed);
            valueSource.advanceEvents(consumed);
        }
    }

    private void appendWindowRun(NestedEventWindow keyWindow, NestedEventWindow valueWindow, int start, int count)
    {
        keyWindow.appendTo(keyValues, start, count);
        valueWindow.appendTo(values, start, count);
    }

    private static boolean allAtLeast(int[] values, int offset, int length, int minimum)
    {
        int end = offset + length;
        int vectorEnd = offset + INT_SPECIES.loopBound(length);
        IntVector threshold = IntVector.broadcast(INT_SPECIES, minimum);
        for (int index = offset; index < vectorEnd; index += INT_SPECIES.length()) {
            if (IntVector.fromArray(INT_SPECIES, values, index)
                    .compare(VectorOperators.LT, threshold)
                    .anyTrue()) {
                return false;
            }
        }
        offset = vectorEnd;
        for (int index = offset; index < end; index++) {
            if (values[index] < minimum) {
                return false;
            }
        }
        return true;
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

    long consumedPageBytes()
    {
        if (!(keyReader instanceof NestedLeafReader physicalKeyReader) ||
                !(valueReader instanceof NestedLeafReader physicalValueReader)) {
            return 0;
        }
        return Math.addExact(physicalKeyReader.consumedPageBytes(), physicalValueReader.consumedPageBytes());
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
        boolean compareRepetitionLevels = !(valueReader instanceof NestedLeafEventSource valueSource) || valueSource.hasRepetitionLevels();
        if (compareRepetitionLevels && keyReader.repetitionLevel() != valueReader.repetitionLevel()) {
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
        try (keyReader; valueReader; keyValues; values) {
            // Closing releases leaf resources and allocator-owned accumulation buffers.
        }
    }
}
