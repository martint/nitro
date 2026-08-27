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
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;

/** Reconstructs a standard three-level Parquet LIST from one physical-leaf event stream. */
final class NestedArrayReader
        implements AutoCloseable
{
    private final ParquetSchema.Group list;
    private final ParquetSchema.Group repeatedValues;
    private final ParquetSchema.Primitive element;
    private final NestedLeafCursor elementReader;
    private final NestedValueAccumulator elements;

    private boolean positioned;
    private boolean exhausted;

    NestedArrayReader(ParquetSchema.Group list, RleReaderPolicy rlePolicy, PrimitiveArrayPool arrayPool)
    {
        this(list, rlePolicy, arrayPool, null);
    }

    NestedArrayReader(ParquetSchema.Group list, RleReaderPolicy rlePolicy, NestedLeafCursor elementCursor)
    {
        this(list, rlePolicy, null, elementCursor);
    }

    private NestedArrayReader(
            ParquetSchema.Group list,
            RleReaderPolicy rlePolicy,
            PrimitiveArrayPool arrayPool,
            NestedLeafCursor elementCursor)
    {
        this.list = requireNonNull(list, "list is null");
        if (list.isList()) {
            if (list.children().size() != 1 || !(list.children().getFirst() instanceof ParquetSchema.Group repeatedGroup)) {
                throw unsupported("LIST must contain one repeated element group");
            }
            this.repeatedValues = repeatedGroup;
        }
        else if (list.repetition() == FieldRepetitionType.REPEATED) {
            this.repeatedValues = list;
        }
        else {
            throw unsupported("field is neither annotated as LIST nor a legacy repeated group");
        }
        if (repeatedValues.repetition() != FieldRepetitionType.REPEATED || repeatedValues.children().size() != 1) {
            throw unsupported("LIST element group must be repeated and contain exactly one field");
        }
        if (!(repeatedValues.children().getFirst() instanceof ParquetSchema.Primitive elementLeaf)) {
            throw unsupported("nested LIST elements are not implemented yet");
        }
        if (elementLeaf.repetition() == FieldRepetitionType.REPEATED) {
            throw unsupported("LIST element cannot be repeated");
        }
        this.element = elementLeaf;
        this.elementReader = elementCursor == null
                ? new NestedLeafReader(element, rlePolicy, requireNonNull(arrayPool, "arrayPool is null"))
                : elementCursor;
        this.elements = NestedValueAccumulators.create(element);
    }

    void addRowGroup(ParquetFile file, RowGroup rowGroup)
    {
        requireNonNull(file, "file is null");
        requireNonNull(rowGroup, "rowGroup is null");
        if (!(elementReader instanceof NestedLeafReader physicalReader)) {
            throw new IllegalStateException("Cannot add Parquet row groups to an injected nested cursor");
        }
        physicalReader.addChunk(file, file.columnChunk(rowGroup, element).meta_data);
    }

    Streams read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
    {
        requireNonNull(allocator, "allocator is null");
        requireNonNull(context, "context is null");
        requireNonNull(mask, "mask is null");
        if (rowCount < 0 || mask.size() != rowCount) {
            throw new IllegalArgumentException("Nested LIST row count and mask length differ: " + rowCount + " != " + mask.size());
        }

        elements.reset(allocator);
        ArrayVector arrays = allocator.allocateArray(context, rowCount);
        BooleanVector listNulls = list.repetition() == FieldRepetitionType.OPTIONAL
                ? allocator.allocate(context, BooleanVector.class, rowCount, BooleanVector::new)
                : null;

        if (rowCount == 0) {
            arrays.setElements(elements.materialize(allocator, context));
            return listNulls == null ? Streams.ofValues(arrays) : Streams.ofValuesAndNulls(arrays, listNulls);
        }

        if (!positioned && elementReader instanceof NestedLeafEventSource eventSource) {
            readEventWindows(eventSource, arrays, listNulls, rowCount, mask);
            arrays.setElements(elements.materialize(allocator, context));
            return listNulls == null ? Streams.ofValues(arrays) : Streams.ofValuesAndNulls(arrays, listNulls);
        }

        int selectedIndex = 0;
        int nextSelected = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        ensurePositioned();
        for (int row = 0; row < rowCount; row++) {
            if (exhausted) {
                throw new IllegalArgumentException("Nested LIST event stream ended before row " + row);
            }
            if (elementReader.repetitionLevel() != 0) {
                throw new IllegalArgumentException("Nested LIST row starts with nonzero repetition level");
            }

            boolean selected = mask.all() || row == nextSelected;
            boolean listIsNull = elementReader.definitionLevel() < list.maximumDefinitionLevel();
            if (selected && listNulls != null) {
                listNulls.values()[row] = listIsNull;
            }

            do {
                boolean hasElement = elementReader.definitionLevel() >= repeatedValues.maximumDefinitionLevel();
                if (selected && hasElement) {
                    if (elementReader.hasValue()) {
                        elements.append(elementReader.valueDecoder(), elementReader.valueOrdinal(), elementReader.dictionaryId());
                    }
                    else {
                        elements.appendNull();
                    }
                }
                advance();
            }
            while (!exhausted && elementReader.repetitionLevel() != 0);

            arrays.offsets()[row + 1] = elements.size();
            if (selected && !mask.all()) {
                selectedIndex++;
                nextSelected = selectedIndex < mask.count() ? mask.position(selectedIndex) : rowCount;
            }
        }

        arrays.setElements(elements.materialize(allocator, context));
        return listNulls == null ? Streams.ofValues(arrays) : Streams.ofValuesAndNulls(arrays, listNulls);
    }

    private void readEventWindows(
            NestedLeafEventSource reader,
            ArrayVector arrays,
            BooleanVector listNulls,
            int rowCount,
            Mask mask)
    {
        if (mask.all()) {
            readAllEventWindows(reader, arrays, listNulls, rowCount);
            return;
        }

        int row = -1;
        int selectedIndex = 0;
        int nextSelected = mask.count() == 0 ? rowCount : mask.position(0);
        boolean selected = false;

        while (true) {
            NestedEventWindow window = reader.eventWindow();
            if (window == null) {
                if (row == rowCount - 1) {
                    arrays.offsets()[rowCount] = elements.size();
                    return;
                }
                throw new IllegalArgumentException("Nested LIST event stream ended before row " + (row + 1));
            }

            int consumed = 0;
            int elementRunStart = -1;
            int elementRunCount = 0;
            while (consumed < window.length()) {
                int repetitionLevel = window.repetitionLevel(consumed);
                if (repetitionLevel == 0) {
                    if (elementRunCount != 0) {
                        window.appendTo(elements, elementRunStart, elementRunCount);
                        elementRunCount = 0;
                    }
                    if (row >= 0) {
                        arrays.offsets()[row + 1] = elements.size();
                        if (row + 1 == rowCount) {
                            reader.advanceEvents(consumed);
                            return;
                        }
                    }
                    row++;
                    selected = row == nextSelected;
                    if (selected) {
                        selectedIndex++;
                        nextSelected = selectedIndex < mask.count() ? mask.position(selectedIndex) : rowCount;
                    }
                    if (selected && listNulls != null) {
                        listNulls.values()[row] = window.definitionLevel(consumed) < list.maximumDefinitionLevel();
                    }
                }
                else if (row < 0) {
                    throw new IllegalArgumentException("Nested LIST row starts with nonzero repetition level");
                }

                boolean hasElement = window.definitionLevel(consumed) >= repeatedValues.maximumDefinitionLevel();
                if (selected && hasElement) {
                    if (elementRunCount == 0) {
                        elementRunStart = consumed;
                    }
                    elementRunCount++;
                }
                else if (elementRunCount != 0) {
                    window.appendTo(elements, elementRunStart, elementRunCount);
                    elementRunCount = 0;
                }
                consumed++;
            }
            if (elementRunCount != 0) {
                window.appendTo(elements, elementRunStart, elementRunCount);
            }
            reader.advanceEvents(consumed);
        }
    }

    private void readAllEventWindows(
            NestedLeafEventSource reader,
            ArrayVector arrays,
            BooleanVector listNulls,
            int rowCount)
    {
        int row = -1;
        int outputElementCount = 0;
        int elementDefinitionLevel = repeatedValues.maximumDefinitionLevel();
        int listDefinitionLevel = list.maximumDefinitionLevel();

        while (true) {
            NestedEventWindow window = reader.eventWindow();
            if (window == null) {
                if (row == rowCount - 1) {
                    arrays.offsets()[rowCount] = outputElementCount;
                    return;
                }
                throw new IllegalArgumentException("Nested LIST event stream ended before row " + (row + 1));
            }

            int windowLength = window.length();
            if (window.allDefinitionLevelsAtLeast(windowLength, elementDefinitionLevel)) {
                int consumed = 0;
                while (consumed < windowLength) {
                    if (window.repetitionLevel(consumed) == 0) {
                        if (row >= 0) {
                            arrays.offsets()[row + 1] = outputElementCount + consumed;
                            if (row + 1 == rowCount) {
                                window.appendTo(elements, 0, consumed);
                                reader.advanceEvents(consumed);
                                return;
                            }
                        }
                        row++;
                    }
                    else if (row < 0) {
                        throw new IllegalArgumentException("Nested LIST row starts with nonzero repetition level");
                    }
                    consumed++;
                }
                window.appendTo(elements, 0, consumed);
                outputElementCount += consumed;
                reader.advanceEvents(consumed);
                continue;
            }

            int consumed = 0;
            int elementRunStart = -1;
            int elementRunCount = 0;
            while (consumed < windowLength) {
                int repetitionLevel = window.repetitionLevel(consumed);
                if (repetitionLevel == 0) {
                    if (row >= 0) {
                        arrays.offsets()[row + 1] = outputElementCount;
                        if (row + 1 == rowCount) {
                            if (elementRunCount != 0) {
                                window.appendTo(elements, elementRunStart, elementRunCount);
                            }
                            reader.advanceEvents(consumed);
                            return;
                        }
                    }
                    row++;
                    if (listNulls != null) {
                        listNulls.values()[row] = window.definitionLevel(consumed) < listDefinitionLevel;
                    }
                }
                else if (row < 0) {
                    throw new IllegalArgumentException("Nested LIST row starts with nonzero repetition level");
                }

                if (window.definitionLevel(consumed) >= elementDefinitionLevel) {
                    if (elementRunCount == 0) {
                        elementRunStart = consumed;
                    }
                    elementRunCount++;
                    outputElementCount++;
                }
                else if (elementRunCount != 0) {
                    window.appendTo(elements, elementRunStart, elementRunCount);
                    elementRunCount = 0;
                }
                consumed++;
            }
            if (elementRunCount != 0) {
                window.appendTo(elements, elementRunStart, elementRunCount);
            }
            reader.advanceEvents(consumed);
        }
    }

    void skip(long rowCount)
    {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount is negative");
        }
        ensurePositioned();
        for (long row = 0; row < rowCount; row++) {
            if (exhausted) {
                throw new IllegalArgumentException("Nested LIST event stream ended before skipped row " + row);
            }
            if (elementReader.repetitionLevel() != 0) {
                throw new IllegalArgumentException("Nested LIST row starts with nonzero repetition level");
            }
            do {
                advance();
            }
            while (!exhausted && elementReader.repetitionLevel() != 0);
        }
    }

    long consumedPageBytes()
    {
        return elementReader instanceof NestedLeafReader physicalReader ? physicalReader.consumedPageBytes() : 0;
    }

    private void ensurePositioned()
    {
        if (!positioned) {
            positioned = true;
            advance();
        }
    }

    private void advance()
    {
        exhausted = !elementReader.next();
    }

    private UnsupportedParquetFeatureException unsupported(String reason)
    {
        return new UnsupportedParquetFeatureException(
                "Unsupported native Parquet LIST layout at '" + list.name() + "': " + reason);
    }

    @Override
    public void close()
    {
        try (elementReader; elements) {
            // Closing releases both leaf resources and allocator-owned accumulation buffers.
        }
    }
}
