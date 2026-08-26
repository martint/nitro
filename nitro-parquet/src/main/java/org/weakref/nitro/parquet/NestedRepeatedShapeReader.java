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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;

import static java.util.Objects.requireNonNull;

/** Reads row nullability and repeated offsets from one anchor leaf without decoding physical child values. */
final class NestedRepeatedShapeReader
        implements AutoCloseable
{
    private final ParquetSchema.Group container;
    private final ParquetSchema.Group repeatedValues;
    private final ParquetSchema.Primitive anchor;
    private final NestedLeafReader reader;

    NestedRepeatedShapeReader(
            ParquetSchema.Group container,
            ParquetSchema.Group repeatedValues,
            ParquetSchema.Primitive anchor,
            RleReaderPolicy rlePolicy,
            PrimitiveArrayPool arrayPool)
    {
        this.container = requireNonNull(container, "container is null");
        this.repeatedValues = requireNonNull(repeatedValues, "repeatedValues is null");
        this.anchor = requireNonNull(anchor, "anchor is null");
        if (repeatedValues.repetition() != FieldRepetitionType.REPEATED) {
            throw new IllegalArgumentException("shape reader requires a repeated path");
        }
        this.reader = new NestedLeafReader(
                anchor,
                requireNonNull(rlePolicy, "rlePolicy is null"),
                requireNonNull(arrayPool, "arrayPool is null"),
                false);
    }

    void addRowGroup(ParquetFile file, RowGroup rowGroup)
    {
        reader.addChunk(file, file.columnChunk(rowGroup, anchor).meta_data);
    }

    void read(int[] offsets, boolean[] nulls, int rowCount, Mask mask)
    {
        requireNonNull(offsets, "offsets is null");
        requireNonNull(mask, "mask is null");
        if (rowCount < 0 || offsets.length < rowCount + 1 || mask.size() != rowCount) {
            throw new IllegalArgumentException("Repeated shape row count does not match its output buffers");
        }
        offsets[0] = 0;
        int row = -1;
        int selectedIndex = 0;
        int nextSelected = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        boolean selected = false;
        int entries = 0;

        while (true) {
            NestedEventWindow window = reader.eventWindow();
            if (window == null) {
                if (row == rowCount - 1) {
                    offsets[rowCount] = entries;
                    return;
                }
                throw new IllegalArgumentException("Nested repeated event stream ended before row " + (row + 1));
            }

            int consumed = 0;
            while (consumed < window.length()) {
                int repetitionLevel = window.repetitionLevel(consumed);
                if (repetitionLevel == 0) {
                    if (row >= 0) {
                        offsets[row + 1] = entries;
                        if (row + 1 == rowCount) {
                            reader.advanceEvents(consumed);
                            return;
                        }
                    }
                    row++;
                    selected = mask.all() || row == nextSelected;
                    if (selected && !mask.all()) {
                        selectedIndex++;
                        nextSelected = selectedIndex < mask.count() ? mask.position(selectedIndex) : rowCount;
                    }
                    if (selected && nulls != null) {
                        nulls[row] = window.definitionLevel(consumed) < container.maximumDefinitionLevel();
                    }
                }
                else if (row < 0) {
                    throw new IllegalArgumentException("Nested repeated row starts with nonzero repetition level");
                }
                if (selected && window.definitionLevel(consumed) >= repeatedValues.maximumDefinitionLevel()) {
                    entries++;
                }
                consumed++;
            }
            reader.advanceEvents(consumed);
        }
    }

    void skip(long rowCount)
    {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount is negative");
        }
        long rows = 0;
        while (true) {
            NestedEventWindow window = reader.eventWindow();
            if (window == null) {
                if (rows == rowCount) {
                    return;
                }
                throw new IllegalArgumentException("Nested repeated event stream ended before skipped rows");
            }
            int consumed = 0;
            while (consumed < window.length()) {
                if (window.repetitionLevel(consumed) == 0) {
                    if (rows == rowCount) {
                        reader.advanceEvents(consumed);
                        return;
                    }
                    rows++;
                }
                consumed++;
            }
            reader.advanceEvents(consumed);
        }
    }

    long consumedPageBytes()
    {
        return reader.consumedPageBytes();
    }

    @Override
    public void close()
    {
        reader.close();
    }
}
