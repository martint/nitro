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

import static java.util.Objects.requireNonNull;

/** Reads only the definition levels needed to expose a nullable non-repeated struct's null stream. */
final class NestedStructNullReader
        implements AutoCloseable
{
    private final ParquetSchema.Group struct;
    private final ParquetSchema.Primitive anchor;
    private final NestedLeafReader reader;

    NestedStructNullReader(ParquetSchema.Group struct, RleReaderPolicy rlePolicy)
    {
        this.struct = requireNonNull(struct, "struct is null");
        if (struct.repetition() != FieldRepetitionType.OPTIONAL || struct.isMap() || struct.isList()) {
            throw new IllegalArgumentException("Null-only reader requires an optional non-repeated struct");
        }
        this.anchor = struct.children().stream()
                .filter(ParquetSchema.Primitive.class::isInstance)
                .map(ParquetSchema.Primitive.class::cast)
                .findFirst()
                .orElseThrow(() -> new UnsupportedParquetFeatureException(
                        "Optional struct '" + struct.name() + "' has no primitive anchor leaf"));
        this.reader = new NestedLeafReader(anchor, requireNonNull(rlePolicy, "rlePolicy is null"), false);
    }

    void addRowGroup(ParquetFile file, RowGroup rowGroup)
    {
        reader.addChunk(file, file.columnChunk(rowGroup, anchor).meta_data);
    }

    BooleanVector read(Allocator allocator, Allocator.Context context, int rowCount, Mask mask)
    {
        if (rowCount < 0 || mask.size() != rowCount) {
            throw new IllegalArgumentException("Nested struct row count and mask length differ: " + rowCount + " != " + mask.size());
        }
        BooleanVector nulls = allocator.allocate(context, BooleanVector.class, rowCount, BooleanVector::new);
        int selectedIndex = 0;
        int nextSelected = mask.all() ? 0 : (mask.count() == 0 ? rowCount : mask.position(0));
        for (int row = 0; row < rowCount; row++) {
            advance();
            if (mask.all() || row == nextSelected) {
                nulls.values()[row] = reader.definitionLevel() < struct.maximumDefinitionLevel();
                if (!mask.all()) {
                    selectedIndex++;
                    nextSelected = selectedIndex < mask.count() ? mask.position(selectedIndex) : rowCount;
                }
            }
        }
        return nulls;
    }

    void skip(long rowCount)
    {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount is negative");
        }
        for (long row = 0; row < rowCount; row++) {
            advance();
        }
    }

    long consumedPageBytes()
    {
        return reader.consumedPageBytes();
    }

    private void advance()
    {
        if (!reader.next()) {
            throw new IllegalArgumentException("Nested struct event stream ended before requested rows");
        }
        if (reader.repetitionLevel() != 0) {
            throw new IllegalArgumentException("Nested struct field starts with nonzero repetition level");
        }
    }

    @Override
    public void close()
    {
        reader.close();
    }
}
