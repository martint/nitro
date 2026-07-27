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
package io.trino.parquet.reader.flat;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.AbstractColumnReader;
import io.trino.parquet.reader.ColumnChunk;
import io.trino.parquet.reader.decoders.ValueDecoder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.reader.decoders.ValueDecoder.ValueDecodersProvider;
import static io.trino.parquet.reader.flat.DictionaryDecoder.DictionaryDecoderProvider;
import static io.trino.parquet.reader.flat.FlatDefinitionLevelDecoder.DefinitionLevelDecoderProvider;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class SkipFlatColumnReader<BufferType>
        extends AbstractColumnReader<BufferType>
{
    private static final int[] EMPTY_DEFINITION_LEVELS = new int[0];
    private static final int[] EMPTY_REPETITION_LEVELS = new int[0];

    private final DefinitionLevelDecoderProvider definitionLevelDecoderProvider;
    private final LocalMemoryContext memoryContext;

    private int remainingPageValueCount;
    private FlatDefinitionLevelDecoder definitionLevelDecoder;
    private ValueDecoder<BufferType> valueDecoder;
    private int readOffset;
    private int nextBatchSize;
    // SKIP-DECODE PROTOTYPE: synthesize per-page firstRowIndex by accumulation so row-range skipping works on
    // parquet WITHOUT an OffsetIndex (Velox does the same -- it counts rows, needs no page index).
    private long pageFirstRow;

    /** Build a SkipFlatColumnReader for a BIGINT/INT64 flat column (mirrors ColumnReaderFactory's long path). */
    public static SkipFlatColumnReader<long[]> createForLong(PrimitiveField field, boolean vectorized, LocalMemoryContext memoryContext)
    {
        io.trino.parquet.reader.decoders.ValueDecoders valueDecoders = new io.trino.parquet.reader.decoders.ValueDecoders(field, vectorized);
        ValueDecodersProvider<long[]> decoders = valueDecoders::getLongDecoder;
        ColumnAdapter<long[]> adapter = LongColumnAdapter.LONG_ADAPTER;
        DictionaryDecoderProvider<long[]> dictionary = (dictionaryPage, isNonNull) ->
                DictionaryDecoder.getDictionaryDecoder(dictionaryPage, adapter, decoders.create(io.trino.parquet.ParquetEncoding.PLAIN), isNonNull, vectorized);
        DefinitionLevelDecoderProvider definitions = maxDef -> FlatDefinitionLevelDecoder.getFlatDefinitionLevelDecoder(maxDef, vectorized);
        return new SkipFlatColumnReader<>(field, decoders, definitions, dictionary, adapter, memoryContext);
    }

    /** Build a SkipFlatColumnReader for an INT32 column widened to a long lane (mirrors getInt32ToLongDecoder). */
    public static SkipFlatColumnReader<long[]> createForInt32AsLong(PrimitiveField field, boolean vectorized, LocalMemoryContext memoryContext)
    {
        io.trino.parquet.reader.decoders.ValueDecoders valueDecoders = new io.trino.parquet.reader.decoders.ValueDecoders(field, vectorized);
        ValueDecodersProvider<long[]> decoders = valueDecoders::getInt32ToLongDecoder;
        ColumnAdapter<long[]> adapter = LongColumnAdapter.LONG_ADAPTER;
        DictionaryDecoderProvider<long[]> dictionary = (dictionaryPage, isNonNull) ->
                DictionaryDecoder.getDictionaryDecoder(dictionaryPage, adapter, decoders.create(io.trino.parquet.ParquetEncoding.PLAIN), isNonNull, vectorized);
        DefinitionLevelDecoderProvider definitions = maxDef -> FlatDefinitionLevelDecoder.getFlatDefinitionLevelDecoder(maxDef, vectorized);
        return new SkipFlatColumnReader<>(field, decoders, definitions, dictionary, adapter, memoryContext);
    }

    /** Build a SkipFlatColumnReader for a short DECIMAL column (INT32/INT64/FLBA/BINARY, precision &le; 18) read into a long lane. */
    public static SkipFlatColumnReader<long[]> createForShortDecimal(PrimitiveField field, boolean vectorized, LocalMemoryContext memoryContext)
    {
        io.trino.parquet.reader.decoders.ValueDecoders valueDecoders = new io.trino.parquet.reader.decoders.ValueDecoders(field, vectorized);
        ValueDecodersProvider<long[]> decoders = valueDecoders::getShortDecimalDecoder;
        ColumnAdapter<long[]> adapter = LongColumnAdapter.LONG_ADAPTER;
        DictionaryDecoderProvider<long[]> dictionary = (dictionaryPage, isNonNull) ->
                DictionaryDecoder.getDictionaryDecoder(dictionaryPage, adapter, decoders.create(io.trino.parquet.ParquetEncoding.PLAIN), isNonNull, vectorized);
        DefinitionLevelDecoderProvider definitions = maxDef -> FlatDefinitionLevelDecoder.getFlatDefinitionLevelDecoder(maxDef, vectorized);
        return new SkipFlatColumnReader<>(field, decoders, definitions, dictionary, adapter, memoryContext);
    }

    /** Build a SkipFlatColumnReader for a DOUBLE column whose raw bits land in a long lane (mirrors getDoubleDecoder). */
    public static SkipFlatColumnReader<long[]> createForDouble(PrimitiveField field, boolean vectorized, LocalMemoryContext memoryContext)
    {
        io.trino.parquet.reader.decoders.ValueDecoders valueDecoders = new io.trino.parquet.reader.decoders.ValueDecoders(field, vectorized);
        ValueDecodersProvider<long[]> decoders = valueDecoders::getDoubleDecoder;
        ColumnAdapter<long[]> adapter = LongColumnAdapter.LONG_ADAPTER;
        DictionaryDecoderProvider<long[]> dictionary = (dictionaryPage, isNonNull) ->
                DictionaryDecoder.getDictionaryDecoder(dictionaryPage, adapter, decoders.create(io.trino.parquet.ParquetEncoding.PLAIN), isNonNull, vectorized);
        DefinitionLevelDecoderProvider definitions = maxDef -> FlatDefinitionLevelDecoder.getFlatDefinitionLevelDecoder(maxDef, vectorized);
        return new SkipFlatColumnReader<>(field, decoders, definitions, dictionary, adapter, memoryContext);
    }

    public SkipFlatColumnReader(
            PrimitiveField field,
            ValueDecodersProvider<BufferType> decodersProvider,
            DefinitionLevelDecoderProvider definitionLevelDecoderProvider,
            DictionaryDecoderProvider<BufferType> dictionaryDecoderProvider,
            ColumnAdapter<BufferType> columnAdapter,
            LocalMemoryContext memoryContext)
    {
        super(field, decodersProvider, dictionaryDecoderProvider, columnAdapter);
        this.definitionLevelDecoderProvider = requireNonNull(definitionLevelDecoderProvider, "definitionLevelDecoderProvider is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
    }

    @Override
    public boolean hasPageReader()
    {
        return pageReader != null;
    }

    // Opt-in (see setPreserveDictionary): preserve the parquet dictionary as a DictionaryBlock for ANY type, not just
    // Trino's string/date default. The dynamic-filtering scan enables it so a filter can be evaluated once per
    // dictionary entry; other consumers leave it off to keep Trino's default (flatten BIGINT/etc.) behaviour.
    private boolean preserveDictionary;

    public void setPreserveDictionary(boolean preserveDictionary)
    {
        this.preserveDictionary = preserveDictionary;
    }

    @Override
    protected boolean produceDictionaryBlock()
    {
        return preserveDictionary && dictionaryDecoder != null && pageReader != null && pageReader.hasOnlyDictionaryEncodedPages();
    }

    @Override
    protected boolean isNonNull()
    {
        return field.isRequired() || pageReader.hasNoNulls();
    }

    @Override
    public ColumnChunk readPrimitive()
    {
        seek();
        ColumnChunk columnChunk;
        if (isNonNull()) {
            columnChunk = readNonNull();
        }
        else {
            columnChunk = readNullable();
        }

        readOffset = 0;
        nextBatchSize = 0;
        return columnChunk;
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    private void seek()
    {
        int remainingInBatch = readOffset;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                remainingInBatch = seekToNextPage(remainingInBatch);
                if (remainingInBatch == 0) {
                    break;
                }
                if (remainingPageValueCount == 0) {
                    throwEndOfBatchException(remainingInBatch);
                }
            }

            int chunkSize = Math.min(remainingPageValueCount, remainingInBatch);
            int nonNullCount;
            if (isNonNull()) {
                nonNullCount = chunkSize;
            }
            else {
                nonNullCount = definitionLevelDecoder.skip(chunkSize);
            }
            valueDecoder.skip(nonNullCount);
            remainingInBatch -= rowRanges.seekForward(chunkSize);
            remainingPageValueCount -= chunkSize;
        }
    }

    @VisibleForTesting
    ColumnChunk readNullable()
    {
        NullableValuesBuffer<BufferType> valuesBuffer = createNullableValuesBuffer(nextBatchSize);
        boolean[] isNull = new boolean[nextBatchSize];
        int remainingInBatch = nextBatchSize;
        int offset = 0;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                if (!readNextPage()) {
                    throwEndOfBatchException(remainingInBatch);
                }
            }

            if (skipToRowRangesStart()) {
                continue;
            }
            int chunkSize = rowRanges.advanceRange(Math.min(remainingPageValueCount, remainingInBatch));
            int nonNullCount = definitionLevelDecoder.readNext(isNull, offset, chunkSize);

            valuesBuffer.readNullableValues(valueDecoder, isNull, offset, nonNullCount, chunkSize);

            offset += chunkSize;
            remainingInBatch -= chunkSize;
            remainingPageValueCount -= chunkSize;
        }
        return valuesBuffer.createNullableBlock(isNull, field.getType());
    }

    @VisibleForTesting
    ColumnChunk readNonNull()
    {
        NonNullValuesBuffer<BufferType> valuesBuffer = createNonNullValuesBuffer(nextBatchSize);
        int remainingInBatch = nextBatchSize;
        int offset = 0;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                if (!readNextPage()) {
                    throwEndOfBatchException(remainingInBatch);
                }
            }

            if (skipToRowRangesStart()) {
                continue;
            }
            int chunkSize = rowRanges.advanceRange(Math.min(remainingPageValueCount, remainingInBatch));

            valuesBuffer.readNonNullValues(valueDecoder, offset, chunkSize);
            offset += chunkSize;
            remainingInBatch -= chunkSize;
            remainingPageValueCount -= chunkSize;
        }
        return valuesBuffer.createNonNullBlock(field.getType());
    }

    /**
     * Overhead-free value-level skip-decode driven by an explicit, sorted, batch-relative
     * selection array (positions {@code 0..nextBatchSize-1}). Unlike {@link #readPrimitive()}
     * — which routes every page chunk through {@code RowRangesIterator} and pays per-range
     * bookkeeping that dominates on scattered single-position survivors — this walks the
     * selection directly: it coalesces consecutive survivors into runs, advances the value
     * (and definition-level) decoder over the gaps with {@code skip}, and decodes only the
     * runs. It consumes exactly {@code nextBatchSize} positions from the decoder (skipping the
     * tail past the last survivor) so the reader stays aligned for the next batch, exactly like
     * {@link #readPrimitive()}.
     *
     * <p>Output is gathered: result block has {@code count} positions, position {@code j}
     * holding the value at batch row {@code selection[j]}.
     */
    public ColumnChunk readSelected(int[] selection, int count)
    {
        seek();
        boolean nonNull = isNonNull();
        ColumnChunk result = nonNull
                ? readSelectedNonNull(selection, count)
                : readSelectedNullable(selection, count);
        readOffset = 0;
        nextBatchSize = 0;
        return result;
    }

    private ColumnChunk readSelectedNonNull(int[] selection, int count)
    {
        NonNullValuesBuffer<BufferType> valuesBuffer = createNonNullValuesBuffer(count);
        int remainingInBatch = nextBatchSize;
        int batchCursor = 0;
        int selectionIndex = 0;
        int offset = 0;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                if (!readNextDataPage()) {
                    throwEndOfBatchException(remainingInBatch);
                }
            }
            int pageChunk = Math.min(remainingPageValueCount, remainingInBatch);
            int pageEnd = batchCursor + pageChunk;
            int decoderCursor = batchCursor;
            while (selectionIndex < count && selection[selectionIndex] < pageEnd) {
                int runStart = selection[selectionIndex];
                int runEnd = runStart + 1;
                int next = selectionIndex + 1;
                while (next < count && selection[next] < pageEnd && selection[next] == runEnd) {
                    runEnd++;
                    next++;
                }
                int gap = runStart - decoderCursor;
                if (gap > 0) {
                    valueDecoder.skip(gap);
                }
                int run = runEnd - runStart;
                valuesBuffer.readNonNullValues(valueDecoder, offset, run);
                offset += run;
                selectionIndex = next;
                decoderCursor = runEnd;
            }
            int tail = pageEnd - decoderCursor;
            if (tail > 0) {
                valueDecoder.skip(tail);
            }
            remainingPageValueCount -= pageChunk;
            remainingInBatch -= pageChunk;
            batchCursor = pageEnd;
        }
        return valuesBuffer.createNonNullBlock(field.getType());
    }

    private ColumnChunk readSelectedNullable(int[] selection, int count)
    {
        NullableValuesBuffer<BufferType> valuesBuffer = createNullableValuesBuffer(count);
        boolean[] isNull = new boolean[count];
        int remainingInBatch = nextBatchSize;
        int batchCursor = 0;
        int selectionIndex = 0;
        int offset = 0;
        while (remainingInBatch > 0) {
            if (remainingPageValueCount == 0) {
                if (!readNextDataPage()) {
                    throwEndOfBatchException(remainingInBatch);
                }
            }
            int pageChunk = Math.min(remainingPageValueCount, remainingInBatch);
            int pageEnd = batchCursor + pageChunk;
            int decoderCursor = batchCursor;
            while (selectionIndex < count && selection[selectionIndex] < pageEnd) {
                int runStart = selection[selectionIndex];
                int runEnd = runStart + 1;
                int next = selectionIndex + 1;
                while (next < count && selection[next] < pageEnd && selection[next] == runEnd) {
                    runEnd++;
                    next++;
                }
                int gap = runStart - decoderCursor;
                if (gap > 0) {
                    valueDecoder.skip(definitionLevelDecoder.skip(gap));
                }
                int run = runEnd - runStart;
                int nonNullCount = definitionLevelDecoder.readNext(isNull, offset, run);
                valuesBuffer.readNullableValues(valueDecoder, isNull, offset, nonNullCount, run);
                offset += run;
                selectionIndex = next;
                decoderCursor = runEnd;
            }
            int tail = pageEnd - decoderCursor;
            if (tail > 0) {
                valueDecoder.skip(definitionLevelDecoder.skip(tail));
            }
            remainingPageValueCount -= pageChunk;
            remainingInBatch -= pageChunk;
            batchCursor = pageEnd;
        }
        return valuesBuffer.createNullableBlock(isNull, field.getType());
    }

    /**
     * Reads the next data page for the selection-driven path. Mirrors {@link #readNextPage()}
     * but does not touch {@code rowRanges} — the selection path bypasses it entirely.
     */
    private boolean readNextDataPage()
    {
        if (!pageReader.hasNext()) {
            return false;
        }
        readPage();
        return true;
    }

    /**
     * Finds the number of values to be skipped in the current page to reach
     * the start of the current row range and uses that to skip ValueDecoder
     * and DefinitionLevelDecoder to the appropriate position.
     *
     * @return Whether to skip the entire remaining page
     */
    private boolean skipToRowRangesStart()
    {
        int skipCount = toIntExact(rowRanges.skipToRangeStart());
        if (skipCount >= remainingPageValueCount) {
            remainingPageValueCount = 0;
            return true;
        }
        if (skipCount > 0) {
            int nonNullsCount;
            if (isNonNull()) {
                nonNullsCount = skipCount;
            }
            else {
                nonNullsCount = definitionLevelDecoder.skip(skipCount);
            }
            valueDecoder.skip(nonNullsCount);
            remainingPageValueCount -= skipCount;
        }
        return false;
    }

    private boolean readNextPage()
    {
        if (!pageReader.hasNext()) {
            return false;
        }
        DataPage page = readPage();
        rowRanges.resetForNewPage(java.util.OptionalLong.of(pageFirstRow));
        pageFirstRow += page.getValueCount();
        return true;
    }

    // When a large enough number of rows are skipped due to `seek` operation,
    // it is possible to skip decompressing and decoding parquet pages entirely.
    private int seekToNextPage(int remainingInBatch)
    {
        while (remainingInBatch > 0 && pageReader.hasNext()) {
            DataPage page = pageReader.getNextPage();
            rowRanges.resetForNewPage(java.util.OptionalLong.of(pageFirstRow));
            pageFirstRow += page.getValueCount();
            if (remainingInBatch < page.getValueCount() || !rowRanges.isPageFullyConsumed(page.getValueCount())) {
                readPage();
                return remainingInBatch;
            }
            remainingInBatch -= page.getValueCount();
            remainingPageValueCount = 0;
            pageReader.skipNextPage();
        }
        return remainingInBatch;
    }

    private DataPage readPage()
    {
        DataPage page = pageReader.readPage();
        requireNonNull(page, "page is null");
        if (page instanceof DataPageV1 dataPageV1) {
            readFlatPageV1(dataPageV1);
        }
        else if (page instanceof DataPageV2 dataPageV2) {
            readFlatPageV2(dataPageV2);
        }
        // For a compressed data page, the memory used by the decompressed values data needs to be accounted
        // for separately because the compatibility reader allocates a new byte array for the decompressed result.
        // For an uncompressed data page, we read directly from input Slices whose memory usage is already accounted
        // for in AbstractParquetDataSource#ReferenceCountedReader.
        int dataPageSizeInBytes = pageReader.arePagesCompressed() ? page.getUncompressedSize() : 0;
        long dictionarySizeInBytes = dictionaryDecoder == null ? 0 : dictionaryDecoder.getRetainedSizeInBytes();
        memoryContext.setBytes(dataPageSizeInBytes + dictionarySizeInBytes);

        remainingPageValueCount = page.getValueCount();
        return page;
    }

    private void readFlatPageV1(DataPageV1 page)
    {
        Slice buffer = page.getSlice();
        ParquetEncoding definitionEncoding = page.getDefinitionLevelEncoding();

        checkArgument(isNonNull() || definitionEncoding == RLE, "Invalid definition level encoding: %s", definitionEncoding);
        int alreadyRead = 0;
        if (definitionEncoding == RLE) {
            // Definition levels are skipped from file when the max definition level is 0 as the bit-width required to store them is 0.
            // This can happen for non-null (required) fields or nullable fields where all values are null.
            // See org.apache.parquet.column.Encoding.RLE.getValuesReader for reference.
            int maxDefinitionLevel = field.getDescriptor().getMaxDefinitionLevel();
            definitionLevelDecoder = definitionLevelDecoderProvider.create(maxDefinitionLevel);
            if (maxDefinitionLevel > 0) {
                int bufferSize = buffer.getInt(0); //  We need to read the size even if nulls are absent
                definitionLevelDecoder.init(buffer.slice(Integer.BYTES, bufferSize));
                alreadyRead = bufferSize + Integer.BYTES;
            }
        }

        valueDecoder = createValueDecoder(decodersProvider, page.getValueEncoding(), buffer.slice(alreadyRead, buffer.length() - alreadyRead));
    }

    private void readFlatPageV2(DataPageV2 page)
    {
        definitionLevelDecoder = definitionLevelDecoderProvider.create(field.getDescriptor().getMaxDefinitionLevel());
        definitionLevelDecoder.init(page.getDefinitionLevels());
        valueDecoder = createValueDecoder(decodersProvider, page.getDataEncoding(), page.getSlice());
    }

    private NonNullValuesBuffer<BufferType> createNonNullValuesBuffer(int batchSize)
    {
        if (produceDictionaryBlock()) {
            return new DictionaryValuesBuffer<>(field, dictionaryDecoder, batchSize);
        }
        return new DataValuesBuffer<>(field, columnAdapter, batchSize);
    }

    private NullableValuesBuffer<BufferType> createNullableValuesBuffer(int batchSize)
    {
        if (produceDictionaryBlock()) {
            return new DictionaryValuesBuffer<>(field, dictionaryDecoder, batchSize);
        }
        return new DataValuesBuffer<>(field, columnAdapter, batchSize);
    }

    private interface NonNullValuesBuffer<T>
    {
        void readNonNullValues(ValueDecoder<T> valueDecoder, int offset, int valuesCount);

        ColumnChunk createNonNullBlock(Type type);
    }

    private interface NullableValuesBuffer<T>
    {
        void readNullableValues(ValueDecoder<T> valueDecoder, boolean[] isNull, int offset, int nonNullCount, int valuesCount);

        ColumnChunk createNullableBlock(boolean[] isNull, Type type);
    }

    private static final class DataValuesBuffer<T>
            implements NonNullValuesBuffer<T>, NullableValuesBuffer<T>
    {
        private final PrimitiveField field;
        private final ColumnAdapter<T> columnAdapter;
        private final T values;
        private final int batchSize;
        private int totalNullsCount;

        private DataValuesBuffer(PrimitiveField field, ColumnAdapter<T> columnAdapter, int batchSize)
        {
            this.field = field;
            this.values = columnAdapter.createBuffer(batchSize);
            this.columnAdapter = columnAdapter;
            this.batchSize = batchSize;
        }

        @Override
        public void readNonNullValues(ValueDecoder<T> valueDecoder, int offset, int valuesCount)
        {
            valueDecoder.read(values, offset, valuesCount);
        }

        @Override
        public void readNullableValues(ValueDecoder<T> valueDecoder, boolean[] isNull, int offset, int nonNullCount, int valuesCount)
        {
            // Only nulls
            if (nonNullCount == 0) {
                // Unpack empty null table. This is almost always a no-op. However, in binary type
                // the last position offset needs to be propagated
                T tmpBuffer = columnAdapter.createTemporaryBuffer(offset, 0, values);
                columnAdapter.unpackNullValues(tmpBuffer, values, isNull, offset, 0, valuesCount);
            }
            // No nulls
            else if (nonNullCount == valuesCount) {
                valueDecoder.read(values, offset, nonNullCount);
            }
            else {
                // Read data values to a temporary array and unpack the nulls to the actual destination
                T tmpBuffer = columnAdapter.createTemporaryBuffer(offset, nonNullCount, values);
                valueDecoder.read(tmpBuffer, 0, nonNullCount);
                columnAdapter.unpackNullValues(tmpBuffer, values, isNull, offset, nonNullCount, valuesCount);
            }
            totalNullsCount += valuesCount - nonNullCount;
        }

        @Override
        public ColumnChunk createNonNullBlock(Type type)
        {
            checkState(
                    totalNullsCount == 0,
                    "totalNonNullsCount %s should be equal to 0 when creating non-null block",
                    totalNullsCount);
            return new ColumnChunk(columnAdapter.createNonNullBlock(values), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
        }

        @Override
        public ColumnChunk createNullableBlock(boolean[] isNull, Type type)
        {
            if (totalNullsCount == batchSize) {
                return new ColumnChunk(RunLengthEncodedBlock.create(type, null, batchSize), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
            }
            if (totalNullsCount == 0) {
                return new ColumnChunk(columnAdapter.createNonNullBlock(values), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
            }
            return new ColumnChunk(columnAdapter.createNullableBlock(isNull, values), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
        }
    }

    private static final class DictionaryValuesBuffer<T>
            implements NonNullValuesBuffer<T>, NullableValuesBuffer<T>
    {
        private final PrimitiveField field;
        private final DictionaryDecoder<T> decoder;
        private final int[] ids;
        private final int batchSize;
        private int totalNullsCount;

        private DictionaryValuesBuffer(PrimitiveField field, DictionaryDecoder<T> dictionaryDecoder, int batchSize)
        {
            this.field = field;
            this.ids = new int[batchSize];
            this.decoder = dictionaryDecoder;
            this.batchSize = batchSize;
        }

        @Override
        public void readNonNullValues(ValueDecoder<T> valueDecoder, int offset, int chunkSize)
        {
            decoder.readDictionaryIds(ids, offset, chunkSize);
        }

        @Override
        public void readNullableValues(ValueDecoder<T> valueDecoder, boolean[] isNull, int offset, int nonNullCount, int valuesCount)
        {
            // Parquet dictionary encodes only non-null values
            // Dictionary size is used as the id to denote nulls for Trino dictionary block
            if (nonNullCount == 0) {
                // Only nulls were encountered in chunkSize, add empty values for nulls
                Arrays.fill(ids, offset, offset + valuesCount, decoder.getDictionarySize());
            }
            // No nulls
            else if (nonNullCount == valuesCount) {
                decoder.readDictionaryIds(ids, offset, valuesCount);
            }
            else {
                // Read data values to a temporary array and unpack the nulls to the actual destination
                int[] tmpBuffer = new int[nonNullCount];
                decoder.readDictionaryIds(tmpBuffer, 0, nonNullCount);
                unpackDictionaryNullId(tmpBuffer, ids, isNull, offset, valuesCount, decoder.getDictionarySize());
            }
            totalNullsCount += valuesCount - nonNullCount;
        }

        @Override
        public ColumnChunk createNonNullBlock(Type type)
        {
            // This will return a nullable dictionary even if we are returning a batch of non-null values
            // for a nullable column. We avoid creating a new non-nullable dictionary to allow the engine
            // to optimize for the unchanged dictionary case.
            checkState(
                    totalNullsCount == 0,
                    "totalNonNullsCount %s should be equal to 0 when creating non-null block",
                    totalNullsCount);
            return createDictionaryBlock(ids, decoder.getDictionaryBlock(), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
        }

        @Override
        public ColumnChunk createNullableBlock(boolean[] isNull, Type type)
        {
            if (totalNullsCount == batchSize) {
                return new ColumnChunk(RunLengthEncodedBlock.create(type, null, batchSize), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
            }
            return createDictionaryBlock(ids, decoder.getDictionaryBlock(), EMPTY_DEFINITION_LEVELS, EMPTY_REPETITION_LEVELS);
        }
    }
}
