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

import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.snappy.SnappyDecompressor;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.weakref.nitro.parquet.ParquetFile.BE_LONG;
import static org.weakref.nitro.parquet.ParquetFile.LE_INT;
import static org.weakref.nitro.parquet.ParquetFile.LE_LONG;

/**
 * Decodes one logical column across all its row-group chunks (and files), page by page, directly into
 * Nitro's flat value arrays. Everything reusable is reused: the decompression output buffer, the
 * dictionary-id and definition-level scratch arrays, and the per-page value/null buffers all grow to a
 * high-water mark and are then reused for the life of the scan. Page bytes are decompressed zero-copy
 * from the mmap (off-heap {@link MemorySegment}) into the reused heap buffer; uncompressed pages are
 * read straight from the mapping.
 *
 * <p>First-slice scope: flat INT32/INT64 columns, PLAIN and (PLAIN_|RLE_)DICTIONARY encodings,
 * UNCOMPRESSED and SNAPPY codecs, DataPageV1, required or optional (definition-level nulls).
 */
public final class ColumnReader
        implements AutoCloseable
{
    public enum Kind
    {
        INT, LONG, BINARY
    }

    // Trailing slack so the bit-unpacker's reads never run off the end of a page body: the SIMD kernel
    // loads 32 bytes at a group's start, the scalar tail loads 8.
    private static final int SLACK = 32;

    private record Chunk(MemorySegment segment, ColumnMetaData metadata) {}

    // Dictionary materialization is a pure gather (out[i] = dictionary[ids[i]]); a Vector-API gather (hardware
    // vpgather) measurably beats the scalar loop on the dict-heavy scans (q82 -3%, q24/q50 -1.6%, byte-identical).
    private static final jdk.incubator.vector.VectorSpecies<Long> LONG_SPECIES = jdk.incubator.vector.LongVector.SPECIES_PREFERRED;
    private static final jdk.incubator.vector.VectorSpecies<Integer> INT_SPECIES = jdk.incubator.vector.IntVector.SPECIES_PREFERRED;

    private final Kind kind;
    private final boolean optional;
    private final Type physicalType;
    private final boolean flbaDecimal;
    private final int typeLength;
    private final List<Chunk> chunks = new ArrayList<>();

    private final Decompressor snappy = SnappyDecompressor.create();
    private final RleReader rle = new RleReader();
    // Skip path: stream the definition levels rather than materializing a per-page prefix. defRle co-advances with
    // the id reader `rle` — skipCountingOnes(gap) returns the non-nulls in a gap (O(1) per RLE run) so `rle` skips
    // exactly that many ids, and readRunCountingOnes decodes only a survivor run's levels (O(1) when the run lies
    // within one RLE run). Mirrors Trino's SkipFlatColumnReader.readSelectedNullable; no pageIdIndex prefix is built.
    private final RleReader defRle = new RleReader();
    private boolean pageDefStreaming;
    private int defPageCursor;
    private int[] runDef = new int[0];
    // Streaming skip for nullable PLAIN (and FLBA-decimal) pages: defRle co-advances and plainValueCursor tracks the
    // running non-null count, which is the index into the densely-stored plain value body (nulls store no value).
    private boolean pagePlainStreaming;
    private int plainValueCursor;
    // Whole-page bulk decode + gather beats per-survivor-run skip-decode only when a page is BOTH dense (lots survive,
    // so bulk decodes little extra) AND fragmented into short runs (skip would pay its per-run RLE-skip cost many
    // times). A dense-but-contiguous page (one long run, e.g. a date-clustered range) stays on the cheap skip path.
    private static final int SKIP_PAGE_DENSE_PERCENT = Integer.getInteger("nitro.parquet.skipPageDensePercent", 50);
    private static final int SKIP_PAGE_MIN_AVG_RUN = Integer.getInteger("nitro.parquet.skipPageMinAvgRun", 100);
    private boolean pageForceWholeDecode;

    // reusable buffers (grow to high-water mark). The decompression target is an off-heap segment from a
    // confined arena so the snappy FFM downcall is native->native (no heap marshalling); the SIMD unpacker
    // loads vectors straight from it via ByteVector.fromMemorySegment.
    private final java.lang.foreign.Arena scratchArena = java.lang.foreign.Arena.ofConfined();
    private MemorySegment decompressSegment = scratchArena.allocate(0);
    private long decompressCapacity;
    private int[] idBuffer = new int[0];
    private int[] defBuffer = new int[0];
    // reusable accumulators for assembling a batch's BinaryVector across pages
    private int[] binaryOutOffsets = new int[0];
    private byte[] binaryOutData = new byte[0];
    private int[] binaryBatchIds = new int[0];
    // Deferred binary-dictionary decode: a dict-encoded BINARY page records its row-aligned dictionary ids here
    // (sentinel 0 at null positions; nulls live in pageNulls) instead of eagerly expanding entries into pageBytes,
    // so a single-dictionary batch is emitted as a DictionaryVector and the per-row byte materialization is skipped.
    private int[] pageDictIds = new int[0];
    private boolean pageBinaryDeferred;
    // The chunk dictionary materialized as a BinaryVector, cached by generation so a DictionaryVector can wrap a
    // stable instance across batches and a flat fallback can expand ids of an earlier generation after a chunk change.
    private final java.util.HashMap<Integer, org.weakref.nitro.data.BinaryVector> dictionaryVectorCache = new java.util.HashMap<>();

    // current chunk dictionary
    private int[] dictionaryInts;
    private long[] dictionaryLongs;
    private byte[] dictionaryBytes;        // BINARY: concatenated dictionary entry bytes
    private int[] dictionaryByteOffsets;   // BINARY: entry i = dictionaryBytes[offsets[i], offsets[i+1])
    private int dictionarySize;

    // current page, fully decoded into these (nulls scattered in place)
    private int[] pageInts = new int[0];
    private long[] pageLongs = new long[0];
    private boolean[] pageNulls = new boolean[0];
    private byte[] pageBytes = new byte[0];          // BINARY: concatenated value bytes
    private int[] pageByteOffsets = new int[0];      // BINARY: value i = pageBytes[offsets[i], offsets[i+1])
    private int pageBytesUsed;
    private int pageValueCount;
    private int pageCursor;

    // chunk / page iteration
    private int chunkIndex = -1;
    private MemorySegment segment;
    private long pagePosition;
    private long chunkEnd;

    // skip-decode page state (readSelected path): for a null-free page the value stream is consumed lazily —
    // dict ids via the positioned `rle`, plain values by random access into `pagePlainBody`. Pages with nulls
    // fall back to a full whole-page decode (pageFullyDecoded) and a gather.
    private boolean pageDict;
    private boolean pageFullyDecoded;
    // pageIdIndex[p] = non-nulls before page position p. Built only on the predicate-over-dictionary lead-filter path
    // (decodeDataPageV1), which tests every position; the skip path streams definition levels instead (see defRle).
    private int[] pageIdIndex = new int[0];

    // Predicate-over-dictionary filtering (lead DF column): when filterScan is set, a dict data page keeps its ids
    // (no value materialization) and a per-chunk acceptById[] is tested over the ids -- only survivors are
    // materialized. Mirrors Trino's filterBlock over a DictionaryBlock. pageFilterDict marks a kept-ids page.
    private boolean filterScan;
    private boolean pageFilterDict;
    private boolean pageFilterNullable;
    private boolean[] acceptById = new boolean[0];
    private int acceptByIdChunk = -1;
    private MemorySegment pagePlainBody;
    private long pagePlainOffset;
    private int pageValueCursor;

    // peeked-but-not-yet-decompressed data page (skip-decode page-skip): lets readSelected decide whether to
    // decompress a page or skip it entirely (no decompress) when no survivor falls in its row range.
    private PageHeader pendingHeader;
    private long pendingBodyPosition;
    private int pendingCompressedSize;
    private int pendingUncompressedSize;
    private long pendingNextPosition;
    private int pendingNumValues;

    public ColumnReader(Type physicalType, boolean optional, int typeLength, boolean decimal)
    {
        this.physicalType = physicalType;
        this.optional = optional;
        this.typeLength = typeLength;
        this.flbaDecimal = physicalType == Type.FIXED_LEN_BYTE_ARRAY && decimal;
        this.kind = switch (physicalType) {
            case INT32 -> Kind.INT;
            case INT64 -> Kind.LONG;
            // DOUBLE is 8 little-endian bytes, bit-identical to INT64 in PLAIN/dictionary encoding; decode it through
            // the long path (raw bits) and let the scan reinterpret to a double vector (see isDouble()).
            case DOUBLE -> Kind.LONG;
            // Short decimal (precision <= 18) stored as fixed bytes decodes to an unscaled long.
            case FIXED_LEN_BYTE_ARRAY -> decimal ? Kind.LONG : Kind.BINARY;
            case BYTE_ARRAY -> Kind.BINARY;
            default -> throw new IllegalArgumentException("Unsupported physical type for NitroParquet: " + physicalType);
        };
        if (flbaDecimal && typeLength > 8) {
            throw new IllegalArgumentException("Only short decimals (<= 8 bytes) supported, got FLBA length " + typeLength);
        }
    }

    public void addChunk(MemorySegment fileSegment, ColumnMetaData metadata)
    {
        chunks.add(new Chunk(fileSegment, metadata));
    }

    public Kind kind()
    {
        return kind;
    }

    /** A DOUBLE column is decoded through the long path as raw bits; the scan reinterprets them to doubles. */
    public boolean isDouble()
    {
        return physicalType == Type.DOUBLE;
    }

    @Override
    public void close()
    {
        scratchArena.close();
    }

    public boolean optional()
    {
        return optional;
    }

    /** Fill {@code count} INT values into {@code out}; nulls (if any) into {@code nullsOut} (may be null when none). */
    public void readInts(int[] out, boolean[] nullsOut, int count)
    {
        int produced = 0;
        while (produced < count) {
            if (pageCursor >= pageValueCount && !decodeNextDataPage()) {
                throw new IllegalStateException("Ran out of Parquet values: needed " + count + ", got " + produced);
            }
            int n = Math.min(pageValueCount - pageCursor, count - produced);
            System.arraycopy(pageInts, pageCursor, out, produced, n);
            if (nullsOut != null) {
                System.arraycopy(pageNulls, pageCursor, nullsOut, produced, n);
            }
            pageCursor += n;
            produced += n;
        }
    }

    /** Fill {@code count} LONG values into {@code out}; nulls (if any) into {@code nullsOut} (may be null when none). */
    public void readLongs(long[] out, boolean[] nullsOut, int count)
    {
        int produced = 0;
        while (produced < count) {
            if (pageCursor >= pageValueCount && !decodeNextDataPage()) {
                throw new IllegalStateException("Ran out of Parquet values: needed " + count + ", got " + produced);
            }
            int n = Math.min(pageValueCount - pageCursor, count - produced);
            System.arraycopy(pageLongs, pageCursor, out, produced, n);
            if (nullsOut != null) {
                System.arraycopy(pageNulls, pageCursor, nullsOut, produced, n);
            }
            pageCursor += n;
            produced += n;
        }
    }

    /**
     * Fast-forward the full-decode reader by {@code n} rows, byte-skipping any whole data page that lies entirely
     * within the skip (no decompress, no decode) and decoding only the final partial page. Dictionary pages are still
     * decoded so a later value decode sees the current dictionary. Leaves the reader in the same full-decode state a
     * run of {@link #readInts}/{@link #readLongs} would, so it composes with a subsequent full decode. This is the
     * mechanism that lets a deferred (un-pulled) column advance across whole 450k-row pages without decoding them,
     * once its skipped rows have been coalesced into one call rather than walked a batch at a time.
     */
    public void skip(long n)
    {
        // Consume the remainder of the currently-decoded page (already decoded; only the cursor moves).
        if (pageCursor < pageValueCount) {
            int take = (int) Math.min(pageValueCount - pageCursor, n);
            pageCursor += take;
            n -= take;
        }
        // Byte-skip whole data pages; decode only the final partial page.
        while (n > 0) {
            if (!peekNextDataPage()) {
                throw new IllegalStateException("Ran out of Parquet values (skip): still needed " + n);
            }
            if (pendingNumValues <= n) {
                pagePosition = pendingNextPosition; // skip the whole page: advance past it, never decompress
                n -= pendingNumValues;
            }
            else {
                CompressionCodec codec = chunks.get(chunkIndex).metadata().codec;
                MemorySegment body = decompress(segment, pendingBodyPosition, pendingCompressedSize, pendingUncompressedSize, codec);
                decodeDataPageV1(body, pendingHeader);
                pagePosition = pendingNextPosition;
                pageCursor = (int) n;
                n = 0;
            }
        }
    }

    /**
     * Predicate-over-dictionary lead filter (LONG): read {@code count} rows; for each passing {@code predicate}
     * (a null fails), append its window position to {@code survivorsOut} and its value to {@code valuesOut}
     * (densely, survivor-aligned). Dict pages test a per-chunk {@code acceptById[]} over the ids WITHOUT
     * materializing the column; plain pages fall back to a per-value test. Advances the reader by {@code count}.
     * {@code nullsOut} (if non-null) is filled false for survivors. Returns the survivor count.
     */
    public int filterDictLongs(java.util.function.LongPredicate predicate, int count, int[] survivorsOut, long[] valuesOut, boolean[] nullsOut)
    {
        int sc = 0;
        int windowPos = 0;
        filterScan = true;
        acceptByIdChunk = -1;
        try {
            while (windowPos < count) {
                if (pageCursor >= pageValueCount && !decodeNextDataPage()) {
                    throw new IllegalStateException("Ran out of Parquet values (filter)");
                }
                int pageRows = Math.min(pageValueCount - pageCursor, count - windowPos);
                if (pageFilterDict) {
                    long[] dict = dictionaryLongs;
                    boolean[] accept = acceptByIdLong(predicate);
                    if (pageFilterNullable) {
                        for (int i = 0; i < pageRows; i++) {
                            int pp = pageCursor + i;
                            if (pageIdIndex[pp + 1] == pageIdIndex[pp]) {
                                continue;
                            }
                            int id = idBuffer[pageIdIndex[pp]];
                            if (accept[id]) {
                                valuesOut[sc] = dict[id];
                                survivorsOut[sc] = windowPos + i;
                                sc++;
                            }
                        }
                    }
                    else {
                        // Branchless compaction: always stage the value/position, advance the survivor cursor only
                        // when accepted. The per-row `if (accept[id])` is a data-dependent branch the hardware cannot
                        // predict (survivors are scattered), so removing it collapses the mispredict stall on the
                        // 230M-row lead scan. The overwritten trailing slot is harmless (sc never exceeds count).
                        for (int i = 0; i < pageRows; i++) {
                            int id = idBuffer[pageCursor + i];
                            valuesOut[sc] = dict[id];
                            survivorsOut[sc] = windowPos + i;
                            sc += accept[id] ? 1 : 0;
                        }
                    }
                }
                else {
                    for (int i = 0; i < pageRows; i++) {
                        int pp = pageCursor + i;
                        if (optional && pageNulls[pp]) {
                            continue;
                        }
                        long v = pageLongs[pp];
                        if (predicate.test(v)) {
                            valuesOut[sc] = v;
                            survivorsOut[sc] = windowPos + i;
                            sc++;
                        }
                    }
                }
                pageCursor += pageRows;
                windowPos += pageRows;
            }
        }
        finally {
            filterScan = false;
        }
        if (nullsOut != null) {
            Arrays.fill(nullsOut, 0, sc, false);
        }
        return sc;
    }

    /** Predicate-over-dictionary lead filter (INT); see {@link #filterDictLongs}. */
    public int filterDictInts(java.util.function.LongPredicate predicate, int count, int[] survivorsOut, int[] valuesOut, boolean[] nullsOut)
    {
        int sc = 0;
        int windowPos = 0;
        filterScan = true;
        acceptByIdChunk = -1;
        try {
            while (windowPos < count) {
                if (pageCursor >= pageValueCount && !decodeNextDataPage()) {
                    throw new IllegalStateException("Ran out of Parquet values (filter)");
                }
                int pageRows = Math.min(pageValueCount - pageCursor, count - windowPos);
                if (pageFilterDict) {
                    int[] dict = dictionaryInts;
                    boolean[] accept = acceptByIdInt(predicate);
                    if (pageFilterNullable) {
                        for (int i = 0; i < pageRows; i++) {
                            int pp = pageCursor + i;
                            if (pageIdIndex[pp + 1] == pageIdIndex[pp]) {
                                continue;
                            }
                            int id = idBuffer[pageIdIndex[pp]];
                            if (accept[id]) {
                                valuesOut[sc] = dict[id];
                                survivorsOut[sc] = windowPos + i;
                                sc++;
                            }
                        }
                    }
                    else {
                        // Branchless compaction (see filterDictLongs): drop the unpredictable per-row accept branch.
                        for (int i = 0; i < pageRows; i++) {
                            int id = idBuffer[pageCursor + i];
                            valuesOut[sc] = dict[id];
                            survivorsOut[sc] = windowPos + i;
                            sc += accept[id] ? 1 : 0;
                        }
                    }
                }
                else {
                    for (int i = 0; i < pageRows; i++) {
                        int pp = pageCursor + i;
                        if (optional && pageNulls[pp]) {
                            continue;
                        }
                        int v = pageInts[pp];
                        if (predicate.test(v)) {
                            valuesOut[sc] = v;
                            survivorsOut[sc] = windowPos + i;
                            sc++;
                        }
                    }
                }
                pageCursor += pageRows;
                windowPos += pageRows;
            }
        }
        finally {
            filterScan = false;
        }
        if (nullsOut != null) {
            Arrays.fill(nullsOut, 0, sc, false);
        }
        return sc;
    }

    private boolean[] acceptByIdLong(java.util.function.LongPredicate predicate)
    {
        if (acceptByIdChunk != chunkIndex) {
            if (acceptById.length < dictionarySize) {
                acceptById = new boolean[dictionarySize];
            }
            long[] dict = dictionaryLongs;
            for (int id = 0; id < dictionarySize; id++) {
                acceptById[id] = predicate.test(dict[id]);
            }
            acceptByIdChunk = chunkIndex;
        }
        return acceptById;
    }

    private boolean[] acceptByIdInt(java.util.function.LongPredicate predicate)
    {
        if (acceptByIdChunk != chunkIndex) {
            if (acceptById.length < dictionarySize) {
                acceptById = new boolean[dictionarySize];
            }
            int[] dict = dictionaryInts;
            for (int id = 0; id < dictionarySize; id++) {
                acceptById[id] = predicate.test(dict[id]);
            }
            acceptByIdChunk = chunkIndex;
        }
        return acceptById;
    }

    /**
     * Read {@code count} binary positions. For a non-null column whose batch is fully dictionary-encoded under a
     * single dictionary, returns a {@link org.weakref.nitro.data.DictionaryVector} (ids + the parquet dictionary) so
     * downstream filters/grouping evaluate per dictionary entry instead of per row — the encoding Trino's reader
     * preserves. Otherwise (nullable, a plain page, or a batch crossing dictionaries) returns a flat
     * {@link org.weakref.nitro.data.BinaryVector}; nulls (if any) go into {@code nullsOut}.
     */
    public org.weakref.nitro.data.Vector readBinary(boolean[] nullsOut, int count)
    {
        if (binaryOutOffsets.length < count + 1) {
            binaryOutOffsets = new int[count + 1];
        }
        if (binaryBatchIds.length < count) {
            binaryBatchIds = new int[count];
        }
        boolean dictionaryEligible = !Boolean.getBoolean("nitro.parquet.disableBinaryDictionary");
        int batchGeneration = -1;
        int produced = 0;
        int dataLength = 0;
        boolean flat = false;
        binaryOutOffsets[0] = 0;
        while (produced < count) {
            if (pageCursor >= pageValueCount && !decodeNextDataPage()) {
                throw new IllegalStateException("Ran out of Parquet values: needed " + count + ", got " + produced);
            }
            int n = Math.min(pageValueCount - pageCursor, count - produced);
            boolean pageIsDictionary = pageBinaryDeferred;
            if (!flat && dictionaryEligible && pageIsDictionary && (batchGeneration == -1 || dictionaryGeneration == batchGeneration)) {
                // Dictionary path: the page recorded row-aligned ids; carry them without materializing bytes.
                batchGeneration = dictionaryGeneration;
                System.arraycopy(pageDictIds, pageCursor, binaryBatchIds, produced, n);
            }
            else {
                if (!flat) {
                    // A plain page or a dictionary change ends the dictionary batch: materialize the ids gathered so
                    // far [0, produced) into flat bytes, then continue flat for the rest of the batch.
                    flat = true;
                    dataLength = spillDictionaryIdsToFlat(batchGeneration, produced, nullsOut);
                }
                dataLength = appendBinaryRunToFlat(pageIsDictionary, pageCursor, n, produced, dataLength, nullsOut);
            }
            if (nullsOut != null) {
                System.arraycopy(pageNulls, pageCursor, nullsOut, produced, n);
            }
            pageCursor += n;
            produced += n;
        }
        if (!flat) {
            return new org.weakref.nitro.data.DictionaryVector(Arrays.copyOf(binaryBatchIds, count), dictionaryVectorCache.get(batchGeneration));
        }
        org.weakref.nitro.data.BinaryVector result = new org.weakref.nitro.data.BinaryVector(count, Arrays.copyOf(binaryOutOffsets, count + 1), Arrays.copyOf(binaryOutData, dataLength));
        result.addTraits(java.util.Set.of(org.weakref.nitro.data.Utf8Traits.UTF8_STRING));
        return result;
    }

    /**
     * Expand the already-gathered dictionary ids {@code binaryBatchIds[0, produced)} into flat bytes at the head of
     * {@code binaryOutData}/{@code binaryOutOffsets}, using the dictionary of {@code generation}. Null positions (per
     * {@code nullsOut}, when present) emit a zero-length entry, matching the eager flat layout. Returns the byte length.
     */
    private int spillDictionaryIdsToFlat(int generation, int produced, boolean[] nullsOut)
    {
        binaryOutOffsets[0] = 0;
        if (produced == 0) {
            return 0;
        }
        org.weakref.nitro.data.BinaryVector dictionary = dictionaryVectorCache.get(generation);
        int[] dictionaryOffsets = dictionary.offsets();
        byte[] dictionaryData = dictionary.data();
        int dataLength = 0;
        for (int i = 0; i < produced; i++) {
            if (nullsOut == null || !nullsOut[i]) {
                int id = binaryBatchIds[i];
                int start = dictionaryOffsets[id];
                int length = dictionaryOffsets[id + 1] - start;
                if (binaryOutData.length < dataLength + length) {
                    binaryOutData = Arrays.copyOf(binaryOutData, Math.max(binaryOutData.length * 2, dataLength + length));
                }
                System.arraycopy(dictionaryData, start, binaryOutData, dataLength, length);
                dataLength += length;
            }
            binaryOutOffsets[i + 1] = dataLength;
        }
        return dataLength;
    }

    /**
     * Append {@code n} flat positions starting at {@code pageCursor} to {@code binaryOutData}/{@code binaryOutOffsets}
     * at output offset {@code produced}. A deferred dictionary page expands ids through the live chunk dictionary; a
     * plain page copies its already-materialized {@code pageBytes}. Returns the updated byte length.
     */
    private int appendBinaryRunToFlat(boolean pageIsDictionary, int pageCursor, int n, int produced, int dataLength, boolean[] nullsOut)
    {
        if (pageIsDictionary) {
            for (int j = 0; j < n; j++) {
                int position = pageCursor + j;
                if (nullsOut == null ? !(optional && pageNulls[position]) : !nullsOut[produced + j]) {
                    int id = pageDictIds[position];
                    int start = dictionaryByteOffsets[id];
                    int length = dictionaryByteOffsets[id + 1] - start;
                    if (binaryOutData.length < dataLength + length) {
                        binaryOutData = Arrays.copyOf(binaryOutData, Math.max(binaryOutData.length * 2, dataLength + length));
                    }
                    System.arraycopy(dictionaryBytes, start, binaryOutData, dataLength, length);
                    dataLength += length;
                }
                binaryOutOffsets[produced + j + 1] = dataLength;
            }
            return dataLength;
        }
        int runStart = pageByteOffsets[pageCursor];
        int runBytes = pageByteOffsets[pageCursor + n] - runStart;
        if (binaryOutData.length < dataLength + runBytes) {
            binaryOutData = Arrays.copyOf(binaryOutData, Math.max(binaryOutData.length * 2, dataLength + runBytes));
        }
        System.arraycopy(pageBytes, runStart, binaryOutData, dataLength, runBytes);
        int shift = dataLength - runStart;
        for (int j = 1; j <= n; j++) {
            binaryOutOffsets[produced + j] = pageByteOffsets[pageCursor + j] + shift;
        }
        return dataLength + runBytes;
    }

    /** Skip-decode: fill {@code count} survivor positions (batch-relative, sorted, within the next {@code batchRows}) into {@code out}. */
    public void readSelectedLongs(int[] survivors, int count, int batchRows, long[] out, boolean[] nullsOut)
    {
        int produced = 0;
        int sel = 0;
        int batchCursor = 0;
        while (batchCursor < batchRows) {
            if (pageCursor >= pageValueCount) {
                int skipped = acquirePageForSkip(survivors, sel, count, batchCursor, batchRows);
                if (skipped >= 0) {
                    batchCursor += skipped;
                    continue;
                }
            }
            int pageRows = Math.min(pageValueCount - pageCursor, batchRows - batchCursor);
            int pageEnd = batchCursor + pageRows;
            while (sel < count && survivors[sel] < pageEnd) {
                // Coalesce consecutive survivors into one run: skip the gap once, then bulk-read the run.
                int runStartPage = pageCursor + (survivors[sel] - batchCursor);
                int runLen = 1;
                int next = sel + 1;
                while (next < count && survivors[next] < pageEnd && survivors[next] == survivors[next - 1] + 1) {
                    runLen++;
                    next++;
                }
                if (pageDefStreaming) {
                    long[] dict = dictionaryLongs;
                    int gap = runStartPage - defPageCursor;
                    if (gap > 0) {
                        rle.skip(defRle.skipCountingOnes(gap));
                    }
                    int nonNullInRun = defRle.readRunCountingOnes(runDef, runLen);
                    if (nonNullInRun == runLen) {
                        rle.read(idBuffer, 0, runLen);
                        for (int k = 0; k < runLen; k++) {
                            out[produced + k] = dict[idBuffer[k]];
                        }
                        if (nullsOut != null) {
                            Arrays.fill(nullsOut, produced, produced + runLen, false);
                        }
                    }
                    else if (nonNullInRun == 0) {
                        Arrays.fill(out, produced, produced + runLen, 0L);
                        if (nullsOut != null) {
                            Arrays.fill(nullsOut, produced, produced + runLen, true);
                        }
                    }
                    else {
                        materializeStreamingRunLong(out, produced, runLen, nonNullInRun, nullsOut, dict);
                    }
                    defPageCursor = runStartPage + runLen;
                }
                else if (pagePlainStreaming) {
                    int gap = runStartPage - defPageCursor;
                    if (gap > 0) {
                        plainValueCursor += defRle.skipCountingOnes(gap);
                    }
                    int nonNullInRun = defRle.readRunCountingOnes(runDef, runLen);
                    if (nonNullInRun == runLen) {
                        readPlainLongsAt(out, produced, plainValueCursor, runLen);
                        plainValueCursor += runLen;
                        if (nullsOut != null) {
                            Arrays.fill(nullsOut, produced, produced + runLen, false);
                        }
                    }
                    else if (nonNullInRun == 0) {
                        Arrays.fill(out, produced, produced + runLen, 0L);
                        if (nullsOut != null) {
                            Arrays.fill(nullsOut, produced, produced + runLen, true);
                        }
                    }
                    else {
                        materializeStreamingPlainRunLong(out, produced, runLen, nullsOut);
                    }
                    defPageCursor = runStartPage + runLen;
                }
                else if (pageDict && !pageFullyDecoded) {
                    int gap = runStartPage - pageValueCursor;
                    if (gap > 0) {
                        rle.skip(gap);
                    }
                    rle.read(idBuffer, 0, runLen);
                    gatherLongs(dictionaryLongs, idBuffer, 0, out, produced, runLen);
                    if (nullsOut != null) {
                        Arrays.fill(nullsOut, produced, produced + runLen, false);
                    }
                    pageValueCursor = runStartPage + runLen;
                }
                else {
                    materializeColdLong(out, produced, runStartPage, runLen, nullsOut);
                }
                produced += runLen;
                sel = next;
            }
            if (pageDefStreaming) {
                int chunkEndPage = pageCursor + pageRows;
                int tail = chunkEndPage - defPageCursor;
                if (tail > 0) {
                    rle.skip(defRle.skipCountingOnes(tail));
                    defPageCursor = chunkEndPage;
                }
            }
            else if (pagePlainStreaming) {
                int chunkEndPage = pageCursor + pageRows;
                int tail = chunkEndPage - defPageCursor;
                if (tail > 0) {
                    plainValueCursor += defRle.skipCountingOnes(tail);
                    defPageCursor = chunkEndPage;
                }
            }
            else if (!pageFullyDecoded && pageDict) {
                int end = pageCursor + pageRows;
                if (end > pageValueCursor) {
                    rle.skip(end - pageValueCursor);
                    pageValueCursor = end;
                }
            }
            pageCursor += pageRows;
            batchCursor = pageEnd;
        }
    }

    /** Materialize a streaming-def run that contains nulls: ids were read densely (one per non-null in run order). */
    private void materializeStreamingRunLong(long[] out, int produced, int runLen, int nonNullInRun, boolean[] nullsOut, long[] dict)
    {
        rle.read(idBuffer, 0, nonNullInRun);
        int idPos = 0;
        for (int k = 0; k < runLen; k++) {
            if (runDef[k] == 0) {
                out[produced + k] = 0;
                if (nullsOut != null) {
                    nullsOut[produced + k] = true;
                }
            }
            else {
                out[produced + k] = dict[idBuffer[idPos++]];
                if (nullsOut != null) {
                    nullsOut[produced + k] = false;
                }
            }
        }
    }

    /** Read {@code n} consecutive plain LONG (or FLBA-decimal) values starting at non-null value index {@code valueIndex}. */
    private void readPlainLongsAt(long[] out, int produced, int valueIndex, int n)
    {
        if (flbaDecimal) {
            long off = pagePlainOffset + (long) valueIndex * typeLength;
            for (int k = 0; k < n; k++) {
                out[produced + k] = bigEndianSignedLong(pagePlainBody, off, typeLength);
                off += typeLength;
            }
        }
        else {
            MemorySegment.copy(pagePlainBody, LE_LONG, pagePlainOffset + (long) valueIndex * 8, out, produced, n);
        }
    }

    /** Materialize a streaming-plain LONG run that contains nulls: only non-null positions consume a plain value. */
    private void materializeStreamingPlainRunLong(long[] out, int produced, int runLen, boolean[] nullsOut)
    {
        int valueIndex = plainValueCursor;
        for (int k = 0; k < runLen; k++) {
            if (runDef[k] == 0) {
                out[produced + k] = 0;
                if (nullsOut != null) {
                    nullsOut[produced + k] = true;
                }
            }
            else {
                out[produced + k] = flbaDecimal
                        ? bigEndianSignedLong(pagePlainBody, pagePlainOffset + (long) valueIndex * typeLength, typeLength)
                        : pagePlainBody.get(LE_LONG, pagePlainOffset + (long) valueIndex * 8);
                valueIndex++;
                if (nullsOut != null) {
                    nullsOut[produced + k] = false;
                }
            }
        }
        plainValueCursor = valueIndex;
    }

    /** Cold long materialization paths (whole-page-decoded, FLBA-decimal plain, plain) — kept out of the hot loop. */
    private void materializeColdLong(long[] out, int produced, int runStartPage, int runLen, boolean[] nullsOut)
    {
        if (pageFullyDecoded) {
            System.arraycopy(pageLongs, runStartPage, out, produced, runLen);
            if (nullsOut != null) {
                System.arraycopy(pageNulls, runStartPage, nullsOut, produced, runLen);
            }
        }
        else if (flbaDecimal) {
            long off = pagePlainOffset + (long) runStartPage * typeLength;
            for (int k = 0; k < runLen; k++) {
                out[produced + k] = bigEndianSignedLong(pagePlainBody, off, typeLength);
                off += typeLength;
            }
            if (nullsOut != null) {
                Arrays.fill(nullsOut, produced, produced + runLen, false);
            }
        }
        else {
            MemorySegment.copy(pagePlainBody, LE_LONG, pagePlainOffset + (long) runStartPage * 8, out, produced, runLen);
            if (nullsOut != null) {
                Arrays.fill(nullsOut, produced, produced + runLen, false);
            }
        }
    }

    /** Skip-decode for INT columns; see {@link #readSelectedLongs}. */
    public void readSelectedInts(int[] survivors, int count, int batchRows, int[] out, boolean[] nullsOut)
    {
        int produced = 0;
        int sel = 0;
        int batchCursor = 0;
        while (batchCursor < batchRows) {
            if (pageCursor >= pageValueCount) {
                int skipped = acquirePageForSkip(survivors, sel, count, batchCursor, batchRows);
                if (skipped >= 0) {
                    batchCursor += skipped;
                    continue;
                }
            }
            int pageRows = Math.min(pageValueCount - pageCursor, batchRows - batchCursor);
            int pageEnd = batchCursor + pageRows;
            while (sel < count && survivors[sel] < pageEnd) {
                int runStartPage = pageCursor + (survivors[sel] - batchCursor);
                int runLen = 1;
                int next = sel + 1;
                while (next < count && survivors[next] < pageEnd && survivors[next] == survivors[next - 1] + 1) {
                    runLen++;
                    next++;
                }
                if (pageDefStreaming) {
                    int[] dict = dictionaryInts;
                    int gap = runStartPage - defPageCursor;
                    if (gap > 0) {
                        rle.skip(defRle.skipCountingOnes(gap));
                    }
                    int nonNullInRun = defRle.readRunCountingOnes(runDef, runLen);
                    if (nonNullInRun == runLen) {
                        rle.read(idBuffer, 0, runLen);
                        for (int k = 0; k < runLen; k++) {
                            out[produced + k] = dict[idBuffer[k]];
                        }
                        if (nullsOut != null) {
                            Arrays.fill(nullsOut, produced, produced + runLen, false);
                        }
                    }
                    else if (nonNullInRun == 0) {
                        Arrays.fill(out, produced, produced + runLen, 0);
                        if (nullsOut != null) {
                            Arrays.fill(nullsOut, produced, produced + runLen, true);
                        }
                    }
                    else {
                        materializeStreamingRunInt(out, produced, runLen, nonNullInRun, nullsOut, dict);
                    }
                    defPageCursor = runStartPage + runLen;
                }
                else if (pagePlainStreaming) {
                    int gap = runStartPage - defPageCursor;
                    if (gap > 0) {
                        plainValueCursor += defRle.skipCountingOnes(gap);
                    }
                    int nonNullInRun = defRle.readRunCountingOnes(runDef, runLen);
                    if (nonNullInRun == runLen) {
                        MemorySegment.copy(pagePlainBody, LE_INT, pagePlainOffset + (long) plainValueCursor * 4, out, produced, runLen);
                        plainValueCursor += runLen;
                        if (nullsOut != null) {
                            Arrays.fill(nullsOut, produced, produced + runLen, false);
                        }
                    }
                    else if (nonNullInRun == 0) {
                        Arrays.fill(out, produced, produced + runLen, 0);
                        if (nullsOut != null) {
                            Arrays.fill(nullsOut, produced, produced + runLen, true);
                        }
                    }
                    else {
                        materializeStreamingPlainRunInt(out, produced, runLen, nullsOut);
                    }
                    defPageCursor = runStartPage + runLen;
                }
                else if (pageDict && !pageFullyDecoded) {
                    int gap = runStartPage - pageValueCursor;
                    if (gap > 0) {
                        rle.skip(gap);
                    }
                    rle.read(idBuffer, 0, runLen);
                    gatherInts(dictionaryInts, idBuffer, 0, out, produced, runLen);
                    if (nullsOut != null) {
                        Arrays.fill(nullsOut, produced, produced + runLen, false);
                    }
                    pageValueCursor = runStartPage + runLen;
                }
                else {
                    materializeColdInt(out, produced, runStartPage, runLen, nullsOut);
                }
                produced += runLen;
                sel = next;
            }
            if (pageDefStreaming) {
                int chunkEndPage = pageCursor + pageRows;
                int tail = chunkEndPage - defPageCursor;
                if (tail > 0) {
                    rle.skip(defRle.skipCountingOnes(tail));
                    defPageCursor = chunkEndPage;
                }
            }
            else if (pagePlainStreaming) {
                int chunkEndPage = pageCursor + pageRows;
                int tail = chunkEndPage - defPageCursor;
                if (tail > 0) {
                    plainValueCursor += defRle.skipCountingOnes(tail);
                    defPageCursor = chunkEndPage;
                }
            }
            else if (!pageFullyDecoded && pageDict) {
                int end = pageCursor + pageRows;
                if (end > pageValueCursor) {
                    rle.skip(end - pageValueCursor);
                    pageValueCursor = end;
                }
            }
            pageCursor += pageRows;
            batchCursor = pageEnd;
        }
    }

    /** Materialize a streaming-plain INT run that contains nulls: only non-null positions consume a plain value. */
    private void materializeStreamingPlainRunInt(int[] out, int produced, int runLen, boolean[] nullsOut)
    {
        int valueIndex = plainValueCursor;
        for (int k = 0; k < runLen; k++) {
            if (runDef[k] == 0) {
                out[produced + k] = 0;
                if (nullsOut != null) {
                    nullsOut[produced + k] = true;
                }
            }
            else {
                out[produced + k] = pagePlainBody.get(LE_INT, pagePlainOffset + (long) valueIndex * 4);
                valueIndex++;
                if (nullsOut != null) {
                    nullsOut[produced + k] = false;
                }
            }
        }
        plainValueCursor = valueIndex;
    }

    /** INT counterpart of {@link #materializeStreamingRunLong}. */
    private void materializeStreamingRunInt(int[] out, int produced, int runLen, int nonNullInRun, boolean[] nullsOut, int[] dict)
    {
        rle.read(idBuffer, 0, nonNullInRun);
        int idPos = 0;
        for (int k = 0; k < runLen; k++) {
            if (runDef[k] == 0) {
                out[produced + k] = 0;
                if (nullsOut != null) {
                    nullsOut[produced + k] = true;
                }
            }
            else {
                out[produced + k] = dict[idBuffer[idPos++]];
                if (nullsOut != null) {
                    nullsOut[produced + k] = false;
                }
            }
        }
    }

    /** Cold INT materialization paths (whole-page-decoded, plain) — kept out of the hot loop. */
    private void materializeColdInt(int[] out, int produced, int runStartPage, int runLen, boolean[] nullsOut)
    {
        if (pageFullyDecoded) {
            System.arraycopy(pageInts, runStartPage, out, produced, runLen);
            if (nullsOut != null) {
                System.arraycopy(pageNulls, runStartPage, nullsOut, produced, runLen);
            }
        }
        else {
            MemorySegment.copy(pagePlainBody, LE_INT, pagePlainOffset + (long) runStartPage * 4, out, produced, runLen);
            if (nullsOut != null) {
                Arrays.fill(nullsOut, produced, produced + runLen, false);
            }
        }
    }

    /**
     * Skip-decode for BINARY columns. Mirrors {@link #readSelectedLongs}: pages with no survivor in their row range
     * are skipped WITHOUT decompressing (via {@link #acquirePageForSkip}); pages with survivors are whole-decoded
     * (binary always takes the whole-page path) and only the survivor positions are gathered. Produces a
     * POSITION-INDEXED {@link org.weakref.nitro.data.BinaryVector} of {@code batchRows} entries — survivor positions
     * hold their bytes, every other position is a zero-length entry — so a constrained operator reads survivors at
     * their original positions. {@code nullsOut} (length {@code batchRows}, optional) receives per-position nulls.
     * The reader advances by exactly {@code batchRows}, so a binary column can use this for its whole life without
     * mixing the full and skip page paths.
     */
    public org.weakref.nitro.data.Vector readSelectedBinary(int[] survivors, int count, int batchRows, boolean[] nullsOut)
    {
        if (binaryOutOffsets.length < batchRows + 1) {
            binaryOutOffsets = new int[batchRows + 1];
        }
        int sel = 0;
        int batchCursor = 0;
        int dataLength = 0;
        binaryOutOffsets[0] = 0;
        while (batchCursor < batchRows) {
            if (pageCursor >= pageValueCount) {
                int skipped = acquirePageForSkip(survivors, sel, count, batchCursor, batchRows);
                if (skipped >= 0) {
                    for (int p = 0; p < skipped; p++) {
                        binaryOutOffsets[batchCursor + p + 1] = dataLength;
                        if (nullsOut != null) {
                            nullsOut[batchCursor + p] = false;
                        }
                    }
                    batchCursor += skipped;
                    continue;
                }
            }
            int pageRows = Math.min(pageValueCount - pageCursor, batchRows - batchCursor);
            int pageEnd = batchCursor + pageRows;
            for (int b = batchCursor; b < pageEnd; b++) {
                if (sel < count && survivors[sel] == b) {
                    int pagePos = pageCursor + (b - batchCursor);
                    int start = pageByteOffsets[pagePos];
                    int length = pageByteOffsets[pagePos + 1] - start;
                    if (binaryOutData.length < dataLength + length) {
                        binaryOutData = Arrays.copyOf(binaryOutData, Math.max(binaryOutData.length * 2, dataLength + length));
                    }
                    System.arraycopy(pageBytes, start, binaryOutData, dataLength, length);
                    dataLength += length;
                    if (nullsOut != null) {
                        nullsOut[b] = optional && pageNulls[pagePos];
                    }
                    sel++;
                }
                else if (nullsOut != null) {
                    nullsOut[b] = false;
                }
                binaryOutOffsets[b + 1] = dataLength;
            }
            pageCursor += pageRows;
            batchCursor = pageEnd;
        }
        org.weakref.nitro.data.BinaryVector result = new org.weakref.nitro.data.BinaryVector(
                batchRows, Arrays.copyOf(binaryOutOffsets, batchRows + 1), Arrays.copyOf(binaryOutData, dataLength));
        result.addTraits(java.util.Set.of(org.weakref.nitro.data.Utf8Traits.UTF8_STRING));
        return result;
    }

    /**
     * Acquire the next data page for skip-decode. Peeks the page header (no decompress) and, if it lies fully
     * within the current batch with NO survivor in its row range, skips it WITHOUT decompressing (the dominant
     * win for selective DFs over clustered data) — returning the number of rows skipped. Otherwise it
     * decompresses and loads the page (value stream positioned lazily) and returns -1.
     */
    private int acquirePageForSkip(int[] survivors, int sel, int count, int batchCursor, int batchRows)
    {
        if (!peekNextDataPage()) {
            throw new IllegalStateException("Ran out of Parquet values (skip)");
        }
        int rowsThisBatch = Math.min(pendingNumValues, batchRows - batchCursor);
        int pageEnd = batchCursor + rowsThisBatch;
        boolean hasSurvivor = sel < count && survivors[sel] < pageEnd;
        if (!hasSurvivor && pendingNumValues <= batchRows - batchCursor) {
            pagePosition = pendingNextPosition; // skip the whole page: advance past it, never decompress
            return pendingNumValues;
        }
        // Count survivors AND survivor runs in this page. Bulk-decode only a dense, fragmented page (many short runs):
        // there the per-run skip cost dominates. A dense contiguous page (few long runs, e.g. a clustered date range)
        // keeps the skip path. Decided per page (immune to date-clustered DFs where whole windows are in/out of range).
        int survivorsInPage = 0;
        int runCount = 0;
        int prev = -2;
        for (int s = sel; s < count && survivors[s] < pageEnd; s++) {
            if (survivors[s] != prev + 1) {
                runCount++;
            }
            prev = survivors[s];
            survivorsInPage++;
        }
        pageForceWholeDecode = (long) survivorsInPage * 100 >= (long) rowsThisBatch * SKIP_PAGE_DENSE_PERCENT
                && (long) survivorsInPage < (long) runCount * SKIP_PAGE_MIN_AVG_RUN;
        CompressionCodec codec = chunks.get(chunkIndex).metadata().codec;
        loadDataPageForSkip(decompress(segment, pendingBodyPosition, pendingCompressedSize, pendingUncompressedSize, codec), pendingHeader);
        pagePosition = pendingNextPosition;
        return -1;
    }

    /** Read the next data page's header (decompressing/decoding any intervening dictionary page) without decompressing it. */
    private boolean peekNextDataPage()
    {
        while (true) {
            if (segment == null || pagePosition >= chunkEnd) {
                if (!advanceChunk()) {
                    return false;
                }
            }
            PageHeader header;
            long bodyPosition;
            try (ParquetFile.SegmentInputStream in = new ParquetFile.SegmentInputStream(segment, pagePosition, chunkEnd - pagePosition)) {
                header = Util.readPageHeader(in);
                bodyPosition = in.position();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Unable to read Parquet page header", e);
            }
            int compressedSize = header.compressed_page_size;
            int uncompressedSize = header.uncompressed_page_size;
            long nextPagePosition = bodyPosition + compressedSize;
            if (header.type == PageType.DICTIONARY_PAGE) {
                CompressionCodec codec = chunks.get(chunkIndex).metadata().codec;
                decodeDictionary(decompress(segment, bodyPosition, compressedSize, uncompressedSize, codec), header.dictionary_page_header.num_values);
                pagePosition = nextPagePosition;
                continue;
            }
            if (header.type == PageType.DATA_PAGE) {
                pendingHeader = header;
                pendingBodyPosition = bodyPosition;
                pendingCompressedSize = compressedSize;
                pendingUncompressedSize = uncompressedSize;
                pendingNextPosition = nextPagePosition;
                pendingNumValues = header.data_page_header.num_values;
                return true;
            }
            throw new IllegalStateException("Unsupported page type for NitroParquet: " + header.type);
        }
    }

    private void loadDataPageForSkip(MemorySegment body, PageHeader header)
    {
        int valueCount = header.data_page_header.num_values;
        Encoding encoding = header.data_page_header.encoding;
        ensurePageCapacity(valueCount);
        pageValueCount = valueCount;
        pageCursor = 0;
        pageValueCursor = 0;
        long offset = 0;
        int nonNullCount = valueCount;
        pageDict = encoding == Encoding.RLE_DICTIONARY || encoding == Encoding.PLAIN_DICTIONARY;
        // A nullable non-binary page takes a streaming skip path: defRle is left positioned at the definition-level
        // stream and co-advances run-by-run, so survivor values are read straight from the dictionary-id stream
        // (dict) or the dense plain value body (plain) without a per-level prefix or a whole-page decode.
        boolean streaming = false;
        long defStreamOffset = 0;
        if (optional) {
            int defLength = body.get(LE_INT, 0);
            offset = 4;
            rle.init(body, offset, 1);
            // Null-free pages (one RLE run of 1s) are the common case; detect them in O(1) and skip the per-level
            // decode. Only pages that actually contain nulls pay more.
            if (!rle.consumeIfAllOnes(valueCount)) {
                if (kind != Kind.BINARY && !pageForceWholeDecode) {
                    // Stream the levels run-by-run; nonNullCount stays unknown (the page is routed to a streaming skip
                    // path regardless, which handles null-free runs as its O(1) fast case).
                    defStreamOffset = offset;
                    streaming = true;
                }
                else {
                    // Whole-page decode (binary, or a dense page forced to bulk) needs the per-level array + non-null count.
                    rle.read(defBuffer, 0, valueCount);
                    nonNullCount = 0;
                    for (int i = 0; i < valueCount; i++) {
                        nonNullCount += defBuffer[i];
                    }
                }
            }
            offset += defLength;
        }
        boolean nullFree = !streaming && nonNullCount == valueCount;
        pageDefStreaming = false;
        pagePlainStreaming = false;
        if (streaming) {
            defRle.init(body, defStreamOffset, 1);
            defPageCursor = 0;
            pageFullyDecoded = false;
            if (pageDict) {
                // Nullable dict page (Trino-style streaming): position the id reader at the id stream; readSelected
                // co-advances it with defRle per survivor run without a pageIdIndex prefix.
                int bitWidth = body.get(ValueLayout.JAVA_BYTE, offset) & 0xFF;
                offset += 1;
                rle.init(body, offset, bitWidth);
                pageDefStreaming = true;
            }
            else {
                // Nullable plain page: survivors index the dense plain value body by their running non-null count.
                pagePlainBody = body;
                pagePlainOffset = offset;
                plainValueCursor = 0;
                pagePlainStreaming = true;
            }
        }
        else if (!pageForceWholeDecode && nullFree && kind != Kind.BINARY) {
            // Null-free non-binary: the page position maps 1:1 to the value stream, so survivors read lazily.
            if (pageDict) {
                int bitWidth = body.get(ValueLayout.JAVA_BYTE, offset) & 0xFF;
                offset += 1;
                rle.init(body, offset, bitWidth); // positioned at the id stream for lazy skip-decode
            }
            else {
                pagePlainBody = body;
                pagePlainOffset = offset;
            }
            pageFullyDecoded = false;
        }
        else {
            // Whole-page decode + later gather: all binary pages, and nullable plain / FLBA-decimal pages.
            if (pageDict) {
                int bitWidth = body.get(ValueLayout.JAVA_BYTE, offset) & 0xFF;
                offset += 1;
                rle.init(body, offset, bitWidth);
                rle.read(idBuffer, 0, nonNullCount);
                if (kind == Kind.BINARY) {
                    gatherDictionaryBinary(nonNullCount);
                }
                else {
                    gatherDictionary(nonNullCount);
                }
            }
            else if (kind == Kind.BINARY) {
                decodePlainBinary(body, offset, nonNullCount);
            }
            else {
                decodePlain(body, offset, nonNullCount);
            }
            pageFullyDecoded = true;
        }
    }

    private boolean advanceChunk()
    {
        chunkIndex++;
        if (chunkIndex >= chunks.size()) {
            return false;
        }
        Chunk chunk = chunks.get(chunkIndex);
        segment = chunk.segment();
        ColumnMetaData metadata = chunk.metadata();
        long start = metadata.dictionary_page_offset > 0 ? metadata.dictionary_page_offset : metadata.data_page_offset;
        pagePosition = start;
        chunkEnd = start + metadata.total_compressed_size;
        dictionaryInts = null;
        dictionaryLongs = null;
        dictionarySize = 0;
        return true;
    }

    private boolean decodeNextDataPage()
    {
        while (true) {
            if (segment == null || pagePosition >= chunkEnd) {
                if (!advanceChunk()) {
                    return false;
                }
            }
            PageHeader header;
            long bodyPosition;
            try (ParquetFile.SegmentInputStream in = new ParquetFile.SegmentInputStream(segment, pagePosition, chunkEnd - pagePosition)) {
                header = Util.readPageHeader(in);
                bodyPosition = in.position();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Unable to read Parquet page header", e);
            }
            int compressedSize = header.compressed_page_size;
            int uncompressedSize = header.uncompressed_page_size;
            CompressionCodec codec = chunks.get(chunkIndex).metadata().codec;
            long nextPagePosition = bodyPosition + compressedSize;

            if (header.type == PageType.DICTIONARY_PAGE) {
                MemorySegment body = decompress(segment, bodyPosition, compressedSize, uncompressedSize, codec);
                decodeDictionary(body, header.dictionary_page_header.num_values);
                pagePosition = nextPagePosition;
                continue;
            }
            if (header.type == PageType.DATA_PAGE) {
                MemorySegment body = decompress(segment, bodyPosition, compressedSize, uncompressedSize, codec);
                decodeDataPageV1(body, header);
                pagePosition = nextPagePosition;
                return true;
            }
            throw new IllegalStateException("Unsupported page type for NitroParquet: " + header.type);
        }
    }

    /**
     * Decompresses a page body into the reused heap buffer (zero-copy from the off-heap mapping), or, for
     * an uncompressed page, returns a zero-copy slice of the mapping itself. The returned segment is valid
     * only until the next call.
     */
    private MemorySegment decompress(MemorySegment fileSegment, long offset, int compressedSize, int uncompressedSize, CompressionCodec codec)
    {
        // Decode out of an off-heap native buffer: measured faster than a heap byte[] for the RLE/bit-unpack hot
        // loop on this JVM (segment LE_LONG loads + ByteVector.fromMemorySegment beat byte[] VarHandle + fromArray).
        // Snappy decompresses native->native (no heap marshalling); SLACK trailing bytes keep the bit-unpacker's
        // 256-bit reads in bounds.
        if (codec == CompressionCodec.UNCOMPRESSED) {
            long available = fileSegment.byteSize() - offset;
            return fileSegment.asSlice(offset, Math.min(compressedSize + SLACK, available));
        }
        if (codec != CompressionCodec.SNAPPY) {
            throw new IllegalStateException("Unsupported codec for NitroParquet: " + codec);
        }
        MemorySegment source = fileSegment.asSlice(offset, compressedSize);
        if (decompressCapacity < uncompressedSize + SLACK) {
            decompressSegment = scratchArena.allocate(uncompressedSize + SLACK);
            decompressCapacity = uncompressedSize + SLACK;
        }
        snappy.decompress(source, decompressSegment.asSlice(0, uncompressedSize));
        return decompressSegment.asSlice(0, uncompressedSize + SLACK);
    }

    // Bumped on every dictionary page; a binary batch can only be emitted as a DictionaryVector if every page it
    // spans shares one dictionary (i.e. the generation does not change while reading it).
    private int dictionaryGeneration;

    private void decodeDictionary(MemorySegment body, int numValues)
    {
        dictionarySize = numValues;
        dictionaryGeneration++;
        if (kind == Kind.INT) {
            if (dictionaryInts == null || dictionaryInts.length < numValues) {
                dictionaryInts = new int[numValues];
            }
            MemorySegment.copy(body, LE_INT, 0, dictionaryInts, 0, numValues);
        }
        else if (kind == Kind.LONG) {
            if (dictionaryLongs == null || dictionaryLongs.length < numValues) {
                dictionaryLongs = new long[numValues];
            }
            if (flbaDecimal) {
                for (int i = 0; i < numValues; i++) {
                    dictionaryLongs[i] = bigEndianSignedLong(body, (long) i * typeLength, typeLength);
                }
            }
            else {
                MemorySegment.copy(body, LE_LONG, 0, dictionaryLongs, 0, numValues);
            }
        }
        else {
            // BINARY dictionary: numValues entries of [4-byte LE length][bytes].
            if (dictionaryByteOffsets == null || dictionaryByteOffsets.length < numValues + 1) {
                dictionaryByteOffsets = new int[numValues + 1];
            }
            long cursor = 0;
            int total = 0;
            for (int i = 0; i < numValues; i++) {
                int length = body.get(LE_INT, cursor);
                cursor += 4 + length;
                total += length;
            }
            if (dictionaryBytes == null || dictionaryBytes.length < total) {
                dictionaryBytes = new byte[total];
            }
            cursor = 0;
            int out = 0;
            for (int i = 0; i < numValues; i++) {
                int length = body.get(LE_INT, cursor);
                cursor += 4;
                dictionaryByteOffsets[i] = out;
                MemorySegment.copy(body, ValueLayout.JAVA_BYTE, cursor, dictionaryBytes, out, length);
                out += length;
                cursor += length;
            }
            dictionaryByteOffsets[numValues] = out;
            // Snapshot the dictionary as a BinaryVector keyed by this generation. dictionaryBytes/Offsets are reused
            // across chunks, so the snapshot must copy; it is then shared by every DictionaryVector emitted for this
            // chunk (a stable identity downstream dict-aware operators can key on).
            org.weakref.nitro.data.BinaryVector dictionaryVector = new org.weakref.nitro.data.BinaryVector(
                    numValues,
                    Arrays.copyOf(dictionaryByteOffsets, numValues + 1),
                    Arrays.copyOf(dictionaryBytes, out));
            dictionaryVector.addTraits(java.util.Set.of(org.weakref.nitro.data.Utf8Traits.UTF8_STRING));
            dictionaryVectorCache.put(dictionaryGeneration, dictionaryVector);
            // Keep only the few most recent generations. A batch spans at most one chunk boundary (row groups are far
            // larger than a batch), so emission needs the current dictionary and a flat fallback needs at most the one
            // the batch started under. Without this the cache would retain every chunk's dictionary for the life of the
            // scan -- unbounded for a many-column scan over many row groups.
            dictionaryVectorCache.keySet().removeIf(generation -> generation < dictionaryGeneration - 3);
        }
    }

    /** Vectorized dictionary gather: {@code out[outOffset+i] = dict[ids[idOffset+i]]} for {@code i} in [0, n). */
    private static void gatherLongs(long[] dict, int[] ids, int idOffset, long[] out, int outOffset, int n)
    {
        int i = 0;
        int bound = LONG_SPECIES.loopBound(n);
        for (; i < bound; i += LONG_SPECIES.length()) {
            jdk.incubator.vector.LongVector.fromArray(LONG_SPECIES, dict, 0, ids, idOffset + i).intoArray(out, outOffset + i);
        }
        for (; i < n; i++) {
            out[outOffset + i] = dict[ids[idOffset + i]];
        }
    }

    /** Vectorized dictionary gather: {@code out[outOffset+i] = dict[ids[idOffset+i]]} for {@code i} in [0, n). */
    private static void gatherInts(int[] dict, int[] ids, int idOffset, int[] out, int outOffset, int n)
    {
        int i = 0;
        int bound = INT_SPECIES.loopBound(n);
        for (; i < bound; i += INT_SPECIES.length()) {
            jdk.incubator.vector.IntVector.fromArray(INT_SPECIES, dict, 0, ids, idOffset + i).intoArray(out, outOffset + i);
        }
        for (; i < n; i++) {
            out[outOffset + i] = dict[ids[idOffset + i]];
        }
    }

    private static long bigEndianSignedLong(MemorySegment segment, long offset, int length)
    {
        int shift = 64 - (length << 3);
        if (offset + 8 <= segment.byteSize()) {
            // One big-endian word load: the value's `length` bytes are the most significant bytes of the word, so an
            // arithmetic right shift sign-extends them and discards the trailing bytes of the following value.
            return segment.get(BE_LONG, offset) >> shift;
        }
        // Tail within 8 bytes of the segment end: read a byte at a time so the load stays in bounds.
        long value = 0;
        for (int i = 0; i < length; i++) {
            value = (value << 8) | (segment.get(ValueLayout.JAVA_BYTE, offset + i) & 0xFF);
        }
        return value << shift >> shift; // sign-extend from length*8 bits
    }

    private void decodeDataPageV1(MemorySegment body, PageHeader header)
    {
        int valueCount = header.data_page_header.num_values;
        Encoding encoding = header.data_page_header.encoding;
        ensurePageCapacity(valueCount);
        pageValueCount = valueCount;
        pageCursor = 0;

        long offset = 0;
        int nonNullCount = valueCount;
        boolean dictionary = encoding == Encoding.RLE_DICTIONARY || encoding == Encoding.PLAIN_DICTIONARY;
        // A predicate-over-dictionary lead-filter scan over a nullable dict page needs the page-position -> id-index
        // prefix (pageIdIndex), not the per-level array; decode the levels straight into that prefix in one pass.
        boolean filterDictPrefixBuilt = false;
        if (optional) {
            // V1 definition levels: 4-byte LE length prefix, then RLE(bitWidth=1) of `valueCount` levels.
            int defLength = body.get(LE_INT, 0);
            offset = 4;
            rle.init(body, offset, 1);
            // Null-free pages (one RLE run of 1s) are the common case; detect them in O(1) and skip the O(page)
            // per-level materialization + sum. Only pages that actually contain nulls pay the full decode.
            if (!rle.consumeIfAllOnes(valueCount)) {
                rle.init(body, offset, 1);
                if (filterScan && dictionary && kind != Kind.BINARY) {
                    if (pageIdIndex.length < valueCount + 1) {
                        pageIdIndex = new int[valueCount + 1];
                    }
                    nonNullCount = rle.readNonNullPrefix(pageIdIndex, valueCount);
                    filterDictPrefixBuilt = true;
                }
                else {
                    rle.read(defBuffer, 0, valueCount);
                    nonNullCount = 0;
                    for (int i = 0; i < valueCount; i++) {
                        nonNullCount += defBuffer[i];
                    }
                }
            }
            offset += defLength;
        }

        pageFilterDict = false;
        if (dictionary) {
            int bitWidth = body.get(ValueLayout.JAVA_BYTE, offset) & 0xFF;
            offset += 1;
            rle.init(body, offset, bitWidth);
            rle.read(idBuffer, 0, nonNullCount);
            if (filterScan && kind != Kind.BINARY) {
                // Predicate-over-dictionary: keep the ids, don't materialize values. The page-position -> id-index
                // map was built straight from the definition levels above when the page has nulls.
                pageFilterDict = true;
                pageFilterNullable = optional && nonNullCount < valueCount;
                if (pageFilterNullable && !filterDictPrefixBuilt) {
                    if (pageIdIndex.length < valueCount + 1) {
                        pageIdIndex = new int[valueCount + 1];
                    }
                    int running = 0;
                    for (int i = 0; i < valueCount; i++) {
                        pageIdIndex[i] = running;
                        running += defBuffer[i];
                    }
                    pageIdIndex[valueCount] = running;
                }
            }
            else if (kind == Kind.BINARY) {
                scatterDictionaryBinaryIds(nonNullCount);
            }
            else {
                gatherDictionary(nonNullCount);
            }
        }
        else if (encoding == Encoding.PLAIN) {
            if (kind == Kind.BINARY) {
                decodePlainBinary(body, offset, nonNullCount);
                pageBinaryDeferred = false;
            }
            else {
                decodePlain(body, offset, nonNullCount);
            }
        }
        else {
            throw new IllegalStateException("Unsupported data encoding for NitroParquet: " + encoding);
        }
    }

    private void gatherDictionary(int nonNullCount)
    {
        // Fast path when the page has no nulls (common for key/measure columns even when the schema marks them
        // optional): tight gather with ids[i] == position i, no per-position null branch/idCursor.
        boolean nullFree = !optional || nonNullCount == pageValueCount;
        if (kind == Kind.INT) {
            int[] dictionary = dictionaryInts;
            if (nullFree) {
                gatherInts(dictionary, idBuffer, 0, pageInts, 0, nonNullCount);
                if (optional) {
                    Arrays.fill(pageNulls, 0, pageValueCount, false);
                }
            }
            else {
                int idCursor = 0;
                for (int i = 0; i < pageValueCount; i++) {
                    if (defBuffer[i] != 0) {
                        pageInts[i] = dictionary[idBuffer[idCursor++]];
                        pageNulls[i] = false;
                    }
                    else {
                        pageInts[i] = 0;
                        pageNulls[i] = true;
                    }
                }
            }
        }
        else {
            long[] dictionary = dictionaryLongs;
            if (nullFree) {
                gatherLongs(dictionary, idBuffer, 0, pageLongs, 0, nonNullCount);
                if (optional) {
                    Arrays.fill(pageNulls, 0, pageValueCount, false);
                }
            }
            else {
                int idCursor = 0;
                for (int i = 0; i < pageValueCount; i++) {
                    if (defBuffer[i] != 0) {
                        pageLongs[i] = dictionary[idBuffer[idCursor++]];
                        pageNulls[i] = false;
                    }
                    else {
                        pageLongs[i] = 0;
                        pageNulls[i] = true;
                    }
                }
            }
        }
    }

    private void decodePlain(MemorySegment body, long offset, int nonNullCount)
    {
        boolean nullFree = !optional || nonNullCount == pageValueCount;
        if (kind == Kind.INT) {
            if (nullFree) {
                MemorySegment.copy(body, LE_INT, offset, pageInts, 0, nonNullCount);
                if (optional) {
                    Arrays.fill(pageNulls, 0, pageValueCount, false);
                }
            }
            else {
                long cursor = offset;
                for (int i = 0; i < pageValueCount; i++) {
                    if (defBuffer[i] != 0) {
                        pageInts[i] = body.get(LE_INT, cursor);
                        cursor += 4;
                        pageNulls[i] = false;
                    }
                    else {
                        pageInts[i] = 0;
                        pageNulls[i] = true;
                    }
                }
            }
        }
        else if (flbaDecimal) {
            // Short decimal: each value is `typeLength` big-endian signed bytes -> unscaled long.
            int width = typeLength;
            if (nullFree) {
                long cursor = offset;
                for (int i = 0; i < nonNullCount; i++) {
                    pageLongs[i] = bigEndianSignedLong(body, cursor, width);
                    cursor += width;
                }
                if (optional) {
                    Arrays.fill(pageNulls, 0, pageValueCount, false);
                }
            }
            else {
                long cursor = offset;
                for (int i = 0; i < pageValueCount; i++) {
                    if (defBuffer[i] != 0) {
                        pageLongs[i] = bigEndianSignedLong(body, cursor, width);
                        cursor += width;
                        pageNulls[i] = false;
                    }
                    else {
                        pageLongs[i] = 0;
                        pageNulls[i] = true;
                    }
                }
            }
        }
        else {
            if (nullFree) {
                MemorySegment.copy(body, LE_LONG, offset, pageLongs, 0, nonNullCount);
                if (optional) {
                    Arrays.fill(pageNulls, 0, pageValueCount, false);
                }
            }
            else {
                long cursor = offset;
                for (int i = 0; i < pageValueCount; i++) {
                    if (defBuffer[i] != 0) {
                        pageLongs[i] = body.get(LE_LONG, cursor);
                        cursor += 8;
                        pageNulls[i] = false;
                    }
                    else {
                        pageLongs[i] = 0;
                        pageNulls[i] = true;
                    }
                }
            }
        }
    }

    /**
     * Deferred binary-dictionary decode for the full read path: record each position's dictionary id row-aligned in
     * {@code pageDictIds} (sentinel 0 at null positions; nulls in {@code pageNulls}) and mark the page deferred,
     * skipping the eager byte expansion {@link #gatherDictionaryBinary} performs. {@link #readBinary} either wraps
     * these ids as a {@link org.weakref.nitro.data.DictionaryVector} or expands them on a flat fallback.
     */
    private void scatterDictionaryBinaryIds(int nonNullCount)
    {
        if (pageDictIds.length < pageValueCount) {
            pageDictIds = new int[pageValueCount];
        }
        if (!optional || nonNullCount == pageValueCount) {
            // Null-free page: idBuffer is already row-aligned (idBuffer[i] is the id at position i).
            System.arraycopy(idBuffer, 0, pageDictIds, 0, pageValueCount);
            if (optional) {
                Arrays.fill(pageNulls, 0, pageValueCount, false);
            }
        }
        else {
            int idCursor = 0;
            for (int i = 0; i < pageValueCount; i++) {
                if (defBuffer[i] != 0) {
                    pageDictIds[i] = idBuffer[idCursor++];
                    pageNulls[i] = false;
                }
                else {
                    pageDictIds[i] = 0;
                    pageNulls[i] = true;
                }
            }
        }
        pageBinaryDeferred = true;
    }

    private void gatherDictionaryBinary(int nonNullCount)
    {
        pageBytesUsed = 0;
        if (!optional || nonNullCount == pageValueCount) {
            for (int i = 0; i < nonNullCount; i++) {
                pageByteOffsets[i] = pageBytesUsed;
                appendDictionaryEntry(idBuffer[i]);
            }
            pageByteOffsets[nonNullCount] = pageBytesUsed;
            if (optional) {
                Arrays.fill(pageNulls, 0, pageValueCount, false);
            }
        }
        else {
            int idCursor = 0;
            for (int i = 0; i < pageValueCount; i++) {
                pageByteOffsets[i] = pageBytesUsed;
                if (defBuffer[i] != 0) {
                    appendDictionaryEntry(idBuffer[idCursor++]);
                    pageNulls[i] = false;
                }
                else {
                    pageNulls[i] = true;
                }
            }
            pageByteOffsets[pageValueCount] = pageBytesUsed;
        }
    }

    private void appendDictionaryEntry(int id)
    {
        int start = dictionaryByteOffsets[id];
        int length = dictionaryByteOffsets[id + 1] - start;
        ensurePageBytes(pageBytesUsed + length);
        System.arraycopy(dictionaryBytes, start, pageBytes, pageBytesUsed, length);
        pageBytesUsed += length;
    }

    private void decodePlainBinary(MemorySegment body, long offset, int nonNullCount)
    {
        pageBytesUsed = 0;
        long cursor = offset;
        if (!optional || nonNullCount == pageValueCount) {
            for (int i = 0; i < nonNullCount; i++) {
                pageByteOffsets[i] = pageBytesUsed;
                cursor = appendPlainEntry(body, cursor);
            }
            pageByteOffsets[nonNullCount] = pageBytesUsed;
            if (optional) {
                Arrays.fill(pageNulls, 0, pageValueCount, false);
            }
        }
        else {
            for (int i = 0; i < pageValueCount; i++) {
                pageByteOffsets[i] = pageBytesUsed;
                if (defBuffer[i] != 0) {
                    cursor = appendPlainEntry(body, cursor);
                    pageNulls[i] = false;
                }
                else {
                    pageNulls[i] = true;
                }
            }
            pageByteOffsets[pageValueCount] = pageBytesUsed;
        }
    }

    private long appendPlainEntry(MemorySegment body, long cursor)
    {
        int length = body.get(LE_INT, cursor);
        cursor += 4;
        ensurePageBytes(pageBytesUsed + length);
        MemorySegment.copy(body, ValueLayout.JAVA_BYTE, cursor, pageBytes, pageBytesUsed, length);
        pageBytesUsed += length;
        return cursor + length;
    }

    private void ensurePageBytes(int needed)
    {
        if (pageBytes.length < needed) {
            pageBytes = Arrays.copyOf(pageBytes, Math.max(pageBytes.length * 2, needed));
        }
    }

    private void ensurePageCapacity(int valueCount)
    {
        if (kind == Kind.INT) {
            if (pageInts.length < valueCount) {
                pageInts = new int[valueCount];
            }
        }
        else if (kind == Kind.LONG) {
            if (pageLongs.length < valueCount) {
                pageLongs = new long[valueCount];
            }
        }
        else if (pageByteOffsets.length < valueCount + 1) {
            pageByteOffsets = new int[valueCount + 1];
        }
        if (optional && pageNulls.length < valueCount) {
            pageNulls = new boolean[valueCount];
        }
        if (idBuffer.length < valueCount) {
            idBuffer = new int[valueCount];
        }
        if (optional && defBuffer.length < valueCount) {
            defBuffer = new int[valueCount];
        }
        if (optional && runDef.length < valueCount) {
            runDef = new int[valueCount];
        }
    }
}
