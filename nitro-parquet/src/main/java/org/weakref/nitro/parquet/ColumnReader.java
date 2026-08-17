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
import org.weakref.nitro.core.function.VersionedLongPredicate;
import org.weakref.nitro.core.source.LongDomain;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongPredicate;

import static java.util.Objects.requireNonNull;
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
    private final PrimitiveArrayPool arrayPool;

    public enum Kind
    {
        INT, LONG, BINARY
    }

    // Trailing slack so the bit-unpacker's reads never run off the end of a page body: the SIMD kernel
    // loads 32 bytes at a group's start, the scalar tail loads 8.
    private static final int SLACK = 32;

    // Fused dict-filter: for a null-free dict page the lead filter unpacks the ids one L1-resident tile at a time and
    // filters each tile before unpacking the next, instead of the two-pass "unpack the whole page to idBuffer, then
    // re-read idBuffer to filter". Fusing keeps the unpacked ids in L1 between produce and consume, removing the
    // page-sized array round-trip to memory (the dominant cost on the 130M-row lead scan is that re-read, not the
    // filter arithmetic). Opt-out for A/B. TILE fits alongside accept[] + dict in L1 (2048 ints = 8KB).
    // SIMD the fused dict-filter tile compaction via jdk.incubator.vector gather+compress (the Java equivalent of
    // Velox's vpgatherdd processRun). On this JVM/hardware the Vector API gather/compress path loses to the scalar
    // branchless loop for q20/q45, so keep it opt-in for future JDK/hardware experiments.
    // The direct binary reader already needs an allocator-owned ID vector when a batch remains dictionary encoded.
    // Stage page IDs in that vector immediately instead of copying them through reader scratch and then copying the
    // completed batch a second time. A batch that encounters a plain page returns the speculative ID vector to the
    // pool when it switches to flat output. Opt-out retains the old two-copy path for focused counter controls.
    // A short dictionary stream cannot amortize changing the allocator/pool lifetime of its mapping buffer, and
    // small dimension scans are especially vulnerable to unrelated heap-placement movement in a much larger query.
    // Admit the one-copy representation only after this reader has established a durable dictionary-only horizon.
    // Selective decoding often produces tiny adjacent runs. Foreign-memory bulk copy bottoms out in libc memcpy;
    // for those runs its setup costs more than JIT-inlined scalar loads. This is a reader-wide policy, with an A/B
    // property, rather than a query or physical-column-shape specialization.
    private static final int FILTER_TILE = 2048;
    private static final int[] EMPTY_INTS = new int[0];
    private static final long[] EMPTY_LONGS = new long[0];
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];
    private int[] filterTile;
    private long directOwnedDictionaryRowsObserved;

    // Compaction strategy for a bit-packed (heterogeneous) dict run, chosen per chunk from the accepted-entry fraction:
    //   - branchy   (skip the value gather + two stores for a rejected row) when the filter is selective: the per-row
    //                accept branch is well-predicted, and skipping the ~90% rejected rows' work dominates.
    //   - branchless (always stage value/position, advance the survivor cursor via `sc += accept ? 1 : 0`) when a
    //                larger fraction survives: survivors are scattered, the accept branch mispredicts, and removing it
    //                collapses the mispredict stall (e.g. TPC-DS q20's cs_sold_date_sk scan, ~14% surviving, -24%).
    // The crossover sits between the measured branchy-favoring queries (accepted fraction <= 0.09) and q20 (0.144);
    // 1/9 puts the boundary at ~0.111 with symmetric margin. Below the threshold a query keeps the branchy path, so a
    // misestimate near the boundary only trades ~equal costs.
    // The fused definition/ID cursor pays only for a durable, physically narrow regime: sparse nulls make almost
    // every ordinary definition tile mixed, wide IDs make the avoided scratch pass expensive, and a small but
    // nonzero acceptance set rules out the cheaper zero-acceptance cursor. Broader admission wins wall time while
    // regressing aggregate cache/TLB work, so delay the decision until the reader has observed a stable row horizon.
    private static final long DIRECT_NULLABLE_DICTIONARY_FILTER_MIN_OBSERVED_ROWS = 1L << 20;
    private static final int DIRECT_NULLABLE_DICTIONARY_FILTER_MIN_PRESENT_PERCENT = 99;
    private static final int DIRECT_NULLABLE_DICTIONARY_FILTER_MAX_ACCEPTED_DENOMINATOR = 50;
    // A null-free fixed-width page does not need a page-sized value array between the encoded page and the caller's
    // batch array. Keep dictionary IDs (or the plain body) live across batch slices and materialize each slice
    // directly into its final array. Nullable pages retain the row-aligned page representation because their dense
    // value stream must still be scattered around null positions; selected readers retain their independent path.

    private record Chunk(MemorySegment segment, ColumnMetaData metadata, long rowCount, DecompressedPageCache.Source source) {}

    // Dictionary materialization is a pure gather (out[i] = dictionary[ids[i]]); a Vector-API gather (hardware
    // vpgather) measurably beats the scalar loop on the dict-heavy scans (q82 -3%, q24/q50 -1.6%, byte-identical).
    private static final jdk.incubator.vector.VectorSpecies<Long> LONG_SPECIES = jdk.incubator.vector.LongVector.SPECIES_PREFERRED;
    private static final jdk.incubator.vector.VectorSpecies<Integer> INT_SPECIES = jdk.incubator.vector.IntVector.SPECIES_PREFERRED;

    private final Kind kind;
    private final boolean optional;
    private final Type physicalType;
    private final DecompressedPageCache decompressedPages;
    private final boolean flbaDecimal;
    private final int typeLength;
    private final List<Chunk> chunks = new ArrayList<>();
    private int cachedDictionarySize = Integer.MIN_VALUE;
    private long zeroAcceptedDictionaryRowsObserved;

    private final ParquetReaderPolicy readerPolicy;
    private final RleReaderPolicy rleReaderPolicy;
    private final ParquetPageNavigationPolicy pageNavigationPolicy;
    private final ParquetReaderDiagnostics diagnostics;
    private final ParquetMaterializationPolicy materializationPolicy;
    private final ParquetNumericDecodePolicy numericDecodePolicy;
    private final ParquetDictionaryFilterPolicy dictionaryFilterPolicy;
    private final ParquetArenaPolicy arenaPolicy;
    private final RleReader rle;
    // Skip path: stream the definition levels rather than materializing a per-page prefix. defRle co-advances with
    // the id reader `rle` — skipCountingOnes(gap) returns the non-nulls in a gap (O(1) per RLE run) so `rle` skips
    // exactly that many ids, and readRunCountingOnes decodes only a survivor run's levels (O(1) when the run lies
    // within one RLE run). Mirrors Trino's SkipFlatColumnReader.readSelectedNullable; no pageIdIndex prefix is built.
    private final RleReader defRle;
    private boolean pageDefStreaming;
    private int defPageCursor;
    private int[] runDef = EMPTY_INTS;
    // Streaming skip for nullable PLAIN (and FLBA-decimal) pages: defRle co-advances and plainValueCursor tracks the
    // running non-null count, which is the index into the densely-stored plain value body (nulls store no value).
    private boolean pagePlainStreaming;
    private int plainValueCursor;
    // Whole-page bulk decode + gather beats per-survivor-run skip-decode only when a page is BOTH dense (lots survive,
    // so bulk decodes little extra) AND fragmented into short runs (skip would pay its per-run RLE-skip cost many
    // times). A dense-but-contiguous page (one long run, e.g. a date-clustered range) stays on the cheap skip path.
    private boolean pageForceWholeDecode;

    // reusable buffers (grow to high-water mark). The decompression target is an off-heap segment from a
    // confined arena so the snappy FFM downcall is native->native (no heap marshalling); the SIMD unpacker
    // loads vectors straight from it via ByteVector.fromMemorySegment.
    private final Arena scratchArena;
    private final boolean ownsScratchArena;
    private final Decompressor snappy = SnappyDecompressor.create();
    private MemorySegment decompressSegment;
    private long decompressCapacity;
    private long compressedPageCount;
    private long compressedPageBytes;
    private long uncompressedPageBytes;
    private long consumedPageBytes;
    private int minimumCompressedPageBytes = Integer.MAX_VALUE;
    private int maximumCompressedPageBytes;
    private int[] idBuffer = EMPTY_INTS;
    // Fused per-page skip-decode of a dict LONG column: the whole survivor gather (def-null walk + value read) runs as
    // one pass with both RLE cursors in locals, no per-run idBuffer/runDef materialization. These capture the page's
    // value-run + def-run offsets so {@link JavaSkipDecode} can gather straight from the page body.
    private MemorySegment skipBody;
    private long skipValueOffset;
    private long skipDefOffset;   // -1 => null-free page
    private int skipBitWidth;
    // int[] mirror of the per-chunk boolean acceptById[] (the Vector API gathers from int[]/long[], not boolean[]).
    private int[] filterAcceptInts = EMPTY_INTS;
    private int filterAcceptIntsChunk = -1;
    // Compact numeric mirror for scalar compaction: unlike boolean[], byte values can advance an output index without
    // a conditional, while touching one quarter of the acceptance-table footprint of the SIMD int[] mirror.
    private byte[] filterAcceptBytes = EMPTY_BYTES;
    private int filterAcceptBytesChunk = -1;
    private int[] defBuffer = EMPTY_INTS;
    // reusable accumulators for assembling a batch's BinaryVector across pages
    private int[] binaryOutOffsets = EMPTY_INTS;
    private byte[] binaryOutData = EMPTY_BYTES;
    private int[] binaryBatchIds = EMPTY_INTS;
    // Selected binary-dictionary decode records a page's row-aligned dictionary ids here (sentinel 0 at null
    // positions; nulls live in pageNulls). Full sequential reads stream the encoded ids directly into batch scratch.
    private int[] pageDictIds = EMPTY_INTS;
    private boolean pageBinaryDeferred;
    // Full, sequential binary reads consume dictionary ids and nullable definition levels directly from their RLE
    // streams in output-batch-sized pieces. Keeping page-wide row-aligned ids for every projected binary column
    // multiplies one large Parquet page by the number of columns and concurrent splits; the selected-read path still
    // uses pageDictIds because it needs random positions within a page.
    private boolean pageBinaryDictionaryStreaming;
    private boolean pageBinaryDictionaryNullFree;
    // The chunk dictionary materialized as a BinaryVector, cached by generation so a DictionaryVector can wrap a
    // stable instance across batches and a flat fallback can expand ids of an earlier generation after a chunk change.
    private final java.util.HashMap<Integer, org.weakref.nitro.data.BinaryVector> dictionaryVectorCache = new java.util.HashMap<>();
    private final java.util.IdentityHashMap<org.weakref.nitro.data.BinaryVector, Boolean> escapedBinaryDictionaries = new java.util.IdentityHashMap<>();

    // current chunk dictionary
    private int[] dictionaryInts;
    private long[] dictionaryLongs;
    private byte[] dictionaryBytes;        // BINARY: concatenated dictionary entry bytes
    private int[] dictionaryByteOffsets;   // BINARY: entry i = dictionaryBytes[offsets[i], offsets[i+1])
    private byte[] reusableDictionaryBytes = EMPTY_BYTES;
    private int dictionarySize;

    // current page, fully decoded into these (nulls scattered in place)
    private int[] pageInts = EMPTY_INTS;
    private long[] pageLongs = EMPTY_LONGS;
    private boolean[] pageNulls = EMPTY_BOOLEANS;
    private byte[] pageBytes = EMPTY_BYTES;          // BINARY: concatenated value bytes
    private int[] pageByteOffsets = EMPTY_INTS;      // BINARY: value i = pageBytes[offsets[i], offsets[i+1])
    private int pageBytesUsed;
    private int pageValueCount;
    private int pageCursor;
    private boolean directFullDecodeRequested;
    private boolean fullSequentialReadRequested;
    private boolean pageDirectDictionary;
    private boolean pageDirectPlain;
    private MemorySegment pageDirectPlainBody;
    private long pageDirectPlainOffset;
    private long directDictionaryRows;
    private long directPlainRows;
    private boolean directNumericBatchDecodeEnabled;

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
    private boolean pageNumericDictionaryIdsDecoded;
    // pageIdIndex[p] = non-nulls before page position p. Built only on the predicate-over-dictionary lead-filter path
    // (decodeDataPageV1), which tests every position; the skip path streams definition levels instead (see defRle).
    private int[] pageIdIndex = EMPTY_INTS;

    // Predicate-over-dictionary filtering (lead DF column): when filterScan is set, a dict data page keeps its ids
    // (no value materialization) and a per-chunk acceptById[] is tested over the ids -- only survivors are
    // materialized. Mirrors Trino's filterBlock over a DictionaryBlock. pageFilterDict marks a kept-ids page.
    private boolean filterScan;
    private boolean pageFilterDict;
    private boolean pageFilterNullable;
    // Nullable counterpart to pageFilterFused: definition levels and dictionary ids remain as independent RLE
    // streams and are consumed together in L1-sized tiles. This avoids constructing and rereading pageIdIndex.
    private boolean pageFilterNullableFused;
    // Fused null-free dict-filter page: the ids were NOT unpacked to idBuffer; the RleReader is left positioned at the
    // id stream and the filter loop tile-reads it. Requires forward-only, in-lockstep consumption of pageCursor.
    private boolean pageFilterFused;
    private boolean[] acceptById = EMPTY_BOOLEANS;
    private int acceptByIdChunk = -1;
    private VersionedLongPredicate acceptByIdPredicate;
    private long acceptByIdPredicateGeneration = -1;
    private boolean versionedDictionaryPredicateReuseReported;
    private int acceptedCount;
    private long dictionaryFilterRowsObserved;
    private long dictionaryFilterRowsAccepted;
    private long directNullableDictionaryFilterRows;
    private long nullableDictionaryFilterRowsObserved;
    private long nullableDictionaryFilterNonNullRowsObserved;
    private MemorySegment pagePlainBody;
    private long pagePlainOffset;
    private int pageValueCursor;

    // peeked-but-not-yet-decompressed data page (skip-decode page-skip): lets readSelected decide whether to
    // decompress a page or skip it entirely (no decompress) when no survivor falls in its row range.
    private long pendingBodyPosition;
    private int pendingCompressedSize;
    private int pendingUncompressedSize;
    private long pendingNextPosition;
    private int pendingNumValues;
    private int pendingEncoding;

    private final FastPageHeaderReader fastPageHeaderReader = new FastPageHeaderReader();
    private int parsedPageType;
    private int parsedUncompressedSize;
    private int parsedCompressedSize;
    private int parsedValueCount;
    private int parsedEncoding;
    private boolean pageHeaderModeDecided;
    private boolean useFastPageHeader;

    public ColumnReader(
            Type physicalType,
            boolean optional,
            int typeLength,
            boolean decimal,
            DecompressedPageCache decompressedPages,
            PrimitiveArrayPool arrayPool,
            ParquetReaderPolicy readerPolicy)
    {
        this(
                physicalType,
                optional,
                typeLength,
                decimal,
                decompressedPages,
                arrayPool,
                readerPolicy,
                ParquetArenaPolicy.confined());
    }

    ColumnReader(
            Type physicalType,
            boolean optional,
            int typeLength,
            boolean decimal,
            DecompressedPageCache decompressedPages,
            PrimitiveArrayPool arrayPool,
            ParquetReaderPolicy readerPolicy,
            ParquetArenaPolicy arenaPolicy)
    {
        this(
                physicalType,
                optional,
                typeLength,
                decimal,
                decompressedPages,
                arrayPool,
                readerPolicy,
                arenaPolicy,
                arenaPolicy.createArena(),
                true);
    }

    ColumnReader(
            Type physicalType,
            boolean optional,
            int typeLength,
            boolean decimal,
            DecompressedPageCache decompressedPages,
            PrimitiveArrayPool arrayPool,
            ParquetReaderPolicy readerPolicy,
            ParquetArenaPolicy arenaPolicy,
            Arena scratchArena)
    {
        this(
                physicalType,
                optional,
                typeLength,
                decimal,
                decompressedPages,
                arrayPool,
                readerPolicy,
                arenaPolicy,
                scratchArena,
                false);
    }

    private ColumnReader(
            Type physicalType,
            boolean optional,
            int typeLength,
            boolean decimal,
            DecompressedPageCache decompressedPages,
            PrimitiveArrayPool arrayPool,
            ParquetReaderPolicy readerPolicy,
            ParquetArenaPolicy arenaPolicy,
            Arena scratchArena,
            boolean ownsScratchArena)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        this.readerPolicy = requireNonNull(readerPolicy, "readerPolicy is null");
        this.arenaPolicy = requireNonNull(arenaPolicy, "arenaPolicy is null");
        this.scratchArena = requireNonNull(scratchArena, "scratchArena is null");
        this.ownsScratchArena = ownsScratchArena;
        this.decompressSegment = scratchArena.allocate(0);
        this.rleReaderPolicy = readerPolicy.rle();
        this.pageNavigationPolicy = readerPolicy.pageNavigation();
        this.diagnostics = readerPolicy.diagnostics();
        this.materializationPolicy = readerPolicy.materialization();
        this.numericDecodePolicy = readerPolicy.numericDecode();
        this.dictionaryFilterPolicy = readerPolicy.dictionaryFilter();
        this.rle = new RleReader(rleReaderPolicy);
        this.defRle = new RleReader(rleReaderPolicy);
        this.physicalType = physicalType;
        this.optional = optional;
        this.typeLength = typeLength;
        this.flbaDecimal = physicalType == Type.FIXED_LEN_BYTE_ARRAY && decimal;
        this.decompressedPages = decompressedPages;
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

    public void addChunk(MemorySegment fileSegment, ColumnMetaData metadata, long rowCount)
    {
        addChunk(fileSegment, metadata, rowCount, null);
    }

    public void addChunk(MemorySegment fileSegment, ColumnMetaData metadata, long rowCount, DecompressedPageCache.Source source)
    {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount is negative");
        }
        chunks.add(new Chunk(fileSegment, metadata, rowCount, source));
        cachedDictionarySize = Integer.MIN_VALUE;
    }

    /**
     * Creates an independent reader over the same immutable column chunks. The sibling has its own cursor and
     * decode scratch, allowing a scan to consume definition levels without disturbing its value reader. No page,
     * dictionary, or decompression state is shared.
     */
    public ColumnReader newSibling()
    {
        ColumnReader sibling;
        if (ownsScratchArena) {
            sibling = new ColumnReader(
                    physicalType,
                    optional,
                    typeLength,
                    flbaDecimal,
                    decompressedPages,
                    arrayPool,
                    readerPolicy,
                    arenaPolicy);
        }
        else {
            sibling = new ColumnReader(
                    physicalType,
                    optional,
                    typeLength,
                    flbaDecimal,
                    decompressedPages,
                    arrayPool,
                    readerPolicy,
                    arenaPolicy,
                    scratchArena);
        }
        for (Chunk chunk : chunks) {
            sibling.addChunk(chunk.segment(), chunk.metadata(), chunk.rowCount(), chunk.source());
        }
        return sibling;
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
        if (diagnostics.directNumericBatchDecode() && directDictionaryRows + directPlainRows > 0) {
            System.err.printf(
                    "[direct-numeric-batch-decode] kind=%s optional=%s dictionaryRows=%d plainRows=%d%n",
                    kind,
                    optional,
                    directDictionaryRows,
                    directPlainRows);
        }
        if (diagnostics.dictionaryFilterSummary() && dictionaryFilterRowsObserved > 0) {
            System.err.printf(
                    "[dictionary-filter] kind=%s optional=%s rows=%d acceptedRows=%d directRows=%d lastDictionary=%d lastAccepted=%d branchless=%s%n",
                    kind,
                    optional,
                    dictionaryFilterRowsObserved,
                    dictionaryFilterRowsAccepted,
                    directNullableDictionaryFilterRows,
                    dictionarySize,
                    acceptedCount,
                    shouldUseBranchlessCompaction());
        }
        arrayPool.release(filterTile);
        filterTile = null;
        arrayPool.release(runDef);
        runDef = EMPTY_INTS;
        arrayPool.release(idBuffer);
        idBuffer = EMPTY_INTS;
        arrayPool.release(filterAcceptInts);
        filterAcceptInts = EMPTY_INTS;
        arrayPool.release(filterAcceptBytes);
        filterAcceptBytes = EMPTY_BYTES;
        arrayPool.release(defBuffer);
        defBuffer = EMPTY_INTS;
        arrayPool.release(binaryOutOffsets);
        binaryOutOffsets = EMPTY_INTS;
        arrayPool.release(binaryOutData);
        binaryOutData = EMPTY_BYTES;
        arrayPool.release(binaryBatchIds);
        binaryBatchIds = EMPTY_INTS;
        arrayPool.release(pageDictIds);
        pageDictIds = EMPTY_INTS;
        arrayPool.release(dictionaryInts);
        dictionaryInts = null;
        arrayPool.release(dictionaryLongs);
        dictionaryLongs = null;
        arrayPool.release(pageInts);
        pageInts = EMPTY_INTS;
        arrayPool.release(pageLongs);
        pageLongs = EMPTY_LONGS;
        arrayPool.release(pageNulls);
        pageNulls = EMPTY_BOOLEANS;
        arrayPool.release(pageBytes);
        pageBytes = EMPTY_BYTES;
        arrayPool.release(pageByteOffsets);
        pageByteOffsets = EMPTY_INTS;
        arrayPool.release(pageIdIndex);
        pageIdIndex = EMPTY_INTS;
        arrayPool.release(acceptById);
        acceptById = EMPTY_BOOLEANS;
        if (materializationPolicy.recycleBinaryDictionaryScratch()) {
            for (org.weakref.nitro.data.BinaryVector dictionary : dictionaryVectorCache.values()) {
                if (!escapedBinaryDictionaries.containsKey(dictionary)) {
                    arrayPool.release(dictionary.offsets());
                    arrayPool.release(dictionary.data());
                }
            }
            arrayPool.release(reusableDictionaryBytes);
        }
        dictionaryVectorCache.clear();
        escapedBinaryDictionaries.clear();
        dictionaryByteOffsets = null;
        dictionaryBytes = null;
        reusableDictionaryBytes = EMPTY_BYTES;
        if (ownsScratchArena) {
            scratchArena.close();
        }
    }

    /**
     * Ends the public batch that may have exposed an immutable binary dictionary. Generations older than the
     * decoder's current dictionary can no longer be referenced by a future batch; after the consumer closes the
     * current batch they are also no longer referenced downstream, so their primitive storage becomes reader-owned
     * capacity-ceiling scratch for a later chunk.
     */
    public void finishBatch()
    {
        if (!materializationPolicy.recycleBinaryDictionaryScratch() || kind != Kind.BINARY || dictionaryVectorCache.size() <= 1) {
            return;
        }
        java.util.Iterator<java.util.Map.Entry<Integer, org.weakref.nitro.data.BinaryVector>> iterator =
                dictionaryVectorCache.entrySet().iterator();
        while (iterator.hasNext()) {
            java.util.Map.Entry<Integer, org.weakref.nitro.data.BinaryVector> entry = iterator.next();
            if (entry.getKey() >= dictionaryGeneration) {
                continue;
            }
            org.weakref.nitro.data.BinaryVector dictionary = entry.getValue();
            if (!escapedBinaryDictionaries.containsKey(dictionary)) {
                arrayPool.release(dictionary.offsets());
                reusableDictionaryBytes = retainLarger(reusableDictionaryBytes, dictionary.data());
            }
            escapedBinaryDictionaries.remove(dictionary);
            iterator.remove();
        }
    }

    /** Marks reader-owned dictionary values that crossed the batch ownership boundary as non-recyclable. */
    public void markDictionaryValuesEscaped(org.weakref.nitro.data.Vector vector)
    {
        if (!materializationPolicy.recycleBinaryDictionaryScratch() || kind != Kind.BINARY) {
            return;
        }
        org.weakref.nitro.data.Vector current = vector;
        while (current instanceof org.weakref.nitro.data.DictionaryVector dictionary) {
            current = dictionary.values();
            if (current instanceof org.weakref.nitro.data.BinaryVector binary && dictionaryVectorCache.containsValue(binary)) {
                escapedBinaryDictionaries.put(binary, Boolean.TRUE);
            }
        }
    }

    public boolean optional()
    {
        return optional;
    }

    /** Enables direct null-free fixed-width page materialization for an execution-admitted scan group. */
    public void enableDirectNumericBatchDecode()
    {
        directNumericBatchDecodeEnabled = true;
    }

    /**
     * Returns whether every physical source behind this reader is registered by at least {@code minimumConsumers}
     * independent readers.
     * This is consulted only after the complete operator tree has been constructed, so registration is complete.
     */
    public boolean hasRepeatedSource(int minimumConsumers)
    {
        if (minimumConsumers < 2) {
            throw new IllegalArgumentException("minimumConsumers must be at least 2");
        }
        if (decompressedPages == null || chunks.isEmpty()) {
            return false;
        }
        for (Chunk chunk : chunks) {
            if (chunk.source() == null || decompressedPages.consumerCount(chunk.source()) < minimumConsumers) {
                return false;
            }
        }
        return true;
    }

    /** Fill {@code count} INT values into {@code out}; nulls (if any) into {@code nullsOut} (may be null when none). */
    public void readInts(int[] out, boolean[] nullsOut, int count)
    {
        int produced = 0;
        while (produced < count) {
            if (pageCursor >= pageValueCount) {
                directFullDecodeRequested = numericDecodePolicy.directBatchDecode() && directNumericBatchDecodeEnabled;
                fullSequentialReadRequested = true;
                try {
                    if (!decodeNextDataPage()) {
                        throw new IllegalStateException("Ran out of Parquet values: needed " + count + ", got " + produced);
                    }
                }
                finally {
                    directFullDecodeRequested = false;
                    fullSequentialReadRequested = false;
                }
            }
            int n = Math.min(pageValueCount - pageCursor, count - produced);
            if (pageDefStreaming) {
                readStreamingNullableDictionaryInts(out, produced, n, nullsOut);
            }
            else if (pagePlainStreaming) {
                readStreamingNullablePlainInts(out, produced, n, nullsOut);
            }
            else if (pageDirectDictionary) {
                gatherInts(dictionaryInts, idBuffer, pageCursor, out, produced, n);
            }
            else if (pageDirectPlain) {
                MemorySegment.copy(pageDirectPlainBody, LE_INT, pageDirectPlainOffset + (long) pageCursor * Integer.BYTES, out, produced, n);
            }
            else {
                System.arraycopy(pageInts, pageCursor, out, produced, n);
            }
            if (nullsOut != null && (pageDirectDictionary || pageDirectPlain)) {
                Arrays.fill(nullsOut, produced, produced + n, false);
            }
            else if (nullsOut != null && !pageDefStreaming && !pagePlainStreaming) {
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
            if (pageCursor >= pageValueCount) {
                directFullDecodeRequested = numericDecodePolicy.directBatchDecode() && directNumericBatchDecodeEnabled;
                fullSequentialReadRequested = true;
                try {
                    if (!decodeNextDataPage()) {
                        throw new IllegalStateException("Ran out of Parquet values: needed " + count + ", got " + produced);
                    }
                }
                finally {
                    directFullDecodeRequested = false;
                    fullSequentialReadRequested = false;
                }
            }
            int n = Math.min(pageValueCount - pageCursor, count - produced);
            if (pageDefStreaming) {
                readStreamingNullableDictionaryLongs(out, produced, n, nullsOut);
            }
            else if (pagePlainStreaming) {
                readStreamingNullablePlainLongs(out, produced, n, nullsOut);
            }
            else if (pageDirectDictionary) {
                gatherLongs(dictionaryLongs, idBuffer, pageCursor, out, produced, n);
            }
            else if (pageDirectPlain) {
                MemorySegment.copy(pageDirectPlainBody, LE_LONG, pageDirectPlainOffset + (long) pageCursor * Long.BYTES, out, produced, n);
            }
            else {
                System.arraycopy(pageLongs, pageCursor, out, produced, n);
            }
            if (nullsOut != null && (pageDirectDictionary || pageDirectPlain)) {
                Arrays.fill(nullsOut, produced, produced + n, false);
            }
            else if (nullsOut != null && !pageDefStreaming && !pagePlainStreaming) {
                System.arraycopy(pageNulls, pageCursor, nullsOut, produced, n);
            }
            pageCursor += n;
            produced += n;
        }
    }

    private void readStreamingNullableDictionaryInts(int[] out, int outputOffset, int count, boolean[] nullsOut)
    {
        ensureRunDefCapacity(count);
        int nonNullCount = defRle.readRunCountingOnes(runDef, count);
        if (nonNullCount == count) {
            ensureIdCapacity(count);
            rle.read(idBuffer, 0, count);
            gatherInts(dictionaryInts, idBuffer, 0, out, outputOffset, count);
            if (nullsOut != null) {
                Arrays.fill(nullsOut, outputOffset, outputOffset + count, false);
            }
        }
        else if (nonNullCount == 0) {
            Arrays.fill(out, outputOffset, outputOffset + count, 0);
            if (nullsOut != null) {
                Arrays.fill(nullsOut, outputOffset, outputOffset + count, true);
            }
        }
        else {
            materializeStreamingRunInt(out, outputOffset, count, nonNullCount, nullsOut, dictionaryInts);
        }
        defPageCursor += count;
    }

    private void readStreamingNullableDictionaryLongs(long[] out, int outputOffset, int count, boolean[] nullsOut)
    {
        ensureRunDefCapacity(count);
        int nonNullCount = defRle.readRunCountingOnes(runDef, count);
        if (nonNullCount == count) {
            ensureIdCapacity(count);
            rle.read(idBuffer, 0, count);
            gatherLongs(dictionaryLongs, idBuffer, 0, out, outputOffset, count);
            if (nullsOut != null) {
                Arrays.fill(nullsOut, outputOffset, outputOffset + count, false);
            }
        }
        else if (nonNullCount == 0) {
            Arrays.fill(out, outputOffset, outputOffset + count, 0);
            if (nullsOut != null) {
                Arrays.fill(nullsOut, outputOffset, outputOffset + count, true);
            }
        }
        else {
            materializeStreamingRunLong(out, outputOffset, count, nonNullCount, nullsOut, dictionaryLongs);
        }
        defPageCursor += count;
    }

    private void readStreamingNullablePlainInts(int[] out, int outputOffset, int count, boolean[] nullsOut)
    {
        ensureRunDefCapacity(count);
        int nonNullCount = defRle.readRunCountingOnes(runDef, count);
        if (nonNullCount == count) {
            copyPlainInts(plainValueCursor, out, outputOffset, count);
            plainValueCursor += count;
            if (nullsOut != null) {
                Arrays.fill(nullsOut, outputOffset, outputOffset + count, false);
            }
        }
        else if (nonNullCount == 0) {
            Arrays.fill(out, outputOffset, outputOffset + count, 0);
            if (nullsOut != null) {
                Arrays.fill(nullsOut, outputOffset, outputOffset + count, true);
            }
        }
        else {
            materializeStreamingPlainRunInt(out, outputOffset, count, nullsOut);
        }
        defPageCursor += count;
    }

    private void readStreamingNullablePlainLongs(long[] out, int outputOffset, int count, boolean[] nullsOut)
    {
        ensureRunDefCapacity(count);
        int nonNullCount = defRle.readRunCountingOnes(runDef, count);
        if (nonNullCount == count) {
            readPlainLongsAt(out, outputOffset, plainValueCursor, count);
            plainValueCursor += count;
            if (nullsOut != null) {
                Arrays.fill(nullsOut, outputOffset, outputOffset + count, false);
            }
        }
        else if (nonNullCount == 0) {
            Arrays.fill(out, outputOffset, outputOffset + count, 0);
            if (nullsOut != null) {
                Arrays.fill(nullsOut, outputOffset, outputOffset + count, true);
            }
        }
        else {
            materializeStreamingPlainRunLong(out, outputOffset, count, nullsOut);
        }
        defPageCursor += count;
    }

    /**
     * Reads only SQL nullness from definition levels. This reader instance must be dedicated to the null stream: it
     * advances past value pages without decoding dictionary ids or gathering values, so it cannot subsequently serve
     * value reads at the same cursor. A scan uses a sibling reader when values and a direct null mask may both be
     * requested.
     */
    public void readNulls(boolean[] out, int count)
    {
        if (!optional) {
            Arrays.fill(out, 0, count, false);
            skipNulls(count);
            return;
        }
        int produced = 0;
        while (produced < count) {
            if (pageCursor >= pageValueCount && !decodeNextNullDataPage()) {
                throw new IllegalStateException("Ran out of Parquet nulls: needed " + count + ", got " + produced);
            }
            int positions = Math.min(pageValueCount - pageCursor, count - produced);
            System.arraycopy(pageNulls, pageCursor, out, produced, positions);
            pageCursor += positions;
            produced += positions;
        }
    }

    /**
     * Advances a dedicated null-only reader by {@code count} positions while retaining the requested nullness in
     * {@code mask}. Unlike {@link #readNulls}, this compacts directly into the mask's owned position buffer and does
     * not materialize a batch-sized Boolean stream. The input mask may be dense or sparse; its selected positions
     * must use the same {@code count}-position batch domain as this reader.
     */
    public void retainNulls(Mask mask, boolean selectNulls, int count)
    {
        if (mask.size() != count) {
            throw new IllegalArgumentException("Mask domain does not match null-read count");
        }
        if (mask.none()) {
            skipNulls(count);
            return;
        }
        if (!optional) {
            skipNulls(count);
            if (selectNulls) {
                mask.clear(count);
            }
            return;
        }

        int selectedCount = mask.selectedCount();
        boolean dense = mask.all();
        int[] selected = dense ? null : mask.selectedPositions();
        int[] retainedPositions = dense ? mask.positionsArrayForOverwrite(selectedCount) : selected;
        int selectedIndex = 0;
        int retained = 0;
        int batchCursor = 0;
        while (batchCursor < count) {
            if (pageCursor >= pageValueCount && !decodeNextNullDataPage()) {
                throw new IllegalStateException("Ran out of Parquet nulls: needed " + count + ", got " + batchCursor);
            }
            int positions = Math.min(pageValueCount - pageCursor, count - batchCursor);
            int batchEnd = batchCursor + positions;
            if (dense) {
                for (int position = 0; position < positions; position++) {
                    if (pageNulls[pageCursor + position] == selectNulls) {
                        retainedPositions[retained++] = batchCursor + position;
                    }
                }
            }
            else {
                while (selectedIndex < selectedCount && selected[selectedIndex] < batchEnd) {
                    int position = selected[selectedIndex++];
                    if (pageNulls[pageCursor + position - batchCursor] == selectNulls) {
                        retainedPositions[retained++] = position;
                    }
                }
            }
            pageCursor += positions;
            batchCursor = batchEnd;
        }
        mask.finishRetain(retained);
    }

    /** Advances a dedicated null-only reader without decoding dictionary ids or values. */
    public void skipNulls(long count)
    {
        if (pageCursor < pageValueCount) {
            int positions = (int) Math.min(pageValueCount - pageCursor, count);
            pageCursor += positions;
            count -= positions;
        }
        if (pageNavigationPolicy.skipWholeChunks()) {
            count = skipWholeChunks(count);
        }
        while (count > 0) {
            if (!peekNextDataPage()) {
                throw new IllegalStateException("Ran out of Parquet nulls (skip): still needed " + count);
            }
            if (pendingNumValues <= count) {
                pagePosition = pendingNextPosition;
                count -= pendingNumValues;
                continue;
            }
            CompressionCodec codec = chunks.get(chunkIndex).metadata().codec;
            MemorySegment body = decompress(segment, pendingBodyPosition, pendingCompressedSize, pendingUncompressedSize, codec);
            decodeNullDataPageV1(body, pendingNumValues);
            pagePosition = pendingNextPosition;
            pageCursor = (int) count;
            count = 0;
        }
    }

    private boolean decodeNextNullDataPage()
    {
        while (true) {
            if (segment == null || pagePosition >= chunkEnd) {
                if (!advanceChunk()) {
                    return false;
                }
            }
            long bodyPosition = readPageHeader(segment, pagePosition, chunkEnd);
            long nextPagePosition = bodyPosition + parsedCompressedSize;
            if (parsedPageType == PageType.DICTIONARY_PAGE.getValue()) {
                // Nullness is encoded only in each data page's definition levels; the dictionary body is irrelevant.
                pagePosition = nextPagePosition;
                continue;
            }
            if (parsedPageType != PageType.DATA_PAGE.getValue()) {
                throw new IllegalStateException("Unsupported page type for null-only read: " + parsedPageType);
            }
            CompressionCodec codec = chunks.get(chunkIndex).metadata().codec;
            MemorySegment body = decompress(segment, bodyPosition, parsedCompressedSize, parsedUncompressedSize, codec);
            decodeNullDataPageV1(body, parsedValueCount);
            pagePosition = nextPagePosition;
            return true;
        }
    }

    private void decodeNullDataPageV1(MemorySegment body, int valueCount)
    {
        pageValueCount = valueCount;
        pageCursor = 0;
        ensurePageNullCapacity(valueCount);
        if (!optional) {
            Arrays.fill(pageNulls, 0, valueCount, false);
            return;
        }

        // V1 definition levels begin with their byte length and use the bit-width-one hybrid encoding.
        rle.init(body, Integer.BYTES, 1);
        if (rle.consumeIfAllOnes(valueCount)) {
            Arrays.fill(pageNulls, 0, valueCount, false);
            return;
        }
        rle.init(body, Integer.BYTES, 1);
        ensureDefCapacity(valueCount);
        rle.read(defBuffer, 0, valueCount);
        for (int position = 0; position < valueCount; position++) {
            pageNulls[position] = defBuffer[position] == 0;
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
            skipDecodedRows(take);
            n -= take;
        }
        if (pageNavigationPolicy.skipWholeChunks()) {
            n = skipWholeChunks(n);
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
                decodeDataPageV1(body, pendingNumValues, pendingEncoding);
                pagePosition = pendingNextPosition;
                skipDecodedRows((int) n);
                n = 0;
            }
        }
    }

    /** Advance the logical row cursor and any streaming definition/value cursors for an already loaded page. */
    private void skipDecodedRows(int rows)
    {
        if (pageBinaryDictionaryStreaming) {
            int values = pageBinaryDictionaryNullFree ? rows : defRle.skipCountingOnes(rows);
            rle.skip(values);
        }
        else if (pageDefStreaming) {
            rle.skip(defRle.skipCountingOnes(rows));
            defPageCursor += rows;
        }
        else if (pagePlainStreaming) {
            plainValueCursor += defRle.skipCountingOnes(rows);
            defPageCursor += rows;
        }
        pageCursor += rows;
    }

    /** Skip complete row-group chunks without parsing any page headers; partial chunks retain the page-safe path. */
    private long skipWholeChunks(long rows)
    {
        while (rows > 0 && (segment == null || pagePosition >= chunkEnd)) {
            int nextChunk = chunkIndex + 1;
            if (nextChunk >= chunks.size()) {
                break;
            }
            Chunk chunk = chunks.get(nextChunk);
            if (chunk.rowCount() > rows) {
                break;
            }
            chunkIndex = nextChunk;
            segment = chunk.segment();
            ColumnMetaData metadata = chunk.metadata();
            long start = metadata.dictionary_page_offset > 0 ? metadata.dictionary_page_offset : metadata.data_page_offset;
            chunkEnd = start + metadata.total_compressed_size;
            pagePosition = chunkEnd;
            pageValueCount = 0;
            pageCursor = 0;
            if (!materializationPolicy.reuseNumericDictionaryScratch()) {
                dictionaryInts = null;
                dictionaryLongs = null;
            }
            dictionarySize = 0;
            rows -= chunk.rowCount();
        }
        return rows;
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
        return filterDictLongs(predicate, null, count, survivorsOut, valuesOut, nullsOut);
    }

    public int filterDictLongs(java.util.function.LongPredicate predicate, VersionedLongPredicate predicateVersion, int count, int[] survivorsOut, long[] valuesOut, boolean[] nullsOut)
    {
        int sc = 0;
        int windowPos = 0;
        filterScan = true;
        prepareDictionaryPredicate(predicateVersion);
        try {
            while (windowPos < count) {
                if (pageCursor >= pageValueCount && !decodeNextDataPage()) {
                    throw new IllegalStateException("Ran out of Parquet values (filter)");
                }
                int pageRows = Math.min(pageValueCount - pageCursor, count - windowPos);
                if (pageFilterDict) {
                    int pageSurvivorsBefore = sc;
                    long[] dict = dictionaryLongs;
                    boolean[] accept = acceptByIdLong(predicate);
                    boolean branchlessCompaction = shouldUseBranchlessCompaction();
                    boolean bitmaskCompaction = shouldUseBitmaskCompaction();
                    if (shouldSkipRejectedDictionaryPage(pageRows)) {
                        skipRejectedDictionaryPage(pageRows);
                        observeDictionaryFilterRows(pageRows, pageSurvivorsBefore, sc);
                        pageCursor += pageRows;
                        windowPos += pageRows;
                        continue;
                    }
                    if (pageFilterNullableFused) {
                        if (shouldUseDirectNullableDictionaryFilter()) {
                            directNullableDictionaryFilterRows += pageRows;
                            sc = defRle.filterNullableDictionaryLongs(rle, accept, dict, pageRows, windowPos,
                                    survivorsOut, valuesOut, sc);
                            observeDictionaryFilterRows(pageRows, pageSurvivorsBefore, sc);
                            pageCursor += pageRows;
                            windowPos += pageRows;
                            continue;
                        }
                        int[] ids = filterTile();
                        int base = 0;
                        while (base < pageRows) {
                            int tileRows = Math.min(FILTER_TILE, pageRows - base);
                            ensureRunDefCapacity(tileRows);
                            int nonNullCount = defRle.readRunCountingOnes(runDef, tileRows);
                            nullableDictionaryFilterRowsObserved += tileRows;
                            nullableDictionaryFilterNonNullRowsObserved += nonNullCount;
                            if (nonNullCount > 0) {
                                rle.read(ids, 0, nonNullCount);
                            }
                            int idIndex = 0;
                            int positionBase = windowPos + base;
                            if (nonNullCount == tileRows) {
                                if (branchlessCompaction) {
                                    for (int i = 0; i < tileRows; i++) {
                                        int id = ids[i];
                                        valuesOut[sc] = dict[id];
                                        survivorsOut[sc] = positionBase + i;
                                        sc += accept[id] ? 1 : 0;
                                    }
                                }
                                else {
                                    for (int i = 0; i < tileRows; i++) {
                                        int id = ids[i];
                                        if (accept[id]) {
                                            valuesOut[sc] = dict[id];
                                            survivorsOut[sc] = positionBase + i;
                                            sc++;
                                        }
                                    }
                                }
                            }
                            else if (nonNullCount == 0) {
                                // No dictionary IDs correspond to this definition-level tile.
                            }
                            else if (branchlessCompaction) {
                                for (int i = 0; i < tileRows; i++) {
                                    if (runDef[i] == 0) {
                                        continue;
                                    }
                                    int id = ids[idIndex++];
                                    valuesOut[sc] = dict[id];
                                    survivorsOut[sc] = positionBase + i;
                                    sc += accept[id] ? 1 : 0;
                                }
                            }
                            else {
                                for (int i = 0; i < tileRows; i++) {
                                    if (runDef[i] == 0) {
                                        continue;
                                    }
                                    int id = ids[idIndex++];
                                    if (accept[id]) {
                                        valuesOut[sc] = dict[id];
                                        survivorsOut[sc] = positionBase + i;
                                        sc++;
                                    }
                                }
                            }
                            base += tileRows;
                        }
                    }
                    else if (pageFilterNullable) {
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
                    else if (pageFilterFused) {
                        // Fused, run-aware: consume the id stream a homogeneous run at a time. An RLE run tests the
                        // predicate ONCE and either emits its whole contiguous position range or skips it in O(1) --
                        // collapsing the per-row accept test that dominates a run-length-encoded key column. Only a
                        // bit-packed (heterogeneous) run falls back to per-value filtering through an L1 tile. rle stays
                        // in lockstep with pageCursor (exactly pageRows values consumed) across window boundaries.
                        int[] tile = filterTile();
                        int base = 0;
                        while (base < pageRows) {
                            int run = rle.nextRun(pageRows - base);
                            if (run > 0) {
                                int id = rle.currentRleValue();
                                if (accept[id]) {
                                    long value = dict[id];
                                    int position = windowPos + base;
                                    for (int i = 0; i < run; i++) {
                                        valuesOut[sc] = value;
                                        survivorsOut[sc] = position + i;
                                        sc++;
                                    }
                                }
                                base += run;
                            }
                            else {
                                int packed = -run;
                                int offset = 0;
                                while (offset < packed) {
                                    int tileRows = Math.min(FILTER_TILE, packed - offset);
                                    rle.read(tile, 0, tileRows);
                                    // Compact this heterogeneous tile branchlessly or branchily depending on the
                                    // accepted-entry fraction selected by the dictionary-filter policy.
                                    int positionBase = windowPos + base + offset;
                                    if (bitmaskCompaction) {
                                        sc = compactLongDictionaryTileByMask(tile, tileRows, positionBase,
                                                filterAcceptBytes(accept), dict, survivorsOut, valuesOut, sc);
                                    }
                                    else if (dictionaryFilterPolicy.vectorFilter() && branchlessCompaction) {
                                        sc = VectorDictFilter.compactTile(tile, tileRows, positionBase,
                                                filterAcceptInts(accept), dict, survivorsOut, valuesOut, sc);
                                    }
                                    else if (branchlessCompaction) {
                                        for (int i = 0; i < tileRows; i++) {
                                            int id = tile[i];
                                            valuesOut[sc] = dict[id];
                                            survivorsOut[sc] = positionBase + i;
                                            sc += accept[id] ? 1 : 0;
                                        }
                                    }
                                    else {
                                        for (int i = 0; i < tileRows; i++) {
                                            int id = tile[i];
                                            if (accept[id]) {
                                                valuesOut[sc] = dict[id];
                                                survivorsOut[sc] = positionBase + i;
                                                sc++;
                                            }
                                        }
                                    }
                                    offset += tileRows;
                                }
                                base += packed;
                            }
                        }
                    }
                    else if (branchlessCompaction) {
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
                    else {
                        for (int i = 0; i < pageRows; i++) {
                            int id = idBuffer[pageCursor + i];
                            if (accept[id]) {
                                valuesOut[sc] = dict[id];
                                survivorsOut[sc] = windowPos + i;
                                sc++;
                            }
                        }
                    }
                    observeDictionaryFilterRows(pageRows, pageSurvivorsBefore, sc);
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

    private boolean shouldUseDirectNullableDictionaryFilter()
    {
        ParquetDictionaryFilterPolicy.NullableFilter nullableFilter = dictionaryFilterPolicy.nullableFilter();
        return nullableFilter.direct() &&
                nullableDictionaryFilterRowsObserved >= DIRECT_NULLABLE_DICTIONARY_FILTER_MIN_OBSERVED_ROWS &&
                nullableDictionaryFilterNonNullRowsObserved * 100 >=
                        nullableDictionaryFilterRowsObserved * DIRECT_NULLABLE_DICTIONARY_FILTER_MIN_PRESENT_PERCENT &&
                rle.bitWidth() >= nullableFilter.directMinIdBitWidth() &&
                acceptedCount > 0 &&
                (long) acceptedCount * nullableFilter.directMinAcceptedDenominator() >= dictionarySize &&
                (long) acceptedCount * DIRECT_NULLABLE_DICTIONARY_FILTER_MAX_ACCEPTED_DENOMINATOR <= dictionarySize;
    }

    /** Predicate-over-dictionary lead filter (INT); see {@link #filterDictLongs}. */
    public int filterDictInts(java.util.function.LongPredicate predicate, int count, int[] survivorsOut, int[] valuesOut, boolean[] nullsOut)
    {
        return filterDictInts(predicate, null, count, survivorsOut, valuesOut, nullsOut);
    }

    public int filterDictInts(java.util.function.LongPredicate predicate, VersionedLongPredicate predicateVersion, int count, int[] survivorsOut, int[] valuesOut, boolean[] nullsOut)
    {
        int sc = 0;
        int windowPos = 0;
        filterScan = true;
        prepareDictionaryPredicate(predicateVersion);
        try {
            while (windowPos < count) {
                if (pageCursor >= pageValueCount && !decodeNextDataPage()) {
                    throw new IllegalStateException("Ran out of Parquet values (filter)");
                }
                int pageRows = Math.min(pageValueCount - pageCursor, count - windowPos);
                if (pageFilterDict) {
                    int pageSurvivorsBefore = sc;
                    int[] dict = dictionaryInts;
                    boolean[] accept = acceptByIdInt(predicate);
                    boolean branchlessCompaction = shouldUseBranchlessCompaction();
                    boolean bitmaskCompaction = shouldUseBitmaskCompaction();
                    if (shouldSkipRejectedDictionaryPage(pageRows)) {
                        skipRejectedDictionaryPage(pageRows);
                        observeDictionaryFilterRows(pageRows, pageSurvivorsBefore, sc);
                        pageCursor += pageRows;
                        windowPos += pageRows;
                        continue;
                    }
                    if (pageFilterNullableFused) {
                        int[] ids = filterTile();
                        int base = 0;
                        while (base < pageRows) {
                            int tileRows = Math.min(FILTER_TILE, pageRows - base);
                            ensureRunDefCapacity(tileRows);
                            int nonNullCount = defRle.readRunCountingOnes(runDef, tileRows);
                            if (nonNullCount > 0) {
                                rle.read(ids, 0, nonNullCount);
                            }
                            int idIndex = 0;
                            int positionBase = windowPos + base;
                            if (nonNullCount == tileRows) {
                                if (branchlessCompaction) {
                                    for (int i = 0; i < tileRows; i++) {
                                        int id = ids[i];
                                        valuesOut[sc] = dict[id];
                                        survivorsOut[sc] = positionBase + i;
                                        sc += accept[id] ? 1 : 0;
                                    }
                                }
                                else {
                                    for (int i = 0; i < tileRows; i++) {
                                        int id = ids[i];
                                        if (accept[id]) {
                                            valuesOut[sc] = dict[id];
                                            survivorsOut[sc] = positionBase + i;
                                            sc++;
                                        }
                                    }
                                }
                            }
                            else if (nonNullCount == 0) {
                                // No dictionary IDs correspond to this definition-level tile.
                            }
                            else if (branchlessCompaction) {
                                for (int i = 0; i < tileRows; i++) {
                                    if (runDef[i] == 0) {
                                        continue;
                                    }
                                    int id = ids[idIndex++];
                                    valuesOut[sc] = dict[id];
                                    survivorsOut[sc] = positionBase + i;
                                    sc += accept[id] ? 1 : 0;
                                }
                            }
                            else {
                                for (int i = 0; i < tileRows; i++) {
                                    if (runDef[i] == 0) {
                                        continue;
                                    }
                                    int id = ids[idIndex++];
                                    if (accept[id]) {
                                        valuesOut[sc] = dict[id];
                                        survivorsOut[sc] = positionBase + i;
                                        sc++;
                                    }
                                }
                            }
                            base += tileRows;
                        }
                    }
                    else if (pageFilterNullable) {
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
                    else if (pageFilterFused) {
                        // Fused, run-aware; see filterDictLongs.
                        int[] tile = filterTile();
                        int base = 0;
                        while (base < pageRows) {
                            int run = rle.nextRun(pageRows - base);
                            if (run > 0) {
                                int id = rle.currentRleValue();
                                if (accept[id]) {
                                    int value = dict[id];
                                    int position = windowPos + base;
                                    for (int i = 0; i < run; i++) {
                                        valuesOut[sc] = value;
                                        survivorsOut[sc] = position + i;
                                        sc++;
                                    }
                                }
                                base += run;
                            }
                            else {
                                int packed = -run;
                                int offset = 0;
                                while (offset < packed) {
                                    int tileRows = Math.min(FILTER_TILE, packed - offset);
                                    rle.read(tile, 0, tileRows);
                                    // Compact this heterogeneous tile branchlessly or branchily depending on the
                                    // accepted-entry fraction selected by the dictionary-filter policy.
                                    int positionBase = windowPos + base + offset;
                                    if (bitmaskCompaction) {
                                        sc = compactIntDictionaryTileByMask(tile, tileRows, positionBase,
                                                filterAcceptBytes(accept), dict, survivorsOut, valuesOut, sc);
                                    }
                                    else if (branchlessCompaction) {
                                        for (int i = 0; i < tileRows; i++) {
                                            int id = tile[i];
                                            valuesOut[sc] = dict[id];
                                            survivorsOut[sc] = positionBase + i;
                                            sc += accept[id] ? 1 : 0;
                                        }
                                    }
                                    else {
                                        for (int i = 0; i < tileRows; i++) {
                                            int id = tile[i];
                                            if (accept[id]) {
                                                valuesOut[sc] = dict[id];
                                                survivorsOut[sc] = positionBase + i;
                                                sc++;
                                            }
                                        }
                                    }
                                    offset += tileRows;
                                }
                                base += packed;
                            }
                        }
                    }
                    else if (branchlessCompaction) {
                        // Branchless compaction (see filterDictLongs): drop the unpredictable per-row accept branch.
                        for (int i = 0; i < pageRows; i++) {
                            int id = idBuffer[pageCursor + i];
                            valuesOut[sc] = dict[id];
                            survivorsOut[sc] = windowPos + i;
                            sc += accept[id] ? 1 : 0;
                        }
                    }
                    else {
                        for (int i = 0; i < pageRows; i++) {
                            int id = idBuffer[pageCursor + i];
                            if (accept[id]) {
                                valuesOut[sc] = dict[id];
                                survivorsOut[sc] = windowPos + i;
                                sc++;
                            }
                        }
                    }
                    observeDictionaryFilterRows(pageRows, pageSurvivorsBefore, sc);
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

    private boolean shouldUseBranchlessCompaction()
    {
        ParquetDictionaryFilterPolicy.Compaction compaction = dictionaryFilterPolicy.compaction();
        if (compaction.branchlessDenominator() <= 0) {
            return false;
        }
        boolean dictionaryBranchless = (long) acceptedCount * compaction.branchlessDenominator() >= dictionarySize;
        // Observed row frequency may demote a dictionary-cardinality branchless choice, but it must not promote a
        // chunk that was already classified branchy. Promotion made clustered large dictionaries oscillate with
        // scan order even though the inexpensive branchy choice was already correct (TPC-DS q45).
        if (!dictionaryBranchless) {
            return false;
        }
        if (compaction.adaptiveBranchless() &&
                dictionaryFilterRowsObserved >= compaction.adaptiveBranchlessMinRows()) {
            return dictionaryFilterRowsAccepted * compaction.branchlessDenominator() >= dictionaryFilterRowsObserved;
        }
        return true;
    }

    private boolean shouldUseBitmaskCompaction()
    {
        ParquetDictionaryFilterPolicy.Compaction compaction = dictionaryFilterPolicy.compaction();
        return compaction.bitmask() &&
                dictionarySize >= compaction.bitmaskMinDictionarySize() &&
                compaction.bitmaskLowerDenominator() > compaction.bitmaskUpperDenominator() &&
                (long) acceptedCount * compaction.bitmaskLowerDenominator() >= dictionarySize &&
                (long) acceptedCount * compaction.bitmaskUpperDenominator() < dictionarySize;
    }

    private static int compactLongDictionaryTileByMask(int[] ids, int count, int positionBase, byte[] accept,
            long[] dictionary, int[] survivorsOut, long[] valuesOut, int survivorCount)
    {
        for (int base = 0; base < count; base += Long.SIZE) {
            int blockRows = Math.min(Long.SIZE, count - base);
            long accepted = 0;
            for (int lane = 0; lane < blockRows; lane++) {
                accepted |= (long) accept[ids[base + lane]] << lane;
            }
            while (accepted != 0) {
                int lane = Long.numberOfTrailingZeros(accepted);
                int id = ids[base + lane];
                survivorsOut[survivorCount] = positionBase + base + lane;
                valuesOut[survivorCount] = dictionary[id];
                survivorCount++;
                accepted &= accepted - 1;
            }
        }
        return survivorCount;
    }

    private static int compactIntDictionaryTileByMask(int[] ids, int count, int positionBase, byte[] accept,
            int[] dictionary, int[] survivorsOut, int[] valuesOut, int survivorCount)
    {
        for (int base = 0; base < count; base += Long.SIZE) {
            int blockRows = Math.min(Long.SIZE, count - base);
            long accepted = 0;
            for (int lane = 0; lane < blockRows; lane++) {
                accepted |= (long) accept[ids[base + lane]] << lane;
            }
            while (accepted != 0) {
                int lane = Long.numberOfTrailingZeros(accepted);
                int id = ids[base + lane];
                survivorsOut[survivorCount] = positionBase + base + lane;
                valuesOut[survivorCount] = dictionary[id];
                survivorCount++;
                accepted &= accepted - 1;
            }
        }
        return survivorCount;
    }

    private void observeDictionaryFilterRows(int rows, int survivorsBefore, int survivorsAfter)
    {
        dictionaryFilterRowsObserved += rows;
        dictionaryFilterRowsAccepted += survivorsAfter - survivorsBefore;
    }

    private void skipRejectedDictionaryPage(int pageRows)
    {
        if (pageFilterNullableFused) {
            rle.skip(defRle.skipCountingOnes(pageRows));
        }
        else if (pageFilterFused) {
            rle.skip(pageRows);
        }
        // Other dictionary page modes materialized their IDs while decoding the page, so their encoded cursor is
        // already at the next page. In every mode no value can survive when the accepted dictionary is empty.
    }

    private boolean shouldSkipRejectedDictionaryPage(int pageRows)
    {
        ParquetDictionaryFilterPolicy.ZeroAcceptedPageSkip pageSkip = dictionaryFilterPolicy.zeroAcceptedPageSkip();
        if (!pageSkip.enabled() || acceptedCount != 0) {
            return false;
        }
        zeroAcceptedDictionaryRowsObserved += pageRows;
        return zeroAcceptedDictionaryRowsObserved >= pageSkip.minObservedRows();
    }

    private boolean[] acceptByIdLong(java.util.function.LongPredicate predicate)
    {
        if (acceptByIdChunk != chunkIndex) {
            if (acceptById.length < dictionarySize) {
                acceptById = replaceBooleans(acceptById, dictionarySize);
            }
            long[] dict = dictionaryLongs;
            int accepted = 0;
            for (int id = 0; id < dictionarySize; id++) {
                boolean keep = predicate.test(dict[id]);
                acceptById[id] = keep;
                accepted += keep ? 1 : 0;
            }
            acceptedCount = accepted;
            acceptByIdChunk = chunkIndex;
        }
        return acceptById;
    }

    private void prepareDictionaryPredicate(VersionedLongPredicate predicate)
    {
        long generation = predicate == null ? -1 : predicate.contentGeneration();
        boolean sameGeneration = generation >= 0 &&
                acceptByIdPredicate == predicate &&
                acceptByIdPredicateGeneration == generation;
        boolean sameChunk = sameGeneration && acceptByIdChunk == chunkIndex;
        // Immediately below the branchless-compaction threshold, the branchy row loop is sensitive to a cold
        // acceptance table. Rebuilding the table is useful warm-up at that shape, so retain versioned reuse only
        // outside the narrow 1/12..1/9 acceptance band.
        boolean warmBranchyTable = sameChunk &&
                (long) acceptedCount * dictionaryFilterPolicy.versionedPredicate().warmBranchyDenominator() >= dictionarySize &&
                (long) acceptedCount * dictionaryFilterPolicy.compaction().branchlessDenominator() < dictionarySize;
        if (diagnostics.versionedDictionaryPredicates() &&
                !versionedDictionaryPredicateReuseReported &&
                sameChunk &&
                !warmBranchyTable) {
            System.err.printf("[versioned-dictionary-predicate] reused chunk=%d entries=%d%n", chunkIndex, dictionarySize);
            versionedDictionaryPredicateReuseReported = true;
        }
        if (!sameGeneration || warmBranchyTable) {
            acceptByIdChunk = -1;
            filterAcceptIntsChunk = -1;
            filterAcceptBytesChunk = -1;
            acceptByIdPredicate = generation >= 0 ? predicate : null;
            acceptByIdPredicateGeneration = generation;
        }
    }

    /** Refresh (per chunk) and return the int[] mirror of {@code accept} for the SIMD gather in {@link VectorDictFilter}. */
    private int[] filterAcceptInts(boolean[] accept)
    {
        if (filterAcceptIntsChunk != chunkIndex) {
            if (filterAcceptInts.length < dictionarySize) {
                filterAcceptInts = replaceInts(filterAcceptInts, dictionarySize);
            }
            for (int e = 0; e < dictionarySize; e++) {
                filterAcceptInts[e] = accept[e] ? 1 : 0;
            }
            filterAcceptIntsChunk = chunkIndex;
        }
        return filterAcceptInts;
    }

    private byte[] filterAcceptBytes(boolean[] accept)
    {
        if (filterAcceptBytesChunk != chunkIndex) {
            if (filterAcceptBytes.length < dictionarySize) {
                int retainedFloor = (int) Math.min(Integer.MAX_VALUE, arrayPool.minRetainedBytes());
                int capacity = arrayPool.isRetainable(retainedFloor)
                        ? Math.max(dictionarySize, retainedFloor)
                        : dictionarySize;
                filterAcceptBytes = replaceBytes(filterAcceptBytes, capacity);
            }
            for (int e = 0; e < dictionarySize; e++) {
                filterAcceptBytes[e] = accept[e] ? (byte) 1 : 0;
            }
            filterAcceptBytesChunk = chunkIndex;
        }
        return filterAcceptBytes;
    }

    private int[] filterTile()
    {
        if (filterTile == null) {
            filterTile = arrayPool.borrowInts(FILTER_TILE);
        }
        return filterTile;
    }

    private boolean[] acceptByIdInt(java.util.function.LongPredicate predicate)
    {
        if (acceptByIdChunk != chunkIndex) {
            if (acceptById.length < dictionarySize) {
                acceptById = replaceBooleans(acceptById, dictionarySize);
            }
            int[] dict = dictionaryInts;
            int accepted = 0;
            for (int id = 0; id < dictionarySize; id++) {
                boolean keep = predicate.test(dict[id]);
                acceptById[id] = keep;
                accepted += keep ? 1 : 0;
            }
            acceptedCount = accepted;
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
        return readBinaryInternal(null, null, nullsOut, count);
    }

    /**
     * Allocator-backed variant of {@link #readBinary(boolean[], int)}. Flat values are assembled directly in their
     * pooled output vector; the compatibility path retains the reusable reader scratch used by callers without an
     * allocator. Fully dictionary-encoded batches remain encoded and return the speculative flat vector to the pool.
     */
    public org.weakref.nitro.data.Vector readBinary(Allocator allocator, Allocator.Context allocationContext, boolean[] nullsOut, int count)
    {
        if (allocator == null || allocationContext == null) {
            throw new IllegalArgumentException("allocator and allocationContext are required");
        }
        if (materializationPolicy.directFlatBinaryOutput()) {
            return readBinaryDirect(allocator, allocationContext, nullsOut, count);
        }
        return readBinaryInternal(allocator, allocationContext, nullsOut, count);
    }

    /**
     * Allocator-backed full binary read that assembles a flat batch directly in its owned output vector. Plain pages
     * retain their page-level compaction and copy into the output one run at a time; this removes only the final
     * batch-scratch-to-output copy. Dictionary-only batches release the speculative flat output and stay encoded.
     */
    private org.weakref.nitro.data.Vector readBinaryDirect(Allocator allocator, Allocator.Context allocationContext, boolean[] nullsOut, int count)
    {
        boolean dictionaryEligible = materializationPolicy.binaryDictionary();
        boolean directOwnedCandidate = materializationPolicy.ownedDictionaryIds() &&
                materializationPolicy.directOwnedDictionaryIds() &&
                dictionaryEligible &&
                count > 0 &&
                directOwnedDictionaryRowsObserved >= materializationPolicy.directOwnedDictionaryIdsMinObservedRows();
        if (directOwnedCandidate && pageCursor >= pageValueCount && !decodeNextDataPage()) {
            throw new IllegalStateException("Ran out of Parquet values: needed " + count + ", got 0");
        }
        // Physical encoding is available after the normal page acquisition above. Plain-leading batches retain the
        // established flat-only path and never speculate an ID vector; dictionary-leading batches stage owned IDs.
        org.weakref.nitro.data.I32Vector stagedOwnedIds = directOwnedCandidate && pageBinaryDeferred
                ? org.weakref.nitro.data.I32Vector.allocate(allocator, allocationContext, count)
                : null;
        if (stagedOwnedIds == null && binaryBatchIds.length < count) {
            binaryBatchIds = replaceInts(binaryBatchIds, count);
        }
        int[] batchIds = stagedOwnedIds == null ? binaryBatchIds : stagedOwnedIds.values();
        int batchGeneration = -1;
        int produced = 0;
        int dataLength = 0;
        boolean flat = false;
        // The compatibility/control path keeps its historical speculative flat vector. When IDs are already staged
        // in their final owned buffer, defer flat allocation until a plain page or dictionary transition proves it
        // necessary; dictionary-only batches then own exactly one integer buffer rather than two concurrent ones.
        BinaryVector result = stagedOwnedIds == null
                ? BinaryVector.allocate(allocator, allocationContext, count, initialSelectedBinaryCapacity(count))
                : null;
        int[] offsets = result == null ? null : result.offsets();
        byte[] data = result == null ? null : result.data();
        if (result != null) {
            result.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
            offsets[0] = 0;
        }

        while (produced < count) {
            if (pageCursor >= pageValueCount && !decodeNextDataPage()) {
                throw new IllegalStateException("Ran out of Parquet values: needed " + count + ", got " + produced);
            }
            int n = Math.min(pageValueCount - pageCursor, count - produced);
            boolean pageIsDictionary = pageBinaryDeferred;
            boolean streamingDictionary = pageBinaryDictionaryStreaming;
            if (streamingDictionary) {
                readStreamingBinaryDictionaryIds(batchIds, produced, n, nullsOut);
            }
            if (!flat && dictionaryEligible && pageIsDictionary && (batchGeneration == -1 || dictionaryGeneration == batchGeneration)) {
                batchGeneration = dictionaryGeneration;
                if (!streamingDictionary) {
                    System.arraycopy(pageDictIds, pageCursor, batchIds, produced, n);
                }
            }
            else {
                if (!flat) {
                    flat = true;
                    if (result == null) {
                        result = BinaryVector.allocate(allocator, allocationContext, count, initialSelectedBinaryCapacity(count));
                        result.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
                        offsets = result.offsets();
                        data = result.data();
                        offsets[0] = 0;
                    }
                    BinaryVector dictionary = dictionaryVectorCache.get(batchGeneration);
                    int[] dictionaryOffsets = dictionary == null ? null : dictionary.offsets();
                    byte[] dictionaryData = dictionary == null ? null : dictionary.data();
                    for (int i = 0; i < produced; i++) {
                        if (nullsOut == null || !nullsOut[i]) {
                            int id = batchIds[i];
                            int start = dictionaryOffsets[id];
                            int length = dictionaryOffsets[id + 1] - start;
                            if (data.length < dataLength + length) {
                                result = BinaryVector.allocateOrGrow(allocator, allocationContext, result, count, dataLength + length, dataLength);
                                offsets = result.offsets();
                                data = result.data();
                            }
                            System.arraycopy(dictionaryData, start, data, dataLength, length);
                            dataLength += length;
                        }
                        offsets[i + 1] = dataLength;
                    }
                }

                if (pageIsDictionary) {
                    for (int j = 0; j < n; j++) {
                        int position = streamingDictionary ? produced + j : pageCursor + j;
                        if (!(optional && (streamingDictionary ? nullsOut[position] : pageNulls[position]))) {
                            int id = streamingDictionary ? batchIds[position] : pageDictIds[position];
                            int start = dictionaryByteOffsets[id];
                            int length = dictionaryByteOffsets[id + 1] - start;
                            if (data.length < dataLength + length) {
                                result = BinaryVector.allocateOrGrow(allocator, allocationContext, result, count, dataLength + length, dataLength);
                                offsets = result.offsets();
                                data = result.data();
                            }
                            System.arraycopy(dictionaryBytes, start, data, dataLength, length);
                            dataLength += length;
                        }
                        offsets[produced + j + 1] = dataLength;
                    }
                }
                else {
                    int runStart = pageByteOffsets[pageCursor];
                    int runBytes = pageByteOffsets[pageCursor + n] - runStart;
                    if (data.length < dataLength + runBytes) {
                        result = BinaryVector.allocateOrGrow(allocator, allocationContext, result, count, dataLength + runBytes, dataLength);
                        offsets = result.offsets();
                        data = result.data();
                    }
                    System.arraycopy(pageBytes, runStart, data, dataLength, runBytes);
                    int shift = dataLength - runStart;
                    for (int j = 1; j <= n; j++) {
                        offsets[produced + j] = pageByteOffsets[pageCursor + j] + shift;
                    }
                    dataLength += runBytes;
                }
            }
            if (nullsOut != null && !streamingDictionary) {
                System.arraycopy(pageNulls, pageCursor, nullsOut, produced, n);
            }
            pageCursor += n;
            produced += n;
        }

        if (!flat) {
            directOwnedDictionaryRowsObserved += count;
            if (result != null) {
                allocator.release(allocationContext, result);
            }
            if (stagedOwnedIds != null) {
                return org.weakref.nitro.data.DictionaryVector.wrapOwnedIds(stagedOwnedIds, count, escapedDictionary(batchGeneration));
            }
            if (materializationPolicy.ownedDictionaryIds()) {
                org.weakref.nitro.data.I32Vector ids = org.weakref.nitro.data.I32Vector.allocate(allocator, allocationContext, count);
                System.arraycopy(batchIds, 0, ids.values(), 0, count);
                return org.weakref.nitro.data.DictionaryVector.wrapOwnedIds(ids, count, escapedDictionary(batchGeneration));
            }
            return org.weakref.nitro.data.DictionaryVector.ofTrustedIds(batchIds, count, escapedDictionary(batchGeneration));
        }
        if (stagedOwnedIds != null) {
            allocator.release(allocationContext, stagedOwnedIds);
        }
        return result;
    }

    private org.weakref.nitro.data.Vector readBinaryInternal(Allocator allocator, Allocator.Context allocationContext, boolean[] nullsOut, int count)
    {
        if (binaryOutOffsets.length < count + 1) {
            binaryOutOffsets = replaceInts(binaryOutOffsets, count + 1);
        }
        if (binaryBatchIds.length < count) {
            binaryBatchIds = replaceInts(binaryBatchIds, count);
        }
        boolean dictionaryEligible = materializationPolicy.binaryDictionary();
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
            boolean streamingDictionary = pageBinaryDictionaryStreaming;
            if (streamingDictionary) {
                readStreamingBinaryDictionaryIds(binaryBatchIds, produced, n, nullsOut);
            }
            if (!flat && dictionaryEligible && pageIsDictionary && (batchGeneration == -1 || dictionaryGeneration == batchGeneration)) {
                // Dictionary path: the page recorded row-aligned ids; carry them without materializing bytes.
                batchGeneration = dictionaryGeneration;
                if (!streamingDictionary) {
                    System.arraycopy(pageDictIds, pageCursor, binaryBatchIds, produced, n);
                }
            }
            else {
                if (!flat) {
                    // A plain page or a dictionary change ends the dictionary batch: materialize the ids gathered so
                    // far [0, produced) into flat bytes, then continue flat for the rest of the batch.
                    flat = true;
                    dataLength = spillDictionaryIdsToFlat(batchGeneration, produced, nullsOut);
                }
                dataLength = streamingDictionary
                        ? appendStreamingDictionaryRunToFlat(binaryBatchIds, n, produced, dataLength, nullsOut)
                        : appendBinaryRunToFlat(pageIsDictionary, pageCursor, n, produced, dataLength, nullsOut);
            }
            if (nullsOut != null && !streamingDictionary) {
                System.arraycopy(pageNulls, pageCursor, nullsOut, produced, n);
            }
            pageCursor += n;
            produced += n;
        }
        if (!flat) {
            if (materializationPolicy.ownedDictionaryIds() && allocator != null) {
                org.weakref.nitro.data.I32Vector ids = org.weakref.nitro.data.I32Vector.allocate(allocator, allocationContext, count);
                System.arraycopy(binaryBatchIds, 0, ids.values(), 0, count);
                return org.weakref.nitro.data.DictionaryVector.wrapOwnedIds(ids, count, escapedDictionary(batchGeneration));
            }
            return org.weakref.nitro.data.DictionaryVector.ofTrustedIds(binaryBatchIds, count, escapedDictionary(batchGeneration));
        }
        org.weakref.nitro.data.BinaryVector result;
        if (allocator == null) {
            result = new org.weakref.nitro.data.BinaryVector(count, Arrays.copyOf(binaryOutOffsets, count + 1), Arrays.copyOf(binaryOutData, dataLength));
        }
        else {
            result = org.weakref.nitro.data.BinaryVector.allocate(allocator, allocationContext, count, dataLength);
            System.arraycopy(binaryOutOffsets, 0, result.offsets(), 0, count + 1);
            System.arraycopy(binaryOutData, 0, result.data(), 0, dataLength);
        }
        result.addTraits(java.util.Set.of(org.weakref.nitro.data.Utf8Traits.UTF8_STRING));
        return result;
    }

    private BinaryVector escapedDictionary(int generation)
    {
        BinaryVector dictionary = dictionaryVectorCache.get(generation);
        // Once a dictionary is returned from the reader, an operator may retain its values after the source batch
        // closes (hash build and grouped output do this deliberately). Batch-boundary observation is therefore too
        // late to prove that the backing offsets and bytes are reader-owned scratch. Evict the cache entry normally,
        // but leave escaped storage to its downstream owner/GC instead of lending the arrays to a later row group.
        escapedBinaryDictionaries.put(dictionary, Boolean.TRUE);
        return dictionary;
    }

    private void readStreamingBinaryDictionaryIds(int[] batchIds, int outputOffset, int count, boolean[] nullsOut)
    {
        if (pageBinaryDictionaryNullFree) {
            rle.read(batchIds, outputOffset, count);
            if (nullsOut != null) {
                Arrays.fill(nullsOut, outputOffset, outputOffset + count, false);
            }
            return;
        }

        ensureDefCapacity(count);
        defRle.read(defBuffer, 0, count);
        int nonNullCount = 0;
        for (int position = 0; position < count; position++) {
            nonNullCount += defBuffer[position];
        }
        ensureIdCapacity(nonNullCount);
        rle.read(idBuffer, 0, nonNullCount);
        int idPosition = 0;
        for (int position = 0; position < count; position++) {
            boolean isNull = defBuffer[position] == 0;
            batchIds[outputOffset + position] = isNull ? 0 : idBuffer[idPosition++];
            if (nullsOut != null) {
                nullsOut[outputOffset + position] = isNull;
            }
        }
    }

    private int appendStreamingDictionaryRunToFlat(
            int[] batchIds,
            int count,
            int outputOffset,
            int dataLength,
            boolean[] nullsOut)
    {
        for (int position = 0; position < count; position++) {
            int outputPosition = outputOffset + position;
            if (nullsOut == null || !nullsOut[outputPosition]) {
                int id = batchIds[outputPosition];
                int start = dictionaryByteOffsets[id];
                int length = dictionaryByteOffsets[id + 1] - start;
                if (binaryOutData.length < dataLength + length) {
                    binaryOutData = growBytes(binaryOutData, Math.max(binaryOutData.length * 2, dataLength + length), dataLength);
                }
                System.arraycopy(dictionaryBytes, start, binaryOutData, dataLength, length);
                dataLength += length;
            }
            binaryOutOffsets[outputPosition + 1] = dataLength;
        }
        return dataLength;
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
                    binaryOutData = growBytes(binaryOutData, Math.max(binaryOutData.length * 2, dataLength + length), dataLength);
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
                        binaryOutData = growBytes(binaryOutData, Math.max(binaryOutData.length * 2, dataLength + length), dataLength);
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
            binaryOutData = growBytes(binaryOutData, Math.max(binaryOutData.length * 2, dataLength + runBytes), dataLength);
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
            if (pageNumericDictionaryIdsDecoded) {
                while (sel < count && survivors[sel] < pageEnd) {
                    int pagePosition = pageCursor + survivors[sel] - batchCursor;
                    boolean isNull = optional && pageNulls[pagePosition];
                    out[produced] = isNull ? 0 : dictionaryLongs[pageDictIds[pagePosition]];
                    if (nullsOut != null) {
                        nullsOut[produced] = isNull;
                    }
                    produced++;
                    sel++;
                }
                pageCursor += pageRows;
                batchCursor = pageEnd;
                continue;
            }
            if (pageDirectDictionary || pageDirectPlain) {
                while (sel < count && survivors[sel] < pageEnd) {
                    int pagePosition = pageCursor + survivors[sel] - batchCursor;
                    out[produced] = pageDirectDictionary
                            ? dictionaryLongs[idBuffer[pagePosition]]
                            : pageDirectPlainBody.get(LE_LONG, pageDirectPlainOffset + (long) pagePosition * Long.BYTES);
                    if (nullsOut != null) {
                        nullsOut[produced] = false;
                    }
                    produced++;
                    sel++;
                }
                pageCursor += pageRows;
                batchCursor = pageEnd;
                continue;
            }
            if (skipBody != null) {
                // Fused per-page skip-decode of a dict LONG page: the fully-inlined pure-Java kernel keeps both RLE
                // cursors in locals/registers with no per-value method call or idBuffer/runDef materialization.
                int selEnd = sel;
                while (selEnd < count && survivors[selEnd] < pageEnd) {
                    selEnd++;
                }
                if (selEnd > sel) {
                    produced = JavaSkipDecode.skipDecodeDictLongs(skipBody, skipValueOffset, skipBitWidth,
                            skipDefOffset, survivors, sel, selEnd, pageCursor - batchCursor, dictionaryLongs, out, produced, nullsOut);
                    sel = selEnd;
                }
                pageValueCursor = pageCursor + pageRows;
                pageCursor += pageRows;
                batchCursor = pageEnd;
                continue;
            }
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
                    ensureRunDefCapacity(runLen);
                    int nonNullInRun = defRle.readRunCountingOnes(runDef, runLen);
                    if (nonNullInRun == runLen) {
                        ensureIdCapacity(runLen);
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
                    ensureRunDefCapacity(runLen);
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
                    ensureIdCapacity(runLen);
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
        ensureIdCapacity(nonNullInRun);
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
            copyPlainLongs(valueIndex, out, produced, n);
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
                if (optional) {
                    System.arraycopy(pageNulls, runStartPage, nullsOut, produced, runLen);
                }
                else {
                    Arrays.fill(nullsOut, produced, produced + runLen, false);
                }
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
            copyPlainLongs(runStartPage, out, produced, runLen);
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
            if (pageNumericDictionaryIdsDecoded) {
                while (sel < count && survivors[sel] < pageEnd) {
                    int pagePosition = pageCursor + survivors[sel] - batchCursor;
                    boolean isNull = optional && pageNulls[pagePosition];
                    out[produced] = isNull ? 0 : dictionaryInts[pageDictIds[pagePosition]];
                    if (nullsOut != null) {
                        nullsOut[produced] = isNull;
                    }
                    produced++;
                    sel++;
                }
                pageCursor += pageRows;
                batchCursor = pageEnd;
                continue;
            }
            if (pageDirectDictionary || pageDirectPlain) {
                while (sel < count && survivors[sel] < pageEnd) {
                    int pagePosition = pageCursor + survivors[sel] - batchCursor;
                    out[produced] = pageDirectDictionary
                            ? dictionaryInts[idBuffer[pagePosition]]
                            : pageDirectPlainBody.get(LE_INT, pageDirectPlainOffset + (long) pagePosition * Integer.BYTES);
                    if (nullsOut != null) {
                        nullsOut[produced] = false;
                    }
                    produced++;
                    sel++;
                }
                pageCursor += pageRows;
                batchCursor = pageEnd;
                continue;
            }
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
                    ensureRunDefCapacity(runLen);
                    int nonNullInRun = defRle.readRunCountingOnes(runDef, runLen);
                    if (nonNullInRun == runLen) {
                        ensureIdCapacity(runLen);
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
                    ensureRunDefCapacity(runLen);
                    int nonNullInRun = defRle.readRunCountingOnes(runDef, runLen);
                    if (nonNullInRun == runLen) {
                        copyPlainInts(plainValueCursor, out, produced, runLen);
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
                    ensureIdCapacity(runLen);
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
        ensureIdCapacity(nonNullInRun);
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
                if (optional) {
                    System.arraycopy(pageNulls, runStartPage, nullsOut, produced, runLen);
                }
                else {
                    Arrays.fill(nullsOut, produced, produced + runLen, false);
                }
            }
        }
        else {
            copyPlainInts(runStartPage, out, produced, runLen);
            if (nullsOut != null) {
                Arrays.fill(nullsOut, produced, produced + runLen, false);
            }
        }
    }

    private void copyPlainInts(int valueIndex, int[] out, int produced, int count)
    {
        if (count <= materializationPolicy.plainCopyLoopMaxValues()) {
            long offset = pagePlainOffset + (long) valueIndex * Integer.BYTES;
            for (int i = 0; i < count; i++) {
                out[produced + i] = pagePlainBody.get(LE_INT, offset + (long) i * Integer.BYTES);
            }
            return;
        }
        MemorySegment.copy(pagePlainBody, LE_INT, pagePlainOffset + (long) valueIndex * Integer.BYTES, out, produced, count);
    }

    private void copyPlainLongs(int valueIndex, long[] out, int produced, int count)
    {
        if (count <= materializationPolicy.plainCopyLoopMaxValues()) {
            long offset = pagePlainOffset + (long) valueIndex * Long.BYTES;
            for (int i = 0; i < count; i++) {
                out[produced + i] = pagePlainBody.get(LE_LONG, offset + (long) i * Long.BYTES);
            }
            return;
        }
        MemorySegment.copy(pagePlainBody, LE_LONG, pagePlainOffset + (long) valueIndex * Long.BYTES, out, produced, count);
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
            binaryOutOffsets = replaceInts(binaryOutOffsets, batchRows + 1);
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
            boolean pageIsDictionary = pageBinaryDeferred;
            for (int b = batchCursor; b < pageEnd; b++) {
                if (sel < count && survivors[sel] == b) {
                    int pagePos = pageCursor + (b - batchCursor);
                    boolean isNull = optional && pageNulls[pagePos];
                    if (!isNull) {
                        if (pageIsDictionary) {
                            int id = pageDictIds[pagePos];
                            int start = dictionaryByteOffsets[id];
                            int length = dictionaryByteOffsets[id + 1] - start;
                            if (binaryOutData.length < dataLength + length) {
                                binaryOutData = growBytes(binaryOutData, Math.max(binaryOutData.length * 2, dataLength + length), dataLength);
                            }
                            System.arraycopy(dictionaryBytes, start, binaryOutData, dataLength, length);
                            dataLength += length;
                        }
                        else {
                            int start = pageByteOffsets[pagePos];
                            int length = pageByteOffsets[pagePos + 1] - start;
                            if (binaryOutData.length < dataLength + length) {
                                binaryOutData = growBytes(binaryOutData, Math.max(binaryOutData.length * 2, dataLength + length), dataLength);
                            }
                            System.arraycopy(pageBytes, start, binaryOutData, dataLength, length);
                            dataLength += length;
                        }
                    }
                    if (nullsOut != null) {
                        nullsOut[b] = isNull;
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

    public BinaryVector readSelectedBinary(Allocator allocator, Allocator.Context allocationContext, int[] survivors, int count, int batchRows, boolean[] nullsOut)
    {
        if (!materializationPolicy.directSelectedBinary()) {
            return (BinaryVector) allocator.adopt(allocationContext, readSelectedBinary(survivors, count, batchRows, nullsOut));
        }
        BinaryVector result = BinaryVector.allocate(allocator, allocationContext, batchRows, initialSelectedBinaryCapacity(count));
        result.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        int[] offsets = result.offsets();
        byte[] data = result.data();

        int sel = 0;
        int batchCursor = 0;
        int dataLength = 0;
        offsets[0] = 0;
        while (batchCursor < batchRows) {
            if (pageCursor >= pageValueCount) {
                int skipped = acquirePageForSkip(survivors, sel, count, batchCursor, batchRows);
                if (skipped >= 0) {
                    Arrays.fill(offsets, batchCursor + 1, batchCursor + skipped + 1, dataLength);
                    if (nullsOut != null) {
                        Arrays.fill(nullsOut, batchCursor, batchCursor + skipped, false);
                    }
                    batchCursor += skipped;
                    continue;
                }
            }
            int pageRows = Math.min(pageValueCount - pageCursor, batchRows - batchCursor);
            int pageEnd = batchCursor + pageRows;
            boolean pageIsDictionary = pageBinaryDeferred;
            for (int b = batchCursor; b < pageEnd; b++) {
                if (sel < count && survivors[sel] == b) {
                    int pagePos = pageCursor + (b - batchCursor);
                    boolean isNull = optional && pageNulls[pagePos];
                    if (!isNull) {
                        byte[] source;
                        int start;
                        int length;
                        if (pageIsDictionary) {
                            int id = pageDictIds[pagePos];
                            source = dictionaryBytes;
                            start = dictionaryByteOffsets[id];
                            length = dictionaryByteOffsets[id + 1] - start;
                        }
                        else {
                            source = pageBytes;
                            start = pageByteOffsets[pagePos];
                            length = pageByteOffsets[pagePos + 1] - start;
                        }
                        if (data.length < dataLength + length) {
                            result = BinaryVector.allocateOrGrow(allocator, allocationContext, result, batchRows, dataLength + length, dataLength);
                            offsets = result.offsets();
                            data = result.data();
                        }
                        System.arraycopy(source, start, data, dataLength, length);
                        dataLength += length;
                    }
                    if (nullsOut != null) {
                        nullsOut[b] = isNull;
                    }
                    sel++;
                }
                else if (nullsOut != null) {
                    nullsOut[b] = false;
                }
                offsets[b + 1] = dataLength;
            }
            pageCursor += pageRows;
            batchCursor = pageEnd;
        }
        return result;
    }

    public void skipSelectedBinary(int batchRows)
    {
        int batchCursor = 0;
        while (batchCursor < batchRows) {
            if (pageCursor >= pageValueCount) {
                int skipped = acquirePageForSkip(EMPTY_INTS, 0, 0, batchCursor, batchRows);
                if (skipped >= 0) {
                    batchCursor += skipped;
                    continue;
                }
            }
            int pageRows = Math.min(pageValueCount - pageCursor, batchRows - batchCursor);
            pageCursor += pageRows;
            batchCursor += pageRows;
        }
    }

    private static int initialSelectedBinaryCapacity(int selectedCount)
    {
        if (selectedCount <= 0) {
            return 0;
        }
        return Math.min(selectedCount * 16, 1 << 20);
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
        int survivorEnd;
        if (pageNavigationPolicy.binarySearchPageSurvivors() &&
                count >= pageNavigationPolicy.binarySearchPageMinSurvivors() &&
                (long) count * 100 <= (long) batchRows * pageNavigationPolicy.binarySearchPageSurvivorMaxPercent()) {
            survivorEnd = lowerBound(survivors, sel, count, pageEnd);
        }
        else {
            survivorEnd = sel;
            while (survivorEnd < count && survivors[survivorEnd] < pageEnd) {
                survivorEnd++;
            }
        }
        int survivorsInPage = survivorEnd - sel;
        pageForceWholeDecode = false;
        if ((long) survivorsInPage * 100 >= (long) rowsThisBatch * pageNavigationPolicy.skipPageDensePercent()) {
            int runCount = 0;
            int previous = -2;
            for (int survivor = sel; survivor < survivorEnd; survivor++) {
                if (survivors[survivor] != previous + 1) {
                    runCount++;
                }
                previous = survivors[survivor];
            }
            pageForceWholeDecode = (long) survivorsInPage < (long) runCount * pageNavigationPolicy.skipPageMinAverageRun();
        }
        CompressionCodec codec = chunks.get(chunkIndex).metadata().codec;
        loadDataPageForSkip(
                decompress(segment, pendingBodyPosition, pendingCompressedSize, pendingUncompressedSize, codec),
                pendingNumValues,
                pendingEncoding,
                batchRows);
        pagePosition = pendingNextPosition;
        return -1;
    }

    private static int lowerBound(int[] sorted, int from, int to, int value)
    {
        int low = from;
        int high = to;
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (sorted[middle] < value) {
                low = middle + 1;
            }
            else {
                high = middle;
            }
        }
        return low;
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
            long bodyPosition = readPageHeader(segment, pagePosition, chunkEnd);
            int compressedSize = parsedCompressedSize;
            int uncompressedSize = parsedUncompressedSize;
            long nextPagePosition = bodyPosition + compressedSize;
            if (parsedPageType == PageType.DICTIONARY_PAGE.getValue()) {
                CompressionCodec codec = chunks.get(chunkIndex).metadata().codec;
                decodeDictionary(decompress(segment, bodyPosition, compressedSize, uncompressedSize, codec), parsedValueCount);
                pagePosition = nextPagePosition;
                continue;
            }
            if (parsedPageType == PageType.DATA_PAGE.getValue()) {
                pendingBodyPosition = bodyPosition;
                pendingCompressedSize = compressedSize;
                pendingUncompressedSize = uncompressedSize;
                pendingNextPosition = nextPagePosition;
                pendingNumValues = parsedValueCount;
                pendingEncoding = parsedEncoding;
                return true;
            }
            throw new IllegalStateException("Unsupported page type for NitroParquet: " + parsedPageType);
        }
    }

    private void loadDataPageForSkip(MemorySegment body, int valueCount, int encodingValue, int selectedBatchRows)
    {
        Encoding encoding = Encoding.findByValue(encodingValue);
        pageValueCount = valueCount;
        pageCursor = 0;
        pageValueCursor = 0;
        pageBinaryDeferred = false;
        pageBinaryDictionaryStreaming = false;
        pageBinaryDictionaryNullFree = false;
        pageNumericDictionaryIdsDecoded = false;
        pageDirectDictionary = false;
        pageDirectPlain = false;
        pageDirectPlainBody = null;
        pageDirectPlainOffset = 0;
        pageFullyDecoded = false;
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
                    // consumeIfAllOnes() is a destructive speculative probe when it returns false. Reset the cursor
                    // before the fallback decode, just as the ordinary full-decode path does below.
                    rle.init(body, offset, 1);
                    ensureDefCapacity(valueCount);
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
        skipBody = null;
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
                if (kind == Kind.LONG) {
                    skipBody = body;
                    skipValueOffset = offset;
                    skipDefOffset = defStreamOffset;
                    skipBitWidth = bitWidth;
                }
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
                if (kind == Kind.LONG) {
                    skipBody = body;
                    skipValueOffset = offset;
                    skipDefOffset = -1L;
                    skipBitWidth = bitWidth;
                }
            }
            else {
                pagePlainBody = body;
                pagePlainOffset = offset;
            }
            pageFullyDecoded = false;
        }
        else {
            // Whole-page decode + later gather: binary plain pages and nullable plain / FLBA-decimal pages. Binary
            // dictionary pages keep row-aligned ids so selected reads copy bytes only for surviving output rows.
            if (pageDict) {
                int bitWidth = body.get(ValueLayout.JAVA_BYTE, offset) & 0xFF;
                offset += 1;
                rle.init(body, offset, bitWidth);
                ensureIdCapacity(nonNullCount);
                rle.read(idBuffer, 0, nonNullCount);
                if (kind == Kind.BINARY) {
                    scatterDictionaryBinaryIds(nonNullCount);
                }
                else {
                    ensurePageCapacity(valueCount);
                    gatherDictionary(nonNullCount);
                }
            }
            else if (kind == Kind.BINARY) {
                ensurePageCapacity(valueCount);
                decodePlainBinary(body, offset, nonNullCount);
            }
            else {
                ensurePageCapacity(valueCount);
                decodePlain(body, offset, nonNullCount);
            }
            pageFullyDecoded = true;
        }
        // Decode page-wide IDs only when this selected call consumes a strict slice of the page. The retained IDs can
        // then amortize their one sequential decode across later calls over the same page. A filter window that spans
        // the whole page has no future reuse; its run-aware streaming cursor does less work by touching survivors only.
        if (materializationPolicy.bulkSelectedNumericDictionaryIds() &&
                (!materializationPolicy.bulkSelectedNumericDictionaryIdsRequirePageReuse() || selectedBatchRows < valueCount) &&
                pageDict && !pageFullyDecoded && kind != Kind.BINARY) {
            materializeNumericDictionaryIds(streaming);
        }
    }

    /**
     * Decode a selected numeric dictionary page's compact id stream once, but defer the wide dictionary lookup to
     * actual survivor rows. This keeps compressed-cursor work sequential across the page without materializing every
     * long/int value. In particular, later output batches no longer restart a stateless skip decoder at the beginning
     * of the same large Parquet page.
     */
    private void materializeNumericDictionaryIds(boolean streamingDefinitions)
    {
        if (pageDictIds.length < pageValueCount) {
            pageDictIds = replaceInts(pageDictIds, pageValueCount);
        }
        ensurePageNullCapacity(pageValueCount);
        if (!streamingDefinitions) {
            rle.read(pageDictIds, 0, pageValueCount);
            if (optional) {
                Arrays.fill(pageNulls, 0, pageValueCount, false);
            }
        }
        else {
            ensureDefCapacity(pageValueCount);
            defRle.read(defBuffer, 0, pageValueCount);
            int nonNullCount = 0;
            for (int position = 0; position < pageValueCount; position++) {
                nonNullCount += defBuffer[position];
            }
            ensureIdCapacity(nonNullCount);
            rle.read(idBuffer, 0, nonNullCount);
            int idPosition = 0;
            for (int position = 0; position < pageValueCount; position++) {
                if (defBuffer[position] != 0) {
                    pageDictIds[position] = idBuffer[idPosition++];
                    pageNulls[position] = false;
                }
                else {
                    pageDictIds[position] = 0;
                    pageNulls[position] = true;
                }
            }
        }
        pageNumericDictionaryIdsDecoded = true;
        pageDefStreaming = false;
        skipBody = null;
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
        if (!materializationPolicy.reuseNumericDictionaryScratch()) {
            dictionaryInts = null;
            dictionaryLongs = null;
        }
        dictionarySize = 0;
        return true;
    }

    /**
     * The largest per-row-group dictionary size across the column's chunks — a proxy for the column's cardinality
     * (domain size), used to estimate a dynamic filter's selectivity as {@code filterValues / cardinality}. The max
     * over all row groups (not just the first) is a far better lower bound on the true distinct-value count: a
     * date-clustered fact column packs only a slice of the date domain into its first row group, so the first chunk
     * alone badly undercounts and makes a genuinely selective filter look non-selective. Reads one page header per
     * chunk (no decompression or decode). Returns {@code -1} when the column is not dictionary-encoded, so the caller
     * can fall back to the raw filter size.
     */
    public int peekDictionarySize()
    {
        if (cachedDictionarySize != Integer.MIN_VALUE) {
            return cachedDictionarySize;
        }
        int max = -1;
        for (int index = 0; index < chunks.size(); index++) {
            Chunk chunk = chunks.get(index);
            ColumnMetaData metadata = chunk.metadata();
            long start = metadata.dictionary_page_offset > 0 ? metadata.dictionary_page_offset : metadata.data_page_offset;
            long limit = start + metadata.total_compressed_size;
            readPageHeader(chunk.segment(), start, limit);
            if (parsedPageType == PageType.DICTIONARY_PAGE.getValue()) {
                max = Math.max(max, parsedValueCount);
            }
        }
        cachedDictionarySize = max;
        return cachedDictionarySize;
    }

    /**
     * Exact fraction of physical dictionary entries accepted across this column's chunks. The bounded dictionary
     * inspection is used only to rank filter work before reading begins; data-page ids are not touched. Returns
     * {@link Double#NaN} for plain/double/binary columns or when the aggregate dictionary exceeds {@code maxEntries}.
     */
    public double estimateDictionaryMatchFraction(LongPredicate predicate, int maxEntries)
    {
        if (maxEntries == 0 || kind == Kind.BINARY || physicalType == Type.DOUBLE || chunks.isEmpty()) {
            return Double.NaN;
        }
        int entries = 0;
        for (Chunk chunk : chunks) {
            ColumnMetaData metadata = chunk.metadata();
            long start = metadata.dictionary_page_offset > 0 ? metadata.dictionary_page_offset : metadata.data_page_offset;
            long limit = start + metadata.total_compressed_size;
            readPageHeader(chunk.segment(), start, limit);
            if (parsedPageType != PageType.DICTIONARY_PAGE.getValue() || parsedValueCount > maxEntries - entries) {
                return Double.NaN;
            }
            entries += parsedValueCount;
        }
        if (entries == 0) {
            return Double.NaN;
        }

        int accepted = 0;
        // Inspection must not overwrite the ordinary decoder's scratch page or pollute decompression diagnostics.
        // This method is intentionally safe to call after a reader has begun consuming data.
        try (Arena inspectionArena = Arena.ofConfined()) {
            for (Chunk chunk : chunks) {
                ColumnMetaData metadata = chunk.metadata();
                long start = metadata.dictionary_page_offset > 0 ? metadata.dictionary_page_offset : metadata.data_page_offset;
                long limit = start + metadata.total_compressed_size;
                long bodyPosition = readPageHeader(chunk.segment(), start, limit);
                MemorySegment body;
                if (metadata.codec == CompressionCodec.UNCOMPRESSED) {
                    body = chunk.segment().asSlice(bodyPosition, parsedCompressedSize);
                }
                else if (metadata.codec == CompressionCodec.SNAPPY) {
                    body = inspectionArena.allocate(parsedUncompressedSize);
                    snappy.decompress(
                            chunk.segment().asSlice(bodyPosition, parsedCompressedSize),
                            body);
                }
                else {
                    throw new IllegalStateException("Unsupported codec for NitroParquet: " + metadata.codec);
                }
                for (int index = 0; index < parsedValueCount; index++) {
                    long value = kind == Kind.INT
                            ? body.get(LE_INT, (long) index * Integer.BYTES)
                            : flbaDecimal
                                    ? bigEndianSignedLong(body, (long) index * typeLength, typeLength)
                                    : body.get(LE_LONG, (long) index * Long.BYTES);
                    accepted += predicate.test(value) ? 1 : 0;
                }
            }
        }
        return (double) accepted / entries;
    }

    /** Whether a numeric row-group chunk can overlap a pushed long domain according to Parquet min/max metadata. */
    public boolean chunkMayMatch(int index, LongDomain domain)
    {
        requireNonNull(domain, "domain is null");
        if (index < 0 || index >= chunks.size()) {
            throw new IndexOutOfBoundsException(index);
        }
        if (kind == Kind.BINARY || physicalType == Type.DOUBLE || flbaDecimal) {
            return true;
        }
        var statistics = chunks.get(index).metadata().statistics;
        if (statistics == null) {
            return true;
        }
        byte[] minimumBytes = statistics.isSetMin_value() ? statistics.getMin_value() : statistics.getMin();
        byte[] maximumBytes = statistics.isSetMax_value() ? statistics.getMax_value() : statistics.getMax();
        int width = kind == Kind.INT ? Integer.BYTES : Long.BYTES;
        if (minimumBytes == null || maximumBytes == null || minimumBytes.length != width || maximumBytes.length != width) {
            return true;
        }
        long minimum = numericStatistic(minimumBytes);
        long maximum = numericStatistic(maximumBytes);
        return minimum <= maximum && domain.mayOverlap(minimum, maximum);
    }

    /**
     * Whether a dictionary-only numeric row group contains at least one value accepted by the domain.
     *
     * <p>Min/max metadata cannot reject a row group whose sparse dictionary straddles the requested range. Reading a
     * bounded dictionary is still much cheaper than decoding its data pages. A row group with any non-dictionary data
     * page, an unsupported physical carrier, or a dictionary above the construction-owned limit remains admitted.
     */
    public boolean dictionaryMayMatch(int index, LongDomain domain, int maxDictionaryValues)
    {
        requireNonNull(domain, "domain is null");
        if (index < 0 || index >= chunks.size()) {
            throw new IndexOutOfBoundsException(index);
        }
        if (maxDictionaryValues == 0 || kind == Kind.BINARY || physicalType == Type.DOUBLE || flbaDecimal) {
            return true;
        }
        Chunk chunk = chunks.get(index);
        ColumnMetaData metadata = chunk.metadata();
        if (!isOnlyDictionaryEncoded(metadata)) {
            return true;
        }
        long start = metadata.dictionary_page_offset > 0 ? metadata.dictionary_page_offset : metadata.data_page_offset;
        long limit = start + metadata.total_compressed_size;
        long bodyPosition = readPageHeader(chunk.segment(), start, limit);
        if (parsedPageType != PageType.DICTIONARY_PAGE.getValue() || parsedValueCount > maxDictionaryValues) {
            return true;
        }
        MemorySegment body = decompress(
                chunk.segment(),
                bodyPosition,
                parsedCompressedSize,
                parsedUncompressedSize,
                metadata.codec);
        for (int dictionaryIndex = 0; dictionaryIndex < parsedValueCount; dictionaryIndex++) {
            long value = kind == Kind.INT
                    ? body.get(LE_INT, (long) dictionaryIndex * Integer.BYTES)
                    : body.get(LE_LONG, (long) dictionaryIndex * Long.BYTES);
            if (domain.test(value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isOnlyDictionaryEncoded(ColumnMetaData metadata)
    {
        if (metadata.isSetEncoding_stats()) {
            boolean dictionaryPage = false;
            for (var encoding : metadata.getEncoding_stats()) {
                if (encoding.getPage_type() == PageType.DICTIONARY_PAGE) {
                    dictionaryPage = true;
                }
                else if ((encoding.getPage_type() == PageType.DATA_PAGE ||
                        encoding.getPage_type() == PageType.DATA_PAGE_V2) &&
                        encoding.getEncoding() != Encoding.PLAIN_DICTIONARY &&
                        encoding.getEncoding() != Encoding.RLE_DICTIONARY) {
                    return false;
                }
            }
            return dictionaryPage;
        }
        return metadata.getEncodings().contains(Encoding.PLAIN_DICTIONARY) &&
                metadata.getEncodings().stream().allMatch(encoding ->
                        encoding == Encoding.PLAIN_DICTIONARY ||
                                encoding == Encoding.RLE ||
                                encoding == Encoding.BIT_PACKED);
    }

    private long numericStatistic(byte[] value)
    {
        ByteBuffer buffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN);
        return kind == Kind.INT ? buffer.getInt() : buffer.getLong();
    }

    /**
     * Returns whether every physical dictionary entry in every chunk is accepted by {@code predicate}. This is an
     * exact, one-time admission check for dropping a dynamic filter that cannot prune the scan. Comparing only the
     * filter and dictionary cardinalities is insufficient because dictionaries are row-group-local: an oversized
     * filter may cover the full column domain, while a same-sized filter may contain different values.
     *
     * <p>Only dictionary pages are decompressed; data-page ids are not read. A plain-encoded chunk returns false.
     */
    public boolean dictionaryValuesCovered(LongPredicate predicate)
    {
        // DynamicFilter is a long-key membership contract. Binary dictionaries and DOUBLE's raw IEEE-754 bits do
        // not share that value domain, so they must retain the ordinary exact join/filter path.
        if (kind == Kind.BINARY || physicalType == Type.DOUBLE) {
            return false;
        }
        for (Chunk chunk : chunks) {
            ColumnMetaData metadata = chunk.metadata();
            long start = metadata.dictionary_page_offset > 0 ? metadata.dictionary_page_offset : metadata.data_page_offset;
            long limit = start + metadata.total_compressed_size;
            long bodyPosition = readPageHeader(chunk.segment(), start, limit);
            if (parsedPageType != PageType.DICTIONARY_PAGE.getValue()) {
                return false;
            }
            MemorySegment body = decompress(
                    chunk.segment(),
                    bodyPosition,
                    parsedCompressedSize,
                    parsedUncompressedSize,
                    metadata.codec);
            for (int index = 0; index < parsedValueCount; index++) {
                long value;
                if (kind == Kind.INT) {
                    value = body.get(LE_INT, (long) index * Integer.BYTES);
                }
                else if (kind == Kind.LONG) {
                    value = flbaDecimal
                            ? bigEndianSignedLong(body, (long) index * typeLength, typeLength)
                            : body.get(LE_LONG, (long) index * Long.BYTES);
                }
                else {
                    throw new AssertionError("Unsupported dictionary coverage kind: " + kind);
                }
                if (!predicate.test(value)) {
                    return false;
                }
            }
        }
        return !chunks.isEmpty();
    }

    private boolean decodeNextDataPage()
    {
        while (true) {
            if (segment == null || pagePosition >= chunkEnd) {
                if (!advanceChunk()) {
                    return false;
                }
            }
            long bodyPosition = readPageHeader(segment, pagePosition, chunkEnd);
            int compressedSize = parsedCompressedSize;
            int uncompressedSize = parsedUncompressedSize;
            CompressionCodec codec = chunks.get(chunkIndex).metadata().codec;
            long nextPagePosition = bodyPosition + compressedSize;

            if (parsedPageType == PageType.DICTIONARY_PAGE.getValue()) {
                MemorySegment body = decompress(segment, bodyPosition, compressedSize, uncompressedSize, codec);
                decodeDictionary(body, parsedValueCount);
                pagePosition = nextPagePosition;
                continue;
            }
            if (parsedPageType == PageType.DATA_PAGE.getValue()) {
                MemorySegment body = decompress(segment, bodyPosition, compressedSize, uncompressedSize, codec);
                decodeDataPageV1(body, parsedValueCount, parsedEncoding);
                pagePosition = nextPagePosition;
                return true;
            }
            throw new IllegalStateException("Unsupported page type for NitroParquet: " + parsedPageType);
        }
    }

    private long readPageHeader(MemorySegment input, long offset, long end)
    {
        if (!pageHeaderModeDecided) {
            useFastPageHeader = pageNavigationPolicy.fastPageHeader() &&
                    chunks.size() <= pageNavigationPolicy.fastPageHeaderMaxChunks();
            pageHeaderModeDecided = true;
        }
        if (useFastPageHeader) {
            long bodyPosition = fastPageHeaderReader.read(input, offset, end);
            parsedPageType = fastPageHeaderReader.type();
            parsedUncompressedSize = fastPageHeaderReader.uncompressedSize();
            parsedCompressedSize = fastPageHeaderReader.compressedSize();
            parsedValueCount = fastPageHeaderReader.valueCount();
            parsedEncoding = fastPageHeaderReader.encoding();
            return bodyPosition;
        }

        try (ParquetFile.SegmentInputStream in = new ParquetFile.SegmentInputStream(input, offset, end - offset)) {
            PageHeader header = Util.readPageHeader(in);
            parsedPageType = header.type.getValue();
            parsedUncompressedSize = header.uncompressed_page_size;
            parsedCompressedSize = header.compressed_page_size;
            if (header.type == PageType.DATA_PAGE) {
                parsedValueCount = header.data_page_header.num_values;
                parsedEncoding = header.data_page_header.encoding.getValue();
            }
            else if (header.type == PageType.DICTIONARY_PAGE) {
                parsedValueCount = header.dictionary_page_header.num_values;
                parsedEncoding = header.dictionary_page_header.encoding.getValue();
            }
            else {
                parsedValueCount = -1;
                parsedEncoding = -1;
            }
            return in.position();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to read Parquet page header", e);
        }
    }

    /**
     * Decompresses a page body into the reused heap buffer (zero-copy from the off-heap mapping), or, for
     * an uncompressed page, returns a zero-copy slice of the mapping itself. The returned segment is valid
     * only until the next call.
     */
    private MemorySegment decompress(MemorySegment fileSegment, long offset, int compressedSize, int uncompressedSize, CompressionCodec codec)
    {
        // Source accounting must follow pages the decoder actually consumes. Summing every selected column chunk
        // overstates physical work when late materialization or selection pushdown leaves payload pages untouched.
        // Count before the cache lookup as a cache hit still represents a page consumed by this source; the metric
        // describes source work rather than process-wide storage traffic shared by multiple source instances.
        consumedPageBytes = Math.addExact(consumedPageBytes, compressedSize);
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
        DecompressedPageCache.Source logicalSource = chunkIndex >= 0 && chunkIndex < chunks.size()
                ? chunks.get(chunkIndex).source()
                : null;
        if (decompressedPages != null && logicalSource != null) {
            MemorySegment cached = decompressedPages.lookup(logicalSource, offset, compressedSize, uncompressedSize);
            if (cached != null) {
                return cached;
            }
        }
        if (diagnostics.decompression()) {
            compressedPageCount++;
            compressedPageBytes += compressedSize;
            uncompressedPageBytes += uncompressedSize;
            minimumCompressedPageBytes = Math.min(minimumCompressedPageBytes, compressedSize);
            maximumCompressedPageBytes = Math.max(maximumCompressedPageBytes, compressedSize);
        }
        MemorySegment compressedSource = fileSegment.asSlice(offset, compressedSize);
        if (decompressedPages != null && logicalSource != null) {
            DecompressedPageCache.Reservation reservation = decompressedPages.reserve(
                    logicalSource, offset, compressedSize, uncompressedSize, SLACK);
            if (reservation != null) {
                MemorySegment cachedTarget = reservation.segment();
                try {
                    snappy.decompress(compressedSource, cachedTarget.asSlice(0, uncompressedSize));
                    decompressedPages.commit(reservation);
                    return cachedTarget;
                }
                catch (RuntimeException | Error failure) {
                    decompressedPages.abort(reservation);
                    throw failure;
                }
            }
        }
        if (decompressCapacity < uncompressedSize + SLACK) {
            decompressSegment = scratchArena.allocate(uncompressedSize + SLACK);
            decompressCapacity = uncompressedSize + SLACK;
        }
        MemorySegment target = decompressSegment.asSlice(0, uncompressedSize);
        snappy.decompress(compressedSource, target);
        return decompressSegment.asSlice(0, uncompressedSize + SLACK);
    }

    public String decompressionSummary()
    {
        if (compressedPageCount == 0) {
            return "pages=0";
        }
        return "pages=" + compressedPageCount +
                " compressed=" + compressedPageBytes +
                " uncompressed=" + uncompressedPageBytes +
                " compressed_min=" + minimumCompressedPageBytes +
                " compressed_max=" + maximumCompressedPageBytes +
                " compressed_avg=" + (compressedPageBytes / compressedPageCount) +
                " uncompressed_avg=" + (uncompressedPageBytes / compressedPageCount);
    }

    long consumedPageBytes()
    {
        return consumedPageBytes;
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
                dictionaryInts = replaceInts(dictionaryInts, numValues);
            }
            MemorySegment.copy(body, LE_INT, 0, dictionaryInts, 0, numValues);
        }
        else if (kind == Kind.LONG) {
            if (dictionaryLongs == null || dictionaryLongs.length < numValues) {
                dictionaryLongs = replaceLongs(dictionaryLongs, numValues);
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
            long cursor = 0;
            int total = 0;
            for (int i = 0; i < numValues; i++) {
                int length = body.get(LE_INT, cursor);
                cursor += 4 + length;
                total += length;
            }
            int[] offsets = arrayPool.borrowInts(numValues + 1);
            byte[] bytes = borrowDictionaryBytes(total);
            cursor = 0;
            int out = 0;
            for (int i = 0; i < numValues; i++) {
                int length = body.get(LE_INT, cursor);
                cursor += 4;
                offsets[i] = out;
                MemorySegment.copy(body, ValueLayout.JAVA_BYTE, cursor, bytes, out, length);
                out += length;
                cursor += length;
            }
            offsets[numValues] = out;
            dictionaryByteOffsets = offsets;
            dictionaryBytes = bytes;
            // The decoded dictionary arrays are the stable snapshot for this generation. They are intentionally not
            // reused across chunks because DictionaryVectors emitted from earlier chunks keep referencing them.
            org.weakref.nitro.data.BinaryVector dictionaryVector = new org.weakref.nitro.data.BinaryVector(
                    numValues,
                    offsets,
                    bytes).freezeContent();
            dictionaryVector.addTraits(java.util.Set.of(org.weakref.nitro.data.Utf8Traits.UTF8_STRING));
            dictionaryVectorCache.put(dictionaryGeneration, dictionaryVector);
            // Keep only the few most recent generations. A batch spans at most one chunk boundary (row groups are far
            // larger than a batch), so emission needs the current dictionary and a flat fallback needs at most the one
            // the batch started under. Without this the cache would retain every chunk's dictionary for the life of the
            // scan -- unbounded for a many-column scan over many row groups.
            if (!materializationPolicy.recycleBinaryDictionaryScratch()) {
                dictionaryVectorCache.keySet().removeIf(generation -> generation < dictionaryGeneration - 3);
            }
        }
    }

    private byte[] borrowDictionaryBytes(int required)
    {
        if (reusableDictionaryBytes.length >= required) {
            byte[] result = reusableDictionaryBytes;
            reusableDictionaryBytes = EMPTY_BYTES;
            return result;
        }
        return arrayPool.borrowBytes(required);
    }

    private byte[] retainLarger(byte[] retained, byte[] candidate)
    {
        if (candidate.length > retained.length) {
            arrayPool.release(retained);
            return candidate;
        }
        arrayPool.release(candidate);
        return retained;
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

    private void decodeDataPageV1(MemorySegment body, int valueCount, int encodingValue)
    {
        Encoding encoding = Encoding.findByValue(encodingValue);
        pageValueCount = valueCount;
        pageCursor = 0;
        pageBinaryDeferred = false;
        pageBinaryDictionaryStreaming = false;
        pageBinaryDictionaryNullFree = false;
        pageDirectDictionary = false;
        pageDirectPlain = false;
        pageDirectPlainBody = null;
        pageDirectPlainOffset = 0;

        long offset = 0;
        int nonNullCount = valueCount;
        boolean dictionary = encoding == Encoding.RLE_DICTIONARY || encoding == Encoding.PLAIN_DICTIONARY;
        // A predicate-over-dictionary lead-filter scan over a nullable dict page needs the page-position -> id-index
        // prefix (pageIdIndex), not the per-level array; decode the levels straight into that prefix in one pass.
        boolean filterDictPrefixBuilt = false;
        boolean streamNullableFilter = false;
        boolean streamBinaryDictionary = false;
        boolean streamNullableNumeric = false;
        pageDefStreaming = false;
        pagePlainStreaming = false;
        skipBody = null;
        pageFullyDecoded = false;
        if (optional) {
            // V1 definition levels: 4-byte LE length prefix, then RLE(bitWidth=1) of `valueCount` levels.
            int defLength = body.get(LE_INT, 0);
            offset = 4;
            rle.init(body, offset, 1);
            // Null-free pages (one RLE run of 1s) are the common case; detect them in O(1) and skip the O(page)
            // per-level materialization + sum. Only pages that actually contain nulls pay the full decode.
            if (!rle.consumeIfAllOnes(valueCount)) {
                rle.init(body, offset, 1);
                if (!filterScan && dictionary && kind == Kind.BINARY) {
                    defRle.init(body, offset, 1);
                    streamBinaryDictionary = true;
                }
                else if (fullSequentialReadRequested && !filterScan && kind != Kind.BINARY) {
                    // A full sequential scan can consume definitions and the dense value stream together in output-
                    // batch-sized runs. Avoid retaining page-sized definitions, ids, values, and nulls for every
                    // projected column and concurrent split of a wide scan.
                    defRle.init(body, offset, 1);
                    streamNullableNumeric = true;
                }
                else if (dictionaryFilterPolicy.nullableFilter().stream() && filterScan && dictionary && kind != Kind.BINARY) {
                    defRle.init(body, offset, 1);
                    streamNullableFilter = true;
                }
                else if (filterScan && dictionary && kind != Kind.BINARY) {
                    if (pageIdIndex.length < valueCount + 1) {
                        pageIdIndex = replaceInts(pageIdIndex, valueCount + 1);
                    }
                    nonNullCount = rle.readNonNullPrefix(pageIdIndex, valueCount);
                    filterDictPrefixBuilt = true;
                }
                else {
                    ensureDefCapacity(valueCount);
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
        pageFilterNullable = false;
        pageFilterFused = false;
        pageFilterNullableFused = false;
        if (dictionary) {
            int bitWidth = body.get(ValueLayout.JAVA_BYTE, offset) & 0xFF;
            offset += 1;
            rle.init(body, offset, bitWidth);
            if (!filterScan && kind == Kind.BINARY) {
                pageBinaryDeferred = true;
                pageBinaryDictionaryStreaming = true;
                pageBinaryDictionaryNullFree = !optional || !streamBinaryDictionary;
                pageFullyDecoded = true;
                return;
            }
            if (streamNullableNumeric) {
                pageDefStreaming = true;
                defPageCursor = 0;
                pageFullyDecoded = false;
                return;
            }
            boolean filterDict = filterScan && kind != Kind.BINARY;
            boolean nullFreePage = !optional || nonNullCount == valueCount;
            if (filterDict && streamNullableFilter) {
                pageFilterDict = true;
                pageFilterNullable = false;
                pageFilterNullableFused = true;
                return;
            }
            if (filterDict && dictionaryFilterPolicy.fusedFilter() && nullFreePage) {
                // Fused path: leave the ids unpacked. The RleReader stays positioned at the id stream; the filter loop
                // unpacks and consumes them one L1 tile at a time. No page-sized idBuffer round-trip to memory.
                pageFilterDict = true;
                pageFilterNullable = false;
                pageFilterFused = true;
                return;
            }
            boolean directDictionary = directFullDecodeRequested && numericDecodePolicy.directDictionaryBatchDecode() &&
                    nullFreePage && kind != Kind.BINARY;
            ensureIdCapacity(nonNullCount);
            rle.read(idBuffer, 0, nonNullCount);
            if (filterDict) {
                // Predicate-over-dictionary: keep the ids, don't materialize values. The page-position -> id-index
                // map was built straight from the definition levels above when the page has nulls.
                pageFilterDict = true;
                pageFilterNullable = optional && nonNullCount < valueCount;
                if (pageFilterNullable && !filterDictPrefixBuilt) {
                    if (pageIdIndex.length < valueCount + 1) {
                        pageIdIndex = replaceInts(pageIdIndex, valueCount + 1);
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
                pageFullyDecoded = true;
            }
            else if (directDictionary) {
                pageDirectDictionary = true;
                directDictionaryRows += valueCount;
            }
            else {
                ensurePageCapacity(valueCount);
                gatherDictionary(nonNullCount);
                pageFullyDecoded = true;
            }
        }
        else if (encoding == Encoding.PLAIN) {
            if (kind == Kind.BINARY) {
                ensurePageCapacity(valueCount);
                decodePlainBinary(body, offset, nonNullCount);
                pageBinaryDeferred = false;
                pageFullyDecoded = true;
            }
            else if (streamNullableNumeric) {
                pagePlainBody = body;
                pagePlainOffset = offset;
                plainValueCursor = 0;
                defPageCursor = 0;
                pagePlainStreaming = true;
                pageFullyDecoded = false;
            }
            else if (directFullDecodeRequested && numericDecodePolicy.directPlainBatchDecode() && nonNullCount == valueCount && !flbaDecimal) {
                pageDirectPlain = true;
                pageDirectPlainBody = body;
                pageDirectPlainOffset = offset;
                directPlainRows += valueCount;
            }
            else {
                ensurePageCapacity(valueCount);
                decodePlain(body, offset, nonNullCount);
                pageFullyDecoded = true;
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
     * Deferred binary-dictionary decode for selected reads: record each position's dictionary id row-aligned in
     * {@code pageDictIds} (sentinel 0 at null positions; nulls in {@code pageNulls}), skipping the eager byte
     * expansion {@link #gatherDictionaryBinary} performs.
     */
    private void scatterDictionaryBinaryIds(int nonNullCount)
    {
        if (pageDictIds.length < pageValueCount) {
            pageDictIds = replaceInts(pageDictIds, pageValueCount);
        }
        ensurePageNullCapacity(pageValueCount);
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
        if (materializationPolicy.presizePlainBinaryPage()) {
            decodePlainBinaryPresized(body, offset, nonNullCount);
            return;
        }
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

    private void decodePlainBinaryPresized(MemorySegment body, long offset, int nonNullCount)
    {
        pageBytesUsed = 0;
        long cursor = offset;
        long remaining = body.byteSize() - offset;
        ensurePageBytes((int) Math.min(Integer.MAX_VALUE, remaining));
        if (!optional || nonNullCount == pageValueCount) {
            for (int i = 0; i < nonNullCount; i++) {
                pageByteOffsets[i] = pageBytesUsed;
                cursor = appendPlainEntryPresized(body, cursor);
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
                    cursor = appendPlainEntryPresized(body, cursor);
                    pageNulls[i] = false;
                }
                else {
                    pageNulls[i] = true;
                }
            }
            pageByteOffsets[pageValueCount] = pageBytesUsed;
        }
    }

    private long appendPlainEntryPresized(MemorySegment body, long cursor)
    {
        int length = body.get(LE_INT, cursor);
        cursor += Integer.BYTES;
        MemorySegment.copy(body, ValueLayout.JAVA_BYTE, cursor, pageBytes, pageBytesUsed, length);
        pageBytesUsed += length;
        return cursor + length;
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
            pageBytes = growBytes(pageBytes, Math.max(pageBytes.length * 2, needed), pageBytesUsed);
        }
    }

    private void ensurePageCapacity(int valueCount)
    {
        ensurePageValueCapacity(valueCount);
        ensurePageNullCapacity(valueCount);
    }

    private void ensurePageValueCapacity(int valueCount)
    {
        if (kind == Kind.INT) {
            if (pageInts.length < valueCount) {
                pageInts = replaceInts(pageInts, valueCount);
            }
        }
        else if (kind == Kind.LONG) {
            if (pageLongs.length < valueCount) {
                pageLongs = replaceLongs(pageLongs, valueCount);
            }
        }
        else if (pageByteOffsets.length < valueCount + 1) {
            pageByteOffsets = replaceInts(pageByteOffsets, valueCount + 1);
        }
    }

    private void ensurePageNullCapacity(int valueCount)
    {
        if (optional && pageNulls.length < valueCount) {
            pageNulls = replaceBooleans(pageNulls, valueCount);
        }
    }

    private void ensureIdCapacity(int valueCount)
    {
        if (idBuffer.length < valueCount) {
            idBuffer = replaceInts(idBuffer, valueCount);
        }
    }

    private void ensureDefCapacity(int valueCount)
    {
        if (defBuffer.length < valueCount) {
            defBuffer = replaceInts(defBuffer, valueCount);
        }
    }

    private void ensureRunDefCapacity(int valueCount)
    {
        if (runDef.length < valueCount) {
            runDef = replaceInts(runDef, valueCount);
        }
    }

    private int[] replaceInts(int[] previous, int size)
    {
        int[] replacement = arrayPool.borrowInts(size);
        arrayPool.release(previous);
        return replacement;
    }

    private long[] replaceLongs(long[] previous, int size)
    {
        long[] replacement = arrayPool.borrowLongs(size);
        arrayPool.release(previous);
        return replacement;
    }

    private byte[] replaceBytes(byte[] previous, int size)
    {
        byte[] replacement = arrayPool.borrowBytes(size);
        arrayPool.release(previous);
        return replacement;
    }

    private boolean[] replaceBooleans(boolean[] previous, int size)
    {
        boolean[] replacement = arrayPool.borrowBooleans(size);
        arrayPool.release(previous);
        return replacement;
    }

    private byte[] growBytes(byte[] previous, int size, int used)
    {
        byte[] replacement = arrayPool.borrowBytes(size);
        System.arraycopy(previous, 0, replacement, 0, used);
        arrayPool.release(previous);
        return replacement;
    }
}
