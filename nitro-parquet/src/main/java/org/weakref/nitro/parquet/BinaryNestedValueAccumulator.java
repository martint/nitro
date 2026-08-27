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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.data.Utf8Traits.UTF8_VALID;

/** Reusable accumulation for BYTE_ARRAY leaves, preserving the schema's UTF-8 annotation. */
final class BinaryNestedValueAccumulator
        implements NestedValueAccumulator
{
    private static final int[] EMPTY_INTS = new int[0];
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private final boolean utf8;
    private final boolean nullable;
    private final ParquetMaterializationPolicy materializationPolicy;
    private PrimitiveArrayPool arrayPool;
    private int[] offsets = EMPTY_INTS;
    private byte[] data = EMPTY_BYTES;
    private boolean[] nulls = EMPTY_BOOLEANS;
    private int[] dictionaryIds = EMPTY_INTS;
    private int[] dictionaryFrequencies = EMPTY_INTS;
    private int[] dictionaryOffsets = EMPTY_INTS;
    private byte[] dictionaryData = EMPTY_BYTES;
    private boolean dictionaryCandidate;
    private int dictionaryGeneration;
    private int dictionarySize;
    private int dictionaryBytes;
    private int size;
    private int bytes;
    private boolean hasNulls;

    BinaryNestedValueAccumulator(boolean utf8, boolean nullable)
    {
        this(utf8, nullable, ParquetMaterializationPolicy.defaults());
    }

    BinaryNestedValueAccumulator(
            boolean utf8,
            boolean nullable,
            ParquetMaterializationPolicy materializationPolicy)
    {
        this.utf8 = utf8;
        this.nullable = nullable;
        this.materializationPolicy = requireNonNull(materializationPolicy, "materializationPolicy is null");
    }

    @Override
    public void reset(Allocator allocator)
    {
        PrimitiveArrayPool requestedPool = requireNonNull(allocator, "allocator is null").primitiveArrays();
        if (arrayPool != null && arrayPool != requestedPool) {
            throw new IllegalArgumentException("Nested accumulator cannot change allocator ownership");
        }
        arrayPool = requestedPool;
        dictionaryCandidate = true;
        dictionaryGeneration = -1;
        dictionarySize = 0;
        dictionaryBytes = 0;
        size = 0;
        bytes = 0;
        hasNulls = false;
    }

    @Override
    public void append(PhysicalValueDecoder decoder, int ordinal, int dictionaryId)
    {
        if (!(decoder instanceof BinaryValueDecoder binary)) {
            throw new IllegalArgumentException("Binary accumulator requires a binary physical decoder");
        }
        if (dictionaryId >= 0 && appendDictionaryId(binary, dictionaryId)) {
            return;
        }
        ensureFlat();
        int length = binary.length(ordinal, dictionaryId);
        ensurePositionCapacity(size + 1);
        ensureByteCapacity(bytes + length);
        offsets[size] = bytes;
        binary.copy(ordinal, dictionaryId, data, bytes);
        bytes += length;
        size++;
        offsets[size] = bytes;
        if (nullable) {
            nulls[size - 1] = false;
        }
    }

    @Override
    public void appendEvents(
            PhysicalValueDecoder decoder,
            int[] valueOrdinals,
            int[] pageDictionaryIds,
            int eventOffset,
            int eventCount)
    {
        if (!(decoder instanceof BinaryValueDecoder binary)) {
            throw new IllegalArgumentException("Binary accumulator requires a binary physical decoder");
        }
        if (pageDictionaryIds == null || !prepareDictionary(binary)) {
            ensureFlat();
            NestedValueAccumulator.super.appendEvents(decoder, valueOrdinals, pageDictionaryIds, eventOffset, eventCount);
            return;
        }
        NestedValueAccumulator.super.appendEvents(decoder, valueOrdinals, pageDictionaryIds, eventOffset, eventCount);
    }

    @Override
    public void appendDictionaryRun(PhysicalValueDecoder decoder, int[] pageDictionaryIds, int ordinal, int count)
    {
        if (!(decoder instanceof BinaryValueDecoder binary) || !prepareDictionary(binary)) {
            NestedValueAccumulator.super.appendDictionaryRun(decoder, pageDictionaryIds, ordinal, count);
            return;
        }
        ensurePositionCapacity(size + count);
        System.arraycopy(pageDictionaryIds, ordinal, dictionaryIds, size, count);
        if (nullable) {
            Arrays.fill(nulls, size, size + count, false);
        }
        size += count;
    }

    @Override
    public void appendPlainRun(PhysicalValueDecoder decoder, int ordinal, int count)
    {
        ensureFlat();
        NestedValueAccumulator.super.appendPlainRun(decoder, ordinal, count);
    }

    @Override
    public void appendNull()
    {
        if (!nullable) {
            throw new IllegalArgumentException("Required nested value is missing");
        }
        hasNulls = true;
        ensurePositionCapacity(size + 1);
        if (dictionaryCandidate) {
            dictionaryIds[size] = -1;
            nulls[size] = true;
            size++;
            return;
        }
        offsets[size] = bytes;
        nulls[size] = true;
        size++;
        offsets[size] = bytes;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public Streams materialize(Allocator allocator, Allocator.Context context)
    {
        if (shouldMaterializeDictionary()) {
            return materializeDictionary(allocator, context);
        }
        ensureFlat();
        Streams recovered = materializeRecoveredDictionary(allocator, context);
        if (recovered != null) {
            return recovered;
        }
        BinaryVector result = BinaryVector.allocate(allocator, context, size, bytes);
        System.arraycopy(offsets, 0, result.offsets(), 0, size + 1);
        System.arraycopy(data, 0, result.data(), 0, bytes);
        if (utf8) {
            result.addTrait(UTF8_VALID);
        }
        if (!nullable) {
            return Streams.ofValues(result);
        }
        BooleanVector resultNulls = allocator.allocate(context, BooleanVector.class, size, BooleanVector::new);
        System.arraycopy(nulls, 0, resultNulls.values(), 0, size);
        return Streams.ofValuesAndNulls(result, resultNulls);
    }

    /**
     * Recovers a small repeated physical domain when a writer abandoned dictionary encoding for a plain page.
     * Values remain physical bytes here; any logical binding transforms the recovered domain after materialization.
     */
    private Streams materializeRecoveredDictionary(Allocator allocator, Allocator.Context context)
    {
        if (!materializationPolicy.binaryDictionary() || size == 0) {
            return null;
        }
        int maxEntries = materializationPolicy.dictionaryDomainFrequencyMaxEntries();
        int minRowsPerEntry = materializationPolicy.dictionaryDomainFrequencyMinRowsPerEntry();
        if (maxEntries == 0 || minRowsPerEntry == 0 || size < minRowsPerEntry) {
            return null;
        }

        dictionaryIds = grow(dictionaryIds, size);
        dictionaryFrequencies = grow(dictionaryFrequencies, maxEntries);
        dictionaryOffsets = grow(dictionaryOffsets, maxEntries + 1);
        Arrays.fill(dictionaryFrequencies, 0, maxEntries, 0);
        dictionarySize = 0;
        dictionaryBytes = 0;
        dictionaryOffsets[0] = 0;
        for (int position = 0; position < size; position++) {
            if (nullable && nulls[position]) {
                dictionaryIds[position] = -1;
                continue;
            }
            int start = offsets[position];
            int end = offsets[position + 1];
            int id = findDictionaryValue(start, end);
            if (id < 0) {
                if (dictionarySize == maxEntries || (long) (dictionarySize + 1) * minRowsPerEntry > size) {
                    return null;
                }
                int length = end - start;
                dictionaryData = grow(dictionaryData, dictionaryBytes + length);
                System.arraycopy(data, start, dictionaryData, dictionaryBytes, length);
                id = dictionarySize++;
                dictionaryBytes += length;
                dictionaryOffsets[dictionarySize] = dictionaryBytes;
            }
            dictionaryIds[position] = id;
            dictionaryFrequencies[id]++;
        }

        int outputDictionarySize = dictionarySize + (hasNulls ? 1 : 0);
        long dictionaryFootprint = dictionaryBytes +
                (long) Integer.BYTES * (dictionarySize + 1L + size + outputDictionarySize) +
                (hasNulls ? outputDictionarySize : 0);
        long flatFootprint = bytes + (long) Integer.BYTES * (size + 1L) + (nullable ? size : 0);
        if (dictionaryFootprint >= flatFootprint) {
            return null;
        }
        return materializeDictionary(allocator, context);
    }

    private int findDictionaryValue(int start, int end)
    {
        int length = end - start;
        for (int id = 0; id < dictionarySize; id++) {
            int dictionaryStart = dictionaryOffsets[id];
            int dictionaryEnd = dictionaryOffsets[id + 1];
            if (dictionaryEnd - dictionaryStart == length &&
                    Arrays.equals(data, start, end, dictionaryData, dictionaryStart, dictionaryEnd)) {
                return id;
            }
        }
        return -1;
    }

    private boolean appendDictionaryId(BinaryValueDecoder decoder, int dictionaryId)
    {
        if (!prepareDictionary(decoder)) {
            return false;
        }
        ensurePositionCapacity(size + 1);
        dictionaryIds[size] = dictionaryId;
        if (nullable) {
            nulls[size] = false;
        }
        size++;
        return true;
    }

    private boolean prepareDictionary(BinaryValueDecoder decoder)
    {
        if (!dictionaryCandidate) {
            return false;
        }
        if (dictionaryGeneration == decoder.dictionaryGeneration()) {
            return true;
        }
        if (dictionaryGeneration >= 0) {
            ensureFlat();
            return false;
        }
        dictionaryGeneration = decoder.dictionaryGeneration();
        dictionarySize = decoder.dictionarySize();
        dictionaryBytes = decoder.dictionaryByteSize();
        dictionaryOffsets = grow(dictionaryOffsets, dictionarySize + 1);
        dictionaryData = grow(dictionaryData, dictionaryBytes);
        dictionaryFrequencies = grow(dictionaryFrequencies, dictionarySize);
        decoder.copyDictionary(dictionaryOffsets, dictionaryData);
        return true;
    }

    private boolean shouldMaterializeDictionary()
    {
        if (!materializationPolicy.binaryDictionary() || !dictionaryCandidate || dictionaryGeneration < 0) {
            return false;
        }
        int outputDictionarySize = dictionarySize + (hasNulls ? 1 : 0);
        Arrays.fill(dictionaryFrequencies, 0, dictionarySize, 0);
        long logicalBytes = 0;
        for (int position = 0; position < size; position++) {
            int id = dictionaryIds[position];
            if (id >= 0) {
                dictionaryFrequencies[id]++;
            }
        }
        for (int id = 0; id < dictionarySize; id++) {
            logicalBytes += (long) dictionaryFrequencies[id] * (dictionaryOffsets[id + 1] - dictionaryOffsets[id]);
        }
        long dictionaryFootprint = dictionaryBytes +
                (long) Integer.BYTES * (dictionarySize + 1L + size + outputDictionarySize) +
                (hasNulls ? outputDictionarySize : 0);
        long flatFootprint = logicalBytes + (long) Integer.BYTES * (size + 1L) + (nullable ? size : 0);
        return dictionaryFootprint < flatFootprint;
    }

    private Streams materializeDictionary(Allocator allocator, Allocator.Context context)
    {
        int outputDictionarySize = dictionarySize + (hasNulls ? 1 : 0);
        int nullId = outputDictionarySize - 1;
        I32Vector ids = I32Vector.allocate(allocator, context, size);
        I32Vector frequencies = I32Vector.allocate(allocator, context, outputDictionarySize);
        System.arraycopy(dictionaryFrequencies, 0, frequencies.values(), 0, dictionarySize);
        if (hasNulls) {
            frequencies.values()[nullId] = 0;
        }
        for (int position = 0; position < size; position++) {
            int id = dictionaryIds[position] < 0 ? nullId : dictionaryIds[position];
            ids.values()[position] = id;
            if (id == nullId && hasNulls) {
                frequencies.values()[nullId]++;
            }
        }
        BinaryVector dictionary = BinaryVector.allocate(allocator, context, outputDictionarySize, dictionaryBytes);
        System.arraycopy(dictionaryOffsets, 0, dictionary.offsets(), 0, dictionarySize + 1);
        if (hasNulls) {
            dictionary.offsets()[outputDictionarySize] = dictionaryBytes;
        }
        System.arraycopy(dictionaryData, 0, dictionary.data(), 0, dictionaryBytes);
        if (utf8) {
            dictionary.addTrait(UTF8_VALID);
        }
        DictionaryVector result = DictionaryVector.wrapOwnedIdsWithDomainFrequencies(
                ids,
                size,
                dictionary.freezeContent(),
                frequencies);
        if (!hasNulls) {
            if (!nullable) {
                return Streams.ofValues(result);
            }
            BooleanVector resultNulls = allocator.allocate(
                    context,
                    BooleanVector.class,
                    size,
                    BooleanVector::new);
            Arrays.fill(resultNulls.values(), 0, size, false);
            return Streams.ofValuesAndNulls(result, resultNulls);
        }
        BooleanVector dictionaryNulls = allocator.allocate(
                context,
                BooleanVector.class,
                outputDictionarySize,
                BooleanVector::new);
        Arrays.fill(dictionaryNulls.values(), 0, outputDictionarySize, false);
        dictionaryNulls.values()[nullId] = true;
        return Streams.of(result, result.sharedMappingWithValues(dictionaryNulls.freezeContent()), null);
    }

    private void ensureFlat()
    {
        if (!dictionaryCandidate) {
            return;
        }
        dictionaryCandidate = false;
        ensurePositionCapacity(size);
        int requiredBytes = 0;
        for (int position = 0; position < size; position++) {
            int id = dictionaryIds[position];
            if (id >= 0) {
                requiredBytes += dictionaryOffsets[id + 1] - dictionaryOffsets[id];
            }
        }
        ensureByteCapacity(requiredBytes);
        bytes = 0;
        for (int position = 0; position < size; position++) {
            offsets[position] = bytes;
            int id = dictionaryIds[position];
            if (id >= 0) {
                int start = dictionaryOffsets[id];
                int length = dictionaryOffsets[id + 1] - start;
                System.arraycopy(dictionaryData, start, data, bytes, length);
                bytes += length;
            }
        }
        offsets[size] = bytes;
    }

    private void ensurePositionCapacity(int required)
    {
        if (offsets.length >= required + 1 &&
                dictionaryIds.length >= required &&
                (!nullable || nulls.length >= required)) {
            return;
        }
        int currentCapacity = Math.max(dictionaryIds.length, Math.max(0, offsets.length - 1));
        int capacity = Math.max(required, Math.max(16, currentCapacity * 2));
        if (offsets.length < capacity + 1) {
            int[] replacementOffsets = arrayPool.borrowInts(capacity + 1);
            System.arraycopy(offsets, 0, replacementOffsets, 0, Math.min(offsets.length, size + 1));
            arrayPool.release(offsets);
            offsets = replacementOffsets;
        }
        if (nullable && nulls.length < capacity) {
            boolean[] replacementNulls = arrayPool.borrowBooleans(capacity);
            System.arraycopy(nulls, 0, replacementNulls, 0, size);
            arrayPool.release(nulls);
            nulls = replacementNulls;
        }
        if (dictionaryIds.length < capacity) {
            int[] replacementIds = arrayPool.borrowInts(capacity);
            System.arraycopy(dictionaryIds, 0, replacementIds, 0, size);
            arrayPool.release(dictionaryIds);
            dictionaryIds = replacementIds;
        }
    }

    private void ensureByteCapacity(int required)
    {
        if (data.length < required) {
            byte[] replacement = arrayPool.borrowBytes(Math.max(required, Math.max(64, data.length * 2)));
            System.arraycopy(data, 0, replacement, 0, bytes);
            arrayPool.release(data);
            data = replacement;
        }
    }

    private int[] grow(int[] current, int required)
    {
        if (current.length >= required) {
            return current;
        }
        int[] replacement = arrayPool.borrowInts(Math.max(required, Math.max(16, current.length * 2)));
        arrayPool.release(current);
        return replacement;
    }

    private byte[] grow(byte[] current, int required)
    {
        if (current.length >= required) {
            return current;
        }
        byte[] replacement = arrayPool.borrowBytes(Math.max(required, Math.max(64, current.length * 2)));
        arrayPool.release(current);
        return replacement;
    }

    @Override
    public void close()
    {
        if (arrayPool != null) {
            arrayPool.release(offsets);
            arrayPool.release(data);
            arrayPool.release(nulls);
            arrayPool.release(dictionaryIds);
            arrayPool.release(dictionaryFrequencies);
            arrayPool.release(dictionaryOffsets);
            arrayPool.release(dictionaryData);
            offsets = EMPTY_INTS;
            data = EMPTY_BYTES;
            nulls = EMPTY_BOOLEANS;
            dictionaryIds = EMPTY_INTS;
            dictionaryFrequencies = EMPTY_INTS;
            dictionaryOffsets = EMPTY_INTS;
            dictionaryData = EMPTY_BYTES;
            arrayPool = null;
            dictionaryCandidate = false;
            dictionaryGeneration = -1;
            dictionarySize = 0;
            dictionaryBytes = 0;
            size = 0;
            bytes = 0;
        }
    }
}
