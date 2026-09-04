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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/** Reusable accumulation for integer-backed physical leaves. */
final class LongNestedValueAccumulator
        implements NestedValueAccumulator
{
    private static final long[] EMPTY_LONGS = new long[0];
    private static final int[] EMPTY_INTS = new int[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private final boolean outputInt32;
    private final boolean nullable;
    private final ParquetMaterializationPolicy materializationPolicy;
    private PrimitiveArrayPool arrayPool;
    private long[] values = EMPTY_LONGS;
    private long[] dictionary = EMPTY_LONGS;
    private int[] dictionaryIds = EMPTY_INTS;
    private int[] dictionaryFrequencies = EMPTY_INTS;
    private boolean[] nulls = EMPTY_BOOLEANS;
    private long[] recoveryKeys = EMPTY_LONGS;
    private int[] recoverySlots = EMPTY_INTS;
    private Vector directValues;
    private BooleanVector directNulls;
    private boolean dictionaryCandidate;
    private int dictionaryGeneration;
    private int dictionarySize;
    private int size;

    LongNestedValueAccumulator(boolean outputInt32, boolean nullable)
    {
        this(outputInt32, nullable, ParquetMaterializationPolicy.defaults());
    }

    LongNestedValueAccumulator(
            boolean outputInt32,
            boolean nullable,
            ParquetMaterializationPolicy materializationPolicy)
    {
        this.outputInt32 = outputInt32;
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
        size = 0;
    }

    @Override
    public void reset(Allocator allocator, Allocator.Context context, int exactSize)
    {
        reset(allocator);
        dictionaryCandidate = false;
        directValues = outputInt32
                ? I32Vector.allocate(allocator, context, exactSize)
                : I64Vector.allocate(allocator, context, exactSize);
        directNulls = nullable
                ? allocator.allocate(context, BooleanVector.class, exactSize, BooleanVector::new)
                : null;
    }

    @Override
    public void append(PhysicalValueDecoder decoder, int ordinal, int dictionaryId)
    {
        if (!(decoder instanceof LongValueDecoder longs)) {
            throw new IllegalArgumentException("Integer accumulator requires an integer physical decoder");
        }
        if (dictionaryId >= 0 && appendDictionaryId(longs, dictionaryId)) {
            return;
        }
        ensureFlat();
        long value = longs.value(ordinal, dictionaryId);
        if (directValues instanceof I32Vector integers) {
            integers.values()[size] = toIntExact(value);
        }
        else if (directValues instanceof I64Vector longValues) {
            longValues.values()[size] = value;
        }
        else {
            ensureCapacity(size + 1);
            values[size] = value;
        }
        if (nullable) {
            nullValues()[size] = false;
        }
        size++;
    }

    @Override
    public void appendPlainRun(PhysicalValueDecoder decoder, int ordinal, int count)
    {
        if (!(decoder instanceof LongValueDecoder longs)) {
            throw new IllegalArgumentException("Integer accumulator requires an integer physical decoder");
        }
        if (count < 0) {
            throw new IllegalArgumentException("count is negative");
        }
        ensureFlat();
        if (directValues instanceof I64Vector longValues) {
            longs.copyPlain(ordinal, longValues.values(), size, count);
        }
        else if (directValues instanceof I32Vector integers) {
            for (int index = 0; index < count; index++) {
                integers.values()[size + index] = toIntExact(longs.value(ordinal + index, -1));
            }
        }
        else {
            ensureCapacity(size + count);
            longs.copyPlain(ordinal, values, size, count);
        }
        if (nullable) {
            java.util.Arrays.fill(nullValues(), size, size + count, false);
        }
        size += count;
    }

    @Override
    public void appendDictionaryRun(PhysicalValueDecoder decoder, int[] dictionaryIds, int ordinal, int count)
    {
        if (!(decoder instanceof LongValueDecoder longs)) {
            throw new IllegalArgumentException("Integer accumulator requires an integer physical decoder");
        }
        if (count < 0) {
            throw new IllegalArgumentException("count is negative");
        }
        if (prepareDictionary(longs)) {
            ensureDictionaryPositionCapacity(size + count);
            System.arraycopy(dictionaryIds, ordinal, this.dictionaryIds, size, count);
            if (nullable) {
                Arrays.fill(nullValues(), size, size + count, false);
            }
            size += count;
            return;
        }
        ensureFlat();
        if (directValues instanceof I64Vector longValues) {
            longs.copyDictionary(dictionaryIds, ordinal, longValues.values(), size, count);
        }
        else {
            ensureCapacity(size + count);
            longs.copyDictionary(dictionaryIds, ordinal, values, size, count);
            if (directValues instanceof I32Vector integers) {
                for (int index = 0; index < count; index++) {
                    integers.values()[size + index] = toIntExact(values[size + index]);
                }
            }
        }
        if (nullable) {
            java.util.Arrays.fill(nullValues(), size, size + count, false);
        }
        size += count;
    }

    @Override
    public void appendNull()
    {
        if (!nullable) {
            throw new IllegalArgumentException("Required nested value is missing");
        }
        if (dictionaryCandidate) {
            ensureDictionaryPositionCapacity(size + 1);
            dictionaryIds[size] = -1;
        }
        else if (directValues == null) {
            ensureCapacity(size + 1);
            values[size] = 0;
        }
        nullValues()[size] = true;
        size++;
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
        if (directValues != null) {
            Vector result = directValues;
            BooleanVector resultNulls = directNulls;
            directValues = null;
            directNulls = null;
            return resultNulls == null ? Streams.ofValues(result) : Streams.ofValuesAndNulls(result, resultNulls);
        }
        Vector result;
        if (outputInt32) {
            I32Vector integers = I32Vector.allocate(allocator, context, size);
            for (int index = 0; index < size; index++) {
                integers.values()[index] = toIntExact(values[index]);
            }
            result = integers;
        }
        else {
            I64Vector longs = I64Vector.allocate(allocator, context, size);
            System.arraycopy(values, 0, longs.values(), 0, size);
            result = longs;
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
     * The decision is representation- and cost-based: it is disabled for compact INT32 output, bounded by the
     * injected materialization policy, and requires both exact average reuse and a smaller encoded footprint.
     */
    private Streams materializeRecoveredDictionary(Allocator allocator, Allocator.Context context)
    {
        if (outputInt32 || !materializationPolicy.numericDictionary() || size == 0) {
            return null;
        }
        int maxEntries = materializationPolicy.dictionaryDomainFrequencyMaxEntries();
        int minRowsPerEntry = materializationPolicy.dictionaryDomainFrequencyMinRowsPerEntry();
        if (maxEntries == 0 || minRowsPerEntry == 0 || size < minRowsPerEntry) {
            return null;
        }
        if ((long) maxEntries * 2 > (1L << 30)) {
            return null;
        }
        int tableSize = 1;
        while (tableSize < maxEntries * 2) {
            tableSize <<= 1;
        }
        recoveryKeys = grow(recoveryKeys, tableSize);
        recoverySlots = grow(recoverySlots, tableSize);
        Arrays.fill(recoverySlots, 0, tableSize, 0);
        dictionary = grow(dictionary, maxEntries);
        dictionaryFrequencies = grow(dictionaryFrequencies, maxEntries);
        if (dictionaryIds.length < size) {
            int[] replacement = arrayPool.borrowInts(size);
            arrayPool.release(dictionaryIds);
            dictionaryIds = replacement;
        }
        Arrays.fill(dictionaryFrequencies, 0, maxEntries, 0);

        long[] flatValues = directValues instanceof I64Vector longs ? longs.values() : values;
        boolean[] flatNulls = nullable ? nullValues() : null;
        int distinct = 0;
        int nullId = -1;
        int mask = tableSize - 1;
        long previousValue = 0;
        int previousId = -1;
        boolean previousNull = false;
        for (int position = 0; position < size; position++) {
            if (flatNulls != null && flatNulls[position]) {
                if (previousNull) {
                    dictionaryIds[position] = previousId;
                    dictionaryFrequencies[previousId]++;
                    continue;
                }
                if (nullId < 0) {
                    if (distinct == maxEntries || (long) (distinct + 1) * minRowsPerEntry > size) {
                        return null;
                    }
                    nullId = distinct++;
                    dictionary[nullId] = 0;
                }
                dictionaryIds[position] = nullId;
                dictionaryFrequencies[nullId]++;
                previousId = nullId;
                previousNull = true;
                continue;
            }
            long value = flatValues[position];
            if (!previousNull && previousId >= 0 && value == previousValue) {
                dictionaryIds[position] = previousId;
                dictionaryFrequencies[previousId]++;
                continue;
            }
            int slot = mix(value) & mask;
            int entry;
            while ((entry = recoverySlots[slot]) != 0 && recoveryKeys[slot] != value) {
                slot = (slot + 1) & mask;
            }
            int id;
            if (entry == 0) {
                if (distinct == maxEntries || (long) (distinct + 1) * minRowsPerEntry > size) {
                    return null;
                }
                id = distinct++;
                recoveryKeys[slot] = value;
                recoverySlots[slot] = id + 1;
                dictionary[id] = value;
            }
            else {
                id = entry - 1;
            }
            dictionaryIds[position] = id;
            dictionaryFrequencies[id]++;
            previousValue = value;
            previousId = id;
            previousNull = false;
        }

        long dictionaryFootprint = (long) Long.BYTES * distinct +
                (long) Integer.BYTES * (size + distinct);
        if (dictionaryFootprint >= (long) Long.BYTES * size) {
            return null;
        }

        I32Vector ids = I32Vector.allocate(allocator, context, size);
        System.arraycopy(dictionaryIds, 0, ids.values(), 0, size);
        I32Vector frequencies = I32Vector.allocate(allocator, context, distinct);
        System.arraycopy(dictionaryFrequencies, 0, frequencies.values(), 0, distinct);
        I64Vector domain = I64Vector.allocate(allocator, context, distinct);
        System.arraycopy(dictionary, 0, domain.values(), 0, distinct);
        DictionaryVector encoded = DictionaryVector.wrapOwnedIdsWithDomainFrequencies(
                ids,
                size,
                domain.freezeContent(),
                frequencies);

        if (directValues != null) {
            allocator.release(context, directValues);
            directValues = null;
        }
        Vector resultNulls = null;
        if (nullId >= 0) {
            if (directNulls != null) {
                allocator.release(context, directNulls);
            }
            BooleanVector domainNulls = allocator.allocate(context, BooleanVector.class, distinct, BooleanVector::new);
            domainNulls.values()[nullId] = true;
            resultNulls = encoded.sharedMappingWithValues(domainNulls.freezeContent());
        }
        else if (nullable) {
            resultNulls = directNulls;
            if (resultNulls == null) {
                resultNulls = allocator.allocate(context, BooleanVector.class, size, BooleanVector::new);
            }
        }
        directNulls = null;
        return resultNulls == null ? Streams.ofValues(encoded) : Streams.of(encoded, resultNulls, null);
    }

    private static int mix(long value)
    {
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        value *= 0xc4ceb9fe1a85ec53L;
        value ^= value >>> 33;
        return (int) value;
    }

    private boolean appendDictionaryId(LongValueDecoder decoder, int dictionaryId)
    {
        if (!prepareDictionary(decoder)) {
            return false;
        }
        ensureDictionaryPositionCapacity(size + 1);
        dictionaryIds[size] = dictionaryId;
        if (nullable) {
            nulls[size] = false;
        }
        size++;
        return true;
    }

    private boolean prepareDictionary(LongValueDecoder decoder)
    {
        if (!dictionaryCandidate || decoder.dictionaryGeneration() < 0) {
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
        dictionary = grow(dictionary, dictionarySize);
        dictionaryFrequencies = grow(dictionaryFrequencies, dictionarySize);
        decoder.copyDictionaryValues(dictionary, 0);
        return true;
    }

    private boolean shouldMaterializeDictionary()
    {
        if (!dictionaryCandidate || dictionaryGeneration < 0) {
            return false;
        }
        int outputDictionarySize = dictionarySize + (nullable ? 1 : 0);
        long dictionaryFootprint = (long) (outputInt32 ? Integer.BYTES : Long.BYTES) * outputDictionarySize +
                (long) Integer.BYTES * (size + outputDictionarySize) +
                (nullable ? outputDictionarySize : 0);
        long flatFootprint = (long) (outputInt32 ? Integer.BYTES : Long.BYTES) * size + (nullable ? size : 0);
        return dictionaryFootprint < flatFootprint;
    }

    private Streams materializeDictionary(Allocator allocator, Allocator.Context context)
    {
        int outputDictionarySize = dictionarySize + (nullable ? 1 : 0);
        int nullId = outputDictionarySize - 1;
        I32Vector ids = I32Vector.allocate(allocator, context, size);
        I32Vector frequencies = I32Vector.allocate(allocator, context, outputDictionarySize);
        Arrays.fill(frequencies.values(), 0, outputDictionarySize, 0);
        Arrays.fill(dictionaryFrequencies, 0, dictionarySize, 0);
        for (int position = 0; position < size; position++) {
            int id = dictionaryIds[position] < 0 ? nullId : dictionaryIds[position];
            ids.values()[position] = id;
            if (id == nullId && nullable) {
                frequencies.values()[nullId]++;
            }
            else {
                dictionaryFrequencies[id]++;
            }
        }
        System.arraycopy(dictionaryFrequencies, 0, frequencies.values(), 0, dictionarySize);

        Vector domain;
        if (outputInt32) {
            I32Vector integers = I32Vector.allocate(allocator, context, outputDictionarySize);
            for (int id = 0; id < dictionarySize; id++) {
                integers.values()[id] = toIntExact(dictionary[id]);
            }
            domain = integers;
        }
        else {
            I64Vector longs = I64Vector.allocate(allocator, context, outputDictionarySize);
            System.arraycopy(dictionary, 0, longs.values(), 0, dictionarySize);
            domain = longs;
        }
        DictionaryVector result = DictionaryVector.wrapOwnedIdsWithDomainFrequencies(
                ids,
                size,
                domain.freezeContent(),
                frequencies);
        if (!nullable) {
            return Streams.ofValues(result);
        }
        BooleanVector domainNulls = allocator.allocate(
                context,
                BooleanVector.class,
                outputDictionarySize,
                BooleanVector::new);
        domainNulls.values()[nullId] = true;
        return Streams.of(result, result.sharedMappingWithValues(domainNulls.freezeContent()), null);
    }

    private void ensureFlat()
    {
        if (!dictionaryCandidate) {
            return;
        }
        dictionaryCandidate = false;
        ensureCapacity(size);
        for (int position = 0; position < size; position++) {
            int id = dictionaryIds[position];
            values[position] = id < 0 ? 0 : dictionary[id];
        }
    }

    private void ensureDictionaryPositionCapacity(int required)
    {
        if (dictionaryIds.length < required) {
            int capacity = Math.max(required, Math.max(16, dictionaryIds.length * 2));
            int[] replacement = arrayPool.borrowInts(capacity);
            System.arraycopy(dictionaryIds, 0, replacement, 0, size);
            arrayPool.release(dictionaryIds);
            dictionaryIds = replacement;
        }
        ensureNullCapacity(required);
    }

    private void ensureCapacity(int required)
    {
        if (values.length >= required) {
            ensureNullCapacity(required);
            return;
        }
        int capacity = Math.max(required, Math.max(16, values.length * 2));
        long[] replacement = arrayPool.borrowLongs(capacity);
        System.arraycopy(values, 0, replacement, 0, Math.min(size, values.length));
        arrayPool.release(values);
        values = replacement;
        ensureNullCapacity(capacity);
    }

    private void ensureNullCapacity(int required)
    {
        if (nullable && nulls.length < required) {
            boolean[] replacementNulls = arrayPool.borrowBooleans(required);
            System.arraycopy(nulls, 0, replacementNulls, 0, size);
            arrayPool.release(nulls);
            nulls = replacementNulls;
        }
    }

    private long[] grow(long[] current, int required)
    {
        if (current.length >= required) {
            return current;
        }
        long[] replacement = arrayPool.borrowLongs(Math.max(required, Math.max(16, current.length * 2)));
        arrayPool.release(current);
        return replacement;
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

    private boolean[] nullValues()
    {
        return directNulls == null ? nulls : directNulls.values();
    }

    @Override
    public void close()
    {
        if (arrayPool != null) {
            arrayPool.release(values);
            arrayPool.release(dictionary);
            arrayPool.release(dictionaryIds);
            arrayPool.release(dictionaryFrequencies);
            arrayPool.release(nulls);
            arrayPool.release(recoveryKeys);
            arrayPool.release(recoverySlots);
            values = EMPTY_LONGS;
            dictionary = EMPTY_LONGS;
            dictionaryIds = EMPTY_INTS;
            dictionaryFrequencies = EMPTY_INTS;
            nulls = EMPTY_BOOLEANS;
            recoveryKeys = EMPTY_LONGS;
            recoverySlots = EMPTY_INTS;
            arrayPool = null;
            dictionaryCandidate = false;
            dictionaryGeneration = -1;
            dictionarySize = 0;
            size = 0;
            directValues = null;
            directNulls = null;
        }
    }
}
