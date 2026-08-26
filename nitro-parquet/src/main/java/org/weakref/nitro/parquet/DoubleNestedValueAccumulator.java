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
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/** Reusable accumulation for DOUBLE physical leaves. */
final class DoubleNestedValueAccumulator
        implements NestedValueAccumulator
{
    private static final double[] EMPTY_DOUBLES = new double[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private final boolean nullable;
    private PrimitiveArrayPool arrayPool;
    private double[] values = EMPTY_DOUBLES;
    private boolean[] nulls = EMPTY_BOOLEANS;
    private F64Vector directValues;
    private BooleanVector directNulls;
    private Allocator directAllocator;
    private Allocator.Context directContext;
    private I32Vector directIds;
    private I32Vector directDomainFrequencies;
    private F64Vector directDictionary;
    private BooleanVector directDictionaryNulls;
    private int directDictionaryGeneration = -1;
    private int exactSize = -1;
    private int size;

    DoubleNestedValueAccumulator(boolean nullable)
    {
        this.nullable = nullable;
    }

    @Override
    public void reset(Allocator allocator)
    {
        releaseDirect();
        PrimitiveArrayPool requestedPool = requireNonNull(allocator, "allocator is null").primitiveArrays();
        if (arrayPool != null && arrayPool != requestedPool) {
            throw new IllegalArgumentException("Nested accumulator cannot change allocator ownership");
        }
        arrayPool = requestedPool;
        directValues = null;
        directNulls = null;
        directAllocator = null;
        directContext = null;
        directIds = null;
        directDomainFrequencies = null;
        directDictionary = null;
        directDictionaryNulls = null;
        directDictionaryGeneration = -1;
        exactSize = -1;
        size = 0;
    }

    @Override
    public void reset(Allocator allocator, Allocator.Context context, int exactSize)
    {
        reset(allocator);
        directAllocator = allocator;
        directContext = requireNonNull(context, "context is null");
        this.exactSize = exactSize;
        directNulls = nullable
                ? allocator.allocate(context, BooleanVector.class, exactSize, BooleanVector::new)
                : null;
    }

    @Override
    public void append(PhysicalValueDecoder decoder, int ordinal, int dictionaryId)
    {
        if (!(decoder instanceof DoubleValueDecoder doubles)) {
            throw new IllegalArgumentException("Double accumulator requires a double physical decoder");
        }
        if (exactSize < 0) {
            ensureCapacity(size + 1);
            values[size] = doubles.value(ordinal, dictionaryId);
        }
        else if (dictionaryId < 0 || directValues != null) {
            ensureDirectFlat();
            directValues.values()[size] = doubles.value(ordinal, dictionaryId);
        }
        else if (directDictionary == null) {
            beginDirectDictionary(doubles);
            directIds.values()[size] = dictionaryId;
            directDomainFrequencies.values()[dictionaryId]++;
        }
        else if (directDictionaryGeneration == doubles.dictionaryGeneration()) {
            directIds.values()[size] = dictionaryId;
            directDomainFrequencies.values()[dictionaryId]++;
        }
        else {
            ensureDirectFlat();
            directValues.values()[size] = doubles.value(ordinal, dictionaryId);
        }
        if (nullable) {
            nullValues()[size] = false;
        }
        size++;
    }

    @Override
    public void appendEvents(
            PhysicalValueDecoder decoder,
            int[] valueOrdinals,
            int[] dictionaryIds,
            int eventOffset,
            int eventCount)
    {
        if (!(decoder instanceof DoubleValueDecoder doubles)) {
            throw new IllegalArgumentException("Double accumulator requires a double physical decoder");
        }
        if (dictionaryIds != null && exactSize >= 0 && directValues == null &&
                (directDictionary == null || directDictionaryGeneration == doubles.dictionaryGeneration())) {
            if (directDictionary == null) {
                beginDirectDictionary(doubles);
            }
            int[] outputIds = directIds.values();
            int[] frequencies = directDomainFrequencies.values();
            boolean[] outputNulls = nullable ? directNulls.values() : null;
            int nullId = directDictionary.length() - 1;
            int output = size;
            int end = eventOffset + eventCount;
            for (int event = eventOffset; event < end; event++) {
                int ordinal = valueOrdinals[event];
                int id = ordinal < 0 ? nullId : dictionaryIds[ordinal];
                outputIds[output] = id;
                frequencies[id]++;
                if (outputNulls != null) {
                    outputNulls[output] = ordinal < 0;
                }
                output++;
            }
            size = output;
            return;
        }
        NestedValueAccumulator.super.appendEvents(decoder, valueOrdinals, dictionaryIds, eventOffset, eventCount);
    }

    @Override
    public void appendPlainRun(PhysicalValueDecoder decoder, int ordinal, int count)
    {
        if (!(decoder instanceof DoubleValueDecoder doubles)) {
            throw new IllegalArgumentException("Double accumulator requires a double physical decoder");
        }
        if (exactSize < 0) {
            ensureCapacity(size + count);
            doubles.copyPlain(ordinal, values, size, count);
        }
        else {
            ensureDirectFlat();
            doubles.copyPlain(ordinal, directValues.values(), size, count);
        }
        if (nullable) {
            Arrays.fill(nullValues(), size, size + count, false);
        }
        size += count;
    }

    @Override
    public void appendNull()
    {
        if (!nullable) {
            throw new IllegalArgumentException("Required nested value is missing");
        }
        if (exactSize < 0) {
            ensureCapacity(size + 1);
            values[size] = 0;
        }
        else if (directIds != null) {
            int nullId = directDictionary.length() - 1;
            directIds.values()[size] = nullId;
            directDomainFrequencies.values()[nullId]++;
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
        if (exactSize >= 0) {
            org.weakref.nitro.data.Vector result;
            org.weakref.nitro.data.Vector resultNulls = directNulls;
            if (directIds != null && directValues == null) {
                DictionaryVector dictionary = DictionaryVector.wrapOwnedIdsWithDomainFrequencies(
                        directIds,
                        size,
                        directDictionary.freezeContent(),
                        directDomainFrequencies);
                result = dictionary;
                if (nullable) {
                    directAllocator.release(directContext, directNulls);
                    resultNulls = dictionary.sharedMappingWithValues(directDictionaryNulls.freezeContent());
                }
            }
            else {
                ensureDirectFlat();
                result = directValues;
            }
            directValues = null;
            directNulls = null;
            directIds = null;
            directDomainFrequencies = null;
            directDictionary = null;
            directDictionaryNulls = null;
            directAllocator = null;
            directContext = null;
            directDictionaryGeneration = -1;
            exactSize = -1;
            return resultNulls == null ? Streams.ofValues(result) : Streams.of(result, resultNulls, null);
        }
        F64Vector result = F64Vector.allocate(allocator, context, size);
        System.arraycopy(values, 0, result.values(), 0, size);
        if (!nullable) {
            return Streams.ofValues(result);
        }
        BooleanVector resultNulls = allocator.allocate(context, BooleanVector.class, size, BooleanVector::new);
        System.arraycopy(nulls, 0, resultNulls.values(), 0, size);
        return Streams.ofValuesAndNulls(result, resultNulls);
    }

    private void ensureCapacity(int required)
    {
        if (values.length >= required) {
            return;
        }
        int capacity = Math.max(required, Math.max(16, values.length * 2));
        double[] replacement = arrayPool.borrowDoubles(capacity);
        System.arraycopy(values, 0, replacement, 0, size);
        arrayPool.release(values);
        values = replacement;
        if (nullable) {
            boolean[] replacementNulls = arrayPool.borrowBooleans(capacity);
            System.arraycopy(nulls, 0, replacementNulls, 0, size);
            arrayPool.release(nulls);
            nulls = replacementNulls;
        }
    }

    private boolean[] nullValues()
    {
        return directNulls == null ? nulls : directNulls.values();
    }

    private void beginDirectDictionary(DoubleValueDecoder decoder)
    {
        directIds = I32Vector.allocate(directAllocator, directContext, exactSize);
        int dictionarySize = decoder.dictionarySize();
        int outputDictionarySize = dictionarySize + (nullable ? 1 : 0);
        directDomainFrequencies = I32Vector.allocate(directAllocator, directContext, outputDictionarySize);
        Arrays.fill(directDomainFrequencies.values(), 0, outputDictionarySize, 0);
        if (size > 0) {
            int nullId = outputDictionarySize - 1;
            Arrays.fill(directIds.values(), 0, size, nullId);
            directDomainFrequencies.values()[nullId] = size;
        }
        directDictionary = F64Vector.allocate(directAllocator, directContext, outputDictionarySize);
        decoder.copyDictionary(directDictionary.values(), 0);
        if (nullable) {
            directDictionary.values()[outputDictionarySize - 1] = 0;
            directDictionaryNulls = directAllocator.allocate(
                    directContext,
                    BooleanVector.class,
                    outputDictionarySize,
                    BooleanVector::new);
            Arrays.fill(directDictionaryNulls.values(), 0, outputDictionarySize, false);
            directDictionaryNulls.values()[outputDictionarySize - 1] = true;
        }
        directDictionaryGeneration = decoder.dictionaryGeneration();
    }

    private void ensureDirectFlat()
    {
        if (directValues != null) {
            return;
        }
        directValues = F64Vector.allocate(directAllocator, directContext, exactSize);
        if (directIds == null) {
            return;
        }
        int[] ids = directIds.values();
        double[] dictionary = directDictionary.values();
        double[] output = directValues.values();
        for (int index = 0; index < size; index++) {
            output[index] = dictionary[ids[index]];
        }
        directAllocator.release(directContext, directIds);
        directAllocator.release(directContext, directDomainFrequencies);
        directAllocator.release(directContext, directDictionary);
        if (directDictionaryNulls != null) {
            directAllocator.release(directContext, directDictionaryNulls);
        }
        directIds = null;
        directDomainFrequencies = null;
        directDictionary = null;
        directDictionaryNulls = null;
        directDictionaryGeneration = -1;
    }

    private void releaseDirect()
    {
        if (directAllocator == null) {
            return;
        }
        if (directValues != null) {
            directAllocator.release(directContext, directValues);
        }
        if (directNulls != null) {
            directAllocator.release(directContext, directNulls);
        }
        if (directIds != null) {
            directAllocator.release(directContext, directIds);
        }
        if (directDomainFrequencies != null) {
            directAllocator.release(directContext, directDomainFrequencies);
        }
        if (directDictionary != null) {
            directAllocator.release(directContext, directDictionary);
        }
        if (directDictionaryNulls != null) {
            directAllocator.release(directContext, directDictionaryNulls);
        }
    }

    @Override
    public void close()
    {
        if (arrayPool != null) {
            releaseDirect();
            arrayPool.release(values);
            arrayPool.release(nulls);
            values = EMPTY_DOUBLES;
            nulls = EMPTY_BOOLEANS;
            arrayPool = null;
            size = 0;
            directValues = null;
            directNulls = null;
            directAllocator = null;
            directContext = null;
            directIds = null;
            directDomainFrequencies = null;
            directDictionary = null;
            directDictionaryNulls = null;
            directDictionaryGeneration = -1;
            exactSize = -1;
        }
    }
}
