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
import org.weakref.nitro.data.RepeatedVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

/**
 * Recovers a bounded parent domain from independently encoded nested leaves.
 *
 * <p>The proof is deliberately representation-based: equal dictionary ids in one immutable child domain (or equal
 * RLE runs) prove equal physical values without invoking logical-type semantics. Unsupported or flat child streams
 * simply make the optimization ineligible.
 */
final class NestedDomainEncoder
{
    private NestedDomainEncoder() {}

    static Streams tryEncode(
            Allocator allocator,
            Allocator.Context context,
            RepeatedVector values,
            Vector nulls,
            int rowCount,
            int minimumRows)
    {
        return tryEncode(allocator, context, values, nulls, rowCount, minimumRows, 1, rowCount);
    }

    static Streams tryEncode(
            Allocator allocator,
            Allocator.Context context,
            RepeatedVector values,
            Vector nulls,
            int rowCount,
            int minimumRows,
            int maximumEntries,
            int minimumRowsPerEntry)
    {
        if (rowCount < minimumRows || rowCount != values.length() || maximumEntries == 0) {
            return null;
        }
        int firstStart = values.startOffset(0);
        int firstEnd = values.endOffset(0);
        int length = firstEnd - firstStart;
        int outputCount = values.repeatedOutputCount();
        Vector[] comparisonStreams = new Vector[outputCount * 3];
        int comparisonStreamCount = 0;
        for (int output = 0; output < outputCount; output++) {
            Streams streams = values.repeatedOutput(output);
            if (!streams.hasValues() || !encoded(streams.values())) {
                return null;
            }
            comparisonStreams[comparisonStreamCount++] = streams.values();
            Vector nullStream = streams.getOrNull(Stream.NULLS);
            Vector errorStream = streams.getOrNull(Stream.ERRORS);
            if (!encodedSideStream(nullStream) || !encodedSideStream(errorStream)) {
                return null;
            }
            nullStream = comparableSideStream(nullStream);
            errorStream = comparableSideStream(errorStream);
            if (nullStream != null) {
                comparisonStreams[comparisonStreamCount++] = nullStream;
            }
            if (errorStream != null) {
                comparisonStreams[comparisonStreamCount++] = errorStream;
            }
        }
        if (constant(nulls, rowCount) && constantRows(values, rowCount, firstStart, length, comparisonStreams, comparisonStreamCount)) {
            return encodeConstant(allocator, context, values, nulls, rowCount);
        }

        return tryEncodeLowCardinality(
                allocator,
                context,
                values,
                nulls,
                rowCount,
                comparisonStreams,
                comparisonStreamCount,
                maximumEntries,
                minimumRowsPerEntry);
    }

    private static Streams encodeConstant(
            Allocator allocator,
            Allocator.Context context,
            RepeatedVector values,
            Vector nulls,
            int rowCount)
    {
        int[] representative = allocator.primitiveArrays().borrowInts(1);
        representative[0] = 0;
        Vector domain;
        try {
            domain = values.copyPositionsInto(allocator, context, null, representative, 1, 0, 1).freezeContent();
        }
        finally {
            allocator.primitiveArrays().release(representative);
        }
        I32Vector ids = I32Vector.allocate(allocator, context, rowCount);
        Arrays.fill(ids.values(), 0, rowCount, 0);
        I32Vector frequencies = I32Vector.allocate(allocator, context, 1);
        frequencies.values()[0] = rowCount;
        DictionaryVector encoded = DictionaryVector.wrapOwnedIdsWithDomainFrequencies(ids, rowCount, domain, frequencies);
        if (nulls == null) {
            return Streams.ofValues(encoded);
        }
        BooleanVector domainNulls = allocator.allocate(context, BooleanVector.class, 1, BooleanVector::new);
        domainNulls.values()[0] = VectorAccess.booleanValues(nulls).value(0);
        return Streams.of(encoded, encoded.sharedMappingWithValues(domainNulls), null);
    }

    private static Streams tryEncodeLowCardinality(
            Allocator allocator,
            Allocator.Context context,
            RepeatedVector values,
            Vector nulls,
            int rowCount,
            Vector[] comparisonStreams,
            int comparisonStreamCount,
            int maximumEntries,
            int minimumRowsPerEntry)
    {
        int tableSize = 1;
        while (tableSize < maximumEntries * 2) {
            tableSize <<= 1;
        }
        int[] table = allocator.primitiveArrays().borrowInts(tableSize);
        int[] representatives = allocator.primitiveArrays().borrowInts(maximumEntries);
        int[] logicalIds = allocator.primitiveArrays().borrowInts(rowCount);
        int[] frequencies = allocator.primitiveArrays().borrowInts(maximumEntries);
        Arrays.fill(table, 0, tableSize, -1);
        Arrays.fill(frequencies, 0, maximumEntries, 0);
        VectorAccess.BooleanValues parentNulls = VectorAccess.booleanValues(nulls);
        try {
            int domainCount = 0;
            for (int row = 0; row < rowCount; row++) {
                int slot = mix(rowHash(values, row, parentNulls, comparisonStreams, comparisonStreamCount)) & (tableSize - 1);
                int domain;
                while ((domain = table[slot]) >= 0 &&
                        !sameRow(values, representatives[domain], row, parentNulls, comparisonStreams, comparisonStreamCount)) {
                    slot = (slot + 1) & (tableSize - 1);
                }
                if (domain < 0) {
                    if (domainCount == maximumEntries) {
                        return null;
                    }
                    domain = domainCount++;
                    table[slot] = domain;
                    representatives[domain] = row;
                }
                logicalIds[row] = domain;
                frequencies[domain]++;
            }
            if ((long) domainCount * minimumRowsPerEntry > rowCount) {
                return null;
            }
            return encode(allocator, context, values, nulls, rowCount, representatives, logicalIds, frequencies, domainCount);
        }
        finally {
            allocator.primitiveArrays().release(frequencies);
            allocator.primitiveArrays().release(logicalIds);
            allocator.primitiveArrays().release(representatives);
            allocator.primitiveArrays().release(table);
        }
    }

    private static Streams encode(
            Allocator allocator,
            Allocator.Context context,
            RepeatedVector values,
            Vector nulls,
            int rowCount,
            int[] representatives,
            int[] logicalIds,
            int[] frequencies,
            int domainCount)
    {
        Vector domain = values.copyPositionsInto(allocator, context, null, representatives, domainCount, 0, domainCount).freezeContent();
        I32Vector ids = I32Vector.allocate(allocator, context, rowCount);
        System.arraycopy(logicalIds, 0, ids.values(), 0, rowCount);
        I32Vector ownedFrequencies = I32Vector.allocate(allocator, context, domainCount);
        System.arraycopy(frequencies, 0, ownedFrequencies.values(), 0, domainCount);
        DictionaryVector encoded = DictionaryVector.wrapOwnedIdsWithDomainFrequencies(ids, rowCount, domain, ownedFrequencies);
        if (nulls == null) {
            return Streams.ofValues(encoded);
        }
        BooleanVector domainNulls = allocator.allocate(context, BooleanVector.class, domainCount, BooleanVector::new);
        VectorAccess.BooleanValues sourceNulls = VectorAccess.booleanValues(nulls);
        for (int domainPosition = 0; domainPosition < domainCount; domainPosition++) {
            domainNulls.values()[domainPosition] = sourceNulls.value(representatives[domainPosition]);
        }
        return Streams.of(encoded, encoded.sharedMappingWithValues(domainNulls), null);
    }

    private static int rowHash(
            RepeatedVector values,
            int row,
            VectorAccess.BooleanValues parentNulls,
            Vector[] comparisonStreams,
            int comparisonStreamCount)
    {
        if (parentNulls.value(row)) {
            return 1;
        }
        int start = values.startOffset(row);
        int end = values.endOffset(row);
        int hash = end - start;
        for (int position = start; position < end; position++) {
            for (int stream = 0; stream < comparisonStreamCount; stream++) {
                hash = 31 * hash + encodedPosition(comparisonStreams[stream], position);
            }
        }
        return hash;
    }

    private static boolean sameRow(
            RepeatedVector values,
            int leftRow,
            int rightRow,
            VectorAccess.BooleanValues parentNulls,
            Vector[] comparisonStreams,
            int comparisonStreamCount)
    {
        boolean leftNull = parentNulls.value(leftRow);
        if (leftNull != parentNulls.value(rightRow)) {
            return false;
        }
        if (leftNull) {
            return true;
        }
        int left = values.startOffset(leftRow);
        int right = values.startOffset(rightRow);
        int length = values.endOffset(leftRow) - left;
        if (values.endOffset(rightRow) - right != length) {
            return false;
        }
        for (int offset = 0; offset < length; offset++) {
            for (int stream = 0; stream < comparisonStreamCount; stream++) {
                if (encodedPosition(comparisonStreams[stream], left + offset) !=
                        encodedPosition(comparisonStreams[stream], right + offset)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static int encodedPosition(Vector vector, int position)
    {
        return switch (vector) {
            case DictionaryVector dictionary -> dictionary.ids()[position];
            case RleVector rle -> rle.runIndex(position);
            default -> throw new IllegalArgumentException("Nested domain stream is not encoded");
        };
    }

    private static int mix(int value)
    {
        value ^= value >>> 16;
        value *= 0x7feb352d;
        value ^= value >>> 15;
        value *= 0x846ca68b;
        return value ^ (value >>> 16);
    }

    private static boolean constantRows(
            RepeatedVector values,
            int rowCount,
            int firstStart,
            int length,
            Vector[] comparisonStreams,
            int comparisonStreamCount)
    {
        int[][] dictionaryIds = new int[comparisonStreamCount][];
        boolean dictionaryOnly = true;
        for (int stream = 0; stream < comparisonStreamCount; stream++) {
            if (comparisonStreams[stream] instanceof DictionaryVector dictionary) {
                dictionaryIds[stream] = dictionary.ids();
            }
            else {
                dictionaryOnly = false;
                break;
            }
        }
        return dictionaryOnly
                ? constantDictionaryRows(values, rowCount, firstStart, length, dictionaryIds)
                : constantEncodedRows(values, rowCount, firstStart, length, comparisonStreams, comparisonStreamCount);
    }

    private static boolean constantDictionaryRows(
            RepeatedVector values,
            int rowCount,
            int firstStart,
            int length,
            int[][] dictionaryIds)
    {
        int nextStart = firstStart + length;
        for (int row = 1; row < rowCount; row++) {
            int start = values.startOffset(row);
            if (start != nextStart || values.endOffset(row) - start != length) {
                return false;
            }
            nextStart = start + length;
        }
        if (length == 0) {
            return true;
        }

        int firstEnd = firstStart + length;
        int end = values.endOffset(rowCount - 1);
        for (int[] ids : dictionaryIds) {
            if (java.util.Arrays.mismatch(ids, firstStart, end - length, ids, firstEnd, end) >= 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean constantEncodedRows(
            RepeatedVector values,
            int rowCount,
            int firstStart,
            int length,
            Vector[] comparisonStreams,
            int comparisonStreamCount)
    {
        for (int row = 1; row < rowCount; row++) {
            int start = values.startOffset(row);
            if (values.endOffset(row) - start != length) {
                return false;
            }
            for (int offset = 0; offset < length; offset++) {
                int left = firstStart + offset;
                int right = start + offset;
                for (int stream = 0; stream < comparisonStreamCount; stream++) {
                    if (!sameEncodedPosition(comparisonStreams[stream], left, right)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static boolean encoded(Vector vector)
    {
        return vector instanceof DictionaryVector || vector instanceof RleVector;
    }

    private static Vector comparableSideStream(Vector vector)
    {
        if (vector == null || VectorAccess.isAllFalseNulls(vector)) {
            return null;
        }
        return vector;
    }

    private static boolean encodedSideStream(Vector vector)
    {
        return vector == null || VectorAccess.isAllFalseNulls(vector) || encoded(vector);
    }

    private static boolean sameEncodedPosition(Vector vector, int left, int right)
    {
        return switch (vector) {
            case DictionaryVector dictionary -> dictionary.ids()[left] == dictionary.ids()[right];
            case RleVector rle -> rle.runIndex(left) == rle.runIndex(right);
            default -> false;
        };
    }

    private static boolean constant(Vector nulls, int rowCount)
    {
        if (nulls == null || VectorAccess.isAllFalseNulls(nulls)) {
            return true;
        }
        VectorAccess.BooleanValues values = VectorAccess.booleanValues(nulls);
        boolean first = values.value(0);
        for (int position = 1; position < rowCount; position++) {
            if (values.value(position) != first) {
                return false;
            }
        }
        return true;
    }
}
