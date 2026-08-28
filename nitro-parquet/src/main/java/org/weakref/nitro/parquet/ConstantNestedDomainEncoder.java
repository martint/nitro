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

/**
 * Recovers a constant parent domain from independently encoded nested leaves.
 *
 * <p>The proof is deliberately representation-based: equal dictionary ids in one immutable child domain (or equal
 * RLE runs) prove equal physical values without invoking logical-type semantics. Unsupported or flat child streams
 * simply make the optimization ineligible.
 */
final class ConstantNestedDomainEncoder
{
    private ConstantNestedDomainEncoder() {}

    static Streams tryEncode(
            Allocator allocator,
            Allocator.Context context,
            RepeatedVector values,
            Vector nulls,
            int rowCount,
            int minimumRows)
    {
        if (rowCount < minimumRows || rowCount != values.length() || !constant(nulls, rowCount)) {
            return null;
        }
        int firstStart = values.startOffset(0);
        int firstEnd = values.endOffset(0);
        int length = firstEnd - firstStart;
        for (int output = 0; output < values.repeatedOutputCount(); output++) {
            if (!encoded(values.repeatedOutput(output))) {
                return null;
            }
        }
        for (int row = 1; row < rowCount; row++) {
            int start = values.startOffset(row);
            if (values.endOffset(row) - start != length) {
                return null;
            }
            for (int offset = 0; offset < length; offset++) {
                for (int output = 0; output < values.repeatedOutputCount(); output++) {
                    if (!sameEncodedPosition(values.repeatedOutput(output), firstStart + offset, start + offset)) {
                        return null;
                    }
                }
            }
        }

        Vector domain = allocator.copyVector(context, values, new int[] {0}).freezeContent();
        I32Vector ids = I32Vector.allocate(allocator, context, rowCount);
        java.util.Arrays.fill(ids.values(), 0, rowCount, 0);
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

    private static boolean encoded(Streams streams)
    {
        if (!streams.hasValues() || !encoded(streams.values())) {
            return false;
        }
        return encodedSideStream(streams.getOrNull(Stream.NULLS)) &&
                encodedSideStream(streams.getOrNull(Stream.ERRORS));
    }

    private static boolean sameEncodedPosition(Streams streams, int left, int right)
    {
        return sameEncodedPosition(streams.values(), left, right) &&
                sameEncodedSidePosition(streams.getOrNull(Stream.NULLS), left, right) &&
                sameEncodedSidePosition(streams.getOrNull(Stream.ERRORS), left, right);
    }

    private static boolean encodedSideStream(Vector vector)
    {
        return vector == null || VectorAccess.isAllFalseNulls(vector) || encoded(vector);
    }

    private static boolean encoded(Vector vector)
    {
        return vector instanceof DictionaryVector || vector instanceof RleVector;
    }

    private static boolean sameEncodedSidePosition(Vector vector, int left, int right)
    {
        return vector == null || VectorAccess.isAllFalseNulls(vector) || sameEncodedPosition(vector, left, right);
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
