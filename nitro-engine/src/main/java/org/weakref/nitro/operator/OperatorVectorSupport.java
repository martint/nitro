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
package org.weakref.nitro.operator;

import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.ConcatenatedBooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Set;

final class OperatorVectorSupport
{
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final long HASH_SEED = 0x9E37_79B9_7F4A_7C15L;
    private static final long HASH_MULTIPLIER = 0x9FB2_1C65_1E98_DF25L;

    private OperatorVectorSupport() {}

    public static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }

    public static boolean isNull(Vector nulls, int position)
    {
        return nulls != null && booleanValue(nulls, position);
    }

    public static Vector flatten(Vector vector)
    {
        return switch (vector) {
            case DictionaryVector values -> flatten(values.values());
            case RleVector values -> flatten(values.values());
            default -> vector;
        };
    }

    public static long longValue(Vector vector, int position)
    {
        return switch (vector) {
            case I32Vector values -> values.values()[position];
            case I64Vector values -> values.values()[position];
            case DictionaryVector values -> longValue(values.values(), values.ids()[position]);
            case RleVector values -> longValue(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected integer vector but found " + vector.getClass().getSimpleName());
        };
    }

    public static boolean booleanValue(Vector vector, int position)
    {
        return switch (vector) {
            case BooleanVector values -> values.values()[position];
            case ConcatenatedBooleanVector values -> values.value(position);
            case DictionaryVector values -> booleanValue(values.values(), values.ids()[position]);
            case RleVector values -> booleanValue(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected boolean vector but found " + vector.getClass().getSimpleName());
        };
    }

    public static double doubleValue(Vector vector, int position)
    {
        return switch (vector) {
            case F64Vector values -> values.values()[position];
            case DictionaryVector values -> doubleValue(values.values(), values.ids()[position]);
            case RleVector values -> doubleValue(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected F64 vector but found " + vector.getClass().getSimpleName());
        };
    }

    public static int binaryLength(Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector values -> values.length(position);
            case DictionaryVector values -> binaryLength(values.values(), values.ids()[position]);
            case RleVector values -> binaryLength(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
        };
    }

    public static int binaryHash(Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector values -> binaryHash(values.data(), values.startOffset(position), values.length(position));
            case DictionaryVector values -> binaryHash(values.values(), values.ids()[position]);
            case RleVector values -> binaryHash(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
        };
    }

    /**
     * Hashes the value at {@code position}, consistent with {@link OperatorEqualitySemantics#equal}:
     * two positions that compare equal produce the same hash. Nulls hash to a fixed sentinel.
     */
    public static int hash(Vector values, Vector nulls, int position)
    {
        if (isNull(nulls, position)) {
            return 0;
        }
        Vector value = flatten(values);
        if (value instanceof I64Vector || value instanceof I32Vector) {
            return Long.hashCode(longValue(values, position));
        }
        if (value instanceof BooleanVector || value instanceof ConcatenatedBooleanVector) {
            return Boolean.hashCode(booleanValue(values, position));
        }
        if (value instanceof F64Vector) {
            return Double.hashCode(doubleValue(values, position));
        }
        if (value instanceof BinaryVector) {
            return binaryHash(values, position);
        }
        throw new IllegalArgumentException("Unsupported hash for " + value.getClass().getSimpleName());
    }

    public static int binaryHash(byte[] bytes)
    {
        return binaryHash(bytes, 0, bytes.length);
    }

    public static int binaryCompare(Vector left, int leftPosition, Vector right, int rightPosition)
    {
        return switch (left) {
            case BinaryVector leftValues -> switch (right) {
                case BinaryVector rightValues -> binaryCompare(
                        leftValues.data(),
                        leftValues.startOffset(leftPosition),
                        leftValues.length(leftPosition),
                        rightValues.data(),
                        rightValues.startOffset(rightPosition),
                        rightValues.length(rightPosition));
                default -> binaryCompare(right, rightPosition, leftValues.data(), leftValues.startOffset(leftPosition), leftValues.length(leftPosition)) * -1;
            };
            case DictionaryVector leftValues -> binaryCompare(leftValues.values(), leftValues.ids()[leftPosition], right, rightPosition);
            case RleVector leftValues -> binaryCompare(leftValues.values(), runIndex(leftValues, leftPosition), right, rightPosition);
            default -> throw new IllegalArgumentException("Expected binary vector but found " + left.getClass().getSimpleName());
        };
    }

    public static boolean binaryEquals(Vector left, int leftPosition, Vector right, int rightPosition)
    {
        if (binaryLength(left, leftPosition) != binaryLength(right, rightPosition)) {
            return false;
        }

        return switch (left) {
            case BinaryVector leftValues -> switch (right) {
                case BinaryVector rightValues -> binaryEquals(
                        leftValues.data(),
                        leftValues.startOffset(leftPosition),
                        rightValues.data(),
                        rightValues.startOffset(rightPosition),
                        leftValues.length(leftPosition));
                default -> binaryEquals(right, rightPosition, leftValues.data(), leftValues.startOffset(leftPosition), leftValues.length(leftPosition));
            };
            case DictionaryVector leftValues -> binaryEquals(leftValues.values(), leftValues.ids()[leftPosition], right, rightPosition);
            case RleVector leftValues -> binaryEquals(leftValues.values(), runIndex(leftValues, leftPosition), right, rightPosition);
            default -> throw new IllegalArgumentException("Expected binary vector but found " + left.getClass().getSimpleName());
        };
    }

    public static boolean binaryEquals(Vector left, int leftPosition, byte[] right)
    {
        return binaryEquals(left, leftPosition, right, 0, right.length);
    }

    public static int binaryCompare(Vector left, int leftPosition, byte[] right, int rightOffset, int rightLength)
    {
        return switch (left) {
            case BinaryVector values -> binaryCompare(values.data(), values.startOffset(leftPosition), values.length(leftPosition), right, rightOffset, rightLength);
            case DictionaryVector values -> binaryCompare(values.values(), values.ids()[leftPosition], right, rightOffset, rightLength);
            case RleVector values -> binaryCompare(values.values(), runIndex(values, leftPosition), right, rightOffset, rightLength);
            default -> throw new IllegalArgumentException("Expected binary vector but found " + left.getClass().getSimpleName());
        };
    }

    public static boolean binaryEquals(Vector left, int leftPosition, byte[] right, int rightOffset, int rightLength)
    {
        if (binaryLength(left, leftPosition) != rightLength) {
            return false;
        }

        return switch (left) {
            case BinaryVector values -> binaryEquals(values.data(), values.startOffset(leftPosition), right, rightOffset, rightLength);
            case DictionaryVector values -> binaryEquals(values.values(), values.ids()[leftPosition], right, rightOffset, rightLength);
            case RleVector values -> binaryEquals(values.values(), runIndex(values, leftPosition), right, rightOffset, rightLength);
            default -> throw new IllegalArgumentException("Expected binary vector but found " + left.getClass().getSimpleName());
        };
    }

    public static Set<BinaryVector.Trait> binaryTraits(Vector vector)
    {
        return switch (flatten(vector)) {
            case BinaryVector values -> values.traits();
            default -> Set.of();
        };
    }

    public static int runIndex(RleVector values, int position)
    {
        return values.runIndex(position);
    }

    public static int binaryHash(byte[] bytes, int offset, int length)
    {
        long hash = HASH_SEED ^ length;
        int index = offset;
        int end = offset + length;

        while (index + Long.BYTES <= end) {
            hash ^= mix64((long) LONG_HANDLE.get(bytes, index));
            hash = Long.rotateLeft(hash, 27) * HASH_MULTIPLIER + 0x52DC_E729L;
            index += Long.BYTES;
        }

        int remaining = end - index;
        long tail;
        if (remaining == 0) {
            tail = 0;
        }
        else if (index + Long.BYTES <= bytes.length) {
            // One masked word for the sub-word tail when 8 bytes are readable; the mask keeps the leading
            // `remaining` bytes (LONG_HANDLE is little-endian) so this is bit-identical to the byte loop and
            // the hash value is unchanged. Falls back to the byte loop only at the very end of the array.
            tail = (long) LONG_HANDLE.get(bytes, index) & ((1L << (remaining << 3)) - 1);
        }
        else {
            tail = 0;
            int shift = 0;
            while (index < end) {
                tail |= (bytes[index] & 0xFFL) << shift;
                shift += Byte.SIZE;
                index++;
            }
        }
        hash ^= mix64(tail);
        hash = fmix64(hash);
        return (int) (hash ^ (hash >>> Integer.SIZE));
    }

    static boolean binaryEquals(byte[] left, int leftOffset, byte[] right, int rightOffset, int length)
    {
        // Long strings: the vectorized intrinsic wins. Short strings -- the common grouping/join key case --
        // are compared a machine word at a time; a byte-at-a-time loop over 5-8 byte keys dominated the
        // string-grouping profile.
        if (length >= 16) {
            return Arrays.mismatch(left, leftOffset, leftOffset + length, right, rightOffset, rightOffset + length) == -1;
        }
        int index = 0;
        while (index + Long.BYTES <= length) {
            if ((long) LONG_HANDLE.get(left, leftOffset + index) != (long) LONG_HANDLE.get(right, rightOffset + index)) {
                return false;
            }
            index += Long.BYTES;
        }
        int remaining = length - index;
        if (remaining == 0) {
            return true;
        }
        // A single masked word settles the sub-word tail when 8 bytes are readable on both sides (true unless
        // the value sits at the very end of its backing array); otherwise fall back to the byte compare. The
        // mask keeps the leading `remaining` bytes (LONG_HANDLE is little-endian), so neighbouring bytes in
        // the over-read never affect the result.
        if (leftOffset + index + Long.BYTES <= left.length && rightOffset + index + Long.BYTES <= right.length) {
            long mask = (1L << (remaining << 3)) - 1;
            long leftWord = (long) LONG_HANDLE.get(left, leftOffset + index) & mask;
            long rightWord = (long) LONG_HANDLE.get(right, rightOffset + index) & mask;
            return leftWord == rightWord;
        }
        for (; index < length; index++) {
            if (left[leftOffset + index] != right[rightOffset + index]) {
                return false;
            }
        }
        return true;
    }

    private static int binaryCompare(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength)
    {
        return Arrays.compareUnsigned(
                left,
                leftOffset,
                leftOffset + leftLength,
                right,
                rightOffset,
                rightOffset + rightLength);
    }

    private static long mix64(long value)
    {
        long mixed = value * HASH_MULTIPLIER;
        mixed ^= mixed >>> 33;
        mixed *= 0xC2B2_AE3D_27D4_EB4FL;
        return mixed ^ (mixed >>> 29);
    }

    private static long fmix64(long value)
    {
        long mixed = value ^ (value >>> 33);
        mixed *= 0xFF51_AFD7_ED55_8CCDL;
        mixed ^= mixed >>> 33;
        mixed *= 0xC4CE_B9FE_1A85_EC53L;
        return mixed ^ (mixed >>> 33);
    }
}
