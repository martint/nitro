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

    public static int binaryHash(byte[] bytes)
    {
        int result = 1;
        for (byte value : bytes) {
            result = 31 * result + value;
        }
        return result;
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

    static int binaryHash(byte[] bytes, int offset, int length)
    {
        long hash = HASH_SEED ^ length;
        int index = offset;
        int end = offset + length;

        while (index + Long.BYTES <= end) {
            hash ^= mix64((long) LONG_HANDLE.get(bytes, index));
            hash = Long.rotateLeft(hash, 27) * HASH_MULTIPLIER + 0x52DC_E729L;
            index += Long.BYTES;
        }

        long tail = 0;
        int shift = 0;
        while (index < end) {
            tail |= (bytes[index] & 0xFFL) << shift;
            shift += Byte.SIZE;
            index++;
        }
        hash ^= mix64(tail);
        hash = fmix64(hash);
        return (int) (hash ^ (hash >>> Integer.SIZE));
    }

    static boolean binaryEquals(byte[] left, int leftOffset, byte[] right, int rightOffset, int length)
    {
        if (length >= 16) {
            return Arrays.mismatch(left, leftOffset, leftOffset + length, right, rightOffset, rightOffset + length) == -1;
        }
        for (int index = 0; index < length; index++) {
            if (left[leftOffset + index] != right[rightOffset + index]) {
                return false;
            }
        }
        return true;
    }

    private static int binaryCompare(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength)
    {
        int minLength = Math.min(leftLength, rightLength);
        for (int index = 0; index < minLength; index++) {
            int comparison = Byte.toUnsignedInt(left[leftOffset + index]) - Byte.toUnsignedInt(right[rightOffset + index]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftLength, rightLength);
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
