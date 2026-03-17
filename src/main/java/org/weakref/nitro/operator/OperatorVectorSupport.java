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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;

import java.util.Set;

final class OperatorVectorSupport
{
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
            case I64Vector values -> values.values()[position];
            case DictionaryVector values -> longValue(values.values(), values.ids()[position]);
            case RleVector values -> longValue(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected I64 vector but found " + vector.getClass().getSimpleName());
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

    public static byte[] binaryBytes(Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector values -> values.copyBytes(position);
            case DictionaryVector values -> binaryBytes(values.values(), values.ids()[position]);
            case RleVector values -> binaryBytes(values.values(), runIndex(values, position));
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
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
        int offset = 0;
        for (int index = 0; index < values.counts().length; index++) {
            offset += values.counts()[index];
            if (position < offset) {
                return index;
            }
        }
        throw new IndexOutOfBoundsException("Position " + position + " is out of bounds for RLE vector of length " + values.length());
    }
}
