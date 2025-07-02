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
package org.weakref.nitro.operator.evaluator.example;

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

public class Vectors
{
    public static BooleanVector booleanVector(Boolean[] values)
    {
        boolean[] result = new boolean[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = values[i] != null && values[i];
        }
        return new BooleanVector(result);
    }

    public static I64Vector i64Vector(Long[] values)
    {
        long[] result = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = values[i] == null ? 0 : values[i];
        }
        return new I64Vector(result);
    }

    public static BooleanVector nulls(Long[] values)
    {
        boolean[] result = new boolean[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = values[i] == null;
        }
        return new BooleanVector(result);
    }

    public static String render(Mask mask, Vector vector, BooleanVector nulls, BooleanVector errors)
    {
        StringBuilder result = new StringBuilder();
        result.append("[");
        for (int i = 0; i < vector.length(); i++) {
            if (!mask.contains(i)) {
                result.append("-");
            }
            else if (nulls.values()[i]) {
                result.append("<NULL>");
            }
            else if (errors.values()[i]) {
                result.append("<ERROR>");
            }
            else {
                result.append(vector.valueAt(i));
            }
            if (i < vector.length() - 1) {
                result.append(", ");
            }
        }
        result.append("]");
        return result.toString();
    }
}
