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
package org.weakref.nitro.data;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public final class DictionaryVector
        implements Vector
{
    private final int[] ids;
    private final Vector values;

    public DictionaryVector(int[] ids, Vector values)
    {
        checkArgument(ids.length >= 0, "ids length is negative");
        this.ids = Arrays.copyOf(ids, ids.length);
        this.values = values;
        validateIds(ids, values.length());
    }

    public int[] ids()
    {
        return ids;
    }

    public Vector values()
    {
        return values;
    }

    @Override
    public int length()
    {
        return ids.length;
    }

    @Override
    public Vector copy(int size)
    {
        checkArgument(size >= 0, "size is negative");
        checkArgument(size <= ids.length, "size exceeds dictionary length");
        return new DictionaryVector(Arrays.copyOf(ids, size), values);
    }

    @Override
    public String toString()
    {
        return "Dictionary {ids: " + Arrays.toString(ids) + ", values: " + values + "}";
    }

    private static void validateIds(int[] ids, int valuesLength)
    {
        for (int id : ids) {
            checkArgument(id >= 0 && id < valuesLength, "Dictionary id %s is out of bounds for values length %s", id, valuesLength);
        }
    }
}
