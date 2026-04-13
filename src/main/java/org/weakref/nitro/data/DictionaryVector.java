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
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;

public final class DictionaryVector
        implements Vector
{
    private final int[] ids;
    private final Vector values;

    public DictionaryVector(int[] ids, Vector values)
    {
        this(ids, values, true);
    }

    public static DictionaryVector wrap(int[] ids, Vector values)
    {
        if (values instanceof DictionaryVector dictionary) {
            int[] composedIds = Arrays.copyOf(ids, ids.length);
            Vector baseValues = dictionary.values();
            int[] baseIds = dictionary.ids();
            for (int index = 0; index < composedIds.length; index++) {
                composedIds[index] = baseIds[composedIds[index]];
            }
            while (baseValues instanceof DictionaryVector nestedDictionary) {
                baseIds = nestedDictionary.ids();
                for (int index = 0; index < composedIds.length; index++) {
                    composedIds[index] = baseIds[composedIds[index]];
                }
                baseValues = nestedDictionary.values();
            }
            return new DictionaryVector(composedIds, baseValues, false);
        }
        return new DictionaryVector(ids, values, false);
    }

    private DictionaryVector(int[] ids, Vector values, boolean copyIds)
    {
        checkArgument(ids.length >= 0, "ids length is negative");
        this.ids = copyIds ? Arrays.copyOf(ids, ids.length) : ids;
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
    public long retainedBytes()
    {
        return (long) ids.length * Integer.BYTES;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        return allocator.allocateDictionary(allocationContext, ids, values.copy(allocator, allocationContext));
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        int[] dictionaryPositions = new int[positions.length];
        for (int index = 0; index < positions.length; index++) {
            dictionaryPositions[index] = ids[positions[index]];
        }
        return values.copy(allocator, allocationContext, dictionaryPositions);
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        for (int position : mask) {
            existing = values.copySinglePositionInto(allocator, allocationContext, existing, ids[position], position, length());
        }
        return existing;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        int[] dictionaryPositions = new int[sourceCount];
        for (int index = 0; index < sourceCount; index++) {
            dictionaryPositions[index] = ids[sourcePositions[index]];
        }
        return values.copyPositionsInto(allocator, allocationContext, existing, dictionaryPositions, sourceCount, outputStart, size);
    }

    @Override
    public Vector copySelectedPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        return values.copySelectedPositionsInto(allocator, allocationContext, existing, SelectedPositions.map(ids, sourcePositions), outputStart, size);
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        return values.copySinglePositionInto(allocator, allocationContext, existing, ids[sourcePosition], outputPosition, size);
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        return values.emptyLike(allocator, allocationContext);
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        return values.materializeRows(allocator, allocationContext, rows);
    }

    @Override
    public void forEachChildVector(Consumer<Vector> consumer)
    {
        consumer.accept(values);
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
