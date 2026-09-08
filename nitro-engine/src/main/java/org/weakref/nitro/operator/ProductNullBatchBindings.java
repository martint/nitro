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

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RegionVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

/** Batch bindings for every nullable ancestor in a generated product-key descriptor. */
final class ProductNullBatchBindings
{
    private record BoundBoolean(boolean[] values, int[] mapping, int mappingOffset, int baseOffset, int[] ownedMapping) {}

    private final ResolvedPersistentKeyLayout layout;
    private final PrimitiveArrayPool arrayPool;
    private final int[] fieldSourceOffsets;
    private final boolean[][] arrays;
    private final int[][] mappings;
    private final int[] mappingOffsets;
    private final int[] baseOffsets;
    private final int[][] ownedMappings;

    ProductNullBatchBindings(ResolvedPersistentKeyLayout layout, PrimitiveArrayPool arrayPool)
    {
        this.layout = layout;
        this.arrayPool = arrayPool;
        fieldSourceOffsets = new int[layout.fields().length + 1];
        for (int field = 0; field < layout.fields().length; field++) {
            fieldSourceOffsets[field + 1] = fieldSourceOffsets[field] + layout.fields()[field].nullPaths().size();
        }
        int sources = fieldSourceOffsets[fieldSourceOffsets.length - 1];
        arrays = new boolean[sources][];
        mappings = new int[sources][];
        mappingOffsets = new int[sources];
        baseOffsets = new int[sources];
        ownedMappings = new int[sources][];
    }

    void bind(Vector[] values, Vector[] nulls)
    {
        release();
        Vector[][] sources = layout.fieldNullSourceVectors(values, nulls);
        try {
            for (int field = 0; field < sources.length; field++) {
                for (int source = 0; source < sources[field].length; source++) {
                    int index = fieldSourceOffsets[field] + source;
                    Vector vector = sources[field][source];
                    if (VectorAccess.isAllFalseNulls(vector)) {
                        continue;
                    }
                    BoundBoolean binding = bindBoolean(vector);
                    arrays[index] = binding.values();
                    mappings[index] = binding.mapping();
                    mappingOffsets[index] = binding.mappingOffset();
                    baseOffsets[index] = binding.baseOffset();
                    ownedMappings[index] = binding.ownedMapping();
                }
            }
        }
        catch (RuntimeException | Error e) {
            release();
            throw e;
        }
    }

    boolean[][] arrays()
    {
        return arrays;
    }

    int[][] mappings()
    {
        return mappings;
    }

    int[] mappingOffsets()
    {
        return mappingOffsets;
    }

    int[] baseOffsets()
    {
        return baseOffsets;
    }

    void release()
    {
        for (int source = 0; source < ownedMappings.length; source++) {
            arrayPool.release(ownedMappings[source]);
            ownedMappings[source] = null;
        }
        Arrays.fill(arrays, null);
        Arrays.fill(mappings, null);
        Arrays.fill(mappingOffsets, 0);
        Arrays.fill(baseOffsets, 0);
    }

    private BoundBoolean bindBoolean(Vector vector)
    {
        return switch (vector) {
            case BooleanVector booleans -> new BoundBoolean(booleans.values(), null, 0, 0, null);
            case RegionVector region -> regionBinding(region);
            case DictionaryVector dictionary -> dictionaryBinding(dictionary);
            case RleVector rle -> rleBinding(rle);
            default -> throw new IllegalArgumentException(
                    "No generated product-null loader for " + vector.getClass().getSimpleName());
        };
    }

    private BoundBoolean regionBinding(RegionVector region)
    {
        BoundBoolean child = bindBoolean(region.values());
        if (child.mapping() == null) {
            return new BoundBoolean(child.values(), null, 0, child.baseOffset() + region.offset(), child.ownedMapping());
        }
        return new BoundBoolean(
                child.values(), child.mapping(), child.mappingOffset() + region.offset(), child.baseOffset(), child.ownedMapping());
    }

    private BoundBoolean dictionaryBinding(DictionaryVector dictionary)
    {
        BoundBoolean child = bindBoolean(dictionary.values());
        if (child.mapping() == null) {
            return new BoundBoolean(child.values(), dictionary.ids(), 0, child.baseOffset(), child.ownedMapping());
        }
        int[] mapping = arrayPool.borrowInts(dictionary.length());
        int[] ids = dictionary.ids();
        for (int position = 0; position < dictionary.length(); position++) {
            mapping[position] = physicalPosition(child, ids[position]);
        }
        arrayPool.release(child.ownedMapping());
        return new BoundBoolean(child.values(), mapping, 0, 0, mapping);
    }

    private BoundBoolean rleBinding(RleVector rle)
    {
        BoundBoolean child = bindBoolean(rle.values());
        int[] mapping = arrayPool.borrowInts(rle.length());
        int output = 0;
        for (int run = 0; run < rle.counts().length; run++) {
            int physical = physicalPosition(child, run);
            Arrays.fill(mapping, output, output + rle.counts()[run], physical);
            output += rle.counts()[run];
        }
        arrayPool.release(child.ownedMapping());
        return new BoundBoolean(child.values(), mapping, 0, 0, mapping);
    }

    private static int physicalPosition(BoundBoolean binding, int logicalPosition)
    {
        int position = binding.mapping() == null
                ? logicalPosition
                : binding.mapping()[logicalPosition + binding.mappingOffset()];
        return position + binding.baseOffset();
    }
}
