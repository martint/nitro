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

import org.weakref.nitro.core.type.FixedWidthKeyLayout;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RegionVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

/** Reusable, allocation-stable physical bindings consumed by generated fixed-width key kernels. */
final class FixedWidthKeyBatchBindings
{
    private record BoundLane(Object values, int[] mapping, int mappingOffset, int baseOffset, int[] ownedMapping) {}

    private final ResolvedFixedWidthKeyLayout layout;
    private final PrimitiveArrayPool arrayPool;
    private final Object[] keyArrays;
    private final int[][] keyMappings;
    private final int[] keyMappingOffsets;
    private final int[] keyBaseOffsets;
    private final boolean[][] nullArrays;
    private final int[][] nullMappings;
    private final int[] nullMappingOffsets;
    private final int[] nullBaseOffsets;
    private final int[][] ownedKeyMappings;
    private final int[][] ownedNullMappings;

    FixedWidthKeyBatchBindings(ResolvedFixedWidthKeyLayout layout, PrimitiveArrayPool arrayPool)
    {
        this.layout = layout;
        this.arrayPool = arrayPool;
        int lanes = layout.lanes().length;
        keyArrays = new Object[lanes];
        keyMappings = new int[lanes][];
        keyMappingOffsets = new int[lanes];
        keyBaseOffsets = new int[lanes];
        nullArrays = new boolean[lanes][];
        nullMappings = new int[lanes][];
        nullMappingOffsets = new int[lanes];
        nullBaseOffsets = new int[lanes];
        ownedKeyMappings = new int[lanes][];
        ownedNullMappings = new int[lanes][];
    }

    void bind(Vector[] values, Vector[] nulls)
    {
        release();
        Arrays.fill(nullArrays, null);
        Arrays.fill(keyMappings, null);
        Arrays.fill(nullMappings, null);
        Arrays.fill(keyMappingOffsets, 0);
        Arrays.fill(keyBaseOffsets, 0);
        Arrays.fill(nullMappingOffsets, 0);
        Arrays.fill(nullBaseOffsets, 0);
        try {
            for (int lane = 0; lane < keyArrays.length; lane++) {
                BoundLane key = bindLane(layout.laneVector(values, lane), layout.lanes()[lane].carrier());
                keyArrays[lane] = key.values();
                keyMappings[lane] = key.mapping();
                keyMappingOffsets[lane] = key.mappingOffset();
                keyBaseOffsets[lane] = key.baseOffset();
                ownedKeyMappings[lane] = key.ownedMapping();

                Vector nullVector = nulls.length == 0 ? null : nulls[layout.lanes()[lane].logicalKey()];
                if (!VectorAccess.isAllFalseNulls(nullVector)) {
                    BoundLane boundNulls = bindLane(nullVector, FixedWidthKeyLayout.Carrier.BOOLEAN);
                    nullArrays[lane] = (boolean[]) boundNulls.values();
                    nullMappings[lane] = boundNulls.mapping();
                    nullMappingOffsets[lane] = boundNulls.mappingOffset();
                    nullBaseOffsets[lane] = boundNulls.baseOffset();
                    ownedNullMappings[lane] = boundNulls.ownedMapping();
                }
            }
        }
        catch (RuntimeException | Error e) {
            release();
            throw e;
        }
    }

    Object[] keyArrays()
    {
        return keyArrays;
    }

    int[][] keyMappings()
    {
        return keyMappings;
    }

    int[] keyMappingOffsets()
    {
        return keyMappingOffsets;
    }

    int[] keyBaseOffsets()
    {
        return keyBaseOffsets;
    }

    boolean[][] nullArrays()
    {
        return nullArrays;
    }

    int[][] nullMappings()
    {
        return nullMappings;
    }

    int[] nullMappingOffsets()
    {
        return nullMappingOffsets;
    }

    int[] nullBaseOffsets()
    {
        return nullBaseOffsets;
    }

    void release()
    {
        for (int lane = 0; lane < ownedKeyMappings.length; lane++) {
            arrayPool.release(ownedKeyMappings[lane]);
            ownedKeyMappings[lane] = null;
            arrayPool.release(ownedNullMappings[lane]);
            ownedNullMappings[lane] = null;
        }
        Arrays.fill(keyArrays, null);
        Arrays.fill(keyMappings, null);
        Arrays.fill(nullArrays, null);
        Arrays.fill(nullMappings, null);
    }

    private BoundLane bindLane(Vector vector, FixedWidthKeyLayout.Carrier carrier)
    {
        return switch (vector) {
            case I32Vector integers when carrier == FixedWidthKeyLayout.Carrier.I32 ->
                    new BoundLane(integers.values(), null, 0, 0, null);
            case I64Vector longs when carrier == FixedWidthKeyLayout.Carrier.I64 ->
                    new BoundLane(longs.values(), null, 0, 0, null);
            case F64Vector doubles when carrier == FixedWidthKeyLayout.Carrier.F64 ->
                    new BoundLane(doubles.values(), null, 0, 0, null);
            case BooleanVector booleans when carrier == FixedWidthKeyLayout.Carrier.BOOLEAN ->
                    new BoundLane(booleans.values(), null, 0, 0, null);
            case RegionVector region -> regionBinding(region, carrier);
            case DictionaryVector dictionary -> dictionaryBinding(dictionary, carrier);
            case RleVector rle -> rleBinding(rle, carrier);
            default -> throw new IllegalArgumentException("No generated fixed-width loader for " + vector.getClass().getSimpleName() + " as " + carrier);
        };
    }

    private BoundLane regionBinding(RegionVector region, FixedWidthKeyLayout.Carrier carrier)
    {
        BoundLane child = bindLane(region.values(), carrier);
        if (child.mapping() == null) {
            return new BoundLane(child.values(), null, 0, child.baseOffset() + region.offset(), child.ownedMapping());
        }
        return new BoundLane(child.values(), child.mapping(), child.mappingOffset() + region.offset(), child.baseOffset(), child.ownedMapping());
    }

    private BoundLane dictionaryBinding(DictionaryVector dictionary, FixedWidthKeyLayout.Carrier carrier)
    {
        BoundLane child = bindLane(dictionary.values(), carrier);
        if (child.mapping() == null) {
            return new BoundLane(child.values(), dictionary.ids(), 0, child.baseOffset(), child.ownedMapping());
        }
        int[] mapping = arrayPool.borrowInts(dictionary.length());
        int[] ids = dictionary.ids();
        for (int position = 0; position < dictionary.length(); position++) {
            mapping[position] = physicalPosition(child, ids[position]);
        }
        arrayPool.release(child.ownedMapping());
        return new BoundLane(child.values(), mapping, 0, 0, mapping);
    }

    private BoundLane rleBinding(RleVector rle, FixedWidthKeyLayout.Carrier carrier)
    {
        BoundLane child = bindLane(rle.values(), carrier);
        int[] mapping = arrayPool.borrowInts(rle.length());
        int output = 0;
        for (int run = 0; run < rle.counts().length; run++) {
            int physical = physicalPosition(child, run);
            Arrays.fill(mapping, output, output + rle.counts()[run], physical);
            output += rle.counts()[run];
        }
        arrayPool.release(child.ownedMapping());
        return new BoundLane(child.values(), mapping, 0, 0, mapping);
    }

    private static int physicalPosition(BoundLane binding, int logicalPosition)
    {
        int position = binding.mapping() == null
                ? logicalPosition
                : binding.mapping()[logicalPosition + binding.mappingOffset()];
        return position + binding.baseOffset();
    }
}
