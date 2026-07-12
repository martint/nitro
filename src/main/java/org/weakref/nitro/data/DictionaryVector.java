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
    private static final boolean TRANSFERABLE_BUFFER_LEASES =
            Boolean.parseBoolean(System.getProperty("nitro.transferableBufferLeases", "true"));
    private final int[] ids;
    private final I32Vector ownedIds;
    private final int length;
    private final Vector values;
    private Allocator.BufferLeaseOwner transferredIdsOwner;

    public DictionaryVector(int[] ids, Vector values)
    {
        this(ids, null, ids.length, values, true, true);
    }

    /**
     * Builds a dictionary from ids the caller guarantees are in-bounds (e.g. produced by a Parquet decoder or a
     * scalar function), keeping a defensive copy of the ids but skipping the O(n) bounds validation that the
     * public constructor runs — the same hot-path tradeoff {@link #wrap} already makes.
     */
    public static DictionaryVector ofTrustedIds(int[] ids, Vector values)
    {
        return ofTrustedIds(ids, ids.length, values);
    }

    public static DictionaryVector ofTrustedIds(int[] ids, int length, Vector values)
    {
        return new DictionaryVector(ids, null, length, values, true, false);
    }

    /**
     * Builds a dictionary over the first {@code length} entries in {@code ids}, sharing the backing ids array. This
     * mirrors Velox's BufferPtr + logical size wrapper and is intended for short-lived operator output mappings whose
     * producer keeps the backing array stable for the vector lifetime.
     */
    public static DictionaryVector wrap(int[] ids, int length, Vector values)
    {
        return wrap(ids, length, values, false);
    }

    public static DictionaryVector wrap(int[] ids, Vector values)
    {
        return wrap(ids, ids.length, values, false);
    }

    /**
     * Builds a dictionary wrapper without composing through an already encoded value vector. This is useful when the
     * caller wants to avoid copying/re-writing an output row mapping and all downstream consumers can read nested
     * dictionary/RLE encodings recursively.
     */
    public static DictionaryVector wrapNested(int[] ids, int length, Vector values)
    {
        return new DictionaryVector(ids, null, length, values, false, false);
    }

    /**
     * Wraps dictionary ids whose backing storage is owned by an allocator context. The id vector is an ownership
     * child, so generic allocator transfer/release traversal follows the mapping buffer without treating borrowed
     * dictionary values as owned by the same producer.
     * <p>
     * Unlike {@link #wrap(int[], int, Vector)}, this method deliberately preserves a nested value encoding: composing
     * ids would require replacing the supplied owned buffer and would sever its lifecycle.
     */
    public static DictionaryVector wrapOwnedIds(I32Vector ids, int length, Vector values)
    {
        checkArgument(ids != null, "ids is null");
        return new DictionaryVector(ids.values(), ids, length, values, false, false);
    }

    private static DictionaryVector wrap(int[] ids, int length, Vector values, boolean copyIds)
    {
        if (values instanceof DictionaryVector dictionary) {
            int[] composedIds = Arrays.copyOf(ids, length);
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
            // composed ids are derived from already-validated id arrays, so bounds are guaranteed
            return new DictionaryVector(composedIds, null, composedIds.length, baseValues, false, false);
        }
        // callers of wrap are expected to supply bounds-valid ids; skip validation in the hot path
        return new DictionaryVector(ids, null, length, values, copyIds, false);
    }

    private DictionaryVector(int[] ids, I32Vector ownedIds, int length, Vector values, boolean copyIds, boolean validate)
    {
        checkArgument(length >= 0, "length is negative");
        checkArgument(length <= ids.length, "length exceeds ids capacity");
        this.ids = copyIds ? Arrays.copyOf(ids, length) : ids;
        this.ownedIds = ownedIds;
        this.length = length;
        this.values = values;
        if (validate) {
            validateIds(ids, length, values.length());
        }
    }

    public int[] ids()
    {
        return ids;
    }

    public Vector values()
    {
        return values;
    }

    /**
     * Returns the first non-dictionary value vector below this wrapper. Consumers that operate on a concrete leaf
     * representation can resolve the encoding once per batch instead of recursively dispatching once per row.
     */
    public Vector baseValues()
    {
        Vector base = values;
        while (base instanceof DictionaryVector dictionary) {
            base = dictionary.values;
        }
        return base;
    }

    /**
     * Maps a logical position through every dictionary layer to the corresponding position in {@link #baseValues()}.
     * No mapping buffer is allocated or composed, so this is safe for borrowed and ownership-carrying dictionaries.
     */
    public int basePosition(int position)
    {
        int basePosition = ids[position];
        Vector base = values;
        while (base instanceof DictionaryVector dictionary) {
            basePosition = dictionary.ids[basePosition];
            base = dictionary.values;
        }
        return basePosition;
    }

    public int dictionaryDepth()
    {
        int depth = 1;
        Vector base = values;
        while (base instanceof DictionaryVector dictionary) {
            depth++;
            base = dictionary.values;
        }
        return depth;
    }

    /** Resolves a position using a depth already hoisted by the caller. */
    public int basePosition(int position, int depth)
    {
        int mapped = ids[position];
        return switch (depth) {
            case 1 -> mapped;
            case 2 -> ((DictionaryVector) values).ids[mapped];
            case 3 -> {
                DictionaryVector level2 = (DictionaryVector) values;
                DictionaryVector level3 = (DictionaryVector) level2.values;
                yield level3.ids[level2.ids[mapped]];
            }
            case 4 -> {
                DictionaryVector level2 = (DictionaryVector) values;
                DictionaryVector level3 = (DictionaryVector) level2.values;
                DictionaryVector level4 = (DictionaryVector) level3.values;
                yield level4.ids[level3.ids[level2.ids[mapped]]];
            }
            case 5 -> {
                DictionaryVector level2 = (DictionaryVector) values;
                DictionaryVector level3 = (DictionaryVector) level2.values;
                DictionaryVector level4 = (DictionaryVector) level3.values;
                DictionaryVector level5 = (DictionaryVector) level4.values;
                yield level5.ids[level4.ids[level3.ids[level2.ids[mapped]]]];
            }
            default -> basePosition(position);
        };
    }

    @Override
    public int length()
    {
        return length;
    }

    @Override
    public long retainedBytes()
    {
        return ownedIds == null ? (long) ids.length * Integer.BYTES : 0;
    }

    @Override
    public boolean isVariableWidth()
    {
        return values.isVariableWidth();
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        return allocator.allocateDictionary(allocationContext, Arrays.copyOf(ids, length), values.copy(allocator, allocationContext));
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
        if (ownedIds != null) {
            consumer.accept(ownedIds);
        }
        consumer.accept(values);
    }

    @Override
    public void prepareBufferTransfer(Allocator allocator, Allocator.Context producerContext)
    {
        if (TRANSFERABLE_BUFFER_LEASES && ownedIds != null && transferredIdsOwner == null) {
            transferredIdsOwner = allocator.lease(producerContext, ownedIds);
        }
    }

    @Override
    public void releaseTransferredBuffers()
    {
        if (transferredIdsOwner != null) {
            transferredIdsOwner.releaseLeased(ownedIds);
            transferredIdsOwner = null;
        }
    }

    @Override
    public String toString()
    {
        return "Dictionary {ids: " + Arrays.toString(Arrays.copyOf(ids, length)) + ", values: " + values + "}";
    }

    private static void validateIds(int[] ids, int length, int valuesLength)
    {
        for (int index = 0; index < length; index++) {
            int id = ids[index];
            checkArgument(id >= 0 && id < valuesLength, "Dictionary id %s is out of bounds for values length %s", id, valuesLength);
        }
    }
}
