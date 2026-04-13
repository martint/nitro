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

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class SelectionVector
        implements Vector
{
    private final SelectedPositions positions;
    private final Vector values;

    public static Vector wrap(SelectedPositions positions, Vector values)
    {
        requireNonNull(positions, "positions is null");
        requireNonNull(values, "values is null");

        if (values instanceof SelectionVector selection) {
            return new SelectionVector(SelectedPositions.positions(composePositions(positions, selection.positions())), selection.values());
        }
        if (values instanceof DictionaryVector dictionary) {
            return new SelectionVector(SelectedPositions.positions(mapPositions(positions, dictionary.ids())), dictionary.values());
        }
        if (values instanceof RleVector rle) {
            return new SelectionVector(SelectedPositions.positions(mapPositions(positions, rle::runIndex)), rle.values());
        }
        return new SelectionVector(positions, values);
    }

    public SelectionVector(SelectedPositions positions, Vector values)
    {
        this.positions = requireNonNull(positions, "positions is null");
        this.values = requireNonNull(values, "values is null");
    }

    public SelectedPositions positions()
    {
        return positions;
    }

    public Vector values()
    {
        return values;
    }

    @Override
    public int length()
    {
        return positions.count();
    }

    @Override
    public long retainedBytes()
    {
        return 0;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        return values.copy(allocator, allocationContext, positions.materialize(null));
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        int[] selectedPositions = composePositions(SelectedPositions.positions(positions), this.positions);
        return values.copy(allocator, allocationContext, selectedPositions);
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        for (int position : mask) {
            existing = values.copySinglePositionInto(allocator, allocationContext, existing, positions.position(position), position, length());
        }
        return existing;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        int[] selectedPositions = composePositions(SelectedPositions.positions(sourcePositions, 0, sourceCount), positions);
        return values.copyPositionsInto(allocator, allocationContext, existing, selectedPositions, selectedPositions.length, outputStart, size);
    }

    @Override
    public Vector copySelectedPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        int[] selectedPositions = composePositions(sourcePositions, positions);
        return values.copyPositionsInto(allocator, allocationContext, existing, selectedPositions, selectedPositions.length, outputStart, size);
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        return values.copySinglePositionInto(allocator, allocationContext, existing, positions.position(sourcePosition), outputPosition, size);
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

    private static int[] composePositions(SelectedPositions positions, SelectedPositions mapping)
    {
        int[] composed = new int[positions.count()];
        int[] positionsArray = positions.backingArrayOrNull();
        int[] mappingArray = mapping.backingArrayOrNull();
        if (positionsArray != null && mappingArray != null) {
            int positionsOffset = positions.backingArrayOffset();
            int mappingOffset = mapping.backingArrayOffset();
            for (int index = 0; index < composed.length; index++) {
                composed[index] = mappingArray[mappingOffset + positionsArray[positionsOffset + index]];
            }
            return composed;
        }
        if (positionsArray != null) {
            int positionsOffset = positions.backingArrayOffset();
            for (int index = 0; index < composed.length; index++) {
                composed[index] = mapping.position(positionsArray[positionsOffset + index]);
            }
            return composed;
        }
        if (mappingArray != null) {
            int mappingOffset = mapping.backingArrayOffset();
            for (int index = 0; index < composed.length; index++) {
                composed[index] = mappingArray[mappingOffset + positions.position(index)];
            }
            return composed;
        }
        for (int index = 0; index < composed.length; index++) {
            composed[index] = mapping.position(positions.position(index));
        }
        return composed;
    }

    private static int[] mapPositions(SelectedPositions positions, int[] mapping)
    {
        int[] mapped = new int[positions.count()];
        int[] positionsArray = positions.backingArrayOrNull();
        if (positionsArray != null) {
            int positionsOffset = positions.backingArrayOffset();
            for (int index = 0; index < mapped.length; index++) {
                mapped[index] = mapping[positionsArray[positionsOffset + index]];
            }
            return mapped;
        }
        for (int index = 0; index < mapped.length; index++) {
            mapped[index] = mapping[positions.position(index)];
        }
        return mapped;
    }

    private static int[] mapPositions(SelectedPositions positions, java.util.function.IntUnaryOperator mapping)
    {
        int[] mapped = new int[positions.count()];
        int[] positionsArray = positions.backingArrayOrNull();
        if (positionsArray != null) {
            int positionsOffset = positions.backingArrayOffset();
            for (int index = 0; index < mapped.length; index++) {
                mapped[index] = mapping.applyAsInt(positionsArray[positionsOffset + index]);
            }
            return mapped;
        }
        for (int index = 0; index < mapped.length; index++) {
            mapped[index] = mapping.applyAsInt(positions.position(index));
        }
        return mapped;
    }
}
