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
    private final ProjectedRows projectedRows;
    private final Vector values;

    public static Vector wrap(SelectedPositions positions, Vector values)
    {
        requireNonNull(positions, "positions is null");
        return wrap(ProjectedRows.rows(positions), values);
    }

    public static Vector wrap(ProjectedRows projectedRows, Vector values)
    {
        requireNonNull(projectedRows, "projectedRows is null");
        requireNonNull(values, "values is null");

        if (values instanceof SelectionVector selection) {
            if (selection.projectedRows() == projectedRows || selection.projectedRows().positions() == projectedRows.positions()) {
                ProjectedRowsDebug.recordWrap(projectedRows.count(), "selection.identity");
                return selection;
            }
            ProjectedRowsDebug.recordWrapSelectionProjectPair(selection.projectedRows(), projectedRows.positions());
            ProjectedRowsDebug.recordWrapSelectionPositionsPair(selection.projectedRows().positions(), projectedRows.positions());
            ProjectedRowsDebug.recordWrapSelectionFingerprintPair(selection.projectedRows().positions(), projectedRows.positions());
            ProjectedRowsDebug.recordWrap(projectedRows.count(), "selection");
            return new SelectionVector(selection.projectedRows().project(projectedRows.positions()), selection.values());
        }
        if (values instanceof DictionaryVector dictionary) {
            ProjectedRowsDebug.recordWrap(projectedRows.count(), "dictionary");
            return new SelectionVector(ProjectedRows.rows(SelectedPositions.map(dictionary.ids(), projectedRows.positions())), dictionary.values());
        }
        if (values instanceof RleVector rle) {
            ProjectedRowsDebug.recordWrap(projectedRows.count(), "rle");
            return new SelectionVector(ProjectedRows.rows(SelectedPositions.map(projectedRows.positions(), rle::runIndex)), rle.values());
        }
        ProjectedRowsDebug.recordWrap(projectedRows.count(), "base");
        return new SelectionVector(projectedRows, values);
    }

    public SelectionVector(SelectedPositions positions, Vector values)
    {
        this(ProjectedRows.rows(positions), values);
    }

    public SelectionVector(ProjectedRows projectedRows, Vector values)
    {
        this.projectedRows = requireNonNull(projectedRows, "projectedRows is null");
        this.values = requireNonNull(values, "values is null");
    }

    public SelectedPositions positions()
    {
        return projectedRows.positions();
    }

    public ProjectedRows projectedRows()
    {
        return projectedRows;
    }

    public Vector values()
    {
        return values;
    }

    @Override
    public int length()
    {
        return projectedRows.count();
    }

    @Override
    public long retainedBytes()
    {
        return 0;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        return values.copy(allocator, allocationContext, projectedRows.materialize(null));
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        int[] selectedPositions = projectedRows.compose(SelectedPositions.positions(positions)).materialize(null);
        return values.copy(allocator, allocationContext, selectedPositions);
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        for (int position : mask) {
            existing = values.copySinglePositionInto(allocator, allocationContext, existing, projectedRows.position(position), position, length());
        }
        return existing;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        int[] selectedPositions = projectedRows.compose(SelectedPositions.positions(sourcePositions, 0, sourceCount)).materialize(null);
        return values.copyPositionsInto(allocator, allocationContext, existing, selectedPositions, selectedPositions.length, outputStart, size);
    }

    @Override
    public Vector copySelectedPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        int[] selectedPositions = projectedRows.compose(sourcePositions).materialize(null);
        return values.copyPositionsInto(allocator, allocationContext, existing, selectedPositions, selectedPositions.length, outputStart, size);
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        return values.copySinglePositionInto(allocator, allocationContext, existing, projectedRows.position(sourcePosition), outputPosition, size);
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
}
