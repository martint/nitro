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

import java.util.IdentityHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class ProjectedRows
{
    private final SelectedPositions positions;
    private Map<SelectedPositions, SelectedPositions> composedSelections;
    private Map<SelectedPositions, ProjectedRows> projectedRows;
    private SelectedPositions lastComposedSource;
    private SelectedPositions lastComposedSelection;
    private SelectedPositions lastProjectedSource;
    private ProjectedRows lastProjectedRows;
    private int[] materializedPositions;

    public static ProjectedRows rows(SelectedPositions positions)
    {
        return new ProjectedRows(positions);
    }

    public ProjectedRows(SelectedPositions positions)
    {
        this.positions = requireNonNull(positions, "positions is null");
        ProjectedRowsDebug.recordProjectedRowsCreated(positions.count());
    }

    public int count()
    {
        return positions.count();
    }

    public int position(int index)
    {
        return positions.position(index);
    }

    public SelectedPositions positions()
    {
        return positions;
    }

    public int[] materialize(int[] target)
    {
        if (target != null) {
            ProjectedRowsDebug.recordMaterialize(positions.count(), true, false);
            return positions.materialize(target);
        }
        if (materializedPositions == null) {
            ProjectedRowsDebug.recordMaterialize(positions.count(), false, false);
            materializedPositions = positions.materialize(null);
        }
        else {
            ProjectedRowsDebug.recordMaterialize(positions.count(), false, true);
        }
        return materializedPositions;
    }

    public SelectedPositions compose(SelectedPositions selectedRows)
    {
        requireNonNull(selectedRows, "selectedRows is null");
        if (selectedRows == positions) {
            ProjectedRowsDebug.recordCompose(selectedRows.count(), "identity");
            return positions;
        }
        if (selectedRows == lastComposedSource) {
            ProjectedRowsDebug.recordCompose(selectedRows.count(), "last");
            return lastComposedSelection;
        }
        if (composedSelections != null) {
            SelectedPositions cached = composedSelections.get(selectedRows);
            if (cached != null) {
                ProjectedRowsDebug.recordCompose(selectedRows.count(), "map");
                lastComposedSource = selectedRows;
                lastComposedSelection = cached;
                return cached;
            }
        }
        ProjectedRowsDebug.recordCompose(selectedRows.count(), "miss");
        SelectedPositions composed = composeSelectedPositions(selectedRows, positions);
        if (lastComposedSource == null) {
            lastComposedSource = selectedRows;
            lastComposedSelection = composed;
            return composed;
        }
        if (composedSelections == null) {
            composedSelections = new IdentityHashMap<>();
            composedSelections.put(lastComposedSource, lastComposedSelection);
        }
        composedSelections.put(selectedRows, composed);
        lastComposedSource = selectedRows;
        lastComposedSelection = composed;
        return composed;
    }

    public ProjectedRows project(SelectedPositions selectedRows)
    {
        requireNonNull(selectedRows, "selectedRows is null");
        if (selectedRows == positions) {
            ProjectedRowsDebug.recordProject(selectedRows.count(), "identity");
            return this;
        }
        if (selectedRows == lastProjectedSource) {
            ProjectedRowsDebug.recordProject(selectedRows.count(), "last");
            return lastProjectedRows;
        }
        if (projectedRows != null) {
            ProjectedRows cached = projectedRows.get(selectedRows);
            if (cached != null) {
                ProjectedRowsDebug.recordProject(selectedRows.count(), "map");
                lastProjectedSource = selectedRows;
                lastProjectedRows = cached;
                return cached;
            }
        }
        ProjectedRowsDebug.recordProject(selectedRows.count(), "miss");
        ProjectedRows projected = new ProjectedRows(compose(selectedRows));
        if (lastProjectedSource == null) {
            lastProjectedSource = selectedRows;
            lastProjectedRows = projected;
            return projected;
        }
        if (projectedRows == null) {
            projectedRows = new IdentityHashMap<>();
            projectedRows.put(lastProjectedSource, lastProjectedRows);
        }
        projectedRows.put(selectedRows, projected);
        lastProjectedSource = selectedRows;
        lastProjectedRows = projected;
        return projected;
    }

    private static SelectedPositions composeSelectedPositions(SelectedPositions sourcePositions, SelectedPositions projection)
    {
        int[] projectionArray = projection.backingArrayOrNull();
        if (projectionArray != null) {
            int[] sourceArray = sourcePositions.backingArrayOrNull();
            if (sourceArray != null) {
                int[] positions = new int[sourcePositions.count()];
                int sourceOffset = sourcePositions.backingArrayOffset();
                int projectionOffset = projection.backingArrayOffset();
                for (int index = 0; index < positions.length; index++) {
                    positions[index] = projectionArray[projectionOffset + sourceArray[sourceOffset + index]];
                }
                return SelectedPositions.positions(positions);
            }
            return SelectedPositions.map(projectionArray, sourcePositions);
        }

        int[] positions = new int[sourcePositions.count()];
        int[] sourceArray = sourcePositions.backingArrayOrNull();
        int sourceOffset = sourcePositions.backingArrayOffset();
        if (sourceArray != null) {
            for (int index = 0; index < positions.length; index++) {
                positions[index] = projection.position(sourceArray[sourceOffset + index]);
            }
            return SelectedPositions.positions(positions);
        }
        for (int index = 0; index < positions.length; index++) {
            positions[index] = projection.position(sourcePositions.position(index));
        }
        return SelectedPositions.positions(positions);
    }
}
