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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A borrowed contiguous logical region of another vector.
 *
 * <p>The region owns no storage. Its producer must keep the base vector stable for the region's lifetime. This
 * representation is useful at explicit engine boundaries where the foreign column format already exposes a
 * contiguous array region and copying it would add no ownership or layout benefit.
 */
public final class RegionVector
        implements Vector
{
    private final Vector values;
    private final int offset;
    private final int length;

    public RegionVector(Vector values, int offset, int length)
    {
        this.values = requireNonNull(values, "values is null");
        checkArgument(offset >= 0, "offset is negative");
        checkArgument(length >= 0, "length is negative");
        checkArgument((long) offset + length <= values.length(), "region exceeds values");
        this.offset = offset;
        this.length = length;
    }

    public Vector values()
    {
        return values;
    }

    public int offset()
    {
        return offset;
    }

    @Override
    public int length()
    {
        return length;
    }

    @Override
    public long retainedBytes()
    {
        return 0;
    }

    @Override
    public boolean isVariableWidth()
    {
        return values.isVariableWidth();
    }

    @Override
    public boolean requiresMonotonicOutputWrites()
    {
        return values.requiresMonotonicOutputWrites();
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        return values.copySelectedPositionsInto(
                allocator,
                allocationContext,
                null,
                SelectedPositions.range(offset, length),
                0,
                length);
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        return values.copySelectedPositionsInto(
                allocator,
                allocationContext,
                null,
                SelectedPositions.map(SelectedPositions.positions(positions), position -> offset + position),
                0,
                positions.length);
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        Vector result = existing;
        for (int position : mask) {
            result = values.copySinglePositionInto(allocator, allocationContext, result, offset + position, position, length);
        }
        return result;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        return values.copySelectedPositionsInto(
                allocator,
                allocationContext,
                existing,
                SelectedPositions.map(
                        SelectedPositions.positions(sourcePositions, 0, sourceCount),
                        position -> offset + position),
                outputStart,
                size);
    }

    @Override
    public Vector copySelectedPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        return values.copySelectedPositionsInto(
                allocator,
                allocationContext,
                existing,
                SelectedPositions.map(sourcePositions, position -> offset + position),
                outputStart,
                size);
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        return values.copySinglePositionInto(allocator, allocationContext, existing, offset + sourcePosition, outputPosition, size);
    }

    @Override
    public Vector copySinglePositionRangeInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputStart, int outputEnd, int size)
    {
        return values.copySinglePositionRangeInto(allocator, allocationContext, existing, offset + sourcePosition, outputStart, outputEnd, size);
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
}
