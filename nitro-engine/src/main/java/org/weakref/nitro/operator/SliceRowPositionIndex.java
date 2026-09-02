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

import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;

/// Zero-allocation logical slice of another row index.
final class SliceRowPositionIndex
        implements RowPositionIndex
{
    private final RowPositionIndex rows;
    private final int start;
    private final int end;

    SliceRowPositionIndex(RowPositionIndex rows, int start, int end)
    {
        this.rows = requireNonNull(rows, "rows is null");
        if (start < 0 || end < start || end > rows.size()) {
            throw new IndexOutOfBoundsException("slice is outside row index");
        }
        this.start = start;
        this.end = end;
    }

    @Override
    public int size()
    {
        return end - start;
    }

    @Override
    public Streams column(int column, int position)
    {
        return rows.column(column, absolute(position));
    }

    @Override
    public int sourcePosition(int position)
    {
        return rows.sourcePosition(absolute(position));
    }

    @Override
    public int sourceSize(int position)
    {
        return rows.sourceSize(absolute(position));
    }

    @Override
    public boolean sharesSource(int leftPosition, int rightPosition)
    {
        return rows.sharesSource(absolute(leftPosition), absolute(rightPosition));
    }

    @Override
    public int compareNonNull(int leftColumn, int leftPosition, int rightColumn, int rightPosition)
    {
        return rows.compareNonNull(leftColumn, absolute(leftPosition), rightColumn, absolute(rightPosition));
    }

    private int absolute(int position)
    {
        if (position < 0 || position >= size()) {
            throw new IndexOutOfBoundsException("position is outside row-index slice");
        }
        return start + position;
    }
}
