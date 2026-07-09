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

import java.util.function.IntUnaryOperator;

import static java.util.Objects.requireNonNull;

public sealed interface SelectedPositions
        permits SelectedPositions.ArraySelection, SelectedPositions.MappedSelection, SelectedPositions.RangeSelection
{
    int count();

    int position(int index);

    default int[] backingArrayOrNull()
    {
        return null;
    }

    default int backingArrayOffset()
    {
        return 0;
    }

    default int[] materialize(int[] target)
    {
        int[] positions = target != null && target.length >= count() ? target : new int[count()];
        for (int index = 0; index < count(); index++) {
            positions[index] = position(index);
        }
        return positions;
    }

    static SelectedPositions positions(int[] positions)
    {
        return positions(positions, 0, positions.length);
    }

    static SelectedPositions positions(int[] positions, int start, int count)
    {
        return new ArraySelection(positions, start, count);
    }

    static SelectedPositions single(int position)
    {
        return new ArraySelection(new int[] {position}, 0, 1);
    }

    static SelectedPositions range(int start, int count)
    {
        return new RangeSelection(start, count);
    }

    static SelectedPositions map(int[] mapping, SelectedPositions positions)
    {
        requireNonNull(mapping, "mapping is null");
        requireNonNull(positions, "positions is null");
        return new MappedSelection(positions, position -> mapping[position]);
    }

    static SelectedPositions map(SelectedPositions positions, IntUnaryOperator mapping)
    {
        requireNonNull(positions, "positions is null");
        requireNonNull(mapping, "mapping is null");
        if (positions instanceof MappedSelection mappedSelection) {
            return new MappedSelection(mappedSelection.positions, position -> mapping.applyAsInt(mappedSelection.mapping.applyAsInt(position)));
        }
        return new MappedSelection(positions, mapping);
    }

    final class ArraySelection
            implements SelectedPositions
    {
        private final int[] positions;
        private final int start;
        private final int count;

        private ArraySelection(int[] positions, int start, int count)
        {
            this.positions = requireNonNull(positions, "positions is null");
            this.start = start;
            this.count = count;
        }

        @Override
        public int count()
        {
            return count;
        }

        @Override
        public int position(int index)
        {
            return positions[start + index];
        }

        @Override
        public int[] backingArrayOrNull()
        {
            return positions;
        }

        @Override
        public int backingArrayOffset()
        {
            return start;
        }

        @Override
        public int[] materialize(int[] target)
        {
            if (target != null && target.length >= count) {
                System.arraycopy(positions, start, target, 0, count);
                return target;
            }
            int[] copy = new int[count];
            System.arraycopy(positions, start, copy, 0, count);
            return copy;
        }
    }

    final class MappedSelection
            implements SelectedPositions
    {
        private final SelectedPositions positions;
        private final IntUnaryOperator mapping;

        private MappedSelection(SelectedPositions positions, IntUnaryOperator mapping)
        {
            this.positions = positions;
            this.mapping = mapping;
        }

        @Override
        public int count()
        {
            return positions.count();
        }

        @Override
        public int position(int index)
        {
            return mapping.applyAsInt(positions.position(index));
        }

        @Override
        public int[] materialize(int[] target)
        {
            int count = count();
            int[] materialized = target != null && target.length >= count ? target : new int[count];
            int[] backingArray = positions.backingArrayOrNull();
            if (backingArray != null) {
                int offset = positions.backingArrayOffset();
                for (int index = 0; index < count; index++) {
                    materialized[index] = mapping.applyAsInt(backingArray[offset + index]);
                }
                return materialized;
            }
            for (int index = 0; index < count; index++) {
                materialized[index] = mapping.applyAsInt(positions.position(index));
            }
            return materialized;
        }
    }

    final class RangeSelection
            implements SelectedPositions
    {
        private final int start;
        private final int count;

        private RangeSelection(int start, int count)
        {
            this.start = start;
            this.count = count;
        }

        @Override
        public int count()
        {
            return count;
        }

        @Override
        public int position(int index)
        {
            return start + index;
        }

        @Override
        public int[] materialize(int[] target)
        {
            int[] positions = target != null && target.length >= count ? target : new int[count];
            for (int index = 0; index < count; index++) {
                positions[index] = start + index;
            }
            return positions;
        }
    }
}
