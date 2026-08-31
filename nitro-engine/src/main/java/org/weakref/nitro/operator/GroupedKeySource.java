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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

interface GroupedKeySource
{
    Streams groupedKeyOutput(int outputIndex, org.weakref.nitro.data.Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext);

    default Streams copyGroupedKeyPosition(int outputIndex, Streams output, int sourcePosition, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        return null;
    }

    default boolean supportsGroupedKeyPositionComparison(int outputIndex)
    {
        return false;
    }

    default boolean groupedKeyPositionsMayBeNull(int outputIndex)
    {
        return true;
    }

    default boolean groupedKeyPositionIsNull(int outputIndex, int position)
    {
        throw new UnsupportedOperationException("Grouped-key position comparison is not supported");
    }

    default int compareGroupedKeyPosition(int outputIndex, int position, Vector otherValues, int otherPosition)
    {
        throw new UnsupportedOperationException("Grouped-key position comparison is not supported");
    }

    default int compareGroupedKeyPositions(int outputIndex, int leftPosition, int rightPosition)
    {
        throw new UnsupportedOperationException("Grouped-key position comparison is not supported");
    }
}
