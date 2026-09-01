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

import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

/// Random access to one logical row domain without flattening its encoded backing vectors.
public interface RowPositionIndex
{
    int size();

    Streams column(int column, int position);

    int sourcePosition(int position);

    /// Returns whether two logical positions are backed by the same physical source vectors.
    boolean sharesSource(int leftPosition, int rightPosition);

    /// Compares two non-null logical values through their registered type semantics.
    default int compareNonNull(int leftColumn, int leftPosition, int rightColumn, int rightPosition)
    {
        throw new UnsupportedOperationException("Logical comparison is not available");
    }

    default boolean isNull(int column, int position)
    {
        return OperatorVectorSupport.isNull(column(column, position).getOrNull(Stream.NULLS), sourcePosition(position));
    }

    default long longValue(int column, int position)
    {
        return OperatorVectorSupport.longValue(column(column, position).values(), sourcePosition(position));
    }
}
