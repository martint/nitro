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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;

/// Random access to one logical row domain without flattening its encoded backing vectors.
public interface RowPositionIndex
{
    int size();

    Streams column(int column, int position);

    int sourcePosition(int position);

    /// Returns the physical row domain size of the source vectors backing the logical position.
    default int sourceSize(int position)
    {
        return column(0, position).values().length();
    }

    /// Returns whether two logical positions are backed by the same physical source vectors.
    boolean sharesSource(int leftPosition, int rightPosition);

    default Batch copyRange(Allocator allocator, int start, int length, int[] channels)
    {
        requireNonNull(allocator, "allocator is null");
        requireNonNull(channels, "channels is null");
        if (start < 0 || length <= 0 || (long) start + length > size()) {
            throw new IndexOutOfBoundsException("range is outside row index");
        }

        Allocator.Context context = new Allocator.Context("RowPositionIndex.copyRange");
        try {
            Output[] outputs = new Output[channels.length];
            for (int output = 0; output < channels.length; output++) {
                Streams copied = Streams.empty();
                int channel = channels[output];
                for (int index = 0; index < length; index++) {
                    int position = start + index;
                    copied = allocator.copySinglePositionInto(
                            context,
                            column(channel, position),
                            copied,
                            sourcePosition(position),
                            index,
                            length);
                }
                Streams result = copied;
                outputs[output] = new Output(
                        result.streams(),
                        result::get,
                        (stream, vector) -> allocator.transfer(context, vector),
                        (stream, vector) -> allocator.release(context, vector));
            }
            Mask mask = allocator.allocateRangeMask(context, 0, length);
            return new Batch(
                    mask,
                    _ -> {},
                    batchMask -> allocator.transfer(context, batchMask),
                    batchMask -> allocator.release(context, batchMask),
                    () -> {},
                    outputs);
        }
        catch (RuntimeException | Error failure) {
            allocator.release(context);
            throw failure;
        }
    }

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
