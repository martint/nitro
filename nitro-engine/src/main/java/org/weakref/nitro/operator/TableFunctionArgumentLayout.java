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

import org.weakref.nitro.core.type.Schema;

import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

/// Physical view of one table argument in a combined, partition-aligned source.
///
/// A marker channel is absent for a single source. When present, non-null marker positions are real argument rows;
/// the null suffix consists of alignment filler and is never exposed to the function or pass-through gathering.
public record TableFunctionArgumentLayout(Schema schema, int[] inputChannels, OptionalInt markerChannel)
{
    public TableFunctionArgumentLayout
    {
        schema = requireNonNull(schema, "schema is null");
        inputChannels = requireNonNull(inputChannels, "inputChannels is null").clone();
        markerChannel = requireNonNull(markerChannel, "markerChannel is null");
        if (schema.size() != inputChannels.length) {
            throw new IllegalArgumentException("argument schema does not match input channels");
        }
        if (markerChannel.isPresent() && markerChannel.orElseThrow() < 0) {
            throw new IllegalArgumentException("marker channel is negative");
        }
    }

    @Override
    public int[] inputChannels()
    {
        return inputChannels.clone();
    }

    int[] inputChannelsInternal()
    {
        return inputChannels;
    }
}
