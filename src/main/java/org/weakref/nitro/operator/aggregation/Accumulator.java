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
package org.weakref.nitro.operator.aggregation;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;

public interface Accumulator
{
    Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size);

    Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size);

    void initialize(Streams state, int offset, int length);

    void accumulate(Streams state, int group, Mask mask, StreamAccessor streams);

    void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams);

    default Streams result(int maxGroup, Streams state, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        return result(maxGroup, state, output, allocator, allocationContext);
    }

    Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext);
}
