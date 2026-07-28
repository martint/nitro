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
package org.weakref.nitro.core.type;

import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAllocator;

import java.util.List;

/// Provider-owned construction of a physical value vector from structural child expressions.
///
/// Every argument bundle has {@code length} logical positions. The provider owns the mapping
/// from those row-wise arguments to its physical representation, including nested child
/// cardinality and offsets. Parent null and error streams remain evaluator concerns.
public interface TypeVectorConstructor
{
    Vector construct(VectorAllocator allocator, List<Streams> arguments, int length);
}
