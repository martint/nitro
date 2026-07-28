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

import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAllocator;

/// Provider-owned construction of physical value vectors for a logical type.
///
/// Nullability remains a separate Nitro stream. [#nullValues] constructs only the placeholder
/// VALUES representation that accompanies an all-true NULLS stream.
public interface TypeVectorFactory
{
    Vector constant(VectorAllocator allocator, Object value, int length);

    Vector nullValues(VectorAllocator allocator, int length);
}
