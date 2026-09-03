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
package org.weakref.nitro.core.function;

import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAllocator;

/// Invocation-local destination for scalar results whose Java carrier is not a primitive stack type.
///
/// A writer may retain sizing history between invocations, but must clear references to the allocator,
/// mask, vectors, and provider result objects when [#finish()] or [#abort()] returns.
public interface ScalarResultWriter
{
    /// Starts one ordered write session. The proposed vector may belong to another allocation scope;
    /// implementations must consult [VectorAllocator#owns(Vector)] before mutating or releasing it. An owned
    /// proposed vector is transferred to the session and must not be observed by the caller until returned by
    /// [#finish()]; [#abort()] provides resource cleanup, not rollback of in-place writes.
    void begin(VectorAllocator allocator, Vector proposed, int positionCount, Mask activeMask);

    /// Completes the session and returns an allocator-owned Nitro vector.
    Vector finish();

    /// Abandons the session and releases storage acquired by the writer where possible.
    void abort();
}
