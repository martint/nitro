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

import java.lang.invoke.MethodHandle;

/// Provider-owned bridge from a scalar Java result carrier into allocator-owned Nitro vector storage.
public interface ScalarResultWriterFactory
{
    ScalarResultWriter createWriter();

    /// Exact provider handle with signature `(ScalarResultWriter, int, carrier) -> void`.
    /// Generated adapters invoke it immediately after each scalar call and retain no carrier objects.
    MethodHandle appendTarget();
}
