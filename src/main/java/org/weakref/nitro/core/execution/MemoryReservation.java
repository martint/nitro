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
package org.weakref.nitro.core.execution;

import java.util.concurrent.CompletionStage;

/// Host-accounted execution memory.
public interface MemoryReservation
{
    /// Adds bytes to the reservation immediately.
    ///
    /// The returned continuation is complete when the host permits execution to continue. A host
    /// may therefore account the bytes and return an incomplete stage to apply backpressure without
    /// exposing its scheduler or memory-context types to Nitro.
    CompletionStage<Void> reserve(long bytes);

    /// Removes bytes from the reservation.
    void release(long bytes);

    long reservedBytes();
}
