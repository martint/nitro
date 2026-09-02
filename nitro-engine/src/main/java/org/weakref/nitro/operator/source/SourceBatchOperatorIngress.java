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
package org.weakref.nitro.operator.source;

import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Batch;

/// Engine-side policy for admitting a format-neutral batch into an operator island.
public interface SourceBatchOperatorIngress
{
    /// On success, takes ownership of [batch] and returns a native batch that owns its resulting
    /// lifetime. If adaptation fails, ownership remains with the caller.
    Batch adapt(SourceBatch batch);

    /// Translates the operator island's row selection into the batch SPI.
    default Selection selection(Mask mask)
    {
        throw new UnsupportedOperationException("selection translation is not supported");
    }
}
