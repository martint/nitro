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

import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.RuntimeFilter;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.Operator;

import java.util.Optional;

/// Engine-side policy for translating a source generation into the native operator island.
///
/// The integration owner constructs this policy and passes it to [BatchSourceOperator]. Source
/// and connector implementations depend only on the core source SPI; they do not see this engine
/// adapter or native operator types.
public interface SourceOperatorIngress
        extends SourceBatchOperatorIngress
{
    /// Returns a direct operator only when the source is already a native compatibility facade.
    default Optional<Operator> directOperator(BatchSource source)
    {
        return Optional.empty();
    }

    /// Returns whether this ingress can translate an operator-island filter for [column] of [source].
    default boolean supportsRuntimeFilter(BatchSource source, SourceColumnHandle column)
    {
        return false;
    }

    /// Translates an operator-island filter into the source SPI.
    default RuntimeFilter runtimeFilter(SourceColumnHandle column, DynamicFilter filter)
    {
        throw new UnsupportedOperationException("runtime filter translation is not supported");
    }
}
