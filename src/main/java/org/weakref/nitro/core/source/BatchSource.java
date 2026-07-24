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
package org.weakref.nitro.core.source;

import org.weakref.nitro.core.type.Schema;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/// Format-neutral source lifecycle consumed by the execution engine.
public interface BatchSource
        extends AutoCloseable
{
    Schema schema();

    /// Returns the source-local handle for an output field.
    default SourceColumnHandle column(int outputIndex)
    {
        return new OrdinalSourceColumnHandle(outputIndex, schema().field(outputIndex).type());
    }

    Set<SourceCapability> capabilities();

    /// Returns the exact output row count when known without reading the source.
    default OptionalLong exactRows()
    {
        return OptionalLong.empty();
    }

    /// Returns an optional source-wide protocol by typed key.
    default <T> Optional<T> protocol(SourceProtocol<T> protocol)
    {
        return Optional.empty();
    }

    SourcePoll poll();

    default RuntimeFilterAcceptance addRuntimeFilter(RuntimeFilter filter)
    {
        return RuntimeFilterAcceptance.REJECTED;
    }

    /// Returns whether the source can accept a runtime filter for this source-local column.
    default boolean supportsRuntimeFilter(SourceColumnHandle column)
    {
        return capabilities().contains(SourceCapability.RUNTIME_FILTER);
    }

    @Override
    void close();
}
