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
import org.weakref.nitro.core.source.DomainCapability;
import org.weakref.nitro.core.source.LongDomainCapability;
import org.weakref.nitro.core.source.RuntimeFilter;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.core.source.TypedDomain;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.operator.DynamicFilter;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// Engine-boundary translation of native long predicates into the neutral source domain protocol.
public final class LongDomainRuntimeFilterSourceIngress
        implements RuntimeFilterSourceIngress
{
    @Override
    public boolean supports(BatchSource source, SourceColumnHandle column)
    {
        requireNonNull(source, "source is null");
        return source.supportsRuntimeFilter(requireNonNull(column, "column is null"));
    }

    @Override
    public RuntimeFilter runtimeFilter(SourceColumnHandle column, DynamicFilter filter)
    {
        requireNonNull(column, "column is null");
        return new RuntimeFilter(
                column,
                new LongDomainView(column.type(), requireNonNull(filter, "filter is null")),
                false);
    }

    private record LongDomainView(TypeBinding type, DynamicFilter filter)
            implements TypedDomain
    {
        private LongDomainView
        {
            requireNonNull(type, "type is null");
            requireNonNull(filter, "filter is null");
        }

        @Override
        public boolean includesNull()
        {
            return false;
        }

        @Override
        public boolean isAll()
        {
            return false;
        }

        @Override
        public boolean isNone()
        {
            return filter.isEmpty();
        }

        @Override
        public <T> Optional<T> capability(DomainCapability<T> capability)
        {
            if (capability == LongDomainCapability.LONG_DOMAIN) {
                return Optional.of(capability.valueType().cast(filter));
            }
            return Optional.empty();
        }
    }
}
