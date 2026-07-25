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
package org.weakref.nitro.operator.source.compatibility;

import org.weakref.nitro.core.source.DomainCapability;
import org.weakref.nitro.core.source.TypedDomain;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.operator.DynamicFilter;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// Typed-domain view of a legacy native runtime filter.
final class NativeRuntimeFilterDomain
        implements TypedDomain
{
    private final TypeBinding type;
    private final DynamicFilter filter;

    NativeRuntimeFilterDomain(TypeBinding type, DynamicFilter filter)
    {
        this.type = requireNonNull(type, "type is null");
        this.filter = requireNonNull(filter, "filter is null");
    }

    @Override
    public TypeBinding type()
    {
        return type;
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
        if (capability == NativeRuntimeFilterCapability.NATIVE_RUNTIME_FILTER) {
            return Optional.of(capability.valueType().cast((NativeRuntimeFilterAccess) filter::withColumn));
        }
        return Optional.empty();
    }
}
