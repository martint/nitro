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
package org.weakref.nitro.benchmark;

import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.TypeRegistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/// Logical type registry used by the standalone benchmark planner adapters.
///
/// The identities are deliberately opaque to Nitro. This registry is test-harness infrastructure,
/// not an engine built-in type vocabulary.
public final class BenchmarkTypeRegistry
        implements TypeRegistry
{
    public static final String BIGINT = "benchmark:bigint";
    public static final String INTEGER = "benchmark:integer";
    public static final String DOUBLE = "benchmark:double";
    public static final String BOOLEAN = "benchmark:boolean";
    public static final String DATE = "benchmark:date";
    public static final String TIME = "benchmark:time";
    public static final String VARCHAR = "benchmark:varchar";

    private final ConcurrentMap<TypeIdentity, TypeBinding> bindings = new ConcurrentHashMap<>();

    @Override
    public TypeBinding resolve(TypeIdentity identity)
    {
        requireNonNull(identity, "identity is null");
        return bindings.computeIfAbsent(identity, BenchmarkTypeRegistry::createBinding);
    }

    private static TypeBinding createBinding(TypeIdentity identity)
    {
        String value = identity.value();
        Class<?> carrierType;
        if (value.equals(DOUBLE)) {
            carrierType = double.class;
        }
        else if (value.equals(BOOLEAN)) {
            carrierType = boolean.class;
        }
        else if (value.equals(VARCHAR) || value.startsWith("benchmark:varchar(") || value.startsWith("benchmark:char(")) {
            carrierType = byte[].class;
        }
        else if (value.equals(BIGINT) ||
                value.equals(INTEGER) ||
                value.equals(DATE) ||
                value.equals(TIME) ||
                value.startsWith("benchmark:decimal(")) {
            carrierType = long.class;
        }
        else {
            throw new IllegalArgumentException("Unknown benchmark type identity: " + value);
        }
        return new RegistryTypeBinding(identity, carrierType);
    }

    private record RegistryTypeBinding(TypeIdentity identity, Class<?> carrierType)
            implements TypeBinding
    {
        private RegistryTypeBinding
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(carrierType, "carrierType is null");
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }
    }
}
