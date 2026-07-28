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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;

import java.util.Set;
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
        Class<? extends Vector> flatVectorType;
        Class<? extends Vector> alternateFlatVectorType = null;
        if (value.equals(DOUBLE)) {
            carrierType = double.class;
            flatVectorType = F64Vector.class;
        }
        else if (value.equals(BOOLEAN)) {
            carrierType = boolean.class;
            flatVectorType = BooleanVector.class;
        }
        else if (value.equals(VARCHAR) || value.startsWith("benchmark:varchar(") || value.startsWith("benchmark:char(")) {
            carrierType = byte[].class;
            flatVectorType = BinaryVector.class;
        }
        else if (value.equals(INTEGER)) {
            carrierType = long.class;
            flatVectorType = I32Vector.class;
            alternateFlatVectorType = I64Vector.class;
        }
        else if (value.equals(DATE)) {
            carrierType = long.class;
            flatVectorType = I64Vector.class;
            alternateFlatVectorType = I32Vector.class;
        }
        else if (value.equals(BIGINT) ||
                value.equals(TIME) ||
                value.startsWith("benchmark:decimal(")) {
            carrierType = long.class;
            flatVectorType = I64Vector.class;
        }
        else {
            throw new IllegalArgumentException("Unknown benchmark type identity: " + value);
        }
        return new RegistryTypeBinding(identity, carrierType, flatVectorType, alternateFlatVectorType);
    }

    private static final class RegistryTypeBinding
            implements TypeBinding
    {
        private final TypeIdentity identity;
        private final Class<?> carrierType;
        private final Class<? extends Vector> flatVectorType;
        private final Class<? extends Vector> alternateFlatVectorType;
        private final Set<Class<? extends Vector>> supportedVectorTypes;

        private RegistryTypeBinding(
                TypeIdentity identity,
                Class<?> carrierType,
                Class<? extends Vector> flatVectorType,
                Class<? extends Vector> alternateFlatVectorType)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.carrierType = requireNonNull(carrierType, "carrierType is null");
            this.flatVectorType = requireNonNull(flatVectorType, "flatVectorType is null");
            this.alternateFlatVectorType = alternateFlatVectorType;
            this.supportedVectorTypes = alternateFlatVectorType == null
                    ? Set.of(flatVectorType, DictionaryVector.class, RleVector.class)
                    : Set.of(flatVectorType, alternateFlatVectorType, DictionaryVector.class, RleVector.class);
        }

        @Override
        public TypeIdentity identity()
        {
            return identity;
        }

        @Override
        public Class<?> carrierType()
        {
            return carrierType;
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }

        @Override
        public Set<Class<? extends Vector>> supportedVectorTypes()
        {
            return supportedVectorTypes;
        }

        @Override
        public boolean supportsVector(Vector vector)
        {
            requireNonNull(vector, "vector is null");
            if (flatVectorType.isInstance(vector) ||
                    (alternateFlatVectorType != null && alternateFlatVectorType.isInstance(vector))) {
                return true;
            }
            if (vector instanceof DictionaryVector dictionary) {
                return supportsVector(dictionary.values());
            }
            if (vector instanceof RleVector rle) {
                return supportsVector(rle.values());
            }
            return false;
        }
    }
}
