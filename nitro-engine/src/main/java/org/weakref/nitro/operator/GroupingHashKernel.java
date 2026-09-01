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
package org.weakref.nitro.operator;

import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Vector;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Computes the stable composite hash used by grouping-hash producer and consumer contracts. */
public final class GroupingHashKernel
{
    private final StructuralKeyKernel[] keys;

    public GroupingHashKernel(StructuralTypeKernelFactory structuralTypes, List<TypeBinding> keyTypes)
    {
        requireNonNull(structuralTypes, "structuralTypes is null");
        keys = requireNonNull(keyTypes, "keyTypes is null").stream()
                .map(structuralTypes::key)
                .toArray(StructuralKeyKernel[]::new);
    }

    public long hash(Vector[] values, Vector[] nulls, int position)
    {
        requireNonNull(values, "values is null");
        requireNonNull(nulls, "nulls is null");
        if (values.length != keys.length || nulls.length != keys.length) {
            throw new IllegalArgumentException("Grouping key vectors do not match the kernel arity");
        }
        long hash = 0;
        for (int key = 0; key < keys.length; key++) {
            long fieldHash = OperatorVectorSupport.isNull(nulls[key], position)
                    ? 0
                    : keys[key].hash(values[key], nulls[key], position);
            hash = 31 * hash + fieldHash;
        }
        return hash;
    }
}
