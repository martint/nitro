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
package org.weakref.nitro.core.type;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// Registry-supplied logical and physical operations for a bound type.
///
/// The handles retain their exact carrier signatures. Structural kernel generators bind them
/// without teaching an operator which Java carrier or logical type they implement.
///
/// [#valueRead()] has signature `(Vector, int) -> carrier`, [#identical()] has signature
/// `(carrier, carrier) -> boolean`, and [#comparison()] has signature `(carrier, carrier) -> int`.
/// The provider-owned reader is responsible for every vector representation advertised by its
/// [TypeBinding].
public record TypeOperators(
        Optional<MethodHandle> identical,
        Optional<MethodHandle> hash,
        Optional<MethodHandle> comparison,
        Optional<MethodHandle> flatRead,
        Optional<MethodHandle> flatWrite,
        Optional<MethodHandle> valueRead)
{
    public static final TypeOperators UNSPECIFIED = new TypeOperators(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    public TypeOperators(
            Optional<MethodHandle> identical,
            Optional<MethodHandle> hash,
            Optional<MethodHandle> comparison,
            Optional<MethodHandle> flatRead,
            Optional<MethodHandle> flatWrite)
    {
        this(identical, hash, comparison, flatRead, flatWrite, Optional.empty());
    }

    public TypeOperators
    {
        identical = requireNonNull(identical, "identical is null");
        hash = requireNonNull(hash, "hash is null");
        comparison = requireNonNull(comparison, "comparison is null");
        flatRead = requireNonNull(flatRead, "flatRead is null");
        flatWrite = requireNonNull(flatWrite, "flatWrite is null");
        valueRead = requireNonNull(valueRead, "valueRead is null");
    }
}
