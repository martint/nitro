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

import org.weakref.nitro.data.Vector;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/// A plan-time logical type binding.
///
/// Bindings are supplied dynamically by the embedding type registry. Operators carry them but do
/// not branch on their identities or Java carrier classes.
public interface TypeBinding
{
    /// Whether this binding contains plan-time logical type information.
    ///
    /// False is reserved for the temporary compatibility binding returned by
    /// [Schema#unspecified(int)]. Registry-provided bindings are always specified.
    default boolean isSpecified()
    {
        return true;
    }

    TypeIdentity identity();

    Class<?> carrierType();

    TypeOperators operators();

    /// Provider-owned construction for typed constants and null placeholders.
    ///
    /// Empty is retained for compatibility bindings and types that cannot be materialized as
    /// literals. The evaluator must not infer a factory from a carrier class or type identity.
    default Optional<TypeVectorFactory> vectorFactory()
    {
        return Optional.empty();
    }

    /// Provider-owned construction from structural child expressions.
    ///
    /// Empty means that the logical type cannot be constructed from child expressions. The
    /// evaluator passes generic stream bundles and must not infer construction from the type
    /// identity, carrier, or admitted vector classes.
    default Optional<TypeVectorConstructor> vectorConstructor()
    {
        return Optional.empty();
    }

    /// Vector representations this type provider permits at an SPI boundary.
    ///
    /// The set is descriptive metadata for connectors and integration adapters. A provider can
    /// override [#supportsVector(Vector)] when validity also depends on nested representation.
    /// An empty set is retained only for legacy bindings and is not eligible for direct vector
    /// source ingress.
    default Set<Class<? extends Vector>> supportedVectorTypes()
    {
        return Set.of();
    }

    default boolean supportsVector(Vector vector)
    {
        requireNonNull(vector, "vector is null");
        return supportedVectorTypes().stream().anyMatch(type -> type.isInstance(vector));
    }
}
