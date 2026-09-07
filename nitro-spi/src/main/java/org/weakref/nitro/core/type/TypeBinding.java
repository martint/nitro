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

import org.weakref.nitro.core.function.ScalarResultWriterFactory;
import org.weakref.nitro.data.Vector;

import java.lang.invoke.MethodHandle;
import java.util.List;
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

    /// Provider-owned scalar carrier reader with exact signature `(Vector, int) -> carrier`.
    ///
    /// Generated scalar adapters use this only for non-primitive stack carriers. It is separate
    /// from [TypeOperators#valueRead()] because that method participates in the structural
    /// comparison kernel contract; a type may support scalar invocation without exposing the
    /// full value/comparison/identity bundle.
    default Optional<MethodHandle> scalarValueReader()
    {
        return Optional.empty();
    }

    /// Provider-owned materialization contract for scalar functions returning a reference carrier.
    default Optional<ScalarResultWriterFactory> scalarResultWriterFactory()
    {
        return Optional.empty();
    }

    /// Optional batch-bound key operations for physical representations whose access path is
    /// expensive to rediscover for every logical row.
    ///
    /// The returned binder remains provider-owned. Operators must treat it as opaque and must
    /// preserve [#operators()] as the semantic fallback when it is absent.
    default Optional<TypeKeyBinder> keyBinder()
    {
        return Optional.empty();
    }

    /**
     * Optional provider proof that logical key identity is the declared tuple of fixed-width primitive lanes.
     *
     * <p>Generated key consumers resolve the current physical vector shape once per batch and specialize their hot
     * loops from this provider-owned logical layout. The proof is deliberately separate from raw-vector identity:
     * the current vector may be structural, dictionary encoded, region backed, or run length encoded.
     */
    default Optional<FixedWidthKeyLayout> fixedWidthKeyLayout()
    {
        return Optional.empty();
    }

    /// Optional exact normalized ordering key for physical domains that fit in 64 bits.
    ///
    /// Unsigned key comparison must agree with this binding's logical comparison for every
    /// non-null admitted value. Consumers use the ordinary comparison capability when absent.
    default Optional<TypeOrderKeyBinder> orderKeyBinder()
    {
        return Optional.empty();
    }

    /// Whether grouping implementations for this binding can consume a provider-compatible,
    /// position-aligned authoritative hash instead of recomputing the logical key hash.
    ///
    /// This is an explicit physical capability. Embeddings must not infer it from a logical
    /// type identity, carrier class, or vector class.
    default boolean supportsAuthoritativeGroupingHash()
    {
        return false;
    }

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

    /// Logical child bindings for a structural value.
    ///
    /// Array bindings expose the element, map bindings expose key then value, and row bindings
    /// expose fields in logical order. Empty identifies a scalar or a structural binding whose
    /// provider has not exposed recursive boundary construction. Consumers must use the admitted
    /// vector shape to distinguish structural kinds; type identities and carrier classes are not
    /// engine vocabulary.
    default List<TypeBinding> nestedValueTypes()
    {
        return List.of();
    }

    /**
     * Canonical fixed-width flat-key storage for a logical type carried as a {@code long}.
     *
     * <p>This is a logical-domain guarantee, not a description of the current batch's vector width. Empty retains
     * the engine's ordinary full-width representation.
     */
    default Optional<LongFlatKeyStorage> longFlatKeyStorage()
    {
        return Optional.empty();
    }

    /**
     * Whether non-null values carried by an integral vector have logical DISTINCT identity exactly when their
     * sign-extended {@code long} values are equal. This provider-owned proof permits primitive key tables without
     * teaching an operator the logical type identity. It must be false for carriers with normalized or otherwise
     * non-bitwise identity semantics.
     */
    default boolean supportsRawLongKeyIdentity()
    {
        return false;
    }

    /**
     * Whether non-null values in every admitted vector representation have logical key identity exactly when their
     * physical carrier values are equal. This provider-owned proof permits the engine's generic physical key tables
     * even when the provider also exposes richer logical hash and identity operations. It must be false for types
     * whose key semantics normalize, canonicalize, or otherwise differ from their physical vector representation.
     */
    default boolean supportsRawKeyIdentity()
    {
        return false;
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
        for (Class<? extends Vector> type : supportedVectorTypes()) {
            if (type.isInstance(vector)) {
                return true;
            }
        }
        return false;
    }
}
