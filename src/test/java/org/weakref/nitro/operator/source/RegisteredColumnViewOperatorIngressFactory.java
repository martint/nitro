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

import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.TypeIdentity;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/// Immutable, constructed registry of physical column-ingress providers.
///
/// Logical type identities are interpreted only as registry keys. The registry does not infer a
/// carrier, physical encoding, vector implementation, or connector protocol from an identity.
/// Those decisions remain inside the provider registered for that identity.
public final class RegisteredColumnViewOperatorIngressFactory
        implements ColumnViewOperatorIngressFactory
{
    private final Map<TypeIdentity, ColumnViewOperatorIngressFactory> providers;

    public RegisteredColumnViewOperatorIngressFactory(Map<TypeIdentity, ColumnViewOperatorIngressFactory> providers)
    {
        this.providers = Map.copyOf(requireNonNull(providers, "providers is null"));
        this.providers.forEach((identity, provider) -> {
            requireNonNull(identity, "providers contains a null identity");
            requireNonNull(provider, "providers contains a null provider");
        });
    }

    @Override
    public ColumnViewOperatorIngress bind(Field field)
    {
        requireNonNull(field, "field is null");
        TypeIdentity identity = field.type().identity();
        ColumnViewOperatorIngressFactory provider = providers.get(identity);
        if (provider == null) {
            throw new IllegalArgumentException("No column ingress provider registered for type: " + identity);
        }
        ColumnViewOperatorIngress ingress = requireNonNull(
                provider.bind(field),
                "column ingress provider returned null");
        if (!identity.equals(ingress.type().identity())) {
            throw new IllegalArgumentException("column ingress provider returned a binding for a different type");
        }
        return ingress;
    }
}
