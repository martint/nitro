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

import org.weakref.nitro.core.source.BinaryDomain;
import org.weakref.nitro.core.source.BinaryDomainCapability;
import org.weakref.nitro.core.source.DomainCapability;
import org.weakref.nitro.core.source.TypedDomain;
import org.weakref.nitro.core.type.TypeBinding;

import java.util.Arrays;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// Exact non-null binary equality or inequality domain derived by a registry-owned function capability.
final class BinaryLiteralDomain
        implements TypedDomain, BinaryDomain
{
    private final TypeBinding type;
    private final byte[] literal;
    private final boolean equal;

    BinaryLiteralDomain(TypeBinding type, byte[] literal, boolean equal)
    {
        this.type = requireNonNull(type, "type is null");
        this.literal = Arrays.copyOf(requireNonNull(literal, "literal is null"), literal.length);
        this.equal = equal;
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
        return false;
    }

    @Override
    public boolean test(byte[] data, int offset, int length)
    {
        return (length == literal.length && Arrays.equals(data, offset, offset + length, literal, 0, literal.length)) == equal;
    }

    @Override
    public <T> Optional<T> capability(DomainCapability<T> capability)
    {
        if (capability == BinaryDomainCapability.BINARY_DOMAIN) {
            return Optional.of(capability.valueType().cast(this));
        }
        return Optional.empty();
    }
}
