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
package org.weakref.nitro.core.source;

import org.weakref.nitro.core.type.TypeBinding;

import static java.util.Objects.requireNonNull;

/// Source-local identity for a field addressed by output ordinal.
///
/// This is deliberately an identity object rather than a value record. A source must create and retain one
/// instance per output field; handles created independently, even with the same ordinal and type, belong to
/// different source identities.
public final class OrdinalSourceColumnHandle
        implements SourceColumnHandle
{
    private final int ordinal;
    private final TypeBinding type;

    public OrdinalSourceColumnHandle(int ordinal, TypeBinding type)
    {
        if (ordinal < 0) {
            throw new IllegalArgumentException("ordinal is negative");
        }
        this.ordinal = ordinal;
        this.type = requireNonNull(type, "type is null");
    }

    public int ordinal()
    {
        return ordinal;
    }

    @Override
    public TypeBinding type()
    {
        return type;
    }
}
