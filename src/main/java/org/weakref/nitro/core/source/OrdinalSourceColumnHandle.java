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

/// Default source-local handle for a field addressed by output ordinal.
public record OrdinalSourceColumnHandle(int ordinal, TypeBinding type)
        implements SourceColumnHandle
{
    public OrdinalSourceColumnHandle
    {
        if (ordinal < 0) {
            throw new IllegalArgumentException("ordinal is negative");
        }
        type = requireNonNull(type, "type is null");
    }
}
