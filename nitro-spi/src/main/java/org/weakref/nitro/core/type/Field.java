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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// One logical output field.
public record Field(Optional<String> name, TypeBinding type, boolean nullable)
{
    public Field
    {
        name = requireNonNull(name, "name is null");
        type = requireNonNull(type, "type is null");
    }

    public Field(String name, TypeBinding type, boolean nullable)
    {
        this(Optional.of(requireNonNull(name, "name is null")), type, nullable);
    }

    public Field(TypeBinding type, boolean nullable)
    {
        this(Optional.empty(), type, nullable);
    }
}
