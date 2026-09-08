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

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Provider proof that logical key identity is the ordered product of independently nullable child identities.
 *
 * <p>Each child selects a physical structural path and supplies the binding that owns its semantics. Children may
 * themselves publish persistent layouts, so a consumer can recursively resolve an arbitrary finite product into one
 * physical descriptor without recognizing a logical type, field count, or carrier combination. Nullness at every
 * enclosing product boundary remains part of the identity.
 */
public record PersistentKeyLayout(List<Field> fields)
{
    public record Field(List<String> fieldPath, TypeBinding type)
    {
        public Field
        {
            fieldPath = List.copyOf(requireNonNull(fieldPath, "fieldPath is null"));
            if (fieldPath.isEmpty()) {
                throw new IllegalArgumentException("fieldPath is empty");
            }
            if (fieldPath.stream().anyMatch(field -> field == null || field.isEmpty())) {
                throw new IllegalArgumentException("fieldPath contains a null or empty field");
            }
            type = requireNonNull(type, "type is null");
        }
    }

    public PersistentKeyLayout
    {
        fields = List.copyOf(requireNonNull(fields, "fields is null"));
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("fields is empty");
        }
        fields.forEach(field -> requireNonNull(field, "field is null"));
    }
}
