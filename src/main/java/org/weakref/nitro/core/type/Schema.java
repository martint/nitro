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

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/// Immutable logical output schema available before execution starts.
public record Schema(List<Field> fields)
{
    private static final TypeBinding UNSPECIFIED_TYPE = new TypeBinding()
    {
        private static final TypeIdentity IDENTITY = new TypeIdentity("nitro:unspecified");

        @Override
        public TypeIdentity identity()
        {
            return IDENTITY;
        }

        @Override
        public Class<?> carrierType()
        {
            return Object.class;
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }
    };

    public Schema
    {
        fields = List.copyOf(requireNonNull(fields, "fields is null"));
    }

    /// Transitional schema for legacy factories that have not yet bound logical types.
    public static Schema unspecified(int fieldCount)
    {
        if (fieldCount < 0) {
            throw new IllegalArgumentException("fieldCount is negative");
        }
        List<Field> fields = new ArrayList<>(fieldCount);
        for (int index = 0; index < fieldCount; index++) {
            fields.add(new Field(UNSPECIFIED_TYPE, true));
        }
        return new Schema(fields);
    }

    public int size()
    {
        return fields.size();
    }

    public Field field(int index)
    {
        return fields.get(index);
    }
}
