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
        public boolean isSpecified()
        {
            return false;
        }

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

    /// Transitional named schema for a source whose logical type registry has not yet been wired.
    public static Schema unspecified(List<String> fieldNames)
    {
        requireNonNull(fieldNames, "fieldNames is null");
        List<Field> fields = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            fields.add(new Field(requireNonNull(fieldName, "fieldName is null"), UNSPECIFIED_TYPE, true));
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

    /// Whether values described by this schema can cross a physical operator edge expecting [other].
    ///
    /// Field names are descriptive metadata and do not affect the value layout. Type bindings and
    /// nullability remain part of the contract.
    public boolean isLayoutCompatibleWith(Schema other)
    {
        requireNonNull(other, "other is null");
        if (size() != other.size()) {
            return false;
        }
        for (int index = 0; index < size(); index++) {
            Field field = field(index);
            Field otherField = other.field(index);
            if (!field.type().equals(otherField.type()) || field.nullable() != otherField.nullable()) {
                return false;
            }
        }
        return true;
    }
}
