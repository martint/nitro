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
package org.weakref.nitro.parquet;

import org.weakref.nitro.core.type.TypeBinding;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Validated alignment between one physical Parquet group and connector-supplied logical children. */
record NestedLogicalBindings(List<TypeBinding> types, List<ParquetValueBinding> values)
{
    NestedLogicalBindings
    {
        types = List.copyOf(requireNonNull(types, "types is null"));
        values = List.copyOf(requireNonNull(values, "values is null"));
        if (types.size() != values.size()) {
            throw new IllegalArgumentException("Nested logical type and value binding counts differ");
        }
    }

    static NestedLogicalBindings require(
            String path,
            TypeBinding outputType,
            ParquetValueBinding.Group binding,
            int physicalChildren)
    {
        requireNonNull(path, "path is null");
        List<TypeBinding> types = requireNonNull(outputType, "outputType is null").nestedValueTypes();
        if (types.size() != physicalChildren) {
            throw new UnsupportedParquetFeatureException(
                    "Logical nested type " + outputType.identity() + " has " + types.size() +
                            " children but Parquet field '" + path + "' requires " + physicalChildren);
        }
        List<ParquetValueBinding> values = requireNonNull(binding, "binding is null").children();
        if (values.size() != physicalChildren) {
            throw new UnsupportedParquetFeatureException(
                    "Logical binding for Parquet field '" + path + "' has " + values.size() +
                            " children but the physical field requires " + physicalChildren);
        }
        return new NestedLogicalBindings(types, values);
    }

    Primitive childPrimitive(int index, ParquetSchema.Primitive leaf)
    {
        return switch (values.get(index)) {
            case ParquetPrimitiveValueBinding primitive -> new Primitive(types.get(index), primitive);
            case ParquetValueBinding.Direct ignored -> new Primitive(types.get(index), null);
            case ParquetValueBinding.Group ignored -> throw new UnsupportedParquetFeatureException(
                    "Logical group binding does not match primitive Parquet field '" + String.join(".", leaf.path()) + "'");
        };
    }

    Group childGroup(int index, ParquetSchema.Group group)
    {
        return switch (values.get(index)) {
            case ParquetValueBinding.Group nested -> new Group(types.get(index), nested);
            case ParquetPrimitiveValueBinding ignored -> throw mismatchedGroup(group);
            case ParquetValueBinding.Direct ignored -> throw mismatchedGroup(group);
        };
    }

    private static UnsupportedParquetFeatureException mismatchedGroup(ParquetSchema.Group group)
    {
        return new UnsupportedParquetFeatureException(
                "Logical primitive binding does not match nested Parquet field '" + group.name() + "'");
    }

    record Primitive(TypeBinding type, ParquetPrimitiveValueBinding value) {}

    record Group(TypeBinding type, ParquetValueBinding.Group value) {}
}
