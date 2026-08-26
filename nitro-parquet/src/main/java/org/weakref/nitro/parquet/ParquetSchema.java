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

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/** Immutable logical tree and physical-leaf layout parsed directly from a Parquet footer. */
final class ParquetSchema
{
    sealed interface Node
            permits Group, Primitive
    {
        String name();

        FieldRepetitionType repetition();

        int maximumDefinitionLevel();

        int maximumRepetitionLevel();

        List<Primitive> leaves();
    }

    record Group(
            String name,
            FieldRepetitionType repetition,
            ConvertedType convertedType,
            List<Node> children,
            int maximumDefinitionLevel,
            int maximumRepetitionLevel)
            implements Node
    {
        Group
        {
            requireNonNull(name, "name is null");
            children = List.copyOf(children);
        }

        @Override
        public List<Primitive> leaves()
        {
            return children.stream().flatMap(child -> child.leaves().stream()).toList();
        }

        boolean isMap()
        {
            return convertedType == ConvertedType.MAP;
        }

        boolean isList()
        {
            return convertedType == ConvertedType.LIST;
        }
    }

    record Primitive(
            String name,
            FieldRepetitionType repetition,
            Type type,
            ConvertedType convertedType,
            int typeLength,
            int leafIndex,
            List<String> path,
            int maximumDefinitionLevel,
            int maximumRepetitionLevel)
            implements Node
    {
        Primitive
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
            path = List.copyOf(path);
        }

        @Override
        public List<Primitive> leaves()
        {
            return List.of(this);
        }

        boolean decimal()
        {
            return convertedType == ConvertedType.DECIMAL;
        }
    }

    private record Parsed(Node node, int nextSchemaIndex, int nextLeafIndex) {}

    private final List<Node> fields;
    private final Map<String, Node> fieldsByName;
    private final List<Primitive> leaves;

    private ParquetSchema(List<Node> fields)
    {
        this.fields = List.copyOf(fields);
        Map<String, Node> byName = new LinkedHashMap<>();
        for (Node field : fields) {
            if (byName.put(field.name(), field) != null) {
                throw new UnsupportedParquetFeatureException("Duplicate top-level Parquet field '" + field.name() + "'");
            }
        }
        this.fieldsByName = Map.copyOf(byName);
        this.leaves = fields.stream().flatMap(field -> field.leaves().stream()).toList();
    }

    static ParquetSchema parse(List<SchemaElement> elements)
    {
        requireNonNull(elements, "elements is null");
        if (elements.isEmpty()) {
            throw new UnsupportedParquetFeatureException("Parquet schema is empty");
        }
        SchemaElement root = elements.getFirst();
        if (root.type != null || root.num_children < 0) {
            throw new UnsupportedParquetFeatureException("Invalid Parquet root schema node '" + root.name + "'");
        }

        List<Node> fields = new ArrayList<>(root.num_children);
        int schemaIndex = 1;
        int leafIndex = 0;
        for (int field = 0; field < root.num_children; field++) {
            Parsed parsed = parseNode(elements, schemaIndex, leafIndex, List.of(), 0, 0);
            fields.add(parsed.node());
            schemaIndex = parsed.nextSchemaIndex();
            leafIndex = parsed.nextLeafIndex();
        }
        if (schemaIndex != elements.size()) {
            throw new UnsupportedParquetFeatureException(
                    "Parquet schema contains " + (elements.size() - schemaIndex) + " unreachable node(s)");
        }
        return new ParquetSchema(fields);
    }

    private static Parsed parseNode(
            List<SchemaElement> elements,
            int schemaIndex,
            int leafIndex,
            List<String> parentPath,
            int parentDefinitionLevel,
            int parentRepetitionLevel)
    {
        if (schemaIndex >= elements.size()) {
            throw new UnsupportedParquetFeatureException("Parquet schema ended before all declared children");
        }
        SchemaElement element = elements.get(schemaIndex);
        FieldRepetitionType repetition = element.repetition_type;
        if (repetition == null) {
            throw new UnsupportedParquetFeatureException("Parquet field '" + element.name + "' has no repetition type");
        }
        int definitionLevel = parentDefinitionLevel + (repetition == FieldRepetitionType.REQUIRED ? 0 : 1);
        int repetitionLevel = parentRepetitionLevel + (repetition == FieldRepetitionType.REPEATED ? 1 : 0);
        List<String> path = new ArrayList<>(parentPath.size() + 1);
        path.addAll(parentPath);
        path.add(element.name);

        if (element.type != null) {
            if (element.num_children > 0) {
                throw new UnsupportedParquetFeatureException("Primitive Parquet field '" + element.name + "' declares children");
            }
            return new Parsed(
                    new Primitive(
                            element.name,
                            repetition,
                            element.type,
                            element.converted_type,
                            element.type_length,
                            leafIndex,
                            path,
                            definitionLevel,
                            repetitionLevel),
                    schemaIndex + 1,
                    leafIndex + 1);
        }

        if (element.num_children <= 0) {
            throw new UnsupportedParquetFeatureException("Parquet group '" + element.name + "' has no children");
        }
        List<Node> children = new ArrayList<>(element.num_children);
        int nextSchemaIndex = schemaIndex + 1;
        int nextLeafIndex = leafIndex;
        for (int child = 0; child < element.num_children; child++) {
            Parsed parsed = parseNode(elements, nextSchemaIndex, nextLeafIndex, path, definitionLevel, repetitionLevel);
            children.add(parsed.node());
            nextSchemaIndex = parsed.nextSchemaIndex();
            nextLeafIndex = parsed.nextLeafIndex();
        }
        return new Parsed(
                new Group(
                        element.name,
                        repetition,
                        element.converted_type,
                        children,
                        definitionLevel,
                        repetitionLevel),
                nextSchemaIndex,
                nextLeafIndex);
    }

    List<Node> fields()
    {
        return fields;
    }

    Node field(String name)
    {
        Node field = fieldsByName.get(name);
        if (field == null) {
            throw new IllegalArgumentException("No such Parquet field: " + name + " (have " + fieldsByName.keySet() + ")");
        }
        return field;
    }

    List<Primitive> leaves()
    {
        return leaves;
    }
}
