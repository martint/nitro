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
package org.weakref.nitro.benchmark;

import io.trino.tpcds.Table;
import io.trino.tpcds.column.ColumnType;
import io.trino.tpch.TpchColumnType;
import io.trino.tpch.TpchTable;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeRegistry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.benchmark.BenchmarkTypeRegistry.BIGINT;
import static org.weakref.nitro.benchmark.BenchmarkTypeRegistry.BOOLEAN;
import static org.weakref.nitro.benchmark.BenchmarkTypeRegistry.DATE;
import static org.weakref.nitro.benchmark.BenchmarkTypeRegistry.DOUBLE;
import static org.weakref.nitro.benchmark.BenchmarkTypeRegistry.INTEGER;
import static org.weakref.nitro.benchmark.BenchmarkTypeRegistry.TIME;
import static org.weakref.nitro.benchmark.BenchmarkTypeRegistry.VARCHAR;

/// Planner-side schema adapter for standalone benchmark catalogs.
///
/// Catalog and Parquet classes terminate here. The returned schema contains only Nitro core types
/// and can cross the source/operator boundary without exposing connector metadata.
public final class BenchmarkSchemaRegistry
{
    private final TypeRegistry types;
    private final ConcurrentMap<SchemaKey, Schema> schemas = new ConcurrentHashMap<>();

    public BenchmarkSchemaRegistry(TypeRegistry types)
    {
        this.types = requireNonNull(types, "types is null");
    }

    public Schema tpch(String tableName, List<String> columnNames)
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");
        return schemas.computeIfAbsent(
                new SchemaKey("tpch:" + tableName, columnNames),
                _ -> loadTpch(tableName, columnNames));
    }

    private Schema loadTpch(String tableName, List<String> columnNames)
    {
        TpchTable<?> table = TpchTable.getTable(tableName);
        return schema(columnNames.stream()
                .map(name -> new Field(name, tpchType(table.getColumn(name).getType()), false))
                .toList());
    }

    public Schema tpcds(String tableName, List<String> columnNames)
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");
        return schemas.computeIfAbsent(
                new SchemaKey("tpcds:" + tableName, columnNames),
                _ -> loadTpcds(tableName, columnNames));
    }

    private Schema loadTpcds(String tableName, List<String> columnNames)
    {
        Table table = Table.getTable(tableName);
        return schema(columnNames.stream()
                .map(name -> new Field(name, tpcdsType(table.getColumn(name).getType()), true))
                .toList());
    }

    public Schema parquet(Path file, List<String> columnNames)
    {
        requireNonNull(file, "file is null");
        requireNonNull(columnNames, "columnNames is null");
        Path source = file.toAbsolutePath().normalize();
        return schemas.computeIfAbsent(
                new SchemaKey("parquet:" + source, columnNames),
                _ -> loadParquet(source, columnNames));
    }

    private Schema loadParquet(Path file, List<String> columnNames)
    {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            MessageType parquetSchema = reader.getFileMetaData().getSchema();
            List<Field> fields = new ArrayList<>(columnNames.size());
            for (String name : columnNames) {
                org.apache.parquet.schema.Type field = parquetSchema.getType(name);
                if (!field.isPrimitive()) {
                    throw new IllegalArgumentException("Nested benchmark column requires an explicit logical catalog binding: " + name);
                }
                fields.add(new Field(name, parquetType(field.asPrimitiveType()), field.isRepetition(org.apache.parquet.schema.Type.Repetition.OPTIONAL)));
            }
            return schema(fields);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to inspect benchmark schema from " + file, exception);
        }
    }

    private static Schema schema(List<Field> fields)
    {
        return new Schema(fields);
    }

    private TypeBinding tpchType(TpchColumnType type)
    {
        return switch (type.getBase()) {
            case IDENTIFIER -> type(BIGINT);
            case INTEGER -> type(INTEGER);
            case DATE -> type(DATE);
            case DOUBLE -> type(DOUBLE);
            case VARCHAR -> type(parameterized("varchar", type.getPrecision().orElseThrow()));
        };
    }

    private TypeBinding tpcdsType(ColumnType type)
    {
        return switch (type.getBase()) {
            case IDENTIFIER -> type(BIGINT);
            case INTEGER -> type(INTEGER);
            case DATE -> type(DATE);
            case TIME -> type(TIME);
            case DECIMAL -> type("benchmark:decimal(" + type.getPrecision().orElseThrow() + "," + type.getScale().orElseThrow() + ")");
            case CHAR -> type(parameterized("char", type.getPrecision().orElseThrow()));
            case VARCHAR -> type(parameterized("varchar", type.getPrecision().orElseThrow()));
        };
    }

    private TypeBinding parquetType(PrimitiveType type)
    {
        if (type.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation) {
            return type(VARCHAR);
        }
        if (type.getLogicalTypeAnnotation() instanceof DateLogicalTypeAnnotation) {
            return type(DATE);
        }
        if (type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation decimal) {
            return type("benchmark:decimal(" + decimal.getPrecision() + "," + decimal.getScale() + ")");
        }
        return switch (type.getPrimitiveTypeName()) {
            case BOOLEAN -> type(BOOLEAN);
            case INT32 -> type(INTEGER);
            case INT64 -> type(BIGINT);
            case FLOAT, DOUBLE -> type(DOUBLE);
            case BINARY, FIXED_LEN_BYTE_ARRAY -> type(VARCHAR);
            case INT96 -> throw new IllegalArgumentException("INT96 requires an explicit logical catalog binding");
        };
    }

    private static String parameterized(String base, long parameter)
    {
        return "benchmark:" + base + "(" + parameter + ")";
    }

    private TypeBinding type(String identity)
    {
        return types.resolve(new TypeIdentity(identity));
    }

    private record SchemaKey(String source, List<String> columns)
    {
        private SchemaKey
        {
            source = requireNonNull(source, "source is null");
            columns = List.copyOf(requireNonNull(columns, "columns is null"));
        }
    }
}
