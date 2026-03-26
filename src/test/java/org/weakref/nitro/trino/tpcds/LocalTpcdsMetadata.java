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
package org.weakref.nitro.trino.tpcds;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;
import io.trino.tpcds.Table;
import io.trino.tpcds.column.Column;
import io.trino.tpcds.column.ColumnType;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.util.Objects.requireNonNull;

public final class LocalTpcdsMetadata
        implements ConnectorMetadata
{
    private static final String TINY_SCHEMA_NAME = "tiny";
    private static final double TINY_SCALE_FACTOR = 0.01;

    private final Set<String> tableNames = Table.getBaseTables().stream()
            .map(Table::getName)
            .map(name -> name.toLowerCase(Locale.ENGLISH))
            .collect(Collectors.toUnmodifiableSet());

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return schemaNameToScaleFactor(schemaName) > 0;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(TINY_SCHEMA_NAME, "sf1", "sf10", "sf100", "sf300", "sf1000", "sf3000", "sf10000", "sf30000", "sf100000");
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        if (!tableNames.contains(tableName.getTableName())) {
            return null;
        }
        double scaleFactor = schemaNameToScaleFactor(tableName.getSchemaName());
        if (scaleFactor <= 0) {
            return null;
        }
        return new LocalTpcdsTableHandle(tableName.getTableName(), scaleFactor);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LocalTpcdsTableHandle tpcdsTableHandle = (LocalTpcdsTableHandle) tableHandle;
        Table table = Table.getTable(tpcdsTableHandle.tableName());
        return tableMetadata(scaleFactorSchemaName(tpcdsTableHandle.scaleFactor()), table);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return TableStatistics.empty();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> handles = ImmutableMap.builder();
        for (ColumnMetadata column : getTableMetadata(session, tableHandle).getColumns()) {
            handles.put(column.getName(), new LocalTpcdsColumnHandle(column.getName(), column.getType()));
        }
        return handles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return new ColumnMetadata(((LocalTpcdsColumnHandle) columnHandle).columnName(), ((LocalTpcdsColumnHandle) columnHandle).type());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (String schemaName : schemaNames(prefix.getSchema())) {
            for (Table table : Table.getBaseTables()) {
                if (prefix.getTable().map(table.getName()::equals).orElse(true)) {
                    ConnectorTableMetadata tableMetadata = tableMetadata(schemaName, table);
                    columns.put(tableMetadata.getTable(), tableMetadata.getColumns());
                }
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> filterSchema)
    {
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (String schemaName : schemaNames(filterSchema)) {
            for (Table table : Table.getBaseTables()) {
                tables.add(new SchemaTableName(schemaName, table.getName()));
            }
        }
        return tables.build();
    }

    private static List<String> schemaNames(Optional<String> schemaName)
    {
        if (schemaName.isEmpty()) {
            return ImmutableList.of(TINY_SCHEMA_NAME, "sf1", "sf10", "sf100", "sf300", "sf1000", "sf3000", "sf10000", "sf30000", "sf100000");
        }
        String name = schemaName.orElseThrow();
        return schemaNameToScaleFactor(name) > 0 ? ImmutableList.of(name) : ImmutableList.of();
    }

    private static ConnectorTableMetadata tableMetadata(String schemaName, Table table)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (Column column : table.getColumns()) {
            columns.add(new ColumnMetadata(column.getName(), trinoType(column.getType())));
        }
        return new ConnectorTableMetadata(new SchemaTableName(schemaName, table.getName()), columns.build());
    }

    private static String scaleFactorSchemaName(double scaleFactor)
    {
        if (scaleFactor == TINY_SCALE_FACTOR) {
            return TINY_SCHEMA_NAME;
        }
        return "sf" + scaleFactor;
    }

    private static double schemaNameToScaleFactor(String schemaName)
    {
        if (TINY_SCHEMA_NAME.equals(schemaName)) {
            return TINY_SCALE_FACTOR;
        }
        if (!schemaName.startsWith("sf")) {
            return -1;
        }
        try {
            return Double.parseDouble(schemaName.substring(2));
        }
        catch (RuntimeException ignored) {
            return -1;
        }
    }

    public static Type trinoType(ColumnType type)
    {
        requireNonNull(type, "type is null");
        return switch (type.getBase()) {
            case IDENTIFIER -> BigintType.BIGINT;
            case INTEGER -> IntegerType.INTEGER;
            case DATE -> DateType.DATE;
            case DECIMAL -> createDecimalType(type.getPrecision().orElseThrow(), type.getScale().orElseThrow());
            case CHAR -> createCharType(type.getPrecision().orElseThrow());
            case VARCHAR -> createVarcharType(type.getPrecision().orElseThrow());
            case TIME -> TimeType.TIME_MILLIS;
        };
    }
}
