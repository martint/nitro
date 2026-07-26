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
package org.weakref.nitro.tpcds;

import org.weakref.nitro.benchmark.BenchmarkSchemaRegistry;
import org.weakref.nitro.benchmark.BenchmarkTypeRegistry;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.operator.source.compatibility.parquet.NitroParquetScanResources;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public final class TpcdsParquetTables
        implements ParquetTables
{
    public static final String TPCDS_PARQUET_PATH_PROPERTY = "nitro.tpcds.parquet.path";
    public static final String TPCDS_PARQUET_SCHEMA_PROPERTY = "nitro.tpcds.parquet.schema";

    private static final Path DEFAULT_TPCDS_PARQUET_ROOT = Path.of(System.getProperty("user.home"), "tmp", "tpcds-parquet-sf10");

    private final Path rootDirectory;
    private final String schema;
    private final NitroParquetScanResources scanResources = NitroParquetScanResources.createDefault();
    private final BenchmarkSchemaRegistry schemas = new BenchmarkSchemaRegistry(new BenchmarkTypeRegistry());
    private final Map<String, List<Path>> tableFiles = new HashMap<>();
    private final AtomicInteger scanCount = new AtomicInteger();

    private TpcdsParquetTables(Path rootDirectory, String schema)
    {
        this.rootDirectory = requireNonNull(rootDirectory, "rootDirectory is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    public static Optional<TpcdsParquetTables> actualIfPresent()
    {
        return actualIfPresent(configuredSchema());
    }

    public static Optional<TpcdsParquetTables> actualIfPresent(String schema)
    {
        Path rootDirectory = configuredRootDirectory();
        Path schemaDirectory = rootDirectory.resolve(schema);
        if (!Files.isDirectory(schemaDirectory)) {
            return Optional.empty();
        }
        return Optional.of(new TpcdsParquetTables(rootDirectory, schema));
    }

    public static TpcdsParquetTables requiredActual()
    {
        return requiredActual(configuredSchema());
    }

    public static TpcdsParquetTables requiredActual(String schema)
    {
        return actualIfPresent(schema)
                .orElseThrow(() -> new IllegalStateException("Set -" + "D" + TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet and ensure schema " + schema + " exists"));
    }

    public static String configuredSchema()
    {
        return System.getProperty(TPCDS_PARQUET_SCHEMA_PROPERTY, "sf10");
    }

    public static Path configuredRootDirectory()
    {
        String configured = System.getProperty(TPCDS_PARQUET_PATH_PROPERTY);
        if (configured == null || configured.isBlank()) {
            return DEFAULT_TPCDS_PARQUET_ROOT;
        }
        return Path.of(configured);
    }

    public String schema()
    {
        return schema;
    }

    public Path rootDirectory()
    {
        return rootDirectory;
    }

    public Path schemaDirectory()
    {
        return rootDirectory.resolve(schema);
    }

    public NitroParquetScanResources scanResources()
    {
        return scanResources;
    }

    Schema tableSchema(String tableName, List<String> columnNames)
    {
        return schemas.tpcds(tableName, columnNames);
    }

    void recordScan()
    {
        scanCount.incrementAndGet();
    }

    int scanCount()
    {
        return scanCount.get();
    }

    void resetScanCount()
    {
        scanCount.set(0);
    }

    public Path tableDirectory(String tableName)
    {
        return schemaDirectory().resolve(tableName);
    }

    @Override
    public synchronized List<Path> tableFiles(String tableName)
    {
        List<Path> files = tableFiles.get(tableName);
        if (files == null) {
            files = loadTableFiles(tableName);
            tableFiles.put(tableName, files);
        }
        return files;
    }

    private List<Path> loadTableFiles(String tableName)
    {
        Path tableDirectory = tableDirectory(tableName);
        if (!Files.isDirectory(tableDirectory)) {
            throw new IllegalArgumentException("TPC-DS parquet table directory does not exist: " + tableDirectory);
        }

        try (var files = Files.list(tableDirectory)) {
            List<Path> result = files
                    .filter(Files::isRegularFile)
                    .filter(path -> !path.getFileName().toString().startsWith("."))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList();
            if (result.isEmpty()) {
                throw new IllegalArgumentException("No parquet data files found in " + tableDirectory);
            }
            return result;
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to list TPC-DS parquet files in " + tableDirectory, exception);
        }
    }
}
