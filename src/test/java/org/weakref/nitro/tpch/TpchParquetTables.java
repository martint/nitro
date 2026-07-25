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
package org.weakref.nitro.tpch;

import org.weakref.nitro.operator.source.compatibility.parquet.NitroParquetScanResources;
import org.weakref.nitro.tpcds.ParquetTables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class TpchParquetTables
        implements ParquetTables
{
    public static final String TPCH_PARQUET_PATH_PROPERTY = "nitro.tpch.parquet.path";
    public static final String TPCH_PARQUET_SCHEMA_PROPERTY = "nitro.tpch.parquet.schema";

    private static final Path DEFAULT_TPCH_PARQUET_ROOT = Path.of(System.getProperty("user.home"), "tmp", "tpch-parquet-sf10");

    private final Path rootDirectory;
    private final String schema;
    private final NitroParquetScanResources scanResources = new NitroParquetScanResources();

    private TpchParquetTables(Path rootDirectory, String schema)
    {
        this.rootDirectory = requireNonNull(rootDirectory, "rootDirectory is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    public static Optional<TpchParquetTables> actualIfPresent()
    {
        Path rootDirectory = configuredRootDirectory();
        String schema = configuredSchema();
        if (!Files.isDirectory(rootDirectory.resolve(schema))) {
            return Optional.empty();
        }
        return Optional.of(new TpchParquetTables(rootDirectory, schema));
    }

    public static TpchParquetTables requiredActual()
    {
        return actualIfPresent()
                .orElseThrow(() -> new IllegalStateException("Set -" + "D" + TPCH_PARQUET_PATH_PROPERTY + "=/path/to/tpch-parquet and ensure schema " + configuredSchema() + " exists"));
    }

    public static String configuredSchema()
    {
        return System.getProperty(TPCH_PARQUET_SCHEMA_PROPERTY, "sf10");
    }

    public static Path configuredRootDirectory()
    {
        String configured = System.getProperty(TPCH_PARQUET_PATH_PROPERTY);
        if (configured == null || configured.isBlank()) {
            return DEFAULT_TPCH_PARQUET_ROOT;
        }
        return Path.of(configured);
    }

    public String schema()
    {
        return schema;
    }

    public Path schemaDirectory()
    {
        return rootDirectory.resolve(schema);
    }

    public NitroParquetScanResources scanResources()
    {
        return scanResources;
    }

    @Override
    public boolean streamOnly(String tableName)
    {
        // The compiled engine streams every TPC-H probe: lineitem and orders are too large for the eager
        // ordered-dictionary drain, and every string consumer here needs id equality only (group keys and
        // predicates; value ORDER BY uses the streamed ordering-dictionary capture).
        return true;
    }

    @Override
    public List<Path> tableFiles(String tableName)
    {
        Path tableDirectory = schemaDirectory().resolve(tableName);
        if (!Files.isDirectory(tableDirectory)) {
            throw new IllegalArgumentException("TPC-H parquet table directory does not exist: " + tableDirectory);
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
            throw new UncheckedIOException("Unable to list TPC-H parquet files in " + tableDirectory, exception);
        }
    }
}
