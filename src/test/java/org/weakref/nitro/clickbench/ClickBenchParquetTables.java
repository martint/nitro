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
package org.weakref.nitro.clickbench;

import org.weakref.nitro.tpcds.ParquetTables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The ClickBench data layout for the compiled engine: one logical {@code hits} table whose rows are split across
 * the parquet files of a single directory (the official {@code athena_partitioned} form), resolved through the
 * same lookup the operator harness uses.
 */
public final class ClickBenchParquetTables
        implements ParquetTables
{
    public static final String HITS_TABLE = "hits";

    private final Path directory;

    private ClickBenchParquetTables(Path directory)
    {
        this.directory = requireNonNull(directory, "directory is null");
    }

    public static Optional<ClickBenchParquetTables> actualIfPresent()
    {
        return ClickBenchHitsSupport.actualHitsDirectoryIfPresent().map(ClickBenchParquetTables::new);
    }

    public static ClickBenchParquetTables forDirectory(Path directory)
    {
        return new ClickBenchParquetTables(directory);
    }

    public Path directory()
    {
        return directory;
    }

    @Override
    public List<Path> tableFiles(String tableName)
    {
        if (!HITS_TABLE.equals(tableName)) {
            throw new IllegalArgumentException("ClickBench has a single table, hits; got: " + tableName);
        }
        if (!Files.isDirectory(directory)) {
            // A single-file layout (e.g. the synthetic fixture) is one "split".
            return List.of(directory);
        }
        try (var files = Files.list(directory)) {
            List<Path> result = files
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList();
            if (result.isEmpty()) {
                throw new IllegalArgumentException("No parquet data files found in " + directory);
            }
            return result;
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to list ClickBench parquet files in " + directory, exception);
        }
    }
}
