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

import java.nio.file.Path;
import java.util.List;

/**
 * Resolves a logical table name to its parquet data files -- the only contact surface between the compiled
 * engine's loader and a benchmark suite's data layout (TPC-DS's schema/table directories, ClickBench's single
 * split-file hits table).
 */
public interface ParquetTables
{
    List<Path> tableFiles(String tableName);

    /**
     * Is {@code tableName} too large to drain eagerly? A pipeline stage normally drains a string-carrying probe
     * up front (the eager load builds ORDERED dictionaries, the contract id-comparing consumers rely on); a
     * stream-only table is instead streamed through the per-query global intern (append-ordered ids), which
     * suits id-equality consumers (grouping, filters) but not id-order ones.
     */
    default boolean streamOnly(String tableName)
    {
        return false;
    }
}
