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

import org.weakref.nitro.core.connector.Connector;
import org.weakref.nitro.core.connector.ConnectorSplit;
import org.weakref.nitro.core.connector.ConnectorSplitConfiguration;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;

import java.nio.file.Path;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/// Connector-owned native Parquet resources and source construction.
public final class NitroParquetConnector
        implements Connector
{
    private final NitroParquetScanResources resources;
    private volatile boolean closed;

    public NitroParquetConnector(NitroParquetScanResources resources)
    {
        this.resources = requireNonNull(resources, "resources is null");
    }

    @Override
    public ConnectorSplit split(ConnectorSplitConfiguration configuration)
    {
        requireOpen();
        Map<String, String> properties = requireNonNull(configuration, "configuration is null").properties();
        if (!properties.keySet().equals(java.util.Set.of("path"))) {
            throw new IllegalArgumentException("Parquet split requires exactly the path property");
        }
        return new NitroParquetSplit(Path.of(properties.get("path")));
    }

    @Override
    public BatchSource open(ConnectorSplit split, Schema schema, Allocator allocator)
    {
        requireOpen();
        if (!(requireNonNull(split, "split is null") instanceof NitroParquetSplit parquetSplit)) {
            throw new IllegalArgumentException("split is not owned by the Nitro Parquet connector");
        }
        return new NitroParquetBatchSource(
                resources,
                requireNonNull(allocator, "allocator is null"),
                parquetSplit.paths(),
                requireNonNull(schema, "schema is null"));
    }

    @Override
    public void close()
    {
        closed = true;
    }

    private void requireOpen()
    {
        if (closed) {
            throw new IllegalStateException("Parquet connector is closed");
        }
    }
}
