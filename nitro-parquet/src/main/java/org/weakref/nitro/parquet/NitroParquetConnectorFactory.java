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
import org.weakref.nitro.core.connector.ConnectorConfiguration;
import org.weakref.nitro.core.connector.ConnectorFactory;

import static java.util.Objects.requireNonNull;

/// Standalone composition adapter for the native Nitro Parquet connector.
public final class NitroParquetConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String name()
    {
        return "parquet";
    }

    @Override
    public Connector create(ConnectorConfiguration configuration)
    {
        requireNonNull(configuration, "configuration is null");
        if (!configuration.properties().isEmpty()) {
            throw new IllegalArgumentException("unsupported Parquet connector properties: " + configuration.properties().keySet());
        }
        return new NitroParquetConnector(NitroParquetScanResources.createDefault());
    }
}
