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
package org.weakref.nitro.connector;

import io.airlift.compress.v3.Decompressor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.weakref.nitro.core.connector.Connector;
import org.weakref.nitro.core.connector.ConnectorConfiguration;
import org.weakref.nitro.core.connector.ConnectorFactory;
import org.weakref.nitro.core.connector.ConnectorSplit;
import org.weakref.nitro.core.connector.ConnectorSplitConfiguration;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.parquet.NitroParquetConnectorFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestIsolatedConnectorLoader
{
    @TempDir
    Path tempDirectory;

    @Test
    void testLoadsAndRunsParquetConnectorThroughSpiOnly()
            throws IOException, ReflectiveOperationException
    {
        Path file = writeParquet();
        List<URL> connectorClassPath = classPath(
                NitroParquetConnectorFactory.class,
                Class.forName("com.google.common.base.Preconditions"),
                Decompressor.class,
                RowGroup.class,
                Class.forName("javax.annotation.Generated"),
                Class.forName("org.slf4j.LoggerFactory"));

        try (IsolatedConnectorLoader loader = new IsolatedConnectorLoader(connectorClassPath);
                AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources)) {
            ConnectorFactory factory = loader.load("parquet");
            ClassLoader providerLoader = factory.getClass().getClassLoader();
            assertThat(providerLoader).isNotSameAs(getClass().getClassLoader());
            assertThat(factory).isInstanceOf(ConnectorFactory.class);
            assertThatThrownBy(() -> providerLoader.loadClass("org.weakref.nitro.operator.Operator"))
                    .isInstanceOf(ClassNotFoundException.class);

            Schema schema = Schema.unspecified(List.of("x"));
            try (Connector connector = factory.create(ConnectorConfiguration.empty())) {
                ConnectorSplit split = connector.split(new ConnectorSplitConfiguration(
                        Map.of("path", file.toString())));
                try (BatchSource source = connector.open(split, schema, allocator)) {
                    assertThat(source.getClass().getClassLoader()).isSameAs(providerLoader);
                    SourcePoll.Ready ready = (SourcePoll.Ready) source.poll();
                    try (var batch = ready.batch()) {
                        assertThat(((I64Vector) batch.column(0).borrow(Stream.VALUES)).values())
                                .containsExactly(41, 42);
                    }
                    assertThat(source.poll()).isSameAs(SourcePoll.Finished.FINISHED);
                }
            }
        }
    }

    private Path writeParquet()
            throws IOException
    {
        Path file = tempDirectory.resolve("isolated.parquet");
        MessageType schema = Types.buildMessage()
                .required(INT64).named("x")
                .named("isolated");
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .build()) {
            writer.write(groups.newGroup().append("x", 41L));
            writer.write(groups.newGroup().append("x", 42L));
        }
        return file;
    }

    private static List<URL> classPath(Class<?>... classes)
    {
        Set<URL> locations = new LinkedHashSet<>();
        for (Class<?> type : classes) {
            locations.add(type.getProtectionDomain().getCodeSource().getLocation());
        }
        return List.copyOf(locations);
    }
}
