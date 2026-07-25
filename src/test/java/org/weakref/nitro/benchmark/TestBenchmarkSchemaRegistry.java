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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.weakref.nitro.clickbench.ClickBenchHitsSupport;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestBenchmarkSchemaRegistry
{
    private final BenchmarkSchemaRegistry schemas = new BenchmarkSchemaRegistry(new BenchmarkTypeRegistry());

    @Test
    void testTypeBindingsDeclareConnectorVectorContracts()
    {
        BenchmarkTypeRegistry types = new BenchmarkTypeRegistry();

        assertThat(types.resolve(new TypeIdentity(BenchmarkTypeRegistry.BIGINT)).supportedVectorTypes())
                .contains(I64Vector.class);
        assertThat(types.resolve(new TypeIdentity(BenchmarkTypeRegistry.INTEGER)).supportedVectorTypes())
                .contains(I32Vector.class);
        assertThat(types.resolve(new TypeIdentity(BenchmarkTypeRegistry.DOUBLE)).supportedVectorTypes())
                .contains(F64Vector.class);
        assertThat(types.resolve(new TypeIdentity(BenchmarkTypeRegistry.VARCHAR)).supportedVectorTypes())
                .contains(BinaryVector.class);
    }

    @Test
    void testTpchSchemaUsesCatalogBindings()
    {
        Schema schema = schemas.tpch(
                "lineitem",
                List.of("l_orderkey", "l_quantity", "l_shipdate", "l_shipmode"));

        assertThat(schema.fields()).extracting(field -> field.name().orElseThrow())
                .containsExactly("l_orderkey", "l_quantity", "l_shipdate", "l_shipmode");
        assertThat(schema.fields()).extracting(field -> field.type().identity().value())
                .containsExactly(
                        BenchmarkTypeRegistry.BIGINT,
                        BenchmarkTypeRegistry.DOUBLE,
                        BenchmarkTypeRegistry.DATE,
                        "benchmark:varchar(10)");
        assertThat(schema.fields()).allMatch(field -> !field.nullable());
    }

    @Test
    void testTpcdsSchemaPreservesParameterizedLogicalTypes()
    {
        Schema schema = schemas.tpcds(
                "store_sales",
                List.of("ss_sold_date_sk", "ss_quantity", "ss_sales_price"));

        assertThat(schema.fields()).extracting(field -> field.type().identity().value())
                .containsExactly(
                        BenchmarkTypeRegistry.BIGINT,
                        BenchmarkTypeRegistry.INTEGER,
                        "benchmark:decimal(7,2)");
        assertThat(schema.fields()).allMatch(field -> field.nullable());
    }

    @Test
    void testParquetAdapterTerminatesAtNitroSchema(@TempDir Path directory)
            throws Exception
    {
        Path file = ClickBenchHitsSupport.writeHitsFixture(directory.resolve("hits.parquet"), 1);
        Schema schema = schemas.parquet(
                file,
                List.of("EventTime", "EventDate", "URL", "DontCountHits"));

        assertThat(schema.fields()).extracting(field -> field.type().identity().value())
                .containsExactly(
                        BenchmarkTypeRegistry.BIGINT,
                        BenchmarkTypeRegistry.INTEGER,
                        BenchmarkTypeRegistry.VARCHAR,
                        BenchmarkTypeRegistry.INTEGER);
        assertThat(schema.fields()).allMatch(field -> !field.nullable());

        Schema second = schemas.parquet(file, List.of("WatchID"));
        assertThat(second.field(0).type()).isSameAs(schema.field(0).type());

        assertThat(schemas.parquet(
                file,
                List.of("EventTime", "EventDate", "URL", "DontCountHits")))
                .isSameAs(schema);
    }
}
