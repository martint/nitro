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
package org.weakref.nitro.trino;

import io.trino.Session;
import io.trino.plugin.hive.HivePlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.weakref.nitro.tpcds.TpcdsParquetTables;
import org.weakref.nitro.tpcds.TpcdsQueryCatalog;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class TrinoTpcdsParquetSqlSupport
        implements AutoCloseable
{
    private final QueryRunner queryRunner;
    private final String schema;

    public TrinoTpcdsParquetSqlSupport(TpcdsParquetTables tables)
    {
        this.schema = tables.schema();
        Session session = testSessionBuilder()
                .setCatalog("hive")
                .setSchema(schema)
                .build();

        queryRunner = new StandaloneQueryRunner(session);
        queryRunner.installPlugin(new HivePlugin());
        queryRunner.createCatalog("hive", "hive", Map.of(
                "hive.metastore", "file",
                "hive.metastore.catalog.dir", tables.rootDirectory().toAbsolutePath().toString(),
                "fs.hadoop.enabled", "true",
                "hive.security", "allow-all"));
    }

    public MaterializedResult executeBenchmarkQuery(String queryId)
    {
        return executeSql(TpcdsQueryCatalog.benchmarkQuerySql(queryId, "hive", schema));
    }

    public MaterializedResult executeSql(String sql)
    {
        try {
            return queryRunner.execute(sql);
        }
        catch (RuntimeException exception) {
            throw new RuntimeException("Failed SQL: " + sql, exception);
        }
    }

    @Override
    public void close()
    {
        queryRunner.close();
    }
}
