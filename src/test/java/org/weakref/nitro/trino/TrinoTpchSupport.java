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
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.planner.Plan;
import io.trino.testing.PlanTester;
import org.weakref.nitro.tpch.TpchQueryCatalog;

import java.util.Map;

import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * An embedded Trino planner over the tpch catalog, used to extract the optimized logical plans the TPC-H
 * harness operator trees mirror (the TPC-H twin of {@link TrinoTpcdsSupport}'s explain path; execution-side
 * reference results come from an independent engine over the same parquet instead).
 */
public final class TrinoTpchSupport
        implements AutoCloseable
{
    private final PlanTester planTester;
    private final String schema;

    public TrinoTpchSupport(String schema)
    {
        this.schema = schema;
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(schema)
                .build();
        planTester = PlanTester.create(session);
        planTester.installPlugin(new TpchPlugin());
        // STANDARD naming exposes the spec's prefixed column names (l_shipdate), matching the query texts.
        planTester.createCatalog("tpch", "tpch", Map.of("tpch.column-naming", "STANDARD"));
    }

    public String explainQuery(String queryId)
    {
        return explainSql(TpchQueryCatalog.sql(queryId, "tpch", schema));
    }

    public String explainSql(String sql)
    {
        planTester.getAccessControl().checkCanExecuteQuery(planTester.getDefaultSession().getIdentity(), planTester.getDefaultSession().getQueryId());
        return planTester.inTransaction(planTester.getDefaultSession(), session -> {
            Plan plan = planTester.createPlan(session, sql);
            return textLogicalPlan(
                    plan.getRoot(),
                    planTester.getPlannerContext().getMetadata(),
                    planTester.getPlannerContext().getFunctionManager(),
                    plan.getStatsAndCosts(),
                    session,
                    0,
                    false);
        });
    }

    @Override
    public void close()
    {
        planTester.close();
    }
}
