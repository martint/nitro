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

import org.weakref.nitro.jit.ColumnEncoding;
import org.weakref.nitro.jit.Plan;
import org.weakref.nitro.jit.QueryLowering;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries.Composite;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries.Ported;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries.Stage;

import java.util.List;

/**
 * The ClickBench queries as compiled-engine lowerings, mirroring the {@link ClickBenchHitsSupport} operator
 * chains operator-for-operator (which themselves mirror the Trino plans of the SQL suite) and validated
 * byte-exact against them. Every query reads the single {@code hits} table; all its columns are NOT NULL.
 */
public final class CompiledClickBenchQueries
{
    private CompiledClickBenchQueries() {}

    /** SELECT COUNT(*) FROM hits */
    public static Ported query01()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE);
        query.count();
        return new Ported(query, List.of());
    }

    /** SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0 */
    public static Ported query02()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("AdvEngineID"));
        query.where(new Plan.Not(new Plan.Predicate("=", query.column("AdvEngineID"), new Plan.Lit(0))))
                .count();
        return new Ported(query, List.of());
    }

    /** SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits */
    public static Ported query03()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("AdvEngineID"),
                new QueryLowering.Column("ResolutionWidth"));
        query.aggregate("sum", "AdvEngineID")
                .count()
                .aggregate("avg", "ResolutionWidth");
        return new Ported(query, List.of());
    }

    /** SELECT AVG(UserID) FROM hits */
    public static Ported query04()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("UserID"));
        query.aggregate("avg", "UserID");
        return new Ported(query, List.of());
    }

    /** SELECT COUNT(DISTINCT UserID) FROM hits: the distinct user set as a stage, counted by the main. */
    public static Composite query05()
    {
        QueryLowering distinct = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("UserID"));
        distinct.groupBy("UserID")
                .count();
        distinct.select(new Plan.Col(0));

        QueryLowering main = QueryLowering.scan("cb05_users",
                new QueryLowering.Column("u_user"));
        main.count();

        return new Composite(List.of(new Stage(distinct, "cb05_users")), main, List.of());
    }

    /** SELECT MIN(EventDate), MAX(EventDate) FROM hits */
    public static Ported query07()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("EventDate"));
        query.aggregate("min", "EventDate")
                .aggregate("max", "EventDate");
        return new Ported(query, List.of());
    }

    /** SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY 1 ORDER BY 2 DESC LIMIT 10 */
    public static Ported query08()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("AdvEngineID"));
        query.where(new Plan.Not(new Plan.Predicate("=", query.column("AdvEngineID"), new Plan.Lit(0))))
                .groupBy("AdvEngineID")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));
        return new Ported(query, List.of());
    }

    /** SELECT UserID, COUNT(*) FROM hits GROUP BY 1 ORDER BY 2 DESC LIMIT 10 */
    public static Ported query16()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("UserID"));
        query.groupBy("UserID")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));
        return new Ported(query, List.of());
    }

    /**
     * SELECT SearchEngineID, ClientIP, COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth) FROM hits
     * WHERE SearchPhrase <> '' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10
     */
    public static Ported query31()
    {
        return topGroupedCountSumAvg("SearchEngineID", true);
    }

    /** Q31's sibling keyed on (WatchID, ClientIP). */
    public static Ported query32()
    {
        return topGroupedCountSumAvg("WatchID", true);
    }

    /** Q32 without the search-phrase filter. */
    public static Ported query33()
    {
        return topGroupedCountSumAvg("WatchID", false);
    }

    private static Ported topGroupedCountSumAvg(String firstKey, boolean filterSearchPhrase)
    {
        QueryLowering query = filterSearchPhrase
                ? QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                        new QueryLowering.Column(firstKey),
                        new QueryLowering.Column("ClientIP"),
                        new QueryLowering.Column("IsRefresh"),
                        new QueryLowering.Column("ResolutionWidth"),
                        new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false))
                : QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                        new QueryLowering.Column(firstKey),
                        new QueryLowering.Column("ClientIP"),
                        new QueryLowering.Column("IsRefresh"),
                        new QueryLowering.Column("ResolutionWidth"));
        if (filterSearchPhrase) {
            query.where(new Plan.StringMatch(query.position("SearchPhrase"), List.of(""), true));
        }
        query.groupBy(firstKey, "ClientIP")
                .count()
                .aggregate("sum", "IsRefresh")
                .aggregate("avg", "ResolutionWidth");
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(2, true)), 10));
        return new Ported(query, List.of());
    }

    /**
     * SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) FROM hits GROUP BY 1, 2, 3, 4
     * ORDER BY 5 DESC LIMIT 10: the offset projection as a stage (the harness's ProjectOperator), grouped by
     * the main.
     */
    public static Composite query36()
    {
        QueryLowering offsets = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("ClientIP"));
        offsets.select(
                offsets.column("ClientIP"),
                new Plan.Bin("+", offsets.column("ClientIP"), new Plan.Lit(-1)),
                new Plan.Bin("+", offsets.column("ClientIP"), new Plan.Lit(-2)),
                new Plan.Bin("+", offsets.column("ClientIP"), new Plan.Lit(-3)));

        QueryLowering main = QueryLowering.scan("cb36_offsets",
                new QueryLowering.Column("ip0"),
                new QueryLowering.Column("ip1"),
                new QueryLowering.Column("ip2"),
                new QueryLowering.Column("ip3"));
        main.groupBy("ip0", "ip1", "ip2", "ip3")
                .count();
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(4, true)), 10));

        return new Composite(List.of(new Stage(offsets, "cb36_offsets")), main, List.of());
    }
}
