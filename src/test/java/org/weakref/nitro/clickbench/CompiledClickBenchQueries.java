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
import org.weakref.nitro.tpcds.CompiledTpcdsQueries;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries.Composite;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries.Ported;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries.Stage;

import java.time.LocalDate;
import java.util.List;

/**
 * The ClickBench queries as compiled-engine lowerings, mirroring the {@link ClickBenchHitsSupport} operator
 * chains operator-for-operator (which themselves mirror the Trino plans of the SQL suite) and validated
 * byte-exact against them. Every query reads the single {@code hits} table; all its columns are NOT NULL.
 */
public final class CompiledClickBenchQueries
{
    private static final LocalDate JULY_2013_START = LocalDate.of(2013, 7, 1);
    private static final LocalDate JULY_2013_END = LocalDate.of(2013, 8, 1);

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

    /**
     * SELECT RegionID, COUNT(DISTINCT UserID) FROM hits GROUP BY 1 ORDER BY 2 DESC LIMIT 10: the distinct
     * (RegionID, UserID) pairs as a stage (the harness's MarkDistinctOperator), counted per region by the main.
     */
    public static Composite query09()
    {
        QueryLowering pairs = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("RegionID"),
                new QueryLowering.Column("UserID"));
        pairs.groupBy("RegionID", "UserID")
                .count();
        pairs.select(new Plan.Col(0), new Plan.Col(1));

        QueryLowering main = QueryLowering.scan("cb09_pairs",
                new QueryLowering.Column("p_region"),
                new QueryLowering.Column("p_user"));
        main.groupBy("p_region")
                .count();
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));

        return new Composite(List.of(new Stage(pairs, "cb09_pairs")), main, List.of());
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

    /** SELECT COUNT(DISTINCT SearchPhrase) FROM hits: the distinct phrases as a stage, counted by the main. */
    public static Composite query06()
    {
        QueryLowering distinct = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false));
        distinct.groupBy("SearchPhrase")
                .count();
        distinct.select(new Plan.Col(0));

        QueryLowering main = QueryLowering.scan("cb06_phrases",
                new QueryLowering.Column("p_phrase", ColumnEncoding.STRING, false));
        main.count();

        return new Composite(
                List.of(new Stage(distinct, "cb06_phrases", List.of(new CompiledTpcdsQueries.DictRef(0, 0, 0)))),
                main,
                List.of());
    }

    /**
     * SELECT MobilePhoneModel, COUNT(DISTINCT UserID) FROM hits WHERE MobilePhoneModel <> '' GROUP BY 1
     * ORDER BY 2 DESC LIMIT 10: the distinct (model, user) pairs as a stage, counted per model by the main.
     */
    public static Composite query11()
    {
        QueryLowering pairs = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("MobilePhoneModel", ColumnEncoding.STRING, false),
                new QueryLowering.Column("UserID"));
        pairs.where(new Plan.StringMatch(pairs.position("MobilePhoneModel"), List.of(""), true))
                .groupBy("MobilePhoneModel", "UserID")
                .count();
        pairs.select(new Plan.Col(0), new Plan.Col(1));

        QueryLowering main = QueryLowering.scan("cb11_pairs",
                new QueryLowering.Column("p_model", ColumnEncoding.STRING, false),
                new QueryLowering.Column("p_user"));
        main.groupBy("p_model")
                .count();
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));

        return new Composite(
                List.of(new Stage(pairs, "cb11_pairs", List.of(new CompiledTpcdsQueries.DictRef(0, 0, 0)))),
                main,
                List.of(new CompiledTpcdsQueries.DictRef(0, 0, 0)));
    }

    /**
     * SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) FROM hits WHERE MobilePhoneModel <> ''
     * GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10
     */
    public static Composite query12()
    {
        QueryLowering triples = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("MobilePhone"),
                new QueryLowering.Column("MobilePhoneModel", ColumnEncoding.STRING, false),
                new QueryLowering.Column("UserID"));
        triples.where(new Plan.StringMatch(triples.position("MobilePhoneModel"), List.of(""), true))
                .groupBy("MobilePhone", "MobilePhoneModel", "UserID")
                .count();
        triples.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2));

        QueryLowering main = QueryLowering.scan("cb12_triples",
                new QueryLowering.Column("p_phone"),
                new QueryLowering.Column("p_model", ColumnEncoding.STRING, false),
                new QueryLowering.Column("p_user"));
        main.groupBy("p_phone", "p_model")
                .count();
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(2, true)), 10));

        return new Composite(
                List.of(new Stage(triples, "cb12_triples", List.of(new CompiledTpcdsQueries.DictRef(1, 0, 1)))),
                main,
                List.of(new CompiledTpcdsQueries.DictRef(1, 0, 1)));
    }

    /** SELECT SearchPhrase, COUNT(*) FROM hits WHERE SearchPhrase <> '' GROUP BY 1 ORDER BY 2 DESC LIMIT 10 */
    public static Ported query13()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false));
        query.where(new Plan.StringMatch(query.position("SearchPhrase"), List.of(""), true))
                .groupBy("SearchPhrase")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));
        return new Ported(query, 0, 0, 0);
    }

    /**
     * SELECT SearchPhrase, COUNT(DISTINCT UserID) FROM hits WHERE SearchPhrase <> '' GROUP BY 1 ORDER BY 2
     * DESC LIMIT 10: the distinct (phrase, user) pairs as a stage, counted per phrase by the main.
     */
    public static Composite query14()
    {
        QueryLowering pairs = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false),
                new QueryLowering.Column("UserID"));
        pairs.where(new Plan.StringMatch(pairs.position("SearchPhrase"), List.of(""), true))
                .groupBy("SearchPhrase", "UserID")
                .count();
        pairs.select(new Plan.Col(0), new Plan.Col(1));

        QueryLowering main = QueryLowering.scan("cb14_pairs",
                new QueryLowering.Column("p_phrase", ColumnEncoding.STRING, false),
                new QueryLowering.Column("p_user"));
        main.groupBy("p_phrase")
                .count();
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));

        return new Composite(
                List.of(new Stage(pairs, "cb14_pairs", List.of(new CompiledTpcdsQueries.DictRef(0, 0, 0)))),
                main,
                List.of(new CompiledTpcdsQueries.DictRef(0, 0, 0)));
    }

    /**
     * SELECT SearchEngineID, SearchPhrase, COUNT(*) FROM hits WHERE SearchPhrase <> '' GROUP BY 1, 2
     * ORDER BY 3 DESC LIMIT 10
     */
    public static Ported query15()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("SearchEngineID"),
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false));
        query.where(new Plan.StringMatch(query.position("SearchPhrase"), List.of(""), true))
                .groupBy("SearchEngineID", "SearchPhrase")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(2, true)), 10));
        return new Ported(query, 1, 0, 1);
    }

    /** SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10 */
    public static Ported query17()
    {
        QueryLowering query = userPhraseCounts();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(2, true)), 10));
        return new Ported(query, 1, 0, 1);
    }

    /**
     * SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY 1, 2 LIMIT 10: a LIMIT with no ORDER BY,
     * so ANY ten groups satisfy the query -- the engines legitimately emit different rows.
     */
    public static Ported query18()
    {
        QueryLowering query = userPhraseCounts();
        query.orderBy(new Plan.Ordering(List.of(), 10));
        return new Ported(query, 1, 0, 1);
    }

    /** q18 without its LIMIT: the full grouping, used as the validation oracle for q18's underdetermined page. */
    static Ported query18Unlimited()
    {
        return new Ported(userPhraseCounts(), 1, 0, 1);
    }

    private static QueryLowering userPhraseCounts()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("UserID"),
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false));
        query.groupBy("UserID", "SearchPhrase")
                .count();
        return query;
    }

    /**
     * SELECT UserID, minute(EventTime), SearchPhrase, COUNT(*) FROM hits GROUP BY 1, 2, 3 ORDER BY 4 DESC
     * LIMIT 10: the minute is EventTime / 60 % 60, the harness's projection folded into a computed group key.
     */
    public static Ported query19()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("UserID"),
                new QueryLowering.Column("EventTime"),
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false));
        query.groupBy(
                query.column("UserID"),
                new Plan.Bin("%", new Plan.Bin("/", query.column("EventTime"), new Plan.Lit(60)), new Plan.Lit(60)),
                query.column("SearchPhrase"))
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(3, true)), 10));
        return new Ported(query, 2, 0, 2);
    }

    /** SELECT UserID FROM hits WHERE UserID = 435090932899640449 */
    public static Ported query20()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("UserID"));
        query.where(new Plan.Predicate("=", query.column("UserID"), new Plan.Lit(ClickBenchHitsSupport.QUERY20_USER_ID)))
                .select(query.column("UserID"));
        return new Ported(query, List.of());
    }

    /** SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%' */
    public static Ported query21()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("URL", ColumnEncoding.STRING, false));
        query.where(new Plan.LikeMatch(query.position("URL"), "%google%", false))
                .count();
        return new Ported(query, List.of());
    }

    /**
     * SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10: the TopN over
     * (EventTime, SearchPhrase) as a stage (the harness's TopNOperator), the phrase projected by the main.
     */
    public static Composite query25()
    {
        return timeOrderedPhrases("cb25_top", List.of(new Plan.SortKey(0, false)));
    }

    /** q25 with the phrase as secondary sort key: ORDER BY EventTime, SearchPhrase LIMIT 10. */
    public static Composite query27()
    {
        return timeOrderedPhrases("cb27_top", List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)));
    }

    private static Composite timeOrderedPhrases(String virtualName, List<Plan.SortKey> keys)
    {
        QueryLowering top = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("EventTime"),
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false));
        top.where(new Plan.StringMatch(top.position("SearchPhrase"), List.of(""), true))
                .select(top.column("EventTime"), top.column("SearchPhrase"));
        top.orderBy(new Plan.Ordering(keys, 10));

        QueryLowering main = QueryLowering.scan(virtualName,
                new QueryLowering.Column("t_time"),
                new QueryLowering.Column("t_phrase", ColumnEncoding.STRING, false));
        main.select(main.column("t_phrase"));

        return new Composite(
                List.of(new Stage(top, virtualName, List.of(new CompiledTpcdsQueries.DictRef(1, 0, 1)))),
                main,
                List.of(new CompiledTpcdsQueries.DictRef(0, 0, 1)));
    }

    /** SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10 */
    public static Ported query26()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false));
        query.where(new Plan.StringMatch(query.position("SearchPhrase"), List.of(""), true))
                .select(query.column("SearchPhrase"));
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 10));
        return new Ported(query, 0, 0, 0);
    }

    /**
     * SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY 1
     * HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25
     */
    public static Ported query28()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("CounterID"),
                new QueryLowering.Column("URL", ColumnEncoding.STRING, false));
        query.where(new Plan.StringMatch(query.position("URL"), List.of(""), true))
                .groupBy("CounterID")
                .aggregate("avg", new Plan.Call("length_utf8", List.of(query.column("URL"))))
                .count();
        query.having(new Plan.Predicate(">", new Plan.Col(2), new Plan.Lit(100_000)));
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 25));
        return new Ported(query, List.of());
    }

    /** SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ..., SUM(ResolutionWidth + 89) FROM hits */
    public static Ported query30()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("ResolutionWidth"));
        query.aggregate("sum", "ResolutionWidth");
        for (int offset = 1; offset < 90; offset++) {
            query.aggregate("sum", new Plan.Bin("+", query.column("ResolutionWidth"), new Plan.Lit(offset)));
        }
        return new Ported(query, List.of());
    }

    /**
     * SELECT URL, COUNT(*) FROM hits WHERE CounterID = 62 AND EventDate in July 2013 AND DontCountHits = 0
     * AND IsRefresh = 0 AND URL <> '' GROUP BY 1 ORDER BY 2 DESC LIMIT 10. The EventDate literals depend on
     * the file's date encoding, so the lowering takes the data location.
     */
    public static Ported query37(java.nio.file.Path hits)
    {
        return topCountedInJuly2013(hits, "URL");
    }

    /** q37 over Title. */
    public static Ported query38(java.nio.file.Path hits)
    {
        return topCountedInJuly2013(hits, "Title");
    }

    private static Ported topCountedInJuly2013(java.nio.file.Path hits, String groupColumn)
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column(groupColumn, ColumnEncoding.STRING, false),
                new QueryLowering.Column("CounterID"),
                new QueryLowering.Column("EventDate"),
                new QueryLowering.Column("DontCountHits"),
                new QueryLowering.Column("IsRefresh"));
        query.where(
                new Plan.StringMatch(query.position(groupColumn), List.of(""), true),
                new Plan.Predicate("=", query.column("CounterID"), new Plan.Lit(62)),
                new Plan.Predicate("=", query.column("IsRefresh"), new Plan.Lit(0)),
                new Plan.Predicate(">", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, JULY_2013_START) - 1)),
                new Plan.Predicate("<", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, JULY_2013_END))),
                new Plan.Predicate("=", query.column("DontCountHits"), new Plan.Lit(0)));
        query.groupBy(groupColumn)
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));
        return new Ported(query, 0, 0, 0);
    }

    /**
     * SELECT URL, COUNT(*) FROM hits WHERE CounterID = 62 AND EventDate in July 2013 AND IsRefresh = 0
     * AND IsLink <> 0 AND IsDownload = 0 GROUP BY 1 ORDER BY 2 DESC LIMIT 10 OFFSET 1000
     */
    public static Ported query39(java.nio.file.Path hits)
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("URL", ColumnEncoding.STRING, false),
                new QueryLowering.Column("CounterID"),
                new QueryLowering.Column("EventDate"),
                new QueryLowering.Column("IsRefresh"),
                new QueryLowering.Column("IsLink"),
                new QueryLowering.Column("IsDownload"));
        query.where(
                new Plan.Predicate("=", query.column("CounterID"), new Plan.Lit(62)),
                new Plan.Predicate("=", query.column("IsRefresh"), new Plan.Lit(0)),
                new Plan.Predicate(">", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, JULY_2013_START) - 1)),
                new Plan.Predicate("<", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, JULY_2013_END))),
                new Plan.Not(new Plan.Predicate("=", query.column("IsLink"), new Plan.Lit(0))),
                new Plan.Predicate("=", query.column("IsDownload"), new Plan.Lit(0)));
        query.groupBy("URL")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10, 1_000));
        return new Ported(query, 0, 0, 0);
    }

    /**
     * SELECT URLHash, EventDate, COUNT(*) FROM hits WHERE CounterID = 62 AND EventDate in July 2013 AND
     * IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = ... GROUP BY 1, 2 ORDER BY 3 DESC
     * LIMIT 10 OFFSET 100. The EventDate literals depend on the file's date encoding, so the lowering
     * takes the data location.
     */
    public static Ported query41(java.nio.file.Path hits)
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("URLHash"),
                new QueryLowering.Column("EventDate"),
                new QueryLowering.Column("CounterID"),
                new QueryLowering.Column("IsRefresh"),
                new QueryLowering.Column("TraficSourceID"),
                new QueryLowering.Column("RefererHash"));
        query.where(
                new Plan.Predicate("=", query.column("CounterID"), new Plan.Lit(62)),
                new Plan.Predicate("=", query.column("IsRefresh"), new Plan.Lit(0)),
                new Plan.Predicate(">", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, JULY_2013_START) - 1)),
                new Plan.Predicate("<", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, JULY_2013_END))),
                new Plan.Or(List.of(
                        new Plan.Predicate("=", query.column("TraficSourceID"), new Plan.Lit(-1)),
                        new Plan.Predicate("=", query.column("TraficSourceID"), new Plan.Lit(6)))),
                new Plan.Predicate("=", query.column("RefererHash"), new Plan.Lit(ClickBenchHitsSupport.QUERY41_REFERER_HASH)));
        query.groupBy("URLHash", "EventDate")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(2, true)), 10, 100));
        return new Ported(query, List.of());
    }

    /**
     * SELECT WindowClientWidth, WindowClientHeight, COUNT(*) FROM hits WHERE CounterID = 62 AND EventDate in
     * July 2013 AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = ... GROUP BY 1, 2 ORDER BY 3 DESC
     * LIMIT 10 OFFSET 10000
     */
    public static Ported query42(java.nio.file.Path hits)
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("WindowClientWidth"),
                new QueryLowering.Column("WindowClientHeight"),
                new QueryLowering.Column("CounterID"),
                new QueryLowering.Column("EventDate"),
                new QueryLowering.Column("IsRefresh"),
                new QueryLowering.Column("DontCountHits"),
                new QueryLowering.Column("URLHash"));
        query.where(
                new Plan.Predicate("=", query.column("CounterID"), new Plan.Lit(62)),
                new Plan.Predicate("=", query.column("IsRefresh"), new Plan.Lit(0)),
                new Plan.Predicate(">", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, JULY_2013_START) - 1)),
                new Plan.Predicate("<", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, JULY_2013_END))),
                new Plan.Predicate("=", query.column("DontCountHits"), new Plan.Lit(0)),
                new Plan.Predicate("=", query.column("URLHash"), new Plan.Lit(ClickBenchHitsSupport.QUERY42_URL_HASH)));
        query.groupBy("WindowClientWidth", "WindowClientHeight")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(2, true)), 10, 10_000));
        return new Ported(query, List.of());
    }

    /**
     * SELECT EventTime / 60 * 60 AS minute, COUNT(*) FROM hits WHERE CounterID = 62 AND EventDate between
     * 2013-07-14 and 2013-07-15 AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY 1 ORDER BY 1 LIMIT 10
     * OFFSET 1000
     */
    public static Ported query43(java.nio.file.Path hits)
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("EventTime"),
                new QueryLowering.Column("CounterID"),
                new QueryLowering.Column("EventDate"),
                new QueryLowering.Column("DontCountHits"),
                new QueryLowering.Column("IsRefresh"));
        query.where(
                new Plan.Predicate("=", query.column("CounterID"), new Plan.Lit(62)),
                new Plan.Predicate("=", query.column("IsRefresh"), new Plan.Lit(0)),
                new Plan.Predicate(">", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, LocalDate.of(2013, 7, 14)) - 1)),
                new Plan.Predicate("<", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, LocalDate.of(2013, 7, 16)))),
                new Plan.Predicate("=", query.column("DontCountHits"), new Plan.Lit(0)));
        query.groupBy(new Plan.Bin("*",
                        new Plan.Bin("/", query.column("EventTime"), new Plan.Lit(60)),
                        new Plan.Lit(60)))
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 10, 1_000));
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
     * SELECT SearchPhrase, MIN(URL), COUNT(*) FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> ''
     * GROUP BY 1 ORDER BY 3 DESC LIMIT 10
     */
    public static Ported query22()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false),
                new QueryLowering.Column("URL", ColumnEncoding.STRING, false));
        query.where(
                new Plan.StringMatch(query.position("SearchPhrase"), List.of(""), true),
                new Plan.LikeMatch(query.position("URL"), "%google%", false))
                .groupBy("SearchPhrase")
                .aggregate("min_utf8", "URL")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(2, true)), 10));
        return new Ported(query, List.of(
                new CompiledTpcdsQueries.DictRef(0, 0, 0),
                new CompiledTpcdsQueries.DictRef(1, 0, 1)));
    }

    /** SELECT URL, COUNT(*) FROM hits GROUP BY 1 ORDER BY 2 DESC LIMIT 10 */
    public static Ported query34()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("URL", ColumnEncoding.STRING, false));
        query.groupBy("URL")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));
        return new Ported(query, 0, 0, 0);
    }

    /** SELECT 1, URL, COUNT(*) FROM hits GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10 */
    public static Ported query35()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("URL", ColumnEncoding.STRING, false));
        query.groupBy("URL")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));
        query.select(new Plan.Lit(1), new Plan.Col(0), new Plan.Col(1));
        return new Ported(query, 1, 0, 0);
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
