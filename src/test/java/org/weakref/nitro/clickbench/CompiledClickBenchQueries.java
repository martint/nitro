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

import org.weakref.nitro.legacy.pipeline.ColumnEncoding;
import org.weakref.nitro.legacy.pipeline.Plan;
import org.weakref.nitro.legacy.pipeline.QueryLowering;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries.Composite;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries.Ported;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries.Stage;

import java.time.LocalDate;
import java.util.List;

/**
 * ClickBench kernel lowerings for the data-centric compiler. These preserve SQL results but do not model distributed
 * fragment, exchange, or partial/final aggregation topology; use {@link ClickBenchHitsSupport} for SQL-plan-shaped
 * operator comparisons. Every query reads the single {@code hits} table; all its columns are NOT NULL.
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

    /** SELECT COUNT(DISTINCT UserID) FROM hits: single-pass, the distinct set fused into the aggregation. */
    public static Ported query05()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("UserID"));
        query.aggregate("count_distinct", "UserID");
        return new Ported(query, List.of());
    }

    /** SELECT RegionID, COUNT(DISTINCT UserID) FROM hits GROUP BY 1 ORDER BY 2 DESC LIMIT 10: single-pass. */
    public static Ported query09()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("RegionID"),
                new QueryLowering.Column("UserID"));
        query.groupBy("RegionID")
                .aggregate("count_distinct", "UserID");
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));
        return new Ported(query, List.of());
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
     * SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits
     * GROUP BY 1 ORDER BY 3 DESC LIMIT 10. This lowering deliberately measures the logical fused kernel. The
     * SQL-shaped harness separately models Trino's bigint coercions and the analyzed physical DISTINCT unit.
     */
    public static Ported query10()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("RegionID"),
                new QueryLowering.Column("AdvEngineID"),
                new QueryLowering.Column("ResolutionWidth"),
                new QueryLowering.Column("UserID"));
        query.groupBy("RegionID")
                .aggregate("sum", "AdvEngineID")
                .count()
                .aggregate("avg", "ResolutionWidth")
                .aggregate("count_distinct", "UserID");
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(2, true)), 10));
        return new Ported(query, List.of());
    }

    /** SELECT COUNT(DISTINCT SearchPhrase) FROM hits: fused kernel, without SQL fragment/exchange topology. */
    public static Ported query06()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false));
        query.aggregate("count_distinct", "SearchPhrase");
        return new Ported(query, List.of());
    }

    /**
     * SELECT MobilePhoneModel, COUNT(DISTINCT UserID) FROM hits WHERE MobilePhoneModel <> '' GROUP BY 1
     * ORDER BY 2 DESC LIMIT 10: single-pass.
     */
    public static Ported query11()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("MobilePhoneModel", ColumnEncoding.STRING, false),
                new QueryLowering.Column("UserID"));
        query.where(new Plan.StringMatch(query.position("MobilePhoneModel"), List.of(""), true))
                .groupBy("MobilePhoneModel")
                .aggregate("count_distinct", "UserID");
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));
        return new Ported(query, 0, 0, 0);
    }

    /**
     * SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) FROM hits WHERE MobilePhoneModel <> ''
     * GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10: single-pass.
     */
    public static Ported query12()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("MobilePhone"),
                new QueryLowering.Column("MobilePhoneModel", ColumnEncoding.STRING, false),
                new QueryLowering.Column("UserID"));
        query.where(new Plan.StringMatch(query.position("MobilePhoneModel"), List.of(""), true))
                .groupBy("MobilePhone", "MobilePhoneModel")
                .aggregate("count_distinct", "UserID");
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(2, true)), 10));
        return new Ported(query, 1, 0, 1);
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
     * DESC LIMIT 10: single-pass.
     */
    public static Ported query14()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false),
                new QueryLowering.Column("UserID"));
        query.where(new Plan.StringMatch(query.position("SearchPhrase"), List.of(""), true))
                .groupBy("SearchPhrase")
                .aggregate("count_distinct", "UserID");
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 10));
        return new Ported(query, 0, 0, 0);
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

    /**
     * SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*), COUNT(DISTINCT UserID) FROM hits
     * WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY 1
     * ORDER BY 4 DESC LIMIT 10
     */
    public static Ported query23()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false),
                new QueryLowering.Column("URL", ColumnEncoding.STRING, false),
                new QueryLowering.Column("Title", ColumnEncoding.STRING, false),
                new QueryLowering.Column("UserID"));
        query.where(
                new Plan.StringMatch(query.position("SearchPhrase"), List.of(""), true),
                new Plan.LikeMatch(query.position("Title"), "%Google%", false),
                new Plan.LikeMatch(query.position("URL"), "%.google.%", true))
                .groupBy("SearchPhrase")
                .aggregate("min_utf8", "URL")
                .aggregate("min_utf8", "Title")
                .count()
                .aggregate("count_distinct", "UserID");
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(3, true)), 10));
        return new Ported(query, List.of(
                new CompiledTpcdsQueries.DictRef(0, 0, 0),
                new CompiledTpcdsQueries.DictRef(1, 0, 1),
                new CompiledTpcdsQueries.DictRef(2, 0, 2)));
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
     * SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10: every hits column in the
     * file's schema order (the harness reads the footer; these are the athena split columns), filtered on URL
     * and top-10 by EventTime.
     */
    public static Ported query24()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
            new QueryLowering.Column("WatchID"),
            new QueryLowering.Column("JavaEnable"),
            new QueryLowering.Column("Title", ColumnEncoding.STRING, false),
            new QueryLowering.Column("GoodEvent"),
            new QueryLowering.Column("EventTime"),
            new QueryLowering.Column("EventDate"),
            new QueryLowering.Column("CounterID"),
            new QueryLowering.Column("ClientIP"),
            new QueryLowering.Column("RegionID"),
            new QueryLowering.Column("UserID"),
            new QueryLowering.Column("CounterClass"),
            new QueryLowering.Column("OS"),
            new QueryLowering.Column("UserAgent"),
            new QueryLowering.Column("URL", ColumnEncoding.STRING, false),
            new QueryLowering.Column("Referer", ColumnEncoding.STRING, false),
            new QueryLowering.Column("IsRefresh"),
            new QueryLowering.Column("RefererCategoryID"),
            new QueryLowering.Column("RefererRegionID"),
            new QueryLowering.Column("URLCategoryID"),
            new QueryLowering.Column("URLRegionID"),
            new QueryLowering.Column("ResolutionWidth"),
            new QueryLowering.Column("ResolutionHeight"),
            new QueryLowering.Column("ResolutionDepth"),
            new QueryLowering.Column("FlashMajor"),
            new QueryLowering.Column("FlashMinor"),
            new QueryLowering.Column("FlashMinor2", ColumnEncoding.STRING, false),
            new QueryLowering.Column("NetMajor"),
            new QueryLowering.Column("NetMinor"),
            new QueryLowering.Column("UserAgentMajor"),
            new QueryLowering.Column("UserAgentMinor", ColumnEncoding.STRING, false),
            new QueryLowering.Column("CookieEnable"),
            new QueryLowering.Column("JavascriptEnable"),
            new QueryLowering.Column("IsMobile"),
            new QueryLowering.Column("MobilePhone"),
            new QueryLowering.Column("MobilePhoneModel", ColumnEncoding.STRING, false),
            new QueryLowering.Column("Params", ColumnEncoding.STRING, false),
            new QueryLowering.Column("IPNetworkID"),
            new QueryLowering.Column("TraficSourceID"),
            new QueryLowering.Column("SearchEngineID"),
            new QueryLowering.Column("SearchPhrase", ColumnEncoding.STRING, false),
            new QueryLowering.Column("AdvEngineID"),
            new QueryLowering.Column("IsArtifical"),
            new QueryLowering.Column("WindowClientWidth"),
            new QueryLowering.Column("WindowClientHeight"),
            new QueryLowering.Column("ClientTimeZone"),
            new QueryLowering.Column("ClientEventTime"),
            new QueryLowering.Column("SilverlightVersion1"),
            new QueryLowering.Column("SilverlightVersion2"),
            new QueryLowering.Column("SilverlightVersion3"),
            new QueryLowering.Column("SilverlightVersion4"),
            new QueryLowering.Column("PageCharset", ColumnEncoding.STRING, false),
            new QueryLowering.Column("CodeVersion"),
            new QueryLowering.Column("IsLink"),
            new QueryLowering.Column("IsDownload"),
            new QueryLowering.Column("IsNotBounce"),
            new QueryLowering.Column("FUniqID"),
            new QueryLowering.Column("OriginalURL", ColumnEncoding.STRING, false),
            new QueryLowering.Column("HID"),
            new QueryLowering.Column("IsOldCounter"),
            new QueryLowering.Column("IsEvent"),
            new QueryLowering.Column("IsParameter"),
            new QueryLowering.Column("DontCountHits"),
            new QueryLowering.Column("WithHash"),
            new QueryLowering.Column("HitColor", ColumnEncoding.STRING, false),
            new QueryLowering.Column("LocalEventTime"),
            new QueryLowering.Column("Age"),
            new QueryLowering.Column("Sex"),
            new QueryLowering.Column("Income"),
            new QueryLowering.Column("Interests"),
            new QueryLowering.Column("Robotness"),
            new QueryLowering.Column("RemoteIP"),
            new QueryLowering.Column("WindowName"),
            new QueryLowering.Column("OpenerName"),
            new QueryLowering.Column("HistoryLength"),
            new QueryLowering.Column("BrowserLanguage", ColumnEncoding.STRING, false),
            new QueryLowering.Column("BrowserCountry", ColumnEncoding.STRING, false),
            new QueryLowering.Column("SocialNetwork", ColumnEncoding.STRING, false),
            new QueryLowering.Column("SocialAction", ColumnEncoding.STRING, false),
            new QueryLowering.Column("HTTPError"),
            new QueryLowering.Column("SendTiming"),
            new QueryLowering.Column("DNSTiming"),
            new QueryLowering.Column("ConnectTiming"),
            new QueryLowering.Column("ResponseStartTiming"),
            new QueryLowering.Column("ResponseEndTiming"),
            new QueryLowering.Column("FetchTiming"),
            new QueryLowering.Column("SocialSourceNetworkID"),
            new QueryLowering.Column("SocialSourcePage", ColumnEncoding.STRING, false),
            new QueryLowering.Column("ParamPrice"),
            new QueryLowering.Column("ParamOrderID", ColumnEncoding.STRING, false),
            new QueryLowering.Column("ParamCurrency", ColumnEncoding.STRING, false),
            new QueryLowering.Column("ParamCurrencyID"),
            new QueryLowering.Column("OpenstatServiceName", ColumnEncoding.STRING, false),
            new QueryLowering.Column("OpenstatCampaignID", ColumnEncoding.STRING, false),
            new QueryLowering.Column("OpenstatAdID", ColumnEncoding.STRING, false),
            new QueryLowering.Column("OpenstatSourceID", ColumnEncoding.STRING, false),
            new QueryLowering.Column("UTMSource", ColumnEncoding.STRING, false),
            new QueryLowering.Column("UTMMedium", ColumnEncoding.STRING, false),
            new QueryLowering.Column("UTMCampaign", ColumnEncoding.STRING, false),
            new QueryLowering.Column("UTMContent", ColumnEncoding.STRING, false),
            new QueryLowering.Column("UTMTerm", ColumnEncoding.STRING, false),
            new QueryLowering.Column("FromTag", ColumnEncoding.STRING, false),
            new QueryLowering.Column("HasGCLID"),
            new QueryLowering.Column("RefererHash"),
            new QueryLowering.Column("URLHash"),
            new QueryLowering.Column("CLID"));
        query.where(new Plan.LikeMatch(query.position("URL"), "%google%", false));
        query.select(
                new Plan.Col(0),
                new Plan.Col(1),
                new Plan.Col(2),
                new Plan.Col(3),
                new Plan.Col(4),
                new Plan.Col(5),
                new Plan.Col(6),
                new Plan.Col(7),
                new Plan.Col(8),
                new Plan.Col(9),
                new Plan.Col(10),
                new Plan.Col(11),
                new Plan.Col(12),
                new Plan.Col(13),
                new Plan.Col(14),
                new Plan.Col(15),
                new Plan.Col(16),
                new Plan.Col(17),
                new Plan.Col(18),
                new Plan.Col(19),
                new Plan.Col(20),
                new Plan.Col(21),
                new Plan.Col(22),
                new Plan.Col(23),
                new Plan.Col(24),
                new Plan.Col(25),
                new Plan.Col(26),
                new Plan.Col(27),
                new Plan.Col(28),
                new Plan.Col(29),
                new Plan.Col(30),
                new Plan.Col(31),
                new Plan.Col(32),
                new Plan.Col(33),
                new Plan.Col(34),
                new Plan.Col(35),
                new Plan.Col(36),
                new Plan.Col(37),
                new Plan.Col(38),
                new Plan.Col(39),
                new Plan.Col(40),
                new Plan.Col(41),
                new Plan.Col(42),
                new Plan.Col(43),
                new Plan.Col(44),
                new Plan.Col(45),
                new Plan.Col(46),
                new Plan.Col(47),
                new Plan.Col(48),
                new Plan.Col(49),
                new Plan.Col(50),
                new Plan.Col(51),
                new Plan.Col(52),
                new Plan.Col(53),
                new Plan.Col(54),
                new Plan.Col(55),
                new Plan.Col(56),
                new Plan.Col(57),
                new Plan.Col(58),
                new Plan.Col(59),
                new Plan.Col(60),
                new Plan.Col(61),
                new Plan.Col(62),
                new Plan.Col(63),
                new Plan.Col(64),
                new Plan.Col(65),
                new Plan.Col(66),
                new Plan.Col(67),
                new Plan.Col(68),
                new Plan.Col(69),
                new Plan.Col(70),
                new Plan.Col(71),
                new Plan.Col(72),
                new Plan.Col(73),
                new Plan.Col(74),
                new Plan.Col(75),
                new Plan.Col(76),
                new Plan.Col(77),
                new Plan.Col(78),
                new Plan.Col(79),
                new Plan.Col(80),
                new Plan.Col(81),
                new Plan.Col(82),
                new Plan.Col(83),
                new Plan.Col(84),
                new Plan.Col(85),
                new Plan.Col(86),
                new Plan.Col(87),
                new Plan.Col(88),
                new Plan.Col(89),
                new Plan.Col(90),
                new Plan.Col(91),
                new Plan.Col(92),
                new Plan.Col(93),
                new Plan.Col(94),
                new Plan.Col(95),
                new Plan.Col(96),
                new Plan.Col(97),
                new Plan.Col(98),
                new Plan.Col(99),
                new Plan.Col(100),
                new Plan.Col(101),
                new Plan.Col(102),
                new Plan.Col(103),
                new Plan.Col(104));
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(4, false)), 10));
        return new Ported(query, List.of(
                new CompiledTpcdsQueries.DictRef(2, 0, 2),
                new CompiledTpcdsQueries.DictRef(13, 0, 13),
                new CompiledTpcdsQueries.DictRef(14, 0, 14),
                new CompiledTpcdsQueries.DictRef(25, 0, 25),
                new CompiledTpcdsQueries.DictRef(29, 0, 29),
                new CompiledTpcdsQueries.DictRef(34, 0, 34),
                new CompiledTpcdsQueries.DictRef(35, 0, 35),
                new CompiledTpcdsQueries.DictRef(39, 0, 39),
                new CompiledTpcdsQueries.DictRef(50, 0, 50),
                new CompiledTpcdsQueries.DictRef(56, 0, 56),
                new CompiledTpcdsQueries.DictRef(63, 0, 63),
                new CompiledTpcdsQueries.DictRef(74, 0, 74),
                new CompiledTpcdsQueries.DictRef(75, 0, 75),
                new CompiledTpcdsQueries.DictRef(76, 0, 76),
                new CompiledTpcdsQueries.DictRef(77, 0, 77),
                new CompiledTpcdsQueries.DictRef(86, 0, 86),
                new CompiledTpcdsQueries.DictRef(88, 0, 88),
                new CompiledTpcdsQueries.DictRef(89, 0, 89),
                new CompiledTpcdsQueries.DictRef(91, 0, 91),
                new CompiledTpcdsQueries.DictRef(92, 0, 92),
                new CompiledTpcdsQueries.DictRef(93, 0, 93),
                new CompiledTpcdsQueries.DictRef(94, 0, 94),
                new CompiledTpcdsQueries.DictRef(95, 0, 95),
                new CompiledTpcdsQueries.DictRef(96, 0, 96),
                new CompiledTpcdsQueries.DictRef(97, 0, 97),
                new CompiledTpcdsQueries.DictRef(98, 0, 98),
                new CompiledTpcdsQueries.DictRef(99, 0, 99),
                new CompiledTpcdsQueries.DictRef(100, 0, 100)));
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

    /**
     * SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k,
     * AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> ''
     * GROUP BY 1 HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25: the host extraction is a
     * regexp-derived column applied per dictionary entry by the loader.
     */
    public static Ported query29()
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                QueryLowering.Column.regexpReplace("RefererHost", "Referer", false, "^https?://(?:www\\.)?([^/]+)/.*$", "\\1"),
                new QueryLowering.Column("Referer", ColumnEncoding.STRING, false));
        query.where(new Plan.StringMatch(query.position("Referer"), List.of(""), true))
                .groupBy("RefererHost")
                .aggregate("avg", new Plan.Call("length_utf8", List.of(query.column("Referer"))))
                .count()
                .aggregate("min_utf8", "Referer");
        query.having(new Plan.Predicate(">", new Plan.Col(2), new Plan.Lit(100_000)));
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 25));
        return new Ported(query, List.of(
                new CompiledTpcdsQueries.DictRef(0, 0, 0),
                new CompiledTpcdsQueries.DictRef(3, 0, 1)));
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
     * SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0)
     * THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) FROM hits WHERE CounterID = 62 AND EventDate in
     * July 2013 AND IsRefresh = 0 GROUP BY 1..5 ORDER BY 6 DESC LIMIT 10 OFFSET 1000. The string CASE folds
     * to ids: the value branch is the referer's dictionary id, the else-'' branch the -1 empty-string
     * sentinel, reconstructed through the referer's dictionary.
     */
    public static Ported query40(java.nio.file.Path hits)
    {
        QueryLowering query = QueryLowering.scan(ClickBenchParquetTables.HITS_TABLE,
                new QueryLowering.Column("TraficSourceID"),
                new QueryLowering.Column("SearchEngineID"),
                new QueryLowering.Column("AdvEngineID"),
                new QueryLowering.Column("Referer", ColumnEncoding.STRING, false),
                new QueryLowering.Column("URL", ColumnEncoding.STRING, false),
                new QueryLowering.Column("CounterID"),
                new QueryLowering.Column("EventDate"),
                new QueryLowering.Column("IsRefresh"));
        query.where(
                new Plan.Predicate("=", query.column("CounterID"), new Plan.Lit(62)),
                new Plan.Predicate("=", query.column("IsRefresh"), new Plan.Lit(0)),
                new Plan.Predicate(">", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, JULY_2013_START) - 1)),
                new Plan.Predicate("<", query.column("EventDate"), new Plan.Lit(ClickBenchHitsSupport.eventDateLiteral(hits, JULY_2013_END))));
        query.groupBy(
                query.column("TraficSourceID"),
                query.column("SearchEngineID"),
                query.column("AdvEngineID"),
                new Plan.Case(
                        List.of(new Plan.Case.Branch(
                                new Plan.And(List.of(
                                        new Plan.Predicate("=", query.column("SearchEngineID"), new Plan.Lit(0)),
                                        new Plan.Predicate("=", query.column("AdvEngineID"), new Plan.Lit(0)))),
                                query.column("Referer"))),
                        new Plan.Lit(-1)),
                query.column("URL"))
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(5, true)), 10, 1_000));
        return new Ported(query, List.of(
                new CompiledTpcdsQueries.DictRef(3, 0, 3),
                new CompiledTpcdsQueries.DictRef(4, 0, 4)));
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
