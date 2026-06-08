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

import org.weakref.nitro.jit.ColumnEncoding;
import org.weakref.nitro.jit.Plan;
import org.weakref.nitro.jit.QueryLowering;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * The TPC-DS queries ported to the data-centric compiler, as reusable {@link QueryLowering} builders shared by the
 * correctness tests ({@link TestCompiledTpcdsQueries}) and the apples-to-apples benchmark
 * ({@code BenchmarkCompiledQueries}) so the two cannot drift. Each builder returns the lowering plus the metadata
 * needed to reconstruct a dictionary-string result column ({@code stringResultColumn} = -1 when none).
 */
public final class CompiledTpcdsQueries
{
    private CompiledTpcdsQueries() {}

    /**
     * Where a dictionary-string result column's dictionary lives: {@code resultColumn} is the position in the
     * compiled result; {@code dictInput}/{@code dictColumn} address the loaded {@link QueryLowering.Input} whose
     * {@code StringColumn} holds the (ordered) dictionary used to reconstruct it.
     */
    public record DictRef(int resultColumn, int dictInput, int dictColumn) {}

    /** A ported query: its name-based lowering and where to find each string result column's dictionary. */
    public record Ported(QueryLowering query, List<DictRef> stringColumns)
    {
        public Ported
        {
            stringColumns = List.copyOf(stringColumns);
        }

        /** Convenience for a query with a single (or no, when {@code stringResultColumn < 0}) string result column. */
        public Ported(QueryLowering query, int stringResultColumn, int dictInput, int dictColumn)
        {
            this(query, stringResultColumn < 0 ? List.of() : List.of(new DictRef(stringResultColumn, dictInput, dictColumn)));
        }
    }

    public static Ported query03()
    {
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manufact_id"),
                        new QueryLowering.Column("i_brand_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(11)),
                        new Plan.Predicate("=", query.column("i_manufact_id"), new Plan.Lit(128)))
                .groupBy("d_year", "i_brand_id", "i_brand")
                .aggregate("sum", "ss_ext_sales_price")
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(3, true), new Plan.SortKey(1, false)), 100));
        return new Ported(query, 2, 2, 3);
    }

    public static Ported query07()
    {
        // store_sales joined to date_dim, item, customer_demographics, promotion; GROUP BY i_item_id;
        // four averages; ORDER BY i_item_id LIMIT 100.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_cdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_promo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_quantity", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_list_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_coupon_amt", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false))
                .join("customer_demographics", "ss_cdemo_sk", "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_gender", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("cd_education_status", ColumnEncoding.STRING, false))
                .join("promotion", "ss_promo_sk", "p_promo_sk",
                        new QueryLowering.Column("p_promo_sk"),
                        new QueryLowering.Column("p_channel_email", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("p_channel_event", ColumnEncoding.STRING, false));
        query.where(
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2000)),
                        new Plan.StringMatch(query.position("cd_gender"), List.of("M"), false),
                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("S"), false),
                        new Plan.StringMatch(query.position("cd_education_status"), List.of("College"), false),
                        new Plan.Or(List.of(
                                new Plan.StringMatch(query.position("p_channel_email"), List.of("N"), false),
                                new Plan.StringMatch(query.position("p_channel_event"), List.of("N"), false))))
                .groupBy("i_item_id")
                .aggregate("avg", "ss_quantity")
                .aggregate("avg", "ss_list_price")
                .aggregate("avg", "ss_coupon_amt")
                .aggregate("avg", "ss_sales_price")
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 100));
        return new Ported(query, 0, 2, 1);
    }

    public static Ported query26()
    {
        // catalog_sales joined to date_dim, item, customer_demographics, promotion; GROUP BY i_item_id; four
        // averages; ORDER BY i_item_id LIMIT 100. Demographic/promotion filter columns are declared nullable so
        // the three-valued null guard applies (needed on catalog_sales).
        QueryLowering query = QueryLowering.scan("catalog_sales",
                        new QueryLowering.Column("cs_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_bill_cdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_promo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_quantity", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_list_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_coupon_amt", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "cs_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"))
                .join("item", "cs_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false))
                .join("customer_demographics", "cs_bill_cdemo_sk", "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_gender", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_education_status", ColumnEncoding.STRING, true))
                .join("promotion", "cs_promo_sk", "p_promo_sk",
                        new QueryLowering.Column("p_promo_sk"),
                        new QueryLowering.Column("p_channel_email", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("p_channel_event", ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2000)),
                        new Plan.StringMatch(query.position("cd_gender"), List.of("M"), false),
                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("S"), false),
                        new Plan.StringMatch(query.position("cd_education_status"), List.of("College"), false),
                        new Plan.Or(List.of(
                                new Plan.StringMatch(query.position("p_channel_email"), List.of("N"), false),
                                new Plan.StringMatch(query.position("p_channel_event"), List.of("N"), false))))
                .groupBy("i_item_id")
                .aggregate("avg", "cs_quantity")
                .aggregate("avg", "cs_list_price")
                .aggregate("avg", "cs_coupon_amt")
                .aggregate("avg", "cs_sales_price")
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 100));
        return new Ported(query, 0, 2, 1);
    }

    public static Ported query42()
    {
        // store_sales ⋈ date_dim(d_moy=11, d_year=2000) ⋈ item(i_manager_id=1); GROUP BY d_year, i_category_id,
        // i_category; sum(ss_ext_sales_price); ORDER BY sum DESC, d_year, category id/name; top 100.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk"),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manager_id"),
                        new QueryLowering.Column("i_category_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(11)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2000)),
                        new Plan.Predicate("=", query.column("i_manager_id"), new Plan.Lit(1)))
                .groupBy("d_year", "i_category_id", "i_category")
                .aggregate("sum", "ss_ext_sales_price")
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(3, true), new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)), 100));
        return new Ported(query, 2, 2, 3);
    }

    public static Ported query52()
    {
        // store_sales ⋈ date_dim(d_moy=11, d_year=2000) ⋈ item(i_manager_id=1); GROUP BY d_year, i_brand_id,
        // i_brand; sum(ss_ext_sales_price); ORDER BY d_year, sum DESC, i_brand_id; top 100.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk"),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manager_id"),
                        new QueryLowering.Column("i_brand_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(11)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2000)),
                        new Plan.Predicate("=", query.column("i_manager_id"), new Plan.Lit(1)))
                .groupBy("d_year", "i_brand_id", "i_brand")
                .aggregate("sum", "ss_ext_sales_price")
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(3, true), new Plan.SortKey(1, false)), 100));
        return new Ported(query, 2, 2, 3);
    }

    public static Ported query55()
    {
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk"),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manager_id"),
                        new QueryLowering.Column("i_brand_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(11)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(1999)),
                        new Plan.Predicate("=", query.column("i_manager_id"), new Plan.Lit(28)))
                .groupBy("i_brand_id", "i_brand")
                .aggregate("sum", "ss_ext_sales_price")
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(2, true), new Plan.SortKey(0, false)), 100));
        return new Ported(query, 1, 2, 3);
    }

    public static Ported query15()
    {
        // catalog_sales -> customer -> customer_address (snowflake: the second join keys on customer's
        // c_current_addr_sk, not the fact) -> date_dim (d_qoy=2, d_year=2001). WHERE substr(ca_zip,1,5) IN (..)
        // OR ca_state IN (CA,WA,GA) OR cs_sales_price > 500 (scaled 50000). GROUP BY ca_zip; sum(cs_sales_price);
        // ORDER BY ca_zip LIMIT 100.
        QueryLowering query = QueryLowering.scan("catalog_sales",
                        new QueryLowering.Column("cs_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_bill_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_sales_price", ColumnEncoding.FLAT, true))
                .join("customer", "cs_bill_customer_sk", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_zip", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, true))
                .join("date_dim", "cs_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_qoy"),
                        new QueryLowering.Column("d_year"));
        query.where(
                        new Plan.Predicate("=", query.column("d_qoy"), new Plan.Lit(2)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2001)),
                        new Plan.Or(List.of(
                                new Plan.SubstringMatch(query.position("ca_zip"), 1, 5,
                                        List.of("85669", "86197", "88274", "83405", "86475", "85392", "85460", "80348", "81792"), false),
                                new Plan.StringMatch(query.position("ca_state"), List.of("CA", "WA", "GA"), false),
                                new Plan.Predicate(">", query.column("cs_sales_price"), new Plan.Lit(50_000)))))
                .groupBy("ca_zip")
                .aggregate("sum", "cs_sales_price")
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 100));
        return new Ported(query, List.of(new DictRef(0, 2, 1)));
    }

    public static Ported query91()
    {
        // catalog_returns -> date_dim(1998-11) -> customer -> [snowflake] customer_demographics, household_demographics,
        // customer_address -> call_center. GROUP BY 5 string keys (call-center id/name/manager + customer
        // marital/education); sum(cr_net_loss); ORDER BY the loss desc then the keys; SELECT only id, name, manager,
        // loss (the two demographics keys are grouped on but projected away). Exercises a 6-join snowflake, five
        // dictionary-string group keys across two dimensions, and a post-aggregation projection of string columns.
        QueryLowering query = QueryLowering.scan("catalog_returns",
                        new QueryLowering.Column("cr_call_center_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cr_returned_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cr_returning_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cr_net_loss", ColumnEncoding.FLAT, true))
                .join("date_dim", "cr_returned_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"),
                        new QueryLowering.Column("d_moy"))
                .join("customer", "cr_returning_customer_sk", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_current_cdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_current_hdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true))
                .join("customer_demographics", "c_current_cdemo_sk", "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_education_status", ColumnEncoding.STRING, true))
                .join("household_demographics", "c_current_hdemo_sk", "hd_demo_sk",
                        new QueryLowering.Column("hd_demo_sk"),
                        new QueryLowering.Column("hd_buy_potential", ColumnEncoding.STRING, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_gmt_offset", ColumnEncoding.FLAT, true))
                .join("call_center", "cr_call_center_sk", "cc_call_center_sk",
                        new QueryLowering.Column("cc_call_center_sk"),
                        new QueryLowering.Column("cc_call_center_id", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cc_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cc_manager", ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(1998)),
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(11)),
                        new Plan.StringMatch(query.position("hd_buy_potential"), List.of("Unknown"), false),
                        new Plan.Predicate("=", query.column("ca_gmt_offset"), new Plan.Lit(-700)),
                        new Plan.Or(List.of(
                                new Plan.And(
                                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("M"), false),
                                        new Plan.StringMatch(query.position("cd_education_status"), List.of("Unknown"), false)),
                                new Plan.And(
                                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("W"), false),
                                        new Plan.StringMatch(query.position("cd_education_status"), List.of("Advanced Degree"), false)))))
                .groupBy("cc_call_center_id", "cc_name", "cc_manager", "cd_marital_status", "cd_education_status")
                .aggregate("sum", "cr_net_loss")
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(5, true), new Plan.SortKey(0, false), new Plan.SortKey(1, false),
                        new Plan.SortKey(2, false), new Plan.SortKey(3, false), new Plan.SortKey(4, false)), 100))
                // SELECT id, name, manager, loss -- the two demographics keys are grouped on but not output.
                .select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(5));
        return new Ported(query, List.of(new DictRef(0, 6, 1), new DictRef(1, 6, 2), new DictRef(2, 6, 3)));
    }

    /**
     * A two-stage (pipeline-breaker) query: a {@code subquery} pipeline whose result feeds the {@code main}
     * pipeline as a join build (the decorrelated form of a correlated aggregate subquery -- there are no correlated
     * subqueries at the operator level). {@code virtualTable} is the placeholder table name {@code main} joins to;
     * the harness substitutes the subquery's result for it rather than reading Parquet.
     */
    public record MultiStage(QueryLowering subquery, QueryLowering main, String virtualTable, List<DictRef> stringColumns) {}

    /**
     * A UNION ALL of {@code branches} (each a grouped sub-pipeline producing the same schema) feeding {@code main}
     * (which scans {@code virtualTable} = the row-wise concatenation of the branch results). The union is a
     * concatenation of independently-computed branch results -- the optimizer's decorrelated shape -- not a replay.
     */
    public record Union(List<QueryLowering> branches, QueryLowering main, String virtualTable,
            List<DictRef> branchStringColumns, List<DictRef> stringColumns) {}

    /**
     * One channel of the Q33/Q56/Q60 union: channel sales JOIN item / date_dim / customer_address, filtered on an
     * item attribute ({@code itemAttribute} in {@code itemValues}), {@code d_year}/{@code d_moy} and gmt_offset, then
     * grouped by {@code groupColumn} (an item attribute -- numeric {@code i_manufact_id} for Q33, the dictionary
     * string {@code i_item_id} for Q56/Q60) summing {@code sales}.
     */
    private static QueryLowering unionChannelGroupedSales(String table, String soldDate, String item, String address, String sales,
            String itemAttribute, List<String> itemValues, String groupColumn, ColumnEncoding groupEncoding, int year, int month)
    {
        QueryLowering query = QueryLowering.scan(table,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(item, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(address, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(sales, ColumnEncoding.FLAT, true))
                .join("item", item, "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column(itemAttribute, ColumnEncoding.STRING, true),
                        new QueryLowering.Column(groupColumn, groupEncoding, false))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"),
                        new QueryLowering.Column("d_moy"))
                .join("customer_address", address, "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_gmt_offset"));
        query.where(
                        new Plan.StringMatch(query.position(itemAttribute), itemValues, false),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(year)),
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(month)),
                        new Plan.Predicate("=", query.column("ca_gmt_offset"), new Plan.Lit(-500)))
                .groupBy(groupColumn)
                .aggregate("sum", sales);
        return query;
    }

    public static Union query33()
    {
        // UNION ALL of store/catalog/web sales, each grouped by i_manufact_id with sum(ext_sales_price) over
        // item(category='Electronics') JOIN date_dim(d_year=1998,d_moy=5) JOIN customer_address(gmt_offset=-5);
        // then a final group by manufacturer summing the per-channel totals, ordered by total, LIMIT 100.
        List<QueryLowering> branches = unionChannelBranches("i_category", List.of("Electronics"), "i_manufact_id", ColumnEncoding.FLAT, 1998, 5);
        QueryLowering main = QueryLowering.scan("__q33_union__",
                        new QueryLowering.Column("g_manufact_id"),
                        new QueryLowering.Column("g_total", ColumnEncoding.FLAT, true))
                .groupBy("g_manufact_id")
                .aggregate("sum", "g_total");
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, false)), 100));
        return new Union(branches, main, "__q33_union__", List.of(), List.of());
    }

    public static Union query56()
    {
        // Same union shape as Q33 but filtered on i_color in {slate,blanched,burnished}, d_year=2001/d_moy=2, and
        // grouped by the dictionary string i_item_id; ordered by (total, item_id), LIMIT 100.
        List<QueryLowering> branches = unionChannelBranches(
                "i_color", List.of("slate", "blanched", "burnished"), "i_item_id", ColumnEncoding.STRING, 2001, 2);
        QueryLowering main = QueryLowering.scan("__q56_union__",
                        new QueryLowering.Column("g_item_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_total", ColumnEncoding.FLAT, true))
                .groupBy("g_item_id")
                .aggregate("sum", "g_total");
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, false), new Plan.SortKey(0, false)), 100));
        // branch result column 0 = i_item_id, a string from input 1 (item), column 2; final output column 0 likewise.
        return new Union(branches, main, "__q56_union__", List.of(new DictRef(0, 1, 2)), List.of(new DictRef(0, 0, 0)));
    }

    public static Union query60()
    {
        // Same union shape as Q33 but filtered on i_category='Music', d_year=1998/d_moy=9, grouped by the dictionary
        // string i_item_id; ordered by (item_id, total), LIMIT 100.
        List<QueryLowering> branches = unionChannelBranches(
                "i_category", List.of("Music"), "i_item_id", ColumnEncoding.STRING, 1998, 9);
        QueryLowering main = QueryLowering.scan("__q60_union__",
                        new QueryLowering.Column("g_item_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_total", ColumnEncoding.FLAT, true))
                .groupBy("g_item_id")
                .aggregate("sum", "g_total");
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100));
        return new Union(branches, main, "__q60_union__", List.of(new DictRef(0, 1, 2)), List.of(new DictRef(0, 0, 0)));
    }

    public static Union query71()
    {
        // A different union shape: the branches are UNGROUPED (projection-only) per-channel sales for d_moy=11,
        // d_year=1999, projected to (item_sk, time_sk, ext_sales_price). Their row-wise concatenation feeds a single
        // main pipeline that joins item (i_manager_id=1) and time_dim (meal in {breakfast,dinner}), groups by
        // (i_brand_id, i_brand, t_hour, t_minute) summing ext_sales_price, ordered by (sum desc, brand_id, hour, minute).
        List<QueryLowering> branches = List.of(
                query71ChannelSales("web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_sold_time_sk", "ws_ext_sales_price"),
                query71ChannelSales("catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_sold_time_sk", "cs_ext_sales_price"),
                query71ChannelSales("store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_sold_time_sk", "ss_ext_sales_price"));
        QueryLowering main = QueryLowering.scan("__q71_union__",
                        new QueryLowering.Column("u_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_time_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_sales", ColumnEncoding.FLAT, true))
                .join("item", "u_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manager_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_brand_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, true))
                .join("time_dim", "u_time_sk", "t_time_sk",
                        new QueryLowering.Column("t_time_sk"),
                        new QueryLowering.Column("t_hour", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("t_minute", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("t_meal_time", ColumnEncoding.STRING, true));
        main.where(
                        new Plan.Predicate("=", main.column("i_manager_id"), new Plan.Lit(1)),
                        new Plan.StringMatch(main.position("t_meal_time"), List.of("breakfast", "dinner"), false))
                .groupBy("i_brand_id", "i_brand", "t_hour", "t_minute")
                .aggregate("sum", "u_sales");
        main.orderBy(new Plan.Ordering(List.of(
                new Plan.SortKey(4, true), new Plan.SortKey(0, false), new Plan.SortKey(2, false), new Plan.SortKey(3, false)), 100));
        // Output column 1 (i_brand) is a dictionary string from main input 1 (item), column 3.
        return new Union(branches, main, "__q71_union__", List.of(), List.of(new DictRef(1, 1, 3)));
    }

    /** One channel of Q71: channel sales JOIN date_dim(d_moy=11, d_year=1999), projected (ungrouped) to (item_sk, time_sk, ext_sales_price). */
    private static QueryLowering query71ChannelSales(String table, String soldDate, String item, String time, String sales)
    {
        QueryLowering query = QueryLowering.scan(table,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(item, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(time, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(sales, ColumnEncoding.FLAT, true))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(11)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(1999)))
                .select(query.column(item), query.column(time), query.column(sales));
        return query;
    }

    /** The three sales-channel branches of a Q33/Q56/Q60-shaped union, parameterized by the item filter and group key. */
    private static List<QueryLowering> unionChannelBranches(String itemAttribute, List<String> itemValues,
            String groupColumn, ColumnEncoding groupEncoding, int year, int month)
    {
        return List.of(
                unionChannelGroupedSales("store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_addr_sk", "ss_ext_sales_price",
                        itemAttribute, itemValues, groupColumn, groupEncoding, year, month),
                unionChannelGroupedSales("catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_bill_addr_sk", "cs_ext_sales_price",
                        itemAttribute, itemValues, groupColumn, groupEncoding, year, month),
                unionChannelGroupedSales("web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_bill_addr_sk", "ws_ext_sales_price",
                        itemAttribute, itemValues, groupColumn, groupEncoding, year, month));
    }

    public static MultiStage query92()
    {
        return excessDiscountSum("web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_ext_discount_amt", 350);
    }

    public static MultiStage query32()
    {
        return excessDiscountSum("catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_ext_discount_amt", 977);
    }

    /**
     * Q92/Q32 shape, decorrelated: stage A computes, per item, the rounded average discount over a 90-day window;
     * stage B sums the discounts that exceed 1.3x their item's average (tested fraction-free as 13*avg < 10*discount).
     * The subquery's per-item averages join stage B as a dimension -- the pipeline-breaker handoff.
     */
    private static MultiStage excessDiscountSum(String salesTable, String soldDate, String itemKey, String discount, long manufactId)
    {
        QueryLowering subquery = QueryLowering.scan(salesTable,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(itemKey, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(discount, ColumnEncoding.FLAT, true))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date"));
        subquery.where(
                        new Plan.Predicate(">", subquery.column("d_date"), new Plan.Lit(10_982)),
                        new Plan.Predicate("<", subquery.column("d_date"), new Plan.Lit(11_074)))
                .groupBy(itemKey)
                .aggregate("sum", discount)
                .aggregate("count", discount)   // count of non-null discounts (CountColumn)
                // SELECT item, round(sum / count) -- the per-item average, matching divide_round_i64.
                .select(new Plan.Col(0), new Plan.Call("divide_round_i64", new Plan.Col(1), new Plan.Col(2)));

        QueryLowering main = QueryLowering.scan(salesTable,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(itemKey, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(discount, ColumnEncoding.FLAT, true))
                .join("item", itemKey, "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manufact_id"))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date"))
                .join("__item_averages__", itemKey, "ia_item",
                        new QueryLowering.Column("ia_item"),
                        new QueryLowering.Column("ia_average"));
        main.where(
                        new Plan.Predicate("=", main.column("i_manufact_id"), new Plan.Lit(manufactId)),
                        new Plan.Predicate(">", main.column("d_date"), new Plan.Lit(10_982)),
                        new Plan.Predicate("<", main.column("d_date"), new Plan.Lit(11_074)),
                        new Plan.Predicate("<",
                                new Plan.Bin("*", main.column("ia_average"), new Plan.Lit(13)),
                                new Plan.Bin("*", main.column(discount), new Plan.Lit(10))))
                .aggregate("sum", discount);
        return new MultiStage(subquery, main, "__item_averages__", List.of());
    }

    /** One stage of a multi-stage operator tree: a lowering plus the virtual-table name its materialized output is exposed under. */
    public record Stage(QueryLowering plan, String virtualName) {}

    /**
     * A multi-stage query as a tree of compiled pipelines (the shape the planner already produced). {@code stages}
     * run in order, each materialized under its {@link Stage#virtualName} and visible to later stages and {@code main}
     * by that name; a subtree shared in the logical plan is listed twice (recomputed, since the engine has no reuse).
     */
    public record Composite(List<Stage> stages, QueryLowering main, List<DictRef> stringColumns) {}

    public static Composite query65()
    {
        // Per-store low-revenue items: items whose store revenue is <= 10% of that store's average item revenue.
        // The store-item revenue subtree feeds both the per-store average AND the detail join, so (no reuse) it is
        // assembled twice -- exactly as the Trino/Nitro operator trees do.
        QueryLowering thresholds = QueryLowering.scan("q65_sales_for_average",
                        new QueryLowering.Column("sis_store"),
                        new QueryLowering.Column("sis_item"),
                        new QueryLowering.Column("sis_revenue", ColumnEncoding.FLAT, true))
                .groupBy("sis_store")
                .count()
                .aggregate("sum", "sis_revenue");
        // result columns: (store=0, count=1, sum=2) -> (store, round(sum / count)) = per-store average revenue.
        thresholds.select(new Plan.Col(0), new Plan.Call("divide_round_i64", new Plan.Col(2), new Plan.Col(1)));

        QueryLowering main = QueryLowering.scan("q65_sales_detail",
                        new QueryLowering.Column("sis_store"),
                        new QueryLowering.Column("sis_item"),
                        new QueryLowering.Column("sis_revenue", ColumnEncoding.FLAT, true))
                .join("q65_thresholds", "sis_store", "thr_store",
                        new QueryLowering.Column("thr_store"),
                        new QueryLowering.Column("thr_average"))   // computed (round(sum/count)) -> never null
                .join("store", "sis_store", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, true))
                .join("item", "sis_item", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_desc", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_current_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_wholesale_cost", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, true));
        main.where(new Plan.Predicate("<=",
                new Plan.Bin("*", main.column("sis_revenue"), new Plan.Lit(10)),
                main.column("thr_average")));
        main.select(main.column("s_store_name"), main.column("i_item_desc"), main.column("sis_revenue"),
                main.column("i_current_price"), main.column("i_wholesale_cost"), main.column("i_brand"));
        main.orderBy(new Plan.Ordering(List.of(
                new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(5, false), new Plan.SortKey(2, false)), 100));

        List<Stage> stages = List.of(
                new Stage(query65StoreItemSales(), "q65_sales_detail"),
                new Stage(query65StoreItemSales(), "q65_sales_for_average"),
                new Stage(thresholds, "q65_thresholds"));
        // Output strings: s_store_name from main input 2 (store) col 1; i_item_desc and i_brand from input 3 (item) cols 1 and 4.
        return new Composite(stages, main, List.of(new DictRef(0, 2, 1), new DictRef(1, 3, 1), new DictRef(5, 3, 4)));
    }

    public static Composite query01()
    {
        // Customers in TN stores whose store return total exceeds 1.2x the store's average customer return total.
        // ctr = per-(customer,store) sum of returns (null when that pair has only null amounts). Like Trino's plan,
        // the threshold subquery is a per-store AVERAGE (avg of the non-null ctr; avg of a decimal(.,2) rounds to
        // cents, i.e. divide_round_i64(sum, count)), and the main keeps ctr1.total > 1.2*avg -- which at scale 3 is
        // exactly 5*total > 6*avg. Stores whose ctr is entirely null have no average (HAVING count > 0); the inner
        // join then drops their detail rows, matching Trino's left-join-then-null-filter.
        QueryLowering thresholds = QueryLowering.scan("q01_ctr_for_totals",
                        new QueryLowering.Column("ctr_customer"),
                        new QueryLowering.Column("ctr_store"),
                        new QueryLowering.Column("ctr_total", ColumnEncoding.FLAT, true))
                .groupBy("ctr_store")
                .aggregate("sum", "ctr_total")
                .aggregate("count", "ctr_total");   // count of non-null ctr (null inputs are guarded out)
        thresholds.having(new Plan.Predicate(">", new Plan.Col(2), new Plan.Lit(0)));
        // result columns (store=0, sum=1, count=2) -> (store, round(sum / count)) = the per-store average return.
        thresholds.select(new Plan.Col(0), new Plan.Call("divide_round_i64", new Plan.Col(1), new Plan.Col(2)));

        QueryLowering main = QueryLowering.scan("q01_ctr_detail",
                        new QueryLowering.Column("d_customer"),
                        new QueryLowering.Column("d_store"),
                        new QueryLowering.Column("d_total", ColumnEncoding.FLAT, true))
                .join("store", "d_store", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_state", ColumnEncoding.STRING, true))
                .join("customer", "d_customer", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_customer_id", ColumnEncoding.STRING, false))
                .leftJoin("q01_store_totals", "d_store", "st_store",
                        new QueryLowering.Column("st_store"),
                        new QueryLowering.Column("st_average"));   // left join: a store with no average reads NULL (and is filtered out)
        main.where(
                new Plan.StringMatch(main.position("s_state"), List.of("TN"), false),
                new Plan.Predicate("<",
                        new Plan.Bin("*", main.column("st_average"), new Plan.Lit(6)),
                        new Plan.Bin("*", main.column("d_total"), new Plan.Lit(5))));
        main.select(main.column("c_customer_id"));
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 100));

        List<Stage> stages = List.of(
                new Stage(query01CustomerStoreReturns(), "q01_ctr_detail"),
                new Stage(query01CustomerStoreReturns(), "q01_ctr_for_totals"),
                new Stage(thresholds, "q01_store_totals"));
        // Output column 0 (c_customer_id) is a dictionary string from main input 2 (customer), column 1.
        return new Composite(stages, main, List.of(new DictRef(0, 2, 1)));
    }

    /** Per-(customer,store) return total over year 2000 -- the shared Q1 subtree (assembled per use). */
    private static QueryLowering query01CustomerStoreReturns()
    {
        QueryLowering returns = QueryLowering.scan("store_returns",
                        new QueryLowering.Column("sr_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_return_amt", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_returned_date_sk", ColumnEncoding.FLAT, true))
                .join("date_dim", "sr_returned_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"));
        returns.where(new Plan.Predicate("=", returns.column("d_year"), new Plan.Lit(2000)))
                .groupBy("sr_customer_sk", "sr_store_sk")
                .aggregate("sum", "sr_return_amt");
        return returns;
    }

    /** Store revenue per (store, item) over month_seq 1176..1187 -- the shared Q65 subtree (assembled per use). */
    private static QueryLowering query65StoreItemSales()
    {
        QueryLowering sales = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_month_seq"));
        sales.where(
                        new Plan.Predicate(">", sales.column("d_month_seq"), new Plan.Lit(1175)),
                        new Plan.Predicate("<", sales.column("d_month_seq"), new Plan.Lit(1188)))
                .groupBy("ss_store_sk", "ss_item_sk")
                .aggregate("sum", "ss_sales_price");
        return sales;
    }

    public static Ported query37()
    {
        return inventorySalesItems("catalog_sales", "cs_item_sk",
                LocalDate.of(2000, 2, 1), 68_00L, 98_00L, 677L, 940L, 694L, 808L);
    }

    public static Ported query82()
    {
        return inventorySalesItems("store_sales", "ss_item_sk",
                LocalDate.of(2000, 5, 25), 62_00L, 92_00L, 129L, 270L, 821L, 423L);
    }

    /**
     * Q37/Q82: {@code SELECT DISTINCT i_item_id, i_item_desc, i_current_price} over items priced in a range and from
     * one of {@code manufacturerIds}, on hand (inv_quantity_on_hand 100..500) during a 60-day window and sold
     * ({@code item_sk IN (SELECT item_sk FROM sales)}). Trino's plan is a single four-way inner join of
     * item / inventory / date_dim / sales whose GROUP BY does the DISTINCT -- there is no separate distinct
     * subquery -- so this mirrors that: inventory is streamed as the probe and item (its selective filter prunes the
     * fact at the join), date_dim, and the sales fact are joined as builds, then GROUP BY the item attributes.
     */
    private static Ported inventorySalesItems(String salesTable, String salesItem,
            LocalDate start, long minimumPrice, long maximumPrice, long... manufacturerIds)
    {
        QueryLowering query = QueryLowering.scan("inventory",
                        new QueryLowering.Column("inv_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_quantity_on_hand", ColumnEncoding.FLAT, true))
                .join("item", "inv_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_item_desc", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_current_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_manufact_id", ColumnEncoding.FLAT, true))
                .join("date_dim", "inv_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true))
                .join(salesTable, "inv_item_sk", salesItem,
                        new QueryLowering.Column(salesItem));   // raw join on item_sk; the GROUP BY does the DISTINCT
        List<Plan.Condition> manufacturers = new ArrayList<>();
        for (long id : manufacturerIds) {
            manufacturers.add(new Plan.Predicate("=", query.column("i_manufact_id"), new Plan.Lit(id)));
        }
        query.where(
                        new Plan.Predicate(">=", query.column("inv_quantity_on_hand"), new Plan.Lit(100)),
                        new Plan.Predicate("<=", query.column("inv_quantity_on_hand"), new Plan.Lit(500)),
                        new Plan.Predicate(">=", query.column("i_current_price"), new Plan.Lit(minimumPrice)),
                        new Plan.Predicate("<=", query.column("i_current_price"), new Plan.Lit(maximumPrice)),
                        new Plan.Predicate(">=", query.column("d_date"), new Plan.Lit(start.toEpochDay())),
                        new Plan.Predicate("<=", query.column("d_date"), new Plan.Lit(start.plusDays(60).toEpochDay())),
                        new Plan.Or(manufacturers))
                .groupBy("i_item_id", "i_item_desc", "i_current_price")
                .count();   // grouping is the DISTINCT; the count is dropped by the SELECT below
        query.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2));
        query.orderBy(new Plan.Ordering(List.of(
                new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)), 100));
        // Output columns 0 (i_item_id) and 1 (i_item_desc) are dictionary strings from input 1 (item), cols 1 and 2.
        return new Ported(query, List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2)));
    }

    public static MultiStage query34()
    {
        // Stage A: store_sales JOIN date_dim/store/household_demographics, GROUP BY (ticket, customer), count(*),
        // HAVING count between 15 and 20. Stage B (projection-only): join the customer dimension and emit the
        // customer identity + ticket + count, ORDER BY name LIMIT 100. The grouped counts feed stage B as its probe.
        QueryLowering subquery = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_hdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_dom"),
                        new QueryLowering.Column("d_year"))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_county", ColumnEncoding.STRING, false))
                .join("household_demographics", "ss_hdemo_sk", "hd_demo_sk",
                        new QueryLowering.Column("hd_demo_sk"),
                        new QueryLowering.Column("hd_buy_potential", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("hd_vehicle_count"),
                        new QueryLowering.Column("hd_dep_count"));
        subquery.where(
                        new Plan.Or(List.of(
                                new Plan.And(new Plan.Predicate(">", subquery.column("d_dom"), new Plan.Lit(0)),
                                        new Plan.Predicate("<", subquery.column("d_dom"), new Plan.Lit(4))),
                                new Plan.And(new Plan.Predicate(">", subquery.column("d_dom"), new Plan.Lit(24)),
                                        new Plan.Predicate("<", subquery.column("d_dom"), new Plan.Lit(29))))),
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", subquery.column("d_year"), new Plan.Lit(1999)),
                                new Plan.Predicate("=", subquery.column("d_year"), new Plan.Lit(2000)),
                                new Plan.Predicate("=", subquery.column("d_year"), new Plan.Lit(2001)))),
                        new Plan.StringMatch(subquery.position("s_county"), List.of("Williamson County"), false),
                        new Plan.StringMatch(subquery.position("hd_buy_potential"), List.of(">10000", "Unknown"), false),
                        new Plan.Predicate(">", subquery.column("hd_vehicle_count"), new Plan.Lit(0)),
                        new Plan.Predicate("<",
                                new Plan.Bin("*", subquery.column("hd_vehicle_count"), new Plan.Lit(12)),
                                new Plan.Bin("*", subquery.column("hd_dep_count"), new Plan.Lit(10))))
                .groupBy("ss_ticket_number", "ss_customer_sk")
                .count()
                .having(new Plan.And(
                        new Plan.Predicate(">", new Plan.Col(2), new Plan.Lit(14)),
                        new Plan.Predicate("<", new Plan.Col(2), new Plan.Lit(21))));

        QueryLowering main = QueryLowering.scan("__q34_groups__",
                        new QueryLowering.Column("g_ticket"),
                        new QueryLowering.Column("g_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_count"))
                .join("customer", "g_customer", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_salutation", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_preferred_cust_flag", ColumnEncoding.STRING, true));
        // SELECT last, first, salutation, preferred_flag, ticket, count (combined columns: probe 0-2, customer 3-7).
        main.select(new Plan.Col(4), new Plan.Col(5), new Plan.Col(6), new Plan.Col(7), new Plan.Col(0), new Plan.Col(2))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false),
                        new Plan.SortKey(3, true), new Plan.SortKey(4, false)), 100));
        return new MultiStage(subquery, main, "__q34_groups__",
                List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 1, 3), new DictRef(3, 1, 4)));
    }

    public static MultiStage query73()
    {
        // Same multi-stage shape as Q34 with different filters: store_sales grouped by (ticket, customer), counts
        // kept in [1,5], then the customer identity joined and ordered by count DESC, last name, ticket.
        QueryLowering subquery = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_hdemo_sk", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_dom"),
                        new QueryLowering.Column("d_year"))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_county", ColumnEncoding.STRING, false))
                .join("household_demographics", "ss_hdemo_sk", "hd_demo_sk",
                        new QueryLowering.Column("hd_demo_sk"),
                        new QueryLowering.Column("hd_buy_potential", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("hd_vehicle_count"),
                        new QueryLowering.Column("hd_dep_count"));
        subquery.where(
                        new Plan.Predicate(">", subquery.column("d_dom"), new Plan.Lit(0)),
                        new Plan.Predicate("<", subquery.column("d_dom"), new Plan.Lit(3)),
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", subquery.column("d_year"), new Plan.Lit(1999)),
                                new Plan.Predicate("=", subquery.column("d_year"), new Plan.Lit(2000)),
                                new Plan.Predicate("=", subquery.column("d_year"), new Plan.Lit(2001)))),
                        new Plan.StringMatch(subquery.position("s_county"),
                                List.of("Williamson County", "Franklin Parish", "Bronx County", "Orange County"), false),
                        new Plan.StringMatch(subquery.position("hd_buy_potential"), List.of(">10000", "Unknown"), false),
                        new Plan.Predicate(">", subquery.column("hd_vehicle_count"), new Plan.Lit(0)),
                        new Plan.Predicate("<", subquery.column("hd_vehicle_count"), subquery.column("hd_dep_count")))
                .groupBy("ss_ticket_number", "ss_customer_sk")
                .count()
                .having(new Plan.And(
                        new Plan.Predicate(">", new Plan.Col(2), new Plan.Lit(0)),
                        new Plan.Predicate("<", new Plan.Col(2), new Plan.Lit(6))));

        QueryLowering main = QueryLowering.scan("__q73_groups__",
                        new QueryLowering.Column("g_ticket"),
                        new QueryLowering.Column("g_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_count"))
                .join("customer", "g_customer", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_salutation", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_preferred_cust_flag", ColumnEncoding.STRING, true));
        // SELECT last, first, salutation, preferred_flag, ticket, count; ORDER BY count DESC, last, ticket.
        main.select(new Plan.Col(4), new Plan.Col(5), new Plan.Col(6), new Plan.Col(7), new Plan.Col(0), new Plan.Col(2))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(5, true), new Plan.SortKey(0, false), new Plan.SortKey(4, false)), 100));
        return new MultiStage(subquery, main, "__q73_groups__",
                List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 1, 3), new DictRef(3, 1, 4)));
    }

    public static Ported query43()
    {
        // store_sales JOIN date_dim(d_year=2000) JOIN store(s_gmt_offset=-5, stored as the scaled decimal -500);
        // GROUP BY s_store_name, s_store_id; one sum(CASE WHEN d_day_name = <day> THEN ss_sales_price ELSE 0) per
        // day of week; ORDER BY all nine output columns LIMIT 100. The CASE conditions are predicate-over-dictionary
        // string matches on d_day_name -- the day-of-week pivot.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_day_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("d_year"))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_gmt_offset"),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_store_id", ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2000)),
                        new Plan.Predicate("=", query.column("s_gmt_offset"), new Plan.Lit(-500)))
                .groupBy("s_store_name", "s_store_id");
        for (String day : List.of("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")) {
            query.aggregate("sum", new Plan.Case(
                    List.of(new Plan.Case.Branch(
                            new Plan.StringMatch(query.position("d_day_name"), List.of(day), false),
                            query.column("ss_sales_price"))),
                    new Plan.Lit(0)));
        }
        query.orderBy(new Plan.Ordering(List.of(
                new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false), new Plan.SortKey(3, false),
                new Plan.SortKey(4, false), new Plan.SortKey(5, false), new Plan.SortKey(6, false), new Plan.SortKey(7, false),
                new Plan.SortKey(8, false)), 100));
        return new Ported(query, List.of(new DictRef(0, 2, 2), new DictRef(1, 2, 3)));
    }

    public static Ported query13()
    {
        // store_sales JOIN store, customer_demographics, household_demographics, customer_address(US),
        // date_dim(d_year=2001); global avg(ss_quantity) + sum/count(ss_ext_sales_price) + sum/count
        // (ss_ext_wholesale_cost) over rows passing a 3-disjunct demographics predicate (marital/education/
        // sales_price/dep_count) AND a 3-disjunct state/net_profit predicate. The harness emits raw sum+count so the
        // SQL avgs are derived downstream; matched here aggregate-for-aggregate.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_cdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_hdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_quantity", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ext_wholesale_cost", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_net_profit", ColumnEncoding.FLAT, true))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"))
                .join("customer_demographics", "ss_cdemo_sk", "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("cd_education_status", ColumnEncoding.STRING, false))
                .join("household_demographics", "ss_hdemo_sk", "hd_demo_sk",
                        new QueryLowering.Column("hd_demo_sk"),
                        new QueryLowering.Column("hd_dep_count"))
                .join("customer_address", "ss_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_country", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, false))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"));
        query.where(
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2001)),
                        new Plan.StringMatch(query.position("ca_country"), List.of("United States"), false),
                        new Plan.Or(
                                new Plan.And(
                                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("M"), false),
                                        new Plan.StringMatch(query.position("cd_education_status"), List.of("Advanced Degree     "), false),
                                        new Plan.Predicate(">=", query.column("ss_sales_price"), new Plan.Lit(10_000)),
                                        new Plan.Predicate("<=", query.column("ss_sales_price"), new Plan.Lit(15_000)),
                                        new Plan.Predicate("=", query.column("hd_dep_count"), new Plan.Lit(3))),
                                new Plan.And(
                                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("S"), false),
                                        new Plan.StringMatch(query.position("cd_education_status"), List.of("College             "), false),
                                        new Plan.Predicate(">=", query.column("ss_sales_price"), new Plan.Lit(5_000)),
                                        new Plan.Predicate("<=", query.column("ss_sales_price"), new Plan.Lit(10_000)),
                                        new Plan.Predicate("=", query.column("hd_dep_count"), new Plan.Lit(1))),
                                new Plan.And(
                                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("W"), false),
                                        new Plan.StringMatch(query.position("cd_education_status"), List.of("2 yr Degree         "), false),
                                        new Plan.Predicate(">=", query.column("ss_sales_price"), new Plan.Lit(15_000)),
                                        new Plan.Predicate("<=", query.column("ss_sales_price"), new Plan.Lit(20_000)),
                                        new Plan.Predicate("=", query.column("hd_dep_count"), new Plan.Lit(1)))),
                        new Plan.Or(
                                new Plan.And(
                                        new Plan.StringMatch(query.position("ca_state"), List.of("TX", "OH"), false),
                                        new Plan.Predicate(">=", query.column("ss_net_profit"), new Plan.Lit(10_000)),
                                        new Plan.Predicate("<=", query.column("ss_net_profit"), new Plan.Lit(20_000))),
                                new Plan.And(
                                        new Plan.StringMatch(query.position("ca_state"), List.of("OR", "NM", "KY"), false),
                                        new Plan.Predicate(">=", query.column("ss_net_profit"), new Plan.Lit(15_000)),
                                        new Plan.Predicate("<=", query.column("ss_net_profit"), new Plan.Lit(30_000))),
                                new Plan.And(
                                        new Plan.StringMatch(query.position("ca_state"), List.of("VA", "TX", "MS"), false),
                                        new Plan.Predicate(">=", query.column("ss_net_profit"), new Plan.Lit(5_000)),
                                        new Plan.Predicate("<=", query.column("ss_net_profit"), new Plan.Lit(25_000)))))
                .aggregate("avg", "ss_quantity")
                .aggregate("avg", "ss_ext_sales_price")
                .aggregate("avg", "ss_ext_wholesale_cost")
                .aggregate("sum", "ss_ext_wholesale_cost");
        return new Ported(query, -1, 0, 0);
    }

    public static Ported query48()
    {
        // store_sales JOIN store, customer_demographics, customer_address(ca_country='United States'),
        // date_dim(d_year=2000); global sum(ss_quantity) over rows passing a 3-disjunct demographics predicate
        // (marital/education/sales_price) AND a 3-disjunct state/net_profit predicate. Each disjunct mixes a fact
        // measure with dimension columns, so those ORs stay as WHERE; d_year and ca_country push into their builds.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_cdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_quantity", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_net_profit", ColumnEncoding.FLAT, true))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"))
                .join("customer_demographics", "ss_cdemo_sk", "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("cd_education_status", ColumnEncoding.STRING, false))
                .join("customer_address", "ss_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_country", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, false))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"));
        query.where(
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2000)),
                        new Plan.StringMatch(query.position("ca_country"), List.of("United States"), false),
                        new Plan.Or(
                                new Plan.And(
                                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("M"), false),
                                        new Plan.StringMatch(query.position("cd_education_status"), List.of("4 yr Degree         "), false),
                                        new Plan.Predicate(">=", query.column("ss_sales_price"), new Plan.Lit(10_000)),
                                        new Plan.Predicate("<=", query.column("ss_sales_price"), new Plan.Lit(15_000))),
                                new Plan.And(
                                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("D"), false),
                                        new Plan.StringMatch(query.position("cd_education_status"), List.of("2 yr Degree         "), false),
                                        new Plan.Predicate(">=", query.column("ss_sales_price"), new Plan.Lit(5_000)),
                                        new Plan.Predicate("<=", query.column("ss_sales_price"), new Plan.Lit(10_000))),
                                new Plan.And(
                                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("S"), false),
                                        new Plan.StringMatch(query.position("cd_education_status"), List.of("College             "), false),
                                        new Plan.Predicate(">=", query.column("ss_sales_price"), new Plan.Lit(15_000)),
                                        new Plan.Predicate("<=", query.column("ss_sales_price"), new Plan.Lit(20_000)))),
                        new Plan.Or(
                                new Plan.And(
                                        new Plan.StringMatch(query.position("ca_state"), List.of("CO", "OH", "TX"), false),
                                        new Plan.Predicate(">=", query.column("ss_net_profit"), new Plan.Lit(0)),
                                        new Plan.Predicate("<=", query.column("ss_net_profit"), new Plan.Lit(200_000))),
                                new Plan.And(
                                        new Plan.StringMatch(query.position("ca_state"), List.of("OR", "MN", "KY"), false),
                                        new Plan.Predicate(">=", query.column("ss_net_profit"), new Plan.Lit(15_000)),
                                        new Plan.Predicate("<=", query.column("ss_net_profit"), new Plan.Lit(300_000))),
                                new Plan.And(
                                        new Plan.StringMatch(query.position("ca_state"), List.of("VA", "CA", "MS"), false),
                                        new Plan.Predicate(">=", query.column("ss_net_profit"), new Plan.Lit(5_000)),
                                        new Plan.Predicate("<=", query.column("ss_net_profit"), new Plan.Lit(2_500_000)))))
                .aggregate("sum", "ss_quantity");
        return new Ported(query, -1, 0, 0);
    }

    public static Ported query62()
    {
        return shippingDelayBuckets("web_sales", "ws_ship_date_sk", "ws_sold_date_sk", "ws_warehouse_sk",
                "ws_ship_mode_sk", "ws_web_site_sk", "web_site", "web_site_sk", "web_name");
    }

    public static Ported query99()
    {
        return shippingDelayBuckets("catalog_sales", "cs_ship_date_sk", "cs_sold_date_sk", "cs_warehouse_sk",
                "cs_ship_mode_sk", "cs_call_center_sk", "call_center", "cc_call_center_sk", "cc_name");
    }

    /**
     * Q62/Q99 shape: a sales table joined to date_dim (d_month_seq in [1200,1211]), warehouse, ship_mode, and a
     * third dimension (web_site / call_center); GROUP BY three dimension names; five {@code sum(CASE)} counts
     * bucketing the shipping delay {@code days = ship_date_sk - sold_date_sk} into 0-30 / 31-60 / 61-90 / 91-120 /
     * >120; ORDER BY the three names LIMIT 100. The warehouse name is genuinely NULL for one warehouse, so the
     * string group keys are declared nullable (a null name forms its own group, kept distinct from any real name),
     * and {@code days} is null when sold_date is null, falling through every bucket's {@code CASE} to 0.
     */
    private static Ported shippingDelayBuckets(String salesTable, String shipDate, String soldDate, String warehouseFk,
            String shipModeFk, String thirdFk, String thirdTable, String thirdKey, String thirdName)
    {
        QueryLowering query = QueryLowering.scan(salesTable,
                        new QueryLowering.Column(shipDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(warehouseFk, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(shipModeFk, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(thirdFk, ColumnEncoding.FLAT, true))
                .join("date_dim", shipDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_month_seq"))
                .join("warehouse", warehouseFk, "w_warehouse_sk",
                        new QueryLowering.Column("w_warehouse_sk"),
                        new QueryLowering.Column("w_warehouse_name", ColumnEncoding.STRING, true))
                .join("ship_mode", shipModeFk, "sm_ship_mode_sk",
                        new QueryLowering.Column("sm_ship_mode_sk"),
                        new QueryLowering.Column("sm_type", ColumnEncoding.STRING, true))
                .join(thirdTable, thirdFk, thirdKey,
                        new QueryLowering.Column(thirdKey),
                        new QueryLowering.Column(thirdName, ColumnEncoding.STRING, true));
        Plan.Expr days = new Plan.Bin("-", query.column(shipDate), query.column(soldDate));
        query.where(
                        new Plan.Predicate(">", query.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", query.column("d_month_seq"), new Plan.Lit(1212)))
                .groupBy("w_warehouse_name", "sm_type", thirdName)
                .aggregate("sum", bucket(new Plan.Predicate("<", days, new Plan.Lit(31))))
                .aggregate("sum", bucket(new Plan.And(
                        new Plan.Predicate(">", days, new Plan.Lit(30)), new Plan.Predicate("<", days, new Plan.Lit(61)))))
                .aggregate("sum", bucket(new Plan.And(
                        new Plan.Predicate(">", days, new Plan.Lit(60)), new Plan.Predicate("<", days, new Plan.Lit(91)))))
                .aggregate("sum", bucket(new Plan.And(
                        new Plan.Predicate(">", days, new Plan.Lit(90)), new Plan.Predicate("<", days, new Plan.Lit(121)))))
                .aggregate("sum", bucket(new Plan.Predicate(">", days, new Plan.Lit(120))))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)), 100));
        return new Ported(query, List.of(new DictRef(0, 2, 1), new DictRef(1, 3, 1), new DictRef(2, 4, 1)));
    }

    /** {@code CASE WHEN condition THEN 1 ELSE 0 END} -- a 0/1 indicator for one shipping-delay bucket. */
    private static Plan.Expr bucket(Plan.Condition condition)
    {
        return new Plan.Case(List.of(new Plan.Case.Branch(condition, new Plan.Lit(1))), new Plan.Lit(0));
    }

    public static Ported query96()
    {
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_time_sk"),
                        new QueryLowering.Column("ss_hdemo_sk"),
                        new QueryLowering.Column("ss_store_sk"))
                .join("time_dim", "ss_sold_time_sk", "t_time_sk",
                        new QueryLowering.Column("t_time_sk"),
                        new QueryLowering.Column("t_hour"),
                        new QueryLowering.Column("t_minute"))
                .join("household_demographics", "ss_hdemo_sk", "hd_demo_sk",
                        new QueryLowering.Column("hd_demo_sk"),
                        new QueryLowering.Column("hd_dep_count"))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, false));
        query.where(
                        new Plan.Predicate("=", query.column("t_hour"), new Plan.Lit(20)),
                        new Plan.Predicate(">", query.column("t_minute"), new Plan.Lit(29)),
                        new Plan.Predicate("=", query.column("hd_dep_count"), new Plan.Lit(7)),
                        new Plan.StringMatch(query.position("s_store_name"), List.of("ese"), false))
                .count();
        return new Ported(query, -1, 0, 0);
    }

    public static Ported query27()
    {
        // store_sales ⋈ date_dim(d_year=2002) ⋈ item ⋈ store(s_state='TN') ⋈ customer_demographics(M/S/College);
        // GROUP BY ROLLUP(i_item_id, s_state); four averages; the trailing grouping_id (= GROUPING(s_state)) is the
        // g_state column. ORDER BY i_item_id, s_state; top 100. ROLLUP nulls s_state at the by-item level, so its
        // string output column reconstructs through its null mask.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_cdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_quantity", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_list_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_coupon_amt", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_state", ColumnEncoding.STRING, false))
                .join("customer_demographics", "ss_cdemo_sk", "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_gender", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("cd_education_status", ColumnEncoding.STRING, false));
        query.where(
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2002)),
                        new Plan.StringMatch(query.position("s_state"), List.of("TN"), false),
                        new Plan.StringMatch(query.position("cd_gender"), List.of("M"), false),
                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("S"), false),
                        new Plan.StringMatch(query.position("cd_education_status"), List.of("College"), false))
                .groupBy("i_item_id", "s_state")
                .groupingSets(List.of(new int[] {0, 1}, new int[] {0}))
                .aggregate("avg", "ss_quantity")
                .aggregate("avg", "ss_list_price")
                .aggregate("avg", "ss_coupon_amt")
                .aggregate("avg", "ss_sales_price")
                .select(
                        new Plan.Col(0),    // i_item_id
                        new Plan.Col(1),    // s_state
                        new Plan.Col(6),    // grouping_id = GROUPING(s_state) = g_state
                        new Plan.Col(2),    // avg(ss_quantity)
                        new Plan.Col(3),    // avg(ss_list_price)
                        new Plan.Col(4),    // avg(ss_coupon_amt)
                        new Plan.Col(5))    // avg(ss_sales_price)
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100));
        return new Ported(query, List.of(new DictRef(0, 2, 1), new DictRef(1, 3, 1)));
    }
}
