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
import org.weakref.nitro.jit.Types;

import java.time.LocalDate;
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

    public static Ported query19()
    {
        // store_sales ⋈ date_dim(d_moy=11, d_year=1998) ⋈ item(i_manager_id=8) ⋈ customer ⋈ customer_address (on the
        // customer's current address) ⋈ store. Keep only rows where the customer's zip and the store's zip differ in
        // their 5-char prefix (substr(ca_zip,1,5) <> substr(s_zip,1,5)); GROUP BY brand/manufacturer; sum ext sales
        // price; ORDER BY sum DESC then the keys; top 100. Exercises the substring string-column compare across two
        // dimension joins, plus two dictionary-string group keys (i_brand, i_manufact).
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk"),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manager_id"),
                        new QueryLowering.Column("i_brand_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_manufact_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_manufact", ColumnEncoding.STRING, true))
                .join("customer", "ss_customer_sk", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_zip", ColumnEncoding.STRING, true))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_zip", ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(11)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(1998)),
                        new Plan.Predicate("=", query.column("i_manager_id"), new Plan.Lit(8)),
                        new Plan.StringColumnCompare(query.position("ca_zip"), query.position("s_zip"), true, 1, 5))
                .groupBy("i_brand_id", "i_brand", "i_manufact_id", "i_manufact")
                .aggregate("sum", "ss_ext_sales_price")
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(4, true), new Plan.SortKey(1, false), new Plan.SortKey(0, false),
                        new Plan.SortKey(2, false), new Plan.SortKey(3, false)), 100));
        return new Ported(query, List.of(new DictRef(1, 2, 3), new DictRef(3, 2, 5)));
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
        List<QueryLowering> branches = List.of(
                unionChannelGroupedSales("store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_addr_sk", "ss_ext_sales_price",
                        "i_category", List.of("Electronics"), "i_manufact_id", ColumnEncoding.FLAT, 1998, 5),
                unionChannelGroupedSales("catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_bill_addr_sk", "cs_ext_sales_price",
                        "i_category", List.of("Electronics"), "i_manufact_id", ColumnEncoding.FLAT, 1998, 5),
                unionChannelGroupedSales("web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_bill_addr_sk", "ws_ext_sales_price",
                        "i_category", List.of("Electronics"), "i_manufact_id", ColumnEncoding.FLAT, 1998, 5));
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
        List<QueryLowering> branches = List.of(
                unionChannelGroupedSales("store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_addr_sk", "ss_ext_sales_price",
                        "i_color", List.of("slate", "blanched", "burnished"), "i_item_id", ColumnEncoding.STRING, 2001, 2),
                unionChannelGroupedSales("catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_bill_addr_sk", "cs_ext_sales_price",
                        "i_color", List.of("slate", "blanched", "burnished"), "i_item_id", ColumnEncoding.STRING, 2001, 2),
                unionChannelGroupedSales("web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_bill_addr_sk", "ws_ext_sales_price",
                        "i_color", List.of("slate", "blanched", "burnished"), "i_item_id", ColumnEncoding.STRING, 2001, 2));
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
        List<QueryLowering> branches = List.of(
                unionChannelGroupedSales("store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_addr_sk", "ss_ext_sales_price",
                        "i_category", List.of("Music"), "i_item_id", ColumnEncoding.STRING, 1998, 9),
                unionChannelGroupedSales("catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_bill_addr_sk", "cs_ext_sales_price",
                        "i_category", List.of("Music"), "i_item_id", ColumnEncoding.STRING, 1998, 9),
                unionChannelGroupedSales("web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_bill_addr_sk", "ws_ext_sales_price",
                        "i_category", List.of("Music"), "i_item_id", ColumnEncoding.STRING, 1998, 9));
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

    public static MultiStage query92()
    {
        // Q92, decorrelated: stage A computes, per item, the rounded average web_sales discount over a 90-day window;
        // stage B sums the discounts that exceed 1.3x their item's average (tested fraction-free as 13*avg < 10*discount)
        // for manufacturer 350. The subquery's per-item averages join stage B as a dimension -- the pipeline-breaker.
        QueryLowering subquery = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_ext_discount_amt", ColumnEncoding.FLAT, true))
                .join("date_dim", "ws_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date"));
        subquery.where(
                        new Plan.Predicate(">", subquery.column("d_date"), new Plan.Lit(10_982)),
                        new Plan.Predicate("<", subquery.column("d_date"), new Plan.Lit(11_074)))
                .groupBy("ws_item_sk")
                .aggregate("sum", "ws_ext_discount_amt")
                .aggregate("count", "ws_ext_discount_amt")   // count of non-null discounts (CountColumn)
                // SELECT item, round(sum / count) -- the per-item average, matching divide_round_i64.
                .select(new Plan.Col(0), new Plan.Call("divide_round_i64", new Plan.Col(1), new Plan.Col(2)));

        QueryLowering main = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_ext_discount_amt", ColumnEncoding.FLAT, true))
                .join("item", "ws_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manufact_id"))
                .join("date_dim", "ws_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date"))
                .join("__item_averages__", "ws_item_sk", "ia_item",
                        new QueryLowering.Column("ia_item"),
                        new QueryLowering.Column("ia_average"));
        main.where(
                        new Plan.Predicate("=", main.column("i_manufact_id"), new Plan.Lit(350)),
                        new Plan.Predicate(">", main.column("d_date"), new Plan.Lit(10_982)),
                        new Plan.Predicate("<", main.column("d_date"), new Plan.Lit(11_074)),
                        new Plan.Predicate("<",
                                new Plan.Bin("*", main.column("ia_average"), new Plan.Lit(13)),
                                new Plan.Bin("*", main.column("ws_ext_discount_amt"), new Plan.Lit(10))))
                .aggregate("sum", "ws_ext_discount_amt");
        return new MultiStage(subquery, main, "__item_averages__", List.of());
    }

    public static MultiStage query32()
    {
        // Q32, decorrelated: stage A computes, per item, the rounded average catalog_sales discount over a 90-day
        // window; stage B sums the discounts that exceed 1.3x their item's average (tested fraction-free as
        // 13*avg < 10*discount) for manufacturer 977. Same pipeline-breaker shape as Q92 over catalog_sales.
        QueryLowering subquery = QueryLowering.scan("catalog_sales",
                        new QueryLowering.Column("cs_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_ext_discount_amt", ColumnEncoding.FLAT, true))
                .join("date_dim", "cs_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date"));
        subquery.where(
                        new Plan.Predicate(">", subquery.column("d_date"), new Plan.Lit(10_982)),
                        new Plan.Predicate("<", subquery.column("d_date"), new Plan.Lit(11_074)))
                .groupBy("cs_item_sk")
                .aggregate("sum", "cs_ext_discount_amt")
                .aggregate("count", "cs_ext_discount_amt")   // count of non-null discounts (CountColumn)
                // SELECT item, round(sum / count) -- the per-item average, matching divide_round_i64.
                .select(new Plan.Col(0), new Plan.Call("divide_round_i64", new Plan.Col(1), new Plan.Col(2)));

        QueryLowering main = QueryLowering.scan("catalog_sales",
                        new QueryLowering.Column("cs_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_ext_discount_amt", ColumnEncoding.FLAT, true))
                .join("item", "cs_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manufact_id"))
                .join("date_dim", "cs_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date"))
                .join("__item_averages__", "cs_item_sk", "ia_item",
                        new QueryLowering.Column("ia_item"),
                        new QueryLowering.Column("ia_average"));
        main.where(
                        new Plan.Predicate("=", main.column("i_manufact_id"), new Plan.Lit(977)),
                        new Plan.Predicate(">", main.column("d_date"), new Plan.Lit(10_982)),
                        new Plan.Predicate("<", main.column("d_date"), new Plan.Lit(11_074)),
                        new Plan.Predicate("<",
                                new Plan.Bin("*", main.column("ia_average"), new Plan.Lit(13)),
                                new Plan.Bin("*", main.column("cs_ext_discount_amt"), new Plan.Lit(10))))
                .aggregate("sum", "cs_ext_discount_amt");
        return new MultiStage(subquery, main, "__item_averages__", List.of());
    }

    /**
     * One stage of a multi-stage operator tree: a lowering plus the virtual-table name its materialized output is
     * exposed under. {@code stringColumns} declares any STRING output columns of this stage (mapping a result column to
     * the stage's own input dictionary) so the stage materializes them as dictionary columns -- then a later stage that
     * passes a string group key through (rather than re-joining the base table) can still reconstruct it from the
     * virtual relation. Empty when the stage emits no strings.
     */
    public record Stage(QueryLowering plan, String virtualName, List<DictRef> stringColumns)
    {
        public Stage(QueryLowering plan, String virtualName)
        {
            this(plan, virtualName, List.of());
        }
    }

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
        // Q37: SELECT DISTINCT i_item_id, i_item_desc, i_current_price over catalog-sold items priced 68.00..98.00
        // from manufacturers {677,940,694,808}, on hand (inv_quantity_on_hand 100..500) during a 60-day window from
        // 2000-02-01, and sold (item_sk IN catalog_sales). Trino's plan is a single four-way inner join of
        // inventory / item / date_dim / catalog_sales whose GROUP BY does the DISTINCT -- there is no separate distinct
        // subquery -- so inventory is the probe and item / date_dim / catalog_sales are builds; GROUP BY item attrs.
        LocalDate start = LocalDate.of(2000, 2, 1);
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
                .join("catalog_sales", "inv_item_sk", "cs_item_sk",
                        new QueryLowering.Column("cs_item_sk"));   // raw join on item_sk; the GROUP BY does the DISTINCT
        query.where(
                        new Plan.Predicate(">=", query.column("inv_quantity_on_hand"), new Plan.Lit(100)),
                        new Plan.Predicate("<=", query.column("inv_quantity_on_hand"), new Plan.Lit(500)),
                        new Plan.Predicate(">=", query.column("i_current_price"), new Plan.Lit(68_00L)),
                        new Plan.Predicate("<=", query.column("i_current_price"), new Plan.Lit(98_00L)),
                        new Plan.Predicate(">=", query.column("d_date"), new Plan.Lit(start.toEpochDay())),
                        new Plan.Predicate("<=", query.column("d_date"), new Plan.Lit(start.plusDays(60).toEpochDay())),
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", query.column("i_manufact_id"), new Plan.Lit(677L)),
                                new Plan.Predicate("=", query.column("i_manufact_id"), new Plan.Lit(940L)),
                                new Plan.Predicate("=", query.column("i_manufact_id"), new Plan.Lit(694L)),
                                new Plan.Predicate("=", query.column("i_manufact_id"), new Plan.Lit(808L)))))
                .groupBy("i_item_id", "i_item_desc", "i_current_price")
                .count();   // grouping is the DISTINCT; the count is dropped by the SELECT below
        query.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2));
        query.orderBy(new Plan.Ordering(List.of(
                new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)), 100));
        // Output columns 0 (i_item_id) and 1 (i_item_desc) are dictionary strings from input 1 (item), cols 1 and 2.
        return new Ported(query, List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2)));
    }

    public static Ported query82()
    {
        // Q82: SELECT DISTINCT i_item_id, i_item_desc, i_current_price over store-sold items priced 62.00..92.00 from
        // manufacturers {129,270,821,423}, on hand (inv_quantity_on_hand 100..500) during a 60-day window from
        // 2000-05-25, and sold (item_sk IN store_sales). Same four-way inner join shape as Q37 (inventory probe;
        // item / date_dim / store_sales builds) whose GROUP BY does the DISTINCT.
        LocalDate start = LocalDate.of(2000, 5, 25);
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
                .join("store_sales", "inv_item_sk", "ss_item_sk",
                        new QueryLowering.Column("ss_item_sk"));   // raw join on item_sk; the GROUP BY does the DISTINCT
        query.where(
                        new Plan.Predicate(">=", query.column("inv_quantity_on_hand"), new Plan.Lit(100)),
                        new Plan.Predicate("<=", query.column("inv_quantity_on_hand"), new Plan.Lit(500)),
                        new Plan.Predicate(">=", query.column("i_current_price"), new Plan.Lit(62_00L)),
                        new Plan.Predicate("<=", query.column("i_current_price"), new Plan.Lit(92_00L)),
                        new Plan.Predicate(">=", query.column("d_date"), new Plan.Lit(start.toEpochDay())),
                        new Plan.Predicate("<=", query.column("d_date"), new Plan.Lit(start.plusDays(60).toEpochDay())),
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", query.column("i_manufact_id"), new Plan.Lit(129L)),
                                new Plan.Predicate("=", query.column("i_manufact_id"), new Plan.Lit(270L)),
                                new Plan.Predicate("=", query.column("i_manufact_id"), new Plan.Lit(821L)),
                                new Plan.Predicate("=", query.column("i_manufact_id"), new Plan.Lit(423L)))))
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

    public static Composite query89()
    {
        // Q89: monthly store_sales sums per (i_category, i_class, i_brand, s_store_name, s_company_name, d_moy) over
        // two item cohorts in 1999, then avg OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name);
        // keep months deviating >10% from that average, ORDER BY (sum - avg), s_store_name, top 100. The five string
        // group keys flow THROUGH the virtual table into the window stage (the main does not re-join item/store), so
        // the pre-aggregate stage declares them as string outputs to carry their dictionaries forward.
        QueryLowering monthly = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_class", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, false))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("s_company_name", ColumnEncoding.STRING, false));
        monthly.where(
                        new Plan.Predicate("=", monthly.column("d_year"), new Plan.Lit(1999)),
                        new Plan.Or(
                                new Plan.And(
                                        new Plan.StringMatch(monthly.position("i_category"), List.of("Books", "Electronics", "Sports"), false),
                                        new Plan.StringMatch(monthly.position("i_class"), List.of("computers", "stereo", "football"), false)),
                                new Plan.And(
                                        new Plan.StringMatch(monthly.position("i_category"), List.of("Men", "Jewelry", "Women"), false),
                                        new Plan.StringMatch(monthly.position("i_class"), List.of("shirts", "birdal", "dresses"), false))))
                .groupBy("i_category", "i_class", "i_brand", "s_store_name", "s_company_name", "d_moy")
                .aggregate("sum", "ss_sales_price");
        // Stage output: i_category(0), i_class(1), i_brand(2), s_store_name(3), s_company_name(4), d_moy(5), sum(6).
        // The string keys reconstruct from the stage's item build (input 1, cols 1-3) and store build (input 3, cols 1-2).
        List<DictRef> stageStrings = List.of(
                new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 1, 3),
                new DictRef(3, 3, 1), new DictRef(4, 3, 2));

        QueryLowering main = QueryLowering.scan("q89_monthly",
                        new QueryLowering.Column("ms_category", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("ms_class", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("ms_brand", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("ms_store_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("ms_company_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("ms_moy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("ms_sum", ColumnEncoding.FLAT, false))
                .window(Plan.Window.partitionAverage(new int[] {0, 2, 3, 4}, 6));
        // Window output: category(0), class(1), brand(2), store_name(3), company_name(4), moy(5), sum(6), avg(7).
        Plan.Expr difference = new Plan.Bin("-", new Plan.Col(6), new Plan.Col(7));
        main.having(new Plan.And(
                        new Plan.Predicate(">", new Plan.Col(7), new Plan.Lit(0)),
                        new Plan.Or(
                                new Plan.Predicate(">", new Plan.Bin("*", new Plan.Bin("-", new Plan.Col(6), new Plan.Col(7)), new Plan.Lit(10)), new Plan.Col(7)),
                                new Plan.Predicate(">", new Plan.Bin("*", new Plan.Bin("-", new Plan.Col(7), new Plan.Col(6)), new Plan.Lit(10)), new Plan.Col(7)))))
                .orderBy(new Plan.Ordering(List.of(
                        Plan.SortKey.expression(difference, Types.LONG, false), new Plan.SortKey(3, false)), 100));
        // The five string outputs reconstruct from the virtual scan (main input 0), cols 0-4.
        return new Composite(List.of(new Stage(monthly, "q89_monthly", stageStrings)), main, List.of(
                new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2),
                new DictRef(3, 0, 3), new DictRef(4, 0, 4)));
    }

    public static Composite query63()
    {
        // Q63: like Q53 but groups store_sales sums per (i_manager_id, d_moy) -- monthly sales per manager over the same
        // two item cohorts -- then avg OVER (PARTITION BY i_manager_id) and keeps months deviating >10% from the
        // manager's average, ORDER BY i_manager_id, the average, then the sum, top 100.
        QueryLowering monthly = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manager_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_class", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, false))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_month_seq"))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"));
        monthly.where(
                        new Plan.Predicate(">", monthly.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", monthly.column("d_month_seq"), new Plan.Lit(1212)),
                        new Plan.Or(
                                new Plan.And(
                                        new Plan.StringMatch(monthly.position("i_category"), List.of("Books", "Children", "Electronics"), false),
                                        new Plan.StringMatch(monthly.position("i_class"), List.of("personal", "portable", "reference", "self-help"), false),
                                        new Plan.StringMatch(monthly.position("i_brand"), List.of("scholaramalgamalg #14", "scholaramalgamalg #7", "exportiunivamalg #9", "scholaramalgamalg #9"), false)),
                                new Plan.And(
                                        new Plan.StringMatch(monthly.position("i_category"), List.of("Women", "Music", "Men"), false),
                                        new Plan.StringMatch(monthly.position("i_class"), List.of("accessories", "classical", "fragrances", "pants"), false),
                                        new Plan.StringMatch(monthly.position("i_brand"), List.of("amalgimporto #1", "edu packscholar #1", "exportiimporto #1", "importoamalg #1"), false))))
                .groupBy("i_manager_id", "d_moy")
                .aggregate("sum", "ss_sales_price");
        // Pre-aggregation output: (i_manager_id = 0, d_moy = 1, sum_sales = 2); all non-null.

        QueryLowering main = QueryLowering.scan("q63_monthly",
                        new QueryLowering.Column("ms_manager_id", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("ms_moy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("ms_sum", ColumnEncoding.FLAT, false))
                .window(Plan.Window.partitionAverage(new int[] {0}, 2));
        // Window output: ms_manager_id (0), ms_moy (1), ms_sum (2), avg_monthly_sales (3).
        main.having(new Plan.And(
                        new Plan.Predicate(">", new Plan.Col(3), new Plan.Lit(0)),
                        new Plan.Or(
                                new Plan.Predicate(">", new Plan.Bin("*", new Plan.Bin("-", new Plan.Col(2), new Plan.Col(3)), new Plan.Lit(10)), new Plan.Col(3)),
                                new Plan.Predicate(">", new Plan.Bin("*", new Plan.Bin("-", new Plan.Col(3), new Plan.Col(2)), new Plan.Lit(10)), new Plan.Col(3)))))
                .select(new Plan.Col(0), new Plan.Col(2), new Plan.Col(3))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(3, false), new Plan.SortKey(2, false)), 100));

        return new Composite(List.of(new Stage(monthly, "q63_monthly")), main, List.of());
    }

    public static Composite query53()
    {
        // Q53: per (i_manufact_id, d_qoy) sum store_sales price over a 12-month window for two item category/class/
        // brand cohorts, then AVG(sum) OVER (PARTITION BY i_manufact_id); keep the (manufact, quarter) rows whose
        // quarterly sum deviates more than 10% from the manufacturer's average quarterly sales; ORDER BY the average,
        // the sum, then i_manufact_id, top 100. Two stages: a grouped pre-aggregate (pipeline breaker), then the
        // partition-average window + deviation HAVING over its output.
        QueryLowering quarterly = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manufact_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_class", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, false))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_qoy"),
                        new QueryLowering.Column("d_month_seq"))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"));
        quarterly.where(
                        new Plan.Predicate(">", quarterly.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", quarterly.column("d_month_seq"), new Plan.Lit(1212)),
                        new Plan.Or(
                                new Plan.And(
                                        new Plan.StringMatch(quarterly.position("i_category"), List.of("Books", "Children", "Electronics"), false),
                                        new Plan.StringMatch(quarterly.position("i_class"), List.of("personal", "portable", "reference", "self-help"), false),
                                        new Plan.StringMatch(quarterly.position("i_brand"), List.of("scholaramalgamalg #14", "scholaramalgamalg #7", "exportiunivamalg #9", "scholaramalgamalg #9"), false)),
                                new Plan.And(
                                        new Plan.StringMatch(quarterly.position("i_category"), List.of("Women", "Music", "Men"), false),
                                        new Plan.StringMatch(quarterly.position("i_class"), List.of("accessories", "classical", "fragrances", "pants"), false),
                                        new Plan.StringMatch(quarterly.position("i_brand"), List.of("amalgimporto #1", "edu packscholar #1", "exportiimporto #1", "importoamalg #1"), false))))
                .groupBy("i_manufact_id", "d_qoy")
                .aggregate("sum", "ss_sales_price");
        // Pre-aggregation output: (i_manufact_id = 0, d_qoy = 1, sum_sales = 2); all non-null.

        QueryLowering main = QueryLowering.scan("q53_quarterly",
                        new QueryLowering.Column("qs_manufact_id", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("qs_qoy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("qs_sum", ColumnEncoding.FLAT, false))
                .window(Plan.Window.partitionAverage(new int[] {0}, 2));
        // Window output: qs_manufact_id (0), qs_qoy (1), qs_sum (2), avg_quarterly_sales (3). Keep rows whose sum
        // deviates > 10% from the average: avg > 0 AND |sum - avg| * 10 > avg (fraction-free, matching the harness).
        main.having(new Plan.And(
                        new Plan.Predicate(">", new Plan.Col(3), new Plan.Lit(0)),
                        new Plan.Or(
                                new Plan.Predicate(">", new Plan.Bin("*", new Plan.Bin("-", new Plan.Col(2), new Plan.Col(3)), new Plan.Lit(10)), new Plan.Col(3)),
                                new Plan.Predicate(">", new Plan.Bin("*", new Plan.Bin("-", new Plan.Col(3), new Plan.Col(2)), new Plan.Lit(10)), new Plan.Col(3)))))
                .select(new Plan.Col(0), new Plan.Col(2), new Plan.Col(3))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(3, false), new Plan.SortKey(2, false), new Plan.SortKey(0, false)), 100));

        return new Composite(List.of(new Stage(quarterly, "q53_quarterly")), main, List.of());
    }

    public static Ported query40()
    {
        // Q40: catalog_sales LEFT JOIN catalog_returns (multi-key on order+item -- most sales were never returned) ⋈
        // warehouse ⋈ item(i_current_price 99..149) ⋈ date_dim(d_date within 30 days of 2000-03-11); per (w_state,
        // i_item_id) sum the net sales (cs_sales_price - coalesce(cr_refunded_cash, 0)) split into before/after the
        // cutoff date; ORDER BY w_state, i_item_id, top 100. The returns join is a left/outer multi-key join, so
        // cr_refunded_cash reads NULL for unreturned sales and coalesce maps it to 0.
        LocalDate cutoff = LocalDate.of(2000, 3, 11);
        QueryLowering query = QueryLowering.scan("catalog_sales",
                        new QueryLowering.Column("cs_order_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_sales_price", ColumnEncoding.FLAT, true))
                .leftJoin("catalog_returns",
                        new String[] {"cs_order_number", "cs_item_sk"},
                        new String[] {"cr_order_number", "cr_item_sk"},
                        new QueryLowering.Column("cr_order_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cr_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cr_refunded_cash", ColumnEncoding.FLAT, true))
                .join("warehouse", "cs_warehouse_sk", "w_warehouse_sk",
                        new QueryLowering.Column("w_warehouse_sk"),
                        new QueryLowering.Column("w_state", ColumnEncoding.STRING, true))
                .join("item", "cs_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_current_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false))
                .join("date_dim", "cs_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true));
        Plan.Expr netSales = new Plan.Bin("-", query.column("cs_sales_price"),
                new Plan.Coalesce(query.column("cr_refunded_cash"), new Plan.Lit(0)));
        Plan.Condition beforeCutoff = new Plan.Predicate("<", query.column("d_date"), new Plan.Lit(cutoff.toEpochDay()));
        query.where(
                        new Plan.Predicate(">=", query.column("i_current_price"), new Plan.Lit(99)),
                        new Plan.Predicate("<=", query.column("i_current_price"), new Plan.Lit(149)),
                        new Plan.Predicate(">=", query.column("d_date"), new Plan.Lit(cutoff.minusDays(30).toEpochDay())),
                        new Plan.Predicate("<=", query.column("d_date"), new Plan.Lit(cutoff.plusDays(30).toEpochDay())))
                .groupBy("w_state", "i_item_id")
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(beforeCutoff, netSales)), new Plan.Lit(0)))
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(
                        new Plan.Predicate(">=", query.column("d_date"), new Plan.Lit(cutoff.toEpochDay())), netSales)), new Plan.Lit(0)))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100));
        // w_state (output 0) from warehouse (input 2, col 1); i_item_id (output 1) from item (input 3, col 2).
        return new Ported(query, List.of(new DictRef(0, 2, 1), new DictRef(1, 3, 2)));
    }

    public static Ported query25()
    {
        // Q25: store_sales ⋈ date_dim(sold: d_moy=4, d_year=2001) ⋈ store_returns (multi-key on customer+item+ticket)
        // ⋈ date_dim(returned: d_moy 4..10, d_year=2001) ⋈ catalog_sales (multi-key on the return's customer+item, a
        // one-to-many snowflake join) ⋈ date_dim(catalog: d_moy 4..10, d_year=2001) ⋈ store ⋈ item; GROUP BY item
        // id/desc, store id/name; sum(ss_net_profit), sum(sr_net_loss), sum(cs_net_profit); ORDER BY the four group
        // columns, top 100. Same three-fact shape as Q29 (date_dim joined three times via aliases).
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_net_profit", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("store_returns",
                        new String[] {"ss_customer_sk", "ss_item_sk", "ss_ticket_number"},
                        new String[] {"sr_customer_sk", "sr_item_sk", "sr_ticket_number"},
                        new QueryLowering.Column("sr_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_returned_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_net_loss", ColumnEncoding.FLAT, true))
                .join("date_dim", "sr_returned_date_sk", "d_date_sk_returned",
                        new QueryLowering.Column("d_date_sk_returned", "d_date_sk", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_moy_returned", "d_moy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_year_returned", "d_year", ColumnEncoding.FLAT, false))
                .join("catalog_sales",
                        new String[] {"sr_customer_sk", "sr_item_sk"},
                        new String[] {"cs_bill_customer_sk", "cs_item_sk"},
                        new QueryLowering.Column("cs_bill_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_net_profit", ColumnEncoding.FLAT, true))
                .join("date_dim", "cs_sold_date_sk", "d_date_sk_catalog",
                        new QueryLowering.Column("d_date_sk_catalog", "d_date_sk", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_moy_catalog", "d_moy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_year_catalog", "d_year", ColumnEncoding.FLAT, false))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_store_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, false))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_item_desc", ColumnEncoding.STRING, false));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(4)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2001)),
                        new Plan.Predicate(">=", query.column("d_moy_returned"), new Plan.Lit(4)),
                        new Plan.Predicate("<=", query.column("d_moy_returned"), new Plan.Lit(10)),
                        new Plan.Predicate("=", query.column("d_year_returned"), new Plan.Lit(2001)),
                        new Plan.Predicate(">=", query.column("d_moy_catalog"), new Plan.Lit(4)),
                        new Plan.Predicate("<=", query.column("d_moy_catalog"), new Plan.Lit(10)),
                        new Plan.Predicate("=", query.column("d_year_catalog"), new Plan.Lit(2001)))
                .groupBy("i_item_id", "i_item_desc", "s_store_id", "s_store_name")
                .aggregate("sum", "ss_net_profit")
                .aggregate("sum", "sr_net_loss")
                .aggregate("sum", "cs_net_profit")
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false),
                        new Plan.SortKey(2, false), new Plan.SortKey(3, false)), 100));
        // Item is input 7 (cols 1-2 = id/desc), store is input 6 (cols 1-2 = id/name).
        return new Ported(query, List.of(
                new DictRef(0, 7, 1), new DictRef(1, 7, 2), new DictRef(2, 6, 1), new DictRef(3, 6, 2)));
    }

    public static Composite query95()
    {
        // Q95: like Q94 but for orders that WERE returned (a SEMI-join to the returned orders instead of Q94's
        // ANTI-join). Same filtered web_sales (60-day window, Illinois, 'pri%' web sites), multi-warehouse-order
        // SEMI-join, then count(distinct order), sum(ext_ship_cost), sum(net_profit) via ROLLUP({order},{}).
        QueryLowering multiWarehouse = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_order_number", ColumnEncoding.FLAT, true))
                .join("web_sales", "ws_order_number", "ws_order_number_2",
                        new QueryLowering.Column("ws_warehouse_sk_2", "ws_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_order_number_2", "ws_order_number", ColumnEncoding.FLAT, true));
        multiWarehouse.where(new Plan.Predicate("<>", multiWarehouse.column("ws_warehouse_sk"), multiWarehouse.column("ws_warehouse_sk_2")))
                .groupBy("ws_order_number")
                .count();

        QueryLowering returned = QueryLowering.scan("web_returns",
                        new QueryLowering.Column("wr_order_number", ColumnEncoding.FLAT, true))
                .groupBy("wr_order_number")
                .count();

        LocalDate start = LocalDate.of(1999, 2, 1);
        QueryLowering qualified = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_ship_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_ship_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_web_site_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_order_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_ext_ship_cost", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_net_profit", ColumnEncoding.FLAT, true))
                .join("date_dim", "ws_ship_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true))
                .join("customer_address", "ws_ship_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, false))
                .join("web_site", "ws_web_site_sk", "web_site_sk",
                        new QueryLowering.Column("web_site_sk"),
                        new QueryLowering.Column("web_company_name", ColumnEncoding.STRING, false))
                .join("q95_mw_orders", "ws_order_number", "mw_order",
                        new QueryLowering.Column("mw_order", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("mw_count", ColumnEncoding.FLAT, false))
                .join("q95_ret_orders", "ws_order_number", "ret_order",
                        new QueryLowering.Column("ret_order", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("ret_count", ColumnEncoding.FLAT, false));
        qualified.where(
                        new Plan.Predicate(">=", qualified.column("d_date"), new Plan.Lit(start.toEpochDay())),
                        new Plan.Predicate("<=", qualified.column("d_date"), new Plan.Lit(start.plusDays(60).toEpochDay())),
                        new Plan.StringMatch(qualified.position("ca_state"), List.of("IL"), false),
                        new Plan.LikeMatch(qualified.position("web_company_name"), "pri%", false))
                .groupBy("ws_order_number")
                .groupingSets(List.of(new int[] {0}, new int[0]))
                .aggregate("sum", "ws_ext_ship_cost")
                .aggregate("sum", "ws_net_profit");

        QueryLowering main = QueryLowering.scan("q95_grouped",
                        new QueryLowering.Column("g_order", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_ship", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_profit", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_grouping", ColumnEncoding.FLAT, false));
        Plan.Condition perOrder = new Plan.Predicate("=", main.column("g_grouping"), new Plan.Lit(0));
        Plan.Condition grandTotal = new Plan.Predicate("=", main.column("g_grouping"), new Plan.Lit(1));
        main.aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(perOrder, new Plan.Lit(1))), new Plan.Lit(0)))
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(grandTotal, main.column("g_ship"))), new Plan.Lit(0)))
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(grandTotal, main.column("g_profit"))), new Plan.Lit(0)));

        return new Composite(List.of(
                new Stage(multiWarehouse, "q95_mw_orders"),
                new Stage(returned, "q95_ret_orders"),
                new Stage(qualified, "q95_grouped")), main, List.of());
    }

    public static Composite query94()
    {
        // Q94: web_sales shipped in a 60-day window from Illinois addresses via 'pri%' web sites, for orders shipped
        // from MORE THAN ONE warehouse and NOT returned; report count(distinct order), sum(ext_ship_cost),
        // sum(net_profit). Four stages: (1) multi-warehouse orders (web_sales self-join on order with differing
        // warehouse, distinct), (2) distinct returned orders, (3) filtered sales SEMI-joined to (1) and ANTI-joined to
        // (2) then grouped by order via ROLLUP({order},{}) -- the per-order set enumerates distinct orders, the empty
        // set carries the grand totals; main = the global count(distinct)+sums via sum(CASE on grouping_id).
        QueryLowering multiWarehouse = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_order_number", ColumnEncoding.FLAT, true))
                .join("web_sales", "ws_order_number", "ws_order_number_2",
                        new QueryLowering.Column("ws_warehouse_sk_2", "ws_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_order_number_2", "ws_order_number", ColumnEncoding.FLAT, true));
        multiWarehouse.where(new Plan.Predicate("<>", multiWarehouse.column("ws_warehouse_sk"), multiWarehouse.column("ws_warehouse_sk_2")))
                .groupBy("ws_order_number")
                .count();   // distinct eligible orders (the count is unused; the order key is the eligible set)

        QueryLowering returned = QueryLowering.scan("web_returns",
                        new QueryLowering.Column("wr_order_number", ColumnEncoding.FLAT, true))
                .groupBy("wr_order_number")
                .count();

        LocalDate start = LocalDate.of(1999, 2, 1);
        QueryLowering qualified = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_ship_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_ship_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_web_site_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_order_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_ext_ship_cost", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_net_profit", ColumnEncoding.FLAT, true))
                .join("date_dim", "ws_ship_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true))
                .join("customer_address", "ws_ship_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, false))
                .join("web_site", "ws_web_site_sk", "web_site_sk",
                        new QueryLowering.Column("web_site_sk"),
                        new QueryLowering.Column("web_company_name", ColumnEncoding.STRING, false))
                .join("q94_mw_orders", "ws_order_number", "mw_order",
                        new QueryLowering.Column("mw_order", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("mw_count", ColumnEncoding.FLAT, false))
                .antiJoin("q94_ret_orders", "ws_order_number", "ret_order",
                        new QueryLowering.Column("ret_order", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("ret_count", ColumnEncoding.FLAT, false));
        qualified.where(
                        new Plan.Predicate(">=", qualified.column("d_date"), new Plan.Lit(start.toEpochDay())),
                        new Plan.Predicate("<=", qualified.column("d_date"), new Plan.Lit(start.plusDays(60).toEpochDay())),
                        new Plan.StringMatch(qualified.position("ca_state"), List.of("IL"), false),
                        new Plan.LikeMatch(qualified.position("web_company_name"), "pri%", false))
                .groupBy("ws_order_number")
                .groupingSets(List.of(new int[] {0}, new int[0]))
                .aggregate("sum", "ws_ext_ship_cost")
                .aggregate("sum", "ws_net_profit");
        // Stage 3 output: order(0), sum_ship(1), sum_profit(2), grouping_id(3). grouping_id = GROUPING(order): 0 for the
        // per-order set (order active), 1 for the empty/grand-total set (order nulled).

        QueryLowering main = QueryLowering.scan("q94_grouped",
                        new QueryLowering.Column("g_order", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_ship", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_profit", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_grouping", ColumnEncoding.FLAT, false));
        Plan.Condition perOrder = new Plan.Predicate("=", main.column("g_grouping"), new Plan.Lit(0));
        Plan.Condition grandTotal = new Plan.Predicate("=", main.column("g_grouping"), new Plan.Lit(1));
        main.aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(perOrder, new Plan.Lit(1))), new Plan.Lit(0)))
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(grandTotal, main.column("g_ship"))), new Plan.Lit(0)))
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(grandTotal, main.column("g_profit"))), new Plan.Lit(0)));

        return new Composite(List.of(
                new Stage(multiWarehouse, "q94_mw_orders"),
                new Stage(returned, "q94_ret_orders"),
                new Stage(qualified, "q94_grouped")), main, List.of());
    }

    public static Composite query16()
    {
        // Q16: the catalog-channel sibling of Q94 -- catalog_sales shipped in a 60-day window to Georgia addresses via
        // the 'Williamson County' call center, for orders shipped from MORE THAN ONE warehouse and NOT returned; report
        // count(distinct order), sum(ext_ship_cost), sum(net_profit). Identical four-stage shape to Q94: (1)
        // multi-warehouse orders (catalog_sales self-join on order with differing warehouse, distinct), (2) distinct
        // returned orders, (3) filtered sales SEMI-joined to (1) and ANTI-joined to (2) then grouped via
        // ROLLUP({order},{}); main = count(distinct)+sums via sum(CASE on grouping_id).
        QueryLowering multiWarehouse = QueryLowering.scan("catalog_sales",
                        new QueryLowering.Column("cs_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_order_number", ColumnEncoding.FLAT, true))
                .join("catalog_sales", "cs_order_number", "cs_order_number_2",
                        new QueryLowering.Column("cs_warehouse_sk_2", "cs_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_order_number_2", "cs_order_number", ColumnEncoding.FLAT, true));
        multiWarehouse.where(new Plan.Predicate("<>", multiWarehouse.column("cs_warehouse_sk"), multiWarehouse.column("cs_warehouse_sk_2")))
                .groupBy("cs_order_number")
                .count();

        QueryLowering returned = QueryLowering.scan("catalog_returns",
                        new QueryLowering.Column("cr_order_number", ColumnEncoding.FLAT, true))
                .groupBy("cr_order_number")
                .count();

        LocalDate start = LocalDate.of(2002, 2, 1);
        QueryLowering qualified = QueryLowering.scan("catalog_sales",
                        new QueryLowering.Column("cs_ship_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_ship_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_call_center_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_order_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_ext_ship_cost", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_net_profit", ColumnEncoding.FLAT, true))
                .join("date_dim", "cs_ship_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true))
                .join("customer_address", "cs_ship_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, false))
                .join("call_center", "cs_call_center_sk", "cc_call_center_sk",
                        new QueryLowering.Column("cc_call_center_sk"),
                        new QueryLowering.Column("cc_county", ColumnEncoding.STRING, false))
                .join("q16_mw_orders", "cs_order_number", "mw_order",
                        new QueryLowering.Column("mw_order", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("mw_count", ColumnEncoding.FLAT, false))
                .antiJoin("q16_ret_orders", "cs_order_number", "ret_order",
                        new QueryLowering.Column("ret_order", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("ret_count", ColumnEncoding.FLAT, false));
        qualified.where(
                        new Plan.Predicate(">=", qualified.column("d_date"), new Plan.Lit(start.toEpochDay())),
                        new Plan.Predicate("<=", qualified.column("d_date"), new Plan.Lit(start.plusDays(60).toEpochDay())),
                        new Plan.StringMatch(qualified.position("ca_state"), List.of("GA"), false),
                        new Plan.StringMatch(qualified.position("cc_county"), List.of("Williamson County"), false))
                .groupBy("cs_order_number")
                .groupingSets(List.of(new int[] {0}, new int[0]))
                .aggregate("sum", "cs_ext_ship_cost")
                .aggregate("sum", "cs_net_profit");

        QueryLowering main = QueryLowering.scan("q16_grouped",
                        new QueryLowering.Column("g_order", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_ship", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_profit", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_grouping", ColumnEncoding.FLAT, false));
        Plan.Condition perOrder = new Plan.Predicate("=", main.column("g_grouping"), new Plan.Lit(0));
        Plan.Condition grandTotal = new Plan.Predicate("=", main.column("g_grouping"), new Plan.Lit(1));
        main.aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(perOrder, new Plan.Lit(1))), new Plan.Lit(0)))
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(grandTotal, main.column("g_ship"))), new Plan.Lit(0)))
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(grandTotal, main.column("g_profit"))), new Plan.Lit(0)));

        return new Composite(List.of(
                new Stage(multiWarehouse, "q16_mw_orders"),
                new Stage(returned, "q16_ret_orders"),
                new Stage(qualified, "q16_grouped")), main, List.of());
    }

    public static Composite query46()
    {
        // Q46: the same shape as Q68 over a different slice -- per (ticket, customer) store-sale coupon and net-profit
        // totals for two stores, on weekends (d_dow in {6,0}) in 1999-2001, for households with four dependents or
        // three vehicles; then the customer's name, current city, the bought city, and the totals, kept only when the
        // current city differs from the bought city. Ordered by name, both cities, and ticket -- so unlike Q68 it also
        // sorts by the virtual-table bought-city string (safe: the reconstructed dictionary keeps its sorted source).
        QueryLowering grouped = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_hdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ticket_number"),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_coupon_amt", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_net_profit", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_dow"),
                        new QueryLowering.Column("d_year"))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_city", ColumnEncoding.STRING, false))
                .join("household_demographics", "ss_hdemo_sk", "hd_demo_sk",
                        new QueryLowering.Column("hd_demo_sk"),
                        new QueryLowering.Column("hd_dep_count", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("hd_vehicle_count", ColumnEncoding.FLAT, true))
                .join("customer_address", "ss_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_city", ColumnEncoding.STRING, true));
        grouped.where(
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", grouped.column("d_dow"), new Plan.Lit(6)),
                                new Plan.Predicate("=", grouped.column("d_dow"), new Plan.Lit(0)))),
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(1999)),
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(2000)),
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(2001)))),
                        new Plan.StringMatch(grouped.position("s_city"), List.of("Fairview", "Midway"), false),
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", grouped.column("hd_dep_count"), new Plan.Lit(4)),
                                new Plan.Predicate("=", grouped.column("hd_vehicle_count"), new Plan.Lit(3)))))
                .groupBy("ss_ticket_number", "ss_customer_sk", "ca_city")
                .aggregate("sum", "ss_coupon_amt")
                .aggregate("sum", "ss_net_profit");
        // grouped result: ticket(0), customer(1), bought_city(2), sum_amt(3), sum_profit(4).

        QueryLowering main = QueryLowering.scan("q46_grouped",
                        new QueryLowering.Column("g_ticket"),
                        new QueryLowering.Column("g_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_bought_city", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_amt", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_profit", ColumnEncoding.FLAT, true))
                .join("customer", "g_customer", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_city_current", "ca_city", ColumnEncoding.STRING, true));
        main.where(new Plan.StringColumnCompare(main.position("ca_city_current"), main.position("g_bought_city"), true))
                .select(main.column("c_last_name"), main.column("c_first_name"), main.column("ca_city_current"),
                        main.column("g_bought_city"), main.column("g_ticket"),
                        main.column("g_amt"), main.column("g_profit"))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false),
                        new Plan.SortKey(3, false), new Plan.SortKey(4, false)), 100));

        return new Composite(
                List.of(new Stage(grouped, "q46_grouped", List.of(new DictRef(2, 4, 1)))),
                main,
                List.of(new DictRef(0, 1, 2), new DictRef(1, 1, 3), new DictRef(2, 2, 1), new DictRef(3, 0, 2)));
    }

    public static Composite query68()
    {
        // Q68: per (ticket, customer) store-sale totals for two specific stores, on the 1st/2nd of the month in
        // 1999-2001, for households with 4 dependents or 3 vehicles -- then for each such basket report the customer's
        // name, the city it was bought in, and the totals, but ONLY when the customer's current home city differs from
        // the bought city. Two stages: (1) GROUP BY ticket/customer/bought-city summing the three price columns
        // (bought-city is a dictionary-string group key carried through the virtual table); (2) a join-only stage that
        // re-attaches the customer and their current address and keeps rows where current-city <> bought-city, then
        // ORDER BY last name, ticket. Exercises a string group key flowing into a projection-only join stage whose
        // WHERE is a whole-value string-column compare and whose output mixes virtual-table and base-table strings.
        QueryLowering grouped = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_hdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ticket_number"),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ext_list_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ext_tax", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_dom"),
                        new QueryLowering.Column("d_year"))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_city", ColumnEncoding.STRING, false))
                .join("household_demographics", "ss_hdemo_sk", "hd_demo_sk",
                        new QueryLowering.Column("hd_demo_sk"),
                        new QueryLowering.Column("hd_dep_count", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("hd_vehicle_count", ColumnEncoding.FLAT, true))
                .join("customer_address", "ss_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_city", ColumnEncoding.STRING, true));
        grouped.where(
                        new Plan.Predicate(">", grouped.column("d_dom"), new Plan.Lit(0)),
                        new Plan.Predicate("<", grouped.column("d_dom"), new Plan.Lit(3)),
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(1999)),
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(2000)),
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(2001)))),
                        new Plan.StringMatch(grouped.position("s_city"), List.of("Fairview", "Midway"), false),
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", grouped.column("hd_dep_count"), new Plan.Lit(4)),
                                new Plan.Predicate("=", grouped.column("hd_vehicle_count"), new Plan.Lit(3)))))
                .groupBy("ss_ticket_number", "ss_customer_sk", "ca_city")
                .aggregate("sum", "ss_ext_sales_price")
                .aggregate("sum", "ss_ext_tax")
                .aggregate("sum", "ss_ext_list_price");
        // grouped result: ticket(0), customer(1), bought_city(2), sum_sales(3), sum_tax(4), sum_list(5).

        QueryLowering main = QueryLowering.scan("q68_grouped",
                        new QueryLowering.Column("g_ticket"),
                        new QueryLowering.Column("g_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_bought_city", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_sum_sales", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_sum_tax", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_sum_list", ColumnEncoding.FLAT, true))
                .join("customer", "g_customer", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_city_current", "ca_city", ColumnEncoding.STRING, true));
        main.where(new Plan.StringColumnCompare(main.position("ca_city_current"), main.position("g_bought_city"), true))
                .select(main.column("c_last_name"), main.column("c_first_name"), main.column("ca_city_current"),
                        main.column("g_bought_city"), main.column("g_ticket"),
                        main.column("g_sum_sales"), main.column("g_sum_tax"), main.column("g_sum_list"))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(4, false)), 100));

        return new Composite(
                List.of(new Stage(grouped, "q68_grouped", List.of(new DictRef(2, 4, 1)))),
                main,
                List.of(new DictRef(0, 1, 2), new DictRef(1, 1, 3), new DictRef(2, 2, 1), new DictRef(3, 0, 2)));
    }

    public static Composite query12()
    {
        // Q12: web_sales ⋈ item (category in {Sports,Books,Home}) ⋈ date_dim (30-day window) GROUP BY item attrs
        // sum(ext_sales_price) = itemrevenue; then sum(itemrevenue) OVER (PARTITION BY i_class) and report each item's
        // revenue as a scaled percentage of its class total. Two stages: the grouped pre-aggregate (pipeline breaker)
        // carrying four dictionary-string keys through the virtual table, then the partition-sum window + ratio + ORDER.
        QueryLowering grouped = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("item", "ws_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_item_desc", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_class", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_current_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ws_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true));
        grouped.where(
                        new Plan.StringMatch(grouped.position("i_category"), List.of("Sports", "Books", "Home"), false),
                        new Plan.Predicate(">", grouped.column("d_date"), new Plan.Lit(10_643)),
                        new Plan.Predicate("<", grouped.column("d_date"), new Plan.Lit(10_675)))
                .groupBy("i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price")
                .aggregate("sum", "ws_ext_sales_price");
        // grouped result: item_id(0), item_desc(1), category(2), class(3), current_price(4), itemrevenue(5).

        QueryLowering main = QueryLowering.scan("q12_grouped",
                        new QueryLowering.Column("g_item_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_item_desc", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_category", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_class", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_current_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_itemrevenue", ColumnEncoding.FLAT, true))
                .window(Plan.Window.partitionSum(new int[] {3}, 5));
        // window output: item_id(0), item_desc(1), category(2), class(3), current_price(4), itemrevenue(5), classrev(6).
        main.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4), new Plan.Col(5),
                        new Plan.Call("divide_scale_round_i64", new Plan.Col(5), new Plan.Col(6), new Plan.Lit(100_000_000L)))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(2, false), new Plan.SortKey(3, false), new Plan.SortKey(0, false),
                        new Plan.SortKey(1, false), new Plan.SortKey(6, false)), 100));

        return new Composite(
                List.of(new Stage(grouped, "q12_grouped",
                        List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 1, 3), new DictRef(3, 1, 4)))),
                main,
                List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2), new DictRef(3, 0, 3)));
    }

    public static Composite query20()
    {
        // Q20: the Q12 revenue-ratio-by-class shape over catalog_sales (identical item/date filters); top 100.
        QueryLowering grouped = QueryLowering.scan("catalog_sales",
                        new QueryLowering.Column("cs_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("item", "cs_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_item_desc", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_class", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_current_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "cs_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true));
        grouped.where(
                        new Plan.StringMatch(grouped.position("i_category"), List.of("Sports", "Books", "Home"), false),
                        new Plan.Predicate(">", grouped.column("d_date"), new Plan.Lit(10_643)),
                        new Plan.Predicate("<", grouped.column("d_date"), new Plan.Lit(10_675)))
                .groupBy("i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price")
                .aggregate("sum", "cs_ext_sales_price");

        QueryLowering main = QueryLowering.scan("q20_grouped",
                        new QueryLowering.Column("g_item_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_item_desc", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_category", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_class", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_current_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_itemrevenue", ColumnEncoding.FLAT, true))
                .window(Plan.Window.partitionSum(new int[] {3}, 5));
        main.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4), new Plan.Col(5),
                        new Plan.Call("divide_scale_round_i64", new Plan.Col(5), new Plan.Col(6), new Plan.Lit(100_000_000L)))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(2, false), new Plan.SortKey(3, false), new Plan.SortKey(0, false),
                        new Plan.SortKey(1, false), new Plan.SortKey(6, false)), 100));

        return new Composite(
                List.of(new Stage(grouped, "q20_grouped",
                        List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 1, 3), new DictRef(3, 1, 4)))),
                main,
                List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2), new DictRef(3, 0, 3)));
    }

    /** Q59 weekly per-(week, store) day-of-week sales buckets over a month-sequence window; the grouped pre-aggregate. */
    private static QueryLowering query59Weekly(int minMonthSeq, int maxMonthSeq)
    {
        QueryLowering weekly = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_week_seq", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_day_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("d_month_seq", ColumnEncoding.FLAT, true));
        weekly.where(
                        new Plan.Predicate(">=", weekly.column("d_month_seq"), new Plan.Lit(minMonthSeq)),
                        new Plan.Predicate("<=", weekly.column("d_month_seq"), new Plan.Lit(maxMonthSeq)))
                .groupBy("d_week_seq", "ss_store_sk");
        for (String day : List.of("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")) {
            weekly.aggregate("sum", new Plan.Case(
                    List.of(new Plan.Case.Branch(
                            new Plan.StringMatch(weekly.position("d_day_name"), List.of(day), false),
                            weekly.column("ss_sales_price"))),
                    new Plan.Lit(0)));
        }
        // grouped result: week_seq(0), store(1), sun(2), mon(3), tue(4), wed(5), thu(6), fri(7), sat(8).
        return weekly;
    }

    public static Composite query59()
    {
        // Q59: compare each store's per-weekday store_sales between a year and the next, week-aligned. Two weekly
        // pre-aggregates (this year d_month_seq 1212..1223, next year 1224..1235), each summing sales into the seven
        // days of the week; self-join them on (this.week_seq + 52, store) = (next.week_seq, store); join store; report
        // the seven day-over-day ratios. All join keys are LONG (week_seq + store), so no string-keyed join is needed.
        // A weekday with no next-year sales makes that ratio's denominator 0 -> NULL (the computed-null divide).
        QueryLowering current = query59Weekly(1212, 1223)
                .select(new Plan.Bin("+", new Plan.Col(0), new Plan.Lit(52)),   // adjusted week = week_seq + 52
                        new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4),
                        new Plan.Col(5), new Plan.Col(6), new Plan.Col(7), new Plan.Col(8));
        // current result: adjusted(0), week_seq(1), store(2), sun(3), mon(4), tue(5), wed(6), thu(7), fri(8), sat(9).
        QueryLowering next = query59Weekly(1224, 1235);

        QueryLowering main = QueryLowering.scan("q59_current",
                        new QueryLowering.Column("c_adjusted", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("c_week_seq", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("c_store", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_sun", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("c_mon", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("c_tue", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("c_wed", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("c_thu", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("c_fri", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("c_sat", ColumnEncoding.FLAT, false))
                .join("q59_next", new String[] {"c_adjusted", "c_store"}, new String[] {"n_week_seq", "n_store"},
                        new QueryLowering.Column("n_week_seq", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("n_store", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("n_sun", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("n_mon", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("n_tue", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("n_wed", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("n_thu", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("n_fri", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("n_sat", ColumnEncoding.FLAT, false))
                .join("store", "c_store", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_store_id", ColumnEncoding.STRING, true));
        main.select(main.column("s_store_name"), main.column("s_store_id"), main.column("c_week_seq"),
                        new Plan.Call("divide_scale_round_i64", main.column("c_sun"), main.column("n_sun"), new Plan.Lit(100L)),
                        new Plan.Call("divide_scale_round_i64", main.column("c_mon"), main.column("n_mon"), new Plan.Lit(100L)),
                        new Plan.Call("divide_scale_round_i64", main.column("c_tue"), main.column("n_tue"), new Plan.Lit(100L)),
                        new Plan.Call("divide_scale_round_i64", main.column("c_wed"), main.column("n_wed"), new Plan.Lit(100L)),
                        new Plan.Call("divide_scale_round_i64", main.column("c_thu"), main.column("n_thu"), new Plan.Lit(100L)),
                        new Plan.Call("divide_scale_round_i64", main.column("c_fri"), main.column("n_fri"), new Plan.Lit(100L)),
                        new Plan.Call("divide_scale_round_i64", main.column("c_sat"), main.column("n_sat"), new Plan.Lit(100L)))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)), 100));

        return new Composite(
                List.of(new Stage(current, "q59_current"), new Stage(next, "q59_next")),
                main,
                List.of(new DictRef(0, 2, 1), new DictRef(1, 2, 2)));
    }

    /** Q21 inventory-on-hand per (warehouse name, item id); reproduces a high-cardinality multi-key grouping bug. */
    public static QueryLowering query21Inventory(boolean before)
    {
        LocalDate cutoff = LocalDate.of(2000, 3, 11);
        QueryLowering inventory = QueryLowering.scan("inventory",
                        new QueryLowering.Column("inv_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_quantity_on_hand", ColumnEncoding.FLAT, true))
                .join("item", "inv_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_current_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false))
                .join("warehouse", "inv_warehouse_sk", "w_warehouse_sk",
                        new QueryLowering.Column("w_warehouse_sk"),
                        new QueryLowering.Column("w_warehouse_name", ColumnEncoding.STRING, true))
                .join("date_dim", "inv_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true));
        inventory.where(
                        new Plan.Predicate(">=", inventory.column("i_current_price"), new Plan.Lit(99L)),
                        new Plan.Predicate("<=", inventory.column("i_current_price"), new Plan.Lit(149L)),
                        before
                                ? new Plan.Predicate(">=", inventory.column("d_date"), new Plan.Lit(cutoff.minusDays(30).toEpochDay()))
                                : new Plan.Predicate(">=", inventory.column("d_date"), new Plan.Lit(cutoff.toEpochDay())),
                        before
                                ? new Plan.Predicate("<", inventory.column("d_date"), new Plan.Lit(cutoff.toEpochDay()))
                                : new Plan.Predicate("<=", inventory.column("d_date"), new Plan.Lit(cutoff.plusDays(30).toEpochDay())))
                .groupBy("w_warehouse_name", "i_item_id")
                .aggregate("sum", "inv_quantity_on_hand");
        return inventory;
    }

    public static Composite query21()
    {
        // Q21: per (warehouse, item) inventory-on-hand 30 days before vs after 2000-03-11, items priced 99..149; keep
        // pairs whose after/before ratio is within [2/3, 3/2] (fraction-free). The before/after pre-aggregates
        // self-join on (w_warehouse_name, i_item_id) -- two dictionary STRING keys (string-keyed join).
        QueryLowering before = query21Inventory(true);
        QueryLowering after = query21Inventory(false);

        QueryLowering main = QueryLowering.scan("q21_before",
                        new QueryLowering.Column("b_warehouse", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("b_item", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("b_qty", ColumnEncoding.FLAT, true))
                .join("q21_after", new String[] {"b_warehouse", "b_item"}, new String[] {"a_warehouse", "a_item"},
                        new QueryLowering.Column("a_warehouse", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("a_item", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("a_qty", ColumnEncoding.FLAT, true));
        main.where(
                        new Plan.Predicate(">=",
                                new Plan.Bin("*", main.column("a_qty"), new Plan.Lit(3)),
                                new Plan.Bin("*", main.column("b_qty"), new Plan.Lit(2))),
                        new Plan.Predicate(">=",
                                new Plan.Bin("*", main.column("b_qty"), new Plan.Lit(3)),
                                new Plan.Bin("*", main.column("a_qty"), new Plan.Lit(2))))
                .select(main.column("b_warehouse"), main.column("b_item"), main.column("b_qty"), main.column("a_qty"))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100));

        return new Composite(
                List.of(new Stage(before, "q21_before", List.of(new DictRef(0, 2, 1), new DictRef(1, 1, 2))),
                        new Stage(after, "q21_after", List.of(new DictRef(0, 2, 1), new DictRef(1, 1, 2)))),
                main,
                List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1)));
    }

    /** Q74 per-customer net-paid total for one channel and year; the grouped pre-aggregate (keyed by customer business id/name). */
    private static QueryLowering query74ChannelYearTotal(String table, String customerColumn, String soldDateColumn, String netPaidColumn, int year)
    {
        QueryLowering channel = QueryLowering.scan(table,
                        new QueryLowering.Column(customerColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(soldDateColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(netPaidColumn, ColumnEncoding.FLAT, true))
                .join("customer", customerColumn, "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_customer_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true))
                .join("date_dim", soldDateColumn, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"));
        channel.where(new Plan.Predicate("=", channel.column("d_year"), new Plan.Lit(year)))
                .groupBy("c_customer_id", "c_first_name", "c_last_name")
                .aggregate("sum", netPaidColumn);
        // result: customer_id(0), first_name(1), last_name(2), year_total(3). customer is input 1 in this stage.
        return channel;
    }

    public static Composite query74()
    {
        // Q74: customers whose store-sales grew slower year-over-year than their web-sales did. Four per-customer
        // net-paid pre-aggregates (store 2001/2002, web 2001/2002), self-joined on c_customer_id (a dictionary string
        // key -- 1:1 and non-null, so clean); keep customers with positive first-year totals on both channels whose
        // store growth ratio is below their web growth ratio (cross-multiplied). Report id + name; top 100.
        QueryLowering storeFirst = query74ChannelYearTotal("store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_net_paid", 2001);
        QueryLowering storeSecond = query74ChannelYearTotal("store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_net_paid", 2002);
        QueryLowering webFirst = query74ChannelYearTotal("web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_net_paid", 2001);
        QueryLowering webSecond = query74ChannelYearTotal("web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_net_paid", 2002);

        QueryLowering main = QueryLowering.scan("q74_store_first",
                        new QueryLowering.Column("sf_customer_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("sf_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("sf_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("sf_total", ColumnEncoding.FLAT, true))
                .join("q74_store_second", "sf_customer_id", "ss_customer_id",
                        new QueryLowering.Column("ss_customer_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("ss_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ss_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ss_total", ColumnEncoding.FLAT, true))
                .join("q74_web_first", "sf_customer_id", "wf_customer_id",
                        new QueryLowering.Column("wf_customer_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("wf_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("wf_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("wf_total", ColumnEncoding.FLAT, true))
                .join("q74_web_second", "sf_customer_id", "ws_customer_id",
                        new QueryLowering.Column("ws_customer_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("ws_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ws_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ws_total", ColumnEncoding.FLAT, true));
        main.where(
                        new Plan.Predicate(">", main.column("sf_total"), new Plan.Lit(0)),
                        new Plan.Predicate(">", main.column("wf_total"), new Plan.Lit(0)),
                        new Plan.Predicate("<",
                                new Plan.Bin("*", main.column("ss_total"), main.column("wf_total")),
                                new Plan.Bin("*", main.column("ws_total"), main.column("sf_total"))))
                .select(main.column("sf_customer_id"), main.column("sf_first"), main.column("sf_last"))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)), 100));

        return new Composite(
                List.of(new Stage(storeFirst, "q74_store_first", List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 1, 3))),
                        new Stage(storeSecond, "q74_store_second", List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 1, 3))),
                        new Stage(webFirst, "q74_web_first", List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 1, 3))),
                        new Stage(webSecond, "q74_web_second", List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 1, 3)))),
                main,
                List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2)));
    }

    /** Q31 per-county revenue for one channel and quarter of 2000; the grouped pre-aggregate (keyed by ca_county). */
    private static QueryLowering query31CountyQuarter(String table, String soldDateColumn, String addressColumn, String salesColumn, int quarter)
    {
        QueryLowering channel = QueryLowering.scan(table,
                        new QueryLowering.Column(soldDateColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(addressColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(salesColumn, ColumnEncoding.FLAT, true))
                .join("date_dim", soldDateColumn, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"),
                        new QueryLowering.Column("d_qoy"))
                .join("customer_address", addressColumn, "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_county", ColumnEncoding.STRING, true));
        channel.where(
                        new Plan.Predicate("=", channel.column("d_year"), new Plan.Lit(2000)),
                        new Plan.Predicate("=", channel.column("d_qoy"), new Plan.Lit(quarter)))
                .groupBy("ca_county")
                .aggregate("sum", salesColumn);
        // result: ca_county(0), revenue(1). customer_address is input 2 in this stage.
        return channel;
    }

    public static Composite query31()
    {
        // Q31: counties where web-sales grew faster than store-sales across the first three quarters of 2000. Six
        // per-county revenue pre-aggregates (store and web, qoy 1/2/3) self-join on ca_county (a nullable dictionary
        // string key -- null counties are dropped by the null-key-skip in the build); keep counties with positive
        // store/web q1+q2 revenue whose store quarter-over-quarter growth is below web's (cross-multiplied). Report the
        // county, year, and the four growth ratios; ordered by county.
        QueryLowering storeQ1 = query31CountyQuarter("store_sales", "ss_sold_date_sk", "ss_addr_sk", "ss_ext_sales_price", 1);
        QueryLowering storeQ2 = query31CountyQuarter("store_sales", "ss_sold_date_sk", "ss_addr_sk", "ss_ext_sales_price", 2);
        QueryLowering storeQ3 = query31CountyQuarter("store_sales", "ss_sold_date_sk", "ss_addr_sk", "ss_ext_sales_price", 3);
        QueryLowering webQ1 = query31CountyQuarter("web_sales", "ws_sold_date_sk", "ws_bill_addr_sk", "ws_ext_sales_price", 1);
        QueryLowering webQ2 = query31CountyQuarter("web_sales", "ws_sold_date_sk", "ws_bill_addr_sk", "ws_ext_sales_price", 2);
        QueryLowering webQ3 = query31CountyQuarter("web_sales", "ws_sold_date_sk", "ws_bill_addr_sk", "ws_ext_sales_price", 3);

        QueryLowering main = QueryLowering.scan("q31_store_q1",
                        new QueryLowering.Column("sc1_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("sc1_rev", ColumnEncoding.FLAT, true))
                .join("q31_store_q2", "sc1_county", "sc2_county",
                        new QueryLowering.Column("sc2_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("sc2_rev", ColumnEncoding.FLAT, true))
                .join("q31_store_q3", "sc1_county", "sc3_county",
                        new QueryLowering.Column("sc3_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("sc3_rev", ColumnEncoding.FLAT, true))
                .join("q31_web_q1", "sc1_county", "wc1_county",
                        new QueryLowering.Column("wc1_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("wc1_rev", ColumnEncoding.FLAT, true))
                .join("q31_web_q2", "sc1_county", "wc2_county",
                        new QueryLowering.Column("wc2_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("wc2_rev", ColumnEncoding.FLAT, true))
                .join("q31_web_q3", "sc1_county", "wc3_county",
                        new QueryLowering.Column("wc3_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("wc3_rev", ColumnEncoding.FLAT, true));
        main.where(
                        new Plan.Predicate(">", main.column("sc1_rev"), new Plan.Lit(0)),
                        new Plan.Predicate(">", main.column("sc2_rev"), new Plan.Lit(0)),
                        new Plan.Predicate(">", main.column("wc1_rev"), new Plan.Lit(0)),
                        new Plan.Predicate(">", main.column("wc2_rev"), new Plan.Lit(0)),
                        new Plan.Predicate("<",
                                new Plan.Bin("*", main.column("sc2_rev"), main.column("wc1_rev")),
                                new Plan.Bin("*", main.column("wc2_rev"), main.column("sc1_rev"))),
                        new Plan.Predicate("<",
                                new Plan.Bin("*", main.column("sc3_rev"), main.column("wc2_rev")),
                                new Plan.Bin("*", main.column("wc3_rev"), main.column("sc2_rev"))))
                .select(main.column("sc1_county"), new Plan.Lit(2000L),
                        new Plan.Call("divide_scale_round_i64", main.column("wc2_rev"), main.column("wc1_rev"), new Plan.Lit(1_000_000L)),
                        new Plan.Call("divide_scale_round_i64", main.column("sc2_rev"), main.column("sc1_rev"), new Plan.Lit(1_000_000L)),
                        new Plan.Call("divide_scale_round_i64", main.column("wc3_rev"), main.column("wc2_rev"), new Plan.Lit(1_000_000L)),
                        new Plan.Call("divide_scale_round_i64", main.column("sc3_rev"), main.column("sc2_rev"), new Plan.Lit(1_000_000L)))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 100));

        return new Composite(
                List.of(new Stage(storeQ1, "q31_store_q1", List.of(new DictRef(0, 2, 1))),
                        new Stage(storeQ2, "q31_store_q2", List.of(new DictRef(0, 2, 1))),
                        new Stage(storeQ3, "q31_store_q3", List.of(new DictRef(0, 2, 1))),
                        new Stage(webQ1, "q31_web_q1", List.of(new DictRef(0, 2, 1))),
                        new Stage(webQ2, "q31_web_q2", List.of(new DictRef(0, 2, 1))),
                        new Stage(webQ3, "q31_web_q3", List.of(new DictRef(0, 2, 1)))),
                main,
                List.of(new DictRef(0, 0, 0)));
    }

    /** Q04 per-customer "year total" (sum of (list-wholesale-discount+sales_price)) for one channel and year. */
    private static QueryLowering query04ChannelYearTotal(String table, String customerColumn, String soldDateColumn,
            String listPriceColumn, String wholesaleColumn, String discountColumn, String salesPriceColumn, int year)
    {
        QueryLowering channel = QueryLowering.scan(table,
                        new QueryLowering.Column(customerColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(soldDateColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(listPriceColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(wholesaleColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(discountColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(salesPriceColumn, ColumnEncoding.FLAT, true))
                .join("date_dim", soldDateColumn, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"));
        channel.where(new Plan.Predicate("=", channel.column("d_year"), new Plan.Lit(year)))
                .groupBy(customerColumn)
                .aggregate("sum", new Plan.Bin("+",
                        new Plan.Bin("-", new Plan.Bin("-", channel.column(listPriceColumn), channel.column(wholesaleColumn)), channel.column(discountColumn)),
                        channel.column(salesPriceColumn)));
        // result: customer_sk(0), year_total(1).
        return channel;
    }

    public static Composite query04()
    {
        // Q04: customers whose CATALOG year-over-year spend grew faster than BOTH their store and web spend. Six
        // per-customer year-total pre-aggregates (store/catalog/web × 2001/2002) self-join on c_customer_sk (a LONG
        // surrogate -- no string-keyed join); keep customers with positive 2001 totals on all three channels whose
        // catalog growth ratio exceeds store's and web's (cross-multiplied). Then join customer for id/name/flag;
        // ORDER BY those, top 100.
        QueryLowering s1 = query04ChannelYearTotal("store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_wholesale_cost", "ss_ext_discount_amt", "ss_ext_sales_price", 2001);
        QueryLowering s2 = query04ChannelYearTotal("store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_wholesale_cost", "ss_ext_discount_amt", "ss_ext_sales_price", 2002);
        QueryLowering c1 = query04ChannelYearTotal("catalog_sales", "cs_bill_customer_sk", "cs_sold_date_sk", "cs_ext_list_price", "cs_ext_wholesale_cost", "cs_ext_discount_amt", "cs_ext_sales_price", 2001);
        QueryLowering c2 = query04ChannelYearTotal("catalog_sales", "cs_bill_customer_sk", "cs_sold_date_sk", "cs_ext_list_price", "cs_ext_wholesale_cost", "cs_ext_discount_amt", "cs_ext_sales_price", 2002);
        QueryLowering w1 = query04ChannelYearTotal("web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_wholesale_cost", "ws_ext_discount_amt", "ws_ext_sales_price", 2001);
        QueryLowering w2 = query04ChannelYearTotal("web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_wholesale_cost", "ws_ext_discount_amt", "ws_ext_sales_price", 2002);

        QueryLowering main = QueryLowering.scan("q04_store_1",
                        new QueryLowering.Column("s1_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("s1_total", ColumnEncoding.FLAT, true))
                .join("q04_store_2", "s1_customer", "s2_customer",
                        new QueryLowering.Column("s2_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("s2_total", ColumnEncoding.FLAT, true))
                .join("q04_catalog_1", "s1_customer", "c1_customer",
                        new QueryLowering.Column("c1_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c1_total", ColumnEncoding.FLAT, true))
                .join("q04_catalog_2", "s1_customer", "c2_customer",
                        new QueryLowering.Column("c2_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c2_total", ColumnEncoding.FLAT, true))
                .join("q04_web_1", "s1_customer", "w1_customer",
                        new QueryLowering.Column("w1_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("w1_total", ColumnEncoding.FLAT, true))
                .join("q04_web_2", "s1_customer", "w2_customer",
                        new QueryLowering.Column("w2_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("w2_total", ColumnEncoding.FLAT, true))
                .join("customer", "s1_customer", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_customer_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_preferred_cust_flag", ColumnEncoding.STRING, true));
        main.where(
                        new Plan.Predicate(">", main.column("s1_total"), new Plan.Lit(0)),
                        new Plan.Predicate(">", main.column("c1_total"), new Plan.Lit(0)),
                        new Plan.Predicate(">", main.column("w1_total"), new Plan.Lit(0)),
                        new Plan.Predicate("<",
                                new Plan.Bin("*", main.column("s2_total"), main.column("c1_total")),
                                new Plan.Bin("*", main.column("c2_total"), main.column("s1_total"))),
                        new Plan.Predicate("<",
                                new Plan.Bin("*", main.column("w2_total"), main.column("c1_total")),
                                new Plan.Bin("*", main.column("c2_total"), main.column("w1_total"))))
                .select(main.column("c_customer_id"), main.column("c_first_name"), main.column("c_last_name"), main.column("c_preferred_cust_flag"))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false), new Plan.SortKey(3, false)), 100));

        return new Composite(
                List.of(new Stage(s1, "q04_store_1"), new Stage(s2, "q04_store_2"),
                        new Stage(c1, "q04_catalog_1"), new Stage(c2, "q04_catalog_2"),
                        new Stage(w1, "q04_web_1"), new Stage(w2, "q04_web_2")),
                main,
                List.of(new DictRef(0, 6, 1), new DictRef(1, 6, 2), new DictRef(2, 6, 3), new DictRef(3, 6, 4)));
    }

    /** Q11 per-customer "year total" (sum of list_price - discount) for one channel and year, keyed by customer identity. */
    private static QueryLowering query11ChannelYearTotal(String table, String customerColumn, String soldDateColumn, String listPriceColumn, String discountColumn, int year)
    {
        QueryLowering channel = QueryLowering.scan(table,
                        new QueryLowering.Column(customerColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(soldDateColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(listPriceColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(discountColumn, ColumnEncoding.FLAT, true))
                .join("customer", customerColumn, "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_customer_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_preferred_cust_flag", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_birth_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_login", ColumnEncoding.STRING, true))
                .join("date_dim", soldDateColumn, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"));
        channel.where(new Plan.Predicate("=", channel.column("d_year"), new Plan.Lit(year)))
                .groupBy("c_customer_id", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_country", "c_login")
                .aggregate("sum", new Plan.Bin("-", channel.column(listPriceColumn), channel.column(discountColumn)));
        // result: customer_id(0), first(1), last(2), preferred(3), birth_country(4), login(5), year_total(6). customer is input 1.
        return channel;
    }

    public static Composite query11()
    {
        // Q11: customers whose store-sales grew slower year-over-year than their web-sales (the Q74 shape, but the year
        // total is sum(list_price - discount) and the grouping/output carry six customer-identity columns). Four
        // per-customer pre-aggregates (store/web x 2001/2002) self-join on c_customer_id (a non-null dictionary string
        // key); keep customers with positive 2001 totals on both channels whose store growth ratio is below web's
        // (cross-multiplied). Report the six identity columns; top 100 by the first four.
        QueryLowering storeFirst = query11ChannelYearTotal("store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_discount_amt", 2001);
        QueryLowering storeSecond = query11ChannelYearTotal("store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_discount_amt", 2002);
        QueryLowering webFirst = query11ChannelYearTotal("web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_discount_amt", 2001);
        QueryLowering webSecond = query11ChannelYearTotal("web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_discount_amt", 2002);

        QueryLowering main = QueryLowering.scan("q11_store_first",
                        new QueryLowering.Column("sf_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("sf_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("sf_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("sf_pref", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("sf_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("sf_login", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("sf_total", ColumnEncoding.FLAT, true))
                .join("q11_store_second", "sf_id", "ss_id",
                        new QueryLowering.Column("ss_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("ss_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ss_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ss_pref", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ss_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ss_login", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ss_total", ColumnEncoding.FLAT, true))
                .join("q11_web_first", "sf_id", "wf_id",
                        new QueryLowering.Column("wf_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("wf_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("wf_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("wf_pref", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("wf_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("wf_login", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("wf_total", ColumnEncoding.FLAT, true))
                .join("q11_web_second", "sf_id", "ws_id",
                        new QueryLowering.Column("ws_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("ws_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ws_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ws_pref", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ws_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ws_login", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ws_total", ColumnEncoding.FLAT, true));
        main.where(
                        new Plan.Predicate(">", main.column("sf_total"), new Plan.Lit(0)),
                        new Plan.Predicate(">", main.column("wf_total"), new Plan.Lit(0)),
                        new Plan.Predicate("<",
                                new Plan.Bin("*", main.column("ss_total"), main.column("wf_total")),
                                new Plan.Bin("*", main.column("ws_total"), main.column("sf_total"))))
                .select(main.column("sf_id"), main.column("sf_first"), main.column("sf_last"),
                        main.column("sf_pref"), main.column("sf_country"), main.column("sf_login"))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false), new Plan.SortKey(3, false)), 100));

        List<DictRef> stageStrings = List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 1, 3),
                new DictRef(3, 1, 4), new DictRef(4, 1, 5), new DictRef(5, 1, 6));
        return new Composite(
                List.of(new Stage(storeFirst, "q11_store_first", stageStrings),
                        new Stage(storeSecond, "q11_store_second", stageStrings),
                        new Stage(webFirst, "q11_web_first", stageStrings),
                        new Stage(webSecond, "q11_web_second", stageStrings)),
                main,
                List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2),
                        new DictRef(3, 0, 3), new DictRef(4, 0, 4), new DictRef(5, 0, 5)));
    }

    public static Composite query93()
    {
        // Q93: per-customer net store-sales value after returns of "reason 28". Mirrors the harness operator tree -- the
        // filtered returns are BUILT first as a small relation, then probed by store_sales (rather than a left-deep chain
        // that builds the full store_returns and filters the reason late). Stage q93_returns: store_returns joined to
        // reason (filter 'reason 28'), output (item, ticket, return_quantity). Main: store_sales joined to that on
        // (item, ticket); each surviving sale contributes sales_price * (quantity - return_quantity); GROUP BY customer;
        // ORDER BY the total then customer; top 100. (The INNER join makes return_quantity non-null, so the SQL CASE on a
        // null return quantity lowers to the direct subtract.)
        QueryLowering returns = QueryLowering.scan("store_returns",
                        new QueryLowering.Column("sr_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_reason_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_return_quantity", ColumnEncoding.FLAT, true))
                .join("reason", "sr_reason_sk", "r_reason_sk",
                        new QueryLowering.Column("r_reason_sk"),
                        new QueryLowering.Column("r_reason_desc", ColumnEncoding.STRING, false));
        returns.where(new Plan.StringMatch(returns.position("r_reason_desc"), List.of("reason 28"), false))
                .select(returns.column("sr_item_sk"), returns.column("sr_ticket_number"), returns.column("sr_return_quantity"));
        // q93_returns: item(0), ticket(1), return_quantity(2).

        QueryLowering main = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_quantity", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true))
                .join("q93_returns", new String[] {"ss_item_sk", "ss_ticket_number"}, new String[] {"qr_item", "qr_ticket"},
                        new QueryLowering.Column("qr_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("qr_ticket", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("qr_return_quantity", ColumnEncoding.FLAT, false));
        main.groupBy("ss_customer_sk")
                .aggregate("sum", new Plan.Bin("*", main.column("ss_sales_price"),
                        new Plan.Bin("-", main.column("ss_quantity"), main.column("qr_return_quantity"))))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, false), new Plan.SortKey(0, false)), 100));

        return new Composite(List.of(new Stage(returns, "q93_returns")), main, List.of());
    }

    public static Ported query29()
    {
        // Q29: store_sales ⋈ date_dim(sold: d_moy=9, d_year=1999) ⋈ item ⋈ store ⋈ store_returns (multi-key on
        // customer+item+ticket) ⋈ date_dim(returned: d_moy 9..12, d_year=1999) ⋈ catalog_sales (multi-key on the
        // RETURN's customer+item -- a snowflake, one-to-many fact-to-fact join) ⋈ date_dim(catalog: d_year in
        // 1999/2000/2001); GROUP BY item id/desc, store id/name; sum(ss_quantity), sum(sr_return_quantity),
        // sum(cs_quantity); ORDER BY the four group columns, top 100. date_dim is joined three times, so the returned
        // and catalog instances load their columns under aliases.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_quantity", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_item_desc", ColumnEncoding.STRING, false))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_store_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, false))
                .join("store_returns",
                        new String[] {"ss_customer_sk", "ss_item_sk", "ss_ticket_number"},
                        new String[] {"sr_customer_sk", "sr_item_sk", "sr_ticket_number"},
                        new QueryLowering.Column("sr_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_returned_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_return_quantity", ColumnEncoding.FLAT, true))
                .join("date_dim", "sr_returned_date_sk", "d_date_sk_returned",
                        new QueryLowering.Column("d_date_sk_returned", "d_date_sk", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_moy_returned", "d_moy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_year_returned", "d_year", ColumnEncoding.FLAT, false))
                .join("catalog_sales",
                        new String[] {"sr_customer_sk", "sr_item_sk"},
                        new String[] {"cs_bill_customer_sk", "cs_item_sk"},
                        new QueryLowering.Column("cs_bill_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_quantity", ColumnEncoding.FLAT, true))
                .join("date_dim", "cs_sold_date_sk", "d_date_sk_catalog",
                        new QueryLowering.Column("d_date_sk_catalog", "d_date_sk", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_year_catalog", "d_year", ColumnEncoding.FLAT, false));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(9)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(1999)),
                        new Plan.Predicate(">=", query.column("d_moy_returned"), new Plan.Lit(9)),
                        new Plan.Predicate("<=", query.column("d_moy_returned"), new Plan.Lit(12)),
                        new Plan.Predicate("=", query.column("d_year_returned"), new Plan.Lit(1999)),
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", query.column("d_year_catalog"), new Plan.Lit(1999)),
                                new Plan.Predicate("=", query.column("d_year_catalog"), new Plan.Lit(2000)),
                                new Plan.Predicate("=", query.column("d_year_catalog"), new Plan.Lit(2001)))))
                .groupBy("i_item_id", "i_item_desc", "s_store_id", "s_store_name")
                .aggregate("sum", "ss_quantity")
                .aggregate("sum", "sr_return_quantity")
                .aggregate("sum", "cs_quantity")
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false),
                        new Plan.SortKey(2, false), new Plan.SortKey(3, false)), 100));
        // The four string group columns reconstruct from item (input 2, cols 1-2) and store (input 3, cols 1-2).
        return new Ported(query, List.of(
                new DictRef(0, 2, 1), new DictRef(1, 2, 2), new DictRef(2, 3, 1), new DictRef(3, 3, 2)));
    }

    public static Ported query50()
    {
        // Q50: store_sales ⋈ store_returns (multi-key fact-to-fact join on ticket+item+customer) ⋈ date_dim(sold)
        // ⋈ date_dim(returned: d_year=2001, d_moy=8) ⋈ store; GROUP BY 10 store address columns; five sum(CASE) counts
        // bucketing the return delay days = sr_returned_date_sk - ss_sold_date_sk into <=30 / 31-60 / 61-90 / 91-120 /
        // >120 (date_sks are day-sequential, so their difference is the day count); ORDER BY the 10 columns, top 100.
        // date_dim is joined twice (sold + returned), so the returned key is loaded under the alias d_date_sk_returned.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true))
                .join("store_returns",
                        new String[] {"ss_ticket_number", "ss_item_sk", "ss_customer_sk"},
                        new String[] {"sr_ticket_number", "sr_item_sk", "sr_customer_sk"},
                        new QueryLowering.Column("sr_returned_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_customer_sk", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"))
                .join("date_dim", "sr_returned_date_sk", "d_date_sk_returned",
                        new QueryLowering.Column("d_date_sk_returned", "d_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_year"),
                        new QueryLowering.Column("d_moy"))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_company_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("s_street_number", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_street_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_street_type", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_suite_number", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_city", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_zip", ColumnEncoding.STRING, true));
        Plan.Expr days = new Plan.Bin("-", query.column("sr_returned_date_sk"), query.column("ss_sold_date_sk"));
        query.where(
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2001)),
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(8)))
                .groupBy("s_store_name", "s_company_id", "s_street_number", "s_street_name", "s_street_type",
                        "s_suite_number", "s_city", "s_county", "s_state", "s_zip")
                .aggregate("sum", bucket(new Plan.Predicate("<", days, new Plan.Lit(31))))
                .aggregate("sum", bucket(new Plan.And(
                        new Plan.Predicate(">", days, new Plan.Lit(30)), new Plan.Predicate("<", days, new Plan.Lit(61)))))
                .aggregate("sum", bucket(new Plan.And(
                        new Plan.Predicate(">", days, new Plan.Lit(60)), new Plan.Predicate("<", days, new Plan.Lit(91)))))
                .aggregate("sum", bucket(new Plan.And(
                        new Plan.Predicate(">", days, new Plan.Lit(90)), new Plan.Predicate("<", days, new Plan.Lit(121)))))
                .aggregate("sum", bucket(new Plan.Predicate(">", days, new Plan.Lit(120))))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false),
                        new Plan.SortKey(3, false), new Plan.SortKey(4, false), new Plan.SortKey(5, false),
                        new Plan.SortKey(6, false), new Plan.SortKey(7, false), new Plan.SortKey(8, false),
                        new Plan.SortKey(9, false)), 100));
        // The 9 string group columns (all but s_company_id, output col 1) reconstruct from the store build (input 4).
        return new Ported(query, List.of(
                new DictRef(0, 4, 1), new DictRef(2, 4, 3), new DictRef(3, 4, 4), new DictRef(4, 4, 5),
                new DictRef(5, 4, 6), new DictRef(6, 4, 7), new DictRef(7, 4, 8), new DictRef(8, 4, 9),
                new DictRef(9, 4, 10)));
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
        // Q62: web_sales joined to date_dim (d_month_seq in [1200,1211]), warehouse, ship_mode, and web_site; GROUP BY
        // warehouse name, ship-mode type, web-site name; five sum(CASE) counts bucketing the shipping delay
        // days = ws_ship_date_sk - ws_sold_date_sk into 0-30 / 31-60 / 61-90 / 91-120 / >120; ORDER BY the three names
        // LIMIT 100. The warehouse name is genuinely NULL for one warehouse, so the string group keys are declared
        // nullable (a null name forms its own group), and days is null when sold_date is null, falling through to 0.
        QueryLowering query = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_ship_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_ship_mode_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_web_site_sk", ColumnEncoding.FLAT, true))
                .join("date_dim", "ws_ship_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_month_seq"))
                .join("warehouse", "ws_warehouse_sk", "w_warehouse_sk",
                        new QueryLowering.Column("w_warehouse_sk"),
                        new QueryLowering.Column("w_warehouse_name", ColumnEncoding.STRING, true))
                .join("ship_mode", "ws_ship_mode_sk", "sm_ship_mode_sk",
                        new QueryLowering.Column("sm_ship_mode_sk"),
                        new QueryLowering.Column("sm_type", ColumnEncoding.STRING, true))
                .join("web_site", "ws_web_site_sk", "web_site_sk",
                        new QueryLowering.Column("web_site_sk"),
                        new QueryLowering.Column("web_name", ColumnEncoding.STRING, true));
        Plan.Expr days = new Plan.Bin("-", query.column("ws_ship_date_sk"), query.column("ws_sold_date_sk"));
        query.where(
                        new Plan.Predicate(">", query.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", query.column("d_month_seq"), new Plan.Lit(1212)))
                .groupBy("w_warehouse_name", "sm_type", "web_name")
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

    public static Ported query99()
    {
        // Q99: catalog_sales joined to date_dim (d_month_seq in [1200,1211]), warehouse, ship_mode, and call_center;
        // GROUP BY warehouse name, ship-mode type, call-center name; five sum(CASE) counts bucketing the shipping delay
        // days = cs_ship_date_sk - cs_sold_date_sk into 0-30 / 31-60 / 61-90 / 91-120 / >120; ORDER BY the three names
        // LIMIT 100. Same nullable-name / null-days handling as Q62, over catalog_sales and the call_center dimension.
        QueryLowering query = QueryLowering.scan("catalog_sales",
                        new QueryLowering.Column("cs_ship_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_ship_mode_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_call_center_sk", ColumnEncoding.FLAT, true))
                .join("date_dim", "cs_ship_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_month_seq"))
                .join("warehouse", "cs_warehouse_sk", "w_warehouse_sk",
                        new QueryLowering.Column("w_warehouse_sk"),
                        new QueryLowering.Column("w_warehouse_name", ColumnEncoding.STRING, true))
                .join("ship_mode", "cs_ship_mode_sk", "sm_ship_mode_sk",
                        new QueryLowering.Column("sm_ship_mode_sk"),
                        new QueryLowering.Column("sm_type", ColumnEncoding.STRING, true))
                .join("call_center", "cs_call_center_sk", "cc_call_center_sk",
                        new QueryLowering.Column("cc_call_center_sk"),
                        new QueryLowering.Column("cc_name", ColumnEncoding.STRING, true));
        Plan.Expr days = new Plan.Bin("-", query.column("cs_ship_date_sk"), query.column("cs_sold_date_sk"));
        query.where(
                        new Plan.Predicate(">", query.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", query.column("d_month_seq"), new Plan.Lit(1212)))
                .groupBy("w_warehouse_name", "sm_type", "cc_name")
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

    public static Composite query22()
    {
        // inventory ⋈ date_dim(d_month_seq in [1200, 1211]); PRE-AGGREGATE by inv_item_sk into a partial sum
        // (sum(coalesce(qoh, 0))) and a partial count (count(*)) -- exactly the harness's per-item grouping that
        // breaks the pipeline before the rollup -- then ⋈ item; ROLLUP(i_product_name, i_brand, i_class, i_category)
        // re-summing the two partials; avg = sum/count as a true double. ORDER BY the average then the four item keys;
        // top 100. The rollup's 5x grouping-set expand now runs over the small per-item relation, not the ~11M-row
        // fact. Re-summing the partials is algebraically identical to summing the raw fact, so the result is byte-exact.
        QueryLowering inventoryByItem = QueryLowering.scan("inventory",
                        new QueryLowering.Column("inv_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_quantity_on_hand", ColumnEncoding.FLAT, true))
                .join("date_dim", "inv_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_month_seq"));
        inventoryByItem.where(
                        new Plan.Predicate(">", inventoryByItem.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", inventoryByItem.column("d_month_seq"), new Plan.Lit(1212)))
                .groupBy("inv_item_sk")
                .aggregate("sum", new Plan.Coalesce(inventoryByItem.column("inv_quantity_on_hand"), new Plan.Lit(0)))
                .count();
        // Pre-aggregation output: (item_sk = 0, partial_sum = 1, partial_count = 2).

        // The pre-aggregation's group key and its two aggregates are all non-null (the group key exists, count(*) is
        // never null, and sum(coalesce(...)) over >= 1 row is non-null), so the virtual columns are non-nullable.
        QueryLowering main = QueryLowering.scan("q22_inv_by_item",
                        new QueryLowering.Column("ibi_item_sk", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("ibi_partial_sum", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("ibi_partial_count", ColumnEncoding.FLAT, false))
                .join("item", "ibi_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_product_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_class", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, false));
        main.groupBy("i_product_name", "i_brand", "i_class", "i_category")
                .groupingSets(List.of(
                        new int[] {0, 1, 2, 3},
                        new int[] {0, 1, 2},
                        new int[] {0, 1},
                        new int[] {0},
                        new int[0]))
                .aggregate("sum", "ibi_partial_sum")
                .aggregate("sum", "ibi_partial_count");
        // Main result columns: i_product_name (0), i_brand (1), i_class (2), i_category (3), sum (4), count (5),
        // grouping_id (6). The average is the computed quotient sum/count as a true double.
        Plan.Expr average = new Plan.Call("divide_i64_to_f64", new Plan.Col(4), new Plan.Col(5));
        main.select(
                        new Plan.Col(0),    // i_product_name
                        new Plan.Col(1),    // i_brand
                        new Plan.Col(2),    // i_class
                        new Plan.Col(3),    // i_category
                        average)            // avg(inv_quantity_on_hand)
                .orderBy(new Plan.Ordering(List.of(
                        Plan.SortKey.expression(average, Types.DOUBLE, false),
                        new Plan.SortKey(0, false),
                        new Plan.SortKey(1, false),
                        new Plan.SortKey(2, false),
                        new Plan.SortKey(3, false)), 100));

        List<Stage> stages = List.of(new Stage(inventoryByItem, "q22_inv_by_item"));
        // The four rolled-up string keys reconstruct from the item build (main input 1): i_product_name/i_brand/
        // i_class/i_category are item columns 1/2/3/4. ROLLUP nulls trailing keys per level; the null mask wins.
        return new Composite(stages, main, List.of(
                new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 1, 3), new DictRef(3, 1, 4)));
    }
}
