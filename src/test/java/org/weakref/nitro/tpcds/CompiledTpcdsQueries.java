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
        return demographicsAverages("store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_cdemo_sk", "ss_promo_sk",
                "ss_quantity", "ss_list_price", "ss_coupon_amt", "ss_sales_price", false);
    }

    public static Ported query26()
    {
        return demographicsAverages("catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_bill_cdemo_sk", "cs_promo_sk",
                "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", true);
    }

    /**
     * The shared Q07/Q26 shape: a sales table joined to date_dim, item, customer_demographics, and promotion;
     * GROUP BY i_item_id; four averages; ORDER BY i_item_id LIMIT 100. {@code nullableDimensionStrings} declares
     * the demographic/promotion filter columns nullable so the three-valued null guard applies (needed on catalog).
     */
    private static Ported demographicsAverages(String salesTable, String soldDate, String item, String cdemo, String promo,
            String quantity, String listPrice, String couponAmt, String salesPrice, boolean nullableDimensionStrings)
    {
        QueryLowering query = QueryLowering.scan(salesTable,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(item, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(cdemo, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(promo, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(quantity, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(listPrice, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(couponAmt, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(salesPrice, ColumnEncoding.FLAT, true))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"))
                .join("item", item, "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false))
                .join("customer_demographics", cdemo, "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_gender", ColumnEncoding.STRING, nullableDimensionStrings),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, nullableDimensionStrings),
                        new QueryLowering.Column("cd_education_status", ColumnEncoding.STRING, nullableDimensionStrings))
                .join("promotion", promo, "p_promo_sk",
                        new QueryLowering.Column("p_promo_sk"),
                        new QueryLowering.Column("p_channel_email", ColumnEncoding.STRING, nullableDimensionStrings),
                        new QueryLowering.Column("p_channel_event", ColumnEncoding.STRING, nullableDimensionStrings));
        query.where(
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2000)),
                        new Plan.StringMatch(query.position("cd_gender"), List.of("M"), false),
                        new Plan.StringMatch(query.position("cd_marital_status"), List.of("S"), false),
                        new Plan.StringMatch(query.position("cd_education_status"), List.of("College"), false),
                        new Plan.Or(List.of(
                                new Plan.StringMatch(query.position("p_channel_email"), List.of("N"), false),
                                new Plan.StringMatch(query.position("p_channel_event"), List.of("N"), false))))
                .groupBy("i_item_id")
                .aggregate("avg", quantity)
                .aggregate("avg", listPrice)
                .aggregate("avg", couponAmt)
                .aggregate("avg", salesPrice)
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 100));
        return new Ported(query, 0, 2, 1);
    }

    public static Ported query42()
    {
        return brandOrCategoryByYear("i_category_id", "i_category", 2000,
                List.of(new Plan.SortKey(3, true), new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)));
    }

    public static Ported query52()
    {
        return brandOrCategoryByYear("i_brand_id", "i_brand", 2000,
                List.of(new Plan.SortKey(0, false), new Plan.SortKey(3, true), new Plan.SortKey(1, false)));
    }

    /** Q42/Q52 shape: store_sales ⋈ date_dim(d_moy=11,d_year) ⋈ item(i_manager_id=1); GROUP BY d_year + a category/brand pair; sum; top 100. */
    private static Ported brandOrCategoryByYear(String idColumn, String nameColumn, long year, List<Plan.SortKey> order)
    {
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk"),
                        new QueryLowering.Column("ss_item_sk"),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manager_id"),
                        new QueryLowering.Column(idColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(nameColumn, ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(11)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(year)),
                        new Plan.Predicate("=", query.column("i_manager_id"), new Plan.Lit(1)))
                .groupBy("d_year", idColumn, nameColumn)
                .aggregate("sum", "ss_ext_sales_price")
                .orderBy(new Plan.Ordering(order, 100));
        return new Ported(query, 2, 2, 3);
    }

    public static Ported query55()
    {
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk"),
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
}
