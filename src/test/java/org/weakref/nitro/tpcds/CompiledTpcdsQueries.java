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

    /** A ported query: its name-based lowering and where to find a string result column's dictionary. */
    public record Ported(QueryLowering query, int stringResultColumn, int dictInput, int dictColumn) {}

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
