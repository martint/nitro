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
    /**
     * Locates a string result column's source dictionary: result column {@code resultColumn} stores a dictionary id
     * into build {@code dictInput}'s scanned column {@code dictColumn}. An optional UTF-8 {@code substring}
     * ({@code substringStart} 1-based, {@code substringLength} code points; {@code substringLength < 0} = whole value)
     * is applied to each dictionary entry at reconstruction, matching a trailing {@code substring(col, start, length)}
     * output projection. NOTE: ordering still runs on the un-truncated dictionary ids, so this is only byte-exact when
     * the substring preserves the columns' relative order (it does whenever distinct values keep distinct prefixes).
     *
     * <p>When {@code literal} is non-null the result column is a constant string: the pipeline emits a placeholder LONG
     * column (e.g. {@code Lit(0)}) and reconstruction maps every row to {@code literal}, matching a constant string
     * projection. The {@code dictInput}/{@code dictColumn} are then ignored.
     */
    public record DictRef(int resultColumn, int dictInput, int dictColumn, int substringStart, int substringLength, String literal, String prefix)
    {
        public DictRef(int resultColumn, int dictInput, int dictColumn)
        {
            this(resultColumn, dictInput, dictColumn, 1, -1, null, null);
        }

        public DictRef(int resultColumn, int dictInput, int dictColumn, int substringStart, int substringLength)
        {
            this(resultColumn, dictInput, dictColumn, substringStart, substringLength, null, null);
        }

        /** A constant string result column (backed by a placeholder numeric column in the pipeline). */
        public static DictRef literal(int resultColumn, String value)
        {
            return new DictRef(resultColumn, 0, 0, 1, -1, value, null);
        }

        /**
         * A string column reconstructed with a constant prefix prepended to every dictionary entry -- the
         * dictionary-level form of {@code 'prefix' || column}. A constant prefix preserves the dictionary's byte
         * order, so id-based ordering and grouping are unaffected.
         */
        public static DictRef prefixed(int resultColumn, int dictInput, int dictColumn, String prefix)
        {
            return new DictRef(resultColumn, dictInput, dictColumn, 1, -1, null, prefix);
        }
    }

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

    public static Union query76()
    {
        // Q76: per (channel, null-column, year, quarter, category) count and total ext-sales of sales rows where a
        // particular dimension key IS NULL -- one branch per channel, each labelling its rows with a constant channel
        // and null-column name. UNION ALL of the three branches, then top 100 by the five group columns. The constant
        // labels are emitted as LitStr; the union merges each branch's labels (and categories) into one dictionary.
        List<QueryLowering> branches = List.of(
                query76ChannelCategorySales("store_sales", "ss_store_sk", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price", "store", "ss_store_sk"),
                query76ChannelCategorySales("web_sales", "ws_ship_customer_sk", "ws_sold_date_sk", "ws_item_sk", "ws_ext_sales_price", "web", "ws_ship_customer_sk"),
                query76ChannelCategorySales("catalog_sales", "cs_ship_addr_sk", "cs_sold_date_sk", "cs_item_sk", "cs_ext_sales_price", "catalog", "cs_ship_addr_sk"));

        QueryLowering main = QueryLowering.scan("__q76_union__",
                        new QueryLowering.Column("u_channel", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("u_col_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("u_year", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("u_qoy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("u_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("u_count", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("u_sum", ColumnEncoding.FLAT, true));
        main.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4), new Plan.Col(5), new Plan.Col(6))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false),
                        new Plan.SortKey(3, false), new Plan.SortKey(4, false)), 100));

        return new Union(branches, main, "__q76_union__",
                List.of(new DictRef(4, 2, 1)),   // each branch's category (result col 4) <- item (build 2) col 1
                List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(4, 0, 4)));
    }

    private static QueryLowering query76ChannelCategorySales(String salesTable, String nullableColumn, String soldDate,
            String item, String sales, String channelName, String columnName)
    {
        QueryLowering branch = QueryLowering.scan(salesTable,
                        new QueryLowering.Column(nullableColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(item, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(sales, ColumnEncoding.FLAT, true))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_qoy", ColumnEncoding.FLAT, true))
                .join("item", item, "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true));
        branch.where(new Plan.IsNull(branch.position(nullableColumn)))
                .groupBy("d_year", "d_qoy", "i_category")
                .count()
                .aggregate("sum", sales);
        // grouped result: year(0), qoy(1), category(2), count(3), sum(4).
        branch.select(new Plan.LitStr(channelName), new Plan.LitStr(columnName),
                new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4));
        return branch;
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

    /**
     * A UNION ALL materialized into a virtual table that a tree of downstream stages then consumes -- the shape behind
     * "union feeding an aggregate" (e.g. the customers-in-all-channels count). {@code branches} are concatenated under
     * {@code unionVirtualName} (string branch outputs unified per {@code branchStringColumns}); {@code stages} then run
     * in order over that relation exactly as in a {@link Composite}, and {@code main} produces the final result.
     */
    public record UnionComposite(List<QueryLowering> branches, String unionVirtualName, List<DictRef> branchStringColumns,
            List<Stage> stages, QueryLowering main, List<DictRef> stringColumns) {}

    /**
     * A year-over-year self-join of one union-aggregate subquery (Q75/Q02): the same {@code branches} (per-channel
     * sales-minus-returns) are unioned and grouped TWICE -- once for the current year, once for the previous -- and the
     * two grouped relations are joined by {@code main}. The subquery is assembled twice (no reuse), matching the
     * operator harness. The {@code currentGroup}/{@code previousGroup} pipelines scan {@code currentUnion}/
     * {@code previousUnion}; they materialize under {@code currentVirtual}/{@code previousVirtual} for {@code main}.
     */
    public record UnionSelfJoin(List<QueryLowering> branches, String currentUnion, String previousUnion,
            QueryLowering currentGroup, String currentVirtual, QueryLowering previousGroup, String previousVirtual,
            QueryLowering main, List<DictRef> stringColumns) {}

    /**
     * The Q51 cumulative web-vs-store shape: each channel's weekly sales are grouped per (item, date) and running-summed
     * over date ({@code webGrouped}/{@code webWindow}, {@code storeGrouped}/{@code storeWindow}); the two windowed,
     * channel-tagged relations are concatenated, re-grouped per (item, date) to merge the channels ({@code merged}),
     * running-maxed to forward-fill each channel's latest cumulative, then filtered and ordered ({@code main}).
     */
    public record RunningCumulative(QueryLowering webGrouped, QueryLowering webWindow,
            QueryLowering storeGrouped, QueryLowering storeWindow, QueryLowering merged, QueryLowering main) {}

    public static RunningCumulative query51()
    {
        // Q51: the items whose cumulative web sales overtake their cumulative store sales over a 12-month window. Each
        // channel's per-(item, date) sales are running-summed over date; the two channels are merged per (item, date);
        // each cumulative is forward-filled with a running max (so a date with no sale carries the latest total); rows
        // where the store cumulative is below the web cumulative are kept, ordered by (item, date), top 100.
        return new RunningCumulative(
                query51Grouped("web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_sales_price"),
                query51Window("q51_web_grouped", true),
                query51Grouped("store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_sales_price"),
                query51Window("q51_store_grouped", false),
                query51Merged(),
                query51Main());
    }

    private static QueryLowering query51Grouped(String salesTable, String soldDate, String item, String price)
    {
        QueryLowering grouped = QueryLowering.scan(salesTable,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(item, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(price, ColumnEncoding.FLAT, true))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_month_seq", ColumnEncoding.FLAT, true));
        grouped.where(
                        new Plan.IsNull(grouped.position(item), true),
                        new Plan.Predicate(">", grouped.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", grouped.column("d_month_seq"), new Plan.Lit(1212)))
                .groupBy(item, "d_date")
                .aggregate("sum", price);
        // grouped result: item(0), d_date(1), sum_price(2).
        return grouped;
    }

    private static QueryLowering query51Window(String groupedVirtual, boolean web)
    {
        QueryLowering window = QueryLowering.scan(groupedVirtual,
                        new QueryLowering.Column("g_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_date", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_sum", ColumnEncoding.FLAT, true));
        window.window(Plan.Window.running(new int[] {0}, List.of(new Plan.SortKey(1, false)),
                List.of(new Plan.WindowAggregate("sum", 2))));
        // after window: item(0), date(1), sum(2), cumulative(3). Tag the cumulative into web/store and null the other.
        if (web) {
            window.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(3), new Plan.NullLit());
        }
        else {
            window.select(new Plan.Col(0), new Plan.Col(1), new Plan.NullLit(), new Plan.Col(3));
        }
        return window;
    }

    private static QueryLowering query51Merged()
    {
        QueryLowering merged = QueryLowering.scan("q51_union",
                        new QueryLowering.Column("u_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("u_date", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("u_web", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_store", ColumnEncoding.FLAT, true));
        merged.groupBy("u_item", "u_date")
                .aggregate("max", "u_web")
                .aggregate("max", "u_store");
        // merged result: item(0), date(1), web_cume(2), store_cume(3).
        return merged;
    }

    private static QueryLowering query51Main()
    {
        QueryLowering main = QueryLowering.scan("q51_merged",
                        new QueryLowering.Column("m_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("m_date", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("m_web", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("m_store", ColumnEncoding.FLAT, true));
        main.window(Plan.Window.running(new int[] {0}, List.of(new Plan.SortKey(1, false)),
                List.of(new Plan.WindowAggregate("max", 2), new Plan.WindowAggregate("max", 3))));
        // after window: item(0), date(1), web(2), store(3), running_web(4), running_store(5). Keep store < web cumulative.
        main.having(new Plan.Predicate("<", new Plan.Col(5), new Plan.Col(4)))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100));
        return main;
    }

    public static UnionSelfJoin query75()
    {
        // Q75: year-over-year change in Books unit sales per (brand, class, category, manufacturer). Each channel's
        // net sales (sold quantity/amount minus returns, via a LEFT join) for 2001 and 2002 are unioned and grouped;
        // the grouped relation is assembled twice (current year 2002, previous 2001) and self-joined on the four item
        // ids, kept where the current count dropped more than 10% (10*current < 9*previous), and the count/amount
        // differences are reported, top 100. All keys are numeric item ids -- no dictionary strings.
        List<QueryLowering> branches = List.of(
                query75Channel("catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_order_number", "cs_quantity", "cs_ext_sales_price", "catalog_returns", "cr_item_sk", "cr_order_number", "cr_return_quantity", "cr_return_amount"),
                query75Channel("store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ticket_number", "ss_quantity", "ss_ext_sales_price", "store_returns", "sr_item_sk", "sr_ticket_number", "sr_return_quantity", "sr_return_amt"),
                query75Channel("web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_order_number", "ws_quantity", "ws_ext_sales_price", "web_returns", "wr_item_sk", "wr_order_number", "wr_return_quantity", "wr_return_amt"));

        QueryLowering currentGroup = query75YearGroup("q75_current_union", 2002);
        QueryLowering previousGroup = query75YearGroup("q75_previous_union", 2001);

        QueryLowering main = QueryLowering.scan("q75_current",
                        new QueryLowering.Column("c_year", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("c_brand", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_class", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_category", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_manufact", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_qty", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("c_amt", ColumnEncoding.FLAT, false))
                .join("q75_previous",
                        new String[] {"c_brand", "c_class", "c_category", "c_manufact"},
                        new String[] {"p_brand", "p_class", "p_category", "p_manufact"},
                        new QueryLowering.Column("p_year", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("p_brand", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("p_class", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("p_category", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("p_manufact", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("p_qty", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("p_amt", ColumnEncoding.FLAT, false));
        // after the join: current {year(0), brand(1), class(2), category(3), manufact(4), qty(5), amt(6)},
        // previous {year(7), brand(8), class(9), category(10), manufact(11), qty(12), amt(13)}.
        main.where(new Plan.Predicate("<",
                        new Plan.Bin("*", new Plan.Col(5), new Plan.Lit(10)),
                        new Plan.Bin("*", new Plan.Col(12), new Plan.Lit(9))))
                .select(new Plan.Col(7), new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4),
                        new Plan.Col(12), new Plan.Col(5),
                        new Plan.Bin("-", new Plan.Col(5), new Plan.Col(12)),
                        new Plan.Bin("-", new Plan.Col(6), new Plan.Col(13)))
                // ORDER BY runs over the projected output: the count and amount differences (columns 8 and 9).
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(8, false), new Plan.SortKey(9, false)), 100));

        return new UnionSelfJoin(branches, "q75_current_union", "q75_previous_union",
                currentGroup, "q75_current", previousGroup, "q75_previous", main, List.of());
    }

    private static QueryLowering query75YearGroup(String unionVirtual, int year)
    {
        QueryLowering group = QueryLowering.scan(unionVirtual,
                        new QueryLowering.Column("u_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_brand", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_class", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_category", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_manufact", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_qty", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_amt", ColumnEncoding.FLAT, true));
        group.groupBy("u_year", "u_brand", "u_class", "u_category", "u_manufact")
                .aggregate("sum", "u_qty")
                .aggregate("sum", "u_amt");
        // grouped: year(0), brand(1), class(2), category(3), manufact(4), sum_qty(5), sum_amt(6).
        group.having(new Plan.Predicate("=", new Plan.Col(0), new Plan.Lit(year)));
        return group;
    }

    private static QueryLowering query75Channel(String salesTable, String soldDate, String item, String order,
            String quantity, String amount, String returnsTable, String returnItem, String returnOrder,
            String returnQuantity, String returnAmount)
    {
        QueryLowering branch = QueryLowering.scan(salesTable,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(item, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(order, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(quantity, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(amount, ColumnEncoding.FLAT, true))
                .join("item", item, "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_brand_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_class_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_category_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_manufact_id", ColumnEncoding.FLAT, true))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true))
                .leftJoin(returnsTable,
                        new String[] {order, item},
                        new String[] {returnOrder, returnItem},
                        new QueryLowering.Column(returnOrder, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(returnItem, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(returnQuantity, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(returnAmount, ColumnEncoding.FLAT, true));
        branch.where(
                        new Plan.StringMatch(branch.position("i_category"), List.of("Books"), false),
                        new Plan.Or(
                                new Plan.Predicate("=", branch.column("d_year"), new Plan.Lit(2001)),
                                new Plan.Predicate("=", branch.column("d_year"), new Plan.Lit(2002))));
        branch.select(
                branch.column("d_year"),
                branch.column("i_brand_id"),
                branch.column("i_class_id"),
                branch.column("i_category_id"),
                branch.column("i_manufact_id"),
                new Plan.Bin("-", branch.column(quantity), new Plan.Coalesce(branch.column(returnQuantity), new Plan.Lit(0))),
                new Plan.Bin("-", branch.column(amount), new Plan.Coalesce(branch.column(returnAmount), new Plan.Lit(0))));
        return branch;
    }

    public static UnionComposite query38()
    {
        // Q38: count the customers who bought through all three channels within the same month-sequence window. Each
        // channel emits one row per sale carrying (last_name, first_name, sale_date) plus a one-hot channel marker;
        // the union is grouped by that customer/date identity, kept only when every channel contributed (each marker
        // sums to > 0), and the surviving identities are counted. The union feeds the grouping stage, which feeds the
        // count -- a union materialized into a virtual table that downstream stages consume.
        List<QueryLowering> branches = List.of(
                customerChannelPresenceBranch("store_sales", "ss_sold_date_sk", "ss_customer_sk", 1, 0, 0),
                customerChannelPresenceBranch("catalog_sales", "cs_sold_date_sk", "cs_bill_customer_sk", 0, 1, 0),
                customerChannelPresenceBranch("web_sales", "ws_sold_date_sk", "ws_bill_customer_sk", 0, 0, 1));
        // Branch output last/first are dictionary strings (from customer, input 2, dict cols 2/1), unified across
        // branches at concatenation; the sale date is a numeric DATE (epoch days), so it groups as a plain column.
        List<DictRef> branchStrings = List.of(new DictRef(0, 2, 2), new DictRef(1, 2, 1));

        // The materialized union's numeric columns (the sale date and the one-hot markers) are non-null, so declare
        // them non-nullable: the eager grouping reads a null-flag array only for nullable columns, and a materialized
        // null-free column carries no such array.
        QueryLowering grouped = QueryLowering.scan("q38_channels",
                        new QueryLowering.Column("g_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_date", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_store", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_catalog", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_web", ColumnEncoding.FLAT, false))
                .groupBy("g_last", "g_first", "g_date")
                .aggregate("sum", "g_store")
                .aggregate("sum", "g_catalog")
                .aggregate("sum", "g_web");
        grouped.having(new Plan.And(List.of(
                        new Plan.Predicate(">", new Plan.Col(3), new Plan.Lit(0)),
                        new Plan.Predicate(">", new Plan.Col(4), new Plan.Lit(0)),
                        new Plan.Predicate(">", new Plan.Col(5), new Plan.Lit(0)))))
                .select(new Plan.Col(3));   // drop the string keys; only the surviving-group count matters

        QueryLowering main = QueryLowering.scan("q38_grouped",
                        new QueryLowering.Column("survivor", ColumnEncoding.FLAT, true))
                .count();

        return new UnionComposite(branches, "q38_channels", branchStrings,
                List.of(new Stage(grouped, "q38_grouped")), main, List.of());
    }

    public static UnionComposite query87()
    {
        // Q87: count the customers who bought in the store channel but in neither catalog nor web within the same
        // month-sequence window (a set difference). Same per-channel presence union as Q38, but the surviving
        // identities are those present in store (its marker sums > 0) and absent from catalog and web (those markers
        // sum to 0).
        List<QueryLowering> branches = List.of(
                customerChannelPresenceBranch("store_sales", "ss_sold_date_sk", "ss_customer_sk", 1, 0, 0),
                customerChannelPresenceBranch("catalog_sales", "cs_sold_date_sk", "cs_bill_customer_sk", 0, 1, 0),
                customerChannelPresenceBranch("web_sales", "ws_sold_date_sk", "ws_bill_customer_sk", 0, 0, 1));
        List<DictRef> branchStrings = List.of(new DictRef(0, 2, 2), new DictRef(1, 2, 1));

        QueryLowering grouped = QueryLowering.scan("q87_channels",
                        new QueryLowering.Column("g_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_date", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_store", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_catalog", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_web", ColumnEncoding.FLAT, false))
                .groupBy("g_last", "g_first", "g_date")
                .aggregate("sum", "g_store")
                .aggregate("sum", "g_catalog")
                .aggregate("sum", "g_web");
        grouped.having(new Plan.And(List.of(
                        new Plan.Predicate(">", new Plan.Col(3), new Plan.Lit(0)),
                        new Plan.Predicate("=", new Plan.Col(4), new Plan.Lit(0)),
                        new Plan.Predicate("=", new Plan.Col(5), new Plan.Lit(0)))))
                .select(new Plan.Col(3));   // drop the string keys; only the surviving-group count matters

        QueryLowering main = QueryLowering.scan("q87_grouped",
                        new QueryLowering.Column("survivor", ColumnEncoding.FLAT, false))
                .count();

        return new UnionComposite(branches, "q87_channels", branchStrings,
                List.of(new Stage(grouped, "q87_grouped")), main, List.of());
    }

    /**
     * The Q05 channel-rollup shape: per channel, a sales leaf and a returns leaf (one-hot value columns) concatenate
     * into {@code unionVirtual}, the union groups by the dimension id under {@code groupedVirtual} (labeled with the
     * channel name and the channel-prefixed id), and the labeled channel relations concatenate -- dictionaries
     * unified -- into the relation {@code main} orders. {@code webReturns} is the date-filtered web-returns build the
     * web returns leaf joins (the return attributes to its sale's site through the item/order join).
     */
    public record ChannelUnion(QueryLowering webReturns, List<Channel> channels, List<DictRef> leafStrings, QueryLowering main) {}

    /** One Q05 channel: its two leaves, the union and grouped virtual names, and the grouped stage's labeled strings. */
    public record Channel(QueryLowering sales, QueryLowering returns, String unionVirtual,
            QueryLowering grouped, String groupedVirtual, List<DictRef> groupedStrings) {}

    public static ChannelUnion query05()
    {
        // Q05: two weeks of sales, returns, and net profit per channel and dimension id. Each channel unions a sales
        // leaf and a returns leaf (date-windowed stars emitting one-hot value columns; the web returns attribute to
        // the site through a sales item/order join against the date-filtered returns), groups by the dimension id
        // summing the three measures, and labels rows with the channel name and the channel-prefixed id. The labeled
        // channels concatenate and order by (channel, id), top 100. The harness omits the SQL ROLLUP, and so does
        // this port.
        QueryLowering webReturns = QueryLowering.scan("web_returns",
                        new QueryLowering.Column("wr_returned_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_order_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_return_amt", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_net_loss", ColumnEncoding.FLAT, true))
                .join("date_dim", "wr_returned_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true));
        query05DateWindow(webReturns);
        webReturns.select(new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4));

        QueryLowering webReturnsLeaf = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_order_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_web_site_sk", ColumnEncoding.FLAT, true))
                .join("q05_web_returns",
                        new String[] {"ws_item_sk", "ws_order_number"},
                        new String[] {"r_item", "r_order"},
                        new QueryLowering.Column("r_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("r_order", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("r_amount", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("r_loss", ColumnEncoding.FLAT, true))
                .join("web_site", "ws_web_site_sk", "web_site_sk",
                        new QueryLowering.Column("web_site_sk"),
                        new QueryLowering.Column("web_site_id", ColumnEncoding.STRING, false));
        // Combined: probe(0-2), returns(3-6), web_site(7-8); the return contributes (0, amount, -loss).
        webReturnsLeaf.select(new Plan.Col(8), new Plan.Lit(0), new Plan.Col(5),
                new Plan.Bin("-", new Plan.Lit(0), new Plan.Col(6)));

        List<Channel> channels = List.of(
                query05Channel("store channel", "store",
                        query05SalesLeaf("store_sales", "ss_sold_date_sk", "ss_store_sk", "ss_ext_sales_price", "ss_net_profit", "store", "s_store_sk", "s_store_id"),
                        query05ReturnsLeaf("store_returns", "sr_returned_date_sk", "sr_store_sk", "sr_return_amt", "sr_net_loss", "store", "s_store_sk", "s_store_id")),
                query05Channel("catalog channel", "catalog_page",
                        query05SalesLeaf("catalog_sales", "cs_sold_date_sk", "cs_catalog_page_sk", "cs_ext_sales_price", "cs_net_profit", "catalog_page", "cp_catalog_page_sk", "cp_catalog_page_id"),
                        query05ReturnsLeaf("catalog_returns", "cr_returned_date_sk", "cr_catalog_page_sk", "cr_return_amount", "cr_net_loss", "catalog_page", "cp_catalog_page_sk", "cp_catalog_page_id")),
                query05Channel("web channel", "web_site",
                        query05SalesLeaf("web_sales", "ws_sold_date_sk", "ws_web_site_sk", "ws_ext_sales_price", "ws_net_profit", "web_site", "web_site_sk", "web_site_id"),
                        webReturnsLeaf));

        QueryLowering main = QueryLowering.scan("q05_channels",
                new QueryLowering.Column("m_channel", ColumnEncoding.STRING, false),
                new QueryLowering.Column("m_id", ColumnEncoding.STRING, false),
                new QueryLowering.Column("m_sales", ColumnEncoding.FLAT, true),
                new QueryLowering.Column("m_returns", ColumnEncoding.FLAT, true),
                new QueryLowering.Column("m_profit", ColumnEncoding.FLAT, true));
        main.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100));

        return new ChannelUnion(webReturns, channels, List.of(new DictRef(0, 2, 1)), main);
    }

    private static Channel query05Channel(String channelName, String idPrefix, QueryLowering sales, QueryLowering returns)
    {
        String shortName = channelName.substring(0, channelName.indexOf(' '));
        return new Channel(sales, returns, "q05_" + shortName + "_union",
                query05ChannelGrouped("q05_" + shortName + "_union", channelName),
                "q05_" + shortName,
                List.of(DictRef.literal(0, channelName), DictRef.prefixed(1, 0, 0, idPrefix)));
    }

    /** A channel's sales leaf: the date-windowed star over its dimension, contributing (id, amount, 0, profit). */
    private static QueryLowering query05SalesLeaf(String salesTable, String soldDate, String dimensionKey,
            String amount, String profit, String dimensionTable, String dimensionSk, String dimensionId)
    {
        QueryLowering leaf = query05LeafStar(salesTable, soldDate, dimensionKey, amount, profit, dimensionTable, dimensionSk, dimensionId);
        // Combined: fact(0-3), date(4-5), dimension(6-7).
        leaf.select(new Plan.Col(7), new Plan.Col(2), new Plan.Lit(0), new Plan.Col(3));
        return leaf;
    }

    /** A channel's returns leaf: the same star over the returns fact, contributing (id, 0, amount, -loss). */
    private static QueryLowering query05ReturnsLeaf(String returnsTable, String returnedDate, String dimensionKey,
            String amount, String loss, String dimensionTable, String dimensionSk, String dimensionId)
    {
        QueryLowering leaf = query05LeafStar(returnsTable, returnedDate, dimensionKey, amount, loss, dimensionTable, dimensionSk, dimensionId);
        leaf.select(new Plan.Col(7), new Plan.Lit(0), new Plan.Col(2),
                new Plan.Bin("-", new Plan.Lit(0), new Plan.Col(3)));
        return leaf;
    }

    private static QueryLowering query05LeafStar(String fact, String date, String dimensionKey, String amount,
            String lastMeasure, String dimensionTable, String dimensionSk, String dimensionId)
    {
        QueryLowering leaf = QueryLowering.scan(fact,
                        new QueryLowering.Column(date, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(dimensionKey, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(amount, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(lastMeasure, ColumnEncoding.FLAT, true))
                .join("date_dim", date, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true))
                .join(dimensionTable, dimensionKey, dimensionSk,
                        new QueryLowering.Column(dimensionSk),
                        new QueryLowering.Column(dimensionId, ColumnEncoding.STRING, false));
        query05DateWindow(leaf);
        return leaf;
    }

    /** The two-week window: 2000-08-23 through 2000-09-06 inclusive. */
    private static void query05DateWindow(QueryLowering query)
    {
        query.where(
                new Plan.Predicate(">=", query.column("d_date"), new Plan.Lit(LocalDate.of(2000, 8, 23).toEpochDay())),
                new Plan.Predicate("<=", query.column("d_date"), new Plan.Lit(LocalDate.of(2000, 9, 6).toEpochDay())));
    }

    /** Group a channel's union by the dimension id, sum the three measures, label with the channel name. */
    private static QueryLowering query05ChannelGrouped(String unionVirtual, String channelName)
    {
        QueryLowering grouped = QueryLowering.scan(unionVirtual,
                        new QueryLowering.Column("u_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("u_sales", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_returns", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_profit", ColumnEncoding.FLAT, true))
                .groupBy("u_id")
                .aggregate("sum", "u_sales")
                .aggregate("sum", "u_returns")
                .aggregate("sum", "u_profit");
        grouped.select(new Plan.LitStr(channelName), new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3));
        return grouped;
    }

    /**
     * A union of independently-materialized branch stages (each with its OWN string reconstruction, e.g. a
     * channel-specific id prefix) concatenated -- dictionaries unified -- under {@code unionVirtual}, which
     * {@code main} then consumes. {@code stages} materialize first, under their own virtual names, for branches that
     * join pre-aggregated relations. Unlike {@link UnionComposite}, the branches do not share one DictRef layout.
     */
    public record LabeledUnion(List<Stage> stages, List<Stage> branches, String unionVirtual, QueryLowering main, List<DictRef> stringColumns)
    {
        public LabeledUnion(List<Stage> branches, String unionVirtual, QueryLowering main, List<DictRef> stringColumns)
        {
            this(List.of(), branches, unionVirtual, main, stringColumns);
        }
    }

    public static LabeledUnion query77()
    {
        // Q77: a month of sales, returns, and profit per channel and location (numeric keys -- store, call center,
        // web page; no dimension join), with channel subtotals and a grand total. Each channel aggregates its sales
        // and its returns separately over the window and LEFT-joins the two on the location key, coalescing missing
        // returns to zero; the labeled channels concatenate and the main re-aggregates under the (channel, id)
        // ROLLUP, ordered by channel, id, and sales, top 100.
        List<Stage> stages = new ArrayList<>();
        List<Stage> branches = new ArrayList<>();
        record ChannelColumns(String name, String soldDate, String salesId, String amount, String profit,
                String returnedDate, String returnsId, String returnAmount, String returnLoss) {}

        for (ChannelColumns channel : List.of(
                new ChannelColumns("store channel", "ss_sold_date_sk", "ss_store_sk", "ss_ext_sales_price", "ss_net_profit",
                        "sr_returned_date_sk", "sr_store_sk", "sr_return_amt", "sr_net_loss"),
                new ChannelColumns("catalog channel", "cs_sold_date_sk", "cs_call_center_sk", "cs_ext_sales_price", "cs_net_profit",
                        "cr_returned_date_sk", "cr_call_center_sk", "cr_return_amount", "cr_net_loss"),
                new ChannelColumns("web channel", "ws_sold_date_sk", "ws_web_page_sk", "ws_ext_sales_price", "ws_net_profit",
                        "wr_returned_date_sk", "wr_web_page_sk", "wr_return_amt", "wr_net_loss"))) {
            String shortName = channel.name().substring(0, channel.name().indexOf(' '));
            String fact = switch (shortName) {
                case "store" -> "store_sales";
                case "catalog" -> "catalog_sales";
                default -> "web_sales";
            };
            String returnsFact = switch (shortName) {
                case "store" -> "store_returns";
                case "catalog" -> "catalog_returns";
                default -> "web_returns";
            };
            String salesVirtual = "q77_" + shortName + "_sales";
            String returnsVirtual = "q77_" + shortName + "_returns";
            stages.add(new Stage(query77WindowedAggregate(fact, channel.soldDate(), channel.salesId(), channel.amount(), channel.profit()), salesVirtual));
            stages.add(new Stage(query77WindowedAggregate(returnsFact, channel.returnedDate(), channel.returnsId(), channel.returnAmount(), channel.returnLoss()), returnsVirtual));

            // Branch combined columns: sales(0-2: id, sales, profit), returns(3-5: id, amount, loss) -- the LEFT
            // join leaves the returns columns NULL for locations with no returns.
            // The location keys are nullable fact columns, so a NULL-keyed group is real: it must ride through as
            // NULL (a distinct rollup group that sorts last), not as the value slot's zero.
            QueryLowering branch = QueryLowering.scan(salesVirtual,
                            new QueryLowering.Column("sl_id", ColumnEncoding.FLAT, true),
                            new QueryLowering.Column("sl_sales", ColumnEncoding.FLAT, true),
                            new QueryLowering.Column("sl_profit", ColumnEncoding.FLAT, true))
                    .leftJoin(returnsVirtual, "sl_id", "rt_id",
                            new QueryLowering.Column("rt_id", ColumnEncoding.FLAT, true),
                            new QueryLowering.Column("rt_amount", ColumnEncoding.FLAT, true),
                            new QueryLowering.Column("rt_loss", ColumnEncoding.FLAT, true));
            branch.select(new Plan.LitStr(channel.name()), new Plan.Col(0), new Plan.Col(1),
                    new Plan.Coalesce(new Plan.Col(4), new Plan.Lit(0)),
                    new Plan.Bin("-", new Plan.Col(2), new Plan.Coalesce(new Plan.Col(5), new Plan.Lit(0))));
            branches.add(new Stage(branch, "q77_" + shortName, List.of(DictRef.literal(0, channel.name()))));
        }

        // The returns column is the branches' coalesce-to-zero -- never null, so it materializes without a null mask.
        QueryLowering main = QueryLowering.scan("q77_channels",
                        new QueryLowering.Column("m_channel", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("m_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("m_sales", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("m_returns", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("m_profit", ColumnEncoding.FLAT, true))
                .groupBy("m_channel", "m_id")
                .groupingSets(List.of(new int[] {0, 1}, new int[] {0}, new int[] {}))
                .aggregate("sum", "m_sales")
                .aggregate("sum", "m_returns")
                .aggregate("sum", "m_profit");
        main.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)), 100));

        return new LabeledUnion(stages, branches, "q77_channels", main, List.of(new DictRef(0, 0, 0)));
    }

    /** A fact's two measures summed per location key over the 30-day window. */
    private static QueryLowering query77WindowedAggregate(String fact, String date, String id, String first, String second)
    {
        QueryLowering aggregate = QueryLowering.scan(fact,
                        new QueryLowering.Column(date, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(id, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(first, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(second, ColumnEncoding.FLAT, true))
                .join("date_dim", date, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true));
        aggregate.where(
                        new Plan.Predicate(">=", aggregate.column("d_date"), new Plan.Lit(LocalDate.of(2000, 8, 23).toEpochDay())),
                        new Plan.Predicate("<=", aggregate.column("d_date"), new Plan.Lit(LocalDate.of(2000, 9, 22).toEpochDay())))
                .groupBy(id)
                .aggregate("sum", first)
                .aggregate("sum", second);
        return aggregate;
    }

    public static LabeledUnion query49()
    {
        // Q49: each channel's worst returners for December 2001 -- items ranked by their return-to-sold quantity
        // ratio and by their refund-to-paid currency ratio, keeping items in either top ten. Each channel joins its
        // sales (positive quantity, payment, profit) to its large-amount returns and the month, sums the four
        // measures per item (returns coalesced to zero, matching the SQL), and derives the scaled ratios; two
        // chained rank windows (no partition) append the ranks, the top-ten filter keeps either rank, and the
        // channel label tags the rows. The labeled channels concatenate and order by channel, ranks, item, top 100.
        List<Stage> stages = new ArrayList<>();
        List<Stage> branches = new ArrayList<>();
        record ChannelColumns(String name, String sales, String soldDate, String item, String order, String quantity,
                String netPaid, String netProfit, String returns, String returnItem, String returnOrder,
                String returnQuantity, String returnAmount) {}

        for (ChannelColumns channel : List.of(
                new ChannelColumns("store", "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ticket_number",
                        "ss_quantity", "ss_net_paid", "ss_net_profit",
                        "store_returns", "sr_item_sk", "sr_ticket_number", "sr_return_quantity", "sr_return_amt"),
                new ChannelColumns("catalog", "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_order_number",
                        "cs_quantity", "cs_net_paid", "cs_net_profit",
                        "catalog_returns", "cr_item_sk", "cr_order_number", "cr_return_quantity", "cr_return_amount"),
                new ChannelColumns("web", "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_order_number",
                        "ws_quantity", "ws_net_paid", "ws_net_profit",
                        "web_returns", "wr_item_sk", "wr_order_number", "wr_return_quantity", "wr_return_amt"))) {
            String metricsVirtual = "q49_" + channel.name() + "_metrics";
            String rankedVirtual = "q49_" + channel.name() + "_ranked";

            QueryLowering metrics = QueryLowering.scan(channel.sales(),
                            new QueryLowering.Column(channel.soldDate(), ColumnEncoding.FLAT, true),
                            new QueryLowering.Column(channel.item(), ColumnEncoding.FLAT, true),
                            new QueryLowering.Column(channel.order(), ColumnEncoding.FLAT, true),
                            new QueryLowering.Column(channel.quantity(), ColumnEncoding.FLAT, true),
                            new QueryLowering.Column(channel.netPaid(), ColumnEncoding.FLAT, true),
                            new QueryLowering.Column(channel.netProfit(), ColumnEncoding.FLAT, true))
                    .join(channel.returns(),
                            new String[] {channel.order(), channel.item()},
                            new String[] {channel.returnOrder(), channel.returnItem()},
                            new QueryLowering.Column(channel.returnItem(), ColumnEncoding.FLAT, true),
                            new QueryLowering.Column(channel.returnOrder(), ColumnEncoding.FLAT, true),
                            new QueryLowering.Column(channel.returnQuantity(), ColumnEncoding.FLAT, true),
                            new QueryLowering.Column(channel.returnAmount(), ColumnEncoding.FLAT, true))
                    .join("date_dim", channel.soldDate(), "d_date_sk",
                            new QueryLowering.Column("d_date_sk"),
                            new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true),
                            new QueryLowering.Column("d_moy", ColumnEncoding.FLAT, true));
            metrics.where(
                            new Plan.Predicate(">", metrics.column(channel.quantity()), new Plan.Lit(0)),
                            new Plan.Predicate(">", metrics.column(channel.netPaid()), new Plan.Lit(0)),
                            new Plan.Predicate(">", metrics.column(channel.netProfit()), new Plan.Lit(100)),
                            new Plan.Predicate(">", metrics.column(channel.returnAmount()), new Plan.Lit(1_000_000)),
                            new Plan.Predicate("=", metrics.column("d_year"), new Plan.Lit(2001)),
                            new Plan.Predicate("=", metrics.column("d_moy"), new Plan.Lit(12)))
                    .groupBy(channel.item())
                    .aggregate("sum", new Plan.Coalesce(metrics.column(channel.returnQuantity()), new Plan.Lit(0)))
                    .aggregate("sum", metrics.column(channel.quantity()))
                    .aggregate("sum", new Plan.Coalesce(metrics.column(channel.returnAmount()), new Plan.Lit(0)))
                    .aggregate("sum", metrics.column(channel.netPaid()));
            // Metrics output: (item, return_ratio, currency_ratio) -- the scaled rounding divides over the sums.
            metrics.select(new Plan.Col(0),
                    new Plan.Call("divide_scale_round_i64", new Plan.Col(1), new Plan.Col(2), new Plan.Lit(1_000_000_000_000L)),
                    new Plan.Call("divide_scale_round_i64", new Plan.Col(3), new Plan.Col(4), new Plan.Lit(1_000_000_000_000L)));
            stages.add(new Stage(metrics, metricsVirtual));

            QueryLowering ranked = QueryLowering.scan(metricsVirtual,
                    new QueryLowering.Column("m_item", ColumnEncoding.FLAT, false),
                    new QueryLowering.Column("m_return_ratio", ColumnEncoding.FLAT, true),
                    new QueryLowering.Column("m_currency_ratio", ColumnEncoding.FLAT, true));
            ranked.window(new Plan.Window(new int[0], List.of(new Plan.SortKey(1, false)), Plan.RankFunction.RANK, -1));
            stages.add(new Stage(ranked, rankedVirtual));

            // Branch: the second rank window appends column 4; keep items in either top ten and label the channel.
            QueryLowering branch = QueryLowering.scan(rankedVirtual,
                    new QueryLowering.Column("r_item", ColumnEncoding.FLAT, false),
                    new QueryLowering.Column("r_return_ratio", ColumnEncoding.FLAT, true),
                    new QueryLowering.Column("r_currency_ratio", ColumnEncoding.FLAT, true),
                    new QueryLowering.Column("r_return_rank", ColumnEncoding.FLAT, false));
            branch.window(new Plan.Window(new int[0], List.of(new Plan.SortKey(2, false)), Plan.RankFunction.RANK, -1));
            branch.having(new Plan.Or(List.of(
                    new Plan.Predicate("<", new Plan.Col(3), new Plan.Lit(11)),
                    new Plan.Predicate("<", new Plan.Col(4), new Plan.Lit(11)))));
            branch.select(new Plan.LitStr(channel.name()), new Plan.Col(0), new Plan.Col(1), new Plan.Col(3), new Plan.Col(4));
            branches.add(new Stage(branch, "q49_" + channel.name(), List.of(DictRef.literal(0, channel.name()))));
        }

        QueryLowering main = QueryLowering.scan("q49_channels",
                new QueryLowering.Column("m_channel", ColumnEncoding.STRING, false),
                new QueryLowering.Column("m_item", ColumnEncoding.FLAT, false),
                new QueryLowering.Column("m_return_ratio", ColumnEncoding.FLAT, true),
                new QueryLowering.Column("m_return_rank", ColumnEncoding.FLAT, false),
                new QueryLowering.Column("m_currency_rank", ColumnEncoding.FLAT, false));
        main.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(3, false),
                        new Plan.SortKey(4, false), new Plan.SortKey(1, false)), 100));

        return new LabeledUnion(stages, branches, "q49_channels", main, List.of(new DictRef(0, 0, 0)));
    }

    public static LabeledUnion query80()
    {
        // Q80: a month of net sales, returns, and profit per channel and dimension id with channel subtotals and a
        // grand total. Each channel: sales LEFT-joined to returns on (item, ticket/order) -- unreturned rows
        // contribute zero return and full profit -- filtered to the window, items over $50, untelevised promotions,
        // grouped by the dimension id and labeled. The labeled channels concatenate and the main re-aggregates under
        // a (channel, id) ROLLUP, ordered by channel and id, top 100.
        List<Stage> branches = List.of(
                query80Branch("store_sales",
                        new String[] {"ss_sold_date_sk", "ss_item_sk", "ss_promo_sk", "ss_store_sk", "ss_ticket_number", "ss_ext_sales_price", "ss_net_profit"},
                        "store_returns", new String[] {"sr_item_sk", "sr_ticket_number", "sr_return_amt", "sr_net_loss"},
                        "store", "s_store_sk", "s_store_id", "store channel", "store"),
                query80Branch("catalog_sales",
                        new String[] {"cs_sold_date_sk", "cs_item_sk", "cs_promo_sk", "cs_catalog_page_sk", "cs_order_number", "cs_ext_sales_price", "cs_net_profit"},
                        "catalog_returns", new String[] {"cr_item_sk", "cr_order_number", "cr_return_amount", "cr_net_loss"},
                        "catalog_page", "cp_catalog_page_sk", "cp_catalog_page_id", "catalog channel", "catalog_page"),
                query80Branch("web_sales",
                        new String[] {"ws_sold_date_sk", "ws_item_sk", "ws_promo_sk", "ws_web_site_sk", "ws_order_number", "ws_ext_sales_price", "ws_net_profit"},
                        "web_returns", new String[] {"wr_item_sk", "wr_order_number", "wr_return_amt", "wr_net_loss"},
                        "web_site", "web_site_sk", "web_site_id", "web channel", "web_site"));

        QueryLowering main = QueryLowering.scan("q80_channels",
                        new QueryLowering.Column("m_channel", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("m_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("m_sales", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("m_returns", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("m_profit", ColumnEncoding.FLAT, true))
                .groupBy("m_channel", "m_id")
                .groupingSets(List.of(new int[] {0, 1}, new int[] {0}, new int[] {}))
                .aggregate("sum", "m_sales")
                .aggregate("sum", "m_returns")
                .aggregate("sum", "m_profit");
        // Grouping-sets output: channel(0), id(1), sums(2-4), grouping_id(5) -- the select drops the grouping id.
        main.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100));

        return new LabeledUnion(branches, "q80_channels", main,
                List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1)));
    }

    /** One Q80 channel: net sales/returns/profit per dimension id over the windowed star, labeled with the channel. */
    private static Stage query80Branch(String salesTable, String[] salesColumns, String returnsTable,
            String[] returnsColumns, String dimensionTable, String dimensionSk, String dimensionId,
            String channelName, String idPrefix)
    {
        QueryLowering branch = QueryLowering.scan(salesTable,
                        new QueryLowering.Column(salesColumns[0], ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(salesColumns[1], ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(salesColumns[2], ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(salesColumns[3], ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(salesColumns[4], ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(salesColumns[5], ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(salesColumns[6], ColumnEncoding.FLAT, true))
                // LEFT join: every column read from the build is NULL on unreturned rows, so all are nullable.
                .leftJoin(returnsTable,
                        new String[] {salesColumns[1], salesColumns[4]},
                        new String[] {returnsColumns[0], returnsColumns[1]},
                        new QueryLowering.Column(returnsColumns[0], ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(returnsColumns[1], ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(returnsColumns[2], ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(returnsColumns[3], ColumnEncoding.FLAT, true))
                .join("date_dim", salesColumns[0], "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true))
                .join(dimensionTable, salesColumns[3], dimensionSk,
                        new QueryLowering.Column(dimensionSk),
                        new QueryLowering.Column(dimensionId, ColumnEncoding.STRING, false))
                .join("item", salesColumns[1], "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_current_price", ColumnEncoding.FLAT, true))
                .join("promotion", salesColumns[2], "p_promo_sk",
                        new QueryLowering.Column("p_promo_sk"),
                        new QueryLowering.Column("p_channel_tv", ColumnEncoding.STRING, true));
        branch.where(
                        new Plan.Predicate(">=", branch.column("d_date"), new Plan.Lit(LocalDate.of(2000, 8, 23).toEpochDay())),
                        new Plan.Predicate("<=", branch.column("d_date"), new Plan.Lit(LocalDate.of(2000, 9, 22).toEpochDay())),
                        new Plan.Predicate(">", branch.column("i_current_price"), new Plan.Lit(5_000)),
                        new Plan.StringMatch(branch.position("p_channel_tv"), List.of("N"), false))
                .groupBy(dimensionId)
                .aggregate("sum", branch.column(salesColumns[5]))
                .aggregate("sum", new Plan.Coalesce(branch.column(returnsColumns[2]), new Plan.Lit(0)))
                .aggregate("sum", new Plan.Bin("-", branch.column(salesColumns[6]),
                        new Plan.Coalesce(branch.column(returnsColumns[3]), new Plan.Lit(0))));
        branch.select(new Plan.LitStr(channelName), new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3));
        return new Stage(branch, "q80_" + idPrefix,
                List.of(DictRef.literal(0, channelName), DictRef.prefixed(1, 3, 1, idPrefix)));
    }

    public static UnionComposite query97()
    {
        // Q97: count the (customer, item) pairs bought only from store, only from catalog, or from both, in a
        // one-year month-seq window. The harness emulates the FULL OUTER JOIN as a union of per-channel presence
        // rows: each channel joins the date window, drops null customers, dedups (customer, item) and tags the pair
        // with a one-hot channel marker plus a null-key discriminator (a null key never joins, so such rows must stay
        // distinct per channel); the union re-groups per (customer, item, discriminator) summing the markers, each
        // group classifies as store-only / catalog-only / both, and the three classes are counted globally.
        List<QueryLowering> branches = List.of(
                query97PresenceBranch("store_sales", "ss_customer_sk", "ss_item_sk", "ss_sold_date_sk", 1),
                query97PresenceBranch("catalog_sales", "cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk", 2));

        QueryLowering presence = QueryLowering.scan("q97_channels",
                        new QueryLowering.Column("u_customer", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("u_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("u_discriminator", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("u_store", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("u_catalog", ColumnEncoding.FLAT, false))
                .groupBy("u_customer", "u_item", "u_discriminator")
                .aggregate("sum", "u_store")
                .aggregate("sum", "u_catalog");
        // presence result: customer(0), item(1), discriminator(2), store_marker_sum(3), catalog_marker_sum(4).

        QueryLowering main = QueryLowering.scan("q97_presence",
                        new QueryLowering.Column("p_customer", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("p_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("p_discriminator", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("p_store", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("p_catalog", ColumnEncoding.FLAT, true))
                .aggregate("sum", query97Indicator(true, false))
                .aggregate("sum", query97Indicator(false, true))
                .aggregate("sum", query97Indicator(true, true));

        return new UnionComposite(branches, "q97_channels", List.of(),
                List.of(new Stage(presence, "q97_presence")), main, List.of());
    }

    /** One row per distinct (customer, item) the channel sold in the window, with the discriminator and one-hot markers. */
    private static QueryLowering query97PresenceBranch(String fact, String customer, String item, String soldDate, long channelId)
    {
        QueryLowering channel = QueryLowering.scan(fact,
                        new QueryLowering.Column(customer, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(item, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_month_seq", ColumnEncoding.FLAT, true));
        channel.where(
                        new Plan.IsNull(channel.position(customer), true),
                        new Plan.Predicate(">", channel.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", channel.column("d_month_seq"), new Plan.Lit(1212)))
                .groupBy(customer, item)
                .count();
        // post-distinct: customer(0), item(1), count(2). The discriminator keeps a null-keyed pair a distinct group
        // per channel (channelId when either key is null, else 0), mirroring the harness's FULL OUTER emulation.
        channel.select(new Plan.Col(0), new Plan.Col(1),
                new Plan.Case(
                        List.of(new Plan.Case.Branch(
                                new Plan.Or(List.of(new Plan.IsNull(0), new Plan.IsNull(1))),
                                new Plan.Lit(channelId))),
                        new Plan.Lit(0)),
                new Plan.Lit(channelId == 1 ? 1 : 0),
                new Plan.Lit(channelId == 1 ? 0 : 1));
        return channel;
    }

    /** 1 when the pair's presence matches (store bought?, catalog bought?), else 0 -- summed into the class count. */
    private static Plan.Expr query97Indicator(boolean store, boolean catalog)
    {
        Plan.Condition storeTest = new Plan.Predicate(store ? ">" : "=", new Plan.Col(3), new Plan.Lit(0));
        Plan.Condition catalogTest = new Plan.Predicate(catalog ? ">" : "=", new Plan.Col(4), new Plan.Lit(0));
        return new Plan.Case(
                List.of(new Plan.Case.Branch(new Plan.And(List.of(storeTest, catalogTest)), new Plan.Lit(1))),
                new Plan.Lit(0));
    }

    private static QueryLowering customerChannelPresenceBranch(String fact, String soldDate, String customer, long store, long catalog, long web)
    {
        QueryLowering channel = QueryLowering.scan(fact,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(customer, ColumnEncoding.FLAT, true))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_month_seq", ColumnEncoding.FLAT, true))
                .join("customer", customer, "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true));
        channel.where(
                        new Plan.Predicate(">", channel.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", channel.column("d_month_seq"), new Plan.Lit(1212)))
                .select(channel.column("c_last_name"), channel.column("c_first_name"), channel.column("d_date"),
                        new Plan.Lit(store), new Plan.Lit(catalog), new Plan.Lit(web));
        return channel;
    }

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

    public static Ported query85()
    {
        // Q85: average return metrics by reason for the web channel. web_sales joined to web_page, to its web_returns
        // (item + order), to date_dim (year 2000), twice to customer_demographics (the refunded and the returning
        // customer's marital/education), to customer_address, and to reason; kept only where the two demographics
        // agree and a per-marital price band holds, and a per-state net-profit band holds; GROUP BY reason; average
        // quantity, refunded cash, and fee; top 100 ordered by the reason and the three averages. The reason text is
        // emitted via the substring(r_reason_desc, 1, 20) projection carried on the DictRef.
        QueryLowering query = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_web_page_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_order_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_quantity", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_sales_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_net_profit", ColumnEncoding.FLAT, true))
                .join("web_page", "ws_web_page_sk", "wp_web_page_sk",
                        new QueryLowering.Column("wp_web_page_sk"))
                .join("web_returns", new String[] {"ws_item_sk", "ws_order_number"}, new String[] {"wr_item_sk", "wr_order_number"},
                        new QueryLowering.Column("wr_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_order_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_refunded_cdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_returning_cdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_refunded_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_reason_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_refunded_cash", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_fee", ColumnEncoding.FLAT, true))
                .join("date_dim", "ws_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true))
                .join("customer_demographics", "wr_refunded_cdemo_sk", "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("cd_education_status", ColumnEncoding.STRING, false))
                .join("customer_demographics", "wr_returning_cdemo_sk", "cd_demo_sk_returning",
                        new QueryLowering.Column("cd_demo_sk_returning", "cd_demo_sk", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("cd_marital_status_returning", "cd_marital_status", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("cd_education_status_returning", "cd_education_status", ColumnEncoding.STRING, false))
                .join("customer_address", "wr_refunded_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, true))
                .join("reason", "wr_reason_sk", "r_reason_sk",
                        new QueryLowering.Column("r_reason_sk"),
                        new QueryLowering.Column("r_reason_desc", ColumnEncoding.STRING, false));
        query.where(
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2000)),
                        new Plan.Or(List.of(
                                query85DemographicsBranch(query, "M", "Advanced Degree", 9_999, 15_001),
                                query85DemographicsBranch(query, "S", "College", 4_999, 10_001),
                                query85DemographicsBranch(query, "W", "2 yr Degree", 14_999, 20_001))),
                        new Plan.Or(List.of(
                                query85AddressBranch(query, List.of("IN", "OH", "NJ"), 9_999, 20_001),
                                query85AddressBranch(query, List.of("WI", "CT", "KY"), 14_999, 30_001),
                                query85AddressBranch(query, List.of("LA", "IA", "AR"), 4_999, 25_001))))
                .groupBy("r_reason_desc")
                .aggregate("avg", "ws_quantity")
                .aggregate("avg", "wr_refunded_cash")
                .aggregate("avg", "wr_fee")
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false),
                        new Plan.SortKey(2, false), new Plan.SortKey(3, false)), 100));
        return new Ported(query, List.of(new DictRef(0, 7, 1, 1, 20)));
    }

    private static Plan.And query85DemographicsBranch(QueryLowering query, String marital, String education, long priceLowExclusive, long priceHighExclusive)
    {
        return new Plan.And(List.of(
                new Plan.StringMatch(query.position("cd_marital_status"), List.of(marital), false),
                new Plan.StringColumnCompare(query.position("cd_marital_status"), query.position("cd_marital_status_returning"), false),
                new Plan.StringMatch(query.position("cd_education_status"), List.of(education), false),
                new Plan.StringColumnCompare(query.position("cd_education_status"), query.position("cd_education_status_returning"), false),
                new Plan.Predicate(">", query.column("ws_sales_price"), new Plan.Lit(priceLowExclusive)),
                new Plan.Predicate("<", query.column("ws_sales_price"), new Plan.Lit(priceHighExclusive))));
    }

    private static Plan.And query85AddressBranch(QueryLowering query, List<String> states, long profitLowExclusive, long profitHighExclusive)
    {
        return new Plan.And(List.of(
                new Plan.StringMatch(query.position("ca_country"), List.of("United States"), false),
                new Plan.StringMatch(query.position("ca_state"), states, false),
                new Plan.Predicate(">", query.column("ws_net_profit"), new Plan.Lit(profitLowExclusive)),
                new Plan.Predicate("<", query.column("ws_net_profit"), new Plan.Lit(profitHighExclusive))));
    }

    public static Composite query79()
    {
        // Q79: per (customer, store-visit) store-sale coupon and net-profit totals on a chosen weekday in 1999-2001,
        // for stores of a certain size and households with six dependents or more than two vehicles; then the customer's
        // name, the store city, the ticket, and the totals, top 100 ordered by name, the city prefix, and the profit.
        // The coupon/profit are summed over coalesce(x, 0): a group whose values are all null must total 0, not null
        // (a bare sum returns null for an all-null group), so the coalesce is required, not redundant.
        QueryLowering grouped = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_hdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ticket_number"),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_coupon_amt", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_net_profit", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_dow", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_city", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_number_employees", ColumnEncoding.FLAT, true))
                .join("household_demographics", "ss_hdemo_sk", "hd_demo_sk",
                        new QueryLowering.Column("hd_demo_sk"),
                        new QueryLowering.Column("hd_dep_count", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("hd_vehicle_count", ColumnEncoding.FLAT, true));
        grouped.where(
                        new Plan.Predicate("=", grouped.column("d_dow"), new Plan.Lit(1)),
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(1999)),
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(2000)),
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(2001)))),
                        new Plan.Predicate(">", grouped.column("s_number_employees"), new Plan.Lit(199)),
                        new Plan.Predicate("<", grouped.column("s_number_employees"), new Plan.Lit(296)),
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", grouped.column("hd_dep_count"), new Plan.Lit(6)),
                                new Plan.Predicate(">", grouped.column("hd_vehicle_count"), new Plan.Lit(2)))))
                .groupBy("ss_ticket_number", "ss_customer_sk", "ss_addr_sk", "s_city")
                .aggregate("sum", new Plan.Coalesce(grouped.column("ss_coupon_amt"), new Plan.Lit(0)))
                .aggregate("sum", new Plan.Coalesce(grouped.column("ss_net_profit"), new Plan.Lit(0)));
        // grouped result: ticket(0), customer(1), addr(2), city(3), sum_coupon(4), sum_profit(5).

        QueryLowering main = QueryLowering.scan("q79_grouped",
                        new QueryLowering.Column("g_ticket"),
                        new QueryLowering.Column("g_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_addr", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_city", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_coupon", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_profit", ColumnEncoding.FLAT, true))
                .join("customer", "g_customer", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true));
        main.select(main.column("c_last_name"), main.column("c_first_name"), main.column("g_city"),
                        main.column("g_ticket"), main.column("g_coupon"), main.column("g_profit"))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false),
                        new Plan.SortKey(2, false), new Plan.SortKey(5, false)), 100));

        return new Composite(
                List.of(new Stage(grouped, "q79_grouped", List.of(new DictRef(3, 2, 1)))),
                main,
                List.of(new DictRef(0, 1, 1), new DictRef(1, 1, 2), new DictRef(2, 0, 3, 1, 30)));
    }

    public static Union query66()
    {
        // Q66: per-warehouse monthly shipping revenue and net for the web and catalog channels, on a time-of-day band
        // via carriers DHL/BARIAN in 2001. Each channel rolls sales (price*qty) and net (net_paid*qty) into twelve
        // per-month buckets grouped by warehouse identity; the two channels are unioned and re-summed, then twelve
        // sales-per-square-foot ratios are emitted alongside. The constant ship-mode label is a string literal.
        List<QueryLowering> branches = List.of(
                query66ChannelRollup("web_sales", "ws_sold_date_sk", "ws_sold_time_sk", "ws_ship_mode_sk", "ws_warehouse_sk",
                        "ws_quantity", "ws_ext_sales_price", "ws_net_paid"),
                query66ChannelRollup("catalog_sales", "cs_sold_date_sk", "cs_sold_time_sk", "cs_ship_mode_sk", "cs_warehouse_sk",
                        "cs_quantity", "cs_sales_price", "cs_net_paid_inc_tax"));
        // branch result: name(0), sqft(1), city(2), county(3), state(4), country(5), year(6), 12 sales sums(7..18),
        // 12 net sums(19..30). The five warehouse strings come from the warehouse build (input 1).
        List<DictRef> branchStrings = List.of(
                new DictRef(0, 1, 1), new DictRef(2, 1, 3), new DictRef(3, 1, 4), new DictRef(4, 1, 5), new DictRef(5, 1, 6));

        QueryLowering main = QueryLowering.scan("__q66_union__",
                        new QueryLowering.Column("g_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_sqft", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_city", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_county", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_state", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_country", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_year", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("s1"), new QueryLowering.Column("s2"), new QueryLowering.Column("s3"),
                        new QueryLowering.Column("s4"), new QueryLowering.Column("s5"), new QueryLowering.Column("s6"),
                        new QueryLowering.Column("s7"), new QueryLowering.Column("s8"), new QueryLowering.Column("s9"),
                        new QueryLowering.Column("s10"), new QueryLowering.Column("s11"), new QueryLowering.Column("s12"),
                        new QueryLowering.Column("n1"), new QueryLowering.Column("n2"), new QueryLowering.Column("n3"),
                        new QueryLowering.Column("n4"), new QueryLowering.Column("n5"), new QueryLowering.Column("n6"),
                        new QueryLowering.Column("n7"), new QueryLowering.Column("n8"), new QueryLowering.Column("n9"),
                        new QueryLowering.Column("n10"), new QueryLowering.Column("n11"), new QueryLowering.Column("n12"))
                .groupBy("g_name", "g_sqft", "g_city", "g_county", "g_state", "g_country", "g_year");
        for (int s = 1; s <= 12; s++) {
            main.aggregate("sum", "s" + s);
        }
        for (int n = 1; n <= 12; n++) {
            main.aggregate("sum", "n" + n);
        }
        // main result: keys 0..6 (name,sqft,city,county,state,country,year), 12 sales sums 7..18, 12 net sums 19..30.
        List<Plan.Expr> outputs = new java.util.ArrayList<>();
        outputs.add(new Plan.Col(0));   // name
        outputs.add(new Plan.Col(1));   // sqft
        outputs.add(new Plan.Col(2));   // city
        outputs.add(new Plan.Col(3));   // county
        outputs.add(new Plan.Col(4));   // state
        outputs.add(new Plan.Col(5));   // country
        outputs.add(new Plan.LitStr("DHL,BARIAN"));   // constant ship-mode label (reconstructed via the literal DictRef)
        outputs.add(new Plan.Col(6));   // year
        for (int m = 0; m < 12; m++) {
            outputs.add(new Plan.Col(7 + m));   // sales sum for month m+1
        }
        for (int m = 0; m < 12; m++) {
            outputs.add(new Plan.Call("divide_round_i64", new Plan.Col(7 + m), new Plan.Col(1)));   // sales / sqft
        }
        for (int m = 0; m < 12; m++) {
            outputs.add(new Plan.Col(19 + m));  // net sum for month m+1
        }
        main.select(outputs.toArray(new Plan.Expr[0]))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 100));

        List<DictRef> mainStrings = List.of(
                new DictRef(0, 0, 0), new DictRef(2, 0, 2), new DictRef(3, 0, 3), new DictRef(4, 0, 4), new DictRef(5, 0, 5),
                DictRef.literal(6, "DHL,BARIAN"));
        return new Union(branches, main, "__q66_union__", branchStrings, mainStrings);
    }

    private static QueryLowering query66ChannelRollup(String fact, String soldDate, String soldTime, String shipMode, String warehouse,
            String quantity, String salesPrice, String netPaid)
    {
        QueryLowering channel = QueryLowering.scan(fact,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(soldTime, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(shipMode, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(warehouse, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(quantity, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(salesPrice, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(netPaid, ColumnEncoding.FLAT, true))
                .join("warehouse", warehouse, "w_warehouse_sk",
                        new QueryLowering.Column("w_warehouse_sk"),
                        new QueryLowering.Column("w_warehouse_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("w_warehouse_sq_ft", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("w_city", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("w_county", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("w_state", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("w_country", ColumnEncoding.STRING, false))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_moy", ColumnEncoding.FLAT, false))
                .join("time_dim", soldTime, "t_time_sk",
                        new QueryLowering.Column("t_time_sk"),
                        new QueryLowering.Column("t_time", ColumnEncoding.FLAT, false))
                .join("ship_mode", shipMode, "sm_ship_mode_sk",
                        new QueryLowering.Column("sm_ship_mode_sk"),
                        new QueryLowering.Column("sm_carrier", ColumnEncoding.STRING, false));
        channel.where(
                        new Plan.Predicate("=", channel.column("d_year"), new Plan.Lit(2001)),
                        new Plan.Predicate(">", channel.column("t_time"), new Plan.Lit(30837)),
                        new Plan.Predicate("<", channel.column("t_time"), new Plan.Lit(59639)),
                        new Plan.StringMatch(channel.position("sm_carrier"), List.of("DHL", "BARIAN"), false))
                .groupBy("w_warehouse_name", "w_warehouse_sq_ft", "w_city", "w_county", "w_state", "w_country", "d_year");
        for (int m = 1; m <= 12; m++) {
            channel.aggregate("sum", new Plan.Case(
                    List.of(new Plan.Case.Branch(new Plan.Predicate("=", channel.column("d_moy"), new Plan.Lit(m)),
                            new Plan.Bin("*", channel.column(salesPrice), channel.column(quantity)))),
                    new Plan.Lit(0)));
        }
        for (int m = 1; m <= 12; m++) {
            channel.aggregate("sum", new Plan.Case(
                    List.of(new Plan.Case.Branch(new Plan.Predicate("=", channel.column("d_moy"), new Plan.Lit(m)),
                            new Plan.Bin("*", channel.column(netPaid), channel.column(quantity)))),
                    new Plan.Lit(0)));
        }
        return channel;
    }

    public static Composite query06()
    {
        // Q06: per-state count of customers who bought (in a target month) an item priced above 1.2x its category
        // average. Mirrors the harness operator tree: two subquery builds -- a scalar month-sequence and a per-category
        // sum/count of item price -- feed a store_sales star, kept where price > 1.2*avg (as 6*sum < 5*count*price, the
        // harness's exact integer form), grouped by state with HAVING count >= 10, top 100.
        QueryLowering month = QueryLowering.scan("date_dim",
                        new QueryLowering.Column("d_month_seq", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_moy", ColumnEncoding.FLAT, false));
        month.where(new Plan.Predicate("=", month.column("d_year"), new Plan.Lit(2001)),
                        new Plan.Predicate("=", month.column("d_moy"), new Plan.Lit(1)))
                .groupBy("d_month_seq")
                .count();   // SELECT DISTINCT d_month_seq -> one row; the trailing count is unused by the join

        // The category aggregate groups item (the probe) by the string i_category, so its key is materialized via the
        // eager-probe stage path (DictRef into input 0). count over i_current_price counts only non-null prices.
        QueryLowering category = QueryLowering.scan("item",
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_current_price", ColumnEncoding.FLAT, true))
                .groupBy("i_category")
                .aggregate("sum", "i_current_price")
                .aggregate("count", "i_current_price");
        // q06_category: category(0), sum(1), count(2).

        QueryLowering main = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true))
                .join("customer", "ss_customer_sk", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_month_seq", ColumnEncoding.FLAT, true))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_current_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true))
                .join("q06_month", "d_month_seq", "qm_month_seq",
                        new QueryLowering.Column("qm_month_seq", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("qm_count", ColumnEncoding.FLAT, false))
                // The category aggregate retains a NULL-category group; the build key must be declared nullable so
                // the null-key build skip drops it (a null never joins) instead of it entering the hash at the null
                // sentinel id, where it would collide with whichever real entry holds id zero.
                .join("q06_category", "i_category", "qc_category",
                        new QueryLowering.Column("qc_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("qc_sum", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("qc_count", ColumnEncoding.FLAT, false));
        main.where(new Plan.Predicate("<",
                        new Plan.Bin("*", main.column("qc_sum"), new Plan.Lit(6)),
                        new Plan.Bin("*", new Plan.Bin("*", main.column("qc_count"), main.column("i_current_price")), new Plan.Lit(5))))
                .groupBy("ca_state")
                .count();
        main.having(new Plan.Predicate(">", new Plan.Col(1), new Plan.Lit(9)))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, false), new Plan.SortKey(0, false)), 100));

        return new Composite(
                List.of(new Stage(month, "q06_month"),
                        new Stage(category, "q06_category", List.of(new DictRef(0, 0, 0)))),
                main,
                List.of(new DictRef(0, 2, 1)));
    }

    public static Composite query88()
    {
        // Q88: a single row of eight store-sale counts, one per half-hour time bucket from 8:30 to 12:30, for one
        // household profile at the 'ese' store. Each count is a global-aggregate subquery (store_sales joined to
        // time_dim / household_demographics / store); the eight are cross-joined (1x1 each) into one row. Q90's
        // scalar-broadcast shape, eight-wide.
        QueryLowering main = QueryLowering.scan("q88_b0",
                        new QueryLowering.Column("c0", ColumnEncoding.FLAT, false))
                .crossJoin("q88_b1", new QueryLowering.Column("c1", ColumnEncoding.FLAT, false))
                .crossJoin("q88_b2", new QueryLowering.Column("c2", ColumnEncoding.FLAT, false))
                .crossJoin("q88_b3", new QueryLowering.Column("c3", ColumnEncoding.FLAT, false))
                .crossJoin("q88_b4", new QueryLowering.Column("c4", ColumnEncoding.FLAT, false))
                .crossJoin("q88_b5", new QueryLowering.Column("c5", ColumnEncoding.FLAT, false))
                .crossJoin("q88_b6", new QueryLowering.Column("c6", ColumnEncoding.FLAT, false))
                .crossJoin("q88_b7", new QueryLowering.Column("c7", ColumnEncoding.FLAT, false));
        main.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3),
                new Plan.Col(4), new Plan.Col(5), new Plan.Col(6), new Plan.Col(7));

        return new Composite(
                List.of(new Stage(query88Count(8, false), "q88_b0"),
                        new Stage(query88Count(9, true), "q88_b1"),
                        new Stage(query88Count(9, false), "q88_b2"),
                        new Stage(query88Count(10, true), "q88_b3"),
                        new Stage(query88Count(10, false), "q88_b4"),
                        new Stage(query88Count(11, true), "q88_b5"),
                        new Stage(query88Count(11, false), "q88_b6"),
                        new Stage(query88Count(12, true), "q88_b7")),
                main,
                List.of());
    }

    private static QueryLowering query88Count(int hour, boolean firstHalf)
    {
        QueryLowering count = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_time_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_hdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true))
                .join("time_dim", "ss_sold_time_sk", "t_time_sk",
                        new QueryLowering.Column("t_time_sk"),
                        new QueryLowering.Column("t_hour", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("t_minute", ColumnEncoding.FLAT, true))
                .join("household_demographics", "ss_hdemo_sk", "hd_demo_sk",
                        new QueryLowering.Column("hd_demo_sk"),
                        new QueryLowering.Column("hd_dep_count", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("hd_vehicle_count", ColumnEncoding.FLAT, true))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, true));
        // The half-hour bucket: a fixed hour, then minute < 30 (first half) or minute >= 30 (second half).
        Plan.Predicate minute = firstHalf
                ? new Plan.Predicate("<", count.column("t_minute"), new Plan.Lit(30))
                : new Plan.Predicate(">", count.column("t_minute"), new Plan.Lit(29));
        count.where(
                        new Plan.Predicate("=", count.column("t_hour"), new Plan.Lit(hour)),
                        minute,
                        new Plan.Or(List.of(
                                new Plan.Predicate("=", count.column("hd_dep_count"), new Plan.Lit(4)),
                                new Plan.Predicate("=", count.column("hd_dep_count"), new Plan.Lit(2)),
                                new Plan.Predicate("=", count.column("hd_dep_count"), new Plan.Lit(0)))),
                        new Plan.Predicate("<", count.column("hd_vehicle_count"),
                                new Plan.Bin("+", count.column("hd_dep_count"), new Plan.Lit(3))),
                        new Plan.StringMatch(count.position("s_store_name"), List.of("ese"), false))
                .count();
        return count;
    }

    public static Composite query83()
    {
        // Q83: items returned in all three channels during three target weeks, with each channel's share of the
        // three-channel total. The date set derives in two steps -- the distinct week sequences of three literal
        // dates, then the distinct date keys of those weeks -- and each channel's returns join item and that date set,
        // summing returned quantity per item id. The per-item channel relations inner-join on the item id (a string
        // key against materialized virtual relations) and the shares are integer-rounded percentages of the
        // three-channel average, top 100 by item and store quantity.
        QueryLowering weeks = QueryLowering.scan("date_dim",
                new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true),
                new QueryLowering.Column("d_week_seq", ColumnEncoding.FLAT, true));
        weeks.where(new Plan.Or(List.of(
                        new Plan.Predicate("=", weeks.column("d_date"), new Plan.Lit(LocalDate.of(2000, 6, 30).toEpochDay())),
                        new Plan.Predicate("=", weeks.column("d_date"), new Plan.Lit(LocalDate.of(2000, 9, 27).toEpochDay())),
                        new Plan.Predicate("=", weeks.column("d_date"), new Plan.Lit(LocalDate.of(2000, 11, 17).toEpochDay())))))
                .groupBy("d_week_seq")
                .count();
        weeks.select(new Plan.Col(0));

        QueryLowering dates = QueryLowering.scan("date_dim",
                        new QueryLowering.Column("d_date_sk", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_week_seq", ColumnEncoding.FLAT, true))
                .join("q83_weeks", "d_week_seq", "qw_week_seq",
                        new QueryLowering.Column("qw_week_seq", ColumnEncoding.FLAT, false));
        dates.groupBy("d_date_sk").count();
        dates.select(new Plan.Col(0));

        // Combined main columns: item(0), sr_qty(1), cr_item(2), cr_qty(3), wr_item(4), wr_qty(5).
        QueryLowering main = QueryLowering.scan("q83_store",
                        new QueryLowering.Column("sr_item", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("sr_qty", ColumnEncoding.FLAT, true))
                .join("q83_catalog", "sr_item", "cr_item",
                        new QueryLowering.Column("cr_item", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("cr_qty", ColumnEncoding.FLAT, true))
                .join("q83_web", "sr_item", "wr_item",
                        new QueryLowering.Column("wr_item", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("wr_qty", ColumnEncoding.FLAT, true));
        channelShareSelect(main);

        return new Composite(
                List.of(new Stage(weeks, "q83_weeks"),
                        new Stage(dates, "q83_dates"),
                        new Stage(query83ChannelReturns("store_returns", "sr_item_sk", "sr_returned_date_sk", "sr_return_quantity"),
                                "q83_store", List.of(new DictRef(0, 1, 1))),
                        new Stage(query83ChannelReturns("catalog_returns", "cr_item_sk", "cr_returned_date_sk", "cr_return_quantity"),
                                "q83_catalog", List.of(new DictRef(0, 1, 1))),
                        new Stage(query83ChannelReturns("web_returns", "wr_item_sk", "wr_returned_date_sk", "wr_return_quantity"),
                                "q83_web", List.of(new DictRef(0, 1, 1)))),
                main,
                List.of(new DictRef(0, 0, 0)));
    }

    /** A channel's returned quantity per item id within the allowed dates: returns joined to item and q83_dates, grouped. */
    private static QueryLowering query83ChannelReturns(String returnsTable, String item, String returnedDate, String quantity)
    {
        QueryLowering channel = QueryLowering.scan(returnsTable,
                        new QueryLowering.Column(item, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(returnedDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(quantity, ColumnEncoding.FLAT, true))
                .join("item", item, "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false))
                .join("q83_dates", returnedDate, "qd_date_sk",
                        new QueryLowering.Column("qd_date_sk", ColumnEncoding.FLAT, false));
        channel.groupBy("i_item_id")
                .aggregate("sum", quantity);
        return channel;
    }

    /**
     * The shared Q58/Q83 output over a three-channel item join (combined columns: item 0, channel values 1/3/5):
     * each channel's value with its integer-rounded share of the three-channel average, plus that average, top 100
     * by item then the store value. The three-channel total reads a NULL channel sum as 0 (the harness's add does
     * not propagate nulls, and an all-null sum's value slot is zero), while each share's numerator DOES carry its
     * own channel's null -- so a channel with a NULL sum reports a NULL share but still contributes 0 to the others'
     * denominators.
     */
    private static void channelShareSelect(QueryLowering main)
    {
        Plan.Expr total = new Plan.Bin("+",
                new Plan.Bin("+",
                        new Plan.Coalesce(new Plan.Col(1), new Plan.Lit(0)),
                        new Plan.Coalesce(new Plan.Col(3), new Plan.Lit(0))),
                new Plan.Coalesce(new Plan.Col(5), new Plan.Lit(0)));
        Plan.Expr denominator = new Plan.Bin("*", total, new Plan.Lit(3));
        main.select(new Plan.Col(0),
                        new Plan.Col(1), channelShare(new Plan.Col(1), denominator),
                        new Plan.Col(3), channelShare(new Plan.Col(3), denominator),
                        new Plan.Col(5), channelShare(new Plan.Col(5), denominator),
                        new Plan.Call("divide_round_i64", new Plan.Bin("*", total, new Plan.Lit(10_000)), new Plan.Lit(3)))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100));
    }

    /** The channel's integer-rounded share of the three-channel average: value * 10000 / (total * 3). */
    private static Plan.Expr channelShare(Plan.Expr value, Plan.Expr denominator)
    {
        return new Plan.Call("divide_round_i64", new Plan.Bin("*", value, new Plan.Lit(10_000)), denominator);
    }

    public static Composite query58()
    {
        // Q58: items whose revenue in one target week is within ten percent of the three-channel average. The allowed
        // dates derive from a scalar subquery -- the week sequence of one literal date, broadcast by a cross join and
        // matched by column equality -- and each channel sums extended sales price per item id over those dates. The
        // three per-item relations inner-join on the item id and survive only when every ordered channel pair is
        // within ten percent (cross-multiplied integer bounds); Q83's share output reports each channel against the
        // three-channel average.
        QueryLowering week = QueryLowering.scan("date_dim",
                new QueryLowering.Column("d_week_seq", ColumnEncoding.FLAT, true),
                new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true));
        week.where(new Plan.Predicate("=", week.column("d_date"), new Plan.Lit(LocalDate.of(2000, 1, 3).toEpochDay())))
                .groupBy("d_week_seq")
                .count();
        week.select(new Plan.Col(0));

        QueryLowering dates = QueryLowering.scan("date_dim",
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_week_seq", ColumnEncoding.FLAT, true))
                .crossJoin("q58_week", new QueryLowering.Column("qw_week_seq", ColumnEncoding.FLAT, false));
        dates.where(new Plan.Predicate("=", dates.column("d_week_seq"), dates.column("qw_week_seq")))
                .groupBy("d_date")
                .count();
        dates.select(new Plan.Col(0));

        // Combined main columns: item(0), store_rev(1), c_item(2), catalog_rev(3), w_item(4), web_rev(5).
        QueryLowering main = QueryLowering.scan("q58_store",
                        new QueryLowering.Column("s_item", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("s_rev", ColumnEncoding.FLAT, true))
                .join("q58_catalog", "s_item", "c_item",
                        new QueryLowering.Column("c_item", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("c_rev", ColumnEncoding.FLAT, true))
                .join("q58_web", "s_item", "w_item",
                        new QueryLowering.Column("w_item", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("w_rev", ColumnEncoding.FLAT, true));
        List<Plan.Condition> similarity = new ArrayList<>();
        int[][] pairs = {{1, 3}, {1, 5}, {3, 1}, {3, 5}, {5, 1}, {5, 3}};
        for (int[] pair : pairs) {
            similarity.addAll(query58WithinTenPercent(pair[0], pair[1]));
        }
        main.where(similarity.toArray(Plan.Condition[]::new));
        channelShareSelect(main);

        return new Composite(
                List.of(new Stage(week, "q58_week"),
                        new Stage(dates, "q58_dates"),
                        new Stage(query58Channel("store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"),
                                "q58_store", List.of(new DictRef(0, 1, 1))),
                        new Stage(query58Channel("catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_ext_sales_price"),
                                "q58_catalog", List.of(new DictRef(0, 1, 1))),
                        new Stage(query58Channel("web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_ext_sales_price"),
                                "q58_web", List.of(new DictRef(0, 1, 1)))),
                main,
                List.of(new DictRef(0, 0, 0)));
    }

    /** A channel's revenue per item id in the target week: sales joined to item, date_dim, and q58_dates, grouped. */
    private static QueryLowering query58Channel(String salesTable, String soldDate, String item, String price)
    {
        QueryLowering channel = QueryLowering.scan(salesTable,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(item, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(price, ColumnEncoding.FLAT, true))
                .join("item", item, "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true))
                .join("q58_dates", "d_date", "qd_date",
                        new QueryLowering.Column("qd_date", ColumnEncoding.FLAT, false));
        channel.groupBy("i_item_id")
                .aggregate("sum", price);
        return channel;
    }

    /** {@code left} within ten percent of {@code right}, as cross-multiplied integer bounds: 9r <= 10l AND 10l <= 11r. */
    private static List<Plan.Condition> query58WithinTenPercent(int left, int right)
    {
        return List.of(
                new Plan.Predicate("<=",
                        new Plan.Bin("*", new Plan.Col(right), new Plan.Lit(9)),
                        new Plan.Bin("*", new Plan.Col(left), new Plan.Lit(10))),
                new Plan.Predicate("<=",
                        new Plan.Bin("*", new Plan.Col(left), new Plan.Lit(10)),
                        new Plan.Bin("*", new Plan.Col(right), new Plan.Lit(11))));
    }

    public static Composite query57()
    {
        // Q57: call-center/brand months whose catalog sales deviate more than ten percent from that year's monthly
        // average, with the neighboring months' sales for context. Monthly sales per (category, brand, call center)
        // are ranked by month over a 14-month window; the ranked relation is assembled three times (no reuse): the
        // current rows keep 1999 and append the year's partition average, and the previous/next rows shift the rank
        // by one so an equi-join on (category, brand, call center, rank) aligns each month with its neighbors. Rows
        // within ten percent of the average are dropped (fraction-free, the Q53 test) and the result orders by the
        // deviation then the call center, top 100.
        List<Stage> stages = new ArrayList<>();
        List<DictRef> groupedStrings = List.of(new DictRef(0, 1, 2), new DictRef(1, 1, 1), new DictRef(2, 3, 1));
        List<DictRef> passThroughStrings = List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2));
        for (String assembly : List.of("a", "b", "c")) {
            stages.add(new Stage(query57MonthlyGroupedSales(), "q57_grouped_" + assembly, groupedStrings));
            stages.add(new Stage(query57MonthlyRankedSales("q57_grouped_" + assembly), "q57_ranked_" + assembly, passThroughStrings));
        }

        // Current rows: the 1999 months with the year's average monthly sales appended per (category, brand, call
        // center, year). Window output appends the average as column 7; the select reorders to
        // (cat, brand, cc, year, moy, avg, sum, rank).
        QueryLowering current = query57ScanRanked("q57_ranked_a");
        current.where(new Plan.Predicate("=", new Plan.Col(3), new Plan.Lit(1999)))
                .window(Plan.Window.partitionAverage(new int[] {0, 1, 2, 3}, 5));
        current.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4),
                new Plan.Col(7), new Plan.Col(5), new Plan.Col(6));
        stages.add(new Stage(current, "q57_current", passThroughStrings));

        stages.add(new Stage(query57AdjacentRows("q57_ranked_b", true), "q57_previous", passThroughStrings));
        stages.add(new Stage(query57AdjacentRows("q57_ranked_c", false), "q57_next", passThroughStrings));

        // Combined main columns: current(0-7: cat, brand, cc, year, moy, avg, sum, rank), previous(8-12), next(13-17).
        QueryLowering main = QueryLowering.scan("q57_current",
                        new QueryLowering.Column("m_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("m_brand", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("m_call_center", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("m_year", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("m_moy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("m_avg", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("m_sum", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("m_rank", ColumnEncoding.FLAT, false))
                .join("q57_previous",
                        new String[] {"m_category", "m_brand", "m_call_center", "m_rank"},
                        new String[] {"p_category", "p_brand", "p_call_center", "p_rank"},
                        new QueryLowering.Column("p_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("p_brand", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("p_call_center", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("p_sum", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("p_rank", ColumnEncoding.FLAT, false))
                .join("q57_next",
                        new String[] {"m_category", "m_brand", "m_call_center", "m_rank"},
                        new String[] {"n_category", "n_brand", "n_call_center", "n_rank"},
                        new QueryLowering.Column("n_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("n_brand", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("n_call_center", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("n_sum", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("n_rank", ColumnEncoding.FLAT, false));
        // Keep months deviating > 10% from the year's average: avg > 0 AND |sum - avg| * 10 > avg.
        main.where(
                        new Plan.Predicate(">", new Plan.Col(5), new Plan.Lit(0)),
                        new Plan.Or(List.of(
                                new Plan.Predicate(">", new Plan.Bin("*", new Plan.Bin("-", new Plan.Col(6), new Plan.Col(5)), new Plan.Lit(10)), new Plan.Col(5)),
                                new Plan.Predicate(">", new Plan.Bin("*", new Plan.Bin("-", new Plan.Col(5), new Plan.Col(6)), new Plan.Lit(10)), new Plan.Col(5)))))
                .select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4),
                        new Plan.Col(5), new Plan.Col(6), new Plan.Col(11), new Plan.Col(16))
                .orderBy(new Plan.Ordering(List.of(
                        Plan.SortKey.expression(new Plan.Bin("-", new Plan.Col(6), new Plan.Col(5)), Types.LONG, false),
                        new Plan.SortKey(2, false)), 100));

        return new Composite(stages, main,
                List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2)));
    }

    public static Composite query47()
    {
        // Q47: Q57's store-channel sibling -- store-brand months whose sales deviate more than ten percent from that
        // year's monthly average, with the neighboring months' sales. Monthly store sales per (category, brand,
        // store name, company name) are ranked chronologically (unlimited rank); the ranked relation is assembled
        // three times, the current rows keep 1999 with the year's partition average, the previous/next rows shift
        // the rank by one, and the three align on the (strings + rank) equi-join. Deviation filter, order by the
        // deviation then the store name, top 100.
        List<Stage> stages = new ArrayList<>();
        List<DictRef> groupedStrings = List.of(
                new DictRef(0, 1, 2), new DictRef(1, 1, 1), new DictRef(2, 3, 1), new DictRef(3, 3, 2));
        List<DictRef> passThroughStrings = List.of(
                new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2), new DictRef(3, 0, 3));
        for (String assembly : List.of("a", "b", "c")) {
            stages.add(new Stage(query47MonthlyGroupedSales(), "q47_grouped_" + assembly, groupedStrings));
            stages.add(new Stage(query47MonthlyRankedSales("q47_grouped_" + assembly), "q47_ranked_" + assembly, passThroughStrings));
        }

        // Current rows: the 1999 months with the year's average appended per (category, brand, store, company).
        // Window output appends the average as column 8; the select reorders to
        // (cat, brand, store, company, year, moy, avg, sum, rank).
        QueryLowering current = query47ScanRanked("q47_ranked_a");
        current.where(new Plan.Predicate("=", new Plan.Col(4), new Plan.Lit(1999)))
                .window(Plan.Window.partitionAverage(new int[] {0, 1, 2, 3}, 6));
        current.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4),
                new Plan.Col(5), new Plan.Col(8), new Plan.Col(6), new Plan.Col(7));
        stages.add(new Stage(current, "q47_current", passThroughStrings));

        stages.add(new Stage(query47AdjacentRows("q47_ranked_b", true), "q47_previous", passThroughStrings));
        stages.add(new Stage(query47AdjacentRows("q47_ranked_c", false), "q47_next", passThroughStrings));

        // Combined main columns: current(0-8: cat, brand, store, company, year, moy, avg, sum, rank),
        // previous(9-14), next(15-20).
        QueryLowering main = QueryLowering.scan("q47_current",
                        new QueryLowering.Column("m_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("m_brand", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("m_store_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("m_company_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("m_year", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("m_moy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("m_avg", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("m_sum", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("m_rank", ColumnEncoding.FLAT, false))
                .join("q47_previous",
                        new String[] {"m_category", "m_brand", "m_store_name", "m_company_name", "m_rank"},
                        new String[] {"p_category", "p_brand", "p_store_name", "p_company_name", "p_rank"},
                        new QueryLowering.Column("p_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("p_brand", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("p_store_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("p_company_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("p_sum", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("p_rank", ColumnEncoding.FLAT, false))
                .join("q47_next",
                        new String[] {"m_category", "m_brand", "m_store_name", "m_company_name", "m_rank"},
                        new String[] {"n_category", "n_brand", "n_store_name", "n_company_name", "n_rank"},
                        new QueryLowering.Column("n_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("n_brand", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("n_store_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("n_company_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("n_sum", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("n_rank", ColumnEncoding.FLAT, false));
        // Keep months deviating > 10% from the year's average: avg > 0 AND |sum - avg| * 10 > avg.
        main.where(
                        new Plan.Predicate(">", new Plan.Col(6), new Plan.Lit(0)),
                        new Plan.Or(List.of(
                                new Plan.Predicate(">", new Plan.Bin("*", new Plan.Bin("-", new Plan.Col(7), new Plan.Col(6)), new Plan.Lit(10)), new Plan.Col(6)),
                                new Plan.Predicate(">", new Plan.Bin("*", new Plan.Bin("-", new Plan.Col(6), new Plan.Col(7)), new Plan.Lit(10)), new Plan.Col(6)))))
                .select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(4),
                        new Plan.Col(5), new Plan.Col(6), new Plan.Col(7), new Plan.Col(13), new Plan.Col(19))
                .orderBy(new Plan.Ordering(List.of(
                        Plan.SortKey.expression(new Plan.Bin("-", new Plan.Col(7), new Plan.Col(6)), Types.LONG, false),
                        new Plan.SortKey(2, false)), 100));

        return new Composite(stages, main,
                List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2), new DictRef(3, 0, 3)));
    }

    /** Monthly store sales per (category, brand, store name, company name) over the 14-month window around 1999. */
    private static QueryLowering query47MonthlyGroupedSales()
    {
        QueryLowering grouped = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sales_price", ColumnEncoding.FLAT, true))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_moy", ColumnEncoding.FLAT, true))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("s_company_name", ColumnEncoding.STRING, false));
        grouped.where(new Plan.Or(List.of(
                        new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(1999)),
                        new Plan.And(List.of(
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(1998)),
                                new Plan.Predicate("=", grouped.column("d_moy"), new Plan.Lit(12)))),
                        new Plan.And(List.of(
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(2000)),
                                new Plan.Predicate("=", grouped.column("d_moy"), new Plan.Lit(1)))))))
                .groupBy("i_category", "i_brand", "s_store_name", "s_company_name", "d_year", "d_moy")
                .aggregate("sum", "ss_sales_price");
        return grouped;
    }

    /** RANK() each (category, brand, store, company)'s months chronologically over the grouped virtual relation. */
    private static QueryLowering query47MonthlyRankedSales(String groupedVirtual)
    {
        QueryLowering ranked = query47ScanGrouped(groupedVirtual);
        ranked.window(new Plan.Window(new int[] {0, 1, 2, 3},
                List.of(new Plan.SortKey(4, false), new Plan.SortKey(5, false)), Plan.RankFunction.RANK, -1));
        return ranked;
    }

    /** The neighbor rows: (category, brand, store, company, sum, rank shifted by one) for the rank equi-join. */
    private static QueryLowering query47AdjacentRows(String rankedVirtual, boolean previous)
    {
        QueryLowering adjacent = query47ScanRanked(rankedVirtual);
        adjacent.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(6),
                new Plan.Bin(previous ? "+" : "-", new Plan.Col(7), new Plan.Lit(1)));
        return adjacent;
    }

    private static QueryLowering query47ScanGrouped(String groupedVirtual)
    {
        return QueryLowering.scan(groupedVirtual,
                new QueryLowering.Column("g_category", ColumnEncoding.STRING, true),
                new QueryLowering.Column("g_brand", ColumnEncoding.STRING, true),
                new QueryLowering.Column("g_store_name", ColumnEncoding.STRING, false),
                new QueryLowering.Column("g_company_name", ColumnEncoding.STRING, false),
                new QueryLowering.Column("g_year", ColumnEncoding.FLAT, false),
                new QueryLowering.Column("g_moy", ColumnEncoding.FLAT, false),
                new QueryLowering.Column("g_sum", ColumnEncoding.FLAT, true));
    }

    private static QueryLowering query47ScanRanked(String rankedVirtual)
    {
        return QueryLowering.scan(rankedVirtual,
                new QueryLowering.Column("r_category", ColumnEncoding.STRING, true),
                new QueryLowering.Column("r_brand", ColumnEncoding.STRING, true),
                new QueryLowering.Column("r_store_name", ColumnEncoding.STRING, false),
                new QueryLowering.Column("r_company_name", ColumnEncoding.STRING, false),
                new QueryLowering.Column("r_year", ColumnEncoding.FLAT, false),
                new QueryLowering.Column("r_moy", ColumnEncoding.FLAT, false),
                new QueryLowering.Column("r_sum", ColumnEncoding.FLAT, true),
                new QueryLowering.Column("r_rank", ColumnEncoding.FLAT, false));
    }

    /** Monthly catalog sales per (category, brand, call center) over the 14-month window around 1999. */
    private static QueryLowering query57MonthlyGroupedSales()
    {
        QueryLowering grouped = QueryLowering.scan("catalog_sales",
                        new QueryLowering.Column("cs_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_call_center_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_sales_price", ColumnEncoding.FLAT, true))
                .join("item", "cs_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true))
                .join("date_dim", "cs_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_moy", ColumnEncoding.FLAT, true))
                .join("call_center", "cs_call_center_sk", "cc_call_center_sk",
                        new QueryLowering.Column("cc_call_center_sk"),
                        new QueryLowering.Column("cc_name", ColumnEncoding.STRING, false));
        grouped.where(new Plan.Or(List.of(
                        new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(1999)),
                        new Plan.And(List.of(
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(1998)),
                                new Plan.Predicate("=", grouped.column("d_moy"), new Plan.Lit(12)))),
                        new Plan.And(List.of(
                                new Plan.Predicate("=", grouped.column("d_year"), new Plan.Lit(2000)),
                                new Plan.Predicate("=", grouped.column("d_moy"), new Plan.Lit(1)))))))
                .groupBy("i_category", "i_brand", "cc_name", "d_year", "d_moy")
                .aggregate("sum", "cs_sales_price");
        return grouped;
    }

    /** RANK() each (category, brand, call center)'s months chronologically over the grouped virtual relation. */
    private static QueryLowering query57MonthlyRankedSales(String groupedVirtual)
    {
        QueryLowering ranked = query57ScanGrouped(groupedVirtual);
        ranked.window(new Plan.Window(new int[] {0, 1, 2},
                List.of(new Plan.SortKey(3, false), new Plan.SortKey(4, false)), Plan.RankFunction.RANK, 32));
        return ranked;
    }

    /** The neighbor rows: (category, brand, call center, sum, rank shifted by one) so the rank equi-join aligns months. */
    private static QueryLowering query57AdjacentRows(String rankedVirtual, boolean previous)
    {
        QueryLowering adjacent = query57ScanRanked(rankedVirtual);
        adjacent.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(5),
                new Plan.Bin(previous ? "+" : "-", new Plan.Col(6), new Plan.Lit(1)));
        return adjacent;
    }

    private static QueryLowering query57ScanGrouped(String groupedVirtual)
    {
        return QueryLowering.scan(groupedVirtual,
                new QueryLowering.Column("g_category", ColumnEncoding.STRING, true),
                new QueryLowering.Column("g_brand", ColumnEncoding.STRING, true),
                new QueryLowering.Column("g_call_center", ColumnEncoding.STRING, false),
                new QueryLowering.Column("g_year", ColumnEncoding.FLAT, false),
                new QueryLowering.Column("g_moy", ColumnEncoding.FLAT, false),
                new QueryLowering.Column("g_sum", ColumnEncoding.FLAT, true));
    }

    private static QueryLowering query57ScanRanked(String rankedVirtual)
    {
        return QueryLowering.scan(rankedVirtual,
                new QueryLowering.Column("r_category", ColumnEncoding.STRING, true),
                new QueryLowering.Column("r_brand", ColumnEncoding.STRING, true),
                new QueryLowering.Column("r_call_center", ColumnEncoding.STRING, false),
                new QueryLowering.Column("r_year", ColumnEncoding.FLAT, false),
                new QueryLowering.Column("r_moy", ColumnEncoding.FLAT, false),
                new QueryLowering.Column("r_sum", ColumnEncoding.FLAT, true),
                new QueryLowering.Column("r_rank", ColumnEncoding.FLAT, false));
    }

    public static Composite query08()
    {
        // Q08: Q2/1998 store net profit per store, for stores whose two-digit zip prefix qualifies -- the prefix of a
        // five-digit zip that is in the literal list AND has more than ten preferred customers. The qualifying
        // prefixes derive in two stages (preferred customers' zips truncated to five digits, list-filtered, grouped
        // with a count-over-ten having; then the two-digit prefix of those, distinct); the main joins the store's
        // own two-digit prefix against that set -- substring-derived string keys on both join sides. Ordered by
        // store name, top 100.
        QueryLowering zips = QueryLowering.scan("customer",
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_preferred_cust_flag", ColumnEncoding.STRING, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        QueryLowering.Column.substring("ca_zip", true, 1, 5));
        zips.where(
                        new Plan.StringMatch(zips.position("c_preferred_cust_flag"), List.of("Y"), false),
                        new Plan.StringMatch(zips.position("ca_zip"), TpcdsQueryLiterals.QUERY08_ZIP_VALUES, false))
                .groupBy("ca_zip")
                .count();
        zips.having(new Plan.Predicate(">", new Plan.Col(1), new Plan.Lit(10)));

        QueryLowering prefixes = QueryLowering.scan("q08_zips",
                        QueryLowering.Column.substring("z_zip", false, 1, 2),
                        new QueryLowering.Column("z_count", ColumnEncoding.FLAT, false))
                .groupBy("z_zip")
                .count();
        prefixes.select(new Plan.Col(0));

        QueryLowering main = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_net_profit", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_qoy", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, false),
                        QueryLowering.Column.substring("s_zip", true, 1, 2))
                .join("q08_prefixes", "s_zip", "p_zip",
                        new QueryLowering.Column("p_zip", ColumnEncoding.STRING, false));
        main.where(
                        new Plan.Predicate("=", main.column("d_qoy"), new Plan.Lit(2)),
                        new Plan.Predicate("=", main.column("d_year"), new Plan.Lit(1998)))
                .groupBy("s_store_name")
                .aggregate("sum", "ss_net_profit");
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 100));

        return new Composite(
                List.of(new Stage(zips, "q08_zips", List.of(new DictRef(0, 1, 1))),
                        new Stage(prefixes, "q08_prefixes", List.of(new DictRef(0, 0, 0)))),
                main,
                List.of(new DictRef(0, 2, 1)));
    }

    public static Composite query09()
    {
        // Q09: five bucket statistics in one row -- for each store-sales quantity band, the average discount when the
        // band's sale count exceeds its literal threshold, else the average net paid. Each band contributes three
        // global aggregates over the same filtered scan (the count and two rounded averages); the fifteen single-row
        // relations broadcast onto the single-reason probe through chained cross joins, and five CASE projections
        // pick each band's value. The Q88/Q90 scalar-broadcast shape, fifteen wide.
        record Bucket(int minimum, int maximum, long threshold) {}

        List<Bucket> buckets = List.of(
                new Bucket(1, 20, 74_129L),
                new Bucket(21, 40, 122_840L),
                new Bucket(41, 60, 56_580L),
                new Bucket(61, 80, 10_097L),
                new Bucket(81, 100, 165_306L));

        List<Stage> stages = new ArrayList<>();
        QueryLowering main = QueryLowering.scan("reason", new QueryLowering.Column("r_reason_sk", ColumnEncoding.FLAT, false));
        for (int b = 0; b < buckets.size(); b++) {
            Bucket bucket = buckets.get(b);
            stages.add(new Stage(query09Count(bucket.minimum(), bucket.maximum()), "q09_count_" + b));
            stages.add(new Stage(query09Average(bucket.minimum(), bucket.maximum(), "ss_ext_discount_amt"), "q09_discount_" + b));
            stages.add(new Stage(query09Average(bucket.minimum(), bucket.maximum(), "ss_net_paid"), "q09_paid_" + b));
            main = main.crossJoin("q09_count_" + b, new QueryLowering.Column("count_" + b, ColumnEncoding.FLAT, false))
                    .crossJoin("q09_discount_" + b, new QueryLowering.Column("discount_" + b, ColumnEncoding.FLAT, true))
                    .crossJoin("q09_paid_" + b, new QueryLowering.Column("paid_" + b, ColumnEncoding.FLAT, true));
        }
        main.where(new Plan.Predicate("=", new Plan.Col(0), new Plan.Lit(1)));
        // Combined columns: r_reason_sk(0), then (count, discount, paid) per bucket at 1 + 3b.
        Plan.Expr[] values = new Plan.Expr[buckets.size()];
        for (int b = 0; b < buckets.size(); b++) {
            values[b] = new Plan.Case(
                    List.of(new Plan.Case.Branch(
                            new Plan.Predicate(">", new Plan.Col(1 + 3 * b), new Plan.Lit(buckets.get(b).threshold())),
                            new Plan.Col(2 + 3 * b))),
                    new Plan.Col(3 + 3 * b));
        }
        main.select(values);

        return new Composite(stages, main, List.of());
    }

    /** The band's sale count: a global count over the quantity-filtered scan. */
    private static QueryLowering query09Count(int minimumQuantity, int maximumQuantity)
    {
        QueryLowering count = QueryLowering.scan("store_sales",
                new QueryLowering.Column("ss_quantity", ColumnEncoding.FLAT, true));
        query09QuantityBand(count, minimumQuantity, maximumQuantity);
        count.count();
        return count;
    }

    /** The band's rounded average of {@code measure}: a global sum/count over the quantity-filtered scan. */
    private static QueryLowering query09Average(int minimumQuantity, int maximumQuantity, String measure)
    {
        QueryLowering average = QueryLowering.scan("store_sales",
                new QueryLowering.Column("ss_quantity", ColumnEncoding.FLAT, true),
                new QueryLowering.Column(measure, ColumnEncoding.FLAT, true));
        query09QuantityBand(average, minimumQuantity, maximumQuantity);
        average.aggregate("sum", measure)
                .aggregate("count", measure);
        average.select(new Plan.Call("divide_round_i64", new Plan.Col(0), new Plan.Col(1)));
        return average;
    }

    private static void query09QuantityBand(QueryLowering query, int minimumQuantity, int maximumQuantity)
    {
        query.where(
                new Plan.Predicate(">", query.column("ss_quantity"), new Plan.Lit(minimumQuantity - 1L)),
                new Plan.Predicate("<", query.column("ss_quantity"), new Plan.Lit(maximumQuantity + 1L)));
    }

    public static UnionComposite query54()
    {
        // Q54: store revenue of "my customers" (December 1998 catalog/web buyers of women's maternity items),
        // segmented into fifty-dollar bands over the three months after the purchase month, counting customers per
        // band. The channel branches carry their joins (the harness joins once over the raw fact union; distributing
        // them lets the union materialize filtered), dedup into the customer set; the revenue stage joins store
        // sales to that set, the customer's county/state, the matching store, and the month, brackets the month
        // between the scalar boundaries, groups revenue per customer, and emits the band (null revenue reads zero,
        // matching the non-null-propagating divide). The main counts customers per band, top 100.
        List<QueryLowering> branches = List.of(
                query54Channel("catalog_sales", "cs_sold_date_sk", "cs_bill_customer_sk", "cs_item_sk"),
                query54Channel("web_sales", "ws_sold_date_sk", "ws_bill_customer_sk", "ws_item_sk"));

        QueryLowering customers = QueryLowering.scan("q54_sales",
                        new QueryLowering.Column("u_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("u_addr", ColumnEncoding.FLAT, true))
                .groupBy("u_customer", "u_addr")
                .count();
        customers.select(new Plan.Col(0), new Plan.Col(1));

        QueryLowering revenue = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("q54_customers", "ss_customer_sk", "mc_customer",
                        new QueryLowering.Column("mc_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("mc_addr", ColumnEncoding.FLAT, true))
                .join("customer_address", "mc_addr", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, true))
                .join("store",
                        new String[] {"ca_county", "ca_state"},
                        new String[] {"s_county", "s_state"},
                        new QueryLowering.Column("s_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_state", ColumnEncoding.STRING, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_month_seq", ColumnEncoding.FLAT, true))
                .crossJoin("q54_lower", new QueryLowering.Column("lower_bound", ColumnEncoding.FLAT, false))
                .crossJoin("q54_upper", new QueryLowering.Column("upper_bound", ColumnEncoding.FLAT, false));
        revenue.where(
                        new Plan.Predicate(">=", revenue.column("d_month_seq"), revenue.column("lower_bound")),
                        new Plan.Predicate("<=", revenue.column("d_month_seq"), revenue.column("upper_bound")))
                .groupBy("ss_customer_sk")
                .aggregate("sum", "ss_ext_sales_price");
        // The fifty-dollar band: revenue (in cents) / 5000; a null revenue's value slot is zero, matching the
        // harness's non-null-propagating divide.
        revenue.select(new Plan.Bin("/", new Plan.Coalesce(new Plan.Col(1), new Plan.Lit(0)), new Plan.Lit(5_000)));

        QueryLowering main = QueryLowering.scan("q54_segments",
                        new QueryLowering.Column("s_segment", ColumnEncoding.FLAT, false))
                .groupBy("s_segment")
                .count();
        main.select(new Plan.Col(0), new Plan.Col(1), new Plan.Bin("*", new Plan.Col(0), new Plan.Lit(50)))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100));

        return new UnionComposite(branches, "q54_sales", List.of(),
                List.of(new Stage(customers, "q54_customers"),
                        new Stage(query54MonthBoundary(1), "q54_lower"),
                        new Stage(query54MonthBoundary(3), "q54_upper"),
                        new Stage(revenue, "q54_segments")),
                main, List.of());
    }

    /** One channel's December 1998 maternity buyers: (customer, current address). */
    private static QueryLowering query54Channel(String salesTable, String soldDate, String customer, String item)
    {
        QueryLowering channel = QueryLowering.scan(salesTable,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(customer, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(item, ColumnEncoding.FLAT, true))
                .join("item", item, "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_class", ColumnEncoding.STRING, true))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true))
                .join("customer", customer, "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true));
        channel.where(
                        new Plan.StringMatch(channel.position("i_category"), List.of("Women"), false),
                        new Plan.StringMatch(channel.position("i_class"), List.of("maternity"), false),
                        new Plan.Predicate("=", channel.column("d_moy"), new Plan.Lit(12)),
                        new Plan.Predicate("=", channel.column("d_year"), new Plan.Lit(1998)))
                .select(channel.column(customer), channel.column("c_current_addr_sk"));
        return channel;
    }

    /** The scalar month boundary: the distinct December 1998 month sequence plus {@code offset}. */
    private static QueryLowering query54MonthBoundary(int offset)
    {
        QueryLowering boundary = QueryLowering.scan("date_dim",
                new QueryLowering.Column("d_month_seq", ColumnEncoding.FLAT, true),
                new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true),
                new QueryLowering.Column("d_moy", ColumnEncoding.FLAT, true));
        boundary.where(
                        new Plan.Predicate("=", boundary.column("d_year"), new Plan.Lit(1998)),
                        new Plan.Predicate("=", boundary.column("d_moy"), new Plan.Lit(12)))
                .groupBy("d_month_seq")
                .count();
        boundary.select(new Plan.Bin("+", new Plan.Col(0), new Plan.Lit(offset)));
        return boundary;
    }

    public static Composite query24()
    {
        // Q24: pale-colored store sales by customer name and store, kept when the customer's per-store-and-item
        // total exceeds twenty times the overall average. The returned-sales base (sales joined to their returns
        // and the customer) assembles twice, mirroring the harness; each side joins the market-8 store and the
        // deduplicated address set on the zip string (zips are zero-padded five-char, so string equality matches
        // the harness's cast-to-i64 keys) and keeps rows whose birth country equals the uppercased address country.
        // Combined columns: base(0-5), store(6-10), item(11-16), addresses(17-19). Group keys in the harness's
        // output order: (last, first, store_name, ca_state, s_state, color, price, manager, units, size).
        QueryLowering grouped = QueryLowering.scan("q24_base_sales",
                        new QueryLowering.Column("b_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("b_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("b_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("b_store", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("b_item", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("b_paid", ColumnEncoding.FLAT, true))
                .join("store", "b_store", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_market_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("s_store_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("s_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_zip", ColumnEncoding.STRING, true))
                .join("item", "b_item", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_current_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_size", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_color", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_units", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_manager_id", ColumnEncoding.FLAT, true))
                .join("q24_addresses_sales", "s_zip", "a_zip",
                        new QueryLowering.Column("a_zip", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("a_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("a_state", ColumnEncoding.STRING, true));
        grouped.where(
                        new Plan.Predicate("=", grouped.column("s_market_id"), new Plan.Lit(8)),
                        new Plan.StringMatch(grouped.position("s_zip"), List.of(""), true),
                        new Plan.StringMatch(grouped.position("i_color"), List.of("pale"), false),
                        new Plan.StringColumnCompare(grouped.position("b_country"), grouped.position("a_country"), false))
                .groupBy("b_last", "b_first", "s_store_name", "a_state", "s_state", "i_color", "i_current_price", "i_manager_id", "i_units", "i_size")
                .aggregate("sum", "b_paid");

        QueryLowering regrouped = QueryLowering.scan("q24_pale_sales",
                        new QueryLowering.Column("g_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_store_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("g_ca_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_s_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_color", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_manager", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("g_units", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_size", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_paid", ColumnEncoding.FLAT, true))
                .groupBy("g_last", "g_first", "g_store_name")
                .aggregate("sum", "g_paid");

        QueryLowering averageBase = query24ReturnedSales();
        // Combined columns: base(0-5), store(6-8), addresses(9-11). Group keys in the harness's output order.
        QueryLowering averaged = QueryLowering.scan("q24_base_average",
                        new QueryLowering.Column("b_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("b_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("b_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("b_store", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("b_item", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("b_paid", ColumnEncoding.FLAT, true))
                .join("store", "b_store", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_market_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("s_zip", ColumnEncoding.STRING, true))
                .join("q24_addresses_average", "s_zip", "a_zip",
                        new QueryLowering.Column("a_zip", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("a_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("a_state", ColumnEncoding.STRING, true));
        averaged.where(
                        new Plan.Predicate("=", averaged.column("s_market_id"), new Plan.Lit(8)),
                        new Plan.StringMatch(averaged.position("s_zip"), List.of(""), true),
                        new Plan.StringColumnCompare(averaged.position("b_country"), averaged.position("a_country"), false))
                .groupBy("b_last", "b_first", "a_state", "b_store", "b_item")
                .aggregate("sum", "b_paid");

        QueryLowering average = QueryLowering.scan("q24_average_groups",
                        new QueryLowering.Column("a_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("a_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("a_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("a_store", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("a_item", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("a_paid", ColumnEncoding.FLAT, true))
                .aggregate("sum", "a_paid")
                .aggregate("count", "a_paid");

        QueryLowering main = QueryLowering.scan("q24_grouped",
                        new QueryLowering.Column("m_last", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("m_first", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("m_store_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("m_paid", ColumnEncoding.FLAT, true))
                .crossJoin("q24_average",
                        new QueryLowering.Column("avg_sum", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("avg_count", ColumnEncoding.FLAT, true));
        main.where(
                        new Plan.IsNull(main.position("m_paid"), true),
                        new Plan.IsNull(main.position("avg_sum"), true),
                        new Plan.IsNull(main.position("avg_count"), true),
                        new Plan.Predicate("<", main.column("avg_sum"),
                                new Plan.Bin("*", new Plan.Bin("*", main.column("avg_count"), main.column("m_paid")), new Plan.Lit(20))))
                .select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3))
                .orderBy(new Plan.Ordering(
                        List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)), 100));

        return new Composite(
                List.of(new Stage(query24ReturnedSales(), "q24_base_sales", List.of(new DictRef(0, 2, 1), new DictRef(1, 2, 2), new DictRef(2, 2, 3))),
                        new Stage(query24Addresses(), "q24_addresses_sales", List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 2), new DictRef(2, 0, 1))),
                        new Stage(grouped, "q24_pale_sales", List.of(
                                new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 1, 2), new DictRef(3, 3, 2),
                                new DictRef(4, 1, 3), new DictRef(5, 2, 3), new DictRef(8, 2, 4), new DictRef(9, 2, 2))),
                        new Stage(regrouped, "q24_grouped", List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2))),
                        new Stage(averageBase, "q24_base_average", List.of(new DictRef(0, 2, 1), new DictRef(1, 2, 2), new DictRef(2, 2, 3))),
                        new Stage(query24Addresses(), "q24_addresses_average", List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 2), new DictRef(2, 0, 1))),
                        new Stage(averaged, "q24_average_groups", List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 2, 2))),
                        new Stage(average, "q24_average")),
                main,
                List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2)));
    }

    /**
     * Q24's returned-sales base, assembled once per side like the harness: store sales joined to their returns and
     * the buying customer, summed per (name, birth country, store, item).
     */
    private static QueryLowering query24ReturnedSales()
    {
        QueryLowering base = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_net_paid", ColumnEncoding.FLAT, true))
                .join("store_returns",
                        new String[] {"ss_ticket_number", "ss_item_sk"},
                        new String[] {"sr_ticket_number", "sr_item_sk"},
                        new QueryLowering.Column("sr_ticket_number", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("sr_item_sk", ColumnEncoding.FLAT, true))
                .join("customer", "ss_customer_sk", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_birth_country", ColumnEncoding.STRING, true));
        base.groupBy("c_last_name", "c_first_name", "c_birth_country", "ss_store_sk", "ss_item_sk")
                .aggregate("sum", "ss_net_paid");
        return base;
    }

    /**
     * Q24's deduplicated address set: (zip, uppercased country, state), distinct over the non-empty-zip addresses.
     * The country uppercases at load so the birth-country comparison operates on the derived values.
     */
    private static QueryLowering query24Addresses()
    {
        QueryLowering addresses = QueryLowering.scan("customer_address",
                new QueryLowering.Column("ca_zip", ColumnEncoding.STRING, true),
                new QueryLowering.Column("ca_state", ColumnEncoding.STRING, true),
                QueryLowering.Column.upper("ca_country", true));
        addresses.where(new Plan.StringMatch(addresses.position("ca_zip"), List.of(""), true))
                .groupBy("ca_zip", "ca_country", "ca_state")
                .count();
        addresses.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2));
        return addresses;
    }

    public static Composite query41()
    {
        // Q41: distinct product names of items in a manufacturer-id band whose manufacturer also makes one of eight
        // category/color/unit/size combinations. The eligible manufacturers distinct into a stage (the SQL's EXISTS
        // as the distinct-then-inner-join shape) and the main joins the banded items to them on the manufacturer
        // string, then dedups the product names, top 100.
        record Combination(String category, String colorA, String colorB, String unitA, String unitB, String sizeA, String sizeB) {}

        QueryLowering manufacturers = QueryLowering.scan("item",
                new QueryLowering.Column("i_manufact", ColumnEncoding.STRING, true),
                new QueryLowering.Column("i_category", ColumnEncoding.STRING, true),
                new QueryLowering.Column("i_color", ColumnEncoding.STRING, true),
                new QueryLowering.Column("i_units", ColumnEncoding.STRING, true),
                new QueryLowering.Column("i_size", ColumnEncoding.STRING, true));
        List<Plan.Condition> combinations = new ArrayList<>();
        for (Combination match : List.of(
                new Combination("Women", "powder", "khaki", "Ounce", "Oz", "medium", "extra large"),
                new Combination("Women", "brown", "honeydew", "Bunch", "Ton", "N/A", "small"),
                new Combination("Men", "floral", "deep", "N/A", "Dozen", "petite", "large"),
                new Combination("Men", "light", "cornflower", "Box", "Pound", "medium", "extra large"),
                new Combination("Women", "midnight", "snow", "Pallet", "Gross", "medium", "extra large"),
                new Combination("Women", "cyan", "papaya", "Cup", "Dram", "N/A", "small"),
                new Combination("Men", "orange", "frosted", "Each", "Tbl", "petite", "large"),
                new Combination("Men", "forest", "ghost", "Lb", "Bundle", "medium", "extra large"))) {
            combinations.add(new Plan.And(List.of(
                    new Plan.StringMatch(manufacturers.position("i_category"), List.of(match.category()), false),
                    new Plan.StringMatch(manufacturers.position("i_color"), List.of(match.colorA(), match.colorB()), false),
                    new Plan.StringMatch(manufacturers.position("i_units"), List.of(match.unitA(), match.unitB()), false),
                    new Plan.StringMatch(manufacturers.position("i_size"), List.of(match.sizeA(), match.sizeB()), false))));
        }
        manufacturers.where(new Plan.Or(combinations))
                .groupBy("i_manufact")
                .count();
        manufacturers.select(new Plan.Col(0));

        QueryLowering main = QueryLowering.scan("item",
                        new QueryLowering.Column("i_product_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_manufact_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_manufact", ColumnEncoding.STRING, true))
                .join("q41_manufacturers", "i_manufact", "m_manufact",
                        new QueryLowering.Column("m_manufact", ColumnEncoding.STRING, true));
        main.where(
                        new Plan.Predicate(">", main.column("i_manufact_id"), new Plan.Lit(737)),
                        new Plan.Predicate("<", main.column("i_manufact_id"), new Plan.Lit(779)))
                .groupBy("i_product_name")
                .count();
        main.select(new Plan.Col(0))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 100));

        return new Composite(
                List.of(new Stage(manufacturers, "q41_manufacturers", List.of(new DictRef(0, 0, 0)))),
                main,
                List.of(new DictRef(0, 0, 0)));
    }

    public static Composite query84()
    {
        // Q84: the customer id and "last, first" name for every store return made by an Edgewood customer in one
        // income band, once per matching return, ordered by id, top 100. Eligible households (demographics in the
        // banded income range) materialize as a stage; the main joins customers to their Edgewood address, the
        // households, and the returns by demographic (a one-to-many fan-out), carrying id/last/first to the output
        // where the bridge concatenates the name.
        QueryLowering households = QueryLowering.scan("household_demographics",
                        new QueryLowering.Column("hd_demo_sk", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("hd_income_band_sk", ColumnEncoding.FLAT, true))
                .join("income_band", "hd_income_band_sk", "ib_income_band_sk",
                        new QueryLowering.Column("ib_income_band_sk"),
                        new QueryLowering.Column("ib_lower_bound", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ib_upper_bound", ColumnEncoding.FLAT, true));
        households.where(
                        new Plan.Predicate(">", households.column("ib_lower_bound"), new Plan.Lit(38_127)),
                        new Plan.Predicate("<", households.column("ib_upper_bound"), new Plan.Lit(88_129)))
                .select(households.column("hd_demo_sk"));

        // Combined main columns: customer(0-5), address(6-7), households(8), store_returns(9).
        QueryLowering main = QueryLowering.scan("customer",
                        new QueryLowering.Column("c_customer_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_current_cdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_current_hdemo_sk", ColumnEncoding.FLAT, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_city", ColumnEncoding.STRING, true))
                .join("q84_households", "c_current_hdemo_sk", "h_demo",
                        new QueryLowering.Column("h_demo", ColumnEncoding.FLAT, false))
                .join("store_returns", "c_current_cdemo_sk", "sr_cdemo_sk",
                        new QueryLowering.Column("sr_cdemo_sk", ColumnEncoding.FLAT, true));
        main.where(new Plan.StringMatch(main.position("ca_city"), List.of("Edgewood"), false))
                .select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 100));

        return new Composite(
                List.of(new Stage(households, "q84_households")),
                main,
                List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1), new DictRef(2, 0, 2)));
    }

    public static Composite query45()
    {
        // Q45: Q2/2001 web sales by the customer's city and zip, keeping sales whose customer lives in one of nine
        // zips OR whose item is in a ten-item list. The item membership (the SQL's IN-subquery, the harness's mark
        // semi-join) is a LEFT join against the prime-keyed items on the item id string, with IS NOT NULL as the
        // membership test inside the OR.
        QueryLowering primes = QueryLowering.scan("item",
                new QueryLowering.Column("i_item_sk", ColumnEncoding.FLAT, false),
                new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false));
        List<Plan.Condition> primeKeys = new ArrayList<>();
        for (long prime : new long[] {2, 3, 5, 7, 11, 13, 17, 19, 23, 29}) {
            primeKeys.add(new Plan.Predicate("=", primes.column("i_item_sk"), new Plan.Lit(prime)));
        }
        primes.where(new Plan.Or(primeKeys))
                .select(primes.column("i_item_id"));

        // Combined main columns: web_sales(0-3), customer(4-5), address(6-8: sk, city, zip), date(9-11),
        // item(12-13), prime items(14, null when the item is not in the list).
        QueryLowering main = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_bill_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_sales_price", ColumnEncoding.FLAT, true))
                .join("customer", "ws_bill_customer_sk", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_city", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_zip", ColumnEncoding.STRING, true))
                .join("date_dim", "ws_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_qoy", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true))
                .join("item", "ws_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_id", ColumnEncoding.STRING, false))
                .leftJoin("q45_primes", "i_item_id", "p_item_id",
                        new QueryLowering.Column("p_item_id", ColumnEncoding.STRING, true));
        main.where(
                        new Plan.Predicate("=", main.column("d_qoy"), new Plan.Lit(2)),
                        new Plan.Predicate("=", main.column("d_year"), new Plan.Lit(2001)),
                        new Plan.Or(List.of(
                                new Plan.StringMatch(main.position("ca_zip"),
                                        List.of("85669", "86197", "88274", "83405", "86475", "85392", "85460", "80348", "81792"), false),
                                new Plan.IsNull(main.position("p_item_id"), true))))
                .groupBy("ca_zip", "ca_city")
                .aggregate("sum", "ws_sales_price");
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100));

        return new Composite(
                List.of(new Stage(primes, "q45_primes", List.of(new DictRef(0, 0, 1)))),
                main,
                List.of(new DictRef(0, 2, 2), new DictRef(1, 2, 1)));
    }

    public static Composite query72()
    {
        // Q72: catalog orders per (item, warehouse, week) where the warehouse was short -- on-hand inventory below
        // the ordered quantity in the order's week -- and the order shipped more than five days after the sale,
        // split into promoted and unpromoted counts. The 1999 inventory joined to its week is the one virtual
        // relation; the main streams catalog sales through the sold-date week, the (item, week) inventory match,
        // warehouse, item, the demographic filters, the ship date, and a LEFT join to promotion whose null-ness
        // one-hot splits the counts (the harness's mark semi-join, the SQL's left join).
        QueryLowering inventory = QueryLowering.scan("inventory",
                        new QueryLowering.Column("inv_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_quantity_on_hand", ColumnEncoding.FLAT, true))
                .join("date_dim", "inv_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_week_seq", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true));
        inventory.where(new Plan.Predicate("=", inventory.column("d_year"), new Plan.Lit(1999)))
                .select(inventory.column("inv_item_sk"), inventory.column("inv_warehouse_sk"),
                        inventory.column("inv_quantity_on_hand"), inventory.column("d_week_seq"));

        // Combined main columns: catalog_sales(0-7), sold date(8-11: sk, week, year, date), inventory(12-15),
        // warehouse(16-17), item(18-19), demographics(20-21, 22-23), ship date(24-25), promotion(26, null for
        // unpromoted orders). All references are by name.
        QueryLowering main = QueryLowering.scan("catalog_sales",
                        new QueryLowering.Column("cs_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_quantity", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_bill_cdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_bill_hdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_ship_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_promo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cs_order_number", ColumnEncoding.FLAT, true))
                .join("date_dim", "cs_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_week_seq", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_date", ColumnEncoding.FLAT, true))
                .join("q72_inventory",
                        new String[] {"cs_item_sk", "d_week_seq"},
                        new String[] {"inv_item", "inv_week"},
                        new QueryLowering.Column("inv_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("inv_warehouse", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("inv_on_hand", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_week", ColumnEncoding.FLAT, false))
                .join("warehouse", "inv_warehouse", "w_warehouse_sk",
                        new QueryLowering.Column("w_warehouse_sk"),
                        new QueryLowering.Column("w_warehouse_name", ColumnEncoding.STRING, true))
                .join("item", "cs_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_item_desc", ColumnEncoding.STRING, true))
                .join("customer_demographics", "cs_bill_cdemo_sk", "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, true))
                .join("household_demographics", "cs_bill_hdemo_sk", "hd_demo_sk",
                        new QueryLowering.Column("hd_demo_sk"),
                        new QueryLowering.Column("hd_buy_potential", ColumnEncoding.STRING, true))
                .join("date_dim", "cs_ship_date_sk", "d_date_sk_2",
                        new QueryLowering.Column("d_date_sk_2", "d_date_sk", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("d_ship_date", "d_date", ColumnEncoding.FLAT, true))
                .leftJoin("promotion", "cs_promo_sk", "p_promo_sk",
                        new QueryLowering.Column("p_promo_sk", ColumnEncoding.FLAT, true));
        Plan.Condition unpromoted = new Plan.IsNull(main.position("p_promo_sk"), false);
        main.where(
                        new Plan.Predicate("=", main.column("d_year"), new Plan.Lit(1999)),
                        new Plan.StringMatch(main.position("cd_marital_status"), List.of("D"), false),
                        new Plan.StringMatch(main.position("hd_buy_potential"), List.of(">10000"), false),
                        new Plan.Predicate("<", main.column("inv_on_hand"), main.column("cs_quantity")),
                        new Plan.Predicate(">", main.column("d_ship_date"),
                                new Plan.Bin("+", main.column("d_date"), new Plan.Lit(5))))
                .groupBy("i_item_desc", "w_warehouse_name", "d_week_seq")
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(unpromoted, new Plan.Lit(1))), new Plan.Lit(0)))
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(unpromoted, new Plan.Lit(0))), new Plan.Lit(1)))
                .count();
        main.orderBy(new Plan.Ordering(List.of(
                new Plan.SortKey(5, true), new Plan.SortKey(0, false),
                new Plan.SortKey(1, false), new Plan.SortKey(2, false)), 100));

        return new Composite(
                List.of(new Stage(inventory, "q72_inventory")),
                main,
                List.of(new DictRef(0, 4, 1), new DictRef(1, 3, 1)));
    }

    public static Composite query44()
    {
        // Q44: one store's best and worst performing items by rank. Items at store four whose average net profit
        // beats 90% of the store's null-address baseline average rank by that average ascending and descending; the
        // two top-ten rankings join on the rank position and each side's product name attaches, ordered by rank and
        // names, top 100. Each side assembles its own aggregates (no reuse): per-item profit aggregate, the
        // single-row baseline broadcast by a cross join, the cross-multiplied threshold deriving the rounded
        // averages, and a top-ten rank window.
        List<Stage> stages = new ArrayList<>();
        for (String side : List.of("asc", "desc")) {
            boolean descending = side.equals("desc");

            QueryLowering items = QueryLowering.scan("store_sales",
                    new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                    new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                    new QueryLowering.Column("ss_net_profit", ColumnEncoding.FLAT, true));
            items.where(new Plan.Predicate("=", items.column("ss_store_sk"), new Plan.Lit(4)))
                    .groupBy("ss_item_sk")
                    .aggregate("sum", "ss_net_profit")
                    .aggregate("count", "ss_net_profit");
            items.having(new Plan.Predicate(">", new Plan.Col(2), new Plan.Lit(0)));
            stages.add(new Stage(items, "q44_items_" + side));

            QueryLowering baseline = QueryLowering.scan("store_sales",
                    new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                    new QueryLowering.Column("ss_addr_sk", ColumnEncoding.FLAT, true),
                    new QueryLowering.Column("ss_net_profit", ColumnEncoding.FLAT, true));
            baseline.where(
                            new Plan.Predicate("=", baseline.column("ss_store_sk"), new Plan.Lit(4)),
                            new Plan.IsNull(baseline.position("ss_addr_sk"), false))
                    .groupBy("ss_store_sk")
                    .aggregate("sum", "ss_net_profit")
                    .aggregate("count", "ss_net_profit");
            baseline.having(new Plan.Predicate(">", new Plan.Col(2), new Plan.Lit(0)))
                    .select(new Plan.Col(1), new Plan.Col(2));
            stages.add(new Stage(baseline, "q44_baseline_" + side));

            // Combined: items(0-2: item, sum, count), baseline(3-4: sum, count). Keep items whose rounded average
            // beats 90% of the baseline average (cross-multiplied), carrying (item, average) to the rank window.
            QueryLowering averages = QueryLowering.scan("q44_items_" + side,
                            new QueryLowering.Column("i_item", ColumnEncoding.FLAT, false),
                            new QueryLowering.Column("i_sum", ColumnEncoding.FLAT, true),
                            new QueryLowering.Column("i_count", ColumnEncoding.FLAT, false))
                    .crossJoin("q44_baseline_" + side,
                            new QueryLowering.Column("b_sum", ColumnEncoding.FLAT, true),
                            new QueryLowering.Column("b_count", ColumnEncoding.FLAT, false));
            averages.where(new Plan.Predicate(">",
                            new Plan.Bin("*", new Plan.Call("divide_round_i64", new Plan.Col(1), new Plan.Col(2)), new Plan.Lit(10)),
                            new Plan.Bin("*", new Plan.Call("divide_round_i64", new Plan.Col(3), new Plan.Col(4)), new Plan.Lit(9))))
                    .select(new Plan.Col(0),
                            new Plan.Call("divide_round_i64", new Plan.Col(1), new Plan.Col(2)));
            stages.add(new Stage(averages, "q44_averages_" + side));

            QueryLowering ranked = QueryLowering.scan("q44_averages_" + side,
                    new QueryLowering.Column("a_item_" + side, ColumnEncoding.FLAT, false),
                    new QueryLowering.Column("a_average_" + side, ColumnEncoding.FLAT, true));
            ranked.window(new Plan.Window(new int[0], List.of(new Plan.SortKey(1, descending)), Plan.RankFunction.RANK, 10));
            ranked.select(new Plan.Col(0), new Plan.Col(2));
            stages.add(new Stage(ranked, "q44_ranked_" + side));
        }

        // Combined main: asc(0-1: item, rank), desc(2-3), item names for each side (4-5, 6-7).
        QueryLowering main = QueryLowering.scan("q44_ranked_asc",
                        new QueryLowering.Column("best_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("best_rank", ColumnEncoding.FLAT, false))
                .join("q44_ranked_desc", "best_rank", "worst_rank",
                        new QueryLowering.Column("worst_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("worst_rank", ColumnEncoding.FLAT, false))
                .join("item", "best_item", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_product_name", ColumnEncoding.STRING, true))
                .join("item", "worst_item", "i_item_sk_2",
                        new QueryLowering.Column("i_item_sk_2", "i_item_sk", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("i_product_name_2", "i_product_name", ColumnEncoding.STRING, true));
        main.select(new Plan.Col(1), new Plan.Col(5), new Plan.Col(7))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)), 100));

        return new Composite(stages, main,
                List.of(new DictRef(1, 2, 1), new DictRef(2, 3, 1)));
    }

    public static Composite query28()
    {
        // Q28: six bucket statistics in one row -- for each store-sales quantity band (each with its own
        // list-price/coupon/wholesale OR-band filter), the average, count, and distinct count of the list price.
        // Each bucket lowers to the Q94 count-distinct pair: ROLLUP({list_price}, {}) carrying sum and count, then a
        // stage summing CASEs on the grouping id (per-price rows count distinct values, the grand row carries the
        // totals) and deriving the rounded average. The six bucket values broadcast through chained cross joins.
        record Bucket(long minimumQuantity, long maximumQuantity, long minimumListPrice, long maximumListPrice,
                long minimumCoupon, long maximumCoupon, long minimumWholesale, long maximumWholesale) {}

        List<Bucket> buckets = List.of(
                new Bucket(0, 5, 8_00L, 18_00L, 459_00L, 1459_00L, 57_00L, 77_00L),
                new Bucket(6, 10, 90_00L, 100_00L, 2323_00L, 3323_00L, 31_00L, 51_00L),
                new Bucket(11, 15, 142_00L, 152_00L, 12214_00L, 13214_00L, 79_00L, 99_00L),
                new Bucket(16, 20, 135_00L, 145_00L, 6071_00L, 7071_00L, 38_00L, 58_00L),
                new Bucket(21, 25, 122_00L, 132_00L, 836_00L, 1836_00L, 17_00L, 37_00L),
                new Bucket(26, 30, 154_00L, 164_00L, 7326_00L, 8326_00L, 7_00L, 27_00L));

        List<Stage> stages = new ArrayList<>();
        QueryLowering main = null;
        List<Plan.Expr> outputs = new ArrayList<>();
        for (int b = 0; b < buckets.size(); b++) {
            Bucket bucket = buckets.get(b);

            QueryLowering prices = QueryLowering.scan("store_sales",
                    new QueryLowering.Column("ss_quantity", ColumnEncoding.FLAT, true),
                    new QueryLowering.Column("ss_list_price", ColumnEncoding.FLAT, true),
                    new QueryLowering.Column("ss_coupon_amt", ColumnEncoding.FLAT, true),
                    new QueryLowering.Column("ss_wholesale_cost", ColumnEncoding.FLAT, true));
            prices.where(
                            new Plan.Predicate(">=", prices.column("ss_quantity"), new Plan.Lit(bucket.minimumQuantity())),
                            new Plan.Predicate("<=", prices.column("ss_quantity"), new Plan.Lit(bucket.maximumQuantity())),
                            new Plan.Or(List.of(
                                    new Plan.And(List.of(
                                            new Plan.Predicate(">=", prices.column("ss_list_price"), new Plan.Lit(bucket.minimumListPrice())),
                                            new Plan.Predicate("<=", prices.column("ss_list_price"), new Plan.Lit(bucket.maximumListPrice())))),
                                    new Plan.And(List.of(
                                            new Plan.Predicate(">=", prices.column("ss_coupon_amt"), new Plan.Lit(bucket.minimumCoupon())),
                                            new Plan.Predicate("<=", prices.column("ss_coupon_amt"), new Plan.Lit(bucket.maximumCoupon())))),
                                    new Plan.And(List.of(
                                            new Plan.Predicate(">=", prices.column("ss_wholesale_cost"), new Plan.Lit(bucket.minimumWholesale())),
                                            new Plan.Predicate("<=", prices.column("ss_wholesale_cost"), new Plan.Lit(bucket.maximumWholesale())))))),
                            new Plan.IsNull(prices.position("ss_list_price"), true))
                    .groupBy("ss_list_price")
                    .groupingSets(List.of(new int[] {0}, new int[0]))
                    .aggregate("sum", "ss_list_price")
                    .aggregate("count", "ss_list_price");
            // Rollup output: list_price(0), sum(1), count(2), grouping_id(3): 0 per price, 1 for the grand total.
            stages.add(new Stage(prices, "q28_rollup_" + b));

            QueryLowering value = QueryLowering.scan("q28_rollup_" + b,
                    new QueryLowering.Column("r_price_" + b, ColumnEncoding.FLAT, true),
                    new QueryLowering.Column("r_sum_" + b, ColumnEncoding.FLAT, true),
                    // count() materializes without a null mask -- declare it non-nullable (the Q22 trap).
                    new QueryLowering.Column("r_count_" + b, ColumnEncoding.FLAT, false),
                    new QueryLowering.Column("r_grouping_" + b, ColumnEncoding.FLAT, false));
            Plan.Condition perPrice = new Plan.Predicate("=", value.column("r_grouping_" + b), new Plan.Lit(0));
            Plan.Condition grandTotal = new Plan.Predicate("=", value.column("r_grouping_" + b), new Plan.Lit(1));
            value.aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(perPrice, new Plan.Lit(1))), new Plan.Lit(0)))
                    .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(grandTotal, value.column("r_sum_" + b))), new Plan.Lit(0)))
                    .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(grandTotal, value.column("r_count_" + b))), new Plan.Lit(0)));
            // Bucket value: (average, count, distinct count) -- the harness's per-bucket projection order.
            value.select(
                    new Plan.Call("divide_round_i64", new Plan.Col(1), new Plan.Col(2)),
                    new Plan.Col(2),
                    new Plan.Col(0));
            stages.add(new Stage(value, "q28_bucket_" + b));

            if (main == null) {
                main = QueryLowering.scan("q28_bucket_" + b,
                        new QueryLowering.Column("avg_" + b, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("count_" + b, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("distinct_" + b, ColumnEncoding.FLAT, true));
            }
            else {
                main = main.crossJoin("q28_bucket_" + b,
                        new QueryLowering.Column("avg_" + b, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("count_" + b, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("distinct_" + b, ColumnEncoding.FLAT, true));
            }
            outputs.add(new Plan.Col(3 * b));
            outputs.add(new Plan.Col(3 * b + 1));
            outputs.add(new Plan.Col(3 * b + 2));
        }
        main.select(outputs.toArray(Plan.Expr[]::new));

        return new Composite(stages, main, List.of());
    }

    public static Composite query30()
    {
        // Q30: Georgia customers whose 2002 web-return total exceeds 1.2x their state's average customer return, with
        // the full customer identity. Per-(customer, state) return totals group over web returns joined to the year
        // and the return address; the state average divides that relation's total by its customer count (the totals
        // relation is assembled a second time -- no reuse); the main joins totals to averages on the state, attaches
        // the customer and their current Georgia address, applies the cross-multiplied threshold, and orders by all
        // thirteen outputs, top 100. A null returning customer is a real group whose total counts toward the state
        // average; a null state rides through grouping but never joins.
        QueryLowering averages = QueryLowering.scan("q30_returns_for_average",
                        new QueryLowering.Column("a_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("a_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("a_total", ColumnEncoding.FLAT, true));
        averages.where(new Plan.IsNull(2, true))
                .groupBy("a_state")
                .aggregate("sum", "a_total")
                .aggregate("count", "a_total");
        averages.select(new Plan.Col(0), new Plan.Call("divide_round_i64", new Plan.Col(1), new Plan.Col(2)));

        // Combined main columns: totals(0-2: customer, state, total), averages(3-4: state, average),
        // customer(5-18), address(19-20).
        QueryLowering main = QueryLowering.scan("q30_returns",
                        new QueryLowering.Column("m_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("m_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("m_total", ColumnEncoding.FLAT, true))
                .join("q30_averages", "m_state", "v_state",
                        new QueryLowering.Column("v_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("v_average", ColumnEncoding.FLAT, true))
                .join("customer", "m_customer", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_customer_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("c_salutation", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_preferred_cust_flag", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_birth_day", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_birth_month", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_birth_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_birth_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_login", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_email_address", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_last_review_date_sk", ColumnEncoding.FLAT, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, true));
        main.where(
                        new Plan.StringMatch(main.position("ca_state"), List.of("GA"), false),
                        new Plan.IsNull(2, true),
                        new Plan.IsNull(4, true),
                        new Plan.Predicate(">",
                                new Plan.Bin("*", new Plan.Col(2), new Plan.Lit(10)),
                                new Plan.Bin("*", new Plan.Col(4), new Plan.Lit(12))))
                .select(new Plan.Col(7), new Plan.Col(8), new Plan.Col(9), new Plan.Col(10), new Plan.Col(11),
                        new Plan.Col(12), new Plan.Col(13), new Plan.Col(14), new Plan.Col(15), new Plan.Col(16),
                        new Plan.Col(17), new Plan.Col(18), new Plan.Col(2))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false),
                        new Plan.SortKey(3, false), new Plan.SortKey(4, false), new Plan.SortKey(5, false),
                        new Plan.SortKey(6, false), new Plan.SortKey(7, false), new Plan.SortKey(8, false),
                        new Plan.SortKey(9, false), new Plan.SortKey(10, false), new Plan.SortKey(11, false),
                        new Plan.SortKey(12, false)), 100));

        return new Composite(
                List.of(new Stage(query30CustomerTotalReturn(), "q30_returns", List.of(new DictRef(1, 2, 1))),
                        new Stage(query30CustomerTotalReturn(), "q30_returns_for_average", List.of(new DictRef(1, 2, 1))),
                        new Stage(averages, "q30_averages", List.of(new DictRef(0, 0, 1)))),
                main,
                List.of(new DictRef(0, 2, 2), new DictRef(1, 2, 3), new DictRef(2, 2, 4), new DictRef(3, 2, 5),
                        new DictRef(4, 2, 6), new DictRef(8, 2, 10), new DictRef(9, 2, 11), new DictRef(10, 2, 12)));
    }

    public static Composite query81()
    {
        // Q81: Q30's catalog sibling -- Georgia customers whose 2000 catalog-return total (including tax) exceeds
        // 1.2x their state's average customer return, with the customer's name and full current address. Same
        // totals/averages/threshold tree over catalog returns; the sixteen-column output carries the address book's
        // street, city, county, zip, country, GMT offset, and location type, ordered across all of it, top 100.
        QueryLowering averages = QueryLowering.scan("q81_returns_for_average",
                        new QueryLowering.Column("a_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("a_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("a_total", ColumnEncoding.FLAT, true));
        averages.where(new Plan.IsNull(2, true))
                .groupBy("a_state")
                .aggregate("sum", "a_total")
                .aggregate("count", "a_total");
        averages.select(new Plan.Col(0), new Plan.Call("divide_round_i64", new Plan.Col(1), new Plan.Col(2)));

        // Combined main columns: totals(0-2), averages(3-4), customer(5-10: sk, addr, id, salutation, first, last),
        // address(11-22: sk, state, street number/name/type, suite, city, county, zip, country, gmt, location type).
        QueryLowering main = QueryLowering.scan("q81_returns",
                        new QueryLowering.Column("m_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("m_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("m_total", ColumnEncoding.FLAT, true))
                .join("q81_averages", "m_state", "v_state",
                        new QueryLowering.Column("v_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("v_average", ColumnEncoding.FLAT, true))
                .join("customer", "m_customer", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_customer_id", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("c_salutation", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_first_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("c_last_name", ColumnEncoding.STRING, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_street_number", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_street_name", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_street_type", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_suite_number", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_city", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_zip", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_country", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("ca_gmt_offset", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ca_location_type", ColumnEncoding.STRING, true));
        main.where(
                        new Plan.StringMatch(main.position("ca_state"), List.of("GA"), false),
                        new Plan.IsNull(2, true),
                        new Plan.IsNull(4, true),
                        new Plan.Predicate(">",
                                new Plan.Bin("*", new Plan.Col(2), new Plan.Lit(10)),
                                new Plan.Bin("*", new Plan.Col(4), new Plan.Lit(12))))
                .select(new Plan.Col(7), new Plan.Col(8), new Plan.Col(9), new Plan.Col(10),
                        new Plan.Col(13), new Plan.Col(14), new Plan.Col(15), new Plan.Col(16), new Plan.Col(17),
                        new Plan.Col(18), new Plan.Col(12), new Plan.Col(19), new Plan.Col(20), new Plan.Col(21),
                        new Plan.Col(22), new Plan.Col(2))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false),
                        new Plan.SortKey(3, false), new Plan.SortKey(4, false), new Plan.SortKey(5, false),
                        new Plan.SortKey(6, false), new Plan.SortKey(7, false), new Plan.SortKey(8, false),
                        new Plan.SortKey(9, false), new Plan.SortKey(10, false), new Plan.SortKey(11, false),
                        new Plan.SortKey(12, false), new Plan.SortKey(13, false), new Plan.SortKey(14, false),
                        new Plan.SortKey(15, false)), 100));

        return new Composite(
                List.of(new Stage(query81CustomerTotalReturn(), "q81_returns", List.of(new DictRef(1, 2, 1))),
                        new Stage(query81CustomerTotalReturn(), "q81_returns_for_average", List.of(new DictRef(1, 2, 1))),
                        new Stage(averages, "q81_averages", List.of(new DictRef(0, 0, 1)))),
                main,
                List.of(new DictRef(0, 2, 2), new DictRef(1, 2, 3), new DictRef(2, 2, 4), new DictRef(3, 2, 5),
                        new DictRef(4, 3, 2), new DictRef(5, 3, 3), new DictRef(6, 3, 4), new DictRef(7, 3, 5),
                        new DictRef(8, 3, 6), new DictRef(9, 3, 7), new DictRef(10, 3, 1), new DictRef(11, 3, 8),
                        new DictRef(12, 3, 9), new DictRef(14, 3, 11)));
    }

    /** 2000 catalog-return totals (including tax) per (returning customer, return-address state). */
    private static QueryLowering query81CustomerTotalReturn()
    {
        QueryLowering totals = QueryLowering.scan("catalog_returns",
                        new QueryLowering.Column("cr_returning_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cr_returned_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cr_returning_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cr_return_amt_inc_tax", ColumnEncoding.FLAT, true))
                .join("date_dim", "cr_returned_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true))
                .join("customer_address", "cr_returning_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, true));
        totals.where(new Plan.Predicate("=", totals.column("d_year"), new Plan.Lit(2000)))
                .groupBy("cr_returning_customer_sk", "ca_state")
                .aggregate("sum", "cr_return_amt_inc_tax");
        return totals;
    }

    /** 2002 web-return totals per (returning customer, return-address state); both keys may be null. */
    private static QueryLowering query30CustomerTotalReturn()
    {
        QueryLowering totals = QueryLowering.scan("web_returns",
                        new QueryLowering.Column("wr_returning_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_returned_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_returning_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("wr_return_amt", ColumnEncoding.FLAT, true))
                .join("date_dim", "wr_returned_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true))
                .join("customer_address", "wr_returning_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, true));
        totals.where(new Plan.Predicate("=", totals.column("d_year"), new Plan.Lit(2002)))
                .groupBy("wr_returning_customer_sk", "ca_state")
                .aggregate("sum", "wr_return_amt");
        return totals;
    }

    public static Composite query61()
    {
        // Q61: the share of November 1998 Jewelry revenue in one GMT zone sold through a promotion channel (direct
        // mail, email, or TV). Promotional and total revenue are each a global sum over the same store-sales star
        // (store, date, customer, current address, item; the promotional side adds the promotion join); the two
        // single-row aggregates are cross-joined (1x1) and the percentage is a scaled rounding divide. Q90's
        // scalar-broadcast shape with sum subqueries.
        QueryLowering main = QueryLowering.scan("q61_promotional",
                        new QueryLowering.Column("promotional", ColumnEncoding.FLAT, true))
                .crossJoin("q61_total", new QueryLowering.Column("total", ColumnEncoding.FLAT, true));
        main.select(new Plan.Col(0), new Plan.Col(1),
                        new Plan.Call("divide_scale_round_i64", new Plan.Col(0), new Plan.Col(1), new Plan.Lit(100_000_000_000_000L)))
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100));

        return new Composite(
                List.of(new Stage(query61Sales(true), "q61_promotional"),
                        new Stage(query61Sales(false), "q61_total")),
                main,
                List.of());
    }

    /** Global Jewelry revenue for November 1998 in the -5.00 GMT zone, optionally restricted to promoted sales. */
    private static QueryLowering query61Sales(boolean promotionalOnly)
    {
        QueryLowering sales = promotionalOnly
                ? QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_promo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true))
                : QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true));
        sales = sales.join("store", "ss_store_sk", "s_store_sk",
                new QueryLowering.Column("s_store_sk"),
                new QueryLowering.Column("s_gmt_offset", ColumnEncoding.FLAT, true));
        if (promotionalOnly) {
            sales = sales.join("promotion", "ss_promo_sk", "p_promo_sk",
                    new QueryLowering.Column("p_promo_sk"),
                    new QueryLowering.Column("p_channel_dmail", ColumnEncoding.STRING, true),
                    new QueryLowering.Column("p_channel_email", ColumnEncoding.STRING, true),
                    new QueryLowering.Column("p_channel_tv", ColumnEncoding.STRING, true));
        }
        sales = sales.join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_moy", ColumnEncoding.FLAT, true))
                .join("customer", "ss_customer_sk", "c_customer_sk",
                        new QueryLowering.Column("c_customer_sk"),
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_gmt_offset", ColumnEncoding.FLAT, true))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true));
        List<Plan.Condition> conditions = new ArrayList<>();
        conditions.add(new Plan.Predicate("=", sales.column("s_gmt_offset"), new Plan.Lit(-500)));
        if (promotionalOnly) {
            conditions.add(new Plan.Or(List.of(
                    new Plan.StringMatch(sales.position("p_channel_dmail"), List.of("Y"), false),
                    new Plan.StringMatch(sales.position("p_channel_email"), List.of("Y"), false),
                    new Plan.StringMatch(sales.position("p_channel_tv"), List.of("Y"), false))));
        }
        conditions.add(new Plan.Predicate("=", sales.column("d_year"), new Plan.Lit(1998)));
        conditions.add(new Plan.Predicate("=", sales.column("d_moy"), new Plan.Lit(11)));
        conditions.add(new Plan.Predicate("=", sales.column("ca_gmt_offset"), new Plan.Lit(-500)));
        conditions.add(new Plan.StringMatch(sales.position("i_category"), List.of("Jewelry"), false));
        sales.where(conditions.toArray(Plan.Condition[]::new))
                .aggregate("sum", "ss_ext_sales_price");
        return sales;
    }

    public static Composite query90()
    {
        // Q90: ratio of web-sale counts in a morning vs an evening time window, for one household profile and web-page
        // size band. Two global-count subqueries (each a web_sales star joined to time_dim/household/web_page) are
        // cross-joined (1x1) and divided -- a scalar-broadcast nested-loop join.
        QueryLowering main = QueryLowering.scan("q90_morning",
                        new QueryLowering.Column("morning_count", ColumnEncoding.FLAT, false))
                .crossJoin("q90_evening", new QueryLowering.Column("evening_count", ColumnEncoding.FLAT, false));
        main.select(new Plan.Call("divide_i64_to_f64", new Plan.Col(0), new Plan.Col(1)));

        return new Composite(
                List.of(new Stage(query90Count(7, 10), "q90_morning"),
                        new Stage(query90Count(18, 21), "q90_evening")),
                main,
                List.of());
    }

    private static QueryLowering query90Count(long hourLowExclusive, long hourHighExclusive)
    {
        QueryLowering count = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_sold_time_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_ship_hdemo_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_web_page_sk", ColumnEncoding.FLAT, true))
                .join("time_dim", "ws_sold_time_sk", "t_time_sk",
                        new QueryLowering.Column("t_time_sk"),
                        new QueryLowering.Column("t_hour", ColumnEncoding.FLAT, true))
                .join("household_demographics", "ws_ship_hdemo_sk", "hd_demo_sk",
                        new QueryLowering.Column("hd_demo_sk"),
                        new QueryLowering.Column("hd_dep_count", ColumnEncoding.FLAT, true))
                .join("web_page", "ws_web_page_sk", "wp_web_page_sk",
                        new QueryLowering.Column("wp_web_page_sk"),
                        new QueryLowering.Column("wp_char_count", ColumnEncoding.FLAT, true));
        count.where(
                        new Plan.Predicate(">", count.column("t_hour"), new Plan.Lit(hourLowExclusive)),
                        new Plan.Predicate("<", count.column("t_hour"), new Plan.Lit(hourHighExclusive)),
                        new Plan.Predicate("=", count.column("hd_dep_count"), new Plan.Lit(6)),
                        new Plan.Predicate(">", count.column("wp_char_count"), new Plan.Lit(4999)),
                        new Plan.Predicate("<", count.column("wp_char_count"), new Plan.Lit(5201)))
                .count();
        return count;
    }

    public static UnionComposite query10()
    {
        // Q10: demographic breakdown of customers in a county set who bought from the store channel AND from the web or
        // catalog channel in a month window. The store customers and the web-or-catalog customers (a union) are the two
        // EXISTS builds; the customer base is filtered to the counties and semi-joined to both, then joined to its
        // demographics and counted per demographic profile. Top 100 ordered by the eight demographic columns.
        List<QueryLowering> otherBranches = List.of(
                query10Customers("web_sales", "ws_bill_customer_sk", "ws_sold_date_sk"),
                query10Customers("catalog_sales", "cs_ship_customer_sk", "cs_sold_date_sk"));

        QueryLowering main = QueryLowering.scan("customer",
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_current_cdemo_sk", ColumnEncoding.FLAT, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_county", ColumnEncoding.STRING, true))
                .semiJoin("q10_store", "c_customer_sk", "qs_customer",
                        new QueryLowering.Column("qs_customer", ColumnEncoding.FLAT, false))
                .semiJoin("q10_other", "c_customer_sk", "qo_customer",
                        new QueryLowering.Column("qo_customer", ColumnEncoding.FLAT, false))
                .join("customer_demographics", "c_current_cdemo_sk", "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_gender", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_education_status", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_purchase_estimate", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cd_credit_rating", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_dep_count", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cd_dep_employed_count", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cd_dep_college_count", ColumnEncoding.FLAT, true));
        main.where(new Plan.StringMatch(main.position("ca_county"),
                        List.of("Rush County", "Toole County", "Jefferson County", "Dona Ana County", "La Porte County"), false))
                .groupBy("cd_gender", "cd_marital_status", "cd_education_status", "cd_purchase_estimate",
                        "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count")
                .count();
        // result: 8 demographic keys (0..7), count (8). ORDER BY runs pre-projection on the eight keys (the harness
        // sorts its reordered output on exactly these columns); then SELECT interleaves the count after each key.
        main.orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false),
                        new Plan.SortKey(3, false), new Plan.SortKey(4, false), new Plan.SortKey(5, false),
                        new Plan.SortKey(6, false), new Plan.SortKey(7, false)), 100))
                .select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(8),
                        new Plan.Col(3), new Plan.Col(8), new Plan.Col(4), new Plan.Col(8),
                        new Plan.Col(5), new Plan.Col(8), new Plan.Col(6), new Plan.Col(8),
                        new Plan.Col(7), new Plan.Col(8));

        return new UnionComposite(otherBranches, "q10_other", List.of(),
                List.of(new Stage(query10Customers("store_sales", "ss_customer_sk", "ss_sold_date_sk"), "q10_store")),
                main,
                List.of(new DictRef(0, 4, 1), new DictRef(1, 4, 2), new DictRef(2, 4, 3), new DictRef(6, 4, 5)));
    }

    public static UnionComposite query69()
    {
        // Q69: demographic breakdown of customers in a state set who bought from the store channel but NOT from web or
        // catalog in a month window. Q10's sibling -- store customers are an EXISTS (semi) build, the web-or-catalog
        // customers (a union) a NOT EXISTS (anti) build; the customer base is filtered to the states, semi-joined to the
        // store set, anti-joined to the excluded set, joined to demographics, and counted per demographic profile.
        List<QueryLowering> excludedBranches = List.of(
                query69Customers("web_sales", "ws_bill_customer_sk", "ws_sold_date_sk"),
                query69Customers("catalog_sales", "cs_ship_customer_sk", "cs_sold_date_sk"));

        QueryLowering main = QueryLowering.scan("customer",
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_current_cdemo_sk", ColumnEncoding.FLAT, true))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, true))
                .semiJoin("q69_store", "c_customer_sk", "qs_customer",
                        new QueryLowering.Column("qs_customer", ColumnEncoding.FLAT, false))
                .antiJoin("q69_excluded", "c_customer_sk", "qe_customer",
                        new QueryLowering.Column("qe_customer", ColumnEncoding.FLAT, false))
                .join("customer_demographics", "c_current_cdemo_sk", "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_gender", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_education_status", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_purchase_estimate", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cd_credit_rating", ColumnEncoding.STRING, true));
        main.where(new Plan.StringMatch(main.position("ca_state"), List.of("KY", "GA", "NM"), false))
                .groupBy("cd_gender", "cd_marital_status", "cd_education_status", "cd_purchase_estimate", "cd_credit_rating")
                .count();
        // result: gender(0), marital(1), education(2), purchase_estimate(3), credit_rating(4), count(5). ORDER BY runs
        // pre-projection on the five group keys; SELECT then repeats the count after each of the three column groups.
        main.orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false),
                        new Plan.SortKey(3, false), new Plan.SortKey(4, false)), 100))
                .select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(5),
                        new Plan.Col(3), new Plan.Col(5), new Plan.Col(4), new Plan.Col(5));

        return new UnionComposite(excludedBranches, "q69_excluded", List.of(),
                List.of(new Stage(query69Customers("store_sales", "ss_customer_sk", "ss_sold_date_sk"), "q69_store")),
                main,
                List.of(new DictRef(0, 4, 1), new DictRef(1, 4, 2), new DictRef(2, 4, 3), new DictRef(6, 4, 5)));
    }

    private static QueryLowering query69Customers(String fact, String customerColumn, String dateColumn)
    {
        QueryLowering customers = QueryLowering.scan(fact,
                        new QueryLowering.Column(customerColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(dateColumn, ColumnEncoding.FLAT, true))
                .join("date_dim", dateColumn, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_moy", ColumnEncoding.FLAT, true));
        customers.where(
                        new Plan.Predicate("=", customers.column("d_year"), new Plan.Lit(2001)),
                        new Plan.Predicate(">", customers.column("d_moy"), new Plan.Lit(3)),
                        new Plan.Predicate("<", customers.column("d_moy"), new Plan.Lit(7)))
                .select(customers.column(customerColumn));
        return customers;
    }

    private static QueryLowering query10Customers(String fact, String customerColumn, String dateColumn)
    {
        QueryLowering customers = QueryLowering.scan(fact,
                        new QueryLowering.Column(customerColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(dateColumn, ColumnEncoding.FLAT, true))
                .join("date_dim", dateColumn, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_moy", ColumnEncoding.FLAT, true));
        customers.where(
                        new Plan.Predicate("=", customers.column("d_year"), new Plan.Lit(2002)),
                        new Plan.Predicate(">", customers.column("d_moy"), new Plan.Lit(0)),
                        new Plan.Predicate("<", customers.column("d_moy"), new Plan.Lit(5)))
                .select(customers.column(customerColumn));
        return customers;
    }

    public static UnionComposite query35()
    {
        // Q35: like Q10 but per (state + demographic profile) and with count/min/max/avg of the three dependent counts.
        // Customers who bought from the store channel AND from web or catalog in a year-quarter window, joined to state
        // and demographics, grouped, top 100 ordered by the six grouping columns.
        List<QueryLowering> otherBranches = List.of(
                query35Customers("web_sales", "ws_bill_customer_sk", "ws_sold_date_sk"),
                query35Customers("catalog_sales", "cs_ship_customer_sk", "cs_sold_date_sk"));

        QueryLowering main = QueryLowering.scan("customer",
                        new QueryLowering.Column("c_current_addr_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_customer_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_current_cdemo_sk", ColumnEncoding.FLAT, true))
                .semiJoin("q35_store", "c_customer_sk", "qs_customer",
                        new QueryLowering.Column("qs_customer", ColumnEncoding.FLAT, false))
                .semiJoin("q35_other", "c_customer_sk", "qo_customer",
                        new QueryLowering.Column("qo_customer", ColumnEncoding.FLAT, false))
                .join("customer_address", "c_current_addr_sk", "ca_address_sk",
                        new QueryLowering.Column("ca_address_sk"),
                        new QueryLowering.Column("ca_state", ColumnEncoding.STRING, true))
                .join("customer_demographics", "c_current_cdemo_sk", "cd_demo_sk",
                        new QueryLowering.Column("cd_demo_sk"),
                        new QueryLowering.Column("cd_gender", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_marital_status", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_education_status", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_purchase_estimate", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cd_credit_rating", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("cd_dep_count", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cd_dep_employed_count", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("cd_dep_college_count", ColumnEncoding.FLAT, true));
        main.groupBy("ca_state", "cd_gender", "cd_marital_status", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count")
                .count()
                .aggregate("min", "cd_dep_count").aggregate("max", "cd_dep_count").aggregate("avg", "cd_dep_count")
                .count()
                .aggregate("min", "cd_dep_employed_count").aggregate("max", "cd_dep_employed_count").aggregate("avg", "cd_dep_employed_count")
                .count()
                .aggregate("min", "cd_dep_college_count").aggregate("max", "cd_dep_college_count").aggregate("avg", "cd_dep_college_count");
        // result: 6 keys (0..5), then count/min/max/avg per dep column (6..17). ORDER BY pre-projection on the 6 keys.
        main.orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false),
                        new Plan.SortKey(3, false), new Plan.SortKey(4, false), new Plan.SortKey(5, false)), 100))
                .select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3),
                        new Plan.Col(6), new Plan.Col(7), new Plan.Col(8), new Plan.Col(9),
                        new Plan.Col(4), new Plan.Col(10), new Plan.Col(11), new Plan.Col(12), new Plan.Col(13),
                        new Plan.Col(5), new Plan.Col(14), new Plan.Col(15), new Plan.Col(16), new Plan.Col(17));

        return new UnionComposite(otherBranches, "q35_other", List.of(),
                List.of(new Stage(query35Customers("store_sales", "ss_customer_sk", "ss_sold_date_sk"), "q35_store")),
                main,
                List.of(new DictRef(0, 3, 1), new DictRef(1, 4, 1), new DictRef(2, 4, 2)));
    }

    private static QueryLowering query35Customers(String fact, String customerColumn, String dateColumn)
    {
        QueryLowering customers = QueryLowering.scan(fact,
                        new QueryLowering.Column(customerColumn, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(dateColumn, ColumnEncoding.FLAT, true))
                .join("date_dim", dateColumn, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_qoy", ColumnEncoding.FLAT, true));
        customers.where(
                        new Plan.Predicate("=", customers.column("d_year"), new Plan.Lit(2002)),
                        new Plan.Predicate(">", customers.column("d_qoy"), new Plan.Lit(0)),
                        new Plan.Predicate("<", customers.column("d_qoy"), new Plan.Lit(4)))
                .select(customers.column(customerColumn));
        return customers;
    }

    public static Composite query39()
    {
        // Q39: inventory items whose monthly quantity-on-hand is volatile (coefficient of variation > 1) in two
        // consecutive months, joined month-to-month. Each month's variation is a stage: inventory ⋈ item ⋈ warehouse ⋈
        // date_dim(2001, that month), grouped by (warehouse, item, month) with count / sample stddev / average of
        // quantity-on-hand, kept where count > 1 AND avg > 0 AND stddev/avg > 1 (the cov threshold, as the harness's
        // exact divide_f64 form), emitting (warehouse, item, month, mean, cov). The two months self-join on
        // (warehouse, item); the (unlimited) output is ordered by both months' (month, mean, cov). The mean and cov are
        // DOUBLE, carried across the stage boundary as raw bits and named back via reinterpret_f64 in the final select.
        QueryLowering january = query39InventoryVariation(1);
        QueryLowering february = query39InventoryVariation(2);

        QueryLowering main = QueryLowering.scan("q39_january",
                        new QueryLowering.Column("j_warehouse", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("j_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("j_month", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("j_mean", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("j_cov", ColumnEncoding.FLAT, false))
                .join("q39_february",
                        new String[] {"j_warehouse", "j_item"},
                        new String[] {"f_warehouse", "f_item"},
                        new QueryLowering.Column("f_warehouse", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("f_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("f_month", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("f_mean", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("f_cov", ColumnEncoding.FLAT, false));
        // after the join: j {warehouse(0), item(1), month(2), mean(3), cov(4)}, f {warehouse(5), item(6), month(7), mean(8), cov(9)}.
        main.orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false),
                        new Plan.SortKey(3, false), new Plan.SortKey(4, false),
                        new Plan.SortKey(7, false), new Plan.SortKey(8, false), new Plan.SortKey(9, false)), -1))
                .select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2),
                        new Plan.Call("reinterpret_f64", new Plan.Col(3)), new Plan.Call("reinterpret_f64", new Plan.Col(4)),
                        new Plan.Col(5), new Plan.Col(6), new Plan.Col(7),
                        new Plan.Call("reinterpret_f64", new Plan.Col(8)), new Plan.Call("reinterpret_f64", new Plan.Col(9)));

        return new Composite(
                List.of(new Stage(january, "q39_january"), new Stage(february, "q39_february")),
                main,
                List.of());
    }

    private static QueryLowering query39InventoryVariation(int month)
    {
        QueryLowering variation = QueryLowering.scan("inventory",
                        new QueryLowering.Column("inv_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_warehouse_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("inv_quantity_on_hand", ColumnEncoding.FLAT, true))
                .join("item", "inv_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"))
                .join("warehouse", "inv_warehouse_sk", "w_warehouse_sk",
                        new QueryLowering.Column("w_warehouse_sk"))
                .join("date_dim", "inv_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("d_moy", ColumnEncoding.FLAT, true));
        variation.where(
                        new Plan.Predicate("=", variation.column("d_year"), new Plan.Lit(2001)),
                        new Plan.Predicate("=", variation.column("d_moy"), new Plan.Lit(month)))
                .groupBy("w_warehouse_sk", "inv_item_sk", "d_moy")
                .aggregate("count", "inv_quantity_on_hand")
                .aggregate("stddev", "inv_quantity_on_hand")
                .aggregate("avg", "inv_quantity_on_hand");
        // grouped result: warehouse(0), item(1), month(2), count(3), stddev(4), avg(5).
        variation.having(new Plan.And(List.of(
                        new Plan.Predicate(">", new Plan.Col(3), new Plan.Lit(1)),
                        new Plan.Predicate(">", new Plan.Col(5), new Plan.Lit(0)),
                        new Plan.Predicate(">", new Plan.Call("divide_f64", new Plan.Col(4), new Plan.Col(5)), new Plan.Lit(1)))));
        variation.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2),
                new Plan.Col(5), new Plan.Call("divide_f64", new Plan.Col(4), new Plan.Col(5)));
        return variation;
    }

    public static Composite query78()
    {
        // Q78: for each (year, item, customer), the ratio of store-channel quantity to the combined web+catalog quantity,
        // over sales that were NOT returned (an anti-join to the channel's returns), in 1998. The store channel is the
        // base; web and catalog are LEFT-joined (their measures null when absent, coalesced to 0); rows are kept where
        // the other-channel quantity is positive, ordered by store metrics, top 100. Three channel stages (each an
        // anti-join + group), a join stage producing the ordered top 100, and a final projection. All keys numeric.
        QueryLowering store = query78Channel("store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_ticket_number", "ss_quantity", "ss_wholesale_cost", "ss_sales_price", "store_returns", "sr_ticket_number");
        QueryLowering web = query78Channel("web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_order_number", "ws_quantity", "ws_wholesale_cost", "ws_sales_price", "web_returns", "wr_order_number");
        QueryLowering catalog = query78Channel("catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_bill_customer_sk", "cs_order_number", "cs_quantity", "cs_wholesale_cost", "cs_sales_price", "catalog_returns", "cr_order_number");

        QueryLowering joined = QueryLowering.scan("q78_store",
                        new QueryLowering.Column("s_year", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("s_item", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("s_customer", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("s_qty", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("s_wholesale", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("s_sales", ColumnEncoding.FLAT, true))
                .leftJoin("q78_web",
                        new String[] {"s_year", "s_item", "s_customer"},
                        new String[] {"w_year", "w_item", "w_customer"},
                        new QueryLowering.Column("w_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("w_item", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("w_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("w_qty", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("w_wholesale", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("w_sales", ColumnEncoding.FLAT, true))
                .leftJoin("q78_catalog",
                        new String[] {"s_year", "s_item", "s_customer"},
                        new String[] {"c_year", "c_item", "c_customer"},
                        new QueryLowering.Column("c_year", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_item", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_customer", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_qty", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_wholesale", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("c_sales", ColumnEncoding.FLAT, true));
        // combined: store(year0,item1,customer2,qty3,wholesale4,sales5), web(year6,item7,customer8,qty9,wholesale10,
        // sales11), catalog(year12,item13,customer14,qty15,wholesale16,sales17).
        Plan.Expr otherQty = new Plan.Bin("+", new Plan.Coalesce(new Plan.Col(9), new Plan.Lit(0)), new Plan.Coalesce(new Plan.Col(15), new Plan.Lit(0)));
        Plan.Expr otherWholesale = new Plan.Bin("+", new Plan.Coalesce(new Plan.Col(10), new Plan.Lit(0)), new Plan.Coalesce(new Plan.Col(16), new Plan.Lit(0)));
        Plan.Expr otherSales = new Plan.Bin("+", new Plan.Coalesce(new Plan.Col(11), new Plan.Lit(0)), new Plan.Coalesce(new Plan.Col(17), new Plan.Lit(0)));
        joined.where(new Plan.Predicate(">", otherQty, new Plan.Lit(0)))
                .select(new Plan.Col(2), new Plan.Call("divide_scale_round_i64", new Plan.Col(3), otherQty, new Plan.Lit(100)),
                        new Plan.Col(3), new Plan.Col(4), new Plan.Col(5), otherQty, otherWholesale, otherSales,
                        new Plan.Col(0), new Plan.Col(1))
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false), new Plan.SortKey(2, true), new Plan.SortKey(3, true), new Plan.SortKey(4, true),
                        new Plan.SortKey(5, false), new Plan.SortKey(6, false), new Plan.SortKey(7, false), new Plan.SortKey(1, false),
                        new Plan.SortKey(8, false), new Plan.SortKey(9, false)), 100));
        // q78_joined output: customer(0), ratio(1), s_qty(2), s_wholesale(3), s_sales(4), other_qty(5),
        // other_wholesale(6), other_sales(7), year(8), item(9).

        QueryLowering main = QueryLowering.scan("q78_joined",
                        new QueryLowering.Column("j_customer", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("j_ratio", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("j_sqty", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("j_swholesale", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("j_ssales", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("j_oqty", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("j_owholesale", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("j_osales", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("j_year", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("j_item", ColumnEncoding.FLAT, false));
        main.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3),
                new Plan.Col(4), new Plan.Col(5), new Plan.Col(6), new Plan.Col(7));

        return new Composite(
                List.of(new Stage(store, "q78_store"), new Stage(web, "q78_web"), new Stage(catalog, "q78_catalog"),
                        new Stage(joined, "q78_joined")),
                main,
                List.of());
    }

    private static QueryLowering query78Channel(String salesTable, String soldDate, String item, String customer,
            String order, String quantity, String wholesale, String salesPrice, String returnsTable, String returnOrder)
    {
        QueryLowering channel = QueryLowering.scan(salesTable,
                        new QueryLowering.Column(soldDate, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(item, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(customer, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(order, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(quantity, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(wholesale, ColumnEncoding.FLAT, true),
                        new QueryLowering.Column(salesPrice, ColumnEncoding.FLAT, true))
                .antiJoin(returnsTable, order, returnOrder,
                        new QueryLowering.Column(returnOrder, ColumnEncoding.FLAT, true))
                .join("date_dim", soldDate, "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true));
        channel.where(new Plan.Predicate("=", channel.column("d_year"), new Plan.Lit(1998)))
                .groupBy("d_year", item, customer)
                .aggregate("sum", quantity)
                .aggregate("sum", wholesale)
                .aggregate("sum", salesPrice);
        return channel;
    }

    public static Composite query70()
    {
        // Q70: store-sale net profit per (s_state, s_county) ROLLUP for a 12-month window, restricted to states that had
        // sales in the window, ranked DESCENDING within each rollup level by the profit total, top 100. Q86's sibling
        // over store geography, plus the active-states semi-filter the harness assembles as a per-state aggregate (it
        // carries no rank<=K, so it admits every state with sales -- but is reproduced as a real stage + join for
        // apples-to-apples). Four stages: the active-states set; the ROLLUP aggregate joined to it; the level/rank-key
        // projection; the RANK window plus the final order.
        QueryLowering states = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_net_profit", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_month_seq", ColumnEncoding.FLAT, true))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_state", ColumnEncoding.STRING, true));
        states.where(new Plan.Predicate(">", states.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", states.column("d_month_seq"), new Plan.Lit(1212)))
                .groupBy("s_state")
                .aggregate("sum", "ss_net_profit");
        // q70_states: state(0), sum(1) -- the sum is the harness's (discarded) per-state total; only the state is joined.

        QueryLowering rollup = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_net_profit", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_month_seq", ColumnEncoding.FLAT, true))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("s_state", ColumnEncoding.STRING, true))
                .join("q70_states", "s_state", "qs_state",
                        new QueryLowering.Column("qs_state", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("qs_sum", ColumnEncoding.FLAT, false));
        rollup.where(new Plan.Predicate(">", rollup.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", rollup.column("d_month_seq"), new Plan.Lit(1212)))
                .groupBy("s_state", "s_county")
                .groupingSets(List.of(new int[] {0, 1}, new int[] {0}, new int[] {}))
                .aggregate("sum", "ss_net_profit");
        // rollup result: state(0), county(1), sum_net_profit(2), grouping_id(3).

        QueryLowering derived = QueryLowering.scan("q70_rollup",
                        new QueryLowering.Column("g_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_sum", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_grouping_id", ColumnEncoding.FLAT, false));
        derived.select(
                derived.column("g_state"),
                derived.column("g_county"),
                derived.column("g_sum"),
                // lochierarchy: detail(gid 0)->0, grand(gid 3)->2, state-subtotal->1.
                new Plan.Case(List.of(new Plan.Case.Branch(new Plan.Predicate("=", derived.column("g_grouping_id"), new Plan.Lit(0)), new Plan.Lit(0))),
                        new Plan.Case(List.of(new Plan.Case.Branch(new Plan.Predicate("=", derived.column("g_grouping_id"), new Plan.Lit(3)), new Plan.Lit(2))), new Plan.Lit(1))),
                // stateForRank: the state's dictionary id for detail rows, else -1.
                new Plan.Case(List.of(new Plan.Case.Branch(new Plan.Predicate("=", derived.column("g_grouping_id"), new Plan.Lit(0)), derived.column("g_state"))), new Plan.Lit(-1)));
        // derived result: state(0), county(1), sum(2), lochierarchy(3), state_rank_key(4).

        QueryLowering ranked = QueryLowering.scan("q70_derived",
                        new QueryLowering.Column("r_state", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("r_county", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("r_sum", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("r_lochierarchy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("r_state_rank_key", ColumnEncoding.FLAT, true));
        ranked.window(new Plan.Window(new int[] {3, 4}, List.of(new Plan.SortKey(2, true)), Plan.RankFunction.RANK, 100));
        // after window: r_state(0), r_county(1), r_sum(2), r_lochierarchy(3), r_state_rank_key(4), rank(5).
        ranked.orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(3, true), new Plan.SortKey(4, false), new Plan.SortKey(5, false)), 100))
                .select(new Plan.Col(2), new Plan.Col(0), new Plan.Col(1), new Plan.Col(3), new Plan.Col(5));

        return new Composite(
                List.of(new Stage(states, "q70_states", List.of(new DictRef(0, 2, 1))),
                        new Stage(rollup, "q70_rollup", List.of(new DictRef(0, 2, 2), new DictRef(1, 2, 1))),
                        new Stage(derived, "q70_derived", List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1)))),
                ranked,
                List.of(new DictRef(1, 0, 0), new DictRef(2, 0, 1)));
    }

    public static Composite query86()
    {
        // Q86: web-sale net paid per (i_category, i_class) ROLLUP for a 12-month window, ranked DESCENDING within each
        // rollup level by the sales total, top 100. Q36's sibling -- same ranking-over-rollup shape, but a single sum
        // ranked directly (no margin ratio) and ranked high-to-low. Three stages: the ROLLUP aggregate; a projection
        // deriving the rollup level and the per-detail category rank key; the RANK window plus the final order.
        QueryLowering rollup = QueryLowering.scan("web_sales",
                        new QueryLowering.Column("ws_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ws_net_paid", ColumnEncoding.FLAT, true))
                .join("date_dim", "ws_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_month_seq", ColumnEncoding.FLAT, true))
                .join("item", "ws_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_class", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true));
        rollup.where(new Plan.Predicate(">", rollup.column("d_month_seq"), new Plan.Lit(1199)),
                        new Plan.Predicate("<", rollup.column("d_month_seq"), new Plan.Lit(1212)))
                .groupBy("i_category", "i_class")
                .groupingSets(List.of(new int[] {0, 1}, new int[] {0}, new int[] {}))
                .aggregate("sum", "ws_net_paid");
        // rollup result: category(0), class(1), sum_net_paid(2), grouping_id(3).

        QueryLowering derived = QueryLowering.scan("q86_rollup",
                        new QueryLowering.Column("g_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_class", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_sum", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_grouping_id", ColumnEncoding.FLAT, false));
        derived.select(
                derived.column("g_category"),
                derived.column("g_class"),
                derived.column("g_sum"),
                // lochierarchy: detail(gid 0)->0, grand(gid 3)->2, category-subtotal->1.
                new Plan.Case(List.of(new Plan.Case.Branch(new Plan.Predicate("=", derived.column("g_grouping_id"), new Plan.Lit(0)), new Plan.Lit(0))),
                        new Plan.Case(List.of(new Plan.Case.Branch(new Plan.Predicate("=", derived.column("g_grouping_id"), new Plan.Lit(3)), new Plan.Lit(2))), new Plan.Lit(1))),
                // categoryForRank: the category's dictionary id for detail rows, else -1 (a LONG case, branch is the string column's slot).
                new Plan.Case(List.of(new Plan.Case.Branch(new Plan.Predicate("=", derived.column("g_grouping_id"), new Plan.Lit(0)), derived.column("g_category"))), new Plan.Lit(-1)));
        // derived result: category(0), class(1), sum(2), lochierarchy(3), category_rank_key(4).

        QueryLowering ranked = QueryLowering.scan("q86_derived",
                        new QueryLowering.Column("r_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("r_class", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("r_sum", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("r_lochierarchy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("r_category_rank_key", ColumnEncoding.FLAT, true));
        ranked.window(new Plan.Window(new int[] {3, 4}, List.of(new Plan.SortKey(2, true)), Plan.RankFunction.RANK, 100));
        // after window: r_category(0), r_class(1), r_sum(2), r_lochierarchy(3), r_category_rank_key(4), rank(5).
        ranked.orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(3, true), new Plan.SortKey(4, false), new Plan.SortKey(5, false)), 100))
                .select(new Plan.Col(2), new Plan.Col(0), new Plan.Col(1), new Plan.Col(3), new Plan.Col(5));

        return new Composite(
                List.of(new Stage(rollup, "q86_rollup", List.of(new DictRef(0, 2, 2), new DictRef(1, 2, 1))),
                        new Stage(derived, "q86_derived", List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1)))),
                ranked,
                List.of(new DictRef(1, 0, 0), new DictRef(2, 0, 1)));
    }

    public static Composite query36()
    {
        // Q36: store-sale gross margin (net_profit/ext_sales) per (category, class) ROLLUP for 2001 TN stores, ranked
        // within each level by margin; the rollup level (lochierarchy), a per-detail category rank-key, and the rank
        // are emitted. Three stages: the ROLLUP aggregate; a projection deriving margin/level/rank-key; the ranking
        // window + final order. categoryForRank is a numeric: a Case typed LONG (Lit(-1) default) whose detail branch
        // is the category column -- emitting its dictionary id -- so the window partitions on it without a string CASE.
        QueryLowering rollup = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_item_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_store_sk", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("ss_net_profit", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year", ColumnEncoding.FLAT, true))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_class", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true))
                .join("store", "ss_store_sk", "s_store_sk",
                        new QueryLowering.Column("s_store_sk"),
                        new QueryLowering.Column("s_state", ColumnEncoding.STRING, true));
        rollup.where(new Plan.Predicate("=", rollup.column("d_year"), new Plan.Lit(2001)),
                        new Plan.StringMatch(rollup.position("s_state"), List.of("TN"), false))
                .groupBy("i_category", "i_class")
                .groupingSets(List.of(new int[] {0, 1}, new int[] {0}, new int[] {}))
                .aggregate("sum", "ss_ext_sales_price")
                .aggregate("sum", "ss_net_profit");
        // rollup result: category(0), class(1), sum_ext_sales(2), sum_net_profit(3), grouping_id(4).
        // grouping_id: detail {cat,class}=0, category-subtotal {cat}=2, grand {}=3.

        QueryLowering derived = QueryLowering.scan("q36_rollup",
                        new QueryLowering.Column("g_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_class", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("g_ext_sales", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_net_profit", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("g_grouping_id", ColumnEncoding.FLAT, false));
        derived.select(
                derived.column("g_category"),
                derived.column("g_class"),
                new Plan.Call("divide_scale_round_i64", derived.column("g_net_profit"), derived.column("g_ext_sales"), new Plan.Lit(1_000_000)),
                // lochierarchy: detail(gid 0)->0, grand(gid 3)->2, category-subtotal->1.
                new Plan.Case(List.of(new Plan.Case.Branch(new Plan.Predicate("=", derived.column("g_grouping_id"), new Plan.Lit(0)), new Plan.Lit(0))),
                        new Plan.Case(List.of(new Plan.Case.Branch(new Plan.Predicate("=", derived.column("g_grouping_id"), new Plan.Lit(3)), new Plan.Lit(2))), new Plan.Lit(1))),
                // categoryForRank: the category's dictionary id for detail rows, else -1 (a LONG case, branch is the string column's slot).
                new Plan.Case(List.of(new Plan.Case.Branch(new Plan.Predicate("=", derived.column("g_grouping_id"), new Plan.Lit(0)), derived.column("g_category"))), new Plan.Lit(-1)));
        // derived result: category(0), class(1), gross_margin(2), lochierarchy(3), category_rank_key(4).

        QueryLowering ranked = QueryLowering.scan("q36_derived",
                        new QueryLowering.Column("r_category", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("r_class", ColumnEncoding.STRING, true),
                        new QueryLowering.Column("r_margin", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("r_lochierarchy", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("r_category_rank_key", ColumnEncoding.FLAT, true));
        ranked.window(new Plan.Window(new int[] {3, 4}, List.of(new Plan.SortKey(2, false)), Plan.RankFunction.RANK, 100));
        // after window: r_category(0), r_class(1), r_margin(2), r_lochierarchy(3), r_category_rank_key(4), rank(5).
        ranked.orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(3, true), new Plan.SortKey(4, false), new Plan.SortKey(5, false),
                        new Plan.SortKey(0, false), new Plan.SortKey(1, false)), 100))
                .select(new Plan.Col(2), new Plan.Col(0), new Plan.Col(1), new Plan.Col(3), new Plan.Col(5));

        return new Composite(
                List.of(new Stage(rollup, "q36_rollup", List.of(new DictRef(0, 2, 2), new DictRef(1, 2, 1))),
                        new Stage(derived, "q36_derived", List.of(new DictRef(0, 0, 0), new DictRef(1, 0, 1)))),
                ranked,
                List.of(new DictRef(1, 0, 0), new DictRef(2, 0, 1)));
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
