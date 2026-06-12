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
package org.weakref.nitro.tpch;

import org.weakref.nitro.jit.ColumnEncoding;
import org.weakref.nitro.jit.Plan;
import org.weakref.nitro.jit.QueryLowering;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries.Ported;

import java.time.LocalDate;
import java.util.List;

/**
 * The TPC-H queries lowered for the compiled engine, validated against the same reference results as the
 * operator harness ({@link TpchParquetSupport}). F64 columns ride the long lanes as raw double bits with
 * explicit {@code _f64} operators and aggregates.
 */
public final class CompiledTpchQueries
{
    private CompiledTpchQueries() {}

    /** Q1: the pricing summary over lineitem, grouped by (returnflag, linestatus). */
    public static Ported query01()
    {
        QueryLowering query = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_returnflag", ColumnEncoding.STRING, false),
                new QueryLowering.Column("l_linestatus", ColumnEncoding.STRING, false),
                f64Column("l_quantity"),
                f64Column("l_extendedprice"),
                f64Column("l_discount"),
                f64Column("l_tax"),
                new QueryLowering.Column("l_shipdate"));
        Plan.Expr discount = query.column("l_discount");
        Plan.Expr extendedPrice = query.column("l_extendedprice");
        Plan.Expr discPrice = new Plan.Bin("multiply_f64", extendedPrice,
                new Plan.Bin("subtract_f64", new Plan.LitF64(1.0), discount));
        Plan.Expr charge = new Plan.Bin("multiply_f64", discPrice,
                new Plan.Bin("add_f64", query.column("l_tax"), new Plan.LitF64(1.0)));
        query.where(new Plan.Predicate("<=", query.column("l_shipdate"), new Plan.Lit(LocalDate.of(1998, 9, 2).toEpochDay())))
                .groupBy("l_returnflag", "l_linestatus")
                .aggregate("sum_f64", "l_quantity")
                .aggregate("sum_f64", "l_extendedprice")
                .aggregate("sum_f64", discPrice)
                .aggregate("sum_f64", charge)
                .aggregate("avg_f64", "l_quantity")
                .aggregate("avg_f64", "l_extendedprice")
                .aggregate("avg_f64", "l_discount")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, false)), -1));
        return new Ported(query, List.of(
                new CompiledTpcdsQueries.DictRef(0, 0, 0),
                new CompiledTpcdsQueries.DictRef(1, 0, 1)));
    }

    /** Q6: the 1994 discount-band revenue, one global sum. */
    public static Ported query06()
    {
        QueryLowering query = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_shipdate"),
                f64Column("l_discount"),
                f64Column("l_quantity"),
                f64Column("l_extendedprice"));
        query.where(
                new Plan.Predicate(">=", query.column("l_shipdate"), new Plan.Lit(LocalDate.of(1994, 1, 1).toEpochDay())),
                new Plan.Predicate("<", query.column("l_shipdate"), new Plan.Lit(LocalDate.of(1995, 1, 1).toEpochDay())),
                new Plan.Predicate("gte_f64", query.column("l_discount"), new Plan.LitF64(0.05)),
                new Plan.Predicate("lte_f64", query.column("l_discount"), new Plan.LitF64(0.07)),
                new Plan.Predicate("lt_f64", query.column("l_quantity"), new Plan.LitF64(24.0)))
                .aggregate("sum_f64", new Plan.Bin("multiply_f64", query.column("l_extendedprice"), query.column("l_discount")));
        return new Ported(query, List.of());
    }

    /** Q3: customer(BUILDING) and date-filtered orders restrict the shipdate-filtered lineitem; revenue per
     * (orderkey, orderdate, shippriority); top 10 by (revenue DESC, orderdate). */
    public static Ported query03()
    {
        QueryLowering query = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_orderkey"),
                f64Column("l_extendedprice"),
                f64Column("l_discount"),
                new QueryLowering.Column("l_shipdate"))
                .join("orders", "l_orderkey", "o_orderkey",
                        new QueryLowering.Column("o_orderkey"),
                        new QueryLowering.Column("o_custkey"),
                        new QueryLowering.Column("o_orderdate"),
                        new QueryLowering.Column("o_shippriority"))
                .join("customer", "o_custkey", "c_custkey",
                        new QueryLowering.Column("c_custkey"),
                        new QueryLowering.Column("c_mktsegment", ColumnEncoding.STRING, false));
        query.where(
                new Plan.Predicate(">", query.column("l_shipdate"), new Plan.Lit(LocalDate.of(1995, 3, 15).toEpochDay())),
                new Plan.Predicate("<", query.column("o_orderdate"), new Plan.Lit(LocalDate.of(1995, 3, 15).toEpochDay())),
                new Plan.StringMatch(query.position("c_mktsegment"), List.of("BUILDING"), false))
                .groupBy("l_orderkey", "o_orderdate", "o_shippriority")
                .aggregate("sum_f64", discPrice(query));
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(3, true), new Plan.SortKey(1, false)), 10));
        query.select(new Plan.Col(0), new Plan.Col(3), new Plan.Col(1), new Plan.Col(2));
        return new Ported(query, List.of());
    }

    /** Q5: the ASIA region chain over the 1994 orders; revenue per nation, descending. */
    public static Ported query05()
    {
        QueryLowering query = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_orderkey"),
                new QueryLowering.Column("l_suppkey"),
                f64Column("l_extendedprice"),
                f64Column("l_discount"))
                .join("orders", "l_orderkey", "o_orderkey",
                        new QueryLowering.Column("o_orderkey"),
                        new QueryLowering.Column("o_custkey"),
                        new QueryLowering.Column("o_orderdate"))
                .join("customer", "o_custkey", "c_custkey",
                        new QueryLowering.Column("c_custkey"),
                        new QueryLowering.Column("c_nationkey"))
                .join("supplier", new String[] {"l_suppkey", "c_nationkey"}, new String[] {"s_suppkey", "s_nationkey"},
                        new QueryLowering.Column("s_suppkey"),
                        new QueryLowering.Column("s_nationkey"))
                .join("nation", "s_nationkey", "n_nationkey",
                        new QueryLowering.Column("n_nationkey"),
                        new QueryLowering.Column("n_regionkey"),
                        new QueryLowering.Column("n_name", ColumnEncoding.STRING, false))
                .join("region", "n_regionkey", "r_regionkey",
                        new QueryLowering.Column("r_regionkey"),
                        new QueryLowering.Column("r_name", ColumnEncoding.STRING, false));
        query.where(
                new Plan.Predicate(">=", query.column("o_orderdate"), new Plan.Lit(LocalDate.of(1994, 1, 1).toEpochDay())),
                new Plan.Predicate("<", query.column("o_orderdate"), new Plan.Lit(LocalDate.of(1995, 1, 1).toEpochDay())),
                new Plan.StringMatch(query.position("r_name"), List.of("ASIA"), false))
                .groupBy("n_name")
                .aggregate("sum_f64", discPrice(query));
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), -1));
        return new Ported(query, List.of(new CompiledTpcdsQueries.DictRef(0, 4, 2)));
    }

    /** Q10: the returned-lineitem revenue per customer over the 1993 Q4 orders; top 20 by revenue. */
    public static Ported query10()
    {
        QueryLowering query = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_orderkey"),
                f64Column("l_extendedprice"),
                f64Column("l_discount"),
                new QueryLowering.Column("l_returnflag", ColumnEncoding.STRING, false))
                .join("orders", "l_orderkey", "o_orderkey",
                        new QueryLowering.Column("o_orderkey"),
                        new QueryLowering.Column("o_custkey"),
                        new QueryLowering.Column("o_orderdate"))
                .join("customer", "o_custkey", "c_custkey",
                        new QueryLowering.Column("c_custkey"),
                        new QueryLowering.Column("c_name", ColumnEncoding.STRING, false),
                        f64Column("c_acctbal"),
                        new QueryLowering.Column("c_phone", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("c_address", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("c_comment", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("c_nationkey"))
                .join("nation", "c_nationkey", "n_nationkey",
                        new QueryLowering.Column("n_nationkey"),
                        new QueryLowering.Column("n_name", ColumnEncoding.STRING, false));
        query.where(
                new Plan.Predicate(">=", query.column("o_orderdate"), new Plan.Lit(LocalDate.of(1993, 10, 1).toEpochDay())),
                new Plan.Predicate("<", query.column("o_orderdate"), new Plan.Lit(LocalDate.of(1994, 1, 1).toEpochDay())),
                new Plan.StringMatch(query.position("l_returnflag"), List.of("R"), false))
                .groupBy("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment")
                .aggregate("sum_f64", discPrice(query));
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(7, true)), 20));
        // SQL order: custkey, name, revenue, acctbal, n_name, address, phone, comment
        query.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(7), new Plan.Col(2), new Plan.Col(4), new Plan.Col(5), new Plan.Col(3), new Plan.Col(6));
        return new Ported(query, List.of(
                new CompiledTpcdsQueries.DictRef(1, 2, 1),
                new CompiledTpcdsQueries.DictRef(4, 3, 1),
                new CompiledTpcdsQueries.DictRef(5, 2, 4),
                new CompiledTpcdsQueries.DictRef(6, 2, 3),
                new CompiledTpcdsQueries.DictRef(7, 2, 5)));
    }

    /** Q12: priority CASE buckets per shipmode over the 1994 receipt-filtered MAIL/SHIP lineitems. */
    public static Ported query12()
    {
        QueryLowering query = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_orderkey"),
                new QueryLowering.Column("l_shipmode", ColumnEncoding.STRING, false),
                new QueryLowering.Column("l_commitdate"),
                new QueryLowering.Column("l_receiptdate"),
                new QueryLowering.Column("l_shipdate"))
                .join("orders", "l_orderkey", "o_orderkey",
                        new QueryLowering.Column("o_orderkey"),
                        new QueryLowering.Column("o_orderpriority", ColumnEncoding.STRING, false));
        Plan.Condition urgent = new Plan.StringMatch(query.position("o_orderpriority"), List.of("1-URGENT", "2-HIGH"), false);
        Plan.Condition notUrgent = new Plan.StringMatch(query.position("o_orderpriority"), List.of("1-URGENT", "2-HIGH"), true);
        query.where(
                new Plan.StringMatch(query.position("l_shipmode"), List.of("MAIL", "SHIP"), false),
                new Plan.Predicate("<", query.column("l_commitdate"), query.column("l_receiptdate")),
                new Plan.Predicate("<", query.column("l_shipdate"), query.column("l_commitdate")),
                new Plan.Predicate(">=", query.column("l_receiptdate"), new Plan.Lit(LocalDate.of(1994, 1, 1).toEpochDay())),
                new Plan.Predicate("<", query.column("l_receiptdate"), new Plan.Lit(LocalDate.of(1995, 1, 1).toEpochDay())))
                .groupBy("l_shipmode")
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(urgent, new Plan.Lit(1))), new Plan.Lit(0)))
                .aggregate("sum", new Plan.Case(List.of(new Plan.Case.Branch(notUrgent, new Plan.Lit(1))), new Plan.Lit(0)));
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), -1));
        return new Ported(query, List.of(new CompiledTpcdsQueries.DictRef(0, 0, 1)));
    }

    /** Q14: the September-1995 promo revenue share over the part-typed lineitems. */
    public static Ported query14()
    {
        QueryLowering query = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_partkey"),
                f64Column("l_extendedprice"),
                f64Column("l_discount"),
                new QueryLowering.Column("l_shipdate"))
                .join("part", "l_partkey", "p_partkey",
                        new QueryLowering.Column("p_partkey"),
                        new QueryLowering.Column("p_type", ColumnEncoding.STRING, false));
        Plan.Condition promo = new Plan.LikeMatch(query.position("p_type"), "PROMO%", false);
        Plan.Expr price = discPrice(query);
        query.where(
                new Plan.Predicate(">=", query.column("l_shipdate"), new Plan.Lit(LocalDate.of(1995, 9, 1).toEpochDay())),
                new Plan.Predicate("<", query.column("l_shipdate"), new Plan.Lit(LocalDate.of(1995, 10, 1).toEpochDay())))
                .aggregate("sum_f64", new Plan.Case(List.of(new Plan.Case.Branch(promo, price)), new Plan.LitF64(0.0)))
                .aggregate("sum_f64", price);
        // 100 * promo / total as the single output column
        query.select(new Plan.Bin("multiply_f64", new Plan.LitF64(100.0),
                new Plan.Bin("divide_f64", new Plan.Col(0), new Plan.Col(1))));
        return new Ported(query, List.of());
    }

    /** Q19: the disjunctive brand / container / quantity / size predicate over DELIVER IN PERSON air lineitems. */
    public static Ported query19()
    {
        QueryLowering query = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_partkey"),
                f64Column("l_quantity"),
                f64Column("l_extendedprice"),
                f64Column("l_discount"),
                new QueryLowering.Column("l_shipinstruct", ColumnEncoding.STRING, false),
                new QueryLowering.Column("l_shipmode", ColumnEncoding.STRING, false))
                .join("part", "l_partkey", "p_partkey",
                        new QueryLowering.Column("p_partkey"),
                        new QueryLowering.Column("p_brand", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("p_container", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("p_size"));
        query.where(
                new Plan.StringMatch(query.position("l_shipinstruct"), List.of("DELIVER IN PERSON"), false),
                new Plan.StringMatch(query.position("l_shipmode"), List.of("AIR", "AIR REG"), false),
                new Plan.Or(List.of(
                        query19Branch(query, "Brand#12", List.of("SM CASE", "SM BOX", "SM PACK", "SM PKG"), 1.0, 11.0, 5),
                        query19Branch(query, "Brand#23", List.of("MED BAG", "MED BOX", "MED PKG", "MED PACK"), 10.0, 20.0, 10),
                        query19Branch(query, "Brand#34", List.of("LG CASE", "LG BOX", "LG PACK", "LG PKG"), 20.0, 30.0, 15))))
                .aggregate("sum_f64", discPrice(query));
        return new Ported(query, List.of());
    }

    private static Plan.Condition query19Branch(QueryLowering query, String brand, List<String> containers, double quantityLow, double quantityHigh, int sizeHigh)
    {
        return new Plan.And(List.of(
                new Plan.StringMatch(query.position("p_brand"), List.of(brand), false),
                new Plan.StringMatch(query.position("p_container"), containers, false),
                new Plan.Predicate("gte_f64", query.column("l_quantity"), new Plan.LitF64(quantityLow)),
                new Plan.Predicate("lte_f64", query.column("l_quantity"), new Plan.LitF64(quantityHigh)),
                new Plan.Predicate(">=", query.column("p_size"), new Plan.Lit(1)),
                new Plan.Predicate("<=", query.column("p_size"), new Plan.Lit(sizeHigh))));
    }

    /** l_extendedprice * (1 - l_discount) over the query's combined namespace. */
    private static Plan.Expr discPrice(QueryLowering query)
    {
        return new Plan.Bin("multiply_f64", query.column("l_extendedprice"),
                new Plan.Bin("subtract_f64", new Plan.LitF64(1.0), query.column("l_discount")));
    }

    /** Q7: the FRANCE/GERMANY shipping volumes by (supplier nation, customer nation, shipment year). */
    public static Ported query07()
    {
        QueryLowering query = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_orderkey"),
                new QueryLowering.Column("l_suppkey"),
                f64Column("l_extendedprice"),
                f64Column("l_discount"),
                new QueryLowering.Column("l_shipdate"))
                .join("supplier", "l_suppkey", "s_suppkey",
                        new QueryLowering.Column("s_suppkey"),
                        new QueryLowering.Column("s_nationkey"))
                .join("orders", "l_orderkey", "o_orderkey",
                        new QueryLowering.Column("o_orderkey"),
                        new QueryLowering.Column("o_custkey"))
                .join("customer", "o_custkey", "c_custkey",
                        new QueryLowering.Column("c_custkey"),
                        new QueryLowering.Column("c_nationkey"))
                .join("nation", "s_nationkey", "n_nationkey",
                        new QueryLowering.Column("n_nationkey"),
                        new QueryLowering.Column("n_name", ColumnEncoding.STRING, false))
                .join("nation", "c_nationkey", "n2_nationkey",
                        new QueryLowering.Column("n2_nationkey", "n_nationkey", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("n2_name", "n_name", ColumnEncoding.STRING, false));
        int supplierNation = query.position("n_name");
        int customerNation = query.position("n2_name");
        query.where(
                new Plan.Predicate(">=", query.column("l_shipdate"), new Plan.Lit(LocalDate.of(1995, 1, 1).toEpochDay())),
                new Plan.Predicate("<=", query.column("l_shipdate"), new Plan.Lit(LocalDate.of(1996, 12, 31).toEpochDay())),
                new Plan.Or(List.of(
                        new Plan.And(List.of(
                                new Plan.StringMatch(supplierNation, List.of("FRANCE"), false),
                                new Plan.StringMatch(customerNation, List.of("GERMANY"), false))),
                        new Plan.And(List.of(
                                new Plan.StringMatch(supplierNation, List.of("GERMANY"), false),
                                new Plan.StringMatch(customerNation, List.of("FRANCE"), false))))))
                .groupBy(new Plan.Col(supplierNation), new Plan.Col(customerNation),
                        new Plan.Call("year_of_date", List.of(query.column("l_shipdate"))))
                .aggregate("sum_f64", discPrice(query));
        query.orderBy(new Plan.Ordering(List.of(
                new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)), -1));
        return new Ported(query, List.of(
                new CompiledTpcdsQueries.DictRef(0, 4, 1),
                new CompiledTpcdsQueries.DictRef(1, 5, 1)));
    }

    /** Q8: the BRAZIL market share of ECONOMY ANODIZED STEEL volume per order year in AMERICA. */
    public static Ported query08()
    {
        QueryLowering query = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_partkey"),
                new QueryLowering.Column("l_orderkey"),
                new QueryLowering.Column("l_suppkey"),
                f64Column("l_extendedprice"),
                f64Column("l_discount"))
                .join("part", "l_partkey", "p_partkey",
                        new QueryLowering.Column("p_partkey"),
                        new QueryLowering.Column("p_type", ColumnEncoding.STRING, false))
                .join("orders", "l_orderkey", "o_orderkey",
                        new QueryLowering.Column("o_orderkey"),
                        new QueryLowering.Column("o_custkey"),
                        new QueryLowering.Column("o_orderdate"))
                .join("customer", "o_custkey", "c_custkey",
                        new QueryLowering.Column("c_custkey"),
                        new QueryLowering.Column("c_nationkey"))
                .join("nation", "c_nationkey", "n_nationkey",
                        new QueryLowering.Column("n_nationkey"),
                        new QueryLowering.Column("n_regionkey"))
                .join("region", "n_regionkey", "r_regionkey",
                        new QueryLowering.Column("r_regionkey"),
                        new QueryLowering.Column("r_name", ColumnEncoding.STRING, false))
                .join("supplier", "l_suppkey", "s_suppkey",
                        new QueryLowering.Column("s_suppkey"),
                        new QueryLowering.Column("s_nationkey"))
                .join("nation", "s_nationkey", "n2_nationkey",
                        new QueryLowering.Column("n2_nationkey", "n_nationkey", ColumnEncoding.FLAT, false),
                        new QueryLowering.Column("n2_name", "n_name", ColumnEncoding.STRING, false));
        int supplierNationName = query.position("n2_name");
        Plan.Expr volume = discPrice(query);
        Plan.Condition brazil = new Plan.StringMatch(supplierNationName, List.of("BRAZIL"), false);
        query.where(
                new Plan.StringMatch(query.position("p_type"), List.of("ECONOMY ANODIZED STEEL"), false),
                new Plan.Predicate(">=", query.column("o_orderdate"), new Plan.Lit(LocalDate.of(1995, 1, 1).toEpochDay())),
                new Plan.Predicate("<=", query.column("o_orderdate"), new Plan.Lit(LocalDate.of(1996, 12, 31).toEpochDay())),
                new Plan.StringMatch(query.position("r_name"), List.of("AMERICA"), false))
                .groupBy(new Plan.Call("year_of_date", List.of(query.column("o_orderdate"))))
                .aggregate("sum_f64", new Plan.Case(List.of(new Plan.Case.Branch(brazil, volume)), new Plan.LitF64(0.0)))
                .aggregate("sum_f64", volume);
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), -1));
        query.select(new Plan.Col(0), new Plan.Bin("divide_f64", new Plan.Col(1), new Plan.Col(2)));
        return new Ported(query, List.of());
    }

    /** Q9: profit on green parts by (nation, order year), year descending within nation. */
    public static Ported query09()
    {
        QueryLowering query = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_partkey"),
                new QueryLowering.Column("l_orderkey"),
                new QueryLowering.Column("l_suppkey"),
                f64Column("l_quantity"),
                f64Column("l_extendedprice"),
                f64Column("l_discount"))
                .join("part", "l_partkey", "p_partkey",
                        new QueryLowering.Column("p_partkey"),
                        new QueryLowering.Column("p_name", ColumnEncoding.STRING, false))
                .join("supplier", "l_suppkey", "s_suppkey",
                        new QueryLowering.Column("s_suppkey"),
                        new QueryLowering.Column("s_nationkey"))
                .join("partsupp", new String[] {"l_suppkey", "l_partkey"}, new String[] {"ps_suppkey", "ps_partkey"},
                        new QueryLowering.Column("ps_partkey"),
                        new QueryLowering.Column("ps_suppkey"),
                        f64Column("ps_supplycost"))
                .join("orders", "l_orderkey", "o_orderkey",
                        new QueryLowering.Column("o_orderkey"),
                        new QueryLowering.Column("o_orderdate"))
                .join("nation", "s_nationkey", "n_nationkey",
                        new QueryLowering.Column("n_nationkey"),
                        new QueryLowering.Column("n_name", ColumnEncoding.STRING, false));
        Plan.Expr amount = new Plan.Bin("subtract_f64", discPrice(query),
                new Plan.Bin("multiply_f64", query.column("ps_supplycost"), query.column("l_quantity")));
        query.where(new Plan.LikeMatch(query.position("p_name"), "%green%", false))
                .groupBy(new Plan.Col(query.position("n_name")),
                        new Plan.Call("year_of_date", List.of(query.column("o_orderdate"))))
                .aggregate("sum_f64", amount);
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false), new Plan.SortKey(1, true)), -1));
        return new Ported(query, List.of(new CompiledTpcdsQueries.DictRef(0, 5, 1)));
    }

    /** Q4: orders in 1993Q3 with at least one late lineitem (semi join, the lateness filter pruning the build),
     * counted per order priority. */
    public static Ported query04()
    {
        QueryLowering query = QueryLowering.scan("orders",
                new QueryLowering.Column("o_orderkey"),
                new QueryLowering.Column("o_orderdate"),
                new QueryLowering.Column("o_orderpriority", ColumnEncoding.STRING, false))
                .semiJoin("lineitem", "o_orderkey", "l_orderkey",
                        new QueryLowering.Column("l_orderkey"),
                        new QueryLowering.Column("l_commitdate"),
                        new QueryLowering.Column("l_receiptdate"));
        query.where(
                new Plan.Predicate(">=", query.column("o_orderdate"), new Plan.Lit(LocalDate.of(1993, 7, 1).toEpochDay())),
                new Plan.Predicate("<", query.column("o_orderdate"), new Plan.Lit(LocalDate.of(1993, 10, 1).toEpochDay())),
                new Plan.Predicate("<", query.column("l_commitdate"), query.column("l_receiptdate")))
                .groupBy("o_orderpriority")
                .count();
        query.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), -1));
        return new Ported(query, List.of(new CompiledTpcdsQueries.DictRef(0, 0, 2)));
    }

    /** Q18: orders whose lineitems sum to over 300 units (the grouped subquery staged as a virtual build),
     * re-aggregated per (customer, order); top 100 by (totalprice DESC, orderdate). */
    public static CompiledTpcdsQueries.Composite query18()
    {
        QueryLowering bigOrders = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_orderkey"),
                f64Column("l_quantity"))
                .groupBy("l_orderkey")
                .aggregate("sum_f64", "l_quantity");

        QueryLowering main = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_orderkey"),
                f64Column("l_quantity"))
                .join("orders", "l_orderkey", "o_orderkey",
                        new QueryLowering.Column("o_orderkey"),
                        new QueryLowering.Column("o_custkey"),
                        new QueryLowering.Column("o_orderdate"),
                        f64Column("o_totalprice"))
                .join("customer", "o_custkey", "c_custkey",
                        new QueryLowering.Column("c_custkey"),
                        new QueryLowering.Column("c_name", ColumnEncoding.STRING, false))
                .join("__big_orders__", "l_orderkey", "bo_orderkey",
                        new QueryLowering.Column("bo_orderkey"),
                        f64Column("bo_quantity"));
        main.where(new Plan.Predicate("gt_f64", main.column("bo_quantity"), new Plan.LitF64(300.0)))
                .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
                .aggregate("sum_f64", "l_quantity");
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(4, true), new Plan.SortKey(3, false)), 100));
        return new CompiledTpcdsQueries.Composite(
                List.of(new CompiledTpcdsQueries.Stage(bigOrders, "__big_orders__")),
                main,
                List.of(new CompiledTpcdsQueries.DictRef(0, 2, 1)));
    }

    /** Q13: the order-count distribution over customers, special-request orders excluded BEFORE the left join
     * (a projection-only stage, since an outer join cannot prune its build), two grouping levels staged. */
    public static CompiledTpcdsQueries.Composite query13()
    {
        QueryLowering orders = QueryLowering.scan("orders",
                new QueryLowering.Column("o_orderkey"),
                new QueryLowering.Column("o_custkey"),
                new QueryLowering.Column("o_comment", ColumnEncoding.STRING, false));
        orders.where(new Plan.LikeMatch(orders.position("o_comment"), "%special%requests%", true))
                .select(new Plan.Col(0), new Plan.Col(1));

        QueryLowering perCustomer = QueryLowering.scan("customer",
                new QueryLowering.Column("c_custkey"))
                .leftJoin("__orders__", "c_custkey", "fo_custkey",
                        new QueryLowering.Column("fo_orderkey", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("fo_custkey", ColumnEncoding.FLAT, true));
        perCustomer.groupBy("c_custkey")
                .aggregate("count", "fo_orderkey");

        QueryLowering main = QueryLowering.scan("__per_customer__",
                new QueryLowering.Column("custkey"),
                new QueryLowering.Column("c_count"));
        main.groupBy("c_count")
                .count();
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true), new Plan.SortKey(0, true)), -1));
        return new CompiledTpcdsQueries.Composite(
                List.of(
                        new CompiledTpcdsQueries.Stage(orders, "__orders__"),
                        new CompiledTpcdsQueries.Stage(perCustomer, "__per_customer__")),
                main,
                List.of());
    }

    /** Q16: suppliers per (brand, type, size) for the qualifying parts, complaining suppliers anti-joined out;
     * the distinct stage (group + count, count dropped) feeds the per-combination count. */
    public static CompiledTpcdsQueries.Composite query16()
    {
        QueryLowering distinct = QueryLowering.scan("partsupp",
                new QueryLowering.Column("ps_partkey"),
                new QueryLowering.Column("ps_suppkey"))
                .join("part", "ps_partkey", "p_partkey",
                        new QueryLowering.Column("p_partkey"),
                        new QueryLowering.Column("p_brand", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("p_type", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("p_size"))
                .antiJoin("supplier", "ps_suppkey", "s_suppkey",
                        new QueryLowering.Column("s_suppkey"),
                        new QueryLowering.Column("s_comment", ColumnEncoding.STRING, false));
        distinct.where(
                new Plan.StringMatch(distinct.position("p_brand"), List.of("Brand#45"), true),
                new Plan.LikeMatch(distinct.position("p_type"), "MEDIUM POLISHED%", true),
                new Plan.Or(List.of(
                        new Plan.Predicate("=", distinct.column("p_size"), new Plan.Lit(3)),
                        new Plan.Predicate("=", distinct.column("p_size"), new Plan.Lit(9)),
                        new Plan.Predicate("=", distinct.column("p_size"), new Plan.Lit(14)),
                        new Plan.Predicate("=", distinct.column("p_size"), new Plan.Lit(19)),
                        new Plan.Predicate("=", distinct.column("p_size"), new Plan.Lit(23)),
                        new Plan.Predicate("=", distinct.column("p_size"), new Plan.Lit(36)),
                        new Plan.Predicate("=", distinct.column("p_size"), new Plan.Lit(45)),
                        new Plan.Predicate("=", distinct.column("p_size"), new Plan.Lit(49)))),
                new Plan.LikeMatch(distinct.position("s_comment"), "%Customer%Complaints%", false))
                .groupBy("p_brand", "p_type", "p_size", "ps_suppkey")
                .count();
        distinct.select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3));

        QueryLowering main = QueryLowering.scan("__distinct_suppliers__",
                new QueryLowering.Column("brand", ColumnEncoding.STRING, false),
                new QueryLowering.Column("type", ColumnEncoding.STRING, false),
                new QueryLowering.Column("size"),
                new QueryLowering.Column("suppkey"));
        main.groupBy("brand", "type", "size")
                .count();
        main.orderBy(new Plan.Ordering(List.of(
                new Plan.SortKey(3, true), new Plan.SortKey(0, false), new Plan.SortKey(1, false), new Plan.SortKey(2, false)), -1));
        return new CompiledTpcdsQueries.Composite(
                List.of(new CompiledTpcdsQueries.Stage(distinct, "__distinct_suppliers__", List.of(
                        new CompiledTpcdsQueries.DictRef(0, 1, 1),
                        new CompiledTpcdsQueries.DictRef(1, 1, 2)))),
                main,
                List.of(
                        new CompiledTpcdsQueries.DictRef(0, 0, 0),
                        new CompiledTpcdsQueries.DictRef(1, 0, 1)));
    }

    /** Q17: the per-partkey 0.2*avg(quantity) threshold (staged over all of lineitem) restricts the Brand#23
     * MED BOX lineitems; sum(extendedprice)/7 is the single output cell. */
    public static CompiledTpcdsQueries.Composite query17()
    {
        QueryLowering thresholds = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_partkey"),
                f64Column("l_quantity"))
                .groupBy("l_partkey")
                .aggregate("avg_f64", "l_quantity");
        thresholds.select(new Plan.Col(0), new Plan.Bin("multiply_f64", new Plan.LitF64(0.2), new Plan.Col(1)));

        QueryLowering main = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_partkey"),
                f64Column("l_quantity"),
                f64Column("l_extendedprice"))
                .join("part", "l_partkey", "p_partkey",
                        new QueryLowering.Column("p_partkey"),
                        new QueryLowering.Column("p_brand", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("p_container", ColumnEncoding.STRING, false))
                .join("__thresholds__", "l_partkey", "t_partkey",
                        new QueryLowering.Column("t_partkey"),
                        f64Column("t_threshold"));
        main.where(
                new Plan.StringMatch(main.position("p_brand"), List.of("Brand#23"), false),
                new Plan.StringMatch(main.position("p_container"), List.of("MED BOX"), false),
                new Plan.Predicate("lt_f64", main.column("l_quantity"), main.column("t_threshold")))
                .aggregate("sum_f64", "l_extendedprice");
        main.select(new Plan.Bin("divide_f64", new Plan.Col(0), new Plan.LitF64(7.0)));
        return new CompiledTpcdsQueries.Composite(
                List.of(new CompiledTpcdsQueries.Stage(thresholds, "__thresholds__")),
                main,
                List.of());
    }

    /** Q15: the quarter's revenue per supplier staged twice (the SQL references the view twice and the engine
     * has no reuse), its max staged over the second copy and broadcast; suppliers at the max, by suppkey. */
    public static CompiledTpcdsQueries.Composite query15()
    {
        QueryLowering revenue = query15Revenue();
        QueryLowering revenueCopy = query15Revenue();
        QueryLowering maxRevenue = QueryLowering.scan("__revenue_copy__",
                new QueryLowering.Column("supplier_no"),
                f64Column("total_revenue"))
                .aggregate("max_f64", "total_revenue");

        QueryLowering main = QueryLowering.scan("supplier",
                new QueryLowering.Column("s_suppkey"),
                new QueryLowering.Column("s_name", ColumnEncoding.STRING, false),
                new QueryLowering.Column("s_address", ColumnEncoding.STRING, false),
                new QueryLowering.Column("s_phone", ColumnEncoding.STRING, false))
                .join("__revenue__", "s_suppkey", "r_supplier",
                        new QueryLowering.Column("r_supplier"),
                        f64Column("r_total"))
                .crossJoin("__max_revenue__", f64Column("m_max"));
        main.where(new Plan.Predicate("eq_f64", main.column("r_total"), main.column("m_max")))
                .select(new Plan.Col(0), new Plan.Col(1), new Plan.Col(2), new Plan.Col(3), new Plan.Col(5));
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), -1));
        return new CompiledTpcdsQueries.Composite(
                List.of(
                        new CompiledTpcdsQueries.Stage(revenue, "__revenue__"),
                        new CompiledTpcdsQueries.Stage(revenueCopy, "__revenue_copy__"),
                        new CompiledTpcdsQueries.Stage(maxRevenue, "__max_revenue__")),
                main,
                List.of(
                        new CompiledTpcdsQueries.DictRef(1, 0, 1),
                        new CompiledTpcdsQueries.DictRef(2, 0, 2),
                        new CompiledTpcdsQueries.DictRef(3, 0, 3)));
    }

    private static QueryLowering query15Revenue()
    {
        QueryLowering revenue = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_suppkey"),
                f64Column("l_extendedprice"),
                f64Column("l_discount"),
                new QueryLowering.Column("l_shipdate"));
        revenue.where(
                new Plan.Predicate(">=", revenue.column("l_shipdate"), new Plan.Lit(LocalDate.of(1996, 1, 1).toEpochDay())),
                new Plan.Predicate("<", revenue.column("l_shipdate"), new Plan.Lit(LocalDate.of(1996, 4, 1).toEpochDay())))
                .groupBy("l_suppkey")
                .aggregate("sum_f64", discPrice(revenue));
        return revenue;
    }

    /** Q2: the minimum EUROPE supplycost per partkey (staged) joins back on cost equality for the size-15
     * %BRASS parts; top 100 by (acctbal DESC, n_name, s_name, partkey). */
    public static CompiledTpcdsQueries.Composite query02()
    {
        QueryLowering minimumCosts = QueryLowering.scan("partsupp",
                new QueryLowering.Column("ps_partkey"),
                new QueryLowering.Column("ps_suppkey"),
                f64Column("ps_supplycost"))
                .join("supplier", "ps_suppkey", "s_suppkey",
                        new QueryLowering.Column("s_suppkey"),
                        new QueryLowering.Column("s_nationkey"))
                .join("nation", "s_nationkey", "n_nationkey",
                        new QueryLowering.Column("n_nationkey"),
                        new QueryLowering.Column("n_regionkey"))
                .join("region", "n_regionkey", "r_regionkey",
                        new QueryLowering.Column("r_regionkey"),
                        new QueryLowering.Column("r_name", ColumnEncoding.STRING, false));
        minimumCosts.where(new Plan.StringMatch(minimumCosts.position("r_name"), List.of("EUROPE"), false))
                .groupBy("ps_partkey")
                .aggregate("min_f64", "ps_supplycost");

        QueryLowering main = QueryLowering.scan("partsupp",
                new QueryLowering.Column("ps_partkey"),
                new QueryLowering.Column("ps_suppkey"),
                f64Column("ps_supplycost"))
                .join("part", "ps_partkey", "p_partkey",
                        new QueryLowering.Column("p_partkey"),
                        new QueryLowering.Column("p_mfgr", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("p_size"),
                        new QueryLowering.Column("p_type", ColumnEncoding.STRING, false))
                .join("supplier", "ps_suppkey", "s_suppkey",
                        new QueryLowering.Column("s_suppkey"),
                        new QueryLowering.Column("s_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("s_address", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("s_nationkey"),
                        new QueryLowering.Column("s_phone", ColumnEncoding.STRING, false),
                        f64Column("s_acctbal"),
                        new QueryLowering.Column("s_comment", ColumnEncoding.STRING, false))
                .join("nation", "s_nationkey", "n_nationkey",
                        new QueryLowering.Column("n_nationkey"),
                        new QueryLowering.Column("n_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("n_regionkey"))
                .join("region", "n_regionkey", "r_regionkey",
                        new QueryLowering.Column("r_regionkey"),
                        new QueryLowering.Column("r_name", ColumnEncoding.STRING, false))
                .join("__minimum_costs__", "ps_partkey", "mc_partkey",
                        new QueryLowering.Column("mc_partkey"),
                        f64Column("mc_cost"));
        main.where(
                new Plan.Predicate("=", main.column("p_size"), new Plan.Lit(15)),
                new Plan.LikeMatch(main.position("p_type"), "%BRASS", false),
                new Plan.StringMatch(main.position("r_name"), List.of("EUROPE"), false),
                new Plan.Predicate("eq_f64", main.column("ps_supplycost"), main.column("mc_cost")))
                .select(
                        main.column("s_acctbal"),
                        main.column("s_name"),
                        main.column("n_name"),
                        main.column("p_partkey"),
                        main.column("p_mfgr"),
                        main.column("s_address"),
                        main.column("s_phone"),
                        main.column("s_comment"));
        main.orderBy(new Plan.Ordering(List.of(
                new Plan.SortKey(0, true), new Plan.SortKey(2, false), new Plan.SortKey(1, false), new Plan.SortKey(3, false)), 100));
        return new CompiledTpcdsQueries.Composite(
                List.of(new CompiledTpcdsQueries.Stage(minimumCosts, "__minimum_costs__")),
                main,
                List.of(
                        new CompiledTpcdsQueries.DictRef(1, 2, 1),
                        new CompiledTpcdsQueries.DictRef(2, 3, 1),
                        new CompiledTpcdsQueries.DictRef(4, 1, 1),
                        new CompiledTpcdsQueries.DictRef(5, 2, 2),
                        new CompiledTpcdsQueries.DictRef(6, 2, 4),
                        new CompiledTpcdsQueries.DictRef(7, 2, 6)));
    }

    /** Q11: the German partsupp value per partkey (staged), the global value scaled by the sf10 fraction
     * broadcast as a single-row stage, and the parts above it, by value descending. */
    public static CompiledTpcdsQueries.Composite query11()
    {
        QueryLowering perPart = query11GermanValue(true);
        QueryLowering total = query11GermanValue(false);
        total.select(new Plan.Bin("multiply_f64", new Plan.Col(0), new Plan.LitF64(0.00001)));

        QueryLowering main = QueryLowering.scan("__value__",
                new QueryLowering.Column("partkey"),
                f64Column("value"))
                .crossJoin("__threshold__", f64Column("threshold"));
        main.where(new Plan.Predicate("gt_f64", main.column("value"), main.column("threshold")))
                .select(new Plan.Col(0), new Plan.Col(1));
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), -1));
        return new CompiledTpcdsQueries.Composite(
                List.of(
                        new CompiledTpcdsQueries.Stage(perPart, "__value__"),
                        new CompiledTpcdsQueries.Stage(total, "__threshold__")),
                main,
                List.of());
    }

    private static QueryLowering query11GermanValue(boolean grouped)
    {
        QueryLowering query = QueryLowering.scan("partsupp",
                new QueryLowering.Column("ps_partkey"),
                new QueryLowering.Column("ps_suppkey"),
                f64Column("ps_supplycost"),
                new QueryLowering.Column("ps_availqty"))
                .join("supplier", "ps_suppkey", "s_suppkey",
                        new QueryLowering.Column("s_suppkey"),
                        new QueryLowering.Column("s_nationkey"))
                .join("nation", "s_nationkey", "n_nationkey",
                        new QueryLowering.Column("n_nationkey"),
                        new QueryLowering.Column("n_name", ColumnEncoding.STRING, false));
        query.where(new Plan.StringMatch(query.position("n_name"), List.of("GERMANY"), false));
        Plan.Expr value = new Plan.Bin("multiply_f64", query.column("ps_supplycost"),
                new Plan.Call("cast_i64_to_f64", List.of(query.column("ps_availqty"))));
        if (grouped) {
            query.groupBy("ps_partkey");
        }
        query.aggregate("sum_f64", value);
        return query;
    }

    /** Q20: the 1994 shipped-quantity halves per (partkey, suppkey) staged; partsupp rows of forest parts with
     * availqty above their threshold stage the qualified-supplier keys; CANADA suppliers semi-join them. */
    public static CompiledTpcdsQueries.Composite query20()
    {
        QueryLowering thresholds = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_partkey"),
                new QueryLowering.Column("l_suppkey"),
                f64Column("l_quantity"),
                new QueryLowering.Column("l_shipdate"));
        thresholds.where(
                new Plan.Predicate(">=", thresholds.column("l_shipdate"), new Plan.Lit(LocalDate.of(1994, 1, 1).toEpochDay())),
                new Plan.Predicate("<", thresholds.column("l_shipdate"), new Plan.Lit(LocalDate.of(1995, 1, 1).toEpochDay())))
                .groupBy("l_partkey", "l_suppkey")
                .aggregate("sum_f64", "l_quantity");
        thresholds.select(new Plan.Col(0), new Plan.Col(1), new Plan.Bin("multiply_f64", new Plan.LitF64(0.5), new Plan.Col(2)));

        QueryLowering qualified = QueryLowering.scan("partsupp",
                new QueryLowering.Column("ps_partkey"),
                new QueryLowering.Column("ps_suppkey"),
                new QueryLowering.Column("ps_availqty"))
                .join("part", "ps_partkey", "p_partkey",
                        new QueryLowering.Column("p_partkey"),
                        new QueryLowering.Column("p_name", ColumnEncoding.STRING, false))
                .join("__thresholds__", new String[] {"ps_partkey", "ps_suppkey"}, new String[] {"t_partkey", "t_suppkey"},
                        new QueryLowering.Column("t_partkey"),
                        new QueryLowering.Column("t_suppkey"),
                        f64Column("t_threshold"));
        qualified.where(
                new Plan.LikeMatch(qualified.position("p_name"), "forest%", false),
                new Plan.Predicate("gt_f64",
                        new Plan.Call("cast_i64_to_f64", List.of(qualified.column("ps_availqty"))),
                        qualified.column("t_threshold")))
                .groupBy("ps_suppkey")
                .count();
        qualified.select(new Plan.Col(0));

        QueryLowering main = QueryLowering.scan("supplier",
                new QueryLowering.Column("s_suppkey"),
                new QueryLowering.Column("s_name", ColumnEncoding.STRING, false),
                new QueryLowering.Column("s_address", ColumnEncoding.STRING, false),
                new QueryLowering.Column("s_nationkey"))
                .join("nation", "s_nationkey", "n_nationkey",
                        new QueryLowering.Column("n_nationkey"),
                        new QueryLowering.Column("n_name", ColumnEncoding.STRING, false))
                .semiJoin("__qualified__", "s_suppkey", "q_suppkey",
                        new QueryLowering.Column("q_suppkey"));
        main.where(new Plan.StringMatch(main.position("n_name"), List.of("CANADA"), false))
                .select(new Plan.Col(1), new Plan.Col(2));
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), -1));
        return new CompiledTpcdsQueries.Composite(
                List.of(
                        new CompiledTpcdsQueries.Stage(thresholds, "__thresholds__"),
                        new CompiledTpcdsQueries.Stage(qualified, "__qualified__")),
                main,
                List.of(
                        new CompiledTpcdsQueries.DictRef(0, 0, 1),
                        new CompiledTpcdsQueries.DictRef(1, 0, 2)));
    }

    /** Q21: per-order distinct-supplier statistics (staged twice: all suppliers, late suppliers) decorrelate
     * the inequality EXISTS pair; waits per SAUDI ARABIA supplier, top 100 by (numwait DESC, name). */
    public static CompiledTpcdsQueries.Composite query21()
    {
        QueryLowering pairs = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_orderkey"),
                new QueryLowering.Column("l_suppkey"))
                .groupBy("l_orderkey", "l_suppkey")
                .count();
        pairs.select(new Plan.Col(0), new Plan.Col(1));
        QueryLowering supplierCounts = QueryLowering.scan("__order_supplier_pairs__",
                new QueryLowering.Column("os_orderkey"),
                new QueryLowering.Column("os_suppkey"))
                .groupBy("os_orderkey")
                .count();

        QueryLowering latePairs = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_orderkey"),
                new QueryLowering.Column("l_suppkey"),
                new QueryLowering.Column("l_commitdate"),
                new QueryLowering.Column("l_receiptdate"));
        latePairs.where(new Plan.Predicate("<", latePairs.column("l_commitdate"), latePairs.column("l_receiptdate")))
                .groupBy("l_orderkey", "l_suppkey")
                .count();
        latePairs.select(new Plan.Col(0), new Plan.Col(1));
        QueryLowering lateCounts = QueryLowering.scan("__late_supplier_pairs__",
                new QueryLowering.Column("ls_orderkey"),
                new QueryLowering.Column("ls_suppkey"))
                .groupBy("ls_orderkey")
                .count();

        QueryLowering main = QueryLowering.scan("lineitem",
                new QueryLowering.Column("l_orderkey"),
                new QueryLowering.Column("l_suppkey"),
                new QueryLowering.Column("l_commitdate"),
                new QueryLowering.Column("l_receiptdate"))
                .join("orders", "l_orderkey", "o_orderkey",
                        new QueryLowering.Column("o_orderkey"),
                        new QueryLowering.Column("o_orderstatus", ColumnEncoding.STRING, false))
                .join("supplier", "l_suppkey", "s_suppkey",
                        new QueryLowering.Column("s_suppkey"),
                        new QueryLowering.Column("s_name", ColumnEncoding.STRING, false),
                        new QueryLowering.Column("s_nationkey"))
                .join("nation", "s_nationkey", "n_nationkey",
                        new QueryLowering.Column("n_nationkey"),
                        new QueryLowering.Column("n_name", ColumnEncoding.STRING, false))
                .join("__supplier_counts__", "l_orderkey", "sc_orderkey",
                        new QueryLowering.Column("sc_orderkey"),
                        new QueryLowering.Column("sc_count"))
                .join("__late_counts__", "l_orderkey", "lc_orderkey",
                        new QueryLowering.Column("lc_orderkey"),
                        new QueryLowering.Column("lc_count"));
        main.where(
                new Plan.Predicate("<", main.column("l_commitdate"), main.column("l_receiptdate")),
                new Plan.StringMatch(main.position("o_orderstatus"), List.of("F"), false),
                new Plan.StringMatch(main.position("n_name"), List.of("SAUDI ARABIA"), false),
                new Plan.Predicate(">", main.column("sc_count"), new Plan.Lit(1)),
                new Plan.Predicate("=", main.column("lc_count"), new Plan.Lit(1)))
                .groupBy("s_name")
                .count();
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(1, true), new Plan.SortKey(0, false)), 100));
        return new CompiledTpcdsQueries.Composite(
                List.of(
                        new CompiledTpcdsQueries.Stage(pairs, "__order_supplier_pairs__"),
                        new CompiledTpcdsQueries.Stage(supplierCounts, "__supplier_counts__"),
                        new CompiledTpcdsQueries.Stage(latePairs, "__late_supplier_pairs__"),
                        new CompiledTpcdsQueries.Stage(lateCounts, "__late_counts__")),
                main,
                List.of(new CompiledTpcdsQueries.DictRef(0, 2, 1)));
    }

    /** Q22: country codes from the phone prefix; the positive-balance average over the seven codes staged as a
     * one-row broadcast (its filter tests the raw phone via SubstringMatch); customers above it with no orders,
     * per code. The main scan loads the code as a substring column, interned globally as the group key. */
    public static CompiledTpcdsQueries.Composite query22()
    {
        List<String> codes = List.of("13", "31", "23", "29", "30", "18", "17");

        QueryLowering average = QueryLowering.scan("customer",
                new QueryLowering.Column("c_phone", ColumnEncoding.STRING, false),
                f64Column("c_acctbal"));
        average.where(
                new Plan.SubstringMatch(average.position("c_phone"), 1, 2, codes, false),
                new Plan.Predicate("gt_f64", average.column("c_acctbal"), new Plan.LitF64(0.0)))
                .aggregate("avg_f64", "c_acctbal");

        QueryLowering main = QueryLowering.scan("customer",
                QueryLowering.Column.substring("c_phone", false, 1, 2),
                f64Column("c_acctbal"),
                new QueryLowering.Column("c_custkey"))
                .crossJoin("__average__", f64Column("a_average"))
                .antiJoin("orders", "c_custkey", "o_custkey",
                        new QueryLowering.Column("o_custkey"));
        main.where(
                new Plan.StringMatch(main.position("c_phone"), codes, false),
                new Plan.Predicate("gt_f64", main.column("c_acctbal"), main.column("a_average")))
                .groupBy("c_phone")
                .count()
                .aggregate("sum_f64", "c_acctbal");
        main.orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), -1));
        return new CompiledTpcdsQueries.Composite(
                List.of(new CompiledTpcdsQueries.Stage(average, "__average__")),
                main,
                List.of(new CompiledTpcdsQueries.DictRef(0, 0, 0)));
    }

    private static QueryLowering.Column f64Column(String name)
    {
        return new QueryLowering.Column(name, ColumnEncoding.F64, false);
    }
}
