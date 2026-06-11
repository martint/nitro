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

    private static QueryLowering.Column f64Column(String name)
    {
        return new QueryLowering.Column(name, ColumnEncoding.F64, false);
    }
}
