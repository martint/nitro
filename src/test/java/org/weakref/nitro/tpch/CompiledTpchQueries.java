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

    private static QueryLowering.Column f64Column(String name)
    {
        return new QueryLowering.Column(name, ColumnEncoding.F64, false);
    }
}
