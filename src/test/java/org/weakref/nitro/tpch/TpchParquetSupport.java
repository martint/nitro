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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.MultiStageOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.SemiJoinOperator;
import org.weakref.nitro.operator.SortOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.TrinoParquetScanOperator;
import org.weakref.nitro.operator.aggregation.AvgF64;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.aggregation.SumF64;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.MaskExpression;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * The TPC-H queries as Nitro operator trees, mirroring Trino's optimized logical plans
 * (target/tpch-explain, regenerate via {@link ExplainTpchQueries}). Single-threaded shape: distributed
 * exchanges and partial/final aggregation pairs collapse to one operator.
 */
final class TpchParquetSupport
{
    private TpchParquetSupport() {}

    /**
     * Q1: lineitem scanned once; ScanFilterProject(l_shipdate <= 1998-09-02, disc price and charge expressions)
     * feeding the (l_returnflag, l_linestatus) aggregation, sorted by the group keys.
     */
    public static Operator query01(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator lineitem = scannedTable(allocator, tables, "lineitem",
                "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_shipdate");
        Operator filtered = filter(allocator, primitiveRegistry, lineitem,
                lessThan(6, LocalDate.of(1998, 9, 2).toEpochDay() + 1));

        // [flag, status, qty, extprice, disc, discPrice, charge]
        Variable one = new Variable(0);
        Variable oneMinusDiscount = new Variable(1);
        Variable discPrice = new Variable(2);
        Variable onePlusTax = new Variable(3);
        Variable charge = new Variable(4);
        Operator projected = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(one, new Literal(1.0), AllMask.ALL),
                                new Assignment(oneMinusDiscount, new Call("subtract_f64", List.of(
                                        new Reference(one, Stream.VALUES),
                                        new Reference(new Input(4), Stream.VALUES))), AllMask.ALL),
                                new Assignment(discPrice, new Call("multiply_f64", List.of(
                                        new Reference(new Input(3), Stream.VALUES),
                                        new Reference(oneMinusDiscount, Stream.VALUES))), AllMask.ALL),
                                new Assignment(onePlusTax, new Call("add_f64", List.of(
                                        new Reference(new Input(5), Stream.VALUES),
                                        new Reference(one, Stream.VALUES))), AllMask.ALL),
                                new Assignment(charge, new Call("multiply_f64", List.of(
                                        new Reference(discPrice, Stream.VALUES),
                                        new Reference(onePlusTax, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(discPrice, Stream.VALUES),
                                new Reference(charge, Stream.VALUES))),
                primitiveRegistry,
                filtered);

        // [flag, status, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count]
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(
                        new SumF64(2),
                        new SumF64(3),
                        new SumF64(5),
                        new SumF64(6),
                        new AvgF64(2),
                        new AvgF64(3),
                        new AvgF64(4),
                        new CountAll()),
                projected);
        return new SortOperator(allocator, new int[] {0, 1}, new boolean[] {false, false}, aggregated);
    }

    /**
     * Q6: one ScanFilter over lineitem (shipdate year 1994, discount within [0.05, 0.07], quantity < 24),
     * projecting extendedprice * discount into a global sum.
     */
    public static Operator query06(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator lineitem = scannedTable(allocator, tables, "lineitem",
                "l_shipdate", "l_discount", "l_quantity", "l_extendedprice");
        Operator filtered = filter(allocator, primitiveRegistry, lineitem,
                and(
                        greaterThan(0, LocalDate.of(1994, 1, 1).toEpochDay() - 1),
                        lessThan(0, LocalDate.of(1995, 1, 1).toEpochDay()),
                        // SQL's 0.06 +/- 0.01 is DECIMAL arithmetic: the bounds are exactly 0.05 and 0.07.
                        compareF64("gte_f64", 1, 0.05),
                        compareF64("lte_f64", 1, 0.07),
                        compareF64("lt_f64", 2, 24.0)));

        Variable product = new Variable(0);
        Operator projected = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(product, new Call("multiply_f64", List.of(
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL)),
                        List.of(new Reference(product, Stream.VALUES))),
                primitiveRegistry,
                filtered);
        return new AggregationOperator(allocator, List.of(new SumF64(0)), projected);
    }

    /**
     * Q3: customer(BUILDING) builds against the date-filtered orders probe, that result builds against the
     * shipdate-filtered lineitem probe; aggregate revenue by (orderkey, orderdate, shippriority); TopN 10 by
     * (revenue DESC, orderdate).
     */
    public static Operator query03(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator customer = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "customer", "c_custkey", "c_mktsegment"),
                        equalUtf8(1, "BUILDING")),
                0);
        Operator orders = filter(allocator, primitiveRegistry,
                scannedTable(allocator, tables, "orders", "o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"),
                lessThan(2, LocalDate.of(1995, 3, 15).toEpochDay()));
        // [o_orderkey, o_custkey, o_orderdate, o_shippriority, c_custkey]
        Operator ordersWithCustomer = new HashJoinOperator(allocator, orders, 1, customer, 0);

        Operator lineitem = filter(allocator, primitiveRegistry,
                scannedTable(allocator, tables, "lineitem", "l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"),
                greaterThan(3, LocalDate.of(1995, 3, 15).toEpochDay()));
        // [l_orderkey, l_extendedprice, l_discount, l_shipdate, o_orderkey, o_custkey, o_orderdate, o_shippriority, c_custkey]
        Operator joined = new HashJoinOperator(allocator, lineitem, 0, ordersWithCustomer, 0);

        // [orderkey, orderdate, shippriority, discPrice]
        Operator projected = projectWithDiscPrice(allocator, primitiveRegistry, joined, 1, 2, 0, 6, 7);
        // [orderkey, orderdate, shippriority, revenue]
        Operator aggregated = new GroupedAggregationOperator(allocator, List.of(0, 1, 2), List.of(new SumF64(3)), projected);
        Operator top = new TopNOperator(allocator, 10, new int[] {3, 1}, new boolean[] {true, false}, aggregated);
        // SQL order: l_orderkey, revenue, o_orderdate, o_shippriority
        return projectInputs(allocator, primitiveRegistry, top, 0, 3, 1, 2);
    }

    /**
     * Q4: the quarter's orders semi-joined against late lineitems (commitdate < receiptdate); order counts by
     * priority, sorted by priority.
     */
    public static Operator query04(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator orders = filter(allocator, primitiveRegistry,
                scannedTable(allocator, tables, "orders", "o_orderkey", "o_orderdate", "o_orderpriority"),
                and(
                        greaterThan(1, LocalDate.of(1993, 7, 1).toEpochDay() - 1),
                        lessThan(1, LocalDate.of(1993, 10, 1).toEpochDay())));
        Operator lateLineitems = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "lineitem", "l_orderkey", "l_commitdate", "l_receiptdate"),
                        lessThanColumns(1, 2)),
                0);
        Operator matched = new SemiJoinOperator(allocator, orders, 0, lateLineitems, 0);
        Operator aggregated = new GroupedAggregationOperator(allocator, List.of(2), List.of(new CountAll()), matched);
        return new SortOperator(allocator, new int[] {0}, new boolean[] {false}, aggregated);
    }

    /**
     * Q5: region(ASIA) -> nation -> supplier build chain; customer -> orders(1994) -> lineitem probe chain;
     * the final join carries both l_suppkey = s_suppkey and c_nationkey = s_nationkey; revenue by n_name,
     * sorted descending.
     */
    public static Operator query05(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator region = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "region", "r_regionkey", "r_name"),
                        equalUtf8(1, "ASIA")),
                0);
        Operator nation = scannedTable(allocator, tables, "nation", "n_nationkey", "n_regionkey", "n_name");
        // [n_nationkey, n_regionkey, n_name, r_regionkey] -> [n_nationkey, n_name]
        Operator nationInAsia = projectInputs(allocator, primitiveRegistry,
                new HashJoinOperator(allocator, nation, 1, region, 0),
                0, 2);
        Operator supplier = scannedTable(allocator, tables, "supplier", "s_suppkey", "s_nationkey");
        // [s_suppkey, s_nationkey, n_nationkey, n_name] -> [s_suppkey, s_nationkey, n_name]
        Operator supplierInAsia = projectInputs(allocator, primitiveRegistry,
                new HashJoinOperator(allocator, supplier, 1, nationInAsia, 0),
                0, 1, 3);

        Operator customer = scannedTable(allocator, tables, "customer", "c_custkey", "c_nationkey");
        Operator orders = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "orders", "o_orderkey", "o_custkey", "o_orderdate"),
                        and(
                                greaterThan(2, LocalDate.of(1994, 1, 1).toEpochDay() - 1),
                                lessThan(2, LocalDate.of(1995, 1, 1).toEpochDay()))),
                0, 1);
        // [o_orderkey, o_custkey, c_custkey, c_nationkey] -> [o_orderkey, c_nationkey]
        Operator ordersWithNation = projectInputs(allocator, primitiveRegistry,
                new HashJoinOperator(allocator, orders, 1, customer, 0),
                0, 3);
        Operator lineitem = scannedTable(allocator, tables, "lineitem", "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount");
        // [l_orderkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, c_nationkey]
        Operator lineitemWithNation = new HashJoinOperator(allocator, lineitem, 0, ordersWithNation, 0);
        // + [s_suppkey, s_nationkey, n_name]
        Operator joined = new HashJoinOperator(allocator, lineitemWithNation, new int[] {1, 5}, supplierInAsia, new int[] {0, 1});

        // [n_name, discPrice]
        Operator projected = projectWithDiscPrice(allocator, primitiveRegistry, joined, 2, 3, 8);
        Operator aggregated = new GroupedAggregationOperator(allocator, List.of(0), List.of(new SumF64(1)), projected);
        return new SortOperator(allocator, new int[] {1}, new boolean[] {true}, aggregated);
    }

    /** Keep {@code keptInputs} and append extendedprice * (1 - discount) as the last output column. */
    private static Operator projectWithDiscPrice(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int extendedPriceIndex, int discountIndex, int... keptInputs)
    {
        Variable one = new Variable(0);
        Variable oneMinusDiscount = new Variable(1);
        Variable discPrice = new Variable(2);
        List<Reference> outputs = new ArrayList<>();
        for (int input : keptInputs) {
            outputs.add(new Reference(new Input(input), Stream.VALUES));
        }
        outputs.add(new Reference(discPrice, Stream.VALUES));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(one, new Literal(1.0), AllMask.ALL),
                                new Assignment(oneMinusDiscount, new Call("subtract_f64", List.of(
                                        new Reference(one, Stream.VALUES),
                                        new Reference(new Input(discountIndex), Stream.VALUES))), AllMask.ALL),
                                new Assignment(discPrice, new Call("multiply_f64", List.of(
                                        new Reference(new Input(extendedPriceIndex), Stream.VALUES),
                                        new Reference(oneMinusDiscount, Stream.VALUES))), AllMask.ALL)),
                        outputs),
                primitiveRegistry,
                source);
    }

    private static Operator projectInputs(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int... inputIndexes)
    {
        List<Reference> outputs = new ArrayList<>();
        for (int input : inputIndexes) {
            outputs.add(new Reference(new Input(input), Stream.VALUES));
        }
        return new ProjectOperator(allocator, new EvaluationPlan(List.of(), outputs), primitiveRegistry, source);
    }

    private static FilterSpec equalUtf8(int inputIndex, String constant)
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(equals, new Call("eq_utf8", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(equals, Stream.VALUES)));
    }

    private static FilterSpec lessThanColumns(int leftInputIndex, int rightInputIndex)
    {
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(result, new Call("lt", List.of(
                        new Reference(new Input(leftInputIndex), Stream.VALUES),
                        new Reference(new Input(rightInputIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    /**
     * Q10: orders(quarter) probes the customer build, the R-flagged lineitem probes that result, nation joins
     * last; revenue grouped over the seven customer output columns; TopN 20 by revenue.
     */
    public static Operator query10(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator customer = scannedTable(allocator, tables, "customer",
                "c_custkey", "c_name", "c_acctbal", "c_phone", "c_address", "c_comment", "c_nationkey");
        Operator orders = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "orders", "o_orderkey", "o_custkey", "o_orderdate"),
                        and(
                                greaterThan(2, LocalDate.of(1993, 10, 1).toEpochDay() - 1),
                                lessThan(2, LocalDate.of(1994, 1, 1).toEpochDay()))),
                0, 1);
        // [o_orderkey, o_custkey, c_custkey, c_name, c_acctbal, c_phone, c_address, c_comment, c_nationkey]
        Operator ordersWithCustomer = new HashJoinOperator(allocator, orders, 1, customer, 0);

        Operator lineitem = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "lineitem", "l_orderkey", "l_extendedprice", "l_discount", "l_returnflag"),
                        equalUtf8(3, "R")),
                0, 1, 2);
        // [l_orderkey, l_extendedprice, l_discount, o_orderkey, o_custkey, c_custkey, c_name, c_acctbal, c_phone, c_address, c_comment, c_nationkey]
        Operator joined = new HashJoinOperator(allocator, lineitem, 0, ordersWithCustomer, 0);
        Operator nation = scannedTable(allocator, tables, "nation", "n_nationkey", "n_name");
        // + [n_nationkey, n_name]
        joined = new HashJoinOperator(allocator, joined, 11, nation, 0);

        // [c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment, discPrice]
        Operator projected = projectWithDiscPrice(allocator, primitiveRegistry, joined, 1, 2, 5, 6, 7, 8, 13, 9, 10);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5, 6),
                List.of(new SumF64(7)),
                projected);
        Operator top = new TopNOperator(allocator, 20, new int[] {7}, new boolean[] {true}, aggregated);
        // SQL order: c_custkey, c_name, revenue, c_acctbal, n_name, c_address, c_phone, c_comment
        return projectInputs(allocator, primitiveRegistry, top, 0, 1, 7, 2, 4, 5, 3, 6);
    }

    /**
     * Q12: orders probes the date/shipmode-filtered lineitem build; per-shipmode sums of the priority CASE
     * buckets, sorted by shipmode.
     */
    public static Operator query12(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator lineitem = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "lineitem",
                                "l_orderkey", "l_shipmode", "l_commitdate", "l_receiptdate", "l_shipdate"),
                        and(
                                inUtf8(1, List.of("MAIL", "SHIP")),
                                lessThanColumns(2, 3),
                                lessThanColumns(4, 2),
                                greaterThan(3, LocalDate.of(1994, 1, 1).toEpochDay() - 1),
                                lessThan(3, LocalDate.of(1995, 1, 1).toEpochDay()))),
                0, 1);
        Operator orders = scannedTable(allocator, tables, "orders", "o_orderkey", "o_orderpriority");
        // [o_orderkey, o_orderpriority, l_orderkey, l_shipmode]
        Operator joined = new HashJoinOperator(allocator, orders, 0, lineitem, 0);

        // [shipmode, high, low]
        Variable urgent = new Variable(0);
        Variable high = new Variable(1);
        Variable isUrgent = new Variable(2);
        Variable zero = new Variable(3);
        Variable one = new Variable(4);
        Variable highBucket = new Variable(5);
        Variable lowBucket = new Variable(6);
        Operator projected = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(urgent, new Literal("1-URGENT"), AllMask.ALL),
                                new Assignment(high, new Literal("2-HIGH"), AllMask.ALL),
                                new Assignment(isUrgent, new Call("in_utf8", List.of(
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(urgent, Stream.VALUES),
                                        new Reference(high, Stream.VALUES))), AllMask.ALL),
                                new Assignment(zero, new Literal(0L), AllMask.ALL),
                                new Assignment(one, new Literal(1L), AllMask.ALL),
                                new Assignment(highBucket, new Call("if_i64", List.of(
                                        new Reference(isUrgent, Stream.VALUES),
                                        new Reference(one, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(lowBucket, new Call("if_i64", List.of(
                                        new Reference(isUrgent, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES),
                                        new Reference(one, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(highBucket, Stream.VALUES),
                                new Reference(lowBucket, Stream.VALUES))),
                primitiveRegistry,
                joined);
        Operator aggregated = new GroupedAggregationOperator(allocator, List.of(0), List.of(new Sum(1), new Sum(2)), projected);
        return new SortOperator(allocator, new int[] {0}, new boolean[] {false}, aggregated);
    }

    /**
     * Q14: the September-1995 lineitem probes the part build; promo and total revenue accumulate globally and
     * the single output row carries 100 * promo / total.
     */
    public static Operator query14(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator lineitem = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "lineitem", "l_partkey", "l_extendedprice", "l_discount", "l_shipdate"),
                        and(
                                greaterThan(3, LocalDate.of(1995, 9, 1).toEpochDay() - 1),
                                lessThan(3, LocalDate.of(1995, 10, 1).toEpochDay()))),
                0, 1, 2);
        Operator part = scannedTable(allocator, tables, "part", "p_partkey", "p_type");
        // [l_partkey, l_extendedprice, l_discount, p_partkey, p_type]
        Operator joined = new HashJoinOperator(allocator, lineitem, 0, part, 0);

        // [promoAmount, discPrice]
        Variable one = new Variable(0);
        Variable oneMinusDiscount = new Variable(1);
        Variable discPrice = new Variable(2);
        Variable promo = new Variable(3);
        Variable isPromo = new Variable(4);
        Variable zero = new Variable(5);
        Variable promoAmount = new Variable(6);
        Operator projected = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(one, new Literal(1.0), AllMask.ALL),
                                new Assignment(oneMinusDiscount, new Call("subtract_f64", List.of(
                                        new Reference(one, Stream.VALUES),
                                        new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                                new Assignment(discPrice, new Call("multiply_f64", List.of(
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(oneMinusDiscount, Stream.VALUES))), AllMask.ALL),
                                new Assignment(promo, new Literal("PROMO"), AllMask.ALL),
                                new Assignment(isPromo, new Call("starts_with_utf8", List.of(
                                        new Reference(new Input(4), Stream.VALUES),
                                        new Reference(promo, Stream.VALUES))), AllMask.ALL),
                                new Assignment(zero, new Literal(0.0), AllMask.ALL),
                                new Assignment(promoAmount, new Call("if_f64", List.of(
                                        new Reference(isPromo, Stream.VALUES),
                                        new Reference(discPrice, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(promoAmount, Stream.VALUES),
                                new Reference(discPrice, Stream.VALUES))),
                primitiveRegistry,
                joined);
        Operator aggregated = new AggregationOperator(allocator, List.of(new SumF64(0), new SumF64(1)), projected);

        // 100 * promo / total
        Variable hundred = new Variable(0);
        Variable ratio = new Variable(1);
        Variable promoRevenue = new Variable(2);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(hundred, new Literal(100.0), AllMask.ALL),
                                new Assignment(ratio, new Call("divide_f64", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                                new Assignment(promoRevenue, new Call("multiply_f64", List.of(
                                        new Reference(hundred, Stream.VALUES),
                                        new Reference(ratio, Stream.VALUES))), AllMask.ALL)),
                        List.of(new Reference(promoRevenue, Stream.VALUES))),
                primitiveRegistry,
                aggregated);
    }

    /**
     * Q19: the shipinstruct/shipmode-filtered lineitem probes the part build; the disjunctive brand /
     * container / quantity / size predicate runs post-join (the plan's join filter); revenue sums globally.
     */
    public static Operator query19(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator lineitem = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "lineitem",
                                "l_partkey", "l_quantity", "l_extendedprice", "l_discount", "l_shipinstruct", "l_shipmode"),
                        and(
                                equalUtf8(4, "DELIVER IN PERSON"),
                                inUtf8(5, List.of("AIR", "AIR REG")))),
                0, 1, 2, 3);
        Operator part = scannedTable(allocator, tables, "part", "p_partkey", "p_brand", "p_container", "p_size");
        // [l_partkey, l_quantity, l_extendedprice, l_discount, p_partkey, p_brand, p_container, p_size]
        Operator joined = new HashJoinOperator(allocator, lineitem, 0, part, 0);
        Operator filtered = filter(allocator, primitiveRegistry, joined,
                or(
                        query19Branch("Brand#12", List.of("SM CASE", "SM BOX", "SM PACK", "SM PKG"), 1.0, 11.0, 5),
                        query19Branch("Brand#23", List.of("MED BAG", "MED BOX", "MED PKG", "MED PACK"), 10.0, 20.0, 10),
                        query19Branch("Brand#34", List.of("LG CASE", "LG BOX", "LG PACK", "LG PKG"), 20.0, 30.0, 15)));
        Operator projected = projectWithDiscPrice(allocator, primitiveRegistry, filtered, 2, 3);
        return new AggregationOperator(allocator, List.of(new SumF64(0)), projected);
    }

    private static FilterSpec query19Branch(String brand, List<String> containers, double quantityLow, double quantityHigh, int sizeHigh)
    {
        return and(
                equalUtf8(5, brand),
                inUtf8(6, containers),
                compareF64("gte_f64", 1, quantityLow),
                compareF64("lte_f64", 1, quantityHigh),
                greaterThan(7, 0),
                lessThan(7, sizeHigh + 1));
    }

    private static FilterSpec inUtf8(int inputIndex, List<String> values)
    {
        List<String> sortedValues = values.stream().sorted().toList();
        List<Assignment> assignments = new ArrayList<>();
        for (int index = 0; index < sortedValues.size(); index++) {
            assignments.add(new Assignment(new Variable(index), new Literal(sortedValues.get(index)), AllMask.ALL));
        }
        Variable result = new Variable(sortedValues.size());
        List<Reference> arguments = new ArrayList<>(sortedValues.size() + 1);
        arguments.add(new Reference(new Input(inputIndex), Stream.VALUES));
        for (int index = 0; index < sortedValues.size(); index++) {
            arguments.add(new Reference(new Variable(index), Stream.VALUES));
        }
        assignments.add(new Assignment(result, new Call("in_utf8", arguments), AllMask.ALL));
        return new FilterSpec(new EvaluationPlan(assignments, List.of()), new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static FilterSpec or(FilterSpec first, FilterSpec... rest)
    {
        FilterSpec result = first;
        for (FilterSpec spec : rest) {
            result = combineBoolean("or", result, spec);
        }
        return result;
    }

    // ---- scan / filter plumbing ----

    private static Operator scannedTable(Allocator allocator, TpchParquetTables tables, String tableName, String... columns)
    {
        List<Path> files = tables.tableFiles(tableName);
        return new MultiStageOperator(columns.length, files, (Function<Path, Operator>) path -> new TrinoParquetScanOperator(allocator, path, List.of(columns)));
    }

    private static Operator filter(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, FilterSpec filterSpec)
    {
        return new FilterOperator(source, filterSpec.plan(), primitiveRegistry, filterSpec.predicate(), allocator);
    }

    record FilterSpec(EvaluationPlan plan, MaskExpression predicate) {}

    private static FilterSpec greaterThan(int inputIndex, long constant)
    {
        // The engine has only lt over I64: constant < input.
        Variable literal = new Variable(0);
        Variable result = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(result, new Call("lt", List.of(
                        new Reference(literal, Stream.VALUES),
                        new Reference(new Input(inputIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static FilterSpec lessThan(int inputIndex, long constant)
    {
        return comparison("lt", inputIndex, new Literal(constant));
    }

    private static FilterSpec compareF64(String function, int inputIndex, double constant)
    {
        return comparison(function, inputIndex, new Literal(constant));
    }

    private static FilterSpec comparison(String function, int inputIndex, Literal constant)
    {
        Variable literal = new Variable(0);
        Variable result = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, constant, AllMask.ALL),
                new Assignment(result, new Call(function, List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static FilterSpec and(FilterSpec first, FilterSpec... rest)
    {
        FilterSpec result = first;
        for (FilterSpec spec : rest) {
            result = combineBoolean("and", result, spec);
        }
        return result;
    }

    private static FilterSpec combineBoolean(String functionName, FilterSpec left, FilterSpec right)
    {
        int rightOffset = maxVariableId(left.plan().assignments()) + 1;
        List<Assignment> assignments = new ArrayList<>(left.plan().assignments());
        assignments.addAll(remap(right.plan().assignments(), rightOffset));

        Variable result = new Variable(maxVariableId(assignments) + 1);
        Reference leftReference = new Reference(left.plan().assignments().getLast().output(), Stream.VALUES);
        Reference rightReference = new Reference(new Variable(right.plan().assignments().getLast().output().id() + rightOffset), Stream.VALUES);
        assignments.add(new Assignment(result, new Call(functionName, List.of(leftReference, rightReference)), AllMask.ALL));
        return new FilterSpec(new EvaluationPlan(assignments, List.of()), new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static List<Assignment> remap(List<Assignment> assignments, int variableOffset)
    {
        return assignments.stream()
                .map(assignment -> new Assignment(
                        new Variable(assignment.output().id() + variableOffset),
                        remap(assignment.operation(), variableOffset),
                        assignment.mask()))
                .toList();
    }

    private static org.weakref.nitro.operator.evaluator.ir.Operation remap(org.weakref.nitro.operator.evaluator.ir.Operation operation, int variableOffset)
    {
        return switch (operation) {
            case Literal literal -> literal;
            case Call(String functionName, List<Reference> arguments) -> new Call(functionName, arguments.stream()
                    .map(argument -> remap(argument, variableOffset))
                    .toList());
            default -> throw new IllegalArgumentException("Unsupported operation in filter spec: " + operation);
        };
    }

    private static Reference remap(Reference reference, int variableOffset)
    {
        return switch (reference.producer()) {
            case Input ignored -> reference;
            case Variable variable -> new Reference(new Variable(variable.id() + variableOffset), reference.stream());
            default -> reference;
        };
    }

    private static int maxVariableId(List<Assignment> assignments)
    {
        return assignments.stream()
                .mapToInt(assignment -> assignment.output().id())
                .max()
                .orElse(-1);
    }
}
