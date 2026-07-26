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

import org.weakref.nitro.benchmark.BenchmarkSchemaRegistry;
import org.weakref.nitro.benchmark.BenchmarkTypeRegistry;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.DistinctCount;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.SemiJoinOperator;
import org.weakref.nitro.operator.SortOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.aggregation.AvgF64;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.CountColumn;
import org.weakref.nitro.operator.aggregation.Max;
import org.weakref.nitro.operator.aggregation.MaxF64;
import org.weakref.nitro.operator.aggregation.Min;
import org.weakref.nitro.operator.aggregation.MinF64;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.aggregation.SumF64;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.AndMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.MaskExpression;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.OrMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.operator.source.BatchSourceOperator;
import org.weakref.nitro.operator.source.compatibility.NativeSourceOperatorIngress;
import org.weakref.nitro.operator.source.compatibility.OperatorBatchSource;
import org.weakref.nitro.operator.source.compatibility.parquet.NitroParquetScanOperator;
import org.weakref.nitro.tpcds.OperatorCpuProfile;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * The TPC-H queries as Nitro operator trees, mirroring Trino's optimized logical plans
 * (target/tpch-explain, regenerate via {@link ExplainTpchQueries}). Single-threaded shape: distributed
 * exchanges and partial/final aggregation pairs collapse to one operator.
 */
final class TpchParquetSupport
{
    private static final BenchmarkSchemaRegistry SCHEMAS = new BenchmarkSchemaRegistry(new BenchmarkTypeRegistry());
    private static final boolean QUERY08_VELOX_JOIN_SHAPE =
            Boolean.parseBoolean(System.getProperty("nitro.tpch.query08VeloxJoinShape", "true"));

    private TpchParquetSupport() {}

    private static Operator profiled(OperatorCpuProfile profile, String name, Operator operator)
    {
        return profile == null ? operator : profile.wrap(name, operator);
    }

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
        long startDate = LocalDate.of(1994, 1, 1).toEpochDay();
        long endDate = LocalDate.of(1994, 12, 31).toEpochDay();
        Operator lineitem = scannedTableWithLongRange(allocator, tables, "lineitem", 0, startDate, endDate,
                "l_shipdate", "l_discount", "l_quantity", "l_extendedprice");
        Operator filtered = filter(allocator, primitiveRegistry, lineitem,
                and(
                        greaterThan(0, startDate - 1),
                        lessThan(0, endDate + 1),
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
        long startDate = LocalDate.of(1993, 7, 1).toEpochDay();
        long endDate = LocalDate.of(1993, 9, 30).toEpochDay();
        Operator orders = filter(allocator, primitiveRegistry,
                scannedTableWithLongRange(allocator, tables, "orders", 1, startDate, endDate,
                        "o_orderkey", "o_orderdate", "o_orderpriority"),
                and(
                        greaterThan(1, startDate - 1),
                        lessThan(1, endDate + 1)));
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
        return query05(allocator, primitiveRegistry, tables, null);
    }

    static Operator query05(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, OperatorCpuProfile profile)
    {
        Operator region = profiled(profile, "q05.project.region", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q05.filter.region", filter(allocator, primitiveRegistry,
                        profiled(profile, "q05.scan.region", scannedTable(allocator, tables, "region", "r_regionkey", "r_name")),
                        equalUtf8(1, "ASIA"))),
                0));
        Operator nation = profiled(profile, "q05.scan.nation", scannedTable(allocator, tables, "nation", "n_nationkey", "n_regionkey", "n_name"));
        // [n_nationkey, n_regionkey, n_name, r_regionkey] -> [n_nationkey, n_name]
        Operator nationInAsia = profiled(profile, "q05.project.nation", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q05.join.region", new HashJoinOperator(allocator, nation, 1, region, 0)),
                0, 2));
        Operator supplier = profiled(profile, "q05.scan.supplier", scannedTable(allocator, tables, "supplier", "s_suppkey", "s_nationkey"));
        // [s_suppkey, s_nationkey, n_nationkey, n_name] -> [s_suppkey, s_nationkey, n_name]
        Operator supplierInAsia = profiled(profile, "q05.project.supplier", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q05.join.nation", new HashJoinOperator(allocator, supplier, 1, nationInAsia, 0)),
                0, 1, 3));

        Operator customer = profiled(profile, "q05.scan.customer", scannedTable(allocator, tables, "customer", "c_custkey", "c_nationkey"));
        Operator orders = profiled(profile, "q05.project.orders", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q05.filter.orders", filter(allocator, primitiveRegistry,
                        profiled(profile, "q05.scan.orders", scannedTable(allocator, tables, "orders", "o_orderkey", "o_custkey", "o_orderdate")),
                        and(
                                greaterThan(2, LocalDate.of(1994, 1, 1).toEpochDay() - 1),
                                lessThan(2, LocalDate.of(1995, 1, 1).toEpochDay())))),
                0, 1));
        // [o_orderkey, o_custkey, c_custkey, c_nationkey] -> [o_orderkey, c_nationkey]
        Operator ordersWithNation = profiled(profile, "q05.project.customer", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q05.join.customer", new HashJoinOperator(allocator, orders, 1, customer, 0)),
                0, 3));
        Operator lineitem = profiled(profile, "q05.scan.lineitem", scannedTable(allocator, tables, "lineitem", "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"));
        // [l_orderkey, l_suppkey, l_extendedprice, l_discount, o_orderkey, c_nationkey]
        Operator lineitemWithNation = profiled(profile, "q05.join.orders", new HashJoinOperator(allocator, lineitem, 0, ordersWithNation, 0));
        // + [s_suppkey, s_nationkey, n_name]
        Operator joined = profiled(profile, "q05.join.supplier", new HashJoinOperator(allocator, lineitemWithNation, new int[] {1, 5}, supplierInAsia, new int[] {0, 1}));

        // [n_name, discPrice]
        Operator projected = profiled(profile, "q05.project.revenue", projectWithDiscPrice(allocator, primitiveRegistry, joined, 2, 3, 8));
        Operator aggregated = profiled(profile, "q05.group.nation", new GroupedAggregationOperator(allocator, List.of(0), List.of(new SumF64(1)), projected));
        return profiled(profile, "q05.sort", new SortOperator(allocator, new int[] {1}, new boolean[] {true}, aggregated));
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

    private static FilterSpec equalColumns(int leftInputIndex, int rightInputIndex)
    {
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(result, new Call("eq", List.of(
                        new Reference(new Input(leftInputIndex), Stream.VALUES),
                        new Reference(new Input(rightInputIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    /**
     * Q10: customer probes the quarter-filtered orders build, that result probes the R-flagged lineitem build,
     * and nation joins last; revenue is grouped over the seven customer output columns and TopN selects 20.
     * This build/probe order mirrors Trino's optimized SQL plan.
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
        // [c_custkey, c_name, c_acctbal, c_phone, c_address, c_comment, c_nationkey, o_orderkey, o_custkey]
        Operator customerWithOrders = new HashJoinOperator(allocator, customer, 0, orders, 1);

        Operator lineitem = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "lineitem", "l_orderkey", "l_extendedprice", "l_discount", "l_returnflag"),
                        equalUtf8(3, "R")),
                0, 1, 2);
        // [c_custkey, c_name, c_acctbal, c_phone, c_address, c_comment, c_nationkey, o_orderkey, o_custkey,
        //  l_orderkey, l_extendedprice, l_discount]
        Operator joined = new HashJoinOperator(allocator, customerWithOrders, 7, lineitem, 0);
        Operator nation = scannedTable(allocator, tables, "nation", "n_nationkey", "n_name");
        // + [n_nationkey, n_name]
        joined = new HashJoinOperator(allocator, joined, 6, nation, 0);

        // [c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment, discPrice]
        Operator projected = projectWithDiscPrice(allocator, primitiveRegistry, joined, 10, 11, 0, 1, 2, 3, 13, 4, 5);
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
        return query12(allocator, primitiveRegistry, tables, null);
    }

    static Operator query12(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, OperatorCpuProfile profile)
    {
        Operator lineitem = profiled(profile, "q12.project.lineitem", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q12.filter.lineitem", filter(allocator, primitiveRegistry,
                        profiled(profile, "q12.scan.lineitem", scannedTable(allocator, tables, "lineitem",
                                "l_orderkey", "l_shipmode", "l_commitdate", "l_receiptdate", "l_shipdate")),
                        and(
                                inUtf8(1, List.of("MAIL", "SHIP")),
                                lessThanColumns(2, 3),
                                lessThanColumns(4, 2),
                                greaterThan(3, LocalDate.of(1994, 1, 1).toEpochDay() - 1),
                                lessThan(3, LocalDate.of(1995, 1, 1).toEpochDay())))),
                0, 1));
        Operator orders = profiled(profile, "q12.scan.orders", scannedTable(allocator, tables, "orders", "o_orderkey", "o_orderpriority"));
        // [o_orderkey, o_orderpriority, l_orderkey, l_shipmode]
        Operator joined = profiled(profile, "q12.join.lineitem", new HashJoinOperator(allocator, orders, 0, lineitem, 0));

        // [shipmode, high, low]
        Variable urgent = new Variable(0);
        Variable high = new Variable(1);
        Variable isUrgent = new Variable(2);
        Variable zero = new Variable(3);
        Variable one = new Variable(4);
        Variable highBucket = new Variable(5);
        Variable lowBucket = new Variable(6);
        Operator projected = profiled(profile, "q12.project.buckets", new ProjectOperator(
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
                joined));
        Operator aggregated = profiled(profile, "q12.group.shipmode", new GroupedAggregationOperator(allocator, List.of(0), List.of(new Sum(1), new Sum(2)), projected));
        return profiled(profile, "q12.sort", new SortOperator(allocator, new int[] {0}, new boolean[] {false}, aggregated));
    }

    /**
     * Q14: the September-1995 lineitem probes the part build; promo and total revenue accumulate globally and
     * the single output row carries 100 * promo / total.
     */
    public static Operator query14(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        long startDate = LocalDate.of(1995, 9, 1).toEpochDay();
        long endDate = LocalDate.of(1995, 9, 30).toEpochDay();
        Operator lineitem = projectWithDiscPrice(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTableWithLongRange(allocator, tables, "lineitem", 3, startDate, endDate,
                                "l_partkey", "l_extendedprice", "l_discount", "l_shipdate"),
                        and(
                                greaterThan(3, startDate - 1),
                                lessThan(3, endDate + 1))),
                1, 2, 0);
        Operator part = scannedTable(allocator, tables, "part", "p_partkey", "p_type");
        // Match Velox's boundary: [part_revenue, p_type].
        Operator joined = new HashJoinOperator(allocator, lineitem, 0, part, 0)
                .withOutputs(1, 3);

        // [promoAmount, discPrice]
        Variable promo = new Variable(0);
        Variable isPromo = new Variable(1);
        Variable zero = new Variable(2);
        Variable promoAmount = new Variable(3);
        Operator projected = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(promo, new Literal("PROMO"), AllMask.ALL),
                                new Assignment(isPromo, new Call("starts_with_utf8", List.of(
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(promo, Stream.VALUES))), AllMask.ALL),
                                new Assignment(zero, new Literal(0.0), AllMask.ALL),
                                new Assignment(promoAmount, new Call("if_f64", List.of(
                                        new Reference(isPromo, Stream.VALUES),
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(promoAmount, Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES))),
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
        return query19(allocator, primitiveRegistry, tables, null);
    }

    static Operator query19(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, OperatorCpuProfile profile)
    {
        if (!Boolean.parseBoolean(System.getProperty("nitro.tpch.query19JoinResidual", "true"))) {
            return query19PostJoinFilter(allocator, primitiveRegistry, tables);
        }
        Operator lineitem = profiled(profile, "q19.project.revenue", projectWithDiscPrice(allocator, primitiveRegistry,
                profiled(profile, "q19.filter.lineitem", filter(allocator, primitiveRegistry,
                        profiled(profile, "q19.scan.lineitem", scannedTable(allocator, tables, "lineitem",
                                "l_partkey", "l_quantity", "l_extendedprice", "l_discount", "l_shipinstruct", "l_shipmode")),
                        and(
                                equalUtf8(4, "DELIVER IN PERSON"),
                                inUtf8(5, List.of("AIR", "AIR REG"))))),
                2, 3, 0, 1));
        lineitem = profiled(profile, "q19.project.lineitem_mask", projectQuery19BranchMask(
                allocator,
                primitiveRegistry,
                lineitem,
                new int[] {0, 1, 2},
                and(compareF64("gte_f64", 1, 1.0), compareF64("lte_f64", 1, 11.0)),
                and(compareF64("gte_f64", 1, 10.0), compareF64("lte_f64", 1, 20.0)),
                and(compareF64("gte_f64", 1, 20.0), compareF64("lte_f64", 1, 30.0))));
        Operator part = profiled(profile, "q19.scan.part", scannedTable(allocator, tables, "part", "p_partkey", "p_brand", "p_container", "p_size"));
        part = profiled(profile, "q19.project.part_mask", projectQuery19BranchMask(
                allocator,
                primitiveRegistry,
                part,
                new int[] {0},
                query19PartBranch("Brand#12", List.of("SM CASE", "SM BOX", "SM PACK", "SM PKG"), 5),
                query19PartBranch("Brand#23", List.of("MED BAG", "MED BOX", "MED PKG", "MED PACK"), 10),
                query19PartBranch("Brand#34", List.of("LG CASE", "LG BOX", "LG PACK", "LG PKG"), 15)));
        // [l_partkey, l_quantity, part_revenue, lineitem_branch_mask, p_partkey, part_branch_mask]
        Operator joined = profiled(profile, "q19.join.part", new HashJoinOperator(
                allocator,
                lineitem,
                0,
                part,
                0,
                HashJoinOperator.JoinFilter.longBitwiseOverlap(3, 1)));
        Operator projected = profiled(profile, "q19.project.final", projectInputs(allocator, primitiveRegistry, joined, 2));
        return profiled(profile, "q19.aggregate.final", new AggregationOperator(allocator, List.of(new SumF64(0)), projected));
    }

    private static Operator query19PostJoinFilter(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator lineitem = projectWithDiscPrice(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "lineitem",
                                "l_partkey", "l_quantity", "l_extendedprice", "l_discount", "l_shipinstruct", "l_shipmode"),
                        and(
                                equalUtf8(4, "DELIVER IN PERSON"),
                                inUtf8(5, List.of("AIR", "AIR REG")))),
                2, 3, 0, 1);
        Operator part = scannedTable(allocator, tables, "part", "p_partkey", "p_brand", "p_container", "p_size");
        Operator joined = new HashJoinOperator(allocator, lineitem, 0, part, 0);
        Operator filtered = filter(allocator, primitiveRegistry, joined,
                or(
                        query19PostJoinBranch("Brand#12", List.of("SM CASE", "SM BOX", "SM PACK", "SM PKG"), 1.0, 11.0, 5),
                        query19PostJoinBranch("Brand#23", List.of("MED BAG", "MED BOX", "MED PKG", "MED PACK"), 10.0, 20.0, 10),
                        query19PostJoinBranch("Brand#34", List.of("LG CASE", "LG BOX", "LG PACK", "LG PKG"), 20.0, 30.0, 15)));
        return new AggregationOperator(
                allocator,
                List.of(new SumF64(0)),
                projectInputs(allocator, primitiveRegistry, filtered, 2));
    }

    private static FilterSpec query19PostJoinBranch(String brand, List<String> containers, double quantityLow, double quantityHigh, int sizeHigh)
    {
        return and(
                equalUtf8(4, brand),
                inUtf8(5, containers),
                compareF64("gte_f64", 1, quantityLow),
                compareF64("lte_f64", 1, quantityHigh),
                greaterThan(6, 0),
                lessThan(6, sizeHigh + 1));
    }

    private static FilterSpec query19PartBranch(String brand, List<String> containers, int sizeHigh)
    {
        return and(
                equalUtf8(1, brand),
                inUtf8(2, containers),
                greaterThan(3, 0),
                lessThan(3, sizeHigh + 1));
    }

    private static Operator projectQuery19BranchMask(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            Operator source,
            int[] retainedInputs,
            FilterSpec... branches)
    {
        List<Assignment> assignments = new ArrayList<>();
        List<Reference> branchPredicates = new ArrayList<>();
        for (FilterSpec branch : branches) {
            int offset = maxVariableId(assignments) + 1;
            List<Assignment> remapped = remap(branch.plan().assignments(), offset);
            assignments.addAll(remapped);
            branchPredicates.add(remap(branch.materializedValue(), offset));
        }

        Variable zero = new Variable(maxVariableId(assignments) + 1);
        assignments.add(new Assignment(zero, new Literal(0L), AllMask.ALL));
        List<Reference> branchBits = new ArrayList<>();
        for (int index = 0; index < branchPredicates.size(); index++) {
            Variable bit = new Variable(maxVariableId(assignments) + 1);
            assignments.add(new Assignment(bit, new Literal(1L << index), AllMask.ALL));
            Variable selectedBit = new Variable(maxVariableId(assignments) + 1);
            assignments.add(new Assignment(selectedBit, new Call("if_i64", List.of(
                    branchPredicates.get(index),
                    new Reference(bit, Stream.VALUES),
                    new Reference(zero, Stream.VALUES))), AllMask.ALL));
            branchBits.add(new Reference(selectedBit, Stream.VALUES));
        }
        Reference branchMask = branchBits.getFirst();
        for (int index = 1; index < branchBits.size(); index++) {
            Variable combined = new Variable(maxVariableId(assignments) + 1);
            assignments.add(new Assignment(combined, new Call("add", List.of(branchMask, branchBits.get(index))), AllMask.ALL));
            branchMask = new Reference(combined, Stream.VALUES);
        }

        List<Reference> outputs = new ArrayList<>();
        for (int input : retainedInputs) {
            outputs.add(new Reference(new Input(input), Stream.VALUES));
        }
        outputs.add(branchMask);
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
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
            result = combineOr(result, spec);
        }
        return result;
    }

    /**
     * Q7: lineitem(1995-1996) probes supplier, orders, customer, then the two nation builds; the
     * FRANCE/GERMANY pairing runs post-join; volume grouped by (supp_nation, cust_nation, year(shipdate)).
     */
    public static Operator query07(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        return query07(allocator, primitiveRegistry, tables, null);
    }

    static Operator query07(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, OperatorCpuProfile profile)
    {
        Operator lineitem = profiled(profile, "q07.filter.lineitem", filter(allocator, primitiveRegistry,
                profiled(profile, "q07.scan.lineitem", scannedTable(allocator, tables, "lineitem",
                        "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount", "l_shipdate")),
                and(
                        greaterThan(4, LocalDate.of(1995, 1, 1).toEpochDay() - 1),
                        lessThan(4, LocalDate.of(1996, 12, 31).toEpochDay() + 1))));
        Operator supplier = profiled(profile, "q07.scan.supplier", scannedTable(allocator, tables, "supplier", "s_suppkey", "s_nationkey"));
        // Match the native plan's physical boundary after every join: only the five columns consumed downstream
        // remain visible. withOutputs is integrated join-output pruning, not a separate projection operator.
        Operator joined = profiled(profile, "q07.join.supplier", new HashJoinOperator(allocator, lineitem, 1, supplier, 0)
                .withOutputs(6, 0, 2, 3, 4));
        Operator orders = profiled(profile, "q07.scan.orders", scannedTable(allocator, tables, "orders", "o_orderkey", "o_custkey"));
        joined = profiled(profile, "q07.join.orders", new HashJoinOperator(allocator, joined, 1, orders, 0)
                .withOutputs(6, 0, 2, 3, 4));
        Operator customer = profiled(profile, "q07.scan.customer", scannedTable(allocator, tables, "customer", "c_custkey", "c_nationkey"));
        joined = profiled(profile, "q07.join.customer", new HashJoinOperator(allocator, joined, 0, customer, 0)
                .withOutputs(6, 1, 2, 3, 4));
        Operator supplierNation = profiled(profile, "q07.filter.supplier_nation", filter(allocator, primitiveRegistry,
                profiled(profile, "q07.scan.supplier_nation", scannedTable(allocator, tables, "nation", "n_nationkey", "n_name")),
                inUtf8(1, List.of("FRANCE", "GERMANY"))));
        joined = profiled(profile, "q07.join.supplier_nation", new HashJoinOperator(allocator, joined, 1, supplierNation, 0)
                .withOutputs(6, 0, 2, 3, 4));
        Operator customerNation = profiled(profile, "q07.filter.customer_nation", filter(allocator, primitiveRegistry,
                profiled(profile, "q07.scan.customer_nation", scannedTable(allocator, tables, "nation", "n_nationkey", "n_name")),
                inUtf8(1, List.of("FRANCE", "GERMANY"))));
        // [supp_nation, customer_nation_key, extendedprice, discount, shipdate] + [nation_key, cust_nation]
        joined = profiled(profile, "q07.join.customer_nation", new HashJoinOperator(
                allocator,
                joined,
                1,
                customerNation,
                0,
                HashJoinOperator.JoinFilter.binaryNotEquals(0, 1))
                .withOutputs(0, 6, 2, 3, 4));

        // [supp_nation, cust_nation, l_year, volume]
        Variable one = new Variable(0);
        Variable oneMinusDiscount = new Variable(1);
        Variable volume = new Variable(2);
        Variable year = new Variable(3);
        Operator projected = profiled(profile, "q07.project", new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(one, new Literal(1.0), AllMask.ALL),
                                new Assignment(oneMinusDiscount, new Call("subtract_f64", List.of(
                                        new Reference(one, Stream.VALUES),
                                        new Reference(new Input(3), Stream.VALUES))), AllMask.ALL),
                                new Assignment(volume, new Call("multiply_f64", List.of(
                                        new Reference(new Input(2), Stream.VALUES),
                                        new Reference(oneMinusDiscount, Stream.VALUES))), AllMask.ALL),
                                new Assignment(year, new Call("year_of_date", List.of(
                                        new Reference(new Input(4), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(year, Stream.VALUES),
                                new Reference(volume, Stream.VALUES))),
                primitiveRegistry,
                joined));
        Operator aggregated = profiled(profile, "q07.group", new GroupedAggregationOperator(allocator, List.of(0, 1, 2), List.of(new SumF64(3)), projected));
        return profiled(profile, "q07.sort", new SortOperator(allocator, new int[] {0, 1, 2}, new boolean[] {false, false, false}, aggregated));
    }

    /**
     * Q8: part(ECONOMY ANODIZED STEEL) restricts lineitem; orders(1995-1996), customer, the AMERICA region
     * chain on the customer nation, and the supplier nation provide the CASE source; one output row per year
     * carries sum(brazil volume) / sum(volume).
     */
    public static Operator query08(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        if (QUERY08_VELOX_JOIN_SHAPE) {
            return query08VeloxJoinShape(allocator, primitiveRegistry, tables);
        }
        Operator part = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "part", "p_partkey", "p_type"),
                        equalUtf8(1, "ECONOMY ANODIZED STEEL")),
                0);
        Operator lineitem = scannedTable(allocator, tables, "lineitem",
                "l_partkey", "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount");
        Operator joined = new HashJoinOperator(allocator, lineitem, 0, part, 0);
        Operator orders = filter(allocator, primitiveRegistry,
                scannedTable(allocator, tables, "orders", "o_orderkey", "o_custkey", "o_orderdate"),
                and(
                        greaterThan(2, LocalDate.of(1995, 1, 1).toEpochDay() - 1),
                        lessThan(2, LocalDate.of(1996, 12, 31).toEpochDay() + 1)));
        // [l x5, p_partkey, o_orderkey, o_custkey, o_orderdate -> 6,7,8]
        joined = new HashJoinOperator(allocator, joined, 1, orders, 0);
        Operator customer = scannedTable(allocator, tables, "customer", "c_custkey", "c_nationkey");
        // + [c_custkey, c_nationkey -> 9,10]
        joined = new HashJoinOperator(allocator, joined, 7, customer, 0);

        Operator region = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "region", "r_regionkey", "r_name"),
                        equalUtf8(1, "AMERICA")),
                0);
        Operator nation = scannedTable(allocator, tables, "nation", "n_nationkey", "n_regionkey");
        Operator nationInAmerica = projectInputs(allocator, primitiveRegistry,
                new HashJoinOperator(allocator, nation, 1, region, 0),
                0);
        // + [n_nationkey -> 11] (customer nation, AMERICA restriction)
        joined = new HashJoinOperator(allocator, joined, 10, nationInAmerica, 0);
        Operator supplierNation = scannedTable(allocator, tables, "nation", "n_nationkey", "n_name");
        Operator supplier = scannedTable(allocator, tables, "supplier", "s_suppkey", "s_nationkey");
        Operator supplierWithNation = projectInputs(allocator, primitiveRegistry,
                new HashJoinOperator(allocator, supplier, 1, supplierNation, 0),
                0, 3);
        // + [s_suppkey, n_name -> 12,13]
        joined = new HashJoinOperator(allocator, joined, 2, supplierWithNation, 0);

        // [o_year, brazilVolume, volume]
        Variable one = new Variable(0);
        Variable oneMinusDiscount = new Variable(1);
        Variable volume = new Variable(2);
        Variable year = new Variable(3);
        Variable brazil = new Variable(4);
        Variable isBrazil = new Variable(5);
        Variable zero = new Variable(6);
        Variable brazilVolume = new Variable(7);
        Operator projected = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(one, new Literal(1.0), AllMask.ALL),
                                new Assignment(oneMinusDiscount, new Call("subtract_f64", List.of(
                                        new Reference(one, Stream.VALUES),
                                        new Reference(new Input(4), Stream.VALUES))), AllMask.ALL),
                                new Assignment(volume, new Call("multiply_f64", List.of(
                                        new Reference(new Input(3), Stream.VALUES),
                                        new Reference(oneMinusDiscount, Stream.VALUES))), AllMask.ALL),
                                new Assignment(year, new Call("year_of_date", List.of(
                                        new Reference(new Input(8), Stream.VALUES))), AllMask.ALL),
                                new Assignment(brazil, new Literal("BRAZIL"), AllMask.ALL),
                                new Assignment(isBrazil, new Call("eq_utf8", List.of(
                                        new Reference(new Input(13), Stream.VALUES),
                                        new Reference(brazil, Stream.VALUES))), AllMask.ALL),
                                new Assignment(zero, new Literal(0.0), AllMask.ALL),
                                new Assignment(brazilVolume, new Call("if_f64", List.of(
                                        new Reference(isBrazil, Stream.VALUES),
                                        new Reference(volume, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(year, Stream.VALUES),
                                new Reference(brazilVolume, Stream.VALUES),
                                new Reference(volume, Stream.VALUES))),
                primitiveRegistry,
                joined);
        Operator aggregated = new GroupedAggregationOperator(allocator, List.of(0), List.of(new SumF64(1), new SumF64(2)), projected);
        Operator sorted = new SortOperator(allocator, new int[] {0}, new boolean[] {false}, aggregated);

        Variable share = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(share, new Call("divide_f64", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(share, Stream.VALUES))),
                primitiveRegistry,
                sorted);
    }

    private static Operator query08VeloxJoinShape(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator region = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "region", "r_regionkey", "r_name"),
                        equalUtf8(1, "AMERICA")),
                0);
        Operator nationInAmerica = new HashJoinOperator(
                allocator,
                scannedTable(allocator, tables, "nation", "n_nationkey", "n_regionkey"),
                1,
                region,
                0)
                .withOutputs(0);
        Operator customersInAmerica = new HashJoinOperator(
                allocator,
                scannedTable(allocator, tables, "customer", "c_custkey", "c_nationkey"),
                1,
                nationInAmerica,
                0)
                .withOutputs(0);
        Operator ordersInAmerica = new HashJoinOperator(
                allocator,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "orders", "o_orderkey", "o_custkey", "o_orderdate"),
                        and(
                                greaterThan(2, LocalDate.of(1995, 1, 1).toEpochDay() - 1),
                                lessThan(2, LocalDate.of(1996, 12, 31).toEpochDay() + 1))),
                1,
                customersInAmerica,
                0)
                .withOutputs(0, 2);

        Operator supplierWithNation = new HashJoinOperator(
                allocator,
                scannedTable(allocator, tables, "supplier", "s_suppkey", "s_nationkey"),
                1,
                scannedTable(allocator, tables, "nation", "n_nationkey", "n_name"),
                0)
                .withOutputs(0, 3);
        Operator part = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "part", "p_partkey", "p_type"),
                        equalUtf8(1, "ECONOMY ANODIZED STEEL")),
                0);

        Operator joined = new HashJoinOperator(
                allocator,
                scannedTable(allocator, tables, "lineitem", "l_partkey", "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"),
                1,
                ordersInAmerica,
                0)
                .withOutputs(0, 2, 6, 3, 4);
        joined = new HashJoinOperator(allocator, joined, 1, supplierWithNation, 0)
                .withOutputs(6, 2, 0, 3, 4);
        joined = new HashJoinOperator(allocator, joined, 2, part, 0)
                .withOutputs(0, 1, 3, 4);

        return query08Finish(allocator, primitiveRegistry, joined, 0, 1, 2, 3);
    }

    private static Operator query08Finish(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            Operator joined,
            int nationNameIndex,
            int orderDateIndex,
            int extendedPriceIndex,
            int discountIndex)
    {
        Variable one = new Variable(0);
        Variable oneMinusDiscount = new Variable(1);
        Variable volume = new Variable(2);
        Variable year = new Variable(3);
        Variable brazil = new Variable(4);
        Variable isBrazil = new Variable(5);
        Variable zero = new Variable(6);
        Variable brazilVolume = new Variable(7);
        Operator projected = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(one, new Literal(1.0), AllMask.ALL),
                                new Assignment(oneMinusDiscount, new Call("subtract_f64", List.of(
                                        new Reference(one, Stream.VALUES),
                                        new Reference(new Input(discountIndex), Stream.VALUES))), AllMask.ALL),
                                new Assignment(volume, new Call("multiply_f64", List.of(
                                        new Reference(new Input(extendedPriceIndex), Stream.VALUES),
                                        new Reference(oneMinusDiscount, Stream.VALUES))), AllMask.ALL),
                                new Assignment(year, new Call("year_of_date", List.of(
                                        new Reference(new Input(orderDateIndex), Stream.VALUES))), AllMask.ALL),
                                new Assignment(brazil, new Literal("BRAZIL"), AllMask.ALL),
                                new Assignment(isBrazil, new Call("eq_utf8", List.of(
                                        new Reference(new Input(nationNameIndex), Stream.VALUES),
                                        new Reference(brazil, Stream.VALUES))), AllMask.ALL),
                                new Assignment(zero, new Literal(0.0), AllMask.ALL),
                                new Assignment(brazilVolume, new Call("if_f64", List.of(
                                        new Reference(isBrazil, Stream.VALUES),
                                        new Reference(volume, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(year, Stream.VALUES),
                                new Reference(brazilVolume, Stream.VALUES),
                                new Reference(volume, Stream.VALUES))),
                primitiveRegistry,
                joined);
        Operator aggregated = new GroupedAggregationOperator(allocator, List.of(0), List.of(new SumF64(1), new SumF64(2)), projected);
        Operator sorted = new SortOperator(allocator, new int[] {0}, new boolean[] {false}, aggregated);

        Variable share = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(share, new Call("divide_f64", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(share, Stream.VALUES))),
                primitiveRegistry,
                sorted);
    }

    /**
     * Q9: part('%green%') restricts lineitem; supplier, the (suppkey, partkey) partsupp join, orders, and
     * nation provide profit = volume - supplycost * quantity, grouped by (nation, year(orderdate)) and
     * sorted (nation, year DESC).
     */
    public static Operator query09(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        return query09(allocator, primitiveRegistry, tables, null);
    }

    static Operator query09(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, OperatorCpuProfile profile)
    {
        boolean integratedJoinOutputs = Boolean.parseBoolean(System.getProperty("nitro.tpch.query09IntegratedJoinOutputs", "false"));
        Operator part = profiled(profile, "q09.filter.part", projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        profiled(profile, "q09.scan.part", scannedTable(allocator, tables, "part", "p_partkey", "p_name")),
                        containsUtf8(1, "green")),
                0));
        Operator lineitem = profiled(profile, "q09.scan.lineitem", scannedTable(allocator, tables, "lineitem",
                "l_partkey", "l_orderkey", "l_suppkey", "l_quantity", "l_extendedprice", "l_discount"));
        HashJoinOperator partJoin = new HashJoinOperator(allocator, lineitem, 0, part, 0)
                .withProfileName("q09.join.part");
        Operator joined = profiled(profile, "q09.join.part", integratedJoinOutputs
                ? partJoin.withOutputs(0, 1, 2, 3, 4, 5)
                : projectInputs(allocator, primitiveRegistry, partJoin, 0, 1, 2, 3, 4, 5));
        Operator supplier = profiled(profile, "q09.scan.supplier", scannedTable(allocator, tables, "supplier", "s_suppkey", "s_nationkey"));
        // [l_partkey, l_orderkey, l_suppkey, l_quantity, l_extendedprice, l_discount, s_nationkey]
        HashJoinOperator supplierJoin = new HashJoinOperator(allocator, joined, 2, supplier, 0)
                .withProfileName("q09.join.supplier");
        joined = profiled(profile, "q09.join.supplier", integratedJoinOutputs
                ? supplierJoin.withOutputs(0, 1, 2, 3, 4, 5, 7)
                : projectInputs(allocator, primitiveRegistry, supplierJoin, 0, 1, 2, 3, 4, 5, 7));
        Operator partsupp = profiled(profile, "q09.scan.partsupp", scannedTable(allocator, tables, "partsupp", "ps_partkey", "ps_suppkey", "ps_supplycost"));
        // Match Velox's build/probe shape: the filtered lineitem intermediate is the build and the 8M-row
        // partsupp table streams as the probe. Retain only the six columns consumed by the next stages.
        HashJoinOperator partsuppJoin = new HashJoinOperator(allocator, partsupp, new int[] {1, 0}, joined, new int[] {2, 0})
                .withProfileName("q09.join.partsupp")
                .withDirectBoundedBuildCoalescing();
        joined = profiled(profile, "q09.join.partsupp", integratedJoinOutputs
                ? partsuppJoin.withOutputs(7, 8, 6, 4, 9, 2)
                : projectInputs(allocator, primitiveRegistry, partsuppJoin, 7, 8, 6, 4, 9, 2));
        Operator orders = profiled(profile, "q09.scan.orders", scannedTable(allocator, tables, "orders", "o_orderkey", "o_orderdate"));
        // Likewise stream the 15M-row orders table over the smaller build, retaining
        // [extendedprice, discount, quantity, nationkey, supplycost, orderdate].
        HashJoinOperator ordersJoin = new HashJoinOperator(allocator, orders, 0, joined, 3)
                .withProfileName("q09.join.orders")
                .withDirectBoundedBuildCoalescing();
        joined = profiled(profile, "q09.join.orders", integratedJoinOutputs
                ? ordersJoin.withOutputs(2, 3, 4, 6, 7, 1)
                : projectInputs(allocator, primitiveRegistry, ordersJoin, 2, 3, 4, 6, 7, 1));
        Operator nation = profiled(profile, "q09.scan.nation", scannedTable(allocator, tables, "nation", "n_nationkey", "n_name"));
        // + [n_nationkey, n_name -> 6,7]
        HashJoinOperator nationJoin = new HashJoinOperator(allocator, joined, 3, nation, 0)
                .withProfileName("q09.join.nation");
        joined = profiled(profile, "q09.join.nation", integratedJoinOutputs ? nationJoin.withOutputs(0, 1, 2, 4, 5, 7) : nationJoin);

        // [nation, o_year, amount]
        Variable one = new Variable(0);
        Variable oneMinusDiscount = new Variable(1);
        Variable volume = new Variable(2);
        Variable cost = new Variable(3);
        Variable amount = new Variable(4);
        Variable year = new Variable(5);
        Operator projected = profiled(profile, "q09.project", new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(one, new Literal(1.0), AllMask.ALL),
                                new Assignment(oneMinusDiscount, new Call("subtract_f64", List.of(
                                        new Reference(one, Stream.VALUES),
                                        new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                                new Assignment(volume, new Call("multiply_f64", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(oneMinusDiscount, Stream.VALUES))), AllMask.ALL),
                                new Assignment(cost, new Call("multiply_f64", List.of(
                                        new Reference(new Input(integratedJoinOutputs ? 3 : 4), Stream.VALUES),
                                        new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                                new Assignment(amount, new Call("subtract_f64", List.of(
                                        new Reference(volume, Stream.VALUES),
                                        new Reference(cost, Stream.VALUES))), AllMask.ALL),
                                new Assignment(year, new Call("year_of_date", List.of(
                                        new Reference(new Input(integratedJoinOutputs ? 4 : 5), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(integratedJoinOutputs ? 5 : 7), Stream.VALUES),
                                new Reference(year, Stream.VALUES),
                                new Reference(amount, Stream.VALUES))),
                primitiveRegistry,
                joined));
        Operator aggregated = profiled(profile, "q09.group", new GroupedAggregationOperator(allocator, List.of(0, 1), List.of(new SumF64(2)), projected));
        return profiled(profile, "q09.sort", new SortOperator(allocator, new int[] {0, 1}, new boolean[] {false, true}, aggregated));
    }

    private static FilterSpec containsUtf8(int inputIndex, String needle)
    {
        Variable literal = new Variable(0);
        Variable result = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(needle), AllMask.ALL),
                new Assignment(result, new Call("contains_utf8", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    /**
     * Q13: customer LEFT-joins the NOT-LIKE-filtered orders (the build filter must precede the outer join);
     * count(o_orderkey) per customer counts only matches, then the count distribution sorts (custdist,
     * c_count) descending.
     */
    public static Operator query13(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        return query13(allocator, primitiveRegistry, tables, null);
    }

    static Operator query13(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, OperatorCpuProfile profile)
    {
        Operator customer = profiled(profile, "q13.scan.customer", scannedTable(allocator, tables, "customer", "c_custkey"));
        Operator orders = projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q13.filter.orders", filter(allocator, primitiveRegistry,
                        profiled(profile, "q13.scan.orders", scannedTable(allocator, tables, "orders", "o_orderkey", "o_custkey", "o_comment")),
                        notLikeUtf8(2, "%special%requests%"))),
                0, 1);
        orders = profiled(profile, "q13.project.orders", orders);
        // Match the native left-join boundary: [c_custkey, o_orderkey]. The duplicate build key is dead.
        Operator joined = profiled(profile, "q13.join", new HashJoinOperator(allocator, customer, 0, orders, 1, true)
                .withOutputs(0, 1));
        // [c_custkey, c_count]
        Operator perCustomer = profiled(profile, "q13.group.customer", new GroupedAggregationOperator(allocator, List.of(0), List.of(new CountColumn(1)), joined));
        // [c_count, custdist]
        Operator distribution = profiled(profile, "q13.group.distribution", new GroupedAggregationOperator(allocator, List.of(1), List.of(new CountAll()),
                projectInputs(allocator, primitiveRegistry, perCustomer, 0, 1)));
        return profiled(profile, "q13.sort", new SortOperator(allocator, new int[] {1, 0}, new boolean[] {true, true}, distribution));
    }

    /**
     * Q16: partsupp joins the brand/type/size-filtered part, anti-joins the complaining suppliers, then the
     * plan's two-level distinct count: group by (brand, type, size, suppkey), then count per (brand, type,
     * size), sorted (supplier_cnt DESC, brand, type, size).
     */
    public static Operator query16(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        return query16(allocator, primitiveRegistry, tables, null);
    }

    static Operator query16(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, OperatorCpuProfile profile)
    {
        Operator part = profiled(profile, "q16.filter.part", filter(allocator, primitiveRegistry,
                profiled(profile, "q16.scan.part", scannedTable(allocator, tables, "part", "p_partkey", "p_brand", "p_type", "p_size")),
                and(
                        notEqualUtf8(1, "Brand#45"),
                        notLikeUtf8(2, "MEDIUM POLISHED%"),
                        inI64(3, List.of(3L, 9L, 14L, 19L, 23L, 36L, 45L, 49L)))));
        Operator partsupp = profiled(profile, "q16.scan.partsupp", scannedTable(allocator, tables, "partsupp", "ps_partkey", "ps_suppkey"));
        // [ps_partkey, ps_suppkey, p_partkey, p_brand, p_type, p_size]
        HashJoinOperator joined = new HashJoinOperator(allocator, partsupp, 0, part, 0);
        boolean projectJoinOutput = Boolean.parseBoolean(System.getProperty("nitro.tpch.query16ProjectJoinOutput", "true"));
        if (projectJoinOutput) {
            // Match Velox's hash-join output layout: [ps_suppkey, p_brand, p_type, p_size]. Neither equi-join key is
            // consumed downstream, so retaining it only widens the anti-join and both grouping inputs.
            joined.withOutputs(1, 3, 4, 5);
        }
        Operator joinedParts = profiled(profile, "q16.join.part", joined);

        Operator complainingSuppliers = profiled(profile, "q16.project.complaining", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q16.filter.supplier", filter(allocator, primitiveRegistry,
                        profiled(profile, "q16.scan.supplier", scannedTable(allocator, tables, "supplier", "s_suppkey", "s_comment")),
                        likeUtf8(1, "%Customer%Complaints%"))),
                0));
        Operator surviving = profiled(profile, "q16.anti_join.supplier", new SemiJoinOperator(allocator, joinedParts, projectJoinOutput ? 0 : 1, complainingSuppliers, 0, false));

        Operator counted;
        if (projectJoinOutput && Boolean.parseBoolean(System.getProperty("nitro.tpch.query16GroupedDistinctAggregation", "true"))) {
            // Directly express GROUP BY (brand,type,size), count(DISTINCT suppkey). GroupOperator emits
            // [group_id, suppkey, brand, type, size], so the aggregation returns [brand,type,size,count].
            Operator grouped = profiled(profile, "q16.group.keys", new GroupOperator(allocator, new int[] {1, 2, 3}, surviving));
            counted = profiled(profile, "q16.group.distinct_count", new GroupedAggregationOperator(
                    allocator,
                    0,
                    List.of(2, 3, 4),
                    List.of(new DistinctCount(1)),
                    grouped));
        }
        else {
            // Expanded equivalent retained as an opt-out control.
            Operator distinctSuppliers = profiled(profile, "q16.group.distinct", new GroupedAggregationOperator(
                    allocator,
                    projectJoinOutput ? List.of(1, 2, 3, 0) : List.of(3, 4, 5, 1),
                    List.of(),
                    surviving));
            counted = profiled(profile, "q16.group.count", new GroupedAggregationOperator(allocator, List.of(0, 1, 2), List.of(new CountAll()), distinctSuppliers));
        }
        // SQL order: p_brand, p_type, p_size, supplier_cnt; sort: cnt DESC, brand, type, size
        Operator sorted = profiled(profile, "q16.sort", new SortOperator(allocator, new int[] {3, 0, 1, 2}, new boolean[] {true, false, false, false}, counted));
        return sorted;
    }

    /**
     * Q18: lineitem grouped per order with sum(quantity) > 300 keys the customer/orders/lineitem join;
     * quantity sums per (name, custkey, orderkey, orderdate, totalprice); TopN 100 by (totalprice DESC,
     * orderdate).
     */
    public static Operator query18(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        // Retain the qualifying sum: it is also the outer query's sum(quantity), so the physical plan needs one
        // lineitem aggregation rather than discarding it, rescanning all lineitems, and rebuilding the same groups.
        Operator bigOrders = filter(allocator, primitiveRegistry,
                new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        List.of(new SumF64(1)),
                        scannedTable(allocator, tables, "lineitem", "l_orderkey", "l_quantity")),
                compareF64("gt_f64", 1, 300.0));
        Operator orders = scannedTable(allocator, tables, "orders", "o_orderkey", "o_custkey", "o_orderdate", "o_totalprice");
        // [o_orderkey, o_custkey, o_orderdate, o_totalprice, quantity]
        Operator filteredOrders = new HashJoinOperator(allocator, orders, 0, bigOrders, 0)
                .withOutputs(0, 1, 2, 3, 5);
        Operator customer = scannedTable(allocator, tables, "customer", "c_custkey", "c_name");
        // Velox boundary: [c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, quantity].
        Operator joined = new HashJoinOperator(allocator, filteredOrders, 1, customer, 0)
                .withOutputs(6, 1, 0, 2, 3, 4);
        return new TopNOperator(allocator, 100, new int[] {4, 3}, new boolean[] {true, false}, joined);
    }

    private static FilterSpec likeUtf8(int inputIndex, String pattern)
    {
        Variable literal = new Variable(0);
        Variable result = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(pattern), AllMask.ALL),
                new Assignment(result, new Call("like_utf8", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static FilterSpec notLikeUtf8(int inputIndex, String pattern)
    {
        Variable literal = new Variable(0);
        Variable like = new Variable(1);
        Variable result = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(pattern), AllMask.ALL),
                new Assignment(like, new Call("like_utf8", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL),
                new Assignment(result, new Call("not", List.of(
                        new Reference(like, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(
                plan,
                new NotMask(new ReferenceMask(new Reference(like, Stream.VALUES))),
                new Reference(result, Stream.VALUES));
    }

    private static FilterSpec notEqualUtf8(int inputIndex, String constant)
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        Variable result = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(equals, new Call("eq_utf8", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL),
                new Assignment(result, new Call("not", List.of(
                        new Reference(equals, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(
                plan,
                new NotMask(new ReferenceMask(new Reference(equals, Stream.VALUES))),
                new Reference(result, Stream.VALUES));
    }

    private static FilterSpec inI64(int inputIndex, List<Long> values)
    {
        FilterSpec result = null;
        for (long value : values) {
            FilterSpec equals = comparison("eq", inputIndex, new Literal(value));
            result = result == null ? equals : combineOr(result, equals);
        }
        return result;
    }

    /**
     * Q15: the quarter's revenue per supplier (the CTE), its max broadcast as a scalar, and supplier rows
     * whose total equals the max, sorted by suppkey.
     */
    public static Operator query15(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator revenue = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new SumF64(1)),
                projectWithDiscPrice(allocator, primitiveRegistry,
                        filter(allocator, primitiveRegistry,
                                query15LineitemScan(allocator, tables),
                                and(
                                        greaterThan(3, LocalDate.of(1996, 1, 1).toEpochDay() - 1),
                                        lessThan(3, LocalDate.of(1996, 4, 1).toEpochDay()))),
                        1, 2, 0));
        Operator maxRevenue = new AggregationOperator(
                allocator,
                List.of(new MaxF64(1)),
                revenueCopy(allocator, primitiveRegistry, tables));
        // [supplier_no, total_revenue, max_revenue]
        Operator withMax = new NestedLoopJoinOperator(allocator, revenue, maxRevenue);
        Operator best = filter(allocator, primitiveRegistry, withMax, equalColumnsF64(1, 2));

        Operator supplier = scannedTable(allocator, tables, "supplier", "s_suppkey", "s_name", "s_address", "s_phone");
        // Match Velox's final boundary directly: supplier fields plus total_revenue; both build keys and max are dead.
        Operator joined = new HashJoinOperator(allocator, supplier, 0, best, 0)
                .withOutputs(0, 1, 2, 3, 5);
        return new SortOperator(allocator, new int[] {0}, new boolean[] {false}, joined);
    }

    // The engine does not support operator-result reuse: the harness assembles the revenue subplan again for
    // the max (the SQL references the view twice), per the no-replay test discipline.
    private static Operator revenueCopy(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        return new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new SumF64(1)),
                projectWithDiscPrice(allocator, primitiveRegistry,
                        filter(allocator, primitiveRegistry,
                                query15LineitemScan(allocator, tables),
                                and(
                                        greaterThan(3, LocalDate.of(1996, 1, 1).toEpochDay() - 1),
                                        lessThan(3, LocalDate.of(1996, 4, 1).toEpochDay()))),
                        1, 2, 0));
    }

    private static Operator query15LineitemScan(Allocator allocator, TpchParquetTables tables)
    {
        return scannedTableWithLongRange(
                allocator,
                tables,
                "lineitem",
                3,
                LocalDate.of(1996, 1, 1).toEpochDay(),
                LocalDate.of(1996, 3, 31).toEpochDay(),
                "l_suppkey", "l_extendedprice", "l_discount", "l_shipdate");
    }

    /**
     * Q17: Brand#23 / MED BOX parts restrict lineitem; the per-partkey 0.2 * avg(quantity) threshold joins
     * back; quantities below it contribute extendedprice / 7 to the single output row.
     */
    public static Operator query17(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        Operator part = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "part", "p_partkey", "p_brand", "p_container"),
                        and(equalUtf8(1, "Brand#23"), equalUtf8(2, "MED BOX"))),
                0);
        Operator lineitem = scannedTable(allocator, tables, "lineitem", "l_partkey", "l_quantity", "l_extendedprice");
        // [l_partkey, l_quantity, l_extendedprice, p_partkey]
        Operator joined = new HashJoinOperator(allocator, lineitem, 0, part, 0);

        // per-partkey threshold = 0.2 * avg(l_quantity) over ALL lineitems of the part
        Operator thresholds = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new AvgF64(1)),
                scannedTable(allocator, tables, "lineitem", "l_partkey", "l_quantity"));
        Variable scale = new Variable(0);
        Variable threshold = new Variable(1);
        Operator scaledThresholds = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(scale, new Literal(0.2), AllMask.ALL),
                                new Assignment(threshold, new Call("multiply_f64", List.of(
                                        new Reference(scale, Stream.VALUES),
                                        new Reference(new Input(1), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(threshold, Stream.VALUES))),
                primitiveRegistry,
                thresholds);
        // + [t_partkey, threshold] -> 4,5
        joined = new HashJoinOperator(allocator, joined, 0, scaledThresholds, 0);
        Operator below = filter(allocator, primitiveRegistry, joined, lessThanColumnsF64(1, 5));

        Operator summed = new AggregationOperator(allocator, List.of(new SumF64(0)),
                projectInputs(allocator, primitiveRegistry, below, 2));
        Variable seven = new Variable(0);
        Variable avgYearly = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(seven, new Literal(7.0), AllMask.ALL),
                                new Assignment(avgYearly, new Call("divide_f64", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(seven, Stream.VALUES))), AllMask.ALL)),
                        List.of(new Reference(avgYearly, Stream.VALUES))),
                primitiveRegistry,
                summed);
    }

    /**
     * Q11: the German partsupp value per partkey, the global value * 0.00001 threshold broadcast as a scalar,
     * and the parts above it, sorted by value descending.
     */
    public static Operator query11(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        return query11(allocator, primitiveRegistry, tables, null);
    }

    static Operator query11(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, OperatorCpuProfile profile)
    {
        Operator perPart = profiled(profile, "q11.per_part", query11GermanValue(allocator, primitiveRegistry, tables, true, "q11.per_part", profile));
        Operator total = profiled(profile, "q11.total", new AggregationOperator(
                allocator,
                List.of(new SumF64(0)),
                query11GermanValue(allocator, primitiveRegistry, tables, false, "q11.total", profile)));
        Variable fraction = new Variable(0);
        Variable threshold = new Variable(1);
        Operator scaledTotal = profiled(profile, "q11.scale_total", new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(fraction, new Literal(0.00001), AllMask.ALL),
                                new Assignment(threshold, new Call("multiply_f64", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(fraction, Stream.VALUES))), AllMask.ALL)),
                        List.of(new Reference(threshold, Stream.VALUES))),
                primitiveRegistry,
                total));
        // [ps_partkey, value, threshold]
        Operator withThreshold = profiled(profile, "q11.broadcast_threshold", new NestedLoopJoinOperator(allocator, perPart, scaledTotal));
        Operator filtered = profiled(profile, "q11.filter_threshold", filter(allocator, primitiveRegistry, withThreshold, greaterThanColumnsF64(1, 2)));
        Operator projected = profiled(profile, "q11.project_output", projectInputs(allocator, primitiveRegistry, filtered, 0, 1));
        return profiled(profile, "q11.sort", new SortOperator(allocator, new int[] {1}, new boolean[] {true}, projected));
    }

    private static Operator query11GermanValue(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpchParquetTables tables,
            boolean grouped,
            String profilePrefix,
            OperatorCpuProfile profile)
    {
        Operator nation = profiled(profile, profilePrefix + ".nation", projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "nation", "n_nationkey", "n_name"),
                        equalUtf8(1, "GERMANY")),
                0));
        Operator supplier = scannedTable(allocator, tables, "supplier", "s_suppkey", "s_nationkey");
        Operator germanSuppliers = profiled(profile, profilePrefix + ".supplier_join", projectInputs(allocator, primitiveRegistry,
                new HashJoinOperator(allocator, supplier, 1, nation, 0),
                0));
        Operator partsupp = grouped
                ? scannedTable(allocator, tables, "partsupp", "ps_partkey", "ps_suppkey", "ps_supplycost", "ps_availqty")
                : scannedTable(allocator, tables, "partsupp", "ps_suppkey", "ps_supplycost", "ps_availqty");
        // Grouped: [ps_partkey, ps_suppkey, ps_supplycost, ps_availqty, s_suppkey].
        // Scalar total: [ps_suppkey, ps_supplycost, ps_availqty, s_suppkey]. The latter deliberately
        // omits ps_partkey, matching the Velox/Trino branch whose consumer needs only the value.
        Operator joined = profiled(profile, profilePrefix + ".partsupp_join", new HashJoinOperator(allocator, partsupp, grouped ? 1 : 0, germanSuppliers, 0));

        Variable availableQuantity = new Variable(0);
        Variable value = new Variable(1);
        Operator projected = profiled(profile, profilePrefix + ".value_project", new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(availableQuantity, new Call("cast_i64_to_f64", List.of(
                                        new Reference(new Input(grouped ? 3 : 2), Stream.VALUES))), AllMask.ALL),
                                new Assignment(value, new Call("multiply_f64", List.of(
                                        new Reference(new Input(grouped ? 2 : 1), Stream.VALUES),
                                        new Reference(availableQuantity, Stream.VALUES))), AllMask.ALL)),
                        grouped
                                ? List.of(new Reference(new Input(0), Stream.VALUES), new Reference(value, Stream.VALUES))
                                : List.of(new Reference(value, Stream.VALUES))),
                primitiveRegistry,
                joined));
        if (!grouped) {
            return projected;
        }
        return profiled(profile, profilePrefix + ".group", new GroupedAggregationOperator(allocator, List.of(0), List.of(new SumF64(1)), projected));
    }

    private static FilterSpec equalColumnsF64(int leftInputIndex, int rightInputIndex)
    {
        return columnsComparisonF64("eq_f64", leftInputIndex, rightInputIndex);
    }

    private static FilterSpec lessThanColumnsF64(int leftInputIndex, int rightInputIndex)
    {
        return columnsComparisonF64("lt_f64", leftInputIndex, rightInputIndex);
    }

    private static FilterSpec greaterThanColumnsF64(int leftInputIndex, int rightInputIndex)
    {
        return columnsComparisonF64("gt_f64", leftInputIndex, rightInputIndex);
    }

    private static FilterSpec columnsComparisonF64(String function, int leftInputIndex, int rightInputIndex)
    {
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(result, new Call(function, List.of(
                        new Reference(new Input(leftInputIndex), Stream.VALUES),
                        new Reference(new Input(rightInputIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    /**
     * Q2: the minimum EUROPE supplycost per partkey joins back by (partkey, cost-equality); the brand-15
     * %BRASS parts ride the partsupp / supplier / nation / region chain; top 100 by (acctbal DESC, n_name,
     * s_name, partkey).
     */
    public static Operator query02(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        // minimum EUROPE supplycost per partkey
        Operator minimumCosts = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new MinF64(2)),
                query02EuropePartsupp(allocator, primitiveRegistry, tables, "ps_partkey", "ps_suppkey", "ps_supplycost"));

        Operator part = filter(allocator, primitiveRegistry,
                scannedTable(allocator, tables, "part", "p_partkey", "p_mfgr", "p_size", "p_type"),
                and(comparison("eq", 2, new Literal(15L)), likeUtf8(3, "%BRASS")));
        Operator partsupp = scannedTable(allocator, tables, "partsupp", "ps_partkey", "ps_suppkey", "ps_supplycost");
        // [ps_partkey, ps_suppkey, ps_supplycost, p_partkey, p_mfgr, p_size, p_type]
        Operator joined = new HashJoinOperator(allocator, partsupp, 0, part, 0);
        Operator supplier = scannedTable(allocator, tables, "supplier",
                "s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment");
        // + s x7 -> 7..13
        joined = new HashJoinOperator(allocator, joined, 1, supplier, 0);
        Operator nation = scannedTable(allocator, tables, "nation", "n_nationkey", "n_name", "n_regionkey");
        // + n x3 -> 14..16
        joined = new HashJoinOperator(allocator, joined, 10, nation, 0);
        Operator region = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "region", "r_regionkey", "r_name"),
                        equalUtf8(1, "EUROPE")),
                0);
        // + r_regionkey -> 17
        joined = new HashJoinOperator(allocator, joined, 16, region, 0);
        // + [m_partkey, minCost] -> 18,19
        joined = new HashJoinOperator(allocator, joined, 0, minimumCosts, 0);
        Operator best = filter(allocator, primitiveRegistry, joined, equalColumnsF64(2, 19));

        // SQL order: s_acctbal(12), s_name(8), n_name(15), p_partkey(0), p_mfgr(4), s_address(9), s_phone(11), s_comment(13)
        Operator projected = projectInputs(allocator, primitiveRegistry, best, 12, 8, 15, 0, 4, 9, 11, 13);
        return new TopNOperator(allocator, 100, new int[] {0, 2, 1, 3}, new boolean[] {true, false, false, false}, projected);
    }

    private static Operator query02EuropePartsupp(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, String... partsuppColumns)
    {
        Operator region = projectInputs(allocator, primitiveRegistry,
                filter(allocator, primitiveRegistry,
                        scannedTable(allocator, tables, "region", "r_regionkey", "r_name"),
                        equalUtf8(1, "EUROPE")),
                0);
        Operator nation = projectInputs(allocator, primitiveRegistry,
                new HashJoinOperator(allocator,
                        scannedTable(allocator, tables, "nation", "n_nationkey", "n_regionkey"), 1, region, 0),
                0);
        Operator supplier = projectInputs(allocator, primitiveRegistry,
                new HashJoinOperator(allocator,
                        scannedTable(allocator, tables, "supplier", "s_suppkey", "s_nationkey"), 1, nation, 0),
                0);
        Operator partsupp = scannedTable(allocator, tables, "partsupp", partsuppColumns);
        return new HashJoinOperator(allocator, partsupp, 1, supplier, 0);
    }

    /**
     * Q20: the forest-part partsupp rows whose availqty exceeds half the 1994 shipped quantity for that
     * (partkey, suppkey) key the CANADA suppliers via a semi join, sorted by name.
     */
    public static Operator query20(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        return query20(allocator, primitiveRegistry, tables, null);
    }

    static Operator query20(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, OperatorCpuProfile profile)
    {
        Operator forestParts = profiled(profile, "q20.project.forest_parts", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q20.filter.forest_parts", filter(allocator, primitiveRegistry,
                        profiled(profile, "q20.scan.part", scannedTable(allocator, tables, "part", "p_partkey", "p_name")),
                        likeUtf8(1, "forest%"))),
                0));
        Operator partsupp = profiled(profile, "q20.scan.partsupp", scannedTable(allocator, tables, "partsupp", "ps_partkey", "ps_suppkey", "ps_availqty"));
        // [ps_partkey, ps_suppkey, ps_availqty, p_partkey]
        Operator forestPartsupp = profiled(profile, "q20.join.forest_parts", new HashJoinOperator(allocator, partsupp, 0, forestParts, 0));

        Operator shipped = profiled(profile, "q20.group.shipped", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new SumF64(2)),
                profiled(profile, "q20.filter.shipped", filter(allocator, primitiveRegistry,
                        profiled(profile, "q20.scan.lineitem", scannedTable(allocator, tables, "lineitem", "l_partkey", "l_suppkey", "l_quantity", "l_shipdate")),
                        and(
                                greaterThan(3, LocalDate.of(1994, 1, 1).toEpochDay() - 1),
                                lessThan(3, LocalDate.of(1995, 1, 1).toEpochDay()))))));
        Variable half = new Variable(0);
        Variable threshold = new Variable(1);
        Operator thresholds = profiled(profile, "q20.project.threshold", new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(half, new Literal(0.5), AllMask.ALL),
                                new Assignment(threshold, new Call("multiply_f64", List.of(
                                        new Reference(half, Stream.VALUES),
                                        new Reference(new Input(2), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(threshold, Stream.VALUES))),
                primitiveRegistry,
                shipped));
        // + [t_partkey, t_suppkey, threshold] -> 4,5,6
        Operator withThresholds = profiled(profile, "q20.join.threshold", new HashJoinOperator(allocator, forestPartsupp, new int[] {0, 1}, thresholds, new int[] {0, 1}));

        Variable available = new Variable(0);
        Operator castAvailable = profiled(profile, "q20.project.available", new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(available, new Call("cast_i64_to_f64", List.of(
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(available, Stream.VALUES),
                                new Reference(new Input(6), Stream.VALUES))),
                primitiveRegistry,
                withThresholds));
        // [ps_suppkey] with availqty > threshold
        Operator qualifiedSuppliers = profiled(profile, "q20.project.qualified", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q20.filter.qualified", filter(allocator, primitiveRegistry, castAvailable, greaterThanColumnsF64(1, 2))),
                0));

        Operator canadaNation = profiled(profile, "q20.project.nation", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q20.filter.nation", filter(allocator, primitiveRegistry,
                        profiled(profile, "q20.scan.nation", scannedTable(allocator, tables, "nation", "n_nationkey", "n_name")),
                        equalUtf8(1, "CANADA"))),
                0));
        Operator supplier = profiled(profile, "q20.scan.supplier", scannedTable(allocator, tables, "supplier", "s_suppkey", "s_name", "s_address", "s_nationkey"));
        Operator canadaSuppliers = profiled(profile, "q20.join.nation", new HashJoinOperator(allocator, supplier, 3, canadaNation, 0));
        Operator matched = profiled(profile, "q20.semi.qualified", new SemiJoinOperator(allocator, canadaSuppliers, 0, qualifiedSuppliers, 0));
        Operator projected = profiled(profile, "q20.project.output", projectInputs(allocator, primitiveRegistry, matched, 1, 2));
        return profiled(profile, "q20.sort", new SortOperator(allocator, new int[] {0}, new boolean[] {false}, projected));
    }

    /**
     * Q21: per-order distinct-supplier statistics decorrelate the inequality EXISTS pair -- a qualifying l1
     * row needs other suppliers on the order (count >= 2) and no OTHER late supplier (late count == 1);
     * counts per SAUDI ARABIA supplier name, top 100 by (numwait DESC, name).
     */
    public static Operator query21(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        return query21(allocator, primitiveRegistry, tables, null);
    }

    static Operator query21(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, OperatorCpuProfile profile)
    {
        // The EXISTS(other supplier) predicate needs only whether an order's supplier range is non-singleton.
        // Keeping min/max per order is equivalent to distinct(orderkey, suppkey) -> count, without materializing
        // the near-lineitem-cardinality pair relation.
        Operator supplierRange = profiled(profile, "q21.group.supplier_range", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Min(1), new Max(1)),
                profiled(profile, "q21.scan.supplier_range", scannedTable(allocator, tables, "lineitem", "l_orderkey", "l_suppkey"))));
        // Since l1 itself is late, NOT EXISTS(other late supplier) is exactly a singleton late-supplier range.
        Operator lateSupplierRange = profiled(profile, "q21.group.late_supplier_range", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Min(1), new Max(1)),
                profiled(profile, "q21.filter.late_supplier_range", filter(allocator, primitiveRegistry,
                        profiled(profile, "q21.scan.late_supplier_range", scannedTable(allocator, tables, "lineitem", "l_orderkey", "l_suppkey", "l_commitdate", "l_receiptdate")),
                        lessThanColumns(2, 3)))));

        Operator l1 = profiled(profile, "q21.filter.l1", filter(allocator, primitiveRegistry,
                profiled(profile, "q21.scan.l1", scannedTable(allocator, tables, "lineitem", "l_orderkey", "l_suppkey", "l_commitdate", "l_receiptdate")),
                lessThanColumns(2, 3)));
        Operator orders = profiled(profile, "q21.project.orders", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q21.filter.orders", filter(allocator, primitiveRegistry,
                        profiled(profile, "q21.scan.orders", scannedTable(allocator, tables, "orders", "o_orderkey", "o_orderstatus")),
                        equalUtf8(1, "F"))),
                0));
        // [l1 x4, o_orderkey]
        Operator joined = profiled(profile, "q21.join.orders", new HashJoinOperator(allocator, l1, 0, orders, 0));
        Operator saudiNation = profiled(profile, "q21.project.nation", projectInputs(allocator, primitiveRegistry,
                profiled(profile, "q21.filter.nation", filter(allocator, primitiveRegistry,
                        profiled(profile, "q21.scan.nation", scannedTable(allocator, tables, "nation", "n_nationkey", "n_name")),
                        equalUtf8(1, "SAUDI ARABIA"))),
                0));
        Operator supplier = profiled(profile, "q21.scan.supplier", scannedTable(allocator, tables, "supplier", "s_suppkey", "s_name", "s_nationkey"));
        Operator saudiSuppliers = profiled(profile, "q21.join.saudi_suppliers", new HashJoinOperator(allocator, supplier, 2, saudiNation, 0));
        // + [s_suppkey, s_name, s_nationkey, n_nationkey] -> 5..8
        joined = profiled(profile, "q21.join.supplier", new HashJoinOperator(allocator, joined, 1, saudiSuppliers, 0));
        // + [orderkey, minSupplier, maxSupplier] -> 9..11
        joined = profiled(profile, "q21.join.supplier_range", new HashJoinOperator(allocator, joined, 0, supplierRange, 0));
        // + [orderkey, minLateSupplier, maxLateSupplier] -> 12..14
        joined = profiled(profile, "q21.join.late_supplier_range", new HashJoinOperator(allocator, joined, 0, lateSupplierRange, 0));
        Operator qualified = profiled(profile, "q21.filter.qualified", filter(allocator, primitiveRegistry, joined,
                and(
                        lessThanColumns(10, 11),
                        equalColumns(13, 14))));

        Operator counted = profiled(profile, "q21.group.supplier_name", new GroupedAggregationOperator(allocator, List.of(0), List.of(new CountAll()),
                profiled(profile, "q21.project.supplier_name", projectInputs(allocator, primitiveRegistry, qualified, 6))));
        return profiled(profile, "q21.topn", new TopNOperator(allocator, 100, new int[] {1, 0}, new boolean[] {true, false}, counted));
    }

    /**
     * Q22: country codes from the phone prefix; the positive-balance average over the seven codes broadcasts
     * as a scalar; customers above it with no orders aggregate per code.
     */
    public static Operator query22(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables)
    {
        return query22(allocator, primitiveRegistry, tables, null);
    }

    static Operator query22(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpchParquetTables tables, OperatorCpuProfile profile)
    {
        List<String> codes = List.of("13", "31", "23", "29", "30", "18", "17");
        // [cntrycode, c_acctbal, c_custkey]
        Operator coded = query22CodedCustomers(allocator, primitiveRegistry, tables, codes, profile);

        Operator average = profiled(profile, "q22.aggregate.average", new AggregationOperator(
                allocator,
                List.of(new AvgF64(1)),
                profiled(profile, "q22.filter.positive", filter(allocator, primitiveRegistry,
                        query22CodedCustomers(allocator, primitiveRegistry, tables, codes, profile),
                        compareF64("gt_f64", 1, 0.0)))));
        // + avg -> 3
        Operator withAverage = profiled(profile, "q22.join.average", new NestedLoopJoinOperator(allocator, coded, average));
        Operator above = profiled(profile, "q22.filter.above_average", filter(allocator, primitiveRegistry, withAverage, greaterThanColumnsF64(1, 3)));

        Operator orders = profiled(profile, "q22.scan.orders", scannedTable(allocator, tables, "orders", "o_custkey"));
        Operator withoutOrders = profiled(profile, "q22.semi.orders", new SemiJoinOperator(allocator, above, 2, orders, 0, false));

        Operator aggregated = profiled(profile, "q22.group", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new CountAll(), new SumF64(1)),
                withoutOrders));
        return profiled(profile, "q22.sort", new SortOperator(allocator, new int[] {0}, new boolean[] {false}, aggregated));
    }

    private static Operator query22CodedCustomers(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpchParquetTables tables,
            List<String> codes,
            OperatorCpuProfile profile)
    {
        Operator customer = profiled(profile, "q22.scan.customer", scannedTable(allocator, tables, "customer", "c_phone", "c_acctbal", "c_custkey"));
        Variable from = new Variable(0);
        Variable length = new Variable(1);
        Variable code = new Variable(2);
        Operator coded = profiled(profile, "q22.project.code", new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(from, new Literal(1L), AllMask.ALL),
                                new Assignment(length, new Literal(2L), AllMask.ALL),
                                new Assignment(code, new Call("substring_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(from, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(code, Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))),
                primitiveRegistry,
                customer));
        return profiled(profile, "q22.filter.codes", filter(allocator, primitiveRegistry, coded, inUtf8(0, codes)));
    }

    // ---- scan / filter plumbing ----

    private static Operator scannedTable(Allocator allocator, TpchParquetTables tables, String tableName, String... columns)
    {
        List<String> columnNames = List.of(columns);
        Operator decoder = new NitroParquetScanOperator(
                tables.scanResources(),
                allocator,
                tables.tableFiles(tableName),
                columnNames);
        return new BatchSourceOperator(
                new OperatorBatchSource(decoder, SCHEMAS.tpch(tableName, columnNames)),
                new NativeSourceOperatorIngress());
    }

    private static Operator scannedTableWithLongRange(
            Allocator allocator,
            TpchParquetTables tables,
            String tableName,
            int column,
            long min,
            long max,
            String... columns)
    {
        Operator scan = scannedTable(allocator, tables, tableName, columns);
        scan.pushDynamicFilter(DynamicFilter.fromRange(column, min, max));
        return scan;
    }

    private static Operator filter(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, FilterSpec filterSpec)
    {
        return new FilterOperator(
                source,
                filterSpec.plan(),
                primitiveRegistry,
                filterSpec.predicate(),
                allocator,
                allocator.engineResources().operatorResources().filter());
    }

    record FilterSpec(EvaluationPlan plan, MaskExpression predicate, Reference materializedValue)
    {
        FilterSpec(EvaluationPlan plan, MaskExpression predicate)
        {
            this(plan, predicate, materializedReference(predicate));
        }

        private static Reference materializedReference(MaskExpression predicate)
        {
            if (predicate instanceof ReferenceMask(Reference reference)) {
                return reference;
            }
            throw new IllegalArgumentException("A composite filter predicate requires an explicit materialized value");
        }
    }

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
            result = combineAnd(result, spec);
        }
        return result;
    }

    private static FilterSpec combineAnd(FilterSpec left, FilterSpec right)
    {
        return combineMasks(left, right, true);
    }

    private static FilterSpec combineOr(FilterSpec left, FilterSpec right)
    {
        return combineMasks(left, right, false);
    }

    private static FilterSpec combineMasks(FilterSpec left, FilterSpec right, boolean conjunction)
    {
        int rightOffset = maxVariableId(left.plan().assignments()) + 1;
        List<Assignment> assignments = new ArrayList<>(left.plan().assignments());
        assignments.addAll(remap(right.plan().assignments(), rightOffset));
        Reference leftValue = left.materializedValue();
        Reference rightValue = remap(right.materializedValue(), rightOffset);
        Variable result = new Variable(maxVariableId(assignments) + 1);
        assignments.add(new Assignment(
                result,
                new Call(conjunction ? "and" : "or", List.of(leftValue, rightValue)),
                AllMask.ALL));
        MaskExpression rightPredicate = remap(right.predicate(), rightOffset);
        MaskExpression predicate = conjunction
                ? new AndMask(List.of(left.predicate(), rightPredicate))
                : new OrMask(List.of(left.predicate(), rightPredicate));
        return new FilterSpec(
                new EvaluationPlan(assignments, List.of()),
                predicate,
                new Reference(result, Stream.VALUES));
    }

    private static List<Assignment> remap(List<Assignment> assignments, int variableOffset)
    {
        return assignments.stream()
                .map(assignment -> new Assignment(
                        new Variable(assignment.output().id() + variableOffset),
                        remap(assignment.operation(), variableOffset),
                        remap(assignment.mask(), variableOffset)))
                .toList();
    }

    private static org.weakref.nitro.operator.evaluator.ir.Operation remap(org.weakref.nitro.operator.evaluator.ir.Operation operation, int variableOffset)
    {
        return switch (operation) {
            case Literal literal -> literal;
            case Call(String functionName, List<Reference> arguments, var resolvedCall) ->
                    new Call(
                            functionName,
                            arguments.stream()
                                    .map(argument -> remap(argument, variableOffset))
                                    .toList(),
                            resolvedCall);
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

    private static MaskExpression remap(MaskExpression expression, int variableOffset)
    {
        return switch (expression) {
            case AllMask _ -> expression;
            case org.weakref.nitro.operator.evaluator.ir.RangeConstrainedAndMask range -> new org.weakref.nitro.operator.evaluator.ir.RangeConstrainedAndMask(
                    remap(range.input(), variableOffset),
                    range.lowerExclusive(),
                    range.upperExclusive(),
                    range.kernel(),
                    range.remainingTerms().stream().map(term -> remap(term, variableOffset)).toList(),
                    (AndMask) remap(range.fallback(), variableOffset));
            case ReferenceMask reference -> new ReferenceMask(remap(reference.reference(), variableOffset));
            case NotMask not -> new NotMask(remap(not.source(), variableOffset));
            case AndMask and -> new AndMask(and.terms().stream()
                    .map(term -> remap(term, variableOffset))
                    .toList());
            case OrMask or -> new OrMask(or.terms().stream()
                    .map(term -> remap(term, variableOffset))
                    .toList());
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
