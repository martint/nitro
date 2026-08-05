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

import it.unimi.dsi.fastutil.ints.IntSet;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.AdaptiveSqlPartialAggregationOperator;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.BatchSliceOperator;
import org.weakref.nitro.operator.DistinctCount;
import org.weakref.nitro.operator.EnforceSingleRowOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.FullJoinOperator;
import org.weakref.nitro.operator.GroupIdOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.MarkDistinctOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.PartitionAverageI64WindowFunction;
import org.weakref.nitro.operator.PartitionSumI64WindowFunction;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.RankWindowFunction;
import org.weakref.nitro.operator.RunningMaxI64WindowFunction;
import org.weakref.nitro.operator.RunningSumI64WindowFunction;
import org.weakref.nitro.operator.SemiJoinOperator;
import org.weakref.nitro.operator.SortOperator;
import org.weakref.nitro.operator.SqlStageAggregationOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.TopNRankingOperator;
import org.weakref.nitro.operator.UnionAllOperator;
import org.weakref.nitro.operator.WindowOperator;
import org.weakref.nitro.operator.aggregation.Avg;
import org.weakref.nitro.operator.aggregation.ConditionalSumsAggregationUnit;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.CountAvgStddevI64AggregationUnit;
import org.weakref.nitro.operator.aggregation.CountColumn;
import org.weakref.nitro.operator.aggregation.Max;
import org.weakref.nitro.operator.aggregation.Min;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;
import org.weakref.nitro.operator.aggregation.StddevSamp;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.aggregation.SumProductIfEqual;
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
import org.weakref.nitro.operator.evaluator.ir.Producer;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.operator.source.BatchSourceOperator;
import org.weakref.nitro.operator.source.compatibility.NativeSourceOperatorIngress;
import org.weakref.nitro.operator.source.compatibility.OperatorBatchSource;
import org.weakref.nitro.operator.source.compatibility.parquet.NitroParquetScanOperator;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

final class TpcdsParquetSupport
{
    private static final String NULLS_LAST_SENTINEL_STRING = "\uFFFF";
    private static final boolean QUERY22_COMPACT_JOIN_LAYOUTS =
            Boolean.parseBoolean(System.getProperty("nitro.tpcds.query22CompactJoinLayouts", "true"));
    private static final boolean QUERY21_COMPACT_JOIN_LAYOUTS =
            Boolean.parseBoolean(System.getProperty("nitro.tpcds.query21CompactJoinLayouts", "true"));
    private static final boolean QUERY39_COMPACT_JOIN_LAYOUTS =
            Boolean.parseBoolean(System.getProperty("nitro.tpcds.query39CompactJoinLayouts", "true"));
    private static final boolean QUERY78_COMPACT_JOIN_LAYOUTS =
            Boolean.parseBoolean(System.getProperty("nitro.tpcds.query78CompactJoinLayouts", "true"));
    private static final ThreadLocal<OperatorCpuProfile> CURRENT_OPERATOR_CPU_PROFILE = new ThreadLocal<>();

    private TpcdsParquetSupport() {}

    static <T> T withOperatorCpuProfile(OperatorCpuProfile profile, Supplier<T> supplier)
    {
        OperatorCpuProfile previous = CURRENT_OPERATOR_CPU_PROFILE.get();
        CURRENT_OPERATOR_CPU_PROFILE.set(profile);
        try {
            return supplier.get();
        }
        finally {
            if (previous == null) {
                CURRENT_OPERATOR_CPU_PROFILE.remove();
            }
            else {
                CURRENT_OPERATOR_CPU_PROFILE.set(previous);
            }
        }
    }

    private static Operator profiled(String name, Operator operator)
    {
        if (operator instanceof HashJoinOperator hashJoinOperator) {
            hashJoinOperator.withProfileName(name);
        }
        OperatorCpuProfile profile = CURRENT_OPERATOR_CPU_PROFILE.get();
        if (profile == null) {
            return operator;
        }
        return profile.wrap(name, operator);
    }

    public static Operator query01(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerStoreReturns = query01CustomerStoreReturns(allocator, primitiveRegistry, tables);
        Operator storeTotals = query01StoreTotals(allocator, primitiveRegistry, tables);
        Operator tnStores = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "store",
                equalUtf8(1, "TN"),
                new String[] {"s_store_sk", "s_state"},
                0);
        Operator customers = scannedTable(allocator, tables, "customer", "c_customer_sk", "c_customer_id");

        Operator joined = new HashJoinOperator(allocator, customerStoreReturns, 1, tnStores, 0);
        joined = new HashJoinOperator(allocator, joined, 0, customers, 0);
        joined = new HashJoinOperator(allocator, joined, 1, storeTotals, 0);
        joined = filter(allocator, primitiveRegistry, joined, query01ReturnThresholdPredicate(2, 7, 8));

        Operator customerIds = projectInputs(allocator, primitiveRegistry, joined, 5);
        return new TopNOperator(allocator, 100, 0, false, customerIds);
    }

    public static Operator query03(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 11),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0, 2),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        equal(1, 128),
                        new String[] {"i_item_sk", "i_manufact_id", "i_brand_id", "i_brand"},
                        0, 2, 3),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 4, 6, 7, 2);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3)),
                sales);
        return new TopNOperator(allocator, 100, new int[] {0, 3, 1}, new boolean[] {false, true, false}, sales);
    }

    public static Operator query04(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator joined = query04Filtered(allocator, primitiveRegistry, tables);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3}, new boolean[] {false, false, false, false}, joined);
    }

    static Operator query04Filtered(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator joined = query04Joined(allocator, primitiveRegistry, tables);
        joined = filter(allocator, primitiveRegistry, joined, query04GrowthPredicate(1, 5, 6, 7, 8, 9));
        return projectInputs(allocator, primitiveRegistry, joined, 0, 2, 3, 4);
    }

    private static Operator query04Joined(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator joined = query04ChannelYearTotal(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_wholesale_cost", "ss_ext_discount_amt", "ss_ext_sales_price", 2001, false);
        joined = new HashJoinOperator(allocator, joined, 0, query04ChannelYearTotal(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_wholesale_cost", "ss_ext_discount_amt", "ss_ext_sales_price", 2002, true), 0)
                .withOutputs(0, 1, 3, 4, 5, 6);
        joined = new HashJoinOperator(allocator, joined, 0, query04ChannelYearTotal(allocator, primitiveRegistry, tables, "catalog_sales", "cs_bill_customer_sk", "cs_sold_date_sk", "cs_ext_list_price", "cs_ext_wholesale_cost", "cs_ext_discount_amt", "cs_ext_sales_price", 2001, false), 0)
                .withOutputs(0, 1, 2, 3, 4, 5, 7);
        joined = new HashJoinOperator(allocator, joined, 0, query04ChannelYearTotal(allocator, primitiveRegistry, tables, "catalog_sales", "cs_bill_customer_sk", "cs_sold_date_sk", "cs_ext_list_price", "cs_ext_wholesale_cost", "cs_ext_discount_amt", "cs_ext_sales_price", 2002, false), 0)
                .withOutputs(0, 1, 2, 3, 4, 5, 6, 8);
        joined = new HashJoinOperator(allocator, joined, 0, query04ChannelYearTotal(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_wholesale_cost", "ws_ext_discount_amt", "ws_ext_sales_price", 2001, false), 0)
                .withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 9);
        return new HashJoinOperator(allocator, joined, 0, query04ChannelYearTotal(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_wholesale_cost", "ws_ext_discount_amt", "ws_ext_sales_price", 2002, false), 0)
                .withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 8, 10);
    }

    public static Operator query06(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator scalarMonthSequence = query06ScalarMonthSequence(allocator, primitiveRegistry, tables);
        Operator categoryAggregates = query06CategoryAggregates(allocator, tables);

        Operator joined = factScan(allocator, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_item_sk");
        joined = new HashJoinOperator(
                allocator,
                joined,
                0,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk"),
                0);
        joined = new HashJoinOperator(
                allocator,
                joined,
                4,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_state"),
                0);
        joined = new HashJoinOperator(
                allocator,
                joined,
                1,
                scannedTable(allocator, tables, "date_dim", "d_date_sk", "d_month_seq"),
                0);
        joined = new HashJoinOperator(
                allocator,
                joined,
                2,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_current_price", "i_category"),
                0);
        joined = new HashJoinOperator(allocator, joined, 8, scalarMonthSequence, 0);
        joined = projectInputs(allocator, primitiveRegistry, joined, 6, 10, 11);
        joined = new HashJoinOperator(allocator, joined, 2, categoryAggregates, 0);
        joined = filter(allocator, primitiveRegistry, joined, query06ThresholdPredicate(1, 4, 5));
        joined = projectInputs(allocator, primitiveRegistry, joined, 0);

        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new CountAll()),
                joined);
        aggregated = filter(allocator, primitiveRegistry, aggregated, greaterThan(1, 9));
        return new TopNOperator(allocator, 100, new int[] {1, 0}, new boolean[] {false, false}, aggregated);
    }

    public static Operator query12(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        // web_sales ⋈ item(category in the Q12 set) ⋈ date_dim(30-day window); GROUP BY item attrs sum(ext_sales_price);
        // PARTITION SUM of those sums over i_class (window); revenue ratio = class-sum-relative percent; top 100.
        Operator facts = factScan(allocator, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_ext_sales_price");
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        query12CategoryPredicate(),
                        new String[] {"i_item_sk", "i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price"}),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query12DatePredicate(),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);
        facts = projectInputs(allocator, primitiveRegistry, facts, 4, 5, 6, 7, 8, 2);
        facts = new GroupedAggregationOperator(allocator, List.of(0, 1, 2, 3, 4), List.of(new Sum(5)), facts);
        facts = new WindowOperator(allocator, facts, new int[] {3}, new int[0], new boolean[0],
                List.of(new PartitionSumI64WindowFunction(5)));
        facts = projectQuery12RevenueRatio(allocator, primitiveRegistry, facts);
        return new TopNOperator(allocator, 100, new int[] {2, 3, 0, 1, 6}, new boolean[] {false, false, false, false, false}, facts);
    }

    public static Operator query13(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(
                allocator,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_cdemo_sk",
                "ss_hdemo_sk",
                "ss_addr_sk",
                "ss_store_sk",
                "ss_quantity",
                "ss_sales_price",
                "ss_ext_sales_price",
                "ss_ext_wholesale_cost",
                "ss_net_profit");
        sales = new HashJoinOperator(
                allocator,
                sales,
                4,
                scannedTable(allocator, tables, "store", "s_store_sk"),
                0).withOutputs(0, 1, 2, 3, 5, 6, 7, 8, 9);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "customer_demographics", "cd_demo_sk", "cd_marital_status", "cd_education_status"),
                0).withOutputs(0, 2, 3, 4, 5, 6, 7, 8, 10, 11);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "household_demographics", "hd_demo_sk", "hd_dep_count"),
                0).withOutputs(0, 2, 3, 4, 5, 6, 7, 8, 9, 11);
        sales = filter(allocator, primitiveRegistry, sales, query13DemographicsPredicate(7, 8, 3, 9));
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_address",
                        equalUtf8(1, "United States"),
                        new String[] {"ca_address_sk", "ca_country", "ca_state"},
                        0, 2),
                0).withOutputs(0, 2, 4, 5, 6, 11);
        sales = filter(allocator, primitiveRegistry, sales, query13StateProfitPredicate(5, 4));
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 2001),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0).withOutputs(1, 2, 3);
        sales = new AggregationOperator(
                allocator,
                List.of(new Avg(0), new Sum(1), new CountColumn(1), new Sum(2), new CountColumn(2)),
                sales);
        return projectQuery13Averages(allocator, primitiveRegistry, sales);
    }

    public static Operator query20(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return query20(TpcdsQueryContext.unprofiled(allocator, primitiveRegistry, tables));
    }

    static Operator query20(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        // Same revenue-ratio-by-class shape as Q12 over catalog_sales: item(category) ⋈ date_dim(30-day window);
        // GROUP BY item attrs sum(ext_sales_price); PARTITION SUM over i_class; revenue ratio; top 100.
        Operator facts = context.profiled("q20.scan.catalog_sales", factScan(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_ext_sales_price"));
        facts = context.profiled("q20.join.item", new HashJoinOperator(
                allocator,
                facts,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        query12CategoryPredicate(),
                        new String[] {"i_item_sk", "i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price"}),
                0));
        facts = context.profiled("q20.join.date", new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query12DatePredicate(),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0));
        facts = context.profiled("q20.project.join", projectInputs(allocator, primitiveRegistry, facts, 4, 5, 6, 7, 8, 2));
        facts = context.profiled("q20.group", new GroupedAggregationOperator(allocator, List.of(0, 1, 2, 3, 4), List.of(new Sum(5)), facts));
        facts = context.profiled("q20.window", new WindowOperator(allocator, facts, new int[] {3}, new int[0], new boolean[0],
                List.of(new PartitionSumI64WindowFunction(5))));
        facts = context.profiled("q20.project.ratio", projectQuery12RevenueRatio(allocator, primitiveRegistry, facts));
        return context.profiled("q20.top", new TopNOperator(allocator, 100, new int[] {2, 3, 0, 1, 6}, new boolean[] {false, false, false, false, false}, facts));
    }

    public static Operator query39(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator inventoryFirst = query39InventoryVariation(allocator, primitiveRegistry, tables, 1, 1.5);
        Operator inventorySecond = query39InventoryVariation(allocator, primitiveRegistry, tables, 2, 1.0);

        Operator joined = profiled("q39.join.months", new HashJoinOperator(allocator, inventoryFirst, new int[] {0, 1}, inventorySecond, new int[] {0, 1}));
        if (QUERY39_COMPACT_JOIN_LAYOUTS) {
            return profiled("q39.sort", new SortOperator(allocator, new int[] {0, 1, 2, 3, 4, 7, 8, 9}, new boolean[] {false, false, false, false, false, false, false, false}, joined));
        }
        joined = projectInputs(allocator, primitiveRegistry, joined, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        return profiled("q39.sort", new SortOperator(allocator, new int[] {0, 1, 2, 3, 4, 7, 8, 9}, new boolean[] {false, false, false, false, false, false, false, false}, joined));
    }

    public static Operator query17(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number", "ss_quantity");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equalUtf8(1, "2001Q1"),
                        new String[] {"d_date_sk", "d_quarter_name"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_item_id", "i_item_desc"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                2,
                scannedTable(allocator, tables, "store", "s_store_sk", "s_state"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                new int[] {3, 1, 4},
                scannedTable(allocator, tables, "store_returns", "sr_returned_date_sk", "sr_customer_sk", "sr_item_sk", "sr_ticket_number", "sr_return_quantity"),
                new int[] {1, 2, 3});
        sales = new HashJoinOperator(
                allocator,
                sales,
                12,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        or(equalUtf8(1, "2001Q1"), equalUtf8(1, "2001Q2"), equalUtf8(1, "2001Q3")),
                        new String[] {"d_date_sk", "d_quarter_name"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                new int[] {13, 14},
                scannedTable(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_bill_customer_sk", "cs_item_sk", "cs_quantity"),
                new int[] {1, 2});
        sales = new HashJoinOperator(
                allocator,
                sales,
                18,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        or(equalUtf8(1, "2001Q1"), equalUtf8(1, "2001Q2"), equalUtf8(1, "2001Q3")),
                        new String[] {"d_date_sk", "d_quarter_name"},
                        0),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 8, 9, 11, 5, 16, 21);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(
                        new CountColumn(3),
                        new Avg(3),
                        new StddevSamp(3),
                        new CountColumn(4),
                        new Avg(4),
                        new StddevSamp(4),
                        new CountColumn(5),
                        new Avg(5),
                        new StddevSamp(5)),
                sales);
        sales = projectQuery17Output(allocator, primitiveRegistry, sales);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, sales);
    }

    public static Operator query64(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return query64(TpcdsQueryContext.unprofiled(allocator, primitiveRegistry, tables));
    }

    static Operator query64(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        Operator crossSalesFirst = query64CrossSales(context, "q64.cross_sales.first", 1999);
        Operator crossSalesSecond = query64CrossSales(context, "q64.cross_sales.second", 2000);

        Operator joined = context.profiled("q64.join.second_year", new HashJoinOperator(allocator, crossSalesFirst, new int[] {1, 2, 3}, crossSalesSecond, new int[] {1, 2, 3}));
        joined = context.profiled("q64.filter.project.output", filter(allocator, primitiveRegistry, joined, query64YearTransitionPredicate(12, 31, 15, 34)));
        joined = context.profiled("q64.project.output", projectInputs(allocator, primitiveRegistry, joined, 0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 17, 18, 35, 36, 37, 31, 34));
        return context.profiled("q64.order_by", new SortOperator(allocator, new int[] {0, 1, 20, 13, 14, 15, 16, 17}, new boolean[] {false, false, false, false, false, false, false, false}, joined));
    }

    public static Operator query16(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator filtered = query16FilteredSales(allocator, primitiveRegistry, tables);
        Operator eligibleOrders = query16MultiWarehouseOrders(allocator, primitiveRegistry, tables);
        Operator filteredEligible = new HashJoinOperator(allocator, filtered, 0, eligibleOrders, 0);
        Operator filteredEligibleProjected = projectInputs(allocator, primitiveRegistry, filteredEligible, 0, 1, 2);
        Operator returnedOrders = query16ReturnedEligibleOrders(allocator, primitiveRegistry, tables);
        Operator qualified = new HashJoinOperator(allocator, filteredEligibleProjected, 0, returnedOrders, 0, true);
        Operator antiJoined = filter(allocator, primitiveRegistry, qualified, isNull(3));
        Operator qualifiedProjected = projectInputs(allocator, primitiveRegistry, antiJoined, 0, 1, 2);
        return summarizeDistinctOrdersAndTotals(allocator, primitiveRegistry, qualifiedProjected);
    }

    public static Operator query18(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(
                allocator,
                tables,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_item_sk",
                "cs_bill_customer_sk",
                "cs_bill_cdemo_sk",
                "cs_quantity",
                "cs_list_price",
                "cs_coupon_amt",
                "cs_sales_price",
                "cs_net_profit");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 1998),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_item_id"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                3,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_demographics",
                        and(equalUtf8(1, "F"), equalUtf8(2, "Unknown")),
                        new String[] {"cd_demo_sk", "cd_gender", "cd_education_status", "cd_dep_count"},
                        0, 3),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                2,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer",
                        anyOf(2, 1, 2, 6, 8, 9, 12),
                        new String[] {"c_customer_sk", "c_current_addr_sk", "c_birth_month", "c_birth_year", "c_current_cdemo_sk"},
                        0, 1, 3, 4),
                0);
        // Canonical q18 joins customer_demographics a second time (cd2) on c_current_cdemo_sk with no filter — a
        // referential-integrity no-op that Trino's plan keeps as a separate scan. Replicate it, then project back to
        // the pre-join layout so downstream column indices are unchanged.
        sales = new HashJoinOperator(
                allocator,
                sales,
                17,
                scannedTable(allocator, tables, "customer_demographics", "cd_demo_sk"),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
        sales = new HashJoinOperator(
                allocator,
                sales,
                15,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_address",
                        utf8AnyOf(2, Set.of("MS", "IN", "ND", "OK", "NM", "VA")),
                        new String[] {"ca_address_sk", "ca_country", "ca_state", "ca_county"},
                        0, 1, 2, 3),
                0);
        sales = projectQuery18Inputs(allocator, primitiveRegistry, sales);
        sales = new GroupIdOperator(
                allocator,
                sales,
                new int[][] {
                        {-1, -1, -1, -1, 4, 5, 6, 7, 8, 9, 10},
                        {0, -1, -1, -1, 4, 5, 6, 7, 8, 9, 10},
                        {0, 1, -1, -1, 4, 5, 6, 7, 8, 9, 10},
                        {0, 1, 2, -1, 4, 5, 6, 7, 8, 9, 10},
                        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy());
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 11),
                List.of(
                        new Sum(4), new CountColumn(4),
                        new Sum(5), new CountColumn(5),
                        new Sum(6), new CountColumn(6),
                        new Sum(7), new CountColumn(7),
                        new Sum(8), new CountColumn(8),
                        new Sum(9), new CountColumn(9),
                        new Sum(10), new CountColumn(10)),
                sales);
        sales = new TopNOperator(allocator, 100, new int[] {1, 2, 3, 0}, new boolean[] {false, false, false, false}, sales);
        return projectQuery18Output(allocator, primitiveRegistry, sales);
    }

    public static Operator query22(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return query22(TpcdsQueryContext.unprofiled(allocator, primitiveRegistry, tables));
    }

    static Operator query22(TpcdsQueryContext context)
    {
        Operator inventory = query22Rollup(context);
        inventory = context.profiled(
                "q22.project.output",
                projectQuery22Output(context.allocator(), context.primitiveRegistry(), inventory, 5, 6));
        return context.profiled(
                "q22.topn",
                new TopNOperator(context.allocator(), 100, new int[] {4, 0, 1, 2, 3}, new boolean[] {false, false, false, false, false}, inventory));
    }

    private static Operator query22Rollup(TpcdsQueryContext context)
    {
        Operator inventory = query22InventoryRollupSource(context);
        inventory = context.profiled("q22.group_id", new GroupIdOperator(
                context.allocator(),
                inventory,
                new int[][] {
                        {-1, -1, -1, -1, 4, 5},
                        {0, -1, -1, -1, 4, 5},
                        {0, 1, -1, -1, 4, 5},
                        {0, 1, 2, -1, 4, 5},
                        {0, 1, 2, 3, 4, 5}},
                EngineResources.from(context.allocator()).operatorResources().groupIdPolicy()));
        return context.profiled("q22.group.rollup", new GroupedAggregationOperator(
                context.allocator(),
                List.of(0, 1, 2, 3, 6),
                List.of(new Sum(4), new Sum(5)),
                inventory));
    }

    private static Operator query22InventoryRollupSource(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        Operator inventory = context.profiled(
                "q22.scan.inventory",
                scannedTable(allocator, tables, "inventory", "inv_date_sk", "inv_item_sk", "inv_quantity_on_hand"));
        HashJoinOperator dateJoin = new HashJoinOperator(
                allocator,
                inventory,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(1, 1199), lessThan(1, 1212)),
                        new String[] {"d_date_sk", "d_month_seq"},
                        0),
                0);
        inventory = context.profiled("q22.join.date_dim", QUERY22_COMPACT_JOIN_LAYOUTS ? dateJoin.withOutputs(1, 2) : dateJoin);
        inventory = context.profiled("q22.group.by_item", new GroupedAggregationOperator(
                allocator,
                List.of(QUERY22_COMPACT_JOIN_LAYOUTS ? 0 : 1),
                List.of(QUERY22_COMPACT_JOIN_LAYOUTS ? 0 : 1),
                List.of(new Sum(QUERY22_COMPACT_JOIN_LAYOUTS ? 1 : 2), new CountColumn(QUERY22_COMPACT_JOIN_LAYOUTS ? 1 : 2)),
                inventory));
        HashJoinOperator itemJoin = new HashJoinOperator(
                allocator,
                inventory,
                0,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_product_name", "i_brand", "i_class", "i_category"),
                0);
        inventory = context.profiled("q22.join.item", QUERY22_COMPACT_JOIN_LAYOUTS ? itemJoin.withOutputs(4, 5, 6, 7, 1, 2) : itemJoin);
        if (QUERY22_COMPACT_JOIN_LAYOUTS) {
            return inventory;
        }
        return context.profiled("q22.project.rollup_source", projectInputs(allocator, primitiveRegistry, inventory, 4, 5, 6, 7, 1, 2));
    }

    private static Operator projectQuery22Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int sumIndex, int countIndex)
    {
        Variable average = new Variable(0);
        List<Assignment> assignments = List.of(
                new Assignment(average, new Call("divide_i64_to_f64", List.of(
                        new Reference(new Input(sumIndex), Stream.VALUES),
                        new Reference(new Input(countIndex), Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(0), Stream.VALUES),
                new Reference(new Input(1), Stream.VALUES),
                new Reference(new Input(2), Stream.VALUES),
                new Reference(new Input(3), Stream.VALUES),
                new Reference(average, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectQuery18Inputs(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable scale = new Variable(0);
        Variable scaledQuantity = new Variable(1);
        Variable scaledBirthYear = new Variable(2);
        Variable scaledDependentCount = new Variable(3);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(scale, new Literal(100L), AllMask.ALL),
                        new Assignment(scaledQuantity, new Call("multiply", List.of(
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(scale, Stream.VALUES))), AllMask.ALL),
                        new Assignment(scaledBirthYear, new Call("multiply", List.of(
                                new Reference(new Input(16), Stream.VALUES),
                                new Reference(scale, Stream.VALUES))), AllMask.ALL),
                        new Assignment(scaledDependentCount, new Call("multiply", List.of(
                                new Reference(new Input(13), Stream.VALUES),
                                new Reference(scale, Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(11), Stream.VALUES),
                        new Reference(new Input(18), Stream.VALUES),
                        new Reference(new Input(19), Stream.VALUES),
                        new Reference(new Input(20), Stream.VALUES),
                        new Reference(scaledQuantity, Stream.VALUES),
                        new Reference(new Input(5), Stream.VALUES),
                        new Reference(new Input(6), Stream.VALUES),
                        new Reference(new Input(7), Stream.VALUES),
                        new Reference(new Input(8), Stream.VALUES),
                        new Reference(scaledBirthYear, Stream.VALUES),
                        new Reference(scaledDependentCount, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery18Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable averageQuantity = new Variable(0);
        Variable averageListPrice = new Variable(1);
        Variable averageCouponAmount = new Variable(2);
        Variable averageSalesPrice = new Variable(3);
        Variable averageNetProfit = new Variable(4);
        Variable averageBirthYear = new Variable(5);
        Variable averageDependentCount = new Variable(6);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(averageQuantity, new Call("divide_round_i64", List.of(
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(new Input(6), Stream.VALUES))), AllMask.ALL),
                        new Assignment(averageListPrice, new Call("divide_round_i64", List.of(
                                new Reference(new Input(7), Stream.VALUES),
                                new Reference(new Input(8), Stream.VALUES))), AllMask.ALL),
                        new Assignment(averageCouponAmount, new Call("divide_round_i64", List.of(
                                new Reference(new Input(9), Stream.VALUES),
                                new Reference(new Input(10), Stream.VALUES))), AllMask.ALL),
                        new Assignment(averageSalesPrice, new Call("divide_round_i64", List.of(
                                new Reference(new Input(11), Stream.VALUES),
                                new Reference(new Input(12), Stream.VALUES))), AllMask.ALL),
                        new Assignment(averageNetProfit, new Call("divide_round_i64", List.of(
                                new Reference(new Input(13), Stream.VALUES),
                                new Reference(new Input(14), Stream.VALUES))), AllMask.ALL),
                        new Assignment(averageBirthYear, new Call("divide_round_i64", List.of(
                                new Reference(new Input(15), Stream.VALUES),
                                new Reference(new Input(16), Stream.VALUES))), AllMask.ALL),
                        new Assignment(averageDependentCount, new Call("divide_round_i64", List.of(
                                new Reference(new Input(17), Stream.VALUES),
                                new Reference(new Input(18), Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(averageQuantity, Stream.VALUES),
                        new Reference(averageListPrice, Stream.VALUES),
                        new Reference(averageCouponAmount, Stream.VALUES),
                        new Reference(averageSalesPrice, Stream.VALUES),
                        new Reference(averageNetProfit, Stream.VALUES),
                        new Reference(averageBirthYear, Stream.VALUES),
                        new Reference(averageDependentCount, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    public static Operator query21(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator before = query21InventoryByWarehouseItem(allocator, primitiveRegistry, tables, true);
        Operator after = query21InventoryByWarehouseItem(allocator, primitiveRegistry, tables, false);
        HashJoinOperator ratioJoin = new HashJoinOperator(allocator, before, new int[] {0, 1}, after, new int[] {0, 1});
        Operator joined = profiled("q21.join.before_after", QUERY21_COMPACT_JOIN_LAYOUTS ? ratioJoin.withOutputs(0, 1, 2, 5) : ratioJoin);
        if (!QUERY21_COMPACT_JOIN_LAYOUTS) {
            joined = projectInputs(allocator, primitiveRegistry, joined, 0, 1, 2, 5);
        }
        joined = profiled("q21.filter.ratio", filter(allocator, primitiveRegistry, joined, query21InventoryRatioPredicate(2, 3)));
        return profiled("q21.topn", new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, joined));
    }

    public static Operator query27(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(
                allocator,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_item_sk",
                "ss_store_sk",
                "ss_cdemo_sk",
                "ss_quantity",
                "ss_list_price",
                "ss_coupon_amt",
                "ss_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 2002),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_item_id"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                2,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "store",
                        equalUtf8(1, "TN"),
                        new String[] {"s_store_sk", "s_state"},
                        0, 1),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                3,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_demographics",
                        and(equalUtf8(1, "M"), equalUtf8(2, "S"), equalUtf8(3, "College")),
                        new String[] {"cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status"},
                        0),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 10, 12, 4, 5, 6, 7);
        sales = new GroupIdOperator(
                allocator,
                sales,
                new int[][] {
                        {0, -1, 2, 3, 4, 5},
                        {0, 1, 2, 3, 4, 5}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy());
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 6),
                List.of(new Avg(2), new Avg(3), new Avg(4), new Avg(5)),
                sales);
        // The Parquet harness stores DECIMAL(7,2) money as physical cents. AVG produces F64 in that physical unit;
        // restore the SQL value before the final projection while leaving average quantity unchanged.
        sales = divideRoundedF64Inputs(allocator, primitiveRegistry, sales, 100.0, 4, 5, 6);
        sales = projectQuery27Output(allocator, primitiveRegistry, sales);
        return new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, sales);
    }

    public static Operator query28(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator buckets = query28Bucket(allocator, primitiveRegistry, tables, 0, 5, 8_00L, 18_00L, 459_00L, 1459_00L, 57_00L, 77_00L);
        buckets = new NestedLoopJoinOperator(allocator, buckets, query28Bucket(allocator, primitiveRegistry, tables, 6, 10, 90_00L, 100_00L, 2323_00L, 3323_00L, 31_00L, 51_00L));
        buckets = new NestedLoopJoinOperator(allocator, buckets, query28Bucket(allocator, primitiveRegistry, tables, 11, 15, 142_00L, 152_00L, 12214_00L, 13214_00L, 79_00L, 99_00L));
        buckets = new NestedLoopJoinOperator(allocator, buckets, query28Bucket(allocator, primitiveRegistry, tables, 16, 20, 135_00L, 145_00L, 6071_00L, 7071_00L, 38_00L, 58_00L));
        buckets = new NestedLoopJoinOperator(allocator, buckets, query28Bucket(allocator, primitiveRegistry, tables, 21, 25, 122_00L, 132_00L, 836_00L, 1836_00L, 17_00L, 37_00L));
        buckets = new NestedLoopJoinOperator(allocator, buckets, query28Bucket(allocator, primitiveRegistry, tables, 26, 30, 154_00L, 164_00L, 7326_00L, 8326_00L, 7_00L, 27_00L));
        return buckets;
    }

    public static Operator query02(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator currentYear = projectQuery02AdjustedWeek(allocator, primitiveRegistry, query02WeeklySales(allocator, primitiveRegistry, tables, 2001));
        Operator nextYear = query02WeeklySales(allocator, primitiveRegistry, tables, 2002);
        Operator joined = profiled("q02.join.years", new HashJoinOperator(allocator, currentYear, 0, nextYear, 0)
                .withOutputs(1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16));
        joined = profiled("q02.project.output", projectQuery02Output(allocator, primitiveRegistry, joined));
        return profiled("q02.topn", new TopNOperator(allocator, 10_000, 0, false, joined));
    }

    public static Operator query42(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = query42SalesByCategory(allocator, primitiveRegistry, tables);
        sales = new TopNOperator(allocator, 100, new int[] {3, 0, 1, 2}, new boolean[] {true, false, false, false}, sales);
        return projectInputs(allocator, primitiveRegistry, sales, 0, 1, 2, 3);
    }

    public static Operator query43(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = scannedTable(
                allocator,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_store_sk",
                "ss_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(2, 2000),
                        new String[] {"d_date_sk", "d_day_name", "d_year"},
                        0, 1),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "store",
                        equal(1, -500L),
                        new String[] {"s_store_sk", "s_gmt_offset", "s_store_name", "s_store_id"},
                        0, 2, 3),
                0);
        sales = projectQuery43Buckets(allocator, primitiveRegistry, sales, 6, 7, 4, 2);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2), new Sum(3), new Sum(4), new Sum(5), new Sum(6), new Sum(7), new Sum(8)),
                sales);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8}, new boolean[] {false, false, false, false, false, false, false, false, false}, sales);
    }

    public static Operator query46(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = scannedTable(
                allocator,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_store_sk",
                "ss_hdemo_sk",
                "ss_addr_sk",
                "ss_ticket_number",
                "ss_customer_sk",
                "ss_coupon_amt",
                "ss_net_profit");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query46DatePredicate(),
                        new String[] {"d_date_sk", "d_dow", "d_year"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "store",
                        utf8AnyOf(1, Set.of("Fairview", "Midway")),
                        new String[] {"s_store_sk", "s_city"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                2,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "household_demographics",
                        query46HouseholdPredicate(),
                        new String[] {"hd_demo_sk", "hd_dep_count", "hd_vehicle_count"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                3,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_city"),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 4, 5, 12, 6, 7);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4)),
                sales);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk", "c_last_name", "c_first_name"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                6,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_city"),
                0);
        sales = filter(allocator, primitiveRegistry, sales, notEqualUtf8Columns(10, 2));
        sales = projectInputs(allocator, primitiveRegistry, sales, 7, 8, 10, 2, 0, 3, 4);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3, 4}, new boolean[] {false, false, false, false, false}, sales);
    }

    public static Operator query34(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = scannedTable(
                allocator,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_store_sk",
                "ss_hdemo_sk",
                "ss_ticket_number",
                "ss_customer_sk");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query34DatePredicate(),
                        new String[] {"d_date_sk", "d_dom", "d_year"},
                        0),
                0).withOutputs(1, 2, 3, 4);
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "store",
                        utf8AnyOf(1, Set.of("Williamson County")),
                        new String[] {"s_store_sk", "s_county"},
                        0),
                0).withOutputs(1, 2, 3);
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "household_demographics",
                        query34HouseholdPredicate(),
                        new String[] {"hd_demo_sk", "hd_buy_potential", "hd_vehicle_count", "hd_dep_count"},
                        0),
                0).withOutputs(1, 2);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new CountAll()),
                sales);
        sales = filter(allocator, primitiveRegistry, sales, query34CountPredicate());
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag"),
                0).withOutputs(4, 5, 6, 7, 0, 2);
        return new SortOperator(allocator, new int[] {0, 1, 2, 3, 4}, new boolean[] {false, false, false, true, false}, sales);
    }

    public static Operator query50(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = scannedTable(
                allocator,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_ticket_number",
                "ss_item_sk",
                "ss_customer_sk",
                "ss_store_sk");
        Operator returns = scannedTable(
                allocator,
                tables,
                "store_returns",
                "sr_returned_date_sk",
                "sr_ticket_number",
                "sr_item_sk",
                "sr_customer_sk");
        boolean compactJoinLayouts = Boolean.parseBoolean(System.getProperty("nitro.tpcds.query50CompactJoinLayouts", "true"));
        HashJoinOperator firstJoin = new HashJoinOperator(allocator, sales, new int[] {1, 2, 3}, returns, new int[] {1, 2, 3});
        if (compactJoinLayouts) {
            // Match Velox: [ss_sold_date_sk, ss_store_sk, sr_returned_date_sk]. Join keys remain private.
            firstJoin.withOutputs(0, 4, 5);
        }
        Operator joined = firstJoin;
        HashJoinOperator soldDateJoin = new HashJoinOperator(
                allocator,
                joined,
                0,
                scannedTable(allocator, tables, "date_dim", "d_date_sk"),
                0);
        if (compactJoinLayouts) {
            // [ss_store_sk, sr_returned_date_sk, ss_sold_date_sk]
            soldDateJoin.withOutputs(1, 2, 0);
        }
        joined = soldDateJoin;
        HashJoinOperator returnedDateJoin = new HashJoinOperator(
                allocator,
                joined,
                compactJoinLayouts ? 1 : 5,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 2001), equal(2, 8)),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0),
                0);
        if (compactJoinLayouts) {
            returnedDateJoin.withOutputs(0, 1, 2);
        }
        joined = returnedDateJoin;
        HashJoinOperator storeJoin = new HashJoinOperator(
                allocator,
                joined,
                compactJoinLayouts ? 0 : 4,
                scannedTable(
                        allocator,
                        tables,
                        "store",
                        "s_store_sk",
                        "s_store_name",
                        "s_company_id",
                        "s_street_number",
                        "s_street_name",
                        "s_street_type",
                        "s_suite_number",
                        "s_city",
                        "s_county",
                        "s_state",
                        "s_zip"),
                0);
        if (compactJoinLayouts) {
            // [sr_returned_date_sk, ss_sold_date_sk, ten store address columns]
            storeJoin.withOutputs(1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
        }
        joined = storeJoin;
        joined = compactJoinLayouts
                ? projectQuery50Buckets(allocator, primitiveRegistry, joined, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0, 1)
                : projectQuery50Buckets(allocator, primitiveRegistry, joined, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 5, 0);
        joined = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                List.of(new Sum(10), new Sum(11), new Sum(12), new Sum(13), new Sum(14)),
                joined);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, new boolean[] {false, false, false, false, false, false, false, false, false, false}, joined);
    }

    public static Operator query52(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 11), equal(2, 2000)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0, 2),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        equal(1, 1),
                        new String[] {"i_item_sk", "i_manager_id", "i_brand_id", "i_brand"},
                        0, 2, 3),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 4, 6, 7, 2);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3)),
                sales);
        return new TopNOperator(allocator, 100, new int[] {0, 3, 1}, new boolean[] {false, true, false}, sales);
    }

    public static Operator query68(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = scannedTable(
                allocator,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_store_sk",
                "ss_hdemo_sk",
                "ss_addr_sk",
                "ss_ticket_number",
                "ss_customer_sk",
                "ss_ext_sales_price",
                "ss_ext_list_price",
                "ss_ext_tax");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query68DatePredicate(),
                        new String[] {"d_date_sk", "d_dom", "d_year"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "store",
                        utf8AnyOf(1, Set.of("Fairview", "Midway")),
                        new String[] {"s_store_sk", "s_city"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                2,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "household_demographics",
                        query46HouseholdPredicate(),
                        new String[] {"hd_demo_sk", "hd_dep_count", "hd_vehicle_count"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                3,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_city"),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 4, 5, 13, 6, 8, 7);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4), new Sum(5)),
                sales);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk", "c_last_name", "c_first_name"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                7,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_city"),
                0);
        sales = filter(allocator, primitiveRegistry, sales, notEqualUtf8Columns(11, 2));
        sales = projectInputs(allocator, primitiveRegistry, sales, 8, 9, 11, 2, 0, 3, 4, 5);
        return new TopNOperator(allocator, 100, new int[] {0, 4}, new boolean[] {false, false}, sales);
    }

    public static Operator query79(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return profiled("q79.topn", new TopNOperator(allocator, 100, new int[] {0, 1, 2, 5}, new boolean[] {false, false, false, false}, query79FinalProjectedRows(allocator, primitiveRegistry, tables)));
    }

    static Operator query79ProjectedSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = profiled("q79.scan.store_sales", factScan(
                allocator,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_store_sk",
                "ss_hdemo_sk",
                "ss_ticket_number",
                "ss_customer_sk",
                "ss_addr_sk",
                "ss_coupon_amt",
                "ss_net_profit"));
        sales = profiled("q79.join.date_dim", new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query79DatePredicate(),
                        new String[] {"d_date_sk", "d_dow", "d_year"},
                        0),
                0).withOutputs(1, 2, 3, 4, 5, 6, 7));
        sales = profiled("q79.join.store", new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "store",
                        betweenInclusive(2, 200, 295),
                        new String[] {"s_store_sk", "s_city", "s_number_employees"},
                        0, 1),
                0).withOutputs(1, 2, 3, 4, 5, 6, 8));
        sales = profiled("q79.join.household_demographics", new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "household_demographics",
                        query79HouseholdPredicate(),
                        new String[] {"hd_demo_sk", "hd_dep_count", "hd_vehicle_count"},
                        0),
                0).withOutputs(1, 2, 3, 4, 5, 6));
        // Preserve the nullable measure streams: SQL SUM returns null, not zero, for an all-null group.
        return profiled("q79.project.sales", projectInputs(allocator, primitiveRegistry, sales, 0, 1, 2, 5, 3, 4));
    }

    static Operator query79GroupedSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return profiled("q79.group.sales", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3),
                List.of(new Sum(4), new Sum(5)),
                query79ProjectedSales(allocator, primitiveRegistry, tables)));
    }

    static Operator query79JoinedSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return profiled("q79.join.customer", new HashJoinOperator(
                allocator,
                query79GroupedSales(allocator, primitiveRegistry, tables),
                1,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_last_name", "c_first_name"),
                0).withOutputs(0, 3, 4, 5, 7, 8));
    }

    static Operator query79FinalProjectedRows(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = query79JoinedSales(allocator, primitiveRegistry, tables);
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable cityPrefix = new Variable(2);
        return profiled("q79.project.final", new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(start, new Literal(1L), AllMask.ALL),
                                new Assignment(length, new Literal(30L), AllMask.ALL),
                                new Assignment(cityPrefix, new Call("substring_utf8", List.of(
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(cityPrefix, Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES))),
                primitiveRegistry,
                sales));
    }

    public static Operator query91(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator returns = scannedTable(
                allocator,
                tables,
                "catalog_returns",
                "cr_call_center_sk",
                "cr_returned_date_sk",
                "cr_returning_customer_sk",
                "cr_net_loss");
        returns = new HashJoinOperator(
                allocator,
                returns,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 1998), equal(2, 11)),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0),
                0);
        returns = new HashJoinOperator(
                allocator,
                returns,
                2,
                scannedTable(
                        allocator,
                        tables,
                        "customer",
                        "c_customer_sk",
                        "c_current_cdemo_sk",
                        "c_current_hdemo_sk",
                        "c_current_addr_sk"),
                0);
        returns = new HashJoinOperator(
                allocator,
                returns,
                6,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_demographics",
                        query91CustomerDemographicsPredicate(),
                        new String[] {"cd_demo_sk", "cd_marital_status", "cd_education_status"}),
                0);
        returns = new HashJoinOperator(
                allocator,
                returns,
                7,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "household_demographics",
                        equalUtf8(1, "Unknown"),
                        new String[] {"hd_demo_sk", "hd_buy_potential"},
                        0),
                0);
        returns = new HashJoinOperator(
                allocator,
                returns,
                8,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_address",
                        equal(1, -700L),
                        new String[] {"ca_address_sk", "ca_gmt_offset"},
                        0),
                0);
        returns = new HashJoinOperator(
                allocator,
                returns,
                0,
                scannedTable(
                        allocator,
                        tables,
                        "call_center",
                        "cc_call_center_sk",
                        "cc_call_center_id",
                        "cc_name",
                        "cc_manager"),
                0);
        returns = projectInputs(allocator, primitiveRegistry, returns, 15, 16, 17, 10, 11, 3);
        returns = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4),
                List.of(new Sum(5)),
                returns);
        returns = new TopNOperator(allocator, 100, new int[] {5, 0, 1, 2, 3, 4}, new boolean[] {true, false, false, false, false, false}, returns);
        return projectInputs(allocator, primitiveRegistry, returns, 0, 1, 2, 5);
    }

    public static Operator query82(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return query82(TpcdsQueryContext.unprofiled(allocator, primitiveRegistry, tables));
    }

    static Operator query82(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        // Q82: SELECT DISTINCT i_item_id, i_item_desc, i_current_price over store-sold items priced 62.00..92.00 from
        // manufacturers {129,270,821,423}, on hand (inv_quantity_on_hand 100..500) in a 60-day window from 2000-05-25,
        // and sold (item_sk IN store_sales). item ⋈ inventory ⋈ date_dim ⋈ DISTINCT(store_sales item_sk); the GROUP BY
        // over the item attributes is the DISTINCT; top 100.
        LocalDate startDate = LocalDate.of(2000, 5, 25);
        Operator items = context.profiled("q82.scan.item", filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "item",
                and(betweenInclusive(3, 62_00L, 92_00L), anyOf(4, 129L, 270L, 821L, 423L)),
                new String[] {"i_item_sk", "i_item_id", "i_item_desc", "i_current_price", "i_manufact_id"},
                0, 1, 2, 3));
        Operator inventory = context.profiled("q82.scan.inventory", filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "inventory",
                betweenInclusive(2, 100, 500),
                new String[] {"inv_item_sk", "inv_date_sk", "inv_quantity_on_hand"},
                0, 1));
        Operator dates = context.profiled("q82.scan.date_dim", filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                betweenInclusive(1, startDate.toEpochDay(), startDate.plusDays(60).toEpochDay()),
                new String[] {"d_date_sk", "d_date"},
                0));
        Operator salesItems = context.profiled("q82.distinct.sales_items", new MarkDistinctOperator(
                allocator,
                0,
                context.profiled("q82.scan.sales_items", scannedTable(allocator, tables, "store_sales", "ss_item_sk")),
                EngineResources.from(allocator).operatorResources()));

        Operator joined = context.profiled("q82.join.inventory", new HashJoinOperator(allocator, items, 0, inventory, 0));
        joined = context.profiled("q82.join.date_dim", new HashJoinOperator(allocator, joined, 5, dates, 0));
        joined = context.profiled("q82.join.sales_items", new HashJoinOperator(allocator, joined, 0, salesItems, 0));
        joined = context.profiled("q82.project.keys", projectInputs(allocator, primitiveRegistry, joined, 1, 2, 3));
        joined = context.profiled("q82.group.keys", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new CountAll()),
                joined));
        joined = context.profiled("q82.project.output", projectInputs(allocator, primitiveRegistry, joined, 0, 1, 2));
        return context.profiled("q82.topn", new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, joined));
    }

    public static Operator query37(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return query37(TpcdsQueryContext.unprofiled(allocator, primitiveRegistry, tables));
    }

    static Operator query37(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        // Q37: SELECT DISTINCT i_item_id, i_item_desc, i_current_price over catalog-sold items priced 68.00..98.00 from
        // manufacturers {677,940,694,808}, on hand (inv_quantity_on_hand 100..500) in a 60-day window from 2000-02-01,
        // and sold (item_sk IN catalog_sales). Same inventory-sales-items shape as Q82 over catalog_sales.
        LocalDate startDate = LocalDate.of(2000, 2, 1);
        Operator items = context.profiled("q37.scan.item", filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "item",
                and(betweenInclusive(3, 68_00L, 98_00L), anyOf(4, 677L, 940L, 694L, 808L)),
                new String[] {"i_item_sk", "i_item_id", "i_item_desc", "i_current_price", "i_manufact_id"},
                0, 1, 2, 3));
        Operator inventory = context.profiled("q37.scan.inventory", filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "inventory",
                betweenInclusive(2, 100, 500),
                new String[] {"inv_item_sk", "inv_date_sk", "inv_quantity_on_hand"},
                0, 1));
        Operator dates = context.profiled("q37.scan.date_dim", filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                betweenInclusive(1, startDate.toEpochDay(), startDate.plusDays(60).toEpochDay()),
                new String[] {"d_date_sk", "d_date"},
                0));
        Operator salesItems = context.profiled("q37.distinct.sales_items", new MarkDistinctOperator(
                allocator,
                0,
                context.profiled("q37.scan.sales_items", scannedTable(allocator, tables, "catalog_sales", "cs_item_sk")),
                EngineResources.from(allocator).operatorResources()));

        Operator joined = context.profiled("q37.join.inventory", new HashJoinOperator(allocator, items, 0, inventory, 0));
        joined = context.profiled("q37.join.date_dim", new HashJoinOperator(allocator, joined, 5, dates, 0));
        joined = context.profiled("q37.join.sales_items", new HashJoinOperator(allocator, joined, 0, salesItems, 0));
        joined = context.profiled("q37.project.keys", projectInputs(allocator, primitiveRegistry, joined, 1, 2, 3));
        joined = context.profiled("q37.group.keys", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new CountAll()),
                joined));
        joined = context.profiled("q37.project.output", projectInputs(allocator, primitiveRegistry, joined, 0, 1, 2));
        return context.profiled("q37.topn", new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, joined));
    }

    public static Operator query40(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        LocalDate cutoffDate = LocalDate.of(2000, 3, 11);
        Operator sales = factScan(
                allocator,
                tables,
                "catalog_sales",
                "cs_order_number",
                "cs_item_sk",
                "cs_warehouse_sk",
                "cs_sold_date_sk",
                "cs_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                new int[] {0, 1},
                scannedTable(allocator, tables, "catalog_returns", "cr_order_number", "cr_item_sk", "cr_refunded_cash"),
                new int[] {0, 1},
                true);
        sales = new HashJoinOperator(
                allocator,
                sales,
                2,
                scannedTable(allocator, tables, "warehouse", "w_warehouse_sk", "w_state"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        betweenInclusive(1, 99L, 149L),
                        new String[] {"i_item_sk", "i_current_price", "i_item_id"},
                        0, 2),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                3,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        betweenInclusive(1, cutoffDate.minusDays(30).toEpochDay(), cutoffDate.plusDays(30).toEpochDay()),
                        new String[] {"d_date_sk", "d_date"},
                        0, 1),
                0);
        sales = projectQuery40Buckets(allocator, primitiveRegistry, sales, 9, 11, 4, 7, 13, cutoffDate.toEpochDay());
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2), new Sum(3)),
                sales);
        return new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, sales);
    }

    public static Operator query31(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator joined = query31ChannelCountyQuarterRevenue(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_addr_sk", "ss_ext_sales_price", 1);
        joined = new HashJoinOperator(allocator, joined, 0, query31ChannelCountyQuarterRevenue(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_addr_sk", "ss_ext_sales_price", 2), 0);
        joined = new HashJoinOperator(allocator, joined, 0, query31ChannelCountyQuarterRevenue(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_addr_sk", "ss_ext_sales_price", 3), 0);
        joined = new HashJoinOperator(allocator, joined, 0, query31ChannelCountyQuarterRevenue(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_bill_addr_sk", "ws_ext_sales_price", 1), 0);
        joined = new HashJoinOperator(allocator, joined, 0, query31ChannelCountyQuarterRevenue(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_bill_addr_sk", "ws_ext_sales_price", 2), 0);
        joined = new HashJoinOperator(allocator, joined, 0, query31ChannelCountyQuarterRevenue(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_bill_addr_sk", "ws_ext_sales_price", 3), 0);
        joined = filter(allocator, primitiveRegistry, joined, query31GrowthPredicate(1, 3, 5, 7, 9, 11));
        joined = projectQuery31Output(allocator, primitiveRegistry, joined, 0, 1, 3, 5, 7, 9, 11);
        return new SortOperator(allocator, new int[] {0}, new boolean[] {false}, joined);
    }

    public static Operator query07(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(
                allocator,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_item_sk",
                "ss_cdemo_sk",
                "ss_promo_sk",
                "ss_quantity",
                "ss_list_price",
                "ss_coupon_amt",
                "ss_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 2000),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_item_id"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                2,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_demographics",
                        and(equalUtf8(1, "M"), equalUtf8(2, "S"), equalUtf8(3, "College")),
                        new String[] {"cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                3,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "promotion",
                        or(equalUtf8(1, "N"), equalUtf8(2, "N")),
                        new String[] {"p_promo_sk", "p_channel_email", "p_channel_event"},
                        0),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 10, 4, 5, 6, 7);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Avg(1), new Avg(2), new Avg(3), new Avg(4)),
                sales);
        // Quantity is unscaled; the remaining three averages originate from physical-cent DECIMAL columns.
        sales = divideRoundedF64Inputs(allocator, primitiveRegistry, sales, 100.0, 2, 3, 4);
        return new TopNOperator(allocator, 100, 0, false, sales);
    }

    public static Operator query08(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator zipPrefixes = query08QualifiedZipPrefixes(allocator, primitiveRegistry, tables);

        Operator sales = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_store_sk", "ss_net_profit");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query08DatePredicate(),
                        new String[] {"d_date_sk", "d_qoy", "d_year"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "store", "s_store_sk", "s_store_name", "s_zip"),
                0);
        sales = projectUtf8Prefix(allocator, primitiveRegistry, sales, 6, 2, 5, 2);
        sales = new HashJoinOperator(allocator, sales, 2, zipPrefixes, 0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 0, 1);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                sales);
        return new TopNOperator(allocator, 100, 0, false, sales);
    }

    public static Operator query19(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(
                allocator,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_item_sk",
                "ss_customer_sk",
                "ss_store_sk",
                "ss_ext_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 11), equal(2, 1998)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        equal(1, 8),
                        new String[] {"i_item_sk", "i_manager_id", "i_brand_id", "i_brand", "i_manufact_id", "i_manufact"},
                        0, 2, 3, 4, 5),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                2,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                12,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_zip"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                3,
                scannedTable(allocator, tables, "store", "s_store_sk", "s_zip"),
                0);
        sales = projectQuery19ZipPrefixes(allocator, primitiveRegistry, sales, 14, 16, 7, 8, 9, 10, 4);
        sales = filter(allocator, primitiveRegistry, sales, notEqualUtf8Columns(5, 6));
        sales = projectInputs(allocator, primitiveRegistry, sales, 0, 1, 2, 3, 4);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3),
                List.of(new Sum(4)),
                sales);
        return new TopNOperator(allocator, 100, new int[] {4, 1, 0, 2, 3}, new boolean[] {true, false, false, false, false}, sales);
    }

    public static Operator query26(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return query26(TpcdsQueryContext.unprofiled(allocator, primitiveRegistry, tables));
    }

    static Operator query26(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        Operator sales = context.profiled("q26.scan.catalog_sales", factScan(
                allocator,
                tables,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_item_sk",
                "cs_bill_cdemo_sk",
                "cs_promo_sk",
                "cs_quantity",
                "cs_list_price",
                "cs_coupon_amt",
                "cs_sales_price"));
        sales = context.profiled("q26.join.customer_demographics", new HashJoinOperator(
                allocator,
                sales,
                2,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_demographics",
                        and(equalUtf8(1, "M"), equalUtf8(2, "S"), equalUtf8(3, "College")),
                        new String[] {"cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status"},
                        0),
                0).withOutputs(0, 1, 3, 4, 5, 6, 7));
        sales = context.profiled("q26.join.date_dim", new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 2000),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0).withOutputs(1, 2, 3, 4, 5, 6));
        sales = context.profiled("q26.join.item", new HashJoinOperator(
                allocator,
                sales,
                0,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_item_id"),
                0).withOutputs(1, 2, 3, 4, 5, 7));
        sales = context.profiled("q26.join.promotion", new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "promotion",
                        or(equalUtf8(1, "N"), equalUtf8(2, "N")),
                        new String[] {"p_promo_sk", "p_channel_email", "p_channel_event"},
                        0),
                0).withOutputs(1, 2, 3, 4, 5));
        sales = context.profiled("q26.project.sales", projectInputs(allocator, primitiveRegistry, sales, 4, 0, 1, 2, 3));
        sales = context.profiled("q26.group.item", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(
                        new Avg(1),
                        new Sum(2),
                        new CountColumn(2),
                        new Sum(3),
                        new CountColumn(3),
                        new Sum(4),
                        new CountColumn(4)),
                sales));
        sales = context.profiled("q26.project.averages", projectQuery26Averages(allocator, primitiveRegistry, sales));
        return context.profiled("q26.topn", new TopNOperator(allocator, 100, 0, false, sales));
    }

    public static Operator query55(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = query55SalesByBrand(allocator, primitiveRegistry, tables);
        sales = new TopNOperator(allocator, 100, new int[] {2, 0}, new boolean[] {true, false}, sales);
        return projectInputs(allocator, primitiveRegistry, sales, 0, 1, 2);
    }

    public static Operator query71(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator combined = new UnionAllOperator(3, List.of(
                query71ChannelSales(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_sold_time_sk", "ws_ext_sales_price"),
                query71ChannelSales(allocator, primitiveRegistry, tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_sold_time_sk", "cs_ext_sales_price"),
                query71ChannelSales(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_sold_time_sk", "ss_ext_sales_price")));
        combined = new HashJoinOperator(
                allocator,
                combined,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        equal(1, 1L),
                        new String[] {"i_item_sk", "i_manager_id", "i_brand_id", "i_brand"},
                        0, 2, 3),
                0);
        combined = new HashJoinOperator(
                allocator,
                combined,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "time_dim",
                        utf8AnyOf(3, Set.of("breakfast", "dinner")),
                        new String[] {"t_time_sk", "t_hour", "t_minute", "t_meal_time"},
                        0, 1, 2),
                0);
        combined = projectInputs(allocator, primitiveRegistry, combined, 4, 5, 7, 8, 2);
        combined = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3),
                List.of(new Sum(4)),
                combined);
        combined = new SortOperator(allocator, new int[] {4, 0, 2, 3}, new boolean[] {true, false, false, false}, combined);
        return projectInputs(allocator, primitiveRegistry, combined, 0, 1, 2, 3, 4);
    }

    public static Operator query72(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = profiled("q72.scan.catalog_sales", factScan(allocator, tables, "catalog_sales", "cs_item_sk", "cs_quantity", "cs_bill_cdemo_sk", "cs_bill_hdemo_sk", "cs_sold_date_sk", "cs_ship_date_sk", "cs_promo_sk", "cs_order_number"));
        HashJoinOperator soldDateJoin = new HashJoinOperator(
                allocator,
                sales,
                4,
                profiled("q72.scan.date_dim.sold", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(2, 1999),
                        new String[] {"d_date_sk", "d_week_seq", "d_year", "d_date"},
                        0, 1, 3)),
                0);
        sales = profiled("q72.join.date_dim.sold", soldDateJoin.withOutputs(0, 1, 2, 3, 5, 6, 10, 9));
        HashJoinOperator inventoryJoin = new HashJoinOperator(allocator, sales, new int[] {0, 7}, query72InventoryByWeek(allocator, primitiveRegistry, tables), new int[] {0, 3});
        sales = profiled("q72.join.inventory", inventoryJoin.withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 9, 10));
        HashJoinOperator warehouseJoin = new HashJoinOperator(
                allocator,
                sales,
                8,
                profiled("q72.scan.warehouse", scannedTable(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name")),
                0);
        sales = profiled("q72.join.warehouse", warehouseJoin.withOutputs(0, 1, 2, 3, 4, 5, 6, 9, 11, 7));
        HashJoinOperator itemJoin = new HashJoinOperator(
                allocator,
                sales,
                0,
                profiled("q72.scan.item", scannedTable(allocator, tables, "item", "i_item_sk", "i_item_desc")),
                0);
        sales = profiled("q72.join.item", itemJoin.withOutputs(1, 2, 3, 4, 5, 6, 7, 8, 9, 11));
        HashJoinOperator customerDemographicsJoin = new HashJoinOperator(
                allocator,
                sales,
                1,
                profiled("q72.scan.customer_demographics", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_demographics",
                        equalUtf8(1, "D"),
                        new String[] {"cd_demo_sk", "cd_marital_status"},
                        0)),
                0);
        // The selective customer join reduces the high-fanout inventory stream from 4.7M to 930K rows. Collapse
        // its accumulated dictionary mappings here instead of repeatedly copying the full fanout at earlier joins.
        sales = profiled("q72.join.customer_demographics", projectInputs(allocator, primitiveRegistry, customerDemographicsJoin, 0, 2, 3, 4, 5, 6, 7, 8, 9));
        HashJoinOperator householdDemographicsJoin = new HashJoinOperator(
                allocator,
                sales,
                1,
                profiled("q72.scan.household_demographics", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "household_demographics",
                        equalUtf8(1, ">10000"),
                        new String[] {"hd_demo_sk", "hd_buy_potential"},
                        0)),
                0);
        sales = profiled("q72.join.household_demographics", householdDemographicsJoin.withOutputs(0, 2, 3, 4, 5, 6, 7, 8));
        HashJoinOperator shipDateJoin = new HashJoinOperator(
                allocator,
                sales,
                1,
                profiled("q72.scan.date_dim.ship", scannedTable(allocator, tables, "date_dim", "d_date_sk", "d_date")),
                0);
        sales = profiled("q72.join.date_dim.ship", projectInputs(allocator, primitiveRegistry, shipDateJoin, 0, 2, 3, 9, 4, 5, 6, 7));
        sales = profiled("q72.semi.promotion", new SemiJoinOperator(
                allocator,
                sales,
                1,
                profiled("q72.scan.promotion", scannedTable(allocator, tables, "promotion", "p_promo_sk")),
                0,
                true,
                true));
        sales = profiled("q72.filter", filter(allocator, primitiveRegistry, sales, query72Predicate(4, 0, 3, 2)));
        sales = profiled("q72.project", projectQuery72GroupedRows(allocator, primitiveRegistry, sales, 7, 5, 6, 8));
        sales = profiled("q72.group", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4), new CountAll()),
                sales));
        return profiled("q72.topn", new TopNOperator(allocator, 100, new int[] {5, 0, 1, 2}, new boolean[] {true, false, false, false}, sales));
    }

    public static Operator query33(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator combined = new UnionAllOperator(2, List.of(
                queryGroupedChannelSalesWithAddressOffset(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_addr_sk", "ss_ext_sales_price", equalUtf8(1, "Electronics"), new String[] {"i_item_sk", "i_category", "i_manufact_id"}, 2, 1998, 5, true),
                queryGroupedChannelSalesWithAddressOffset(allocator, primitiveRegistry, tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_bill_addr_sk", "cs_ext_sales_price", equalUtf8(1, "Electronics"), new String[] {"i_item_sk", "i_category", "i_manufact_id"}, 2, 1998, 5, true),
                queryGroupedChannelSalesWithAddressOffset(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_bill_addr_sk", "ws_ext_sales_price", equalUtf8(1, "Electronics"), new String[] {"i_item_sk", "i_category", "i_manufact_id"}, 2, 1998, 5, true)));
        combined = new GroupedAggregationOperator(allocator, List.of(0), List.of(new Sum(1)), combined);
        return new TopNOperator(allocator, 100, 1, false, combined);
    }

    public static Operator query56(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator combined = new UnionAllOperator(2, List.of(
                queryGroupedChannelSalesWithAddressOffset(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_addr_sk", "ss_ext_sales_price", utf8AnyOf(1, Set.of("slate", "blanched", "burnished")), new String[] {"i_item_sk", "i_color", "i_item_id"}, 2, 2001, 2, true),
                queryGroupedChannelSalesWithAddressOffset(allocator, primitiveRegistry, tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_bill_addr_sk", "cs_ext_sales_price", utf8AnyOf(1, Set.of("slate", "blanched", "burnished")), new String[] {"i_item_sk", "i_color", "i_item_id"}, 2, 2001, 2, true),
                queryGroupedChannelSalesWithAddressOffset(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_bill_addr_sk", "ws_ext_sales_price", utf8AnyOf(1, Set.of("slate", "blanched", "burnished")), new String[] {"i_item_sk", "i_color", "i_item_id"}, 2, 2001, 2, true)));
        combined = new GroupedAggregationOperator(allocator, List.of(0), List.of(new Sum(1)), combined);
        combined = projectInputs(allocator, primitiveRegistry, combined, 0, 1);
        return new TopNOperator(allocator, 100, new int[] {1, 0}, new boolean[] {false, false}, combined);
    }

    public static Operator query60(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator combined = new UnionAllOperator(2, List.of(
                queryGroupedChannelSalesWithAddressOffset(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_addr_sk", "ss_ext_sales_price", equalUtf8(1, "Music"), new String[] {"i_item_sk", "i_category", "i_item_id"}, 2, 1998, 9, true),
                queryGroupedChannelSalesWithAddressOffset(allocator, primitiveRegistry, tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_bill_addr_sk", "cs_ext_sales_price", equalUtf8(1, "Music"), new String[] {"i_item_sk", "i_category", "i_item_id"}, 2, 1998, 9, true),
                queryGroupedChannelSalesWithAddressOffset(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_bill_addr_sk", "ws_ext_sales_price", equalUtf8(1, "Music"), new String[] {"i_item_sk", "i_category", "i_item_id"}, 2, 1998, 9, true)));
        combined = new GroupedAggregationOperator(allocator, List.of(0), List.of(new Sum(1)), combined);
        return new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, combined);
    }

    public static Operator query65(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator storeItemSales = query65StoreItemSales(allocator, primitiveRegistry, tables);
        Operator storeThresholds = query65StoreThresholds(allocator, primitiveRegistry, tables);

        Operator joined = profiled("q65.join.threshold", new HashJoinOperator(allocator, storeItemSales, 0, storeThresholds, 0));
        joined = profiled("q65.filter.threshold", filter(allocator, primitiveRegistry, joined, query65ThresholdPredicate(2, 4)));
        joined = profiled("q65.join.store", new HashJoinOperator(
                allocator,
                joined,
                0,
                scannedTable(allocator, tables, "store", "s_store_sk", "s_store_name"),
                0));
        joined = profiled("q65.join.item", new HashJoinOperator(
                allocator,
                joined,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand"),
                0));
        joined = profiled("q65.project", projectInputs(allocator, primitiveRegistry, joined, 6, 8, 2, 9, 10, 11));
        return profiled("q65.top", new TopNOperator(allocator, 100, new int[] {0, 1, 5, 2}, new boolean[] {false, false, false, false}, joined));
    }

    public static Operator query66(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator webChannel = query66ChannelMonthlyWarehouseRollup(
                allocator,
                primitiveRegistry,
                tables,
                "web_sales",
                "ws_sold_date_sk",
                "ws_sold_time_sk",
                "ws_ship_mode_sk",
                "ws_warehouse_sk",
                "ws_quantity",
                "ws_ext_sales_price",
                "ws_net_paid");
        Operator catalogChannel = query66ChannelMonthlyWarehouseRollup(
                allocator,
                primitiveRegistry,
                tables,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_sold_time_sk",
                "cs_ship_mode_sk",
                "cs_warehouse_sk",
                "cs_quantity",
                "cs_sales_price",
                "cs_net_paid_inc_tax");
        webChannel = profiled("q66.web.project.channel_output", projectQuery66Output(allocator, primitiveRegistry, webChannel));
        catalogChannel = profiled("q66.catalog.project.channel_output", projectQuery66Output(allocator, primitiveRegistry, catalogChannel));
        Operator combined = profiled("q66.union", new UnionAllOperator(44, List.of(webChannel, catalogChannel)));
        combined = profiled("q66.group.combined", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5, 6, 7),
                List.of(
                        new Sum(8), new Sum(9), new Sum(10), new Sum(11), new Sum(12), new Sum(13),
                        new Sum(14), new Sum(15), new Sum(16), new Sum(17), new Sum(18), new Sum(19),
                        new Sum(20), new Sum(21), new Sum(22), new Sum(23), new Sum(24), new Sum(25),
                        new Sum(26), new Sum(27), new Sum(28), new Sum(29), new Sum(30), new Sum(31),
                        new Sum(32), new Sum(33), new Sum(34), new Sum(35), new Sum(36), new Sum(37),
                        new Sum(38), new Sum(39), new Sum(40), new Sum(41), new Sum(42), new Sum(43)),
                combined));
        return profiled("q66.topn", new TopNOperator(allocator, 100, new int[] {0}, new boolean[] {false}, combined));
    }

    public static Operator query98(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        // Same revenue-ratio-by-class shape as Q12 over store_sales, but the final ordering is a full SORT (no LIMIT):
        // item(category) ⋈ date_dim(30-day window); GROUP BY item attrs sum(ext_sales_price); PARTITION SUM over
        // i_class; revenue ratio; ORDER BY category, class, item id/desc, ratio.
        Operator facts = query98RevenueWindowed(allocator, primitiveRegistry, tables);
        facts = projectQuery12RevenueRatio(allocator, primitiveRegistry, facts);
        return new SortOperator(allocator, new int[] {2, 3, 0, 1, 6}, new boolean[] {false, false, false, false, false}, facts);
    }

    /** Q98 sub-stage (tested independently against Trino): store_sales ⋈ item(category) ⋈ date_dim, grouped sums. */
    static Operator query98RevenueGrouped(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator facts = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price");
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        query12CategoryPredicate(),
                        new String[] {"i_item_sk", "i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price"}),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query12DatePredicate(),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);
        facts = projectInputs(allocator, primitiveRegistry, facts, 4, 5, 6, 7, 8, 2);
        return new GroupedAggregationOperator(allocator, List.of(0, 1, 2, 3, 4), List.of(new Sum(5)), facts);
    }

    /** Q98 sub-stage (tested independently against Trino): the grouped sums with a PARTITION SUM over i_class. */
    static Operator query98RevenueWindowed(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return new WindowOperator(
                allocator,
                query98RevenueGrouped(allocator, primitiveRegistry, tables),
                new int[] {3},
                new int[0],
                new boolean[0],
                List.of(new PartitionSumI64WindowFunction(5)));
    }

    public static Operator query89(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator monthlySales = query89MonthlySalesByCategoryClassBrandStore(allocator, primitiveRegistry, tables);
        monthlySales = new WindowOperator(
                allocator,
                monthlySales,
                new int[] {0, 2, 3, 4},
                new int[0],
                new boolean[0],
                List.of(new PartitionAverageI64WindowFunction(6)));
        monthlySales = filter(allocator, primitiveRegistry, monthlySales, query53QuarterlyDeviationPredicate(6, 7));
        monthlySales = projectQuery89SortKey(allocator, primitiveRegistry, monthlySales);
        monthlySales = new TopNOperator(allocator, 100, new int[] {8, 3}, new boolean[] {false, false}, monthlySales);
        return projectInputs(allocator, primitiveRegistry, monthlySales, 0, 1, 2, 3, 4, 5, 6, 7);
    }

    public static Operator query63(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator monthlySales = query63MonthlySalesByManager(allocator, primitiveRegistry, tables);
        monthlySales = new WindowOperator(
                allocator,
                monthlySales,
                new int[] {0},
                new int[0],
                new boolean[0],
                List.of(new PartitionAverageI64WindowFunction(2)));
        monthlySales = filter(allocator, primitiveRegistry, monthlySales, query53QuarterlyDeviationPredicate(2, 3));
        monthlySales = projectInputs(allocator, primitiveRegistry, monthlySales, 0, 2, 3);
        return new TopNOperator(allocator, 100, new int[] {0, 2, 1}, new boolean[] {false, false, false}, monthlySales);
    }

    public static Operator query86(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator grouped = new GroupIdOperator(
                allocator,
                query86SalesByCategoryClass(allocator, primitiveRegistry, tables),
                new int[][] {
                        {-1, -1, 2},
                        {0, -1, 2},
                        {0, 1, 2}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy());
        grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 3),
                List.of(new Sum(2)),
                grouped);
        grouped = projectQuery86Rollup(allocator, primitiveRegistry, grouped);
        grouped = new WindowOperator(
                allocator,
                grouped,
                new int[] {3, 4},
                new int[] {2},
                new boolean[] {true},
                List.of(new RankWindowFunction(new int[] {2}, new boolean[] {true})));
        grouped = new TopNOperator(allocator, 100, new int[] {3, 4, 5}, new boolean[] {true, false, false}, grouped);
        return projectInputs(allocator, primitiveRegistry, grouped, 2, 0, 1, 3, 5);
    }

    public static Operator query36(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator grouped = new GroupIdOperator(
                allocator,
                query36SalesByCategoryClass(allocator, primitiveRegistry, tables),
                new int[][] {
                        {-1, -1, 2, 3},
                        {0, -1, 2, 3},
                        {0, 1, 2, 3}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy());
        grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 4),
                List.of(new Sum(2), new Sum(3)),
                grouped);
        grouped = projectQuery36Rollup(allocator, primitiveRegistry, grouped);
        grouped = new TopNRankingOperator(allocator, 100, new int[] {3, 4}, new int[] {2}, new boolean[] {false}, grouped, EngineResources.from(allocator).operatorResources().topNRankingPolicy());
        grouped = new TopNOperator(allocator, 100, new int[] {3, 4, 5, 0, 1}, new boolean[] {true, false, false, false, false}, grouped);
        return projectInputs(allocator, primitiveRegistry, grouped, 2, 0, 1, 3, 5);
    }

    public static Operator query49(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator store = query49Channel(allocator, primitiveRegistry, tables, "store", "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ticket_number", "ss_quantity", "ss_net_paid", "ss_net_profit", "store_returns", "sr_item_sk", "sr_ticket_number", "sr_return_quantity", "sr_return_amt");
        Operator catalog = query49Channel(allocator, primitiveRegistry, tables, "catalog", "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_order_number", "cs_quantity", "cs_net_paid", "cs_net_profit", "catalog_returns", "cr_item_sk", "cr_order_number", "cr_return_quantity", "cr_return_amount");
        Operator web = query49Channel(allocator, primitiveRegistry, tables, "web", "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_order_number", "ws_quantity", "ws_net_paid", "ws_net_profit", "web_returns", "wr_item_sk", "wr_order_number", "wr_return_quantity", "wr_return_amt");
        Operator combined = new UnionAllOperator(5, List.of(store, catalog, web));
        return new TopNOperator(allocator, 100, new int[] {0, 3, 4, 1}, new boolean[] {false, false, false, false}, combined);
    }

    public static Operator query47(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator current = projectQuery47CurrentRows(allocator, primitiveRegistry, query47MonthlySales(allocator, primitiveRegistry, tables));
        Operator previous = projectQuery47AdjacentRows(allocator, primitiveRegistry, query47MonthlySales(allocator, primitiveRegistry, tables), true);
        Operator next = projectQuery47AdjacentRows(allocator, primitiveRegistry, query47MonthlySales(allocator, primitiveRegistry, tables), false);

        Operator joined = profiled("q47.join.previous", new HashJoinOperator(allocator, current, new int[] {0, 1, 2, 3, 8}, previous, new int[] {0, 1, 2, 3, 5}));
        joined = profiled("q47.join.next", new HashJoinOperator(allocator, joined, new int[] {0, 1, 2, 3, 8}, next, new int[] {0, 1, 2, 3, 5}));
        joined = projectQuery47Output(allocator, primitiveRegistry, joined);
        joined = new BatchSliceOperator(allocator, 4_096, joined);
        joined = profiled("q47.filter.deviation", filter(allocator, primitiveRegistry, joined, query53QuarterlyDeviationPredicate(7, 6)));
        joined = projectQuery47SortKey(allocator, primitiveRegistry, joined);
        joined = profiled("q47.topn", new TopNOperator(allocator, 100, new int[] {10, 2}, new boolean[] {false, false}, joined));
        return projectInputs(allocator, primitiveRegistry, joined, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    public static Operator query09(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator reason = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "reason",
                equal(0, 1L),
                new String[] {"r_reason_sk"},
                0);
        reason = new NestedLoopJoinOperator(allocator, reason, query09Bucket(allocator, primitiveRegistry, tables, 1, 20, 74_129L));
        reason = new NestedLoopJoinOperator(allocator, reason, query09Bucket(allocator, primitiveRegistry, tables, 21, 40, 122_840L));
        reason = new NestedLoopJoinOperator(allocator, reason, query09Bucket(allocator, primitiveRegistry, tables, 41, 60, 56_580L));
        reason = new NestedLoopJoinOperator(allocator, reason, query09Bucket(allocator, primitiveRegistry, tables, 61, 80, 10_097L));
        reason = new NestedLoopJoinOperator(allocator, reason, query09Bucket(allocator, primitiveRegistry, tables, 81, 100, 165_306L));
        return projectInputs(allocator, primitiveRegistry, reason, 1, 2, 3, 4, 5);
    }

    public static Operator query11(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator joined = query11ChannelYearTotal(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_discount_amt", 2001, false);
        joined = profiled("q11.join.store2002", new HashJoinOperator(allocator, joined, 0, query11ChannelYearTotal(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_list_price", "ss_ext_discount_amt", 2002, true), 0)
                .withOutputs(0, 1, 3, 4, 5, 6, 7, 8));
        joined = profiled("q11.join.web2001", new HashJoinOperator(allocator, joined, 0, query11ChannelYearTotal(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_discount_amt", 2001, false), 0)
                .withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 9));
        joined = profiled("q11.join.web2002", new HashJoinOperator(allocator, joined, 0, query11ChannelYearTotal(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_ext_list_price", "ws_ext_discount_amt", 2002, false), 0)
                .withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 8, 10));
        joined = profiled("q11.filter.growth", filter(allocator, primitiveRegistry, joined, query11GrowthPredicate(1, 7, 8, 9)));
        joined = profiled("q11.project.output", projectInputs(allocator, primitiveRegistry, joined, 0, 2, 3, 4, 5, 6));
        return profiled("q11.topn", new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3}, new boolean[] {false, false, false, false}, joined));
    }

    public static Operator query14(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator grouped = profiled("q14.group_id", new GroupIdOperator(
                allocator,
                profiled("q14.union.channels", new UnionAllOperator(6, List.of(
                        query14ChannelBranch(allocator, primitiveRegistry, tables, query14CrossItems(allocator, primitiveRegistry, tables), query14AverageSales(allocator, primitiveRegistry, tables), "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_quantity", "ss_list_price", "store"),
                        query14ChannelBranch(allocator, primitiveRegistry, tables, query14CrossItems(allocator, primitiveRegistry, tables), query14AverageSales(allocator, primitiveRegistry, tables), "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_quantity", "cs_list_price", "catalog"),
                        query14ChannelBranch(allocator, primitiveRegistry, tables, query14CrossItems(allocator, primitiveRegistry, tables), query14AverageSales(allocator, primitiveRegistry, tables), "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_quantity", "ws_list_price", "web")))),
                new int[][] {
                        {-1, -1, -1, -1, 4, 5},
                        {0, -1, -1, -1, 4, 5},
                        {0, 1, -1, -1, 4, 5},
                        {0, 1, 2, -1, 4, 5},
                        {0, 1, 2, 3, 4, 5}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy()));
        grouped = profiled("q14.group.rollup", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 6),
                List.of(new Sum(4), new Sum(5)),
                grouped));
        grouped = profiled("q14.project.output", projectInputs(allocator, primitiveRegistry, grouped, 0, 1, 2, 3, 5, 6));
        return profiled("q14.topn", new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3}, new boolean[] {false, false, false, false}, grouped));
    }

    public static Operator query15(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_bill_customer_sk", "cs_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                4,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_zip", "ca_state"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 2), equal(2, 2001)),
                        new String[] {"d_date_sk", "d_qoy", "d_year"},
                        0),
                0);
        sales = filter(allocator, primitiveRegistry, sales, query15ZipStateOrPricePredicate(6, 7, 2));
        sales = projectInputs(allocator, primitiveRegistry, sales, 6, 2);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                sales);
        return new TopNOperator(allocator, 100, 0, false, sales);
    }

    public static Operator query41(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator eligibleManufacturers = filter(
                allocator,
                primitiveRegistry,
                itemScan(allocator, tables, "i_manufact", "i_category", "i_color", "i_units", "i_size"),
                query41EligibilityPredicate(1, 2, 3, 4));
        eligibleManufacturers = projectInputs(allocator, primitiveRegistry, eligibleManufacturers, 0);
        Operator probe = filter(
                allocator,
                primitiveRegistry,
                itemScan(allocator, tables, "i_product_name", "i_manufact_id", "i_manufact"),
                and(
                        greaterThan(1, 737),
                        lessThan(1, 779)));
        Operator matched = new SemiJoinOperator(allocator, probe, 2, eligibleManufacturers, 0);
        Operator productNames = projectInputs(allocator, primitiveRegistry, matched, 0);
        Operator distinct = new MarkDistinctOperator(allocator, 0, productNames, EngineResources.from(allocator).operatorResources());
        return new TopNOperator(allocator, 100, 0, false, distinct);
    }

    public static Operator query44(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator ascending = query44RankedItems(allocator, primitiveRegistry, tables, false);
        Operator descending = query44RankedItems(allocator, primitiveRegistry, tables, true);
        Operator joined = new HashJoinOperator(allocator, ascending, 1, descending, 1);
        joined = new HashJoinOperator(allocator, joined, 0, scannedTable(allocator, tables, "item", "i_item_sk", "i_product_name"), 0);
        joined = new HashJoinOperator(allocator, joined, 2, scannedTable(allocator, tables, "item", "i_item_sk", "i_product_name"), 0);
        Operator projected = projectInputs(allocator, primitiveRegistry, joined, 1, 5, 7);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, projected);
    }

    public static Operator query45(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator projected = query45Rows(allocator, primitiveRegistry, tables);
        Operator aggregated = profiled("q45.group.zip_city", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2)),
                projected));
        return profiled("q45.topn", new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, aggregated));
    }

    /** Diagnostic pre-aggregation pipeline; query45 consumes this exact operator tree. */
    public static Operator query45Rows(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator filtered = query45WebSales(allocator, primitiveRegistry, tables);
        filtered = profiled("q45.join.customer", new HashJoinOperator(
                allocator,
                filtered,
                2,
                profiled("q45.scan.customer", scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk")),
                0));
        filtered = profiled("q45.join.customer_address", new HashJoinOperator(
                allocator,
                filtered,
                5,
                profiled("q45.scan.customer_address", scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_city", "ca_zip")),
                0));
        filtered = profiled("q45.join.date_dim", new HashJoinOperator(
                allocator,
                filtered,
                1,
                profiled("q45.scan.date_dim", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 2), equal(2, 2001)),
                        new String[] {"d_date_sk", "d_qoy", "d_year"},
                        0)),
                0));
        filtered = profiled("q45.join.item", new HashJoinOperator(
                allocator,
                filtered,
                0,
                profiled("q45.scan.item", scannedTable(allocator, tables, "item", "i_item_sk", "i_item_id")),
                0));
        filtered = profiled("q45.semi.eligible_items", new SemiJoinOperator(
                allocator,
                filtered,
                11,
                profiled("q45.scan.prime_item_ids", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        query45ItemPredicate(),
                        new String[] {"i_item_sk", "i_item_id"},
                        1)),
                0,
                true,
                true));
        filtered = profiled("q45.filter.project.sales", filter(allocator, primitiveRegistry, filtered, query45ZipOrItemPredicate(8, 12)));
        return profiled("q45.project.group_inputs", projectInputs(allocator, primitiveRegistry, filtered, 8, 7, 3));
    }

    /** Diagnostic scan leaf; query45 consumes this exact operator. */
    public static Operator query45WebSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return profiled("q45.scan.web_sales", factScan(allocator, tables, "web_sales", "ws_item_sk", "ws_sold_date_sk", "ws_bill_customer_sk", "ws_sales_price"));
    }

    public static Operator query51(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator joined = profiled("q51.full_join.channels", FullJoinOperator.sorted(
                allocator,
                query51Channel(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_sales_price"),
                new int[] {0, 1},
                query51Channel(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_sales_price"),
                new int[] {0, 1},
                EngineResources.from(allocator).operatorResources().fullJoinPolicy()));

        Variable item = new Variable(0);
        Variable date = new Variable(1);
        joined = profiled("q51.project.coalesced_keys", new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(item, new Call("coalesce_i64", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(new Input(3), Stream.VALUES))), AllMask.ALL),
                                new Assignment(date, new Call("coalesce_i64", List.of(
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(new Input(4), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(item, Stream.VALUES),
                                new Reference(date, Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES))),
                primitiveRegistry,
                joined));
        joined = profiled("q51.window.merged", new WindowOperator(
                allocator,
                joined,
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(
                        new RunningMaxI64WindowFunction(2),
                        new RunningMaxI64WindowFunction(3))));
        joined = profiled("q51.filter.cumulative", filter(allocator, primitiveRegistry, joined, query51CumulativePredicate(4, 5)));
        return profiled("q51.top100", new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, joined));
    }

    public static Operator query53(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator quarterlySales = query53QuarterlySalesByManufact(allocator, primitiveRegistry, tables);
        quarterlySales = new WindowOperator(
                allocator,
                quarterlySales,
                new int[] {0},
                new int[0],
                new boolean[0],
                List.of(new PartitionAverageI64WindowFunction(2)));
        quarterlySales = filter(allocator, primitiveRegistry, quarterlySales, query53QuarterlyDeviationPredicate(2, 3));
        quarterlySales = projectInputs(allocator, primitiveRegistry, quarterlySales, 0, 2, 3);
        return new TopNOperator(allocator, 100, new int[] {2, 1, 0}, new boolean[] {false, false, false}, quarterlySales);
    }

    public static Operator query54(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator lowerMonthSequence = query54ScalarMonthBoundary(allocator, primitiveRegistry, tables, 1);
        Operator upperMonthSequence = query54ScalarMonthBoundary(allocator, primitiveRegistry, tables, 3);

        Operator revenue = query54RevenueByCustomer(allocator, primitiveRegistry, tables);
        revenue = profiled("q54.join.lower_month_seq", new NestedLoopJoinOperator(allocator, revenue, lowerMonthSequence));
        revenue = profiled("q54.join.upper_month_seq", new NestedLoopJoinOperator(allocator, revenue, upperMonthSequence));
        revenue = profiled("q54.filter.month_between", filter(allocator, primitiveRegistry, revenue, query54MonthBetweenPredicate(2, 3, 4)));
        revenue = projectInputs(allocator, primitiveRegistry, revenue, 0, 1);
        revenue = profiled("q54.group.customer", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                revenue));
        revenue = projectQuery54Segment(allocator, primitiveRegistry, revenue);

        Operator grouped = profiled("q54.group.segment", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new CountAll()),
                revenue));
        grouped = new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, grouped);
        return projectQuery54Output(allocator, primitiveRegistry, grouped);
    }

    public static Operator query58(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator store = query58Channel(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price");
        Operator catalog = query58Channel(allocator, primitiveRegistry, tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_ext_sales_price");
        Operator web = query58Channel(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_ext_sales_price");

        Operator joined = new HashJoinOperator(allocator, store, 0, catalog, 0);
        joined = new HashJoinOperator(allocator, joined, 0, web, 0);
        joined = projectInputs(allocator, primitiveRegistry, joined, 0, 1, 3, 5);
        joined = filter(allocator, primitiveRegistry, joined, query58SimilarityPredicate(1, 2, 3));
        joined = new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, joined);
        return projectThreeChannelOutput(allocator, primitiveRegistry, joined, 10_000L);
    }

    public static Operator query59(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator currentYear = projectQuery59AdjustedWeek(allocator, primitiveRegistry, query59WeeklyStoreSales(allocator, primitiveRegistry, tables, 1212, 1223));
        Operator nextYear = query59WeeklyStoreSales(allocator, primitiveRegistry, tables, 1224, 1235);

        Operator joined = new HashJoinOperator(allocator, currentYear, new int[] {0, 2}, nextYear, new int[] {0, 1});
        joined = new HashJoinOperator(allocator, joined, 2, scannedTable(allocator, tables, "store", "s_store_sk", "s_store_name", "s_store_id"), 0);
        joined = projectQuery59Output(allocator, primitiveRegistry, joined);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, joined);
    }

    public static Operator query61(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator promotionalSales = query61Sales(allocator, primitiveRegistry, tables, true);
        Operator totalSales = query61Sales(allocator, primitiveRegistry, tables, false);
        Operator joined = new NestedLoopJoinOperator(allocator, promotionalSales, totalSales);
        joined = new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, joined);
        return projectQuery61Output(allocator, primitiveRegistry, joined);
    }

    public static Operator query57(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return query57(TpcdsQueryContext.unprofiled(allocator, primitiveRegistry, tables));
    }

    static Operator query57(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        Operator rankedMonthlySales = query57MonthlyRankedSales(context);
        Operator currentRows = projectQuery57CurrentRows(context, rankedMonthlySales);
        Operator previousRows = projectQuery57AdjacentRows(context, query57MonthlyRankedSales(context), true);
        Operator nextRows = projectQuery57AdjacentRows(context, query57MonthlyRankedSales(context), false);

        Operator monthlySales = context.profiled("q57.join.previous", new HashJoinOperator(allocator, currentRows, new int[] {0, 1, 2, 7}, previousRows, new int[] {0, 1, 2, 4}));
        monthlySales = context.profiled("q57.join.next", new HashJoinOperator(allocator, monthlySales, new int[] {0, 1, 2, 7}, nextRows, new int[] {0, 1, 2, 4}));
        monthlySales = context.profiled("q57.filter.deviation", filter(allocator, primitiveRegistry, monthlySales, query53QuarterlyDeviationPredicate(6, 5)));
        monthlySales = projectQuery57Output(context, monthlySales);
        monthlySales = projectQuery57SortKey(context, monthlySales);
        monthlySales = context.profiled("q57.topn", new TopNOperator(allocator, 100, new int[] {9, 2}, new boolean[] {false, false}, monthlySales));
        return context.profiled("q57.project.final", projectInputs(allocator, primitiveRegistry, monthlySales, 0, 1, 2, 3, 4, 5, 6, 7, 8));
    }

    public static Operator query62(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator allowedShipDates = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                and(greaterThan(1, 1199), lessThan(1, 1212)),
                new String[] {"d_date_sk", "d_month_seq"},
                0);
        Operator warehouseNames = scannedTable(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name");
        Operator shipModeNames = scannedTable(allocator, tables, "ship_mode", "sm_ship_mode_sk", "sm_type");
        Operator webSiteNames = scannedTable(allocator, tables, "web_site", "web_site_sk", "web_name");
        Operator joined = factScan(allocator, tables, "web_sales", "ws_ship_date_sk", "ws_sold_date_sk", "ws_warehouse_sk", "ws_ship_mode_sk", "ws_web_site_sk");
        joined = new HashJoinOperator(allocator, joined, 0, allowedShipDates, 0);
        joined = new HashJoinOperator(allocator, joined, 2, warehouseNames, 0);
        joined = new HashJoinOperator(allocator, joined, 3, shipModeNames, 0);
        joined = new HashJoinOperator(allocator, joined, 4, webSiteNames, 0);
        Operator projected = projectShippingBuckets(allocator, primitiveRegistry, joined, 7, 9, 11, 0, 1);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4), new Sum(5), new Sum(6), new Sum(7)),
                projected);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, aggregated);
    }

    public static Operator query96(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator timeKeys = filteredProjectedTable(allocator, primitiveRegistry, tables, "time_dim", query96TimePredicate(), new String[] {"t_time_sk", "t_hour", "t_minute"});
        Operator householdKeys = filteredProjectedTable(allocator, primitiveRegistry, tables, "household_demographics", equal(1, 7), new String[] {"hd_demo_sk", "hd_dep_count"});
        Operator storeKeys = filteredProjectedTable(allocator, primitiveRegistry, tables, "store", equalUtf8(1, "ese"), new String[] {"s_store_sk", "s_store_name"});
        Operator filtered = factScan(allocator, tables, "store_sales", "ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk");
        filtered = new HashJoinOperator(allocator, filtered, 0, timeKeys, 0);
        filtered = new HashJoinOperator(allocator, filtered, 1, householdKeys, 0);
        filtered = new HashJoinOperator(allocator, filtered, 2, storeKeys, 0);
        return new AggregationOperator(allocator, List.of(new CountAll()), filtered);
    }

    public static Operator query99(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator allowedShipDates = profiled("q99.scan.allowed_ship_dates", filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                and(greaterThan(1, 1199), lessThan(1, 1212)),
                new String[] {"d_date_sk", "d_month_seq"},
                0));
        Operator warehouseNames = profiled("q99.scan.warehouse", scannedTable(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name"));
        Operator shipModeNames = profiled("q99.scan.ship_mode", scannedTable(allocator, tables, "ship_mode", "sm_ship_mode_sk", "sm_type"));
        Operator callCenterNames = profiled("q99.scan.call_center", scannedTable(allocator, tables, "call_center", "cc_call_center_sk", "cc_name"));
        Operator joined = profiled("q99.scan.catalog_sales", factScan(allocator, tables, "catalog_sales", "cs_ship_date_sk", "cs_sold_date_sk", "cs_warehouse_sk", "cs_ship_mode_sk", "cs_call_center_sk"));
        joined = profiled("q99.join.date_dim", new HashJoinOperator(allocator, joined, 0, allowedShipDates, 0));
        joined = profiled("q99.join.warehouse", new HashJoinOperator(allocator, joined, 2, warehouseNames, 0));
        joined = profiled("q99.join.ship_mode", new HashJoinOperator(allocator, joined, 3, shipModeNames, 0));
        joined = profiled("q99.join.call_center", new HashJoinOperator(allocator, joined, 4, callCenterNames, 0));
        Operator projected = profiled("q99.project.shipping_buckets", projectShippingBuckets(allocator, primitiveRegistry, joined, 7, 9, 11, 0, 1));
        Operator aggregated = profiled("q99.aggregate.shipping_buckets", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4), new Sum(5), new Sum(6), new Sum(7)),
                projected));
        return profiled("q99.topn", new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, aggregated));
    }

    public static Operator query10(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        FilterSpec eligibleDates = yearMonthRangePredicate(1, 2, 2002, 1, 4);
        Operator eligibleAddresses = filteredProjectedTable(allocator, primitiveRegistry, tables, "customer_address", utf8AnyOf(1, Set.of("Rush County", "Toole County", "Jefferson County", "Dona Ana County", "La Porte County")), new String[] {"ca_address_sk", "ca_county"});
        Operator storeCustomers = customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", eligibleDates);
        Operator otherCustomers = new UnionAllOperator(1, List.of(
                customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", eligibleDates),
                customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "catalog_sales", "cs_ship_customer_sk", "cs_sold_date_sk", eligibleDates)));
        Operator eligibleCustomers = customerScan(allocator, tables, "c_current_addr_sk", "c_customer_sk", "c_current_cdemo_sk");
        eligibleCustomers = new HashJoinOperator(allocator, eligibleCustomers, 0, eligibleAddresses, 0);
        eligibleCustomers = new SemiJoinOperator(allocator, eligibleCustomers, 1, storeCustomers, 0);
        eligibleCustomers = new SemiJoinOperator(allocator, eligibleCustomers, 1, otherCustomers, 0);
        Operator joined = new HashJoinOperator(
                allocator,
                eligibleCustomers,
                2,
                factScan(
                        allocator,
                        tables,
                        "customer_demographics",
                        "cd_demo_sk",
                        "cd_gender",
                        "cd_marital_status",
                        "cd_education_status",
                        "cd_purchase_estimate",
                        "cd_credit_rating",
                        "cd_dep_count",
                        "cd_dep_employed_count",
                        "cd_dep_college_count"),
                0);
        Operator projected = projectInputs(allocator, primitiveRegistry, joined, 6, 7, 8, 9, 10, 11, 12, 13);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5, 6, 7),
                List.of(new CountAll()),
                projected);
        Operator reordered = projectInputs(allocator, primitiveRegistry, aggregated, 0, 1, 2, 8, 3, 8, 4, 8, 5, 8, 6, 8, 7, 8);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 4, 6, 8, 10, 12}, new boolean[] {false, false, false, false, false, false, false, false}, reordered);
    }

    public static Operator query35(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator reordered = query35Reordered(allocator, primitiveRegistry, tables);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3, 8, 13}, new boolean[] {false, false, false, false, false, false}, reordered);
    }

    static Operator query35Projected(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        FilterSpec eligibleDates = yearQuarterRangePredicate(1, 2, 2002, 1, 3);
        Operator storeCustomers = customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", new String[] {"d_date_sk", "d_year", "d_qoy"}, eligibleDates);
        Operator otherCustomers = new UnionAllOperator(1, List.of(
                customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", new String[] {"d_date_sk", "d_year", "d_qoy"}, eligibleDates),
                customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "catalog_sales", "cs_ship_customer_sk", "cs_sold_date_sk", new String[] {"d_date_sk", "d_year", "d_qoy"}, eligibleDates)));
        Operator eligibleCustomers = query35EligibleCustomers(allocator, primitiveRegistry, tables, storeCustomers, otherCustomers);
        eligibleCustomers = new HashJoinOperator(allocator, eligibleCustomers, 0, factScan(allocator, tables, "customer_address", "ca_address_sk", "ca_state"), 0);
        Operator joined = new HashJoinOperator(
                allocator,
                eligibleCustomers,
                2,
                factScan(
                        allocator,
                        tables,
                        "customer_demographics",
                        "cd_demo_sk",
                        "cd_gender",
                        "cd_marital_status",
                        "cd_education_status",
                        "cd_purchase_estimate",
                        "cd_credit_rating",
                        "cd_dep_count",
                        "cd_dep_employed_count",
                        "cd_dep_college_count"),
                0);
        return projectInputs(allocator, primitiveRegistry, joined, 4, 6, 7, 11, 12, 13);
    }

    static Operator query35EligibleCustomers(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        FilterSpec eligibleDates = yearQuarterRangePredicate(1, 2, 2002, 1, 3);
        Operator storeCustomers = customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", new String[] {"d_date_sk", "d_year", "d_qoy"}, eligibleDates);
        Operator otherCustomers = new UnionAllOperator(1, List.of(
                customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", new String[] {"d_date_sk", "d_year", "d_qoy"}, eligibleDates),
                customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "catalog_sales", "cs_ship_customer_sk", "cs_sold_date_sk", new String[] {"d_date_sk", "d_year", "d_qoy"}, eligibleDates)));
        return query35EligibleCustomers(allocator, primitiveRegistry, tables, storeCustomers, otherCustomers);
    }

    private static Operator query35EligibleCustomers(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, Operator storeCustomers, Operator otherCustomers)
    {
        Operator eligibleCustomers = customerScan(allocator, tables, "c_current_addr_sk", "c_customer_sk", "c_current_cdemo_sk");
        eligibleCustomers = new SemiJoinOperator(allocator, eligibleCustomers, 1, storeCustomers, 0);
        eligibleCustomers = new SemiJoinOperator(allocator, eligibleCustomers, 1, otherCustomers, 0);
        return eligibleCustomers;
    }

    static Operator query35Aggregated(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5),
                List.of(
                        new CountAll(),
                        new Min(3),
                        new Max(3),
                        new Avg(3),
                        new CountAll(),
                        new Min(4),
                        new Max(4),
                        new Avg(4),
                        new CountAll(),
                        new Min(5),
                        new Max(5),
                        new Avg(5)),
                query35Projected(allocator, primitiveRegistry, tables));
    }

    static Operator query35Reordered(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return projectInputs(allocator, primitiveRegistry, query35Aggregated(allocator, primitiveRegistry, tables), 0, 1, 2, 3, 6, 7, 8, 9, 4, 10, 11, 12, 13, 5, 14, 15, 16, 17);
    }

    public static Operator query69(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        FilterSpec eligibleDates = yearMonthRangePredicate(1, 2, 2001, 4, 6);
        Operator eligibleAddresses = filteredProjectedTable(allocator, primitiveRegistry, tables, "customer_address", utf8AnyOf(1, Set.of("KY", "GA", "NM")), new String[] {"ca_address_sk", "ca_state"});
        Operator storeCustomers = customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", eligibleDates);
        Operator excludedCustomers = new UnionAllOperator(1, List.of(
                customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", eligibleDates),
                customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "catalog_sales", "cs_ship_customer_sk", "cs_sold_date_sk", eligibleDates)));
        Operator eligibleCustomers = customerScan(allocator, tables, "c_current_addr_sk", "c_customer_sk", "c_current_cdemo_sk");
        eligibleCustomers = new HashJoinOperator(allocator, eligibleCustomers, 0, eligibleAddresses, 0);
        eligibleCustomers = new SemiJoinOperator(allocator, eligibleCustomers, 1, storeCustomers, 0);
        eligibleCustomers = new SemiJoinOperator(allocator, eligibleCustomers, 1, excludedCustomers, 0, false);
        Operator joined = new HashJoinOperator(
                allocator,
                eligibleCustomers,
                2,
                factScan(
                        allocator,
                        tables,
                        "customer_demographics",
                        "cd_demo_sk",
                        "cd_gender",
                        "cd_marital_status",
                        "cd_education_status",
                        "cd_purchase_estimate",
                        "cd_credit_rating",
                        "cd_dep_count",
                        "cd_dep_employed_count",
                        "cd_dep_college_count"),
                0);
        Operator projected = projectInputs(allocator, primitiveRegistry, joined, 6, 7, 8, 9, 10);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4),
                List.of(new CountAll()),
                projected);
        Operator reordered = projectInputs(allocator, primitiveRegistry, aggregated, 0, 1, 2, 5, 3, 5, 4, 5);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 4, 6}, new boolean[] {false, false, false, false, false}, reordered);
    }

    public static Operator query67(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator grouped = new GroupIdOperator(
                allocator,
                query67SalesByRollupKey(allocator, primitiveRegistry, tables),
                new int[][] {
                        {-1, -1, -1, -1, -1, -1, -1, -1, 8},
                        {0, -1, -1, -1, -1, -1, -1, -1, 8},
                        {0, 1, -1, -1, -1, -1, -1, -1, 8},
                        {0, 1, 2, -1, -1, -1, -1, -1, 8},
                        {0, 1, 2, 3, -1, -1, -1, -1, 8},
                        {0, 1, 2, 3, 4, -1, -1, -1, 8},
                        {0, 1, 2, 3, 4, 5, -1, -1, 8},
                        {0, 1, 2, 3, 4, 5, 6, -1, 8},
                        {0, 1, 2, 3, 4, 5, 6, 7, 8}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy());
        grouped = profiled("q67.group.partial", new SqlStageAggregationOperator(
                allocator,
                grouped,
                2,
                new int[0],
                List.of(SqlStageAggregationOperator.aggregate(
                        List.of(0, 1, 2, 3, 4, 5, 6, 7, 9),
                        () -> List.of(new Sum(8))))));
        grouped = profiled("q67.group.final", new SqlStageAggregationOperator(
                allocator,
                grouped,
                2,
                new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8},
                List.of(SqlStageAggregationOperator.aggregate(
                        List.of(0, 1, 2, 3, 4, 5, 6, 7, 8),
                        () -> List.of(new Sum(9))))));
        grouped = projectInputs(allocator, primitiveRegistry, grouped, 0, 1, 2, 3, 4, 5, 6, 7, 9);
        grouped = new TopNRankingOperator(allocator, 100, new int[] {0}, new int[] {8}, new boolean[] {true}, grouped, EngineResources.from(allocator).operatorResources().topNRankingPolicy());
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, new boolean[] {false, false, false, false, false, false, false, false, false, false}, grouped);
    }

    public static Operator query70(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator activeStates = query70ActiveStates(allocator, primitiveRegistry, tables);
        Operator sales = query70SalesByLocation(allocator, primitiveRegistry, tables);
        sales = new HashJoinOperator(allocator, sales, 0, activeStates, 0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 0, 1, 2);

        Operator grouped = new GroupIdOperator(
                allocator,
                sales,
                new int[][] {
                        {-1, -1, 2},
                        {0, -1, 2},
                        {0, 1, 2}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy());
        grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 3),
                List.of(new Sum(2)),
                grouped);
        grouped = projectQuery70Rollup(allocator, primitiveRegistry, grouped);
        grouped = new TopNRankingOperator(allocator, 100, new int[] {3, 4}, new int[] {2}, new boolean[] {true}, grouped, EngineResources.from(allocator).operatorResources().topNRankingPolicy());
        grouped = new TopNOperator(allocator, 100, new int[] {3, 4, 5}, new boolean[] {true, false, false}, grouped);
        return projectInputs(allocator, primitiveRegistry, grouped, 2, 0, 1, 3, 5);
    }

    public static Operator query80(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator grouped = profiled("q80.group_id", new GroupIdOperator(
                allocator,
                profiled("q80.union.channels", new UnionAllOperator(5, List.of(
                        query80ChannelBranch(
                                allocator,
                                primitiveRegistry,
                                tables,
                                "store_sales",
                                new String[] {"ss_sold_date_sk", "ss_item_sk", "ss_promo_sk", "ss_store_sk", "ss_ticket_number", "ss_ext_sales_price", "ss_net_profit"},
                                "store_returns",
                                new String[] {"sr_item_sk", "sr_ticket_number", "sr_return_amt", "sr_net_loss"},
                                "store",
                                new String[] {"s_store_sk", "s_store_id"},
                                "store channel",
                                "store"),
                        query80ChannelBranch(
                                allocator,
                                primitiveRegistry,
                                tables,
                                "catalog_sales",
                                new String[] {"cs_sold_date_sk", "cs_item_sk", "cs_promo_sk", "cs_catalog_page_sk", "cs_order_number", "cs_ext_sales_price", "cs_net_profit"},
                                "catalog_returns",
                                new String[] {"cr_item_sk", "cr_order_number", "cr_return_amount", "cr_net_loss"},
                                "catalog_page",
                                new String[] {"cp_catalog_page_sk", "cp_catalog_page_id"},
                                "catalog channel",
                                "catalog_page"),
                        query80ChannelBranch(
                                allocator,
                                primitiveRegistry,
                                tables,
                                "web_sales",
                                new String[] {"ws_sold_date_sk", "ws_item_sk", "ws_promo_sk", "ws_web_site_sk", "ws_order_number", "ws_ext_sales_price", "ws_net_profit"},
                                "web_returns",
                                new String[] {"wr_item_sk", "wr_order_number", "wr_return_amt", "wr_net_loss"},
                                "web_site",
                                new String[] {"web_site_sk", "web_site_id"},
                                "web channel",
                                "web_site")))),
                new int[][] {
                        {-1, -1, 2, 3, 4},
                        {0, -1, 2, 3, 4},
                        {0, 1, 2, 3, 4}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy()));
        grouped = profiled("q80.group.final", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 5),
                List.of(new Sum(2), new Sum(3), new Sum(4)),
                grouped));
        grouped = profiled("q80.project.final", projectInputs(allocator, primitiveRegistry, grouped, 0, 1, 3, 4, 5));
        return profiled("q80.topn", new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, grouped));
    }

    public static Operator query77(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator grouped = profiled("q77.group_id", new GroupIdOperator(
                allocator,
                new UnionAllOperator(5, List.of(
                        query77ChannelBranch(
                                allocator,
                                primitiveRegistry,
                                tables,
                                "q77.store",
                                "store_sales",
                                "ss_sold_date_sk",
                                "ss_store_sk",
                                "ss_ext_sales_price",
                                "ss_net_profit",
                                "store_returns",
                                "sr_returned_date_sk",
                                "sr_store_sk",
                                "sr_return_amt",
                                "sr_net_loss",
                                "store",
                                "s_store_sk",
                                "store channel"),
                        query77ChannelBranch(
                                allocator,
                                primitiveRegistry,
                                tables,
                                "q77.catalog",
                                "catalog_sales",
                                "cs_sold_date_sk",
                                "cs_call_center_sk",
                                "cs_ext_sales_price",
                                "cs_net_profit",
                                "catalog_returns",
                                "cr_returned_date_sk",
                                "cr_call_center_sk",
                                "cr_return_amount",
                                "cr_net_loss",
                                null,
                                null,
                                "catalog channel"),
                        query77ChannelBranch(
                                allocator,
                                primitiveRegistry,
                                tables,
                                "q77.web",
                                "web_sales",
                                "ws_sold_date_sk",
                                "ws_web_page_sk",
                                "ws_ext_sales_price",
                                "ws_net_profit",
                                "web_returns",
                                "wr_returned_date_sk",
                                "wr_web_page_sk",
                                "wr_return_amt",
                                "wr_net_loss",
                                "web_page",
                                "wp_web_page_sk",
                                "web channel"))),
                new int[][] {
                        {-1, -1, 2, 3, 4},
                        {0, -1, 2, 3, 4},
                        {0, 1, 2, 3, 4}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy()));
        grouped = profiled("q77.group.final", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 5),
                List.of(new Sum(2), new Sum(3), new Sum(4)),
                grouped));
        grouped = profiled("q77.project.final", projectInputs(allocator, primitiveRegistry, grouped, 0, 1, 3, 4, 5));
        return profiled("q77.topn", new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, grouped));
    }

    public static Operator query05(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator union = new UnionAllOperator(5, List.of(
                query05ChannelBranch(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "store_sales",
                        "ss_sold_date_sk",
                        "ss_store_sk",
                        "ss_ext_sales_price",
                        "ss_net_profit",
                        "store_returns",
                        "sr_returned_date_sk",
                        "sr_store_sk",
                        "sr_return_amt",
                        "sr_net_loss",
                        "store",
                        "s_store_sk",
                        "s_store_id",
                        "store channel",
                        "store"),
                query05ChannelBranch(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "catalog_sales",
                        "cs_sold_date_sk",
                        "cs_catalog_page_sk",
                        "cs_ext_sales_price",
                        "cs_net_profit",
                        "catalog_returns",
                        "cr_returned_date_sk",
                        "cr_catalog_page_sk",
                        "cr_return_amount",
                        "cr_net_loss",
                        "catalog_page",
                        "cp_catalog_page_sk",
                        "cp_catalog_page_id",
                        "catalog channel",
                        "catalog_page"),
                query05WebChannelBranch(allocator, primitiveRegistry, tables)));
        return new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, union);
    }

    public static Operator query94(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator filtered = query95FilteredSales(allocator, primitiveRegistry, tables);
        Operator eligibleOrders = query95MultiWarehouseOrders(allocator, primitiveRegistry, tables);
        Operator filteredEligible = new HashJoinOperator(allocator, filtered, 0, eligibleOrders, 0);
        Operator filteredEligibleProjected = projectInputs(allocator, primitiveRegistry, filteredEligible, 0, 1, 2);
        Operator returnedOrders = query95ReturnedEligibleOrders(allocator, primitiveRegistry, tables);
        Operator qualified = new HashJoinOperator(allocator, filteredEligibleProjected, 0, returnedOrders, 0, true);
        Operator antiJoined = filter(allocator, primitiveRegistry, qualified, isNull(3));
        Operator qualifiedProjected = projectInputs(allocator, primitiveRegistry, antiJoined, 0, 1, 2);
        return summarizeDistinctOrdersAndTotals(allocator, primitiveRegistry, qualifiedProjected);
    }

    public static Operator query95(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator filtered = query95FilteredSales(allocator, primitiveRegistry, tables);
        Operator eligibleOrders = query95MultiWarehouseOrders(allocator, primitiveRegistry, tables);
        Operator filteredEligible = new HashJoinOperator(allocator, filtered, 0, eligibleOrders, 0);
        Operator filteredEligibleProjected = projectInputs(allocator, primitiveRegistry, filteredEligible, 0, 1, 2);
        Operator returnedOrders = query95ReturnedEligibleOrders(allocator, primitiveRegistry, tables);
        Operator qualified = new HashJoinOperator(allocator, filteredEligibleProjected, 0, returnedOrders, 0);
        Operator qualifiedProjected = projectInputs(allocator, primitiveRegistry, qualified, 0, 1, 2);
        return summarizeDistinctOrdersAndTotals(allocator, primitiveRegistry, qualifiedProjected);
    }

    private static Operator summarizeDistinctOrdersAndTotals(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator qualifiedRows)
    {
        Operator grouped = new GroupIdOperator(
                allocator,
                qualifiedRows,
                new int[][] {
                        {-1, 1, 2},
                        {0, -1, -1}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy());
        grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0, 3),
                List.of(new Sum(1), new Sum(2)),
                grouped);
        grouped = projectDistinctOrdersAndTotals(allocator, primitiveRegistry, grouped);
        return new AggregationOperator(allocator, List.of(new Sum(0), new Sum(1), new Sum(2)), grouped);
    }

    private static Operator projectDistinctOrdersAndTotals(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable grandTotalGroup = new Variable(2);
        Variable orderGroup = new Variable(3);
        Variable isGrandTotal = new Variable(4);
        Variable isOrderGroup = new Variable(5);
        Variable distinctOrderCount = new Variable(6);
        Variable shipCostContribution = new Variable(7);
        Variable profitContribution = new Variable(8);

        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(zero, new Literal(0L), AllMask.ALL),
                                new Assignment(one, new Literal(1L), AllMask.ALL),
                                new Assignment(grandTotalGroup, new Literal(0L), AllMask.ALL),
                                new Assignment(orderGroup, new Literal(1L), AllMask.ALL),
                                new Assignment(isGrandTotal, new Call("eq", List.of(
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(grandTotalGroup, Stream.VALUES))), AllMask.ALL),
                                new Assignment(isOrderGroup, new Call("eq", List.of(
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(orderGroup, Stream.VALUES))), AllMask.ALL),
                                new Assignment(distinctOrderCount, new Call("if_i64", List.of(
                                        new Reference(isOrderGroup, Stream.VALUES),
                                        new Reference(one, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(shipCostContribution, new Call("if_i64", List.of(
                                        new Reference(isGrandTotal, Stream.VALUES),
                                        new Reference(new Input(2), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(profitContribution, new Call("if_i64", List.of(
                                        new Reference(isGrandTotal, Stream.VALUES),
                                        new Reference(new Input(3), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(distinctOrderCount, Stream.VALUES),
                                new Reference(shipCostContribution, Stream.VALUES),
                                new Reference(profitContribution, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    public static Operator query30(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerTotalReturn = query30CustomerTotalReturn(allocator, primitiveRegistry, tables);
        Operator stateAverages = query30StateAverageReturns(allocator, primitiveRegistry, tables);

        Operator joined = new HashJoinOperator(allocator, customerTotalReturn, 1, stateAverages, 0);
        joined = new HashJoinOperator(
                allocator,
                joined,
                0,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk", "c_customer_id", "c_salutation", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_day", "c_birth_month", "c_birth_year", "c_birth_country", "c_login", "c_email_address", "c_last_review_date_sk"),
                0);
        joined = new HashJoinOperator(
                allocator,
                joined,
                6,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_address",
                        equalUtf8(1, "GA"),
                        new String[] {"ca_address_sk", "ca_state"}),
                0);
        joined = filter(allocator, primitiveRegistry, joined, query81ReturnThresholdPredicate(2, 4));
        joined = projectInputs(allocator, primitiveRegistry, joined, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 2);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, new boolean[] {false, false, false, false, false, false, false, false, false, false, false, false, false}, joined);
    }

    public static Operator query76(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator combined = new UnionAllOperator(7, List.of(
                query76ChannelCategorySales(allocator, primitiveRegistry, tables, "store", "ss_store_sk", "store_sales", "ss_store_sk", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"),
                query76ChannelCategorySales(allocator, primitiveRegistry, tables, "web", "ws_ship_customer_sk", "web_sales", "ws_ship_customer_sk", "ws_sold_date_sk", "ws_item_sk", "ws_ext_sales_price"),
                query76ChannelCategorySales(allocator, primitiveRegistry, tables, "catalog", "cs_ship_addr_sk", "catalog_sales", "cs_ship_addr_sk", "cs_sold_date_sk", "cs_item_sk", "cs_ext_sales_price")));
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3, 4}, new boolean[] {false, false, false, false, false}, combined);
    }

    public static Operator query29(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator joined = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number", "ss_quantity");
        joined = new HashJoinOperator(
                allocator,
                joined,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 9), equal(2, 1999)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0),
                0).withOutputs(1, 2, 3, 4, 5);
        joined = new HashJoinOperator(
                allocator,
                joined,
                0,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_item_id", "i_item_desc"),
                0).withOutputs(6, 7, 0, 1, 2, 3, 4);
        joined = new HashJoinOperator(
                allocator,
                joined,
                3,
                scannedTable(allocator, tables, "store", "s_store_sk", "s_store_id", "s_store_name"),
                0).withOutputs(0, 1, 8, 9, 2, 4, 5, 6);
        joined = new HashJoinOperator(
                allocator,
                joined,
                new int[] {5, 4, 6},
                scannedTable(allocator, tables, "store_returns", "sr_customer_sk", "sr_item_sk", "sr_ticket_number", "sr_returned_date_sk", "sr_return_quantity"),
                new int[] {0, 1, 2}).withOutputs(0, 1, 2, 3, 7, 8, 9, 11, 12);
        joined = new HashJoinOperator(
                allocator,
                joined,
                7,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(betweenInclusive(1, 9, 12), equal(2, 1999)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0),
                0).withOutputs(0, 1, 2, 3, 4, 5, 6, 8);
        joined = new HashJoinOperator(
                allocator,
                joined,
                new int[] {5, 6},
                factScan(allocator, tables, "catalog_sales", "cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk", "cs_quantity"),
                new int[] {0, 1}).withOutputs(0, 1, 2, 3, 4, 7, 10, 11);
        joined = new HashJoinOperator(
                allocator,
                joined,
                6,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        or(equal(1, 1999), equal(1, 2000), equal(1, 2001)),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0).withOutputs(0, 1, 2, 3, 4, 5, 7);
        joined = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3),
                List.of(new Sum(4), new Sum(5), new Sum(6)),
                joined);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3}, new boolean[] {false, false, false, false}, joined);
    }

    public static Operator query32(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator discounted = factScan(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_ext_discount_amt");
        discounted = new HashJoinOperator(
                allocator,
                discounted,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        equal(1, 977),
                        new String[] {"i_item_sk", "i_manufact_id"},
                        0),
                0);
        discounted = new HashJoinOperator(
                allocator,
                discounted,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query92DatePredicate(),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);
        discounted = projectInputs(allocator, primitiveRegistry, discounted, 1, 2);

        Operator itemAverages = query32ItemAverageDiscounts(allocator, primitiveRegistry, tables);
        Operator joined = new HashJoinOperator(allocator, discounted, 0, itemAverages, 0);
        joined = filter(allocator, primitiveRegistry, joined, query92DiscountThresholdPredicate(1, 3));
        joined = projectInputs(allocator, primitiveRegistry, joined, 1);
        return new AggregationOperator(
                allocator,
                List.of(new Sum(0)),
                joined);
    }

    public static Operator query23(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return query23(TpcdsQueryContext.unprofiled(allocator, primitiveRegistry, tables));
    }

    static Operator query23(TpcdsQueryContext context)
    {
        Operator catalog = context.profiled("q23.catalog.channel", query23Channel(
                context,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_bill_customer_sk",
                "cs_item_sk",
                "cs_quantity",
                "cs_list_price"));
        Operator web = context.profiled("q23.web.channel", query23Channel(
                context,
                "web_sales",
                "ws_sold_date_sk",
                "ws_bill_customer_sk",
                "ws_item_sk",
                "ws_quantity",
                "ws_list_price"));
        return context.profiled("q23.final.sum", new AggregationOperator(
                context.allocator(),
                List.of(new Sum(0)),
                context.profiled("q23.union", new UnionAllOperator(1, List.of(catalog, web)))));
    }

    public static Operator query24(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return query24(TpcdsQueryContext.unprofiled(allocator, primitiveRegistry, tables));
    }

    static Operator query24(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        Operator groupedSales = context.profiled("q24.pale.sales", query24Sales(context, "pale"));
        groupedSales = context.profiled("q24.pale.outer_group", new GroupedAggregationOperator(
                context.allocator(),
                List.of(0, 1, 2),
                List.of(new Sum(10)),
                groupedSales));

        Operator averageSales = context.profiled("q24.average.sales", query24AverageSales(context));
        averageSales = context.profiled("q24.average.final", new AggregationOperator(
                allocator,
                List.of(new Sum(5), new CountColumn(5)),
                averageSales));

        Operator joined = context.profiled("q24.join.threshold", new NestedLoopJoinOperator(allocator, groupedSales, averageSales));
        joined = context.profiled("q24.filter.threshold", filter(allocator, primitiveRegistry, joined, query24ThresholdPredicate(3, 4, 5)));
        joined = context.profiled("q24.project.output", projectInputs(allocator, primitiveRegistry, joined, 0, 1, 2, 3));
        return context.profiled("q24.topn", new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, joined));
    }

    public static Operator query25(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator joined = profiled("q25.scan.store_sales", factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_ticket_number", "ss_net_profit"));
        joined = profiled("q25.join.sold_date", new HashJoinOperator(
                allocator,
                joined,
                0,
                profiled("q25.scan.sold_date", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 4), equal(2, 2001)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0)),
                0).withOutputs(1, 2, 3, 4, 5));
        joined = profiled("q25.join.store_returns", new HashJoinOperator(
                allocator,
                joined,
                new int[] {1, 0, 3},
                profiled("q25.scan.store_returns", scannedTable(allocator, tables, "store_returns", "sr_item_sk", "sr_customer_sk", "sr_ticket_number", "sr_returned_date_sk", "sr_net_loss")),
                new int[] {1, 0, 2}).withOutputs(0, 1, 2, 4, 8, 9));
        joined = profiled("q25.join.returned_date", new HashJoinOperator(
                allocator,
                joined,
                4,
                profiled("q25.scan.returned_date", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(betweenInclusive(1, 4, 10), equal(2, 2001)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0)),
                0).withOutputs(0, 1, 2, 3, 5));
        joined = profiled("q25.join.catalog_sales", new HashJoinOperator(
                allocator,
                joined,
                new int[] {1, 0},
                profiled("q25.scan.catalog_sales", factScan(allocator, tables, "catalog_sales", "cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk", "cs_net_profit")),
                new int[] {0, 1}).withOutputs(0, 2, 3, 4, 7, 8));
        joined = profiled("q25.join.catalog_date", new HashJoinOperator(
                allocator,
                joined,
                4,
                profiled("q25.scan.catalog_date", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(betweenInclusive(1, 4, 10), equal(2, 2001)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0)),
                0).withOutputs(0, 1, 2, 3, 5));
        joined = profiled("q25.join.store", new HashJoinOperator(
                allocator,
                joined,
                1,
                profiled("q25.scan.store", scannedTable(allocator, tables, "store", "s_store_sk", "s_store_id", "s_store_name")),
                0).withOutputs(0, 2, 3, 4, 6, 7));
        joined = profiled("q25.join.item", new HashJoinOperator(
                allocator,
                joined,
                0,
                profiled("q25.scan.item", scannedTable(allocator, tables, "item", "i_item_sk", "i_item_id", "i_item_desc")),
                0).withOutputs(7, 8, 4, 5, 1, 2, 3));
        joined = profiled("q25.group", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3),
                List.of(new Sum(4), new Sum(5), new Sum(6)),
                joined));
        return profiled("q25.topn", new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3}, new boolean[] {false, false, false, false}, joined));
    }

    public static Operator query38(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4), new Sum(5)),
                new UnionAllOperator(6, List.of(
                        query38Channel(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_customer_sk", 1, 0, 0),
                        query38Channel(allocator, primitiveRegistry, tables, "catalog_sales", "cs_sold_date_sk", "cs_bill_customer_sk", 0, 1, 0),
                        query38Channel(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_bill_customer_sk", 0, 0, 1))));
        grouped = filter(allocator, primitiveRegistry, grouped, and(greaterThan(3, 0), greaterThan(4, 0), greaterThan(5, 0)));
        return new AggregationOperator(allocator, List.of(new CountAll()), grouped);
    }

    public static Operator query48(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(
                allocator,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_cdemo_sk",
                "ss_addr_sk",
                "ss_store_sk",
                "ss_quantity",
                "ss_sales_price",
                "ss_net_profit");
        sales = new HashJoinOperator(
                allocator,
                sales,
                3,
                scannedTable(allocator, tables, "store", "s_store_sk"),
                0).withOutputs(0, 1, 2, 4, 5, 6);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "customer_demographics", "cd_demo_sk", "cd_marital_status", "cd_education_status"),
                0).withOutputs(0, 2, 3, 4, 5, 7, 8);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_address",
                        equalUtf8(1, "United States"),
                        new String[] {"ca_address_sk", "ca_country", "ca_state"},
                        0, 2),
                0).withOutputs(0, 2, 3, 4, 5, 6, 8);
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 2000),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0).withOutputs(1, 2, 3, 4, 5, 6);
        sales = filter(allocator, primitiveRegistry, sales, query48DemographicsPredicate(3, 4, 1));
        sales = filter(allocator, primitiveRegistry, sales, query48StateProfitPredicate(5, 2));
        sales = projectInputs(allocator, primitiveRegistry, sales, 0);
        return new AggregationOperator(allocator, List.of(new Sum(0)), sales);
    }

    private static Operator query38Channel(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String factTable, String soldDateColumn, String customerColumn, long storeMarker, long catalogMarker, long webMarker)
    {
        Operator facts = factScan(allocator, tables, factTable, soldDateColumn, customerColumn);
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(2, 1199), lessThan(2, 1212)),
                        new String[] {"d_date_sk", "d_date", "d_month_seq"},
                        0, 1),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_first_name", "c_last_name"),
                0);
        Variable store = new Variable(0);
        Variable catalog = new Variable(1);
        Variable web = new Variable(2);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(store, new Literal(storeMarker), AllMask.ALL),
                                new Assignment(catalog, new Literal(catalogMarker), AllMask.ALL),
                                new Assignment(web, new Literal(webMarker), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(6), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(store, Stream.VALUES),
                                new Reference(catalog, Stream.VALUES),
                                new Reference(web, Stream.VALUES))),
                primitiveRegistry,
                facts);
    }

    public static Operator query81(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerTotalReturn = profiled("q81.customer_totals.primary", query81CustomerTotalReturn(allocator, primitiveRegistry, tables));
        Operator stateAverages = profiled("q81.state_averages", query81StateAverageReturns(allocator, primitiveRegistry, tables));

        Operator joined = profiled("q81.join.state_average", new HashJoinOperator(allocator, customerTotalReturn, 1, stateAverages, 0));
        joined = profiled("q81.join.customer", new HashJoinOperator(
                allocator,
                joined,
                0,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk", "c_customer_id", "c_salutation", "c_first_name", "c_last_name"),
                0));
        joined = profiled("q81.join.address", new HashJoinOperator(
                allocator,
                joined,
                6,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_address",
                        equalUtf8(1, "GA"),
                        new String[] {"ca_address_sk", "ca_state", "ca_street_number", "ca_street_name", "ca_street_type", "ca_suite_number", "ca_city", "ca_county", "ca_zip", "ca_country", "ca_gmt_offset", "ca_location_type"}),
                0));
        joined = profiled("q81.filter.threshold", filter(allocator, primitiveRegistry, joined, query81ReturnThresholdPredicate(2, 4)));
        joined = profiled("q81.project.output", projectInputs(allocator, primitiveRegistry, joined, 7, 8, 9, 10, 13, 14, 15, 16, 17, 18, 12, 19, 20, 21, 22, 2));
        return profiled("q81.topn", new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, new boolean[] {false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false}, joined));
    }

    public static Operator query83(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator store = query83ChannelReturns(allocator, primitiveRegistry, tables, "store_returns", "sr_item_sk", "sr_returned_date_sk", "sr_return_quantity");
        Operator catalog = query83ChannelReturns(allocator, primitiveRegistry, tables, "catalog_returns", "cr_item_sk", "cr_returned_date_sk", "cr_return_quantity");
        Operator web = query83ChannelReturns(allocator, primitiveRegistry, tables, "web_returns", "wr_item_sk", "wr_returned_date_sk", "wr_return_quantity");

        Operator joined = new HashJoinOperator(allocator, store, 0, catalog, 0);
        joined = new HashJoinOperator(allocator, joined, 0, web, 0);
        joined = projectInputs(allocator, primitiveRegistry, joined, 0, 1, 3, 5);
        joined = new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, joined);
        // Trino's BIGINT / DECIMAL '3.00' average has six fractional digits, whereas q58's decimal-revenue
        // average has four.  Keep the same generic three-channel projection and supply the SQL result scale.
        return projectThreeChannelOutput(allocator, primitiveRegistry, joined, 1_000_000L);
    }

    public static Operator query97(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator indicators = profiled("q97.union.channels", new UnionAllOperator(5, List.of(
                query97PresenceChannel(allocator, primitiveRegistry, tables, "q97.store", "store_sales", "ss_customer_sk", "ss_item_sk", "ss_sold_date_sk", true),
                query97PresenceChannel(allocator, primitiveRegistry, tables, "q97.catalog", "catalog_sales", "cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk", false))));
        indicators = profiled("q97.group.presence", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4)),
                indicators));
        indicators = profiled("q97.project.indicators", projectQuery97PresenceIndicators(allocator, primitiveRegistry, indicators));
        return profiled("q97.group.final", new AggregationOperator(
                allocator,
                List.of(new Sum(0), new Sum(1), new Sum(2)),
                indicators));
    }

    public static Operator query73(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator allowedDates = filteredProjectedTable(allocator, primitiveRegistry, tables, "date_dim", dayOfMonthAndYearsPredicate(1, 2, 1, 2, 1999, 2000, 2001), new String[] {"d_date_sk", "d_dom", "d_year"}, 0);
        Operator allowedStores = filteredProjectedTable(allocator, primitiveRegistry, tables, "store", utf8AnyOf(1, Set.of("Williamson County", "Franklin Parish", "Bronx County", "Orange County")), new String[] {"s_store_sk", "s_county"}, 0);
        Operator allowedHouseholds = filteredProjectedTable(allocator, primitiveRegistry, tables, "household_demographics", query73HouseholdPredicate(), new String[] {"hd_demo_sk", "hd_buy_potential", "hd_vehicle_count", "hd_dep_count"}, 0);
        Operator projected = factScan(allocator, tables, "store_sales", "ss_ticket_number", "ss_customer_sk", "ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk");
        projected = new HashJoinOperator(allocator, projected, 2, allowedDates, 0).withOutputs(0, 1, 3, 4);
        projected = new HashJoinOperator(allocator, projected, 2, allowedStores, 0).withOutputs(0, 1, 3);
        projected = new HashJoinOperator(allocator, projected, 2, allowedHouseholds, 0).withOutputs(0, 1);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new CountAll()),
                projected);
        Operator filtered = filter(allocator, primitiveRegistry, aggregated, and(greaterThan(2, 0), lessThan(2, 6)));
        Operator joined = new HashJoinOperator(
                allocator,
                filtered,
                1,
                scannedTable(allocator, tables, "customer",
                        "c_customer_sk",
                        "c_last_name",
                        "c_first_name",
                        "c_salutation",
                        "c_preferred_cust_flag"),
                0).withOutputs(4, 5, 6, 7, 0, 2);
        Operator enriched = joined;
        return new TopNOperator(allocator, 100, new int[] {5, 0, 4}, new boolean[] {true, false, false}, enriched);
    }

    public static Operator query84(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator eligibleIncomeBands = profiled("q84.scan.income_band", filteredProjectedTable(allocator, primitiveRegistry, tables, "income_band", and(greaterThan(1, 38127), lessThan(2, 88129)), new String[] {"ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"}));
        Operator eligibleHouseholds = profiled("q84.join.income_band", new HashJoinOperator(
                allocator,
                profiled("q84.scan.household_demographics", factScan(allocator, tables, "household_demographics", "hd_demo_sk", "hd_income_band_sk")),
                1,
                eligibleIncomeBands,
                0).withOutputs(0));
        Operator eligibleAddresses = profiled("q84.scan.customer_address", filteredProjectedTable(allocator, primitiveRegistry, tables, "customer_address", equalUtf8(1, "Edgewood"), new String[] {"ca_address_sk", "ca_city"}));
        Operator customers = profiled("q84.scan.customer", customerScan(allocator, tables,
                "c_customer_id",
                "c_last_name",
                "c_first_name",
                "c_current_addr_sk",
                "c_current_cdemo_sk",
                "c_current_hdemo_sk"));
        customers = profiled("q84.join.customer_address", new HashJoinOperator(allocator, customers, 3, eligibleAddresses, 0)
                .withOutputs(0, 1, 2, 4, 5));
        customers = profiled("q84.join.household_demographics", new HashJoinOperator(allocator, customers, 4, eligibleHouseholds, 0)
                .withOutputs(0, 1, 2, 3));
        customers = profiled("q84.join.store_returns", new HashJoinOperator(allocator, customers, 3, profiled("q84.scan.store_returns", scannedTable(allocator, tables, "store_returns", "sr_cdemo_sk")), 0)
                .withOutputs(0, 1, 2));
        Operator projected = projectCustomerName(allocator, primitiveRegistry, customers, 0, 1, 2);
        return new TopNOperator(allocator, 100, 0, false, projected);
    }

    public static Operator query74(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator joined = query74ChannelYearTotal(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_net_paid", 2001, false);
        joined = profiled("q74.join.store.2002", new HashJoinOperator(allocator, joined, 0, query74ChannelYearTotal(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_net_paid", 2002, true), 0)
                .withOutputs(0, 1, 3, 4, 5));
        joined = profiled("q74.join.web.2001", new HashJoinOperator(allocator, joined, 0, query74ChannelYearTotal(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_net_paid", 2001, false), 0)
                .withOutputs(0, 1, 2, 3, 4, 6));
        joined = profiled("q74.join.web.2002", new HashJoinOperator(allocator, joined, 0, query74ChannelYearTotal(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", "ws_net_paid", 2002, false), 0)
                .withOutputs(0, 1, 2, 3, 4, 5, 7));
        joined = profiled("q74.filter.growth", filter(allocator, primitiveRegistry, joined, query11GrowthPredicate(1, 4, 5, 6)));
        joined = profiled("q74.project.output", projectInputs(allocator, primitiveRegistry, joined, 0, 2, 3));
        return profiled("q74.topn", new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, joined));
    }

    public static Operator query75(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator currentYear = profiled("q75.filter.current", filter(allocator, primitiveRegistry, query75AllSales(allocator, primitiveRegistry, tables, "q75.current"), equal(0, 2002)));
        Operator previousYear = profiled("q75.filter.previous", filter(allocator, primitiveRegistry, query75AllSales(allocator, primitiveRegistry, tables, "q75.previous"), equal(0, 2001)));
        Operator joined = profiled("q75.join.previous", new HashJoinOperator(allocator, currentYear, new int[] {1, 2, 3, 4}, previousYear, new int[] {1, 2, 3, 4}));
        joined = profiled("q75.filter.ratio", filter(allocator, primitiveRegistry, joined, query75CountRatioPredicate(5, 12)));
        joined = profiled("q75.project.output", projectQuery75Output(allocator, primitiveRegistry, joined));
        return profiled("q75.topn", new TopNOperator(allocator, 100, new int[] {8, 9}, new boolean[] {false, false}, joined));
    }

    public static Operator query78(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator store = query78Channel(
                allocator,
                primitiveRegistry,
                tables,
                "store_sales",
                "ss_sold_date_sk",
                "ss_item_sk",
                "ss_customer_sk",
                "ss_ticket_number",
                "ss_quantity",
                "ss_wholesale_cost",
                "ss_sales_price",
                "store_returns",
                "sr_item_sk",
                "sr_ticket_number",
                "q78.store");
        Operator web = query78Channel(
                allocator,
                primitiveRegistry,
                tables,
                "web_sales",
                "ws_sold_date_sk",
                "ws_item_sk",
                "ws_bill_customer_sk",
                "ws_order_number",
                "ws_quantity",
                "ws_wholesale_cost",
                "ws_sales_price",
                "web_returns",
                "wr_item_sk",
                "wr_order_number",
                "q78.web");
        Operator catalog = query78Channel(
                allocator,
                primitiveRegistry,
                tables,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_item_sk",
                "cs_bill_customer_sk",
                "cs_order_number",
                "cs_quantity",
                "cs_wholesale_cost",
                "cs_sales_price",
                "catalog_returns",
                "cr_item_sk",
                "cr_order_number",
                "q78.catalog");
        HashJoinOperator storeWebJoin = new HashJoinOperator(allocator, store, new int[] {0, 1, 2}, web, new int[] {0, 1, 2}, true);
        if (QUERY78_COMPACT_JOIN_LAYOUTS) {
            storeWebJoin = storeWebJoin.withOutputs(0, 1, 2, 3, 4, 5, 9, 10, 11);
        }
        Operator joined = profiled("q78.join.store_web", storeWebJoin);
        HashJoinOperator catalogJoin = new HashJoinOperator(allocator, joined, new int[] {0, 1, 2}, catalog, new int[] {0, 1, 2}, true);
        if (QUERY78_COMPACT_JOIN_LAYOUTS) {
            catalogJoin = catalogJoin.withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 13, 14);
            joined = profiled("q78.join.catalog", catalogJoin);
            joined = profiled("q78.filter.positive_quantities", filter(allocator, primitiveRegistry, joined, or(greaterThan(6, 0), greaterThan(9, 0))));
            joined = profiled("q78.project.output", projectQuery78CompactOutput(allocator, primitiveRegistry, joined));
        }
        else {
            joined = profiled("q78.join.catalog", catalogJoin);
            joined = profiled("q78.filter.positive_quantities", filter(allocator, primitiveRegistry, joined, or(greaterThan(9, 0), greaterThan(15, 0))));
            joined = profiled("q78.project.output", projectQuery78Output(allocator, primitiveRegistry, joined));
        }
        joined = profiled("q78.top100", new TopNOperator(allocator, 100, new int[] {8, 9, 0, 2, 3, 4, 5, 6, 7, 1}, new boolean[] {false, false, false, true, true, true, false, false, false, false}, joined));
        return profiled("q78.project.final", projectInputs(allocator, primitiveRegistry, joined, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7));
    }

    public static Operator query90(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator morningCount = query90Count(
                allocator,
                primitiveRegistry,
                tables,
                filteredProjectedTable(allocator, primitiveRegistry, tables, "time_dim", and(greaterThan(1, 7), lessThan(1, 10)), new String[] {"t_time_sk", "t_hour"}),
                filteredProjectedTable(allocator, primitiveRegistry, tables, "household_demographics", equal(1, 6), new String[] {"hd_demo_sk", "hd_dep_count"}),
                filteredProjectedTable(allocator, primitiveRegistry, tables, "web_page", and(greaterThan(1, 4999), lessThan(1, 5201)), new String[] {"wp_web_page_sk", "wp_char_count"}));
        Operator eveningCount = query90Count(
                allocator,
                primitiveRegistry,
                tables,
                filteredProjectedTable(allocator, primitiveRegistry, tables, "time_dim", and(greaterThan(1, 18), lessThan(1, 21)), new String[] {"t_time_sk", "t_hour"}),
                filteredProjectedTable(allocator, primitiveRegistry, tables, "household_demographics", equal(1, 6), new String[] {"hd_demo_sk", "hd_dep_count"}),
                filteredProjectedTable(allocator, primitiveRegistry, tables, "web_page", and(greaterThan(1, 4999), lessThan(1, 5201)), new String[] {"wp_web_page_sk", "wp_char_count"}));
        Operator counts = new NestedLoopJoinOperator(allocator, morningCount, eveningCount);
        return projectQuery90Ratio(allocator, primitiveRegistry, counts, 0, 1);
    }

    public static Operator query87(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator union = new UnionAllOperator(6, List.of(
                query87ChannelPresence(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", 0),
                query87ChannelPresence(allocator, primitiveRegistry, tables, "catalog_sales", "cs_bill_customer_sk", "cs_sold_date_sk", 1),
                query87ChannelPresence(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", 2)));
        union = new GroupedAggregationOperator(allocator, List.of(0, 1, 2), List.of(new Sum(3), new Sum(4), new Sum(5)), union);
        union = filter(allocator, primitiveRegistry, union, and(greaterThan(3, 0), equal(4, 0), equal(5, 0)));
        return new AggregationOperator(allocator, List.of(new CountAll()), union);
    }

    public static Operator query85(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(allocator, tables, "web_sales", "ws_web_page_sk", "ws_item_sk", "ws_order_number", "ws_sold_date_sk", "ws_quantity", "ws_sales_price", "ws_net_profit");
        sales = profiled("q85.join.web_page", new HashJoinOperator(allocator, sales, 0, scannedTable(allocator, tables, "web_page", "wp_web_page_sk"), 0)
                .withOutputs(1, 2, 3, 4, 5, 6)
                .withComposeEncodedOuterDictionaryDepth(0)
                .withLazyDuplicateSlotState()
                .withDirectExactBuildCoalescing());
        sales = profiled("q85.join.web_returns", new HashJoinOperator(allocator, sales, new int[] {0, 1}, scannedTable(allocator, tables, "web_returns", "wr_item_sk", "wr_order_number", "wr_refunded_cdemo_sk", "wr_returning_cdemo_sk", "wr_refunded_addr_sk", "wr_reason_sk", "wr_refunded_cash", "wr_fee"), new int[] {0, 1})
                .withOutputs(2, 3, 4, 5, 8, 9, 10, 11, 12, 13)
                .withComposeEncodedOuterDictionaryDepth(0)
                .withLazyDuplicateSlotState()
                .withDirectExactBuildCoalescing());
        sales = profiled("q85.join.date_dim", new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 2000),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0).withOutputs(1, 2, 3, 4, 5, 6, 7, 8, 9)
                .withComposeEncodedOuterDictionaryDepth(0)
                .withLazyDuplicateSlotState()
                .withDirectExactBuildCoalescing());
        sales = profiled("q85.join.refunded_demo", new HashJoinOperator(allocator, sales, 3, scannedTable(allocator, tables, "customer_demographics", "cd_demo_sk", "cd_marital_status", "cd_education_status"), 0)
                .withOutputs(0, 1, 2, 4, 5, 6, 7, 8, 10, 11)
                .withComposeEncodedOuterDictionaryDepth(0)
                .withLazyDuplicateSlotState()
                .withDirectExactBuildCoalescing());
        sales = profiled("q85.join.returning_demo", new HashJoinOperator(allocator, sales, 3, scannedTable(allocator, tables, "customer_demographics", "cd_demo_sk", "cd_marital_status", "cd_education_status"), 0)
                .withOutputs(0, 1, 2, 4, 5, 6, 7, 8, 9, 11, 12)
                .withComposeEncodedOuterDictionaryDepth(0)
                .withLazyDuplicateSlotState()
                .withDirectExactBuildCoalescing());
        sales = profiled("q85.join.customer_address", new HashJoinOperator(allocator, sales, 3, scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_country", "ca_state"), 0)
                .withOutputs(0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 12, 13)
                .withComposeEncodedOuterDictionaryDepth(0)
                .withLazyDuplicateSlotState()
                .withDirectExactBuildCoalescing());
        sales = profiled("q85.join.reason", new HashJoinOperator(allocator, sales, 3, scannedTable(allocator, tables, "reason", "r_reason_sk", "r_reason_desc"), 0)
                .withOutputs(0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 13)
                .withComposeEncodedOuterDictionaryDepth(0)
                .withLazyDuplicateSlotState()
                .withDirectExactBuildCoalescing());
        sales = profiled("q85.filter", filter(allocator, primitiveRegistry, sales, and(
                query85DemographicsAndPricePredicate(5, 6, 7, 8, 1),
                query85AddressProfitPredicate(9, 10, 2))));
        sales = projectInputs(allocator, primitiveRegistry, sales, 11, 0, 3, 4);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Avg(1), new Avg(2), new Avg(3)),
                sales);
        sales = projectQuery85ReasonOutput(allocator, primitiveRegistry, sales, 0, 1, 2, 3);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3}, new boolean[] {false, false, false, false}, sales);
    }

    public static Operator query92(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator allowedDates = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                query92DatePredicate(),
                new String[] {"d_date_sk", "d_date"},
                0);
        Operator manufacturedItems = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "item",
                equal(1, 350),
                new String[] {"i_item_sk", "i_manufact_id"},
                0);

        Operator discounted = factScan(allocator, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_ext_discount_amt");
        discounted = new HashJoinOperator(allocator, discounted, 1, manufacturedItems, 0);
        discounted = new HashJoinOperator(allocator, discounted, 0, allowedDates, 0);
        discounted = projectInputs(allocator, primitiveRegistry, discounted, 1, 2);

        Operator itemAverages = query92ItemAverageDiscounts(allocator, primitiveRegistry, tables);
        Operator joined = new HashJoinOperator(allocator, discounted, 0, itemAverages, 0);
        joined = filter(allocator, primitiveRegistry, joined, query92DiscountThresholdPredicate(1, 3));
        joined = projectInputs(allocator, primitiveRegistry, joined, 1);
        return new AggregationOperator(
                allocator,
                List.of(new Sum(0)),
                joined);
    }

    public static Operator query93(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return profiled("q93.topn", new TopNOperator(allocator, 100, new int[] {1, 0}, new boolean[] {false, false}, query93GroupedSales(allocator, primitiveRegistry, tables)));
    }

    static Operator query93ProjectedSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator returns = profiled("q93.scan.store_returns", scannedTable(allocator, tables, "store_returns", "sr_item_sk", "sr_reason_sk", "sr_ticket_number", "sr_return_quantity"));
        returns = profiled("q93.join.reason", new HashJoinOperator(
                allocator,
                returns,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "reason",
                        equalUtf8(1, "reason 28"),
                        new String[] {"r_reason_sk", "r_reason_desc"},
                        0),
                0));
        returns = profiled("q93.project.returns", projectInputs(allocator, primitiveRegistry, returns, 0, 2, 3));

        Operator sales = profiled("q93.scan.store_sales", factScan(allocator, tables, "store_sales", "ss_item_sk", "ss_customer_sk", "ss_ticket_number", "ss_quantity", "ss_sales_price"));
        sales = profiled("q93.join.returns", new HashJoinOperator(allocator, sales, new int[] {0, 2}, returns, new int[] {0, 1}));
        sales = profiled("q93.project.sales_value", projectQuery93SalesValue(allocator, primitiveRegistry, sales));
        return sales;
    }

    static Operator query93JoinedSalesReturns(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator returns = scannedTable(allocator, tables, "store_returns", "sr_item_sk", "sr_reason_sk", "sr_ticket_number", "sr_return_quantity");
        returns = new HashJoinOperator(
                allocator,
                returns,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "reason",
                        equalUtf8(1, "reason 28"),
                        new String[] {"r_reason_sk", "r_reason_desc"},
                        0),
                0);
        returns = projectInputs(allocator, primitiveRegistry, returns, 0, 2, 3);

        Operator sales = factScan(allocator, tables, "store_sales", "ss_item_sk", "ss_customer_sk", "ss_ticket_number", "ss_quantity", "ss_sales_price");
        sales = new HashJoinOperator(allocator, sales, new int[] {0, 2}, returns, new int[] {0, 1});
        return sales;
    }

    private static Operator query93GroupedSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return profiled("q93.group.customer", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                query93ProjectedSales(allocator, primitiveRegistry, tables)));
    }

    public static Operator query88(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator counts = query88BucketCount(allocator, primitiveRegistry, tables, 0);
        counts = new NestedLoopJoinOperator(allocator, counts, query88BucketCount(allocator, primitiveRegistry, tables, 1));
        counts = new NestedLoopJoinOperator(allocator, counts, query88BucketCount(allocator, primitiveRegistry, tables, 2));
        counts = new NestedLoopJoinOperator(allocator, counts, query88BucketCount(allocator, primitiveRegistry, tables, 3));
        counts = new NestedLoopJoinOperator(allocator, counts, query88BucketCount(allocator, primitiveRegistry, tables, 4));
        counts = new NestedLoopJoinOperator(allocator, counts, query88BucketCount(allocator, primitiveRegistry, tables, 5));
        counts = new NestedLoopJoinOperator(allocator, counts, query88BucketCount(allocator, primitiveRegistry, tables, 6));
        counts = new NestedLoopJoinOperator(allocator, counts, query88BucketCount(allocator, primitiveRegistry, tables, 7));
        return counts;
    }

    private static FilterSpec utf8AnyOf(int inputIndex, Set<String> values)
    {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("values is empty");
        }

        List<String> sortedValues = values.stream()
                .sorted()
                .toList();
        List<Assignment> assignments = new java.util.ArrayList<>();
        assignments.add(new Assignment(new Variable(0), new Literal(sortedValues.getFirst()), AllMask.ALL));
        for (int index = 1; index < sortedValues.size(); index++) {
            assignments.add(new Assignment(new Variable(index), new Literal(sortedValues.get(index)), AllMask.ALL));
        }

        Variable result = new Variable(sortedValues.size());
        List<Reference> arguments = new java.util.ArrayList<>(sortedValues.size() + 1);
        arguments.add(new Reference(new Input(inputIndex), Stream.VALUES));
        for (int index = 0; index < sortedValues.size(); index++) {
            arguments.add(new Reference(new Variable(index), Stream.VALUES));
        }
        assignments.add(new Assignment(result, new Call("in_utf8", arguments), AllMask.ALL));
        return new FilterSpec(new EvaluationPlan(assignments, List.of()), new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static FilterSpec query45ItemPredicate()
    {
        return or(
                equal(0, 2),
                equal(0, 3),
                equal(0, 5),
                equal(0, 7),
                equal(0, 11),
                equal(0, 13),
                equal(0, 17),
                equal(0, 19),
                equal(0, 23),
                equal(0, 29));
    }

    private static FilterSpec query53ItemPredicate()
    {
        FilterSpec firstBranch = and(
                utf8AnyOf(2, Set.of("Books", "Children", "Electronics")),
                utf8AnyOf(3, Set.of("personal", "portable", "reference", "self-help")),
                utf8AnyOf(4, Set.of("scholaramalgamalg #14", "scholaramalgamalg #7", "exportiunivamalg #9", "scholaramalgamalg #9")));
        FilterSpec secondBranch = and(
                utf8AnyOf(2, Set.of("Women", "Music", "Men")),
                utf8AnyOf(3, Set.of("accessories", "classical", "fragrances", "pants")),
                utf8AnyOf(4, Set.of("amalgimporto #1", "edu packscholar #1", "exportiimporto #1", "importoamalg #1")));
        return or(firstBranch, secondBranch);
    }

    private static FilterSpec query12CategoryPredicate()
    {
        return utf8AnyOf(3, Set.of("Sports", "Books", "Home"));
    }

    private static FilterSpec query89ItemPredicate()
    {
        FilterSpec firstBranch = and(
                utf8AnyOf(1, Set.of("Books", "Electronics", "Sports")),
                utf8AnyOf(2, Set.of("computers", "stereo", "football")));
        FilterSpec secondBranch = and(
                utf8AnyOf(1, Set.of("Men", "Jewelry", "Women")),
                utf8AnyOf(2, Set.of("shirts", "birdal", "dresses")));
        return or(firstBranch, secondBranch);
    }

    private static FilterSpec query53QuarterlyDeviationPredicate(int sumIndex, int averageIndex)
    {
        Variable ten = new Variable(0);
        Variable result = new Variable(1);

        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(ten, new Literal(10L), AllMask.ALL),
                new Assignment(result, new Call("scaled_relative_difference_gt_i64", List.of(
                        new Reference(new Input(sumIndex), Stream.VALUES),
                        new Reference(new Input(averageIndex), Stream.VALUES),
                        new Reference(ten, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static FilterSpec query49SalesPredicate(int quantityIndex, int netPaidIndex, int netProfitIndex)
    {
        return and(
                greaterThan(quantityIndex, 0),
                and(
                        greaterThan(netPaidIndex, 0),
                        greaterThan(netProfitIndex, 100)));
    }

    private static FilterSpec query49TopRankPredicate(int returnRankIndex, int currencyRankIndex)
    {
        return or(
                lessThan(returnRankIndex, 11),
                lessThan(currencyRankIndex, 11));
    }

    private static Operator query44RankedItems(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, boolean descending)
    {
        Operator joined = query44AverageRows(allocator, primitiveRegistry, tables);
        joined = filter(allocator, primitiveRegistry, joined, query44ThresholdPredicate(1, 2));
        joined = projectInputs(allocator, primitiveRegistry, joined, 0, 1);
        joined = new TopNRankingOperator(allocator, 10, new int[] {1}, new boolean[] {descending}, joined, EngineResources.from(allocator).operatorResources().topNRankingPolicy());
        return projectInputs(allocator, primitiveRegistry, joined, 0, 2);
    }

    private static Operator query44AverageRows(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return projectQuery44AverageKeys(allocator, primitiveRegistry, query44JoinedAggregateRows(allocator, primitiveRegistry, tables));
    }

    private static Operator query44JoinedAggregateRows(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator itemAggregates = filteredProjectedScan(
                allocator,
                primitiveRegistry,
                tables,
                "store_sales",
                equal(1, 4),
                new String[] {"ss_item_sk", "ss_store_sk", "ss_net_profit"},
                0,
                2);
        itemAggregates = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new CountColumn(1)),
                itemAggregates);
        itemAggregates = filter(allocator, primitiveRegistry, itemAggregates, greaterThan(2, 0));
        itemAggregates = projectQuery44ItemAggregates(allocator, primitiveRegistry, itemAggregates);

        Operator scalarAggregate = query44ScalarAggregate(allocator, primitiveRegistry, tables);

        return new HashJoinOperator(allocator, itemAggregates, 0, scalarAggregate, 0);
    }

    private static Operator query44ScalarAggregate(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator scalarAggregate = query44RawScalarAggregate(allocator, primitiveRegistry, tables);
        scalarAggregate = filter(allocator, primitiveRegistry, scalarAggregate, greaterThan(2, 0));
        scalarAggregate = new EnforceSingleRowOperator(allocator, scalarAggregate);
        return projectQuery44ScalarAggregate(allocator, primitiveRegistry, scalarAggregate);
    }

    private static Operator query44RawScalarAggregate(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator scalarAggregate = filteredProjectedScan(
                allocator,
                primitiveRegistry,
                tables,
                "store_sales",
                and(equal(0, 4), isNull(1)),
                new String[] {"ss_store_sk", "ss_addr_sk", "ss_net_profit"},
                0,
                2);
        scalarAggregate = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new CountColumn(1)),
                scalarAggregate);
        return scalarAggregate;
    }

    private static Operator query06ScalarMonthSequence(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator monthSequence = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                and(equal(1, 2001), equal(2, 1)),
                new String[] {"d_month_seq", "d_year", "d_moy"},
                0);
        monthSequence = new MarkDistinctOperator(allocator, 0, monthSequence, EngineResources.from(allocator).operatorResources());
        return new EnforceSingleRowOperator(allocator, monthSequence);
    }

    private static Operator query06CategoryAggregates(Allocator allocator, TpcdsParquetTables tables)
    {
        return new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new CountColumn(1)),
                scannedTable(allocator, tables, "item", "i_category", "i_current_price"));
    }

    private static Operator projectQuery44ItemAggregates(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable joinKey = new Variable(0);
        List<Assignment> assignments = List.of(
                new Assignment(joinKey, new Literal(1L), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(joinKey, Stream.VALUES),
                new Reference(new Input(0), Stream.VALUES),
                new Reference(new Input(1), Stream.VALUES),
                new Reference(new Input(2), Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectQuery44ScalarAggregate(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable joinKey = new Variable(0);
        List<Assignment> assignments = List.of(
                new Assignment(joinKey, new Literal(1L), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(joinKey, Stream.VALUES),
                new Reference(new Input(1), Stream.VALUES),
                new Reference(new Input(2), Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectQuery44AverageKeys(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable itemAverage = new Variable(0);
        Variable scalarAverage = new Variable(1);
        List<Assignment> assignments = List.of(
                new Assignment(itemAverage, new Call("divide_round_i64", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES))), AllMask.ALL),
                new Assignment(scalarAverage, new Call("divide_round_i64", List.of(
                        new Reference(new Input(5), Stream.VALUES),
                        new Reference(new Input(6), Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(1), Stream.VALUES),
                new Reference(itemAverage, Stream.VALUES),
                new Reference(scalarAverage, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static FilterSpec query44ThresholdPredicate(int itemAverageIndex, int scalarAverageIndex)
    {
        Variable ten = new Variable(0);
        Variable nine = new Variable(1);
        Variable scaledItem = new Variable(2);
        Variable scaledScalar = new Variable(3);
        Variable greater = new Variable(4);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(ten, new Literal(10L), AllMask.ALL),
                new Assignment(nine, new Literal(9L), AllMask.ALL),
                new Assignment(scaledItem, new Call("multiply", List.of(
                        new Reference(new Input(itemAverageIndex), Stream.VALUES),
                        new Reference(ten, Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledScalar, new Call("multiply", List.of(
                        new Reference(new Input(scalarAverageIndex), Stream.VALUES),
                        new Reference(nine, Stream.VALUES))), AllMask.ALL),
                new Assignment(greater, new Call("lt", List.of(
                        new Reference(scaledScalar, Stream.VALUES),
                        new Reference(scaledItem, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(greater, Stream.VALUES)));
    }

    private static FilterSpec query45ZipOrItemPredicate(int zipIndex, int itemMatchIndex)
    {
        List<String> zipValues = List.of("80348", "81792", "83405", "85392", "85460", "85669", "86197", "86475", "88274");
        List<Assignment> assignments = new ArrayList<>();
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        assignments.add(new Assignment(start, new Literal(1L), AllMask.ALL));
        assignments.add(new Assignment(length, new Literal(5L), AllMask.ALL));

        Variable zipPrefix = new Variable(2);
        assignments.add(new Assignment(zipPrefix, new Call("substring_utf8", List.of(
                new Reference(new Input(zipIndex), Stream.VALUES),
                new Reference(start, Stream.VALUES),
                new Reference(length, Stream.VALUES))), AllMask.ALL));

        int nextVariable = 3;
        List<Reference> zipArguments = new ArrayList<>(zipValues.size() + 1);
        zipArguments.add(new Reference(zipPrefix, Stream.VALUES));
        for (String zipValue : zipValues) {
            Variable literal = new Variable(nextVariable++);
            assignments.add(new Assignment(literal, new Literal(zipValue), AllMask.ALL));
            zipArguments.add(new Reference(literal, Stream.VALUES));
        }

        Variable zipMatch = new Variable(nextVariable++);
        assignments.add(new Assignment(zipMatch, new Call("in_utf8", zipArguments), AllMask.ALL));

        Variable matches = new Variable(nextVariable);
        assignments.add(new Assignment(matches, new Call("or", List.of(
                new Reference(zipMatch, Stream.VALUES),
                new Reference(new Input(itemMatchIndex), Stream.VALUES))), AllMask.ALL));
        return new FilterSpec(
                new EvaluationPlan(assignments, List.of()),
                orMask(zipMatch, new Input(itemMatchIndex)),
                new Reference(matches, Stream.VALUES));
    }

    private static FilterSpec query15ZipStateOrPricePredicate(int zipIndex, int stateIndex, int salesPriceIndex)
    {
        List<String> zipValues = List.of("85669", "86197", "88274", "83405", "86475", "85392", "85460", "80348", "81792");
        List<String> states = List.of("CA", "WA", "GA");
        List<Assignment> assignments = new ArrayList<>();
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        assignments.add(new Assignment(start, new Literal(1L), AllMask.ALL));
        assignments.add(new Assignment(length, new Literal(5L), AllMask.ALL));

        Variable zipPrefix = new Variable(2);
        assignments.add(new Assignment(zipPrefix, new Call("substring_utf8", List.of(
                new Reference(new Input(zipIndex), Stream.VALUES),
                new Reference(start, Stream.VALUES),
                new Reference(length, Stream.VALUES))), AllMask.ALL));

        int nextVariable = 3;
        List<Reference> zipArguments = new ArrayList<>(zipValues.size() + 1);
        zipArguments.add(new Reference(zipPrefix, Stream.VALUES));
        for (String zipValue : zipValues) {
            Variable literal = new Variable(nextVariable++);
            assignments.add(new Assignment(literal, new Literal(zipValue), AllMask.ALL));
            zipArguments.add(new Reference(literal, Stream.VALUES));
        }
        Variable zipMatch = new Variable(nextVariable++);
        assignments.add(new Assignment(zipMatch, new Call("in_utf8", zipArguments), AllMask.ALL));

        List<Reference> stateArguments = new ArrayList<>(states.size() + 1);
        stateArguments.add(new Reference(new Input(stateIndex), Stream.VALUES));
        for (String state : states) {
            Variable literal = new Variable(nextVariable++);
            assignments.add(new Assignment(literal, new Literal(state), AllMask.ALL));
            stateArguments.add(new Reference(literal, Stream.VALUES));
        }
        Variable stateMatch = new Variable(nextVariable++);
        assignments.add(new Assignment(stateMatch, new Call("in_utf8", stateArguments), AllMask.ALL));

        Variable priceThreshold = new Variable(nextVariable++);
        assignments.add(new Assignment(priceThreshold, new Literal(50_000L), AllMask.ALL));
        Variable priceMatch = new Variable(nextVariable++);
        assignments.add(new Assignment(priceMatch, new Call("lt", List.of(
                new Reference(priceThreshold, Stream.VALUES),
                new Reference(new Input(salesPriceIndex), Stream.VALUES))), AllMask.ALL));

        Variable zipOrState = new Variable(nextVariable++);
        assignments.add(new Assignment(zipOrState, new Call("or", List.of(
                new Reference(zipMatch, Stream.VALUES),
                new Reference(stateMatch, Stream.VALUES))), AllMask.ALL));

        Variable accepted = new Variable(nextVariable);
        assignments.add(new Assignment(accepted, new Call("or", List.of(
                new Reference(zipOrState, Stream.VALUES),
                new Reference(priceMatch, Stream.VALUES))), AllMask.ALL));
        return new FilterSpec(
                new EvaluationPlan(assignments, List.of()),
                orMask(zipMatch, stateMatch, priceMatch),
                new Reference(accepted, Stream.VALUES));
    }

    private static Operator factScan(Allocator allocator, TpcdsParquetTables tables, String tableName, String... columns)
    {
        tables.recordScan();
        List<String> columnNames = List.of(columns);
        Operator decoder = new NitroParquetScanOperator(
                tables.scanResources(),
                allocator,
                tables.tableFiles(tableName),
                columnNames);
        return new BatchSourceOperator(
                new OperatorBatchSource(decoder, tables.tableSchema(tableName, columnNames)),
                new NativeSourceOperatorIngress());
    }

    private static Operator customerScan(Allocator allocator, TpcdsParquetTables tables, String... columns)
    {
        return factScan(allocator, tables, "customer", columns);
    }

    private static Operator scannedTable(Allocator allocator, TpcdsParquetTables tables, String tableName, String... columns)
    {
        return factScan(allocator, tables, tableName, columns);
    }

    private static Operator itemScan(Allocator allocator, TpcdsParquetTables tables, String... columns)
    {
        return factScan(allocator, tables, "item", columns);
    }

    private static Operator filter(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, FilterSpec filterSpec)
    {
        return new FilterOperator(
                source,
                filterSpec.plan(),
                primitiveRegistry,
                filterSpec.predicate(),
                allocator,
                EngineResources.from(allocator).operatorResources().filter());
    }

    private static Operator filteredProjectedScan(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String tableName, FilterSpec filterSpec, String[] columns, int... inputIndexes)
    {
        Operator source = factScan(allocator, tables, tableName, columns);
        if (filterSpec != null) {
            source = filter(allocator, primitiveRegistry, source, filterSpec);
        }
        if (inputIndexes.length == 0) {
            return source;
        }
        return projectInputs(allocator, primitiveRegistry, source, inputIndexes);
    }

    private static Operator filteredProjectedTable(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String tableName, FilterSpec filterSpec, String[] columns, int... inputIndexes)
    {
        Operator source = scannedTable(allocator, tables, tableName, columns);
        if (filterSpec != null) {
            source = filter(allocator, primitiveRegistry, source, filterSpec);
        }
        if (inputIndexes.length == 0) {
            return source;
        }
        return projectInputs(allocator, primitiveRegistry, source, inputIndexes);
    }

    private static Operator projectInputs(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int... inputIndexes)
    {
        List<Reference> outputs = new ArrayList<>(inputIndexes.length);
        for (int inputIndex : inputIndexes) {
            outputs.add(new Reference(new Input(inputIndex), Stream.VALUES));
        }
        outputs = List.copyOf(outputs);
        return new ProjectOperator(allocator, new EvaluationPlan(List.of(), outputs), primitiveRegistry, source);
    }

    /** Rounds physical-unit averages and converts them to their SQL unit. */
    private static Operator divideRoundedF64Inputs(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, double divisor, int... inputIndexes)
    {
        int[] replacementVariables = new int[source.outputCount()];
        Arrays.fill(replacementVariables, -1);

        List<Assignment> assignments = new ArrayList<>(inputIndexes.length * 2 + 1);
        Variable divisorVariable = new Variable(0);
        assignments.add(new Assignment(divisorVariable, new Literal(divisor), AllMask.ALL));
        for (int index = 0; index < inputIndexes.length; index++) {
            int inputIndex = inputIndexes[index];
            if (inputIndex < 0 || inputIndex >= source.outputCount()) {
                throw new IllegalArgumentException("Input index is out of bounds: " + inputIndex);
            }
            if (replacementVariables[inputIndex] >= 0) {
                throw new IllegalArgumentException("Duplicate input index: " + inputIndex);
            }
            Variable rounded = new Variable(index * 2 + 1);
            Variable scaled = new Variable(index * 2 + 2);
            replacementVariables[inputIndex] = index * 2 + 2;
            assignments.add(new Assignment(rounded, new Call("round_f64", List.of(
                    new Reference(new Input(inputIndex), Stream.VALUES))), AllMask.ALL));
            assignments.add(new Assignment(scaled, new Call("divide_f64", List.of(
                    new Reference(rounded, Stream.VALUES),
                    new Reference(divisorVariable, Stream.VALUES))), AllMask.ALL));
        }

        List<Reference> outputs = new ArrayList<>(source.outputCount());
        for (int inputIndex = 0; inputIndex < source.outputCount(); inputIndex++) {
            int variable = replacementVariables[inputIndex];
            outputs.add(variable < 0
                    ? new Reference(new Input(inputIndex), Stream.VALUES)
                    : new Reference(new Variable(variable), Stream.VALUES));
        }
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.copyOf(assignments), List.copyOf(outputs)),
                primitiveRegistry,
                source);
    }

    private static Operator projectCustomerName(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int customerIdIndex, int lastNameIndex, int firstNameIndex)
    {
        Variable separator = new Variable(0);
        Variable lastWithSeparator = new Variable(1);
        Variable customerName = new Variable(2);

        List<Assignment> assignments = List.of(
                new Assignment(separator, new Literal(", "), AllMask.ALL),
                new Assignment(lastWithSeparator, new Call("concat_utf8", List.of(
                        new Reference(new Input(lastNameIndex), Stream.VALUES),
                        new Reference(separator, Stream.VALUES))), AllMask.ALL),
                new Assignment(customerName, new Call("concat_utf8", List.of(
                        new Reference(lastWithSeparator, Stream.VALUES),
                        new Reference(new Input(firstNameIndex), Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(customerIdIndex), Stream.VALUES),
                new Reference(customerName, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectCustomerIdentity(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int lastNameIndex, int firstNameIndex, int salutationIndex, int preferredCustomerFlagIndex, int ticketNumberIndex, int countIndex)
    {
        Variable alwaysTrue = new Variable(0);
        Variable lastName = new Variable(1);
        Variable firstName = new Variable(2);
        Variable salutation = new Variable(3);
        Variable preferredCustomerFlag = new Variable(4);

        List<Assignment> assignments = List.of(
                new Assignment(alwaysTrue, new Literal(true), AllMask.ALL),
                new Assignment(lastName, new Call("if_utf8", List.of(
                        new Reference(alwaysTrue, Stream.VALUES),
                        new Reference(new Input(lastNameIndex), Stream.VALUES),
                        new Reference(new Input(lastNameIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(firstName, new Call("if_utf8", List.of(
                        new Reference(alwaysTrue, Stream.VALUES),
                        new Reference(new Input(firstNameIndex), Stream.VALUES),
                        new Reference(new Input(firstNameIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(salutation, new Call("if_utf8", List.of(
                        new Reference(alwaysTrue, Stream.VALUES),
                        new Reference(new Input(salutationIndex), Stream.VALUES),
                        new Reference(new Input(salutationIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(preferredCustomerFlag, new Call("if_utf8", List.of(
                        new Reference(alwaysTrue, Stream.VALUES),
                        new Reference(new Input(preferredCustomerFlagIndex), Stream.VALUES),
                        new Reference(new Input(preferredCustomerFlagIndex), Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(lastName, Stream.VALUES),
                new Reference(firstName, Stream.VALUES),
                new Reference(salutation, Stream.VALUES),
                new Reference(preferredCustomerFlag, Stream.VALUES),
                new Reference(new Input(ticketNumberIndex), Stream.VALUES),
                new Reference(new Input(countIndex), Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectShippingBuckets(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int firstNameIndex, int secondNameIndex, int thirdNameIndex, int shipDateIndex, int soldDateIndex)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable thirty = new Variable(2);
        Variable thirtyOne = new Variable(3);
        Variable sixty = new Variable(4);
        Variable sixtyOne = new Variable(5);
        Variable ninety = new Variable(6);
        Variable ninetyOne = new Variable(7);
        Variable oneHundredTwenty = new Variable(8);
        Variable oneHundredTwentyOne = new Variable(9);
        Variable days = new Variable(10);
        Variable bucket30Condition = new Variable(11);
        Variable greaterThanThirty = new Variable(12);
        Variable lessThanSixtyOne = new Variable(13);
        Variable bucket31To60Condition = new Variable(14);
        Variable greaterThanSixty = new Variable(15);
        Variable lessThanNinetyOne = new Variable(16);
        Variable bucket61To90Condition = new Variable(17);
        Variable greaterThanNinety = new Variable(18);
        Variable lessThanOneHundredTwentyOne = new Variable(19);
        Variable bucket91To120Condition = new Variable(20);
        Variable greaterThanOneHundredTwenty = new Variable(21);
        Variable bucket30 = new Variable(22);
        Variable bucket31To60 = new Variable(23);
        Variable bucket61To90 = new Variable(24);
        Variable bucket91To120 = new Variable(25);
        Variable bucketOver120 = new Variable(26);
        Variable prefixStart = new Variable(29);
        Variable prefixLength = new Variable(30);
        Variable firstNamePrefix = new Variable(31);

        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(thirty, new Literal(30L), AllMask.ALL),
                new Assignment(thirtyOne, new Literal(31L), AllMask.ALL),
                new Assignment(sixty, new Literal(60L), AllMask.ALL),
                new Assignment(sixtyOne, new Literal(61L), AllMask.ALL),
                new Assignment(ninety, new Literal(90L), AllMask.ALL),
                new Assignment(ninetyOne, new Literal(91L), AllMask.ALL),
                new Assignment(oneHundredTwenty, new Literal(120L), AllMask.ALL),
                new Assignment(oneHundredTwentyOne, new Literal(121L), AllMask.ALL),
                new Assignment(days, new Call("subtract", List.of(
                        new Reference(new Input(shipDateIndex), Stream.VALUES),
                        new Reference(new Input(soldDateIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket30Condition, new Call("lt", List.of(
                        new Reference(days, Stream.VALUES),
                        new Reference(thirtyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThanThirty, new Call("lt", List.of(
                        new Reference(thirty, Stream.VALUES),
                        new Reference(days, Stream.VALUES))), AllMask.ALL),
                new Assignment(lessThanSixtyOne, new Call("lt", List.of(
                        new Reference(days, Stream.VALUES),
                        new Reference(sixtyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket31To60Condition, new Call("and", List.of(
                        new Reference(greaterThanThirty, Stream.VALUES),
                        new Reference(lessThanSixtyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThanSixty, new Call("lt", List.of(
                        new Reference(sixty, Stream.VALUES),
                        new Reference(days, Stream.VALUES))), AllMask.ALL),
                new Assignment(lessThanNinetyOne, new Call("lt", List.of(
                        new Reference(days, Stream.VALUES),
                        new Reference(ninetyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket61To90Condition, new Call("and", List.of(
                        new Reference(greaterThanSixty, Stream.VALUES),
                        new Reference(lessThanNinetyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThanNinety, new Call("lt", List.of(
                        new Reference(ninety, Stream.VALUES),
                        new Reference(days, Stream.VALUES))), AllMask.ALL),
                new Assignment(lessThanOneHundredTwentyOne, new Call("lt", List.of(
                        new Reference(days, Stream.VALUES),
                        new Reference(oneHundredTwentyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket91To120Condition, new Call("and", List.of(
                        new Reference(greaterThanNinety, Stream.VALUES),
                        new Reference(lessThanOneHundredTwentyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThanOneHundredTwenty, new Call("lt", List.of(
                        new Reference(oneHundredTwenty, Stream.VALUES),
                        new Reference(days, Stream.VALUES))), AllMask.ALL),
                new Assignment(prefixStart, new Literal(1L), AllMask.ALL),
                new Assignment(prefixLength, new Literal(20L), AllMask.ALL),
                new Assignment(firstNamePrefix, new Call("substring_utf8", List.of(
                        new Reference(new Input(firstNameIndex), Stream.VALUES),
                        new Reference(prefixStart, Stream.VALUES),
                        new Reference(prefixLength, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket30, new Call("if_i64", List.of(
                        new Reference(bucket30Condition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket31To60, new Call("if_i64", List.of(
                        new Reference(bucket31To60Condition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket61To90, new Call("if_i64", List.of(
                        new Reference(bucket61To90Condition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket91To120, new Call("if_i64", List.of(
                        new Reference(bucket91To120Condition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucketOver120, new Call("if_i64", List.of(
                        new Reference(greaterThanOneHundredTwenty, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL));

        List<Reference> outputs = List.of(
                new Reference(firstNamePrefix, Stream.VALUES),
                new Reference(new Input(secondNameIndex), Stream.VALUES),
                new Reference(new Input(thirdNameIndex), Stream.VALUES),
                new Reference(bucket30, Stream.VALUES),
                new Reference(bucket31To60, Stream.VALUES),
                new Reference(bucket61To90, Stream.VALUES),
                new Reference(bucket91To120, Stream.VALUES),
                new Reference(bucketOver120, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator query80ChannelBranch(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            String salesTable,
            String[] salesColumns,
            String returnsTable,
            String[] returnsColumns,
            String dimensionTable,
            String[] dimensionColumns,
            String channelName,
            String idPrefix)
    {
        String profilePrefix = switch (idPrefix) {
            case "store" -> "q80.store";
            case "catalog_page" -> "q80.catalog";
            case "web_site" -> "q80.web";
            default -> "q80." + idPrefix;
        };
        Operator dateKeys = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                query80DatePredicate(),
                new String[] {"d_date_sk", "d_date"},
                0);
        dateKeys = profiled(profilePrefix + ".scan.date_dim", dateKeys);
        Operator itemKeys = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "item",
                greaterThan(1, 5_000),
                new String[] {"i_item_sk", "i_current_price"},
                0);
        itemKeys = profiled(profilePrefix + ".scan.item", itemKeys);
        Operator promotionKeys = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "promotion",
                equalUtf8(1, "N"),
                new String[] {"p_promo_sk", "p_channel_tv"},
                0);
        promotionKeys = profiled(profilePrefix + ".scan.promotion", promotionKeys);
        Operator dimension = profiled(profilePrefix + ".scan.dimension", scannedTable(allocator, tables, dimensionTable, dimensionColumns));

        Operator joined = profiled(profilePrefix + ".scan.sales", factScan(allocator, tables, salesTable, salesColumns));
        joined = profiled(profilePrefix + ".join.returns", new HashJoinOperator(
                allocator,
                joined,
                new int[] {1, 4},
                factScan(allocator, tables, returnsTable, returnsColumns),
                new int[] {0, 1},
                true));
        joined = profiled(profilePrefix + ".project.inputs", projectInputs(allocator, primitiveRegistry, joined, 0, 1, 2, 3, 5, 6, 9, 10));
        joined = profiled(profilePrefix + ".join.date_dim", new HashJoinOperator(allocator, joined, 0, dateKeys, 0));
        joined = profiled(profilePrefix + ".join.dimension", new HashJoinOperator(allocator, joined, 3, dimension, 0));
        joined = profiled(profilePrefix + ".join.item", new HashJoinOperator(allocator, joined, 1, itemKeys, 0));
        joined = profiled(profilePrefix + ".join.promotion", new HashJoinOperator(allocator, joined, 2, promotionKeys, 0));
        joined = profiled(profilePrefix + ".project.values", projectQuery80BranchValues(allocator, primitiveRegistry, joined, 10, 4, 5, 6, 7));
        joined = profiled(profilePrefix + ".group.values", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new Sum(2), new Sum(3)),
                joined));
        return profiled(profilePrefix + ".project.output", projectQuery80BranchOutput(allocator, primitiveRegistry, joined, channelName, idPrefix));
    }

    private static Operator query77ChannelBranch(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            String profilePrefix,
            String salesTable,
            String salesDateColumn,
            String salesIdColumn,
            String salesAmountColumn,
            String salesProfitColumn,
            String returnsTable,
            String returnsDateColumn,
            String returnsIdColumn,
            String returnAmountColumn,
            String returnLossColumn,
            String dimensionTable,
            String dimensionKeyColumn,
            String channelName)
    {
        Operator sales = profiled(profilePrefix + ".sales.scan", factScan(allocator, tables, salesTable, salesDateColumn, salesIdColumn, salesAmountColumn, salesProfitColumn));
        sales = profiled(profilePrefix + ".sales.join.date", new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query80DatePredicate(),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0));
        // The store and web CTEs inner-join the store/web_page dimension on the id column, which drops sales
        // with a null id; the catalog CTE groups by cs_call_center_sk directly and keeps null ids.
        if (dimensionTable != null) {
            sales = profiled(profilePrefix + ".sales.join.dimension", new HashJoinOperator(
                    allocator,
                    sales,
                    1,
                    scannedTable(allocator, tables, dimensionTable, dimensionKeyColumn),
                    0));
        }
        sales = profiled(profilePrefix + ".sales.project", projectInputs(allocator, primitiveRegistry, sales, 1, 2, 3));
        sales = profiled(profilePrefix + ".sales.group", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new Sum(2)),
                sales));

        Operator returns = profiled(profilePrefix + ".returns.scan", factScan(allocator, tables, returnsTable, returnsDateColumn, returnsIdColumn, returnAmountColumn, returnLossColumn));
        returns = profiled(profilePrefix + ".returns.join.date", new HashJoinOperator(
                allocator,
                returns,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query80DatePredicate(),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0));
        if (dimensionTable != null) {
            returns = profiled(profilePrefix + ".returns.join.dimension", new HashJoinOperator(
                    allocator,
                    returns,
                    1,
                    scannedTable(allocator, tables, dimensionTable, dimensionKeyColumn),
                    0));
        }
        returns = profiled(profilePrefix + ".returns.project", projectInputs(allocator, primitiveRegistry, returns, 1, 2, 3));
        returns = profiled(profilePrefix + ".returns.group", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new Sum(2)),
                returns));

        Operator joined = channelName.equals("catalog channel")
                ? new NestedLoopJoinOperator(allocator, sales, returns)
                : new HashJoinOperator(allocator, sales, 0, returns, 0, true);
        joined = profiled(profilePrefix + ".join.sales_returns", joined);
        return profiled(profilePrefix + ".project.output", projectQuery77BranchOutput(allocator, primitiveRegistry, joined, channelName));
    }

    private static Operator query95FilteredSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(
                allocator,
                tables,
                "web_sales",
                "ws_ship_date_sk",
                "ws_ship_addr_sk",
                "ws_web_site_sk",
                "ws_warehouse_sk",
                "ws_order_number",
                "ws_ext_ship_cost",
                "ws_net_profit");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        betweenInclusive(1, LocalDate.of(1999, 2, 1).toEpochDay(), LocalDate.of(1999, 4, 2).toEpochDay()),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                query95IllinoisAddressKeys(allocator, primitiveRegistry, tables),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                2,
                query95WebSiteKeys(allocator, primitiveRegistry, tables),
                0);
        // The order-level multi-warehouse set is equivalent to the correlated SQL EXISTS only when
        // ws1.ws_warehouse_sk is non-null: SQL '<>' evaluates UNKNOWN for a null outer warehouse.
        sales = filter(allocator, primitiveRegistry, sales, isNotNullI64(3));
        return projectInputs(allocator, primitiveRegistry, sales, 4, 5, 6);
    }

    private static Operator query16FilteredSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(
                allocator,
                tables,
                "catalog_sales",
                "cs_ship_date_sk",
                "cs_ship_addr_sk",
                "cs_call_center_sk",
                "cs_warehouse_sk",
                "cs_order_number",
                "cs_ext_ship_cost",
                "cs_net_profit");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        betweenInclusive(1, LocalDate.of(2002, 2, 1).toEpochDay(), LocalDate.of(2002, 4, 2).toEpochDay()),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_address",
                        equalUtf8(1, "GA"),
                        new String[] {"ca_address_sk", "ca_state"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                2,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "call_center",
                        equalUtf8(1, "Williamson County"),
                        new String[] {"cc_call_center_sk", "cc_county"},
                        0),
                0);
        // The correlated EXISTS compares cs1.cs_warehouse_sk with cs2.cs_warehouse_sk. An order-level
        // multi-warehouse membership set is equivalent only for non-null cs1 warehouse keys; SQL '<>' rejects NULL.
        sales = filter(allocator, primitiveRegistry, sales, isNotNullI64(3));
        return projectInputs(allocator, primitiveRegistry, sales, 4, 5, 6);
    }

    private static Operator query16MultiWarehouseOrders(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator left = factScan(allocator, tables, "catalog_sales", "cs_warehouse_sk", "cs_order_number");
        Operator right = factScan(allocator, tables, "catalog_sales", "cs_warehouse_sk", "cs_order_number");
        Operator joined = new HashJoinOperator(allocator, left, 1, right, 1);
        joined = filter(allocator, primitiveRegistry, joined, notEqualColumns(0, 2));
        joined = projectInputs(allocator, primitiveRegistry, joined, 1);
        return new MarkDistinctOperator(allocator, 0, joined, EngineResources.from(allocator).operatorResources());
    }

    private static Operator query16ReturnedEligibleOrders(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator returns = scannedTable(allocator, tables, "catalog_returns", "cr_order_number");
        returns = new HashJoinOperator(allocator, returns, 0, query16MultiWarehouseOrders(allocator, primitiveRegistry, tables), 0);
        returns = projectInputs(allocator, primitiveRegistry, returns, 0);
        return new MarkDistinctOperator(allocator, 0, returns, EngineResources.from(allocator).operatorResources());
    }

    private static Operator query05ChannelBranch(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            String salesTable,
            String salesDateColumn,
            String salesDimensionColumn,
            String salesAmountColumn,
            String salesProfitColumn,
            String returnsTable,
            String returnsDateColumn,
            String returnsDimensionColumn,
            String returnsAmountColumn,
            String returnsLossColumn,
            String dimensionTable,
            String dimensionKeyColumn,
            String dimensionIdColumn,
            String channelName,
            String idPrefix)
    {
        Operator sales = factScan(allocator, tables, salesTable, salesDateColumn, salesDimensionColumn, salesAmountColumn, salesProfitColumn);
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        betweenInclusive(1, LocalDate.of(2000, 8, 23).toEpochDay(), LocalDate.of(2000, 9, 6).toEpochDay()),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, dimensionTable, dimensionKeyColumn, dimensionIdColumn),
                0);
        sales = projectQuery05SalesValues(allocator, primitiveRegistry, sales, 6, 2, 3);

        Operator returns = factScan(allocator, tables, returnsTable, returnsDateColumn, returnsDimensionColumn, returnsAmountColumn, returnsLossColumn);
        returns = new HashJoinOperator(
                allocator,
                returns,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        betweenInclusive(1, LocalDate.of(2000, 8, 23).toEpochDay(), LocalDate.of(2000, 9, 6).toEpochDay()),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);
        returns = new HashJoinOperator(
                allocator,
                returns,
                1,
                scannedTable(allocator, tables, dimensionTable, dimensionKeyColumn, dimensionIdColumn),
                0);
        returns = projectQuery05ReturnsValues(allocator, primitiveRegistry, returns, 6, 2, 3);

        Operator branch = new UnionAllOperator(4, List.of(sales, returns));
        branch = new GroupedAggregationOperator(allocator, List.of(0), List.of(new Sum(1), new Sum(2), new Sum(3)), branch);
        return projectQuery80BranchOutput(allocator, primitiveRegistry, branch, channelName, idPrefix);
    }

    private static Operator query05WebChannelBranch(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(allocator, tables, "web_sales", "ws_sold_date_sk", "ws_web_site_sk", "ws_ext_sales_price", "ws_net_profit");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        betweenInclusive(1, LocalDate.of(2000, 8, 23).toEpochDay(), LocalDate.of(2000, 9, 6).toEpochDay()),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "web_site", "web_site_sk", "web_site_id"),
                0);
        sales = projectQuery05SalesValues(allocator, primitiveRegistry, sales, 6, 2, 3);

        Operator returns = factScan(allocator, tables, "web_returns", "wr_returned_date_sk", "wr_item_sk", "wr_order_number", "wr_return_amt", "wr_net_loss");
        returns = new HashJoinOperator(
                allocator,
                returns,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        betweenInclusive(1, LocalDate.of(2000, 8, 23).toEpochDay(), LocalDate.of(2000, 9, 6).toEpochDay()),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);

        // Build the two-key hash on the date-filtered returns side, which is much smaller than all web sales.
        Operator salesSiteKeys = factScan(allocator, tables, "web_sales", "ws_item_sk", "ws_order_number", "ws_web_site_sk");
        returns = new HashJoinOperator(
                allocator,
                salesSiteKeys,
                new int[] {0, 1},
                returns,
                new int[] {1, 2});
        returns = new HashJoinOperator(
                allocator,
                returns,
                2,
                scannedTable(allocator, tables, "web_site", "web_site_sk", "web_site_id"),
                0);
        returns = projectQuery05ReturnsValues(allocator, primitiveRegistry, returns, 10, 6, 7);

        Operator branch = new UnionAllOperator(4, List.of(sales, returns));
        branch = new GroupedAggregationOperator(allocator, List.of(0), List.of(new Sum(1), new Sum(2), new Sum(3)), branch);
        return projectQuery80BranchOutput(allocator, primitiveRegistry, branch, "web channel", "web_site");
    }

    private static Operator query95MultiWarehouseOrders(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator left = factScan(allocator, tables, "web_sales", "ws_warehouse_sk", "ws_order_number");
        Operator right = factScan(allocator, tables, "web_sales", "ws_warehouse_sk", "ws_order_number");
        Operator joined = new HashJoinOperator(
                allocator,
                left,
                1,
                right,
                1,
                HashJoinOperator.JoinFilter.longNotEqual(0, 0));
        joined = projectInputs(allocator, primitiveRegistry, joined, 1);
        return new MarkDistinctOperator(allocator, 0, joined, EngineResources.from(allocator).operatorResources());
    }

    private static Operator query95ReturnedEligibleOrders(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        // The multi-warehouse constraint is already applied on the sales side (the eligible-orders
        // join), and the downstream join of these returned orders against the eligible sales restricts
        // the result to multi-warehouse orders. Joining returns to the multi-warehouse set here would
        // be redundant, so this is a plain distinct of returned order numbers (matching Trino).
        Operator returns = scannedTable(allocator, tables, "web_returns", "wr_order_number");
        return new MarkDistinctOperator(allocator, 0, returns, EngineResources.from(allocator).operatorResources());
    }

    private static Operator query95IllinoisAddressKeys(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "customer_address",
                equalUtf8(1, "IL"),
                new String[] {"ca_address_sk", "ca_state"},
                0);
    }

    private static Operator query95WebSiteKeys(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "web_site",
                utf8StartsWith(1, "pri"),
                new String[] {"web_site_sk", "web_company_name"},
                0);
    }

    private static Operator projectQuery05SalesValues(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int idIndex, int salesIndex, int profitIndex)
    {
        Variable zero = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(zero, new Literal(0L), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(idIndex), Stream.VALUES),
                                new Reference(new Input(salesIndex), Stream.VALUES),
                                new Reference(zero, Stream.VALUES),
                                new Reference(new Input(profitIndex), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery05ReturnsValues(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int idIndex, int returnAmountIndex, int returnLossIndex)
    {
        Variable zero = new Variable(0);
        Variable profit = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(zero, new Literal(0L), AllMask.ALL),
                                new Assignment(profit, new Call("subtract", List.of(
                                        new Reference(zero, Stream.VALUES),
                                        new Reference(new Input(returnLossIndex), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(idIndex), Stream.VALUES),
                                new Reference(zero, Stream.VALUES),
                                new Reference(new Input(returnAmountIndex), Stream.VALUES),
                                new Reference(profit, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query81CustomerTotalReturn(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator returns = profiled("q81.customer_totals.scan", factScan(allocator, tables, "catalog_returns", "cr_returning_customer_sk", "cr_returned_date_sk", "cr_returning_addr_sk", "cr_return_amt_inc_tax"));
        returns = profiled("q81.customer_totals.join.date", new HashJoinOperator(
                allocator,
                returns,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 2000),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0));
        returns = profiled("q81.customer_totals.join.address", new HashJoinOperator(
                allocator,
                returns,
                2,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_state"),
                0));
        returns = profiled("q81.customer_totals.project", projectInputs(allocator, primitiveRegistry, returns, 0, 6, 3));
        return profiled("q81.customer_totals.group", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2)),
                returns));
    }

    private static Operator query81StateAverageReturns(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerTotalReturn = query81CustomerTotalReturn(allocator, primitiveRegistry, tables);
        customerTotalReturn = profiled("q81.state_averages.filter", filter(allocator, primitiveRegistry, customerTotalReturn, isNotNullI64(2)));
        customerTotalReturn = profiled("q81.state_averages.group", new GroupedAggregationOperator(
                allocator,
                List.of(1),
                List.of(new Sum(2), new CountColumn(2)),
                customerTotalReturn));
        return profiled("q81.state_averages.project", projectQuery81StateAverage(allocator, primitiveRegistry, customerTotalReturn));
    }

    private static Operator query30CustomerTotalReturn(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator returns = factScan(allocator, tables, "web_returns", "wr_returning_customer_sk", "wr_returned_date_sk", "wr_returning_addr_sk", "wr_return_amt");
        returns = new HashJoinOperator(
                allocator,
                returns,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 2002),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0);
        returns = new HashJoinOperator(
                allocator,
                returns,
                2,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_state"),
                0);
        returns = projectInputs(allocator, primitiveRegistry, returns, 0, 6, 3);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2)),
                returns);
    }

    private static Operator query30StateAverageReturns(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerTotalReturn = query30CustomerTotalReturn(allocator, primitiveRegistry, tables);
        customerTotalReturn = filter(allocator, primitiveRegistry, customerTotalReturn, isNotNullI64(2));
        customerTotalReturn = new GroupedAggregationOperator(
                allocator,
                List.of(1),
                List.of(new Sum(2), new CountColumn(2)),
                customerTotalReturn);
        return projectQuery81StateAverage(allocator, primitiveRegistry, customerTotalReturn);
    }

    private static Operator query14CrossItems(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sharedTriples = profiled("q14.cross.join.store_catalog", new HashJoinOperator(
                allocator,
                query14ChannelTriples(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk"),
                new int[] {0, 1, 2},
                query14ChannelTriples(allocator, primitiveRegistry, tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk"),
                new int[] {0, 1, 2}));
        sharedTriples = profiled("q14.cross.project.store_catalog", projectInputs(allocator, primitiveRegistry, sharedTriples, 0, 1, 2));
        sharedTriples = profiled("q14.cross.join.web", new HashJoinOperator(
                allocator,
                sharedTriples,
                new int[] {0, 1, 2},
                query14ChannelTriples(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk"),
                new int[] {0, 1, 2}));
        sharedTriples = profiled("q14.cross.project.web", projectInputs(allocator, primitiveRegistry, sharedTriples, 0, 1, 2));
        sharedTriples = profiled("q14.cross.distinct.triples", new MarkDistinctOperator(allocator, new int[] {0, 1, 2}, sharedTriples, EngineResources.from(allocator).operatorResources()));

        Operator crossItems = profiled("q14.cross.scan.item", scannedTable(allocator, tables, "item", "i_item_sk", "i_brand_id", "i_class_id", "i_category_id"));
        crossItems = profiled("q14.cross.join.item", new HashJoinOperator(
                allocator,
                crossItems,
                new int[] {1, 2, 3},
                sharedTriples,
                new int[] {0, 1, 2}));
        crossItems = profiled("q14.cross.project.item", projectInputs(allocator, primitiveRegistry, crossItems, 0));
        return profiled("q14.cross.distinct.item", new MarkDistinctOperator(allocator, 0, crossItems, EngineResources.from(allocator).operatorResources()));
    }

    private static Operator query14ChannelTriples(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn)
    {
        Operator triples = profiled("q14.triples.scan." + salesTable, factScan(allocator, tables, salesTable, soldDateColumn, itemColumn));
        triples = profiled("q14.triples.join.item." + salesTable, new HashJoinOperator(
                allocator,
                triples,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_brand_id", "i_class_id", "i_category_id"),
                0));
        triples = profiled("q14.triples.join.date." + salesTable, new HashJoinOperator(
                allocator,
                triples,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query14YearRangePredicate(),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0));
        triples = profiled("q14.triples.project." + salesTable, projectInputs(allocator, primitiveRegistry, triples, 3, 4, 5));
        return profiled("q14.triples.distinct." + salesTable, new MarkDistinctOperator(allocator, new int[] {0, 1, 2}, triples, EngineResources.from(allocator).operatorResources()));
    }

    private static Operator query14AverageSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = new UnionAllOperator(
                1,
                List.of(
                        query14ChannelSalesValues(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_quantity", "ss_list_price"),
                        query14ChannelSalesValues(allocator, primitiveRegistry, tables, "catalog_sales", "cs_sold_date_sk", "cs_quantity", "cs_list_price"),
                        query14ChannelSalesValues(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_quantity", "ws_list_price")));
        sales = new AggregationOperator(
                allocator,
                List.of(new Sum(0), new CountColumn(0)),
                sales);
        return projectQuery09Average(allocator, primitiveRegistry, sales, 0, 1);
    }

    private static Operator query08QualifiedZipPrefixes(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        // TPC-DS Q08 keeps zip prefixes that appear in the literal list AND have more than ten
        // preferred customers. This is the canonical single-scan form: join the preferred customers
        // to their addresses, apply the literal zip list as an inline IN filter, group by the
        // five-character zip prefix keeping counts above ten, then distinct the two-character prefix.
        Operator qualifiedZipValues = new HashJoinOperator(
                allocator,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer",
                        equalUtf8(1, "Y"),
                        new String[] {"c_current_addr_sk", "c_preferred_cust_flag"},
                        0),
                0,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_zip"),
                0);
        qualifiedZipValues = projectUtf8Prefix(allocator, primitiveRegistry, qualifiedZipValues, 2, 5);
        qualifiedZipValues = filter(allocator, primitiveRegistry, qualifiedZipValues, query08ZipPrefixListPredicate(0));
        qualifiedZipValues = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new CountAll()),
                qualifiedZipValues);
        qualifiedZipValues = filter(allocator, primitiveRegistry, qualifiedZipValues, greaterThan(1, 10));
        qualifiedZipValues = projectUtf8Prefix(allocator, primitiveRegistry, qualifiedZipValues, 0, 2);
        qualifiedZipValues = new MarkDistinctOperator(allocator, 0, qualifiedZipValues, EngineResources.from(allocator).operatorResources());
        return qualifiedZipValues;
    }

    private static Operator query14ChannelSalesValues(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String soldDateColumn, String quantityColumn, String listPriceColumn)
    {
        Operator sales = factScan(allocator, tables, salesTable, soldDateColumn, quantityColumn, listPriceColumn);
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query14YearRangePredicate(),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0);
        return projectQuery14SalesOnly(allocator, primitiveRegistry, sales, 1, 2);
    }

    private static Operator query14ChannelBranch(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            Operator crossItems,
            Operator averageSales,
            String salesTable,
            String soldDateColumn,
            String itemColumn,
            String quantityColumn,
            String listPriceColumn,
            String channelName)
    {
        Operator branch = factScan(allocator, tables, salesTable, soldDateColumn, itemColumn, quantityColumn, listPriceColumn);
        branch = new SemiJoinOperator(allocator, branch, 1, crossItems, 0);
        branch = new HashJoinOperator(
                allocator,
                branch,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_brand_id", "i_class_id", "i_category_id"),
                0);
        branch = new HashJoinOperator(
                allocator,
                branch,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 2001), equal(2, 11)),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0),
                0);
        branch = projectQuery14SalesByCategory(allocator, primitiveRegistry, branch, 5, 6, 7, 2, 3);
        branch = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new CountAll()),
                branch);
        branch = new NestedLoopJoinOperator(allocator, branch, averageSales);
        branch = filter(allocator, primitiveRegistry, branch, query14ThresholdPredicate(3, 5));
        return projectQuery14ChannelOutput(allocator, primitiveRegistry, branch, channelName);
    }

    private static Operator query24Sales(TpcdsQueryContext context, String colorFilter)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        Operator sales = context.profiled("q24.pale.base", query24CustomerStoreItemSales(context));
        HashJoinOperator storeJoin = new HashJoinOperator(
                allocator,
                sales,
                4,
                projectQuery24StoreDetails(
                        allocator,
                        primitiveRegistry,
                        filteredProjectedTable(
                                allocator,
                                primitiveRegistry,
                                tables,
                                "store",
                                and(equal(1, 8), notEmptyUtf8(4)),
                                new String[] {"s_store_sk", "s_market_id", "s_store_name", "s_state", "s_zip"},
                                0, 2, 3, 4)),
                        0);
        storeJoin.withOutputs(0, 1, 2, 3, 4, 5, 6, 8, 9, 10);
        sales = context.profiled("q24.pale.join.store", storeJoin);
        HashJoinOperator itemJoin = new HashJoinOperator(
                allocator,
                sales,
                5,
                colorFilter == null
                        ? scannedTable(allocator, tables, "item", "i_item_sk", "i_current_price", "i_size", "i_color", "i_units", "i_manager_id")
                        : filteredProjectedTable(
                                allocator,
                                primitiveRegistry,
                                tables,
                                "item",
                                equalUtf8(3, colorFilter),
                                new String[] {"i_item_sk", "i_current_price", "i_size", "i_color", "i_units", "i_manager_id"}),
                0);
        itemJoin.withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15);
        sales = context.profiled("q24.pale.join.item", itemJoin);
        HashJoinOperator addressJoin = new HashJoinOperator(
                allocator,
                sales,
                new int[] {3, 9},
                query24AddressLookup(allocator, primitiveRegistry, tables),
                new int[] {0, 1});
        sales = context.profiled("q24.pale.join.address", addressJoin);
        // SQL requires c_birth_country <> upper(ca_country). Keeping this as a post-join predicate gives
        // all three engines the same composite (address, zip) equi-join shape and exact SQL NULL semantics.
        sales = context.profiled("q24.pale.filter.country", filter(allocator, primitiveRegistry, sales, notEqualUtf8Columns(2, 17)));
        sales = context.profiled("q24.pale.project", projectInputs(allocator, primitiveRegistry, sales, 0, 1, 7, 18, 8, 12, 10, 14, 13, 11, 6));
        return context.profiled("q24.pale.group", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                List.of(new Sum(10)),
                sales));
    }

    private static Operator query24AverageSales(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        Operator sales = context.profiled("q24.average.base", query24CustomerStoreItemSales(context));
        HashJoinOperator storeJoin = new HashJoinOperator(
                allocator,
                sales,
                4,
                projectQuery24StoreJoinKey(
                        allocator,
                        primitiveRegistry,
                        filteredProjectedTable(
                                allocator,
                                primitiveRegistry,
                                tables,
                                "store",
                                and(equal(1, 8), notEmptyUtf8(4)),
                                new String[] {"s_store_sk", "s_market_id", "s_store_name", "s_state", "s_zip"},
                                0, 4)),
                        0);
        storeJoin.withOutputs(0, 1, 2, 3, 4, 5, 6, 8);
        sales = context.profiled("q24.average.join.store", storeJoin);
        HashJoinOperator addressJoin = new HashJoinOperator(
                allocator,
                sales,
                new int[] {3, 7},
                query24AddressLookup(allocator, primitiveRegistry, tables),
                new int[] {0, 1});
        sales = context.profiled("q24.average.join.address", addressJoin);
        sales = context.profiled("q24.average.filter.country", filter(allocator, primitiveRegistry, sales, notEqualUtf8Columns(2, 10)));
        sales = context.profiled("q24.average.project", projectInputs(allocator, primitiveRegistry, sales, 0, 1, 11, 4, 5, 6));
        return context.profiled("q24.average.group", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4),
                List.of(new Sum(5)),
                sales));
    }

    private static Operator query24CustomerStoreItemSales(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        TpcdsParquetTables tables = context.tables();
        Operator sales = context.profiled("q24.scan.store_sales", factScan(allocator, tables, "store_sales", "ss_ticket_number", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_net_paid"));
        HashJoinOperator returnsJoin = new HashJoinOperator(
                allocator,
                sales,
                new int[] {0, 1},
                context.profiled("q24.scan.store_returns", scannedTable(allocator, tables, "store_returns", "sr_ticket_number", "sr_item_sk")),
                new int[] {0, 1});
        returnsJoin.withOutputs(1, 2, 3, 4);
        sales = context.profiled("q24.join.store_returns", returnsJoin);
        HashJoinOperator customerJoin = new HashJoinOperator(
                allocator,
                sales,
                1,
                context.profiled("q24.scan.customer", scannedTable(allocator, tables, "customer", "c_customer_sk", "c_last_name", "c_first_name", "c_birth_country", "c_current_addr_sk")),
                0);
        customerJoin.withOutputs(5, 6, 7, 8, 2, 0, 3);
        sales = context.profiled("q24.join.customer", customerJoin);
        return context.profiled("q24.group", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5),
                List.of(new Sum(6)),
                sales));
    }

    private static Operator query24AddressLookup(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return projectQuery24AddressCountry(
                allocator,
                primitiveRegistry,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_address",
                        notEmptyUtf8(1),
                        new String[] {"ca_address_sk", "ca_zip", "ca_state", "ca_country"}));
    }

    private static Operator projectQuery24StoreDetails(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zipKey = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(zipKey, new Call("cast_utf8_to_i64", List.of(
                                new Reference(new Input(3), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(zipKey, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery19ZipPrefixes(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int addressZipIndex, int storeZipIndex, int brandIdIndex, int brandIndex, int manufacturerIdIndex, int manufacturerIndex, int salesIndex)
    {
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable addressPrefix = new Variable(2);
        Variable storePrefix = new Variable(3);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(start, new Literal(1L), AllMask.ALL),
                                new Assignment(length, new Literal(5L), AllMask.ALL),
                                new Assignment(addressPrefix, new Call("substring_utf8", List.of(
                                        new Reference(new Input(addressZipIndex), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))), AllMask.ALL),
                                new Assignment(storePrefix, new Call("substring_utf8", List.of(
                                        new Reference(new Input(storeZipIndex), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(brandIdIndex), Stream.VALUES),
                                new Reference(new Input(brandIndex), Stream.VALUES),
                                new Reference(new Input(manufacturerIdIndex), Stream.VALUES),
                                new Reference(new Input(manufacturerIndex), Stream.VALUES),
                                new Reference(new Input(salesIndex), Stream.VALUES),
                                new Reference(addressPrefix, Stream.VALUES),
                                new Reference(storePrefix, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery24StoreJoinKey(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zipKey = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(zipKey, new Call("cast_utf8_to_i64", List.of(
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(zipKey, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery24AddressCountry(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zipKey = new Variable(0);
        Variable upperCountry = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(zipKey, new Call("cast_utf8_to_i64", List.of(
                                        new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                                new Assignment(upperCountry, new Call("upper_utf8", List.of(
                                        new Reference(new Input(3), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(zipKey, Stream.VALUES),
                                new Reference(upperCountry, Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query32ItemAverageDiscounts(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator discounts = factScan(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_ext_discount_amt");
        discounts = new HashJoinOperator(
                allocator,
                discounts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query92DatePredicate(),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);
        discounts = projectInputs(allocator, primitiveRegistry, discounts, 1, 2);
        discounts = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new CountColumn(1)),
                discounts);
        return projectQuery81StateAverage(allocator, primitiveRegistry, discounts);
    }

    private static Operator query09Bucket(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, int minimumQuantityInclusive, int maximumQuantityInclusive, long threshold)
    {
        FilterSpec quantityPredicate = query09QuantityPredicate(minimumQuantityInclusive, maximumQuantityInclusive);

        Operator count = filteredProjectedScan(
                allocator,
                primitiveRegistry,
                tables,
                "store_sales",
                quantityPredicate,
                new String[] {"ss_quantity"});
        count = new AggregationOperator(
                allocator,
                List.of(new CountAll()),
                count);

        Operator discountAverage = filteredProjectedScan(
                allocator,
                primitiveRegistry,
                tables,
                "store_sales",
                quantityPredicate,
                new String[] {"ss_quantity", "ss_ext_discount_amt"},
                1);
        discountAverage = new AggregationOperator(
                allocator,
                List.of(new Sum(0), new CountColumn(0)),
                discountAverage);
        discountAverage = projectQuery09Average(allocator, primitiveRegistry, discountAverage, 0, 1);

        Operator netPaidAverage = filteredProjectedScan(
                allocator,
                primitiveRegistry,
                tables,
                "store_sales",
                quantityPredicate,
                new String[] {"ss_quantity", "ss_net_paid"},
                1);
        netPaidAverage = new AggregationOperator(
                allocator,
                List.of(new Sum(0), new CountColumn(0)),
                netPaidAverage);
        netPaidAverage = projectQuery09Average(allocator, primitiveRegistry, netPaidAverage, 0, 1);

        Operator bucket = new NestedLoopJoinOperator(allocator, count, discountAverage);
        bucket = new NestedLoopJoinOperator(allocator, bucket, netPaidAverage);
        return projectQuery09BucketValue(allocator, primitiveRegistry, bucket, threshold);
    }

    private static Operator query23FrequentItems(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        Operator frequentItems = context.profiled("q23.frequent.scan.store_sales", factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk"));
        frequentItems = context.profiled("q23.frequent.join.date", new HashJoinOperator(
                allocator,
                frequentItems,
                0,
                context.profiled("q23.frequent.scan.date", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query23YearRangePredicate(),
                        new String[] {"d_date_sk", "d_year", "d_date"},
                        0, 2)),
                0));
        frequentItems = context.profiled("q23.frequent.join.item", new HashJoinOperator(
                allocator,
                frequentItems,
                1,
                context.profiled("q23.frequent.scan.item", scannedTable(allocator, tables, "item", "i_item_sk", "i_item_desc")),
                0));
        frequentItems = context.profiled("q23.frequent.project", projectUtf8Prefix(allocator, primitiveRegistry, frequentItems, 5, 30, 1, 3));
        frequentItems = context.profiled("q23.frequent.group.partial", new AdaptiveSqlPartialAggregationOperator(
                allocator,
                frequentItems,
                2,
                List.of(0, 1, 2),
                () -> List.of(new CountAll()),
                16L * 1024 * 1024,
                0.8));
        frequentItems = context.profiled("q23.frequent.group.final", new SqlStageAggregationOperator(
                allocator,
                frequentItems,
                2,
                new int[] {0, 1, 2},
                List.of(SqlStageAggregationOperator.aggregate(
                        List.of(0, 1, 2),
                        () -> List.of(new Sum(3))))));
        frequentItems = context.profiled("q23.frequent.filter", filter(allocator, primitiveRegistry, frequentItems, greaterThan(3, 4)));
        frequentItems = context.profiled("q23.frequent.project_item", projectInputs(allocator, primitiveRegistry, frequentItems, 0));
        return context.profiled("q23.frequent.distinct", new MarkDistinctOperator(allocator, 0, frequentItems, EngineResources.from(allocator).operatorResources()));
    }

    private static Operator query23CustomerSales(TpcdsQueryContext context, boolean filterYears)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        String profile = filterYears ? "q23.customer_window" : "q23.customer_all";
        Operator sales = filterYears
                ? context.profiled(profile + ".scan.store_sales", factScan(allocator, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_quantity", "ss_sales_price"))
                : context.profiled(profile + ".scan.store_sales", factScan(allocator, tables, "store_sales", "ss_customer_sk", "ss_quantity", "ss_sales_price"));
        if (filterYears) {
            sales = context.profiled(profile + ".join.date", new HashJoinOperator(
                    allocator,
                    sales,
                    1,
                    context.profiled(profile + ".scan.date", filteredProjectedTable(
                            allocator,
                            primitiveRegistry,
                            tables,
                            "date_dim",
                            query23YearRangePredicate(),
                            new String[] {"d_date_sk", "d_year"},
                            0)),
                    0));
        }
        HashJoinOperator customerJoin = new HashJoinOperator(
                allocator,
                sales,
                0,
                context.profiled(profile + ".scan.customer", scannedTable(allocator, tables, "customer", "c_customer_sk")),
                0);
        // The customer join is an eligibility join; its key is equal to ss_customer_sk and no inner payload is
        // consumed. Retain the SQL-derived store_sales columns in their original order and discard only the inner
        // duplicate key. The former ordinals came from the pre-compaction wide scan and are now out of bounds.
        sales = context.profiled(profile + ".join.customer", filterYears
                ? customerJoin.withOutputs(0, 1, 2, 3)
                : customerJoin.withOutputs(0, 1, 2));
        sales = context.profiled(profile + ".project_value", projectQuery23SalesValue(allocator, primitiveRegistry, sales, 0, filterYears ? 2 : 1, filterYears ? 3 : 2));
        return context.profiled(profile + ".group", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                sales));
    }

    private static Operator query23BestCustomers(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        Operator customerSales = context.profiled("q23.best.customer_sales", query23CustomerSales(context, false));
        Operator maxStoreSales = context.profiled("q23.best.max", new AggregationOperator(
                context.allocator(),
                List.of(new Max(0)),
                context.profiled("q23.best.project_window", projectInputs(allocator, primitiveRegistry, query23CustomerSales(context, true), 1))));
        customerSales = context.profiled("q23.best.join_max", new NestedLoopJoinOperator(allocator, customerSales, maxStoreSales));
        customerSales = context.profiled("q23.best.filter", filter(allocator, primitiveRegistry, customerSales, query23BestCustomerPredicate(1, 2)));
        return context.profiled("q23.best.project_customer", projectInputs(allocator, primitiveRegistry, customerSales, 0));
    }

    private static Operator query23Channel(
            TpcdsQueryContext context,
            String salesTable,
            String soldDateColumn,
            String customerColumn,
            String itemColumn,
            String quantityColumn,
            String listPriceColumn)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        String profile = salesTable.equals("catalog_sales") ? "q23.catalog" : "q23.web";
        Operator channel = context.profiled(profile + ".scan", factScan(allocator, tables, salesTable, soldDateColumn, customerColumn, itemColumn, quantityColumn, listPriceColumn));
        channel = context.profiled(profile + ".join.date", new HashJoinOperator(
                allocator,
                channel,
                0,
                context.profiled(profile + ".scan.date", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 2000), equal(2, 2)),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0)),
                0));
        channel = context.profiled(profile + ".semi.frequent", new SemiJoinOperator(allocator, channel, 2, query23FrequentItems(context), 0));
        channel = context.profiled(profile + ".semi.best", new SemiJoinOperator(allocator, channel, 1, query23BestCustomers(context), 0));
        return context.profiled(profile + ".project_sales", projectQuery23SalesOnly(allocator, primitiveRegistry, channel, 3, 4));
    }

    private static Operator query92ItemAverageDiscounts(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator discounts = factScan(allocator, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_ext_discount_amt");
        discounts = new HashJoinOperator(
                allocator,
                discounts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query92DatePredicate(),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);
        discounts = projectInputs(allocator, primitiveRegistry, discounts, 1, 2);
        discounts = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new CountColumn(1)),
                discounts);
        return projectQuery81StateAverage(allocator, primitiveRegistry, discounts);
    }

    private static Operator projectQuery80BranchValues(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int idIndex, int salesIndex, int profitIndex, int returnAmountIndex, int returnLossIndex)
    {
        Variable zero = new Variable(0);
        Variable returnAmountIsNull = new Variable(1);
        Variable returnAmount = new Variable(2);
        Variable returnLossIsNull = new Variable(3);
        Variable returnLoss = new Variable(4);
        Variable profit = new Variable(5);

        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(returnAmountIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(returnAmountIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(returnAmount, new Call("if_i64", List.of(
                        new Reference(returnAmountIsNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(returnAmountIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(returnLossIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(returnLossIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(returnLoss, new Call("if_i64", List.of(
                        new Reference(returnLossIsNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(returnLossIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(profit, new Call("subtract", List.of(
                        new Reference(new Input(profitIndex), Stream.VALUES),
                        new Reference(returnLoss, Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(idIndex), Stream.VALUES),
                new Reference(new Input(salesIndex), Stream.VALUES),
                new Reference(returnAmount, Stream.VALUES),
                new Reference(profit, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectQuery80BranchOutput(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, String channelName, String idPrefix)
    {
        Variable channel = new Variable(0);
        Variable prefix = new Variable(1);
        Variable id = new Variable(2);

        List<Assignment> assignments = List.of(
                new Assignment(channel, new Literal(channelName), AllMask.ALL),
                new Assignment(prefix, new Literal(idPrefix), AllMask.ALL),
                new Assignment(id, new Call("concat_utf8", List.of(
                        new Reference(prefix, Stream.VALUES),
                        new Reference(new Input(0), Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(channel, Stream.VALUES),
                new Reference(id, Stream.VALUES),
                new Reference(new Input(1), Stream.VALUES),
                new Reference(new Input(2), Stream.VALUES),
                new Reference(new Input(3), Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator query90Count(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, Operator timeKeys, Operator householdKeys, Operator pageKeys)
    {
        Operator filtered = factScan(allocator, tables, "web_sales", "ws_sold_time_sk", "ws_ship_hdemo_sk", "ws_web_page_sk");
        Operator timeJoined = new HashJoinOperator(allocator, filtered, 0, timeKeys, 0);
        Operator householdJoined = new HashJoinOperator(allocator, timeJoined, 1, householdKeys, 0);
        Operator pageJoined = new HashJoinOperator(allocator, householdJoined, 2, pageKeys, 0);
        return new AggregationOperator(allocator, List.of(new CountAll()), pageJoined);
    }

    private static Operator projectQuery90Ratio(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int morningCountIndex, int eveningCountIndex)
    {
        Variable ratio = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(ratio, new Call("divide_i64_to_f64", List.of(
                                new Reference(new Input(morningCountIndex), Stream.VALUES),
                                new Reference(new Input(eveningCountIndex), Stream.VALUES))), AllMask.ALL)),
                        List.of(new Reference(ratio, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectJoinedShippingNames(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int firstNameIndex, int secondNameIndex, int thirdNameIndex, int firstCountIndex, int secondCountIndex, int thirdCountIndex, int fourthCountIndex, int fifthCountIndex)
    {
        Variable alwaysTrue = new Variable(0);
        Variable firstName = new Variable(1);
        Variable secondName = new Variable(2);
        Variable thirdName = new Variable(3);

        List<Assignment> assignments = List.of(
                new Assignment(alwaysTrue, new Literal(true), AllMask.ALL),
                new Assignment(firstName, new Call("if_utf8", List.of(
                        new Reference(alwaysTrue, Stream.VALUES),
                        new Reference(new Input(firstNameIndex), Stream.VALUES),
                        new Reference(new Input(firstNameIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(secondName, new Call("if_utf8", List.of(
                        new Reference(alwaysTrue, Stream.VALUES),
                        new Reference(new Input(secondNameIndex), Stream.VALUES),
                        new Reference(new Input(secondNameIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(thirdName, new Call("if_utf8", List.of(
                        new Reference(alwaysTrue, Stream.VALUES),
                        new Reference(new Input(thirdNameIndex), Stream.VALUES),
                        new Reference(new Input(thirdNameIndex), Stream.VALUES))), AllMask.ALL));

        List<Reference> outputs = List.of(
                new Reference(firstName, Stream.VALUES),
                new Reference(secondName, Stream.VALUES),
                new Reference(thirdName, Stream.VALUES),
                new Reference(new Input(firstCountIndex), Stream.VALUES),
                new Reference(new Input(secondCountIndex), Stream.VALUES),
                new Reference(new Input(thirdCountIndex), Stream.VALUES),
                new Reference(new Input(fourthCountIndex), Stream.VALUES),
                new Reference(new Input(fifthCountIndex), Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static FilterSpec query41EligibilityPredicate(int categoryIndex, int colorIndex, int unitsIndex, int sizeIndex)
    {
        java.util.Iterator<FilterSpec> iterator = query41EligibilityBranches(categoryIndex, colorIndex, unitsIndex, sizeIndex).values().iterator();
        FilterSpec result = iterator.next();
        while (iterator.hasNext()) {
            result = or(result, iterator.next());
        }
        return result;
    }

    private static Map<String, FilterSpec> query41EligibilityBranches(int categoryIndex, int colorIndex, int unitsIndex, int sizeIndex)
    {
        FilterSpec womenPowderKhaki = and(
                equalUtf8(categoryIndex, "Women"),
                or(equalUtf8(colorIndex, "powder"), equalUtf8(colorIndex, "khaki")),
                or(equalUtf8(unitsIndex, "Ounce"), equalUtf8(unitsIndex, "Oz")),
                or(equalUtf8(sizeIndex, "medium"), equalUtf8(sizeIndex, "extra large")));
        FilterSpec womenBrownHoneydew = and(
                equalUtf8(categoryIndex, "Women"),
                or(equalUtf8(colorIndex, "brown"), equalUtf8(colorIndex, "honeydew")),
                or(equalUtf8(unitsIndex, "Bunch"), equalUtf8(unitsIndex, "Ton")),
                or(equalUtf8(sizeIndex, "N/A"), equalUtf8(sizeIndex, "small")));
        FilterSpec menFloralDeep = and(
                equalUtf8(categoryIndex, "Men"),
                or(equalUtf8(colorIndex, "floral"), equalUtf8(colorIndex, "deep")),
                or(equalUtf8(unitsIndex, "N/A"), equalUtf8(unitsIndex, "Dozen")),
                or(equalUtf8(sizeIndex, "petite"), equalUtf8(sizeIndex, "large")));
        FilterSpec menLightCornflower = and(
                equalUtf8(categoryIndex, "Men"),
                or(equalUtf8(colorIndex, "light"), equalUtf8(colorIndex, "cornflower")),
                or(equalUtf8(unitsIndex, "Box"), equalUtf8(unitsIndex, "Pound")),
                or(equalUtf8(sizeIndex, "medium"), equalUtf8(sizeIndex, "extra large")));
        FilterSpec womenMidnightSnow = and(
                equalUtf8(categoryIndex, "Women"),
                or(equalUtf8(colorIndex, "midnight"), equalUtf8(colorIndex, "snow")),
                or(equalUtf8(unitsIndex, "Pallet"), equalUtf8(unitsIndex, "Gross")),
                or(equalUtf8(sizeIndex, "medium"), equalUtf8(sizeIndex, "extra large")));
        FilterSpec womenCyanPapaya = and(
                equalUtf8(categoryIndex, "Women"),
                or(equalUtf8(colorIndex, "cyan"), equalUtf8(colorIndex, "papaya")),
                or(equalUtf8(unitsIndex, "Cup"), equalUtf8(unitsIndex, "Dram")),
                or(equalUtf8(sizeIndex, "N/A"), equalUtf8(sizeIndex, "small")));
        FilterSpec menOrangeFrosted = and(
                equalUtf8(categoryIndex, "Men"),
                or(equalUtf8(colorIndex, "orange"), equalUtf8(colorIndex, "frosted")),
                or(equalUtf8(unitsIndex, "Each"), equalUtf8(unitsIndex, "Tbl")),
                or(equalUtf8(sizeIndex, "petite"), equalUtf8(sizeIndex, "large")));
        FilterSpec menForestGhost = and(
                equalUtf8(categoryIndex, "Men"),
                or(equalUtf8(colorIndex, "forest"), equalUtf8(colorIndex, "ghost")),
                or(equalUtf8(unitsIndex, "Lb"), equalUtf8(unitsIndex, "Bundle")),
                or(equalUtf8(sizeIndex, "medium"), equalUtf8(sizeIndex, "extra large")));

        Map<String, FilterSpec> branches = new java.util.LinkedHashMap<>();
        branches.put("womenPowderKhaki", womenPowderKhaki);
        branches.put("womenBrownHoneydew", womenBrownHoneydew);
        branches.put("menFloralDeep", menFloralDeep);
        branches.put("menLightCornflower", menLightCornflower);
        branches.put("womenMidnightSnow", womenMidnightSnow);
        branches.put("womenCyanPapaya", womenCyanPapaya);
        branches.put("menOrangeFrosted", menOrangeFrosted);
        branches.put("menForestGhost", menForestGhost);
        return branches;
    }

    private static FilterSpec yearMonthRangePredicate(int yearIndex, int monthIndex, int year, int minimumMonthInclusive, int maximumMonthInclusive)
    {
        return and(
                equal(yearIndex, year),
                greaterThan(monthIndex, minimumMonthInclusive - 1L),
                lessThan(monthIndex, maximumMonthInclusive + 1L));
    }

    private static FilterSpec yearQuarterRangePredicate(int yearIndex, int quarterIndex, int year, int minimumQuarterInclusive, int maximumQuarterInclusive)
    {
        return and(
                equal(yearIndex, year),
                greaterThan(quarterIndex, minimumQuarterInclusive - 1L),
                lessThan(quarterIndex, maximumQuarterInclusive + 1L));
    }

    private static FilterSpec dayOfMonthAndYearsPredicate(int dayIndex, int yearIndex, int minimumDayInclusive, int maximumDayInclusive, int... years)
    {
        FilterSpec allowedYears = equal(yearIndex, years[0]);
        for (int index = 1; index < years.length; index++) {
            allowedYears = or(allowedYears, equal(yearIndex, years[index]));
        }
        return and(
                greaterThan(dayIndex, minimumDayInclusive - 1L),
                lessThan(dayIndex, maximumDayInclusive + 1L),
                allowedYears);
    }

    private static FilterSpec query96TimePredicate()
    {
        return and(equal(1, 20), greaterThan(2, 29));
    }

    private static FilterSpec query80DatePredicate()
    {
        return and(
                greaterThan(1, 11_191),
                lessThan(1, 11_223));
    }

    private static FilterSpec query92DatePredicate()
    {
        return and(
                greaterThan(1, 10_982),
                lessThan(1, 11_074));
    }

    private static FilterSpec query12DatePredicate()
    {
        return and(
                greaterThan(1, 10_643),
                lessThan(1, 10_675));
    }

    private static FilterSpec query23YearRangePredicate()
    {
        return and(
                greaterThan(1, 1999),
                lessThan(1, 2004));
    }

    private static FilterSpec query14YearRangePredicate()
    {
        return and(
                greaterThan(1, 1998),
                lessThan(1, 2002));
    }

    private static FilterSpec query09QuantityPredicate(int minimumQuantityInclusive, int maximumQuantityInclusive)
    {
        return and(
                greaterThan(0, minimumQuantityInclusive - 1L),
                lessThan(0, maximumQuantityInclusive + 1L));
    }

    private static FilterSpec query81ReturnThresholdPredicate(int returnSumIndex, int stateAverageIndex)
    {
        Variable twelve = new Variable(0);
        Variable ten = new Variable(1);
        Variable scaledReturn = new Variable(2);
        Variable scaledAverage = new Variable(3);
        Variable greaterThan = new Variable(4);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(twelve, new Literal(12L), AllMask.ALL),
                new Assignment(ten, new Literal(10L), AllMask.ALL),
                new Assignment(scaledReturn, new Call("multiply", List.of(
                        new Reference(new Input(returnSumIndex), Stream.VALUES),
                        new Reference(ten, Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledAverage, new Call("multiply", List.of(
                        new Reference(new Input(stateAverageIndex), Stream.VALUES),
                        new Reference(twelve, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThan, new Call("lt", List.of(
                        new Reference(scaledAverage, Stream.VALUES),
                        new Reference(scaledReturn, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(returnSumIndex),
                isNotNullI64(stateAverageIndex),
                new FilterSpec(plan, new ReferenceMask(new Reference(greaterThan, Stream.VALUES))));
    }

    private static FilterSpec query72Predicate(int inventoryQuantityIndex, int salesQuantityIndex, int shipDateIndex, int soldDateIndex)
    {
        Variable five = new Variable(0);
        Variable quantitySatisfied = new Variable(1);
        Variable shipBoundary = new Variable(2);
        Variable shipSatisfied = new Variable(3);
        Variable accepted = new Variable(4);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(five, new Literal(5L), AllMask.ALL),
                new Assignment(quantitySatisfied, new Call("lt", List.of(
                        new Reference(new Input(inventoryQuantityIndex), Stream.VALUES),
                        new Reference(new Input(salesQuantityIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(shipBoundary, new Call("add", List.of(
                        new Reference(new Input(soldDateIndex), Stream.VALUES),
                        new Reference(five, Stream.VALUES))), AllMask.ALL),
                new Assignment(shipSatisfied, new Call("lt", List.of(
                        new Reference(shipBoundary, Stream.VALUES),
                        new Reference(new Input(shipDateIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(accepted, new Call("and", List.of(
                        new Reference(quantitySatisfied, Stream.VALUES),
                        new Reference(shipSatisfied, Stream.VALUES))), AllMask.ALL)),
                List.of());
        return new FilterSpec(
                plan,
                andMask(quantitySatisfied, shipSatisfied),
                new Reference(accepted, Stream.VALUES));
    }

    private static FilterSpec query92DiscountThresholdPredicate(int discountIndex, int averageIndex)
    {
        Variable thirteen = new Variable(0);
        Variable ten = new Variable(1);
        Variable scaledDiscount = new Variable(2);
        Variable scaledAverage = new Variable(3);
        Variable greaterThan = new Variable(4);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(thirteen, new Literal(13L), AllMask.ALL),
                new Assignment(ten, new Literal(10L), AllMask.ALL),
                new Assignment(scaledDiscount, new Call("multiply", List.of(
                        new Reference(new Input(discountIndex), Stream.VALUES),
                        new Reference(ten, Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledAverage, new Call("multiply", List.of(
                        new Reference(new Input(averageIndex), Stream.VALUES),
                        new Reference(thirteen, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThan, new Call("lt", List.of(
                        new Reference(scaledAverage, Stream.VALUES),
                        new Reference(scaledDiscount, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(discountIndex),
                isNotNullI64(averageIndex),
                new FilterSpec(plan, new ReferenceMask(new Reference(greaterThan, Stream.VALUES))));
    }

    private static FilterSpec query23BestCustomerPredicate(int salesIndex, int maxIndex)
    {
        Variable twenty = new Variable(0);
        Variable nineteen = new Variable(1);
        Variable scaledSales = new Variable(2);
        Variable scaledMax = new Variable(3);
        Variable greaterThan = new Variable(4);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(twenty, new Literal(20L), AllMask.ALL),
                new Assignment(nineteen, new Literal(19L), AllMask.ALL),
                new Assignment(scaledSales, new Call("multiply", List.of(
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(twenty, Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledMax, new Call("multiply", List.of(
                        new Reference(new Input(maxIndex), Stream.VALUES),
                        new Reference(nineteen, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThan, new Call("lt", List.of(
                        new Reference(scaledMax, Stream.VALUES),
                        new Reference(scaledSales, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(salesIndex),
                isNotNullI64(maxIndex),
                new FilterSpec(plan, new ReferenceMask(new Reference(greaterThan, Stream.VALUES))));
    }

    private static FilterSpec query14ThresholdPredicate(int salesIndex, int averageIndex)
    {
        Variable greaterThan = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(greaterThan, new Call("lt", List.of(
                        new Reference(new Input(averageIndex), Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(salesIndex),
                isNotNullI64(averageIndex),
                new FilterSpec(plan, new ReferenceMask(new Reference(greaterThan, Stream.VALUES))));
    }

    private static FilterSpec query13DemographicsPredicate(int maritalStatusIndex, int educationStatusIndex, int salesPriceIndex, int depCountIndex)
    {
        return or(
                and(
                        equalUtf8(maritalStatusIndex, "M"),
                        equalUtf8(educationStatusIndex, "Advanced Degree"),
                        betweenInclusive(salesPriceIndex, 10_000, 15_000),
                        equal(depCountIndex, 3)),
                and(
                        equalUtf8(maritalStatusIndex, "S"),
                        equalUtf8(educationStatusIndex, "College"),
                        betweenInclusive(salesPriceIndex, 5_000, 10_000),
                        equal(depCountIndex, 1)),
                and(
                        equalUtf8(maritalStatusIndex, "W"),
                        equalUtf8(educationStatusIndex, "2 yr Degree"),
                        betweenInclusive(salesPriceIndex, 15_000, 20_000),
                        equal(depCountIndex, 1)));
    }

    private static FilterSpec query13StateProfitPredicate(int stateIndex, int netProfitIndex)
    {
        return or(
                and(
                        utf8AnyOf(stateIndex, Set.of("TX", "OH")),
                        betweenInclusive(netProfitIndex, 10_000, 20_000)),
                and(
                        utf8AnyOf(stateIndex, Set.of("OR", "NM", "KY")),
                        betweenInclusive(netProfitIndex, 15_000, 30_000)),
                and(
                        utf8AnyOf(stateIndex, Set.of("VA", "TX", "MS")),
                        betweenInclusive(netProfitIndex, 5_000, 25_000)));
    }

    private static FilterSpec query48DemographicsPredicate(int maritalStatusIndex, int educationStatusIndex, int salesPriceIndex)
    {
        return or(
                and(
                        equalUtf8(maritalStatusIndex, "M"),
                        equalUtf8(educationStatusIndex, "4 yr Degree"),
                        betweenInclusive(salesPriceIndex, 10_000, 15_000)),
                and(
                        equalUtf8(maritalStatusIndex, "D"),
                        equalUtf8(educationStatusIndex, "2 yr Degree"),
                        betweenInclusive(salesPriceIndex, 5_000, 10_000)),
                and(
                        equalUtf8(maritalStatusIndex, "S"),
                        equalUtf8(educationStatusIndex, "College"),
                        betweenInclusive(salesPriceIndex, 15_000, 20_000)));
    }

    private static FilterSpec query48StateProfitPredicate(int stateIndex, int netProfitIndex)
    {
        return or(
                and(
                        utf8AnyOf(stateIndex, Set.of("CO", "OH", "TX")),
                        betweenInclusive(netProfitIndex, 0, 200_000)),
                and(
                        utf8AnyOf(stateIndex, Set.of("OR", "MN", "KY")),
                        betweenInclusive(netProfitIndex, 15_000, 300_000)),
                and(
                        utf8AnyOf(stateIndex, Set.of("VA", "CA", "MS")),
                        betweenInclusive(netProfitIndex, 5_000, 2_500_000)));
    }

    private static FilterSpec query08DatePredicate()
    {
        return and(equal(1, 2), equal(2, 1998));
    }

    private static FilterSpec query08ZipPrefixListPredicate(int prefixIndex)
    {
        List<Assignment> assignments = new ArrayList<>();
        int nextVariable = 0;
        List<Reference> arguments = new ArrayList<>(TpcdsQueryLiterals.QUERY08_ZIP_VALUES.size() + 1);
        arguments.add(new Reference(new Input(prefixIndex), Stream.VALUES));
        for (String zipValue : TpcdsQueryLiterals.QUERY08_ZIP_VALUES) {
            Variable literal = new Variable(nextVariable++);
            assignments.add(new Assignment(literal, new Literal(zipValue), AllMask.ALL));
            arguments.add(new Reference(literal, Stream.VALUES));
        }

        Variable matches = new Variable(nextVariable);
        assignments.add(new Assignment(matches, new Call("in_utf8", arguments), AllMask.ALL));
        return new FilterSpec(new EvaluationPlan(assignments, List.of()), new ReferenceMask(new Reference(matches, Stream.VALUES)));
    }

    private static FilterSpec query24ThresholdPredicate(int salesIndex, int totalSumIndex, int countIndex)
    {
        Variable twenty = new Variable(0);
        Variable countTimesSales = new Variable(1);
        Variable scaledSales = new Variable(2);
        Variable greaterThan = new Variable(3);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(twenty, new Literal(20L), AllMask.ALL),
                new Assignment(countTimesSales, new Call("multiply", List.of(
                        new Reference(new Input(countIndex), Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledSales, new Call("multiply", List.of(
                        new Reference(countTimesSales, Stream.VALUES),
                        new Reference(twenty, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThan, new Call("lt", List.of(
                        new Reference(new Input(totalSumIndex), Stream.VALUES),
                        new Reference(scaledSales, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(salesIndex),
                isNotNullI64(totalSumIndex),
                isNotNullI64(countIndex),
                new FilterSpec(plan, new ReferenceMask(new Reference(greaterThan, Stream.VALUES))));
    }

    private static FilterSpec query65ThresholdPredicate(int salesIndex, int averageIndex)
    {
        Variable ten = new Variable(0);
        Variable scaledSales = new Variable(1);
        Variable lessThan = new Variable(2);
        Variable equal = new Variable(3);
        Variable accepted = new Variable(4);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(ten, new Literal(10L), AllMask.ALL),
                new Assignment(scaledSales, new Call("multiply", List.of(
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(ten, Stream.VALUES))), AllMask.ALL),
                new Assignment(lessThan, new Call("lt", List.of(
                        new Reference(scaledSales, Stream.VALUES),
                        new Reference(new Input(averageIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(equal, new Call("eq", List.of(
                        new Reference(scaledSales, Stream.VALUES),
                        new Reference(new Input(averageIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(accepted, new Call("or", List.of(
                        new Reference(lessThan, Stream.VALUES),
                        new Reference(equal, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(salesIndex),
                isNotNullI64(averageIndex),
                new FilterSpec(
                        plan,
                        orMask(lessThan, equal),
                        new Reference(accepted, Stream.VALUES)));
    }

    private static FilterSpec query31GrowthPredicate(int storeQuarterOneIndex, int storeQuarterTwoIndex, int storeQuarterThreeIndex, int webQuarterOneIndex, int webQuarterTwoIndex, int webQuarterThreeIndex)
    {
        Variable firstComparisonLeft = new Variable(0);
        Variable firstComparisonRight = new Variable(1);
        Variable firstGrowthAccepted = new Variable(2);
        Variable secondComparisonLeft = new Variable(3);
        Variable secondComparisonRight = new Variable(4);
        Variable secondGrowthAccepted = new Variable(5);
        Variable accepted = new Variable(6);

        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(firstComparisonLeft, new Call("multiply", List.of(
                        new Reference(new Input(webQuarterTwoIndex), Stream.VALUES),
                        new Reference(new Input(storeQuarterOneIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(firstComparisonRight, new Call("multiply", List.of(
                        new Reference(new Input(storeQuarterTwoIndex), Stream.VALUES),
                        new Reference(new Input(webQuarterOneIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(firstGrowthAccepted, new Call("lt", List.of(
                        new Reference(firstComparisonRight, Stream.VALUES),
                        new Reference(firstComparisonLeft, Stream.VALUES))), AllMask.ALL),
                new Assignment(secondComparisonLeft, new Call("multiply", List.of(
                        new Reference(new Input(webQuarterThreeIndex), Stream.VALUES),
                        new Reference(new Input(storeQuarterTwoIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(secondComparisonRight, new Call("multiply", List.of(
                        new Reference(new Input(storeQuarterThreeIndex), Stream.VALUES),
                        new Reference(new Input(webQuarterTwoIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(secondGrowthAccepted, new Call("lt", List.of(
                        new Reference(secondComparisonRight, Stream.VALUES),
                        new Reference(secondComparisonLeft, Stream.VALUES))), AllMask.ALL),
                new Assignment(accepted, new Call("and", List.of(
                        new Reference(firstGrowthAccepted, Stream.VALUES),
                        new Reference(secondGrowthAccepted, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(storeQuarterOneIndex),
                isNotNullI64(storeQuarterTwoIndex),
                isNotNullI64(storeQuarterThreeIndex),
                isNotNullI64(webQuarterOneIndex),
                isNotNullI64(webQuarterTwoIndex),
                isNotNullI64(webQuarterThreeIndex),
                greaterThan(storeQuarterOneIndex, 0),
                greaterThan(storeQuarterTwoIndex, 0),
                greaterThan(webQuarterOneIndex, 0),
                greaterThan(webQuarterTwoIndex, 0),
                new FilterSpec(
                        plan,
                        andMask(firstGrowthAccepted, secondGrowthAccepted),
                        new Reference(accepted, Stream.VALUES)));
    }

    private static FilterSpec query11GrowthPredicate(int storeFirstYearIndex, int storeSecondYearIndex, int webFirstYearIndex, int webSecondYearIndex)
    {
        Variable zero = new Variable(0);
        Variable storePositive = new Variable(1);
        Variable webPositive = new Variable(2);
        Variable firstComparisonLeft = new Variable(3);
        Variable firstComparisonRight = new Variable(4);
        Variable webGrowthAccepted = new Variable(5);
        Variable accepted = new Variable(6);

        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(storePositive, new Call("lt", List.of(
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(storeFirstYearIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(webPositive, new Call("lt", List.of(
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(webFirstYearIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(firstComparisonLeft, new Call("multiply", List.of(
                        new Reference(new Input(webSecondYearIndex), Stream.VALUES),
                        new Reference(new Input(storeFirstYearIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(firstComparisonRight, new Call("multiply", List.of(
                        new Reference(new Input(storeSecondYearIndex), Stream.VALUES),
                        new Reference(new Input(webFirstYearIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(webGrowthAccepted, new Call("lt", List.of(
                        new Reference(firstComparisonRight, Stream.VALUES),
                        new Reference(firstComparisonLeft, Stream.VALUES))), AllMask.ALL),
                new Assignment(accepted, new Call("and", List.of(
                        new Reference(storePositive, Stream.VALUES),
                        new Reference(webPositive, Stream.VALUES),
                        new Reference(webGrowthAccepted, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(storeFirstYearIndex),
                isNotNullI64(storeSecondYearIndex),
                isNotNullI64(webFirstYearIndex),
                isNotNullI64(webSecondYearIndex),
                new FilterSpec(
                        plan,
                        andMask(storePositive, webPositive, webGrowthAccepted),
                        new Reference(accepted, Stream.VALUES)));
    }

    private static FilterSpec query04GrowthPredicate(int storeFirstYearIndex, int storeSecondYearIndex, int catalogFirstYearIndex, int catalogSecondYearIndex, int webFirstYearIndex, int webSecondYearIndex)
    {
        Variable zero = new Variable(0);
        Variable storePositive = new Variable(1);
        Variable catalogPositive = new Variable(2);
        Variable webPositive = new Variable(3);
        Variable catalogVsStoreLeft = new Variable(4);
        Variable catalogVsStoreRight = new Variable(5);
        Variable catalogVsWebLeft = new Variable(6);
        Variable catalogVsWebRight = new Variable(7);
        Variable catalogBeatsStore = new Variable(8);
        Variable catalogBeatsWeb = new Variable(9);
        Variable accepted = new Variable(10);

        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(storePositive, new Call("lt", List.of(
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(storeFirstYearIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogPositive, new Call("lt", List.of(
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(catalogFirstYearIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(webPositive, new Call("lt", List.of(
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(webFirstYearIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogVsStoreLeft, new Call("multiply", List.of(
                        new Reference(new Input(catalogSecondYearIndex), Stream.VALUES),
                        new Reference(new Input(storeFirstYearIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogVsStoreRight, new Call("multiply", List.of(
                        new Reference(new Input(storeSecondYearIndex), Stream.VALUES),
                        new Reference(new Input(catalogFirstYearIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogVsWebLeft, new Call("multiply", List.of(
                        new Reference(new Input(catalogSecondYearIndex), Stream.VALUES),
                        new Reference(new Input(webFirstYearIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogVsWebRight, new Call("multiply", List.of(
                        new Reference(new Input(webSecondYearIndex), Stream.VALUES),
                        new Reference(new Input(catalogFirstYearIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogBeatsStore, new Call("lt", List.of(
                        new Reference(catalogVsStoreRight, Stream.VALUES),
                        new Reference(catalogVsStoreLeft, Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogBeatsWeb, new Call("lt", List.of(
                        new Reference(catalogVsWebRight, Stream.VALUES),
                        new Reference(catalogVsWebLeft, Stream.VALUES))), AllMask.ALL),
                new Assignment(accepted, new Call("and", List.of(
                        new Reference(storePositive, Stream.VALUES),
                        new Reference(catalogPositive, Stream.VALUES),
                        new Reference(webPositive, Stream.VALUES),
                        new Reference(catalogBeatsStore, Stream.VALUES),
                        new Reference(catalogBeatsWeb, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(storeFirstYearIndex),
                isNotNullI64(storeSecondYearIndex),
                isNotNullI64(catalogFirstYearIndex),
                isNotNullI64(catalogSecondYearIndex),
                isNotNullI64(webFirstYearIndex),
                isNotNullI64(webSecondYearIndex),
                new FilterSpec(
                        plan,
                        andMask(storePositive, catalogPositive, webPositive, catalogBeatsStore, catalogBeatsWeb),
                        new Reference(accepted, Stream.VALUES)));
    }

    private static FilterSpec query21InventoryRatioPredicate(int beforeIndex, int afterIndex)
    {
        Variable two = new Variable(0);
        Variable three = new Variable(1);
        Variable afterTimesThree = new Variable(2);
        Variable beforeTimesTwo = new Variable(3);
        Variable afterTimesTwo = new Variable(4);
        Variable beforeTimesThree = new Variable(5);
        Variable lowerBoundMissing = new Variable(6);
        Variable upperBoundMissing = new Variable(7);
        Variable lowerBoundSatisfied = new Variable(8);
        Variable upperBoundSatisfied = new Variable(9);
        Variable accepted = new Variable(10);

        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(two, new Literal(2L), AllMask.ALL),
                new Assignment(three, new Literal(3L), AllMask.ALL),
                new Assignment(afterTimesThree, new Call("multiply", List.of(
                        new Reference(new Input(afterIndex), Stream.VALUES),
                        new Reference(three, Stream.VALUES))), AllMask.ALL),
                new Assignment(beforeTimesTwo, new Call("multiply", List.of(
                        new Reference(new Input(beforeIndex), Stream.VALUES),
                        new Reference(two, Stream.VALUES))), AllMask.ALL),
                new Assignment(afterTimesTwo, new Call("multiply", List.of(
                        new Reference(new Input(afterIndex), Stream.VALUES),
                        new Reference(two, Stream.VALUES))), AllMask.ALL),
                new Assignment(beforeTimesThree, new Call("multiply", List.of(
                        new Reference(new Input(beforeIndex), Stream.VALUES),
                        new Reference(three, Stream.VALUES))), AllMask.ALL),
                new Assignment(lowerBoundMissing, new Call("lt", List.of(
                        new Reference(afterTimesThree, Stream.VALUES),
                        new Reference(beforeTimesTwo, Stream.VALUES))), AllMask.ALL),
                new Assignment(upperBoundMissing, new Call("lt", List.of(
                        new Reference(beforeTimesThree, Stream.VALUES),
                        new Reference(afterTimesTwo, Stream.VALUES))), AllMask.ALL),
                new Assignment(lowerBoundSatisfied, new Call("not", List.of(
                        new Reference(lowerBoundMissing, Stream.VALUES))), AllMask.ALL),
                new Assignment(upperBoundSatisfied, new Call("not", List.of(
                        new Reference(upperBoundMissing, Stream.VALUES))), AllMask.ALL),
                new Assignment(accepted, new Call("and", List.of(
                        new Reference(lowerBoundSatisfied, Stream.VALUES),
                        new Reference(upperBoundSatisfied, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(beforeIndex),
                isNotNullI64(afterIndex),
                greaterThan(beforeIndex, 0),
                new FilterSpec(
                        plan,
                        new AndMask(List.of(notMask(lowerBoundMissing), notMask(upperBoundMissing))),
                        new Reference(accepted, Stream.VALUES)));
    }

    private static FilterSpec query46DatePredicate()
    {
        return and(
                or(equal(1, 6), equal(1, 0)),
                or(equal(2, 1999), equal(2, 2000), equal(2, 2001)));
    }

    private static FilterSpec query79DatePredicate()
    {
        return and(
                equal(1, 1),
                or(equal(2, 1999), equal(2, 2000), equal(2, 2001)));
    }

    private static FilterSpec query91CustomerDemographicsPredicate()
    {
        FilterSpec marriedUnknown = and(equalUtf8(1, "M"), equalUtf8(2, "Unknown"));
        FilterSpec widowedAdvancedDegree = and(equalUtf8(1, "W"), equalUtf8(2, "Advanced Degree"));
        return or(marriedUnknown, widowedAdvancedDegree);
    }

    private static FilterSpec query46HouseholdPredicate()
    {
        return or(equal(1, 4), equal(2, 3));
    }

    private static FilterSpec query79HouseholdPredicate()
    {
        return or(equal(1, 6), greaterThan(2, 2));
    }

    private static FilterSpec query34DatePredicate()
    {
        return and(
                or(
                        and(greaterThan(1, 0), lessThan(1, 4)),
                        and(greaterThan(1, 24), lessThan(1, 29))),
                or(equal(2, 1999), equal(2, 2000), equal(2, 2001)));
    }

    private static FilterSpec query68DatePredicate()
    {
        return and(
                greaterThan(1, 0),
                lessThan(1, 3),
                or(equal(2, 1999), equal(2, 2000), equal(2, 2001)));
    }

    private static FilterSpec query34HouseholdPredicate()
    {
        Variable ten = new Variable(0);
        Variable twelve = new Variable(1);
        Variable scaledDependents = new Variable(2);
        Variable scaledVehicles = new Variable(3);
        Variable ratioSatisfied = new Variable(4);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(ten, new Literal(10L), AllMask.ALL),
                new Assignment(twelve, new Literal(12L), AllMask.ALL),
                new Assignment(scaledDependents, new Call("multiply", List.of(
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(ten, Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledVehicles, new Call("multiply", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(twelve, Stream.VALUES))), AllMask.ALL),
                new Assignment(ratioSatisfied, new Call("lt", List.of(
                        new Reference(scaledVehicles, Stream.VALUES),
                        new Reference(scaledDependents, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                utf8AnyOf(1, Set.of(">10000", "Unknown")),
                greaterThan(2, 0),
                new FilterSpec(plan, new ReferenceMask(new Reference(ratioSatisfied, Stream.VALUES))));
    }

    private static FilterSpec query34CountPredicate()
    {
        return and(greaterThan(2, 14), lessThan(2, 21));
    }

    private static FilterSpec query01ReturnThresholdPredicate(int returnSumIndex, int storeTotalSumIndex, int storeCountIndex)
    {
        Variable five = new Variable(0);
        Variable six = new Variable(1);
        Variable countTimesReturn = new Variable(2);
        Variable scaledReturn = new Variable(3);
        Variable scaledAverage = new Variable(4);
        Variable greaterThan = new Variable(5);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(five, new Literal(5L), AllMask.ALL),
                new Assignment(six, new Literal(6L), AllMask.ALL),
                new Assignment(countTimesReturn, new Call("multiply", List.of(
                        new Reference(new Input(storeCountIndex), Stream.VALUES),
                        new Reference(new Input(returnSumIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledReturn, new Call("multiply", List.of(
                        new Reference(countTimesReturn, Stream.VALUES),
                        new Reference(five, Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledAverage, new Call("multiply", List.of(
                        new Reference(new Input(storeTotalSumIndex), Stream.VALUES),
                        new Reference(six, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThan, new Call("lt", List.of(
                        new Reference(scaledAverage, Stream.VALUES),
                        new Reference(scaledReturn, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(greaterThan, Stream.VALUES)));
    }

    private static FilterSpec query06ThresholdPredicate(int priceIndex, int categorySumIndex, int categoryCountIndex)
    {
        Variable five = new Variable(0);
        Variable six = new Variable(1);
        Variable countTimesPrice = new Variable(2);
        Variable scaledPrice = new Variable(3);
        Variable scaledAverage = new Variable(4);
        Variable greaterThan = new Variable(5);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(five, new Literal(5L), AllMask.ALL),
                new Assignment(six, new Literal(6L), AllMask.ALL),
                new Assignment(countTimesPrice, new Call("multiply", List.of(
                        new Reference(new Input(categoryCountIndex), Stream.VALUES),
                        new Reference(new Input(priceIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledPrice, new Call("multiply", List.of(
                        new Reference(countTimesPrice, Stream.VALUES),
                        new Reference(five, Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledAverage, new Call("multiply", List.of(
                        new Reference(new Input(categorySumIndex), Stream.VALUES),
                        new Reference(six, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThan, new Call("lt", List.of(
                        new Reference(scaledAverage, Stream.VALUES),
                        new Reference(scaledPrice, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(priceIndex),
                isNotNullI64(categorySumIndex),
                isNotNullI64(categoryCountIndex),
                new FilterSpec(plan, new ReferenceMask(new Reference(greaterThan, Stream.VALUES))));
    }

    private static FilterSpec query73HouseholdPredicate()
    {
        Variable greaterThan = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(greaterThan, new Call("lt", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                utf8AnyOf(1, Set.of(">10000", "Unknown")),
                greaterThan(2, 0),
                new FilterSpec(plan, new ReferenceMask(new Reference(greaterThan, Stream.VALUES))));
    }

    private static FilterSpec query88HouseholdPredicate()
    {
        Variable three = new Variable(0);
        Variable limit = new Variable(1);
        Variable vehicleOk = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(three, new Literal(3L), AllMask.ALL),
                new Assignment(limit, new Call("add", List.of(
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(three, Stream.VALUES))), AllMask.ALL),
                new Assignment(vehicleOk, new Call("lt", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(limit, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                or(equal(1, 4), equal(1, 2), equal(1, 0)),
                new FilterSpec(plan, new ReferenceMask(new Reference(vehicleOk, Stream.VALUES))));
    }

    private static FilterSpec query88TimeBucketPredicate(int bucket)
    {
        return switch (bucket) {
            case 0 -> and(equal(1, 8), greaterThan(2, 29));
            case 1 -> and(equal(1, 9), lessThan(2, 30));
            case 2 -> and(equal(1, 9), greaterThan(2, 29));
            case 3 -> and(equal(1, 10), lessThan(2, 30));
            case 4 -> and(equal(1, 10), greaterThan(2, 29));
            case 5 -> and(equal(1, 11), lessThan(2, 30));
            case 6 -> and(equal(1, 11), greaterThan(2, 29));
            case 7 -> and(equal(1, 12), lessThan(2, 30));
            default -> throw new IllegalArgumentException("Unexpected Q88 bucket: " + bucket);
        };
    }

    private static Operator customersForEligibleDates(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String customerColumn, String dateColumn, FilterSpec eligibleDateFilter)
    {
        return customersForEligibleDates(allocator, primitiveRegistry, tables, salesTable, customerColumn, dateColumn, new String[] {"d_date_sk", "d_year", "d_moy", "d_qoy"}, eligibleDateFilter);
    }

    private static Operator customersForEligibleDates(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String customerColumn, String dateColumn, String[] dateColumns, FilterSpec eligibleDateFilter)
    {
        Operator sales = factScan(allocator, tables, salesTable, customerColumn, dateColumn);
        Operator eligibleDates = filteredProjectedTable(allocator, primitiveRegistry, tables, "date_dim", eligibleDateFilter, dateColumns);
        return new HashJoinOperator(allocator, sales, 1, eligibleDates, 0);
    }

    private static Operator customerKeysForEligibleDates(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String customerColumn, String dateColumn, FilterSpec eligibleDateFilter)
    {
        return customerKeysForEligibleDates(allocator, primitiveRegistry, tables, salesTable, customerColumn, dateColumn, new String[] {"d_date_sk", "d_year", "d_moy", "d_qoy"}, eligibleDateFilter);
    }

    private static Operator customerKeysForEligibleDates(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String customerColumn, String dateColumn, String[] dateColumns, FilterSpec eligibleDateFilter)
    {
        return projectInputs(
                allocator,
                primitiveRegistry,
                customersForEligibleDates(allocator, primitiveRegistry, tables, salesTable, customerColumn, dateColumn, dateColumns, eligibleDateFilter),
                0);
    }

    private static Operator query01CustomerStoreReturns(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return projectInputs(
                allocator,
                primitiveRegistry,
                query01CustomerStoreReturnsWithValueCounts(allocator, primitiveRegistry, tables),
                0,
                1,
                2);
    }

    private static Operator query01CustomerStoreReturnsWithValueCounts(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator returns = filteredProjectedScan(
                allocator,
                primitiveRegistry,
                tables,
                "store_returns",
                null,
                new String[] {"sr_customer_sk", "sr_store_sk", "sr_return_amt", "sr_returned_date_sk"});
        Operator year2000Dates = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                equal(1, 2000),
                new String[] {"d_date_sk", "d_year"},
                0);
        returns = new HashJoinOperator(allocator, returns, 3, year2000Dates, 0);
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable isNull = new Variable(2);
        Variable hasReturnValue = new Variable(3);
        returns = new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(isNull, new Call("is_null_i64", List.of(
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                        new Assignment(hasReturnValue, new Call("if_i64", List.of(
                                new Reference(isNull, Stream.VALUES),
                                new Reference(zero, Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(hasReturnValue, Stream.VALUES))),
                primitiveRegistry,
                returns);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2), new Sum(3)),
                returns);
    }

    private static Operator query01StoreTotals(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerStoreReturns = query01CustomerStoreReturnsWithValueCounts(allocator, primitiveRegistry, tables);
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable hasValueCondition = new Variable(2);
        Variable hasReturnValue = new Variable(3);
        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(hasValueCondition, new Call("lt", List.of(
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES))), AllMask.ALL),
                new Assignment(hasReturnValue, new Call("if_i64", List.of(
                        new Reference(hasValueCondition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL));
        Operator projected = new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(hasReturnValue, Stream.VALUES))),
                primitiveRegistry,
                customerStoreReturns);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new Sum(2)),
                projected);
    }

    private static Operator query51Channel(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String salesPriceColumn)
    {
        String profilePrefix = "q51." + salesTable;
        Operator facts = profiled(profilePrefix + ".scan", factScan(allocator, tables, salesTable, soldDateColumn, itemColumn, salesPriceColumn));
        facts = profiled(profilePrefix + ".filter.item", filter(allocator, primitiveRegistry, facts, isNotNullI64(1)));
        Operator allowedDates = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                and(greaterThan(2, 1199), lessThan(2, 1212)),
                new String[] {"d_date_sk", "d_date", "d_month_seq"},
                0,
                1);
        facts = profiled(profilePrefix + ".join.date", new HashJoinOperator(allocator, facts, 0, allowedDates, 0));
        Operator grouped = profiled(profilePrefix + ".group.item_date", new GroupedAggregationOperator(
                allocator,
                List.of(1, 4),
                List.of(new Sum(2)),
                facts));
        Operator running = profiled(profilePrefix + ".window.running_sum", new WindowOperator(
                allocator,
                grouped,
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(new RunningSumI64WindowFunction(2))));
        return projectInputs(allocator, primitiveRegistry, running, 0, 1, 3);
    }

    private static Operator query53QuarterlySalesByManufact(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator facts = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price");
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        query53ItemPredicate(),
                        new String[] {"i_item_sk", "i_manufact_id", "i_category", "i_class", "i_brand"},
                        0,
                        1),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(2, 1199), lessThan(2, 1212)),
                        new String[] {"d_date_sk", "d_qoy", "d_month_seq"},
                        0,
                        1),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                2,
                scannedTable(allocator, tables, "store", "s_store_sk"),
                0);
        facts = projectInputs(allocator, primitiveRegistry, facts, 5, 7, 3);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2)),
                facts);
    }

    private static Operator query89MonthlySalesByCategoryClassBrandStore(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator facts = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price");
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        query89ItemPredicate(),
                        new String[] {"i_item_sk", "i_category", "i_class", "i_brand"}),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(2, 1999),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0,
                        1),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                2,
                scannedTable(allocator, tables, "store", "s_store_sk", "s_store_name", "s_company_name"),
                0);
        facts = projectInputs(allocator, primitiveRegistry, facts, 5, 6, 7, 11, 12, 9, 3);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5),
                List.of(new Sum(6)),
                facts);
    }

    private static Operator query63MonthlySalesByManager(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator facts = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price");
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        query53ItemPredicate(),
                        new String[] {"i_item_sk", "i_manager_id", "i_category", "i_class", "i_brand"},
                        0,
                        1),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(2, 1199), lessThan(2, 1212)),
                        new String[] {"d_date_sk", "d_moy", "d_month_seq"},
                        0,
                        1),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                2,
                scannedTable(allocator, tables, "store", "s_store_sk"),
                0);
        facts = projectInputs(allocator, primitiveRegistry, facts, 5, 7, 3);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2)),
                facts);
    }

    private static Operator query54RevenueByCustomer(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator revenue = profiled("q54.scan.store_sales", factScan(allocator, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_sales_price"));
        revenue = profiled("q54.join.my_customers", new HashJoinOperator(allocator, revenue, 0, query54MyCustomers(allocator, primitiveRegistry, tables), 0));
        revenue = profiled("q54.join.customer_address", new HashJoinOperator(
                allocator,
                revenue,
                4,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_county", "ca_state"),
                0));
        revenue = profiled("q54.join.store", new HashJoinOperator(
                allocator,
                revenue,
                new int[] {6, 7},
                scannedTable(allocator, tables, "store", "s_county", "s_state"),
                new int[] {0, 1}));
        revenue = profiled("q54.join.date_dim", new HashJoinOperator(
                allocator,
                revenue,
                1,
                scannedTable(allocator, tables, "date_dim", "d_date_sk", "d_month_seq"),
                0));
        return projectInputs(allocator, primitiveRegistry, revenue, 3, 2, 11);
    }

    private static Operator query54MyCustomers(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerSales = new UnionAllOperator(3, List.of(
                profiled("q54.scan.catalog_sales", factScan(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_bill_customer_sk", "cs_item_sk")),
                profiled("q54.scan.web_sales", factScan(allocator, tables, "web_sales", "ws_sold_date_sk", "ws_bill_customer_sk", "ws_item_sk"))));
        customerSales = profiled("q54.join.item", new HashJoinOperator(
                allocator,
                customerSales,
                2,
                profiled("q54.scan.item", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        and(equalUtf8(1, "Women"), equalUtf8(2, "maternity")),
                        new String[] {"i_item_sk", "i_category", "i_class"},
                        0)),
                0));
        customerSales = profiled("q54.join.sales_date", new HashJoinOperator(
                allocator,
                customerSales,
                0,
                profiled("q54.scan.sales_date", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 12), equal(2, 1998)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0)),
                0));
        customerSales = profiled("q54.join.customer", new HashJoinOperator(
                allocator,
                customerSales,
                1,
                profiled("q54.scan.customer", scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk")),
                0));
        customerSales = projectInputs(allocator, primitiveRegistry, customerSales, 1, 6);
        return profiled("q54.distinct.customer_address", new MarkDistinctOperator(allocator, new int[] {0, 1}, customerSales, EngineResources.from(allocator).operatorResources()));
    }

    private static Operator query54ScalarMonthBoundary(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, int offset)
    {
        Operator boundary = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                and(equal(1, 1998), equal(2, 12)),
                new String[] {"d_month_seq", "d_year", "d_moy"},
                0);
        boundary = projectQuery54ScalarMonthBoundary(allocator, primitiveRegistry, boundary, offset);
        boundary = new MarkDistinctOperator(allocator, 0, boundary, EngineResources.from(allocator).operatorResources());
        return new EnforceSingleRowOperator(allocator, boundary);
    }

    private static Operator projectQuery54ScalarMonthBoundary(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int offset)
    {
        Variable offsetValue = new Variable(0);
        Variable boundary = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(offsetValue, new Literal((long) offset), AllMask.ALL),
                        new Assignment(boundary, new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(offsetValue, Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(boundary, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery77BranchOutput(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, String channelName)
    {
        Variable channel = new Variable(0);
        Variable zero = new Variable(1);
        Variable returnAmountIsNull = new Variable(2);
        Variable returnAmount = new Variable(3);
        Variable returnLossIsNull = new Variable(4);
        Variable returnLoss = new Variable(5);
        Variable profit = new Variable(6);

        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(channel, new Literal(channelName), AllMask.ALL),
                                new Assignment(zero, new Literal(0L), AllMask.ALL),
                                new Assignment(returnAmountIsNull, new Call("is_null_i64", List.of(
                                        new Reference(new Input(4), Stream.VALUES))), AllMask.ALL),
                                new Assignment(returnAmount, new Call("if_i64", List.of(
                                        new Reference(returnAmountIsNull, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES),
                                        new Reference(new Input(4), Stream.VALUES))), AllMask.ALL),
                                new Assignment(returnLossIsNull, new Call("is_null_i64", List.of(
                                        new Reference(new Input(5), Stream.VALUES))), AllMask.ALL),
                                new Assignment(returnLoss, new Call("if_i64", List.of(
                                        new Reference(returnLossIsNull, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES),
                                        new Reference(new Input(5), Stream.VALUES))), AllMask.ALL),
                                new Assignment(profit, new Call("subtract", List.of(
                                        new Reference(new Input(2), Stream.VALUES),
                                        new Reference(returnLoss, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(channel, Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(returnAmount, Stream.VALUES),
                                new Reference(profit, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static FilterSpec query54MonthBetweenPredicate(int monthSequenceIndex, int minimumIndex, int maximumIndex)
    {
        Variable greaterThanMinimum = new Variable(0);
        Variable equalsMinimum = new Variable(1);
        Variable minimumSatisfied = new Variable(2);
        Variable lessThanMaximum = new Variable(3);
        Variable equalsMaximum = new Variable(4);
        Variable maximumSatisfied = new Variable(5);
        Variable result = new Variable(6);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(greaterThanMinimum, new Call("lt", List.of(
                        new Reference(new Input(minimumIndex), Stream.VALUES),
                        new Reference(new Input(monthSequenceIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(equalsMinimum, new Call("eq", List.of(
                        new Reference(new Input(monthSequenceIndex), Stream.VALUES),
                        new Reference(new Input(minimumIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(minimumSatisfied, new Call("or", List.of(
                        new Reference(greaterThanMinimum, Stream.VALUES),
                        new Reference(equalsMinimum, Stream.VALUES))), AllMask.ALL),
                new Assignment(lessThanMaximum, new Call("lt", List.of(
                        new Reference(new Input(monthSequenceIndex), Stream.VALUES),
                        new Reference(new Input(maximumIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(equalsMaximum, new Call("eq", List.of(
                        new Reference(new Input(monthSequenceIndex), Stream.VALUES),
                        new Reference(new Input(maximumIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(maximumSatisfied, new Call("or", List.of(
                        new Reference(lessThanMaximum, Stream.VALUES),
                        new Reference(equalsMaximum, Stream.VALUES))), AllMask.ALL),
                new Assignment(result, new Call("and", List.of(
                        new Reference(maximumSatisfied, Stream.VALUES),
                        new Reference(minimumSatisfied, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(monthSequenceIndex),
                isNotNullI64(minimumIndex),
                isNotNullI64(maximumIndex),
                new FilterSpec(
                        plan,
                        new AndMask(List.of(
                                orMask(greaterThanMinimum, equalsMinimum),
                                orMask(lessThanMaximum, equalsMaximum))),
                        new Reference(result, Stream.VALUES)));
    }

    private static Operator projectQuery54Segment(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable divisor = new Variable(0);
        Variable segment = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(divisor, new Literal(5_000L), AllMask.ALL),
                        new Assignment(segment, new Call("divide", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(divisor, Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(segment, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery54Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable multiplier = new Variable(0);
        Variable segmentBase = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(multiplier, new Literal(50L), AllMask.ALL),
                        new Assignment(segmentBase, new Call("multiply", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(multiplier, Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(segmentBase, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query58Channel(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String priceColumn)
    {
        Operator facts = factScan(allocator, tables, salesTable, soldDateColumn, itemColumn, priceColumn);
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_item_id"),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                scannedTable(allocator, tables, "date_dim", "d_date_sk", "d_date"),
                0);
        facts = new HashJoinOperator(allocator, facts, 6, query58AllowedDates(allocator, primitiveRegistry, tables), 0);
        facts = projectInputs(allocator, primitiveRegistry, facts, 4, 2);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                facts);
    }

    private static Operator query83ChannelReturns(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String returnsTable, String itemColumn, String returnedDateColumn, String quantityColumn)
    {
        Operator facts = factScan(allocator, tables, returnsTable, itemColumn, returnedDateColumn, quantityColumn);
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_item_id"),
                0);
        facts = new HashJoinOperator(allocator, facts, 1, query83AllowedDates(allocator, primitiveRegistry, tables), 0);
        facts = projectInputs(allocator, primitiveRegistry, facts, 4, 2);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                facts);
    }

    private static Operator query21InventoryByWarehouseItem(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, boolean beforeCutoff)
    {
        LocalDate cutoffDate = LocalDate.of(2000, 3, 11);
        String profilePrefix = beforeCutoff ? "q21.before" : "q21.after";
        Operator inventory = profiled(profilePrefix + ".scan.inventory", scannedTable(allocator, tables, "inventory", "inv_item_sk", "inv_warehouse_sk", "inv_date_sk", "inv_quantity_on_hand"));
        HashJoinOperator itemJoin = new HashJoinOperator(
                allocator,
                inventory,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        betweenInclusive(1, 99L, 149L),
                        new String[] {"i_item_sk", "i_current_price", "i_item_id"},
                        0, 2),
                0);
        inventory = profiled(profilePrefix + ".join.item", QUERY21_COMPACT_JOIN_LAYOUTS ? itemJoin.withOutputs(1, 2, 3, 5) : itemJoin);
        HashJoinOperator warehouseJoin = new HashJoinOperator(
                allocator,
                inventory,
                QUERY21_COMPACT_JOIN_LAYOUTS ? 0 : 1,
                scannedTable(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name"),
                0);
        inventory = profiled(profilePrefix + ".join.warehouse", QUERY21_COMPACT_JOIN_LAYOUTS ? warehouseJoin.withOutputs(1, 2, 3, 5) : warehouseJoin);
        HashJoinOperator dateJoin = new HashJoinOperator(
                allocator,
                inventory,
                QUERY21_COMPACT_JOIN_LAYOUTS ? 0 : 2,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(
                                betweenInclusive(1, cutoffDate.minusDays(30).toEpochDay(), cutoffDate.plusDays(30).toEpochDay()),
                                beforeCutoff ? lessThan(1, cutoffDate.toEpochDay()) : greaterThan(1, cutoffDate.toEpochDay() - 1)),
                        new String[] {"d_date_sk", "d_date"},
                        0),
                0);
        inventory = profiled(profilePrefix + ".join.date_dim", QUERY21_COMPACT_JOIN_LAYOUTS ? dateJoin.withOutputs(1, 2, 3) : dateJoin);
        inventory = profiled(profilePrefix + ".project.inventory", projectInputs(
                allocator,
                primitiveRegistry,
                inventory,
                QUERY21_COMPACT_JOIN_LAYOUTS ? 2 : 7,
                QUERY21_COMPACT_JOIN_LAYOUTS ? 1 : 5,
                QUERY21_COMPACT_JOIN_LAYOUTS ? 0 : 3));
        return profiled(profilePrefix + ".group.inventory", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2)),
                inventory));
    }

    private static Operator query04ChannelYearTotal(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String customerColumn, String soldDateColumn, String listPriceColumn, String wholesaleCostColumn, String discountColumn, String salesPriceColumn, int year, boolean retainCustomerDetails)
    {
        Operator sales = factScan(allocator, tables, salesTable, soldDateColumn, customerColumn, discountColumn, salesPriceColumn, wholesaleCostColumn, listPriceColumn);
        sales = new HashJoinOperator(
                allocator,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_country", "c_login", "c_email_address"),
                0,
                sales,
                1).withOutputs(1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13);
        sales = new HashJoinOperator(
                allocator,
                sales,
                7,
                filteredProjectedTable(allocator, primitiveRegistry, tables, "date_dim", equal(1, year), new String[] {"d_date_sk", "d_year"}, 0, 1),
                0);
        sales = filter(allocator, primitiveRegistry, sales, and(isNotNullI64(8), isNotNullI64(9), isNotNullI64(10), isNotNullI64(11)));
        sales = projectQuery04GroupedYearTotal(allocator, primitiveRegistry, sales, new int[] {0, 1, 2, 3, 4, 5, 6, 13}, 11, 10, 8, 9);
        sales = new GroupedAggregationOperator(allocator, List.of(0, 1, 2, 3, 4, 5, 6, 7), List.of(new Sum(8)), sales);
        if (retainCustomerDetails) {
            return projectInputs(allocator, primitiveRegistry, sales, 0, 1, 2, 3, 8);
        }
        return projectInputs(allocator, primitiveRegistry, sales, 0, 8);
    }

    private static Operator projectQuery04GroupedYearTotal(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int[] groupKeyIndices, int listPriceIndex, int wholesaleCostIndex, int discountIndex, int salesPriceIndex)
    {
        Variable listMinusWholesale = new Variable(0);
        Variable grossMargin = new Variable(1);
        Variable yearTotal = new Variable(2);
        List<Reference> outputs = new ArrayList<>();
        for (int groupKeyIndex : groupKeyIndices) {
            outputs.add(new Reference(new Input(groupKeyIndex), Stream.VALUES));
        }
        outputs.add(new Reference(yearTotal, Stream.VALUES));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(listMinusWholesale, new Call("subtract", List.of(new Reference(new Input(listPriceIndex), Stream.VALUES), new Reference(new Input(wholesaleCostIndex), Stream.VALUES))), AllMask.ALL),
                                new Assignment(grossMargin, new Call("subtract", List.of(new Reference(listMinusWholesale, Stream.VALUES), new Reference(new Input(discountIndex), Stream.VALUES))), AllMask.ALL),
                                new Assignment(yearTotal, new Call("add", List.of(new Reference(grossMargin, Stream.VALUES), new Reference(new Input(salesPriceIndex), Stream.VALUES))), AllMask.ALL)),
                        List.copyOf(outputs)),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery04YearTotal(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int customerKeyIndex, int listPriceIndex, int wholesaleCostIndex, int discountIndex, int salesPriceIndex)
    {
        Variable listMinusWholesale = new Variable(0);
        Variable grossMargin = new Variable(1);
        Variable yearTotal = new Variable(2);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(listMinusWholesale, new Call("subtract", List.of(
                                        new Reference(new Input(listPriceIndex), Stream.VALUES),
                                        new Reference(new Input(wholesaleCostIndex), Stream.VALUES))), AllMask.ALL),
                                new Assignment(grossMargin, new Call("subtract", List.of(
                                        new Reference(listMinusWholesale, Stream.VALUES),
                                        new Reference(new Input(discountIndex), Stream.VALUES))), AllMask.ALL),
                                new Assignment(yearTotal, new Call("add", List.of(
                                        new Reference(grossMargin, Stream.VALUES),
                                        new Reference(new Input(salesPriceIndex), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(customerKeyIndex), Stream.VALUES),
                                new Reference(yearTotal, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery04CustomerYearTotal(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int customerIdIndex, int listPriceIndex, int wholesaleCostIndex, int discountIndex, int salesPriceIndex)
    {
        Variable listMinusWholesale = new Variable(0);
        Variable grossMargin = new Variable(1);
        Variable yearTotal = new Variable(2);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(listMinusWholesale, new Call("subtract", List.of(
                                        new Reference(new Input(listPriceIndex), Stream.VALUES),
                                        new Reference(new Input(wholesaleCostIndex), Stream.VALUES))), AllMask.ALL),
                                new Assignment(grossMargin, new Call("subtract", List.of(
                                        new Reference(listMinusWholesale, Stream.VALUES),
                                        new Reference(new Input(discountIndex), Stream.VALUES))), AllMask.ALL),
                                new Assignment(yearTotal, new Call("add", List.of(
                                        new Reference(grossMargin, Stream.VALUES),
                                        new Reference(new Input(salesPriceIndex), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(customerIdIndex), Stream.VALUES),
                                new Reference(yearTotal, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query61Sales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, boolean promotionalOnly)
    {
        Operator sales = promotionalOnly
                ? factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_promo_sk", "ss_ext_sales_price")
                : factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_ext_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                promotionalOnly ? 3 : 3,
                filteredProjectedTable(allocator, primitiveRegistry, tables, "store", equal(1, -500L), new String[] {"s_store_sk", "s_gmt_offset"}, 0),
                0);
        if (promotionalOnly) {
            sales = new HashJoinOperator(
                    allocator,
                    sales,
                    4,
                    filteredProjectedTable(
                            allocator,
                            primitiveRegistry,
                            tables,
                            "promotion",
                            or(equalUtf8(1, "Y"), equalUtf8(2, "Y"), equalUtf8(3, "Y")),
                            new String[] {"p_promo_sk", "p_channel_dmail", "p_channel_email", "p_channel_tv"},
                            0),
                    0);
        }
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 1998), equal(2, 11)),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                2,
                customerScan(allocator, tables, "c_customer_sk", "c_current_addr_sk"),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                promotionalOnly ? 10 : 8,
                filteredProjectedTable(allocator, primitiveRegistry, tables, "customer_address", equal(1, -500L), new String[] {"ca_address_sk", "ca_gmt_offset"}, 0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(allocator, primitiveRegistry, tables, "item", equalUtf8(1, "Jewelry"), new String[] {"i_item_sk", "i_category"}, 0),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, promotionalOnly ? 5 : 4);
        return new AggregationOperator(allocator, List.of(new Sum(0)), sales);
    }

    private static Operator query58AllowedDates(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator allowedDates = scannedTable(allocator, tables, "date_dim", "d_date", "d_week_seq");
        allowedDates = new NestedLoopJoinOperator(allocator, allowedDates, query58ScalarWeekSequence(allocator, primitiveRegistry, tables));
        allowedDates = filter(allocator, primitiveRegistry, allowedDates, equalColumns(1, 2));
        allowedDates = projectInputs(allocator, primitiveRegistry, allowedDates, 0);
        return new MarkDistinctOperator(allocator, 0, allowedDates, EngineResources.from(allocator).operatorResources());
    }

    private static Operator query83AllowedDates(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator targetWeekSequences = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                anyOf(
                        0,
                        LocalDate.of(2000, 6, 30).toEpochDay(),
                        LocalDate.of(2000, 9, 27).toEpochDay(),
                        LocalDate.of(2000, 11, 17).toEpochDay()),
                new String[] {"d_date", "d_week_seq"},
                1);
        targetWeekSequences = new MarkDistinctOperator(allocator, 0, targetWeekSequences, EngineResources.from(allocator).operatorResources());
        Operator allowedDates = scannedTable(allocator, tables, "date_dim", "d_date_sk", "d_week_seq");
        allowedDates = new HashJoinOperator(allocator, allowedDates, 1, targetWeekSequences, 0);
        allowedDates = projectInputs(allocator, primitiveRegistry, allowedDates, 0);
        return new MarkDistinctOperator(allocator, 0, allowedDates, EngineResources.from(allocator).operatorResources());
    }

    private static Operator query58ScalarWeekSequence(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator weekSequence = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                equal(1, 10_959L),
                new String[] {"d_week_seq", "d_date"},
                0);
        weekSequence = new MarkDistinctOperator(allocator, 0, weekSequence, EngineResources.from(allocator).operatorResources());
        return new EnforceSingleRowOperator(allocator, weekSequence);
    }

    private static FilterSpec query58SimilarityPredicate(int storeRevenueIndex, int catalogRevenueIndex, int webRevenueIndex)
    {
        return and(
                query58WithinTenPercentPredicate(storeRevenueIndex, catalogRevenueIndex),
                query58WithinTenPercentPredicate(storeRevenueIndex, webRevenueIndex),
                query58WithinTenPercentPredicate(catalogRevenueIndex, storeRevenueIndex),
                query58WithinTenPercentPredicate(catalogRevenueIndex, webRevenueIndex),
                query58WithinTenPercentPredicate(webRevenueIndex, storeRevenueIndex),
                query58WithinTenPercentPredicate(webRevenueIndex, catalogRevenueIndex));
    }

    private static FilterSpec query58WithinTenPercentPredicate(int leftIndex, int rightIndex)
    {
        Variable nine = new Variable(0);
        Variable ten = new Variable(1);
        Variable eleven = new Variable(2);
        Variable leftTimesTen = new Variable(3);
        Variable rightTimesNine = new Variable(4);
        Variable rightTimesEleven = new Variable(5);
        Variable lowerLessThan = new Variable(6);
        Variable lowerEqual = new Variable(7);
        Variable lowerSatisfied = new Variable(8);
        Variable upperLessThan = new Variable(9);
        Variable upperEqual = new Variable(10);
        Variable upperSatisfied = new Variable(11);
        Variable result = new Variable(12);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(nine, new Literal(9L), AllMask.ALL),
                new Assignment(ten, new Literal(10L), AllMask.ALL),
                new Assignment(eleven, new Literal(11L), AllMask.ALL),
                new Assignment(leftTimesTen, new Call("multiply", List.of(
                        new Reference(new Input(leftIndex), Stream.VALUES),
                        new Reference(ten, Stream.VALUES))), AllMask.ALL),
                new Assignment(rightTimesNine, new Call("multiply", List.of(
                        new Reference(new Input(rightIndex), Stream.VALUES),
                        new Reference(nine, Stream.VALUES))), AllMask.ALL),
                new Assignment(rightTimesEleven, new Call("multiply", List.of(
                        new Reference(new Input(rightIndex), Stream.VALUES),
                        new Reference(eleven, Stream.VALUES))), AllMask.ALL),
                new Assignment(lowerLessThan, new Call("lt", List.of(
                        new Reference(rightTimesNine, Stream.VALUES),
                        new Reference(leftTimesTen, Stream.VALUES))), AllMask.ALL),
                new Assignment(lowerEqual, new Call("eq", List.of(
                        new Reference(leftTimesTen, Stream.VALUES),
                        new Reference(rightTimesNine, Stream.VALUES))), AllMask.ALL),
                new Assignment(lowerSatisfied, new Call("or", List.of(
                        new Reference(lowerLessThan, Stream.VALUES),
                        new Reference(lowerEqual, Stream.VALUES))), AllMask.ALL),
                new Assignment(upperLessThan, new Call("lt", List.of(
                        new Reference(leftTimesTen, Stream.VALUES),
                        new Reference(rightTimesEleven, Stream.VALUES))), AllMask.ALL),
                new Assignment(upperEqual, new Call("eq", List.of(
                        new Reference(leftTimesTen, Stream.VALUES),
                        new Reference(rightTimesEleven, Stream.VALUES))), AllMask.ALL),
                new Assignment(upperSatisfied, new Call("or", List.of(
                        new Reference(upperLessThan, Stream.VALUES),
                        new Reference(upperEqual, Stream.VALUES))), AllMask.ALL),
                new Assignment(result, new Call("and", List.of(
                        new Reference(lowerSatisfied, Stream.VALUES),
                        new Reference(upperSatisfied, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(
                plan,
                new AndMask(List.of(
                        orMask(lowerLessThan, lowerEqual),
                        orMask(upperLessThan, upperEqual))),
                new Reference(result, Stream.VALUES));
    }

    private static Operator projectThreeChannelOutput(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, long averageScale)
    {
        Variable three = new Variable(0);
        Variable deviationScale = new Variable(1);
        Variable resultAverageScale = new Variable(13);
        Variable firstPairTotal = new Variable(2);
        Variable totalRevenue = new Variable(3);
        Variable denominator = new Variable(4);
        Variable scaledStoreRevenue = new Variable(5);
        Variable scaledCatalogRevenue = new Variable(6);
        Variable scaledWebRevenue = new Variable(7);
        Variable scaledTotalRevenue = new Variable(8);
        Variable storeDeviation = new Variable(9);
        Variable catalogDeviation = new Variable(10);
        Variable webDeviation = new Variable(11);
        Variable average = new Variable(12);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(three, new Literal(3L), AllMask.ALL),
                        new Assignment(deviationScale, new Literal(10_000L), AllMask.ALL),
                        new Assignment(resultAverageScale, new Literal(averageScale), AllMask.ALL),
                        new Assignment(firstPairTotal, new Call("add", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                        new Assignment(totalRevenue, new Call("add", List.of(
                                new Reference(firstPairTotal, Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES))), AllMask.ALL),
                        new Assignment(denominator, new Call("multiply", List.of(
                                new Reference(totalRevenue, Stream.VALUES),
                                new Reference(three, Stream.VALUES))), AllMask.ALL),
                        new Assignment(scaledStoreRevenue, new Call("multiply", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(deviationScale, Stream.VALUES))), AllMask.ALL),
                        new Assignment(scaledCatalogRevenue, new Call("multiply", List.of(
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(deviationScale, Stream.VALUES))), AllMask.ALL),
                        new Assignment(scaledWebRevenue, new Call("multiply", List.of(
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(deviationScale, Stream.VALUES))), AllMask.ALL),
                        new Assignment(scaledTotalRevenue, new Call("multiply", List.of(
                                new Reference(totalRevenue, Stream.VALUES),
                                new Reference(resultAverageScale, Stream.VALUES))), AllMask.ALL),
                        new Assignment(storeDeviation, new Call("divide_round_i64", List.of(
                                new Reference(scaledStoreRevenue, Stream.VALUES),
                                new Reference(denominator, Stream.VALUES))), AllMask.ALL),
                        new Assignment(catalogDeviation, new Call("divide_round_i64", List.of(
                                new Reference(scaledCatalogRevenue, Stream.VALUES),
                                new Reference(denominator, Stream.VALUES))), AllMask.ALL),
                        new Assignment(webDeviation, new Call("divide_round_i64", List.of(
                                new Reference(scaledWebRevenue, Stream.VALUES),
                                new Reference(denominator, Stream.VALUES))), AllMask.ALL),
                        new Assignment(average, new Call("divide_round_i64", List.of(
                                new Reference(scaledTotalRevenue, Stream.VALUES),
                                new Reference(three, Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(storeDeviation, Stream.VALUES),
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(catalogDeviation, Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(webDeviation, Stream.VALUES),
                        new Reference(average, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery61Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable scale = new Variable(0);
        Variable percent = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(scale, new Literal(100_000_000_000_000L), AllMask.ALL),
                        new Assignment(percent, new Call("divide_scale_round_i64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(scale, Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(percent, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery81StateAverage(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable average = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(average, new Call("divide_round_i64", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(average, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery13Averages(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable averageSalesPrice = new Variable(0);
        Variable averageWholesaleCost = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(averageSalesPrice, new Call("divide_round_i64", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                        new Assignment(averageWholesaleCost, new Call("divide_round_i64", List.of(
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(averageSalesPrice, Stream.VALUES),
                        new Reference(averageWholesaleCost, Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery26Averages(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable averageListPrice = new Variable(0);
        Variable averageCouponAmount = new Variable(1);
        Variable averageSalesPrice = new Variable(2);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(averageListPrice, new Call("divide_round_i64", List.of(
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES))), AllMask.ALL),
                        new Assignment(averageCouponAmount, new Call("divide_round_i64", List.of(
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES))), AllMask.ALL),
                        new Assignment(averageSalesPrice, new Call("divide_round_i64", List.of(
                                new Reference(new Input(6), Stream.VALUES),
                                new Reference(new Input(7), Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(averageListPrice, Stream.VALUES),
                        new Reference(averageCouponAmount, Stream.VALUES),
                        new Reference(averageSalesPrice, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectUtf8Prefix(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int inputIndex, int prefixLength, int... passthroughInputs)
    {
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable prefix = new Variable(2);
        List<Assignment> assignments = List.of(
                new Assignment(start, new Literal(1L), AllMask.ALL),
                new Assignment(length, new Literal((long) prefixLength), AllMask.ALL),
                new Assignment(prefix, new Call("substring_utf8", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(start, Stream.VALUES),
                        new Reference(length, Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = new ArrayList<>(passthroughInputs.length + 1);
        for (int passthroughInput : passthroughInputs) {
            outputs.add(new Reference(new Input(passthroughInput), Stream.VALUES));
        }
        outputs.add(new Reference(prefix, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectQuery09Average(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int sumIndex, int countIndex)
    {
        Variable average = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(average, new Call("divide_round_i64", List.of(
                                new Reference(new Input(sumIndex), Stream.VALUES),
                                new Reference(new Input(countIndex), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(average, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery50Buckets(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int storeNameIndex, int companyIdIndex, int streetNumberIndex, int streetNameIndex, int streetTypeIndex, int suiteNumberIndex, int cityIndex, int countyIndex, int stateIndex, int zipIndex, int returnedDateIndex, int soldDateIndex)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable thirty = new Variable(2);
        Variable thirtyOne = new Variable(3);
        Variable sixty = new Variable(4);
        Variable sixtyOne = new Variable(5);
        Variable ninety = new Variable(6);
        Variable ninetyOne = new Variable(7);
        Variable oneHundredTwenty = new Variable(8);
        Variable oneHundredTwentyOne = new Variable(9);
        Variable days = new Variable(10);
        Variable bucket30Condition = new Variable(11);
        Variable greaterThanThirty = new Variable(12);
        Variable lessThanSixtyOne = new Variable(13);
        Variable bucket31To60Condition = new Variable(14);
        Variable greaterThanSixty = new Variable(15);
        Variable lessThanNinetyOne = new Variable(16);
        Variable bucket61To90Condition = new Variable(17);
        Variable greaterThanNinety = new Variable(18);
        Variable lessThanOneHundredTwentyOne = new Variable(19);
        Variable bucket91To120Condition = new Variable(20);
        Variable greaterThanOneHundredTwenty = new Variable(21);
        Variable bucket30 = new Variable(22);
        Variable bucket31To60 = new Variable(23);
        Variable bucket61To90 = new Variable(24);
        Variable bucket91To120 = new Variable(25);
        Variable bucketOver120 = new Variable(26);
        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(thirty, new Literal(30L), AllMask.ALL),
                new Assignment(thirtyOne, new Literal(31L), AllMask.ALL),
                new Assignment(sixty, new Literal(60L), AllMask.ALL),
                new Assignment(sixtyOne, new Literal(61L), AllMask.ALL),
                new Assignment(ninety, new Literal(90L), AllMask.ALL),
                new Assignment(ninetyOne, new Literal(91L), AllMask.ALL),
                new Assignment(oneHundredTwenty, new Literal(120L), AllMask.ALL),
                new Assignment(oneHundredTwentyOne, new Literal(121L), AllMask.ALL),
                new Assignment(days, new Call("subtract", List.of(
                        new Reference(new Input(returnedDateIndex), Stream.VALUES),
                        new Reference(new Input(soldDateIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket30Condition, new Call("lt", List.of(
                        new Reference(days, Stream.VALUES),
                        new Reference(thirtyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThanThirty, new Call("lt", List.of(
                        new Reference(thirty, Stream.VALUES),
                        new Reference(days, Stream.VALUES))), AllMask.ALL),
                new Assignment(lessThanSixtyOne, new Call("lt", List.of(
                        new Reference(days, Stream.VALUES),
                        new Reference(sixtyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket31To60Condition, new Call("and", List.of(
                        new Reference(greaterThanThirty, Stream.VALUES),
                        new Reference(lessThanSixtyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThanSixty, new Call("lt", List.of(
                        new Reference(sixty, Stream.VALUES),
                        new Reference(days, Stream.VALUES))), AllMask.ALL),
                new Assignment(lessThanNinetyOne, new Call("lt", List.of(
                        new Reference(days, Stream.VALUES),
                        new Reference(ninetyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket61To90Condition, new Call("and", List.of(
                        new Reference(greaterThanSixty, Stream.VALUES),
                        new Reference(lessThanNinetyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThanNinety, new Call("lt", List.of(
                        new Reference(ninety, Stream.VALUES),
                        new Reference(days, Stream.VALUES))), AllMask.ALL),
                new Assignment(lessThanOneHundredTwentyOne, new Call("lt", List.of(
                        new Reference(days, Stream.VALUES),
                        new Reference(oneHundredTwentyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket91To120Condition, new Call("and", List.of(
                        new Reference(greaterThanNinety, Stream.VALUES),
                        new Reference(lessThanOneHundredTwentyOne, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThanOneHundredTwenty, new Call("lt", List.of(
                        new Reference(oneHundredTwenty, Stream.VALUES),
                        new Reference(days, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket30, new Call("if_i64", List.of(
                        new Reference(bucket30Condition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket31To60, new Call("if_i64", List.of(
                        new Reference(bucket31To60Condition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket61To90, new Call("if_i64", List.of(
                        new Reference(bucket61To90Condition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucket91To120, new Call("if_i64", List.of(
                        new Reference(bucket91To120Condition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(bucketOver120, new Call("if_i64", List.of(
                        new Reference(greaterThanOneHundredTwenty, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(storeNameIndex), Stream.VALUES),
                new Reference(new Input(companyIdIndex), Stream.VALUES),
                new Reference(new Input(streetNumberIndex), Stream.VALUES),
                new Reference(new Input(streetNameIndex), Stream.VALUES),
                new Reference(new Input(streetTypeIndex), Stream.VALUES),
                new Reference(new Input(suiteNumberIndex), Stream.VALUES),
                new Reference(new Input(cityIndex), Stream.VALUES),
                new Reference(new Input(countyIndex), Stream.VALUES),
                new Reference(new Input(stateIndex), Stream.VALUES),
                new Reference(new Input(zipIndex), Stream.VALUES),
                new Reference(bucket30, Stream.VALUES),
                new Reference(bucket31To60, Stream.VALUES),
                new Reference(bucket61To90, Stream.VALUES),
                new Reference(bucket91To120, Stream.VALUES),
                new Reference(bucketOver120, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectQuery43Buckets(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int storeNameIndex, int storeIdIndex, int dayNameIndex, int salesIndex)
    {
        Variable zero = new Variable(0);
        Variable sunday = new Variable(1);
        Variable monday = new Variable(2);
        Variable tuesday = new Variable(3);
        Variable wednesday = new Variable(4);
        Variable thursday = new Variable(5);
        Variable friday = new Variable(6);
        Variable saturday = new Variable(7);
        Variable sundayMatch = new Variable(8);
        Variable mondayMatch = new Variable(9);
        Variable tuesdayMatch = new Variable(10);
        Variable wednesdayMatch = new Variable(11);
        Variable thursdayMatch = new Variable(12);
        Variable fridayMatch = new Variable(13);
        Variable saturdayMatch = new Variable(14);
        Variable sundaySales = new Variable(15);
        Variable mondaySales = new Variable(16);
        Variable tuesdaySales = new Variable(17);
        Variable wednesdaySales = new Variable(18);
        Variable thursdaySales = new Variable(19);
        Variable fridaySales = new Variable(20);
        Variable saturdaySales = new Variable(21);
        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(sunday, new Literal("Sunday"), AllMask.ALL),
                new Assignment(monday, new Literal("Monday"), AllMask.ALL),
                new Assignment(tuesday, new Literal("Tuesday"), AllMask.ALL),
                new Assignment(wednesday, new Literal("Wednesday"), AllMask.ALL),
                new Assignment(thursday, new Literal("Thursday"), AllMask.ALL),
                new Assignment(friday, new Literal("Friday"), AllMask.ALL),
                new Assignment(saturday, new Literal("Saturday"), AllMask.ALL),
                new Assignment(sundayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(sunday, Stream.VALUES))), AllMask.ALL),
                new Assignment(mondayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(monday, Stream.VALUES))), AllMask.ALL),
                new Assignment(tuesdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(tuesday, Stream.VALUES))), AllMask.ALL),
                new Assignment(wednesdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(wednesday, Stream.VALUES))), AllMask.ALL),
                new Assignment(thursdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(thursday, Stream.VALUES))), AllMask.ALL),
                new Assignment(fridayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(friday, Stream.VALUES))), AllMask.ALL),
                new Assignment(saturdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(saturday, Stream.VALUES))), AllMask.ALL),
                new Assignment(sundaySales, new Call("if_i64", List.of(
                        new Reference(sundayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(mondaySales, new Call("if_i64", List.of(
                        new Reference(mondayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(tuesdaySales, new Call("if_i64", List.of(
                        new Reference(tuesdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(wednesdaySales, new Call("if_i64", List.of(
                        new Reference(wednesdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(thursdaySales, new Call("if_i64", List.of(
                        new Reference(thursdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(fridaySales, new Call("if_i64", List.of(
                        new Reference(fridayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(saturdaySales, new Call("if_i64", List.of(
                        new Reference(saturdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(storeNameIndex), Stream.VALUES),
                new Reference(new Input(storeIdIndex), Stream.VALUES),
                new Reference(sundaySales, Stream.VALUES),
                new Reference(mondaySales, Stream.VALUES),
                new Reference(tuesdaySales, Stream.VALUES),
                new Reference(wednesdaySales, Stream.VALUES),
                new Reference(thursdaySales, Stream.VALUES),
                new Reference(fridaySales, Stream.VALUES),
                new Reference(saturdaySales, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectQuery09BucketValue(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, long threshold)
    {
        Variable thresholdLiteral = new Variable(0);
        Variable countGreaterThanThreshold = new Variable(1);
        Variable bucketValue = new Variable(2);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(thresholdLiteral, new Literal(threshold), AllMask.ALL),
                        new Assignment(countGreaterThanThreshold, new Call("lt", List.of(
                                new Reference(thresholdLiteral, Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                        new Assignment(bucketValue, new Call("if_i64", List.of(
                                new Reference(countGreaterThanThreshold, Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(bucketValue, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery02Buckets(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int weekSequenceIndex, int dayNameIndex, int salesIndex)
    {
        Variable zero = new Variable(0);
        Variable sunday = new Variable(1);
        Variable monday = new Variable(2);
        Variable tuesday = new Variable(3);
        Variable wednesday = new Variable(4);
        Variable thursday = new Variable(5);
        Variable friday = new Variable(6);
        Variable saturday = new Variable(7);
        Variable sundayMatch = new Variable(8);
        Variable mondayMatch = new Variable(9);
        Variable tuesdayMatch = new Variable(10);
        Variable wednesdayMatch = new Variable(11);
        Variable thursdayMatch = new Variable(12);
        Variable fridayMatch = new Variable(13);
        Variable saturdayMatch = new Variable(14);
        Variable sundaySales = new Variable(15);
        Variable mondaySales = new Variable(16);
        Variable tuesdaySales = new Variable(17);
        Variable wednesdaySales = new Variable(18);
        Variable thursdaySales = new Variable(19);
        Variable fridaySales = new Variable(20);
        Variable saturdaySales = new Variable(21);
        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(sunday, new Literal("Sunday"), AllMask.ALL),
                new Assignment(monday, new Literal("Monday"), AllMask.ALL),
                new Assignment(tuesday, new Literal("Tuesday"), AllMask.ALL),
                new Assignment(wednesday, new Literal("Wednesday"), AllMask.ALL),
                new Assignment(thursday, new Literal("Thursday"), AllMask.ALL),
                new Assignment(friday, new Literal("Friday"), AllMask.ALL),
                new Assignment(saturday, new Literal("Saturday"), AllMask.ALL),
                new Assignment(sundayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(sunday, Stream.VALUES))), AllMask.ALL),
                new Assignment(mondayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(monday, Stream.VALUES))), AllMask.ALL),
                new Assignment(tuesdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(tuesday, Stream.VALUES))), AllMask.ALL),
                new Assignment(wednesdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(wednesday, Stream.VALUES))), AllMask.ALL),
                new Assignment(thursdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(thursday, Stream.VALUES))), AllMask.ALL),
                new Assignment(fridayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(friday, Stream.VALUES))), AllMask.ALL),
                new Assignment(saturdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(saturday, Stream.VALUES))), AllMask.ALL),
                new Assignment(sundaySales, new Call("if_i64", List.of(
                        new Reference(sundayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(mondaySales, new Call("if_i64", List.of(
                        new Reference(mondayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(tuesdaySales, new Call("if_i64", List.of(
                        new Reference(tuesdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(wednesdaySales, new Call("if_i64", List.of(
                        new Reference(wednesdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(thursdaySales, new Call("if_i64", List.of(
                        new Reference(thursdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(fridaySales, new Call("if_i64", List.of(
                        new Reference(fridayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(saturdaySales, new Call("if_i64", List.of(
                        new Reference(saturdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(new Input(weekSequenceIndex), Stream.VALUES),
                        new Reference(sundaySales, Stream.VALUES),
                        new Reference(mondaySales, Stream.VALUES),
                        new Reference(tuesdaySales, Stream.VALUES),
                        new Reference(wednesdaySales, Stream.VALUES),
                        new Reference(thursdaySales, Stream.VALUES),
                        new Reference(fridaySales, Stream.VALUES),
                        new Reference(saturdaySales, Stream.VALUES),
                        // Hidden physical channels for conditional-aggregation fusion. The SQL projection outputs
                        // above remain present and lazy; a fused accumulator can consume the original discriminator
                        // and value without materializing seven sparse vectors.
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query02WeeklySales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, long year)
    {
        String prefix = "q02." + year;
        Operator union = new UnionAllOperator(2, List.of(
                profiled(prefix + ".scan.web_sales", factScan(allocator, tables, "web_sales", "ws_sold_date_sk", "ws_ext_sales_price")),
                profiled(prefix + ".scan.catalog_sales", factScan(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_ext_sales_price"))));
        union = profiled(prefix + ".join.date_dim", new HashJoinOperator(
                allocator,
                union,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        null,
                        new String[] {"d_date_sk", "d_week_seq", "d_day_name"},
                        0, 1, 2),
                0).withOutputs(1, 3, 4));
        union = profiled(prefix + ".project.buckets", projectQuery02Buckets(allocator, primitiveRegistry, union, 1, 2, 0));
        PhysicalAggregationProgram sums = Boolean.parseBoolean(System.getProperty("nitro.tpcds.query02ConditionalAggregation", "true"))
                ? PhysicalAggregationProgram.singleUnit(ConditionalSumsAggregationUnit.equalUtf8(
                        8,
                        9,
                        List.of("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")))
                : PhysicalAggregationProgram.independent(List.of(
                        new Sum(1),
                        new Sum(2),
                        new Sum(3),
                        new Sum(4),
                        new Sum(5),
                        new Sum(6),
                        new Sum(7)));
        Operator weekly = profiled(prefix + ".group.week", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                sums,
                union));
        weekly = profiled(prefix + ".join.year", new HashJoinOperator(
                allocator,
                weekly,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, year),
                        new String[] {"d_week_seq", "d_year"},
                        0),
                0).withOutputs(0, 1, 2, 3, 4, 5, 6, 7));
        return weekly;
    }

    private static Operator projectQuery02AdjustedWeek(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable offset = new Variable(0);
        Variable adjustedWeek = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(offset, new Literal(53L), AllMask.ALL),
                                new Assignment(adjustedWeek, new Call("add", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(offset, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(adjustedWeek, Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(new Input(6), Stream.VALUES),
                                new Reference(new Input(7), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery02Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable hundred = new Variable(0);
        Variable sundayRatio = new Variable(1);
        Variable mondayRatio = new Variable(2);
        Variable tuesdayRatio = new Variable(3);
        Variable wednesdayRatio = new Variable(4);
        Variable thursdayRatio = new Variable(5);
        Variable fridayRatio = new Variable(6);
        Variable saturdayRatio = new Variable(7);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(hundred, new Literal(100L), AllMask.ALL),
                                new Assignment(sundayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(new Input(8), Stream.VALUES),
                                        new Reference(hundred, Stream.VALUES))), AllMask.ALL),
                                new Assignment(mondayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(2), Stream.VALUES),
                                        new Reference(new Input(9), Stream.VALUES),
                                        new Reference(hundred, Stream.VALUES))), AllMask.ALL),
                                new Assignment(tuesdayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(3), Stream.VALUES),
                                        new Reference(new Input(10), Stream.VALUES),
                                        new Reference(hundred, Stream.VALUES))), AllMask.ALL),
                                new Assignment(wednesdayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(4), Stream.VALUES),
                                        new Reference(new Input(11), Stream.VALUES),
                                        new Reference(hundred, Stream.VALUES))), AllMask.ALL),
                                new Assignment(thursdayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(5), Stream.VALUES),
                                        new Reference(new Input(12), Stream.VALUES),
                                        new Reference(hundred, Stream.VALUES))), AllMask.ALL),
                                new Assignment(fridayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(6), Stream.VALUES),
                                        new Reference(new Input(13), Stream.VALUES),
                                        new Reference(hundred, Stream.VALUES))), AllMask.ALL),
                                new Assignment(saturdayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(7), Stream.VALUES),
                                        new Reference(new Input(14), Stream.VALUES),
                                        new Reference(hundred, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(sundayRatio, Stream.VALUES),
                                new Reference(mondayRatio, Stream.VALUES),
                                new Reference(tuesdayRatio, Stream.VALUES),
                                new Reference(wednesdayRatio, Stream.VALUES),
                                new Reference(thursdayRatio, Stream.VALUES),
                                new Reference(fridayRatio, Stream.VALUES),
                                new Reference(saturdayRatio, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query59WeeklyStoreSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, long minimumMonthSequenceInclusive, long maximumMonthSequenceInclusive)
    {
        Operator sales = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_store_sk", "ss_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                scannedTable(allocator, tables, "date_dim", "d_date_sk", "d_week_seq", "d_day_name"),
                0);
        sales = projectQuery59Buckets(allocator, primitiveRegistry, sales, 4, 1, 5, 2);
        Operator weekly = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2), new Sum(3), new Sum(4), new Sum(5), new Sum(6), new Sum(7), new Sum(8)),
                sales);
        weekly = new HashJoinOperator(
                allocator,
                weekly,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(1, minimumMonthSequenceInclusive - 1), lessThan(1, maximumMonthSequenceInclusive + 1)),
                        new String[] {"d_week_seq", "d_month_seq"},
                        0),
                0);
        return projectInputs(allocator, primitiveRegistry, weekly, 0, 1, 2, 3, 4, 5, 6, 7, 8);
    }

    private static Operator projectQuery59Buckets(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int weekSequenceIndex, int storeIndex, int dayNameIndex, int salesIndex)
    {
        Variable zero = new Variable(0);
        Variable sunday = new Variable(1);
        Variable monday = new Variable(2);
        Variable tuesday = new Variable(3);
        Variable wednesday = new Variable(4);
        Variable thursday = new Variable(5);
        Variable friday = new Variable(6);
        Variable saturday = new Variable(7);
        Variable sundayMatch = new Variable(8);
        Variable mondayMatch = new Variable(9);
        Variable tuesdayMatch = new Variable(10);
        Variable wednesdayMatch = new Variable(11);
        Variable thursdayMatch = new Variable(12);
        Variable fridayMatch = new Variable(13);
        Variable saturdayMatch = new Variable(14);
        Variable sundaySales = new Variable(15);
        Variable mondaySales = new Variable(16);
        Variable tuesdaySales = new Variable(17);
        Variable wednesdaySales = new Variable(18);
        Variable thursdaySales = new Variable(19);
        Variable fridaySales = new Variable(20);
        Variable saturdaySales = new Variable(21);
        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(sunday, new Literal("Sunday"), AllMask.ALL),
                new Assignment(monday, new Literal("Monday"), AllMask.ALL),
                new Assignment(tuesday, new Literal("Tuesday"), AllMask.ALL),
                new Assignment(wednesday, new Literal("Wednesday"), AllMask.ALL),
                new Assignment(thursday, new Literal("Thursday"), AllMask.ALL),
                new Assignment(friday, new Literal("Friday"), AllMask.ALL),
                new Assignment(saturday, new Literal("Saturday"), AllMask.ALL),
                new Assignment(sundayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(sunday, Stream.VALUES))), AllMask.ALL),
                new Assignment(mondayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(monday, Stream.VALUES))), AllMask.ALL),
                new Assignment(tuesdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(tuesday, Stream.VALUES))), AllMask.ALL),
                new Assignment(wednesdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(wednesday, Stream.VALUES))), AllMask.ALL),
                new Assignment(thursdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(thursday, Stream.VALUES))), AllMask.ALL),
                new Assignment(fridayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(friday, Stream.VALUES))), AllMask.ALL),
                new Assignment(saturdayMatch, new Call("eq_utf8", List.of(
                        new Reference(new Input(dayNameIndex), Stream.VALUES),
                        new Reference(saturday, Stream.VALUES))), AllMask.ALL),
                new Assignment(sundaySales, new Call("if_i64", List.of(
                        new Reference(sundayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(mondaySales, new Call("if_i64", List.of(
                        new Reference(mondayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(tuesdaySales, new Call("if_i64", List.of(
                        new Reference(tuesdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(wednesdaySales, new Call("if_i64", List.of(
                        new Reference(wednesdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(thursdaySales, new Call("if_i64", List.of(
                        new Reference(thursdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(fridaySales, new Call("if_i64", List.of(
                        new Reference(fridayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(saturdaySales, new Call("if_i64", List.of(
                        new Reference(saturdayMatch, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(new Input(weekSequenceIndex), Stream.VALUES),
                        new Reference(new Input(storeIndex), Stream.VALUES),
                        new Reference(sundaySales, Stream.VALUES),
                        new Reference(mondaySales, Stream.VALUES),
                        new Reference(tuesdaySales, Stream.VALUES),
                        new Reference(wednesdaySales, Stream.VALUES),
                        new Reference(thursdaySales, Stream.VALUES),
                        new Reference(fridaySales, Stream.VALUES),
                        new Reference(saturdaySales, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery59AdjustedWeek(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable offset = new Variable(0);
        Variable adjustedWeek = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(offset, new Literal(52L), AllMask.ALL),
                                new Assignment(adjustedWeek, new Call("add", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(offset, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(adjustedWeek, Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(new Input(6), Stream.VALUES),
                                new Reference(new Input(7), Stream.VALUES),
                                new Reference(new Input(8), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery59Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable decimalScale = new Variable(0);
        Variable sundayRatio = new Variable(1);
        Variable mondayRatio = new Variable(2);
        Variable tuesdayRatio = new Variable(3);
        Variable wednesdayRatio = new Variable(4);
        Variable thursdayRatio = new Variable(5);
        Variable fridayRatio = new Variable(6);
        Variable saturdayRatio = new Variable(7);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(decimalScale, new Literal(1_000_000L), AllMask.ALL),
                                new Assignment(sundayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(3), Stream.VALUES),
                                        new Reference(new Input(12), Stream.VALUES),
                                        new Reference(decimalScale, Stream.VALUES))), AllMask.ALL),
                                new Assignment(mondayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(4), Stream.VALUES),
                                        new Reference(new Input(13), Stream.VALUES),
                                        new Reference(decimalScale, Stream.VALUES))), AllMask.ALL),
                                new Assignment(tuesdayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(5), Stream.VALUES),
                                        new Reference(new Input(14), Stream.VALUES),
                                        new Reference(decimalScale, Stream.VALUES))), AllMask.ALL),
                                new Assignment(wednesdayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(6), Stream.VALUES),
                                        new Reference(new Input(15), Stream.VALUES),
                                        new Reference(decimalScale, Stream.VALUES))), AllMask.ALL),
                                new Assignment(thursdayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(7), Stream.VALUES),
                                        new Reference(new Input(16), Stream.VALUES),
                                        new Reference(decimalScale, Stream.VALUES))), AllMask.ALL),
                                new Assignment(fridayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(8), Stream.VALUES),
                                        new Reference(new Input(17), Stream.VALUES),
                                        new Reference(decimalScale, Stream.VALUES))), AllMask.ALL),
                                new Assignment(saturdayRatio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(9), Stream.VALUES),
                                        new Reference(new Input(18), Stream.VALUES),
                                        new Reference(decimalScale, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(20), Stream.VALUES),
                                new Reference(new Input(21), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(sundayRatio, Stream.VALUES),
                                new Reference(mondayRatio, Stream.VALUES),
                                new Reference(tuesdayRatio, Stream.VALUES),
                                new Reference(wednesdayRatio, Stream.VALUES),
                                new Reference(thursdayRatio, Stream.VALUES),
                                new Reference(fridayRatio, Stream.VALUES),
                                new Reference(saturdayRatio, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery27Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable rolledUp = new Variable(2);
        Variable groupingState = new Variable(3);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(zero, new Literal(0L), AllMask.ALL),
                                new Assignment(one, new Literal(1L), AllMask.ALL),
                                new Assignment(rolledUp, new Call("eq", List.of(
                                        new Reference(new Input(2), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(groupingState, new Call("if_i64", List.of(
                                        new Reference(rolledUp, Stream.VALUES),
                                        new Reference(one, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(groupingState, Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(new Input(6), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query28Bucket(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, long minimumQuantityInclusive, long maximumQuantityInclusive, long minimumListPriceInclusive, long maximumListPriceInclusive, long minimumCouponAmountInclusive, long maximumCouponAmountInclusive, long minimumWholesaleCostInclusive, long maximumWholesaleCostInclusive)
    {
        Operator prices = filteredProjectedScan(
                allocator,
                primitiveRegistry,
                tables,
                "store_sales",
                query28BucketPredicate(
                        0,
                        1,
                        2,
                        3,
                        minimumQuantityInclusive,
                        maximumQuantityInclusive,
                        minimumListPriceInclusive,
                        maximumListPriceInclusive,
                        minimumCouponAmountInclusive,
                        maximumCouponAmountInclusive,
                        minimumWholesaleCostInclusive,
                        maximumWholesaleCostInclusive),
                new String[] {"ss_quantity", "ss_list_price", "ss_coupon_amt", "ss_wholesale_cost"});
        prices = filter(allocator, primitiveRegistry, prices, isNotNullI64(1));
        prices = new AggregationOperator(
                allocator,
                List.of(new Sum(1), new CountColumn(1), new DistinctCount(1)),
                prices);
        return projectQuery28Bucket(allocator, primitiveRegistry, prices);
    }

    private static Operator projectQuery28Bucket(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable average = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(average, new Call("divide_round_i64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(average, Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static FilterSpec query28BucketPredicate(int quantityIndex, int listPriceIndex, int couponAmountIndex, int wholesaleCostIndex, long minimumQuantityInclusive, long maximumQuantityInclusive, long minimumListPriceInclusive, long maximumListPriceInclusive, long minimumCouponAmountInclusive, long maximumCouponAmountInclusive, long minimumWholesaleCostInclusive, long maximumWholesaleCostInclusive)
    {
        return and(
                betweenInclusive(quantityIndex, minimumQuantityInclusive, maximumQuantityInclusive),
                or(
                        betweenInclusive(listPriceIndex, minimumListPriceInclusive, maximumListPriceInclusive),
                        betweenInclusive(couponAmountIndex, minimumCouponAmountInclusive, maximumCouponAmountInclusive),
                        betweenInclusive(wholesaleCostIndex, minimumWholesaleCostInclusive, maximumWholesaleCostInclusive)));
    }

    private static Operator projectQuery40Buckets(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int stateIndex, int itemIdIndex, int salesPriceIndex, int refundedCashIndex, int dateIndex, long cutoffDate)
    {
        Variable zero = new Variable(0);
        Variable refundedCashIsNull = new Variable(1);
        Variable effectiveRefundedCash = new Variable(2);
        Variable netSales = new Variable(3);
        Variable cutoff = new Variable(4);
        Variable before = new Variable(5);
        Variable salesBefore = new Variable(6);
        Variable salesAfter = new Variable(7);
        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(refundedCashIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(refundedCashIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(effectiveRefundedCash, new Call("if_i64", List.of(
                        new Reference(refundedCashIsNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(refundedCashIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(netSales, new Call("subtract", List.of(
                        new Reference(new Input(salesPriceIndex), Stream.VALUES),
                        new Reference(effectiveRefundedCash, Stream.VALUES))), AllMask.ALL),
                new Assignment(cutoff, new Literal(cutoffDate), AllMask.ALL),
                new Assignment(before, new Call("lt", List.of(
                        new Reference(new Input(dateIndex), Stream.VALUES),
                        new Reference(cutoff, Stream.VALUES))), AllMask.ALL),
                new Assignment(salesBefore, new Call("if_i64", List.of(
                        new Reference(before, Stream.VALUES),
                        new Reference(netSales, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(salesAfter, new Call("if_i64", List.of(
                        new Reference(before, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(netSales, Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(new Input(stateIndex), Stream.VALUES),
                        new Reference(new Input(itemIdIndex), Stream.VALUES),
                        new Reference(salesBefore, Stream.VALUES),
                        new Reference(salesAfter, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery23SalesValue(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int customerIndex, int quantityIndex, int priceIndex)
    {
        Variable sales = new Variable(0);
        List<Assignment> assignments = List.of(new Assignment(sales, new Call("multiply", List.of(
                new Reference(new Input(priceIndex), Stream.VALUES),
                new Reference(new Input(quantityIndex), Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(new Input(customerIndex), Stream.VALUES),
                        new Reference(sales, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery23SalesOnly(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int quantityIndex, int priceIndex)
    {
        Variable sales = new Variable(0);
        List<Assignment> assignments = List.of(new Assignment(sales, new Call("multiply", List.of(
                new Reference(new Input(priceIndex), Stream.VALUES),
                new Reference(new Input(quantityIndex), Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(new Reference(sales, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery93SalesValue(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable adjustedQuantity = new Variable(0);
        Variable sales = new Variable(1);
        List<Assignment> assignments = List.of(
                new Assignment(adjustedQuantity, new Call("subtract", List.of(
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(new Input(7), Stream.VALUES))), AllMask.ALL),
                new Assignment(sales, new Call("multiply", List.of(
                        new Reference(new Input(4), Stream.VALUES),
                        new Reference(adjustedQuantity, Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(sales, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery14SalesOnly(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int quantityIndex, int priceIndex)
    {
        Variable sales = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(sales, new Call("multiply", List.of(
                                new Reference(new Input(priceIndex), Stream.VALUES),
                                new Reference(new Input(quantityIndex), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(sales, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery14SalesByCategory(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int brandIndex, int classIndex, int categoryIndex, int quantityIndex, int priceIndex)
    {
        Variable sales = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(sales, new Call("multiply", List.of(
                                new Reference(new Input(priceIndex), Stream.VALUES),
                                new Reference(new Input(quantityIndex), Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(brandIndex), Stream.VALUES),
                        new Reference(new Input(classIndex), Stream.VALUES),
                        new Reference(new Input(categoryIndex), Stream.VALUES),
                        new Reference(sales, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery14ChannelOutput(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, String channelName)
    {
        Variable channel = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(channel, new Literal(channelName), AllMask.ALL)),
                List.of(
                        new Reference(channel, Stream.VALUES),
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(new Input(4), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery12RevenueRatio(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable scale = new Variable(0);
        Variable ratio = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(scale, new Literal(100_000_000L), AllMask.ALL),
                                new Assignment(ratio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(5), Stream.VALUES),
                                        new Reference(new Input(6), Stream.VALUES),
                                        new Reference(scale, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(ratio, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery89SortKey(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable difference = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(difference, new Call("subtract", List.of(
                                new Reference(new Input(6), Stream.VALUES),
                                new Reference(new Input(7), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(new Input(6), Stream.VALUES),
                                new Reference(new Input(7), Stream.VALUES),
                                new Reference(difference, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    static Operator query57JoinedFacts(TpcdsQueryContext context)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        Operator factRows = context.profiled("q57.scan.catalog_sales", factScan(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_call_center_sk", "cs_item_sk", "cs_sales_price"));
        Operator itemRows = context.profiled("q57.scan.item", scannedTable(allocator, tables, "item", "i_item_sk", "i_brand", "i_category"));
        Operator facts = context.profiled("q57.join.catalog_sales", new HashJoinOperator(
                allocator,
                itemRows,
                0,
                factRows,
                2));
        Operator allowedDates = context.profiled("q57.scan.date_dim", filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                query57DatePredicate(),
                new String[] {"d_date_sk", "d_year", "d_moy"},
                0, 1, 2));
        facts = context.profiled("q57.join.date_dim", new HashJoinOperator(
                allocator,
                facts,
                3,
                allowedDates,
                0));
        Operator callCenters = context.profiled("q57.scan.call_center", scannedTable(allocator, tables, "call_center", "cc_call_center_sk", "cc_name"));
        facts = context.profiled("q57.join.call_center", new HashJoinOperator(
                allocator,
                facts,
                4,
                callCenters,
                0));
        return context.profiled("q57.project.joined_facts", projectInputs(allocator, primitiveRegistry, facts, 2, 1, 11, 8, 9, 6));
    }

    static Operator query57MonthlyGroupedSales(TpcdsQueryContext context)
    {
        Operator facts = query57JoinedFacts(context);
        facts = context.profiled("q57.group.monthly_sales", new GroupedAggregationOperator(
                context.allocator(),
                List.of(0, 1, 2, 3, 4),
                List.of(new Sum(5)),
                facts));
        return facts;
    }

    static Operator query57MonthlyRankedSales(TpcdsQueryContext context)
    {
        Operator facts = query57MonthlyGroupedSales(context);
        facts = context.profiled("q57.rank.monthly_sales", new TopNRankingOperator(
                context.allocator(),
                32,
                new int[] {0, 1, 2},
                new int[] {3, 4},
                new boolean[] {false, false},
                facts,
                EngineResources.from(context.allocator()).operatorResources().topNRankingPolicy()));
        return context.profiled("q57.project.ranked_sales", projectInputs(context.allocator(), context.primitiveRegistry(), facts, 0, 1, 2, 3, 4, 5, 6));
    }

    private static Operator query57MonthlyWindowedSales(TpcdsQueryContext context)
    {
        Operator currentRows = projectQuery57CurrentRows(context, query57MonthlyRankedSales(context));
        Operator previousRows = projectQuery57AdjacentRows(context, query57MonthlyRankedSales(context), true);
        Operator nextRows = projectQuery57AdjacentRows(context, query57MonthlyRankedSales(context), false);
        Operator facts = new HashJoinOperator(context.allocator(), currentRows, new int[] {0, 1, 2, 7}, previousRows, new int[] {0, 1, 2, 4});
        facts = new HashJoinOperator(context.allocator(), facts, new int[] {0, 1, 2, 7}, nextRows, new int[] {0, 1, 2, 4});
        return projectQuery57Output(context, facts);
    }

    private static Operator projectQuery57CurrentRows(TpcdsQueryContext context, Operator source)
    {
        source = context.profiled("q57.filter.current_year", filter(context.allocator(), context.primitiveRegistry(), source, equal(3, 1999)));
        source = context.profiled("q57.window.current_avg", new WindowOperator(
                context.allocator(),
                source,
                new int[] {0, 1, 2, 3},
                new int[0],
                new boolean[0],
                List.of(new PartitionAverageI64WindowFunction(5))));
        return context.profiled("q57.project.current", projectInputs(context.allocator(), context.primitiveRegistry(), source, 0, 1, 2, 3, 4, 7, 5, 6));
    }

    private static Operator projectQuery57AdjacentRows(TpcdsQueryContext context, Operator source, boolean previous)
    {
        Variable one = new Variable(0);
        Variable adjustedRank = new Variable(1);
        List<Assignment> assignments = List.of(
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(adjustedRank, new Call(previous ? "add" : "subtract", List.of(
                        new Reference(new Input(6), Stream.VALUES),
                        new Reference(one, Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(0), Stream.VALUES),
                new Reference(new Input(1), Stream.VALUES),
                new Reference(new Input(2), Stream.VALUES),
                new Reference(new Input(5), Stream.VALUES),
                new Reference(adjustedRank, Stream.VALUES));
        return context.profiled(previous ? "q57.project.previous" : "q57.project.next", new ProjectOperator(context.allocator(), new EvaluationPlan(assignments, outputs), context.primitiveRegistry(), source));
    }

    private static Operator projectQuery57Output(TpcdsQueryContext context, Operator source)
    {
        return context.profiled("q57.project.output", projectInputs(context.allocator(), context.primitiveRegistry(), source, 0, 1, 2, 3, 4, 5, 6, 11, 16));
    }

    private static Operator projectQuery47Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        return projectInputs(allocator, primitiveRegistry, source, 0, 1, 2, 3, 4, 5, 6, 7, 13, 19);
    }

    private static Operator projectQuery57SortKey(TpcdsQueryContext context, Operator source)
    {
        Variable difference = new Variable(0);
        List<Assignment> assignments = List.of(
                new Assignment(difference, new Call("subtract", List.of(
                        new Reference(new Input(6), Stream.VALUES),
                        new Reference(new Input(5), Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(0), Stream.VALUES),
                new Reference(new Input(1), Stream.VALUES),
                new Reference(new Input(2), Stream.VALUES),
                new Reference(new Input(3), Stream.VALUES),
                new Reference(new Input(4), Stream.VALUES),
                new Reference(new Input(5), Stream.VALUES),
                new Reference(new Input(6), Stream.VALUES),
                new Reference(new Input(7), Stream.VALUES),
                new Reference(new Input(8), Stream.VALUES),
                new Reference(difference, Stream.VALUES));
        return context.profiled("q57.project.sort_key", new ProjectOperator(context.allocator(), new EvaluationPlan(assignments, outputs), context.primitiveRegistry(), source));
    }

    private static Operator projectQuery47SortKey(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable difference = new Variable(0);
        List<Assignment> assignments = List.of(
                new Assignment(difference, new Call("subtract", List.of(
                        new Reference(new Input(7), Stream.VALUES),
                        new Reference(new Input(6), Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(0), Stream.VALUES),
                new Reference(new Input(1), Stream.VALUES),
                new Reference(new Input(2), Stream.VALUES),
                new Reference(new Input(3), Stream.VALUES),
                new Reference(new Input(4), Stream.VALUES),
                new Reference(new Input(5), Stream.VALUES),
                new Reference(new Input(6), Stream.VALUES),
                new Reference(new Input(7), Stream.VALUES),
                new Reference(new Input(8), Stream.VALUES),
                new Reference(new Input(9), Stream.VALUES),
                new Reference(difference, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static FilterSpec query57DatePredicate()
    {
        return or(
                equal(1, 1999),
                and(equal(1, 1998), equal(2, 12)),
                and(equal(1, 2000), equal(2, 1)));
    }

    private static Operator query70ActiveStates(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator facts = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_store_sk", "ss_net_profit");
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(1, 1199), lessThan(1, 1212)),
                        new String[] {"d_date_sk", "d_month_seq"},
                        0),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                scannedTable(allocator, tables, "store", "s_store_sk", "s_state"),
                0);
        facts = projectInputs(allocator, primitiveRegistry, facts, 5, 2);
        facts = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                facts);
        return projectInputs(allocator, primitiveRegistry, facts, 0);
    }

    private static Operator query67SalesByRollupKey(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator facts = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_quantity", "ss_sales_price");
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(4, 1199), lessThan(4, 1212)),
                        new String[] {"d_date_sk", "d_year", "d_qoy", "d_moy", "d_month_seq"},
                        0, 1, 2, 3),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                2,
                scannedTable(allocator, tables, "store", "s_store_sk", "s_store_id"),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_brand", "i_class", "i_category", "i_product_name"),
                0);
        return projectQuery67RollupKey(allocator, primitiveRegistry, facts);
    }

    private static Operator query70SalesByLocation(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator facts = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_store_sk", "ss_net_profit");
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(1, 1199), lessThan(1, 1212)),
                        new String[] {"d_date_sk", "d_month_seq"},
                        0),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                scannedTable(allocator, tables, "store", "s_store_sk", "s_county", "s_state"),
                0);
        return projectInputs(allocator, primitiveRegistry, facts, 6, 5, 2);
    }

    private static Operator query86SalesByCategoryClass(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator facts = factScan(allocator, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_net_paid");
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(1, 1199), lessThan(1, 1212)),
                        new String[] {"d_date_sk", "d_month_seq"},
                        0),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_class", "i_category"),
                0);
        return projectInputs(allocator, primitiveRegistry, facts, 6, 5, 2);
    }

    private static Operator query36SalesByCategoryClass(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator facts = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_ext_sales_price", "ss_net_profit");
        facts = new HashJoinOperator(
                allocator,
                facts,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 2001),
                        new String[] {"d_date_sk", "d_year"},
                        0),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_class", "i_category"),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                2,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "store",
                        equalUtf8(1, "TN"),
                        new String[] {"s_store_sk", "s_state"},
                        0),
                0);
        return projectInputs(allocator, primitiveRegistry, facts, 8, 7, 3, 4);
    }

    private static Operator query42SalesByCategory(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 11), equal(2, 2000)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0, 2),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        equal(1, 1L),
                        new String[] {"i_item_sk", "i_manager_id", "i_category_id", "i_category"},
                        0, 2, 3),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 4, 6, 7, 2);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3)),
                sales);
    }

    private static Operator query31ChannelCountyQuarterRevenue(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            String salesTable,
            String soldDateColumn,
            String addressColumn,
            String salesColumn,
            int quarter)
    {
        Operator sales = factScan(allocator, tables, salesTable, soldDateColumn, addressColumn, salesColumn);
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 2000), equal(2, quarter)),
                        new String[] {"d_date_sk", "d_year", "d_qoy"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_county"),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 5, 2);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                sales);
    }

    private static Operator query11ChannelYearTotal(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            String salesTable,
            String customerColumn,
            String soldDateColumn,
            String listPriceColumn,
            String discountColumn,
            int year,
            boolean retainCustomerDetails)
    {
        String profile = "q11." + salesTable + "." + year;
        Operator sales = profiled(profile + ".scan", factScan(allocator, tables, salesTable, soldDateColumn, customerColumn, discountColumn, listPriceColumn));
        sales = profiled(profile + ".join.customer", new HashJoinOperator(
                allocator,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_country", "c_login", "c_email_address"),
                0,
                sales,
                1).withOutputs(1, 2, 3, 4, 5, 6, 7, 8, 10, 11));
        sales = profiled(profile + ".join.date", new HashJoinOperator(
                allocator,
                sales,
                7,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, year),
                        new String[] {"d_date_sk", "d_year"},
                        0, 1),
                0));
        sales = profiled(profile + ".project", projectQuery11SalesValue(allocator, primitiveRegistry, sales, 0, 1, 2, 3, 4, 5, 6, 11, 9, 8));
        sales = profiled(profile + ".group", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5, 6, 7),
                List.of(new Sum(8)),
                sales));
        if (retainCustomerDetails) {
            return projectInputs(allocator, primitiveRegistry, sales, 0, 1, 2, 3, 4, 5, 8);
        }
        return projectInputs(allocator, primitiveRegistry, sales, 0, 8);
    }

    private static Operator query76ChannelCategorySales(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            String channelName,
            String columnName,
            String salesTable,
            String nullableColumn,
            String soldDateColumn,
            String itemColumn,
            String salesColumn)
    {
        String profile = "q76." + channelName;
        Operator sales = profiled(profile + ".scan_filter_project", filteredProjectedScan(
                allocator,
                primitiveRegistry,
                tables,
                salesTable,
                isNull(0),
                new String[] {nullableColumn, soldDateColumn, itemColumn, salesColumn},
                1, 2, 3));
        sales = profiled(profile + ".join.date_dim", new HashJoinOperator(
                allocator,
                sales,
                0,
                scannedTable(allocator, tables, "date_dim", "d_date_sk", "d_year", "d_qoy"),
                0).withOutputs(1, 2, 4, 5));
        sales = profiled(profile + ".join.item", new HashJoinOperator(
                allocator,
                sales,
                0,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_category"),
                0).withOutputs(1, 2, 3, 5));
        sales = profiled(profile + ".project.channel_output", projectQuery76ChannelOutput(allocator, primitiveRegistry, sales, channelName, columnName));
        return profiled(profile + ".group.channel_category", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4),
                List.of(new CountAll(), new Sum(5)),
                sales));
    }

    private static Operator query74ChannelYearTotal(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            String salesTable,
            String customerColumn,
            String soldDateColumn,
            String netPaidColumn,
            int year,
            boolean retainCustomerDetails)
    {
        String profile = "q74." + (salesTable.equals("store_sales") ? "store" : "web") + "." + year;
        Operator sales = profiled(profile + ".scan.sales", factScan(allocator, tables, salesTable, soldDateColumn, customerColumn, netPaidColumn));
        sales = profiled(profile + ".join.customer", new HashJoinOperator(
                allocator,
                profiled(profile + ".scan.customer", customerScan(allocator, tables, "c_customer_sk", "c_customer_id", "c_first_name", "c_last_name")),
                0,
                sales,
                1).withOutputs(1, 2, 3, 4, 6));
        sales = profiled(profile + ".join.date_dim", new HashJoinOperator(
                allocator,
                sales,
                3,
                profiled(profile + ".scan.date_dim", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, year),
                        new String[] {"d_date_sk", "d_year"},
                        0, 1)),
                0));
        sales = profiled(profile + ".project.group_input", projectInputs(allocator, primitiveRegistry, sales, 0, 1, 2, 6, 4));
        sales = profiled(profile + ".group.customer", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3),
                List.of(new Sum(4)),
                sales));
        if (retainCustomerDetails) {
            return projectInputs(allocator, primitiveRegistry, sales, 0, 1, 2, 4);
        }
        return projectInputs(allocator, primitiveRegistry, sales, 0, 4);
    }

    private static Operator query87ChannelPresence(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            String salesTable,
            String customerColumn,
            String soldDateColumn,
            int activeChannel)
    {
        Operator sales = factScan(allocator, tables, salesTable, customerColumn, soldDateColumn);
        sales = profiled("q87." + activeChannel + ".group.sales_keys", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(),
                sales));
        sales = profiled("q87." + activeChannel + ".join.date_dim", new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(1, 1199), lessThan(1, 1212)),
                        new String[] {"d_date_sk", "d_month_seq", "d_date"},
                        0, 2),
                0).withOutputs(0, 3));
        sales = profiled("q87." + activeChannel + ".join.customer", new HashJoinOperator(
                allocator,
                sales,
                0,
                customerScan(allocator, tables, "c_customer_sk", "c_last_name", "c_first_name"),
                0).withOutputs(3, 4, 1));
        sales = profiled("q87." + activeChannel + ".group.distinct", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(),
                sales));
        return projectQuery87ChannelPresence(allocator, primitiveRegistry, sales, activeChannel);
    }

    private static Operator query55SalesByBrand(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price");
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 11), equal(2, 1999)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0),
                0);
        sales = new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        equal(1, 28L),
                        new String[] {"i_item_sk", "i_manager_id", "i_brand_id", "i_brand"},
                        0, 2, 3),
                0);
        sales = projectInputs(allocator, primitiveRegistry, sales, 5, 6, 2);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2)),
                sales);
    }

    private static Operator query47MonthlySales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = profiled("q47.scan.store_sales", factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"));
        Operator facts = profiled("q47.join.item", new HashJoinOperator(
                allocator,
                profiled("q47.scan.item", scannedTable(allocator, tables, "item", "i_item_sk", "i_brand", "i_category")),
                0,
                sales,
                1));
        facts = profiled("q47.join.date_dim", new HashJoinOperator(
                allocator,
                facts,
                3,
                profiled("q47.scan.date_dim", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query57DatePredicate(),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0, 1, 2)),
                0));
        facts = profiled("q47.join.store", new HashJoinOperator(
                allocator,
                facts,
                5,
                profiled("q47.scan.store", scannedTable(allocator, tables, "store", "s_store_sk", "s_store_name", "s_company_name")),
                0));
        facts = profiled("q47.project.group_inputs", projectInputs(allocator, primitiveRegistry, facts, 2, 1, 11, 12, 8, 9, 6));
        facts = profiled("q47.group.monthly_sales", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5),
                List.of(new Sum(6)),
                facts));
        return profiled("q47.rank.monthly_sales", new TopNRankingOperator(
                allocator,
                32,
                new int[] {0, 1, 2, 3},
                new int[] {4, 5},
                new boolean[] {false, false},
                facts,
                EngineResources.from(allocator).operatorResources().topNRankingPolicy()));
    }

    private static Operator projectQuery31Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int countyIndex, int storeQuarterOneIndex, int storeQuarterTwoIndex, int storeQuarterThreeIndex, int webQuarterOneIndex, int webQuarterTwoIndex, int webQuarterThreeIndex)
    {
        Variable year = new Variable(0);
        Variable scale = new Variable(1);
        Variable webQuarterOneToTwoIncrease = new Variable(2);
        Variable storeQuarterOneToTwoIncrease = new Variable(3);
        Variable webQuarterTwoToThreeIncrease = new Variable(4);
        Variable storeQuarterTwoToThreeIncrease = new Variable(5);

        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(year, new Literal(2000L), AllMask.ALL),
                                new Assignment(scale, new Literal(1_000_000L), AllMask.ALL),
                                new Assignment(webQuarterOneToTwoIncrease, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(webQuarterTwoIndex), Stream.VALUES),
                                        new Reference(new Input(webQuarterOneIndex), Stream.VALUES),
                                        new Reference(scale, Stream.VALUES))), AllMask.ALL),
                                new Assignment(storeQuarterOneToTwoIncrease, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(storeQuarterTwoIndex), Stream.VALUES),
                                        new Reference(new Input(storeQuarterOneIndex), Stream.VALUES),
                                        new Reference(scale, Stream.VALUES))), AllMask.ALL),
                                new Assignment(webQuarterTwoToThreeIncrease, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(webQuarterThreeIndex), Stream.VALUES),
                                        new Reference(new Input(webQuarterTwoIndex), Stream.VALUES),
                                        new Reference(scale, Stream.VALUES))), AllMask.ALL),
                                new Assignment(storeQuarterTwoToThreeIncrease, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(storeQuarterThreeIndex), Stream.VALUES),
                                        new Reference(new Input(storeQuarterTwoIndex), Stream.VALUES),
                                        new Reference(scale, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(countyIndex), Stream.VALUES),
                                new Reference(year, Stream.VALUES),
                                new Reference(webQuarterOneToTwoIncrease, Stream.VALUES),
                                new Reference(storeQuarterOneToTwoIncrease, Stream.VALUES),
                                new Reference(webQuarterTwoToThreeIncrease, Stream.VALUES),
                                new Reference(storeQuarterTwoToThreeIncrease, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery11SalesValue(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int customerIdIndex, int firstNameIndex, int lastNameIndex, int preferredCustomerFlagIndex, int birthCountryIndex, int loginIndex, int emailAddressIndex, int yearIndex, int listPriceIndex, int discountIndex)
    {
        Variable yearTotal = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(yearTotal, new Call("subtract", List.of(
                                new Reference(new Input(listPriceIndex), Stream.VALUES),
                                new Reference(new Input(discountIndex), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(customerIdIndex), Stream.VALUES),
                                new Reference(new Input(firstNameIndex), Stream.VALUES),
                                new Reference(new Input(lastNameIndex), Stream.VALUES),
                                new Reference(new Input(preferredCustomerFlagIndex), Stream.VALUES),
                                new Reference(new Input(birthCountryIndex), Stream.VALUES),
                                new Reference(new Input(loginIndex), Stream.VALUES),
                                new Reference(new Input(emailAddressIndex), Stream.VALUES),
                                new Reference(new Input(yearIndex), Stream.VALUES),
                                new Reference(yearTotal, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery76ChannelOutput(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, String channelName, String columnName)
    {
        Variable channel = new Variable(0);
        Variable column = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(channel, new Literal(channelName), AllMask.ALL),
                                new Assignment(column, new Literal(columnName), AllMask.ALL)),
                        List.of(
                                new Reference(channel, Stream.VALUES),
                                new Reference(column, Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query71ChannelSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String timeColumn, String salesColumn)
    {
        Operator sales = factScan(allocator, tables, salesTable, soldDateColumn, itemColumn, timeColumn, salesColumn);
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 11), equal(2, 1999)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0),
                0);
        return projectInputs(allocator, primitiveRegistry, sales, 1, 2, 3);
    }

    private static Operator projectQuery17Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable storeRatio = new Variable(0);
        Variable returnsRatio = new Variable(1);
        Variable catalogRatio = new Variable(2);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(storeRatio, new Call("divide_f64", List.of(
                                        new Reference(new Input(5), Stream.VALUES),
                                        new Reference(new Input(4), Stream.VALUES))), AllMask.ALL),
                                new Assignment(returnsRatio, new Call("divide_f64", List.of(
                                        new Reference(new Input(8), Stream.VALUES),
                                        new Reference(new Input(7), Stream.VALUES))), AllMask.ALL),
                                new Assignment(catalogRatio, new Call("divide_f64", List.of(
                                        new Reference(new Input(11), Stream.VALUES),
                                        new Reference(new Input(10), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(storeRatio, Stream.VALUES),
                                new Reference(new Input(6), Stream.VALUES),
                                new Reference(new Input(7), Stream.VALUES),
                                new Reference(new Input(8), Stream.VALUES),
                                new Reference(returnsRatio, Stream.VALUES),
                                new Reference(new Input(9), Stream.VALUES),
                                new Reference(new Input(10), Stream.VALUES),
                                new Reference(new Input(11), Stream.VALUES),
                                new Reference(catalogRatio, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query39InventoryVariation(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, int month, double covarianceThreshold)
    {
        String profilePrefix = "q39.month" + month;
        Operator inventory = profiled(profilePrefix + ".scan.inventory", scannedTable(allocator, tables, "inventory", "inv_item_sk", "inv_warehouse_sk", "inv_date_sk", "inv_quantity_on_hand"));
        HashJoinOperator itemJoin = new HashJoinOperator(
                allocator,
                inventory,
                0,
                profiled(profilePrefix + ".scan.item", scannedTable(allocator, tables, "item", "i_item_sk")),
                0);
        // Compact layouts below are semantic, not merely width reductions. Keep the comments synchronized with
        // Trino q39.sql so fields used only by GROUP BY are not accidentally discarded.
        inventory = profiled(profilePrefix + ".join.item", QUERY39_COMPACT_JOIN_LAYOUTS ? itemJoin.withOutputs(1, 2, 3, 0) : itemJoin);
        // Compact: [inv_warehouse_sk, inv_date_sk, inv_quantity_on_hand, inv_item_sk].
        HashJoinOperator warehouseJoin = new HashJoinOperator(
                allocator,
                inventory,
                QUERY39_COMPACT_JOIN_LAYOUTS ? 0 : 1,
                profiled(profilePrefix + ".scan.warehouse", scannedTable(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name")),
                0);
        inventory = profiled(profilePrefix + ".join.warehouse", QUERY39_COMPACT_JOIN_LAYOUTS ? warehouseJoin.withOutputs(1, 2, 3, 4, 5) : warehouseJoin);
        // Compact: [inv_date_sk, inv_quantity_on_hand, inv_item_sk, w_warehouse_sk, w_warehouse_name].
        HashJoinOperator dateJoin = new HashJoinOperator(
                allocator,
                inventory,
                QUERY39_COMPACT_JOIN_LAYOUTS ? 0 : 2,
                profiled(profilePrefix + ".scan.date_dim", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 2001), equal(2, month)),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0, 2)),
                0);
        inventory = profiled(profilePrefix + ".join.date_dim", QUERY39_COMPACT_JOIN_LAYOUTS ? dateJoin.withOutputs(4, 3, 2, 6, 1) : dateJoin);
        // Group input: [w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, inv_quantity_on_hand].
        if (!QUERY39_COMPACT_JOIN_LAYOUTS) {
            inventory = projectInputs(allocator, primitiveRegistry, inventory, 6, 5, 0, 8, 3);
        }
        inventory = profiled(profilePrefix + ".group", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3),
                new PhysicalAggregationProgram(
                        List.of(new CountAvgStddevI64AggregationUnit(4)),
                        List.of(
                                new PhysicalAggregationProgram.Output(0, 0),
                                new PhysicalAggregationProgram.Output(0, 2),
                                new PhysicalAggregationProgram.Output(0, 1))),
                inventory));
        inventory = profiled(profilePrefix + ".filter.cov", filter(allocator, primitiveRegistry, inventory, query39CovarianceThresholdPredicate(4, 6, 5, covarianceThreshold)));
        return profiled(profilePrefix + ".project.output", projectQuery39InventoryOutput(allocator, primitiveRegistry, inventory));
    }

    private static Operator projectQuery39InventoryOutput(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable coefficient = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(coefficient, new Call("divide_f64", List.of(
                                        new Reference(new Input(5), Stream.VALUES),
                                        new Reference(new Input(6), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(6), Stream.VALUES),
                                new Reference(coefficient, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static FilterSpec query39CovarianceThresholdPredicate(int countIndex, int averageIndex, int stddevIndex, double threshold)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable thresholdValue = new Variable(2);
        Variable countSatisfied = new Variable(3);
        Variable meanPositive = new Variable(4);
        Variable coefficient = new Variable(5);
        Variable coefficientSatisfied = new Variable(6);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(zero, new Literal(0.0), AllMask.ALL),
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(thresholdValue, new Literal(threshold), AllMask.ALL),
                new Assignment(countSatisfied, new Call("lt", List.of(
                        new Reference(one, Stream.VALUES),
                        new Reference(new Input(countIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(meanPositive, new Call("gt_f64", List.of(
                        new Reference(new Input(averageIndex), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(coefficient, new Call("divide_f64", List.of(
                        new Reference(new Input(stddevIndex), Stream.VALUES),
                        new Reference(new Input(averageIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(coefficientSatisfied, new Call("gt_f64", List.of(
                        new Reference(coefficient, Stream.VALUES),
                        new Reference(thresholdValue, Stream.VALUES))), AllMask.ALL)),
                List.of());
        return new FilterSpec(plan, new AndMask(List.of(
                new ReferenceMask(new Reference(countSatisfied, Stream.VALUES)),
                new ReferenceMask(new Reference(meanPositive, Stream.VALUES)),
                new ReferenceMask(new Reference(coefficientSatisfied, Stream.VALUES)))));
    }

    private static Operator query64EligibleCatalogItems(TpcdsQueryContext context, String profilePrefix)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        Operator sales = context.profiled(profilePrefix + ".scan.catalog_sales", factScan(allocator, tables, "catalog_sales", "cs_item_sk", "cs_order_number", "cs_ext_list_price"));
        sales = context.profiled(profilePrefix + ".join.catalog_returns", new HashJoinOperator(
                allocator,
                sales,
                new int[] {0, 1},
                scannedTable(allocator, tables, "catalog_returns", "cr_item_sk", "cr_order_number", "cr_refunded_cash", "cr_reversed_charge", "cr_store_credit"),
                new int[] {0, 1}));
        sales = context.profiled(profilePrefix + ".project.sales_refunds", projectQuery64CatalogRefunds(allocator, primitiveRegistry, sales));
        sales = context.profiled(profilePrefix + ".group.cs_ui.partial", new SqlStageAggregationOperator(
                allocator,
                sales,
                2,
                new int[0],
                List.of(SqlStageAggregationOperator.aggregate(
                        List.of(0),
                        () -> List.of(new Sum(1), new Sum(2))))));
        sales = context.profiled(profilePrefix + ".group.cs_ui.final", new SqlStageAggregationOperator(
                allocator,
                sales,
                2,
                new int[] {0},
                List.of(SqlStageAggregationOperator.aggregate(
                        List.of(0),
                        () -> List.of(new Sum(1), new Sum(2))))));
        sales = context.profiled(profilePrefix + ".filter.item_sk", filter(allocator, primitiveRegistry, sales, query64RefundThresholdPredicate(1, 2)));
        return context.profiled(profilePrefix + ".project.item_sk", projectInputs(allocator, primitiveRegistry, sales, 0));
    }

    private static Operator projectQuery64CatalogRefunds(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable refundedCashAndReversedCharge = new Variable(0);
        Variable totalRefund = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(refundedCashAndReversedCharge, new Call("add", List.of(
                                        new Reference(new Input(5), Stream.VALUES),
                                        new Reference(new Input(6), Stream.VALUES))), AllMask.ALL),
                                new Assignment(totalRefund, new Call("add", List.of(
                                        new Reference(refundedCashAndReversedCharge, Stream.VALUES),
                                        new Reference(new Input(7), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(totalRefund, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static FilterSpec query64RefundThresholdPredicate(int salesIndex, int refundIndex)
    {
        Variable two = new Variable(0);
        Variable scaledRefund = new Variable(1);
        Variable accepted = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(two, new Literal(2L), AllMask.ALL),
                new Assignment(scaledRefund, new Call("multiply", List.of(
                        new Reference(new Input(refundIndex), Stream.VALUES),
                        new Reference(two, Stream.VALUES))), AllMask.ALL),
                new Assignment(accepted, new Call("lt", List.of(
                        new Reference(scaledRefund, Stream.VALUES),
                        new Reference(new Input(salesIndex), Stream.VALUES))), AllMask.ALL)),
                List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(accepted, Stream.VALUES)));
    }

    private static Operator query64CrossSales(TpcdsQueryContext context, String profilePrefix, long soldYear)
    {
        Allocator allocator = context.allocator();
        PrimitiveRegistry primitiveRegistry = context.primitiveRegistry();
        TpcdsParquetTables tables = context.tables();
        Operator sales = context.profiled(profilePrefix + ".scan.store_sales", factScan(allocator, tables, "store_sales", "ss_store_sk", "ss_sold_date_sk", "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_item_sk", "ss_ticket_number", "ss_promo_sk", "ss_wholesale_cost", "ss_list_price", "ss_coupon_amt"));
        sales = context.profiled(profilePrefix + ".join.store_returns", new HashJoinOperator(
                allocator,
                sales,
                new int[] {6, 7},
                scannedTable(allocator, tables, "store_returns", "sr_item_sk", "sr_ticket_number"),
                new int[] {0, 1}).withOutputs(0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11));
        sales = context.profiled(profilePrefix + ".join.eligible_items", new HashJoinOperator(allocator, sales, 6, query64EligibleCatalogItems(context, profilePrefix + ".eligible_catalog"), 0)
                .withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        sales = context.profiled(profilePrefix + ".join.store", new HashJoinOperator(
                allocator,
                sales,
                0,
                scannedTable(allocator, tables, "store", "s_store_sk", "s_store_name", "s_zip"),
                0).withOutputs(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13));
        sales = context.profiled(profilePrefix + ".join.customer", new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_cdemo_sk", "c_current_hdemo_sk", "c_current_addr_sk", "c_first_sales_date_sk", "c_first_shipto_date_sk"),
                0).withOutputs(0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17));
        sales = context.profiled(profilePrefix + ".join.customer_demographics.bought", new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "customer_demographics", "cd_demo_sk", "cd_marital_status"),
                0).withOutputs(0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17));
        sales = context.profiled(profilePrefix + ".join.customer_demographics.current", new HashJoinOperator(
                allocator,
                sales,
                10,
                scannedTable(allocator, tables, "customer_demographics", "cd_demo_sk", "cd_marital_status"),
                0).withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 17));
        sales = context.profiled(profilePrefix + ".join.household_demographics.bought", new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "household_demographics", "hd_demo_sk", "hd_income_band_sk"),
                0).withOutputs(0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17));
        sales = context.profiled(profilePrefix + ".join.household_demographics.current", new HashJoinOperator(
                allocator,
                sales,
                9,
                scannedTable(allocator, tables, "household_demographics", "hd_demo_sk", "hd_income_band_sk"),
                0).withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 17));
        sales = context.profiled(profilePrefix + ".join.customer_address.bought", new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_street_number", "ca_street_name", "ca_city", "ca_zip"),
                0).withOutputs(0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20));
        sales = context.profiled(profilePrefix + ".join.customer_address.current", new HashJoinOperator(
                allocator,
                sales,
                8,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_street_number", "ca_street_name", "ca_city", "ca_zip"),
                0).withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21, 22, 23));
        sales = context.profiled(profilePrefix + ".join.date_dim.sold", new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, soldYear),
                        new String[] {"d_date_sk", "d_year"},
                        0,
                        1),
                0).withOutputs(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23));
        sales = context.profiled(profilePrefix + ".join.date_dim.first_sales", new HashJoinOperator(
                allocator,
                sales,
                7,
                scannedTable(allocator, tables, "date_dim", "d_date_sk", "d_year"),
                0).withOutputs(0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23));
        sales = context.profiled(profilePrefix + ".join.date_dim.first_ship", new HashJoinOperator(
                allocator,
                sales,
                7,
                scannedTable(allocator, tables, "date_dim", "d_date_sk", "d_year"),
                0).withOutputs(0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23));
        sales = context.profiled(profilePrefix + ".join.promotion", new HashJoinOperator(
                allocator,
                sales,
                1,
                scannedTable(allocator, tables, "promotion", "p_promo_sk"),
                0).withOutputs(0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21));
        sales = context.profiled(profilePrefix + ".join.income_band.bought", new HashJoinOperator(
                allocator,
                sales,
                8,
                scannedTable(allocator, tables, "income_band", "ib_income_band_sk"),
                0).withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20));
        sales = context.profiled(profilePrefix + ".join.income_band.current", new HashJoinOperator(
                allocator,
                sales,
                8,
                scannedTable(allocator, tables, "income_band", "ib_income_band_sk"),
                0).withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
        sales = context.profiled(profilePrefix + ".join.item", new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        and(
                                betweenInclusive(2, 65_00L, 74_00L),
                                utf8AnyOf(3, Set.of("purple", "burlywood", "indian", "spring", "floral", "medium"))),
                        new String[] {"i_item_sk", "i_product_name", "i_current_price", "i_color"}),
                0).withOutputs(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20));
        sales = context.profiled(profilePrefix + ".filter.same_address", filter(allocator, primitiveRegistry, sales, notEqualUtf8Columns(6, 7)));
        sales = context.profiled(profilePrefix + ".project.group_rows", projectQuery64CrossSalesRows(allocator, primitiveRegistry, sales));
        // The optimized SQL plan partitions the item join on item_sk, then runs partial and final aggregation in
        // the same task with only a local exchange between them.
        return context.profiled(profilePrefix + ".group.cross_sales", new SqlStageAggregationOperator(
                allocator,
                sales,
                2,
                new int[] {1},
                List.of(
                        SqlStageAggregationOperator.aggregate(
                                List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
                                () -> List.of(new CountAll(), new Sum(15), new Sum(16), new Sum(17))),
                        SqlStageAggregationOperator.aggregate(
                                List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
                                () -> List.of(new Sum(15), new Sum(16), new Sum(17), new Sum(18))))));
    }

    private static Operator projectQuery64CrossSalesRows(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(),
                        List.of(
                                new Reference(new Input(19), Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(new Input(8), Stream.VALUES),
                                new Reference(new Input(9), Stream.VALUES),
                                new Reference(new Input(10), Stream.VALUES),
                                new Reference(new Input(11), Stream.VALUES),
                                new Reference(new Input(12), Stream.VALUES),
                                new Reference(new Input(13), Stream.VALUES),
                                new Reference(new Input(14), Stream.VALUES),
                                new Reference(new Input(15), Stream.VALUES),
                                new Reference(new Input(16), Stream.VALUES),
                                new Reference(new Input(17), Stream.VALUES),
                                new Reference(new Input(18), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static FilterSpec query64YearTransitionPredicate(int firstYearIndex, int secondYearIndex, int firstCountIndex, int secondCountIndex)
    {
        Variable one = new Variable(0);
        Variable firstYearLiteral = new Variable(1);
        Variable secondYearLiteral = new Variable(2);
        Variable firstCountUpperBound = new Variable(3);
        Variable firstYearSatisfied = new Variable(4);
        Variable secondYearSatisfied = new Variable(5);
        Variable countSatisfied = new Variable(6);
        Variable preliminarilyAccepted = new Variable(7);
        Variable accepted = new Variable(8);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(firstYearLiteral, new Literal(1999L), AllMask.ALL),
                new Assignment(secondYearLiteral, new Literal(2000L), AllMask.ALL),
                new Assignment(firstCountUpperBound, new Call("add", List.of(
                        new Reference(new Input(firstCountIndex), Stream.VALUES),
                        new Reference(one, Stream.VALUES))), AllMask.ALL),
                new Assignment(firstYearSatisfied, new Call("eq", List.of(
                        new Reference(new Input(firstYearIndex), Stream.VALUES),
                        new Reference(firstYearLiteral, Stream.VALUES))), AllMask.ALL),
                new Assignment(secondYearSatisfied, new Call("eq", List.of(
                        new Reference(new Input(secondYearIndex), Stream.VALUES),
                        new Reference(secondYearLiteral, Stream.VALUES))), AllMask.ALL),
                new Assignment(countSatisfied, new Call("lt", List.of(
                        new Reference(new Input(secondCountIndex), Stream.VALUES),
                        new Reference(firstCountUpperBound, Stream.VALUES))), AllMask.ALL),
                new Assignment(preliminarilyAccepted, new Call("and", List.of(
                        new Reference(firstYearSatisfied, Stream.VALUES),
                        new Reference(secondYearSatisfied, Stream.VALUES))), AllMask.ALL),
                new Assignment(accepted, new Call("and", List.of(
                        new Reference(preliminarilyAccepted, Stream.VALUES),
                        new Reference(countSatisfied, Stream.VALUES))), AllMask.ALL)),
                List.of());
        return new FilterSpec(
                plan,
                andMask(firstYearSatisfied, secondYearSatisfied, countSatisfied),
                new Reference(accepted, Stream.VALUES));
    }

    private static Operator projectQuery72GroupedRows(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int itemDescriptionIndex, int warehouseNameIndex, int weekSequenceIndex, int promotionMatchIndex)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable noPromotion = new Variable(2);
        Variable promotion = new Variable(3);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(noPromotion, new Call("if_i64", List.of(
                                new Reference(new Input(promotionMatchIndex), Stream.VALUES),
                                new Reference(zero, Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL),
                        new Assignment(promotion, new Call("if_i64", List.of(
                                new Reference(new Input(promotionMatchIndex), Stream.VALUES),
                                new Reference(one, Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(itemDescriptionIndex), Stream.VALUES),
                        new Reference(new Input(warehouseNameIndex), Stream.VALUES),
                        new Reference(new Input(weekSequenceIndex), Stream.VALUES),
                        new Reference(noPromotion, Stream.VALUES),
                        new Reference(promotion, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query72InventoryByWeek(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator inventory = profiled("q72.scan.inventory", scannedTable(allocator, tables, "inventory", "inv_item_sk", "inv_warehouse_sk", "inv_date_sk", "inv_quantity_on_hand"));
        inventory = profiled("q72.join.date_dim.inventory", new HashJoinOperator(
                allocator,
                inventory,
                2,
                profiled("q72.scan.date_dim.inventory", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(2, 1999),
                        new String[] {"d_date_sk", "d_week_seq", "d_year"},
                        0, 1)),
                0));
        return profiled("q72.project.inventory_by_week", projectInputs(allocator, primitiveRegistry, inventory, 0, 1, 3, 5));
    }

    private static Operator query65StoreItemSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator sales = profiled("q65.sales.scan", factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"));
        sales = profiled("q65.sales.join.date", new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(1, 1175), lessThan(1, 1188)),
                        new String[] {"d_date_sk", "d_month_seq"},
                        0),
                0));
        sales = profiled("q65.sales.project", projectInputs(allocator, primitiveRegistry, sales, 2, 1, 3));
        return profiled("q65.sales.group", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2)),
                sales));
    }

    private static Operator query65StoreThresholds(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator grouped = profiled("q65.threshold.group", new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new CountAll(), new Sum(2)),
                query65StoreItemSales(allocator, primitiveRegistry, tables)));
        return profiled("q65.threshold.project", projectQuery65StoreAverage(allocator, primitiveRegistry, grouped));
    }

    private static Operator projectQuery65StoreAverage(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable roundedAverage = new Variable(0);
        List<Assignment> assignments = List.of(
                new Assignment(roundedAverage, new Call("divide_round_i64", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(0), Stream.VALUES),
                new Reference(roundedAverage, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator query66ChannelMonthlyWarehouseRollup(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            String salesTable,
            String soldDateColumn,
            String soldTimeColumn,
            String shipModeColumn,
            String warehouseColumn,
            String quantityColumn,
            String salesPriceColumn,
            String netPaidColumn)
    {
        String profile = "q66." + salesTable;
        Operator sales = profiled(profile + ".scan", factScan(allocator, tables, salesTable, soldDateColumn, soldTimeColumn, shipModeColumn, warehouseColumn, quantityColumn, salesPriceColumn, netPaidColumn));
        sales = profiled(profile + ".join.warehouse", new HashJoinOperator(
                allocator,
                sales,
                3,
                scannedTable(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name", "w_warehouse_sq_ft", "w_city", "w_county", "w_state", "w_country"),
                0));
        sales = profiled(profile + ".join.date", new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 2001),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0, 1, 2),
                0));
        sales = profiled(profile + ".join.time", new HashJoinOperator(
                allocator,
                sales,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "time_dim",
                        and(greaterThan(1, 30_837), lessThan(1, 59_639)),
                        new String[] {"t_time_sk", "t_time"},
                        0),
                0));
        sales = profiled(profile + ".join.ship_mode", new HashJoinOperator(
                allocator,
                sales,
                2,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "ship_mode",
                        utf8AnyOf(1, Set.of("DHL", "BARIAN")),
                        new String[] {"sm_ship_mode_sk", "sm_carrier"},
                        0),
                0));
        if (Boolean.parseBoolean(System.getProperty("nitro.tpcds.query66ConditionalAggregation", "true"))) {
            sales = profiled(profile + ".project.pivot_inputs", projectQuery66PivotInputs(allocator, primitiveRegistry, sales, 8, 9, 10, 11, 12, 13, 15, 16, 4, 5, 6));
            List<org.weakref.nitro.operator.aggregation.Accumulator> sums = new ArrayList<>();
            for (int month = 1; month <= 12; month++) {
                sums.add(new SumProductIfEqual(8, month, 10, 9));
            }
            for (int month = 1; month <= 12; month++) {
                sums.add(new SumProductIfEqual(8, month, 11, 9));
            }
            return profiled(profile + ".group", new GroupedAggregationOperator(
                    allocator,
                    List.of(0, 1, 2, 3, 4, 5, 6, 7),
                    sums,
                    sales));
        }
        sales = profiled(profile + ".project.months", projectQuery66MonthlyBuckets(allocator, primitiveRegistry, sales, 8, 9, 10, 11, 12, 13, 15, 16, 4, 5, 6));
        return profiled(profile + ".group", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5, 6, 7),
                List.of(
                        new Sum(8), new Sum(9), new Sum(10), new Sum(11), new Sum(12), new Sum(13),
                        new Sum(14), new Sum(15), new Sum(16), new Sum(17), new Sum(18), new Sum(19),
                        new Sum(20), new Sum(21), new Sum(22), new Sum(23), new Sum(24), new Sum(25),
                        new Sum(26), new Sum(27), new Sum(28), new Sum(29), new Sum(30), new Sum(31)),
                sales));
    }

    private static Operator projectQuery66PivotInputs(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            Operator source,
            int warehouseNameIndex,
            int warehouseSquareFeetIndex,
            int warehouseCityIndex,
            int warehouseCountyIndex,
            int warehouseStateIndex,
            int warehouseCountryIndex,
            int yearIndex,
            int monthIndex,
            int quantityIndex,
            int salesPriceIndex,
            int netPaidIndex)
    {
        Variable shipCarriers = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(new Assignment(shipCarriers, new Literal("DHL,BARIAN"), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(warehouseNameIndex), Stream.VALUES),
                                new Reference(new Input(warehouseSquareFeetIndex), Stream.VALUES),
                                new Reference(new Input(warehouseCityIndex), Stream.VALUES),
                                new Reference(new Input(warehouseCountyIndex), Stream.VALUES),
                                new Reference(new Input(warehouseStateIndex), Stream.VALUES),
                                new Reference(new Input(warehouseCountryIndex), Stream.VALUES),
                                new Reference(shipCarriers, Stream.VALUES),
                                new Reference(new Input(yearIndex), Stream.VALUES),
                                new Reference(new Input(monthIndex), Stream.VALUES),
                                new Reference(new Input(quantityIndex), Stream.VALUES),
                                new Reference(new Input(salesPriceIndex), Stream.VALUES),
                                new Reference(new Input(netPaidIndex), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery66MonthlyBuckets(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            Operator source,
            int warehouseNameIndex,
            int warehouseSquareFeetIndex,
            int warehouseCityIndex,
            int warehouseCountyIndex,
            int warehouseStateIndex,
            int warehouseCountryIndex,
            int yearIndex,
            int monthIndex,
            int quantityIndex,
            int salesPriceIndex,
            int netPaidIndex)
    {
        Variable shipCarriers = new Variable(0);
        Variable zero = new Variable(1);
        Variable sales = new Variable(2);
        Variable net = new Variable(3);
        List<Assignment> assignments = new ArrayList<>();
        assignments.add(new Assignment(shipCarriers, new Literal("DHL,BARIAN"), AllMask.ALL));
        assignments.add(new Assignment(zero, new Literal(0L), AllMask.ALL));
        assignments.add(new Assignment(sales, new Call("multiply", List.of(
                new Reference(new Input(salesPriceIndex), Stream.VALUES),
                new Reference(new Input(quantityIndex), Stream.VALUES))), AllMask.ALL));
        assignments.add(new Assignment(net, new Call("multiply", List.of(
                new Reference(new Input(netPaidIndex), Stream.VALUES),
                new Reference(new Input(quantityIndex), Stream.VALUES))), AllMask.ALL));

        List<Reference> outputs = new ArrayList<>();
        outputs.add(new Reference(new Input(warehouseNameIndex), Stream.VALUES));
        outputs.add(new Reference(new Input(warehouseSquareFeetIndex), Stream.VALUES));
        outputs.add(new Reference(new Input(warehouseCityIndex), Stream.VALUES));
        outputs.add(new Reference(new Input(warehouseCountyIndex), Stream.VALUES));
        outputs.add(new Reference(new Input(warehouseStateIndex), Stream.VALUES));
        outputs.add(new Reference(new Input(warehouseCountryIndex), Stream.VALUES));
        outputs.add(new Reference(shipCarriers, Stream.VALUES));
        outputs.add(new Reference(new Input(yearIndex), Stream.VALUES));

        for (int month = 1; month <= 12; month++) {
            Variable monthLiteral = new Variable(assignments.size() + 10);
            Variable monthMatch = new Variable(assignments.size() + 11);
            Variable monthSales = new Variable(assignments.size() + 12);
            Variable monthNet = new Variable(assignments.size() + 13);
            assignments.add(new Assignment(monthLiteral, new Literal((long) month), AllMask.ALL));
            assignments.add(new Assignment(monthMatch, new Call("eq", List.of(
                    new Reference(new Input(monthIndex), Stream.VALUES),
                    new Reference(monthLiteral, Stream.VALUES))), AllMask.ALL));
            assignments.add(new Assignment(monthSales, new Call("if_i64", List.of(
                    new Reference(monthMatch, Stream.VALUES),
                    new Reference(sales, Stream.VALUES),
                    new Reference(zero, Stream.VALUES))), AllMask.ALL));
            assignments.add(new Assignment(monthNet, new Call("if_i64", List.of(
                    new Reference(monthMatch, Stream.VALUES),
                    new Reference(net, Stream.VALUES),
                    new Reference(zero, Stream.VALUES))), AllMask.ALL));
            outputs.add(new Reference(monthSales, Stream.VALUES));
        }
        for (int month = 1; month <= 12; month++) {
            Variable monthNet = new Variable(4 + ((month - 1) * 4) + 13);
            outputs.add(new Reference(monthNet, Stream.VALUES));
        }

        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectQuery66Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable[] perSquareFoot = new Variable[12];
        List<Assignment> assignments = new ArrayList<>();
        Variable decimalScale = new Variable(12);
        assignments.add(new Assignment(decimalScale, new Literal(10_000L), AllMask.ALL));
        for (int month = 0; month < 12; month++) {
            perSquareFoot[month] = new Variable(month);
            assignments.add(new Assignment(perSquareFoot[month], new Call("divide_scale_round_i64", List.of(
                    new Reference(new Input(8 + month), Stream.VALUES),
                    new Reference(new Input(1), Stream.VALUES),
                    new Reference(decimalScale, Stream.VALUES))), AllMask.ALL));
        }

        List<Reference> outputs = new ArrayList<>();
        for (int index = 0; index < 8; index++) {
            outputs.add(new Reference(new Input(index), Stream.VALUES));
        }
        for (int index = 8; index < 20; index++) {
            outputs.add(new Reference(new Input(index), Stream.VALUES));
        }
        for (Variable variable : perSquareFoot) {
            outputs.add(new Reference(variable, Stream.VALUES));
        }
        for (int index = 20; index < 32; index++) {
            outputs.add(new Reference(new Input(index), Stream.VALUES));
        }

        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator queryGroupedChannelSalesWithAddressOffset(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            String salesTable,
            String soldDateColumn,
            String itemColumn,
            String addressColumn,
            String salesColumn,
            FilterSpec itemFilter,
            String[] itemColumns,
            int itemKeyInputIndex,
            int year,
            int month,
            boolean qualifyByKeySemiJoin)
    {
        Operator sales = factScan(allocator, tables, salesTable, soldDateColumn, itemColumn, addressColumn, salesColumn);
        if (qualifyByKeySemiJoin) {
            // SQL: i_<key> IN (SELECT i_<key> FROM item WHERE <itemFilter>). Join to ALL items to attach the
            // group key, then semi-join the key against the qualifying key set. Only valid for I64 keys
            // (SemiJoinOperator supports I64 membership only).
            sales = new HashJoinOperator(
                    allocator,
                    sales,
                    1,
                    filteredProjectedTable(allocator, primitiveRegistry, tables, "item", null, itemColumns, 0, itemKeyInputIndex),
                    0).withOutputs(0, 2, 3, 5);
            sales = new SemiJoinOperator(
                    allocator,
                    sales,
                    3,
                    filteredProjectedTable(allocator, primitiveRegistry, tables, "item", itemFilter, itemColumns, itemKeyInputIndex),
                    0);
        }
        else {
            sales = new HashJoinOperator(
                    allocator,
                    sales,
                    1,
                    filteredProjectedTable(allocator, primitiveRegistry, tables, "item", itemFilter, itemColumns, 0, itemKeyInputIndex),
                    0).withOutputs(0, 2, 3, 5);
        }
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, year), equal(2, month)),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0),
                0).withOutputs(1, 2, 3);
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "customer_address",
                        equal(1, -500L),
                        new String[] {"ca_address_sk", "ca_gmt_offset"},
                        0),
                0).withOutputs(1, 2);
        return new GroupedAggregationOperator(
                allocator,
                List.of(1),
                List.of(new Sum(0)),
                sales);
    }

    private static Operator query49Channel(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String channel, String salesTable, String soldDateColumn, String itemColumn, String orderColumn, String quantityColumn, String netPaidColumn, String netProfitColumn, String returnsTable, String returnItemColumn, String returnOrderColumn, String returnQuantityColumn, String returnAmountColumn)
    {
        Operator sales = filteredProjectedScan(
                allocator,
                primitiveRegistry,
                tables,
                salesTable,
                query49SalesPredicate(3, 4, 5),
                new String[] {soldDateColumn, itemColumn, orderColumn, quantityColumn, netPaidColumn, netProfitColumn});
        Operator returns = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                returnsTable,
                greaterThan(3, 1_000_000L),
                new String[] {returnItemColumn, returnOrderColumn, returnQuantityColumn, returnAmountColumn});
        sales = new HashJoinOperator(allocator, sales, new int[] {2, 1}, returns, new int[] {1, 0})
                .withOutputs(0, 1, 3, 4, 8, 9);
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 2001), equal(2, 12)),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0),
                0).withOutputs(1, 2, 3, 4, 5);
        sales = projectQuery49Measures(allocator, primitiveRegistry, sales);
        sales = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new Sum(2), new Sum(3), new Sum(4)),
                sales);
        sales = projectQuery49Ratios(allocator, primitiveRegistry, sales);
        return query49Ranks(allocator, primitiveRegistry, sales, channel);
    }

    private static Operator query75Channel(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String orderColumn, String quantityColumn, String salesAmountColumn, String returnsTable, String returnItemColumn, String returnOrderColumn, String returnQuantityColumn, String returnAmountColumn, String profilePrefix)
    {
        Operator sales = profiled(profilePrefix + ".scan.sales", factScan(allocator, tables, salesTable, soldDateColumn, itemColumn, orderColumn, quantityColumn, salesAmountColumn));
        sales = profiled(profilePrefix + ".join.item", new HashJoinOperator(
                allocator,
                sales,
                1,
                profiled(profilePrefix + ".scan.item", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        equalUtf8(1, "Books"),
                        new String[] {"i_item_sk", "i_category", "i_brand_id", "i_class_id", "i_category_id", "i_manufact_id"},
                        0, 2, 3, 4, 5)),
                0));
        sales = profiled(profilePrefix + ".join.date_dim", new HashJoinOperator(
                allocator,
                sales,
                0,
                profiled(profilePrefix + ".scan.date_dim", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        or(equal(1, 2001), equal(1, 2002)),
                        new String[] {"d_date_sk", "d_year"},
                        0, 1)),
                0));
        sales = profiled(profilePrefix + ".join.returns", new HashJoinOperator(
                allocator,
                sales,
                new int[] {2, 1},
                profiled(profilePrefix + ".scan.returns", scannedTable(allocator, tables, returnsTable, returnItemColumn, returnOrderColumn, returnQuantityColumn, returnAmountColumn)),
                new int[] {1, 0},
                true));
        return profiled(profilePrefix + ".project.net", projectQuery75Sales(allocator, primitiveRegistry, sales));
    }

    private static Operator query75AllSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String profilePrefix)
    {
        return profiled(profilePrefix + ".group.all_sales", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4),
                List.of(new Sum(5), new Sum(6)),
                profiled(profilePrefix + ".distinct.all_sales", new MarkDistinctOperator(allocator, new int[] {0, 1, 2, 3, 4, 5, 6}, new UnionAllOperator(7, List.of(
                        query75Channel(allocator, primitiveRegistry, tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_order_number", "cs_quantity", "cs_ext_sales_price", "catalog_returns", "cr_item_sk", "cr_order_number", "cr_return_quantity", "cr_return_amount", profilePrefix + ".catalog"),
                        query75Channel(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_ticket_number", "ss_quantity", "ss_ext_sales_price", "store_returns", "sr_item_sk", "sr_ticket_number", "sr_return_quantity", "sr_return_amt", profilePrefix + ".store"),
                        query75Channel(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_order_number", "ws_quantity", "ws_ext_sales_price", "web_returns", "wr_item_sk", "wr_order_number", "wr_return_quantity", "wr_return_amt", profilePrefix + ".web"))),
                        true,
                        EngineResources.from(allocator).operatorResources()))));
    }

    private static Operator query78Channel(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String soldDateColumn, String itemColumn, String customerColumn, String orderColumn, String quantityColumn, String wholesaleCostColumn, String salesPriceColumn, String returnsTable, String returnItemColumn, String returnOrderColumn, String profilePrefix)
    {
        Operator sales = profiled(profilePrefix + ".scan.sales", factScan(allocator, tables, salesTable, soldDateColumn, itemColumn, customerColumn, orderColumn, quantityColumn, wholesaleCostColumn, salesPriceColumn));
        HashJoinOperator returnsJoin = new HashJoinOperator(
                allocator,
                sales,
                new int[] {3, 1},
                profiled(profilePrefix + ".scan.returns", scannedTable(allocator, tables, returnsTable, returnItemColumn, returnOrderColumn)),
                new int[] {1, 0},
                true);
        if (QUERY78_COMPACT_JOIN_LAYOUTS) {
            returnsJoin = returnsJoin.withOutputs(0, 1, 2, 4, 5, 6, 8);
        }
        sales = profiled(profilePrefix + ".join.returns", returnsJoin);
        sales = profiled(profilePrefix + ".filter.not_returned", filter(allocator, primitiveRegistry, sales, isNull(QUERY78_COMPACT_JOIN_LAYOUTS ? 6 : 8)));
        HashJoinOperator dateJoin = new HashJoinOperator(
                allocator,
                sales,
                0,
                profiled(profilePrefix + ".scan.date_dim", filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        equal(1, 2000),
                        new String[] {"d_date_sk", "d_year"},
                        0, 1)),
                0);
        if (QUERY78_COMPACT_JOIN_LAYOUTS) {
            dateJoin = dateJoin.withOutputs(8, 1, 2, 3, 4, 5);
        }
        sales = profiled(profilePrefix + ".join.date_dim", dateJoin);
        if (!QUERY78_COMPACT_JOIN_LAYOUTS) {
            sales = profiled(profilePrefix + ".project.group_inputs", projectInputs(allocator, primitiveRegistry, sales, 10, 1, 2, 4, 5, 6));
        }
        sales = profiled(profilePrefix + ".group.partial", new SqlStageAggregationOperator(
                allocator,
                sales,
                2,
                new int[0],
                List.of(SqlStageAggregationOperator.aggregate(
                        List.of(0, 1, 2),
                        () -> List.of(new Sum(3), new Sum(4), new Sum(5))))));
        return profiled(profilePrefix + ".group.final", new SqlStageAggregationOperator(
                allocator,
                sales,
                2,
                new int[] {0, 1, 2},
                List.of(SqlStageAggregationOperator.aggregate(
                        List.of(0, 1, 2),
                        () -> List.of(new Sum(3), new Sum(4), new Sum(5))))));
    }

    private static Operator projectQuery47CurrentRows(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        source = filter(allocator, primitiveRegistry, source, equal(4, 1999));
        source = new WindowOperator(
                allocator,
                source,
                new int[] {0, 1, 2, 3},
                new int[0],
                new boolean[0],
                List.of(new PartitionAverageI64WindowFunction(6)));
        return projectInputs(allocator, primitiveRegistry, source, 0, 1, 2, 3, 4, 5, 8, 6, 7);
    }

    private static Operator projectQuery47AdjacentRows(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, boolean previous)
    {
        Variable one = new Variable(0);
        Variable adjustedRank = new Variable(1);
        List<Assignment> assignments = List.of(
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(adjustedRank, new Call(previous ? "add" : "subtract", List.of(
                        new Reference(new Input(7), Stream.VALUES),
                        new Reference(one, Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(0), Stream.VALUES),
                new Reference(new Input(1), Stream.VALUES),
                new Reference(new Input(2), Stream.VALUES),
                new Reference(new Input(3), Stream.VALUES),
                new Reference(new Input(6), Stream.VALUES),
                new Reference(adjustedRank, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectQuery70Rollup(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable two = new Variable(2);
        Variable groupIdIsZero = new Variable(3);
        Variable groupIdIsOne = new Variable(4);
        Variable groupIdIsTwo = new Variable(5);
        Variable stateSubtotalHierarchy = new Variable(6);
        Variable hierarchyLong = new Variable(7);
        Variable hierarchy = new Variable(8);
        Variable sentinel = new Variable(9);
        Variable stateForRank = new Variable(10);

        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(two, new Literal(2L), AllMask.ALL),
                new Assignment(groupIdIsZero, new Call("eq", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(groupIdIsOne, new Call("eq", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(one, Stream.VALUES))), AllMask.ALL),
                new Assignment(groupIdIsTwo, new Call("eq", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(two, Stream.VALUES))), AllMask.ALL),
                new Assignment(stateSubtotalHierarchy, new Call("if_i64", List.of(
                        new Reference(groupIdIsOne, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(hierarchyLong, new Call("if_i64", List.of(
                        new Reference(groupIdIsZero, Stream.VALUES),
                        new Reference(two, Stream.VALUES),
                        new Reference(stateSubtotalHierarchy, Stream.VALUES))), AllMask.ALL),
                new Assignment(hierarchy, new Call("cast_i64_to_i32", List.of(
                        new Reference(hierarchyLong, Stream.VALUES))), AllMask.ALL),
                new Assignment(sentinel, new Literal(NULLS_LAST_SENTINEL_STRING), AllMask.ALL),
                new Assignment(stateForRank, new Call("if_utf8", List.of(
                        new Reference(groupIdIsTwo, Stream.VALUES),
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(sentinel, Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(hierarchy, Stream.VALUES),
                        new Reference(stateForRank, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery49Measures(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable returnQuantityIsNull = new Variable(1);
        Variable returnQuantity = new Variable(2);
        Variable returnAmountIsNull = new Variable(3);
        Variable returnAmount = new Variable(4);

        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(returnQuantityIsNull, new Call("is_null_i64", List.of(
                                new Reference(new Input(3), Stream.VALUES))), AllMask.ALL),
                        new Assignment(returnQuantity, new Call("if_i64", List.of(
                                new Reference(returnQuantityIsNull, Stream.VALUES),
                                new Reference(zero, Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES))), AllMask.ALL),
                        new Assignment(returnAmountIsNull, new Call("is_null_i64", List.of(
                                new Reference(new Input(4), Stream.VALUES))), AllMask.ALL),
                        new Assignment(returnAmount, new Call("if_i64", List.of(
                                new Reference(returnAmountIsNull, Stream.VALUES),
                                new Reference(zero, Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(returnQuantity, Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(returnAmount, Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery49Ratios(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable scale = new Variable(0);
        Variable returnRatio = new Variable(1);
        Variable currencyRatio = new Variable(2);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(scale, new Literal(1_000_000_000_000L), AllMask.ALL),
                        new Assignment(returnRatio, new Call("divide_scale_round_i64", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(scale, Stream.VALUES))), AllMask.ALL),
                        new Assignment(currencyRatio, new Call("divide_scale_round_i64", List.of(
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(scale, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(returnRatio, Stream.VALUES),
                                new Reference(currencyRatio, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query49Ranks(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, String channel)
    {
        Operator ranked = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[] {1},
                new boolean[] {false},
                List.of(new RankWindowFunction(new int[] {1}, new boolean[] {false})));
        ranked = new WindowOperator(
                allocator,
                ranked,
                new int[0],
                new int[] {2},
                new boolean[] {false},
                List.of(new RankWindowFunction(new int[] {2}, new boolean[] {false})));
        ranked = filter(allocator, primitiveRegistry, ranked, query49TopRankPredicate(3, 4));

        Variable channelName = new Variable(0);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(
                        new Assignment(channelName, new Literal(channel), AllMask.ALL)),
                        List.of(
                                new Reference(channelName, Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES))),
                primitiveRegistry,
                ranked);
    }

    private static Operator projectQuery36Rollup(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable two = new Variable(2);
        Variable scale = new Variable(3);
        Variable groupIdIsZero = new Variable(4);
        Variable groupIdIsOne = new Variable(5);
        Variable groupIdIsTwo = new Variable(6);
        Variable classSubtotalHierarchy = new Variable(7);
        Variable hierarchyLong = new Variable(8);
        Variable hierarchy = new Variable(9);
        Variable sentinel = new Variable(10);
        Variable categoryForRank = new Variable(11);
        Variable grossMargin = new Variable(12);

        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(two, new Literal(2L), AllMask.ALL),
                new Assignment(scale, new Literal(1_000_000L), AllMask.ALL),
                new Assignment(groupIdIsZero, new Call("eq", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(groupIdIsOne, new Call("eq", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(one, Stream.VALUES))), AllMask.ALL),
                new Assignment(groupIdIsTwo, new Call("eq", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(two, Stream.VALUES))), AllMask.ALL),
                new Assignment(classSubtotalHierarchy, new Call("if_i64", List.of(
                        new Reference(groupIdIsOne, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(hierarchyLong, new Call("if_i64", List.of(
                        new Reference(groupIdIsZero, Stream.VALUES),
                        new Reference(two, Stream.VALUES),
                        new Reference(classSubtotalHierarchy, Stream.VALUES))), AllMask.ALL),
                new Assignment(hierarchy, new Call("cast_i64_to_i32", List.of(
                        new Reference(hierarchyLong, Stream.VALUES))), AllMask.ALL),
                new Assignment(sentinel, new Literal(NULLS_LAST_SENTINEL_STRING), AllMask.ALL),
                new Assignment(categoryForRank, new Call("if_utf8", List.of(
                        new Reference(groupIdIsTwo, Stream.VALUES),
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(sentinel, Stream.VALUES))), AllMask.ALL),
                new Assignment(grossMargin, new Call("divide_scale_round_i64", List.of(
                        new Reference(new Input(4), Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(scale, Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(grossMargin, Stream.VALUES),
                        new Reference(hierarchy, Stream.VALUES),
                        new Reference(categoryForRank, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery86Rollup(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable two = new Variable(2);
        Variable groupIdIsZero = new Variable(3);
        Variable groupIdIsOne = new Variable(4);
        Variable groupIdIsTwo = new Variable(5);
        Variable classSubtotalHierarchy = new Variable(6);
        Variable hierarchyLong = new Variable(7);
        Variable hierarchy = new Variable(8);
        Variable sentinel = new Variable(9);
        Variable categoryForRank = new Variable(10);

        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(two, new Literal(2L), AllMask.ALL),
                new Assignment(groupIdIsZero, new Call("eq", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(groupIdIsOne, new Call("eq", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(one, Stream.VALUES))), AllMask.ALL),
                new Assignment(groupIdIsTwo, new Call("eq", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(two, Stream.VALUES))), AllMask.ALL),
                new Assignment(classSubtotalHierarchy, new Call("if_i64", List.of(
                        new Reference(groupIdIsOne, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(hierarchyLong, new Call("if_i64", List.of(
                        new Reference(groupIdIsZero, Stream.VALUES),
                        new Reference(two, Stream.VALUES),
                        new Reference(classSubtotalHierarchy, Stream.VALUES))), AllMask.ALL),
                new Assignment(hierarchy, new Call("cast_i64_to_i32", List.of(
                        new Reference(hierarchyLong, Stream.VALUES))), AllMask.ALL),
                new Assignment(sentinel, new Literal(NULLS_LAST_SENTINEL_STRING), AllMask.ALL),
                new Assignment(categoryForRank, new Call("if_utf8", List.of(
                        new Reference(groupIdIsTwo, Stream.VALUES),
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(sentinel, Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(hierarchy, Stream.VALUES),
                        new Reference(categoryForRank, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery67RollupKey(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable priceIsNull = new Variable(1);
        Variable quantityIsNull = new Variable(2);
        Variable anyNull = new Variable(3);
        Variable multiplied = new Variable(4);
        Variable sales = new Variable(5);
        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(priceIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(4), Stream.VALUES))), AllMask.ALL),
                new Assignment(quantityIsNull, new Call("is_null_i32", List.of(
                        new Reference(new Input(3), Stream.VALUES))), AllMask.ALL),
                new Assignment(anyNull, new Call("or", List.of(
                        new Reference(priceIsNull, Stream.VALUES),
                        new Reference(quantityIsNull, Stream.VALUES))), AllMask.ALL),
                new Assignment(multiplied, new Call("multiply", List.of(
                        new Reference(new Input(4), Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES))), AllMask.ALL),
                new Assignment(sales, new Call("if_i64", List.of(
                        new Reference(anyNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(multiplied, Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(new Input(14), Stream.VALUES),
                        new Reference(new Input(13), Stream.VALUES),
                        new Reference(new Input(12), Stream.VALUES),
                        new Reference(new Input(15), Stream.VALUES),
                        new Reference(new Input(6), Stream.VALUES),
                        new Reference(new Input(7), Stream.VALUES),
                        new Reference(new Input(8), Stream.VALUES),
                        new Reference(new Input(10), Stream.VALUES),
                        new Reference(sales, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query97Channel(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String profilePrefix, String salesTable, String customerColumn, String itemColumn, String soldDateColumn)
    {
        Operator facts = profiled(profilePrefix + ".scan.sales", factScan(allocator, tables, salesTable, customerColumn, itemColumn, soldDateColumn));
        Operator allowedDates = profiled(profilePrefix + ".scan.date_dim", filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                and(greaterThan(1, 1199), lessThan(1, 1212)),
                new String[] {"d_date_sk", "d_month_seq"},
                0));
        facts = profiled(profilePrefix + ".join.date_dim", new HashJoinOperator(allocator, facts, 2, allowedDates, 0));
        // The customer key never participates in the FULL OUTER JOIN emulation when it is null
        // (NULL = NULL is false), so discard null customer keys before marking distinct, matching the
        // Trino plan. This keeps both harnesses pushing the same row count through the distinct.
        facts = profiled(profilePrefix + ".filter.non_null_customer", filter(allocator, primitiveRegistry, facts, isNotNullI64(0)));
        facts = profiled(profilePrefix + ".project.keys", projectInputs(allocator, primitiveRegistry, facts, 0, 1));
        return profiled(profilePrefix + ".distinct.mark_distinct", new MarkDistinctOperator(allocator, new int[] {0, 1}, facts, EngineResources.from(allocator).operatorResources()));
    }

    private static Operator query97PresenceChannel(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String profilePrefix, String salesTable, String customerColumn, String itemColumn, String soldDateColumn, boolean storeChannel)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable channelId = new Variable(2);
        Variable customerIsNull = new Variable(3);
        Variable itemIsNull = new Variable(4);
        Variable keyIsNull = new Variable(5);
        Variable discriminator = new Variable(6);
        Operator source = query97Channel(allocator, primitiveRegistry, tables, profilePrefix, salesTable, customerColumn, itemColumn, soldDateColumn);

        // Emulating the FULL OUTER JOIN with a UNION ALL plus group-by requires a per-channel
        // discriminator for rows with a null join key, since such rows never join and must remain
        // distinct groups. This matches the Trino plan and avoids collapsing null-keyed rows across
        // channels.
        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(channelId, new Literal(storeChannel ? 1L : 2L), AllMask.ALL),
                new Assignment(customerIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                new Assignment(itemIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                new Assignment(keyIsNull, new Call("or", List.of(
                        new Reference(customerIsNull, Stream.VALUES),
                        new Reference(itemIsNull, Stream.VALUES))), AllMask.ALL),
                new Assignment(discriminator, new Call("if_i64", List.of(
                        new Reference(keyIsNull, Stream.VALUES),
                        new Reference(channelId, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL));
        return profiled(profilePrefix + ".project.presence", new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(discriminator, Stream.VALUES),
                        storeChannel ? new Reference(one, Stream.VALUES) : new Reference(zero, Stream.VALUES),
                        storeChannel ? new Reference(zero, Stream.VALUES) : new Reference(one, Stream.VALUES))),
                primitiveRegistry,
                source));
    }

    private static Operator projectQuery97PresenceIndicators(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable storePresent = new Variable(2);
        Variable catalogPresent = new Variable(3);
        Variable storeMissing = new Variable(4);
        Variable catalogMissing = new Variable(5);
        Variable storeOnlyCondition = new Variable(6);
        Variable catalogOnlyCondition = new Variable(7);
        Variable storeAndCatalogCondition = new Variable(8);
        Variable storeOnly = new Variable(9);
        Variable catalogOnly = new Variable(10);
        Variable storeAndCatalog = new Variable(11);

        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(storePresent, new Call("lt", List.of(
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogPresent, new Call("lt", List.of(
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(4), Stream.VALUES))), AllMask.ALL),
                new Assignment(storeMissing, new Call("eq", List.of(
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogMissing, new Call("eq", List.of(
                        new Reference(new Input(4), Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(storeOnlyCondition, new Call("and", List.of(
                        new Reference(storePresent, Stream.VALUES),
                        new Reference(catalogMissing, Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogOnlyCondition, new Call("and", List.of(
                        new Reference(storeMissing, Stream.VALUES),
                        new Reference(catalogPresent, Stream.VALUES))), AllMask.ALL),
                new Assignment(storeAndCatalogCondition, new Call("and", List.of(
                        new Reference(storePresent, Stream.VALUES),
                        new Reference(catalogPresent, Stream.VALUES))), AllMask.ALL),
                new Assignment(storeOnly, new Call("if_i64", List.of(
                        new Reference(storeOnlyCondition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogOnly, new Call("if_i64", List.of(
                        new Reference(catalogOnlyCondition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(storeAndCatalog, new Call("if_i64", List.of(
                        new Reference(storeAndCatalogCondition, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(storeOnly, Stream.VALUES),
                        new Reference(catalogOnly, Stream.VALUES),
                        new Reference(storeAndCatalog, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static FilterSpec query51CumulativePredicate(int webCumulativeIndex, int storeCumulativeIndex)
    {
        Variable greaterThan = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(greaterThan, new Call("lt", List.of(
                        new Reference(new Input(storeCumulativeIndex), Stream.VALUES),
                        new Reference(new Input(webCumulativeIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(greaterThan, Stream.VALUES)));
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

    private static FilterSpec notEmptyUtf8(int inputIndex)
    {
        Variable zero = new Variable(0);
        Variable length = new Variable(1);
        Variable notEmpty = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(length, new Call("length_utf8", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(notEmpty, new Call("lt", List.of(
                        new Reference(zero, Stream.VALUES),
                        new Reference(length, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(notEmpty, Stream.VALUES)));
    }

    private static FilterSpec utf8StartsWith(int inputIndex, String prefix)
    {
        Variable literal = new Variable(0);
        Variable start = new Variable(1);
        Variable length = new Variable(2);
        Variable substring = new Variable(3);
        Variable equals = new Variable(4);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(prefix), AllMask.ALL),
                new Assignment(start, new Literal(1L), AllMask.ALL),
                new Assignment(length, new Literal((long) prefix.length()), AllMask.ALL),
                new Assignment(substring, new Call("substring_utf8", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(start, Stream.VALUES),
                        new Reference(length, Stream.VALUES))), AllMask.ALL),
                new Assignment(equals, new Call("eq_utf8", List.of(
                        new Reference(substring, Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(equals, Stream.VALUES)));
    }

    private static FilterSpec greaterThan(int inputIndex, long constant)
    {
        Variable literal = new Variable(0);
        Variable lessThan = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(lessThan, new Call("lt", List.of(
                        new Reference(literal, Stream.VALUES),
                        new Reference(new Input(inputIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(lessThan, Stream.VALUES)));
    }

    private static FilterSpec greaterThanF64(int inputIndex, double constant)
    {
        Variable literal = new Variable(0);
        Variable greaterThan = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(greaterThan, new Call("gt_f64", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(greaterThan, Stream.VALUES)));
    }

    private static FilterSpec equal(int inputIndex, long constant)
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(equals, new Call("eq", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(equals, Stream.VALUES)));
    }

    private static FilterSpec equalColumns(int leftInputIndex, int rightInputIndex)
    {
        Variable equals = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(equals, new Call("eq", List.of(
                        new Reference(new Input(leftInputIndex), Stream.VALUES),
                        new Reference(new Input(rightInputIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(equals, Stream.VALUES)));
    }

    private static FilterSpec notEqualColumns(int leftInputIndex, int rightInputIndex)
    {
        Variable equals = new Variable(0);
        Variable notEquals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(equals, new Call("eq", List.of(
                        new Reference(new Input(leftInputIndex), Stream.VALUES),
                        new Reference(new Input(rightInputIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(notEquals, new Call("not", List.of(
                        new Reference(equals, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(
                plan,
                notMask(equals),
                new Reference(notEquals, Stream.VALUES));
    }

    private static Operator projectQuery87ChannelPresence(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int activeChannel)
    {
        long storeFlagValue = activeChannel == 0 ? 1L : 0L;
        long catalogFlagValue = activeChannel == 1 ? 1L : 0L;
        long webFlagValue = activeChannel == 2 ? 1L : 0L;
        Variable storeFlag = new Variable(0);
        Variable catalogFlag = new Variable(1);
        Variable webFlag = new Variable(2);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(storeFlag, new Literal(storeFlagValue), AllMask.ALL),
                                new Assignment(catalogFlag, new Literal(catalogFlagValue), AllMask.ALL),
                                new Assignment(webFlag, new Literal(webFlagValue), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(storeFlag, Stream.VALUES),
                                new Reference(catalogFlag, Stream.VALUES),
                                new Reference(webFlag, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static FilterSpec query85DemographicsAndPricePredicate(int refundedMaritalStatusIndex, int refundedEducationIndex, int returningMaritalStatusIndex, int returningEducationIndex, int salesPriceIndex)
    {
        FilterSpec marriedBranch = and(
                equalUtf8(refundedMaritalStatusIndex, "M"),
                equalUtf8Columns(refundedMaritalStatusIndex, returningMaritalStatusIndex),
                equalUtf8(refundedEducationIndex, "Advanced Degree"),
                equalUtf8Columns(refundedEducationIndex, returningEducationIndex),
                greaterThan(salesPriceIndex, 9_999),
                lessThan(salesPriceIndex, 15_001));
        FilterSpec singleBranch = and(
                equalUtf8(refundedMaritalStatusIndex, "S"),
                equalUtf8Columns(refundedMaritalStatusIndex, returningMaritalStatusIndex),
                equalUtf8(refundedEducationIndex, "College"),
                equalUtf8Columns(refundedEducationIndex, returningEducationIndex),
                greaterThan(salesPriceIndex, 4_999),
                lessThan(salesPriceIndex, 10_001));
        FilterSpec widowedBranch = and(
                equalUtf8(refundedMaritalStatusIndex, "W"),
                equalUtf8Columns(refundedMaritalStatusIndex, returningMaritalStatusIndex),
                equalUtf8(refundedEducationIndex, "2 yr Degree"),
                equalUtf8Columns(refundedEducationIndex, returningEducationIndex),
                greaterThan(salesPriceIndex, 14_999),
                lessThan(salesPriceIndex, 20_001));
        return or(marriedBranch, singleBranch, widowedBranch);
    }

    private static FilterSpec query85AddressProfitPredicate(int countryIndex, int stateIndex, int netProfitIndex)
    {
        FilterSpec firstBranch = and(
                equalUtf8(countryIndex, "United States"),
                utf8AnyOf(stateIndex, Set.of("IN", "OH", "NJ")),
                greaterThan(netProfitIndex, 9_999),
                lessThan(netProfitIndex, 20_001));
        FilterSpec secondBranch = and(
                equalUtf8(countryIndex, "United States"),
                utf8AnyOf(stateIndex, Set.of("WI", "CT", "KY")),
                greaterThan(netProfitIndex, 14_999),
                lessThan(netProfitIndex, 30_001));
        FilterSpec thirdBranch = and(
                equalUtf8(countryIndex, "United States"),
                utf8AnyOf(stateIndex, Set.of("LA", "IA", "AR")),
                greaterThan(netProfitIndex, 4_999),
                lessThan(netProfitIndex, 25_001));
        return or(firstBranch, secondBranch, thirdBranch);
    }

    private static Operator projectQuery85ReasonOutput(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int reasonIndex, int quantityAverageIndex, int refundedCashAverageIndex, int feeAverageIndex)
    {
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable prefix = new Variable(2);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(start, new Literal(1L), AllMask.ALL),
                                new Assignment(length, new Literal(20L), AllMask.ALL),
                                new Assignment(prefix, new Call("substring_utf8", List.of(
                                        new Reference(new Input(reasonIndex), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(prefix, Stream.VALUES),
                                new Reference(new Input(quantityAverageIndex), Stream.VALUES),
                                new Reference(new Input(refundedCashAverageIndex), Stream.VALUES),
                                new Reference(new Input(feeAverageIndex), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery78Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable hundred = new Variable(1);
        Variable webQuantity = new Variable(2);
        Variable catalogQuantity = new Variable(3);
        Variable otherQuantity = new Variable(4);
        Variable webWholesaleCost = new Variable(5);
        Variable catalogWholesaleCost = new Variable(6);
        Variable otherWholesaleCost = new Variable(7);
        Variable webSalesPrice = new Variable(8);
        Variable catalogSalesPrice = new Variable(9);
        Variable otherSalesPrice = new Variable(10);
        Variable ratio = new Variable(11);

        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(zero, new Literal(0L), AllMask.ALL),
                                new Assignment(hundred, new Literal(100L), AllMask.ALL),
                                new Assignment(webQuantity, new Call("coalesce_i64", List.of(
                                        new Reference(new Input(9), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(catalogQuantity, new Call("coalesce_i64", List.of(
                                        new Reference(new Input(15), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(otherQuantity, new Call("add", List.of(
                                        new Reference(webQuantity, Stream.VALUES),
                                        new Reference(catalogQuantity, Stream.VALUES))), AllMask.ALL),
                                new Assignment(webWholesaleCost, new Call("coalesce_i64", List.of(
                                        new Reference(new Input(10), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(catalogWholesaleCost, new Call("coalesce_i64", List.of(
                                        new Reference(new Input(16), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(otherWholesaleCost, new Call("add", List.of(
                                        new Reference(webWholesaleCost, Stream.VALUES),
                                        new Reference(catalogWholesaleCost, Stream.VALUES))), AllMask.ALL),
                                new Assignment(webSalesPrice, new Call("coalesce_i64", List.of(
                                        new Reference(new Input(11), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(catalogSalesPrice, new Call("coalesce_i64", List.of(
                                        new Reference(new Input(17), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(otherSalesPrice, new Call("add", List.of(
                                        new Reference(webSalesPrice, Stream.VALUES),
                                        new Reference(catalogSalesPrice, Stream.VALUES))), AllMask.ALL),
                                new Assignment(ratio, new Call("divide_scale_round_i64", List.of(
                                        new Reference(new Input(3), Stream.VALUES),
                                        new Reference(otherQuantity, Stream.VALUES),
                                        new Reference(hundred, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(ratio, Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(otherQuantity, Stream.VALUES),
                                new Reference(otherWholesaleCost, Stream.VALUES),
                                new Reference(otherSalesPrice, Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery78CompactOutput(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable hundred = new Variable(1);
        Variable webQuantity = new Variable(2);
        Variable catalogQuantity = new Variable(3);
        Variable otherQuantity = new Variable(4);
        Variable webWholesaleCost = new Variable(5);
        Variable catalogWholesaleCost = new Variable(6);
        Variable otherWholesaleCost = new Variable(7);
        Variable webSalesPrice = new Variable(8);
        Variable catalogSalesPrice = new Variable(9);
        Variable otherSalesPrice = new Variable(10);
        Variable ratio = new Variable(11);

        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(zero, new Literal(0L), AllMask.ALL),
                                new Assignment(hundred, new Literal(100L), AllMask.ALL),
                                new Assignment(webQuantity, new Call("coalesce_i64", List.of(new Reference(new Input(6), Stream.VALUES), new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(catalogQuantity, new Call("coalesce_i64", List.of(new Reference(new Input(9), Stream.VALUES), new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(otherQuantity, new Call("add", List.of(new Reference(webQuantity, Stream.VALUES), new Reference(catalogQuantity, Stream.VALUES))), AllMask.ALL),
                                new Assignment(webWholesaleCost, new Call("coalesce_i64", List.of(new Reference(new Input(7), Stream.VALUES), new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(catalogWholesaleCost, new Call("coalesce_i64", List.of(new Reference(new Input(10), Stream.VALUES), new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(otherWholesaleCost, new Call("add", List.of(new Reference(webWholesaleCost, Stream.VALUES), new Reference(catalogWholesaleCost, Stream.VALUES))), AllMask.ALL),
                                new Assignment(webSalesPrice, new Call("coalesce_i64", List.of(new Reference(new Input(8), Stream.VALUES), new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(catalogSalesPrice, new Call("coalesce_i64", List.of(new Reference(new Input(11), Stream.VALUES), new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(otherSalesPrice, new Call("add", List.of(new Reference(webSalesPrice, Stream.VALUES), new Reference(catalogSalesPrice, Stream.VALUES))), AllMask.ALL),
                                new Assignment(ratio, new Call("divide_scale_round_i64", List.of(new Reference(new Input(3), Stream.VALUES), new Reference(otherQuantity, Stream.VALUES), new Reference(hundred, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(ratio, Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(otherQuantity, Stream.VALUES),
                                new Reference(otherWholesaleCost, Stream.VALUES),
                                new Reference(otherSalesPrice, Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator projectQuery75Sales(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable returnedQuantity = new Variable(1);
        Variable netQuantity = new Variable(2);
        Variable returnedAmount = new Variable(3);
        Variable netSalesAmount = new Variable(4);

        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(zero, new Literal(0L), AllMask.ALL),
                                new Assignment(returnedQuantity, new Call("coalesce_i64", List.of(
                                        new Reference(new Input(14), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(netQuantity, new Call("subtract", List.of(
                                        new Reference(new Input(3), Stream.VALUES),
                                        new Reference(returnedQuantity, Stream.VALUES))), AllMask.ALL),
                                new Assignment(returnedAmount, new Call("coalesce_i64", List.of(
                                        new Reference(new Input(15), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                                new Assignment(netSalesAmount, new Call("subtract", List.of(
                                        new Reference(new Input(4), Stream.VALUES),
                                        new Reference(returnedAmount, Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(11), Stream.VALUES),
                                new Reference(new Input(6), Stream.VALUES),
                                new Reference(new Input(7), Stream.VALUES),
                                new Reference(new Input(8), Stream.VALUES),
                                new Reference(new Input(9), Stream.VALUES),
                                new Reference(netQuantity, Stream.VALUES),
                                new Reference(netSalesAmount, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static FilterSpec query75CountRatioPredicate(int currentCountIndex, int previousCountIndex)
    {
        Variable ten = new Variable(0);
        Variable nine = new Variable(1);
        Variable scaledCurrent = new Variable(2);
        Variable scaledPrevious = new Variable(3);
        Variable accepted = new Variable(4);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(ten, new Literal(10L), AllMask.ALL),
                new Assignment(nine, new Literal(9L), AllMask.ALL),
                new Assignment(scaledCurrent, new Call("multiply", List.of(
                        new Reference(new Input(currentCountIndex), Stream.VALUES),
                        new Reference(ten, Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledPrevious, new Call("multiply", List.of(
                        new Reference(new Input(previousCountIndex), Stream.VALUES),
                        new Reference(nine, Stream.VALUES))), AllMask.ALL),
                new Assignment(accepted, new Call("lt", List.of(
                        new Reference(scaledCurrent, Stream.VALUES),
                        new Reference(scaledPrevious, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(accepted, Stream.VALUES)));
    }

    private static Operator projectQuery75Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable salesCountDifference = new Variable(0);
        Variable salesAmountDifference = new Variable(1);
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(salesCountDifference, new Call("subtract", List.of(
                                        new Reference(new Input(5), Stream.VALUES),
                                        new Reference(new Input(12), Stream.VALUES))), AllMask.ALL),
                                new Assignment(salesAmountDifference, new Call("subtract", List.of(
                                        new Reference(new Input(6), Stream.VALUES),
                                        new Reference(new Input(13), Stream.VALUES))), AllMask.ALL)),
                        List.of(
                                new Reference(new Input(7), Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(12), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES),
                                new Reference(salesCountDifference, Stream.VALUES),
                                new Reference(salesAmountDifference, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static FilterSpec equalUtf8Columns(int leftInputIndex, int rightInputIndex)
    {
        Variable equals = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(equals, new Call("eq_utf8", List.of(
                        new Reference(new Input(leftInputIndex), Stream.VALUES),
                        new Reference(new Input(rightInputIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(equals, Stream.VALUES)));
    }

    private static FilterSpec notEqualUtf8Columns(int leftInputIndex, int rightInputIndex)
    {
        Variable equals = new Variable(0);
        Variable notEquals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(equals, new Call("eq_utf8", List.of(
                        new Reference(new Input(leftInputIndex), Stream.VALUES),
                        new Reference(new Input(rightInputIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(notEquals, new Call("not", List.of(
                        new Reference(equals, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(
                plan,
                notMask(equals),
                new Reference(notEquals, Stream.VALUES));
    }

    private static FilterSpec isNull(int inputIndex)
    {
        Variable isNull = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(isNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(isNull, Stream.VALUES)));
    }

    private static FilterSpec isNotNullI64(int inputIndex)
    {
        Variable isNull = new Variable(0);
        Variable notNull = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(isNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(notNull, new Call("not", List.of(
                        new Reference(isNull, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(
                plan,
                notMask(isNull),
                new Reference(notNull, Stream.VALUES));
    }

    private static FilterSpec lessThan(int inputIndex, long constant)
    {
        Variable literal = new Variable(0);
        Variable lessThan = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(lessThan, new Call("lt", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(lessThan, Stream.VALUES)));
    }

    private static FilterSpec betweenInclusive(int inputIndex, long minimumInclusive, long maximumInclusive)
    {
        return and(greaterThan(inputIndex, minimumInclusive - 1), lessThan(inputIndex, maximumInclusive + 1));
    }

    private static FilterSpec anyOf(int inputIndex, long... values)
    {
        if (values.length == 0) {
            throw new IllegalArgumentException("values is empty");
        }

        FilterSpec filter = equal(inputIndex, values[0]);
        for (int index = 1; index < values.length; index++) {
            filter = or(filter, equal(inputIndex, values[index]));
        }
        return filter;
    }

    private static FilterSpec and(FilterSpec first, FilterSpec second, FilterSpec... rest)
    {
        FilterSpec result = combineAnd(first, second);
        for (FilterSpec filterSpec : rest) {
            result = combineAnd(result, filterSpec);
        }
        return result;
    }

    private static FilterSpec or(FilterSpec first, FilterSpec second, FilterSpec... rest)
    {
        FilterSpec result = combineOr(first, second);
        for (FilterSpec filterSpec : rest) {
            result = combineOr(result, filterSpec);
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
        int rightOffset = maxVariableId(left.plan()) + 1;
        List<Assignment> assignments = new java.util.ArrayList<>(left.plan().assignments());
        assignments.addAll(remap(right.plan().assignments(), rightOffset));

        Variable result = new Variable(maxVariableId(assignments) + 1);
        Reference leftValue = left.requireMaterializedValue();
        Reference rightValue = remap(right.requireMaterializedValue(), rightOffset);
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
        List<Assignment> remapped = new java.util.ArrayList<>(assignments.size());
        for (Assignment assignment : assignments) {
            remapped.add(new Assignment(
                    new Variable(assignment.output().id() + variableOffset),
                    remap(assignment.operation(), variableOffset),
                    remap(assignment.mask(), variableOffset)));
        }
        return List.copyOf(remapped);
    }

    private static org.weakref.nitro.operator.evaluator.ir.Operation remap(org.weakref.nitro.operator.evaluator.ir.Operation operation, int variableOffset)
    {
        return switch (operation) {
            case Literal literal -> literal;
            case Call(String functionName, List<Reference> arguments, var resolvedCall) ->
                    new Call(functionName, remapReferences(arguments, variableOffset), resolvedCall);
            case org.weakref.nitro.operator.evaluator.ir.Coalesce(Reference first, Reference second) ->
                    new org.weakref.nitro.operator.evaluator.ir.Coalesce(
                            remap(first, variableOffset),
                            remap(second, variableOffset));
            case org.weakref.nitro.operator.evaluator.ir.Conditional(Reference condition, Reference whenTrue, Reference whenFalse) ->
                    new org.weakref.nitro.operator.evaluator.ir.Conditional(
                            remap(condition, variableOffset),
                            remap(whenTrue, variableOffset),
                            remap(whenFalse, variableOffset));
            case org.weakref.nitro.operator.evaluator.ir.Construct(var type, List<Reference> arguments) ->
                    new org.weakref.nitro.operator.evaluator.ir.Construct(
                            type,
                            remapReferences(arguments, variableOffset));
            case org.weakref.nitro.operator.evaluator.ir.Copy(Reference source) -> new org.weakref.nitro.operator.evaluator.ir.Copy(remap(source, variableOffset));
            case org.weakref.nitro.operator.evaluator.ir.Sequence(Reference first, Reference result) ->
                    new org.weakref.nitro.operator.evaluator.ir.Sequence(
                            remap(first, variableOffset),
                            remap(result, variableOffset));
            case org.weakref.nitro.operator.evaluator.ir.StructField(Reference source, int field) -> new org.weakref.nitro.operator.evaluator.ir.StructField(remap(source, variableOffset), field);
            case org.weakref.nitro.operator.evaluator.ir.Merge _ -> throw new UnsupportedOperationException("Merge remapping is not implemented for TPC-DS helper filters");
        };
    }

    private static List<Reference> remapReferences(List<Reference> references, int variableOffset)
    {
        List<Reference> remapped = new java.util.ArrayList<>(references.size());
        for (Reference reference : references) {
            remapped.add(remap(reference, variableOffset));
        }
        return List.copyOf(remapped);
    }

    private static Reference remap(Reference reference, int variableOffset)
    {
        return switch (reference.producer()) {
            case Input input -> reference;
            case Variable variable -> new Reference(new Variable(variable.id() + variableOffset), reference.stream());
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

    private static ReferenceMask valueMask(Producer producer)
    {
        return new ReferenceMask(new Reference(producer, Stream.VALUES));
    }

    private static AndMask andMask(Producer... producers)
    {
        return new AndMask(valueMasks(producers));
    }

    private static OrMask orMask(Producer... producers)
    {
        return new OrMask(valueMasks(producers));
    }

    private static NotMask notMask(Producer producer)
    {
        return new NotMask(valueMask(producer));
    }

    private static List<MaskExpression> valueMasks(Producer... producers)
    {
        return Arrays.stream(producers)
                .<MaskExpression>map(TpcdsParquetSupport::valueMask)
                .toList();
    }

    private static int maxVariableId(EvaluationPlan plan)
    {
        return maxVariableId(plan.assignments());
    }

    private static int maxVariableId(List<Assignment> assignments)
    {
        int maxVariableId = -1;
        for (Assignment assignment : assignments) {
            maxVariableId = Math.max(maxVariableId, assignment.output().id());
        }
        return maxVariableId;
    }

    private record FilterSpec(EvaluationPlan plan, MaskExpression predicate, Reference materializedValue)
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
            return null;
        }

        private Reference requireMaterializedValue()
        {
            if (materializedValue == null) {
                throw new IllegalArgumentException("Filter composition requires an explicit materialized value");
            }
            return materializedValue;
        }
    }

    private static int integerField(Row row, int index)
    {
        return ((Number) row.values()[index]).intValue();
    }

    private static String stringField(Row row, int index)
    {
        return (String) row.values()[index];
    }

    private static List<Row> keyRows(IntSet keys)
    {
        int[] values = keys.toIntArray();
        Arrays.sort(values);
        List<Row> rows = new ArrayList<>(values.length);
        for (int value : values) {
            rows.add(new Row((long) value));
        }
        return rows;
    }

    private static Operator query88Bucket(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, int bucket)
    {
        Operator filtered = factScan(allocator, tables, "store_sales", "ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk");
        filtered = new HashJoinOperator(allocator, filtered, 0, filteredProjectedTable(allocator, primitiveRegistry, tables, "time_dim", query88TimeBucketPredicate(bucket), new String[] {"t_time_sk", "t_hour", "t_minute"}), 0)
                .withOutputs(1, 2);
        filtered = new HashJoinOperator(allocator, filtered, 0, filteredProjectedTable(allocator, primitiveRegistry, tables, "household_demographics", query88HouseholdPredicate(), new String[] {"hd_demo_sk", "hd_dep_count", "hd_vehicle_count"}), 0)
                .withOutputs(1);
        return new HashJoinOperator(allocator, filtered, 0, filteredProjectedTable(allocator, primitiveRegistry, tables, "store", equalUtf8(1, "ese"), new String[] {"s_store_sk", "s_store_name"}), 0)
                .withOutputs(0);
    }

    private static Operator query88BucketCount(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, int bucket)
    {
        return new AggregationOperator(allocator, List.of(new CountAll()), query88Bucket(allocator, primitiveRegistry, tables, bucket));
    }

    private static Streams borrowedStreams(Output output)
    {
        Streams.Builder streams = Streams.builder();
        for (Stream stream : output.streams()) {
            streams.put(stream, output.borrow(stream));
        }
        return streams.build();
    }
}
