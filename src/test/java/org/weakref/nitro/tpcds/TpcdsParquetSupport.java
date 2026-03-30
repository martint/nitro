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
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
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
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.SemiJoinOperator;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.TopNRankingOperator;
import org.weakref.nitro.operator.TrinoParquetScanOperator;
import org.weakref.nitro.operator.UnionAllOperator;
import org.weakref.nitro.operator.WindowOperator;
import org.weakref.nitro.operator.aggregation.Avg;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.CountColumn;
import org.weakref.nitro.operator.aggregation.Max;
import org.weakref.nitro.operator.aggregation.Min;
import org.weakref.nitro.operator.aggregation.Sum;
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

final class TpcdsParquetSupport
{
    private static final String NULLS_LAST_SENTINEL_STRING = "\uFFFF";
    private static final byte[] NULLS_LAST_SENTINEL = "\uFFFF".getBytes(StandardCharsets.UTF_8);

    private TpcdsParquetSupport() {}

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
        joined = materializedTable(allocator, joined);
        joined = new HashJoinOperator(allocator, joined, 1, storeTotals, 0);
        joined = filter(allocator, primitiveRegistry, joined, query01ReturnThresholdPredicate(2, 7, 8));

        Operator customerIds = projectInputs(allocator, primitiveRegistry, joined, 5);
        return new TopNOperator(allocator, 100, 0, false, customerIds);
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
        joined = new HashJoinOperator(allocator, joined, 2, categoryAggregates, 0, true);
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
        Operator distinct = new MarkDistinctOperator(allocator, 0, productNames);
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
        Operator filtered = factScan(allocator, tables, "web_sales", "ws_item_sk", "ws_sold_date_sk", "ws_bill_customer_sk", "ws_sales_price");
        filtered = new HashJoinOperator(
                allocator,
                filtered,
                2,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk"),
                0);
        filtered = new HashJoinOperator(
                allocator,
                filtered,
                5,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_city", "ca_zip"),
                0);
        filtered = new HashJoinOperator(
                allocator,
                filtered,
                1,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 2), equal(2, 2001)),
                        new String[] {"d_date_sk", "d_qoy", "d_year"},
                        0),
                0);
        filtered = new HashJoinOperator(
                allocator,
                filtered,
                0,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_item_id"),
                0);
        filtered = new SemiJoinOperator(
                allocator,
                filtered,
                11,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        query45ItemPredicate(),
                        new String[] {"i_item_sk", "i_item_id"},
                        1),
                0,
                true,
                true);
        filtered = filter(allocator, primitiveRegistry, filtered, query45ZipOrItemPredicate(8, 12));
        Operator projected = projectInputs(allocator, primitiveRegistry, filtered, 8, 7, 3);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new Sum(2)),
                projected);
        return new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, aggregated);
    }

    public static Operator query51(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator web = query51Channel(allocator, primitiveRegistry, tables, "web_sales", "ws_sold_date_sk", "ws_item_sk", "ws_sales_price");
        Operator store = query51Channel(allocator, primitiveRegistry, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_sales_price");
        Operator joined = new FullJoinOperator(allocator, web, new int[] {0, 1}, store, new int[] {0, 1});
        joined = projectQuery51JoinOutput(allocator, primitiveRegistry, joined);
        joined = new WindowOperator(
                allocator,
                joined,
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(
                        new WindowOperator.RunningMaxI64WindowFunction(2),
                        new WindowOperator.RunningMaxI64WindowFunction(3)));
        joined = filter(allocator, primitiveRegistry, joined, query51CumulativePredicate(4, 5));
        return new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, joined);
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
                List.of(new WindowOperator.PartitionAverageI64WindowFunction(2)));
        quarterlySales = filter(allocator, primitiveRegistry, quarterlySales, query53QuarterlyDeviationPredicate(2, 3));
        quarterlySales = projectInputs(allocator, primitiveRegistry, quarterlySales, 0, 2, 3);
        return new TopNOperator(allocator, 100, new int[] {2, 1, 0}, new boolean[] {false, false, false}, quarterlySales);
    }

    public static Operator query54(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator lowerMonthSequence = query54ScalarMonthBoundary(allocator, primitiveRegistry, tables, 1);
        Operator upperMonthSequence = query54ScalarMonthBoundary(allocator, primitiveRegistry, tables, 3);

        Operator revenue = query54RevenueByCustomer(allocator, primitiveRegistry, tables);
        revenue = new NestedLoopJoinOperator(allocator, revenue, lowerMonthSequence);
        revenue = new NestedLoopJoinOperator(allocator, revenue, upperMonthSequence);
        revenue = filter(allocator, primitiveRegistry, revenue, query54MonthBetweenPredicate(2, 3, 4));
        revenue = projectInputs(allocator, primitiveRegistry, revenue, 0, 1);
        revenue = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                revenue);
        revenue = projectQuery54Segment(allocator, primitiveRegistry, revenue);

        Operator grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new CountAll()),
                revenue);
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
        return projectQuery58Output(allocator, primitiveRegistry, joined);
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
        List<Row> monthlyRankedRows;
        try (Operator monthlyRanked = query57MonthlyRankedSales(allocator, primitiveRegistry, tables)) {
            monthlyRankedRows = OperatorAssertions.OperatorAssert.toRows(monthlyRanked);
        }
        int monthlyRankedOutputCount = monthlyRankedRows.isEmpty() ? 8 : monthlyRankedRows.getFirst().values().length;

        Operator current = projectQuery57CurrentRows(allocator, primitiveRegistry, new ConstantTableOperator(allocator, monthlyRankedOutputCount, monthlyRankedRows));
        Operator previous = projectQuery57AdjacentRows(allocator, primitiveRegistry, new ConstantTableOperator(allocator, monthlyRankedOutputCount, monthlyRankedRows), true);
        Operator next = projectQuery57AdjacentRows(allocator, primitiveRegistry, new ConstantTableOperator(allocator, monthlyRankedOutputCount, monthlyRankedRows), false);

        Operator joined = new HashJoinOperator(allocator, current, new int[] {0, 1, 2, 7}, previous, new int[] {0, 1, 2, 4});
        joined = new HashJoinOperator(allocator, joined, new int[] {0, 1, 2, 7}, next, new int[] {0, 1, 2, 4});
        joined = projectQuery57Output(allocator, primitiveRegistry, joined);
        joined = filter(allocator, primitiveRegistry, joined, query53QuarterlyDeviationPredicate(6, 5));
        joined = projectQuery57SortKey(allocator, primitiveRegistry, joined);
        joined = new TopNOperator(allocator, 100, new int[] {9, 2}, new boolean[] {false, false}, joined);
        return projectInputs(allocator, primitiveRegistry, joined, 0, 1, 2, 3, 4, 5, 6, 7, 8);
    }

    public static Operator query62(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator allowedShipDates = materializedTable(
                allocator,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(1, 1199), lessThan(1, 1212)),
                        new String[] {"d_date_sk", "d_month_seq"},
                        0));
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
        Operator allowedShipDates = materializedTable(
                allocator,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(greaterThan(1, 1199), lessThan(1, 1212)),
                        new String[] {"d_date_sk", "d_month_seq"},
                        0));
        Operator warehouseNames = scannedTable(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name");
        Operator shipModeNames = scannedTable(allocator, tables, "ship_mode", "sm_ship_mode_sk", "sm_type");
        Operator callCenterNames = scannedTable(allocator, tables, "call_center", "cc_call_center_sk", "cc_name");
        Operator joined = factScan(allocator, tables, "catalog_sales", "cs_ship_date_sk", "cs_sold_date_sk", "cs_warehouse_sk", "cs_ship_mode_sk", "cs_call_center_sk");
        joined = new HashJoinOperator(allocator, joined, 0, allowedShipDates, 0);
        joined = new HashJoinOperator(allocator, joined, 2, warehouseNames, 0);
        joined = new HashJoinOperator(allocator, joined, 3, shipModeNames, 0);
        joined = new HashJoinOperator(allocator, joined, 4, callCenterNames, 0);
        Operator projected = projectShippingBuckets(allocator, primitiveRegistry, joined, 7, 9, 11, 0, 1);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4), new Sum(5), new Sum(6), new Sum(7)),
                projected);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, aggregated);
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
                new ConstantTableOperator(allocator, 9, customerDemographicsRows(allocator, tables)),
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
        FilterSpec eligibleDates = yearQuarterRangePredicate(1, 2, 2002, 1, 3);
        Operator storeCustomers = customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", eligibleDates);
        Operator otherCustomers = new UnionAllOperator(1, List.of(
                customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", eligibleDates),
                customerKeysForEligibleDates(allocator, primitiveRegistry, tables, "catalog_sales", "cs_ship_customer_sk", "cs_sold_date_sk", eligibleDates)));
        Operator eligibleCustomers = customerScan(allocator, tables, "c_current_addr_sk", "c_customer_sk", "c_current_cdemo_sk");
        eligibleCustomers = new SemiJoinOperator(allocator, eligibleCustomers, 1, storeCustomers, 0);
        eligibleCustomers = new SemiJoinOperator(allocator, eligibleCustomers, 1, otherCustomers, 0);
        eligibleCustomers = new HashJoinOperator(allocator, eligibleCustomers, 0, scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_state"), 0);
        Operator joined = new HashJoinOperator(
                allocator,
                eligibleCustomers,
                2,
                new ConstantTableOperator(allocator, 9, customerDemographicsRows(allocator, tables)),
                0);
        Operator projected = projectInputs(allocator, primitiveRegistry, joined, 4, 6, 7, 11, 12, 13);
        Operator aggregated = new GroupedAggregationOperator(
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
                projected);
        Operator reordered = projectInputs(allocator, primitiveRegistry, aggregated, 0, 1, 2, 3, 6, 7, 8, 9, 4, 10, 11, 12, 13, 5, 14, 15, 16, 17);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3, 8, 13}, new boolean[] {false, false, false, false, false, false}, reordered);
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
                new ConstantTableOperator(allocator, 9, customerDemographicsRows(allocator, tables)),
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
                        {0, 1, 2, 3, 4, 5, 6, 7, 8}});
        grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5, 6, 7, 9),
                List.of(new Sum(8)),
                grouped);
        grouped = projectInputs(allocator, primitiveRegistry, grouped, 0, 1, 2, 3, 4, 5, 6, 7, 9);
        grouped = new TopNRankingOperator(allocator, 100, new int[] {0}, new int[] {8}, new boolean[] {true}, grouped);
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
                        {0, 1, 2}});
        grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 3),
                List.of(new Sum(2)),
                grouped);
        grouped = projectQuery70Rollup(allocator, primitiveRegistry, grouped);
        grouped = new TopNRankingOperator(allocator, 100, new int[] {3, 4}, new int[] {2}, new boolean[] {true}, grouped);
        grouped = new TopNOperator(allocator, 100, new int[] {3, 4, 5}, new boolean[] {true, false, false}, grouped);
        return projectInputs(allocator, primitiveRegistry, grouped, 2, 0, 1, 3, 5);
    }

    public static Operator query80(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator grouped = new GroupIdOperator(
                allocator,
                new UnionAllOperator(5, List.of(
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
                                "web_site"))),
                new int[][] {
                        {-1, -1, 2, 3, 4},
                        {0, -1, 2, 3, 4},
                        {0, 1, 2, 3, 4}});
        grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 5),
                List.of(new Sum(2), new Sum(3), new Sum(4)),
                grouped);
        grouped = projectInputs(allocator, primitiveRegistry, grouped, 0, 1, 3, 4, 5);
        return new TopNOperator(allocator, 100, new int[] {0, 1}, new boolean[] {false, false}, grouped);
    }

    public static Operator query30(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerTotalReturn = query30CustomerTotalReturn(allocator, primitiveRegistry, tables);
        Operator stateAverages = query30StateAverageReturns(allocator, primitiveRegistry, tables);

        Operator joined = new HashJoinOperator(allocator, customerTotalReturn, 1, stateAverages, 0, true);
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

    public static Operator query23(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator catalog = query23Channel(
                allocator,
                primitiveRegistry,
                tables,
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_bill_customer_sk",
                "cs_item_sk",
                "cs_quantity",
                "cs_list_price");
        Operator web = query23Channel(
                allocator,
                primitiveRegistry,
                tables,
                "web_sales",
                "ws_sold_date_sk",
                "ws_bill_customer_sk",
                "ws_item_sk",
                "ws_quantity",
                "ws_list_price");
        return new AggregationOperator(
                allocator,
                List.of(new Sum(0)),
                new UnionAllOperator(1, List.of(catalog, web)));
    }

    public static Operator query81(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerTotalReturn = query81CustomerTotalReturn(allocator, primitiveRegistry, tables);
        Operator stateAverages = query81StateAverageReturns(allocator, primitiveRegistry, tables);

        Operator joined = new HashJoinOperator(allocator, customerTotalReturn, 1, stateAverages, 0, true);
        joined = new HashJoinOperator(
                allocator,
                joined,
                0,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk", "c_customer_id", "c_salutation", "c_first_name", "c_last_name"),
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
                        new String[] {"ca_address_sk", "ca_state", "ca_street_number", "ca_street_name", "ca_street_type", "ca_suite_number", "ca_city", "ca_county", "ca_zip", "ca_country", "ca_gmt_offset", "ca_location_type"}),
                0);
        joined = filter(allocator, primitiveRegistry, joined, query81ReturnThresholdPredicate(2, 4));
        joined = projectInputs(allocator, primitiveRegistry, joined, 7, 8, 9, 10, 13, 14, 15, 16, 17, 18, 12, 19, 20, 21, 22, 2);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, new boolean[] {false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false}, joined);
    }

    public static Operator query97(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator store = query97Channel(allocator, primitiveRegistry, tables, "store_sales", "ss_customer_sk", "ss_item_sk", "ss_sold_date_sk");
        Operator catalog = query97Channel(allocator, primitiveRegistry, tables, "catalog_sales", "cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk");
        Operator joined = new FullJoinOperator(allocator, store, new int[] {0, 1}, catalog, new int[] {0, 1});
        Operator indicators = projectQuery97JoinIndicators(allocator, primitiveRegistry, joined);
        return new AggregationOperator(
                allocator,
                List.of(new Sum(0), new Sum(1), new Sum(2)),
                indicators);
    }

    public static Operator query73(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator allowedDates = filteredProjectedTable(allocator, primitiveRegistry, tables, "date_dim", dayOfMonthAndYearsPredicate(1, 2, 1, 2, 1999, 2000, 2001), new String[] {"d_date_sk", "d_dom", "d_year"});
        Operator allowedStores = filteredProjectedTable(allocator, primitiveRegistry, tables, "store", utf8AnyOf(1, Set.of("Williamson County", "Franklin Parish", "Bronx County", "Orange County")), new String[] {"s_store_sk", "s_county"});
        Operator allowedHouseholds = filteredProjectedTable(allocator, primitiveRegistry, tables, "household_demographics", query73HouseholdPredicate(), new String[] {"hd_demo_sk", "hd_buy_potential", "hd_vehicle_count", "hd_dep_count"});
        Operator projected = factScan(allocator, tables, "store_sales", "ss_ticket_number", "ss_customer_sk", "ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk");
        projected = new HashJoinOperator(allocator, projected, 2, allowedDates, 0);
        projected = new HashJoinOperator(allocator, projected, 3, allowedStores, 0);
        projected = new HashJoinOperator(allocator, projected, 4, allowedHouseholds, 0);
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
                0);
        Operator enriched = projectCustomerIdentity(allocator, primitiveRegistry, joined, 4, 5, 6, 7, 0, 2);
        return new TopNOperator(allocator, 100, new int[] {5, 0, 4}, new boolean[] {true, false, false}, enriched);
    }

    public static Operator query84(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator eligibleIncomeBands = filteredProjectedTable(allocator, primitiveRegistry, tables, "income_band", and(greaterThan(1, 38127), lessThan(2, 88129)), new String[] {"ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"});
        Operator eligibleHouseholds = new HashJoinOperator(
                allocator,
                factScan(allocator, tables, "household_demographics", "hd_demo_sk", "hd_income_band_sk"),
                1,
                eligibleIncomeBands,
                0);
        Operator eligibleAddresses = filteredProjectedTable(allocator, primitiveRegistry, tables, "customer_address", equalUtf8(1, "Edgewood"), new String[] {"ca_address_sk", "ca_city"});
        Operator customers = customerScan(allocator, tables,
                "c_customer_id",
                "c_last_name",
                "c_first_name",
                "c_current_addr_sk",
                "c_current_cdemo_sk",
                "c_current_hdemo_sk");
        customers = new HashJoinOperator(allocator, customers, 3, eligibleAddresses, 0);
        customers = new HashJoinOperator(allocator, customers, 5, eligibleHouseholds, 0);
        customers = new HashJoinOperator(allocator, customers, 4, scannedTable(allocator, tables, "store_returns", "sr_cdemo_sk"), 0);
        Operator projected = projectCustomerName(allocator, primitiveRegistry, customers, 0, 1, 2);
        return new TopNOperator(allocator, 100, 0, false, projected);
    }

    public static Operator query90(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        long morningCount = query90Count(
                allocator,
                primitiveRegistry,
                tables,
                filteredProjectedTable(allocator, primitiveRegistry, tables, "time_dim", and(greaterThan(1, 7), lessThan(1, 10)), new String[] {"t_time_sk", "t_hour"}),
                filteredProjectedTable(allocator, primitiveRegistry, tables, "household_demographics", equal(1, 6), new String[] {"hd_demo_sk", "hd_dep_count"}),
                filteredProjectedTable(allocator, primitiveRegistry, tables, "web_page", and(greaterThan(1, 4999), lessThan(1, 5201)), new String[] {"wp_web_page_sk", "wp_char_count"}));
        long eveningCount = query90Count(
                allocator,
                primitiveRegistry,
                tables,
                filteredProjectedTable(allocator, primitiveRegistry, tables, "time_dim", and(greaterThan(1, 18), lessThan(1, 21)), new String[] {"t_time_sk", "t_hour"}),
                filteredProjectedTable(allocator, primitiveRegistry, tables, "household_demographics", equal(1, 6), new String[] {"hd_demo_sk", "hd_dep_count"}),
                filteredProjectedTable(allocator, primitiveRegistry, tables, "web_page", and(greaterThan(1, 4999), lessThan(1, 5201)), new String[] {"wp_web_page_sk", "wp_char_count"}));
        double ratio = (double) morningCount / eveningCount;
        return new ConstantTableOperator(allocator, 1, List.of(new Row(ratio)));
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
        Operator joined = new HashJoinOperator(allocator, discounted, 0, itemAverages, 0, true);
        joined = filter(allocator, primitiveRegistry, joined, query92DiscountThresholdPredicate(1, 3));
        joined = projectInputs(allocator, primitiveRegistry, joined, 1);
        return new AggregationOperator(
                allocator,
                List.of(new Sum(0)),
                joined);
    }

    public static Operator query88(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        long[] counts = new long[8];
        for (int bucket = 0; bucket < counts.length; bucket++) {
            try (Operator filtered = query88Bucket(allocator, primitiveRegistry, tables, bucket);
                    Operator aggregated = new AggregationOperator(allocator, List.of(new CountAll()), filtered)) {
                counts[bucket] = singleLongResult(aggregated);
            }
        }
        return new ConstantTableOperator(allocator, 8, List.of(new Row(
                counts[0],
                counts[1],
                counts[2],
                counts[3],
                counts[4],
                counts[5],
                counts[6],
                counts[7])));
    }

    static Set<String> query41EligibleManufacturers(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        return query41EligibleManufacturers(allocator, primitiveRegistry, tables, query41EligibilityPredicate(1, 2, 3, 4));
    }

    private static Set<String> query41EligibleManufacturers(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, FilterSpec filterSpec)
    {
        Set<String> result = new HashSet<>();
        for (Row row : query41EligibleRows(allocator, primitiveRegistry, tables, filterSpec)) {
            result.add((String) row.values()[0]);
        }
        return result;
    }

    private static List<Row> query41EligibleRows(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, FilterSpec filterSpec)
    {
        Operator filtered = filter(
                allocator,
                primitiveRegistry,
                itemScan(allocator, tables, "i_manufact", "i_category", "i_color", "i_units", "i_size"),
                filterSpec);
        try (filtered) {
            return OperatorAssertions.OperatorAssert.toRows(filtered);
        }
    }

    private static List<Row> customerDemographicsRows(Allocator allocator, TpcdsParquetTables tables)
    {
        return scanRows(allocator, tables, "customer_demographics",
                "cd_demo_sk",
                "cd_gender",
                "cd_marital_status",
                "cd_education_status",
                "cd_purchase_estimate",
                "cd_credit_rating",
                "cd_dep_count",
                "cd_dep_employed_count",
                "cd_dep_college_count").stream()
                .map(row -> new Row(
                        ((Number) row.values()[0]).longValue(),
                        stringField(row, 1),
                        stringField(row, 2),
                        stringField(row, 3),
                        ((Number) row.values()[4]).longValue(),
                        stringField(row, 5),
                        ((Number) row.values()[6]).longValue(),
                        ((Number) row.values()[7]).longValue(),
                        ((Number) row.values()[8]).longValue()))
                .toList();
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

    private static FilterSpec query53QuarterlyDeviationPredicate(int sumIndex, int averageIndex)
    {
        Variable zero = new Variable(0);
        Variable ten = new Variable(1);
        Variable averagePositive = new Variable(2);
        Variable sumLessThanAverage = new Variable(3);
        Variable averageMinusSum = new Variable(4);
        Variable sumMinusAverage = new Variable(5);
        Variable absoluteDifference = new Variable(6);
        Variable scaledDifference = new Variable(7);
        Variable deviationLarge = new Variable(8);
        Variable result = new Variable(9);

        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(ten, new Literal(10L), AllMask.ALL),
                new Assignment(averagePositive, new Call("lt", List.of(
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(averageIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(sumLessThanAverage, new Call("lt", List.of(
                        new Reference(new Input(sumIndex), Stream.VALUES),
                        new Reference(new Input(averageIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(averageMinusSum, new Call("subtract", List.of(
                        new Reference(new Input(averageIndex), Stream.VALUES),
                        new Reference(new Input(sumIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(sumMinusAverage, new Call("subtract", List.of(
                        new Reference(new Input(sumIndex), Stream.VALUES),
                        new Reference(new Input(averageIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(absoluteDifference, new Call("if_i64", List.of(
                        new Reference(sumLessThanAverage, Stream.VALUES),
                        new Reference(averageMinusSum, Stream.VALUES),
                        new Reference(sumMinusAverage, Stream.VALUES))), AllMask.ALL),
                new Assignment(scaledDifference, new Call("multiply", List.of(
                        new Reference(absoluteDifference, Stream.VALUES),
                        new Reference(ten, Stream.VALUES))), AllMask.ALL),
                new Assignment(deviationLarge, new Call("lt", List.of(
                        new Reference(new Input(averageIndex), Stream.VALUES),
                        new Reference(scaledDifference, Stream.VALUES))), AllMask.ALL),
                new Assignment(result, new Call("and", List.of(
                        new Reference(averagePositive, Stream.VALUES),
                        new Reference(deviationLarge, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static Operator query44RankedItems(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, boolean descending)
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
        scalarAggregate = filter(allocator, primitiveRegistry, scalarAggregate, greaterThan(2, 0));
        scalarAggregate = new EnforceSingleRowOperator(allocator, scalarAggregate);
        scalarAggregate = projectQuery44ScalarAggregate(allocator, primitiveRegistry, scalarAggregate);

        Operator joined = new HashJoinOperator(allocator, itemAggregates, 0, scalarAggregate, 0);
        joined = projectQuery44AverageKeys(allocator, primitiveRegistry, joined);
        joined = filter(allocator, primitiveRegistry, joined, query44ThresholdPredicate(1, 2));
        joined = projectInputs(allocator, primitiveRegistry, joined, 0, 1);
        joined = new TopNRankingOperator(allocator, 10, new int[] {1}, new boolean[] {descending}, joined);
        return projectInputs(allocator, primitiveRegistry, joined, 0, 2);
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
        monthSequence = new MarkDistinctOperator(allocator, 0, monthSequence);
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
        return new FilterSpec(new EvaluationPlan(assignments, List.of()), new ReferenceMask(new Reference(matches, Stream.VALUES)));
    }

    private static List<Row> scanRows(Allocator allocator, TpcdsParquetTables tables, String tableName, String... columns)
    {
        try (Operator scan = multiFileScan(
                tables.tableFiles(tableName),
                columns.length,
                path -> new TrinoParquetScanOperator(allocator, path, List.of(columns)))) {
            return OperatorAssertions.OperatorAssert.toRows(scan);
        }
    }

    private static Operator factScan(Allocator allocator, TpcdsParquetTables tables, String tableName, String... columns)
    {
        return multiFileScan(
                tables.tableFiles(tableName),
                columns.length,
                path -> new TrinoParquetScanOperator(allocator, path, List.of(columns)));
    }

    private static Operator customerScan(Allocator allocator, TpcdsParquetTables tables, String... columns)
    {
        return multiFileScan(
                tables.tableFiles("customer"),
                columns.length,
                path -> new TrinoParquetScanOperator(allocator, path, List.of(columns)));
    }

    private static Operator scannedTable(Allocator allocator, TpcdsParquetTables tables, String tableName, String... columns)
    {
        List<TableOperator.Page> pages = new ArrayList<>();
        Allocator.Context allocationContext = new Allocator.Context("TpcdsParquetSupport");
        try (Operator scan = multiFileScan(
                tables.tableFiles(tableName),
                columns.length,
                path -> new TrinoParquetScanOperator(allocator, path, List.of(columns)))) {
            while (scan.hasNext()) {
                try (Batch batch = scan.next()) {
                    int rowCount = batch.borrowMask().count();
                    if (rowCount == 0) {
                        continue;
                    }
                    Streams[] pageColumns = new Streams[columns.length];
                    for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                        pageColumns[outputIndex] = allocator.copyStreams(allocationContext, borrowedStreams(batch.output(outputIndex)), batch.borrowMask());
                    }
                    pages.add(new TableOperator.Page(rowCount, pageColumns, Mask.all(rowCount)));
                }
            }
        }
        return new TableOperator(columns.length, pages);
    }

    private static Operator itemScan(Allocator allocator, TpcdsParquetTables tables, String... columns)
    {
        return multiFileScan(
                tables.tableFiles("item"),
                columns.length,
                path -> new TrinoParquetScanOperator(allocator, path, List.of(columns)));
    }

    private static Operator multiFileScan(List<Path> files, int outputCount, Function<Path, Operator> operatorFactory)
    {
        return new MultiStageOperator(outputCount, files, operatorFactory);
    }

    private static Operator filter(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, FilterSpec filterSpec)
    {
        return new FilterOperator(source, filterSpec.plan(), primitiveRegistry, filterSpec.predicate(), allocator);
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
        List<Reference> outputs = java.util.Arrays.stream(inputIndexes)
                .mapToObj(index -> new Reference(new Input(index), Stream.VALUES))
                .toList();
        return new ProjectOperator(allocator, new EvaluationPlan(List.of(), outputs), primitiveRegistry, source);
    }

    private static Operator materializedTable(Allocator allocator, int outputCount, Operator... sources)
    {
        List<Row> rows = new ArrayList<>();
        for (Operator source : sources) {
            try (source) {
                rows.addAll(OperatorAssertions.OperatorAssert.toRows(source));
            }
        }
        return new ConstantTableOperator(allocator, outputCount, rows);
    }

    private static Operator materializedTable(Allocator allocator, Operator source)
    {
        try (source) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(source);
            int outputCount = rows.isEmpty() ? 0 : rows.getFirst().values().length;
            return new ConstantTableOperator(allocator, outputCount, rows);
        }
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
        Variable alwaysTrue = new Variable(27);
        Variable firstName = new Variable(28);
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
                new Assignment(alwaysTrue, new Literal(true), AllMask.ALL),
                new Assignment(firstName, new Call("if_utf8", List.of(
                        new Reference(alwaysTrue, Stream.VALUES),
                        new Reference(new Input(firstNameIndex), Stream.VALUES),
                        new Reference(new Input(firstNameIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(prefixStart, new Literal(1L), AllMask.ALL),
                new Assignment(prefixLength, new Literal(20L), AllMask.ALL),
                new Assignment(firstNamePrefix, new Call("substring_utf8", List.of(
                        new Reference(firstName, Stream.VALUES),
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
        Operator dateKeys = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                query80DatePredicate(),
                new String[] {"d_date_sk", "d_date"},
                0);
        Operator itemKeys = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "item",
                greaterThan(1, 5_000),
                new String[] {"i_item_sk", "i_current_price"},
                0);
        Operator promotionKeys = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "promotion",
                equalUtf8(1, "N"),
                new String[] {"p_promo_sk", "p_channel_tv"},
                0);
        Operator dimension = scannedTable(allocator, tables, dimensionTable, dimensionColumns);

        Operator joined = factScan(allocator, tables, salesTable, salesColumns);
        joined = new HashJoinOperator(
                allocator,
                joined,
                new int[] {1, 4},
                factScan(allocator, tables, returnsTable, returnsColumns),
                new int[] {0, 1},
                true);
        joined = projectInputs(allocator, primitiveRegistry, joined, 0, 1, 2, 3, 5, 6, 9, 10);
        joined = new HashJoinOperator(allocator, joined, 0, dateKeys, 0);
        joined = new HashJoinOperator(allocator, joined, 3, dimension, 0);
        joined = new HashJoinOperator(allocator, joined, 1, itemKeys, 0);
        joined = new HashJoinOperator(allocator, joined, 2, promotionKeys, 0);
        joined = projectQuery80BranchValues(allocator, primitiveRegistry, joined, 10);
        joined = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new Sum(2), new Sum(3)),
                joined);
        return projectQuery80BranchOutput(allocator, primitiveRegistry, joined, channelName, idPrefix);
    }

    private static Operator query81CustomerTotalReturn(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator returns = factScan(allocator, tables, "catalog_returns", "cr_returning_customer_sk", "cr_returned_date_sk", "cr_returning_addr_sk", "cr_return_amt_inc_tax");
        returns = new HashJoinOperator(
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

    private static Operator query81StateAverageReturns(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerTotalReturn = query81CustomerTotalReturn(allocator, primitiveRegistry, tables);
        customerTotalReturn = filter(allocator, primitiveRegistry, customerTotalReturn, isNotNullI64(2));
        customerTotalReturn = new GroupedAggregationOperator(
                allocator,
                List.of(1),
                List.of(new Sum(2), new CountColumn(2)),
                customerTotalReturn);
        return projectQuery81StateAverage(allocator, primitiveRegistry, customerTotalReturn);
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

    private static Operator query23FrequentItems(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator frequentItems = factScan(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk");
        frequentItems = new HashJoinOperator(
                allocator,
                frequentItems,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        query23YearRangePredicate(),
                        new String[] {"d_date_sk", "d_year", "d_date"},
                        0, 2),
                0);
        frequentItems = new HashJoinOperator(
                allocator,
                frequentItems,
                1,
                scannedTable(allocator, tables, "item", "i_item_sk"),
                0);
        frequentItems = projectInputs(allocator, primitiveRegistry, frequentItems, 1, 3);
        frequentItems = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new CountAll()),
                frequentItems);
        frequentItems = filter(allocator, primitiveRegistry, frequentItems, greaterThan(2, 4));
        frequentItems = projectInputs(allocator, primitiveRegistry, frequentItems, 0);
        return new MarkDistinctOperator(allocator, 0, frequentItems);
    }

    private static Operator query23CustomerSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, boolean filterYears)
    {
        Operator sales = filterYears
                ? factScan(allocator, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_quantity", "ss_sales_price")
                : factScan(allocator, tables, "store_sales", "ss_customer_sk", "ss_quantity", "ss_sales_price");
        if (filterYears) {
            sales = new HashJoinOperator(
                    allocator,
                    sales,
                    1,
                    filteredProjectedTable(
                            allocator,
                            primitiveRegistry,
                            tables,
                            "date_dim",
                            query23YearRangePredicate(),
                            new String[] {"d_date_sk", "d_year"},
                            0),
                    0);
        }
        sales = new HashJoinOperator(
                allocator,
                sales,
                0,
                scannedTable(allocator, tables, "customer", "c_customer_sk"),
                0);
        sales = projectQuery23SalesValue(allocator, primitiveRegistry, sales, 0, filterYears ? 2 : 1, filterYears ? 3 : 2);
        return new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                sales);
    }

    private static Operator query23BestCustomers(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerSales = query23CustomerSales(allocator, primitiveRegistry, tables, false);
        Operator maxStoreSales = new AggregationOperator(
                allocator,
                List.of(new Max(0)),
                projectInputs(allocator, primitiveRegistry, query23CustomerSales(allocator, primitiveRegistry, tables, true), 1));
        customerSales = new NestedLoopJoinOperator(allocator, customerSales, maxStoreSales);
        customerSales = filter(allocator, primitiveRegistry, customerSales, query23HalfMaxPredicate(1, 2));
        return projectInputs(allocator, primitiveRegistry, customerSales, 0);
    }

    private static Operator query23Channel(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            TpcdsParquetTables tables,
            String salesTable,
            String soldDateColumn,
            String customerColumn,
            String itemColumn,
            String quantityColumn,
            String listPriceColumn)
    {
        Operator channel = factScan(allocator, tables, salesTable, soldDateColumn, customerColumn, itemColumn, quantityColumn, listPriceColumn);
        channel = new HashJoinOperator(
                allocator,
                channel,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 2000), equal(2, 2)),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0),
                0);
        channel = new SemiJoinOperator(allocator, channel, 2, query23FrequentItems(allocator, primitiveRegistry, tables), 0);
        channel = new SemiJoinOperator(allocator, channel, 1, query23BestCustomers(allocator, primitiveRegistry, tables), 0);
        return projectQuery23SalesOnly(allocator, primitiveRegistry, channel, 3, 4);
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

    private static Operator projectQuery80BranchValues(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int idIndex)
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
                        new Reference(new Input(6), Stream.VALUES))), AllMask.ALL),
                new Assignment(returnAmount, new Call("if_i64", List.of(
                        new Reference(returnAmountIsNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(6), Stream.VALUES))), AllMask.ALL),
                new Assignment(returnLossIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(7), Stream.VALUES))), AllMask.ALL),
                new Assignment(returnLoss, new Call("if_i64", List.of(
                        new Reference(returnLossIsNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(new Input(7), Stream.VALUES))), AllMask.ALL),
                new Assignment(profit, new Call("subtract", List.of(
                        new Reference(new Input(5), Stream.VALUES),
                        new Reference(returnLoss, Stream.VALUES))), AllMask.ALL));
        List<Reference> outputs = List.of(
                new Reference(new Input(idIndex), Stream.VALUES),
                new Reference(new Input(4), Stream.VALUES),
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

    private static long query90Count(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, Operator timeKeys, Operator householdKeys, Operator pageKeys)
    {
        try (Operator filtered = factScan(allocator, tables, "web_sales", "ws_sold_time_sk", "ws_ship_hdemo_sk", "ws_web_page_sk");
                Operator timeJoined = new HashJoinOperator(allocator, filtered, 0, timeKeys, 0);
                Operator householdJoined = new HashJoinOperator(allocator, timeJoined, 1, householdKeys, 0);
                Operator pageJoined = new HashJoinOperator(allocator, householdJoined, 2, pageKeys, 0);
                Operator aggregated = new AggregationOperator(allocator, List.of(new CountAll()), pageJoined)) {
            return singleLongResult(aggregated);
        }
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

    private static FilterSpec query23YearRangePredicate()
    {
        return and(
                greaterThan(1, 1999),
                lessThan(1, 2004));
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

    private static FilterSpec query23HalfMaxPredicate(int salesIndex, int maxIndex)
    {
        Variable two = new Variable(0);
        Variable doubledSales = new Variable(1);
        Variable greaterThan = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(two, new Literal(2L), AllMask.ALL),
                new Assignment(doubledSales, new Call("multiply", List.of(
                        new Reference(new Input(salesIndex), Stream.VALUES),
                        new Reference(two, Stream.VALUES))), AllMask.ALL),
                new Assignment(greaterThan, new Call("lt", List.of(
                        new Reference(new Input(maxIndex), Stream.VALUES),
                        new Reference(doubledSales, Stream.VALUES))), AllMask.ALL)), List.of());
        return and(
                isNotNullI64(salesIndex),
                isNotNullI64(maxIndex),
                new FilterSpec(plan, new ReferenceMask(new Reference(greaterThan, Stream.VALUES))));
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
        Operator sales = factScan(allocator, tables, salesTable, customerColumn, dateColumn);
        Operator eligibleDates = filteredProjectedTable(allocator, primitiveRegistry, tables, "date_dim", eligibleDateFilter, new String[] {"d_date_sk", "d_year", "d_moy", "d_qoy"});
        return new HashJoinOperator(allocator, sales, 1, eligibleDates, 0);
    }

    private static Operator customerKeysForEligibleDates(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String customerColumn, String dateColumn, FilterSpec eligibleDateFilter)
    {
        return projectInputs(
                allocator,
                primitiveRegistry,
                customersForEligibleDates(allocator, primitiveRegistry, tables, salesTable, customerColumn, dateColumn, eligibleDateFilter),
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
        Operator facts = factScan(allocator, tables, salesTable, soldDateColumn, itemColumn, salesPriceColumn);
        facts = filter(allocator, primitiveRegistry, facts, isNotNullI64(1));
        Operator allowedDates = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                and(greaterThan(2, 1199), lessThan(2, 1212)),
                new String[] {"d_date_sk", "d_date", "d_month_seq"},
                0,
                1);
        facts = new HashJoinOperator(allocator, facts, 0, allowedDates, 0);
        Operator grouped = new GroupedAggregationOperator(
                allocator,
                List.of(1, 4),
                List.of(new Sum(2)),
                facts);
        Operator running = new WindowOperator(
                allocator,
                grouped,
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(new WindowOperator.RunningSumI64WindowFunction(2)));
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

    private static Operator query54RevenueByCustomer(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator revenue = factScan(allocator, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", "ss_ext_sales_price");
        revenue = new HashJoinOperator(allocator, revenue, 0, query54MyCustomers(allocator, primitiveRegistry, tables), 0);
        revenue = new HashJoinOperator(
                allocator,
                revenue,
                4,
                scannedTable(allocator, tables, "customer_address", "ca_address_sk", "ca_county", "ca_state"),
                0);
        revenue = new HashJoinOperator(
                allocator,
                revenue,
                new int[] {6, 7},
                scannedTable(allocator, tables, "store", "s_county", "s_state"),
                new int[] {0, 1});
        revenue = new HashJoinOperator(
                allocator,
                revenue,
                1,
                scannedTable(allocator, tables, "date_dim", "d_date_sk", "d_month_seq"),
                0);
        return projectInputs(allocator, primitiveRegistry, revenue, 3, 2, 11);
    }

    private static Operator query54MyCustomers(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator customerSales = new UnionAllOperator(3, List.of(
                factScan(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_bill_customer_sk", "cs_item_sk"),
                factScan(allocator, tables, "web_sales", "ws_sold_date_sk", "ws_bill_customer_sk", "ws_item_sk")));
        customerSales = new HashJoinOperator(
                allocator,
                customerSales,
                2,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "item",
                        and(equalUtf8(1, "Women"), equalUtf8(2, "maternity")),
                        new String[] {"i_item_sk", "i_category", "i_class"},
                        0),
                0);
        customerSales = new HashJoinOperator(
                allocator,
                customerSales,
                0,
                filteredProjectedTable(
                        allocator,
                        primitiveRegistry,
                        tables,
                        "date_dim",
                        and(equal(1, 12), equal(2, 1998)),
                        new String[] {"d_date_sk", "d_moy", "d_year"},
                        0),
                0);
        customerSales = new HashJoinOperator(
                allocator,
                customerSales,
                1,
                scannedTable(allocator, tables, "customer", "c_customer_sk", "c_current_addr_sk"),
                0);
        customerSales = projectInputs(allocator, primitiveRegistry, customerSales, 1, 6);
        return new MarkDistinctOperator(allocator, new int[] {0, 1}, customerSales);
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
        boundary = new MarkDistinctOperator(allocator, 0, boundary);
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
                new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES))));
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
        return new MarkDistinctOperator(allocator, 0, allowedDates);
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
        weekSequence = new MarkDistinctOperator(allocator, 0, weekSequence);
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
        return new FilterSpec(plan, new ReferenceMask(new Reference(result, Stream.VALUES)));
    }

    private static Operator projectQuery58Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable three = new Variable(0);
        Variable tenThousand = new Variable(1);
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
                        new Assignment(tenThousand, new Literal(10_000L), AllMask.ALL),
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
                                new Reference(tenThousand, Stream.VALUES))), AllMask.ALL),
                        new Assignment(scaledCatalogRevenue, new Call("multiply", List.of(
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(tenThousand, Stream.VALUES))), AllMask.ALL),
                        new Assignment(scaledWebRevenue, new Call("multiply", List.of(
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(tenThousand, Stream.VALUES))), AllMask.ALL),
                        new Assignment(scaledTotalRevenue, new Call("multiply", List.of(
                                new Reference(totalRevenue, Stream.VALUES),
                                new Reference(tenThousand, Stream.VALUES))), AllMask.ALL),
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

    private static Operator projectQuery23SalesValue(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int customerIndex, int quantityIndex, int priceIndex)
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
                        new Reference(new Input(priceIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(quantityIsNull, new Call("is_null_i32", List.of(
                        new Reference(new Input(quantityIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(anyNull, new Call("or", List.of(
                        new Reference(priceIsNull, Stream.VALUES),
                        new Reference(quantityIsNull, Stream.VALUES))), AllMask.ALL),
                new Assignment(multiplied, new Call("multiply", List.of(
                        new Reference(new Input(priceIndex), Stream.VALUES),
                        new Reference(new Input(quantityIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(sales, new Call("if_i64", List.of(
                        new Reference(anyNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(multiplied, Stream.VALUES))), AllMask.ALL));
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
        Variable zero = new Variable(0);
        Variable priceIsNull = new Variable(1);
        Variable quantityIsNull = new Variable(2);
        Variable anyNull = new Variable(3);
        Variable multiplied = new Variable(4);
        Variable sales = new Variable(5);
        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(priceIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(priceIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(quantityIsNull, new Call("is_null_i32", List.of(
                        new Reference(new Input(quantityIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(anyNull, new Call("or", List.of(
                        new Reference(priceIsNull, Stream.VALUES),
                        new Reference(quantityIsNull, Stream.VALUES))), AllMask.ALL),
                new Assignment(multiplied, new Call("multiply", List.of(
                        new Reference(new Input(priceIndex), Stream.VALUES),
                        new Reference(new Input(quantityIndex), Stream.VALUES))), AllMask.ALL),
                new Assignment(sales, new Call("if_i64", List.of(
                        new Reference(anyNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(multiplied, Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(new Reference(sales, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query57MonthlyRankedSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Operator facts = factScan(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_call_center_sk", "cs_item_sk", "cs_sales_price");
        facts = new HashJoinOperator(
                allocator,
                facts,
                2,
                scannedTable(allocator, tables, "item", "i_item_sk", "i_brand", "i_category"),
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
                        query57DatePredicate(),
                        new String[] {"d_date_sk", "d_year", "d_moy"},
                        0, 1, 2),
                0);
        facts = new HashJoinOperator(
                allocator,
                facts,
                1,
                scannedTable(allocator, tables, "call_center", "cc_call_center_sk", "cc_name"),
                0);
        facts = projectInputs(allocator, primitiveRegistry, facts, 6, 5, 11, 8, 9, 3);
        facts = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4),
                List.of(new Sum(5)),
                facts);
        facts = new TopNRankingOperator(
                allocator,
                32,
                new int[] {0, 1, 2},
                new int[] {3, 4},
                new boolean[] {false, false},
                facts);
        facts = new WindowOperator(
                allocator,
                facts,
                new int[] {0, 1, 2, 3},
                new int[0],
                new boolean[0],
                List.of(new WindowOperator.PartitionAverageI64WindowFunction(5)));
        return projectInputs(allocator, primitiveRegistry, facts, 0, 1, 2, 3, 4, 7, 5, 6);
    }

    private static Operator projectQuery57CurrentRows(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        source = filter(allocator, primitiveRegistry, source, equal(3, 1999));
        return projectInputs(allocator, primitiveRegistry, source, 0, 1, 2, 3, 4, 5, 6, 7);
    }

    private static Operator projectQuery57AdjacentRows(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, boolean previous)
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
                new Reference(new Input(6), Stream.VALUES),
                new Reference(adjustedRank, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static Operator projectQuery57Output(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        return projectInputs(allocator, primitiveRegistry, source, 0, 1, 2, 3, 4, 5, 6, 11, 16);
    }

    private static Operator projectQuery57SortKey(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
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
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static FilterSpec query57DatePredicate()
    {
        return or(
                equal(1, 1999),
                and(equal(1, 1998), equal(2, 12)),
                and(equal(1, 2000), equal(2, 1)));
    }

    private static Operator projectQuery51JoinOutput(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable webItemIsNull = new Variable(0);
        Variable joinedItem = new Variable(1);
        Variable webDateIsNull = new Variable(2);
        Variable joinedDate = new Variable(3);
        Variable joinedDateAsDate = new Variable(4);

        List<Assignment> assignments = List.of(
                new Assignment(webItemIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                new Assignment(joinedItem, new Call("if_i64", List.of(
                        new Reference(webItemIsNull, Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                new Assignment(webDateIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                new Assignment(joinedDate, new Call("if_i64", List.of(
                        new Reference(webDateIsNull, Stream.VALUES),
                        new Reference(new Input(4), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                new Assignment(joinedDateAsDate, new Call("cast_i64_to_i32", List.of(
                        new Reference(joinedDate, Stream.VALUES))), AllMask.ALL));
        return new ProjectOperator(
                allocator,
                new EvaluationPlan(assignments, List.of(
                        new Reference(joinedItem, Stream.VALUES),
                        new Reference(joinedDateAsDate, Stream.VALUES),
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(new Input(5), Stream.VALUES))),
                primitiveRegistry,
                source);
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
        facts = projectQuery67FactSales(allocator, primitiveRegistry, facts);
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
        return projectInputs(allocator, primitiveRegistry, facts, 13, 12, 11, 14, 5, 6, 7, 9, 3);
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

    private static Operator projectQuery67FactSales(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
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
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(sales, Stream.VALUES))),
                primitiveRegistry,
                source);
    }

    private static Operator query97Channel(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, String salesTable, String customerColumn, String itemColumn, String soldDateColumn)
    {
        Operator facts = factScan(allocator, tables, salesTable, customerColumn, itemColumn, soldDateColumn);
        Operator allowedDates = filteredProjectedTable(
                allocator,
                primitiveRegistry,
                tables,
                "date_dim",
                and(greaterThan(1, 1199), lessThan(1, 1212)),
                new String[] {"d_date_sk", "d_month_seq"},
                0);
        facts = new HashJoinOperator(allocator, facts, 2, allowedDates, 0);
        facts = projectInputs(allocator, primitiveRegistry, facts, 0, 1);
        return new MarkDistinctOperator(allocator, new int[] {0, 1}, facts);
    }

    private static Operator projectQuery97JoinIndicators(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable storeCustomerIsNull = new Variable(2);
        Variable catalogCustomerIsNull = new Variable(3);
        Variable storeOnlyWhenStorePresent = new Variable(4);
        Variable catalogOnlyWhenStoreMissing = new Variable(5);
        Variable storeAndCatalogWhenStorePresent = new Variable(6);
        Variable storeOnly = new Variable(7);
        Variable catalogOnly = new Variable(8);
        Variable storeAndCatalog = new Variable(9);

        List<Assignment> assignments = List.of(
                new Assignment(zero, new Literal(0L), AllMask.ALL),
                new Assignment(one, new Literal(1L), AllMask.ALL),
                new Assignment(storeCustomerIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogCustomerIsNull, new Call("is_null_i64", List.of(
                        new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                new Assignment(storeOnlyWhenStorePresent, new Call("if_i64", List.of(
                        new Reference(catalogCustomerIsNull, Stream.VALUES),
                        new Reference(one, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogOnlyWhenStoreMissing, new Call("if_i64", List.of(
                        new Reference(catalogCustomerIsNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(one, Stream.VALUES))), AllMask.ALL),
                new Assignment(storeAndCatalogWhenStorePresent, new Call("if_i64", List.of(
                        new Reference(catalogCustomerIsNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(one, Stream.VALUES))), AllMask.ALL),
                new Assignment(storeOnly, new Call("if_i64", List.of(
                        new Reference(storeCustomerIsNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(storeOnlyWhenStorePresent, Stream.VALUES))), AllMask.ALL),
                new Assignment(catalogOnly, new Call("if_i64", List.of(
                        new Reference(storeCustomerIsNull, Stream.VALUES),
                        new Reference(catalogOnlyWhenStoreMissing, Stream.VALUES),
                        new Reference(zero, Stream.VALUES))), AllMask.ALL),
                new Assignment(storeAndCatalog, new Call("if_i64", List.of(
                        new Reference(storeCustomerIsNull, Stream.VALUES),
                        new Reference(zero, Stream.VALUES),
                        new Reference(storeAndCatalogWhenStorePresent, Stream.VALUES))), AllMask.ALL));
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
        return new FilterSpec(plan, new ReferenceMask(new Reference(notNull, Stream.VALUES)));
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

    private static FilterSpec and(FilterSpec first, FilterSpec second, FilterSpec... rest)
    {
        FilterSpec result = combineBoolean("and", first, second);
        for (FilterSpec filterSpec : rest) {
            result = combineBoolean("and", result, filterSpec);
        }
        return result;
    }

    private static FilterSpec or(FilterSpec first, FilterSpec second, FilterSpec... rest)
    {
        FilterSpec result = combineBoolean("or", first, second);
        for (FilterSpec filterSpec : rest) {
            result = combineBoolean("or", result, filterSpec);
        }
        return result;
    }

    private static FilterSpec combineBoolean(String functionName, FilterSpec left, FilterSpec right)
    {
        int rightOffset = maxVariableId(left.plan()) + 1;
        List<Assignment> assignments = new java.util.ArrayList<>(left.plan().assignments());
        assignments.addAll(remap(right.plan().assignments(), rightOffset));

        Variable result = new Variable(maxVariableId(assignments) + 1);
        Reference leftReference = referenceFor(left.plan());
        Reference rightReference = referenceFor(right.plan(), rightOffset);
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
            case org.weakref.nitro.operator.evaluator.ir.Copy(Reference source) -> new org.weakref.nitro.operator.evaluator.ir.Copy(remap(source, variableOffset));
            case org.weakref.nitro.operator.evaluator.ir.StructField(Reference source, String fieldName) -> new org.weakref.nitro.operator.evaluator.ir.StructField(remap(source, variableOffset), fieldName);
            case org.weakref.nitro.operator.evaluator.ir.Merge _ -> throw new UnsupportedOperationException("Merge remapping is not implemented for TPC-DS helper filters");
        };
    }

    private static Reference remap(Reference reference, int variableOffset)
    {
        return switch (reference.producer()) {
            case Input input -> reference;
            case Variable variable -> new Reference(new Variable(variable.id() + variableOffset), reference.stream());
        };
    }

    private static Reference referenceFor(EvaluationPlan plan)
    {
        return new Reference(plan.assignments().getLast().output(), Stream.VALUES);
    }

    private static Reference referenceFor(EvaluationPlan plan, int variableOffset)
    {
        Variable variable = plan.assignments().getLast().output();
        return new Reference(new Variable(variable.id() + variableOffset), Stream.VALUES);
    }

    private static int maxVariableId(EvaluationPlan plan)
    {
        return maxVariableId(plan.assignments());
    }

    private static int maxVariableId(List<Assignment> assignments)
    {
        return assignments.stream()
                .mapToInt(assignment -> assignment.output().id())
                .max()
                .orElse(-1);
    }

    private record FilterSpec(EvaluationPlan plan, MaskExpression predicate) {}

    private static int integerField(Row row, int index)
    {
        return ((Number) row.values()[index]).intValue();
    }

    private static String stringField(Row row, int index)
    {
        return (String) row.values()[index];
    }

    private static long singleLongResult(Operator operator)
    {
        List<Row> rows = OperatorAssertions.OperatorAssert.toRows(operator);
        if (rows.size() != 1) {
            throw new IllegalStateException("Expected exactly one row but found " + rows.size());
        }
        return ((Number) rows.getFirst().values()[0]).longValue();
    }

    private static boolean isNullsLastSentinel(BinaryVector values, int position)
    {
        return values.length(position) == NULLS_LAST_SENTINEL.length &&
                Arrays.mismatch(
                        values.data(),
                        values.startOffset(position),
                        values.endOffset(position),
                        NULLS_LAST_SENTINEL,
                        0,
                        NULLS_LAST_SENTINEL.length) == -1;
    }

    private static Output valuesAndNullsOutput(Allocator allocator, Allocator.Context context, BinaryVector values, BooleanVector nulls)
    {
        return new Output(
                java.util.Set.of(Stream.VALUES, Stream.NULLS),
                stream -> stream == Stream.VALUES ? values : nulls,
                (stream, vector) -> allocator.transfer(context, vector),
                (stream, vector) -> allocator.discard(context, vector));
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
        filtered = new HashJoinOperator(allocator, filtered, 0, filteredProjectedTable(allocator, primitiveRegistry, tables, "time_dim", query88TimeBucketPredicate(bucket), new String[] {"t_time_sk", "t_hour", "t_minute"}), 0);
        filtered = new HashJoinOperator(allocator, filtered, 1, filteredProjectedTable(allocator, primitiveRegistry, tables, "household_demographics", query88HouseholdPredicate(), new String[] {"hd_demo_sk", "hd_dep_count", "hd_vehicle_count"}), 0);
        return new HashJoinOperator(allocator, filtered, 2, filteredProjectedTable(allocator, primitiveRegistry, tables, "store", equalUtf8(1, "ese"), new String[] {"s_store_sk", "s_store_name"}), 0);
    }

    private static final class MultiStageOperator
            implements Operator
    {
        private final int outputCount;
        private final List<Path> files;
        private final Function<Path, Operator> operatorFactory;

        private int fileIndex;
        private Operator current;

        private MultiStageOperator(int outputCount, List<Path> files, Function<Path, Operator> operatorFactory)
        {
            this.outputCount = outputCount;
            this.files = List.copyOf(files);
            this.operatorFactory = operatorFactory;
        }

        @Override
        public int outputCount()
        {
            return outputCount;
        }

        @Override
        public boolean hasNext()
        {
            advanceIfNecessary();
            return current != null && current.hasNext();
        }

        @Override
        public Batch next()
        {
            if (!hasNext()) {
                throw new IllegalStateException("No more parquet rows");
            }
            return current.next();
        }

        @Override
        public void constrain(Mask mask)
        {
            if (current != null) {
                current.constrain(mask);
            }
        }

        @Override
        public void close()
        {
            if (current != null) {
                current.close();
                current = null;
            }
        }

        private void advanceIfNecessary()
        {
            while ((current == null || !current.hasNext()) && fileIndex < files.size()) {
                if (current != null) {
                    current.close();
                }
                current = operatorFactory.apply(files.get(fileIndex++));
            }
        }
    }

    private static final class SentinelNullRestoringOperator
            implements Operator
    {
        private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("SentinelNullRestoringOperator");

        private final Allocator allocator;
        private final int[] nullableColumns;
        private final Operator source;
        private Batch nextBatch;

        private SentinelNullRestoringOperator(Allocator allocator, int[] nullableColumns, Operator source)
        {
            this.allocator = allocator;
            this.nullableColumns = Arrays.copyOf(nullableColumns, nullableColumns.length);
            this.source = source;
        }

        @Override
        public int outputCount()
        {
            return source.outputCount();
        }

        @Override
        public boolean hasNext()
        {
            loadNextBatch();
            return nextBatch != null;
        }

        @Override
        public Batch next()
        {
            if (!hasNext()) {
                throw new IllegalStateException("No more rows");
            }
            Batch batch = nextBatch;
            nextBatch = null;
            return batch;
        }

        @Override
        public void constrain(Mask mask)
        {
            if (nextBatch != null) {
                nextBatch.constrain(mask);
            }
        }

        @Override
        public void close()
        {
            if (nextBatch != null) {
                nextBatch.close();
                nextBatch = null;
            }
            source.close();
            allocator.release(ALLOCATION_CONTEXT);
        }

        private void loadNextBatch()
        {
            if (nextBatch != null || !source.hasNext()) {
                return;
            }

            Batch sourceBatch = source.next();
            Mask mask = sourceBatch.borrowMask();
            Output[] outputs = new Output[outputCount()];
            Set<Integer> nullable = Arrays.stream(nullableColumns).boxed().collect(java.util.stream.Collectors.toSet());
            for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
                if (!nullable.contains(outputIndex)) {
                    Output sourceOutput = sourceBatch.output(outputIndex);
                    outputs[outputIndex] = new Output(
                            sourceOutput.streams(),
                            sourceOutput::borrow,
                            (stream, vector) -> sourceOutput.take(stream),
                            (stream, vector) -> {},
                            sourceOutput::copySinglePosition);
                    continue;
                }

                BinaryVector values = (BinaryVector) sourceBatch.output(outputIndex).borrow(Stream.VALUES);
                BooleanVector nulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, values.length(), BooleanVector::new);
                for (int position : mask) {
                    if (isNullsLastSentinel(values, position)) {
                        nulls.values()[position] = true;
                    }
                }
                outputs[outputIndex] = valuesAndNullsOutput(allocator, ALLOCATION_CONTEXT, values, nulls);
            }

            nextBatch = new Batch(
                    mask,
                    sourceBatch::constrain,
                    takenMask -> takenMask == mask ? sourceBatch.takeMask() : allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                    releasedMask -> {
                        if (releasedMask != mask) {
                            allocator.release(ALLOCATION_CONTEXT, releasedMask);
                        }
                    },
                    sourceBatch::close,
                    outputs);
        }
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
