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
