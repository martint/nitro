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
package org.weakref.nitro;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.MarkDistinctOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.TrinoParquetScanOperator;
import org.weakref.nitro.operator.aggregation.CountAll;
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
import org.weakref.nitro.tpcds.TpcdsParquetTables;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

    public static Operator query41(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        // q41's correlated count(*) > 0 predicate is an existence check, so we lower it as:
        // build distinct qualifying manufacturers, then filter outer item rows by that set.
        Set<String> eligibleManufacturers = query41EligibleManufacturers(allocator, primitiveRegistry, tables);
        Operator eligibleItems = filter(
                allocator,
                primitiveRegistry,
                itemScan(allocator, tables, "i_product_name", "i_manufact_id", "i_manufact"),
                and(
                        greaterThan(1, 737),
                        lessThan(1, 779),
                        utf8AnyOf(2, eligibleManufacturers)));
        Operator productNames = projectInputs(allocator, primitiveRegistry, eligibleItems, 0);
        Operator distinct = new MarkDistinctOperator(allocator, 0, productNames);
        return new TopNOperator(allocator, 100, 0, false, distinct);
    }

    public static Operator query62(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        ShippingBucketsLookup lookup = query62Lookup(allocator, tables);
        Operator joined = factScan(allocator, tables, "web_sales", "ws_ship_date_sk", "ws_sold_date_sk", "ws_warehouse_sk", "ws_ship_mode_sk", "ws_web_site_sk");
        joined = new HashJoinOperator(allocator, joined, 0, new ConstantTableOperator(allocator, 1, keyRows(lookup.allowedShipDates())), 0);
        joined = new HashJoinOperator(allocator, joined, 2, new ConstantTableOperator(allocator, 2, keyGroupRows(lookup.firstNames())), 0);
        joined = new HashJoinOperator(allocator, joined, 3, new ConstantTableOperator(allocator, 2, keyGroupRows(lookup.secondNames())), 0);
        joined = new HashJoinOperator(allocator, joined, 4, new ConstantTableOperator(allocator, 2, keyGroupRows(lookup.thirdNames())), 0);
        Operator projected = projectShippingBuckets(allocator, primitiveRegistry, joined, 7, 9, 11, 0, 1);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4), new Sum(5), new Sum(6), new Sum(7)),
                projected);
        Operator namesJoined = new HashJoinOperator(allocator, aggregated, 0, new ConstantTableOperator(allocator, 2, lookupRows(lookup.firstNames().namesByGroupId())), 0);
        namesJoined = new HashJoinOperator(allocator, namesJoined, 1, new ConstantTableOperator(allocator, 2, lookupRows(lookup.secondNames().namesByGroupId())), 0);
        namesJoined = new HashJoinOperator(allocator, namesJoined, 2, new ConstantTableOperator(allocator, 2, lookupRows(lookup.thirdNames().namesByGroupId())), 0);
        Operator named = projectInputs(allocator, primitiveRegistry, namesJoined, 9, 11, 13, 3, 4, 5, 6, 7);
        return new SentinelNullRestoringOperator(
                allocator,
                new int[] {0, 1, 2},
                new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, named));
    }

    public static Operator query96(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Query96Lookup lookup = query96Lookup(allocator, tables);
        Operator filtered = factScan(allocator, tables, "store_sales", "ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk");
        filtered = new HashJoinOperator(allocator, filtered, 0, new ConstantTableOperator(allocator, 1, keyRows(lookup.timeKeys())), 0);
        filtered = new HashJoinOperator(allocator, filtered, 1, new ConstantTableOperator(allocator, 1, keyRows(lookup.householdKeys())), 0);
        filtered = new HashJoinOperator(allocator, filtered, 2, new ConstantTableOperator(allocator, 1, keyRows(lookup.storeKeys())), 0);
        return new AggregationOperator(allocator, List.of(new CountAll()), filtered);
    }

    public static Operator query99(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        ShippingBucketsLookup lookup = query99Lookup(allocator, tables);
        Operator joined = factScan(allocator, tables, "catalog_sales", "cs_ship_date_sk", "cs_sold_date_sk", "cs_warehouse_sk", "cs_ship_mode_sk", "cs_call_center_sk");
        joined = new HashJoinOperator(allocator, joined, 0, new ConstantTableOperator(allocator, 1, keyRows(lookup.allowedShipDates())), 0);
        joined = new HashJoinOperator(allocator, joined, 2, new ConstantTableOperator(allocator, 2, keyGroupRows(lookup.firstNames())), 0);
        joined = new HashJoinOperator(allocator, joined, 3, new ConstantTableOperator(allocator, 2, keyGroupRows(lookup.secondNames())), 0);
        joined = new HashJoinOperator(allocator, joined, 4, new ConstantTableOperator(allocator, 2, keyGroupRows(lookup.thirdNames())), 0);
        Operator projected = projectShippingBuckets(allocator, primitiveRegistry, joined, 7, 9, 11, 0, 1);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4), new Sum(5), new Sum(6), new Sum(7)),
                projected);
        Operator namesJoined = new HashJoinOperator(allocator, aggregated, 0, new ConstantTableOperator(allocator, 2, lookupRows(lookup.firstNames().namesByGroupId())), 0);
        namesJoined = new HashJoinOperator(allocator, namesJoined, 1, new ConstantTableOperator(allocator, 2, lookupRows(lookup.secondNames().namesByGroupId())), 0);
        namesJoined = new HashJoinOperator(allocator, namesJoined, 2, new ConstantTableOperator(allocator, 2, lookupRows(lookup.thirdNames().namesByGroupId())), 0);
        Operator named = projectInputs(allocator, primitiveRegistry, namesJoined, 9, 11, 13, 3, 4, 5, 6, 7);
        return new SentinelNullRestoringOperator(
                allocator,
                new int[] {0, 1, 2},
                new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, named));
    }

    public static Operator query10(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Query10Lookup lookup = query10Lookup(allocator, tables);
        Operator eligibleCustomers = customerScan(allocator, tables, "c_current_addr_sk", "c_customer_sk", "c_current_cdemo_sk");
        eligibleCustomers = new HashJoinOperator(allocator, eligibleCustomers, 0, new ConstantTableOperator(allocator, 1, keyRows(lookup.eligibleAddressKeys())), 0);
        eligibleCustomers = new HashJoinOperator(allocator, eligibleCustomers, 1, new ConstantTableOperator(allocator, 1, keyRows(lookup.storeCustomerKeys())), 0);
        eligibleCustomers = new HashJoinOperator(allocator, eligibleCustomers, 1, new ConstantTableOperator(allocator, 1, keyRows(unionKeys(lookup.webCustomerKeys(), lookup.catalogCustomerKeys()))), 0);
        Operator joined = new HashJoinOperator(
                allocator,
                eligibleCustomers,
                2,
                new ConstantTableOperator(allocator, 9, customerDemographicsRows(allocator, tables)),
                0);
        Operator projected = projectInputs(allocator, primitiveRegistry, joined, 7, 8, 9, 10, 11, 12, 13, 14);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3, 4, 5, 6, 7),
                List.of(new CountAll()),
                projected);
        Operator reordered = projectInputs(allocator, primitiveRegistry, aggregated, 0, 1, 2, 8, 3, 8, 4, 8, 5, 8, 6, 8, 7, 8);
        return new TopNOperator(allocator, 100, new int[] {0, 1, 2, 4, 6, 8, 10, 12}, new boolean[] {false, false, false, false, false, false, false, false}, reordered);
    }

    public static Operator query73(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Query73Lookup lookup = query73Lookup(allocator, tables);
        Operator projected = factScan(allocator, tables, "store_sales", "ss_ticket_number", "ss_customer_sk", "ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk");
        projected = new HashJoinOperator(allocator, projected, 2, new ConstantTableOperator(allocator, 1, keyRows(lookup.allowedDateKeys())), 0);
        projected = new HashJoinOperator(allocator, projected, 3, new ConstantTableOperator(allocator, 1, keyRows(lookup.allowedStoreKeys())), 0);
        projected = new HashJoinOperator(allocator, projected, 4, new ConstantTableOperator(allocator, 1, keyRows(lookup.allowedHouseholdKeys())), 0);
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
                new ConstantTableOperator(allocator, 5, customerIdentityRows(allocator, tables)),
                0);
        Operator enriched = projectInputs(allocator, primitiveRegistry, joined, 4, 5, 6, 7, 0, 2);
        return new TopNOperator(allocator, 100, new int[] {5, 0, 4}, new boolean[] {true, false, false}, enriched);
    }

    public static Operator query88(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Query88Lookup lookup = query88Lookup(allocator, tables);
        long[] counts = new long[8];
        for (int bucket = 0; bucket < counts.length; bucket++) {
            try (Operator filtered = query88Bucket(allocator, primitiveRegistry, tables, lookup, bucket);
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

    private static ShippingBucketsLookup query62Lookup(Allocator allocator, TpcdsParquetTables tables)
    {
        return new ShippingBucketsLookup(
                dateKeysForMonthSequence(allocator, tables, 1200, 1211),
                canonicalNameLookup(prefixedUtf8Map(scanRows(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name"), 0, 1, 20)),
                canonicalNameLookup(utf8Map(scanRows(allocator, tables, "ship_mode", "sm_ship_mode_sk", "sm_type"), 0, 1)),
                canonicalNameLookup(utf8Map(scanRows(allocator, tables, "web_site", "web_site_sk", "web_name"), 0, 1)));
    }

    private static Query96Lookup query96Lookup(Allocator allocator, TpcdsParquetTables tables)
    {
        return new Query96Lookup(
                integerKeySet(scanRows(allocator, tables, "time_dim", "t_time_sk", "t_hour", "t_minute"), row ->
                        integerField(row, 1) == 20 && integerField(row, 2) >= 30),
                integerKeySet(scanRows(allocator, tables, "household_demographics", "hd_demo_sk", "hd_dep_count"), row ->
                        integerField(row, 1) == 7),
                integerKeySet(scanRows(allocator, tables, "store", "s_store_sk", "s_store_name"), row ->
                        "ese".equals(stringField(row, 1))));
    }

    private static ShippingBucketsLookup query99Lookup(Allocator allocator, TpcdsParquetTables tables)
    {
        return new ShippingBucketsLookup(
                dateKeysForMonthSequence(allocator, tables, 1200, 1211),
                canonicalNameLookup(prefixedUtf8Map(scanRows(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name"), 0, 1, 20)),
                canonicalNameLookup(utf8Map(scanRows(allocator, tables, "ship_mode", "sm_ship_mode_sk", "sm_type"), 0, 1)),
                canonicalNameLookup(utf8Map(scanRows(allocator, tables, "call_center", "cc_call_center_sk", "cc_name"), 0, 1)));
    }

    private static IntSet dateKeysForYearMonthRange(Allocator allocator, TpcdsParquetTables tables, int year, int minimumMonthOfYear, int maximumMonthOfYear)
    {
        return integerKeySet(scanRows(allocator, tables, "date_dim", "d_date_sk", "d_year", "d_moy"), row ->
                integerField(row, 1) == year &&
                        integerField(row, 2) >= minimumMonthOfYear &&
                        integerField(row, 2) <= maximumMonthOfYear);
    }

    private static IntSet dateKeysForDayOfMonthAndYears(Allocator allocator, TpcdsParquetTables tables, int minimumDayOfMonth, int maximumDayOfMonth, int... years)
    {
        IntSet allowedYears = new IntOpenHashSet(years);
        return integerKeySet(scanRows(allocator, tables, "date_dim", "d_date_sk", "d_dom", "d_year"), row ->
                integerField(row, 1) >= minimumDayOfMonth &&
                        integerField(row, 1) <= maximumDayOfMonth &&
                        allowedYears.contains(integerField(row, 2)));
    }

    private static IntSet customerKeysForDates(Allocator allocator, TpcdsParquetTables tables, String tableName, String customerColumn, String dateColumn, IntSet allowedDates)
    {
        return integerKeySet(scanRows(allocator, tables, tableName, customerColumn, dateColumn), row -> {
            Object customerKey = row.values()[0];
            Object dateKey = row.values()[1];
            return customerKey != null &&
                    dateKey != null &&
                    allowedDates.contains(((Number) dateKey).intValue());
        });
    }

    private static IntSet addressKeysForCounties(Allocator allocator, TpcdsParquetTables tables, String... counties)
    {
        Set<String> allowedCounties = Set.of(counties);
        return integerKeySet(scanRows(allocator, tables, "customer_address", "ca_address_sk", "ca_county"), row -> {
            String county = stringField(row, 1);
            return county != null && allowedCounties.contains(county);
        });
    }

    private static IntSet storeKeysForCounties(Allocator allocator, TpcdsParquetTables tables, String... counties)
    {
        Set<String> allowedCounties = Set.of(counties);
        return integerKeySet(scanRows(allocator, tables, "store", "s_store_sk", "s_county"), row -> {
            String county = stringField(row, 1);
            return county != null && allowedCounties.contains(county);
        });
    }

    private static IntSet storeKeysByName(Allocator allocator, TpcdsParquetTables tables, String storeName)
    {
        return integerKeySet(scanRows(allocator, tables, "store", "s_store_sk", "s_store_name"), row -> {
            String name = stringField(row, 1);
            return name != null && storeName.equals(name);
        });
    }

    private static IntSet householdKeysForQuery73(Allocator allocator, TpcdsParquetTables tables)
    {
        return integerKeySet(scanRows(allocator, tables, "household_demographics", "hd_demo_sk", "hd_buy_potential", "hd_vehicle_count", "hd_dep_count"), row -> {
            String buyPotential = stringField(row, 1);
            int vehicleCount = integerField(row, 2);
            int dependentCount = integerField(row, 3);
            return (">10000".equals(buyPotential) || "Unknown".equals(buyPotential)) &&
                    vehicleCount > 0 &&
                    ((double) dependentCount / vehicleCount) > 1.0;
        });
    }

    private static IntSet householdKeysForQuery88(Allocator allocator, TpcdsParquetTables tables)
    {
        return integerKeySet(scanRows(allocator, tables, "household_demographics", "hd_demo_sk", "hd_dep_count", "hd_vehicle_count"), row -> {
            int dependentCount = integerField(row, 1);
            int vehicleCount = integerField(row, 2);
            return (dependentCount == 4 || dependentCount == 2 || dependentCount == 0) &&
                    vehicleCount <= (dependentCount + 2);
        });
    }

    @SuppressWarnings("unchecked")
    private static IntSet[] timeBucketKeysForQuery88(Allocator allocator, TpcdsParquetTables tables)
    {
        IntSet[] buckets = new IntSet[8];
        for (int bucket = 0; bucket < buckets.length; bucket++) {
            buckets[bucket] = new IntOpenHashSet();
        }
        for (Row row : scanRows(allocator, tables, "time_dim", "t_time_sk", "t_hour", "t_minute")) {
            int hour = integerField(row, 1);
            int minute = integerField(row, 2);
            int bucket = switch (hour) {
                case 8 -> minute >= 30 ? 0 : -1;
                case 9 -> minute < 30 ? 1 : 2;
                case 10 -> minute < 30 ? 3 : 4;
                case 11 -> minute < 30 ? 5 : 6;
                case 12 -> minute < 30 ? 7 : -1;
                default -> -1;
            };
            if (bucket >= 0) {
                buckets[bucket].add(integerField(row, 0));
            }
        }
        return buckets;
    }

    private static Query10Lookup query10Lookup(Allocator allocator, TpcdsParquetTables tables)
    {
        IntSet eligibleDates = dateKeysForYearMonthRange(allocator, tables, 2002, 1, 4);
        return new Query10Lookup(
                addressKeysForCounties(allocator, tables, "Rush County", "Toole County", "Jefferson County", "Dona Ana County", "La Porte County"),
                customerKeysForDates(allocator, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", eligibleDates),
                customerKeysForDates(allocator, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", eligibleDates),
                customerKeysForDates(allocator, tables, "catalog_sales", "cs_ship_customer_sk", "cs_sold_date_sk", eligibleDates));
    }

    private static Query73Lookup query73Lookup(Allocator allocator, TpcdsParquetTables tables)
    {
        return new Query73Lookup(
                dateKeysForDayOfMonthAndYears(allocator, tables, 1, 2, 1999, 2000, 2001),
                storeKeysForCounties(allocator, tables, "Williamson County", "Franklin Parish", "Bronx County", "Orange County"),
                householdKeysForQuery73(allocator, tables));
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

    private static List<Row> customerIdentityRows(Allocator allocator, TpcdsParquetTables tables)
    {
        return scanRows(allocator, tables, "customer",
                "c_customer_sk",
                "c_last_name",
                "c_first_name",
                "c_salutation",
                "c_preferred_cust_flag").stream()
                .map(row -> new Row(
                        ((Number) row.values()[0]).longValue(),
                        stringField(row, 1),
                        stringField(row, 2),
                        stringField(row, 3),
                        stringField(row, 4)))
                .toList();
    }

    @SuppressWarnings("unchecked")
    private static Query88Lookup query88Lookup(Allocator allocator, TpcdsParquetTables tables)
    {
        return new Query88Lookup(
                timeBucketKeysForQuery88(allocator, tables),
                householdKeysForQuery88(allocator, tables),
                storeKeysByName(allocator, tables, "ese"));
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

    private static IntSet dateKeysForMonthSequence(Allocator allocator, TpcdsParquetTables tables, int minimumMonthSequence, int maximumMonthSequence)
    {
        return integerKeySet(scanRows(allocator, tables, "date_dim", "d_date_sk", "d_month_seq"), row -> {
            int monthSequence = integerField(row, 1);
            return monthSequence >= minimumMonthSequence && monthSequence <= maximumMonthSequence;
        });
    }

    private static IntSet integerKeySet(List<Row> rows, java.util.function.Predicate<Row> predicate)
    {
        IntSet result = new IntOpenHashSet();
        for (Row row : rows) {
            if (predicate.test(row)) {
                result.add(integerField(row, 0));
            }
        }
        return result;
    }

    private static Map<Integer, String> utf8Map(List<Row> rows, int keyIndex, int valueIndex)
    {
        Map<Integer, String> result = new HashMap<>();
        for (Row row : rows) {
            String value = stringField(row, valueIndex);
            result.put(integerField(row, keyIndex), value);
        }
        return result;
    }

    private static Map<Integer, String> prefixedUtf8Map(List<Row> rows, int keyIndex, int valueIndex, int prefixLength)
    {
        Map<Integer, String> result = new HashMap<>();
        for (Row row : rows) {
            String value = stringField(row, valueIndex);
            result.put(integerField(row, keyIndex), value == null ? null : value.substring(0, Math.min(prefixLength, value.length())));
        }
        return result;
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

    private static Operator projectInputs(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int... inputIndexes)
    {
        List<Reference> outputs = java.util.Arrays.stream(inputIndexes)
                .mapToObj(index -> new Reference(new Input(index), Stream.VALUES))
                .toList();
        return new ProjectOperator(allocator, new EvaluationPlan(List.of(), outputs), primitiveRegistry, source);
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
                new Reference(new Input(firstNameIndex), Stream.VALUES),
                new Reference(new Input(secondNameIndex), Stream.VALUES),
                new Reference(new Input(thirdNameIndex), Stream.VALUES),
                new Reference(bucket30, Stream.VALUES),
                new Reference(bucket31To60, Stream.VALUES),
                new Reference(bucket61To90, Stream.VALUES),
                new Reference(bucket91To120, Stream.VALUES),
                new Reference(bucketOver120, Stream.VALUES));
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
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

    private record Query10Lookup(
            IntSet eligibleAddressKeys,
            IntSet storeCustomerKeys,
            IntSet webCustomerKeys,
            IntSet catalogCustomerKeys) {}

    private record Query73Lookup(
            IntSet allowedDateKeys,
            IntSet allowedStoreKeys,
            IntSet allowedHouseholdKeys) {}

    private record Query88Lookup(IntSet[] timeBucketKeys, IntSet allowedHouseholdKeys, IntSet allowedStoreKeys) {}

    private record Query96Lookup(IntSet timeKeys, IntSet householdKeys, IntSet storeKeys) {}

    private record CanonicalNameLookup(Int2IntMap keyToGroupId, Map<Integer, String> namesByGroupId) {}

    private record ShippingBucketsLookup(IntSet allowedShipDates, CanonicalNameLookup firstNames, CanonicalNameLookup secondNames, CanonicalNameLookup thirdNames) {}

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

    private static List<Row> lookupRows(Map<Integer, String> values)
    {
        int[] keys = values.keySet().stream()
                .mapToInt(Integer::intValue)
                .toArray();
        Arrays.sort(keys);
        List<Row> rows = new ArrayList<>(keys.length);
        for (int key : keys) {
            rows.add(new Row((long) key, sentinelValue(values.get(key))));
        }
        return rows;
    }

    private static List<Row> keyGroupRows(CanonicalNameLookup lookup)
    {
        int[] keys = lookup.keyToGroupId().keySet().toIntArray();
        Arrays.sort(keys);
        List<Row> rows = new ArrayList<>(keys.length);
        for (int key : keys) {
            rows.add(new Row((long) key, (long) lookup.keyToGroupId().get(key)));
        }
        return rows;
    }

    private static String sentinelValue(String value)
    {
        return value == null ? NULLS_LAST_SENTINEL_STRING : value;
    }

    private static IntSet unionKeys(IntSet first, IntSet second)
    {
        IntOpenHashSet union = new IntOpenHashSet(first);
        union.addAll(second);
        return union;
    }

    private static CanonicalNameLookup canonicalNameLookup(Map<Integer, String> values)
    {
        Map<String, Integer> groupsByName = new HashMap<>();
        Int2IntMap keyToGroupId = new Int2IntOpenHashMap();
        keyToGroupId.defaultReturnValue(-1);
        Map<Integer, String> namesByGroupId = new HashMap<>();

        int nextGroupId = 0;
        int[] keys = values.keySet().stream()
                .mapToInt(Integer::intValue)
                .toArray();
        Arrays.sort(keys);
        for (int key : keys) {
            String name = values.get(key);
            Integer groupId = groupsByName.get(name);
            if (groupId == null) {
                groupId = nextGroupId++;
                groupsByName.put(name, groupId);
                namesByGroupId.put(groupId.intValue(), name);
            }
            keyToGroupId.put(key, groupId.intValue());
        }
        return new CanonicalNameLookup(keyToGroupId, namesByGroupId);
    }

    private static Operator query88Bucket(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables, Query88Lookup lookup, int bucket)
    {
        Operator filtered = factScan(allocator, tables, "store_sales", "ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk");
        filtered = new HashJoinOperator(allocator, filtered, 0, new ConstantTableOperator(allocator, 1, keyRows(lookup.timeBucketKeys()[bucket])), 0);
        filtered = new HashJoinOperator(allocator, filtered, 1, new ConstantTableOperator(allocator, 1, keyRows(lookup.allowedHouseholdKeys())), 0);
        return new HashJoinOperator(allocator, filtered, 2, new ConstantTableOperator(allocator, 1, keyRows(lookup.allowedStoreKeys())), 0);
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
}
