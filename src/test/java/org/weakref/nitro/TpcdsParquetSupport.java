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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.lang.Math.toIntExact;

final class TpcdsParquetSupport
{
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

    public static Operator query62(Allocator allocator, TpcdsParquetTables tables)
    {
        ShippingBucketsLookup lookup = query62Lookup(allocator, tables);
        Operator projected = new ShippingBucketsProjectOperator(
                allocator,
                factScan(allocator, tables, "web_sales", "ws_ship_date_sk", "ws_sold_date_sk", "ws_warehouse_sk", "ws_ship_mode_sk", "ws_web_site_sk"),
                lookup);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4), new Sum(5), new Sum(6), new Sum(7)),
                projected);
        return new SentinelNullRestoringOperator(
                allocator,
                new int[] {0, 1, 2},
                new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, aggregated));
    }

    public static Operator query96(Allocator allocator, TpcdsParquetTables tables)
    {
        Query96Lookup lookup = query96Lookup(allocator, tables);
        Operator filtered = new IntegerDimensionFilterOperator(
                allocator,
                factScan(allocator, tables, "store_sales", "ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk"),
                new int[] {0, 1, 2},
                new Set[] {lookup.timeKeys(), lookup.householdKeys(), lookup.storeKeys()});
        return new AggregationOperator(allocator, List.of(new CountAll()), filtered);
    }

    public static Operator query99(Allocator allocator, TpcdsParquetTables tables)
    {
        ShippingBucketsLookup lookup = query99Lookup(allocator, tables);
        Operator projected = new ShippingBucketsProjectOperator(
                allocator,
                factScan(allocator, tables, "catalog_sales", "cs_ship_date_sk", "cs_sold_date_sk", "cs_warehouse_sk", "cs_ship_mode_sk", "cs_call_center_sk"),
                lookup);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new Sum(3), new Sum(4), new Sum(5), new Sum(6), new Sum(7)),
                projected);
        return new SentinelNullRestoringOperator(
                allocator,
                new int[] {0, 1, 2},
                new TopNOperator(allocator, 100, new int[] {0, 1, 2}, new boolean[] {false, false, false}, aggregated));
    }

    public static Operator query10(Allocator allocator, PrimitiveRegistry primitiveRegistry, TpcdsParquetTables tables)
    {
        Query10Lookup lookup = query10Lookup(allocator, tables);
        Operator projected = new CustomerDemographicsProjectOperator(
                allocator,
                customerScan(allocator, tables, "c_current_addr_sk", "c_customer_sk", "c_current_cdemo_sk"),
                lookup);
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
        Operator projected = new TicketCustomerFilterProjectOperator(
                allocator,
                factScan(allocator, tables, "store_sales", "ss_ticket_number", "ss_customer_sk", "ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk"),
                lookup);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new CountAll()),
                projected);
        Operator enriched = new CustomerTicketProjectOperator(allocator, aggregated, lookup.customers());
        return new TopNOperator(allocator, 100, new int[] {5, 0, 4}, new boolean[] {true, false, false}, enriched);
    }

    public static Operator query88(Allocator allocator, TpcdsParquetTables tables)
    {
        Query88Lookup lookup = query88Lookup(allocator, tables);
        long[] counts = new long[8];
        for (int bucket = 0; bucket < counts.length; bucket++) {
            try (Operator filtered = new IntegerDimensionFilterOperator(
                    allocator,
                    factScan(allocator, tables, "store_sales", "ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk"),
                    new int[] {0, 1, 2},
                    new Set[] {lookup.timeBucketKeys()[bucket], lookup.allowedHouseholdKeys(), lookup.allowedStoreKeys()});
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
        Operator filtered = filter(
                allocator,
                primitiveRegistry,
                itemScan(allocator, tables, "i_manufact", "i_category", "i_color", "i_units", "i_size"),
                query41EligibilityPredicate(1, 2, 3, 4));
        try (Operator manufacturers = new MarkDistinctOperator(allocator, 0, filtered)) {
            Set<String> result = new HashSet<>();
            for (Row row : OperatorAssertions.OperatorAssert.toRows(manufacturers)) {
                result.add((String) row.values()[0]);
            }
            return result;
        }
    }

    private static ShippingBucketsLookup query62Lookup(Allocator allocator, TpcdsParquetTables tables)
    {
        return new ShippingBucketsLookup(
                dateKeysForMonthSequence(allocator, tables, 1200, 1211),
                prefixedUtf8Map(scanRows(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name"), 0, 1, 20),
                utf8Map(scanRows(allocator, tables, "ship_mode", "sm_ship_mode_sk", "sm_type"), 0, 1),
                utf8Map(scanRows(allocator, tables, "web_site", "web_site_sk", "web_name"), 0, 1));
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
                prefixedUtf8Map(scanRows(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name"), 0, 1, 20),
                utf8Map(scanRows(allocator, tables, "ship_mode", "sm_ship_mode_sk", "sm_type"), 0, 1),
                utf8Map(scanRows(allocator, tables, "call_center", "cc_call_center_sk", "cc_name"), 0, 1));
    }

    private static Set<Integer> dateKeysForYearMonthRange(Allocator allocator, TpcdsParquetTables tables, int year, int minimumMonthOfYear, int maximumMonthOfYear)
    {
        return integerKeySet(scanRows(allocator, tables, "date_dim", "d_date_sk", "d_year", "d_moy"), row ->
                integerField(row, 1) == year &&
                        integerField(row, 2) >= minimumMonthOfYear &&
                        integerField(row, 2) <= maximumMonthOfYear);
    }

    private static Set<Integer> dateKeysForDayOfMonthAndYears(Allocator allocator, TpcdsParquetTables tables, int minimumDayOfMonth, int maximumDayOfMonth, int... years)
    {
        Set<Integer> allowedYears = Arrays.stream(years).boxed().collect(java.util.stream.Collectors.toSet());
        return integerKeySet(scanRows(allocator, tables, "date_dim", "d_date_sk", "d_dom", "d_year"), row ->
                integerField(row, 1) >= minimumDayOfMonth &&
                        integerField(row, 1) <= maximumDayOfMonth &&
                        allowedYears.contains(integerField(row, 2)));
    }

    private static Set<Integer> customerKeysForDates(Allocator allocator, TpcdsParquetTables tables, String tableName, String customerColumn, String dateColumn, Set<Integer> allowedDates)
    {
        return integerKeySet(scanRows(allocator, tables, tableName, customerColumn, dateColumn), row -> {
            Object customerKey = row.values()[0];
            Object dateKey = row.values()[1];
            return customerKey != null &&
                    dateKey != null &&
                    allowedDates.contains(((Number) dateKey).intValue());
        });
    }

    private static Set<Integer> addressKeysForCounties(Allocator allocator, TpcdsParquetTables tables, String... counties)
    {
        Set<String> allowedCounties = Set.of(counties);
        return integerKeySet(scanRows(allocator, tables, "customer_address", "ca_address_sk", "ca_county"), row -> {
            String county = stringField(row, 1);
            return county != null && allowedCounties.contains(county);
        });
    }

    private static Set<Integer> storeKeysForCounties(Allocator allocator, TpcdsParquetTables tables, String... counties)
    {
        Set<String> allowedCounties = Set.of(counties);
        return integerKeySet(scanRows(allocator, tables, "store", "s_store_sk", "s_county"), row -> {
            String county = stringField(row, 1);
            return county != null && allowedCounties.contains(county);
        });
    }

    private static Set<Integer> storeKeysByName(Allocator allocator, TpcdsParquetTables tables, String storeName)
    {
        return integerKeySet(scanRows(allocator, tables, "store", "s_store_sk", "s_store_name"), row -> {
            String name = stringField(row, 1);
            return name != null && storeName.equals(name);
        });
    }

    private static Set<Integer> householdKeysForQuery73(Allocator allocator, TpcdsParquetTables tables)
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

    private static Set<Integer> householdKeysForQuery88(Allocator allocator, TpcdsParquetTables tables)
    {
        return integerKeySet(scanRows(allocator, tables, "household_demographics", "hd_demo_sk", "hd_dep_count", "hd_vehicle_count"), row -> {
            int dependentCount = integerField(row, 1);
            int vehicleCount = integerField(row, 2);
            return (dependentCount == 4 || dependentCount == 2 || dependentCount == 0) &&
                    vehicleCount <= (dependentCount + 2);
        });
    }

    @SuppressWarnings("unchecked")
    private static Set<Integer>[] timeBucketKeysForQuery88(Allocator allocator, TpcdsParquetTables tables)
    {
        Set<Integer>[] buckets = new Set[8];
        for (int bucket = 0; bucket < buckets.length; bucket++) {
            buckets[bucket] = new HashSet<>();
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

    private static Map<Integer, CustomerDemographicsRecord> customerDemographicsLookup(Allocator allocator, TpcdsParquetTables tables)
    {
        Map<Integer, CustomerDemographicsRecord> result = new HashMap<>();
        for (Row row : scanRows(allocator, tables, "customer_demographics",
                "cd_demo_sk",
                "cd_gender",
                "cd_marital_status",
                "cd_education_status",
                "cd_purchase_estimate",
                "cd_credit_rating",
                "cd_dep_count",
                "cd_dep_employed_count",
                "cd_dep_college_count")) {
            result.put(integerField(row, 0), new CustomerDemographicsRecord(
                    utf8OrNull(stringField(row, 1)),
                    utf8OrNull(stringField(row, 2)),
                    utf8OrNull(stringField(row, 3)),
                    ((Number) row.values()[4]).longValue(),
                    utf8OrNull(stringField(row, 5)),
                    ((Number) row.values()[6]).longValue(),
                    ((Number) row.values()[7]).longValue(),
                    ((Number) row.values()[8]).longValue()));
        }
        return result;
    }

    private static Map<Integer, CustomerIdentityRecord> customerIdentityLookup(Allocator allocator, TpcdsParquetTables tables)
    {
        Map<Integer, CustomerIdentityRecord> result = new HashMap<>();
        for (Row row : scanRows(allocator, tables, "customer",
                "c_customer_sk",
                "c_last_name",
                "c_first_name",
                "c_salutation",
                "c_preferred_cust_flag")) {
            result.put(integerField(row, 0), new CustomerIdentityRecord(
                    utf8OrNull(stringField(row, 1)),
                    utf8OrNull(stringField(row, 2)),
                    utf8OrNull(stringField(row, 3)),
                    utf8OrNull(stringField(row, 4))));
        }
        return result;
    }

    private static Query10Lookup query10Lookup(Allocator allocator, TpcdsParquetTables tables)
    {
        Set<Integer> eligibleDates = dateKeysForYearMonthRange(allocator, tables, 2002, 1, 4);
        return new Query10Lookup(
                addressKeysForCounties(allocator, tables, "Rush County", "Toole County", "Jefferson County", "Dona Ana County", "La Porte County"),
                customerKeysForDates(allocator, tables, "store_sales", "ss_customer_sk", "ss_sold_date_sk", eligibleDates),
                customerKeysForDates(allocator, tables, "web_sales", "ws_bill_customer_sk", "ws_sold_date_sk", eligibleDates),
                customerKeysForDates(allocator, tables, "catalog_sales", "cs_ship_customer_sk", "cs_sold_date_sk", eligibleDates),
                customerDemographicsLookup(allocator, tables));
    }

    private static Query73Lookup query73Lookup(Allocator allocator, TpcdsParquetTables tables)
    {
        return new Query73Lookup(
                dateKeysForDayOfMonthAndYears(allocator, tables, 1, 2, 1999, 2000, 2001),
                storeKeysForCounties(allocator, tables, "Williamson County", "Franklin Parish", "Bronx County", "Orange County"),
                householdKeysForQuery73(allocator, tables),
                customerIdentityLookup(allocator, tables));
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

    private static Set<Integer> dateKeysForMonthSequence(Allocator allocator, TpcdsParquetTables tables, int minimumMonthSequence, int maximumMonthSequence)
    {
        return integerKeySet(scanRows(allocator, tables, "date_dim", "d_date_sk", "d_month_seq"), row -> {
            int monthSequence = integerField(row, 1);
            return monthSequence >= minimumMonthSequence && monthSequence <= maximumMonthSequence;
        });
    }

    private static Set<Integer> integerKeySet(List<Row> rows, java.util.function.Predicate<Row> predicate)
    {
        Set<Integer> result = new HashSet<>();
        for (Row row : rows) {
            if (predicate.test(row)) {
                result.add(integerField(row, 0));
            }
        }
        return result;
    }

    private static Map<Integer, byte[]> utf8Map(List<Row> rows, int keyIndex, int valueIndex)
    {
        Map<Integer, byte[]> result = new HashMap<>();
        for (Row row : rows) {
            String value = stringField(row, valueIndex);
            result.put(integerField(row, keyIndex), value == null ? null : utf8(value));
        }
        return result;
    }

    private static Map<Integer, byte[]> prefixedUtf8Map(List<Row> rows, int keyIndex, int valueIndex, int prefixLength)
    {
        Map<Integer, byte[]> result = new HashMap<>();
        for (Row row : rows) {
            String value = stringField(row, valueIndex);
            result.put(integerField(row, keyIndex), value == null ? null : utf8(value.substring(0, Math.min(prefixLength, value.length()))));
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

    private static FilterSpec query41EligibilityPredicate(int categoryIndex, int colorIndex, int unitsIndex, int sizeIndex)
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

        return or(
                womenPowderKhaki,
                womenBrownHoneydew,
                menFloralDeep,
                menLightCornflower,
                womenMidnightSnow,
                womenCyanPapaya,
                menOrangeFrosted,
                menForestGhost);
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
            Set<Integer> eligibleAddressKeys,
            Set<Integer> storeCustomerKeys,
            Set<Integer> webCustomerKeys,
            Set<Integer> catalogCustomerKeys,
            Map<Integer, CustomerDemographicsRecord> demographics) {}

    private record Query73Lookup(
            Set<Integer> allowedDateKeys,
            Set<Integer> allowedStoreKeys,
            Set<Integer> allowedHouseholdKeys,
            Map<Integer, CustomerIdentityRecord> customers) {}

    private record Query88Lookup(Set<Integer>[] timeBucketKeys, Set<Integer> allowedHouseholdKeys, Set<Integer> allowedStoreKeys) {}

    private record Query96Lookup(Set<Integer> timeKeys, Set<Integer> householdKeys, Set<Integer> storeKeys) {}

    private record CustomerDemographicsRecord(
            byte[] gender,
            byte[] maritalStatus,
            byte[] educationStatus,
            long purchaseEstimate,
            byte[] creditRating,
            long dependentCount,
            long employedDependentCount,
            long collegeDependentCount) {}

    private record CustomerIdentityRecord(byte[] lastName, byte[] firstName, byte[] salutation, byte[] preferredCustomerFlag) {}

    private record ShippingBucketsLookup(Set<Integer> allowedShipDates, Map<Integer, byte[]> firstNames, Map<Integer, byte[]> secondNames, Map<Integer, byte[]> thirdNames) {}

    private static int integerField(Row row, int index)
    {
        return ((Number) row.values()[index]).intValue();
    }

    private static String stringField(Row row, int index)
    {
        return (String) row.values()[index];
    }

    private static byte[] utf8(String value)
    {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] utf8OrNull(String value)
    {
        return value == null ? null : utf8(value);
    }

    private static int length(byte[] bytes)
    {
        return bytes == null ? 0 : bytes.length;
    }

    private static void setBytesOrNull(BinaryVector values, BooleanVector nulls, int position, byte[] bytes)
    {
        if (bytes == null) {
            nulls.values()[position] = true;
            values.setNull(position);
            return;
        }
        values.setBytes(position, bytes);
    }

    private static long singleLongResult(Operator operator)
    {
        List<Row> rows = OperatorAssertions.OperatorAssert.toRows(operator);
        if (rows.size() != 1) {
            throw new IllegalStateException("Expected exactly one row but found " + rows.size());
        }
        return ((Number) rows.getFirst().values()[0]).longValue();
    }

    private static int integerValue(Vector vector, int position)
    {
        return switch (vector) {
            case I32Vector values -> values.values()[position];
            case I64Vector values -> toIntExact(values.values()[position]);
            case DictionaryVector values -> integerValue(values.values(), values.ids()[position]);
            case RleVector values -> integerValue(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected integer vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static Output valuesOnlyOutput(Allocator allocator, Allocator.Context context, Vector values)
    {
        return new Output(
                java.util.Set.of(Stream.VALUES),
                _ -> values,
                (stream, vector) -> allocator.transfer(context, vector),
                (stream, vector) -> allocator.discard(context, vector));
    }

    private static Output valuesAndNullsOutput(Allocator allocator, Allocator.Context context, Vector values, BooleanVector nulls)
    {
        return new Output(
                java.util.Set.of(Stream.VALUES, Stream.NULLS),
                stream -> stream == Stream.VALUES ? values : nulls,
                (stream, vector) -> allocator.transfer(context, vector),
                (stream, vector) -> allocator.discard(context, vector));
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

    private static final class IntegerDimensionFilterOperator
            implements Operator
    {
        private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("IntegerDimensionFilterOperator");

        private final Allocator allocator;
        private final Operator source;
        private final int[] inputChannels;
        private final Set<Integer>[] allowedValues;

        private int[] selectedPositions = new int[0];
        private Batch nextBatch;

        @SuppressWarnings("unchecked")
        private IntegerDimensionFilterOperator(Allocator allocator, Operator source, int[] inputChannels, Set<Integer>[] allowedValues)
        {
            this.allocator = allocator;
            this.source = source;
            this.inputChannels = Arrays.copyOf(inputChannels, inputChannels.length);
            this.allowedValues = Arrays.copyOf(allowedValues, allowedValues.length);
        }

        @Override
        public int outputCount()
        {
            return 0;
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
                throw new IllegalStateException("No more parquet rows");
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
            while (nextBatch == null && source.hasNext()) {
                Batch sourceBatch = source.next();
                Mask sourceMask = sourceBatch.borrowMask();
                if (sourceMask.none()) {
                    sourceBatch.close();
                    continue;
                }

                if (selectedPositions.length < sourceMask.selectedCount()) {
                    selectedPositions = new int[sourceMask.selectedCount()];
                }

                Vector[] values = new Vector[inputChannels.length];
                for (int index = 0; index < inputChannels.length; index++) {
                    values[index] = sourceBatch.output(inputChannels[index]).borrow(Stream.VALUES);
                }

                int selectedCount = 0;
                for (int position : sourceMask) {
                    boolean keep = true;
                    for (int index = 0; index < values.length; index++) {
                        if (!allowedValues[index].contains(integerValue(values[index], position))) {
                            keep = false;
                            break;
                        }
                    }
                    if (keep) {
                        selectedPositions[selectedCount++] = position;
                    }
                }

                if (selectedCount == 0) {
                    sourceBatch.close();
                    continue;
                }

                Mask outputMask = allocator.allocateSparseMask(ALLOCATION_CONTEXT, selectedPositions, selectedCount, sourceMask.size());
                nextBatch = new Batch(
                        outputMask,
                        _ -> {},
                        takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                        releasedMask -> allocator.release(ALLOCATION_CONTEXT, releasedMask),
                        sourceBatch::close);
            }
        }
    }

    private static final class CustomerDemographicsProjectOperator
            implements Operator
    {
        private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("CustomerDemographicsProjectOperator");

        private final Allocator allocator;
        private final Operator source;
        private final Query10Lookup lookup;

        private Batch nextBatch;

        private CustomerDemographicsProjectOperator(Allocator allocator, Operator source, Query10Lookup lookup)
        {
            this.allocator = allocator;
            this.source = source;
            this.lookup = lookup;
        }

        @Override
        public int outputCount()
        {
            return 8;
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
                throw new IllegalStateException("No more parquet rows");
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
            while (nextBatch == null && source.hasNext()) {
                Batch sourceBatch = source.next();
                Mask sourceMask = sourceBatch.borrowMask();
                if (sourceMask.none()) {
                    sourceBatch.close();
                    continue;
                }

                Vector addressValues = sourceBatch.output(0).borrow(Stream.VALUES);
                Vector customerValues = sourceBatch.output(1).borrow(Stream.VALUES);
                Vector demographicsValues = sourceBatch.output(2).borrow(Stream.VALUES);

                int selectedCount = 0;
                int genderBytes = 0;
                int maritalBytes = 0;
                int educationBytes = 0;
                int creditBytes = 0;
                for (int position : sourceMask) {
                    int addressKey = integerValue(addressValues, position);
                    int customerKey = integerValue(customerValues, position);
                    if (!lookup.eligibleAddressKeys().contains(addressKey) ||
                            !lookup.storeCustomerKeys().contains(customerKey) ||
                            (!lookup.webCustomerKeys().contains(customerKey) && !lookup.catalogCustomerKeys().contains(customerKey))) {
                        continue;
                    }

                    CustomerDemographicsRecord demographics = lookup.demographics().get(integerValue(demographicsValues, position));
                    if (demographics == null) {
                        continue;
                    }
                    selectedCount++;
                    genderBytes += length(demographics.gender());
                    maritalBytes += length(demographics.maritalStatus());
                    educationBytes += length(demographics.educationStatus());
                    creditBytes += length(demographics.creditRating());
                }

                if (selectedCount == 0) {
                    sourceBatch.close();
                    continue;
                }

                BinaryVector gender = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, selectedCount, genderBytes);
                BinaryVector maritalStatus = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, selectedCount, maritalBytes);
                BinaryVector educationStatus = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, selectedCount, educationBytes);
                BinaryVector creditRating = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, selectedCount, creditBytes);
                gender.addTrait(BinaryVector.Trait.UTF8_STRING);
                maritalStatus.addTrait(BinaryVector.Trait.UTF8_STRING);
                educationStatus.addTrait(BinaryVector.Trait.UTF8_STRING);
                creditRating.addTrait(BinaryVector.Trait.UTF8_STRING);

                BooleanVector genderNulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, selectedCount, BooleanVector::new);
                BooleanVector maritalStatusNulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, selectedCount, BooleanVector::new);
                BooleanVector educationStatusNulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, selectedCount, BooleanVector::new);
                BooleanVector creditRatingNulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, selectedCount, BooleanVector::new);
                I64Vector purchaseEstimate = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);
                I64Vector dependentCount = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);
                I64Vector employedDependentCount = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);
                I64Vector collegeDependentCount = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);

                int outputPosition = 0;
                for (int position : sourceMask) {
                    int addressKey = integerValue(addressValues, position);
                    int customerKey = integerValue(customerValues, position);
                    if (!lookup.eligibleAddressKeys().contains(addressKey) ||
                            !lookup.storeCustomerKeys().contains(customerKey) ||
                            (!lookup.webCustomerKeys().contains(customerKey) && !lookup.catalogCustomerKeys().contains(customerKey))) {
                        continue;
                    }

                    CustomerDemographicsRecord demographics = lookup.demographics().get(integerValue(demographicsValues, position));
                    if (demographics == null) {
                        continue;
                    }

                    setBytesOrNull(gender, genderNulls, outputPosition, demographics.gender());
                    setBytesOrNull(maritalStatus, maritalStatusNulls, outputPosition, demographics.maritalStatus());
                    setBytesOrNull(educationStatus, educationStatusNulls, outputPosition, demographics.educationStatus());
                    setBytesOrNull(creditRating, creditRatingNulls, outputPosition, demographics.creditRating());
                    purchaseEstimate.values()[outputPosition] = demographics.purchaseEstimate();
                    dependentCount.values()[outputPosition] = demographics.dependentCount();
                    employedDependentCount.values()[outputPosition] = demographics.employedDependentCount();
                    collegeDependentCount.values()[outputPosition] = demographics.collegeDependentCount();
                    outputPosition++;
                }

                Mask outputMask = allocator.allocateRangeMask(ALLOCATION_CONTEXT, 0, selectedCount);
                nextBatch = new Batch(
                        outputMask,
                        _ -> {},
                        takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                        releasedMask -> allocator.release(ALLOCATION_CONTEXT, releasedMask),
                        sourceBatch::close,
                        valuesAndNullsOutput(allocator, ALLOCATION_CONTEXT, gender, genderNulls),
                        valuesAndNullsOutput(allocator, ALLOCATION_CONTEXT, maritalStatus, maritalStatusNulls),
                        valuesAndNullsOutput(allocator, ALLOCATION_CONTEXT, educationStatus, educationStatusNulls),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, purchaseEstimate),
                        valuesAndNullsOutput(allocator, ALLOCATION_CONTEXT, creditRating, creditRatingNulls),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, dependentCount),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, employedDependentCount),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, collegeDependentCount));
            }
        }
    }

    private static final class TicketCustomerFilterProjectOperator
            implements Operator
    {
        private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("TicketCustomerFilterProjectOperator");

        private final Allocator allocator;
        private final Operator source;
        private final Query73Lookup lookup;

        private Batch nextBatch;

        private TicketCustomerFilterProjectOperator(Allocator allocator, Operator source, Query73Lookup lookup)
        {
            this.allocator = allocator;
            this.source = source;
            this.lookup = lookup;
        }

        @Override
        public int outputCount()
        {
            return 2;
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
                throw new IllegalStateException("No more parquet rows");
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
            while (nextBatch == null && source.hasNext()) {
                Batch sourceBatch = source.next();
                Mask sourceMask = sourceBatch.borrowMask();
                if (sourceMask.none()) {
                    sourceBatch.close();
                    continue;
                }

                Vector ticketValues = sourceBatch.output(0).borrow(Stream.VALUES);
                Vector customerValues = sourceBatch.output(1).borrow(Stream.VALUES);
                Vector dateValues = sourceBatch.output(2).borrow(Stream.VALUES);
                Vector storeValues = sourceBatch.output(3).borrow(Stream.VALUES);
                Vector householdValues = sourceBatch.output(4).borrow(Stream.VALUES);

                int selectedCount = 0;
                for (int position : sourceMask) {
                    if (lookup.allowedDateKeys().contains(integerValue(dateValues, position)) &&
                            lookup.allowedStoreKeys().contains(integerValue(storeValues, position)) &&
                            lookup.allowedHouseholdKeys().contains(integerValue(householdValues, position))) {
                        selectedCount++;
                    }
                }

                if (selectedCount == 0) {
                    sourceBatch.close();
                    continue;
                }

                I64Vector ticketNumbers = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);
                I64Vector customerKeys = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);
                int outputPosition = 0;
                for (int position : sourceMask) {
                    if (!lookup.allowedDateKeys().contains(integerValue(dateValues, position)) ||
                            !lookup.allowedStoreKeys().contains(integerValue(storeValues, position)) ||
                            !lookup.allowedHouseholdKeys().contains(integerValue(householdValues, position))) {
                        continue;
                    }
                    ticketNumbers.values()[outputPosition] = ((I64Vector) ticketValues).values()[position];
                    customerKeys.values()[outputPosition] = ((I64Vector) customerValues).values()[position];
                    outputPosition++;
                }

                Mask outputMask = allocator.allocateRangeMask(ALLOCATION_CONTEXT, 0, selectedCount);
                nextBatch = new Batch(
                        outputMask,
                        _ -> {},
                        takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                        releasedMask -> allocator.release(ALLOCATION_CONTEXT, releasedMask),
                        sourceBatch::close,
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, ticketNumbers),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, customerKeys));
            }
        }
    }

    private static final class CustomerTicketProjectOperator
            implements Operator
    {
        private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("CustomerTicketProjectOperator");

        private final Allocator allocator;
        private final Operator source;
        private final Map<Integer, CustomerIdentityRecord> customers;

        private Batch nextBatch;

        private CustomerTicketProjectOperator(Allocator allocator, Operator source, Map<Integer, CustomerIdentityRecord> customers)
        {
            this.allocator = allocator;
            this.source = source;
            this.customers = customers;
        }

        @Override
        public int outputCount()
        {
            return 6;
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
                throw new IllegalStateException("No more parquet rows");
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
            while (nextBatch == null && source.hasNext()) {
                Batch sourceBatch = source.next();
                Mask sourceMask = sourceBatch.borrowMask();
                if (sourceMask.none()) {
                    sourceBatch.close();
                    continue;
                }

                Vector ticketValues = sourceBatch.output(0).borrow(Stream.VALUES);
                Vector customerValues = sourceBatch.output(1).borrow(Stream.VALUES);
                Vector countValues = sourceBatch.output(2).borrow(Stream.VALUES);

                int selectedCount = 0;
                int lastNameBytes = 0;
                int firstNameBytes = 0;
                int salutationBytes = 0;
                int preferredFlagBytes = 0;
                for (int position : sourceMask) {
                    long count = ((I64Vector) countValues).values()[position];
                    if (count < 1 || count > 5) {
                        continue;
                    }
                    CustomerIdentityRecord customer = customers.get(integerValue(customerValues, position));
                    if (customer == null) {
                        continue;
                    }
                    selectedCount++;
                    lastNameBytes += length(customer.lastName());
                    firstNameBytes += length(customer.firstName());
                    salutationBytes += length(customer.salutation());
                    preferredFlagBytes += length(customer.preferredCustomerFlag());
                }

                if (selectedCount == 0) {
                    sourceBatch.close();
                    continue;
                }

                BinaryVector lastName = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, selectedCount, lastNameBytes);
                BinaryVector firstName = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, selectedCount, firstNameBytes);
                BinaryVector salutation = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, selectedCount, salutationBytes);
                BinaryVector preferredFlag = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, selectedCount, preferredFlagBytes);
                lastName.addTrait(BinaryVector.Trait.UTF8_STRING);
                firstName.addTrait(BinaryVector.Trait.UTF8_STRING);
                salutation.addTrait(BinaryVector.Trait.UTF8_STRING);
                preferredFlag.addTrait(BinaryVector.Trait.UTF8_STRING);

                BooleanVector lastNameNulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, selectedCount, BooleanVector::new);
                BooleanVector firstNameNulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, selectedCount, BooleanVector::new);
                BooleanVector salutationNulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, selectedCount, BooleanVector::new);
                BooleanVector preferredFlagNulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, selectedCount, BooleanVector::new);
                I64Vector ticketNumbers = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);
                I64Vector counts = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);

                int outputPosition = 0;
                for (int position : sourceMask) {
                    long count = ((I64Vector) countValues).values()[position];
                    if (count < 1 || count > 5) {
                        continue;
                    }
                    CustomerIdentityRecord customer = customers.get(integerValue(customerValues, position));
                    if (customer == null) {
                        continue;
                    }

                    setBytesOrNull(lastName, lastNameNulls, outputPosition, customer.lastName());
                    setBytesOrNull(firstName, firstNameNulls, outputPosition, customer.firstName());
                    setBytesOrNull(salutation, salutationNulls, outputPosition, customer.salutation());
                    setBytesOrNull(preferredFlag, preferredFlagNulls, outputPosition, customer.preferredCustomerFlag());
                    ticketNumbers.values()[outputPosition] = ((I64Vector) ticketValues).values()[position];
                    counts.values()[outputPosition] = count;
                    outputPosition++;
                }

                Mask outputMask = allocator.allocateRangeMask(ALLOCATION_CONTEXT, 0, selectedCount);
                nextBatch = new Batch(
                        outputMask,
                        _ -> {},
                        takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                        releasedMask -> allocator.release(ALLOCATION_CONTEXT, releasedMask),
                        sourceBatch::close,
                        valuesAndNullsOutput(allocator, ALLOCATION_CONTEXT, lastName, lastNameNulls),
                        valuesAndNullsOutput(allocator, ALLOCATION_CONTEXT, firstName, firstNameNulls),
                        valuesAndNullsOutput(allocator, ALLOCATION_CONTEXT, salutation, salutationNulls),
                        valuesAndNullsOutput(allocator, ALLOCATION_CONTEXT, preferredFlag, preferredFlagNulls),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, ticketNumbers),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, counts));
            }
        }
    }

    private static final class ShippingBucketsProjectOperator
            implements Operator
    {
        private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ShippingBucketsProjectOperator");

        private final Allocator allocator;
        private final Operator source;
        private final ShippingBucketsLookup lookup;

        private Batch nextBatch;

        private ShippingBucketsProjectOperator(Allocator allocator, Operator source, ShippingBucketsLookup lookup)
        {
            this.allocator = allocator;
            this.source = source;
            this.lookup = lookup;
        }

        @Override
        public int outputCount()
        {
            return 8;
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
                throw new IllegalStateException("No more parquet rows");
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
            while (nextBatch == null && source.hasNext()) {
                Batch sourceBatch = source.next();
                Mask sourceMask = sourceBatch.borrowMask();
                if (sourceMask.none()) {
                    sourceBatch.close();
                    continue;
                }

                Vector shipDateValues = sourceBatch.output(0).borrow(Stream.VALUES);
                Vector soldDateValues = sourceBatch.output(1).borrow(Stream.VALUES);
                Vector firstKeyValues = sourceBatch.output(2).borrow(Stream.VALUES);
                Vector secondKeyValues = sourceBatch.output(3).borrow(Stream.VALUES);
                Vector thirdKeyValues = sourceBatch.output(4).borrow(Stream.VALUES);

                int selectedCount = 0;
                int firstBytes = 0;
                int secondBytes = 0;
                int thirdBytes = 0;
                for (int position : sourceMask) {
                    int shipDate = integerValue(shipDateValues, position);
                    if (!lookup.allowedShipDates().contains(shipDate)) {
                        continue;
                    }

                    int firstKey = integerValue(firstKeyValues, position);
                    int secondKey = integerValue(secondKeyValues, position);
                    int thirdKey = integerValue(thirdKeyValues, position);
                    if (!lookup.firstNames().containsKey(firstKey) || !lookup.secondNames().containsKey(secondKey) || !lookup.thirdNames().containsKey(thirdKey)) {
                        continue;
                    }
                    byte[] first = lookup.firstNames().get(firstKey);
                    byte[] second = lookup.secondNames().get(secondKey);
                    byte[] third = lookup.thirdNames().get(thirdKey);
                    if (first == null) {
                        first = NULLS_LAST_SENTINEL;
                    }
                    if (second == null) {
                        second = NULLS_LAST_SENTINEL;
                    }
                    if (third == null) {
                        third = NULLS_LAST_SENTINEL;
                    }

                    selectedCount++;
                    firstBytes += first.length;
                    secondBytes += second.length;
                    thirdBytes += third.length;
                }

                if (selectedCount == 0) {
                    sourceBatch.close();
                    continue;
                }

                BinaryVector firstOutput = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, selectedCount, firstBytes);
                BinaryVector secondOutput = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, selectedCount, secondBytes);
                BinaryVector thirdOutput = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, selectedCount, thirdBytes);
                firstOutput.addTrait(BinaryVector.Trait.UTF8_STRING);
                secondOutput.addTrait(BinaryVector.Trait.UTF8_STRING);
                thirdOutput.addTrait(BinaryVector.Trait.UTF8_STRING);
                I64Vector bucket30 = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);
                I64Vector bucket31To60 = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);
                I64Vector bucket61To90 = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);
                I64Vector bucket91To120 = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);
                I64Vector bucketOver120 = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, selectedCount, I64Vector::new);

                int outputPosition = 0;
                for (int position : sourceMask) {
                    int shipDate = integerValue(shipDateValues, position);
                    if (!lookup.allowedShipDates().contains(shipDate)) {
                        continue;
                    }

                    int firstKey = integerValue(firstKeyValues, position);
                    int secondKey = integerValue(secondKeyValues, position);
                    int thirdKey = integerValue(thirdKeyValues, position);
                    if (!lookup.firstNames().containsKey(firstKey) || !lookup.secondNames().containsKey(secondKey) || !lookup.thirdNames().containsKey(thirdKey)) {
                        continue;
                    }
                    byte[] first = lookup.firstNames().get(firstKey);
                    byte[] second = lookup.secondNames().get(secondKey);
                    byte[] third = lookup.thirdNames().get(thirdKey);

                    firstOutput.setBytes(outputPosition, first == null ? NULLS_LAST_SENTINEL : first);
                    secondOutput.setBytes(outputPosition, second == null ? NULLS_LAST_SENTINEL : second);
                    thirdOutput.setBytes(outputPosition, third == null ? NULLS_LAST_SENTINEL : third);

                    int days = shipDate - integerValue(soldDateValues, position);
                    bucket30.values()[outputPosition] = days <= 30 ? 1 : 0;
                    bucket31To60.values()[outputPosition] = days > 30 && days <= 60 ? 1 : 0;
                    bucket61To90.values()[outputPosition] = days > 60 && days <= 90 ? 1 : 0;
                    bucket91To120.values()[outputPosition] = days > 90 && days <= 120 ? 1 : 0;
                    bucketOver120.values()[outputPosition] = days > 120 ? 1 : 0;
                    outputPosition++;
                }

                Mask outputMask = allocator.allocateRangeMask(ALLOCATION_CONTEXT, 0, selectedCount);
                nextBatch = new Batch(
                        outputMask,
                        _ -> {},
                        takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                        releasedMask -> allocator.release(ALLOCATION_CONTEXT, releasedMask),
                        sourceBatch::close,
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, firstOutput),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, secondOutput),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, thirdOutput),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, bucket30),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, bucket31To60),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, bucket61To90),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, bucket91To120),
                        valuesOnlyOutput(allocator, ALLOCATION_CONTEXT, bucketOver120));
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
