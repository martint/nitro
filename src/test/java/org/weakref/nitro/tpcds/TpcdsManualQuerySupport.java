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

import io.trino.spi.type.SqlDate;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Row;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TpcdsManualQuerySupport
{
    private TpcdsManualQuerySupport() {}

    public static List<Row> query17Rows(Allocator allocator, TpcdsParquetTables tables)
    {
        Set<Long> storeSalesDates = dateKeys(allocator, tables, Set.of("2001Q1"));
        Set<Long> returnDates = dateKeys(allocator, tables, Set.of("2001Q1", "2001Q2", "2001Q3"));

        Map<ItemCustomerTicketKey, List<Integer>> returnQuantities = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "store_returns", "sr_returned_date_sk", "sr_item_sk", "sr_customer_sk", "sr_ticket_number", "sr_return_quantity")) {
            if (hasNull(row, 0, 1, 2, 3, 4)) {
                continue;
            }
            long returnedDateKey = ((Number) row.values()[0]).longValue();
            if (!returnDates.contains(returnedDateKey)) {
                continue;
            }
            ItemCustomerTicketKey key = new ItemCustomerTicketKey(
                    ((Number) row.values()[1]).longValue(),
                    ((Number) row.values()[2]).longValue(),
                    ((Number) row.values()[3]).longValue());
            returnQuantities.computeIfAbsent(key, ignored -> new ArrayList<>())
                    .add(((Number) row.values()[4]).intValue());
        }

        Map<ItemCustomerKey, List<Integer>> catalogQuantities = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_item_sk", "cs_bill_customer_sk", "cs_quantity")) {
            if (hasNull(row, 0, 1, 2, 3)) {
                continue;
            }
            long soldDateKey = ((Number) row.values()[0]).longValue();
            if (!returnDates.contains(soldDateKey)) {
                continue;
            }
            ItemCustomerKey key = new ItemCustomerKey(
                    ((Number) row.values()[1]).longValue(),
                    ((Number) row.values()[2]).longValue());
            catalogQuantities.computeIfAbsent(key, ignored -> new ArrayList<>())
                    .add(((Number) row.values()[3]).intValue());
        }

        Map<Long, String[]> itemLookup = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "item", "i_item_sk", "i_item_id", "i_item_desc")) {
            if (hasNull(row, 0)) {
                continue;
            }
            itemLookup.put(((Number) row.values()[0]).longValue(), new String[] {(String) row.values()[1], (String) row.values()[2]});
        }

        Map<Long, String> storeStateLookup = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "store", "s_store_sk", "s_state")) {
            if (hasNull(row, 0)) {
                continue;
            }
            storeStateLookup.put(((Number) row.values()[0]).longValue(), (String) row.values()[1]);
        }

        Map<GroupKey, GroupState> groups = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_ticket_number", "ss_quantity")) {
            if (hasNull(row, 0, 1, 2, 3, 4, 5)) {
                continue;
            }
            long soldDateKey = ((Number) row.values()[0]).longValue();
            if (!storeSalesDates.contains(soldDateKey)) {
                continue;
            }

            long itemKey = ((Number) row.values()[1]).longValue();
            long customerKey = ((Number) row.values()[2]).longValue();
            long storeKey = ((Number) row.values()[3]).longValue();
            long ticketNumber = ((Number) row.values()[4]).longValue();
            int storeSalesQuantity = ((Number) row.values()[5]).intValue();

            List<Integer> matchedReturns = returnQuantities.get(new ItemCustomerTicketKey(itemKey, customerKey, ticketNumber));
            if (matchedReturns == null) {
                continue;
            }
            List<Integer> matchedCatalogSales = catalogQuantities.get(new ItemCustomerKey(itemKey, customerKey));
            if (matchedCatalogSales == null) {
                continue;
            }

            String[] item = itemLookup.get(itemKey);
            String state = storeStateLookup.get(storeKey);
            if (item == null || state == null) {
                continue;
            }

            GroupKey groupKey = new GroupKey(item[0], item[1], state);
            GroupState group = groups.computeIfAbsent(groupKey, ignored -> new GroupState());
            for (Integer returnQuantity : matchedReturns) {
                for (Integer catalogQuantity : matchedCatalogSales) {
                    group.storeSales.add(storeSalesQuantity);
                    group.storeReturns.add(returnQuantity);
                    group.catalogSales.add(catalogQuantity);
                }
            }
        }

        return groups.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator
                        .comparing(GroupKey::itemId, Comparator.nullsLast(String::compareTo))
                        .thenComparing(GroupKey::itemDescription, Comparator.nullsLast(String::compareTo))
                        .thenComparing(GroupKey::state, Comparator.nullsLast(String::compareTo))))
                .limit(100)
                .map(entry -> {
                    GroupKey key = entry.getKey();
                    GroupState state = entry.getValue();
                    return new Row(
                            key.itemId(),
                            key.itemDescription(),
                            key.state(),
                            state.storeSales.count(),
                            state.storeSales.average(),
                            state.storeSales.standardDeviationSample(),
                            state.storeSales.coefficientOfVariation(),
                            state.storeReturns.count(),
                            state.storeReturns.average(),
                            state.storeReturns.standardDeviationSample(),
                            state.storeReturns.coefficientOfVariation(),
                            state.catalogSales.count(),
                            state.catalogSales.average(),
                            state.catalogSales.standardDeviationSample(),
                            state.catalogSales.coefficientOfVariation());
                })
                .toList();
    }

    public static List<Row> query39Rows(Allocator allocator, TpcdsParquetTables tables)
    {
        Map<Long, Integer> trackedMonths = monthNumbers(allocator, tables, 2001, Set.of(1, 2));
        Map<InventoryKey, MonthStatistics> statistics = inventoryStatisticsByMonth(allocator, tables, trackedMonths);

        return statistics.entrySet().stream()
                .filter(entry -> hasCoefficientAbove(entry.getValue().january(), 1.5))
                .flatMap(entry -> {
                    Statistics februaryStats = entry.getValue().february();
                    if (!hasCoefficientAbove(februaryStats, 1.0)) {
                        return java.util.stream.Stream.empty();
                    }
                    return java.util.stream.Stream.of(new Row(
                            entry.getKey().warehouseKey(),
                            entry.getKey().itemKey(),
                            1,
                            entry.getValue().january().average(),
                            entry.getValue().january().coefficientOfVariation(),
                            entry.getKey().warehouseKey(),
                            entry.getKey().itemKey(),
                            2,
                            februaryStats.average(),
                            februaryStats.coefficientOfVariation()));
                })
                .sorted(Comparator
                        .comparing((Row row) -> ((Number) row.values()[0]).longValue())
                        .thenComparing(row -> ((Number) row.values()[1]).longValue())
                        .thenComparing(row -> ((Number) row.values()[2]).intValue())
                        .thenComparing(row -> ((Double) row.values()[3]))
                        .thenComparing(row -> ((Double) row.values()[4]))
                        .thenComparing(row -> ((Number) row.values()[7]).intValue())
                .thenComparing(row -> ((Double) row.values()[8]))
                .thenComparing(row -> ((Double) row.values()[9])))
                .toList();
    }

    public static List<Row> query64Rows(Allocator allocator, TpcdsParquetTables tables)
    {
        Set<Long> qualifyingItems = query64QualifiedCatalogItems(allocator, tables);

        Map<Long, Integer> dateYears = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "date_dim", "d_date_sk", "d_year")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            dateYears.put(((Number) row.values()[0]).longValue(), ((Number) row.values()[1]).intValue());
        }

        Map<Long, String> maritalStatusByDemo = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "customer_demographics", "cd_demo_sk", "cd_marital_status")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            maritalStatusByDemo.put(((Number) row.values()[0]).longValue(), (String) row.values()[1]);
        }

        Map<Long, Long> incomeBandByHousehold = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "household_demographics", "hd_demo_sk", "hd_income_band_sk")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            incomeBandByHousehold.put(((Number) row.values()[0]).longValue(), ((Number) row.values()[1]).longValue());
        }

        Set<Long> incomeBands = new HashSet<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "income_band", "ib_income_band_sk")) {
            if (hasNull(row, 0)) {
                continue;
            }
            incomeBands.add(((Number) row.values()[0]).longValue());
        }

        Set<Long> promotions = new HashSet<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "promotion", "p_promo_sk")) {
            if (hasNull(row, 0)) {
                continue;
            }
            promotions.add(((Number) row.values()[0]).longValue());
        }

        Map<Long, Query64StoreInfo> stores = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "store", "s_store_sk", "s_store_name", "s_zip")) {
            if (hasNull(row, 0, 1, 2)) {
                continue;
            }
            stores.put(((Number) row.values()[0]).longValue(), new Query64StoreInfo((String) row.values()[1], (String) row.values()[2]));
        }

        Map<Long, Query64AddressInfo> addresses = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "customer_address", "ca_address_sk", "ca_street_number", "ca_street_name", "ca_city", "ca_zip")) {
            if (hasNull(row, 0, 1, 2, 3, 4)) {
                continue;
            }
            addresses.put(((Number) row.values()[0]).longValue(), new Query64AddressInfo(
                    (String) row.values()[1],
                    (String) row.values()[2],
                    (String) row.values()[3],
                    (String) row.values()[4]));
        }

        Map<Long, Query64ItemInfo> items = new HashMap<>();
        Set<String> acceptedColors = Set.of("purple", "burlywood", "indian", "spring", "floral", "medium");
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "item", "i_item_sk", "i_product_name", "i_color", "i_current_price")) {
            if (hasNull(row, 0, 1, 2, 3)) {
                continue;
            }
            long itemKey = ((Number) row.values()[0]).longValue();
            long currentPrice = decimalCents(row.values()[3]);
            String color = normalizeString((String) row.values()[2]);
            if (!qualifyingItems.contains(itemKey) || !acceptedColors.contains(color) || currentPrice < 6_500L || currentPrice > 7_400L) {
                continue;
            }
            items.put(itemKey, new Query64ItemInfo((String) row.values()[1], currentPrice));
        }

        Map<Long, Query64CustomerInfo> customers = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(
                allocator,
                tables,
                "customer",
                "c_customer_sk",
                "c_current_cdemo_sk",
                "c_current_hdemo_sk",
                "c_current_addr_sk",
                "c_first_sales_date_sk",
                "c_first_shipto_date_sk")) {
            if (hasNull(row, 0, 1, 2, 3, 4, 5)) {
                continue;
            }
            customers.put(((Number) row.values()[0]).longValue(), new Query64CustomerInfo(
                    ((Number) row.values()[1]).longValue(),
                    ((Number) row.values()[2]).longValue(),
                    ((Number) row.values()[3]).longValue(),
                    ((Number) row.values()[4]).longValue(),
                    ((Number) row.values()[5]).longValue()));
        }

        Map<ItemTicketKey, Long> returnCountByKey = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "store_returns", "sr_item_sk", "sr_ticket_number")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            returnCountByKey.merge(
                    new ItemTicketKey(((Number) row.values()[0]).longValue(), ((Number) row.values()[1]).longValue()),
                    1L,
                    Long::sum);
        }

        Map<Query64CrossSalesKey, Query64CrossSalesAggregate> grouped = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(
                allocator,
                tables,
                "store_sales",
                "ss_store_sk",
                "ss_sold_date_sk",
                "ss_customer_sk",
                "ss_cdemo_sk",
                "ss_hdemo_sk",
                "ss_addr_sk",
                "ss_item_sk",
                "ss_ticket_number",
                "ss_promo_sk",
                "ss_wholesale_cost",
                "ss_list_price",
                "ss_coupon_amt")) {
            if (hasNull(row, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)) {
                continue;
            }

            long storeKey = ((Number) row.values()[0]).longValue();
            long soldDateKey = ((Number) row.values()[1]).longValue();
            long customerKey = ((Number) row.values()[2]).longValue();
            long soldDemoKey = ((Number) row.values()[3]).longValue();
            long soldHouseholdKey = ((Number) row.values()[4]).longValue();
            long soldAddressKey = ((Number) row.values()[5]).longValue();
            long itemKey = ((Number) row.values()[6]).longValue();
            long ticketNumber = ((Number) row.values()[7]).longValue();
            long promoKey = ((Number) row.values()[8]).longValue();

            Query64ItemInfo item = items.get(itemKey);
            if (item == null) {
                continue;
            }
            Integer soldYear = dateYears.get(soldDateKey);
            if (soldYear == null || (soldYear != 1999 && soldYear != 2000)) {
                continue;
            }
            Query64StoreInfo store = stores.get(storeKey);
            if (store == null || !promotions.contains(promoKey)) {
                continue;
            }
            Query64CustomerInfo customer = customers.get(customerKey);
            if (customer == null) {
                continue;
            }
            String soldMaritalStatus = maritalStatusByDemo.get(soldDemoKey);
            String currentMaritalStatus = maritalStatusByDemo.get(customer.currentDemoKey());
            if (soldMaritalStatus == null || currentMaritalStatus == null || normalizeString(soldMaritalStatus).equals(normalizeString(currentMaritalStatus))) {
                continue;
            }

            Long soldIncomeBand = incomeBandByHousehold.get(soldHouseholdKey);
            Long currentIncomeBand = incomeBandByHousehold.get(customer.currentHouseholdKey());
            if (soldIncomeBand == null || currentIncomeBand == null || !incomeBands.contains(soldIncomeBand) || !incomeBands.contains(currentIncomeBand)) {
                continue;
            }

            Query64AddressInfo billingAddress = addresses.get(soldAddressKey);
            Query64AddressInfo currentAddress = addresses.get(customer.currentAddressKey());
            Integer firstSalesYear = dateYears.get(customer.firstSalesDateKey());
            Integer firstShiptoYear = dateYears.get(customer.firstShiptoDateKey());
            Long returnCount = returnCountByKey.get(new ItemTicketKey(itemKey, ticketNumber));
            if (billingAddress == null || currentAddress == null || firstSalesYear == null || firstShiptoYear == null || returnCount == null) {
                continue;
            }

            Query64CrossSalesKey key = new Query64CrossSalesKey(
                    item.productName(),
                    itemKey,
                    store.storeName(),
                    store.storeZip(),
                    billingAddress.streetNumber(),
                    billingAddress.streetName(),
                    billingAddress.city(),
                    billingAddress.zip(),
                    currentAddress.streetNumber(),
                    currentAddress.streetName(),
                    currentAddress.city(),
                    currentAddress.zip(),
                    soldYear,
                    firstSalesYear,
                    firstShiptoYear);

            Query64CrossSalesAggregate aggregate = grouped.computeIfAbsent(key, ignored -> new Query64CrossSalesAggregate());
            aggregate.add(
                    returnCount,
                    decimalCents(row.values()[9]),
                    decimalCents(row.values()[10]),
                    decimalCents(row.values()[11]));
        }

        Map<Query64JoinKey, List<Query64CrossSalesRow>> groupedByJoinKey = new HashMap<>();
        for (Map.Entry<Query64CrossSalesKey, Query64CrossSalesAggregate> entry : grouped.entrySet()) {
            Query64CrossSalesKey key = entry.getKey();
            Query64CrossSalesAggregate aggregate = entry.getValue();
            groupedByJoinKey.computeIfAbsent(
                            new Query64JoinKey(key.itemKey(), key.storeName(), key.storeZip()),
                            ignored -> new ArrayList<>())
                    .add(new Query64CrossSalesRow(key, aggregate.count(), aggregate.sumWholesaleCost(), aggregate.sumListPrice(), aggregate.sumCouponAmount()));
        }

        List<Row> result = new ArrayList<>();
        for (List<Query64CrossSalesRow> rows : groupedByJoinKey.values()) {
            List<Query64CrossSalesRow> baseRows = rows.stream()
                    .filter(row -> row.key().soldYear() == 1999)
                    .toList();
            List<Query64CrossSalesRow> comparisonRows = rows.stream()
                    .filter(row -> row.key().soldYear() == 2000)
                    .toList();

            for (Query64CrossSalesRow first : baseRows) {
                for (Query64CrossSalesRow second : comparisonRows) {
                    if (second.count() > first.count()) {
                        continue;
                    }
                    result.add(new Row(
                            first.key().productName(),
                            first.key().storeName(),
                            first.key().storeZip(),
                            first.key().billingStreetNumber(),
                            first.key().billingStreetName(),
                            first.key().billingCity(),
                            first.key().billingZip(),
                            first.key().currentStreetNumber(),
                            first.key().currentStreetName(),
                            first.key().currentCity(),
                            first.key().currentZip(),
                            first.key().soldYear(),
                            first.count(),
                            first.sumWholesaleCost(),
                            first.sumListPrice(),
                            first.sumCouponAmount(),
                            second.sumWholesaleCost(),
                            second.sumListPrice(),
                            second.sumCouponAmount(),
                            second.key().soldYear(),
                            second.count()));
                }
            }
        }

        result.sort(Comparator
                .comparing((Row row) -> normalizeString((String) row.values()[0]), Comparator.nullsLast(String::compareTo))
                .thenComparing(row -> normalizeString((String) row.values()[1]), Comparator.nullsLast(String::compareTo))
                .thenComparingLong(row -> ((Number) row.values()[20]).longValue())
                .thenComparingLong(row -> ((Number) row.values()[13]).longValue())
                .thenComparingLong(row -> ((Number) row.values()[14]).longValue())
                .thenComparingLong(row -> ((Number) row.values()[15]).longValue())
                .thenComparingLong(row -> ((Number) row.values()[16]).longValue())
                .thenComparingLong(row -> ((Number) row.values()[17]).longValue())
                .thenComparingLong(row -> ((Number) row.values()[18]).longValue())
                .thenComparing(row -> normalizeString((String) row.values()[2]), Comparator.nullsLast(String::compareTo))
                .thenComparing(row -> normalizeString((String) row.values()[3]), Comparator.nullsLast(String::compareTo))
                .thenComparing(row -> normalizeString((String) row.values()[4]), Comparator.nullsLast(String::compareTo))
                .thenComparing(row -> normalizeString((String) row.values()[5]), Comparator.nullsLast(String::compareTo))
                .thenComparing(row -> normalizeString((String) row.values()[6]), Comparator.nullsLast(String::compareTo))
                .thenComparing(row -> normalizeString((String) row.values()[7]), Comparator.nullsLast(String::compareTo))
                .thenComparing(row -> normalizeString((String) row.values()[8]), Comparator.nullsLast(String::compareTo))
                .thenComparing(row -> normalizeString((String) row.values()[9]), Comparator.nullsLast(String::compareTo))
                .thenComparing(row -> normalizeString((String) row.values()[10]), Comparator.nullsLast(String::compareTo))
                .thenComparingInt(row -> ((Number) row.values()[11]).intValue())
                .thenComparingLong(row -> ((Number) row.values()[12]).longValue())
                .thenComparingInt(row -> ((Number) row.values()[19]).intValue()));
        return result;
    }

    public static List<Row> query05Rows(Allocator allocator, TpcdsParquetTables tables)
    {
        Set<Long> allowedDates = new HashSet<>();
        long startDate = LocalDate.of(2000, 8, 23).toEpochDay();
        long endDate = LocalDate.of(2000, 9, 6).toEpochDay();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "date_dim", "d_date_sk", "d_date")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            long epochDay = dateDays(row.values()[1]);
            if (epochDay >= startDate && epochDay <= endDate) {
                allowedDates.add(((Number) row.values()[0]).longValue());
            }
        }

        Map<Long, String> storeIds = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "store", "s_store_sk", "s_store_id")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            storeIds.put(((Number) row.values()[0]).longValue(), (String) row.values()[1]);
        }

        Map<Long, String> catalogPageIds = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "catalog_page", "cp_catalog_page_sk", "cp_catalog_page_id")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            catalogPageIds.put(((Number) row.values()[0]).longValue(), (String) row.values()[1]);
        }

        Map<Long, String> webSiteIds = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "web_site", "web_site_sk", "web_site_id")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            webSiteIds.put(((Number) row.values()[0]).longValue(), (String) row.values()[1]);
        }

        Map<String, Query05Aggregate> storeChannel = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "store_sales", "ss_sold_date_sk", "ss_store_sk", "ss_ext_sales_price", "ss_net_profit")) {
            if (hasNull(row, 0, 1, 2, 3)) {
                continue;
            }
            if (!allowedDates.contains(((Number) row.values()[0]).longValue())) {
                continue;
            }
            String storeId = storeIds.get(((Number) row.values()[1]).longValue());
            if (storeId == null) {
                continue;
            }
            storeChannel.computeIfAbsent("store" + storeId, ignored -> new Query05Aggregate())
                    .add(decimalCents(row.values()[2]), 0L, decimalCents(row.values()[3]), 0L);
        }
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "store_returns", "sr_returned_date_sk", "sr_store_sk", "sr_return_amt", "sr_net_loss")) {
            if (hasNull(row, 0, 1, 2, 3)) {
                continue;
            }
            if (!allowedDates.contains(((Number) row.values()[0]).longValue())) {
                continue;
            }
            String storeId = storeIds.get(((Number) row.values()[1]).longValue());
            if (storeId == null) {
                continue;
            }
            storeChannel.computeIfAbsent("store" + storeId, ignored -> new Query05Aggregate())
                    .add(0L, decimalCents(row.values()[2]), 0L, decimalCents(row.values()[3]));
        }

        Map<String, Query05Aggregate> catalogChannel = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "catalog_sales", "cs_sold_date_sk", "cs_catalog_page_sk", "cs_ext_sales_price", "cs_net_profit")) {
            if (hasNull(row, 0, 1, 2, 3)) {
                continue;
            }
            if (!allowedDates.contains(((Number) row.values()[0]).longValue())) {
                continue;
            }
            String catalogPageId = catalogPageIds.get(((Number) row.values()[1]).longValue());
            if (catalogPageId == null) {
                continue;
            }
            catalogChannel.computeIfAbsent("catalog_page" + catalogPageId, ignored -> new Query05Aggregate())
                    .add(decimalCents(row.values()[2]), 0L, decimalCents(row.values()[3]), 0L);
        }
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "catalog_returns", "cr_returned_date_sk", "cr_catalog_page_sk", "cr_return_amount", "cr_net_loss")) {
            if (hasNull(row, 0, 1, 2, 3)) {
                continue;
            }
            if (!allowedDates.contains(((Number) row.values()[0]).longValue())) {
                continue;
            }
            String catalogPageId = catalogPageIds.get(((Number) row.values()[1]).longValue());
            if (catalogPageId == null) {
                continue;
            }
            catalogChannel.computeIfAbsent("catalog_page" + catalogPageId, ignored -> new Query05Aggregate())
                    .add(0L, decimalCents(row.values()[2]), 0L, decimalCents(row.values()[3]));
        }

        Map<ItemTicketKey, Map<Long, Long>> webSiteMatchCounts = new HashMap<>();
        Map<String, Query05Aggregate> webChannel = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "web_sales", "ws_sold_date_sk", "ws_web_site_sk", "ws_ext_sales_price", "ws_net_profit", "ws_item_sk", "ws_order_number")) {
            if (hasNull(row, 0, 1, 2, 3, 4, 5)) {
                continue;
            }
            long webSiteKey = ((Number) row.values()[1]).longValue();
            String webSiteId = webSiteIds.get(webSiteKey);
            if (webSiteId == null) {
                continue;
            }
            webSiteMatchCounts
                    .computeIfAbsent(new ItemTicketKey(((Number) row.values()[4]).longValue(), ((Number) row.values()[5]).longValue()), ignored -> new HashMap<>())
                    .merge(webSiteKey, 1L, Long::sum);
            if (!allowedDates.contains(((Number) row.values()[0]).longValue())) {
                continue;
            }
            webChannel.computeIfAbsent("web_site" + webSiteId, ignored -> new Query05Aggregate())
                    .add(decimalCents(row.values()[2]), 0L, decimalCents(row.values()[3]), 0L);
        }
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "web_returns", "wr_item_sk", "wr_order_number", "wr_returned_date_sk", "wr_return_amt", "wr_net_loss")) {
            if (hasNull(row, 0, 1, 2, 3, 4)) {
                continue;
            }
            if (!allowedDates.contains(((Number) row.values()[2]).longValue())) {
                continue;
            }
            Map<Long, Long> siteMatches = webSiteMatchCounts.get(new ItemTicketKey(((Number) row.values()[0]).longValue(), ((Number) row.values()[1]).longValue()));
            if (siteMatches == null) {
                continue;
            }
            long returnAmount = decimalCents(row.values()[3]);
            long netLoss = decimalCents(row.values()[4]);
            for (Map.Entry<Long, Long> match : siteMatches.entrySet()) {
                String webSiteId = webSiteIds.get(match.getKey());
                if (webSiteId == null) {
                    continue;
                }
                long multiplicity = match.getValue();
                webChannel.computeIfAbsent("web_site" + webSiteId, ignored -> new Query05Aggregate())
                        .add(0L, returnAmount * multiplicity, 0L, netLoss * multiplicity);
            }
        }

        Map<Query05RollupKey, Query05Aggregate> rolledUp = new HashMap<>();
        query05AddChannelRows(rolledUp, "store channel", storeChannel);
        query05AddChannelRows(rolledUp, "catalog channel", catalogChannel);
        query05AddChannelRows(rolledUp, "web channel", webChannel);

        List<Row> rows = rolledUp.entrySet().stream()
                .map(entry -> new Row(
                        entry.getKey().channel(),
                        entry.getKey().id(),
                        entry.getValue().sales(),
                        entry.getValue().returns(),
                        entry.getValue().profit()))
                .sorted(Comparator
                        .comparing((Row row) -> normalizeString((String) row.values()[0]), Comparator.nullsLast(String::compareTo))
                        .thenComparing(row -> normalizeString((String) row.values()[1]), Comparator.nullsLast(String::compareTo)))
                .limit(100)
                .toList();
        return rows;
    }

    public static List<Row> query78Rows(Allocator allocator, TpcdsParquetTables tables)
    {
        Set<Long> allowedDates = new HashSet<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "date_dim", "d_date_sk", "d_year")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            if (((Number) row.values()[1]).intValue() == 1998) {
                allowedDates.add(((Number) row.values()[0]).longValue());
            }
        }

        Map<Query78ChannelKey, Query78ChannelAggregate> storeChannel = query78ChannelRows(
                allocator,
                tables,
                allowedDates,
                query78ReturnKeys(allocator, tables, "store_returns", "sr_item_sk", "sr_ticket_number"),
                "store_sales",
                "ss_sold_date_sk",
                "ss_item_sk",
                "ss_customer_sk",
                "ss_ticket_number",
                "ss_quantity",
                "ss_wholesale_cost",
                "ss_sales_price");
        Map<Query78ChannelKey, Query78ChannelAggregate> webChannel = query78ChannelRows(
                allocator,
                tables,
                allowedDates,
                query78ReturnKeys(allocator, tables, "web_returns", "wr_item_sk", "wr_order_number"),
                "web_sales",
                "ws_sold_date_sk",
                "ws_item_sk",
                "ws_bill_customer_sk",
                "ws_order_number",
                "ws_quantity",
                "ws_wholesale_cost",
                "ws_sales_price");
        Map<Query78ChannelKey, Query78ChannelAggregate> catalogChannel = query78ChannelRows(
                allocator,
                tables,
                allowedDates,
                query78ReturnKeys(allocator, tables, "catalog_returns", "cr_item_sk", "cr_order_number"),
                "catalog_sales",
                "cs_sold_date_sk",
                "cs_item_sk",
                "cs_bill_customer_sk",
                "cs_order_number",
                "cs_quantity",
                "cs_wholesale_cost",
                "cs_sales_price");

        return storeChannel.entrySet().stream()
                .map(entry -> {
                    Query78ChannelKey key = entry.getKey();
                    Query78ChannelAggregate store = entry.getValue();
                    Query78ChannelAggregate web = webChannel.get(key);
                    Query78ChannelAggregate catalog = catalogChannel.get(key);
                    long otherQuantity = (web == null ? 0L : web.quantity()) + (catalog == null ? 0L : catalog.quantity());
                    if (otherQuantity <= 0) {
                        return null;
                    }
                    long otherWholesaleCost = (web == null ? 0L : web.wholesaleCost()) + (catalog == null ? 0L : catalog.wholesaleCost());
                    long otherSalesPrice = (web == null ? 0L : web.salesPrice()) + (catalog == null ? 0L : catalog.salesPrice());
                    return new Query78ResultRow(
                            key.soldYear(),
                            key.itemKey(),
                            key.customerKey(),
                            divideScaleRound(store.quantity(), otherQuantity, 2),
                            store.quantity(),
                            store.wholesaleCost(),
                            store.salesPrice(),
                            otherQuantity,
                            otherWholesaleCost,
                            otherSalesPrice);
                })
                .filter(java.util.Objects::nonNull)
                .sorted(Comparator
                        .comparingLong(Query78ResultRow::customerKey)
                        .thenComparing(Query78ResultRow::storeQuantity, Comparator.reverseOrder())
                        .thenComparing(Query78ResultRow::storeWholesaleCost, Comparator.reverseOrder())
                        .thenComparing(Query78ResultRow::storeSalesPrice, Comparator.reverseOrder())
                        .thenComparing(Query78ResultRow::otherQuantity)
                        .thenComparing(Query78ResultRow::otherWholesaleCost)
                        .thenComparing(Query78ResultRow::otherSalesPrice)
                        .thenComparing(Query78ResultRow::ratio)
                        .thenComparing(Query78ResultRow::soldYear)
                        .thenComparing(Query78ResultRow::itemKey))
                .limit(100)
                .map(row -> new Row(
                        row.customerKey(),
                        row.ratio(),
                        row.storeQuantity(),
                        row.storeWholesaleCost(),
                        row.storeSalesPrice(),
                        row.otherQuantity(),
                        row.otherWholesaleCost(),
                        row.otherSalesPrice()))
                .toList();
    }

    public static List<Row> query72Rows(Allocator allocator, TpcdsParquetTables tables)
    {
        Map<Long, Query72DateInfo> dateInfo = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "date_dim", "d_date_sk", "d_date", "d_week_seq", "d_year")) {
            if (hasNull(row, 0, 1, 2, 3)) {
                continue;
            }
            dateInfo.put(((Number) row.values()[0]).longValue(), new Query72DateInfo(
                    dateDays(row.values()[1]),
                    ((Number) row.values()[2]).longValue(),
                    ((Number) row.values()[3]).intValue()));
        }

        Set<Long> eligibleCustomerDemographics = new HashSet<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "customer_demographics", "cd_demo_sk", "cd_marital_status")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            if ("D".equals(normalizeString((String) row.values()[1]))) {
                eligibleCustomerDemographics.add(((Number) row.values()[0]).longValue());
            }
        }

        Set<Long> eligibleHouseholds = new HashSet<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "household_demographics", "hd_demo_sk", "hd_buy_potential")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            if (">10000".equals(normalizeString((String) row.values()[1]))) {
                eligibleHouseholds.add(((Number) row.values()[0]).longValue());
            }
        }

        Set<Long> promotions = new HashSet<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "promotion", "p_promo_sk")) {
            if (hasNull(row, 0)) {
                continue;
            }
            promotions.add(((Number) row.values()[0]).longValue());
        }

        Map<Long, String> itemDescriptions = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "item", "i_item_sk", "i_item_desc")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            itemDescriptions.put(((Number) row.values()[0]).longValue(), (String) row.values()[1]);
        }

        Map<Long, String> warehouseNames = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "warehouse", "w_warehouse_sk", "w_warehouse_name")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            warehouseNames.put(((Number) row.values()[0]).longValue(), (String) row.values()[1]);
        }

        Map<ItemTicketKey, Long> returnCounts = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "catalog_returns", "cr_item_sk", "cr_order_number")) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            returnCounts.merge(
                    new ItemTicketKey(((Number) row.values()[0]).longValue(), ((Number) row.values()[1]).longValue()),
                    1L,
                    Long::sum);
        }

        Map<Query72ItemWeekKey, Query72SalesHistogram> salesHistograms = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "catalog_sales", "cs_item_sk", "cs_bill_cdemo_sk", "cs_bill_hdemo_sk", "cs_sold_date_sk", "cs_ship_date_sk", "cs_promo_sk", "cs_order_number", "cs_quantity")) {
            if (hasNull(row, 0, 1, 2, 3, 4, 6, 7)) {
                continue;
            }
            long itemKey = ((Number) row.values()[0]).longValue();
            if (!eligibleCustomerDemographics.contains(((Number) row.values()[1]).longValue()) ||
                    !eligibleHouseholds.contains(((Number) row.values()[2]).longValue())) {
                continue;
            }

            Query72DateInfo soldDate = dateInfo.get(((Number) row.values()[3]).longValue());
            Query72DateInfo shipDate = dateInfo.get(((Number) row.values()[4]).longValue());
            if (soldDate == null || shipDate == null || soldDate.year() != 1999 || shipDate.day() <= soldDate.day() + 5) {
                continue;
            }

            String itemDescription = itemDescriptions.get(itemKey);
            if (itemDescription == null) {
                continue;
            }
            long multiplicity = returnCounts.getOrDefault(
                    new ItemTicketKey(itemKey, ((Number) row.values()[6]).longValue()),
                    1L);
            boolean hasPromotion = row.values()[5] != null && promotions.contains(((Number) row.values()[5]).longValue());
            long quantity = ((Number) row.values()[7]).longValue();
            salesHistograms.computeIfAbsent(
                            new Query72ItemWeekKey(itemKey, soldDate.weekSequence()),
                            ignored -> new Query72SalesHistogram(itemDescription))
                    .add(quantity, hasPromotion, multiplicity);
        }

        Map<Query72GroupKey, Query72Aggregate> grouped = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "inventory", "inv_item_sk", "inv_warehouse_sk", "inv_date_sk", "inv_quantity_on_hand")) {
            if (hasNull(row, 0, 1, 2, 3)) {
                continue;
            }
            long itemKey = ((Number) row.values()[0]).longValue();
            Query72DateInfo inventoryDate = dateInfo.get(((Number) row.values()[2]).longValue());
            if (inventoryDate == null) {
                continue;
            }
            Query72SalesHistogram salesHistogram = salesHistograms.get(new Query72ItemWeekKey(itemKey, inventoryDate.weekSequence()));
            if (salesHistogram == null) {
                continue;
            }
            Query72Counts counts = salesHistogram.countsForInventoryQuantity(((Number) row.values()[3]).longValue());
            if (counts.noPromotionCount() == 0 && counts.promotionCount() == 0) {
                continue;
            }
            String warehouseName = warehouseNames.get(((Number) row.values()[1]).longValue());
            if (warehouseName == null) {
                continue;
            }
            grouped.computeIfAbsent(
                            new Query72GroupKey(salesHistogram.itemDescription(), warehouseName, inventoryDate.weekSequence()),
                            ignored -> new Query72Aggregate())
                    .add(counts.noPromotionCount(), counts.promotionCount());
        }

        return grouped.entrySet().stream()
                .sorted(Comparator
                        .comparingLong((Map.Entry<Query72GroupKey, Query72Aggregate> entry) -> entry.getValue().totalCount()).reversed()
                        .thenComparing(entry -> normalizeString(entry.getKey().itemDescription()), Comparator.nullsLast(String::compareTo))
                        .thenComparing(entry -> normalizeString(entry.getKey().warehouseName()), Comparator.nullsLast(String::compareTo))
                        .thenComparingLong(entry -> entry.getKey().weekSequence()))
                .limit(100)
                .map(entry -> new Row(
                        entry.getKey().itemDescription(),
                        entry.getKey().warehouseName(),
                        entry.getKey().weekSequence(),
                        entry.getValue().noPromotionCount(),
                        entry.getValue().promotionCount(),
                        entry.getValue().totalCount()))
                .toList();
    }

    private static Set<Long> dateKeys(Allocator allocator, TpcdsParquetTables tables, Set<String> quarterNames)
    {
        return TpcdsParquetSupport.scanRows(allocator, tables, "date_dim", "d_date_sk", "d_quarter_name").stream()
                .filter(row -> !hasNull(row, 0, 1))
                .filter(row -> quarterNames.contains(((String) row.values()[1]).stripTrailing()))
                .map(row -> ((Number) row.values()[0]).longValue())
                .collect(java.util.stream.Collectors.toSet());
    }

    private static Set<ItemTicketKey> query78ReturnKeys(Allocator allocator, TpcdsParquetTables tables, String table, String itemColumn, String orderColumn)
    {
        Set<ItemTicketKey> returnKeys = new HashSet<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, table, itemColumn, orderColumn)) {
            if (hasNull(row, 0, 1)) {
                continue;
            }
            returnKeys.add(new ItemTicketKey(
                    ((Number) row.values()[0]).longValue(),
                    ((Number) row.values()[1]).longValue()));
        }
        return returnKeys;
    }

    private static Map<Query78ChannelKey, Query78ChannelAggregate> query78ChannelRows(
            Allocator allocator,
            TpcdsParquetTables tables,
            Set<Long> allowedDates,
            Set<ItemTicketKey> returnKeys,
            String table,
            String soldDateColumn,
            String itemColumn,
            String customerColumn,
            String orderColumn,
            String quantityColumn,
            String wholesaleCostColumn,
            String salesPriceColumn)
    {
        Map<Query78ChannelKey, Query78ChannelAggregate> grouped = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, table, soldDateColumn, itemColumn, customerColumn, orderColumn, quantityColumn, wholesaleCostColumn, salesPriceColumn)) {
            if (hasNull(row, 0, 1, 2, 3, 4, 5, 6)) {
                continue;
            }
            long soldDateKey = ((Number) row.values()[0]).longValue();
            if (!allowedDates.contains(soldDateKey)) {
                continue;
            }
            long itemKey = ((Number) row.values()[1]).longValue();
            long orderNumber = ((Number) row.values()[3]).longValue();
            if (returnKeys.contains(new ItemTicketKey(itemKey, orderNumber))) {
                continue;
            }
            Query78ChannelKey key = new Query78ChannelKey(1998, itemKey, ((Number) row.values()[2]).longValue());
            grouped.computeIfAbsent(key, ignored -> new Query78ChannelAggregate())
                    .add(
                            ((Number) row.values()[4]).longValue(),
                            decimalCents(row.values()[5]),
                            decimalCents(row.values()[6]));
        }
        return grouped;
    }

    private static Map<Long, Integer> monthNumbers(Allocator allocator, TpcdsParquetTables tables, int year, Set<Integer> months)
    {
        Map<Long, Integer> trackedDates = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "date_dim", "d_date_sk", "d_year", "d_moy")) {
            if (hasNull(row, 0, 1, 2)) {
                continue;
            }
            if (((Number) row.values()[1]).intValue() != year) {
                continue;
            }
            int month = ((Number) row.values()[2]).intValue();
            if (!months.contains(month)) {
                continue;
            }
            trackedDates.put(((Number) row.values()[0]).longValue(), month);
        }
        return trackedDates;
    }

    private static Map<InventoryKey, MonthStatistics> inventoryStatisticsByMonth(Allocator allocator, TpcdsParquetTables tables, Map<Long, Integer> trackedMonths)
    {
        Map<InventoryKey, MonthStatistics> statistics = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "inventory", "inv_date_sk", "inv_item_sk", "inv_warehouse_sk", "inv_quantity_on_hand")) {
            if (hasNull(row, 0, 1, 2, 3)) {
                continue;
            }
            long dateKey = ((Number) row.values()[0]).longValue();
            Integer month = trackedMonths.get(dateKey);
            if (month == null) {
                continue;
            }
            long itemKey = ((Number) row.values()[1]).longValue();
            long warehouseKey = ((Number) row.values()[2]).longValue();
            MonthStatistics monthStatistics = statistics.computeIfAbsent(new InventoryKey(warehouseKey, itemKey), ignored -> new MonthStatistics());
            if (month == 1) {
                monthStatistics.january().add(((Number) row.values()[3]).longValue());
            }
            else if (month == 2) {
                monthStatistics.february().add(((Number) row.values()[3]).longValue());
            }
        }
        return statistics;
    }

    private static boolean hasCoefficientAbove(Statistics statistics, double threshold)
    {
        if (statistics == null) {
            return false;
        }
        Double average = statistics.average();
        if (average == null || average == 0.0d) {
            return false;
        }
        Double coefficient = statistics.coefficientOfVariation();
        return coefficient != null && coefficient > threshold;
    }

    private static Set<Long> query64QualifiedCatalogItems(Allocator allocator, TpcdsParquetTables tables)
    {
        Map<ItemTicketKey, Query64CatalogReturnsAggregate> returnsByKey = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "catalog_returns", "cr_item_sk", "cr_order_number", "cr_refunded_cash", "cr_reversed_charge", "cr_store_credit")) {
            if (hasNull(row, 0, 1, 2, 3, 4)) {
                continue;
            }
            returnsByKey.computeIfAbsent(
                            new ItemTicketKey(((Number) row.values()[0]).longValue(), ((Number) row.values()[1]).longValue()),
                            ignored -> new Query64CatalogReturnsAggregate())
                    .add(decimalCents(row.values()[2]), decimalCents(row.values()[3]), decimalCents(row.values()[4]));
        }

        Map<ItemTicketKey, Query64CatalogSalesAggregate> salesByKey = new HashMap<>();
        for (Row row : TpcdsParquetSupport.scanRows(allocator, tables, "catalog_sales", "cs_item_sk", "cs_order_number", "cs_ext_list_price")) {
            if (hasNull(row, 0, 1, 2)) {
                continue;
            }
            salesByKey.computeIfAbsent(
                            new ItemTicketKey(((Number) row.values()[0]).longValue(), ((Number) row.values()[1]).longValue()),
                            ignored -> new Query64CatalogSalesAggregate())
                    .add(decimalCents(row.values()[2]));
        }

        Map<Long, Query64CatalogItemAggregate> perItem = new HashMap<>();
        for (Map.Entry<ItemTicketKey, Query64CatalogSalesAggregate> salesEntry : salesByKey.entrySet()) {
            Query64CatalogReturnsAggregate returns = returnsByKey.get(salesEntry.getKey());
            if (returns == null) {
                continue;
            }
            Query64CatalogSalesAggregate sales = salesEntry.getValue();
            perItem.computeIfAbsent(salesEntry.getKey().itemKey(), ignored -> new Query64CatalogItemAggregate())
                    .add(sales.count(), sales.sale(), returns.count(), returns.refund());
        }

        Set<Long> qualifyingItems = new HashSet<>();
        for (Map.Entry<Long, Query64CatalogItemAggregate> entry : perItem.entrySet()) {
            if (entry.getValue().sale() > (2L * entry.getValue().refund())) {
                qualifyingItems.add(entry.getKey());
            }
        }
        return qualifyingItems;
    }

    private static long decimalCents(Object value)
    {
        if (value instanceof BigDecimal decimal) {
            return decimal.unscaledValue().longValueExact();
        }
        return ((Number) value).longValue();
    }

    private static long divideScaleRound(long numerator, long denominator, int scale)
    {
        return BigDecimal.valueOf(numerator)
                .multiply(BigDecimal.TEN.pow(scale))
                .divide(BigDecimal.valueOf(denominator), 0, RoundingMode.HALF_UP)
                .longValueExact();
    }

    private static long dateDays(Object value)
    {
        if (value instanceof LocalDate date) {
            return date.toEpochDay();
        }
        if (value instanceof SqlDate date) {
            return date.getDays();
        }
        return ((Number) value).longValue();
    }

    private static String normalizeString(String value)
    {
        return value == null ? null : value.stripTrailing();
    }

    private static void query05AddChannelRows(Map<Query05RollupKey, Query05Aggregate> rolledUp, String channel, Map<String, Query05Aggregate> detailRows)
    {
        Query05Aggregate channelTotal = rolledUp.computeIfAbsent(new Query05RollupKey(channel, null), ignored -> new Query05Aggregate());
        Query05Aggregate grandTotal = rolledUp.computeIfAbsent(new Query05RollupKey(null, null), ignored -> new Query05Aggregate());
        for (Map.Entry<String, Query05Aggregate> detail : detailRows.entrySet()) {
            Query05Aggregate aggregate = detail.getValue();
            rolledUp.put(new Query05RollupKey(channel, detail.getKey()), aggregate.copy());
            channelTotal.add(aggregate.sales(), aggregate.returns(), aggregate.rawProfit(), aggregate.profitLoss());
            grandTotal.add(aggregate.sales(), aggregate.returns(), aggregate.rawProfit(), aggregate.profitLoss());
        }
    }

    private static boolean hasNull(Row row, int... indexes)
    {
        for (int index : indexes) {
            if (row.values()[index] == null) {
                return true;
            }
        }
        return false;
    }

    private record ItemCustomerTicketKey(long itemKey, long customerKey, long ticketNumber) {}

    private record ItemCustomerKey(long itemKey, long customerKey) {}

    private record ItemTicketKey(long itemKey, long ticketNumber) {}

    private record GroupKey(String itemId, String itemDescription, String state) {}

    private record InventoryKey(long warehouseKey, long itemKey) {}

    private record Query05RollupKey(String channel, String id) {}

    private record Query72DateInfo(long day, long weekSequence, int year) {}

    private record Query72ItemWeekKey(long itemKey, long weekSequence) {}

    private record Query72Counts(long noPromotionCount, long promotionCount) {}

    private record Query72GroupKey(String itemDescription, String warehouseName, long weekSequence) {}

    private record Query78ChannelKey(int soldYear, long itemKey, long customerKey) {}

    private record Query78ResultRow(
            int soldYear,
            long itemKey,
            long customerKey,
            long ratio,
            long storeQuantity,
            long storeWholesaleCost,
            long storeSalesPrice,
            long otherQuantity,
            long otherWholesaleCost,
            long otherSalesPrice) {}

    private record Query64StoreInfo(String storeName, String storeZip) {}

    private record Query64AddressInfo(String streetNumber, String streetName, String city, String zip) {}

    private record Query64ItemInfo(String productName, long currentPrice) {}

    private record Query64CustomerInfo(long currentDemoKey, long currentHouseholdKey, long currentAddressKey, long firstSalesDateKey, long firstShiptoDateKey) {}

    private record Query64JoinKey(long itemKey, String storeName, String storeZip) {}

    private record Query64CrossSalesKey(
            String productName,
            long itemKey,
            String storeName,
            String storeZip,
            String billingStreetNumber,
            String billingStreetName,
            String billingCity,
            String billingZip,
            String currentStreetNumber,
            String currentStreetName,
            String currentCity,
            String currentZip,
            int soldYear,
            int firstSalesYear,
            int firstShiptoYear) {}

    private record Query64CrossSalesRow(Query64CrossSalesKey key, long count, long sumWholesaleCost, long sumListPrice, long sumCouponAmount) {}

    private record MonthStatistics(Statistics january, Statistics february)
    {
        private MonthStatistics()
        {
            this(new Statistics(), new Statistics());
        }
    }

    private static final class GroupState
    {
        private final Statistics storeSales = new Statistics();
        private final Statistics storeReturns = new Statistics();
        private final Statistics catalogSales = new Statistics();
    }

    private static final class Query05Aggregate
    {
        private long sales;
        private long returns;
        private long rawProfit;
        private long profitLoss;

        public void add(long salesValue, long returnsValue, long profitValue, long profitLossValue)
        {
            sales += salesValue;
            returns += returnsValue;
            rawProfit += profitValue;
            profitLoss += profitLossValue;
        }

        public Query05Aggregate copy()
        {
            Query05Aggregate copy = new Query05Aggregate();
            copy.sales = sales;
            copy.returns = returns;
            copy.rawProfit = rawProfit;
            copy.profitLoss = profitLoss;
            return copy;
        }

        public long sales()
        {
            return sales;
        }

        public long returns()
        {
            return returns;
        }

        public long rawProfit()
        {
            return rawProfit;
        }

        public long profitLoss()
        {
            return profitLoss;
        }

        public long profit()
        {
            return rawProfit - profitLoss;
        }
    }

    private static final class Query72Aggregate
    {
        private long noPromotionCount;
        private long promotionCount;

        public void add(boolean hasPromotion, long multiplicity)
        {
            if (hasPromotion) {
                promotionCount += multiplicity;
            }
            else {
                noPromotionCount += multiplicity;
            }
        }

        public void add(long noPromotionMultiplicity, long promotionMultiplicity)
        {
            noPromotionCount += noPromotionMultiplicity;
            promotionCount += promotionMultiplicity;
        }

        public long noPromotionCount()
        {
            return noPromotionCount;
        }

        public long promotionCount()
        {
            return promotionCount;
        }

        public long totalCount()
        {
            return noPromotionCount + promotionCount;
        }
    }

    private static final class Query72SalesHistogram
    {
        private final String itemDescription;
        private final Map<Long, Query72Aggregate> countsByQuantity = new HashMap<>();
        private long[] sortedQuantities;
        private long[] suffixNoPromotionCounts;
        private long[] suffixPromotionCounts;

        private Query72SalesHistogram(String itemDescription)
        {
            this.itemDescription = itemDescription;
        }

        public void add(long quantity, boolean hasPromotion, long multiplicity)
        {
            countsByQuantity.computeIfAbsent(quantity, ignored -> new Query72Aggregate()).add(hasPromotion, multiplicity);
        }

        public Query72Counts countsForInventoryQuantity(long quantityOnHand)
        {
            prepare();
            int index = Arrays.binarySearch(sortedQuantities, quantityOnHand);
            int insertionPoint = index >= 0 ? index + 1 : -index - 1;
            if (insertionPoint >= sortedQuantities.length) {
                return new Query72Counts(0, 0);
            }
            return new Query72Counts(suffixNoPromotionCounts[insertionPoint], suffixPromotionCounts[insertionPoint]);
        }

        public String itemDescription()
        {
            return itemDescription;
        }

        private void prepare()
        {
            if (sortedQuantities != null) {
                return;
            }
            List<Map.Entry<Long, Query72Aggregate>> entries = new ArrayList<>(countsByQuantity.entrySet());
            entries.sort(Map.Entry.comparingByKey());
            sortedQuantities = new long[entries.size()];
            suffixNoPromotionCounts = new long[entries.size()];
            suffixPromotionCounts = new long[entries.size()];
            for (int index = 0; index < entries.size(); index++) {
                Map.Entry<Long, Query72Aggregate> entry = entries.get(index);
                sortedQuantities[index] = entry.getKey();
            }
            long runningNoPromotionCount = 0;
            long runningPromotionCount = 0;
            for (int index = entries.size() - 1; index >= 0; index--) {
                Query72Aggregate aggregate = entries.get(index).getValue();
                runningNoPromotionCount += aggregate.noPromotionCount();
                runningPromotionCount += aggregate.promotionCount();
                suffixNoPromotionCounts[index] = runningNoPromotionCount;
                suffixPromotionCounts[index] = runningPromotionCount;
            }
            countsByQuantity.clear();
        }
    }

    private static final class Query78ChannelAggregate
    {
        private long quantity;
        private long wholesaleCost;
        private long salesPrice;

        public void add(long quantityValue, long wholesaleCostValue, long salesPriceValue)
        {
            quantity += quantityValue;
            wholesaleCost += wholesaleCostValue;
            salesPrice += salesPriceValue;
        }

        public long quantity()
        {
            return quantity;
        }

        public long wholesaleCost()
        {
            return wholesaleCost;
        }

        public long salesPrice()
        {
            return salesPrice;
        }
    }

    private static final class Query64CatalogReturnsAggregate
    {
        private long count;
        private long refund;

        public void add(long refundedCash, long reversedCharge, long storeCredit)
        {
            count++;
            refund += refundedCash + reversedCharge + storeCredit;
        }

        public long count()
        {
            return count;
        }

        public long refund()
        {
            return refund;
        }
    }

    private static final class Query64CatalogSalesAggregate
    {
        private long count;
        private long sale;

        public void add(long extListPrice)
        {
            count++;
            sale += extListPrice;
        }

        public long count()
        {
            return count;
        }

        public long sale()
        {
            return sale;
        }
    }

    private static final class Query64CatalogItemAggregate
    {
        private long sale;
        private long refund;

        public void add(long salesCount, long salesSum, long returnCount, long returnSum)
        {
            sale += salesSum * returnCount;
            refund += returnSum * salesCount;
        }

        public long sale()
        {
            return sale;
        }

        public long refund()
        {
            return refund;
        }
    }

    private static final class Query64CrossSalesAggregate
    {
        private long count;
        private long sumWholesaleCost;
        private long sumListPrice;
        private long sumCouponAmount;

        public void add(long multiplicity, long wholesaleCost, long listPrice, long couponAmount)
        {
            count += multiplicity;
            sumWholesaleCost += wholesaleCost * multiplicity;
            sumListPrice += listPrice * multiplicity;
            sumCouponAmount += couponAmount * multiplicity;
        }

        public long count()
        {
            return count;
        }

        public long sumWholesaleCost()
        {
            return sumWholesaleCost;
        }

        public long sumListPrice()
        {
            return sumListPrice;
        }

        public long sumCouponAmount()
        {
            return sumCouponAmount;
        }
    }

    private static final class Statistics
    {
        private long count;
        private double mean;
        private double sumSquares;

        public void add(long value)
        {
            count++;
            double delta = value - mean;
            mean += delta / count;
            double delta2 = value - mean;
            sumSquares += delta * delta2;
        }

        public Long count()
        {
            return count;
        }

        public Double average()
        {
            return count == 0 ? null : mean;
        }

        public Double standardDeviationSample()
        {
            if (count < 2) {
                return null;
            }
            return Math.sqrt(sumSquares / (count - 1));
        }

        public Double coefficientOfVariation()
        {
            Double standardDeviation = standardDeviationSample();
            if (standardDeviation == null) {
                return null;
            }
            return standardDeviation / mean;
        }
    }
}
