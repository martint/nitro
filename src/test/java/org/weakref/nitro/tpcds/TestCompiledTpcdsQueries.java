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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.jit.Column;
import org.weakref.nitro.jit.ColumnEncoding;
import org.weakref.nitro.jit.Plan;
import org.weakref.nitro.jit.QueryLowering;
import org.weakref.nitro.operator.CompiledOperator;
import org.weakref.nitro.operator.Operator;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Ports individual TPC-DS queries to the data-centric compiler and validates each, apples-to-apples, against the
 * Nitro operator-chain harness ({@link TpcdsParquetSupport}) on real sf10 Parquet: the query is lowered by column
 * name, compiled, bridged back into an {@link Operator}, and asserted row-for-row (order included, for top-N
 * queries) identical to the harness chain. Skipped when the dataset is not configured.
 */
public class TestCompiledTpcdsQueries
{
    @Test
    void query42()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        // SELECT d_year, i_category_id, i_category, sum(ss_ext_sales_price)
        // FROM store_sales, date_dim, item
        // WHERE ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk
        //   AND i_manager_id = 1 AND d_moy = 11 AND d_year = 2000
        // GROUP BY d_year, i_category_id, i_category
        // ORDER BY sum(ss_ext_sales_price) DESC, d_year, i_category_id, i_category
        // LIMIT 100
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk"),
                        new QueryLowering.Column("ss_item_sk"),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manager_id"),
                        new QueryLowering.Column("i_category_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_category", ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(11)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2000)),
                        new Plan.Predicate("=", query.column("i_manager_id"), new Plan.Lit(1)))
                .groupBy("d_year", "i_category_id", "i_category")
                .aggregate("sum", "ss_ext_sales_price")
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(3, true),    // sum(ss_ext_sales_price) DESC
                        new Plan.SortKey(0, false),   // d_year
                        new Plan.SortKey(1, false),   // i_category_id
                        new Plan.SortKey(2, false)),  // i_category
                        100));

        // Result column 2 is the i_category STRING key, reconstructed from the item input's dictionary.
        assertCompiledMatchesHarness(tables, query, 2,
                TpcdsParquetSupport.query42(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query52()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        // d_year, i_brand_id, i_brand, sum(ss_ext_sales_price) for d_moy=11, d_year=2000, i_manager_id=1;
        // top 100 by d_year, sum desc, i_brand_id.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk"),
                        new QueryLowering.Column("ss_item_sk"),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manager_id"),
                        new QueryLowering.Column("i_brand_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(11)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(2000)),
                        new Plan.Predicate("=", query.column("i_manager_id"), new Plan.Lit(1)))
                .groupBy("d_year", "i_brand_id", "i_brand")
                .aggregate("sum", "ss_ext_sales_price")
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(0, false),   // d_year
                        new Plan.SortKey(3, true),    // sum desc
                        new Plan.SortKey(1, false)),  // i_brand_id
                        100));

        assertCompiledMatchesHarness(tables, query, 2,
                TpcdsParquetSupport.query52(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query55()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        // i_brand_id, i_brand, sum(ss_ext_sales_price) for d_moy=11, d_year=1999, i_manager_id=28;
        // top 100 by sum desc, i_brand_id.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk"),
                        new QueryLowering.Column("ss_item_sk"),
                        new QueryLowering.Column("ss_ext_sales_price", ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_moy"),
                        new QueryLowering.Column("d_year"))
                .join("item", "ss_item_sk", "i_item_sk",
                        new QueryLowering.Column("i_item_sk"),
                        new QueryLowering.Column("i_manager_id"),
                        new QueryLowering.Column("i_brand_id", ColumnEncoding.FLAT, true),
                        new QueryLowering.Column("i_brand", ColumnEncoding.STRING, true));
        query.where(
                        new Plan.Predicate("=", query.column("d_moy"), new Plan.Lit(11)),
                        new Plan.Predicate("=", query.column("d_year"), new Plan.Lit(1999)),
                        new Plan.Predicate("=", query.column("i_manager_id"), new Plan.Lit(28)))
                .groupBy("i_brand_id", "i_brand")
                .aggregate("sum", "ss_ext_sales_price")
                .orderBy(new Plan.Ordering(List.of(
                        new Plan.SortKey(2, true),    // sum desc
                        new Plan.SortKey(0, false)),  // i_brand_id
                        100));

        assertCompiledMatchesHarness(tables, query, 1,
                TpcdsParquetSupport.query55(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    /**
     * Run {@code query} lowered + compiled + bridged, and assert its rows equal the harness operator chain's,
     * in order. {@code stringResultColumn} is the index of a dictionary-string result column to reconstruct (-1
     * if none); its dictionary is taken from the last input (the item build side, column 3).
     */
    private static void assertCompiledMatchesHarness(TpcdsParquetTables tables, QueryLowering query, int stringResultColumn, Operator harness)
    {
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runLowered(new Allocator(), tables, query.lower());
        byte[][][] dictionaries = new byte[run.result().columns().length][][];
        if (stringResultColumn >= 0) {
            dictionaries[stringResultColumn] = ((Column.StringColumn) run.inputs()[run.inputs().length - 1][3]).dictionary();
        }
        Operator compiled = new CompiledOperator(run.result(), dictionaries);

        List<Row> expected = normalize(OperatorAssertions.OperatorAssert.toRows(harness));
        List<Row> actual = normalize(OperatorAssertions.OperatorAssert.toRows(compiled));
        assertThat(expected).isNotEmpty();
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    /** Coerce values to a representation-independent form: any number to {@code long}, raw bytes to a UTF-8 string. */
    private static List<Row> normalize(List<Row> rows)
    {
        List<Row> normalized = new ArrayList<>(rows.size());
        for (Row row : rows) {
            Object[] values = row.values().clone();
            for (int i = 0; i < values.length; i++) {
                if (values[i] instanceof Number number) {
                    values[i] = number.longValue();
                }
                else if (values[i] instanceof byte[] bytes) {
                    values[i] = new String(bytes, UTF_8);
                }
            }
            normalized.add(new Row(values));
        }
        return normalized;
    }
}
