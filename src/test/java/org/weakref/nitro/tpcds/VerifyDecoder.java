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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.NitroParquetScanOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.TrinoParquetScanOperator;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.nio.file.Path;
import java.util.List;

/**
 * Computes order-sensitive per-column checksums by independently draining {@link NitroParquetScanOperator}
 * and {@link TrinoParquetScanOperator} (their batch boundaries differ — Trino ramps batch size), then asserts
 * the checksums match. Covers all decoded types (int/long/decimal/date/string) and null flags.
 */
public final class VerifyDecoder
{
    private VerifyDecoder() {}

    public static void main(String[] args)
    {
        String table = args.length > 0 ? args[0] : "item";
        List<String> columns = args.length > 1 ? List.of(args[1].split(",")) : defaultColumns(table);

        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");
        List<Path> files = tables.tableFiles(table);

        long[][] nitro = digest(new NitroParquetScanOperator(new Allocator(EngineResources.createDefault()), files, columns), columns.size());
        long[][] trino = digest(new TrinoParquetScanOperator(new Allocator(EngineResources.createDefault()), files, columns), columns.size());

        if (nitro[0][0] != trino[0][0]) {
            throw new AssertionError("row count: nitro=" + nitro[0][0] + " trino=" + trino[0][0]);
        }
        boolean ok = true;
        for (int c = 0; c < columns.size(); c++) {
            if (nitro[1][c] != trino[1][c]) {
                System.out.println("  MISMATCH " + columns.get(c) + ": nitro=" + nitro[1][c] + " trino=" + trino[1][c]);
                ok = false;
            }
        }
        if (!ok) {
            throw new AssertionError(table + " column checksum mismatch");
        }
        System.out.println("OK " + table + " rows=" + nitro[0][0] + " columns=" + columns + " — checksums match Trino");
    }

    /** Returns {@code [{rowCount}, perColumnChecksum]}. */
    private static long[][] digest(Operator operator, int columnCount)
    {
        long[] checksum = new long[columnCount];
        long rows = 0;
        try (operator) {
            while (operator.hasNext()) {
                Batch batch = operator.next();
                Mask mask = batch.borrowMask();
                int count = mask.count();
                for (int c = 0; c < columnCount; c++) {
                    Output output = batch.output(c);
                    Vector values = output.borrow(Stream.VALUES);
                    Vector nullsVector = output.borrowOrNull(Stream.NULLS);
                    boolean[] nulls = nullsVector instanceof BooleanVector b ? b.values() : null;
                    long h = checksum[c];
                    for (int i = 0; i < count; i++) {
                        h *= 1000003L;
                        h += (nulls != null && nulls[i]) ? 0x9E3779B97F4A7C15L : elementHash(values, i);
                    }
                    checksum[c] = h;
                }
                rows += count;
                batch.close();
            }
        }
        return new long[][] {{rows}, checksum};
    }

    private static long elementHash(Vector values, int position)
    {
        return switch (values) {
            case I32Vector v -> v.values()[position];
            case I64Vector v -> v.values()[position];
            case BinaryVector v -> binaryHash(v, position);
            case org.weakref.nitro.data.DictionaryVector d -> elementHash(d.values(), d.ids()[position]);
            default -> throw new AssertionError("unexpected vector " + values.getClass());
        };
    }

    private static long binaryHash(BinaryVector vector, int position)
    {
        byte[] data = vector.data();
        long f = 1125899906842597L;
        for (int p = vector.startOffset(position); p < vector.endOffset(position); p++) {
            f = f * 31 + data[p];
        }
        return f;
    }

    private static List<String> defaultColumns(String table)
    {
        return switch (table) {
            case "item" -> List.of("i_item_sk", "i_item_id", "i_rec_start_date", "i_item_desc", "i_current_price", "i_manager_id");
            case "date_dim" -> List.of("d_date_sk", "d_date_id", "d_date", "d_month_seq", "d_day_name", "d_holiday");
            case "store_sales" -> List.of("ss_sold_date_sk", "ss_item_sk", "ss_quantity", "ss_sales_price", "ss_net_profit", "ss_ext_tax");
            case "customer" -> List.of("c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_birth_year", "c_birth_country");
            default -> throw new IllegalArgumentException("no default columns for " + table);
        };
    }
}
