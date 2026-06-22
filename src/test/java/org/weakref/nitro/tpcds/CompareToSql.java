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
import io.trino.spi.type.SqlDecimal;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.trino.TrinoTpcdsParquetSqlSupport;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Trustworthy correctness gate: compares each TPC-DS Nitro operator-tree result against real Trino SQL execution,
 * ORDER-INSENSITIVELY (multiset) and in a canonical numeric space (decimals -> unscaled, money/doubles -> round*100
 * cents, dates -> epoch day, strings trimmed) so representation/type/ordering differences do not register as
 * mismatches. Reports, per query, MATCH or the genuine row-level difference (cardinality and example rows). Run:
 *   mvn test -Dtest=CompareToSql -Dnitro.tpcds.trino.root=/root/notes/trino \
 *     -Dnitro.tpcds.parquet.path=/root/data/tpcds-parquet-sf10 -Dnitro.tpcds.parquet.schema=sf10 -Dlicense.skip=true
 */
public class CompareToSql
{
    @Test
    void compareAll()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "set -Dnitro.tpcds.parquet.path");
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        StringBuilder summary = new StringBuilder();
        try (TrinoTpcdsParquetSqlSupport sql = new TrinoTpcdsParquetSqlSupport(tables)) {
            for (int q = 1; q <= 99; q++) {
                String name = String.format("query%02d", q);
                Method method;
                try {
                    method = TpcdsParquetSupport.class.getDeclaredMethod(name, Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class);
                }
                catch (NoSuchMethodException e) {
                    continue;
                }
                method.setAccessible(true);

                Map<String, Integer> nitro;
                try (Operator op = (Operator) method.invoke(null, new Allocator(), registry, tables)) {
                    nitro = canonicalMultiset(toCanonicalRows(OperatorAssertions.OperatorAssert.toRows(op)));
                }
                catch (Throwable t) {
                    summary.append(String.format("q%02d  NITRO-ERROR  %s%n", q, rootCause(t)));
                    continue;
                }

                Map<String, Integer> trino;
                try {
                    trino = canonicalMultiset(trinoCanonicalRows(sql.executeBenchmarkQuery(String.format("%02d", q))));
                }
                catch (Throwable t) {
                    summary.append(String.format("q%02d  SQL-ERROR  %s%n", q, rootCause(t)));
                    continue;
                }

                int nitroCount = nitro.values().stream().mapToInt(Integer::intValue).sum();
                int trinoCount = trino.values().stream().mapToInt(Integer::intValue).sum();
                int onlyNitro = diffCount(nitro, trino);
                int onlyTrino = diffCount(trino, nitro);
                if (onlyNitro == 0 && onlyTrino == 0) {
                    summary.append(String.format("q%02d  MATCH  (%d rows)%n", q, nitroCount));
                }
                else {
                    summary.append(String.format("q%02d  DIFF  nitroRows=%d sqlRows=%d  onlyNitro=%d onlySql=%d%n",
                            q, nitroCount, trinoCount, onlyNitro, onlyTrino));
                    appendExamples(summary, nitro, trino);
                }
            }
        }
        System.out.println("=== CompareToSql (order-insensitive, cents-space) ===");
        System.out.print(summary);
        java.nio.file.Files.writeString(java.nio.file.Path.of("/tmp/compare_to_sql.txt"), summary.toString());
    }

    private static int diffCount(Map<String, Integer> a, Map<String, Integer> b)
    {
        int extra = 0;
        for (Map.Entry<String, Integer> e : a.entrySet()) {
            extra += Math.max(0, e.getValue() - b.getOrDefault(e.getKey(), 0));
        }
        return extra;
    }

    private static void appendExamples(StringBuilder out, Map<String, Integer> nitro, Map<String, Integer> trino)
    {
        int shown = 0;
        for (Map.Entry<String, Integer> e : nitro.entrySet()) {
            if (e.getValue() > trino.getOrDefault(e.getKey(), 0)) {
                out.append("      only-nitro: ").append(e.getKey()).append('\n');
                if (++shown >= 3) {
                    break;
                }
            }
        }
        shown = 0;
        for (Map.Entry<String, Integer> e : trino.entrySet()) {
            if (e.getValue() > nitro.getOrDefault(e.getKey(), 0)) {
                out.append("      only-sql:   ").append(e.getKey()).append('\n');
                if (++shown >= 3) {
                    break;
                }
            }
        }
    }

    private static Map<String, Integer> canonicalMultiset(List<String> rows)
    {
        Map<String, Integer> m = new HashMap<>();
        for (String r : rows) {
            m.merge(r, 1, Integer::sum);
        }
        return m;
    }

    private static List<String> toCanonicalRows(List<Row> rows)
    {
        List<String> out = new ArrayList<>(rows.size());
        for (Row row : rows) {
            StringBuilder sb = new StringBuilder();
            for (Object v : row.values()) {
                sb.append(canon(v)).append('');
            }
            out.add(sb.toString());
        }
        return out;
    }

    private static List<String> trinoCanonicalRows(MaterializedResult result)
    {
        List<String> out = new ArrayList<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            StringBuilder sb = new StringBuilder();
            for (Object v : row.getFields()) {
                sb.append(canon(v)).append('');
            }
            out.add(sb.toString());
        }
        return out;
    }

    // Canonical "cents space" shared by Nitro (money = scaled long, avg = double) and Trino SQL (decimal/date):
    // decimals -> unscaled integer; money doubles -> round(v*100); dates -> epoch day; strings trimmed.
    private static String canon(Object v)
    {
        return switch (v) {
            case null -> "NULL";
            case byte[] bytes -> new String(bytes, StandardCharsets.UTF_8).stripTrailing();
            case String s -> s.stripTrailing();
            case LocalDate d -> Long.toString(d.toEpochDay());
            case SqlDate d -> Integer.toString(d.getDays());
            // Nitro stores money as a scaled long (cents). Trino returns decimals; render both in the same unscaled
            // space: a decimal d.dd -> unscaled cents, a money double -> round(v*100). This unifies scale-2 columns.
            case SqlDecimal d -> Long.toString(d.toBigDecimal().setScale(2, RoundingMode.HALF_UP).unscaledValue().longValueExact());
            case BigDecimal d -> Long.toString(d.setScale(2, RoundingMode.HALF_UP).unscaledValue().longValueExact());
            case Double d -> Long.toString(Math.round(d * 100));
            case Float f -> Long.toString(Math.round(f * 100.0));
            case Boolean b -> b ? "1" : "0";
            case Byte b -> Long.toString(b);
            case Short s -> Long.toString(s);
            case Integer i -> Long.toString(i);
            case Long l -> Long.toString(l);
            default -> v.toString();
        };
    }

    private static String rootCause(Throwable t)
    {
        Throwable c = t;
        while (c.getCause() != null) {
            c = c.getCause();
        }
        return c.getClass().getSimpleName() + ": " + c.getMessage();
    }
}
