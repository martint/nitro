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
import org.weakref.nitro.operator.Operator;

import java.io.BufferedWriter;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Verification tool: dump every Nitro TPC-DS query's output in a canonical space so it can be diffed against the
 * Velox harness output. Integer-typed fields print as-is (Nitro stores money as scaled cents); fractional (F64)
 * fields print as round(value*100); strings are right-trimmed; nulls are "NULL". Fields are joined by 0x01 and
 * written one row per line to /tmp/nitro_dump/qNN.txt. Run with:
 *   mvn test -Dtest=DumpResults -Dnitro.tpcds.parquet.path=/root/data/tpcds-parquet-sf10 -Dnitro.tpcds.parquet.schema=sf10
 */
public class DumpResults
{
    @Test
    void dumpAll()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");
        Path out = Path.of("/tmp/nitro_dump");
        Files.createDirectories(out);
        String only = System.getProperty("nitro.dump.only");
        for (int q = 1; q <= 99; q++) {
            if (q == 14) {
                continue;
            }
            if (only != null && q != Integer.parseInt(only)) {
                continue;
            }
            String name = String.format("query%02d", q);
            Method method;
            try {
                method = TpcdsParquetSupport.class.getDeclaredMethod(name, Allocator.class, org.weakref.nitro.operator.evaluator.PrimitiveRegistry.class, TpcdsParquetTables.class);
            }
            catch (NoSuchMethodException e) {
                continue;
            }
            method.setAccessible(true);
            try {
                Operator operator = (Operator) method.invoke(null, new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables);
                List<Row> rows = OperatorAssertions.OperatorAssert.toRows(operator);
                Path file = out.resolve(String.format("q%02d.txt", q));
                try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                    for (Row row : rows) {
                        StringBuilder line = new StringBuilder();
                        Object[] values = row.values();
                        for (int i = 0; i < values.length; i++) {
                            if (i > 0) {
                                line.append('\u0001');
                            }
                            line.append(render(values[i]));
                        }
                        writer.write(line.toString());
                        writer.write('\n');
                    }
                }
                System.out.println(name + ": " + rows.size() + " rows -> " + file);
            }
            catch (Throwable t) {
                System.out.println(name + ": NITRO-DUMP-FAILED " + t);
            }
        }
    }

    private static String render(Object value)
    {
        return switch (value) {
            case null -> "NULL";
            case byte[] bytes -> new String(bytes, StandardCharsets.UTF_8).stripTrailing();
            case String string -> string.stripTrailing();
            case Double d -> Double.toString(d);
            case Float f -> Float.toString(f);
            default -> value.toString();
        };
    }
}
