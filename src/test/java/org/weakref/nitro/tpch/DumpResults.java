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
package org.weakref.nitro.tpch;

import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.io.BufferedWriter;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Verification tool: dump every Nitro TPC-H query's output one row per line so the Trino-reader and Nitro-reader
 * paths can be diffed. Output dir is {@code -Dnitro.dump.dir} (default {@code /tmp/nitro_dump_tpch}); a single query
 * can be selected with {@code -Dnitro.dump.only=16}.
 */
public final class DumpResults
{
    private DumpResults() {}

    public static void main(String[] args)
            throws Exception
    {
        TpchParquetTables tables = TpchParquetTables.requiredActual();
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        Path out = Path.of(System.getProperty("nitro.dump.dir", "/tmp/nitro_dump_tpch"));
        Files.createDirectories(out);
        String only = System.getProperty("nitro.dump.only");
        for (int q = 1; q <= 22; q++) {
            if (only != null && q != Integer.parseInt(only)) {
                continue;
            }
            String name = String.format("query%02d", q);
            Method method = TpchParquetSupport.class.getDeclaredMethod(
                    name, Allocator.class, PrimitiveRegistry.class, TpchParquetTables.class);
            method.setAccessible(true);
            Operator operator = (Operator) method.invoke(null, new Allocator(), registry, tables);
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(operator);
            Path file = out.resolve(String.format("q%02d.txt", q));
            try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                for (Row row : rows) {
                    StringBuilder line = new StringBuilder();
                    Object[] values = row.values();
                    for (int i = 0; i < values.length; i++) {
                        if (i > 0) {
                            line.append('');
                        }
                        line.append(render(values[i]));
                    }
                    writer.write(line.toString());
                    writer.write('\n');
                }
            }
            System.out.println(name + ": " + rows.size() + " rows -> " + file);
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
