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
package org.weakref.nitro.clickbench;

import org.junit.jupiter.api.Test;
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
 * Verification tool: dump each Nitro ClickBench query (query01..43 = standard ClickBench Q0..Q42) over the real
 * hits parquet directory, in a canonical space for diffing against DuckDB SQL. Run with:
 *   mvn test -Dtest=org.weakref.nitro.clickbench.DumpResults -Dnitro.clickbench.hits.path=/tmp/cb_subset
 */
public class DumpResults
{
    @Test
    void dumpAll()
            throws Exception
    {
        Path dir = Path.of(System.getProperty("nitro.clickbench.hits.path", "/tmp/cb_subset"));
        Path out = Path.of("/tmp/cb_nitro_dump");
        Files.createDirectories(out);
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        for (int q = 1; q <= 43; q++) {
            String name = String.format("query%02d", q);
            Operator operator = null;
            try {
                Method m;
                try {
                    m = ClickBenchHitsSupport.class.getDeclaredMethod(name, Allocator.class, PrimitiveRegistry.class, Path.class);
                    m.setAccessible(true);
                    operator = (Operator) m.invoke(null, new Allocator(), registry, dir);
                }
                catch (NoSuchMethodException e) {
                    m = ClickBenchHitsSupport.class.getDeclaredMethod(name, Allocator.class, Path.class);
                    m.setAccessible(true);
                    operator = (Operator) m.invoke(null, new Allocator(), dir);
                }
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
                System.out.println(name + ": " + rows.size() + " rows");
            }
            catch (Throwable t) {
                System.out.println(name + ": CB-DUMP-FAILED " + t);
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
