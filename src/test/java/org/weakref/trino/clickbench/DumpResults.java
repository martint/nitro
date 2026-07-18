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
package org.weakref.trino.clickbench;

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.trino.TrinoClickBenchSupport;

import java.io.BufferedWriter;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/** Canonical real-data dump for exact Trino-operator/Nitro/Velox multiset parity checks. */
public class DumpResults
{
    @Test
    void dumpSelected()
            throws Exception
    {
        String only = System.getProperty("nitro.dump.only");
        Set<Integer> selectedQueries = only == null ? Set.of() : Arrays.stream(only.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toUnmodifiableSet());
        Path outputDirectory = Path.of("/tmp/cb_trino_dump");
        Files.createDirectories(outputDirectory);

        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            Path input = support.requiredActualHitsPath();
            for (int query = 1; query <= 43; query++) {
                if (!selectedQueries.isEmpty() && !selectedQueries.contains(query)) {
                    continue;
                }
                Method method = TrinoClickBenchSupport.class.getMethod(String.format("query%02d", query), Path.class);
                MaterializedResult result = (MaterializedResult) method.invoke(support, input);
                Path output = outputDirectory.resolve(String.format("q%02d.txt", query));
                try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
                    for (var row : result.getMaterializedRows()) {
                        for (int field = 0; field < row.getFieldCount(); field++) {
                            if (field > 0) {
                                writer.write('\u0001');
                            }
                            writer.write(render(row.getField(field)));
                        }
                        writer.newLine();
                    }
                }
            }
        }
    }

    private static String render(Object value)
    {
        return switch (value) {
            case null -> "NULL";
            case byte[] bytes -> new String(bytes, StandardCharsets.UTF_8).stripTrailing();
            case String string -> string.stripTrailing();
            default -> value.toString();
        };
    }
}
