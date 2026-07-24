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
package org.weakref.nitro.architecture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class TestArchitectureDependencies
{
    private static final Path MAIN_SOURCES = Path.of("src/main/java");
    private static final Pattern FORBIDDEN_CORE_REFERENCE = Pattern.compile(
            "(?:io\\.trino|org\\.apache\\.parquet|dev\\.hardwood|org\\.weakref\\.nitro\\.(?:operator|parquet))\\.");
    private static final Pattern FUNCTION_VOCABULARY = Pattern.compile("(?:case \"[^\"]+\"|\\.equals\\(\"[^\"]+\"\\))");

    @Test
    void testCorePortsRemainHostAndEngineNeutral()
            throws IOException
    {
        List<Path> violations;
        try (var files = Files.walk(MAIN_SOURCES.resolve("org/weakref/nitro/core"))) {
            violations = files.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> matches(path, FORBIDDEN_CORE_REFERENCE))
                    .toList();
        }

        assertThat(violations).isEmpty();
    }

    @Test
    void testFormatImportsInOperatorPackageDoNotExpand()
            throws IOException
    {
        Pattern formatImport = Pattern.compile(
                "^import (?:io\\.trino|org\\.apache\\.parquet|dev\\.hardwood)\\.",
                Pattern.MULTILINE);
        Set<String> existingViolations = Set.of(
                "HardwoodParquetScanOperator.java",
                "NitroParquetScanOperator.java",
                "ParquetScanOperator.java",
                "SkipDecodeScanOperator.java",
                "TrinoParquetScanOperator.java");

        Set<String> actual;
        try (var files = Files.walk(MAIN_SOURCES.resolve("org/weakref/nitro/operator"))) {
            actual = files.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> matches(path, formatImport))
                    .map(path -> path.getFileName().toString())
                    .collect(java.util.stream.Collectors.toSet());
        }

        assertThat(actual).isEqualTo(existingViolations);
    }

    @Test
    void testFunctionVocabularyInEngineDoesNotExpand()
            throws IOException
    {
        List<Path> roots = List.of(
                MAIN_SOURCES.resolve("org/weakref/nitro/operator"),
                MAIN_SOURCES.resolve("org/weakref/nitro/jit"));
        long occurrences = 0;
        for (Path root : roots) {
            try (var files = Files.walk(root)) {
                occurrences += files.filter(path -> path.toString().endsWith(".java"))
                        .mapToLong(path -> matchCount(path, FUNCTION_VOCABULARY))
                        .sum();
            }
        }

        assertThat(occurrences)
                .as("legacy function-name branches are a ratchet, not an extension point")
                .isLessThanOrEqualTo(104);
    }

    private static boolean matches(Path path, Pattern pattern)
    {
        return pattern.matcher(read(path)).find();
    }

    private static long matchCount(Path path, Pattern pattern)
    {
        return pattern.matcher(read(path)).results().count();
    }

    private static String read(Path path)
    {
        try {
            return Files.readString(path);
        }
        catch (IOException exception) {
            throw new IllegalStateException("Unable to read " + path, exception);
        }
    }
}
