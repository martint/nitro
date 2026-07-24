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

    @Test
    void testPublishedBenchmarkScansCarryLogicalSchemas()
    {
        List<Path> scanHarnesses = List.of(
                Path.of("src/test/java/org/weakref/nitro/tpch/TpchParquetSupport.java"),
                Path.of("src/test/java/org/weakref/nitro/tpcds/TpcdsParquetSupport.java"),
                Path.of("src/test/java/org/weakref/nitro/clickbench/ClickBenchHitsSupport.java"));

        assertThat(scanHarnesses)
                .noneMatch(path -> read(path).contains("Schema.unspecified"));
    }

    @Test
    void testPrimitivePoolsAreExplicitlyOwned()
            throws IOException
    {
        Pattern ambientPool = Pattern.compile(
                "PrimitiveArrayPool\\.(?:shared|sharedNativeBuffers)\\(|" +
                        "static\\s+(?:final\\s+)?PrimitiveArrayPool\\s+\\w+\\s*(?:=|;)|" +
                        "static\\s+final\\s+AtomicLong\\s+NEXT_SCOPE_ID");
        List<Path> violations;
        try (var files = Files.walk(MAIN_SOURCES.resolve("org/weakref/nitro"))) {
            violations = files.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> matches(path, ambientPool))
                    .toList();
        }

        assertThat(violations)
                .as("stateful pools must be owned by EngineResources and passed through constructors")
                .isEmpty();
    }

    @Test
    void testCompilerRegistriesAndCachesAreExplicitlyOwned()
    {
        Pattern ambientCompilerResource = Pattern.compile(
                "static\\s+final\\s+[^;\\n]*(?:REGISTRY|DOUBLE_RESULTS|CLASS_CACHE|\\bCOUNTER\\b)\\s*(?:=|;)");
        List<Path> compilerSources = List.of(
                MAIN_SOURCES.resolve("org/weakref/nitro/jit/Types.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/jit/ScalarLibrary.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/jit/AggregateLibrary.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/jit/CompilerResources.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/jit/PipelineCompiler.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/jit/BatchFunctionCompiler.java"));

        assertThat(compilerSources)
                .as("function/type registries and generated-class caches are integration-owned compiler dependencies")
                .noneMatch(path -> matches(path, ambientCompilerResource));
    }

    @Test
    void testFunctionImplementationsDoNotOwnStaticCaches()
    {
        Pattern ambientFunctionCache = Pattern.compile(
                "static\\s+final\\s+[^;\\n]*(?:ConcurrentHashMap|\\bCACHE\\b|\\bPATTERNS\\b)\\s*(?:=|;)");

        assertThat(matches(
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/LikeUtf8.java"),
                ambientFunctionCache))
                .as("function implementation caches belong to the dynamically constructed function instance or registry")
                .isFalse();
    }

    @Test
    void testOutputDiagnosticsAreNotProcessGlobal()
    {
        assertThat(MAIN_SOURCES.resolve("org/weakref/nitro/operator/OutputDebug.java"))
                .as("runtime diagnostics must be explicitly constructed and scoped to an execution")
                .doesNotExist();
        assertThat(read(MAIN_SOURCES.resolve("org/weakref/nitro/operator/Output.java")))
                .doesNotContain("OutputDebug");
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
