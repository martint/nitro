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
    void testMigratedHarnessesCarryFilterStructureExplicitly()
    {
        String tpch = read(Path.of("src/test/java/org/weakref/nitro/tpch/TpchParquetSupport.java"));
        String tpcds = read(Path.of("src/test/java/org/weakref/nitro/tpcds/TpcdsParquetSupport.java"));
        String clickBench = read(Path.of("src/test/java/org/weakref/nitro/clickbench/ClickBenchHitsSupport.java"));

        assertThat(tpch)
                .doesNotContain("LegacyLogicalMaskAdapter")
                .doesNotContain("combineBoolean(")
                .doesNotContain("remapped.getLast().output()");
        assertThat(tpch)
                .as("materialized boolean values may use registry calls, but filter control flow must be explicit")
                .contains("new AndMask(")
                .contains("new OrMask(")
                .contains("new NotMask(")
                .contains("branch.materializedValue()");
        assertThat(clickBench)
                .doesNotContain("LegacyLogicalMaskAdapter")
                .doesNotContain("combineBoolean(");
        assertThat(tpcds)
                .doesNotContain("LegacyLogicalMaskAdapter")
                .doesNotContain("combineBoolean(")
                .doesNotContain("referenceFor(");
        assertThat(Path.of("src/test/java/org/weakref/nitro/LegacyLogicalMaskAdapter.java"))
                .as("all operator harnesses must carry logical filter structure explicitly")
                .doesNotExist();
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
    void testProjectEvaluatorCompatibilityDomainIsExplicitlyOwned()
            throws IOException
    {
        String projectOperator = Files.readString(
                MAIN_SOURCES.resolve("org/weakref/nitro/operator/ProjectOperator.java"));

        assertThat(projectOperator)
                .doesNotContain("static final Object EVALUATOR_BUFFER_POOL")
                .contains("allocator.engineResources().projectOperator().evaluatorBufferPoolGroup()");
    }

    @Test
    void testAggregationCompatibilityDomainIsExplicitlyOwned()
            throws IOException
    {
        String aggregationOperator = Files.readString(
                MAIN_SOURCES.resolve("org/weakref/nitro/operator/AggregationOperator.java"));

        assertThat(aggregationOperator)
                .doesNotContain("static final Object ALLOCATION_POOL")
                .contains("allocator.engineResources().aggregationOperator().bufferPoolGroup()");
    }

    @Test
    void testHashJoinCompatibilityDomainIsExplicitlyOwned()
            throws IOException
    {
        String hashJoinOperator = Files.readString(
                MAIN_SOURCES.resolve("org/weakref/nitro/operator/HashJoinOperator.java"));

        assertThat(hashJoinOperator)
                .doesNotContain("static final Object SHARED_BUFFER_POOL_GROUP")
                .doesNotContain("SHARE_BUFFER_POOL_ACROSS_OPERATORS")
                .contains(".hashJoinOperator()")
                .contains(".bufferPoolCompatibilityGroup(allocationPoolGroup)");
    }

    @Test
    void testGroupingPoolFamilyIsExplicitlyOwned()
            throws IOException
    {
        String groupingState = Files.readString(
                MAIN_SOURCES.resolve("org/weakref/nitro/operator/GroupingState.java"));

        assertThat(groupingState)
                .doesNotContain("static final Object ZEROED_LONG_DIRECT_IDS_FAMILY")
                .doesNotContain("ZEROED_LONG_DIRECT_IDS_POOL")
                .contains("resources.zeroedLongDirectIdsFamily()")
                .contains("resources.poolZeroedLongDirectIds()");
    }

    @Test
    void testScalarFunctionAllocationContextsAreProviderOwned()
            throws IOException
    {
        Pattern staticAllocationContext = Pattern.compile(
                "static\\s+final\\s+Allocator\\.Context");
        Path builtins = MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin");
        List<Path> violations;
        try (var files = Files.walk(builtins)) {
            violations = files.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> matches(path, staticAllocationContext))
                    .toList();
        }

        assertThat(violations)
                .as("dynamically registered function providers must own their allocation contexts")
                .isEmpty();
    }

    @Test
    void testSyntheticTypedSourcesAreNotProductionOperators()
    {
        Path operators = MAIN_SOURCES.resolve("org/weakref/nitro/operator");

        assertThat(operators.resolve("ConstantTableOperator.java")).doesNotExist();
        assertThat(operators.resolve("GeneratorOperator.java")).doesNotExist();
        assertThat(operators.resolve("generator/ConstantGenerator.java")).doesNotExist();
        assertThat(operators.resolve("generator/I64Generator.java")).doesNotExist();
        assertThat(operators.resolve("generator/SequenceGenerator.java")).doesNotExist();
    }

    @Test
    void testTypeSwitchingConsoleSinkIsNotAProductionOperator()
    {
        assertThat(MAIN_SOURCES.resolve("org/weakref/nitro/operator/OutputOperator.java"))
                .doesNotExist();
    }

    @Test
    void testCompilerRegistriesAndCachesAreExplicitlyOwned()
    {
        Pattern ambientCompilerResource = Pattern.compile(
                "static\\s+final\\s+[^;\\n]*(?:REGISTRY|DOUBLE_RESULTS|CLASS_CACHE|\\bCOUNTER\\b)\\s*(?:=|;)");
        Path legacyPipeline = MAIN_SOURCES.resolve("org/weakref/nitro/legacy/pipeline");
        List<Path> compilerSources = List.of(
                legacyPipeline.resolve("Types.java"),
                legacyPipeline.resolve("ScalarLibrary.java"),
                legacyPipeline.resolve("AggregateLibrary.java"),
                legacyPipeline.resolve("CompilerResources.java"),
                legacyPipeline.resolve("PipelineCompiler.java"),
                legacyPipeline.resolve("BatchFunctionCompiler.java"));

        assertThat(compilerSources)
                .as("function/type registries and generated-class caches are integration-owned compiler dependencies")
                .noneMatch(path -> matches(path, ambientCompilerResource));
    }

    @Test
    void testLegacyPipelineIsIsolatedFromActiveEngine()
            throws IOException
    {
        Path nitro = MAIN_SOURCES.resolve("org/weakref/nitro");
        Path legacyPipeline = nitro.resolve("legacy/pipeline");
        Pattern legacyReference = Pattern.compile("org\\.weakref\\.nitro\\.legacy\\.pipeline");
        List<Path> violations;
        try (var files = Files.walk(nitro)) {
            violations = files.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !path.startsWith(legacyPipeline))
                    .filter(path -> matches(path, legacyReference))
                    .toList();
        }

        assertThat(violations)
                .as("active engine code must not depend on the legacy whole-pipeline compiler")
                .isEmpty();
        assertThat(nitro.resolve("operator/CompiledOperator.java"))
                .as("the legacy result bridge must not appear to be an active Nitro operator")
                .doesNotExist();
    }

    @Test
    void testActiveJitPackageContainsOnlyProjectionCompilerBackend()
            throws IOException
    {
        Path jit = MAIN_SOURCES.resolve("org/weakref/nitro/jit");
        Set<String> files;
        try (var paths = Files.list(jit)) {
            files = paths.filter(path -> path.toString().endsWith(".java"))
                    .map(path -> path.getFileName().toString())
                    .collect(java.util.stream.Collectors.toSet());
        }

        assertThat(files).containsExactlyInAnyOrder(
                "DoubleComparisonMaskSupport.java",
                "FusedMultiProjection.java",
                "FusedProjectionCompiler.java",
                "InMemoryCompiler.java",
                "LongComparisonMaskSupport.java",
                "ProjectionMaskCompiler.java",
                "ProjectionProgramBuilder.java",
                "Utf8DynamicMaskSupport.java",
                "Utf8DynamicMaskKernel.java",
                "Utf8DynamicMaskKernelGenerator.java",
                "Utf8LiteralMaskSupport.java",
                "Utf8LiteralMatcher.java",
                "Utf8LiteralMatcherGenerator.java");
    }

    @Test
    void testProjectionCompilerHasNoFunctionVocabulary()
    {
        List<String> compilerSources = List.of(
                read(MAIN_SOURCES.resolve("org/weakref/nitro/jit/FusedProjectionCompiler.java")),
                read(MAIN_SOURCES.resolve("org/weakref/nitro/jit/ProjectionMaskCompiler.java")),
                read(MAIN_SOURCES.resolve("org/weakref/nitro/jit/DoubleComparisonMaskSupport.java")),
                read(MAIN_SOURCES.resolve("org/weakref/nitro/jit/Utf8DynamicMaskSupport.java")),
                read(MAIN_SOURCES.resolve("org/weakref/nitro/jit/Utf8DynamicMaskKernelGenerator.java")),
                read(MAIN_SOURCES.resolve("org/weakref/nitro/jit/Utf8LiteralMaskSupport.java")),
                read(MAIN_SOURCES.resolve("org/weakref/nitro/jit/Utf8LiteralMatcherGenerator.java")));

        assertThat(compilerSources)
                .allMatch(source ->
                        !source.contains("function.scalar.builtin") &&
                                !source.contains("BuiltinProjectionPrograms") &&
                                !source.contains("validateInstructionShape") &&
                                !source.contains("call.name().equals") &&
                                !source.contains("switch (call.name())"));
        assertThat(MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/BuiltinProjectionPrograms.java"))
                .as("projection lowering belongs to the dynamically registered provider, not a central catalog")
                .doesNotExist();
    }

    @Test
    void testIrNormalizerHasNoFunctionVocabulary()
    {
        Path ir = MAIN_SOURCES.resolve("org/weakref/nitro/operator/evaluator/ir");

        assertThat(read(ir.resolve("ConditionalNormalizationRule.java")))
                .doesNotContain("Call")
                .doesNotContain("\"if\"");
        assertThat(read(ir.resolve("CoalesceNormalizationRule.java")))
                .doesNotContain("Call")
                .doesNotContain("\"coalesce\"");
        assertThat(read(ir.resolve("NormalizedIrValidator.java")))
                .doesNotContain("SPECIAL_FORMS")
                .doesNotContain("call.name()");
        assertThat(ir.resolve("IfNormalizationRule.java")).doesNotExist();
        assertThat(read(ir.resolve("MaskExpressionResolver.java")))
                .doesNotContain("case Call")
                .doesNotContain("name.equals")
                .doesNotContain("\"and\"")
                .doesNotContain("\"or\"")
                .doesNotContain("\"not\"");
    }

    @Test
    void testFilterOperatorDoesNotRecognizeEqualityFunctionName()
    {
        assertThat(read(MAIN_SOURCES.resolve("org/weakref/nitro/operator/FilterOperator.java")))
                .doesNotContain("\"eq\"");
    }

    @Test
    void testEvaluatorDoesNotRecognizeRangeFunctionIdentity()
    {
        String evaluator = read(MAIN_SOURCES.resolve("org/weakref/nitro/operator/evaluator/PlanEvaluator.java"));
        String filter = read(MAIN_SOURCES.resolve("org/weakref/nitro/operator/FilterOperator.java"));

        assertThat(evaluator)
                .doesNotContain("\"lt\"")
                .doesNotContain("LessThanI64")
                .doesNotContain("F64Comparison")
                .doesNotContain("DoubleComparison")
                .doesNotContain("ProjectionMaskCompiler.Operation")
                .doesNotContain("constantBound(")
                .doesNotContain("RangeFusion");
        assertThat(filter)
                .doesNotContain("\"lt\"")
                .doesNotContain("LessThanI64");
        List<Path> migratedScalars = List.of(
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/AddF64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/AddI64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/EqualF64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/EqualI64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/GreaterThanF64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/GreaterThanOrEqualF64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/LessThanF64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/LessThanI64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/LessThanOrEqualF64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/MultiplyF64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/MultiplyI64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/SubtractF64.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/function/scalar/builtin/SubtractI64.java"));
        assertThat(migratedScalars.stream().map(TestArchitectureDependencies::read))
                .as("optional lowering capabilities must not alter the hot scalar implementation shape")
                .allMatch(source ->
                        !source.contains("ProjectionCodeProvider") &&
                                !source.contains("RangeBoundProvider"));
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

    @Test
    void testPrimitiveCallSiteGenerationIsExplicitlyOwned()
    {
        assertThat(matches(
                MAIN_SOURCES.resolve("org/weakref/nitro/operator/evaluator/PrimitiveCallSiteBinder.java"),
                Pattern.compile("static\\s+(?:final\\s+)?AtomicInteger")))
                .as("generated call-site identity and lifetime belong to the constructed binder")
                .isFalse();
    }

    @Test
    void testOperatorKernelCachesAreExplicitlyOwned()
    {
        List<Path> generators = List.of(
                MAIN_SOURCES.resolve("org/weakref/nitro/operator/FusedGroupingAggregationKernelGenerator.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/operator/MultiLongGroupingTableGenerator.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/operator/AdaptiveLongGroupingTableGenerator.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/operator/DictionaryHashBatchKernelGenerator.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/operator/MixedComposite3GroupingKernelGenerator.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/operator/DictionaryRecordEqualityKernelGenerator.java"),
                MAIN_SOURCES.resolve("org/weakref/nitro/jit/FusedProjectionCompiler.java"));

        assertThat(generators.stream().map(TestArchitectureDependencies::read))
                .as("generated operator kernels belong to OperatorCodeGenerationResources")
                .allMatch(source ->
                        !source.contains("static final ConcurrentHashMap") &&
                                !source.contains("static final AtomicInteger"));
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
