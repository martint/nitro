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
package org.weakref.nitro.jit;

import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import static java.util.stream.Collectors.joining;

/**
 * Data-centric query compiler (prototype). Translates a {@link Plan.Pipeline} into a single fused Java
 * routine — one loop that scans, (inner hash) joins, filters, projects, and aggregates with no intermediate
 * materialization and no per-element dispatch — then compiles it in-process and loads it as a
 * {@link CompiledPipeline}.
 */
public final class PipelineCompiler
{
    private static final String PACKAGE = "org.weakref.nitro.jit.generated";
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private PipelineCompiler() {}

    public static CompiledPipeline compile(Plan.Pipeline pipeline)
    {
        String simpleName = "Pipeline_" + COUNTER.incrementAndGet();
        String source = render(pipeline, simpleName);
        try {
            Class<?> compiled = InMemoryCompiler.compile(PACKAGE + "." + simpleName, source);
            return (CompiledPipeline) compiled.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate compiled pipeline:\n" + source, e);
        }
    }

    /** Exposed for inspection/tests: the Java source that would be compiled. */
    public static String render(Plan.Pipeline pipeline)
    {
        return render(pipeline, "Pipeline_preview");
    }

    private static String render(Plan.Pipeline pipeline, String simpleName)
    {
        if (pipeline.groupKeys().size() > 1) {
            throw new UnsupportedOperationException("multi-key grouping not yet supported (same shape, composite hash)");
        }
        StringBuilder out = new StringBuilder();
        out.append("package ").append(PACKAGE).append(";\n");
        out.append("public final class ").append(simpleName)
                .append(" implements org.weakref.nitro.jit.CompiledPipeline {\n");
        boolean hashGroup = !pipeline.groupKeys().isEmpty() && pipeline.groupDomain() == null;
        boolean needsMix = hashGroup
                || (pipeline.build() != null && !pipeline.build().denseKeys());
        if (needsMix) {
            emitMix(out);
        }
        out.append("  @Override public org.weakref.nitro.jit.CompiledPipeline.Result execute(long[][][] inputs, int[] rowCounts) {\n");
        if (pipeline.build() != null) {
            emitJoinBody(out, pipeline);
        }
        else {
            emitScanBody(out, pipeline);
        }
        out.append("  }\n}\n");
        return out.toString();
    }

    // ---- single-input scan -> filter -> aggregate ----

    private static void emitScanBody(StringBuilder out, Plan.Pipeline pipeline)
    {
        out.append("    long[][] in = inputs[0]; int rowCount = rowCounts[0];\n");
        TreeSet<Integer> referenced = referencedColumns(pipeline);
        for (int column : referenced) {
            out.append("    long[] c").append(column).append(" = in[").append(column).append("];\n");
        }
        IntFunction<String> resolver = index -> "c" + index + "[i]";
        boolean grouped = !pipeline.groupKeys().isEmpty();
        if (grouped) {
            emitGroupedState(out, pipeline);
        }
        else {
            emitGlobalState(out, pipeline.aggregates().size());
        }
        out.append("    for (int i = 0; i < rowCount; i++) {\n");
        emitRowBody(out, "      ", pipeline, resolver, grouped);
        out.append("    }\n");
        if (grouped) {
            emitGroupedResult(out, pipeline);
        }
        else {
            emitGlobalResult(out, pipeline.aggregates().size());
        }
    }

    // ---- scan(probe) inner-join build -> filter -> aggregate ----

    private static void emitJoinBody(StringBuilder out, Plan.Pipeline pipeline)
    {
        Plan.Build build = pipeline.build();
        int probeColumns = pipeline.columnCount();

        TreeSet<Integer> combined = referencedColumns(pipeline);
        combined.add(pipeline.probeKeyColumn());
        TreeSet<Integer> probeReferenced = new TreeSet<>();
        TreeSet<Integer> buildReferenced = new TreeSet<>();
        buildReferenced.add(build.keyColumn());
        for (int column : combined) {
            if (column < probeColumns) {
                probeReferenced.add(column);
            }
            else {
                buildReferenced.add(column - probeColumns);
            }
        }

        out.append("    long[][] probe = inputs[0]; int probeRows = rowCounts[0];\n");
        out.append("    long[][] build = inputs[1]; int buildRows = rowCounts[1];\n");
        for (int column : probeReferenced) {
            out.append("    long[] p").append(column).append(" = probe[").append(column).append("];\n");
        }
        for (int column : buildReferenced) {
            out.append("    long[] b").append(column).append(" = build[").append(column).append("];\n");
        }
        String buildKey = "b" + build.keyColumn();
        boolean dense = build.denseKeys();
        if (dense) {
            // Array mode: index a build-row array directly by (key - min) -- no hashing, no probe loop.
            out.append("    long minKey = Long.MAX_VALUE, maxKey = Long.MIN_VALUE;\n");
            out.append("    for (int r = 0; r < buildRows; r++) { long key = ").append(buildKey)
                    .append("[r]; if (key < minKey) { minKey = key; } if (key > maxKey) { maxKey = key; } }\n");
            out.append("    int range = buildRows == 0 ? 1 : (int) (maxKey - minKey + 1);\n");
            out.append("    int[] buildRowByKey = new int[range]; java.util.Arrays.fill(buildRowByKey, -1);\n");
            out.append("    for (int r = 0; r < buildRows; r++) { buildRowByKey[(int) (").append(buildKey)
                    .append("[r] - minKey)] = r; }\n");
        }
        else {
            // Open-addressing key -> build-row hash table (build keys assumed unique).
            out.append("    int jcap = 16; while (jcap * 0.75f < buildRows) { jcap <<= 1; }\n");
            out.append("    long[] jKey = new long[jcap]; int[] jRow = new int[jcap];\n");
            out.append("    java.util.Arrays.fill(jRow, -1); int jMask = jcap - 1;\n");
            out.append("    for (int r = 0; r < buildRows; r++) {\n");
            out.append("      long key = ").append(buildKey).append("[r];\n");
            out.append("      int slot = mix(key) & jMask;\n");
            out.append("      while (jRow[slot] != -1 && jKey[slot] != key) { slot = (slot + 1) & jMask; }\n");
            out.append("      jKey[slot] = key; jRow[slot] = r;\n");
            out.append("    }\n");
        }

        IntFunction<String> resolver = index -> index < probeColumns
                ? "p" + index + "[i]"
                : "b" + (index - probeColumns) + "[buildRow]";
        boolean grouped = !pipeline.groupKeys().isEmpty();
        if (grouped) {
            emitGroupedState(out, pipeline);
        }
        else {
            emitGlobalState(out, pipeline.aggregates().size());
        }
        String probeKey = "p" + pipeline.probeKeyColumn();
        out.append("    for (int i = 0; i < probeRows; i++) {\n");
        out.append("      long jk = ").append(probeKey).append("[i];\n");
        if (dense) {
            out.append("      int buildRow = (jk >= minKey && jk <= maxKey) ? buildRowByKey[(int) (jk - minKey)] : -1;\n");
        }
        else {
            out.append("      int js = mix(jk) & jMask;\n");
            out.append("      while (jRow[js] != -1 && jKey[js] != jk) { js = (js + 1) & jMask; }\n");
            out.append("      int buildRow = jRow[js];\n");
        }
        out.append("      if (buildRow != -1) {\n");
        emitRowBody(out, "        ", pipeline, resolver, grouped);
        out.append("      }\n");
        out.append("    }\n");
        if (grouped) {
            emitGroupedResult(out, pipeline);
        }
        else {
            emitGlobalResult(out, pipeline.aggregates().size());
        }
    }

    // ---- shared per-row body: optional filter, then accumulate ----

    private static void emitRowBody(StringBuilder out, String indent, Plan.Pipeline pipeline, IntFunction<String> resolver, boolean grouped)
    {
        String bodyIndent = indent;
        if (!pipeline.filters().isEmpty()) {
            String condition = pipeline.filters().stream()
                    .map(p -> "(" + expr(p.left(), resolver) + " " + p.op() + " " + expr(p.right(), resolver) + ")")
                    .collect(joining(" && "));
            out.append(indent).append("if (").append(condition).append(") {\n");
            bodyIndent = indent + "  ";
        }
        if (grouped) {
            emitGroupedAccumulate(out, bodyIndent, pipeline, resolver);
        }
        else {
            emitGlobalAccumulate(out, bodyIndent, pipeline.aggregates(), resolver);
        }
        if (!pipeline.filters().isEmpty()) {
            out.append(indent).append("}\n");
        }
    }

    // ---- global aggregation ----

    private static void emitGlobalState(StringBuilder out, int aggregateCount)
    {
        for (int a = 0; a < aggregateCount; a++) {
            out.append("    long a").append(a).append(" = 0L;\n");
        }
    }

    private static void emitGlobalAccumulate(StringBuilder out, String indent, List<Plan.Aggregate> aggregates, IntFunction<String> resolver)
    {
        for (int a = 0; a < aggregates.size(); a++) {
            out.append(indent).append("a").append(a).append(" += ").append(increment(aggregates.get(a), resolver)).append(";\n");
        }
    }

    private static void emitGlobalResult(StringBuilder out, int aggregateCount)
    {
        out.append("    long[][] result = new long[").append(aggregateCount).append("][];\n");
        for (int a = 0; a < aggregateCount; a++) {
            out.append("    result[").append(a).append("] = new long[] { a").append(a).append(" };\n");
        }
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(1, result);\n");
    }

    // ---- grouped aggregation (single long key) ----

    private static void emitGroupedState(StringBuilder out, Plan.Pipeline pipeline)
    {
        int aggregateCount = pipeline.aggregates().size();
        Plan.Domain domain = pipeline.groupDomain();
        if (domain != null) {
            // Array mode: aggregate arrays indexed directly by (key - min). No hashing, no rehash.
            out.append("    long gmin = ").append(domain.min()).append("L;\n");
            out.append("    int grange = (int) (").append(domain.max()).append("L - ").append(domain.min()).append("L + 1);\n");
            out.append("    boolean[] gused = new boolean[grange]; int groupCount = 0;\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("    long[] agg").append(a).append(" = new long[grange];\n");
            }
            return;
        }
        out.append("    int cap = 1024;\n");
        out.append("    long[] htKey = new long[cap]; int[] htGid = new int[cap];\n");
        out.append("    java.util.Arrays.fill(htGid, -1);\n");
        out.append("    int htMask = cap - 1; int htFill = (int) (cap * 0.75f); int groupCount = 0;\n");
        out.append("    long[] keyByGid = new long[16];\n");
        for (int a = 0; a < aggregateCount; a++) {
            out.append("    long[] agg").append(a).append(" = new long[16];\n");
        }
    }

    private static void emitGroupedAccumulate(StringBuilder out, String indent, Plan.Pipeline pipeline, IntFunction<String> resolver)
    {
        List<Plan.Aggregate> aggregates = pipeline.aggregates();
        if (pipeline.groupDomain() != null) {
            out.append(indent).append("long gkey = ").append(expr(pipeline.groupKeys().getFirst(), resolver)).append(";\n");
            out.append(indent).append("int goff = (int) (gkey - gmin);\n");
            out.append(indent).append("if (!gused[goff]) { gused[goff] = true; groupCount++; }\n");
            for (int a = 0; a < aggregates.size(); a++) {
                out.append(indent).append("agg").append(a).append("[goff] += ").append(increment(aggregates.get(a), resolver)).append(";\n");
            }
            return;
        }
        out.append(indent).append("long gkey = ").append(expr(pipeline.groupKeys().getFirst(), resolver)).append(";\n");
        out.append(indent).append("int gslot = mix(gkey) & htMask;\n");
        out.append(indent).append("while (htGid[gslot] != -1 && htKey[gslot] != gkey) { gslot = (gslot + 1) & htMask; }\n");
        out.append(indent).append("int gid = htGid[gslot];\n");
        out.append(indent).append("if (gid == -1) {\n");
        String b = indent + "  ";
        out.append(b).append("gid = groupCount++; htKey[gslot] = gkey; htGid[gslot] = gid;\n");
        out.append(b).append("if (gid == keyByGid.length) {\n");
        out.append(b).append("  int n = keyByGid.length * 2; keyByGid = java.util.Arrays.copyOf(keyByGid, n);\n");
        for (int a = 0; a < aggregates.size(); a++) {
            out.append(b).append("  agg").append(a).append(" = java.util.Arrays.copyOf(agg").append(a).append(", n);\n");
        }
        out.append(b).append("}\n");
        out.append(b).append("keyByGid[gid] = gkey;\n");
        out.append(b).append("if (groupCount > htFill) {\n");
        out.append(b).append("  int ncap = cap * 2; long[] nKey = new long[ncap]; int[] nGid = new int[ncap];\n");
        out.append(b).append("  java.util.Arrays.fill(nGid, -1); int nMask = ncap - 1;\n");
        out.append(b).append("  for (int s = 0; s < cap; s++) { if (htGid[s] != -1) {\n");
        out.append(b).append("    int ns = mix(htKey[s]) & nMask; while (nGid[ns] != -1) { ns = (ns + 1) & nMask; }\n");
        out.append(b).append("    nKey[ns] = htKey[s]; nGid[ns] = htGid[s]; } }\n");
        out.append(b).append("  htKey = nKey; htGid = nGid; htMask = nMask; cap = ncap; htFill = (int) (cap * 0.75f);\n");
        out.append(b).append("}\n");
        out.append(indent).append("}\n");
        for (int a = 0; a < aggregates.size(); a++) {
            out.append(indent).append("agg").append(a).append("[gid] += ").append(increment(aggregates.get(a), resolver)).append(";\n");
        }
    }

    private static void emitGroupedResult(StringBuilder out, Plan.Pipeline pipeline)
    {
        int aggregateCount = pipeline.aggregates().size();
        if (pipeline.groupDomain() != null) {
            // Compact the dense aggregate arrays to the occupied offsets; key = offset + min.
            out.append("    long[][] result = new long[").append(1 + aggregateCount).append("][];\n");
            out.append("    long[] outKey = new long[groupCount];\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("    long[] outAgg").append(a).append(" = new long[groupCount];\n");
            }
            out.append("    int w = 0;\n");
            out.append("    for (int o = 0; o < grange; o++) {\n");
            out.append("      if (gused[o]) {\n");
            out.append("        outKey[w] = o + gmin;\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("        outAgg").append(a).append("[w] = agg").append(a).append("[o];\n");
            }
            out.append("        w++;\n");
            out.append("      }\n");
            out.append("    }\n");
            out.append("    result[0] = outKey;\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("    result[").append(a + 1).append("] = outAgg").append(a).append(";\n");
            }
            out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(groupCount, result);\n");
            return;
        }
        out.append("    long[][] result = new long[").append(1 + aggregateCount).append("][];\n");
        out.append("    result[0] = java.util.Arrays.copyOf(keyByGid, groupCount);\n");
        for (int a = 0; a < aggregateCount; a++) {
            out.append("    result[").append(a + 1).append("] = java.util.Arrays.copyOf(agg").append(a).append(", groupCount);\n");
        }
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(groupCount, result);\n");
    }

    // ---- helpers ----

    private static void emitMix(StringBuilder out)
    {
        out.append("  private static int mix(long key) {\n");
        out.append("    long h = key; h ^= h >>> 33; h *= 0xff51afd7ed558ccdL; h ^= h >>> 33;");
        out.append(" h *= 0xc4ceb9fe1a85ec53L; h ^= h >>> 33; return (int) h;\n  }\n");
    }

    private static String increment(Plan.Aggregate aggregate, IntFunction<String> resolver)
    {
        return switch (aggregate.fn()) {
            case "sum" -> expr(aggregate.input(), resolver);
            case "count" -> "1L";
            default -> throw new UnsupportedOperationException("aggregate: " + aggregate.fn());
        };
    }

    private static TreeSet<Integer> referencedColumns(Plan.Pipeline pipeline)
    {
        TreeSet<Integer> referenced = new TreeSet<>();
        for (Plan.Predicate predicate : pipeline.filters()) {
            collectColumns(predicate.left(), referenced);
            collectColumns(predicate.right(), referenced);
        }
        for (Plan.Expr groupKey : pipeline.groupKeys()) {
            collectColumns(groupKey, referenced);
        }
        for (Plan.Aggregate aggregate : pipeline.aggregates()) {
            if (aggregate.input() != null) {
                collectColumns(aggregate.input(), referenced);
            }
        }
        return referenced;
    }

    private static void collectColumns(Plan.Expr expr, TreeSet<Integer> into)
    {
        if (expr instanceof Plan.Col col) {
            into.add(col.index());
        }
        else if (expr instanceof Plan.Bin bin) {
            collectColumns(bin.left(), into);
            collectColumns(bin.right(), into);
        }
        // Plan.Lit references no columns.
    }

    private static String expr(Plan.Expr expr, IntFunction<String> resolver)
    {
        return switch (expr) {
            case Plan.Col col -> resolver.apply(col.index());
            case Plan.Lit lit -> lit.value() + "L";
            case Plan.Bin bin -> "(" + expr(bin.left(), resolver) + " " + bin.op() + " " + expr(bin.right(), resolver) + ")";
        };
    }

    private static final class InMemoryCompiler
    {
        private InMemoryCompiler() {}

        static Class<?> compile(String className, String source)
        {
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            if (compiler == null) {
                throw new IllegalStateException("No system Java compiler available (run on a JDK, not a JRE)");
            }
            DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
            StandardJavaFileManager standard = compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8);
            MemoryFileManager fileManager = new MemoryFileManager(standard);
            JavaFileObject sourceObject = new SourceObject(className, source);
            List<String> options = List.of("-classpath", System.getProperty("java.class.path"));
            boolean ok = compiler.getTask(null, fileManager, diagnostics, options, null, List.of(sourceObject)).call();
            if (!ok) {
                StringBuilder message = new StringBuilder("Compilation failed:\n");
                diagnostics.getDiagnostics().forEach(d -> message.append(d).append('\n'));
                message.append("--- source ---\n").append(source);
                throw new IllegalStateException(message.toString());
            }
            try {
                return fileManager.loader().loadClass(className);
            }
            catch (ClassNotFoundException e) {
                throw new IllegalStateException("Compiled class not found: " + className, e);
            }
        }
    }

    private static final class SourceObject
            extends SimpleJavaFileObject
    {
        private final String code;

        SourceObject(String className, String code)
        {
            super(URI.create("string:///" + className.replace('.', '/') + ".java"), Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors)
        {
            return code;
        }
    }

    private static final class MemoryFileManager
            extends ForwardingJavaFileManager<StandardJavaFileManager>
    {
        private final Map<String, ByteArrayOutputStream> bytecode = new HashMap<>();

        MemoryFileManager(StandardJavaFileManager fileManager)
        {
            super(fileManager);
        }

        @Override
        public JavaFileObject getJavaFileForOutput(Location location, String className, JavaFileObject.Kind kind, FileObject sibling)
        {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            bytecode.put(className, stream);
            return new SimpleJavaFileObject(URI.create("bytes:///" + className.replace('.', '/') + ".class"), kind)
            {
                @Override
                public OutputStream openOutputStream()
                {
                    return stream;
                }
            };
        }

        ClassLoader loader()
        {
            Map<String, byte[]> classes = new HashMap<>();
            bytecode.forEach((name, stream) -> classes.put(name, stream.toByteArray()));
            return new ClassLoader(PipelineCompiler.class.getClassLoader())
            {
                @Override
                protected Class<?> findClass(String name)
                        throws ClassNotFoundException
                {
                    byte[] definition = classes.get(name);
                    if (definition == null) {
                        throw new ClassNotFoundException(name);
                    }
                    return defineClass(name, definition, 0, definition.length);
                }
            };
        }
    }
}
