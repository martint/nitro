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

import static java.util.stream.Collectors.joining;

/**
 * Data-centric query compiler (prototype). Translates a {@link Plan.Pipeline} into a single fused Java
 * routine — one loop over the input rows that filters, projects, and aggregates with no intermediate
 * materialization and no per-element dispatch — then compiles it in-process and loads it as a
 * {@link CompiledPipeline}. This is the "what a compiler emits" path whose ceiling the PoC measured.
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
        return switch (pipeline.groupKeys().size()) {
            case 0 -> generateGlobalAggregate(pipeline, simpleName);
            case 1 -> generateGroupedAggregate(pipeline, simpleName);
            default -> throw new UnsupportedOperationException(
                    "multi-key grouping not yet supported (same shape, composite hash)");
        };
    }

    private static String generateGlobalAggregate(Plan.Pipeline pipeline, String simpleName)
    {
        // Columns actually referenced, so we only bind locals we use.
        TreeSet<Integer> referenced = new TreeSet<>();
        for (Plan.Predicate predicate : pipeline.filters()) {
            collectColumns(predicate.left(), referenced);
            collectColumns(predicate.right(), referenced);
        }
        for (Plan.Aggregate aggregate : pipeline.aggregates()) {
            if (aggregate.input() != null) {
                collectColumns(aggregate.input(), referenced);
            }
        }

        StringBuilder out = new StringBuilder();
        out.append("package ").append(PACKAGE).append(";\n");
        out.append("public final class ").append(simpleName)
                .append(" implements org.weakref.nitro.jit.CompiledPipeline {\n");
        out.append("  @Override public org.weakref.nitro.jit.CompiledPipeline.Result execute(long[][] columns, int rowCount) {\n");
        for (int column : referenced) {
            out.append("    long[] c").append(column).append(" = columns[").append(column).append("];\n");
        }
        for (int a = 0; a < pipeline.aggregates().size(); a++) {
            out.append("    long a").append(a).append(" = 0L;\n");
        }
        out.append("    for (int i = 0; i < rowCount; i++) {\n");
        String indent = "      ";
        if (!pipeline.filters().isEmpty()) {
            String condition = pipeline.filters().stream()
                    .map(p -> "(" + expr(p.left()) + " " + p.op() + " " + expr(p.right()) + ")")
                    .collect(joining(" && "));
            out.append(indent).append("if (").append(condition).append(") {\n");
            indent = "        ";
        }
        List<Plan.Aggregate> aggregates = pipeline.aggregates();
        for (int a = 0; a < aggregates.size(); a++) {
            Plan.Aggregate aggregate = aggregates.get(a);
            String increment = switch (aggregate.fn()) {
                case "sum" -> expr(aggregate.input());
                case "count" -> "1L";
                default -> throw new UnsupportedOperationException("aggregate: " + aggregate.fn());
            };
            out.append(indent).append("a").append(a).append(" += ").append(increment).append(";\n");
        }
        if (!pipeline.filters().isEmpty()) {
            out.append("      }\n");
        }
        out.append("    }\n");
        out.append("    long[][] result = new long[").append(aggregates.size()).append("][];\n");
        for (int a = 0; a < aggregates.size(); a++) {
            out.append("    result[").append(a).append("] = new long[] { a").append(a).append(" };\n");
        }
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(1, result);\n");
        out.append("  }\n}\n");
        return out.toString();
    }

    private static String generateGroupedAggregate(Plan.Pipeline pipeline, String simpleName)
    {
        Plan.Expr groupKey = pipeline.groupKeys().getFirst();
        List<Plan.Aggregate> aggregates = pipeline.aggregates();

        TreeSet<Integer> referenced = new TreeSet<>();
        collectColumns(groupKey, referenced);
        for (Plan.Predicate predicate : pipeline.filters()) {
            collectColumns(predicate.left(), referenced);
            collectColumns(predicate.right(), referenced);
        }
        for (Plan.Aggregate aggregate : aggregates) {
            if (aggregate.input() != null) {
                collectColumns(aggregate.input(), referenced);
            }
        }

        StringBuilder out = new StringBuilder();
        out.append("package ").append(PACKAGE).append(";\n");
        out.append("public final class ").append(simpleName)
                .append(" implements org.weakref.nitro.jit.CompiledPipeline {\n");
        // Murmur3 64-bit finalizer for key hashing.
        out.append("  private static int mix(long key) {\n");
        out.append("    long h = key; h ^= h >>> 33; h *= 0xff51afd7ed558ccdL; h ^= h >>> 33;");
        out.append(" h *= 0xc4ceb9fe1a85ec53L; h ^= h >>> 33; return (int) h;\n  }\n");
        out.append("  @Override public org.weakref.nitro.jit.CompiledPipeline.Result execute(long[][] columns, int rowCount) {\n");
        for (int column : referenced) {
            out.append("    long[] c").append(column).append(" = columns[").append(column).append("];\n");
        }
        // Open-addressing key -> dense group-id table; aggregate arrays indexed by the dense group id so a
        // rehash only moves the key table, never the accumulators.
        out.append("    int cap = 1024;\n");
        out.append("    long[] htKey = new long[cap];\n");
        out.append("    int[] htGid = new int[cap];\n");
        out.append("    java.util.Arrays.fill(htGid, -1);\n");
        out.append("    int htMask = cap - 1;\n");
        out.append("    int htFill = (int) (cap * 0.75f);\n");
        out.append("    int groupCount = 0;\n");
        out.append("    long[] keyByGid = new long[16];\n");
        for (int a = 0; a < aggregates.size(); a++) {
            out.append("    long[] agg").append(a).append(" = new long[16];\n");
        }
        out.append("    for (int i = 0; i < rowCount; i++) {\n");
        String indent = "      ";
        if (!pipeline.filters().isEmpty()) {
            String condition = pipeline.filters().stream()
                    .map(p -> "(" + expr(p.left()) + " " + p.op() + " " + expr(p.right()) + ")")
                    .collect(joining(" && "));
            out.append(indent).append("if (").append(condition).append(") {\n");
            indent = "        ";
        }
        out.append(indent).append("long key = ").append(expr(groupKey)).append(";\n");
        out.append(indent).append("int slot = mix(key) & htMask;\n");
        out.append(indent).append("while (htGid[slot] != -1 && htKey[slot] != key) { slot = (slot + 1) & htMask; }\n");
        out.append(indent).append("int gid = htGid[slot];\n");
        out.append(indent).append("if (gid == -1) {\n");
        String b = indent + "  ";
        out.append(b).append("gid = groupCount++;\n");
        out.append(b).append("htKey[slot] = key; htGid[slot] = gid;\n");
        out.append(b).append("if (gid == keyByGid.length) {\n");
        out.append(b).append("  int n = keyByGid.length * 2;\n");
        out.append(b).append("  keyByGid = java.util.Arrays.copyOf(keyByGid, n);\n");
        for (int a = 0; a < aggregates.size(); a++) {
            out.append(b).append("  agg").append(a).append(" = java.util.Arrays.copyOf(agg").append(a).append(", n);\n");
        }
        out.append(b).append("}\n");
        out.append(b).append("keyByGid[gid] = key;\n");
        out.append(b).append("if (groupCount > htFill) {\n");
        // Rehash the key table (group ids and accumulators are unaffected).
        out.append(b).append("  int ncap = cap * 2; long[] nKey = new long[ncap]; int[] nGid = new int[ncap];\n");
        out.append(b).append("  java.util.Arrays.fill(nGid, -1); int nMask = ncap - 1;\n");
        out.append(b).append("  for (int s = 0; s < cap; s++) { if (htGid[s] != -1) {\n");
        out.append(b).append("    int ns = mix(htKey[s]) & nMask; while (nGid[ns] != -1) { ns = (ns + 1) & nMask; }\n");
        out.append(b).append("    nKey[ns] = htKey[s]; nGid[ns] = htGid[s]; } }\n");
        out.append(b).append("  htKey = nKey; htGid = nGid; htMask = nMask; cap = ncap; htFill = (int) (cap * 0.75f);\n");
        out.append(b).append("}\n");
        out.append(indent).append("}\n");
        for (int a = 0; a < aggregates.size(); a++) {
            Plan.Aggregate aggregate = aggregates.get(a);
            String increment = switch (aggregate.fn()) {
                case "sum" -> expr(aggregate.input());
                case "count" -> "1L";
                default -> throw new UnsupportedOperationException("aggregate: " + aggregate.fn());
            };
            out.append(indent).append("agg").append(a).append("[gid] += ").append(increment).append(";\n");
        }
        if (!pipeline.filters().isEmpty()) {
            out.append("      }\n");
        }
        out.append("    }\n");
        out.append("    long[][] result = new long[").append(1 + aggregates.size()).append("][];\n");
        out.append("    result[0] = java.util.Arrays.copyOf(keyByGid, groupCount);\n");
        for (int a = 0; a < aggregates.size(); a++) {
            out.append("    result[").append(a + 1).append("] = java.util.Arrays.copyOf(agg").append(a).append(", groupCount);\n");
        }
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(groupCount, result);\n");
        out.append("  }\n}\n");
        return out.toString();
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

    private static String expr(Plan.Expr expr)
    {
        return switch (expr) {
            case Plan.Col col -> "c" + col.index() + "[i]";
            case Plan.Lit lit -> lit.value() + "L";
            case Plan.Bin bin -> "(" + expr(bin.left()) + " " + bin.op() + " " + expr(bin.right()) + ")";
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
