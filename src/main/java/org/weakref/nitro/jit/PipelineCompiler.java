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
import java.util.ArrayList;
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

    // Runtime array-mode guards: use a direct-indexed array when the key domain is no larger than
    // DENSITY_FACTOR x the row count (dense enough) and at most MAX_ARRAY_RANGE entries (bounded memory).
    private static final long MAX_ARRAY_RANGE = 1L << 27;
    private static final int DENSITY_FACTOR = 8;
    // Rows sampled to estimate a streaming group key's domain before speculating array-mode grouping.
    private static final int SAMPLE_SIZE = 4096;

    private PipelineCompiler() {}

    public static CompiledPipeline compile(Plan.Pipeline pipeline)
    {
        return compile(pipeline, null);
    }

    /**
     * Compile {@code pipeline} for inputs with the given physical encodings. {@code encodings[input][column]}
     * declares each scan column's {@link ColumnEncoding}; {@code null} (or any unlisted column) means
     * {@link ColumnEncoding#FLAT}. Build (join) inputs are read as flat.
     */
    public static CompiledPipeline compile(Plan.Pipeline pipeline, ColumnEncoding[][] encodings)
    {
        String simpleName = "Pipeline_" + COUNTER.incrementAndGet();
        String source = render(pipeline, encodings, simpleName);
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
        return render(pipeline, null, "Pipeline_preview");
    }

    public static String render(Plan.Pipeline pipeline, ColumnEncoding[][] encodings)
    {
        return render(pipeline, encodings, "Pipeline_preview");
    }

    private static ColumnEncoding encodingOf(ColumnEncoding[][] encodings, int input, int column)
    {
        if (encodings == null || input >= encodings.length || encodings[input] == null
                || column >= encodings[input].length || encodings[input][column] == null) {
            return ColumnEncoding.FLAT;
        }
        return encodings[input][column];
    }

    private static String render(Plan.Pipeline pipeline, ColumnEncoding[][] encodings, String simpleName)
    {
        StringBuilder out = new StringBuilder();
        out.append("package ").append(PACKAGE).append(";\n");
        out.append("public final class ").append(simpleName)
                .append(" implements org.weakref.nitro.jit.CompiledPipeline {\n");
        // Any grouping needs mix() (array-mode grouping still keeps a hash table for the deopt fallback); so
        // does a join (its hash-table fallback branch).
        boolean needsMix = !pipeline.groupKeys().isEmpty() || !pipeline.joins().isEmpty();
        if (needsMix) {
            emitMix(out);
        }
        out.append("  @Override public org.weakref.nitro.jit.CompiledPipeline.Result execute(org.weakref.nitro.jit.Column[][] inputs, int[] rowCounts) {\n");
        if (!pipeline.joins().isEmpty()) {
            emitJoinBody(out, pipeline);
        }
        else {
            emitScanBody(out, pipeline, encodings);
        }
        out.append("  }\n}\n");
        return out.toString();
    }

    // ---- single-input scan -> filter -> aggregate ----

    private static void emitScanBody(StringBuilder out, Plan.Pipeline pipeline, ColumnEncoding[][] encodings)
    {
        out.append("    org.weakref.nitro.jit.Column[] in = inputs[0]; int rowCount = rowCounts[0];\n");
        TreeSet<Integer> referenced = referencedColumns(pipeline);
        for (int column : referenced) {
            emitScanColumnLoad(out, column, encodingOf(encodings, 0, column));
        }
        IntFunction<String> resolver = index -> scanAccess(index, encodingOf(encodings, 0, index), "i");
        boolean grouped = !pipeline.groupKeys().isEmpty();
        // A single-key scan group can speculate array mode: estimate the key domain from a sample, bet on a
        // direct-indexed array, and deopt to a hash table if a later key falls outside the bet.
        boolean speculate = grouped && pipeline.groupKeys().size() == 1;
        if (speculate) {
            IntFunction<String> sampleResolver = index -> scanAccess(index, encodingOf(encodings, 0, index), "s");
            emitGroupSampleProlog(out, pipeline.groupKeys().getFirst(), sampleResolver);
        }
        if (grouped) {
            emitGroupedState(out, pipeline, speculate);
        }
        else {
            emitGlobalState(out, pipeline.aggregates());
        }
        out.append("    for (int i = 0; i < rowCount; i++) {\n");
        emitRowBody(out, "      ", pipeline, resolver, grouped, speculate);
        out.append("    }\n");
        if (grouped) {
            emitGroupedResult(out, pipeline, speculate);
        }
        else {
            emitGlobalResult(out, pipeline.aggregates().size());
        }
    }

    /** Declare the local(s) for a scan column according to its encoding: flat values, dict ids + dictionary, or a constant. */
    private static void emitScanColumnLoad(StringBuilder out, int column, ColumnEncoding encoding)
    {
        switch (encoding) {
            case FLAT -> out.append("    long[] c").append(column).append(" = ((org.weakref.nitro.jit.Column.FlatColumn) in[").append(column).append("]).values();\n");
            case DICTIONARY -> {
                out.append("    int[] cIds").append(column).append(" = ((org.weakref.nitro.jit.Column.DictionaryColumn) in[").append(column).append("]).ids();\n");
                out.append("    long[] cDict").append(column).append(" = ((org.weakref.nitro.jit.Column.DictionaryColumn) in[").append(column).append("]).dictionary();\n");
            }
            case CONSTANT -> out.append("    long cConst").append(column).append(" = ((org.weakref.nitro.jit.Column.ConstantColumn) in[").append(column).append("]).value();\n");
        }
    }

    /** Access expression for a scan column at row {@code row}: flat index, dictionary indirection, or hoisted constant. */
    private static String scanAccess(int column, ColumnEncoding encoding, String row)
    {
        return switch (encoding) {
            case FLAT -> "c" + column + "[" + row + "]";
            case DICTIONARY -> "cDict" + column + "[cIds" + column + "[" + row + "]]";
            case CONSTANT -> "cConst" + column;
        };
    }

    /** Sample the first rows to estimate the single group key's domain and decide whether to speculate array mode. */
    private static void emitGroupSampleProlog(StringBuilder out, Plan.Expr groupKey, IntFunction<String> sampleResolver)
    {
        String sampleKey = expr(groupKey, sampleResolver);
        out.append("    int sampleCount = Math.min(rowCount, ").append(SAMPLE_SIZE).append(");\n");
        out.append("    long sMin = Long.MAX_VALUE, sMax = Long.MIN_VALUE;\n");
        out.append("    for (int s = 0; s < sampleCount; s++) { long kk = ").append(sampleKey)
                .append("; if (kk < sMin) { sMin = kk; } if (kk > sMax) { sMax = kk; } }\n");
        out.append("    long sRange = sampleCount == 0 ? 0 : (sMax - sMin + 1);\n");
        out.append("    boolean speculateArray = sampleCount > 0 && sRange >= 1 && sRange <= ").append(MAX_ARRAY_RANGE).append("L;\n");
        out.append("    long aMin = sMin;\n");
        // Give the bet headroom above the sampled max so minor domain underestimates do not deopt immediately.
        out.append("    int aSize = speculateArray ? (int) Math.min(").append(MAX_ARRAY_RANGE).append("L, sRange + (sRange >> 1) + 64) : 0;\n");
    }

    // ---- scan(probe) inner-join builds -> filter -> aggregate ----

    private static void emitJoinBody(StringBuilder out, Plan.Pipeline pipeline)
    {
        int probeColumns = pipeline.columnCount();
        List<Plan.Join> joins = pipeline.joins();
        int joinCount = joins.size();

        // Where each build's columns begin in the combined column space (probe columns first, then each build).
        int[] buildOffset = new int[joinCount];
        int offset = probeColumns;
        for (int k = 0; k < joinCount; k++) {
            buildOffset[k] = offset;
            offset += joins.get(k).build().columnCount();
        }

        // Partition referenced columns into probe columns and per-build columns, always including join keys.
        TreeSet<Integer> combined = referencedColumns(pipeline);
        for (Plan.Join join : joins) {
            for (int probeKey : join.probeKeyColumns()) {
                combined.add(probeKey);
            }
        }
        TreeSet<Integer> probeReferenced = new TreeSet<>();
        List<TreeSet<Integer>> buildReferenced = new ArrayList<>();
        for (int k = 0; k < joinCount; k++) {
            TreeSet<Integer> set = new TreeSet<>();
            for (int buildKey : joins.get(k).build().keyColumns()) {
                set.add(buildKey);
            }
            buildReferenced.add(set);
        }
        for (int column : combined) {
            if (column < probeColumns) {
                probeReferenced.add(column);
            }
            else {
                int build = buildOf(joins, buildOffset, column);
                buildReferenced.get(build).add(column - buildOffset[build]);
            }
        }

        IntFunction<String> resolver = index -> {
            if (index < probeColumns) {
                return "p" + index + "[i]";
            }
            int build = buildOf(joins, buildOffset, index);
            return "b" + build + "_" + (index - buildOffset[build]) + "[buildRow" + build + "]";
        };

        out.append("    org.weakref.nitro.jit.Column[] probe = inputs[0]; int probeRows = rowCounts[0];\n");
        for (int column : probeReferenced) {
            out.append("    long[] p").append(column).append(" = ((org.weakref.nitro.jit.Column.FlatColumn) probe[").append(column).append("]).values();\n");
        }
        for (int k = 0; k < joinCount; k++) {
            Plan.Join join = joins.get(k);
            int keyCount = join.build().keyColumns().length;
            if (join.probeKeyColumns().length != keyCount) {
                throw new IllegalArgumentException("join " + k + " key count mismatch: probe " + join.probeKeyColumns().length + " vs build " + keyCount);
            }
            out.append("    org.weakref.nitro.jit.Column[] build").append(k).append(" = inputs[").append(k + 1).append("]; int build").append(k).append("Rows = rowCounts[").append(k + 1).append("];\n");
            for (int column : buildReferenced.get(k)) {
                out.append("    long[] b").append(k).append("_").append(column).append(" = ((org.weakref.nitro.jit.Column.FlatColumn) build").append(k).append("[").append(column).append("]).values();\n");
            }
            emitBuildStructures(out, k, join.build().keyColumns());
        }

        boolean grouped = !pipeline.groupKeys().isEmpty();
        if (grouped) {
            emitGroupedState(out, pipeline, false);
        }
        else {
            emitGlobalState(out, pipeline.aggregates());
        }

        out.append("    for (int i = 0; i < probeRows; i++) {\n");
        String indent = "      ";
        for (int k = 0; k < joinCount; k++) {
            emitProbeLookup(out, indent, k, joins.get(k));
            out.append(indent).append("if (buildRow").append(k).append(" != -1) {\n");
            indent += "  ";
        }
        emitRowBody(out, indent, pipeline, resolver, grouped, false);
        for (int k = 0; k < joinCount; k++) {
            indent = indent.substring(2);
            out.append(indent).append("}\n");
        }
        out.append("    }\n");

        if (grouped) {
            emitGroupedResult(out, pipeline, false);
        }
        else {
            emitGlobalResult(out, pipeline.aggregates().size());
        }
    }

    private static int buildOf(List<Plan.Join> joins, int[] buildOffset, int combinedColumn)
    {
        for (int k = 0; k < joins.size(); k++) {
            int start = buildOffset[k];
            int end = start + joins.get(k).build().columnCount();
            if (combinedColumn >= start && combinedColumn < end) {
                return k;
            }
        }
        throw new IllegalArgumentException("combined column out of range: " + combinedColumn);
    }

    /**
     * Build the lookup structure for join {@code k} into method-scope variables. A single-key build measures its
     * key range and chooses array mode (direct index) or an open-addressing hash table at runtime; a composite
     * key always uses the hash table. The probe loop selects the matching lookup via {@code useArray<k>}.
     */
    private static void emitBuildStructures(StringBuilder out, int k, int[] buildKeys)
    {
        int keyCount = buildKeys.length;
        String rows = "build" + k + "Rows";
        if (keyCount == 1) {
            String buildKey = "b" + k + "_" + buildKeys[0];
            out.append("    long minKey").append(k).append(" = Long.MAX_VALUE, maxKey").append(k).append(" = Long.MIN_VALUE;\n");
            out.append("    for (int r = 0; r < ").append(rows).append("; r++) { long key = ").append(buildKey)
                    .append("[r]; if (key < minKey").append(k).append(") { minKey").append(k).append(" = key; } if (key > maxKey").append(k).append(") { maxKey").append(k).append(" = key; } }\n");
            out.append("    long keyRange").append(k).append(" = ").append(rows).append(" == 0 ? 0 : (maxKey").append(k).append(" - minKey").append(k).append(" + 1);\n");
            out.append("    boolean useArray").append(k).append(" = ").append(rows).append(" > 0 && keyRange").append(k).append(" >= 1 && keyRange").append(k).append(" <= ")
                    .append(MAX_ARRAY_RANGE).append("L && keyRange").append(k).append(" <= (long) ").append(rows).append(" * ").append(DENSITY_FACTOR).append("L;\n");
            out.append("    int[] buildRowByKey").append(k).append(" = null;\n");
            out.append("    long[] jKey").append(k).append("_0 = null; int[] jRow").append(k).append(" = null; int jMask").append(k).append(" = 0;\n");
            out.append("    if (useArray").append(k).append(") {\n");
            out.append("      int range = (int) keyRange").append(k).append("; buildRowByKey").append(k).append(" = new int[range]; java.util.Arrays.fill(buildRowByKey").append(k).append(", -1);\n");
            out.append("      for (int r = 0; r < ").append(rows).append("; r++) { buildRowByKey").append(k).append("[(int) (").append(buildKey).append("[r] - minKey").append(k).append(")] = r; }\n");
            out.append("    }\n    else {\n");
            emitHashBuild(out, "      ", k, buildKeys);
            out.append("    }\n");
        }
        else {
            for (int kx = 0; kx < keyCount; kx++) {
                out.append("    long[] jKey").append(k).append("_").append(kx).append(" = null;\n");
            }
            out.append("    int[] jRow").append(k).append(" = null; int jMask").append(k).append(" = 0;\n");
            out.append("    {\n");
            emitHashBuild(out, "      ", k, buildKeys);
            out.append("    }\n");
        }
    }

    /** Open-addressing build for join {@code k} (build keys assumed unique), populating the join's slot arrays. */
    private static void emitHashBuild(StringBuilder out, String indent, int k, int[] buildKeys)
    {
        int keyCount = buildKeys.length;
        String rows = "build" + k + "Rows";
        out.append(indent).append("int jcap = 16; while (jcap * 0.75f < ").append(rows).append(") { jcap <<= 1; }\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("jKey").append(k).append("_").append(kx).append(" = new long[jcap];\n");
        }
        out.append(indent).append("jRow").append(k).append(" = new int[jcap]; java.util.Arrays.fill(jRow").append(k).append(", -1); jMask").append(k).append(" = jcap - 1;\n");
        out.append(indent).append("for (int r = 0; r < ").append(rows).append("; r++) {\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("  long bk").append(kx).append(" = b").append(k).append("_").append(buildKeys[kx]).append("[r];\n");
        }
        out.append(indent).append("  int slot = mix(").append(hashFold("bk", "", keyCount)).append(") & jMask").append(k).append(";\n");
        out.append(indent).append("  while (jRow").append(k).append("[slot] != -1) { slot = (slot + 1) & jMask").append(k).append("; }\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("  jKey").append(k).append("_").append(kx).append("[slot] = bk").append(kx).append(";\n");
        }
        out.append(indent).append("  jRow").append(k).append("[slot] = r;\n");
        out.append(indent).append("}\n");
    }

    /** Per-row lookup for join {@code k}, leaving {@code int buildRow<k>} in scope (-1 = no match). */
    private static void emitProbeLookup(StringBuilder out, String indent, int k, Plan.Join join)
    {
        int[] probeKeys = join.probeKeyColumns();
        int keyCount = probeKeys.length;
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("long pk").append(k).append("_").append(kx).append(" = p").append(probeKeys[kx]).append("[i];\n");
        }
        out.append(indent).append("int buildRow").append(k).append(";\n");
        if (keyCount == 1) {
            out.append(indent).append("if (useArray").append(k).append(") {\n");
            out.append(indent).append("  long jk = pk").append(k).append("_0;\n");
            out.append(indent).append("  buildRow").append(k).append(" = (jk >= minKey").append(k).append(" && jk <= maxKey").append(k).append(") ? buildRowByKey").append(k).append("[(int) (jk - minKey").append(k).append(")] : -1;\n");
            out.append(indent).append("}\n    ").append(indent).append("else {\n");
        }
        else {
            out.append(indent).append("{\n");
        }
        String body = indent + "  ";
        out.append(body).append("int js = mix(").append(hashFold("pk" + k + "_", "", keyCount)).append(") & jMask").append(k).append(";\n");
        out.append(body).append("int br = -1;\n");
        out.append(body).append("while (jRow").append(k).append("[js] != -1) {\n");
        out.append(body).append("  if (").append(probeKeyCompare(k, keyCount)).append(") { br = jRow").append(k).append("[js]; break; }\n");
        out.append(body).append("  js = (js + 1) & jMask").append(k).append(";\n");
        out.append(body).append("}\n");
        out.append(body).append("buildRow").append(k).append(" = br;\n");
        out.append(indent).append("}\n");
    }

    /** Conjunction {@code jKey<k>_0[js] == pk<k>_0 && ...} comparing every stored build key component. */
    private static String probeKeyCompare(int k, int keyCount)
    {
        StringBuilder compare = new StringBuilder();
        for (int kx = 0; kx < keyCount; kx++) {
            if (kx > 0) {
                compare.append(" && ");
            }
            compare.append("jKey").append(k).append("_").append(kx).append("[js] == pk").append(k).append("_").append(kx);
        }
        return compare.toString();
    }

    // ---- shared per-row body: optional filter, then accumulate ----

    private static void emitRowBody(StringBuilder out, String indent, Plan.Pipeline pipeline, IntFunction<String> resolver, boolean grouped, boolean speculate)
    {
        String bodyIndent = indent;
        if (!pipeline.filters().isEmpty()) {
            String condition = pipeline.filters().stream()
                    .map(c -> condition(c, resolver))
                    .collect(joining(" && "));
            out.append(indent).append("if (").append(condition).append(") {\n");
            bodyIndent = indent + "  ";
        }
        if (grouped) {
            emitGroupedAccumulate(out, bodyIndent, pipeline, resolver, speculate);
        }
        else {
            emitGlobalAccumulate(out, bodyIndent, pipeline.aggregates(), resolver);
        }
        if (!pipeline.filters().isEmpty()) {
            out.append(indent).append("}\n");
        }
    }

    // ---- global aggregation ----

    private static void emitGlobalState(StringBuilder out, List<Plan.Aggregate> aggregates)
    {
        for (int a = 0; a < aggregates.size(); a++) {
            out.append("    long a").append(a).append(" = ").append(aggregator(aggregates.get(a)).identity()).append(";\n");
        }
    }

    private static void emitGlobalAccumulate(StringBuilder out, String indent, List<Plan.Aggregate> aggregates, IntFunction<String> resolver)
    {
        for (int a = 0; a < aggregates.size(); a++) {
            String state = "a" + a;
            out.append(indent).append(state).append(" = ").append(aggregator(aggregates.get(a)).update(state, input(aggregates.get(a), resolver))).append(";\n");
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

    private static void emitGroupedState(StringBuilder out, Plan.Pipeline pipeline, boolean speculate)
    {
        int aggregateCount = pipeline.aggregates().size();
        if (speculate) {
            // Array structures (sized from the sampled domain) plus a hash table that stays empty unless a key
            // falls outside the bet and we deopt into it. deopted starts true when we never speculated at all.
            out.append("    boolean[] gused = new boolean[aSize];\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("    long[] aAgg").append(a).append(" = new long[aSize];\n");
            }
            out.append("    int arrayGroupCount = 0; boolean deopted = !speculateArray;\n");
            emitSingleKeyHashState(out, aggregateCount);
            return;
        }
        int keyCount = pipeline.groupKeys().size();
        out.append("    int cap = 1024;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append("    long[] htKey").append(kx).append(" = new long[cap];\n");
        }
        out.append("    int[] htGid = new int[cap];\n");
        out.append("    java.util.Arrays.fill(htGid, -1);\n");
        out.append("    int htMask = cap - 1; int htFill = (int) (cap * 0.75f); int groupCount = 0;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append("    long[] keyByGid").append(kx).append(" = new long[16];\n");
        }
        for (int a = 0; a < aggregateCount; a++) {
            out.append("    long[] agg").append(a).append(" = new long[16];\n");
        }
    }

    private static void emitGroupedAccumulate(StringBuilder out, String indent, Plan.Pipeline pipeline, IntFunction<String> resolver, boolean speculate)
    {
        List<Plan.Aggregate> aggregates = pipeline.aggregates();
        if (speculate) {
            int aggregateCount = aggregates.size();
            out.append(indent).append("long gkey = ").append(expr(pipeline.groupKeys().getFirst(), resolver)).append(";\n");
            out.append(indent).append("if (!deopted) {\n");
            String a1 = indent + "  ";
            out.append(a1).append("long aoff = gkey - aMin;\n");
            out.append(a1).append("if (aoff >= 0 && aoff < aSize) {\n");
            String a2 = a1 + "  ";
            out.append(a2).append("int ao = (int) aoff;\n");
            out.append(a2).append("if (!gused[ao]) { gused[ao] = true; arrayGroupCount++;\n");
            emitStateIdentity(out, a2 + "  ", aggregates, "aAgg", "ao");
            out.append(a2).append("}\n");
            for (int a = 0; a < aggregateCount; a++) {
                String state = "aAgg" + a + "[ao]";
                out.append(a2).append(state).append(" = ").append(aggregator(aggregates.get(a)).update(state, input(aggregates.get(a), resolver))).append(";\n");
            }
            out.append(a1).append("}\n");
            out.append(a1).append("else {\n");
            // Deopt: migrate every populated array group into the hash table, then handle this row via hash.
            out.append(a2).append("for (int mo = 0; mo < aSize; mo++) {\n");
            out.append(a2).append("  if (gused[mo]) {\n");
            String m = a2 + "    ";
            out.append(m).append("long hkey = mo + aMin;\n");
            emitSingleKeyHashFindOrCreate(out, m, aggregates);
            for (int a = 0; a < aggregateCount; a++) {
                String state = "agg" + a + "[gid]";
                out.append(m).append(state).append(" = ").append(aggregator(aggregates.get(a)).merge(state, "aAgg" + a + "[mo]")).append(";\n");
            }
            out.append(a2).append("  }\n");
            out.append(a2).append("}\n");
            out.append(a2).append("deopted = true;\n");
            out.append(a1).append("}\n");
            out.append(indent).append("}\n");
            out.append(indent).append("if (deopted) {\n");
            out.append(a1).append("long hkey = gkey;\n");
            emitSingleKeyHashFindOrCreate(out, a1, aggregates);
            for (int a = 0; a < aggregateCount; a++) {
                String state = "agg" + a + "[gid]";
                out.append(a1).append(state).append(" = ").append(aggregator(aggregates.get(a)).update(state, input(aggregates.get(a), resolver))).append(";\n");
            }
            out.append(indent).append("}\n");
            return;
        }
        int keyCount = pipeline.groupKeys().size();
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("long gk").append(kx).append(" = ").append(expr(pipeline.groupKeys().get(kx), resolver)).append(";\n");
        }
        out.append(indent).append("int gslot = mix(").append(hashFold("gk", "", keyCount)).append(") & htMask;\n");
        out.append(indent).append("while (htGid[gslot] != -1 && !(").append(keyCompare("gslot", keyCount)).append(")) { gslot = (gslot + 1) & htMask; }\n");
        out.append(indent).append("int gid = htGid[gslot];\n");
        out.append(indent).append("if (gid == -1) {\n");
        String b = indent + "  ";
        out.append(b).append("gid = groupCount++; htGid[gslot] = gid;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("htKey").append(kx).append("[gslot] = gk").append(kx).append(";\n");
        }
        out.append(b).append("if (gid == keyByGid0.length) {\n");
        out.append(b).append("  int n = keyByGid0.length * 2;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("  keyByGid").append(kx).append(" = java.util.Arrays.copyOf(keyByGid").append(kx).append(", n);\n");
        }
        for (int a = 0; a < aggregates.size(); a++) {
            out.append(b).append("  agg").append(a).append(" = java.util.Arrays.copyOf(agg").append(a).append(", n);\n");
        }
        out.append(b).append("}\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("keyByGid").append(kx).append("[gid] = gk").append(kx).append(";\n");
        }
        emitStateIdentity(out, b, aggregates, "agg", "gid");
        out.append(b).append("if (groupCount > htFill) {\n");
        out.append(b).append("  int ncap = cap * 2;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("  long[] nKey").append(kx).append(" = new long[ncap];\n");
        }
        out.append(b).append("  int[] nGid = new int[ncap];\n");
        out.append(b).append("  java.util.Arrays.fill(nGid, -1); int nMask = ncap - 1;\n");
        out.append(b).append("  for (int s = 0; s < cap; s++) { if (htGid[s] != -1) {\n");
        out.append(b).append("    int ns = mix(").append(hashFold("htKey", "[s]", keyCount)).append(") & nMask; while (nGid[ns] != -1) { ns = (ns + 1) & nMask; }\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("    nKey").append(kx).append("[ns] = htKey").append(kx).append("[s];\n");
        }
        out.append(b).append("    nGid[ns] = htGid[s]; } }\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("  htKey").append(kx).append(" = nKey").append(kx).append(";\n");
        }
        out.append(b).append("  htGid = nGid; htMask = nMask; cap = ncap; htFill = (int) (cap * 0.75f);\n");
        out.append(b).append("}\n");
        out.append(indent).append("}\n");
        for (int a = 0; a < aggregates.size(); a++) {
            String state = "agg" + a + "[gid]";
            out.append(indent).append(state).append(" = ").append(aggregator(aggregates.get(a)).update(state, input(aggregates.get(a), resolver))).append(";\n");
        }
    }

    private static void emitGroupedResult(StringBuilder out, Plan.Pipeline pipeline, boolean speculate)
    {
        int aggregateCount = pipeline.aggregates().size();
        if (speculate) {
            // If the array bet held, compact it to its occupied offsets; otherwise emit the hash table we
            // deopted into. Both shapes are key column then aggregate columns.
            out.append("    if (!deopted) {\n");
            out.append("      long[][] result = new long[").append(1 + aggregateCount).append("][];\n");
            out.append("      long[] outKey = new long[arrayGroupCount];\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("      long[] outAgg").append(a).append(" = new long[arrayGroupCount];\n");
            }
            out.append("      int w = 0;\n");
            out.append("      for (int o = 0; o < aSize; o++) {\n");
            out.append("        if (gused[o]) {\n");
            out.append("          outKey[w] = o + aMin;\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("          outAgg").append(a).append("[w] = aAgg").append(a).append("[o];\n");
            }
            out.append("          w++;\n");
            out.append("        }\n");
            out.append("      }\n");
            out.append("      result[0] = outKey;\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("      result[").append(a + 1).append("] = outAgg").append(a).append(";\n");
            }
            out.append("      return new org.weakref.nitro.jit.CompiledPipeline.Result(arrayGroupCount, result);\n");
            out.append("    }\n");
            out.append("    long[][] result = new long[").append(1 + aggregateCount).append("][];\n");
            out.append("    result[0] = java.util.Arrays.copyOf(keyByGid0, groupCount);\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("    result[").append(a + 1).append("] = java.util.Arrays.copyOf(agg").append(a).append(", groupCount);\n");
            }
            out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(groupCount, result);\n");
            return;
        }
        int keyCount = pipeline.groupKeys().size();
        out.append("    long[][] result = new long[").append(keyCount + aggregateCount).append("][];\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append("    result[").append(kx).append("] = java.util.Arrays.copyOf(keyByGid").append(kx).append(", groupCount);\n");
        }
        for (int a = 0; a < aggregateCount; a++) {
            out.append("    result[").append(keyCount + a).append("] = java.util.Arrays.copyOf(agg").append(a).append(", groupCount);\n");
        }
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(groupCount, result);\n");
    }

    /** Single-key open-addressing grouping table (the deopt target for speculative array grouping). */
    private static void emitSingleKeyHashState(StringBuilder out, int aggregateCount)
    {
        out.append("    int cap = 1024;\n");
        out.append("    long[] htKey0 = new long[cap];\n");
        out.append("    int[] htGid = new int[cap];\n");
        out.append("    java.util.Arrays.fill(htGid, -1);\n");
        out.append("    int htMask = cap - 1; int htFill = (int) (cap * 0.75f); int groupCount = 0;\n");
        out.append("    long[] keyByGid0 = new long[16];\n");
        for (int a = 0; a < aggregateCount; a++) {
            out.append("    long[] agg").append(a).append(" = new long[16];\n");
        }
    }

    /** Emit a find-or-create lookup of {@code hkey} in the single-key hash table, leaving {@code int gid} in scope. */
    private static void emitSingleKeyHashFindOrCreate(StringBuilder out, String indent, List<Plan.Aggregate> aggregates)
    {
        out.append(indent).append("int hslot = mix(hkey) & htMask;\n");
        out.append(indent).append("while (htGid[hslot] != -1 && htKey0[hslot] != hkey) { hslot = (hslot + 1) & htMask; }\n");
        out.append(indent).append("int gid = htGid[hslot];\n");
        out.append(indent).append("if (gid == -1) {\n");
        String b = indent + "  ";
        out.append(b).append("gid = groupCount++; htGid[hslot] = gid; htKey0[hslot] = hkey;\n");
        out.append(b).append("if (gid == keyByGid0.length) {\n");
        out.append(b).append("  int n = keyByGid0.length * 2; keyByGid0 = java.util.Arrays.copyOf(keyByGid0, n);\n");
        for (int a = 0; a < aggregates.size(); a++) {
            out.append(b).append("  agg").append(a).append(" = java.util.Arrays.copyOf(agg").append(a).append(", n);\n");
        }
        out.append(b).append("}\n");
        out.append(b).append("keyByGid0[gid] = hkey;\n");
        emitStateIdentity(out, b, aggregates, "agg", "gid");
        out.append(b).append("if (groupCount > htFill) {\n");
        out.append(b).append("  int ncap = cap * 2; long[] nKey0 = new long[ncap]; int[] nGid = new int[ncap];\n");
        out.append(b).append("  java.util.Arrays.fill(nGid, -1); int nMask = ncap - 1;\n");
        out.append(b).append("  for (int s = 0; s < cap; s++) { if (htGid[s] != -1) {\n");
        out.append(b).append("    int ns = mix(htKey0[s]) & nMask; while (nGid[ns] != -1) { ns = (ns + 1) & nMask; }\n");
        out.append(b).append("    nKey0[ns] = htKey0[s]; nGid[ns] = htGid[s]; } }\n");
        out.append(b).append("  htKey0 = nKey0; htGid = nGid; htMask = nMask; cap = ncap; htFill = (int) (cap * 0.75f);\n");
        out.append(b).append("}\n");
        out.append(indent).append("}\n");
    }

    // ---- helpers ----

    /**
     * Composite-key hash input over {@code keyCount} components named {@code <base>{i}<suffix>} (e.g.
     * {@code gk0}, or {@code htKey0[s]}). One component folds to itself (preserving the single-key form);
     * multiple fold with a Fibonacci-style multiply-add so each component shifts the others' bits.
     */
    private static String hashFold(String base, String suffix, int keyCount)
    {
        String folded = base + "0" + suffix;
        for (int kx = 1; kx < keyCount; kx++) {
            folded = "(" + folded + ") * 0x9E3779B97F4A7C15L + " + base + kx + suffix;
        }
        return folded;
    }

    /** Conjunction {@code htKey0[slot] == gk0 && ...} comparing every stored key component to the probe. */
    private static String keyCompare(String slot, int keyCount)
    {
        StringBuilder compare = new StringBuilder();
        for (int kx = 0; kx < keyCount; kx++) {
            if (kx > 0) {
                compare.append(" && ");
            }
            compare.append("htKey").append(kx).append("[").append(slot).append("] == gk").append(kx);
        }
        return compare.toString();
    }

    private static void emitMix(StringBuilder out)
    {
        out.append("  private static int mix(long key) {\n");
        out.append("    long h = key; h ^= h >>> 33; h *= 0xff51afd7ed558ccdL; h ^= h >>> 33;");
        out.append(" h *= 0xc4ceb9fe1a85ec53L; h ^= h >>> 33; return (int) h;\n  }\n");
    }

    private static AggregateLibrary.AggregateCompiler aggregator(Plan.Aggregate aggregate)
    {
        return AggregateLibrary.get(aggregate.fn());
    }

    /** Rendered input expression for an aggregate, or {@code null} for a nullary aggregate such as {@code count}. */
    private static String input(Plan.Aggregate aggregate, IntFunction<String> resolver)
    {
        return aggregate.input() == null ? null : expr(aggregate.input(), resolver);
    }

    /** Identity assignments for every aggregate's state cell at array index {@code index} in arrays named {@code prefix<a>}. */
    private static void emitStateIdentity(StringBuilder out, String indent, List<Plan.Aggregate> aggregates, String prefix, String index)
    {
        for (int a = 0; a < aggregates.size(); a++) {
            out.append(indent).append(prefix).append(a).append("[").append(index).append("] = ").append(aggregator(aggregates.get(a)).identity()).append(";\n");
        }
    }

    private static TreeSet<Integer> referencedColumns(Plan.Pipeline pipeline)
    {
        TreeSet<Integer> referenced = new TreeSet<>();
        for (Plan.Condition filter : pipeline.filters()) {
            collectConditionColumns(filter, referenced);
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

    private static void collectConditionColumns(Plan.Condition condition, TreeSet<Integer> into)
    {
        switch (condition) {
            case Plan.Predicate predicate -> {
                collectColumns(predicate.left(), into);
                collectColumns(predicate.right(), into);
            }
            case Plan.And and -> and.conditions().forEach(child -> collectConditionColumns(child, into));
            case Plan.Or or -> or.conditions().forEach(child -> collectConditionColumns(child, into));
            case Plan.Not not -> collectConditionColumns(not.condition(), into);
        }
    }

    /** Render a boolean condition tree as a Java expression. */
    private static String condition(Plan.Condition condition, IntFunction<String> resolver)
    {
        return switch (condition) {
            case Plan.Predicate predicate -> "(" + expr(predicate.left(), resolver) + " " + predicate.op() + " " + expr(predicate.right(), resolver) + ")";
            case Plan.And and -> and.conditions().isEmpty() ? "true"
                    : "(" + and.conditions().stream().map(child -> condition(child, resolver)).collect(joining(" && ")) + ")";
            case Plan.Or or -> or.conditions().isEmpty() ? "false"
                    : "(" + or.conditions().stream().map(child -> condition(child, resolver)).collect(joining(" || ")) + ")";
            case Plan.Not not -> "(!" + condition(not.condition(), resolver) + ")";
        };
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
