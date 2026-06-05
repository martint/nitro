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

    // Runtime array-mode guards: use a direct-indexed array when the key domain is no larger than
    // DENSITY_FACTOR x the row count (dense enough) and at most MAX_ARRAY_RANGE entries (bounded memory).
    private static final long MAX_ARRAY_RANGE = 1L << 27;
    private static final int DENSITY_FACTOR = 8;
    // Rows sampled to estimate a streaming group key's domain before speculating array-mode grouping.
    private static final int SAMPLE_SIZE = 4096;

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
        StringBuilder out = new StringBuilder();
        out.append("package ").append(PACKAGE).append(";\n");
        out.append("public final class ").append(simpleName)
                .append(" implements org.weakref.nitro.jit.CompiledPipeline {\n");
        // Any grouping needs mix() (array-mode grouping still keeps a hash table for the deopt fallback); so
        // does a join (its hash-table fallback branch).
        boolean needsMix = !pipeline.groupKeys().isEmpty() || pipeline.build() != null;
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
        // A single-key scan group can speculate array mode: estimate the key domain from a sample, bet on a
        // direct-indexed array, and deopt to a hash table if a later key falls outside the bet.
        boolean speculate = grouped && pipeline.groupKeys().size() == 1;
        if (speculate) {
            emitGroupSampleProlog(out, pipeline.groupKeys().getFirst());
        }
        if (grouped) {
            emitGroupedState(out, pipeline, speculate);
        }
        else {
            emitGlobalState(out, pipeline.aggregates().size());
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

    /** Sample the first rows to estimate the single group key's domain and decide whether to speculate array mode. */
    private static void emitGroupSampleProlog(StringBuilder out, Plan.Expr groupKey)
    {
        String sampleKey = expr(groupKey, index -> "c" + index + "[s]");
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

    // ---- scan(probe) inner-join build -> filter -> aggregate ----

    private static void emitJoinBody(StringBuilder out, Plan.Pipeline pipeline)
    {
        Plan.Build build = pipeline.build();
        int probeColumns = pipeline.columnCount();
        int[] probeKeys = pipeline.probeKeyColumns();
        int[] buildKeys = build.keyColumns();
        int keyCount = buildKeys.length;
        if (probeKeys.length != keyCount) {
            throw new IllegalArgumentException("join key count mismatch: probe " + probeKeys.length + " vs build " + keyCount);
        }

        TreeSet<Integer> combined = referencedColumns(pipeline);
        for (int probeKey : probeKeys) {
            combined.add(probeKey);
        }
        TreeSet<Integer> probeReferenced = new TreeSet<>();
        TreeSet<Integer> buildReferenced = new TreeSet<>();
        for (int buildKey : buildKeys) {
            buildReferenced.add(buildKey);
        }
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

        IntFunction<String> resolver = index -> index < probeColumns
                ? "p" + index + "[i]"
                : "b" + (index - probeColumns) + "[buildRow]";
        boolean grouped = !pipeline.groupKeys().isEmpty();

        // Only a single key can use array mode (it needs a dense scalar domain). For one key, measure the build
        // key range and decide array vs hash at runtime; for composite keys, go straight to the hash table.
        boolean arrayCandidate = keyCount == 1;
        if (arrayCandidate) {
            String buildKey = "b" + buildKeys[0];
            out.append("    long minKey = Long.MAX_VALUE, maxKey = Long.MIN_VALUE;\n");
            out.append("    for (int r = 0; r < buildRows; r++) { long key = ").append(buildKey)
                    .append("[r]; if (key < minKey) { minKey = key; } if (key > maxKey) { maxKey = key; } }\n");
            out.append("    long keyRange = buildRows == 0 ? 0 : (maxKey - minKey + 1);\n");
            // Array mode pays off when the key domain is dense (range within a small factor of the row count)
            // and bounded (array fits in memory); otherwise the open-addressing table wins.
            out.append("    boolean useArray = buildRows > 0 && keyRange >= 1 && keyRange <= ")
                    .append(MAX_ARRAY_RANGE).append("L && keyRange <= (long) buildRows * ").append(DENSITY_FACTOR).append("L;\n");
        }

        if (grouped) {
            emitGroupedState(out, pipeline, false);
        }
        else {
            emitGlobalState(out, pipeline.aggregates().size());
        }

        if (arrayCandidate) {
            out.append("    if (useArray) {\n");
            emitArrayJoinProbe(out, "      ", pipeline, probeKeys[0], "b" + buildKeys[0], resolver, grouped);
            out.append("    }\n    else {\n");
            emitHashJoinProbe(out, "      ", pipeline, probeKeys, buildKeys, keyCount, resolver, grouped);
            out.append("    }\n");
        }
        else {
            emitHashJoinProbe(out, "    ", pipeline, probeKeys, buildKeys, keyCount, resolver, grouped);
        }

        if (grouped) {
            emitGroupedResult(out, pipeline, false);
        }
        else {
            emitGlobalResult(out, pipeline.aggregates().size());
        }
    }

    /** Array-mode build + probe (single key): direct index by {@code key - minKey}. Assumes minKey/maxKey/keyRange in scope. */
    private static void emitArrayJoinProbe(StringBuilder out, String indent, Plan.Pipeline pipeline, int probeKey, String buildKey, IntFunction<String> resolver, boolean grouped)
    {
        out.append(indent).append("int range = (int) keyRange;\n");
        out.append(indent).append("int[] buildRowByKey = new int[range]; java.util.Arrays.fill(buildRowByKey, -1);\n");
        out.append(indent).append("for (int r = 0; r < buildRows; r++) { buildRowByKey[(int) (").append(buildKey).append("[r] - minKey)] = r; }\n");
        out.append(indent).append("for (int i = 0; i < probeRows; i++) {\n");
        out.append(indent).append("  long jk = p").append(probeKey).append("[i];\n");
        out.append(indent).append("  int buildRow = (jk >= minKey && jk <= maxKey) ? buildRowByKey[(int) (jk - minKey)] : -1;\n");
        out.append(indent).append("  if (buildRow != -1) {\n");
        emitRowBody(out, indent + "    ", pipeline, resolver, grouped, false);
        out.append(indent).append("  }\n");
        out.append(indent).append("}\n");
    }

    /** Open-addressing composite-key build + probe (build keys assumed unique). */
    private static void emitHashJoinProbe(StringBuilder out, String indent, Plan.Pipeline pipeline, int[] probeKeys, int[] buildKeys, int keyCount, IntFunction<String> resolver, boolean grouped)
    {
        out.append(indent).append("int jcap = 16; while (jcap * 0.75f < buildRows) { jcap <<= 1; }\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("long[] jKey").append(kx).append(" = new long[jcap];\n");
        }
        out.append(indent).append("int[] jRow = new int[jcap]; java.util.Arrays.fill(jRow, -1); int jMask = jcap - 1;\n");
        out.append(indent).append("for (int r = 0; r < buildRows; r++) {\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("  long bk").append(kx).append(" = b").append(buildKeys[kx]).append("[r];\n");
        }
        out.append(indent).append("  int slot = mix(").append(hashFold("bk", "", keyCount)).append(") & jMask;\n");
        out.append(indent).append("  while (jRow[slot] != -1) { slot = (slot + 1) & jMask; }\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("  jKey").append(kx).append("[slot] = bk").append(kx).append(";\n");
        }
        out.append(indent).append("  jRow[slot] = r;\n");
        out.append(indent).append("}\n");
        out.append(indent).append("for (int i = 0; i < probeRows; i++) {\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("  long pk").append(kx).append(" = p").append(probeKeys[kx]).append("[i];\n");
        }
        out.append(indent).append("  int js = mix(").append(hashFold("pk", "", keyCount)).append(") & jMask;\n");
        out.append(indent).append("  int buildRow = -1;\n");
        out.append(indent).append("  while (jRow[js] != -1) {\n");
        out.append(indent).append("    if (").append(joinKeyCompare(keyCount)).append(") { buildRow = jRow[js]; break; }\n");
        out.append(indent).append("    js = (js + 1) & jMask;\n");
        out.append(indent).append("  }\n");
        out.append(indent).append("  if (buildRow != -1) {\n");
        emitRowBody(out, indent + "    ", pipeline, resolver, grouped, false);
        out.append(indent).append("  }\n");
        out.append(indent).append("}\n");
    }

    // ---- shared per-row body: optional filter, then accumulate ----

    private static void emitRowBody(StringBuilder out, String indent, Plan.Pipeline pipeline, IntFunction<String> resolver, boolean grouped, boolean speculate)
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
            out.append(a2).append("if (!gused[ao]) { gused[ao] = true; arrayGroupCount++; }\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append(a2).append("aAgg").append(a).append("[ao] += ").append(increment(aggregates.get(a), resolver)).append(";\n");
            }
            out.append(a1).append("}\n");
            out.append(a1).append("else {\n");
            // Deopt: migrate every populated array group into the hash table, then handle this row via hash.
            out.append(a2).append("for (int mo = 0; mo < aSize; mo++) {\n");
            out.append(a2).append("  if (gused[mo]) {\n");
            String m = a2 + "    ";
            out.append(m).append("long hkey = mo + aMin;\n");
            emitSingleKeyHashFindOrCreate(out, m, aggregateCount);
            for (int a = 0; a < aggregateCount; a++) {
                out.append(m).append("agg").append(a).append("[gid] += aAgg").append(a).append("[mo];\n");
            }
            out.append(a2).append("  }\n");
            out.append(a2).append("}\n");
            out.append(a2).append("deopted = true;\n");
            out.append(a1).append("}\n");
            out.append(indent).append("}\n");
            out.append(indent).append("if (deopted) {\n");
            out.append(a1).append("long hkey = gkey;\n");
            emitSingleKeyHashFindOrCreate(out, a1, aggregateCount);
            for (int a = 0; a < aggregateCount; a++) {
                out.append(a1).append("agg").append(a).append("[gid] += ").append(increment(aggregates.get(a), resolver)).append(";\n");
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
            out.append(indent).append("agg").append(a).append("[gid] += ").append(increment(aggregates.get(a), resolver)).append(";\n");
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
    private static void emitSingleKeyHashFindOrCreate(StringBuilder out, String indent, int aggregateCount)
    {
        out.append(indent).append("int hslot = mix(hkey) & htMask;\n");
        out.append(indent).append("while (htGid[hslot] != -1 && htKey0[hslot] != hkey) { hslot = (hslot + 1) & htMask; }\n");
        out.append(indent).append("int gid = htGid[hslot];\n");
        out.append(indent).append("if (gid == -1) {\n");
        String b = indent + "  ";
        out.append(b).append("gid = groupCount++; htGid[hslot] = gid; htKey0[hslot] = hkey;\n");
        out.append(b).append("if (gid == keyByGid0.length) {\n");
        out.append(b).append("  int n = keyByGid0.length * 2; keyByGid0 = java.util.Arrays.copyOf(keyByGid0, n);\n");
        for (int a = 0; a < aggregateCount; a++) {
            out.append(b).append("  agg").append(a).append(" = java.util.Arrays.copyOf(agg").append(a).append(", n);\n");
        }
        out.append(b).append("}\n");
        out.append(b).append("keyByGid0[gid] = hkey;\n");
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

    /** Conjunction {@code jKey0[js] == pk0 && ...} comparing every stored build key component to the probe. */
    private static String joinKeyCompare(int keyCount)
    {
        StringBuilder compare = new StringBuilder();
        for (int kx = 0; kx < keyCount; kx++) {
            if (kx > 0) {
                compare.append(" && ");
            }
            compare.append("jKey").append(kx).append("[js] == pk").append(kx);
        }
        return compare.toString();
    }

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
