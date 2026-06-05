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
    // Null resolver for non-null contexts (group keys, join inputs): nothing is ever null.
    private static final IntFunction<String> NEVER_NULL = index -> "false";

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
        return compile(pipeline, encodings, null);
    }

    /**
     * Compile with declared per-scan-column nullability. {@code nullable[input][column]} true means the column
     * may carry SQL nulls (its {@link Column} supplies a {@code nulls} mask and the generated code is
     * null-aware); false (the default for any unlisted column) takes the branch-free null-free fast path.
     * Group keys and join keys are assumed non-null.
     */
    public static CompiledPipeline compile(Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable)
    {
        String simpleName = "Pipeline_" + COUNTER.incrementAndGet();
        String source = render(pipeline, encodings, nullable, simpleName);
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
        return render(pipeline, null, null, "Pipeline_preview");
    }

    public static String render(Plan.Pipeline pipeline, ColumnEncoding[][] encodings)
    {
        return render(pipeline, encodings, null, "Pipeline_preview");
    }

    public static String render(Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable)
    {
        return render(pipeline, encodings, nullable, "Pipeline_preview");
    }

    private static ColumnEncoding encodingOf(ColumnEncoding[][] encodings, int input, int column)
    {
        if (encodings == null || input >= encodings.length || encodings[input] == null
                || column >= encodings[input].length || encodings[input][column] == null) {
            return ColumnEncoding.FLAT;
        }
        return encodings[input][column];
    }

    private static boolean nullableOf(boolean[][] nullable, int input, int column)
    {
        return nullable != null && input < nullable.length && nullable[input] != null
                && column < nullable[input].length && nullable[input][column];
    }

    private static String render(Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, String simpleName)
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
            emitScanBody(out, pipeline, encodings, nullable);
        }
        out.append("  }\n");
        List<ColumnType> resultTypes = outputColumnTypes(pipeline);
        emitApplyHaving(out, pipeline.having(), resultTypes);
        emitApplyOrdering(out, pipeline.ordering(), resultTypes);
        out.append("}\n");
        return out.toString();
    }

    /** Logical types of the result columns: the group-key columns (LONG) then each aggregate's output type. */
    private static List<ColumnType> outputColumnTypes(Plan.Pipeline pipeline)
    {
        List<ColumnType> types = new ArrayList<>();
        for (int k = 0; k < pipeline.groupKeys().size(); k++) {
            types.add(ColumnType.LONG);
        }
        for (Plan.Aggregate aggregate : pipeline.aggregates()) {
            types.add(aggregator(aggregate).outputType());
        }
        return types;
    }

    /** Read a result column at {@code row}, decoding a DOUBLE column from its bits. */
    private static String resultColumnAccess(int column, List<ColumnType> types, String row)
    {
        String raw = "cols[" + column + "][" + row + "]";
        return types.get(column) == ColumnType.DOUBLE ? "Double.longBitsToDouble(" + raw + ")" : raw;
    }

    /**
     * Post-aggregation HAVING applied to the materialized result. Identity when no HAVING; otherwise keeps the
     * rows whose condition (over result columns: group keys then aggregates) holds and compacts.
     */
    private static void emitApplyHaving(StringBuilder out, Plan.Condition having, List<ColumnType> types)
    {
        out.append("  private static org.weakref.nitro.jit.CompiledPipeline.Result applyHaving(org.weakref.nitro.jit.CompiledPipeline.Result result) {\n");
        if (having == null) {
            out.append("    return result;\n  }\n");
            return;
        }
        out.append("    int n = result.rowCount(); long[][] cols = result.columns();\n");
        out.append("    int[] keep = new int[n]; int w = 0;\n");
        out.append("    for (int r = 0; r < n; r++) {\n");
        out.append("      if (").append(condition(having, index -> resultColumnAccess(index, types, "r"))).append(") { keep[w++] = r; }\n");
        out.append("    }\n");
        out.append("    long[][] kept = new long[cols.length][w];\n");
        out.append("    for (int i = 0; i < w; i++) { int s = keep[i]; for (int c = 0; c < cols.length; c++) { kept[c][i] = cols[c][s]; } }\n");
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(w, kept, result.types());\n");
        out.append("  }\n");
    }

    /**
     * Post-aggregation ORDER BY / LIMIT applied to the materialized result. Identity when no ordering; otherwise
     * sorts a row-index permutation by the sort keys and gathers (optionally truncated to the limit). A full
     * sort for now; a bounded top-N heap is the perf refinement.
     */
    private static void emitApplyOrdering(StringBuilder out, Plan.Ordering ordering, List<ColumnType> types)
    {
        out.append("  private static org.weakref.nitro.jit.CompiledPipeline.Result applyOrdering(org.weakref.nitro.jit.CompiledPipeline.Result result) {\n");
        if (ordering == null) {
            out.append("    return result;\n  }\n");
            return;
        }
        out.append("    int n = result.rowCount(); long[][] cols = result.columns();\n");
        out.append("    Integer[] order = new Integer[n];\n");
        out.append("    for (int i = 0; i < n; i++) { order[i] = i; }\n");
        out.append("    java.util.Arrays.sort(order, (a, b) -> {\n");
        out.append("      int c;\n");
        for (Plan.SortKey key : ordering.keys()) {
            String compare = types.get(key.column()) == ColumnType.DOUBLE
                    ? "Double.compare(" + resultColumnAccess(key.column(), types, "a") + ", " + resultColumnAccess(key.column(), types, "b") + ")"
                    : "Long.compare(cols[" + key.column() + "][a], cols[" + key.column() + "][b])";
            out.append("      c = ").append(compare).append(";");
            if (key.descending()) {
                out.append(" c = -c;");
            }
            out.append(" if (c != 0) { return c; }\n");
        }
        out.append("      return 0;\n");
        out.append("    });\n");
        String limit = ordering.limit() < 0 ? "n" : "Math.min(" + ordering.limit() + ", n)";
        out.append("    int outN = ").append(limit).append(";\n");
        out.append("    long[][] sorted = new long[cols.length][outN];\n");
        out.append("    for (int w = 0; w < outN; w++) { int s = order[w]; for (int c2 = 0; c2 < cols.length; c2++) { sorted[c2][w] = cols[c2][s]; } }\n");
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(outN, sorted, result.types());\n");
        out.append("  }\n");
    }

    // ---- single-input scan -> filter -> aggregate ----

    private static void emitScanBody(StringBuilder out, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable)
    {
        out.append("    org.weakref.nitro.jit.Column[] in = inputs[0]; int rowCount = rowCounts[0];\n");
        TreeSet<Integer> referenced = referencedColumns(pipeline);
        for (int column : referenced) {
            emitScanColumnLoad(out, column, encodingOf(encodings, 0, column), nullableOf(nullable, 0, column));
        }
        // Predicate-over-dictionary: evaluate each string filter once per dictionary entry into an id mask.
        List<Plan.StringMatch> stringMatches = new ArrayList<>();
        for (Plan.Condition filter : pipeline.filters()) {
            collectStringMatches(filter, stringMatches);
        }
        for (Plan.StringMatch match : stringMatches) {
            emitStringMaskPrelude(out, match);
        }
        IntFunction<String> resolver = index -> scanAccess(index, encodingOf(encodings, 0, index), "i");
        IntFunction<String> nullResolver = index -> nullAccess(index, encodingOf(encodings, 0, index), nullableOf(nullable, 0, index), "i");
        boolean grouped = !pipeline.groupKeys().isEmpty();
        // A single-key scan group can speculate array mode: estimate the key domain from a sample, bet on a
        // direct-indexed array, and deopt to a hash table if a later key falls outside the bet.
        boolean speculate = grouped && pipeline.groupKeys().size() == 1;

        // Group-on-id: when the single group key is a dictionary column, group on its dense id (so array mode
        // applies even when the dictionary's values are sparse) and reconstruct the value at finalize.
        int dictKeyColumn = -1;
        if (speculate && pipeline.groupKeys().getFirst() instanceof Plan.Col col
                && encodingOf(encodings, 0, col.index()) == ColumnEncoding.DICTIONARY) {
            dictKeyColumn = col.index();
        }
        int dictKey = dictKeyColumn;
        IntFunction<String> groupKeyResolver = index -> index == dictKey ? "cIds" + index + "[i]" : scanAccess(index, encodingOf(encodings, 0, index), "i");

        if (speculate) {
            IntFunction<String> sampleResolver = index -> index == dictKey ? "cIds" + index + "[s]" : scanAccess(index, encodingOf(encodings, 0, index), "s");
            emitGroupSampleProlog(out, pipeline.groupKeys().getFirst(), sampleResolver);
        }
        if (grouped) {
            emitGroupedState(out, pipeline, speculate);
        }
        else {
            emitGlobalState(out, pipeline.aggregates());
        }
        out.append("    for (int i = 0; i < rowCount; i++) {\n");
        emitRowBody(out, "      ", pipeline, resolver, groupKeyResolver, nullResolver, grouped, speculate);
        out.append("    }\n");
        if (grouped) {
            emitGroupedResult(out, pipeline, speculate, dictKeyColumn);
        }
        else {
            emitGlobalResult(out, pipeline.aggregates());
        }
    }

    /** Declare the local(s) for a scan column according to its encoding: flat values, dict ids + dictionary, or a constant; plus a null mask when nullable. */
    private static void emitScanColumnLoad(StringBuilder out, int column, ColumnEncoding encoding, boolean nullable)
    {
        switch (encoding) {
            case FLAT -> {
                out.append("    long[] c").append(column).append(" = ((org.weakref.nitro.jit.Column.FlatColumn) in[").append(column).append("]).values();\n");
                if (nullable) {
                    out.append("    boolean[] cN").append(column).append(" = ((org.weakref.nitro.jit.Column.FlatColumn) in[").append(column).append("]).nulls();\n");
                }
            }
            case DICTIONARY -> {
                out.append("    int[] cIds").append(column).append(" = ((org.weakref.nitro.jit.Column.DictionaryColumn) in[").append(column).append("]).ids();\n");
                out.append("    long[] cDict").append(column).append(" = ((org.weakref.nitro.jit.Column.DictionaryColumn) in[").append(column).append("]).dictionary();\n");
                if (nullable) {
                    out.append("    boolean[] cN").append(column).append(" = ((org.weakref.nitro.jit.Column.DictionaryColumn) in[").append(column).append("]).nulls();\n");
                }
            }
            case CONSTANT -> {
                out.append("    long cConst").append(column).append(" = ((org.weakref.nitro.jit.Column.ConstantColumn) in[").append(column).append("]).value();\n");
                if (nullable) {
                    out.append("    boolean cNconst").append(column).append(" = ((org.weakref.nitro.jit.Column.ConstantColumn) in[").append(column).append("]).isNull();\n");
                }
            }
            case STRING -> {
                out.append("    int[] cIds").append(column).append(" = ((org.weakref.nitro.jit.Column.StringColumn) in[").append(column).append("]).ids();\n");
                out.append("    byte[][] cStr").append(column).append(" = ((org.weakref.nitro.jit.Column.StringColumn) in[").append(column).append("]).dictionary();\n");
                if (nullable) {
                    out.append("    boolean[] cN").append(column).append(" = ((org.weakref.nitro.jit.Column.StringColumn) in[").append(column).append("]).nulls();\n");
                }
            }
        }
    }

    /** Access expression for a scan column at row {@code row}: flat index, dictionary indirection, hoisted constant, or string id. */
    private static String scanAccess(int column, ColumnEncoding encoding, String row)
    {
        return switch (encoding) {
            case FLAT -> "c" + column + "[" + row + "]";
            case DICTIONARY -> "cDict" + column + "[cIds" + column + "[" + row + "]]";
            case CONSTANT -> "cConst" + column;
            case STRING -> "cIds" + column + "[" + row + "]";   // the dense id is the value the loop works on
        };
    }

    /** Is-null expression for a scan column at row {@code row}; {@code "false"} (the fast path) when not nullable. */
    private static String nullAccess(int column, ColumnEncoding encoding, boolean nullable, String row)
    {
        if (!nullable) {
            return "false";
        }
        return switch (encoding) {
            case FLAT, DICTIONARY, STRING -> "cN" + column + "[" + row + "]";
            case CONSTANT -> "cNconst" + column;
        };
    }

    private static void collectStringMatches(Plan.Condition condition, List<Plan.StringMatch> into)
    {
        if (condition instanceof Plan.StringMatch match) {
            into.add(match);
        }
        else if (condition instanceof Plan.And and) {
            and.conditions().forEach(child -> collectStringMatches(child, into));
        }
        else if (condition instanceof Plan.Or or) {
            or.conditions().forEach(child -> collectStringMatches(child, into));
        }
        else if (condition instanceof Plan.Not not) {
            collectStringMatches(not.condition(), into);
        }
        // Plan.Predicate contains no string matches.
    }

    /** Build the id mask for a string filter once, by testing each dictionary entry against the literal set. */
    private static void emitStringMaskPrelude(StringBuilder out, Plan.StringMatch match)
    {
        int column = match.column();
        List<String> values = match.values();
        for (int v = 0; v < values.size(); v++) {
            out.append("    byte[] sLit").append(column).append("_").append(v).append(" = ")
                    .append(javaStringLiteral(values.get(v))).append(".getBytes(java.nio.charset.StandardCharsets.UTF_8);\n");
        }
        out.append("    boolean[] sMask").append(column).append(" = new boolean[cStr").append(column).append(".length];\n");
        out.append("    for (int e = 0; e < cStr").append(column).append(".length; e++) {\n");
        out.append("      byte[] sv = cStr").append(column).append("[e];\n");
        StringBuilder member = new StringBuilder();
        for (int v = 0; v < values.size(); v++) {
            member.append(member.length() == 0 ? "" : " || ").append("java.util.Arrays.equals(sv, sLit").append(column).append("_").append(v).append(")");
        }
        String matches = values.isEmpty() ? "false" : member.toString();
        out.append("      sMask").append(column).append("[e] = ").append(match.negated() ? "!(" + matches + ")" : "(" + matches + ")").append(";\n");
        out.append("    }\n");
    }

    /** Render a Java double-quoted string literal, escaping backslashes and quotes. */
    private static String javaStringLiteral(String value)
    {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
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
        // Join inputs are read as non-null (the null-free fast path), so the null resolver is always false.
        emitRowBody(out, indent, pipeline, resolver, resolver, index -> "false", grouped, false);
        for (int k = 0; k < joinCount; k++) {
            indent = indent.substring(2);
            out.append(indent).append("}\n");
        }
        out.append("    }\n");

        if (grouped) {
            emitGroupedResult(out, pipeline, false, -1);
        }
        else {
            emitGlobalResult(out, pipeline.aggregates());
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

    private static void emitRowBody(StringBuilder out, String indent, Plan.Pipeline pipeline, IntFunction<String> resolver, IntFunction<String> groupKeyResolver, IntFunction<String> nullResolver, boolean grouped, boolean speculate)
    {
        String bodyIndent = indent;
        if (!pipeline.filters().isEmpty()) {
            // A row passes WHERE only when the condition is TRUE (not FALSE, not NULL) -- three-valued logic.
            String condition = pipeline.filters().stream()
                    .map(c -> conditionTrue(c, resolver, nullResolver))
                    .collect(joining(" && "));
            out.append(indent).append("if (").append(condition).append(") {\n");
            bodyIndent = indent + "  ";
        }
        if (grouped) {
            emitGroupedAccumulate(out, bodyIndent, pipeline, resolver, groupKeyResolver, nullResolver, speculate);
        }
        else {
            emitGlobalAccumulate(out, bodyIndent, pipeline.aggregates(), resolver, nullResolver);
        }
        if (!pipeline.filters().isEmpty()) {
            out.append(indent).append("}\n");
        }
    }

    // ---- global aggregation ----

    private static void emitGlobalState(StringBuilder out, List<Plan.Aggregate> aggregates)
    {
        int total = cellCount(aggregates);
        for (int c = 0; c < total; c++) {
            out.append("    long a").append(c).append(" = 0L;\n");
        }
        for (int a = 0; a < aggregates.size(); a++) {
            aggregator(aggregates.get(a)).emitIdentity(out, "    ", cells(aggregates, a, "a", null));
        }
    }

    private static void emitGlobalAccumulate(StringBuilder out, String indent, List<Plan.Aggregate> aggregates, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        for (int a = 0; a < aggregates.size(); a++) {
            emitAggregateUpdate(out, indent, aggregates.get(a), cells(aggregates, a, "a", null), resolver, nullResolver);
        }
    }

    /** Fold one row into an aggregate's cells, skipping the row when the aggregate's input is null (so nulls are ignored). */
    private static void emitAggregateUpdate(StringBuilder out, String indent, Plan.Aggregate aggregate, List<String> cells, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        AggregateLibrary.AggregateCompiler aggregator = aggregator(aggregate);
        String inputExpr = input(aggregate, resolver, nullResolver);
        String guard = aggregate.input() == null ? "false" : nullExpr(aggregate.input(), resolver, nullResolver);
        if (guard.equals("false")) {
            aggregator.emitUpdate(out, indent, cells, inputExpr);
            return;
        }
        out.append(indent).append("if (!(").append(guard).append(")) {\n");
        aggregator.emitUpdate(out, indent + "  ", cells, inputExpr);
        out.append(indent).append("}\n");
    }

    private static void emitGlobalResult(StringBuilder out, List<Plan.Aggregate> aggregates)
    {
        int n = aggregates.size();
        out.append("    long[][] result = new long[").append(n).append("][];\n");
        for (int a = 0; a < n; a++) {
            out.append("    result[").append(a).append("] = new long[] { ").append(aggregator(aggregates.get(a)).result(cells(aggregates, a, "a", null))).append(" };\n");
        }
        emitResultTypes(out, "    ", 0, aggregates);
        out.append("    return applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(1, result, types)));\n");
    }

    // ---- grouped aggregation (single long key) ----

    private static void emitGroupedState(StringBuilder out, Plan.Pipeline pipeline, boolean speculate)
    {
        List<Plan.Aggregate> aggregates = pipeline.aggregates();
        int total = cellCount(aggregates);
        if (speculate) {
            // Array structures (sized from the sampled domain) plus a hash table that stays empty unless a key
            // falls outside the bet and we deopt into it. deopted starts true when we never speculated at all.
            out.append("    boolean[] gused = new boolean[aSize];\n");
            for (int c = 0; c < total; c++) {
                out.append("    long[] aAgg").append(c).append(" = new long[aSize];\n");
            }
            out.append("    int arrayGroupCount = 0; boolean deopted = !speculateArray;\n");
            emitSingleKeyHashState(out, aggregates);
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
        for (int c = 0; c < total; c++) {
            out.append("    long[] agg").append(c).append(" = new long[16];\n");
        }
    }

    private static void emitGroupedAccumulate(StringBuilder out, String indent, Plan.Pipeline pipeline, IntFunction<String> resolver, IntFunction<String> groupKeyResolver, IntFunction<String> nullResolver, boolean speculate)
    {
        List<Plan.Aggregate> aggregates = pipeline.aggregates();
        if (speculate) {
            int aggregateCount = aggregates.size();
            out.append(indent).append("long gkey = ").append(expr(pipeline.groupKeys().getFirst(), groupKeyResolver)).append(";\n");
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
                emitAggregateUpdate(out, a2, aggregates.get(a), cells(aggregates, a, "aAgg", "ao"), resolver, nullResolver);
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
                aggregator(aggregates.get(a)).emitMerge(out, m, cells(aggregates, a, "agg", "gid"), cells(aggregates, a, "aAgg", "mo"));
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
                emitAggregateUpdate(out, a1, aggregates.get(a), cells(aggregates, a, "agg", "gid"), resolver, nullResolver);
            }
            out.append(indent).append("}\n");
            return;
        }
        int keyCount = pipeline.groupKeys().size();
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("long gk").append(kx).append(" = ").append(expr(pipeline.groupKeys().get(kx), groupKeyResolver)).append(";\n");
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
        for (int c = 0; c < cellCount(aggregates); c++) {
            out.append(b).append("  agg").append(c).append(" = java.util.Arrays.copyOf(agg").append(c).append(", n);\n");
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
            emitAggregateUpdate(out, indent, aggregates.get(a), cells(aggregates, a, "agg", "gid"), resolver, nullResolver);
        }
    }

    private static void emitGroupedResult(StringBuilder out, Plan.Pipeline pipeline, boolean speculate, int reconstructDictColumn)
    {
        List<Plan.Aggregate> aggregates = pipeline.aggregates();
        int aggregateCount = aggregates.size();
        if (speculate) {
            // If the array bet held, compact it to its occupied offsets; otherwise emit the hash table we
            // deopted into. Both shapes are key column then aggregate columns; aggregates are finalized here.
            out.append("    if (!deopted) {\n");
            out.append("      long[][] result = new long[").append(1 + aggregateCount).append("][];\n");
            out.append("      long[] outKey = new long[arrayGroupCount];\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("      long[] outAgg").append(a).append(" = new long[arrayGroupCount];\n");
            }
            out.append("      int w = 0;\n");
            out.append("      for (int o = 0; o < aSize; o++) {\n");
            out.append("        if (gused[o]) {\n");
            out.append("          outKey[w] = ").append(reconstructKey(reconstructDictColumn, "o + aMin")).append(";\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("          outAgg").append(a).append("[w] = ").append(aggregator(aggregates.get(a)).result(cells(aggregates, a, "aAgg", "o"))).append(";\n");
            }
            out.append("          w++;\n");
            out.append("        }\n");
            out.append("      }\n");
            out.append("      result[0] = outKey;\n");
            for (int a = 0; a < aggregateCount; a++) {
                out.append("      result[").append(a + 1).append("] = outAgg").append(a).append(";\n");
            }
            emitResultTypes(out, "      ", 1, aggregates);
            out.append("      return applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(arrayGroupCount, result, types)));\n");
            out.append("    }\n");
            out.append("    long[][] result = new long[").append(1 + aggregateCount).append("][];\n");
            emitKeyResultColumn(out, "    ", 0, 0, reconstructDictColumn, "groupCount");
            emitAggregateResultColumns(out, "    ", 1, aggregates);
            emitResultTypes(out, "    ", 1, aggregates);
            out.append("    return applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(groupCount, result, types)));\n");
            return;
        }
        int keyCount = pipeline.groupKeys().size();
        out.append("    long[][] result = new long[").append(keyCount + aggregateCount).append("][];\n");
        for (int kx = 0; kx < keyCount; kx++) {
            emitKeyResultColumn(out, "    ", kx, kx, kx == 0 ? reconstructDictColumn : -1, "groupCount");
        }
        emitAggregateResultColumns(out, "    ", keyCount, aggregates);
        emitResultTypes(out, "    ", keyCount, aggregates);
        out.append("    return applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(groupCount, result, types)));\n");
    }

    /** Finalize each aggregate's cells into one result column (over {@code groupCount} groups in {@code agg<cell>}). */
    private static void emitAggregateResultColumns(StringBuilder out, String indent, int firstResultColumn, List<Plan.Aggregate> aggregates)
    {
        for (int a = 0; a < aggregates.size(); a++) {
            out.append(indent).append("long[] outAgg").append(a).append(" = new long[groupCount];\n");
            out.append(indent).append("for (int g = 0; g < groupCount; g++) { outAgg").append(a).append("[g] = ")
                    .append(aggregator(aggregates.get(a)).result(cells(aggregates, a, "agg", "g"))).append("; }\n");
            out.append(indent).append("result[").append(firstResultColumn + a).append("] = outAgg").append(a).append(";\n");
        }
    }

    /** Reconstruct a group key for output: identity for a plain key, or a dictionary lookup for a group-on-id key. */
    private static String reconstructKey(int dictColumn, String keyExpr)
    {
        return dictColumn < 0 ? "(" + keyExpr + ")" : "cDict" + dictColumn + "[(int) (" + keyExpr + ")]";
    }

    /** Emit one key result column, copying the stored keys or reconstructing dictionary values for a group-on-id key. */
    private static void emitKeyResultColumn(StringBuilder out, String indent, int resultIndex, int keyIndex, int reconstructDictColumn, String count)
    {
        if (reconstructDictColumn < 0) {
            out.append(indent).append("result[").append(resultIndex).append("] = java.util.Arrays.copyOf(keyByGid").append(keyIndex).append(", ").append(count).append(");\n");
            return;
        }
        out.append(indent).append("long[] outKey").append(keyIndex).append(" = new long[").append(count).append("];\n");
        out.append(indent).append("for (int g = 0; g < ").append(count).append("; g++) { outKey").append(keyIndex)
                .append("[g] = cDict").append(reconstructDictColumn).append("[(int) keyByGid").append(keyIndex).append("[g]]; }\n");
        out.append(indent).append("result[").append(resultIndex).append("] = outKey").append(keyIndex).append(";\n");
    }

    /** Single-key open-addressing grouping table (the deopt target for speculative array grouping). */
    private static void emitSingleKeyHashState(StringBuilder out, List<Plan.Aggregate> aggregates)
    {
        out.append("    int cap = 1024;\n");
        out.append("    long[] htKey0 = new long[cap];\n");
        out.append("    int[] htGid = new int[cap];\n");
        out.append("    java.util.Arrays.fill(htGid, -1);\n");
        out.append("    int htMask = cap - 1; int htFill = (int) (cap * 0.75f); int groupCount = 0;\n");
        out.append("    long[] keyByGid0 = new long[16];\n");
        for (int c = 0; c < cellCount(aggregates); c++) {
            out.append("    long[] agg").append(c).append(" = new long[16];\n");
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
        for (int c = 0; c < cellCount(aggregates); c++) {
            out.append(b).append("  agg").append(c).append(" = java.util.Arrays.copyOf(agg").append(c).append(", n);\n");
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
    private static String input(Plan.Aggregate aggregate, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        return aggregate.input() == null ? null : expr(aggregate.input(), resolver, nullResolver);
    }

    /** Total number of {@code long} state cells across all aggregates. */
    private static int cellCount(List<Plan.Aggregate> aggregates)
    {
        int total = 0;
        for (Plan.Aggregate aggregate : aggregates) {
            total += aggregator(aggregate).cells();
        }
        return total;
    }

    /** The flat cell index at which aggregate {@code index}'s cells begin. */
    private static int cellBase(List<Plan.Aggregate> aggregates, int index)
    {
        int base = 0;
        for (int a = 0; a < index; a++) {
            base += aggregator(aggregates.get(a)).cells();
        }
        return base;
    }

    /**
     * Lvalue strings for aggregate {@code index}'s state cells in storage named {@code prefix<cell>}. When
     * {@code element} is null the cells are scalars ({@code a3}); otherwise they are array elements
     * ({@code agg3[gid]}).
     */
    private static List<String> cells(List<Plan.Aggregate> aggregates, int index, String prefix, String element)
    {
        int base = cellBase(aggregates, index);
        int n = aggregator(aggregates.get(index)).cells();
        List<String> result = new ArrayList<>(n);
        for (int c = 0; c < n; c++) {
            String name = prefix + (base + c);
            result.add(element == null ? name : name + "[" + element + "]");
        }
        return result;
    }

    /** Emit {@code ColumnType[] types} for the result: {@code keyCount} LONG key columns then each aggregate's output type. */
    private static void emitResultTypes(StringBuilder out, String indent, int keyCount, List<Plan.Aggregate> aggregates)
    {
        StringBuilder elements = new StringBuilder();
        for (int k = 0; k < keyCount; k++) {
            elements.append(elements.length() == 0 ? "" : ", ").append("org.weakref.nitro.jit.ColumnType.LONG");
        }
        for (Plan.Aggregate aggregate : aggregates) {
            elements.append(elements.length() == 0 ? "" : ", ").append("org.weakref.nitro.jit.ColumnType.").append(aggregator(aggregate).outputType().name());
        }
        out.append(indent).append("org.weakref.nitro.jit.ColumnType[] types = new org.weakref.nitro.jit.ColumnType[] { ").append(elements).append(" };\n");
    }

    /** Identity assignment for every aggregate's state cells at array index {@code index} in storage named {@code prefix<cell>}. */
    private static void emitStateIdentity(StringBuilder out, String indent, List<Plan.Aggregate> aggregates, String prefix, String index)
    {
        for (int a = 0; a < aggregates.size(); a++) {
            aggregator(aggregates.get(a)).emitIdentity(out, indent, cells(aggregates, a, prefix, index));
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
        else if (expr instanceof Plan.Call call) {
            call.arguments().forEach(argument -> collectColumns(argument, into));
        }
        else if (expr instanceof Plan.Case kase) {
            for (Plan.Case.Branch branch : kase.branches()) {
                collectConditionColumns(branch.condition(), into);
                collectColumns(branch.value(), into);
            }
            collectColumns(kase.defaultValue(), into);
        }
        else if (expr instanceof Plan.Coalesce coalesce) {
            coalesce.arguments().forEach(argument -> collectColumns(argument, into));
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
            case Plan.StringMatch match -> into.add(match.column());
        }
    }

    /** Render a boolean condition tree as a Java expression. */
    /** Null-unaware boolean rendering (for contexts whose operands are non-null: HAVING result columns, CASE whens). */
    private static String condition(Plan.Condition condition, IntFunction<String> resolver)
    {
        return condition(condition, resolver, NEVER_NULL);
    }

    private static String condition(Plan.Condition condition, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        return switch (condition) {
            case Plan.Predicate predicate -> "(" + expr(predicate.left(), resolver, nullResolver) + " " + predicate.op() + " " + expr(predicate.right(), resolver, nullResolver) + ")";
            case Plan.And and -> and.conditions().isEmpty() ? "true"
                    : "(" + and.conditions().stream().map(child -> condition(child, resolver, nullResolver)).collect(joining(" && ")) + ")";
            case Plan.Or or -> or.conditions().isEmpty() ? "false"
                    : "(" + or.conditions().stream().map(child -> condition(child, resolver, nullResolver)).collect(joining(" || ")) + ")";
            case Plan.Not not -> "(!" + condition(not.condition(), resolver, nullResolver) + ")";
            case Plan.StringMatch ignored -> throw new UnsupportedOperationException("StringMatch is only supported in WHERE filters");
        };
    }

    private static String expr(Plan.Expr expr, IntFunction<String> resolver)
    {
        return expr(expr, resolver, NEVER_NULL);
    }

    private static String expr(Plan.Expr expr, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        return switch (expr) {
            case Plan.Col col -> resolver.apply(col.index());
            case Plan.Lit lit -> lit.value() + "L";
            case Plan.Bin bin -> ScalarLibrary.get(bin.op()).emit(List.of(expr(bin.left(), resolver, nullResolver), expr(bin.right(), resolver, nullResolver)));
            case Plan.Call call -> ScalarLibrary.get(call.name()).emit(call.arguments().stream().map(argument -> expr(argument, resolver, nullResolver)).toList());
            case Plan.Case kase -> caseExpression(kase, resolver, nullResolver);
            case Plan.Coalesce coalesce -> coalesceExpression(coalesce, resolver, nullResolver);
        };
    }

    /** Render a CASE as a right-nested conditional, falling through to the default value. */
    private static String caseExpression(Plan.Case kase, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        StringBuilder out = new StringBuilder();
        for (Plan.Case.Branch branch : kase.branches()) {
            out.append("(").append(condition(branch.condition(), resolver, nullResolver)).append(" ? ").append(expr(branch.value(), resolver, nullResolver)).append(" : ");
        }
        out.append(expr(kase.defaultValue(), resolver, nullResolver));
        out.append(")".repeat(kase.branches().size()));
        return out.toString();
    }

    /** Render COALESCE as a right-nested conditional returning the first non-null argument. */
    private static String coalesceExpression(Plan.Coalesce coalesce, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        List<Plan.Expr> arguments = coalesce.arguments();
        // The fallback is the first argument that is never null (it always wins if reached), or the last one.
        int fallback = arguments.size() - 1;
        for (int a = 0; a < arguments.size(); a++) {
            if (nullExpr(arguments.get(a), resolver, nullResolver).equals("false")) {
                fallback = a;
                break;
            }
        }
        StringBuilder out = new StringBuilder();
        for (int a = 0; a < fallback; a++) {
            out.append("(").append(nullExpr(arguments.get(a), resolver, nullResolver)).append(" ? ");
        }
        out.append(expr(arguments.get(fallback), resolver, nullResolver));
        for (int a = fallback - 1; a >= 0; a--) {
            out.append(" : ").append(expr(arguments.get(a), resolver, nullResolver)).append(")");
        }
        return out.toString();
    }

    // ---- three-valued (null-aware) expression and condition rendering ----

    /** Boolean expression that is true when {@code expr} evaluates to SQL null. {@code "false"} on the fast path. */
    private static String nullExpr(Plan.Expr expr, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        return switch (expr) {
            case Plan.Col col -> nullResolver.apply(col.index());
            case Plan.Lit ignored -> "false";
            case Plan.Bin bin -> orNull(nullExpr(bin.left(), resolver, nullResolver), nullExpr(bin.right(), resolver, nullResolver));
            case Plan.Call call -> {
                String nulls = "false";
                for (Plan.Expr argument : call.arguments()) {
                    nulls = orNull(nulls, nullExpr(argument, resolver, nullResolver));
                }
                yield nulls;
            }
            case Plan.Case kase -> caseNull(kase, resolver, nullResolver);
            case Plan.Coalesce coalesce -> {
                // COALESCE is null only when every argument is null.
                String allNull = "true";
                for (Plan.Expr argument : coalesce.arguments()) {
                    allNull = andNull(allNull, nullExpr(argument, resolver, nullResolver));
                }
                yield allNull;
            }
        };
    }

    private static String andNull(String left, String right)
    {
        if (left.equals("false") || right.equals("false")) {
            return "false";
        }
        if (left.equals("true")) {
            return right;
        }
        if (right.equals("true")) {
            return left;
        }
        return "(" + left + " && " + right + ")";
    }

    /** Null-ness of a CASE: the null-ness of whichever branch value is selected (when-conditions assumed non-null). */
    private static String caseNull(Plan.Case kase, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        boolean anyNull = !nullExpr(kase.defaultValue(), resolver, nullResolver).equals("false");
        for (Plan.Case.Branch branch : kase.branches()) {
            anyNull |= !nullExpr(branch.value(), resolver, nullResolver).equals("false");
        }
        if (!anyNull) {
            return "false";
        }
        StringBuilder out = new StringBuilder();
        for (Plan.Case.Branch branch : kase.branches()) {
            out.append("(").append(condition(branch.condition(), resolver)).append(" ? ").append(nullExpr(branch.value(), resolver, nullResolver)).append(" : ");
        }
        out.append(nullExpr(kase.defaultValue(), resolver, nullResolver));
        out.append(")".repeat(kase.branches().size()));
        return out.toString();
    }

    private static String orNull(String left, String right)
    {
        if (left.equals("false")) {
            return right;
        }
        if (right.equals("false")) {
            return left;
        }
        return "(" + left + " || " + right + ")";
    }

    /** {@code !(nullExpr)}, or empty when provably non-null (drops the guard on the fast path). */
    private static String notNullGuard(String nullExpression)
    {
        return nullExpression.equals("false") ? "" : "!(" + nullExpression + ")";
    }

    private static String andGuards(String left, String right)
    {
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        return left + " && " + right;
    }

    /** Boolean expression that is true when {@code condition} evaluates to SQL TRUE (three-valued logic). */
    private static String conditionTrue(Plan.Condition condition, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        switch (condition) {
            case Plan.Predicate predicate -> {
                String guard = andGuards(
                        notNullGuard(nullExpr(predicate.left(), resolver, nullResolver)),
                        notNullGuard(nullExpr(predicate.right(), resolver, nullResolver)));
                String comparison = "(" + expr(predicate.left(), resolver, nullResolver) + " " + predicate.op() + " " + expr(predicate.right(), resolver, nullResolver) + ")";
                return guard.isEmpty() ? comparison : "(" + guard + " && " + comparison + ")";
            }
            case Plan.And and -> {
                if (and.conditions().isEmpty()) {
                    return "true";
                }
                return "(" + and.conditions().stream().map(child -> conditionTrue(child, resolver, nullResolver)).collect(joining(" && ")) + ")";
            }
            case Plan.Or or -> {
                if (or.conditions().isEmpty()) {
                    return "false";
                }
                return "(" + or.conditions().stream().map(child -> conditionTrue(child, resolver, nullResolver)).collect(joining(" || ")) + ")";
            }
            case Plan.Not not -> {
                return conditionFalse(not.condition(), resolver, nullResolver);
            }
            case Plan.StringMatch match -> {
                String guard = notNullGuard(nullResolver.apply(match.column()));
                String lookup = "sMask" + match.column() + "[" + resolver.apply(match.column()) + "]";
                return guard.isEmpty() ? lookup : "(" + guard + " && " + lookup + ")";
            }
        }
    }

    /** Boolean expression that is true when {@code condition} evaluates to SQL FALSE (three-valued logic). */
    private static String conditionFalse(Plan.Condition condition, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        switch (condition) {
            case Plan.Predicate predicate -> {
                String guard = andGuards(
                        notNullGuard(nullExpr(predicate.left(), resolver, nullResolver)),
                        notNullGuard(nullExpr(predicate.right(), resolver, nullResolver)));
                String negated = "!(" + expr(predicate.left(), resolver, nullResolver) + " " + predicate.op() + " " + expr(predicate.right(), resolver, nullResolver) + ")";
                return guard.isEmpty() ? negated : "(" + guard + " && " + negated + ")";
            }
            case Plan.And and -> {
                if (and.conditions().isEmpty()) {
                    return "false";
                }
                return "(" + and.conditions().stream().map(child -> conditionFalse(child, resolver, nullResolver)).collect(joining(" || ")) + ")";
            }
            case Plan.Or or -> {
                if (or.conditions().isEmpty()) {
                    return "true";
                }
                return "(" + or.conditions().stream().map(child -> conditionFalse(child, resolver, nullResolver)).collect(joining(" && ")) + ")";
            }
            case Plan.Not not -> {
                return conditionTrue(not.condition(), resolver, nullResolver);
            }
            case Plan.StringMatch match -> {
                String guard = notNullGuard(nullResolver.apply(match.column()));
                String lookup = "!sMask" + match.column() + "[" + resolver.apply(match.column()) + "]";
                return guard.isEmpty() ? lookup : "(" + guard + " && " + lookup + ")";
            }
        }
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
