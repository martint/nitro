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

import java.util.ArrayList;
import java.util.IdentityHashMap;
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
    // Compiled pipelines are cached by their generated source (a query engine compiles a plan once and executes it
    // many times). The key renders with a fixed class name so two structurally identical pipelines -- e.g. a subtree
    // assembled twice in a multi-stage query -- share one compiled, stateless class. A miss mints a uniquely-named
    // class and compiles it.
    private static final String CACHE_NAME = "Cached";
    private static final java.util.concurrent.ConcurrentHashMap<String, Class<?>> CLASS_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    private static Class<?> cachedClass(String cacheKey, String prefix, java.util.function.Function<String, String> renderWithName)
    {
        return CLASS_CACHE.computeIfAbsent(cacheKey, key -> {
            String simpleName = prefix + COUNTER.incrementAndGet();
            return InMemoryCompiler.compile(PACKAGE + "." + simpleName, renderWithName.apply(simpleName));
        });
    }

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
        Class<?> compiled = cachedClass("P|" + render(pipeline, encodings, nullable, CACHE_NAME), "Pipeline_",
                name -> render(pipeline, encodings, nullable, name));
        try {
            return (CompiledPipeline) compiled.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate compiled pipeline", e);
        }
    }

    /**
     * Compile {@code pipeline} as a {@link StreamingPipeline}: the probe / scanned input is consumed
     * batch-by-batch, folding into grouping/aggregation state that persists across batches; join build sides are
     * materialized once into hash tables before the probe streams. Array-mode group speculation is off (the
     * domain is not known up front), so grouping uses the hash path. Probe-side string group keys would need a
     * dictionary consistent across batches (a later step); build-side string keys (the usual dimension case)
     * stream correctly since their dictionary is materialized once.
     */
    public static StreamingPipeline compileStreaming(Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable)
    {
        Class<?> compiled = cachedClass("S|" + renderStreaming(pipeline, encodings, nullable, CACHE_NAME), "Streaming_",
                name -> renderStreaming(pipeline, encodings, nullable, name));
        try {
            return (StreamingPipeline) compiled.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate compiled streaming pipeline", e);
        }
    }

    /** Exposed for inspection/tests: the streaming Java source that would be compiled. */
    public static String renderStreaming(Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable)
    {
        return renderStreaming(pipeline, encodings, nullable, "Streaming_preview");
    }

    private static String renderStreaming(Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, String simpleName)
    {
        StringBuilder out = new StringBuilder();
        out.append("package ").append(PACKAGE).append(";\n");
        out.append("public final class ").append(simpleName)
                .append(" implements org.weakref.nitro.jit.StreamingPipeline {\n");
        boolean needsMix = !pipeline.groupKeys().isEmpty() || !pipeline.joins().isEmpty();
        if (needsMix) {
            emitMix(out);
        }
        List<Type> resultTypes = outputColumnTypes(pipeline, encodings);
        out.append("  @Override public org.weakref.nitro.jit.CompiledPipeline.Result execute("
                + "org.weakref.nitro.jit.StreamingPipeline.Source source, org.weakref.nitro.jit.Column[][] builds, int[] buildRowCounts) {\n");

        if (!pipeline.joins().isEmpty()) {
            emitJoinBody(out, pipeline, encodings, nullable, resultTypes, true);
            out.append("  }\n");
            emitApplyHaving(out, pipeline.having(), resultTypes);
            emitApplyOrdering(out, pipeline.ordering(), resultTypes);
            emitApplyProjection(out, pipeline.projections(), resultTypes);
            out.append("}\n");
            return out.toString();
        }

        boolean grouped = !pipeline.groupKeys().isEmpty();
        // State lives across batches: initialize it once, before the batch loop.
        if (grouped) {
            emitGroupedState(out, pipeline, nullable, false);
        }
        else {
            emitGlobalState(out, pipeline.aggregates());
        }

        IntFunction<String> resolver = index -> scanAccess(index, encodingOf(encodings, 0, index), "i");
        IntFunction<String> nullResolver = index -> nullAccess(index, encodingOf(encodings, 0, index), nullableOf(nullable, 0, index), "i");
        List<Plan.Condition> stringMatches = collectPipelineStringMatches(pipeline);
        Map<Plan.Condition, Integer> stringMaskIds = new IdentityHashMap<>();
        for (int s = 0; s < stringMatches.size(); s++) {
            stringMaskIds.put(stringMatches.get(s), s);
        }

        // Selection-driven lazy materialization with staged (per-conjunct) filtering: rather than decode every
        // filter column up front, treat each top-level conjunct as a narrowing stage -- materialize only that
        // conjunct's column(s) (for the rows still alive), evaluate it, and shrink the selection -- so a later
        // conjunct's column is converted only for the rows the earlier conjuncts kept. Finally materialize the
        // payload columns (group keys, measures) for the survivors and fold them in. Conjuncts are reordered
        // most-selective-first by a static heuristic (AND is commutative, so this is safe) to shrink the selection
        // as early as possible. No filter -> every row survives, so a single eager pass is cheaper.
        if (!pipeline.filters().isEmpty()) {
            List<Plan.Condition> conjuncts = orderBySelectivity(pipeline.filters());
            out.append("    while (source.advance()) {\n");
            out.append("      int rowCount = source.rows();\n");
            out.append("      int[] selection = new int[rowCount]; int selected = rowCount;\n");
            out.append("      for (int i = 0; i < rowCount; i++) { selection[i] = i; }\n");
            for (Plan.Condition conjunct : conjuncts) {
                TreeSet<Integer> columns = new TreeSet<>();
                collectConditionColumns(conjunct, columns);
                List<Plan.Condition> matches = new ArrayList<>();
                collectStringMatches(conjunct, matches);
                out.append("      {\n");
                out.append("        org.weakref.nitro.jit.Column[] in = source.materialize(").append(intArrayLiteral(columns)).append(", selection, selected);\n");
                for (int column : columns) {
                    emitScanColumnLoad(out, column, encodingOf(encodings, 0, column), nullableOf(nullable, 0, column));
                }
                for (Plan.Condition match : matches) {
                    emitStringMaskPrelude(out, match, stringMaskIds.get(match), "cStr" + stringMatchColumn(match));
                }
                out.append("        int kept = 0;\n");
                out.append("        for (int i = 0; i < selected; i++) {\n");
                out.append("          if (").append(conditionTrue(conjunct, resolver, nullResolver, stringMaskIds)).append(") { selection[kept++] = selection[i]; }\n");
                out.append("        }\n");
                out.append("        selected = kept;\n");
                out.append("      }\n");
            }
            // Payload stage: materialize the columns the aggregation reads (group keys, measures) for the survivors.
            TreeSet<Integer> payloadColumns = accumulateColumns(pipeline);
            List<Plan.Condition> aggregateMatches = new ArrayList<>();
            for (Plan.Aggregate aggregate : pipeline.aggregates()) {
                if (aggregate.input() != null) {
                    collectStringMatchesInExpr(aggregate.input(), aggregateMatches);
                }
            }
            out.append("      {\n");
            out.append("        org.weakref.nitro.jit.Column[] in = source.materialize(").append(intArrayLiteral(payloadColumns)).append(", selection, selected);\n");
            for (int column : payloadColumns) {
                emitScanColumnLoad(out, column, encodingOf(encodings, 0, column), nullableOf(nullable, 0, column));
            }
            for (Plan.Condition match : aggregateMatches) {
                emitStringMaskPrelude(out, match, stringMaskIds.get(match), "cStr" + stringMatchColumn(match));
            }
            out.append("        for (int i = 0; i < selected; i++) {\n");
            if (grouped) {
                emitGroupedAccumulate(out, "          ", pipeline, nullable, resolver, resolver, nullResolver, stringMaskIds, false);
            }
            else {
                emitGlobalAccumulate(out, "          ", pipeline.aggregates(), resolver, nullResolver, stringMaskIds);
            }
            out.append("        }\n");
            out.append("      }\n");
            out.append("    }\n");
        }
        else {
            out.append("    while (source.advance()) {\n");
            out.append("      int rowCount = source.rows();\n");
            out.append("      org.weakref.nitro.jit.Column[] in = source.columns();\n");
            for (int column : referencedColumns(pipeline)) {
                emitScanColumnLoad(out, column, encodingOf(encodings, 0, column), nullableOf(nullable, 0, column));
            }
            for (Plan.Condition match : stringMatches) {
                emitStringMaskPrelude(out, match, stringMaskIds.get(match), "cStr" + stringMatchColumn(match));
            }
            out.append("      for (int i = 0; i < rowCount; i++) {\n");
            emitRowBody(out, "        ", pipeline, encodings, nullable, resolver, resolver, nullResolver, stringMaskIds, grouped, false);
            out.append("      }\n");
            out.append("    }\n");
        }

        if (grouped) {
            emitGroupedResult(out, pipeline, nullable, false, -1, resultTypes);
        }
        else {
            emitGlobalResult(out, pipeline.aggregates(), resultTypes);
        }
        out.append("  }\n");
        emitApplyHaving(out, pipeline.having(), resultTypes);
        emitApplyOrdering(out, pipeline.ordering(), resultTypes);
        emitApplyProjection(out, pipeline.projections(), resultTypes);
        out.append("}\n");
        return out.toString();
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

    /**
     * Encoding of a column addressed in the combined space (probe columns first, then each build's columns).
     * Resolves the combined index back to its (input, local) coordinates so build-side columns keep their own
     * encoding.
     */
    private static ColumnEncoding combinedEncoding(Plan.Pipeline pipeline, ColumnEncoding[][] encodings, int combined)
    {
        int probeColumns = pipeline.columnCount();
        if (combined < probeColumns) {
            return encodingOf(encodings, 0, combined);
        }
        int offset = probeColumns;
        List<Plan.Join> joins = pipeline.joins();
        for (int k = 0; k < joins.size(); k++) {
            int count = joins.get(k).build().columnCount();
            if (combined < offset + count) {
                return encodingOf(encodings, k + 1, combined - offset);
            }
            offset += count;
        }
        return ColumnEncoding.FLAT;
    }

    /** Whether a combined-space column is nullable; resolves the index back to its (input, local) coordinates. */
    private static boolean combinedNullable(Plan.Pipeline pipeline, boolean[][] nullable, int combined)
    {
        int probeColumns = pipeline.columnCount();
        if (combined < probeColumns) {
            return nullableOf(nullable, 0, combined);
        }
        int offset = probeColumns;
        List<Plan.Join> joins = pipeline.joins();
        for (int k = 0; k < joins.size(); k++) {
            int count = joins.get(k).build().columnCount();
            if (combined < offset + count) {
                return nullableOf(nullable, k + 1, combined - offset);
            }
            offset += count;
        }
        return false;
    }

    /** Whether group key {@code kx} is a nullable column (so the grouping must treat null as its own group). */
    private static boolean keyNullable(Plan.Pipeline pipeline, boolean[][] nullable, int kx)
    {
        Plan.Expr key = pipeline.groupKeys().get(kx);
        return key instanceof Plan.Col col && combinedNullable(pipeline, nullable, col.index());
    }

    private static boolean anyGroupKeyNullable(Plan.Pipeline pipeline, boolean[][] nullable)
    {
        for (int kx = 0; kx < pipeline.groupKeys().size(); kx++) {
            if (keyNullable(pipeline, nullable, kx)) {
                return true;
            }
        }
        return false;
    }

    /** Whether any aggregate can finalize to SQL NULL (e.g. an average over zero non-null inputs). */
    private static boolean anyAggregateNullable(Plan.Pipeline pipeline)
    {
        List<Plan.Aggregate> aggregates = pipeline.aggregates();
        for (int a = 0; a < aggregates.size(); a++) {
            if (aggregator(aggregates.get(a)).resultNull(cells(aggregates, a, "c", "i")) != null) {
                return true;
            }
        }
        return false;
    }

    /** The candidate generated-variable names for a join column, selected by encoding (flat array, dictionary ids/values, string dictionary, constant) plus its null mask. */
    private record ColumnVars(String flat, String ids, String dict, String stringDict, String constant, String nulls) {}

    private static ColumnVars probeVars(int column)
    {
        return new ColumnVars("p" + column, "pIds" + column, "pDict" + column, "pStr" + column, "pConst" + column, "pN" + column);
    }

    private static ColumnVars buildVars(int build, int local)
    {
        String suffix = build + "_" + local;
        return new ColumnVars("b" + suffix, "bIds" + suffix, "bDict" + suffix, "bStr" + suffix, "bConst" + suffix, "bN" + suffix);
    }

    /** Access expression for a join column at {@code row}: flat index, dictionary indirection, hoisted constant, or string id. */
    private static String joinAccess(ColumnEncoding encoding, ColumnVars vars, String row)
    {
        return switch (encoding) {
            case FLAT -> vars.flat() + "[" + row + "]";
            case STRING -> vars.ids() + "[" + row + "]";   // the dense id is the value the loop works on
            case DICTIONARY -> vars.dict() + "[" + vars.ids() + "[" + row + "]]";
            case CONSTANT -> vars.constant();
        };
    }

    /** Load a join column from its input array into the encoding-appropriate generated variable(s), with its null mask when nullable. */
    private static void emitJoinColumnLoad(StringBuilder out, ColumnEncoding encoding, boolean nullable, String source, ColumnVars vars)
    {
        String type = "org.weakref.nitro.jit.Column.";
        String columnType = switch (encoding) {
            case FLAT -> "FlatColumn";
            case STRING -> "StringColumn";
            case DICTIONARY -> "DictionaryColumn";
            case CONSTANT -> "ConstantColumn";
        };
        switch (encoding) {
            case FLAT -> out.append("    long[] ").append(vars.flat()).append(" = ((").append(type).append("FlatColumn) ").append(source).append(").values();\n");
            case STRING -> {
                out.append("    int[] ").append(vars.ids()).append(" = ((").append(type).append("StringColumn) ").append(source).append(").ids();\n");
                // The dictionary is needed for string filters (predicate-over-dictionary); a cheap array reference.
                out.append("    byte[][] ").append(vars.stringDict()).append(" = ((").append(type).append("StringColumn) ").append(source).append(").dictionary();\n");
            }
            case DICTIONARY -> {
                out.append("    int[] ").append(vars.ids()).append(" = ((").append(type).append("DictionaryColumn) ").append(source).append(").ids();\n");
                out.append("    long[] ").append(vars.dict()).append(" = ((").append(type).append("DictionaryColumn) ").append(source).append(").dictionary();\n");
            }
            case CONSTANT -> out.append("    long ").append(vars.constant()).append(" = ((").append(type).append("ConstantColumn) ").append(source).append(").value();\n");
        }
        if (nullable) {
            if (encoding == ColumnEncoding.CONSTANT) {
                out.append("    boolean ").append(vars.nulls()).append(" = ((").append(type).append("ConstantColumn) ").append(source).append(").isNull();\n");
            }
            else {
                out.append("    boolean[] ").append(vars.nulls()).append(" = ((").append(type).append(columnType).append(") ").append(source).append(").nulls();\n");
            }
        }
    }

    /** Is-null expression for a join column at {@code row}; {@code "false"} (fast path) when not nullable. */
    private static String joinNullAccess(ColumnEncoding encoding, boolean nullable, ColumnVars vars, String row)
    {
        if (!nullable) {
            return "false";
        }
        return encoding == ColumnEncoding.CONSTANT ? vars.nulls() : vars.nulls() + "[" + row + "]";
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
        boolean projectionOnly = projectionOnly(pipeline);
        List<Type> resultTypes = projectionOnly ? projectionOutputTypes(pipeline, encodings) : outputColumnTypes(pipeline, encodings);
        out.append("  @Override public org.weakref.nitro.jit.CompiledPipeline.Result execute(org.weakref.nitro.jit.Column[][] inputs, int[] rowCounts) {\n");
        if (!pipeline.joins().isEmpty()) {
            emitJoinBody(out, pipeline, encodings, nullable, resultTypes);
        }
        else {
            emitScanBody(out, pipeline, encodings, nullable, resultTypes);
        }
        out.append("  }\n");
        emitApplyHaving(out, pipeline.having(), resultTypes);
        emitApplyOrdering(out, pipeline.ordering(), resultTypes);
        // A projection-only pipeline applies its projections inline (they define the output), so there is no
        // separate post-aggregation projection step.
        emitApplyProjection(out, projectionOnly ? List.of() : pipeline.projections(), resultTypes);
        out.append("}\n");
        return out.toString();
    }

    /**
     * Logical types of the result columns: each group-key column (STRING when it is a string-encoded column,
     * else LONG) then each aggregate's output type.
     */
    private static List<Type> outputColumnTypes(Plan.Pipeline pipeline, ColumnEncoding[][] encodings)
    {
        List<Type> types = new ArrayList<>();
        for (Plan.Expr groupKey : pipeline.groupKeys()) {
            boolean string = groupKey instanceof Plan.Col col && combinedEncoding(pipeline, encodings, col.index()) == ColumnEncoding.STRING;
            types.add(string ? Types.STRING : Types.LONG);
        }
        for (Plan.Aggregate aggregate : pipeline.aggregates()) {
            types.add(aggregator(aggregate).outputType());
        }
        return types;
    }

    /** Read a result column at {@code row}, decoding a DOUBLE column from its bits. */
    private static String resultColumnAccess(int column, List<Type> types, String row)
    {
        return types.get(column).decode("cols[" + column + "][" + row + "]");
    }

    /**
     * Post-aggregation HAVING applied to the materialized result. Identity when no HAVING; otherwise keeps the
     * rows whose condition (over result columns: group keys then aggregates) holds and compacts.
     */
    private static void emitApplyHaving(StringBuilder out, Plan.Condition having, List<Type> types)
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
        out.append("    int outN = w;\n");
        emitGatherNulls(out, "keep[g]");
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(w, kept, result.types(), outNulls);\n");
        out.append("  }\n");
    }

    /**
     * Post-aggregation ORDER BY / LIMIT applied to the materialized result. Identity when no ordering; otherwise
     * sorts a row-index permutation by the sort keys and gathers (optionally truncated to the limit). A full
     * sort for now; a bounded top-N heap is the perf refinement.
     */
    private static void emitApplyOrdering(StringBuilder out, Plan.Ordering ordering, List<Type> types)
    {
        out.append("  private static org.weakref.nitro.jit.CompiledPipeline.Result applyOrdering(org.weakref.nitro.jit.CompiledPipeline.Result result) {\n");
        if (ordering == null) {
            out.append("    return result;\n  }\n");
            return;
        }
        out.append("    int n = result.rowCount(); long[][] cols = result.columns(); boolean[][] on = result.nulls();\n");
        out.append("    Integer[] order = new Integer[n];\n");
        out.append("    for (int i = 0; i < n; i++) { order[i] = i; }\n");
        out.append("    java.util.Arrays.sort(order, (a, b) -> {\n");
        out.append("      int c;\n");
        for (Plan.SortKey key : ordering.keys()) {
            int col = key.column();
            String compare = types.get(col).compare("cols[" + col + "][a]", "cols[" + col + "][b]");
            // Null ordering mirrors the operator path (OperatorOrderingSemantics): a null compares as greater than
            // any value (so ascending puts nulls last), and the descending flip then yields nulls first for DESC.
            out.append("      { boolean an = on != null && on[").append(col).append("] != null && on[").append(col).append("][a];")
                    .append(" boolean bn = on != null && on[").append(col).append("] != null && on[").append(col).append("][b];\n");
            out.append("        if (an || bn) { c = (an == bn) ? 0 : (an ? 1 : -1); } else { c = ").append(compare).append("; }");
            if (key.descending()) {
                out.append(" c = -c;");
            }
            out.append(" if (c != 0) { return c; } }\n");
        }
        out.append("      return 0;\n");
        out.append("    });\n");
        String limit = ordering.limit() < 0 ? "n" : "Math.min(" + ordering.limit() + ", n)";
        out.append("    int outN = ").append(limit).append(";\n");
        out.append("    long[][] sorted = new long[cols.length][outN];\n");
        out.append("    for (int w = 0; w < outN; w++) { int s = order[w]; for (int c2 = 0; c2 < cols.length; c2++) { sorted[c2][w] = cols[c2][s]; } }\n");
        emitGatherNulls(out, "order[g]");
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(outN, sorted, result.types(), outNulls);\n");
        out.append("  }\n");
    }

    /** Gather the result's per-column null masks through a row permutation ({@code sourceRow}, in terms of output index {@code g}, maps to the source row). Leaves {@code boolean[][] outNulls} (length {@code outN}) in scope. */
    private static void emitGatherNulls(StringBuilder out, String sourceRow)
    {
        out.append("    boolean[][] srcNulls = result.nulls();\n");
        out.append("    boolean[][] outNulls = null;\n");
        out.append("    if (srcNulls != null) {\n");
        out.append("      outNulls = new boolean[cols.length][];\n");
        out.append("      for (int c = 0; c < cols.length; c++) {\n");
        out.append("        if (srcNulls[c] != null) {\n");
        out.append("          outNulls[c] = new boolean[outN];\n");
        out.append("          for (int g = 0; g < outN; g++) { outNulls[c][g] = srcNulls[c][").append(sourceRow).append("]; }\n");
        out.append("        }\n");
        out.append("      }\n");
        out.append("    }\n");
    }

    /**
     * Final SELECT projection over the result columns (group keys then aggregates), applied after HAVING and
     * ORDER BY / LIMIT. Identity when there are no projections; otherwise each projection -- a column reference
     * (select / reorder) or a computation over the columns -- becomes one output column. Input columns are
     * decoded by their type, the expression evaluated, and the result re-encoded into its slot. A plain column
     * reference carries the source column's null mask through; computed projections are produced non-null.
     */
    private static void emitApplyProjection(StringBuilder out, List<Plan.Expr> projections, List<Type> inputTypes)
    {
        out.append("  private static org.weakref.nitro.jit.CompiledPipeline.Result applyProjection(org.weakref.nitro.jit.CompiledPipeline.Result result) {\n");
        if (projections.isEmpty()) {
            out.append("    return result;\n  }\n");
            return;
        }
        int outCount = projections.size();
        out.append("    int n = result.rowCount(); long[][] cols = result.columns(); boolean[][] inNulls = result.nulls();\n");
        out.append("    long[][] proj = new long[").append(outCount).append("][n];\n");
        out.append("    boolean[][] projNulls = null;\n");
        IntFunction<String> decode = i -> inputTypes.get(i).decode("cols[" + i + "][r]");
        List<Type> outputTypes = new ArrayList<>();
        for (int p = 0; p < outCount; p++) {
            Plan.Expr projection = projections.get(p);
            Type type = projectionType(projection, inputTypes);
            outputTypes.add(type);
            out.append("    for (int r = 0; r < n; r++) { proj[").append(p).append("][r] = ")
                    .append(encodeSlot(type, expr(projection, decode))).append("; }\n");
            if (projection instanceof Plan.Col col) {
                out.append("    if (inNulls != null && inNulls[").append(col.index()).append("] != null) { if (projNulls == null) { projNulls = new boolean[")
                        .append(outCount).append("][]; } projNulls[").append(p).append("] = inNulls[").append(col.index()).append("]; }\n");
            }
        }
        emitResultTypes(out, "    ", outputTypes);
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(n, proj, types, projNulls);\n");
        out.append("  }\n");
    }

    /** Inferred type of a projection expression: a column reference keeps its source type; arithmetic is DOUBLE when any operand is DOUBLE, else LONG. */
    private static Type projectionType(Plan.Expr expr, List<Type> inputTypes)
    {
        return switch (expr) {
            case Plan.Col col -> inputTypes.get(col.index());
            case Plan.Lit ignored -> Types.LONG;
            case Plan.Bin bin -> projectionType(bin.left(), inputTypes) == Types.DOUBLE || projectionType(bin.right(), inputTypes) == Types.DOUBLE ? Types.DOUBLE : Types.LONG;
            case Plan.Call call -> call.arguments().stream().anyMatch(a -> projectionType(a, inputTypes) == Types.DOUBLE) ? Types.DOUBLE : Types.LONG;
            case Plan.Coalesce coalesce -> coalesce.arguments().stream().anyMatch(a -> projectionType(a, inputTypes) == Types.DOUBLE) ? Types.DOUBLE : Types.LONG;
            case Plan.Case caseExpr -> projectionType(caseExpr.defaultValue(), inputTypes);
        };
    }

    /** Encode a decoded projection value back into its long result slot, per the output type. */
    private static String encodeSlot(Type type, String value)
    {
        return type == Types.DOUBLE ? "Double.doubleToRawLongBits(" + value + ")" : value;
    }

    // ---- projection-only (no GROUP BY, no aggregates): one output row per surviving input row ----

    /** A pipeline that selects/computes columns over its (joined, filtered) rows without aggregating -- a SELECT ... LIMIT shape. */
    private static boolean projectionOnly(Plan.Pipeline pipeline)
    {
        return pipeline.groupKeys().isEmpty() && pipeline.aggregates().isEmpty() && !pipeline.projections().isEmpty();
    }

    /** Logical types of every combined input column (probe then each build): STRING for a dictionary column, else LONG. */
    private static List<Type> combinedInputTypes(Plan.Pipeline pipeline, ColumnEncoding[][] encodings)
    {
        int total = pipeline.columnCount();
        for (Plan.Join join : pipeline.joins()) {
            total += join.build().columnCount();
        }
        List<Type> types = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            types.add(combinedEncoding(pipeline, encodings, i) == ColumnEncoding.STRING ? Types.STRING : Types.LONG);
        }
        return types;
    }

    /** Output types of a projection-only pipeline: each projection's type over the combined input columns. */
    private static List<Type> projectionOutputTypes(Plan.Pipeline pipeline, ColumnEncoding[][] encodings)
    {
        List<Type> inputTypes = combinedInputTypes(pipeline, encodings);
        List<Type> types = new ArrayList<>();
        for (Plan.Expr projection : pipeline.projections()) {
            types.add(projectionType(projection, inputTypes));
        }
        return types;
    }

    /** Whether projection {@code p} carries a null mask: a column reference to a nullable source column (computed projections are produced non-null). */
    private static boolean projectionCarriesNull(Plan.Pipeline pipeline, boolean[][] nullable, int p)
    {
        return pipeline.projections().get(p) instanceof Plan.Col col && combinedNullable(pipeline, nullable, col.index());
    }

    /** Declare the growable output arrays (one per projection, plus a null mask per nullable column reference) before the row loop. */
    private static void emitProjectionState(StringBuilder out, Plan.Pipeline pipeline, boolean[][] nullable)
    {
        int count = pipeline.projections().size();
        out.append("    int outCap = 1024; int outRow = 0;\n");
        for (int p = 0; p < count; p++) {
            out.append("    long[] out").append(p).append(" = new long[outCap];\n");
            if (projectionCarriesNull(pipeline, nullable, p)) {
                out.append("    boolean[] outN").append(p).append(" = new boolean[outCap];\n");
            }
        }
    }

    /** Append one surviving row's projected values to the output arrays, growing them when full. */
    private static void emitProjectionAppend(StringBuilder out, String indent, Plan.Pipeline pipeline, ColumnEncoding[][] encodings,
            boolean[][] nullable, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        List<Plan.Expr> projections = pipeline.projections();
        List<Type> types = projectionOutputTypes(pipeline, encodings);
        out.append(indent).append("if (outRow == outCap) {\n");
        out.append(indent).append("  outCap *= 2;\n");
        for (int p = 0; p < projections.size(); p++) {
            out.append(indent).append("  out").append(p).append(" = java.util.Arrays.copyOf(out").append(p).append(", outCap);\n");
            if (projectionCarriesNull(pipeline, nullable, p)) {
                out.append(indent).append("  outN").append(p).append(" = java.util.Arrays.copyOf(outN").append(p).append(", outCap);\n");
            }
        }
        out.append(indent).append("}\n");
        for (int p = 0; p < projections.size(); p++) {
            out.append(indent).append("out").append(p).append("[outRow] = ")
                    .append(encodeSlot(types.get(p), expr(projections.get(p), resolver, nullResolver, stringMaskIds))).append(";\n");
            if (projectionCarriesNull(pipeline, nullable, p)) {
                out.append(indent).append("outN").append(p).append("[outRow] = ")
                        .append(nullExpr(projections.get(p), resolver, nullResolver, stringMaskIds)).append(";\n");
            }
        }
        out.append(indent).append("outRow++;\n");
    }

    /** Trim the output arrays and return the materialized result (then ORDER BY / LIMIT); projections were applied inline. */
    private static void emitProjectionResult(StringBuilder out, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes)
    {
        int count = pipeline.projections().size();
        out.append("    long[][] result = new long[").append(count).append("][];\n");
        for (int p = 0; p < count; p++) {
            out.append("    result[").append(p).append("] = java.util.Arrays.copyOf(out").append(p).append(", outRow);\n");
        }
        emitResultTypes(out, "    ", resultTypes);
        boolean anyNull = false;
        for (int p = 0; p < count; p++) {
            anyNull |= projectionCarriesNull(pipeline, nullable, p);
        }
        if (anyNull) {
            out.append("    boolean[][] resultNulls = new boolean[").append(count).append("][];\n");
            for (int p = 0; p < count; p++) {
                if (projectionCarriesNull(pipeline, nullable, p)) {
                    out.append("    resultNulls[").append(p).append("] = java.util.Arrays.copyOf(outN").append(p).append(", outRow);\n");
                }
            }
            out.append("    return applyOrdering(new org.weakref.nitro.jit.CompiledPipeline.Result(outRow, result, types, resultNulls));\n");
            return;
        }
        out.append("    return applyOrdering(new org.weakref.nitro.jit.CompiledPipeline.Result(outRow, result, types));\n");
    }

    // ---- single-input scan -> filter -> aggregate ----

    private static void emitScanBody(StringBuilder out, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, List<Type> resultTypes)
    {
        out.append("    org.weakref.nitro.jit.Column[] in = inputs[0]; int rowCount = rowCounts[0];\n");
        TreeSet<Integer> referenced = referencedColumns(pipeline);
        for (int column : referenced) {
            emitScanColumnLoad(out, column, encodingOf(encodings, 0, column), nullableOf(nullable, 0, column));
        }
        // Predicate-over-dictionary: evaluate each string filter once per dictionary entry into an id mask.
        List<Plan.Condition> stringMatches = collectPipelineStringMatches(pipeline);
        Map<Plan.Condition, Integer> stringMaskIds = new IdentityHashMap<>();
        for (int s = 0; s < stringMatches.size(); s++) {
            Plan.Condition match = stringMatches.get(s);
            stringMaskIds.put(match, s);
            emitStringMaskPrelude(out, match, s, "cStr" + stringMatchColumn(match));
        }
        IntFunction<String> resolver = index -> scanAccess(index, encodingOf(encodings, 0, index), "i");
        IntFunction<String> nullResolver = index -> nullAccess(index, encodingOf(encodings, 0, index), nullableOf(nullable, 0, index), "i");
        boolean grouped = !pipeline.groupKeys().isEmpty();
        // A single-key scan group can speculate array mode: estimate the key domain from a sample, bet on a
        // direct-indexed array, and deopt to a hash table if a later key falls outside the bet. A nullable key or
        // a nullable aggregate routes through the null-aware hash path instead (array mode emits no result null mask).
        boolean speculate = grouped && pipeline.groupKeys().size() == 1
                && !anyGroupKeyNullable(pipeline, nullable) && !anyAggregateNullable(pipeline);

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
        boolean projection = projectionOnly(pipeline);
        if (projection) {
            emitProjectionState(out, pipeline, nullable);
        }
        else if (grouped) {
            emitGroupedState(out, pipeline, nullable, speculate);
        }
        else {
            emitGlobalState(out, pipeline.aggregates());
        }
        out.append("    for (int i = 0; i < rowCount; i++) {\n");
        emitRowBody(out, "      ", pipeline, encodings, nullable, resolver, groupKeyResolver, nullResolver, stringMaskIds, grouped, speculate);
        out.append("    }\n");
        if (projection) {
            emitProjectionResult(out, pipeline, nullable, resultTypes);
        }
        else if (grouped) {
            emitGroupedResult(out, pipeline, nullable, speculate, dictKeyColumn, resultTypes);
        }
        else {
            emitGlobalResult(out, pipeline.aggregates(), resultTypes);
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

    /** Collect every predicate-over-dictionary leaf (exact, LIKE, substring) reachable through and/or/not. */
    private static void collectStringMatches(Plan.Condition condition, List<Plan.Condition> into)
    {
        if (condition instanceof Plan.StringMatch || condition instanceof Plan.LikeMatch || condition instanceof Plan.SubstringMatch) {
            into.add(condition);
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

    /** Collect predicate-over-dictionary leaves nested in an expression's CASE conditions (e.g. {@code sum(CASE WHEN s IN (..) THEN ..)}). */
    private static void collectStringMatchesInExpr(Plan.Expr expr, List<Plan.Condition> into)
    {
        switch (expr) {
            case Plan.Case kase -> {
                for (Plan.Case.Branch branch : kase.branches()) {
                    collectStringMatches(branch.condition(), into);
                    collectStringMatchesInExpr(branch.value(), into);
                }
                collectStringMatchesInExpr(kase.defaultValue(), into);
            }
            case Plan.Bin bin -> {
                collectStringMatchesInExpr(bin.left(), into);
                collectStringMatchesInExpr(bin.right(), into);
            }
            case Plan.Call call -> call.arguments().forEach(argument -> collectStringMatchesInExpr(argument, into));
            case Plan.Coalesce coalesce -> coalesce.arguments().forEach(argument -> collectStringMatchesInExpr(argument, into));
            case Plan.Col ignored -> {}
            case Plan.Lit ignored -> {}
        }
    }

    /** Collect every predicate-over-dictionary leaf in the pipeline: from WHERE filters and from CASE conditions in aggregate inputs. */
    private static List<Plan.Condition> collectPipelineStringMatches(Plan.Pipeline pipeline)
    {
        List<Plan.Condition> into = new ArrayList<>();
        for (Plan.Condition filter : pipeline.filters()) {
            collectStringMatches(filter, into);
        }
        for (Plan.Aggregate aggregate : pipeline.aggregates()) {
            if (aggregate.input() != null) {
                collectStringMatchesInExpr(aggregate.input(), into);
            }
        }
        return into;
    }

    /** The dictionary-string column a predicate-over-dictionary leaf tests. */
    private static int stringMatchColumn(Plan.Condition condition)
    {
        return switch (condition) {
            case Plan.StringMatch match -> match.column();
            case Plan.LikeMatch match -> match.column();
            case Plan.SubstringMatch match -> match.column();
            default -> throw new IllegalArgumentException("not a string match: " + condition);
        };
    }

    /**
     * Build the id mask for a predicate-over-dictionary leaf once, by testing each dictionary entry. {@code id} is
     * the predicate's unique index in the query (a column may carry several string predicates -- e.g. one per OR
     * branch -- so masks are keyed per predicate, not per column).
     */
    private static void emitStringMaskPrelude(StringBuilder out, Plan.Condition match, int id, String dictionaryVar)
    {
        if (match instanceof Plan.LikeMatch like) {
            out.append("    java.util.regex.Pattern sLikePat").append(id).append(" = org.weakref.nitro.jit.StringMatching.likePattern(")
                    .append(javaStringLiteral(like.pattern())).append(");\n");
            out.append("    boolean[] sMask").append(id).append(" = new boolean[").append(dictionaryVar).append(".length];\n");
            out.append("    for (int e = 0; e < ").append(dictionaryVar).append(".length; e++) {\n");
            String matches = "sLikePat" + id + ".matcher(new String(" + dictionaryVar + "[e], java.nio.charset.StandardCharsets.UTF_8)).matches()";
            out.append("      sMask").append(id).append("[e] = ").append(like.negated() ? "!(" + matches + ")" : "(" + matches + ")").append(";\n");
            out.append("    }\n");
            return;
        }
        List<String> values;
        boolean negated;
        String entry;   // the dictionary entry's bytes to test (possibly a substring)
        if (match instanceof Plan.SubstringMatch substring) {
            values = substring.values();
            negated = substring.negated();
            entry = "org.weakref.nitro.function.scalar.builtin.Utf8Support.substring(" + dictionaryVar + "[e], 0, " + dictionaryVar + "[e].length, "
                    + (long) substring.start() + "L, " + (long) substring.length() + "L)";
        }
        else {
            Plan.StringMatch exact = (Plan.StringMatch) match;
            values = exact.values();
            negated = exact.negated();
            entry = dictionaryVar + "[e]";
        }
        for (int v = 0; v < values.size(); v++) {
            out.append("    byte[] sLit").append(id).append("_").append(v).append(" = ")
                    .append(javaStringLiteral(values.get(v))).append(".getBytes(java.nio.charset.StandardCharsets.UTF_8);\n");
        }
        out.append("    boolean[] sMask").append(id).append(" = new boolean[").append(dictionaryVar).append(".length];\n");
        out.append("    for (int e = 0; e < ").append(dictionaryVar).append(".length; e++) {\n");
        out.append("      byte[] sv = ").append(entry).append(";\n");
        StringBuilder member = new StringBuilder();
        for (int v = 0; v < values.size(); v++) {
            member.append(member.length() == 0 ? "" : " || ").append("java.util.Arrays.equals(sv, sLit").append(id).append("_").append(v).append(")");
        }
        String matches = values.isEmpty() ? "false" : member.toString();
        out.append("      sMask").append(id).append("[e] = ").append(negated ? "!(" + matches + ")" : "(" + matches + ")").append(";\n");
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

    /**
     * The join level at which every column a filter references is available: -1 when it reads only probe columns
     * (so it can be applied before any join probe), else the highest build index it reaches. Used to place a filter
     * right after the build that completes its inputs -- matching where the operator/Trino plan applies it -- rather
     * than after every probe.
     */
    private static int filterLevel(Plan.Condition filter, List<Plan.Join> joins, int[] buildOffset, int probeColumns)
    {
        TreeSet<Integer> columns = new TreeSet<>();
        collectConditionColumns(filter, columns);
        int level = -1;
        for (int column : columns) {
            if (column >= probeColumns) {
                level = Math.max(level, buildOf(joins, buildOffset, column));
            }
        }
        return level;
    }

    /** Conjunction (Java {@code &&}) of the rendered filter conditions. */
    private static String conjunction(List<Plan.Condition> conditions, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        return conditions.stream().map(c -> conditionTrue(c, resolver, nullResolver, stringMaskIds)).collect(joining(" && "));
    }

    /** Zero literal of a column's Java type (the masked-out value of an unmatched left-join build column). */
    private static String zeroFor(ColumnEncoding encoding)
    {
        return encoding == ColumnEncoding.STRING ? "0" : "0L";
    }

    /** A left-join build column's value: the masked-out zero when the row had no match ({@code rowVar == -1}), else the access. */
    private static String outerValue(String rowVar, ColumnEncoding encoding, String access)
    {
        return "(" + rowVar + " == -1 ? " + zeroFor(encoding) + " : " + access + ")";
    }

    /** A left-join build column is NULL when the row had no match, or when the underlying value is null. */
    private static String outerNull(String rowVar, String nullAccess)
    {
        return nullAccess.equals("false") ? "(" + rowVar + " == -1)" : "(" + rowVar + " == -1 || " + nullAccess + ")";
    }

    /**
     * Emit the per-row join probes with each filter interleaved at the level its inputs become available -- probe-only
     * filters before any probe, the rest right after the build that completes them -- so a row that fails is dropped
     * before the remaining probes (matching where the operator/Trino plan applies the filter). Opens one {@code if}
     * brace per probe and per interleaved filter group; returns the number opened so the caller can close them.
     */
    private static int emitProbesWithFilters(StringBuilder out, String baseIndent, Plan.Pipeline pipeline, List<Plan.Join> joins,
            int[] buildOffset, int probeColumns, int joinCount, IntFunction<String> resolver, IntFunction<String> nullResolver,
            Map<Plan.Condition, Integer> stringMaskIds)
    {
        Map<Integer, List<Plan.Condition>> filtersByLevel = new java.util.LinkedHashMap<>();
        for (Plan.Condition filter : pipeline.filters()) {
            filtersByLevel.computeIfAbsent(filterLevel(filter, joins, buildOffset, probeColumns), level -> new ArrayList<>()).add(filter);
        }
        String indent = baseIndent;
        int openBraces = 0;
        List<Plan.Condition> probeOnly = filtersByLevel.get(-1);
        if (probeOnly != null) {
            out.append(indent).append("if (").append(conjunction(probeOnly, resolver, nullResolver, stringMaskIds)).append(") {\n");
            indent += "  ";
            openBraces++;
        }
        for (int k = 0; k < joinCount; k++) {
            emitProbeLookup(out, indent, k, joins.get(k), resolver, nullResolver);
            // An inner join drops a probe row with no match; a left join keeps it (build columns read NULL).
            if (!joins.get(k).outer()) {
                out.append(indent).append("if (buildRow").append(k).append(" != -1) {\n");
                indent += "  ";
                openBraces++;
            }
            List<Plan.Condition> atLevel = filtersByLevel.get(k);
            if (atLevel != null) {
                out.append(indent).append("if (").append(conjunction(atLevel, resolver, nullResolver, stringMaskIds)).append(") {\n");
                indent += "  ";
                openBraces++;
            }
        }
        return openBraces;
    }

    private static void emitJoinBody(StringBuilder out, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, List<Type> resultTypes)
    {
        emitJoinBody(out, pipeline, encodings, nullable, resultTypes, false);
    }

    /**
     * Probe ⋈ builds → filter → aggregate. When {@code streaming}, the build sides are materialized once from
     * {@code builds}/{@code buildRowCounts} into hash tables and the probe is consumed batch-by-batch from a
     * {@link StreamingPipeline.Source} {@code source}; otherwise everything comes materialized from
     * {@code inputs}/{@code rowCounts}. Build-side string-filter masks build once; probe-side masks rebuild per
     * batch (the probe's per-batch dictionary).
     */
    private static void emitJoinBody(StringBuilder out, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, List<Type> resultTypes, boolean streaming)
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
                return joinAccess(combinedEncoding(pipeline, encodings, index), probeVars(index), "i");
            }
            int build = buildOf(joins, buildOffset, index);
            int local = index - buildOffset[build];
            ColumnEncoding encoding = combinedEncoding(pipeline, encodings, index);
            String access = joinAccess(encoding, buildVars(build, local), "buildRow" + build);
            return joins.get(build).outer() ? outerValue("buildRow" + build, encoding, access) : access;
        };

        IntFunction<String> nullResolver = index -> {
            if (index < probeColumns) {
                return joinNullAccess(combinedEncoding(pipeline, encodings, index), combinedNullable(pipeline, nullable, index), probeVars(index), "i");
            }
            int build = buildOf(joins, buildOffset, index);
            int local = index - buildOffset[build];
            String nullAccess = joinNullAccess(combinedEncoding(pipeline, encodings, index), combinedNullable(pipeline, nullable, index), buildVars(build, local), "buildRow" + build);
            return joins.get(build).outer() ? outerNull("buildRow" + build, nullAccess) : nullAccess;
        };

        // Predicate-over-dictionary string masks: assign a stable id per match up front (the build-side masks are
        // emitted per dimension below, before that dimension's structures, so a pushed string filter can read them).
        List<Plan.Condition> stringMatches = collectPipelineStringMatches(pipeline);
        Map<Plan.Condition, Integer> stringMaskIds = new IdentityHashMap<>();
        for (int s = 0; s < stringMatches.size(); s++) {
            stringMaskIds.put(stringMatches.get(s), s);
        }

        // Filter pushdown: a filter referencing only one build's columns is enforced while constructing that build,
        // so the join prunes the probe early (the same early pruning an operator engine gets by filtering a
        // dimension before its build). Includes string-match filters, whose dictionary mask is emitted just before
        // the build structures below.
        String[] buildFilter = new String[joinCount];
        for (int k = 0; k < joinCount; k++) {
            int start = buildOffset[k];
            int end = start + joins.get(k).build().columnCount();
            int build = k;
            IntFunction<String> buildResolver = index -> joinAccess(combinedEncoding(pipeline, encodings, index), buildVars(build, index - buildOffset[build]), "r");
            IntFunction<String> buildNullResolver = index -> joinNullAccess(combinedEncoding(pipeline, encodings, index), combinedNullable(pipeline, nullable, index), buildVars(build, index - buildOffset[build]), "r");
            List<String> pushed = new ArrayList<>();
            // A left join must keep non-matching probe rows, so a filter on its build is NOT a build-side prune.
            if (!joins.get(k).outer()) {
                for (Plan.Condition filter : pipeline.filters()) {
                    if (pushableToBuild(filter, start, end)) {
                        pushed.add(conditionTrue(filter, buildResolver, buildNullResolver, stringMaskIds));
                    }
                }
            }
            buildFilter[k] = pushed.isEmpty() ? null : String.join(" && ", pushed);
        }

        // Builds (dimensions) are materialized once into hash tables: eager from inputs[k+1], streaming from builds[k].
        String buildsArray = streaming ? "builds" : "inputs";
        String buildCounts = streaming ? "buildRowCounts" : "rowCounts";
        int buildBase = streaming ? 0 : 1;
        for (int k = 0; k < joinCount; k++) {
            Plan.Join join = joins.get(k);
            int keyCount = join.build().keyColumns().length;
            if (join.probeKeyColumns().length != keyCount) {
                throw new IllegalArgumentException("join " + k + " key count mismatch: probe " + join.probeKeyColumns().length + " vs build " + keyCount);
            }
            out.append("    org.weakref.nitro.jit.Column[] build").append(k).append(" = ").append(buildsArray).append("[").append(k + buildBase)
                    .append("]; int build").append(k).append("Rows = ").append(buildCounts).append("[").append(k + buildBase).append("];\n");
            for (int column : buildReferenced.get(k)) {
                int combinedIndex = buildOffset[k] + column;
                emitJoinColumnLoad(out, combinedEncoding(pipeline, encodings, combinedIndex), combinedNullable(pipeline, nullable, combinedIndex), "build" + k + "[" + column + "]", buildVars(k, column));
            }
            // Build-side predicate-over-dictionary masks for this dimension, emitted before its structures so a
            // pushed string-match filter can read them (their dictionary is now materialized). Includes masks for
            // CASE conditions in aggregate inputs, not just WHERE filters; probe-side masks build per batch below.
            for (int s = 0; s < stringMatches.size(); s++) {
                Plan.Condition match = stringMatches.get(s);
                int column = stringMatchColumn(match);
                if (column >= buildOffset[k] && column < buildOffset[k] + join.build().columnCount()) {
                    emitStringMaskPrelude(out, match, s, buildVars(k, column - buildOffset[k]).stringDict());
                }
            }
            emitBuildStructures(out, k, join.build().keyColumns(), buildFilter[k]);
        }

        boolean grouped = !pipeline.groupKeys().isEmpty();
        boolean projection = projectionOnly(pipeline);
        if (projection) {
            emitProjectionState(out, pipeline, nullable);
        }
        else if (grouped) {
            emitGroupedState(out, pipeline, nullable, false);
        }
        else {
            emitGlobalState(out, pipeline.aggregates());
        }

        // Join-driven late materialization (streaming only): decode just the probe's join keys and filter columns,
        // run the joins + filters recording each surviving probe row and its matched build rows, then materialize
        // the probe payload columns (measures, group keys) for only the survivors -- skipping the payload's
        // conversion for probe rows that fail the joins/filters. Falls back to a single eager pass when not
        // streaming, projecting, or when there is no deferrable probe payload (e.g. count(*)).
        TreeSet<Integer> eagerProbe = new TreeSet<>();
        for (Plan.Join join : joins) {
            for (int key : join.probeKeyColumns()) {
                if (key < probeColumns) {
                    eagerProbe.add(key);
                }
            }
        }
        for (Plan.Condition filter : pipeline.filters()) {
            TreeSet<Integer> filterCols = new TreeSet<>();
            collectConditionColumns(filter, filterCols);
            for (int column : filterCols) {
                if (column < probeColumns) {
                    eagerProbe.add(column);
                }
            }
        }
        TreeSet<Integer> payloadProbe = new TreeSet<>();
        for (int column : accumulateColumns(pipeline)) {
            if (column < probeColumns) {
                payloadProbe.add(column);
            }
        }
        boolean lateMaterialize = streaming && !projection && !payloadProbe.isEmpty();

        if (lateMaterialize) {
            // In the survivor (accumulate) loop a probe column is read at the compacted index j, and a build column
            // at the build row recorded for survivor j (bsel<k>[j]) rather than the live join variable.
            IntFunction<String> lazyResolver = index -> {
                if (index < probeColumns) {
                    return joinAccess(combinedEncoding(pipeline, encodings, index), probeVars(index), "j");
                }
                int build = buildOf(joins, buildOffset, index);
                ColumnEncoding encoding = combinedEncoding(pipeline, encodings, index);
                String row = "bsel" + build + "[j]";
                String access = joinAccess(encoding, buildVars(build, index - buildOffset[build]), row);
                return joins.get(build).outer() ? outerValue(row, encoding, access) : access;
            };
            IntFunction<String> lazyNullResolver = index -> {
                if (index < probeColumns) {
                    return joinNullAccess(combinedEncoding(pipeline, encodings, index), combinedNullable(pipeline, nullable, index), probeVars(index), "j");
                }
                int build = buildOf(joins, buildOffset, index);
                String row = "bsel" + build + "[j]";
                String nullAccess = joinNullAccess(combinedEncoding(pipeline, encodings, index), combinedNullable(pipeline, nullable, index), buildVars(build, index - buildOffset[build]), row);
                return joins.get(build).outer() ? outerNull(row, nullAccess) : nullAccess;
            };
            List<Plan.Condition> filterMatches = new ArrayList<>();
            for (Plan.Condition filter : pipeline.filters()) {
                collectStringMatches(filter, filterMatches);
            }
            List<Plan.Condition> aggregateMatches = new ArrayList<>();
            for (Plan.Aggregate aggregate : pipeline.aggregates()) {
                if (aggregate.input() != null) {
                    collectStringMatchesInExpr(aggregate.input(), aggregateMatches);
                }
            }
            out.append("    while (source.advance()) {\n");
            out.append("      int probeRows = source.rows();\n");
            out.append("      int[] selection = new int[probeRows]; int selected = 0;\n");
            for (int k = 0; k < joinCount; k++) {
                out.append("      int[] bsel").append(k).append(" = new int[probeRows];\n");
            }
            // Phase 1: eager probe columns -> joins + filters -> selection + matched build rows.
            out.append("      {\n");
            out.append("        org.weakref.nitro.jit.Column[] probe = source.materialize(").append(intArrayLiteral(eagerProbe)).append(");\n");
            for (int column : eagerProbe) {
                emitJoinColumnLoad(out, combinedEncoding(pipeline, encodings, column), combinedNullable(pipeline, nullable, column), "probe[" + column + "]", probeVars(column));
            }
            for (Plan.Condition match : filterMatches) {
                if (stringMatchColumn(match) < probeColumns) {
                    emitStringMaskPrelude(out, match, stringMaskIds.get(match), probeVars(stringMatchColumn(match)).stringDict());
                }
            }
            out.append("        for (int i = 0; i < probeRows; i++) {\n");
            int openBraces = emitProbesWithFilters(out, "          ", pipeline, joins, buildOffset, probeColumns, joinCount, resolver, nullResolver, stringMaskIds);
            String indent = "          " + "  ".repeat(openBraces);
            out.append(indent).append("selection[selected] = i;\n");
            for (int k = 0; k < joinCount; k++) {
                out.append(indent).append("bsel").append(k).append("[selected] = buildRow").append(k).append(";\n");
            }
            out.append(indent).append("selected++;\n");
            for (int brace = 0; brace < openBraces; brace++) {
                indent = indent.substring(2);
                out.append(indent).append("}\n");
            }
            out.append("        }\n");
            out.append("      }\n");
            // Phase 2: payload probe columns materialized for the survivors only, then folded in.
            out.append("      {\n");
            out.append("        org.weakref.nitro.jit.Column[] probe = source.materialize(").append(intArrayLiteral(payloadProbe)).append(", selection, selected);\n");
            for (int column : payloadProbe) {
                emitJoinColumnLoad(out, combinedEncoding(pipeline, encodings, column), combinedNullable(pipeline, nullable, column), "probe[" + column + "]", probeVars(column));
            }
            for (Plan.Condition match : aggregateMatches) {
                if (stringMatchColumn(match) < probeColumns) {
                    emitStringMaskPrelude(out, match, stringMaskIds.get(match), probeVars(stringMatchColumn(match)).stringDict());
                }
            }
            out.append("        for (int j = 0; j < selected; j++) {\n");
            if (grouped) {
                emitGroupedAccumulate(out, "          ", pipeline, nullable, lazyResolver, lazyResolver, lazyNullResolver, stringMaskIds, false);
            }
            else {
                emitGlobalAccumulate(out, "          ", pipeline.aggregates(), lazyResolver, lazyNullResolver, stringMaskIds);
            }
            out.append("        }\n");
            out.append("      }\n");
            out.append("    }\n");   // close the batch loop
        }
        else {
            // Probe rows: one pass over the materialized probe (eager), or batch-by-batch from the source (streaming).
            if (streaming) {
                out.append("    while (source.advance()) {\n");
                out.append("      org.weakref.nitro.jit.Column[] probe = source.columns(); int probeRows = source.rows();\n");
            }
            else {
                out.append("    org.weakref.nitro.jit.Column[] probe = inputs[0]; int probeRows = rowCounts[0];\n");
            }
            for (int column : probeReferenced) {
                emitJoinColumnLoad(out, combinedEncoding(pipeline, encodings, column), combinedNullable(pipeline, nullable, column), "probe[" + column + "]", probeVars(column));
            }
            for (int s = 0; s < stringMatches.size(); s++) {
                Plan.Condition match = stringMatches.get(s);
                int column = stringMatchColumn(match);
                if (column < probeColumns) {
                    emitStringMaskPrelude(out, match, s, probeVars(column).stringDict());
                }
            }

            out.append("    for (int i = 0; i < probeRows; i++) {\n");
            // Filters are interleaved with the probes (early-out); the body then runs without re-checking them.
            // Nullable join inputs carry a null mask; non-nullable columns resolve to the "false" fast path.
            int openBraces = emitProbesWithFilters(out, "      ", pipeline, joins, buildOffset, probeColumns, joinCount, resolver, nullResolver, stringMaskIds);
            String indent = "      " + "  ".repeat(openBraces);
            emitRowBody(out, indent, pipeline, encodings, nullable, resolver, resolver, nullResolver, stringMaskIds, grouped, false, true);
            for (int brace = 0; brace < openBraces; brace++) {
                indent = indent.substring(2);
                out.append(indent).append("}\n");
            }
            out.append("    }\n");
            if (streaming) {
                out.append("    }\n");   // close the batch loop
            }
        }

        if (projection) {
            emitProjectionResult(out, pipeline, nullable, resultTypes);
        }
        else if (grouped) {
            emitGroupedResult(out, pipeline, nullable, false, -1, resultTypes);
        }
        else {
            emitGlobalResult(out, pipeline.aggregates(), resultTypes);
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
    /**
     * Build the lookup structure for join {@code k}. When {@code buildFilter} is non-null, build rows failing it are
     * not inserted -- so a probe key matching a filtered-out dimension row gets no match and is pruned before any
     * downstream nested join, the same early pruning an operator engine gets by filtering the dimension before the
     * build. (The filter remains in the probe WHERE too; for survivors that is a redundant, always-true re-check.)
     */
    private static void emitBuildStructures(StringBuilder out, int k, int[] buildKeys, String buildFilter)
    {
        int keyCount = buildKeys.length;
        String rows = "build" + k + "Rows";
        String skip = buildFilter == null ? "" : "if (!(" + buildFilter + ")) { continue; } ";
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
            out.append("      for (int r = 0; r < ").append(rows).append("; r++) { ").append(skip).append("buildRowByKey").append(k).append("[(int) (").append(buildKey).append("[r] - minKey").append(k).append(")] = r; }\n");
            out.append("    }\n    else {\n");
            emitHashBuild(out, "      ", k, buildKeys, buildFilter);
            out.append("    }\n");
        }
        else {
            for (int kx = 0; kx < keyCount; kx++) {
                out.append("    long[] jKey").append(k).append("_").append(kx).append(" = null;\n");
            }
            out.append("    int[] jRow").append(k).append(" = null; int jMask").append(k).append(" = 0;\n");
            out.append("    {\n");
            emitHashBuild(out, "      ", k, buildKeys, buildFilter);
            out.append("    }\n");
        }
    }

    /** Open-addressing build for join {@code k} (build keys assumed unique), populating the join's slot arrays. */
    private static void emitHashBuild(StringBuilder out, String indent, int k, int[] buildKeys, String buildFilter)
    {
        int keyCount = buildKeys.length;
        String rows = "build" + k + "Rows";
        out.append(indent).append("int jcap = 16; while (jcap * 0.75f < ").append(rows).append(") { jcap <<= 1; }\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("jKey").append(k).append("_").append(kx).append(" = new long[jcap];\n");
        }
        out.append(indent).append("jRow").append(k).append(" = new int[jcap]; java.util.Arrays.fill(jRow").append(k).append(", -1); jMask").append(k).append(" = jcap - 1;\n");
        out.append(indent).append("for (int r = 0; r < ").append(rows).append("; r++) {\n");
        if (buildFilter != null) {
            out.append(indent).append("  if (!(").append(buildFilter).append(")) { continue; }\n");
        }
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
    private static void emitProbeLookup(StringBuilder out, String indent, int k, Plan.Join join, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        int[] probeKeys = join.probeKeyColumns();
        int keyCount = probeKeys.length;
        // The probe key is resolved through the combined resolver, so it may be a fact (probe) column or a column
        // from an earlier dimension already matched in this nest -- a snowflake join (e.g. fact -> customer ->
        // customer_address keyed on customer's c_current_addr_sk). A null key matches nothing (SQL three-valued).
        StringBuilder keyNull = new StringBuilder();
        for (int kx = 0; kx < keyCount; kx++) {
            String isNull = nullResolver.apply(probeKeys[kx]);
            if (!isNull.equals("false")) {
                keyNull.append(keyNull.length() == 0 ? "" : " || ").append(isNull);
            }
        }
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("long pk").append(k).append("_").append(kx).append(" = ").append(resolver.apply(probeKeys[kx])).append(";\n");
        }
        out.append(indent).append("int buildRow").append(k).append(";\n");
        if (keyNull.length() > 0) {
            out.append(indent).append("if (").append(keyNull).append(") { buildRow").append(k).append(" = -1; }\n");
            out.append(indent).append("else {\n");
            indent += "  ";
        }
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
        if (keyNull.length() > 0) {
            indent = indent.substring(2);
            out.append(indent).append("}\n");
        }
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

    private static void emitRowBody(StringBuilder out, String indent, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, IntFunction<String> resolver, IntFunction<String> groupKeyResolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds, boolean grouped, boolean speculate)
    {
        emitRowBody(out, indent, pipeline, encodings, nullable, resolver, groupKeyResolver, nullResolver, stringMaskIds, grouped, speculate, false);
    }

    /**
     * Emit one surviving row's body (filter then projection / group / global accumulate). When {@code filtersApplied}
     * the WHERE was already applied upstream (interleaved with the join probes for early-out), so it is not re-checked.
     */
    private static void emitRowBody(StringBuilder out, String indent, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, IntFunction<String> resolver, IntFunction<String> groupKeyResolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds, boolean grouped, boolean speculate, boolean filtersApplied)
    {
        String bodyIndent = indent;
        boolean emitFilter = !filtersApplied && !pipeline.filters().isEmpty();
        if (emitFilter) {
            // A row passes WHERE only when the condition is TRUE (not FALSE, not NULL) -- three-valued logic.
            out.append(indent).append("if (").append(conjunction(pipeline.filters(), resolver, nullResolver, stringMaskIds)).append(") {\n");
            bodyIndent = indent + "  ";
        }
        if (projectionOnly(pipeline)) {
            emitProjectionAppend(out, bodyIndent, pipeline, encodings, nullable, resolver, nullResolver, stringMaskIds);
        }
        else if (grouped) {
            emitGroupedAccumulate(out, bodyIndent, pipeline, nullable, resolver, groupKeyResolver, nullResolver, stringMaskIds, speculate);
        }
        else {
            emitGlobalAccumulate(out, bodyIndent, pipeline.aggregates(), resolver, nullResolver, stringMaskIds);
        }
        if (emitFilter) {
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

    private static void emitGlobalAccumulate(StringBuilder out, String indent, List<Plan.Aggregate> aggregates, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        for (int a = 0; a < aggregates.size(); a++) {
            emitAggregateUpdate(out, indent, aggregates.get(a), cells(aggregates, a, "a", null), resolver, nullResolver, stringMaskIds);
        }
    }

    /** Fold one row into an aggregate's cells, skipping the row when the aggregate's input is null (so nulls are ignored). */
    private static void emitAggregateUpdate(StringBuilder out, String indent, Plan.Aggregate aggregate, List<String> cells, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        AggregateLibrary.AggregateCompiler aggregator = aggregator(aggregate);
        String inputExpr = input(aggregate, resolver, nullResolver, stringMaskIds);
        String guard = aggregate.input() == null ? "false" : nullExpr(aggregate.input(), resolver, nullResolver, stringMaskIds);
        if (guard.equals("false")) {
            aggregator.emitUpdate(out, indent, cells, inputExpr);
            return;
        }
        out.append(indent).append("if (!(").append(guard).append(")) {\n");
        aggregator.emitUpdate(out, indent + "  ", cells, inputExpr);
        out.append(indent).append("}\n");
    }

    private static void emitGlobalResult(StringBuilder out, List<Plan.Aggregate> aggregates, List<Type> resultTypes)
    {
        int n = aggregates.size();
        out.append("    long[][] result = new long[").append(n).append("][];\n");
        for (int a = 0; a < n; a++) {
            out.append("    result[").append(a).append("] = new long[] { ").append(aggregator(aggregates.get(a)).result(cells(aggregates, a, "a", null))).append(" };\n");
        }
        emitResultTypes(out, "    ", resultTypes);
        boolean nullableAggregate = false;
        for (int a = 0; a < n; a++) {
            if (aggregator(aggregates.get(a)).resultNull(cells(aggregates, a, "a", null)) != null) {
                nullableAggregate = true;
            }
        }
        if (nullableAggregate) {
            out.append("    boolean[][] resultNulls = new boolean[").append(n).append("][];\n");
            for (int a = 0; a < n; a++) {
                String resultNull = aggregator(aggregates.get(a)).resultNull(cells(aggregates, a, "a", null));
                if (resultNull != null) {
                    out.append("    resultNulls[").append(a).append("] = new boolean[] { ").append(resultNull).append(" };\n");
                }
            }
            out.append("    return applyProjection(applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(1, result, types, resultNulls))));\n");
            return;
        }
        out.append("    return applyProjection(applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(1, result, types))));\n");
    }

    // ---- grouped aggregation (single long key) ----

    private static void emitGroupedState(StringBuilder out, Plan.Pipeline pipeline, boolean[][] nullable, boolean speculate)
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
            if (keyNullable(pipeline, nullable, kx)) {
                out.append("    boolean[] htKeyN").append(kx).append(" = new boolean[cap];\n");
            }
        }
        out.append("    int[] htGid = new int[cap];\n");
        out.append("    java.util.Arrays.fill(htGid, -1);\n");
        out.append("    int htMask = cap - 1; int htFill = (int) (cap * 0.75f); int groupCount = 0;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append("    long[] keyByGid").append(kx).append(" = new long[16];\n");
            if (keyNullable(pipeline, nullable, kx)) {
                out.append("    boolean[] nullByGid").append(kx).append(" = new boolean[16];\n");
            }
        }
        for (int c = 0; c < total; c++) {
            out.append("    long[] agg").append(c).append(" = new long[16];\n");
        }
    }

    private static void emitGroupedAccumulate(StringBuilder out, String indent, Plan.Pipeline pipeline, boolean[][] nullable, IntFunction<String> resolver, IntFunction<String> groupKeyResolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds, boolean speculate)
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
                emitAggregateUpdate(out, a2, aggregates.get(a), cells(aggregates, a, "aAgg", "ao"), resolver, nullResolver, stringMaskIds);
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
                emitAggregateUpdate(out, a1, aggregates.get(a), cells(aggregates, a, "agg", "gid"), resolver, nullResolver, stringMaskIds);
            }
            out.append(indent).append("}\n");
            return;
        }
        int keyCount = pipeline.groupKeys().size();
        for (int kx = 0; kx < keyCount; kx++) {
            String value = expr(pipeline.groupKeys().get(kx), groupKeyResolver);
            if (keyNullable(pipeline, nullable, kx)) {
                int colIndex = ((Plan.Col) pipeline.groupKeys().get(kx)).index();
                // A null key is canonicalized to value 0 so all nulls land in one group; the null flag keeps it
                // distinct from a real 0.
                out.append(indent).append("boolean gkN").append(kx).append(" = ").append(nullResolver.apply(colIndex)).append(";\n");
                out.append(indent).append("long gk").append(kx).append(" = gkN").append(kx).append(" ? 0L : (").append(value).append(");\n");
            }
            else {
                out.append(indent).append("long gk").append(kx).append(" = ").append(value).append(";\n");
            }
        }
        out.append(indent).append("int gslot = mix(").append(hashFold("gk", "", keyCount)).append(") & htMask;\n");
        out.append(indent).append("while (htGid[gslot] != -1 && !(").append(keyCompare("gslot", keyCount, pipeline, nullable)).append(")) { gslot = (gslot + 1) & htMask; }\n");
        out.append(indent).append("int gid = htGid[gslot];\n");
        out.append(indent).append("if (gid == -1) {\n");
        String b = indent + "  ";
        out.append(b).append("gid = groupCount++; htGid[gslot] = gid;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("htKey").append(kx).append("[gslot] = gk").append(kx).append(";\n");
            if (keyNullable(pipeline, nullable, kx)) {
                out.append(b).append("htKeyN").append(kx).append("[gslot] = gkN").append(kx).append(";\n");
            }
        }
        out.append(b).append("if (gid == keyByGid0.length) {\n");
        out.append(b).append("  int n = keyByGid0.length * 2;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("  keyByGid").append(kx).append(" = java.util.Arrays.copyOf(keyByGid").append(kx).append(", n);\n");
            if (keyNullable(pipeline, nullable, kx)) {
                out.append(b).append("  nullByGid").append(kx).append(" = java.util.Arrays.copyOf(nullByGid").append(kx).append(", n);\n");
            }
        }
        for (int c = 0; c < cellCount(aggregates); c++) {
            out.append(b).append("  agg").append(c).append(" = java.util.Arrays.copyOf(agg").append(c).append(", n);\n");
        }
        out.append(b).append("}\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("keyByGid").append(kx).append("[gid] = gk").append(kx).append(";\n");
            if (keyNullable(pipeline, nullable, kx)) {
                out.append(b).append("nullByGid").append(kx).append("[gid] = gkN").append(kx).append(";\n");
            }
        }
        emitStateIdentity(out, b, aggregates, "agg", "gid");
        out.append(b).append("if (groupCount > htFill) {\n");
        out.append(b).append("  int ncap = cap * 2;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("  long[] nKey").append(kx).append(" = new long[ncap];\n");
            if (keyNullable(pipeline, nullable, kx)) {
                out.append(b).append("  boolean[] nKeyN").append(kx).append(" = new boolean[ncap];\n");
            }
        }
        out.append(b).append("  int[] nGid = new int[ncap];\n");
        out.append(b).append("  java.util.Arrays.fill(nGid, -1); int nMask = ncap - 1;\n");
        out.append(b).append("  for (int s = 0; s < cap; s++) { if (htGid[s] != -1) {\n");
        out.append(b).append("    int ns = mix(").append(hashFold("htKey", "[s]", keyCount)).append(") & nMask; while (nGid[ns] != -1) { ns = (ns + 1) & nMask; }\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("    nKey").append(kx).append("[ns] = htKey").append(kx).append("[s];\n");
            if (keyNullable(pipeline, nullable, kx)) {
                out.append(b).append("    nKeyN").append(kx).append("[ns] = htKeyN").append(kx).append("[s];\n");
            }
        }
        out.append(b).append("    nGid[ns] = htGid[s]; } }\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(b).append("  htKey").append(kx).append(" = nKey").append(kx).append(";\n");
            if (keyNullable(pipeline, nullable, kx)) {
                out.append(b).append("  htKeyN").append(kx).append(" = nKeyN").append(kx).append(";\n");
            }
        }
        out.append(b).append("  htGid = nGid; htMask = nMask; cap = ncap; htFill = (int) (cap * 0.75f);\n");
        out.append(b).append("}\n");
        out.append(indent).append("}\n");
        for (int a = 0; a < aggregates.size(); a++) {
            emitAggregateUpdate(out, indent, aggregates.get(a), cells(aggregates, a, "agg", "gid"), resolver, nullResolver, stringMaskIds);
        }
    }

    private static void emitGroupedResult(StringBuilder out, Plan.Pipeline pipeline, boolean[][] nullable, boolean speculate, int reconstructDictColumn, List<Type> resultTypes)
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
            emitResultTypes(out, "      ", resultTypes);
            out.append("      return applyProjection(applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(arrayGroupCount, result, types))));\n");
            out.append("    }\n");
            out.append("    long[][] result = new long[").append(1 + aggregateCount).append("][];\n");
            emitKeyResultColumn(out, "    ", 0, 0, reconstructDictColumn, "groupCount");
            emitAggregateResultColumns(out, "    ", 1, aggregates);
            emitResultTypes(out, "    ", resultTypes);
            out.append("    return applyProjection(applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(groupCount, result, types))));\n");
            return;
        }
        int keyCount = pipeline.groupKeys().size();
        out.append("    long[][] result = new long[").append(keyCount + aggregateCount).append("][];\n");
        for (int kx = 0; kx < keyCount; kx++) {
            emitKeyResultColumn(out, "    ", kx, kx, kx == 0 ? reconstructDictColumn : -1, "groupCount");
        }
        emitAggregateResultColumns(out, "    ", keyCount, aggregates);
        emitResultTypes(out, "    ", resultTypes);
        if (anyGroupKeyNullable(pipeline, nullable) || anyAggregateNullable(pipeline)) {
            out.append("    boolean[][] resultNulls = new boolean[").append(keyCount + aggregateCount).append("][];\n");
            for (int kx = 0; kx < keyCount; kx++) {
                if (keyNullable(pipeline, nullable, kx)) {
                    out.append("    resultNulls[").append(kx).append("] = java.util.Arrays.copyOf(nullByGid").append(kx).append(", groupCount);\n");
                }
            }
            for (int a = 0; a < aggregateCount; a++) {
                String resultNull = aggregator(aggregates.get(a)).resultNull(cells(aggregates, a, "agg", "g"));
                if (resultNull != null) {
                    out.append("    boolean[] aggNull").append(a).append(" = new boolean[groupCount];\n");
                    out.append("    for (int g = 0; g < groupCount; g++) { aggNull").append(a).append("[g] = ").append(resultNull).append("; }\n");
                    out.append("    resultNulls[").append(keyCount + a).append("] = aggNull").append(a).append(";\n");
                }
            }
            out.append("    return applyProjection(applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(groupCount, result, types, resultNulls))));\n");
            return;
        }
        out.append("    return applyProjection(applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(groupCount, result, types))));\n");
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
    private static String keyCompare(String slot, int keyCount, Plan.Pipeline pipeline, boolean[][] nullable)
    {
        StringBuilder compare = new StringBuilder();
        for (int kx = 0; kx < keyCount; kx++) {
            if (kx > 0) {
                compare.append(" && ");
            }
            compare.append("htKey").append(kx).append("[").append(slot).append("] == gk").append(kx);
            if (keyNullable(pipeline, nullable, kx)) {
                compare.append(" && htKeyN").append(kx).append("[").append(slot).append("] == gkN").append(kx);
            }
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
    private static String input(Plan.Aggregate aggregate, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        return aggregate.input() == null ? null : expr(aggregate.input(), resolver, nullResolver, stringMaskIds);
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

    /** Emit the {@code Type[] types} literal for the result columns, each resolved by name from {@link Types}. */
    private static void emitResultTypes(StringBuilder out, String indent, List<Type> resultTypes)
    {
        StringBuilder elements = new StringBuilder();
        for (Type type : resultTypes) {
            elements.append(elements.length() == 0 ? "" : ", ").append("org.weakref.nitro.jit.Types.get(\"").append(type.name()).append("\")");
        }
        out.append(indent).append("org.weakref.nitro.jit.Type[] types = new org.weakref.nitro.jit.Type[] { ").append(elements).append(" };\n");
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
        // A projection-only pipeline's projections are over the input columns (they define the output), so they
        // must be loaded too. (For an aggregating pipeline, projections are over result columns -- not inputs.)
        if (projectionOnly(pipeline)) {
            for (Plan.Expr projection : pipeline.projections()) {
                collectColumns(projection, referenced);
            }
        }
        return referenced;
    }

    /** Columns the aggregation body reads: the group keys and the aggregate inputs (the "payload" of a filtering scan). */
    private static TreeSet<Integer> accumulateColumns(Plan.Pipeline pipeline)
    {
        TreeSet<Integer> columns = new TreeSet<>();
        for (Plan.Expr groupKey : pipeline.groupKeys()) {
            collectColumns(groupKey, columns);
        }
        for (Plan.Aggregate aggregate : pipeline.aggregates()) {
            if (aggregate.input() != null) {
                collectColumns(aggregate.input(), columns);
            }
        }
        return columns;
    }

    /** Order top-level (AND-ed) conjuncts most-selective-first by a static heuristic; AND is commutative so this is safe. */
    private static List<Plan.Condition> orderBySelectivity(List<Plan.Condition> conjuncts)
    {
        List<Plan.Condition> ordered = new ArrayList<>(conjuncts);
        ordered.sort(java.util.Comparator.comparingInt(PipelineCompiler::selectivityRank));
        return ordered;
    }

    /**
     * Static selectivity estimate (lower = more selective = evaluated earlier). A real cost model needs column
     * statistics; this orders by predicate shape: equality and single-value membership are assumed most selective,
     * ranges and pattern matches middling, negations and disjunctions least.
     */
    private static int selectivityRank(Plan.Condition condition)
    {
        return switch (condition) {
            case Plan.Predicate predicate -> switch (predicate.op()) {
                case "=", "==" -> 0;
                case "<>", "!=" -> 4;
                default -> 2;   // <, <=, >, >=
            };
            case Plan.StringMatch match -> match.negated() ? 4 : (match.values().size() == 1 ? 0 : 1);
            case Plan.SubstringMatch match -> match.negated() ? 4 : 2;
            case Plan.LikeMatch match -> match.negated() ? 4 : 3;
            case Plan.And ignored -> 1;
            case Plan.Or ignored -> 5;
            case Plan.Not ignored -> 4;
        };
    }

    /** A Java {@code new int[] {...}} literal of the given column indices, for a {@code Source.materialize} call. */
    private static String intArrayLiteral(Iterable<Integer> values)
    {
        StringBuilder literal = new StringBuilder("new int[] {");
        boolean first = true;
        for (int value : values) {
            if (!first) {
                literal.append(", ");
            }
            literal.append(value);
            first = false;
        }
        return literal.append("}").toString();
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
            case Plan.LikeMatch match -> into.add(match.column());
            case Plan.SubstringMatch match -> into.add(match.column());
        }
    }

    /**
     * Whether {@code filter} can be enforced while building the dimension whose combined columns occupy
     * {@code [start, end)}: it must reference at least one column and every referenced column must be that build's.
     * String-match filters qualify -- this dimension's predicate-over-dictionary masks are emitted before its
     * structures.
     */
    private static boolean pushableToBuild(Plan.Condition filter, int start, int end)
    {
        TreeSet<Integer> columns = new TreeSet<>();
        collectConditionColumns(filter, columns);
        if (columns.isEmpty()) {
            return false;
        }
        return columns.first() >= start && columns.last() < end;
    }

    /** Render a boolean condition tree as a Java expression. */
    /** Null-unaware boolean rendering (for contexts whose operands are non-null: HAVING result columns, CASE whens). */
    private static String condition(Plan.Condition condition, IntFunction<String> resolver)
    {
        return condition(condition, resolver, NEVER_NULL);
    }

    /** Map a SQL comparison operator to its Java equivalent ({@code =} -> {@code ==}, {@code <>} -> {@code !=}). */
    private static String comparison(String sqlOperator)
    {
        return switch (sqlOperator) {
            case "=" -> "==";
            case "<>" -> "!=";
            default -> sqlOperator;
        };
    }

    private static String condition(Plan.Condition condition, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        return switch (condition) {
            case Plan.Predicate predicate -> "(" + expr(predicate.left(), resolver, nullResolver) + " " + comparison(predicate.op()) + " " + expr(predicate.right(), resolver, nullResolver) + ")";
            case Plan.And and -> and.conditions().isEmpty() ? "true"
                    : "(" + and.conditions().stream().map(child -> condition(child, resolver, nullResolver)).collect(joining(" && ")) + ")";
            case Plan.Or or -> or.conditions().isEmpty() ? "false"
                    : "(" + or.conditions().stream().map(child -> condition(child, resolver, nullResolver)).collect(joining(" || ")) + ")";
            case Plan.Not not -> "(!" + condition(not.condition(), resolver, nullResolver) + ")";
            case Plan.StringMatch ignored -> throw new UnsupportedOperationException("string match is only supported in WHERE filters");
            case Plan.LikeMatch ignored -> throw new UnsupportedOperationException("string match is only supported in WHERE filters");
            case Plan.SubstringMatch ignored -> throw new UnsupportedOperationException("string match is only supported in WHERE filters");
        };
    }

    private static String expr(Plan.Expr expr, IntFunction<String> resolver)
    {
        return expr(expr, resolver, NEVER_NULL);
    }

    private static String expr(Plan.Expr expr, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        return expr(expr, resolver, nullResolver, Map.of());
    }

    private static String expr(Plan.Expr expr, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        return switch (expr) {
            case Plan.Col col -> resolver.apply(col.index());
            case Plan.Lit lit -> lit.value() + "L";
            case Plan.Bin bin -> ScalarLibrary.get(bin.op()).emit(List.of(expr(bin.left(), resolver, nullResolver, stringMaskIds), expr(bin.right(), resolver, nullResolver, stringMaskIds)));
            case Plan.Call call -> ScalarLibrary.get(call.name()).emit(call.arguments().stream().map(argument -> expr(argument, resolver, nullResolver, stringMaskIds)).toList());
            case Plan.Case kase -> caseExpression(kase, resolver, nullResolver, stringMaskIds);
            case Plan.Coalesce coalesce -> coalesceExpression(coalesce, resolver, nullResolver, stringMaskIds);
        };
    }

    /** Render a CASE as a right-nested conditional, falling through to the default value. */
    private static String caseExpression(Plan.Case kase, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        StringBuilder out = new StringBuilder();
        for (Plan.Case.Branch branch : kase.branches()) {
            // Three-valued WHEN: a branch is taken only when its condition is TRUE -- a NULL operand (e.g. a null
            // column in an arithmetic comparison) yields UNKNOWN, which falls through to the next branch / ELSE.
            // The condition may be a predicate-over-dictionary (e.g. a day-of-week pivot), so it needs the mask ids.
            out.append("(").append(conditionTrue(branch.condition(), resolver, nullResolver, stringMaskIds)).append(" ? ").append(expr(branch.value(), resolver, nullResolver, stringMaskIds)).append(" : ");
        }
        out.append(expr(kase.defaultValue(), resolver, nullResolver, stringMaskIds));
        out.append(")".repeat(kase.branches().size()));
        return out.toString();
    }

    /** Render COALESCE as a right-nested conditional returning the first non-null argument. */
    private static String coalesceExpression(Plan.Coalesce coalesce, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        List<Plan.Expr> arguments = coalesce.arguments();
        // The fallback is the first argument that is never null (it always wins if reached), or the last one.
        int fallback = arguments.size() - 1;
        for (int a = 0; a < arguments.size(); a++) {
            if (nullExpr(arguments.get(a), resolver, nullResolver, stringMaskIds).equals("false")) {
                fallback = a;
                break;
            }
        }
        StringBuilder out = new StringBuilder();
        for (int a = 0; a < fallback; a++) {
            out.append("(").append(nullExpr(arguments.get(a), resolver, nullResolver, stringMaskIds)).append(" ? ");
        }
        out.append(expr(arguments.get(fallback), resolver, nullResolver, stringMaskIds));
        for (int a = fallback - 1; a >= 0; a--) {
            out.append(" : ").append(expr(arguments.get(a), resolver, nullResolver, stringMaskIds)).append(")");
        }
        return out.toString();
    }

    // ---- three-valued (null-aware) expression and condition rendering ----

    /** Boolean expression that is true when {@code expr} evaluates to SQL null. {@code "false"} on the fast path. */
    private static String nullExpr(Plan.Expr expr, IntFunction<String> resolver, IntFunction<String> nullResolver)
    {
        return nullExpr(expr, resolver, nullResolver, Map.of());
    }

    private static String nullExpr(Plan.Expr expr, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        return switch (expr) {
            case Plan.Col col -> nullResolver.apply(col.index());
            case Plan.Lit ignored -> "false";
            case Plan.Bin bin -> orNull(nullExpr(bin.left(), resolver, nullResolver, stringMaskIds), nullExpr(bin.right(), resolver, nullResolver, stringMaskIds));
            case Plan.Call call -> {
                String nulls = "false";
                for (Plan.Expr argument : call.arguments()) {
                    nulls = orNull(nulls, nullExpr(argument, resolver, nullResolver, stringMaskIds));
                }
                yield nulls;
            }
            case Plan.Case kase -> caseNull(kase, resolver, nullResolver, stringMaskIds);
            case Plan.Coalesce coalesce -> {
                // COALESCE is null only when every argument is null.
                String allNull = "true";
                for (Plan.Expr argument : coalesce.arguments()) {
                    allNull = andNull(allNull, nullExpr(argument, resolver, nullResolver, stringMaskIds));
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
    private static String caseNull(Plan.Case kase, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        boolean anyNull = !nullExpr(kase.defaultValue(), resolver, nullResolver, stringMaskIds).equals("false");
        for (Plan.Case.Branch branch : kase.branches()) {
            anyNull |= !nullExpr(branch.value(), resolver, nullResolver, stringMaskIds).equals("false");
        }
        if (!anyNull) {
            return "false";
        }
        StringBuilder out = new StringBuilder();
        for (Plan.Case.Branch branch : kase.branches()) {
            // Selecting which branch's null-ness applies uses the same three-valued WHEN as caseExpression, so the
            // condition may be a predicate-over-dictionary and needs the mask ids.
            out.append("(").append(conditionTrue(branch.condition(), resolver, nullResolver, stringMaskIds)).append(" ? ").append(nullExpr(branch.value(), resolver, nullResolver, stringMaskIds)).append(" : ");
        }
        out.append(nullExpr(kase.defaultValue(), resolver, nullResolver, stringMaskIds));
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
    private static String conditionTrue(Plan.Condition condition, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        switch (condition) {
            case Plan.Predicate predicate -> {
                String guard = andGuards(
                        notNullGuard(nullExpr(predicate.left(), resolver, nullResolver)),
                        notNullGuard(nullExpr(predicate.right(), resolver, nullResolver)));
                String comparison = "(" + expr(predicate.left(), resolver, nullResolver) + " " + comparison(predicate.op()) + " " + expr(predicate.right(), resolver, nullResolver) + ")";
                return guard.isEmpty() ? comparison : "(" + guard + " && " + comparison + ")";
            }
            case Plan.And and -> {
                if (and.conditions().isEmpty()) {
                    return "true";
                }
                return "(" + and.conditions().stream().map(child -> conditionTrue(child, resolver, nullResolver, stringMaskIds)).collect(joining(" && ")) + ")";
            }
            case Plan.Or or -> {
                if (or.conditions().isEmpty()) {
                    return "false";
                }
                return "(" + or.conditions().stream().map(child -> conditionTrue(child, resolver, nullResolver, stringMaskIds)).collect(joining(" || ")) + ")";
            }
            case Plan.Not not -> {
                return conditionFalse(not.condition(), resolver, nullResolver, stringMaskIds);
            }
            case Plan.StringMatch match -> {
                return stringMaskTrue(match, resolver, nullResolver, stringMaskIds);
            }
            case Plan.LikeMatch match -> {
                return stringMaskTrue(match, resolver, nullResolver, stringMaskIds);
            }
            case Plan.SubstringMatch match -> {
                return stringMaskTrue(match, resolver, nullResolver, stringMaskIds);
            }
        }
    }

    /** A predicate-over-dictionary leaf as SQL TRUE: the row's id is in the mask (and the value is not null). */
    private static String stringMaskTrue(Plan.Condition match, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        int column = stringMatchColumn(match);
        String guard = notNullGuard(nullResolver.apply(column));
        String lookup = "sMask" + stringMaskIds.get(match) + "[" + resolver.apply(column) + "]";
        return guard.isEmpty() ? lookup : "(" + guard + " && " + lookup + ")";
    }

    /** Boolean expression that is true when {@code condition} evaluates to SQL FALSE (three-valued logic). */
    private static String conditionFalse(Plan.Condition condition, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        switch (condition) {
            case Plan.Predicate predicate -> {
                String guard = andGuards(
                        notNullGuard(nullExpr(predicate.left(), resolver, nullResolver)),
                        notNullGuard(nullExpr(predicate.right(), resolver, nullResolver)));
                String negated = "!(" + expr(predicate.left(), resolver, nullResolver) + " " + comparison(predicate.op()) + " " + expr(predicate.right(), resolver, nullResolver) + ")";
                return guard.isEmpty() ? negated : "(" + guard + " && " + negated + ")";
            }
            case Plan.And and -> {
                if (and.conditions().isEmpty()) {
                    return "false";
                }
                return "(" + and.conditions().stream().map(child -> conditionFalse(child, resolver, nullResolver, stringMaskIds)).collect(joining(" || ")) + ")";
            }
            case Plan.Or or -> {
                if (or.conditions().isEmpty()) {
                    return "true";
                }
                return "(" + or.conditions().stream().map(child -> conditionFalse(child, resolver, nullResolver, stringMaskIds)).collect(joining(" && ")) + ")";
            }
            case Plan.Not not -> {
                return conditionTrue(not.condition(), resolver, nullResolver, stringMaskIds);
            }
            case Plan.StringMatch match -> {
                return stringMaskFalse(match, resolver, nullResolver, stringMaskIds);
            }
            case Plan.LikeMatch match -> {
                return stringMaskFalse(match, resolver, nullResolver, stringMaskIds);
            }
            case Plan.SubstringMatch match -> {
                return stringMaskFalse(match, resolver, nullResolver, stringMaskIds);
            }
        }
    }

    /** A predicate-over-dictionary leaf as SQL FALSE: the row's id is not in the mask (and the value is not null). */
    private static String stringMaskFalse(Plan.Condition match, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        int column = stringMatchColumn(match);
        String guard = notNullGuard(nullResolver.apply(column));
        String lookup = "!sMask" + stringMaskIds.get(match) + "[" + resolver.apply(column) + "]";
        return guard.isEmpty() ? lookup : "(" + guard + " && " + lookup + ")";
    }
}
