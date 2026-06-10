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
import java.util.Set;
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
        boolean projectionOnly = projectionOnly(pipeline);
        List<Type> resultTypes;
        if (pipeline.window() != null) {
            resultTypes = windowOutputTypes(pipeline, encodings);
        }
        else if (projectionOnly) {
            resultTypes = projectionOutputTypes(pipeline, encodings);
        }
        else {
            resultTypes = outputColumnTypes(pipeline, encodings);
        }
        // Build-sourced string sort keys capture their dictionary up front; a probe-sourced key value-compares
        // only when the streamed column is globally interned (ids stable across batches) -- its dictionary is then
        // re-captured per batch as the backing grows, and the final capture covers every appended row's id.
        Map<Integer, int[]> orderingSources = orderingStringSources(pipeline, resultTypes);
        orderingSources.values().removeIf(source -> source[0] == 0 && !stringIdsCrossBatches(pipeline, source[1]));
        Map<Integer, int[]> probeOrderingSources = new java.util.TreeMap<>();
        Map<Integer, int[]> buildOrderingSources = new java.util.TreeMap<>();
        for (Map.Entry<Integer, int[]> entry : orderingSources.entrySet()) {
            (entry.getValue()[0] == 0 ? probeOrderingSources : buildOrderingSources).put(entry.getKey(), entry.getValue());
        }
        emitOrderingDictionaryFields(out, orderingSources);
        ClassBody body = new ClassBody();
        out.append("  @Override public org.weakref.nitro.jit.CompiledPipeline.Result execute("
                + "org.weakref.nitro.jit.StreamingPipeline.Source source, org.weakref.nitro.jit.Column[][] builds, int[] buildRowCounts) {\n");
        emitOrderingDictionaryCapture(out, buildOrderingSources, input -> "builds[" + (input - 1) + "]");

        if (pipeline.window() != null) {
            // A window is a pipeline breaker: drain all probe batches into a buffer, then run the eager ranking logic.
            emitWindowBodyStreaming(out, pipeline, encodings, nullable, resultTypes);
            out.append("  }\n");
            emitApplyHaving(out, pipeline.having(), resultTypes);
            emitApplyOrdering(out, pipeline.ordering(), resultTypes, orderingSources);
            emitApplyProjection(out, pipeline.projections(), resultTypes);
            out.append("}\n");
            return out.toString();
        }

        if (!pipeline.joins().isEmpty()) {
            emitJoinBody(out, body, pipeline, encodings, nullable, resultTypes, true);
            out.append("  }\n");
            emitApplyHaving(out, pipeline.having(), resultTypes);
            emitApplyOrdering(out, pipeline.ordering(), resultTypes, orderingSources);
            // A projection-only pipeline applied its projections inline (they define the output); no post step.
            emitApplyProjection(out, projectionOnly ? List.of() : pipeline.projections(), resultTypes);
            out.append(body.fields());
            out.append(body.methods());
            out.append("}\n");
            return out.toString();
        }

        boolean grouped = !pipeline.groupKeys().isEmpty();
        // State lives across batches: initialize it once, before the batch loop. A join-less projection-only
        // pipeline (a plain scan-and-project, e.g. a raw union branch) appends to projection output arrays.
        if (grouped) {
            emitGroupedState(out, body, pipeline, nullable, false);
        }
        else if (projectionOnly) {
            emitProjectionState(out, pipeline, nullable);
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
            // The per-batch selection index is transient scratch reused across batches: allocated once, grown
            // geometrically when a batch is larger, and refilled to the identity each batch (the batch is fully
            // processed before the next advance()).
            out.append("    int[] selection = new int[0];\n");
            out.append("    while (source.advance()) {\n");
            out.append("      int rowCount = source.rows();\n");
            out.append("      if (selection.length < rowCount) { selection = new int[org.weakref.nitro.jit.StreamingScratch.grow(selection.length, rowCount)]; }\n");
            out.append("      int selected = rowCount;\n");
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
                    emitStreamingStringConditionPrelude(out, body, pipeline, match, stringMaskIds.get(match));
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
            emitProbeOrderingDictionaryCapture(out, probeOrderingSources, payloadColumns);
            for (Plan.Condition match : aggregateMatches) {
                emitStreamingStringConditionPrelude(out, body, pipeline, match, stringMaskIds.get(match));
            }
            out.append("        for (int i = 0; i < selected; i++) {\n");
            if (grouped) {
                emitGroupedAccumulate(out, body, "          ", pipeline, nullable, resolver, resolver, nullResolver, stringMaskIds, false);
            }
            else if (projectionOnly) {
                emitProjectionAppend(out, "          ", pipeline, encodings, nullable, resolver, nullResolver, stringMaskIds);
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
            emitProbeOrderingDictionaryCapture(out, probeOrderingSources, referencedColumns(pipeline));
            for (Plan.Condition match : stringMatches) {
                emitStreamingStringConditionPrelude(out, body, pipeline, match, stringMaskIds.get(match));
            }
            out.append("      for (int i = 0; i < rowCount; i++) {\n");
            emitRowBody(out, body, "        ", pipeline, encodings, nullable, resolver, resolver, nullResolver, stringMaskIds, grouped, false);
            out.append("      }\n");
            out.append("    }\n");
        }

        if (grouped) {
            emitGroupedResult(out, pipeline, nullable, false, -1, resultTypes);
        }
        else if (projectionOnly) {
            emitProjectionResult(out, pipeline, nullable, resultTypes);
        }
        else {
            emitGlobalResult(out, pipeline.aggregates(), resultTypes);
        }
        out.append("  }\n");
        emitApplyHaving(out, pipeline.having(), resultTypes);
        emitApplyOrdering(out, pipeline.ordering(), resultTypes, orderingSources);
        // A projection-only pipeline applied its projections inline (they define the output); no post step.
        emitApplyProjection(out, projectionOnly ? List.of() : pipeline.projections(), resultTypes);
        out.append(body.fields());
        out.append(body.methods());
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
    /**
     * Out-of-line code for the class being rendered: instance fields and helper methods the body emitters add.
     * HotSpot refuses to compile methods beyond {@code HugeMethodLimit} (8000 bytecodes) -- and force-compiling
     * such a method produces deopt-churning code -- so each join's build construction and each grouping
     * find-or-create moves into its own small method, with the state they share with the probe loop held in
     * instance fields. Every emitted method then compiles cleanly at C2.
     */
    private record ClassBody(StringBuilder fields, StringBuilder methods)
    {
        ClassBody()
        {
            this(new StringBuilder(), new StringBuilder());
        }

        void field(String type, String name)
        {
            String declaration = "  private " + type + " " + name + ";\n";
            if (fields.indexOf(declaration) < 0) {
                fields.append(declaration);
            }
        }
    }

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
        List<Type> resultTypes;
        if (pipeline.window() != null) {
            resultTypes = windowOutputTypes(pipeline, encodings);
        }
        else if (projectionOnly) {
            resultTypes = projectionOutputTypes(pipeline, encodings);
        }
        else {
            resultTypes = outputColumnTypes(pipeline, encodings);
        }
        Map<Integer, int[]> orderingSources = orderingStringSources(pipeline, resultTypes);
        emitOrderingDictionaryFields(out, orderingSources);
        ClassBody body = new ClassBody();
        out.append("  @Override public org.weakref.nitro.jit.CompiledPipeline.Result execute(org.weakref.nitro.jit.Column[][] inputs, int[] rowCounts) {\n");
        emitOrderingDictionaryCapture(out, orderingSources, input -> "inputs[" + input + "]");
        if (pipeline.window() != null) {
            emitWindowBody(out, pipeline, encodings, nullable, resultTypes);
        }
        else if (!pipeline.joins().isEmpty()) {
            emitJoinBody(out, body, pipeline, encodings, nullable, resultTypes);
        }
        else {
            emitScanBody(out, body, pipeline, encodings, nullable, resultTypes);
        }
        out.append("  }\n");
        emitApplyHaving(out, pipeline.having(), resultTypes);
        emitApplyOrdering(out, pipeline.ordering(), resultTypes, orderingSources);
        // A projection-only pipeline applies its projections inline (they define the output), so there is no
        // separate post-aggregation projection step.
        emitApplyProjection(out, projectionOnly ? List.of() : pipeline.projections(), resultTypes);
        out.append(body.fields());
        out.append(body.methods());
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
        // Grouping sets append a trailing grouping_id (the GROUPING() bitmask) column.
        if (!pipeline.groupingSets().isEmpty()) {
            types.add(Types.LONG);
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
        out.append("    int n = result.rowCount(); long[][] cols = result.columns(); boolean[][] hn = result.nulls();\n");
        out.append("    int[] keep = new int[n]; int w = 0;\n");
        out.append("    for (int r = 0; r < n; r++) {\n");
        IntFunction<String> havingNull = index -> "(hn != null && hn[" + index + "] != null && hn[" + index + "][r])";
        out.append("      if (").append(condition(having, index -> resultColumnAccess(index, types, "r"), havingNull)).append(") { keep[w++] = r; }\n");
        out.append("    }\n");
        out.append("    long[][] kept = new long[cols.length][w];\n");
        out.append("    for (int i = 0; i < w; i++) { int s = keep[i]; for (int c = 0; c < cols.length; c++) { kept[c][i] = cols[c][s]; } }\n");
        out.append("    int outN = w;\n");
        emitGatherNulls(out, "keep[g]");
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(w, kept, result.types(), outNulls);\n");
        out.append("  }\n");
    }

    /**
     * The source input column behind each STRING-typed ordering sort key, as {@code resultColumn -> {input, column}}:
     * a string result column is either a group key or a passthrough projection, both bare {@code Col} references into
     * the combined input space, which splits into (probe, build...) positions. Lets the ordering comparator read the
     * column's dictionary entries instead of relying on id order. Empty for pipelines without an ordering; entries
     * whose source the execution path cannot reach (a streamed probe carries no dictionaries) are skipped by the
     * caller.
     */
    private static Map<Integer, int[]> orderingStringSources(Plan.Pipeline pipeline, List<Type> resultTypes)
    {
        Map<Integer, int[]> sources = new java.util.HashMap<>();
        if (pipeline.ordering() == null) {
            return sources;
        }
        for (Plan.SortKey key : pipeline.ordering().keys()) {
            if (key.expr() != null) {
                continue;
            }
            int column = key.column();
            if (column >= resultTypes.size() || resultTypes.get(column) != Types.STRING) {
                continue;
            }
            Integer combined = null;
            if (pipeline.window() != null) {
                // Window pipelines order over the window result: input columns pass through positionally.
                if (column < pipeline.columnCount()) {
                    combined = column;
                }
            }
            else if (projectionOnly(pipeline)) {
                if (column < pipeline.projections().size() && pipeline.projections().get(column) instanceof Plan.Col col) {
                    combined = col.index();
                }
            }
            else if (column < pipeline.groupKeys().size() && pipeline.groupKeys().get(column) instanceof Plan.Col col) {
                combined = col.index();
            }
            if (combined == null) {
                continue;
            }
            int input = 0;
            int offset = pipeline.columnCount();
            int local = combined;
            for (Plan.Join join : pipeline.joins()) {
                if (combined < offset) {
                    break;
                }
                input++;
                local = combined - offset;
                offset += join.build().columnCount();
            }
            sources.put(column, new int[] {input, local});
        }
        return sources;
    }

    /** Emit the per-sort-key dictionary fields and their capture assignments (at the top of {@code execute}). */
    private static void emitOrderingDictionaryCapture(StringBuilder out, Map<Integer, int[]> sources, java.util.function.IntFunction<String> inputAccess)
    {
        for (Map.Entry<Integer, int[]> entry : sources.entrySet()) {
            int[] source = entry.getValue();
            out.append("    orderingDictionary").append(entry.getKey()).append(" = ((org.weakref.nitro.jit.Column.StringColumn) ")
                    .append(inputAccess.apply(source[0])).append("[").append(source[1]).append("]).dictionary();\n");
        }
    }

    /** Per-batch re-capture of a globally-interned streamed probe column's dictionary for a string sort key. */
    private static void emitProbeOrderingDictionaryCapture(StringBuilder out, Map<Integer, int[]> probeSources, java.util.Set<Integer> loadedColumns)
    {
        for (Map.Entry<Integer, int[]> entry : probeSources.entrySet()) {
            int column = entry.getValue()[1];
            if (loadedColumns.contains(column)) {
                out.append("      orderingDictionary").append(entry.getKey()).append(" = cStr").append(column).append(";\n");
            }
        }
    }

    private static void emitOrderingDictionaryFields(StringBuilder out, Map<Integer, int[]> sources)
    {
        for (int column : sources.keySet()) {
            out.append("  private static byte[][] orderingDictionary").append(column).append(";\n");
        }
    }

    /**
     * Post-aggregation ORDER BY / LIMIT applied to the materialized result. Identity when no ordering; otherwise
     * sorts a row-index permutation by the sort keys and gathers (optionally truncated to the limit). A full
     * sort for now; a bounded top-N heap is the perf refinement. A string sort key with a captured source dictionary
     * compares the entries' bytes (value order without depending on dictionary order); other keys compare slots.
     */
    private static void emitApplyOrdering(StringBuilder out, Plan.Ordering ordering, List<Type> types)
    {
        emitApplyOrdering(out, ordering, types, Map.of());
    }

    private static void emitApplyOrdering(StringBuilder out, Plan.Ordering ordering, List<Type> types, Map<Integer, int[]> stringSources)
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
            if (key.expr() != null) {
                // Expression key: evaluate the expression over the result columns for each side and compare as its
                // declared type. This lets an aggregating pipeline ORDER BY a value the final projection computes
                // (e.g. avg(x)) without first projecting -- the same pre-projection columns the projection reads are
                // visible here. Computed keys are non-null (no result-column null mask short-circuit).
                Type type = key.type();
                IntFunction<String> decodeA = index -> types.get(index).decode("cols[" + index + "][a]");
                IntFunction<String> decodeB = index -> types.get(index).decode("cols[" + index + "][b]");
                String valueA = encodeSlot(type, expr(key.expr(), decodeA));
                String valueB = encodeSlot(type, expr(key.expr(), decodeB));
                out.append("      { c = ").append(type.compare(valueA, valueB)).append(";");
                if (key.descending()) {
                    out.append(" c = -c;");
                }
                out.append(" if (c != 0) { return c; } }\n");
                continue;
            }
            int col = key.column();
            String compare = stringSources.containsKey(col)
                    ? "java.util.Arrays.compareUnsigned(orderingDictionary" + col + "[(int) cols[" + col + "][a]], orderingDictionary" + col + "[(int) cols[" + col + "][b]])"
                    : types.get(col).compare("cols[" + col + "][a]", "cols[" + col + "][b]");
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
        // OFFSET skips the first rows of the sorted order; LIMIT then bounds what remains (SQL OFFSET/LIMIT).
        int offset = ordering.offset();
        out.append("    int skip = Math.min(").append(offset).append(", n);\n");
        String limit = ordering.limit() < 0 ? "(n - skip)" : "Math.min(" + ordering.limit() + ", n - skip)";
        out.append("    int outN = ").append(limit).append(";\n");
        out.append("    long[][] sorted = new long[cols.length][outN];\n");
        out.append("    for (int w = 0; w < outN; w++) { int s = order[skip + w]; for (int c2 = 0; c2 < cols.length; c2++) { sorted[c2][w] = cols[c2][s]; } }\n");
        emitGatherNulls(out, "order[skip + g]");
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
        IntFunction<String> columnNull = i -> "(inNulls != null && inNulls[" + i + "] != null && inNulls[" + i + "][r])";
        List<Type> outputTypes = new ArrayList<>();
        for (int p = 0; p < outCount; p++) {
            Plan.Expr projection = projections.get(p);
            Type type = projectionType(projection, inputTypes);
            outputTypes.add(type);
            out.append("    for (int r = 0; r < n; r++) { proj[").append(p).append("][r] = ")
                    .append(encodeSlot(type, expr(projection, decode, columnNull))).append("; }\n");
            if (projection instanceof Plan.Col col) {
                out.append("    if (inNulls != null && inNulls[").append(col.index()).append("] != null) { if (projNulls == null) { projNulls = new boolean[")
                        .append(outCount).append("][]; } projNulls[").append(p).append("] = inNulls[").append(col.index()).append("]; }\n");
            }
            else {
                String nullCondition = computedProjectionNull(projection, decode, columnNull);
                if (nullCondition != null) {
                    out.append("    if (projNulls == null) { projNulls = new boolean[").append(outCount).append("][]; }\n");
                    out.append("    projNulls[").append(p).append("] = new boolean[n];\n");
                    out.append("    for (int r = 0; r < n; r++) { projNulls[").append(p).append("][r] = ").append(nullCondition).append("; }\n");
                }
            }
        }
        emitResultTypes(out, "    ", outputTypes);
        out.append("    return new org.weakref.nitro.jit.CompiledPipeline.Result(n, proj, types, projNulls);\n");
        out.append("  }\n");
    }

    /**
     * Scalar functions whose result is NULL when the denominator (argument index 1) is zero -- the rounding integer
     * divides, matching the interpreted {@code DivideRoundI64} / {@code DivideScaleRoundI64} (which null on a zero
     * divisor rather than producing a value). The compiled kernels return 0 for that case, so the null must be applied
     * to the result column's mask instead.
     */
    private static final Set<String> NULL_ON_ZERO_DENOMINATOR = Set.of("divide_round_i64", "divide_scale_round_i64");

    /**
     * The per-row Java boolean expression under which a computed projection is NULL, or {@code null} when it carries no
     * computed null. A rounding divide is NULL when its denominator is zero or any operand is null (matching the
     * interpreted kernel); {@code columnNull} renders a column operand's per-row null test in the caller's scope.
     */
    private static String computedProjectionNull(Plan.Expr projection, IntFunction<String> decode, IntFunction<String> columnNull)
    {
        if (projection instanceof Plan.NullLit) {
            // A NULL literal projection is unconditionally SQL null -- its value slot is a placeholder 0.
            return "true";
        }
        if (!(projection instanceof Plan.Call call) || !NULL_ON_ZERO_DENOMINATOR.contains(call.name())) {
            return null;
        }
        List<String> terms = new ArrayList<>();
        terms.add("(" + expr(call.arguments().get(1), decode) + " == 0L)");
        for (Plan.Expr argument : call.arguments()) {
            if (argument instanceof Plan.Col column) {
                terms.add(columnNull.apply(column.index()));
            }
        }
        return String.join(" || ", terms);
    }

    /** Inferred type of a projection expression: a column reference keeps its source type; arithmetic is DOUBLE when any operand is DOUBLE, else LONG. */
    private static Type projectionType(Plan.Expr expr, List<Type> inputTypes)
    {
        return switch (expr) {
            case Plan.Col col -> inputTypes.get(col.index());
            case Plan.Lit ignored -> Types.LONG;
            case Plan.LitStr ignored -> Types.STRING;
            case Plan.NullLit ignored -> Types.LONG;
            case Plan.Bin bin -> projectionType(bin.left(), inputTypes) == Types.DOUBLE || projectionType(bin.right(), inputTypes) == Types.DOUBLE ? Types.DOUBLE : Types.LONG;
            case Plan.Call call -> ScalarLibrary.isDoubleResult(call.name()) || call.arguments().stream().anyMatch(a -> projectionType(a, inputTypes) == Types.DOUBLE) ? Types.DOUBLE : Types.LONG;
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
        // A window pipeline is never projection-only: its projections are a post-window SELECT applied after the
        // window appends its column, not the inline output of a bare scan/project.
        return pipeline.window() == null && pipeline.groupKeys().isEmpty() && pipeline.aggregates().isEmpty() && !pipeline.projections().isEmpty();
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

    /**
     * Whether projection {@code p} carries a null mask: a column reference to a nullable source column, or a rounding
     * divide (NULL on a zero denominator). Other computed projections are produced non-null.
     */
    private static boolean projectionCarriesNull(Plan.Pipeline pipeline, boolean[][] nullable, int p)
    {
        return exprCarriesNull(pipeline, nullable, pipeline.projections().get(p));
    }

    /**
     * Whether {@code expr} can evaluate to SQL null, structurally -- the static counterpart of {@link #nullExpr}: a
     * nullable column reference, a divide that is null on a zero denominator, or any compound expression with a
     * null-bearing operand (for CASE, a null-bearing branch value or default; for COALESCE, only when every argument
     * is null-bearing). Drives whether a projection emits a null mask, so the value and null paths stay in lockstep.
     */
    private static boolean exprCarriesNull(Plan.Pipeline pipeline, boolean[][] nullable, Plan.Expr expr)
    {
        return switch (expr) {
            case Plan.Col col -> combinedNullable(pipeline, nullable, col.index());
            case Plan.Lit ignored -> false;
            case Plan.LitStr ignored -> false;
            case Plan.NullLit ignored -> true;
            case Plan.Bin bin -> exprCarriesNull(pipeline, nullable, bin.left()) || exprCarriesNull(pipeline, nullable, bin.right());
            case Plan.Call call -> NULL_ON_ZERO_DENOMINATOR.contains(call.name())
                    || call.arguments().stream().anyMatch(argument -> exprCarriesNull(pipeline, nullable, argument));
            case Plan.Case kase -> exprCarriesNull(pipeline, nullable, kase.defaultValue())
                    || kase.branches().stream().anyMatch(branch -> exprCarriesNull(pipeline, nullable, branch.value()));
            case Plan.Coalesce coalesce -> coalesce.arguments().stream().allMatch(argument -> exprCarriesNull(pipeline, nullable, argument));
        };
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

    private static void emitScanBody(StringBuilder out, ClassBody body, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, List<Type> resultTypes)
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
            emitStringConditionPrelude(out, match, s, column -> "cStr" + column);
        }
        IntFunction<String> resolver = index -> scanAccess(index, encodingOf(encodings, 0, index), "i");
        IntFunction<String> nullResolver = index -> nullAccess(index, encodingOf(encodings, 0, index), nullableOf(nullable, 0, index), "i");
        boolean grouped = !pipeline.groupKeys().isEmpty();
        // A single-key scan group can speculate array mode: estimate the key domain from a sample, bet on a
        // direct-indexed array, and deopt to a hash table if a later key falls outside the bet. A nullable key or
        // a nullable aggregate routes through the null-aware hash path instead (array mode emits no result null mask).
        boolean speculate = grouped && pipeline.groupKeys().size() == 1
                && pipeline.groupingSets().isEmpty()
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
            emitGroupedState(out, body, pipeline, nullable, speculate);
        }
        else {
            emitGlobalState(out, pipeline.aggregates());
        }
        out.append("    for (int i = 0; i < rowCount; i++) {\n");
        emitRowBody(out, body, "      ", pipeline, encodings, nullable, resolver, groupKeyResolver, nullResolver, stringMaskIds, grouped, speculate);
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

    // ---- ranking window (rank / row_number per partition, top-N) ----
    //
    // A window is a pipeline breaker: scan + filter, materialize every surviving row's columns, sort a row-index
    // permutation by (partition columns ascending, then the window's ORDER BY keys), then walk that order assigning
    // each row its rank within its partition and keeping the rows within the rank limit. The result is all input
    // columns ([0, columnCount)) followed by a trailing LONG rank column, in that same global (partition, order)
    // order -- byte-for-byte the operator harness's TopNRankingOperator. A row with a NULL partition column is its
    // own singleton partition (rank 1), because partition equality is value equality and that is false for nulls.

    /**
     * Logical types of a window pipeline's result columns: every input column ({@code [0, columnCount)}) typed by its
     * encoding (STRING for a string-encoded column, else LONG), then a trailing LONG rank column.
     */
    private static List<Type> windowOutputTypes(Plan.Pipeline pipeline, ColumnEncoding[][] encodings)
    {
        List<Type> types = new ArrayList<>();
        for (int column = 0; column < pipeline.columnCount(); column++) {
            types.add(combinedEncoding(pipeline, encodings, column) == ColumnEncoding.STRING ? Types.STRING : Types.LONG);
        }
        // A ranking or partition-aggregate window appends one trailing column; a running window appends one per aggregate.
        int trailing = pipeline.window().runningAggregates().isEmpty() ? 1 : pipeline.window().runningAggregates().size();
        for (int t = 0; t < trailing; t++) {
            types.add(Types.LONG);
        }
        return types;
    }

    private static void emitWindowBody(StringBuilder out, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, List<Type> resultTypes)
    {
        int columnCount = pipeline.columnCount();
        Map<Plan.Condition, Integer> stringMaskIds = emitWindowMaterializationArrays(out, pipeline, nullable, resultTypes);
        IntFunction<String> resolver = index -> scanAccess(index, encodingOf(encodings, 0, index), "i");
        IntFunction<String> nullResolver = index -> nullAccess(index, encodingOf(encodings, 0, index), nullableOf(nullable, 0, index), "i");

        // Eager: the whole input is materialized up front, so load every column once and fill in a single pass.
        out.append("    org.weakref.nitro.jit.Column[] in = inputs[0]; int rowCount = rowCounts[0];\n");
        // Load every input column: all are output (matching the operator's "all source columns + rank").
        for (int column = 0; column < columnCount; column++) {
            emitScanColumnLoad(out, column, encodingOf(encodings, 0, column), nullableOf(nullable, 0, column));
        }
        for (Map.Entry<Plan.Condition, Integer> match : stringMaskIds.entrySet()) {
            emitStringConditionPrelude(out, match.getKey(), match.getValue(), column -> "cStr" + column);
        }
        emitWindowFillLoop(out, "    ", pipeline, nullable, resultTypes, resolver, nullResolver, stringMaskIds);

        emitWindowRankAndGather(out, pipeline, nullable, resultTypes);
    }

    /**
     * Streaming variant of {@link #emitWindowBody}: a window is a pipeline breaker, so drain every probe batch --
     * applying any scan filters -- into the same materialization arrays the eager path builds, then run the identical
     * ranking logic. Reuses the shared fill loop and rank/gather codegen so the two paths produce identical results.
     */
    private static void emitWindowBodyStreaming(StringBuilder out, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, List<Type> resultTypes)
    {
        int columnCount = pipeline.columnCount();
        Map<Plan.Condition, Integer> stringMaskIds = emitWindowMaterializationArrays(out, pipeline, nullable, resultTypes);
        IntFunction<String> resolver = index -> scanAccess(index, encodingOf(encodings, 0, index), "i");
        IntFunction<String> nullResolver = index -> nullAccess(index, encodingOf(encodings, 0, index), nullableOf(nullable, 0, index), "i");

        // Drain the probe batch by batch; the materialization arrays persist across batches and grow as needed.
        out.append("    while (source.advance()) {\n");
        out.append("      int rowCount = source.rows();\n");
        out.append("      org.weakref.nitro.jit.Column[] in = source.columns();\n");
        for (int column = 0; column < columnCount; column++) {
            emitScanColumnLoad(out, column, encodingOf(encodings, 0, column), nullableOf(nullable, 0, column));
        }
        for (Map.Entry<Plan.Condition, Integer> match : stringMaskIds.entrySet()) {
            emitStringConditionPrelude(out, match.getKey(), match.getValue(), column -> "cStr" + column);
        }
        emitWindowFillLoop(out, "      ", pipeline, nullable, resultTypes, resolver, nullResolver, stringMaskIds);
        out.append("    }\n");

        emitWindowRankAndGather(out, pipeline, nullable, resultTypes);
    }

    /**
     * Declare the grow-able per-column materialization arrays ({@code w<c>}/{@code wN<c>}, plus {@code cap}/{@code rows})
     * shared by the eager and streaming window paths, and return the predicate-over-dictionary mask ids (stable across
     * batches because each string filter is reduced once per batch into the same {@code stringMask<id>} variable).
     */
    private static Map<Plan.Condition, Integer> emitWindowMaterializationArrays(StringBuilder out, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes)
    {
        int columnCount = pipeline.columnCount();
        List<Plan.Condition> stringMatches = collectPipelineStringMatches(pipeline);
        Map<Plan.Condition, Integer> stringMaskIds = new IdentityHashMap<>();
        for (int s = 0; s < stringMatches.size(); s++) {
            stringMaskIds.put(stringMatches.get(s), s);
        }
        // Materialize every surviving row's columns (value + null) into per-column arrays.
        out.append("    int cap = 1024; int rows = 0;\n");
        for (int column = 0; column < columnCount; column++) {
            out.append("    long[] w").append(column).append(" = new long[cap];\n");
            if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
                out.append("    boolean[] wN").append(column).append(" = new boolean[cap];\n");
            }
        }
        return stringMaskIds;
    }

    /** Per-batch (or whole-input) fill loop appending each surviving row's columns into the materialization arrays. */
    private static void emitWindowFillLoop(StringBuilder out, String indent, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        int columnCount = pipeline.columnCount();
        out.append(indent).append("for (int i = 0; i < rowCount; i++) {\n");
        String bodyIndent = indent + "  ";
        boolean emitFilter = !pipeline.filters().isEmpty();
        if (emitFilter) {
            out.append(bodyIndent).append("if (").append(conjunction(pipeline.filters(), resolver, nullResolver, stringMaskIds)).append(") {\n");
            bodyIndent = bodyIndent + "  ";
        }
        out.append(bodyIndent).append("if (rows == cap) {\n");
        out.append(bodyIndent).append("  cap *= 2;\n");
        for (int column = 0; column < columnCount; column++) {
            out.append(bodyIndent).append("  w").append(column).append(" = java.util.Arrays.copyOf(w").append(column).append(", cap);\n");
            if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
                out.append(bodyIndent).append("  wN").append(column).append(" = java.util.Arrays.copyOf(wN").append(column).append(", cap);\n");
            }
        }
        out.append(bodyIndent).append("}\n");
        for (int column = 0; column < columnCount; column++) {
            out.append(bodyIndent).append("w").append(column).append("[rows] = ")
                    .append(encodeSlot(resultTypes.get(column), resolver.apply(column))).append(";\n");
            if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
                out.append(bodyIndent).append("wN").append(column).append("[rows] = ").append(nullResolver.apply(column)).append(";\n");
            }
        }
        out.append(bodyIndent).append("rows++;\n");
        if (emitFilter) {
            out.append(indent).append("  }\n");
        }
        out.append(indent).append("}\n");
    }

    /**
     * Sort the materialized rows by {@code (partition, orderBy)}, assign ranks within each partition (keeping
     * {@code rank <= limit}), and gather the kept rows plus the trailing rank column into the result. Shared by the
     * eager and streaming window paths once the {@code w<c>}/{@code wN<c>}/{@code rows} arrays are filled.
     */
    private static void emitWindowRankAndGather(StringBuilder out, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes)
    {
        Plan.Window window = pipeline.window();
        if (!window.runningAggregates().isEmpty()) {
            emitWindowRunningGather(out, pipeline, nullable, resultTypes);
            return;
        }
        if (window.aggregate() != null) {
            emitWindowAggregateGather(out, pipeline, nullable, resultTypes);
            return;
        }
        int columnCount = pipeline.columnCount();
        // Freeze the (grown) materialization arrays into final locals so the sort comparator lambda can capture them.
        for (int column = 0; column < columnCount; column++) {
            out.append("    final long[] fw").append(column).append(" = w").append(column).append(";\n");
            if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
                out.append("    final boolean[] fwN").append(column).append(" = wN").append(column).append(";\n");
            }
        }

        // Sort a row-index permutation by (partition columns ascending, then ORDER BY keys); this is both the
        // grouping order (equal-partition rows become contiguous) and the final output order.
        out.append("    Integer[] order = new Integer[rows];\n");
        out.append("    for (int i = 0; i < rows; i++) { order[i] = i; }\n");
        out.append("    java.util.Arrays.sort(order, (pa, pb) -> {\n");
        out.append("      int c;\n");
        for (int partitionColumn : window.partitionColumns()) {
            emitWindowCompare(out, pipeline, nullable, resultTypes, partitionColumn, false);
        }
        for (Plan.SortKey key : window.orderBy()) {
            emitWindowCompare(out, pipeline, nullable, resultTypes, key.column(), key.descending());
        }
        out.append("      return 0;\n");
        out.append("    });\n");

        // Walk the ordered rows, tracking partition boundaries and assigning ranks, keeping rank <= limit.
        out.append("    long[] outRank = new long[rows]; int[] keep = new int[rows]; int kept = 0;\n");
        out.append("    long partitionRowNumber = 0; long rank = 0; boolean newPartition = true;\n");
        out.append("    int prev = -1;\n");
        out.append("    for (int oi = 0; oi < rows; oi++) {\n");
        out.append("      int r = order[oi];\n");
        // A row is a singleton partition when any partition column is null; otherwise the partition breaks when a
        // partition column differs from the previous row.
        out.append("      boolean nullPartition = ").append(windowNullPartitionTest(window, pipeline, nullable, resultTypes, "r")).append(";\n");
        out.append("      if (prev == -1 || nullPartition || ").append(windowPartitionChanged(window, pipeline, nullable, resultTypes)).append(") {\n");
        out.append("        partitionRowNumber = 1; rank = 1;\n");
        out.append("      }\n");
        out.append("      else {\n");
        out.append("        partitionRowNumber++;\n");
        if (window.function() == Plan.RankFunction.RANK) {
            out.append("        if (").append(windowOrderingChanged(window, pipeline, nullable, resultTypes)).append(") { rank = partitionRowNumber; }\n");
        }
        else {
            out.append("        rank = partitionRowNumber;\n");
        }
        out.append("      }\n");
        String limit = window.rankLimit() < 0 ? "true" : "rank <= " + window.rankLimit() + "L";
        out.append("      if (").append(limit).append(") { outRank[kept] = rank; keep[kept] = r; kept++; }\n");
        out.append("      prev = r;\n");
        out.append("    }\n");

        // Gather kept rows (in order) into the result columns plus the trailing rank column.
        int resultColumnCount = columnCount + 1;
        out.append("    long[][] result = new long[").append(resultColumnCount).append("][kept];\n");
        out.append("    for (int g = 0; g < kept; g++) {\n");
        out.append("      int s = keep[g];\n");
        for (int column = 0; column < columnCount; column++) {
            out.append("      result[").append(column).append("][g] = w").append(column).append("[s];\n");
        }
        out.append("      result[").append(columnCount).append("][g] = outRank[g];\n");
        out.append("    }\n");
        emitResultTypes(out, "    ", resultTypes);
        boolean anyNull = false;
        for (int column = 0; column < columnCount; column++) {
            anyNull |= windowColumnNullable(pipeline, nullable, column, resultTypes);
        }
        if (anyNull) {
            out.append("    boolean[][] resultNulls = new boolean[").append(resultColumnCount).append("][];\n");
            for (int column = 0; column < columnCount; column++) {
                if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
                    out.append("    boolean[] rn").append(column).append(" = new boolean[kept];\n");
                    out.append("    for (int g = 0; g < kept; g++) { rn").append(column).append("[g] = wN").append(column).append("[keep[g]]; }\n");
                    out.append("    resultNulls[").append(column).append("] = rn").append(column).append(";\n");
                }
            }
            out.append("    return applyProjection(applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(kept, result, types, resultNulls))));\n");
            return;
        }
        out.append("    return applyProjection(applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(kept, result, types))));\n");
    }

    /**
     * Partition-aggregate window: sort the materialized rows by the partition columns (so equal-partition rows are
     * contiguous), average the value column over each partition's non-null rows (round-half-up, matching
     * {@code PartitionAverageI64WindowFunction#roundDivide}), and append that average to every row of the partition.
     * Every row is kept; the result is the input columns plus the trailing average column (NULL for an all-null
     * partition). The pipeline's following HAVING/ORDER BY/projection then apply.
     */
    private static void emitWindowAggregateGather(StringBuilder out, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes)
    {
        Plan.Window window = pipeline.window();
        int columnCount = pipeline.columnCount();
        int valueColumn = window.aggregate().inputColumn();
        boolean valueNullable = windowColumnNullable(pipeline, nullable, valueColumn, resultTypes);

        // Freeze the (grown) materialization arrays into final locals so the sort comparator lambda can capture them.
        for (int column = 0; column < columnCount; column++) {
            out.append("    final long[] fw").append(column).append(" = w").append(column).append(";\n");
            if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
                out.append("    final boolean[] fwN").append(column).append(" = wN").append(column).append(";\n");
            }
        }

        // Sort a row-index permutation by the partition columns (ascending) so each partition is a contiguous run.
        out.append("    Integer[] order = new Integer[rows];\n");
        out.append("    for (int i = 0; i < rows; i++) { order[i] = i; }\n");
        out.append("    java.util.Arrays.sort(order, (pa, pb) -> {\n");
        out.append("      int c;\n");
        for (int partitionColumn : window.partitionColumns()) {
            emitWindowCompare(out, pipeline, nullable, resultTypes, partitionColumn, false);
        }
        out.append("      return 0;\n");
        out.append("    });\n");

        // Walk the ordered rows; close each partition at a boundary, average its non-null values, and back-fill the
        // average (aligned to the gather position) for every row of the partition. PARTITION BY groups all null keys
        // together (two nulls share a partition, a null and a non-null do not), matching the operator harness; the
        // sort above already made null keys contiguous (nulls compare last and equal).
        out.append("    long[] outAgg = new long[rows]; boolean[] outAggNull = new boolean[rows];\n");
        out.append("    int pStart = 0;\n");
        out.append("    for (int oi = 1; oi <= rows; oi++) {\n");
        out.append("      boolean boundary = oi == rows;\n");
        out.append("      if (!boundary) {\n");
        out.append("        int r = order[oi]; int prev = order[oi - 1];\n");
        out.append("        boundary = ").append(windowPartitionChangedNullsEqual(window, pipeline, nullable, resultTypes)).append(";\n");
        out.append("      }\n");
        out.append("      if (boundary) {\n");
        out.append("        long sum = 0; long cnt = 0;\n");
        out.append("        for (int k = pStart; k < oi; k++) {\n");
        out.append("          int rr = order[k];\n");
        if (valueNullable) {
            out.append("          if (!wN").append(valueColumn).append("[rr]) { sum += w").append(valueColumn).append("[rr]; cnt++; }\n");
        }
        else {
            out.append("          sum += w").append(valueColumn).append("[rr]; cnt++;\n");
        }
        out.append("        }\n");
        out.append("        long avg = 0; boolean has = cnt > 0;\n");
        if (window.aggregate().function().equals("sum")) {
            // Partition SUM: broadcast the partition's total (over non-null values) to every row; NULL if no values.
            out.append("        if (has) { avg = sum; }\n");
        }
        else {
            // Partition AVG: round half away from zero, matching the operator's integer-average semantics.
            out.append("        if (has) { long pn = sum >= 0 ? sum : -sum; long rounded = (pn + (cnt / 2)) / cnt; avg = sum < 0 ? -rounded : rounded; }\n");
        }
        out.append("        for (int k = pStart; k < oi; k++) { outAgg[k] = avg; outAggNull[k] = !has; }\n");
        out.append("        pStart = oi;\n");
        out.append("      }\n");
        out.append("    }\n");

        // Gather all rows (in partition order) into the result columns plus the trailing average column.
        int resultColumnCount = columnCount + 1;
        out.append("    int kept = rows;\n");
        out.append("    long[][] result = new long[").append(resultColumnCount).append("][kept];\n");
        out.append("    for (int g = 0; g < kept; g++) {\n");
        out.append("      int s = order[g];\n");
        for (int column = 0; column < columnCount; column++) {
            out.append("      result[").append(column).append("][g] = w").append(column).append("[s];\n");
        }
        out.append("      result[").append(columnCount).append("][g] = outAgg[g];\n");
        out.append("    }\n");
        emitResultTypes(out, "    ", resultTypes);
        out.append("    boolean[][] resultNulls = new boolean[").append(resultColumnCount).append("][];\n");
        for (int column = 0; column < columnCount; column++) {
            if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
                out.append("    boolean[] rn").append(column).append(" = new boolean[kept];\n");
                out.append("    for (int g = 0; g < kept; g++) { rn").append(column).append("[g] = wN").append(column).append("[order[g]]; }\n");
                out.append("    resultNulls[").append(column).append("] = rn").append(column).append(";\n");
            }
        }
        out.append("    resultNulls[").append(columnCount).append("] = outAggNull;\n");
        out.append("    return applyProjection(applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(kept, result, types, resultNulls))));\n");
    }

    /**
     * Running (cumulative) window: sort the rows by (partition, ORDER BY), then for each running aggregate walk each
     * partition keeping a running value over its non-null inputs (reset at every partition boundary; a null partition
     * column is a singleton) and assign every row its value up to and including itself. Every row is kept; the result is
     * the input columns plus one trailing column per running aggregate (NULL until the partition's first non-null).
     */
    private static void emitWindowRunningGather(StringBuilder out, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes)
    {
        Plan.Window window = pipeline.window();
        int columnCount = pipeline.columnCount();
        List<Plan.WindowAggregate> aggregates = window.runningAggregates();

        for (int column = 0; column < columnCount; column++) {
            out.append("    final long[] fw").append(column).append(" = w").append(column).append(";\n");
            if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
                out.append("    final boolean[] fwN").append(column).append(" = wN").append(column).append(";\n");
            }
        }

        // Sort by (partition columns ascending, then the ORDER BY keys): partitions become contiguous, in cumulative order.
        out.append("    Integer[] order = new Integer[rows];\n");
        out.append("    for (int i = 0; i < rows; i++) { order[i] = i; }\n");
        out.append("    java.util.Arrays.sort(order, (pa, pb) -> {\n");
        out.append("      int c;\n");
        for (int partitionColumn : window.partitionColumns()) {
            emitWindowCompare(out, pipeline, nullable, resultTypes, partitionColumn, false);
        }
        for (Plan.SortKey key : window.orderBy()) {
            emitWindowCompare(out, pipeline, nullable, resultTypes, key.column(), key.descending());
        }
        out.append("      return 0;\n");
        out.append("    });\n");

        for (int a = 0; a < aggregates.size(); a++) {
            out.append("    long[] outRun").append(a).append(" = new long[rows]; boolean[] outRunNull").append(a).append(" = new boolean[rows];\n");
            out.append("    long run").append(a).append(" = 0; boolean has").append(a).append(" = false;\n");
        }
        out.append("    int prev = -1;\n");
        out.append("    for (int oi = 0; oi < rows; oi++) {\n");
        out.append("      int r = order[oi];\n");
        out.append("      boolean nullPartition = ").append(windowNullPartitionTest(window, pipeline, nullable, resultTypes, "r")).append(";\n");
        out.append("      if (prev == -1 || nullPartition || ").append(windowPartitionChanged(window, pipeline, nullable, resultTypes)).append(") {\n");
        for (int a = 0; a < aggregates.size(); a++) {
            out.append("        has").append(a).append(" = false;\n");
        }
        out.append("      }\n");
        for (int a = 0; a < aggregates.size(); a++) {
            int valueColumn = aggregates.get(a).inputColumn();
            boolean valueNullable = windowColumnNullable(pipeline, nullable, valueColumn, resultTypes);
            String guard = valueNullable ? "!wN" + valueColumn + "[r]" : "true";
            String combine = runningCombine(aggregates.get(a).function(), "run" + a, "w" + valueColumn + "[r]", "has" + a);
            out.append("      if (").append(guard).append(") { run").append(a).append(" = ").append(combine).append("; has").append(a).append(" = true; }\n");
            out.append("      outRun").append(a).append("[oi] = has").append(a).append(" ? run").append(a).append(" : 0; outRunNull").append(a).append("[oi] = !has").append(a).append(";\n");
        }
        out.append("      prev = r;\n");
        out.append("    }\n");

        int resultColumnCount = columnCount + aggregates.size();
        out.append("    long[][] result = new long[").append(resultColumnCount).append("][rows];\n");
        out.append("    for (int g = 0; g < rows; g++) {\n");
        out.append("      int s = order[g];\n");
        for (int column = 0; column < columnCount; column++) {
            out.append("      result[").append(column).append("][g] = w").append(column).append("[s];\n");
        }
        for (int a = 0; a < aggregates.size(); a++) {
            out.append("      result[").append(columnCount + a).append("][g] = outRun").append(a).append("[g];\n");
        }
        out.append("    }\n");
        emitResultTypes(out, "    ", resultTypes);
        out.append("    boolean[][] resultNulls = new boolean[").append(resultColumnCount).append("][];\n");
        for (int column = 0; column < columnCount; column++) {
            if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
                out.append("    boolean[] rn").append(column).append(" = new boolean[rows];\n");
                out.append("    for (int g = 0; g < rows; g++) { rn").append(column).append("[g] = wN").append(column).append("[order[g]]; }\n");
                out.append("    resultNulls[").append(column).append("] = rn").append(column).append(";\n");
            }
        }
        for (int a = 0; a < aggregates.size(); a++) {
            out.append("    resultNulls[").append(columnCount + a).append("] = outRunNull").append(a).append(";\n");
        }
        out.append("    return applyProjection(applyOrdering(applyHaving(new org.weakref.nitro.jit.CompiledPipeline.Result(rows, result, types, resultNulls))));\n");
    }

    /** The running-aggregate combine of {@code accumulator} with {@code value}: the new value for the first row, else folded. */
    private static String runningCombine(String function, String accumulator, String value, String has)
    {
        return switch (function) {
            case "max" -> has + " ? Math.max(" + accumulator + ", " + value + ") : " + value;
            case "min" -> has + " ? Math.min(" + accumulator + ", " + value + ") : " + value;
            case "sum" -> has + " ? " + accumulator + " + " + value + " : " + value;
            default -> throw new UnsupportedOperationException("running window aggregate: " + function);
        };
    }

    /** A window output column carries nulls when its (nullable) source column does -- the rank column never does. */
    private static boolean windowColumnNullable(Plan.Pipeline pipeline, boolean[][] nullable, int column, List<Type> resultTypes)
    {
        return combinedNullable(pipeline, nullable, column);
    }

    /** Emit one comparator clause over materialized window columns {@code wC[pa]} vs {@code wC[pb]}, null-aware. */
    private static void emitWindowCompare(StringBuilder out, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes, int column, boolean descending)
    {
        String compare = resultTypes.get(column).compare("fw" + column + "[pa]", "fw" + column + "[pb]");
        if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
            // Mirror OperatorOrderingSemantics: a null compares greater (ascending puts nulls last; the descending
            // flip then yields nulls first), and two nulls are equal.
            out.append("      { boolean an = fwN").append(column).append("[pa]; boolean bn = fwN").append(column).append("[pb];\n");
            out.append("        if (an || bn) { c = (an == bn) ? 0 : (an ? 1 : -1); } else { c = ").append(compare).append("; }");
        }
        else {
            out.append("      { c = ").append(compare).append(";");
        }
        if (descending) {
            out.append(" c = -c;");
        }
        out.append(" if (c != 0) { return c; } }\n");
    }

    /** True when row {@code rowVar} has a null in any partition column (so it is a singleton partition). */
    private static String windowNullPartitionTest(Plan.Window window, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes, String rowVar)
    {
        StringBuilder test = new StringBuilder("false");
        for (int partitionColumn : window.partitionColumns()) {
            if (windowColumnNullable(pipeline, nullable, partitionColumn, resultTypes)) {
                test.append(" || wN").append(partitionColumn).append("[").append(rowVar).append("]");
            }
        }
        return test.toString();
    }

    /** True when the current row {@code r} differs from {@code prev} on any partition column (value equality, null-aware). */
    private static String windowPartitionChanged(Plan.Window window, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes)
    {
        return windowValuesDiffer(window.partitionColumns(), pipeline, nullable, resultTypes);
    }

    /**
     * True when the current row {@code r} is in a different partition than {@code prev} under grouping equality:
     * two nulls are EQUAL (PARTITION BY groups all null keys together, like the operator harness's window), a null
     * and a value differ. Used by the partition-aggregate gather; the ranking walk keeps its singleton-null rule.
     */
    private static String windowPartitionChangedNullsEqual(Plan.Window window, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes)
    {
        int[] columns = window.partitionColumns();
        if (columns.length == 0) {
            return "false";
        }
        StringBuilder differ = new StringBuilder();
        for (int column : columns) {
            if (differ.length() > 0) {
                differ.append(" || ");
            }
            if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
                differ.append("(wN").append(column).append("[r] != wN").append(column).append("[prev]")
                        .append(" || (!wN").append(column).append("[r] && w").append(column).append("[r] != w").append(column).append("[prev]))");
            }
            else {
                differ.append("(w").append(column).append("[r] != w").append(column).append("[prev])");
            }
        }
        return "(" + differ + ")";
    }

    /** True when the current row {@code r} differs from {@code prev} on any ORDER BY key (drives RANK ties). */
    private static String windowOrderingChanged(Plan.Window window, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes)
    {
        int[] columns = new int[window.orderBy().size()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = window.orderBy().get(i).column();
        }
        return windowValuesDiffer(columns, pipeline, nullable, resultTypes);
    }

    /**
     * Disjunction that is true when row {@code r} differs from row {@code prev} on any of {@code columns}, by value
     * equality. Matches {@code OperatorEqualitySemantics}: two nulls are NOT equal (so they count as differing) and
     * a null differs from a value.
     */
    private static String windowValuesDiffer(int[] columns, Plan.Pipeline pipeline, boolean[][] nullable, List<Type> resultTypes)
    {
        if (columns.length == 0) {
            return "false";
        }
        StringBuilder differ = new StringBuilder();
        for (int column : columns) {
            if (differ.length() > 0) {
                differ.append(" || ");
            }
            if (windowColumnNullable(pipeline, nullable, column, resultTypes)) {
                differ.append("(wN").append(column).append("[r] || wN").append(column).append("[prev]")
                        .append(" || w").append(column).append("[r] != w").append(column).append("[prev])");
            }
            else {
                differ.append("(w").append(column).append("[r] != w").append(column).append("[prev])");
            }
        }
        return "(" + differ + ")";
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
                out.append("    int cStrLen").append(column).append(" = ((org.weakref.nitro.jit.Column.StringColumn) in[").append(column).append("]).dictionarySize();\n");
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
        if (condition instanceof Plan.StringMatch || condition instanceof Plan.LikeMatch || condition instanceof Plan.SubstringMatch
                || condition instanceof Plan.StringColumnCompare) {
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
            case Plan.LitStr ignored -> {}
            case Plan.NullLit ignored -> {}
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
        out.append("    boolean[] sMask").append(id).append(" = new boolean[").append(dictionaryVar).append(".length];\n");
        emitStringMaskFill(out, match, id, dictionaryVar, "0", dictionaryVar + ".length");
    }

    /**
     * As {@link #emitStringMaskPrelude}, but for a streamed column whose dictionary is a per-query global intern
     * (entries only ever appended, ids stable across batches): the mask lives in fields and each batch evaluates
     * only the entries added since the last one, so the total per-query mask work is one pass over the final
     * dictionary rather than one pass per batch.
     */
    private static void emitIncrementalStringMaskPrelude(StringBuilder out, ClassBody body, Plan.Condition match, int id, String dictionaryVar, String sizeVar)
    {
        body.field("boolean[]", "sMask" + id);
        body.field("int", "sMaskLen" + id);
        out.append("    if (sMask").append(id).append(" == null) { sMask").append(id).append(" = new boolean[0]; }\n");
        out.append("    if (").append(sizeVar).append(" > sMaskLen").append(id).append(") {\n");
        out.append("      if (sMask").append(id).append(".length < ").append(sizeVar).append(") { sMask").append(id)
                .append(" = java.util.Arrays.copyOf(sMask").append(id).append(", Math.max(").append(sizeVar)
                .append(", sMask").append(id).append(".length * 2)); }\n");
        emitStringMaskFill(out, match, id, dictionaryVar, "sMaskLen" + id, sizeVar);
        out.append("      sMaskLen").append(id).append(" = ").append(sizeVar).append(";\n");
        out.append("    }\n");
    }

    /** Fill {@code sMask<id>[from, to)} by testing the dictionary entries, declaring the match's literals locally. */
    private static void emitStringMaskFill(StringBuilder out, Plan.Condition match, int id, String dictionaryVar, String from, String to)
    {
        if (match instanceof Plan.LikeMatch like) {
            out.append("    java.util.regex.Pattern sLikePat").append(id).append(" = org.weakref.nitro.jit.StringMatching.likePattern(")
                    .append(javaStringLiteral(like.pattern())).append(");\n");
            out.append("    for (int e = ").append(from).append("; e < ").append(to).append("; e++) {\n");
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
        out.append("    for (int e = ").append(from).append("; e < ").append(to).append("; e++) {\n");
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
            int[] buildOffset, int probeColumns, int joinCount, ColumnEncoding[][] encodings, IntFunction<String> resolver, IntFunction<String> nullResolver,
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
            if (joins.get(k).cross()) {
                // Cross / nested-loop join: no key, so pair the probe row with every build row (a scalar-subquery
                // build has a single row, so this is one iteration). Build columns are read at the loop variable.
                out.append(indent).append("for (int buildRow").append(k).append(" = 0; buildRow").append(k).append(" < build").append(k).append("Rows; buildRow").append(k).append("++) {\n");
                indent += "  ";
                openBraces++;
                List<Plan.Condition> crossLevel = filtersByLevel.get(k);
                if (crossLevel != null) {
                    out.append(indent).append("if (").append(conjunction(crossLevel, resolver, nullResolver, stringMaskIds)).append(") {\n");
                    indent += "  ";
                    openBraces++;
                }
                continue;
            }
            emitProbeLookup(out, indent, k, joins.get(k), pipeline, encodings, resolver, nullResolver);
            // An inner join drops a probe row with no match; a left join keeps it (build columns read NULL); an
            // anti-join (NOT EXISTS) keeps only the rows with no match (its build contributes no columns).
            if (joins.get(k).anti()) {
                out.append(indent).append("if (buildRow").append(k).append(" == -1) {\n");
                indent += "  ";
                openBraces++;
            }
            else if (joins.get(k).semi()) {
                // EXISTS / semi-join: keep each matching probe row exactly once (no fan-out over the build chain),
                // whether or not the build key repeats. The build contributes no columns downstream.
                out.append(indent).append("if (buildRow").append(k).append(" != -1) {\n");
                indent += "  ";
                openBraces++;
            }
            else if (!joins.get(k).outer()) {
                // Inner join: emit a row for EVERY matching build row by walking the key's chain (emitProbeLookup left
                // buildRow<k> at the chain head, -1 if no match). A unique build has buildNext<k> == null, so the loop
                // runs exactly once -- identical to the former single-match `if`. A one-to-many build fans out here.
                out.append(indent).append("for ( ; buildRow").append(k).append(" != -1; buildRow").append(k)
                        .append(" = (buildNext").append(k).append(" == null ? -1 : buildNext").append(k).append("[buildRow").append(k).append("])) {\n");
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

    /**
     * Phase 1 of the late-materialize streaming path, staged per join: stage {@code k} materializes only the probe
     * columns join {@code k} and its same-level filters read -- the first stage over the whole batch, later stages
     * gathered by the selection the earlier joins left -- probes the build, applies the filters that became
     * evaluable, and compacts the surviving (probe row, matched build rows) tuples into the Next buffers, which
     * then swap in. Probe-only filters run in the first stage before any join. A selective early join prunes the
     * decode of every later join key and filter column.
     */
    private static void emitStagedSelection(StringBuilder execute, ClassBody body, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable,
            List<Plan.Join> joins, int[] buildOffset, int probeColumns, List<Plan.Condition> filterMatches, Map<Plan.Condition, Integer> stringMaskIds)
    {
        int joinCount = joins.size();
        Map<Integer, List<Plan.Condition>> filtersByLevel = new java.util.LinkedHashMap<>();
        for (Plan.Condition filter : pipeline.filters()) {
            filtersByLevel.computeIfAbsent(filterLevel(filter, joins, buildOffset, probeColumns), level -> new ArrayList<>()).add(filter);
        }
        for (int k = 0; k < joinCount; k++) {
            boolean first = k == 0;
            String loop = first ? "i" : "j";
            int stage = k;

            // The probe columns this stage decodes: join k's probe-side keys plus the probe columns of the filters
            // that become evaluable here (probe-only filters fold into the first stage).
            TreeSet<Integer> stageColumns = new TreeSet<>();
            for (int key : joins.get(k).probeKeyColumns()) {
                if (key < probeColumns) {
                    stageColumns.add(key);
                }
            }
            List<Plan.Condition> stageFilters = new ArrayList<>();
            if (first && filtersByLevel.containsKey(-1)) {
                stageFilters.addAll(filtersByLevel.get(-1));
            }
            List<Plan.Condition> levelFilters = filtersByLevel.get(k);
            for (Plan.Condition filter : stageFilters) {
                collectProbeConditionColumns(filter, probeColumns, stageColumns);
            }
            if (levelFilters != null) {
                for (Plan.Condition filter : levelFilters) {
                    collectProbeConditionColumns(filter, probeColumns, stageColumns);
                }
            }

            IntFunction<String> stageResolver = index -> {
                if (index < probeColumns) {
                    return joinAccess(combinedEncoding(pipeline, encodings, index), probeVars(index), loop);
                }
                int build = buildOf(joins, buildOffset, index);
                ColumnEncoding encoding = combinedEncoding(pipeline, encodings, index);
                String row = build == stage ? "buildRow" + build : "bsel" + build + "[j]";
                String access = joinAccess(encoding, buildVars(build, index - buildOffset[build]), row);
                return joins.get(build).outer() ? outerValue(row, encoding, access) : access;
            };
            IntFunction<String> stageNullResolver = index -> {
                if (index < probeColumns) {
                    return joinNullAccess(combinedEncoding(pipeline, encodings, index), combinedNullable(pipeline, nullable, index), probeVars(index), loop);
                }
                int build = buildOf(joins, buildOffset, index);
                String row = build == stage ? "buildRow" + build : "bsel" + build + "[j]";
                String nullAccess = joinNullAccess(combinedEncoding(pipeline, encodings, index), combinedNullable(pipeline, nullable, index), buildVars(build, index - buildOffset[build]), row);
                return joins.get(build).outer() ? outerNull(row, nullAccess) : nullAccess;
            };

            // Skip the whole stage once a batch's selection is empty: materializing would borrow (and so decode)
            // this stage's columns for nothing. On a clustered fact most batches die at the first selective stage,
            // so the later joins' key columns are never decoded for them -- the actual decode save, since a
            // selection-gathered materialize still decodes the full batch column before gathering.
            execute.append(first ? "      joinStage" + k + "(source, probeRows);\n"
                    : "      if (selected > 0) { joinStage" + k + "(source, probeRows); }\n");
            StringBuilder out = body.methods();
            out.append("  private void joinStage").append(k)
                    .append("(org.weakref.nitro.jit.StreamingPipeline.Source source, int probeRows) {\n");
            if (!stageColumns.isEmpty()) {
                if (first) {
                    out.append("        org.weakref.nitro.jit.Column[] probe = source.materialize(").append(intArrayLiteral(stageColumns)).append(");\n");
                }
                else {
                    out.append("        org.weakref.nitro.jit.Column[] probe = source.materialize(").append(intArrayLiteral(stageColumns)).append(", selection, selected);\n");
                }
                for (int column : stageColumns) {
                    emitJoinColumnLoad(out, combinedEncoding(pipeline, encodings, column), combinedNullable(pipeline, nullable, column), "probe[" + column + "]", probeVars(column));
                }
                for (Plan.Condition match : filterMatches) {
                    if (match instanceof Plan.StringColumnCompare) {
                        continue;   // column-vs-column needs both dictionaries; its remap/prefix prelude is emitted once after all builds load
                    }
                    if (stringMatchColumn(match) < probeColumns && stageColumns.contains(stringMatchColumn(match))) {
                        emitStringMaskPrelude(out, match, stringMaskIds.get(match), probeVars(stringMatchColumn(match)).stringDict());
                    }
                }
            }
            out.append("        int kept = 0;\n");
            if (first) {
                out.append("        for (int i = 0; i < probeRows; i++) {\n");
            }
            else {
                out.append("        for (int j = 0; j < selected; j++) {\n");
            }
            String indent = "          ";
            int openBraces = 0;
            if (!stageFilters.isEmpty()) {
                out.append(indent).append("if (").append(conjunction(stageFilters, stageResolver, stageNullResolver, stringMaskIds)).append(") {\n");
                indent += "  ";
                openBraces++;
            }
            if (joins.get(k).cross()) {
                out.append(indent).append("for (int buildRow").append(k).append(" = 0; buildRow").append(k).append(" < build").append(k).append("Rows; buildRow").append(k).append("++) {\n");
                indent += "  ";
                openBraces++;
            }
            else {
                emitProbeLookup(out, indent, k, joins.get(k), pipeline, encodings, stageResolver, stageNullResolver);
                if (joins.get(k).anti()) {
                    out.append(indent).append("if (buildRow").append(k).append(" == -1) {\n");
                    indent += "  ";
                    openBraces++;
                }
                else if (joins.get(k).semi()) {
                    out.append(indent).append("if (buildRow").append(k).append(" != -1) {\n");
                    indent += "  ";
                    openBraces++;
                }
                else if (!joins.get(k).outer()) {
                    out.append(indent).append("for ( ; buildRow").append(k).append(" != -1; buildRow").append(k)
                            .append(" = (buildNext").append(k).append(" == null ? -1 : buildNext").append(k).append("[buildRow").append(k).append("])) {\n");
                    indent += "  ";
                    openBraces++;
                }
            }
            if (levelFilters != null) {
                out.append(indent).append("if (").append(conjunction(levelFilters, stageResolver, stageNullResolver, stringMaskIds)).append(") {\n");
                indent += "  ";
                openBraces++;
            }
            // An inner one-to-many or cross join fans one input tuple out to several survivors, so the Next buffers
            // can outgrow the input; semi/anti/left emit at most one survivor per tuple.
            if (joins.get(k).cross() || (!joins.get(k).anti() && !joins.get(k).semi() && !joins.get(k).outer())) {
                out.append(indent).append("if (kept == selectionNext.length) {\n");
                out.append(indent).append("  int grown = kept * 2;\n");
                out.append(indent).append("  selectionNext = java.util.Arrays.copyOf(selectionNext, grown);\n");
                for (int m = 0; m < joinCount; m++) {
                    out.append(indent).append("  bsel").append(m).append("Next = java.util.Arrays.copyOf(bsel").append(m).append("Next, grown);\n");
                }
                out.append(indent).append("}\n");
            }
            out.append(indent).append("selectionNext[kept] = ").append(first ? "i" : "selection[j]").append(";\n");
            for (int m = 0; m < k; m++) {
                out.append(indent).append("bsel").append(m).append("Next[kept] = bsel").append(m).append("[j];\n");
            }
            out.append(indent).append("bsel").append(k).append("Next[kept] = buildRow").append(k).append(";\n");
            out.append(indent).append("kept++;\n");
            for (int brace = 0; brace < openBraces; brace++) {
                indent = indent.substring(2);
                out.append(indent).append("}\n");
            }
            out.append("        }\n");
            out.append("        { int[] t = selection; selection = selectionNext; selectionNext = t; }\n");
            for (int m = 0; m <= k; m++) {
                out.append("        { int[] t = bsel").append(m).append("; bsel").append(m).append(" = bsel").append(m).append("Next; bsel").append(m).append("Next = t; }\n");
            }
            out.append("        selected = kept;\n");
            out.append("  }\n");
        }
    }

    /** Add the probe-side ({@code < probeColumns}) columns {@code condition} reads into {@code into}. */
    private static void collectProbeConditionColumns(Plan.Condition condition, int probeColumns, TreeSet<Integer> into)
    {
        TreeSet<Integer> columns = new TreeSet<>();
        collectConditionColumns(condition, columns);
        for (int column : columns) {
            if (column < probeColumns) {
                into.add(column);
            }
        }
    }

    private static void emitJoinBody(StringBuilder out, ClassBody body, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, List<Type> resultTypes)
    {
        emitJoinBody(out, body, pipeline, encodings, nullable, resultTypes, false);
    }

    /**
     * Probe ⋈ builds → filter → aggregate. When {@code streaming}, the build sides are materialized once from
     * {@code builds}/{@code buildRowCounts} into hash tables and the probe is consumed batch-by-batch from a
     * {@link StreamingPipeline.Source} {@code source}; otherwise everything comes materialized from
     * {@code inputs}/{@code rowCounts}. Build-side string-filter masks build once; probe-side masks rebuild per
     * batch (the probe's per-batch dictionary).
     */
    private static void emitJoinBody(StringBuilder out, ClassBody body, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, List<Type> resultTypes, boolean streaming)
    {
        int probeColumns = pipeline.columnCount();
        List<Plan.Join> joins = pipeline.joins();
        int joinCount = joins.size();

        // A string join key is remapped by value against a once-materialized dictionary. That is valid on the streaming
        // path only when the key comes from a materialized build (an earlier-joined dimension, e.g. item.i_category):
        // its dictionary is fully built, so the remap is build-time. A string key on the STREAMED FACT itself is not --
        // each advance delivers a fresh, possibly re-dictionarized batch, so its ids have no stable mapping.
        if (streaming && hasStreamedProbeStringJoinKey(pipeline, encodings)) {
            throw new UnsupportedOperationException("string-keyed join on a streamed fact column is not supported");
        }

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
        // Each join's construction compiles into its own method writing instance fields (a seventeen-join build
        // section alone exceeds HotSpot's huge-method limit inline); the probe code reads the same names as fields.
        String buildsArray = streaming ? "builds" : "inputs";
        String buildCounts = streaming ? "buildRowCounts" : "rowCounts";
        int buildBase = streaming ? 0 : 1;
        for (int k = 0; k < joinCount; k++) {
            Plan.Join join = joins.get(k);
            int keyCount = join.build().keyColumns().length;
            if (join.probeKeyColumns().length != keyCount) {
                throw new IllegalArgumentException("join " + k + " key count mismatch: probe " + join.probeKeyColumns().length + " vs build " + keyCount);
            }
            body.field("int", "build" + k + "Rows");
            out.append("    org.weakref.nitro.jit.Column[] build").append(k).append(" = ").append(buildsArray).append("[").append(k + buildBase)
                    .append("]; build").append(k).append("Rows = ").append(buildCounts).append("[").append(k + buildBase).append("];\n");
            out.append("    buildJoin").append(k).append("(build").append(k).append(", build").append(k).append("Rows);\n");

            StringBuilder buildCode = new StringBuilder();
            List<String[]> exports = new ArrayList<>();
            for (int column : buildReferenced.get(k)) {
                int combinedIndex = buildOffset[k] + column;
                ColumnEncoding encoding = combinedEncoding(pipeline, encodings, combinedIndex);
                boolean columnNullable = combinedNullable(pipeline, nullable, combinedIndex);
                emitJoinColumnLoad(buildCode, encoding, columnNullable, "build" + k + "[" + column + "]", buildVars(k, column));
                ColumnVars vars = buildVars(k, column);
                switch (encoding) {
                    case FLAT -> exports.add(new String[] {"long[]", vars.flat()});
                    case STRING -> {
                        exports.add(new String[] {"int[]", vars.ids()});
                        exports.add(new String[] {"byte[][]", vars.stringDict()});
                    }
                    case DICTIONARY -> {
                        exports.add(new String[] {"int[]", vars.ids()});
                        exports.add(new String[] {"long[]", vars.dict()});
                    }
                    case CONSTANT -> exports.add(new String[] {"long", vars.constant()});
                }
                if (columnNullable) {
                    exports.add(new String[] {encoding == ColumnEncoding.CONSTANT ? "boolean" : "boolean[]", vars.nulls()});
                }
            }
            // Build-side predicate-over-dictionary masks for this dimension, emitted before its structures so a
            // pushed string-match filter can read them (their dictionary is now materialized). Includes masks for
            // CASE conditions in aggregate inputs, not just WHERE filters; probe-side masks build per batch below.
            for (int s = 0; s < stringMatches.size(); s++) {
                Plan.Condition match = stringMatches.get(s);
                if (match instanceof Plan.StringColumnCompare) {
                    continue;   // column-vs-column remap needs both dictionaries; emitted once below, after all builds load
                }
                int column = stringMatchColumn(match);
                if (column >= buildOffset[k] && column < buildOffset[k] + join.build().columnCount()) {
                    emitStringMaskPrelude(buildCode, match, s, buildVars(k, column - buildOffset[k]).stringDict());
                    exports.add(new String[] {"boolean[]", "sMask" + s});
                }
            }
            if (!join.cross()) {
                emitBuildStructures(buildCode, k, join.build().keyColumns(), buildFilter[k], pipeline, encodings, nullable, buildOffset);
                exports.add(new String[] {"int[]", "buildNext" + k});
                if (keyCount == 1) {
                    exports.add(new String[] {"long", "minKey" + k});
                    exports.add(new String[] {"long", "maxKey" + k});
                    exports.add(new String[] {"boolean", "useArray" + k});
                    exports.add(new String[] {"int[]", "buildRowByKey" + k});
                }
                for (int kx = 0; kx < keyCount; kx++) {
                    exports.add(new String[] {"long[]", "jKey" + k + "_" + kx});
                }
                exports.add(new String[] {"int[]", "jRow" + k});
                exports.add(new String[] {"int", "jMask" + k});
            }
            body.methods().append("  private void buildJoin").append(k)
                    .append("(org.weakref.nitro.jit.Column[] build").append(k).append(", int build").append(k).append("Rows) {\n");
            body.methods().append(buildCode);
            for (String[] export : exports) {
                body.field(export[0], export[1]);
                body.methods().append("    this.").append(export[1]).append(" = ").append(export[1]).append(";\n");
            }
            body.methods().append("  }\n");
        }

        // A column-vs-column string compare whose BOTH operands are build (dimension) columns can be remapped now that
        // every build dictionary is materialized. Emit it here so the lazy streaming path -- which has no post-probe
        // prelude site -- shares it; compares that touch a probe column are emitted after the probe loads (below).
        for (int s = 0; s < stringMatches.size(); s++) {
            if (stringMatches.get(s) instanceof Plan.StringColumnCompare compare
                    && compare.left() >= probeColumns && compare.right() >= probeColumns) {
                emitStringConditionPrelude(out, streaming ? body : null, compare, s, column ->
                        buildVars(buildOf(joins, buildOffset, column), column - buildOffset[buildOf(joins, buildOffset, column)]).stringDict());
            }
        }

        // A string join key sourced from an earlier build (e.g. item.i_category) has a constant dictionary once the
        // builds are materialized -- emit its value remap here, OUTSIDE any batch loop. Rebuilding a large remap per
        // streamed batch dominated string-keyed fact scans. Fact-sourced string keys (eager path only) remap after
        // the probe loads.
        emitJoinKeyRemaps(out, streaming ? body : null, pipeline, encodings, joins, buildOffset, probeColumns, true);

        boolean grouped = !pipeline.groupKeys().isEmpty();
        boolean projection = projectionOnly(pipeline);
        if (projection) {
            emitProjectionState(out, pipeline, nullable);
        }
        else if (grouped) {
            emitGroupedState(out, body, pipeline, nullable, false);
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
        boolean lateMaterialize = streaming && !payloadProbe.isEmpty();

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
            // The per-batch survivor index (selection) and matched build-row indices (bsel<k>) are transient scratch
            // reused across batches: allocate them once, grow geometrically when a batch needs more capacity, and
            // reset the logical length (selected = 0) each batch. The whole batch is processed before the next
            // advance(), so last-batch's buffer is free to be the next batch's. Each ping-pongs with a Next buffer
            // that the per-join stages compact into; the fan-out grow path reassigns these same locals when one
            // probe row yields more survivors than rows.
            body.field("int[]", "selection");
            body.field("int[]", "selectionNext");
            body.field("int", "selected");
            out.append("    selection = new int[0]; selectionNext = new int[0];\n");
            for (int k = 0; k < joinCount; k++) {
                body.field("int[]", "bsel" + k);
                body.field("int[]", "bsel" + k + "Next");
                out.append("    bsel").append(k).append(" = new int[0]; bsel").append(k).append("Next = new int[0];\n");
            }
            out.append("    while (source.advance()) {\n");
            out.append("      int probeRows = source.rows();\n");
            out.append("      if (selection.length < probeRows) {\n");
            out.append("        int grown = org.weakref.nitro.jit.StreamingScratch.grow(selection.length, probeRows);\n");
            out.append("        selection = new int[grown]; selectionNext = new int[grown];\n");
            for (int k = 0; k < joinCount; k++) {
                out.append("        bsel").append(k).append(" = new int[grown]; bsel").append(k).append("Next = new int[grown];\n");
            }
            out.append("      }\n");
            out.append("      selected = 0;\n");
            // Phase 1, staged per join: each stage materializes only its own probe columns -- gathered by the
            // selection the earlier joins left -- probes, and compacts the surviving (row, build rows) tuples.
            // A selective early join thus prunes the decode of every later join key and filter column, the
            // compiled analogue of the operator scan's constrain() pushback. Each stage compiles into its own
            // method over the field-held selection (seventeen inline stages exceed the huge-method limit).
            emitStagedSelection(out, body, pipeline, encodings, nullable, joins, buildOffset, probeColumns, filterMatches, stringMaskIds);
            // Phase 2: payload probe columns materialized for the survivors only, then folded in. Skip the whole
            // phase when the batch has no survivors -- materializing would borrow (and so decode) the payload columns
            // over the batch for nothing. This is how an operator scan avoids decoding payload for batches a selective
            // join/filter fully prunes (e.g. a date-clustered fact where most batches contain no qualifying rows).
            out.append("      if (selected > 0) {\n");
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
            if (projection) {
                emitProjectionAppend(out, "          ", pipeline, encodings, nullable, lazyResolver, lazyNullResolver, stringMaskIds);
            }
            else if (grouped) {
                emitGroupedAccumulate(out, body, "          ", pipeline, nullable, lazyResolver, lazyResolver, lazyNullResolver, stringMaskIds, false);
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
                if (match instanceof Plan.StringColumnCompare compare) {
                    if (compare.left() >= probeColumns && compare.right() >= probeColumns) {
                        continue;   // build-only: already emitted once after the builds loaded
                    }
                    // A probe column is involved; its dictionary is materialized now (probe just loaded), so emit the
                    // remap here, resolving each operand's dictionary variable from whichever side it lives on.
                    emitStringConditionPrelude(out, match, s, column -> column < probeColumns
                            ? probeVars(column).stringDict()
                            : buildVars(buildOf(joins, buildOffset, column), column - buildOffset[buildOf(joins, buildOffset, column)]).stringDict());
                    continue;
                }
                int column = stringMatchColumn(match);
                if (column < probeColumns) {
                    emitStringMaskPrelude(out, match, s, probeVars(column).stringDict());
                }
            }
            // Build-sourced string keys remapped once after the builds loaded (above); a fact-sourced key's
            // dictionary is materialized with the probe, so its remap belongs here.
            emitJoinKeyRemaps(out, pipeline, encodings, joins, buildOffset, probeColumns, false);

            out.append("    for (int i = 0; i < probeRows; i++) {\n");
            // Filters are interleaved with the probes (early-out); the body then runs without re-checking them.
            // Nullable join inputs carry a null mask; non-nullable columns resolve to the "false" fast path.
            int openBraces = emitProbesWithFilters(out, "      ", pipeline, joins, buildOffset, probeColumns, joinCount, encodings, resolver, nullResolver, stringMaskIds);
            String indent = "      " + "  ".repeat(openBraces);
            emitRowBody(out, body, indent, pipeline, encodings, nullable, resolver, resolver, nullResolver, stringMaskIds, grouped, false, true);
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
    /** The build key's hash/array value array: a string (or dictionary) key is keyed by its dense dict ids, else flat longs. */
    private static String buildKeyArray(int k, int localKey, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, int[] buildOffset)
    {
        ColumnEncoding encoding = combinedEncoding(pipeline, encodings, buildOffset[k] + localKey);
        ColumnVars vars = buildVars(k, localKey);
        return encoding == ColumnEncoding.STRING || encoding == ColumnEncoding.DICTIONARY ? vars.ids() : vars.flat();
    }

    /**
     * Per-row guard that drops a build row whose join key is null: an equi-join never matches on a null key (SQL
     * three-valued), and a null string key canonicalizes to dictionary id 0 -- colliding with the real entry 0 -- so a
     * null build row must not enter the table. Returns the OR of the nullable keys' null masks, or {@code null} when no
     * key is nullable.
     */
    private static String buildKeyNullGuard(int k, int[] buildKeys, Plan.Pipeline pipeline, boolean[][] nullable, int[] buildOffset)
    {
        List<String> terms = new ArrayList<>();
        for (int localKey : buildKeys) {
            if (combinedNullable(pipeline, nullable, buildOffset[k] + localKey)) {
                terms.add(buildVars(k, localKey).nulls() + "[r]");
            }
        }
        return terms.isEmpty() ? null : String.join(" || ", terms);
    }

    private static void emitBuildStructures(StringBuilder out, int k, int[] buildKeys, String buildFilter, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, int[] buildOffset)
    {
        int keyCount = buildKeys.length;
        String rows = "build" + k + "Rows";
        String nullGuard = buildKeyNullGuard(k, buildKeys, pipeline, nullable, buildOffset);
        String skip = nullGuard == null ? "" : "if (" + nullGuard + ") { continue; } ";
        if (buildFilter != null) {
            skip += "if (!(" + buildFilter + ")) { continue; } ";
        }
        // buildNext<k> chains build rows sharing a join key (lazily allocated on the first duplicate key). It stays
        // null when every key is unique -- the common dimension-PK case -- so the probe loop runs exactly once per
        // match and unique-build queries are byte-identical with no chain overhead. A non-unique build (a one-to-many
        // join) populates it so the probe emits every matching build row. The compiler does not assume uniqueness.
        out.append("    int[] buildNext").append(k).append(" = null;\n");
        if (keyCount == 1) {
            String buildKey = buildKeyArray(k, buildKeys[0], pipeline, encodings, buildOffset);
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
            // Array mode: buildRowByKey holds the chain head per key; a duplicate prepends through buildNext (the new
            // row becomes the head, pointing at the previous head). Unique keys never collide, so buildNext stays null.
            out.append("      for (int r = 0; r < ").append(rows).append("; r++) { ").append(skip)
                    .append("int idx = (int) (").append(buildKey).append("[r] - minKey").append(k).append(");")
                    .append(" if (buildRowByKey").append(k).append("[idx] != -1) { if (buildNext").append(k).append(" == null) { buildNext").append(k).append(" = new int[").append(rows).append("]; java.util.Arrays.fill(buildNext").append(k).append(", -1); } buildNext").append(k).append("[r] = buildRowByKey").append(k).append("[idx]; }")
                    .append(" buildRowByKey").append(k).append("[idx] = r; }\n");
            out.append("    }\n    else {\n");
            emitHashBuild(out, "      ", k, buildKeys, buildFilter, pipeline, encodings, nullable, buildOffset);
            out.append("    }\n");
        }
        else {
            for (int kx = 0; kx < keyCount; kx++) {
                out.append("    long[] jKey").append(k).append("_").append(kx).append(" = null;\n");
            }
            out.append("    int[] jRow").append(k).append(" = null; int jMask").append(k).append(" = 0;\n");
            out.append("    {\n");
            emitHashBuild(out, "      ", k, buildKeys, buildFilter, pipeline, encodings, nullable, buildOffset);
            out.append("    }\n");
        }
    }

    /**
     * Open-addressing build for join {@code k}. Each slot owns one distinct key; build rows that share a key are
     * chained through {@code buildNext<k>} (declared by {@link #emitBuildStructures}, lazily allocated on the first
     * duplicate) with the latest row as the chain head in {@code jRow<k>[slot]}. A unique build never collides on key,
     * so {@code buildNext<k>} stays null and the probe runs a single iteration -- byte-identical to a unique-key table.
     */
    private static void emitHashBuild(StringBuilder out, String indent, int k, int[] buildKeys, String buildFilter, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, int[] buildOffset)
    {
        int keyCount = buildKeys.length;
        String rows = "build" + k + "Rows";
        String nullGuard = buildKeyNullGuard(k, buildKeys, pipeline, nullable, buildOffset);
        StringBuilder keyMatch = new StringBuilder();
        for (int kx = 0; kx < keyCount; kx++) {
            keyMatch.append(kx == 0 ? "" : " && ").append("jKey").append(k).append("_").append(kx).append("[slot] == bk").append(kx);
        }
        out.append(indent).append("int jcap = 16; while (jcap * 0.75f < ").append(rows).append(") { jcap <<= 1; }\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("jKey").append(k).append("_").append(kx).append(" = new long[jcap];\n");
        }
        out.append(indent).append("jRow").append(k).append(" = new int[jcap]; java.util.Arrays.fill(jRow").append(k).append(", -1); jMask").append(k).append(" = jcap - 1;\n");
        out.append(indent).append("for (int r = 0; r < ").append(rows).append("; r++) {\n");
        if (nullGuard != null) {
            out.append(indent).append("  if (").append(nullGuard).append(") { continue; }\n");
        }
        if (buildFilter != null) {
            out.append(indent).append("  if (!(").append(buildFilter).append(")) { continue; }\n");
        }
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("  long bk").append(kx).append(" = ").append(buildKeyArray(k, buildKeys[kx], pipeline, encodings, buildOffset)).append("[r];\n");
        }
        out.append(indent).append("  int slot = mix(").append(hashFold("bk", "", keyCount)).append(") & jMask").append(k).append(";\n");
        out.append(indent).append("  while (jRow").append(k).append("[slot] != -1 && !(").append(keyMatch).append(")) { slot = (slot + 1) & jMask").append(k).append("; }\n");
        out.append(indent).append("  if (jRow").append(k).append("[slot] == -1) {\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(indent).append("    jKey").append(k).append("_").append(kx).append("[slot] = bk").append(kx).append(";\n");
        }
        out.append(indent).append("    jRow").append(k).append("[slot] = r;\n");
        out.append(indent).append("  }\n");
        out.append(indent).append("  else {\n");
        out.append(indent).append("    if (buildNext").append(k).append(" == null) { buildNext").append(k).append(" = new int[").append(rows).append("]; java.util.Arrays.fill(buildNext").append(k).append(", -1); }\n");
        out.append(indent).append("    buildNext").append(k).append("[r] = jRow").append(k).append("[slot]; jRow").append(k).append("[slot] = r;\n");
        out.append(indent).append("  }\n");
        out.append(indent).append("}\n");
    }

    /** Per-row lookup for join {@code k}, leaving {@code int buildRow<k>} in scope (-1 = no match). */
    /** Whether any join's probe key column is a dictionary string (needing the value remap before matching). */
    /**
     * Whether any string join key is a column of the streamed fact (probe) itself, rather than of a materialized build.
     * Fact columns have index {@code < columnCount()}; build (dimension) columns come after. Only the former blocks the
     * streaming path -- a build-originated string key (e.g. {@code item.i_category}) remaps against a fully-built
     * dictionary.
     */
    private static boolean hasStreamedProbeStringJoinKey(Plan.Pipeline pipeline, ColumnEncoding[][] encodings)
    {
        for (Plan.Join join : pipeline.joins()) {
            for (int probeKey : join.probeKeyColumns()) {
                if (probeKey < pipeline.columnCount() && combinedEncoding(pipeline, encodings, probeKey) == ColumnEncoding.STRING) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * For each string join key, emit {@code int[] jRemap<k>_<kx>} translating a probe-key dictionary id into the
     * build-key dictionary id of the entry with the same bytes (or {@code -1} when the value is absent from the build).
     * Probe and build carry independent dictionaries, so the join must match by value; the remap reduces that to the
     * existing integer-keyed hash/array probe (the build is keyed by its own dense dict ids).
     */
    private static void emitJoinKeyRemaps(StringBuilder out, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, List<Plan.Join> joins, int[] buildOffset, int probeColumns)
    {
        emitJoinKeyRemaps(out, pipeline, encodings, joins, buildOffset, probeColumns, null);
    }

    /** {@code buildOriginated} selects which keys to emit: TRUE for build-sourced probe keys only (their dictionaries
     * are constant once the builds load, so the remap hoists out of the streaming batch loop), FALSE for fact-sourced
     * keys only (the probe dictionary loads with the probe), {@code null} for all. */
    private static void emitJoinKeyRemaps(StringBuilder out, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, List<Plan.Join> joins, int[] buildOffset, int probeColumns, Boolean buildOriginated)
    {
        emitJoinKeyRemaps(out, null, pipeline, encodings, joins, buildOffset, probeColumns, buildOriginated);
    }

    private static void emitJoinKeyRemaps(StringBuilder out, ClassBody body, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, List<Plan.Join> joins, int[] buildOffset, int probeColumns, Boolean buildOriginated)
    {
        for (int k = 0; k < joins.size(); k++) {
            Plan.Join join = joins.get(k);
            int[] probeKeys = join.probeKeyColumns();
            int[] buildKeys = join.build().keyColumns();
            for (int kx = 0; kx < probeKeys.length; kx++) {
                if (combinedEncoding(pipeline, encodings, probeKeys[kx]) != ColumnEncoding.STRING) {
                    continue;
                }
                if (buildOriginated != null && buildOriginated != (probeKeys[kx] >= probeColumns)) {
                    continue;
                }
                // The probe key may be a fact column (probe dictionary) or an earlier-joined build column (e.g.
                // item.i_category) -- resolve its dictionary variable accordingly so a build-originated string key works.
                int probeKey = probeKeys[kx];
                String probeDictionary = probeKey < probeColumns
                        ? probeVars(probeKey).stringDict()
                        : buildVars(buildOf(joins, buildOffset, probeKey), probeKey - buildOffset[buildOf(joins, buildOffset, probeKey)]).stringDict();
                String buildDictionary = buildVars(k, buildKeys[kx]).stringDict();
                String id = k + "_" + kx;
                out.append("    java.util.HashMap<String, Integer> jBuildIdx").append(id).append(" = new java.util.HashMap<>();\n");
                out.append("    for (int e = 0; e < ").append(buildDictionary).append(".length; e++) {\n");
                out.append("      jBuildIdx").append(id).append(".putIfAbsent(new String(").append(buildDictionary).append("[e], java.nio.charset.StandardCharsets.UTF_8), e);\n");
                out.append("    }\n");
                if (body != null) {
                    body.field("int[]", "jRemap" + id);
                    out.append("    jRemap").append(id).append(" = new int[").append(probeDictionary).append(".length];\n");
                }
                else {
                    out.append("    int[] jRemap").append(id).append(" = new int[").append(probeDictionary).append(".length];\n");
                }
                out.append("    for (int e = 0; e < ").append(probeDictionary).append(".length; e++) {\n");
                out.append("      Integer b = jBuildIdx").append(id).append(".get(new String(").append(probeDictionary).append("[e], java.nio.charset.StandardCharsets.UTF_8));\n");
                out.append("      jRemap").append(id).append("[e] = b == null ? -1 : b;\n");
                out.append("    }\n");
            }
        }
    }

    private static void emitProbeLookup(StringBuilder out, String indent, int k, Plan.Join join, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, IntFunction<String> resolver, IntFunction<String> nullResolver)
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
            // A string key carries the probe's dictionary id; remap it into the build's dictionary id space (by value)
            // so it matches the build hash/array, which is keyed by the build dict id. An absent value remaps to -1,
            // which never matches (array: below minKey; hash: build ids are non-negative).
            String key = combinedEncoding(pipeline, encodings, probeKeys[kx]) == ColumnEncoding.STRING
                    ? "jRemap" + k + "_" + kx + "[(int) (" + resolver.apply(probeKeys[kx]) + ")]"
                    : resolver.apply(probeKeys[kx]);
            out.append(indent).append("long pk").append(k).append("_").append(kx).append(" = ").append(key).append(";\n");
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

    private static void emitRowBody(StringBuilder out, ClassBody body, String indent, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, IntFunction<String> resolver, IntFunction<String> groupKeyResolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds, boolean grouped, boolean speculate)
    {
        emitRowBody(out, body, indent, pipeline, encodings, nullable, resolver, groupKeyResolver, nullResolver, stringMaskIds, grouped, speculate, false);
    }

    /**
     * Emit one surviving row's body (filter then projection / group / global accumulate). When {@code filtersApplied}
     * the WHERE was already applied upstream (interleaved with the join probes for early-out), so it is not re-checked.
     */
    private static void emitRowBody(StringBuilder out, ClassBody body, String indent, Plan.Pipeline pipeline, ColumnEncoding[][] encodings, boolean[][] nullable, IntFunction<String> resolver, IntFunction<String> groupKeyResolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds, boolean grouped, boolean speculate, boolean filtersApplied)
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
            emitGroupedAccumulate(out, body, bodyIndent, pipeline, nullable, resolver, groupKeyResolver, nullResolver, stringMaskIds, speculate);
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
        String dictionary = aggregateInputDictionary(aggregate);
        String guard = aggregate.input() == null ? "false" : nullExpr(aggregate.input(), resolver, nullResolver, stringMaskIds);
        if (guard.equals("false")) {
            aggregator.emitUpdate(out, indent, cells, inputExpr, dictionary);
            return;
        }
        out.append(indent).append("if (!(").append(guard).append(")) {\n");
        aggregator.emitUpdate(out, indent + "  ", cells, inputExpr, dictionary);
        out.append(indent).append("}\n");
    }

    /** The dictionary variable for a string aggregate's input: set when the input is a plain scan column (whose string id the row loop works on), else null. */
    private static String aggregateInputDictionary(Plan.Aggregate aggregate)
    {
        return aggregate.input() instanceof Plan.Col col ? "cStr" + col.index() : null;
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

    // ---- grouping sets / ROLLUP (single-pass EXPAND) ----
    //
    // One aggregation matching the operator harness's GroupId expand: each input row is folded once per grouping
    // set into a hash table keyed by (setId, k0', k1', ...), where an active key keeps its value (and real null
    // flag) and an inactive key is forced null. The leading setId disambiguates two sets that null to the same key
    // when the data has real nulls. The per-set fold is unrolled at compile time (each set is a compile-time
    // bitmask), so the inactive columns are constant-folded to null in the emitted code. Result columns are the
    // group-key columns (null where inactive in the group's set), the aggregate columns, then a trailing
    // grouping_id column (the GROUPING() bitmask: bit i set for each group key i not in the set).

    private static void emitGroupingSetsState(StringBuilder out, ClassBody body, Plan.Pipeline pipeline)
    {
        List<Plan.Aggregate> aggregates = pipeline.aggregates();
        int keyCount = pipeline.groupKeys().size();
        int total = cellCount(aggregates);
        // Interleaved slot records in one array, so a probe touches one or two cache lines instead of one per
        // parallel key array, and an aggregate update lands in the lines the probe just loaded (the dominant cost
        // at multi-million-group scale). Each slot is (keyCount + 2 + cellCount) longs: word 0 packs the mixed
        // hash (high int, the probe fingerprint) with gid + 1 (low int; 0 = empty slot), word 1 packs the setId
        // (high int) with the key null bits (low; an inactive key is null by construction), words 2.. hold the
        // canonical key values (null canonicalized to 0, distinguished by its null bit), and the trailing words
        // the aggregate state cells. The result emitters extract the cells into gid-ordered arrays at the end.
        body.field("long[]", "gsT");
        body.field("int", "gsCap");
        body.field("int", "gsMask");
        body.field("int", "gsFill");
        body.field("int", "groupCount");
        body.field("int[]", "setByGid");
        out.append("    gsCap = 1024;\n");
        out.append("    gsT = new long[gsCap * ").append(keyCount + 2 + total).append("];\n");
        out.append("    gsMask = gsCap - 1; gsFill = (int) (gsCap * 0.75f); groupCount = 0;\n");
        out.append("    setByGid = new int[16];\n");
        for (int kx = 0; kx < keyCount; kx++) {
            body.field("long[]", "keyByGid" + kx);
            body.field("boolean[]", "nullByGid" + kx);
            out.append("    keyByGid").append(kx).append(" = new long[16];\n");
            out.append("    nullByGid").append(kx).append(" = new boolean[16];\n");
        }
    }

    private static void emitGroupingSetsAccumulate(StringBuilder out, ClassBody body, String indent, Plan.Pipeline pipeline, boolean[][] nullable, IntFunction<String> resolver, IntFunction<String> groupKeyResolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        List<Plan.Aggregate> aggregates = pipeline.aggregates();
        int keyCount = pipeline.groupKeys().size();
        // Materialize each data key's raw value and null once per row (shared by every set this row folds into).
        for (int kx = 0; kx < keyCount; kx++) {
            String value = expr(pipeline.groupKeys().get(kx), groupKeyResolver);
            if (keyNullable(pipeline, nullable, kx)) {
                int colIndex = ((Plan.Col) pipeline.groupKeys().get(kx)).index();
                out.append(indent).append("boolean rvN").append(kx).append(" = ").append(nullResolver.apply(colIndex)).append(";\n");
                out.append(indent).append("long rv").append(kx).append(" = ").append(value).append(";\n");
            }
            else {
                out.append(indent).append("boolean rvN").append(kx).append(" = false;\n");
                out.append(indent).append("long rv").append(kx).append(" = ").append(value).append(";\n");
            }
        }
        // Each set's find-or-create compiles into its own method over the field-held table (the inlined blocks
        // for many sets push execute() past the huge-method limit); the aggregate updates stay at the call site,
        // where the input expressions' row context lives.
        List<int[]> sets = pipeline.groupingSets();
        StringBuilder arguments = new StringBuilder();
        StringBuilder parameters = new StringBuilder();
        for (int kx = 0; kx < keyCount; kx++) {
            arguments.append(kx == 0 ? "" : ", ").append("rv").append(kx).append(", rvN").append(kx);
            parameters.append(kx == 0 ? "" : ", ").append("long rv").append(kx).append(", boolean rvN").append(kx);
        }
        for (int s = 0; s < sets.size(); s++) {
            out.append(indent).append("{\n");
            String b = indent + "  ";
            out.append(b).append("int gbase = findGroupSet").append(s).append("(").append(arguments).append(");\n");
            for (int a = 0; a < aggregates.size(); a++) {
                emitAggregateUpdate(out, b, aggregates.get(a), slotCells(aggregates, a, "gsT", "gbase", keyCount + 2), resolver, nullResolver, stringMaskIds);
            }
            out.append(indent).append("}\n");
            if (body.methods().indexOf("int findGroupSet" + s + "(") >= 0) {
                continue;
            }
            emitGroupingSetFindMethod(body, s, sets.get(s), parameters.toString(), keyCount, aggregates);
        }
    }

    /** The find-or-create method for grouping set {@code s}: canonicalize, probe, insert, resize; returns the slot base. */
    private static void emitGroupingSetFindMethod(ClassBody body, int s, int[] set, String parameters, int keyCount, List<Plan.Aggregate> aggregates)
    {
        java.util.Set<Integer> active = new java.util.HashSet<>();
        for (int index : set) {
            active.add(index);
        }
        StringBuilder out = body.methods();
        int stride = keyCount + 2 + cellCount(aggregates);
        out.append("  private int findGroupSet").append(s).append("(").append(parameters).append(") {\n");
        String b = "    ";
        // Canonical per-set key components: an active key keeps its value (null canonicalized to 0); an inactive
        // key is constant-folded to null.
        for (int kx = 0; kx < keyCount; kx++) {
            if (active.contains(kx)) {
                out.append(b).append("boolean ckN").append(kx).append(" = rvN").append(kx).append(";\n");
                out.append(b).append("long ck").append(kx).append(" = rvN").append(kx).append(" ? 0L : rv").append(kx).append(";\n");
            }
            else {
                out.append(b).append("boolean ckN").append(kx).append(" = true;\n");
                out.append(b).append("long ck").append(kx).append(" = 0L;\n");
            }
        }
        out.append(b).append("int setId = ").append(s).append(";\n");
        StringBuilder nullBits = new StringBuilder("0L");
        for (int kx = 0; kx < keyCount; kx++) {
            nullBits.append(" | (ckN").append(kx).append(" ? ").append(1L << kx).append("L : 0L)");
        }
        out.append(b).append("long gMeta = ((long) setId << 32) | (").append(nullBits).append(");\n");
        out.append(b).append("int gHash = mix(").append(groupingSetsHashFold(keyCount)).append(");\n");
        out.append(b).append("int gslot = gHash & gsMask;\n");
        out.append(b).append("int gbase = gslot * ").append(stride).append(";\n");
        out.append(b).append("long gw0 = gsT[gbase];\n");
        out.append(b).append("while ((int) gw0 != 0 && !((int) (gw0 >>> 32) == gHash && gsT[gbase + 1] == gMeta")
                .append(groupingSetsKeyCompare(keyCount))
                .append(")) { gslot = (gslot + 1) & gsMask; gbase = gslot * ").append(stride).append("; gw0 = gsT[gbase]; }\n");
        out.append(b).append("int gid = ((int) gw0) - 1;\n");
        out.append(b).append("if (gid == -1) {\n");
        String c = b + "  ";
        out.append(c).append("gid = groupCount++;\n");
        out.append(c).append("gsT[gbase] = ((long) gHash << 32) | (gid + 1);\n");
        out.append(c).append("gsT[gbase + 1] = gMeta;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(c).append("gsT[gbase + ").append(kx + 2).append("] = ck").append(kx).append(";\n");
        }
        out.append(c).append("if (gid == setByGid.length) {\n");
        out.append(c).append("  int n = setByGid.length * 2;\n");
        out.append(c).append("  setByGid = java.util.Arrays.copyOf(setByGid, n);\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(c).append("  keyByGid").append(kx).append(" = java.util.Arrays.copyOf(keyByGid").append(kx).append(", n);\n");
            out.append(c).append("  nullByGid").append(kx).append(" = java.util.Arrays.copyOf(nullByGid").append(kx).append(", n);\n");
        }
        out.append(c).append("}\n");
        out.append(c).append("setByGid[gid] = setId;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append(c).append("keyByGid").append(kx).append("[gid] = ck").append(kx).append(";\n");
            out.append(c).append("nullByGid").append(kx).append("[gid] = ckN").append(kx).append(";\n");
        }
        for (int a = 0; a < aggregates.size(); a++) {
            aggregator(aggregates.get(a)).emitIdentity(out, c, slotCells(aggregates, a, "gsT", "gbase", keyCount + 2));
        }
        out.append(c).append("if (groupCount > gsFill) {\n");
        out.append(c).append("  int ncap = gsCap * 2;\n");
        out.append(c).append("  long[] nT = new long[ncap * ").append(stride).append("];\n");
        out.append(c).append("  int nMask = ncap - 1;\n");
        out.append(c).append("  int gPrev = gslot;\n");
        out.append(c).append("  for (int t = 0; t < gsCap; t++) {\n");
        out.append(c).append("    long w0 = gsT[t * ").append(stride).append("];\n");
        out.append(c).append("    if ((int) w0 != 0) {\n");
        out.append(c).append("      int ns = (int) (w0 >>> 32) & nMask; while ((int) nT[ns * ").append(stride).append("] != 0) { ns = (ns + 1) & nMask; }\n");
        out.append(c).append("      System.arraycopy(gsT, t * ").append(stride).append(", nT, ns * ").append(stride).append(", ").append(stride).append(");\n");
        // The aggregate update at the call site addresses this row's record through the returned base: track the move.
        out.append(c).append("      if (t == gPrev) { gslot = ns; gbase = ns * ").append(stride).append("; }\n");
        out.append(c).append("    }\n");
        out.append(c).append("  }\n");
        out.append(c).append("  gsT = nT; gsMask = nMask; gsCap = ncap; gsFill = (int) (gsCap * 0.75f);\n");
        out.append(c).append("}\n");
        out.append(b).append("}\n");
        out.append(b).append("return gbase;\n");
        out.append("  }\n");
    }

    private static void emitGroupingSetsResult(StringBuilder out, Plan.Pipeline pipeline, int reconstructDictColumn, List<Type> resultTypes)
    {
        List<Plan.Aggregate> aggregates = pipeline.aggregates();
        int keyCount = pipeline.groupKeys().size();
        int aggregateCount = aggregates.size();
        emitCellExtraction(out, "gsT", "gsMask", keyCount + 2 + cellCount(aggregates), keyCount + 2, aggregates);
        // Result columns: each group key, then each aggregate, then a trailing grouping_id (the GROUPING() bitmask).
        int columnCount = keyCount + aggregateCount + 1;
        out.append("    long[][] result = new long[").append(columnCount).append("][];\n");
        for (int kx = 0; kx < keyCount; kx++) {
            int dict = kx == 0 ? reconstructDictColumn : -1;
            emitKeyResultColumn(out, "    ", kx, kx, dict, "groupCount");
        }
        emitAggregateResultColumns(out, "    ", keyCount, aggregates);
        // grouping_id from setByGid: a precomputed per-set bitmask, indexed by each gid's set.
        out.append("    long[] setMask = new long[").append(pipeline.groupingSets().size()).append("];\n");
        List<int[]> sets = pipeline.groupingSets();
        for (int s = 0; s < sets.size(); s++) {
            long mask = groupingIdBitmask(sets.get(s), keyCount);
            out.append("    setMask[").append(s).append("] = ").append(mask).append("L;\n");
        }
        out.append("    long[] outGroupingId = new long[groupCount];\n");
        out.append("    for (int g = 0; g < groupCount; g++) { outGroupingId[g] = setMask[setByGid[g]]; }\n");
        out.append("    result[").append(keyCount + aggregateCount).append("] = outGroupingId;\n");
        emitResultTypes(out, "    ", resultTypes);
        // Every group key is nullable in a grouping-sets result (inactive in some set), so emit a null mask driven by
        // each gid's set membership; aggregates may also finalize to null.
        out.append("    boolean[][] resultNulls = new boolean[").append(columnCount).append("][];\n");
        for (int kx = 0; kx < keyCount; kx++) {
            out.append("    resultNulls[").append(kx).append("] = java.util.Arrays.copyOf(nullByGid").append(kx).append(", groupCount);\n");
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
    }

    /** Hash input over (setId, ck0, ..., ck{keyCount-1}) for the grouping-sets accumulate. */
    private static String groupingSetsHashFold(int keyCount)
    {
        String folded = "(long) setId";
        for (int kx = 0; kx < keyCount; kx++) {
            folded = "(" + folded + ") * 0x9E3779B97F4A7C15L + ck" + kx;
        }
        return folded;
    }

    /** Conjunction comparing a stored slot's (setId, keys, null flags) against the probe's. */
    private static String groupingSetsKeyCompare(int keyCount)
    {
        // The setId and null bits are covered by the gMeta word; only the key values remain to compare.
        StringBuilder compare = new StringBuilder();
        for (int kx = 0; kx < keyCount; kx++) {
            compare.append(" && gsT[gbase + ").append(kx + 2).append("] == ck").append(kx);
        }
        return compare.toString();
    }

    /** The GROUPING() bitmask for a set: bit {@code kx} set when group key {@code kx} is NOT active in the set. */
    private static long groupingIdBitmask(int[] set, int keyCount)
    {
        java.util.Set<Integer> active = new java.util.HashSet<>();
        for (int index : set) {
            active.add(index);
        }
        long mask = 0L;
        for (int kx = 0; kx < keyCount; kx++) {
            if (!active.contains(kx)) {
                mask |= 1L << (keyCount - 1 - kx);
            }
        }
        return mask;
    }

    // ---- grouped aggregation (single long key) ----

    private static void emitGroupedState(StringBuilder out, ClassBody body, Plan.Pipeline pipeline, boolean[][] nullable, boolean speculate)
    {
        if (!pipeline.groupingSets().isEmpty()) {
            emitGroupingSetsState(out, body, pipeline);
            return;
        }
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
        // Interleaved slot records in one array, so a probe touches one or two cache lines instead of one per
        // parallel key array, and an aggregate update lands in the lines the probe just loaded (the dominant cost
        // at multi-million-group scale). Each slot is (keyCount + 2 + cellCount) longs: word 0 packs the mixed
        // hash (high int, the probe fingerprint) with gid + 1 (low int; 0 = empty slot), word 1 holds the key
        // null bits, words 2.. the key values (null canonicalized to 0), and the trailing words the aggregate
        // state cells. The result emitters extract the cells into gid-ordered arrays at the end.
        body.field("long[]", "htT");
        body.field("int", "htCap");
        body.field("int", "htMask");
        body.field("int", "htFill");
        body.field("int", "groupCount");
        out.append("    htCap = 1024;\n");
        out.append("    htT = new long[htCap * ").append(keyCount + 2 + total).append("];\n");
        out.append("    htMask = htCap - 1; htFill = (int) (htCap * 0.75f); groupCount = 0;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            body.field("long[]", "keyByGid" + kx);
            out.append("    keyByGid").append(kx).append(" = new long[16];\n");
            if (keyNullable(pipeline, nullable, kx)) {
                body.field("boolean[]", "nullByGid" + kx);
                out.append("    nullByGid").append(kx).append(" = new boolean[16];\n");
            }
        }
    }

    private static void emitGroupedAccumulate(StringBuilder out, ClassBody body, String indent, Plan.Pipeline pipeline, boolean[][] nullable, IntFunction<String> resolver, IntFunction<String> groupKeyResolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds, boolean speculate)
    {
        if (!pipeline.groupingSets().isEmpty()) {
            emitGroupingSetsAccumulate(out, body, indent, pipeline, nullable, resolver, groupKeyResolver, nullResolver, stringMaskIds);
            return;
        }
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
                aggregator(aggregates.get(a)).emitMerge(out, m, cells(aggregates, a, "agg", "gid"), cells(aggregates, a, "aAgg", "mo"), aggregateInputDictionary(aggregates.get(a)));
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
        // The find-or-create compiles into its own method over the field-held table (inlined it pushes a wide
        // pipeline's execute() past the huge-method limit); the aggregate updates stay at the call site, where the
        // input expressions' row context lives.
        StringBuilder arguments = new StringBuilder();
        StringBuilder parameters = new StringBuilder();
        for (int kx = 0; kx < keyCount; kx++) {
            arguments.append(kx == 0 ? "" : ", ").append("gk").append(kx);
            parameters.append(kx == 0 ? "" : ", ").append("long gk").append(kx);
            if (keyNullable(pipeline, nullable, kx)) {
                arguments.append(", gkN").append(kx);
                parameters.append(", boolean gkN").append(kx);
            }
        }
        out.append(indent).append("int gbase = findGroup(").append(arguments).append(");\n");
        for (int a = 0; a < aggregates.size(); a++) {
            emitAggregateUpdate(out, indent, aggregates.get(a), slotCells(aggregates, a, "htT", "gbase", keyCount + 2), resolver, nullResolver, stringMaskIds);
        }
        if (body.methods().indexOf("int findGroup(") >= 0) {
            return;
        }
        StringBuilder method = body.methods();
        int stride = keyCount + 2 + cellCount(aggregates);
        method.append("  private int findGroup(").append(parameters).append(") {\n");
        String b = "    ";
        StringBuilder nullBits = new StringBuilder("0L");
        for (int kx = 0; kx < keyCount; kx++) {
            if (keyNullable(pipeline, nullable, kx)) {
                nullBits.append(" | (gkN").append(kx).append(" ? ").append(1L << kx).append("L : 0L)");
            }
        }
        method.append(b).append("long gMeta = ").append(nullBits).append(";\n");
        method.append(b).append("int gHash = mix(").append(hashFold("gk", "", keyCount)).append(");\n");
        method.append(b).append("int gslot = gHash & htMask;\n");
        method.append(b).append("int gbase = gslot * ").append(stride).append(";\n");
        method.append(b).append("long gw0 = htT[gbase];\n");
        method.append(b).append("while ((int) gw0 != 0 && !((int) (gw0 >>> 32) == gHash && htT[gbase + 1] == gMeta")
                .append(keyCompare(keyCount))
                .append(")) { gslot = (gslot + 1) & htMask; gbase = gslot * ").append(stride).append("; gw0 = htT[gbase]; }\n");
        method.append(b).append("int gid = ((int) gw0) - 1;\n");
        method.append(b).append("if (gid == -1) {\n");
        String c = b + "  ";
        method.append(c).append("gid = groupCount++;\n");
        method.append(c).append("htT[gbase] = ((long) gHash << 32) | (gid + 1);\n");
        method.append(c).append("htT[gbase + 1] = gMeta;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            method.append(c).append("htT[gbase + ").append(kx + 2).append("] = gk").append(kx).append(";\n");
        }
        method.append(c).append("if (gid == keyByGid0.length) {\n");
        method.append(c).append("  int n = keyByGid0.length * 2;\n");
        for (int kx = 0; kx < keyCount; kx++) {
            method.append(c).append("  keyByGid").append(kx).append(" = java.util.Arrays.copyOf(keyByGid").append(kx).append(", n);\n");
            if (keyNullable(pipeline, nullable, kx)) {
                method.append(c).append("  nullByGid").append(kx).append(" = java.util.Arrays.copyOf(nullByGid").append(kx).append(", n);\n");
            }
        }
        method.append(c).append("}\n");
        for (int kx = 0; kx < keyCount; kx++) {
            method.append(c).append("keyByGid").append(kx).append("[gid] = gk").append(kx).append(";\n");
            if (keyNullable(pipeline, nullable, kx)) {
                method.append(c).append("nullByGid").append(kx).append("[gid] = gkN").append(kx).append(";\n");
            }
        }
        for (int a = 0; a < aggregates.size(); a++) {
            aggregator(aggregates.get(a)).emitIdentity(method, c, slotCells(aggregates, a, "htT", "gbase", keyCount + 2));
        }
        method.append(c).append("if (groupCount > htFill) {\n");
        method.append(c).append("  int ncap = htCap * 2;\n");
        method.append(c).append("  long[] nT = new long[ncap * ").append(stride).append("];\n");
        method.append(c).append("  int nMask = ncap - 1;\n");
        method.append(c).append("  int gPrev = gslot;\n");
        method.append(c).append("  for (int s = 0; s < htCap; s++) {\n");
        method.append(c).append("    long w0 = htT[s * ").append(stride).append("];\n");
        method.append(c).append("    if ((int) w0 != 0) {\n");
        method.append(c).append("      int ns = (int) (w0 >>> 32) & nMask; while ((int) nT[ns * ").append(stride).append("] != 0) { ns = (ns + 1) & nMask; }\n");
        method.append(c).append("      System.arraycopy(htT, s * ").append(stride).append(", nT, ns * ").append(stride).append(", ").append(stride).append(");\n");
        // The aggregate update at the call site addresses this row's record through the returned base: track the move.
        method.append(c).append("      if (s == gPrev) { gslot = ns; gbase = ns * ").append(stride).append("; }\n");
        method.append(c).append("    }\n");
        method.append(c).append("  }\n");
        method.append(c).append("  htT = nT; htMask = nMask; htCap = ncap; htFill = (int) (htCap * 0.75f);\n");
        method.append(c).append("}\n");
        method.append(b).append("}\n");
        method.append(b).append("return gbase;\n");
        method.append("  }\n");
    }

    private static void emitGroupedResult(StringBuilder out, Plan.Pipeline pipeline, boolean[][] nullable, boolean speculate, int reconstructDictColumn, List<Type> resultTypes)
    {
        if (!pipeline.groupingSets().isEmpty()) {
            emitGroupingSetsResult(out, pipeline, reconstructDictColumn, resultTypes);
            return;
        }
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
        emitCellExtraction(out, "htT", "htMask", keyCount + 2 + cellCount(aggregates), keyCount + 2, aggregates);
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
    private static String keyCompare(int keyCount)
    {
        // The null bits are covered by the gMeta word; only the key values remain to compare.
        StringBuilder compare = new StringBuilder();
        for (int kx = 0; kx < keyCount; kx++) {
            compare.append(" && htT[gbase + ").append(kx + 2).append("] == gk").append(kx);
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

    /** Lvalue strings for aggregate {@code index}'s state cells stored inline in a slot record at {@code base}. */
    private static List<String> slotCells(List<Plan.Aggregate> aggregates, int index, String table, String base, int firstCellWord)
    {
        int cellBase = cellBase(aggregates, index);
        int n = aggregator(aggregates.get(index)).cells();
        List<String> result = new ArrayList<>(n);
        for (int c = 0; c < n; c++) {
            result.add(table + "[" + base + " + " + (firstCellWord + cellBase + c) + "]");
        }
        return result;
    }

    /**
     * Extract the in-record aggregate cells into gid-ordered {@code agg<cell>} arrays with one sequential walk of
     * the slot table, so the result emitters (which finalize from {@code agg<cell>[g]}) stay layout-agnostic.
     */
    private static void emitCellExtraction(StringBuilder out, String table, String maskVar, int stride, int firstCellWord, List<Plan.Aggregate> aggregates)
    {
        int total = cellCount(aggregates);
        for (int c = 0; c < total; c++) {
            out.append("    long[] agg").append(c).append(" = new long[groupCount];\n");
        }
        out.append("    for (int xs = 0; xs <= ").append(maskVar).append("; xs++) {\n");
        out.append("      long xw0 = ").append(table).append("[xs * ").append(stride).append("];\n");
        out.append("      if ((int) xw0 != 0) {\n");
        out.append("        int xg = ((int) xw0) - 1;\n");
        for (int c = 0; c < total; c++) {
            out.append("        agg").append(c).append("[xg] = ").append(table).append("[xs * ").append(stride).append(" + ").append(firstCellWord + c).append("];\n");
        }
        out.append("      }\n");
        out.append("    }\n");
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
        if (projectionOnly(pipeline)) {
            // A projection-only pipeline's payload is the columns its projections read (over the combined inputs);
            // grouped/global projections instead run post-aggregation over result columns and are not collected here.
            for (Plan.Expr projection : pipeline.projections()) {
                collectColumns(projection, columns);
            }
            return columns;
        }
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

    /**
     * Does a streamed string column's id numbering have to be stable across batches? True when the column is
     * consumed beyond filter predicates (group key, aggregate input, or projected output): those ids land in
     * cross-batch state, so the source must intern the column into a per-query global dictionary, and its
     * predicate masks extend incrementally rather than rebuilding per batch. A filter-only column's ids die with
     * the batch, so a per-batch (page-local) dictionary suffices.
     */
    public static boolean stringIdsCrossBatches(Plan.Pipeline pipeline, int column)
    {
        return accumulateColumns(pipeline).contains(column);
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
            case Plan.StringColumnCompare compare -> compare.negated() ? 4 : 1;
            case Plan.IsNull isNull -> isNull.negated() ? 0 : 4;   // IS NOT NULL is highly selective, IS NULL rarely
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
            case Plan.StringColumnCompare compare -> {
                into.add(compare.left());
                into.add(compare.right());
            }
            case Plan.IsNull isNull -> into.add(isNull.column());
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
            case Plan.Predicate predicate -> {
                // SQL three-valued logic: a comparison with a null operand is NULL, which a filter treats as false.
                // Guard the value comparison with the operands' not-null tests. Under NEVER_NULL the guard collapses
                // away (nullExpr -> "false"), so callers without null information emit the bare comparison unchanged.
                String comparison = "(" + expr(predicate.left(), resolver, nullResolver) + " " + comparison(predicate.op()) + " " + expr(predicate.right(), resolver, nullResolver) + ")";
                String guard = notNullGuard(orNull(nullExpr(predicate.left(), resolver, nullResolver), nullExpr(predicate.right(), resolver, nullResolver)));
                yield guard.isEmpty() ? comparison : "(" + guard + " && " + comparison + ")";
            }
            case Plan.And and -> and.conditions().isEmpty() ? "true"
                    : "(" + and.conditions().stream().map(child -> condition(child, resolver, nullResolver)).collect(joining(" && ")) + ")";
            case Plan.Or or -> or.conditions().isEmpty() ? "false"
                    : "(" + or.conditions().stream().map(child -> condition(child, resolver, nullResolver)).collect(joining(" || ")) + ")";
            case Plan.Not not -> "(!" + condition(not.condition(), resolver, nullResolver) + ")";
            case Plan.StringMatch ignored -> throw new UnsupportedOperationException("string match is only supported in WHERE filters");
            case Plan.LikeMatch ignored -> throw new UnsupportedOperationException("string match is only supported in WHERE filters");
            case Plan.SubstringMatch ignored -> throw new UnsupportedOperationException("string match is only supported in WHERE filters");
            case Plan.StringColumnCompare ignored -> throw new UnsupportedOperationException("string column compare is only supported in WHERE filters");
            case Plan.IsNull isNull -> isNull.negated()
                    ? "(!(" + nullResolver.apply(isNull.column()) + "))"
                    : "(" + nullResolver.apply(isNull.column()) + ")";
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
            case Plan.LitStr ignored -> "0L";   // constant string: emit dictionary id 0 (the value comes from the consumer's single-entry dictionary)
            case Plan.NullLit ignored -> "0L";   // null long: a placeholder value; the null mask (below) is what matters
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
            case Plan.LitStr ignored -> "false";
            case Plan.NullLit ignored -> "true";
            case Plan.Bin bin -> orNull(nullExpr(bin.left(), resolver, nullResolver, stringMaskIds), nullExpr(bin.right(), resolver, nullResolver, stringMaskIds));
            case Plan.Call call -> {
                String nulls = "false";
                for (Plan.Expr argument : call.arguments()) {
                    nulls = orNull(nulls, nullExpr(argument, resolver, nullResolver, stringMaskIds));
                }
                if (NULL_ON_ZERO_DENOMINATOR.contains(call.name())) {
                    // A rounding divide is NULL on a zero denominator (argument 1), matching the interpreted kernel.
                    nulls = orNull(nulls, "(" + expr(call.arguments().get(1), resolver, nullResolver, stringMaskIds) + " == 0L)");
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
            case Plan.StringColumnCompare compare -> {
                return stringColumnCompareTest(compare, compare.negated(), resolver, nullResolver, stringMaskIds);
            }
            case Plan.IsNull isNull -> {
                String isNullExpr = nullResolver.apply(isNull.column());
                return isNull.negated() ? "(!(" + isNullExpr + "))" : "(" + isNullExpr + ")";
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
            case Plan.StringColumnCompare compare -> {
                // The complement of an equality is an inequality and vice versa (over non-null rows).
                return stringColumnCompareTest(compare, !compare.negated(), resolver, nullResolver, stringMaskIds);
            }
            case Plan.IsNull isNull -> {
                // IS NULL is two-valued (never UNKNOWN), so its FALSE is simply its negation.
                String isNullExpr = nullResolver.apply(isNull.column());
                return isNull.negated() ? "(" + isNullExpr + ")" : "(!(" + isNullExpr + "))";
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

    /**
     * Per-row test for a {@link Plan.StringColumnCompare}: {@code sRemap<id>[leftId] == rightId} (or {@code !=} when
     * {@code inequality}), guarded so a null on either side makes the row fail (SQL: the comparison is null). The remap
     * (emitted by {@link #emitStringRemapPrelude}) translates a left-dictionary id into the right column's id space, so
     * value equality becomes the integer test; a left value absent from the right dictionary maps to {@code -1}, which
     * never equals a (non-negative) right id.
     */
    private static String stringColumnCompareTest(Plan.StringColumnCompare compare, boolean inequality, IntFunction<String> resolver, IntFunction<String> nullResolver, Map<Plan.Condition, Integer> stringMaskIds)
    {
        String guard = andGuards(notNullGuard(nullResolver.apply(compare.left())), notNullGuard(nullResolver.apply(compare.right())));
        int id = stringMaskIds.get(compare);
        String operator = inequality ? "!=" : "==";
        String test = compare.hasSubstring()
                ? "sLeftClass" + id + "[" + resolver.apply(compare.left()) + "] " + operator + " sRightClass" + id + "[" + resolver.apply(compare.right()) + "]"
                : "sRemap" + id + "[" + resolver.apply(compare.left()) + "] " + operator + " " + resolver.apply(compare.right());
        return guard.isEmpty() ? "(" + test + ")" : "(" + guard + " && (" + test + "))";
    }

    /**
     * Precompute the {@code int[] sRemap<id>} that maps each entry of the left column's dictionary to the id of the
     * right column's dictionary entry with the same bytes (or {@code -1}). A {@link Plan.StringColumnCompare} then
     * compares two columns carrying independent dictionaries with a single integer lookup per row.
     */
    /** Emit the per-dictionary prelude for a string condition: a remap for a column-vs-column compare, else a mask. {@code dictionaryVar} maps a column index to its materialized dictionary variable. */
    private static void emitStringConditionPrelude(StringBuilder out, Plan.Condition match, int id, IntFunction<String> dictionaryVar)
    {
        emitStringConditionPrelude(out, null, match, id, dictionaryVar);
    }

    /**
     * The streaming variant of {@link #emitStringConditionPrelude}: a match over a globally-interned column
     * (ids stable across batches, see {@link #stringIdsCrossBatches}) gets an incremental field-held mask;
     * everything else rebuilds per batch over the page-local dictionary, as in the materialized path.
     */
    private static void emitStreamingStringConditionPrelude(StringBuilder out, ClassBody body, Plan.Pipeline pipeline, Plan.Condition match, int id)
    {
        if (!(match instanceof Plan.StringColumnCompare) && stringIdsCrossBatches(pipeline, stringMatchColumn(match))) {
            int column = stringMatchColumn(match);
            emitIncrementalStringMaskPrelude(out, body, match, id, "cStr" + column, "cStrLen" + column);
            return;
        }
        emitStringConditionPrelude(out, match, id, column -> "cStr" + column);
    }

    /** With a {@code body}, the remap / class arrays become instance fields (read from out-of-line stage methods). */
    private static void emitStringConditionPrelude(StringBuilder out, ClassBody body, Plan.Condition match, int id, IntFunction<String> dictionaryVar)
    {
        if (match instanceof Plan.StringColumnCompare compare) {
            if (compare.hasSubstring()) {
                if (body != null) {
                    body.field("int[]", "sLeftClass" + id);
                    body.field("int[]", "sRightClass" + id);
                }
                emitStringPrefixClassPrelude(out, body != null, id, dictionaryVar.apply(compare.left()), dictionaryVar.apply(compare.right()), compare.substringStart(), compare.substringLength());
            }
            else {
                if (body != null) {
                    body.field("int[]", "sRemap" + id);
                }
                emitStringRemapPrelude(out, body != null, id, dictionaryVar.apply(compare.left()), dictionaryVar.apply(compare.right()));
            }
        }
        else {
            emitStringMaskPrelude(out, match, id, dictionaryVar.apply(stringMatchColumn(match)));
        }
    }

    private static void emitStringRemapPrelude(StringBuilder out, boolean asField, int id, String leftDictionaryVar, String rightDictionaryVar)
    {
        out.append("    java.util.HashMap<String, Integer> sRightIdx").append(id).append(" = new java.util.HashMap<>();\n");
        out.append("    for (int e = 0; e < ").append(rightDictionaryVar).append(".length; e++) {\n");
        out.append("      sRightIdx").append(id).append(".putIfAbsent(new String(").append(rightDictionaryVar).append("[e], java.nio.charset.StandardCharsets.UTF_8), e);\n");
        out.append("    }\n");
        out.append(asField ? "    sRemap" : "    int[] sRemap").append(id).append(" = new int[").append(leftDictionaryVar).append(".length];\n");
        out.append("    for (int e = 0; e < ").append(leftDictionaryVar).append(".length; e++) {\n");
        out.append("      Integer r = sRightIdx").append(id).append(".get(new String(").append(leftDictionaryVar).append("[e], java.nio.charset.StandardCharsets.UTF_8));\n");
        out.append("      sRemap").append(id).append("[e] = r == null ? -1 : r;\n");
        out.append("    }\n");
    }

    /**
     * Precompute {@code int[] sLeftClass<id>} / {@code int[] sRightClass<id>} mapping each side's dictionary id to a
     * shared prefix-class id: entries whose {@code substring(value, start, length)} bytes are equal get the same class,
     * across both dictionaries. A {@link Plan.StringColumnCompare} over a substring then reduces to
     * {@code sLeftClass[leftId] == sRightClass[rightId]}. Unlike the whole-value remap, both sides are canonicalized,
     * since two distinct right entries can share a prefix.
     */
    private static void emitStringPrefixClassPrelude(StringBuilder out, boolean asField, int id, String leftDictionaryVar, String rightDictionaryVar, int start, int length)
    {
        out.append("    java.util.HashMap<String, Integer> sClass").append(id).append(" = new java.util.HashMap<>();\n");
        emitPrefixClassLoop(out, asField, id, "sLeftClass", leftDictionaryVar, start, length);
        emitPrefixClassLoop(out, asField, id, "sRightClass", rightDictionaryVar, start, length);
    }

    private static void emitPrefixClassLoop(StringBuilder out, boolean asField, int id, String arrayName, String dictionaryVar, int start, int length)
    {
        out.append(asField ? "    " : "    int[] ").append(arrayName).append(id).append(" = new int[").append(dictionaryVar).append(".length];\n");
        out.append("    for (int e = 0; e < ").append(dictionaryVar).append(".length; e++) {\n");
        out.append("      byte[] sub = org.weakref.nitro.function.scalar.builtin.Utf8Support.substring(")
                .append(dictionaryVar).append("[e], 0, ").append(dictionaryVar).append("[e].length, ")
                .append((long) start).append("L, ").append((long) length).append("L);\n");
        out.append("      String key = new String(sub, java.nio.charset.StandardCharsets.UTF_8);\n");
        out.append("      Integer c = sClass").append(id).append(".get(key);\n");
        out.append("      if (c == null) { c = sClass").append(id).append(".size(); sClass").append(id).append(".put(key, c); }\n");
        out.append("      ").append(arrayName).append(id).append("[e] = c;\n");
        out.append("    }\n");
    }
}
