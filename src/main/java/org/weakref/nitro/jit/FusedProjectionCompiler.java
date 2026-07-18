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

import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.Operation;
import org.weakref.nitro.operator.evaluator.ir.Producer;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Compiles the fusible outputs of a {@link org.weakref.nitro.operator.ProjectOperator} into one
 * {@link FusedMultiProjection}: a single monomorphic loop over the source columns that reads each input once, computes
 * each shared subexpression once, and writes every output. Intermediate values live in local registers, so there is no
 * per-node megamorphic dispatch and no intermediate {@link org.weakref.nitro.data.Vector} -- the two costs the
 * interpreter pays walking a subtree and invoking one atomic batch primitive per node.
 * <p>
 * Values are computed with three-valued (value, is-null) pairs so nullable inputs are handled without a separate
 * boolean-vector pipeline. Scope: integer inputs (read as {@code long}, I32 widened once), double inputs (read as
 * {@code double}), and UTF-8 input-to-literal categorical equality; the {@code long}/{@code double} arithmetic,
 * comparison, boolean and {@code if_i64}/{@code if_f64} operator set; and {@code long}-typed (I64) or
 * {@code double}-typed (F64) outputs. Any output outside that set is left to the interpreter, so this is a
 * speedup-only substitution behind the same ABI.
 */
public final class FusedProjectionCompiler
{
    private static final String PACKAGE = "org.weakref.nitro.jit.generated";
    private static final boolean COMPILE_UTF8_IN =
            Boolean.parseBoolean(System.getProperty("nitro.project.compileUtf8In", "true"));
    private static final boolean POOLED_DICTIONARY_SCRATCH =
            Boolean.parseBoolean(System.getProperty("nitro.project.pooledDictionaryScratch", "true"));
    private static final boolean MAPPED_DICTIONARY_DOUBLE_INPUTS =
            Boolean.parseBoolean(System.getProperty("nitro.project.mappedDictionaryDoubleInputs", "true"));
    private static final AtomicInteger COUNTER = new AtomicInteger();
    // Kernels are stateless, so identical projection shapes share one compiled class (amortizing javac cost across
    // operators / benchmark iterations). Keyed by the fully-rendered source with a stable placeholder class name.
    private static final Map<String, FusedMultiProjection> CACHE = new ConcurrentHashMap<>();

    private FusedProjectionCompiler() {}

    private enum ValueType { LONG, DOUBLE, BOOL, UTF8 }

    private sealed interface Operand
            permits ColumnOperand, StepOperand, LongConstant, DoubleConstant, BoolConstant, Utf8Constant {}

    private record ColumnOperand(int slot, ValueType type) implements Operand {}

    private record StepOperand(int stepId, ValueType type) implements Operand {}

    private record LongConstant(long value) implements Operand {}

    private record DoubleConstant(double value) implements Operand {}

    private record BoolConstant(boolean value) implements Operand {}

    private record Utf8Constant(String value) implements Operand {}

    /** One shared assignment in the fused program: {@code step<id> = op(operands)}. */
    private record Step(int id, String op, List<Operand> operands, ValueType type) {}

    /** The compiled shape: the ordered source columns (with their Java type) to feed, the shared steps, the roots. */
    private record Slice(List<Integer> columns, List<ValueType> columnTypes, List<Step> steps, List<Operand> roots) {}

    /** A fused multi-output kernel, the ordered source columns it expects, and the outputs it produces (in order). */
    public record CompiledMultiProjection(FusedMultiProjection kernel, List<Integer> columns, List<Reference> outputs) {}

    /**
     * Compile every fusible output among {@code candidateOutputs} into one shared-loop kernel. Returns empty if none
     * qualifies. An output qualifies when its whole SSA slice is in the supported set, its result is I64- or F64-typed,
     * and it is worth fusing (its slice has at least two operations -- a single-op projection is left to the
     * interpreter, where fusion would only add call overhead without saving an intermediate).
     */
    public static Optional<CompiledMultiProjection> tryCompile(EvaluationPlan plan, List<Reference> candidateOutputs)
    {
        Map<Integer, Assignment> assignments = new LinkedHashMap<>();
        for (Assignment assignment : plan.assignments()) {
            assignments.put(assignment.output().id(), assignment);
        }

        List<Reference> fusible = new ArrayList<>();
        for (Reference candidate : candidateOutputs) {
            if (candidate.producer() instanceof Input || candidate.stream() != Stream.VALUES) {
                continue;
            }
            try {
                SliceBuilder trial = new SliceBuilder(assignments);
                Operand root = trial.operand(candidate, null);
                ValueType rootType = operandType(root);
                if ((rootType == ValueType.LONG || rootType == ValueType.DOUBLE) && worthFusing(trial.steps())) {
                    fusible.add(candidate);
                }
            }
            catch (Unsupported ignored) {
                // not fusible; leave to the interpreter
            }
        }
        if (fusible.isEmpty()) {
            return Optional.empty();
        }

        SliceBuilder combined = new SliceBuilder(assignments);
        List<Operand> roots = new ArrayList<>();
        for (Reference output : fusible) {
            roots.add(combined.operand(output, null));
        }
        Slice slice = new Slice(combined.columns(), combined.columnTypes(), combined.steps(), roots);

        FusedMultiProjection kernel = CACHE.computeIfAbsent(render(slice, "K"), ignored -> {
            String simpleName = "FusedProj_" + COUNTER.incrementAndGet();
            String source = render(slice, simpleName);
            try {
                Class<?> compiled = InMemoryCompiler.compile(PACKAGE + "." + simpleName, source);
                return (FusedMultiProjection) compiled.getDeclaredConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Failed to instantiate fused projection:\n" + source, e);
            }
        });
        return Optional.of(new CompiledMultiProjection(kernel, List.copyOf(slice.columns()), List.copyOf(fusible)));
    }

    private static boolean worthFusing(List<Step> steps)
    {
        // A slice with a single operation writes one output from one primitive; the interpreter already does that with
        // no intermediate vector, so fusing only adds javac + call overhead. Two or more operations means the
        // interpreter materializes at least one intermediate vector that fusion keeps in a register.
        return steps.size() >= 2;
    }

    private static final class Unsupported
            extends RuntimeException
    {
        Unsupported()
        {
            super(null, null, false, false);
        }
    }

    /** Walks the SSA graph from outputs, inlining shared variables once so a shared subexpression is a single step. */
    private static final class SliceBuilder
    {
        private final Map<Integer, Assignment> assignments;
        private final List<Integer> columns = new ArrayList<>();
        private final Map<Integer, Integer> columnSlots = new LinkedHashMap<>();
        private final Map<Integer, ValueType> columnTypeBySlot = new LinkedHashMap<>();
        private final Map<Integer, Operand> variableSteps = new LinkedHashMap<>();
        private final List<Step> steps = new ArrayList<>();
        private final AtomicInteger nextStep = new AtomicInteger();

        SliceBuilder(Map<Integer, Assignment> assignments)
        {
            this.assignments = assignments;
        }

        List<Integer> columns()
        {
            return columns;
        }

        List<ValueType> columnTypes()
        {
            List<ValueType> types = new ArrayList<>(columns.size());
            for (int slot = 0; slot < columns.size(); slot++) {
                types.add(columnTypeBySlot.get(slot));
            }
            return types;
        }

        List<Step> steps()
        {
            return steps;
        }

        /**
         * Resolves a reference into an operand. {@code expected} is the type the consuming operator requires (null for a
         * root, whose type is whatever its operator produces). An input column is typed by its consumer; if two
         * consumers disagree on a column's type the slice is not fusible.
         */
        Operand operand(Reference reference, ValueType expected)
        {
            if (reference.stream() != Stream.VALUES) {
                throw new Unsupported();
            }
            Producer producer = reference.producer();
            if (producer instanceof Input input) {
                if (expected == null || expected == ValueType.BOOL) {
                    // A raw column can feed a numeric or UTF-8 operator; bool-typed column input is out of scope.
                    throw new Unsupported();
                }
                int slot = columnSlots.computeIfAbsent(input.index(), index -> {
                    columns.add(index);
                    return columns.size() - 1;
                });
                ValueType existing = columnTypeBySlot.putIfAbsent(slot, expected);
                if (existing != null && existing != expected) {
                    throw new Unsupported();
                }
                return new ColumnOperand(slot, expected);
            }
            if (producer instanceof Variable variable) {
                Operand existing = variableSteps.get(variable.id());
                Operand built = existing != null ? existing : buildVariable(variable);
                if (existing == null) {
                    variableSteps.put(variable.id(), built);
                }
                if (expected != null && operandType(built) != expected) {
                    throw new Unsupported();
                }
                return built;
            }
            throw new Unsupported();
        }

        private Operand buildVariable(Variable variable)
        {
            Assignment assignment = assignments.get(variable.id());
            if (assignment == null) {
                throw new Unsupported();
            }
            Operation operation = assignment.operation();
            if (operation instanceof Literal literal) {
                return literalOperand(literal.value());
            }
            if (operation instanceof Call call) {
                String op = call.name();
                if (op.equals("in_utf8") && !COMPILE_UTF8_IN) {
                    throw new Unsupported();
                }
                ValueType type = resultType(op);
                List<ValueType> argTypes = op.equals("in_utf8")
                        ? java.util.Collections.nCopies(call.arguments().size(), ValueType.UTF8)
                        : argumentTypes(op);
                if (call.arguments().size() != argTypes.size()) {
                    throw new Unsupported();
                }
                List<Operand> operands = new ArrayList<>();
                for (int index = 0; index < call.arguments().size(); index++) {
                    operands.add(operand(call.arguments().get(index), argTypes.get(index)));
                }
                if (op.equals("eq_utf8") && !isUtf8ColumnLiteralEquality(operands)) {
                    // Rendering deliberately supports categorical column-to-literal tests only. Reject other
                    // syntactically valid UTF-8 equalities here so tryCompile cleanly falls back instead of failing
                    // later while rendering an otherwise accepted slice.
                    throw new Unsupported();
                }
                if (op.equals("in_utf8") && !isUtf8ColumnLiteralSet(operands)) {
                    // The generated categorical dispatch is intentionally column-to-constant only. This covers the
                    // common SQL IN-list shape while preserving the interpreter fallback for dynamic list members.
                    throw new Unsupported();
                }
                int id = nextStep.getAndIncrement();
                steps.add(new Step(id, op, operands, type));
                return new StepOperand(id, type);
            }
            throw new Unsupported();
        }

        private static boolean isUtf8ColumnLiteralEquality(List<Operand> operands)
        {
            return (operands.get(0) instanceof ColumnOperand && operands.get(1) instanceof Utf8Constant) ||
                    (operands.get(1) instanceof ColumnOperand && operands.get(0) instanceof Utf8Constant);
        }

        private static boolean isUtf8ColumnLiteralSet(List<Operand> operands)
        {
            if (operands.size() < 2 || !(operands.getFirst() instanceof ColumnOperand)) {
                return false;
            }
            for (int index = 1; index < operands.size(); index++) {
                if (!(operands.get(index) instanceof Utf8Constant)) {
                    return false;
                }
            }
            return true;
        }

        private static Operand literalOperand(Object value)
        {
            if (value instanceof Long longValue) {
                return new LongConstant(longValue);
            }
            if (value instanceof Integer intValue) {
                return new LongConstant(intValue.longValue());
            }
            if (value instanceof Double doubleValue) {
                if (!Double.isFinite(doubleValue)) {
                    throw new Unsupported();
                }
                return new DoubleConstant(doubleValue);
            }
            if (value instanceof Boolean boolValue) {
                return new BoolConstant(boolValue);
            }
            if (value instanceof String stringValue) {
                return new Utf8Constant(stringValue);
            }
            throw new Unsupported();
        }
    }

    private static ValueType resultType(String op)
    {
        return switch (op) {
            case "add", "subtract", "multiply", "if_i64" -> ValueType.LONG;
            case "add_f64", "subtract_f64", "multiply_f64", "if_f64" -> ValueType.DOUBLE;
            case "lt", "gt", "lte", "gte", "eq",
                 "lt_f64", "gt_f64", "lte_f64", "gte_f64", "eq_f64",
                 "eq_utf8", "in_utf8", "and", "or", "not" -> ValueType.BOOL;
            default -> throw new Unsupported();
        };
    }

    /** The operand types each operator requires, in order (also fixes its arity). */
    private static List<ValueType> argumentTypes(String op)
    {
        return switch (op) {
            case "add", "subtract", "multiply", "lt", "gt", "lte", "gte", "eq" -> List.of(ValueType.LONG, ValueType.LONG);
            case "add_f64", "subtract_f64", "multiply_f64", "lt_f64", "gt_f64", "lte_f64", "gte_f64", "eq_f64" ->
                    List.of(ValueType.DOUBLE, ValueType.DOUBLE);
            case "and", "or" -> List.of(ValueType.BOOL, ValueType.BOOL);
            case "not" -> List.of(ValueType.BOOL);
            case "eq_utf8" -> List.of(ValueType.UTF8, ValueType.UTF8);
            case "if_i64" -> List.of(ValueType.BOOL, ValueType.LONG, ValueType.LONG);
            case "if_f64" -> List.of(ValueType.BOOL, ValueType.DOUBLE, ValueType.DOUBLE);
            default -> throw new Unsupported();
        };
    }

    private static ValueType operandType(Operand operand)
    {
        return switch (operand) {
            case ColumnOperand column -> column.type();
            case StepOperand step -> step.type();
            case LongConstant ignored -> ValueType.LONG;
            case DoubleConstant ignored -> ValueType.DOUBLE;
            case BoolConstant ignored -> ValueType.BOOL;
            case Utf8Constant ignored -> ValueType.UTF8;
        };
    }

    /**
     * Whether an operand's value is provably non-null regardless of runtime data. Constants are non-null; a column may
     * carry nulls (unknown at compile time); a step is non-null when its null-producing inputs are non-null -- notably
     * {@code if_*} is non-null iff both branches are (the condition's nullity only steers which branch is chosen, and a
     * null condition falls to the else branch). Matches the interpreter, which emits no NULLS stream for a null-free
     * result so the consumer keeps its null-free fast path.
     */
    private static boolean alwaysNonNull(Operand operand, List<Step> steps)
    {
        return switch (operand) {
            case LongConstant ignored -> true;
            case DoubleConstant ignored -> true;
            case BoolConstant ignored -> true;
            case Utf8Constant ignored -> true;
            case ColumnOperand ignored -> false;
            case StepOperand stepOperand -> {
                Step step = steps.get(stepOperand.stepId());
                List<Operand> args = step.operands();
                yield switch (step.op()) {
                    case "if_i64", "if_f64" -> alwaysNonNull(args.get(1), steps) && alwaysNonNull(args.get(2), steps);
                    default -> {
                        for (Operand argument : args) {
                            if (!alwaysNonNull(argument, steps)) {
                                yield false;
                            }
                        }
                        yield true;
                    }
                };
            }
        };
    }

    // ---- code generation -------------------------------------------------------------------------------------------

    private static String render(Slice slice, String simpleName)
    {
        int outputCount = slice.roots().size();
        boolean[] nullable = new boolean[outputCount];
        ValueType[] outputType = new ValueType[outputCount];
        for (int output = 0; output < outputCount; output++) {
            nullable[output] = !alwaysNonNull(slice.roots().get(output), slice.steps());
            outputType[output] = operandType(slice.roots().get(output));
        }
        StringBuilder out = new StringBuilder();
        out.append("package ").append(PACKAGE).append(";\n");
        out.append("import org.weakref.nitro.data.BooleanVector;\n");
        out.append("import org.weakref.nitro.data.F64Vector;\n");
        out.append("import org.weakref.nitro.data.I32Vector;\n");
        out.append("import org.weakref.nitro.data.I64Vector;\n");
        out.append("import org.weakref.nitro.data.Vector;\n");
        out.append("import org.weakref.nitro.operator.Streams;\n");
        out.append("import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;\n");
        out.append("import org.weakref.nitro.operator.evaluator.ir.Stream;\n");
        out.append("public final class ").append(simpleName).append(" implements org.weakref.nitro.jit.FusedMultiProjection {\n");
        Map<String, Integer> utf8Constants = utf8Constants(slice);
        for (Map.Entry<String, Integer> entry : utf8Constants.entrySet()) {
            byte[] bytes = entry.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.append("  private static final byte[] U").append(entry.getValue()).append(" = new byte[] {");
            for (int index = 0; index < bytes.length; index++) {
                if (index > 0) {
                    out.append(", ");
                }
                out.append(bytes[index]);
            }
            out.append("};\n");
        }
        if (!utf8Constants.isEmpty()) {
            out.append("  private static boolean eqUtf8(byte[] data, int start, int length, byte[] expected) { return length == expected.length && java.util.Arrays.equals(data, start, start + length, expected, 0, length); }\n");
        }
        out.append("  private static final Stream V = Stream.VALUES;\n");
        out.append("  private static final Stream N = Stream.NULLS;\n");
        out.append("  private static final Stream E = Stream.ERRORS;\n");
        out.append("  @Override public Streams[] apply(java.util.List<Streams> inputs, org.weakref.nitro.data.Mask mask, "
                + "java.util.Set<Stream> requestedStreams, PrimitiveExecutionContext context) {\n");

        if (POOLED_DICTIONARY_SCRATCH) {
            out.append("    var scratchContext = context.allocationContext(\"FusedProjectionScratch\");\n");
            for (int slot = 0; slot < slice.columns().size(); slot++) {
                if (slice.columnTypes().get(slot) == ValueType.DOUBLE) {
                    if (MAPPED_DICTIONARY_DOUBLE_INPUTS) {
                        out.append("    I32Vector scratchIds").append(slot).append(" = null;\n");
                    }
                    else {
                        out.append("    F64Vector scratchValues").append(slot).append(" = null;\n");
                    }
                }
                else if (slice.columnTypes().get(slot) != ValueType.UTF8) {
                    out.append("    I64Vector scratchValues").append(slot).append(" = null;\n");
                }
                out.append("    BooleanVector scratchNulls").append(slot).append(" = null;\n");
            }
            out.append("    try {\n");
        }

        // Hoist each source column to a monomorphic long[]/double[] view once plus a nulls[]. If any input is an
        // unsupported flat layout, bail to null so the caller runs the interpreter for this batch.
        for (int slot = 0; slot < slice.columns().size(); slot++) {
            if (slice.columnTypes().get(slot) == ValueType.DOUBLE) {
                appendDoubleColumn(out, slot);
            }
            else if (slice.columnTypes().get(slot) == ValueType.UTF8) {
                appendUtf8Column(out, slot);
            }
            else {
                appendLongColumn(out, slot);
            }
            // NULLS may arrive flat (BooleanVector) or, on a column carried through joins, dictionary-wrapped over a
            // boolean base with its own ids (independent of the VALUES dictionary). Gather the dict case through its ids
            // into a per-row boolean[] so the loop stays monomorphic; bail on any other layout.
            out.append("    Vector nv").append(slot).append(" = inputs.get(").append(slot).append(").getOrNull(N);\n");
            out.append("    boolean[] nul").append(slot).append(";\n");
            out.append("    if (org.weakref.nitro.function.scalar.builtin.VectorAccess.isAllFalseNulls(nv").append(slot)
                    .append(")) { nul").append(slot).append(" = null; }\n");
            out.append("    else if (nv").append(slot).append(" instanceof BooleanVector bv").append(slot)
                    .append(") { nul").append(slot).append(" = bv").append(slot).append(".values(); }\n");
            out.append("    else if (nv").append(slot).append(" instanceof org.weakref.nitro.data.DictionaryVector ndv").append(slot)
                    .append(" && ndv").append(slot).append(".values() instanceof BooleanVector nbase").append(slot)
                    .append(") { int[] nids = ndv").append(slot).append(".ids(); int nlen = ndv").append(slot).append(".length(); boolean[] nb = nbase").append(slot).append(".values();")
                    .append(POOLED_DICTIONARY_SCRATCH
                            ? " scratchNulls" + slot + " = context.allocator().allocatePooled(scratchContext, BooleanVector.class, nlen, false, BooleanVector.class, org.weakref.nitro.data.Allocator.growthCapacity(nlen), BooleanVector::new); nul" + slot + " = scratchNulls" + slot + ".values();"
                            : " nul" + slot + " = new boolean[nlen];")
                    .append(" for (int j = 0; j < nlen; j++) { nul").append(slot)
                    .append("[j] = nb[nids[j]]; } }\n");
            out.append("    else { int nlen = nv").append(slot).append(".length(); nul").append(slot)
                    .append(POOLED_DICTIONARY_SCRATCH
                            ? " = (scratchNulls" + slot + " = context.allocator().allocatePooled(scratchContext, BooleanVector.class, nlen, false, BooleanVector.class, org.weakref.nitro.data.Allocator.growthCapacity(nlen), BooleanVector::new)).values();"
                            : " = new boolean[nlen];")
                    .append(" var na = org.weakref.nitro.function.scalar.builtin.VectorAccess.booleanValues(nv")
                    .append(slot).append("); for (int j = 0; j < nlen; j++) { nul").append(slot)
                    .append("[j] = na.value(j); } }\n");
        }

        out.append("    int required = mask.maxPosition() + 1;\n");
        out.append("    boolean wantNulls = requestedStreams.contains(N);\n");
        for (int output = 0; output < outputCount; output++) {
            if (outputType[output] == ValueType.DOUBLE) {
                out.append("    F64Vector out").append(output).append(" = context.allocator().allocate("
                        + "context.allocationContext(\"FusedProjection\"), F64Vector.class, required, F64Vector::new);\n");
                out.append("    double[] o").append(output).append(" = out").append(output).append(".values();\n");
            }
            else {
                out.append("    I64Vector out").append(output).append(" = context.allocator().allocate("
                        + "context.allocationContext(\"FusedProjection\"), I64Vector.class, required, I64Vector::new);\n");
                out.append("    long[] o").append(output).append(" = out").append(output).append(".values();\n");
            }
            if (nullable[output]) {
                out.append("    BooleanVector outNulls").append(output).append(" = wantNulls ? context.allocator().allocate("
                        + "context.allocationContext(\"FusedProjection\"), BooleanVector.class, required, BooleanVector::new) : null;\n");
                out.append("    boolean[] on").append(output).append(" = wantNulls ? outNulls").append(output).append(".values() : null;\n");
            }
        }

        String body = loopBody(slice, nullable, utf8Constants);
        out.append("    if (mask.all()) {\n");
        out.append("      int n = mask.count();\n");
        out.append("      for (int i = 0; i < n; i++) {\n").append(body).append("      }\n");
        out.append("    } else {\n");
        out.append("      int n = mask.count();\n");
        out.append("      for (int k = 0; k < n; k++) { int i = mask.position(k);\n").append(body).append("      }\n");
        out.append("    }\n");

        // A null-free output emits no NULLS stream (like the interpreter), so the consumer keeps its null-free fast
        // path. The supported op set never raises, so ERRORS is left absent (synthesized all-false downstream if asked).
        out.append("    Streams[] result = new Streams[").append(outputCount).append("];\n");
        for (int output = 0; output < outputCount; output++) {
            out.append("    { Streams.Builder b = Streams.builder();\n");
            out.append("      if (requestedStreams.contains(V)) { b.put(V, out").append(output).append("); }\n");
            if (nullable[output]) {
                out.append("      if (wantNulls) { b.put(N, outNulls").append(output).append("); }\n");
            }
            out.append("      result[").append(output).append("] = b.build(); }\n");
        }
        out.append("    return result;\n");
        if (POOLED_DICTIONARY_SCRATCH) {
            out.append("    } finally {\n");
            for (int slot = 0; slot < slice.columns().size(); slot++) {
                if (slice.columnTypes().get(slot) != ValueType.UTF8) {
                    if (slice.columnTypes().get(slot) == ValueType.DOUBLE && MAPPED_DICTIONARY_DOUBLE_INPUTS) {
                        out.append("      if (scratchIds").append(slot).append(" != null) { context.allocator().release(scratchContext, scratchIds").append(slot).append("); }\n");
                    }
                    else {
                        out.append("      if (scratchValues").append(slot).append(" != null) { context.allocator().release(scratchContext, scratchValues").append(slot).append("); }\n");
                    }
                }
                out.append("      if (scratchNulls").append(slot).append(" != null) { context.allocator().release(scratchContext, scratchNulls").append(slot).append("); }\n");
            }
            out.append("    }\n");
        }
        out.append("  }\n");
        out.append("}\n");
        return out.toString();
    }

    private static void appendLongColumn(StringBuilder out, int slot)
    {
        out.append("    Vector vals").append(slot).append(" = inputs.get(").append(slot).append(").values();\n");
        out.append("    long[] col").append(slot).append(";\n");
        out.append("    if (vals").append(slot).append(" instanceof I64Vector iv").append(slot)
                .append(") { col").append(slot).append(" = iv").append(slot).append(".values(); }\n");
        out.append("    else if (vals").append(slot).append(" instanceof I32Vector wv").append(slot)
                .append(") { int[] s = wv").append(slot).append(".values(); col").append(slot)
                .append(POOLED_DICTIONARY_SCRATCH
                        ? " = (scratchValues" + slot + " = context.allocator().allocatePooled(scratchContext, I64Vector.class, s.length, false, I64Vector.class, org.weakref.nitro.data.Allocator.growthCapacity(s.length), I64Vector::new)).values();"
                        : " = new long[s.length];")
                .append(" for (int j = 0; j < s.length; j++) { col").append(slot)
                .append("[j] = s[j]; } }\n");
        // Join outputs arrive dictionary-wrapped; gather the base values through the ids into a flat long[] once so the
        // per-row loop stays monomorphic (and auto-vectorizable) instead of the interpreter's per-position peel.
        out.append("    else if (vals").append(slot).append(" instanceof org.weakref.nitro.data.DictionaryVector) { int len = vals")
                .append(slot).append(".length(); col").append(slot)
                .append(POOLED_DICTIONARY_SCRATCH
                        ? " = (scratchValues" + slot + " = context.allocator().allocatePooled(scratchContext, I64Vector.class, len, false, I64Vector.class, org.weakref.nitro.data.Allocator.growthCapacity(len), I64Vector::new)).values();"
                        : " = new long[len];")
                .append(" var a = ")
                .append("org.weakref.nitro.function.scalar.builtin.VectorAccess.longValues(vals").append(slot)
                .append("); for (int j = 0; j < len; j++) { col").append(slot).append("[j] = a.value(j); } }\n");
        out.append("    else { return null; }\n");
    }

    private static void appendDoubleColumn(StringBuilder out, int slot)
    {
        out.append("    Vector vals").append(slot).append(" = inputs.get(").append(slot).append(").values();\n");
        out.append("    double[] col").append(slot).append(";\n");
        if (MAPPED_DICTIONARY_DOUBLE_INPUTS) {
            out.append("    int[] map").append(slot).append(";\n");
            out.append("    if (vals").append(slot).append(" instanceof F64Vector fv").append(slot)
                    .append(") { col").append(slot).append(" = fv").append(slot).append(".values(); map").append(slot).append(" = null; }\n");
            out.append("    else if (vals").append(slot).append(" instanceof org.weakref.nitro.data.DictionaryVector dv").append(slot)
                    .append(") { Vector base = dv").append(slot).append(".values(); if (base instanceof F64Vector bf) { col")
                    .append(slot).append(" = bf.values(); map").append(slot).append(" = dv").append(slot).append(".ids();")
                    .append(" }")
                    .append(" else { int len = dv").append(slot).append(".length(); map").append(slot)
                    .append(POOLED_DICTIONARY_SCRATCH
                            ? " = (scratchIds" + slot + " = context.allocator().allocatePooled(scratchContext, I32Vector.class, len, false, I32Vector.class, org.weakref.nitro.data.Allocator.growthCapacity(len), I32Vector::new)).values();"
                            : " = new int[len];")
                    .append(" base = dv").append(slot).append(".composeBasePositions(map").append(slot).append(");")
                    .append(" if (base instanceof F64Vector bf) { col").append(slot).append(" = bf.values();")
                    .append(" } else { return null; } } }\n");
            out.append("    else { return null; }\n");
            return;
        }
        out.append("    if (vals").append(slot).append(" instanceof F64Vector fv").append(slot)
                .append(") { col").append(slot).append(" = fv").append(slot).append(".values(); }\n");
        out.append("    else if (vals").append(slot).append(" instanceof org.weakref.nitro.data.DictionaryVector dv").append(slot)
                .append(") { int[] ids = dv").append(slot).append(".ids(); Vector base = dv").append(slot).append(".values();")
                .append(" if (base instanceof F64Vector bf) { double[] bv = bf.values(); col").append(slot)
                .append(POOLED_DICTIONARY_SCRATCH
                        ? " = (scratchValues" + slot + " = context.allocator().allocatePooled(scratchContext, F64Vector.class, dv" + slot + ".length(), false, F64Vector.class, org.weakref.nitro.data.Allocator.growthCapacity(dv" + slot + ".length()), F64Vector::new)).values();"
                        : " = new double[dv" + slot + ".length()];")
                .append(" for (int j = 0; j < dv").append(slot).append(".length(); j++) { col").append(slot).append("[j] = bv[ids[j]]; } }")
                .append(" else { return null; } }\n");
        out.append("    else { return null; }\n");
    }

    private static void appendUtf8Column(StringBuilder out, int slot)
    {
        out.append("    Vector vals").append(slot).append(" = inputs.get(").append(slot).append(").values();\n");
        out.append("    org.weakref.nitro.function.scalar.builtin.VectorAccess.BinaryValues bin").append(slot)
                .append("; try { bin").append(slot).append(" = org.weakref.nitro.function.scalar.builtin.VectorAccess.binaryValues(vals")
                .append(slot).append("); } catch (IllegalArgumentException e) { return null; }\n");
    }

    /** The per-position body: one local (value, is-null) pair per shared step, then each output's writes. */
    private static String loopBody(Slice slice, boolean[] nullable, Map<String, Integer> utf8Constants)
    {
        StringBuilder body = new StringBuilder();
        for (int slot = 0; slot < slice.columnTypes().size(); slot++) {
            if (slice.columnTypes().get(slot) == ValueType.UTF8) {
                body.append("        var bx").append(slot).append(" = bin").append(slot).append(".value(i);\n");
                body.append("        byte[] bd").append(slot).append(" = bx").append(slot).append(".data();\n");
                body.append("        int bs").append(slot).append(" = bx").append(slot).append(".offset();\n");
                body.append("        int bl").append(slot).append(" = bx").append(slot).append(".length();\n");
            }
        }
        for (Map.Entry<Integer, List<Integer>> entry : utf8Categories(slice, utf8Constants).entrySet()) {
            int slot = entry.getKey();
            body.append("        int bt").append(slot).append(" = -1;\n");
            for (int index = 0; index < entry.getValue().size(); index++) {
                int category = entry.getValue().get(index);
                body.append("        ").append(index == 0 ? "if" : "else if")
                        .append(" (eqUtf8(bd").append(slot).append(", bs").append(slot).append(", bl").append(slot)
                        .append(", U").append(category).append(")) { bt").append(slot).append(" = ").append(category).append("; }\n");
            }
        }
        for (Step step : slice.steps()) {
            String javaType = switch (step.type()) {
                case LONG -> "long";
                case DOUBLE -> "double";
                case BOOL -> "boolean";
                case UTF8 -> throw new Unsupported();
            };
            body.append("        ").append(javaType).append(" sv").append(step.id()).append(" = ").append(valueExpr(step, utf8Constants)).append(";\n");
            // A step's is-null local is only needed when some output (or a downstream step) reads it; the null-free
            // outputs still need the internal nulls of their inputs (e.g. a null condition steering an if), so the
            // is-null locals are always emitted -- the JIT drops the dead ones.
            body.append("        boolean sn").append(step.id()).append(" = ").append(nullExpr(step)).append(";\n");
        }
        List<Operand> roots = slice.roots();
        for (int output = 0; output < roots.size(); output++) {
            body.append("        o").append(output).append("[i] = ").append(value(roots.get(output))).append(";\n");
            if (nullable[output]) {
                body.append("        if (wantNulls) { on").append(output).append("[i] = ").append(isNull(roots.get(output))).append("; }\n");
            }
        }
        return body.toString();
    }

    private static String valueExpr(Step step, Map<String, Integer> utf8Constants)
    {
        List<Operand> args = step.operands();
        return switch (step.op()) {
            case "add", "add_f64" -> "(" + value(args.get(0)) + " + " + value(args.get(1)) + ")";
            case "subtract", "subtract_f64" -> "(" + value(args.get(0)) + " - " + value(args.get(1)) + ")";
            case "multiply", "multiply_f64" -> "(" + value(args.get(0)) + " * " + value(args.get(1)) + ")";
            case "lt", "lt_f64" -> "(" + value(args.get(0)) + " < " + value(args.get(1)) + ")";
            case "gt", "gt_f64" -> "(" + value(args.get(0)) + " > " + value(args.get(1)) + ")";
            case "lte", "lte_f64" -> "(" + value(args.get(0)) + " <= " + value(args.get(1)) + ")";
            case "gte", "gte_f64" -> "(" + value(args.get(0)) + " >= " + value(args.get(1)) + ")";
            case "eq", "eq_f64" -> "(" + value(args.get(0)) + " == " + value(args.get(1)) + ")";
            case "eq_utf8" -> utf8Equals(args.get(0), args.get(1), utf8Constants);
            case "in_utf8" -> utf8In(args, utf8Constants);
            case "and" -> "(" + value(args.get(0)) + " && " + value(args.get(1)) + ")";
            case "or" -> "(" + value(args.get(0)) + " || " + value(args.get(1)) + ")";
            case "not" -> "(!" + value(args.get(0)) + ")";
            // if_*: SQL CASE -- the true branch is taken only when the condition is non-null AND true.
            case "if_i64", "if_f64" -> "((!" + isNull(args.get(0)) + " && " + value(args.get(0)) + ") ? " + value(args.get(1)) + " : " + value(args.get(2)) + ")";
            default -> throw new Unsupported();
        };
    }

    private static String nullExpr(Step step)
    {
        List<Operand> args = step.operands();
        return switch (step.op()) {
            case "add", "subtract", "multiply", "add_f64", "subtract_f64", "multiply_f64",
                 "lt", "gt", "lte", "gte", "eq", "lt_f64", "gt_f64", "lte_f64", "gte_f64", "eq_f64" ->
                    "(" + isNull(args.get(0)) + " || " + isNull(args.get(1)) + ")";
            case "eq_utf8" -> "(" + isNull(args.get(0)) + " || " + isNull(args.get(1)) + ")";
            // The accepted IN shape has one nullable column followed only by non-null constants.
            case "in_utf8" -> isNull(args.getFirst());
            // Three-valued AND: false if either operand is (non-null) false; else null if any operand is null.
            case "and" -> "(!((!" + isNull(args.get(0)) + " && !" + value(args.get(0)) + ") || (!" + isNull(args.get(1)) + " && !" + value(args.get(1)) + "))"
                    + " && (" + isNull(args.get(0)) + " || " + isNull(args.get(1)) + "))";
            // Three-valued OR: true if either operand is (non-null) true; else null if any operand is null.
            case "or" -> "(!((!" + isNull(args.get(0)) + " && " + value(args.get(0)) + ") || (!" + isNull(args.get(1)) + " && " + value(args.get(1)) + "))"
                    + " && (" + isNull(args.get(0)) + " || " + isNull(args.get(1)) + "))";
            case "not" -> isNull(args.get(0));
            case "if_i64", "if_f64" -> "((!" + isNull(args.get(0)) + " && " + value(args.get(0)) + ") ? " + isNull(args.get(1)) + " : " + isNull(args.get(2)) + ")";
            default -> throw new Unsupported();
        };
    }

    private static String value(Operand operand)
    {
        return switch (operand) {
            case ColumnOperand column -> MAPPED_DICTIONARY_DOUBLE_INPUTS && column.type() == ValueType.DOUBLE
                    ? "col" + column.slot() + "[map" + column.slot() + " == null ? i : map" + column.slot() + "[i]]"
                    : "col" + column.slot() + "[i]";
            case StepOperand step -> "sv" + step.stepId();
            case LongConstant constant -> constant.value() + "L";
            case DoubleConstant constant -> Double.toString(constant.value()) + "d";
            case BoolConstant constant -> Boolean.toString(constant.value());
            case Utf8Constant ignored -> throw new Unsupported();
        };
    }

    private static String isNull(Operand operand)
    {
        return switch (operand) {
            case ColumnOperand column -> "(nul" + column.slot() + " != null && nul" + column.slot() + "[i])";
            case StepOperand step -> "sn" + step.stepId();
            case LongConstant ignored -> "false";
            case DoubleConstant ignored -> "false";
            case BoolConstant ignored -> "false";
            case Utf8Constant ignored -> "false";
        };
    }

    private static String utf8Equals(Operand left, Operand right, Map<String, Integer> constants)
    {
        if (left instanceof ColumnOperand column && right instanceof Utf8Constant constant) {
            return "(bt" + column.slot() + " == " + constants.get(constant.value()) + ")";
        }
        if (right instanceof ColumnOperand column && left instanceof Utf8Constant constant) {
            return "(bt" + column.slot() + " == " + constants.get(constant.value()) + ")";
        }
        throw new Unsupported();
    }

    private static String utf8In(List<Operand> operands, Map<String, Integer> constants)
    {
        ColumnOperand column = (ColumnOperand) operands.getFirst();
        StringBuilder expression = new StringBuilder("(");
        for (int index = 1; index < operands.size(); index++) {
            if (index > 1) {
                expression.append(" || ");
            }
            expression.append("bt").append(column.slot()).append(" == ")
                    .append(constants.get(((Utf8Constant) operands.get(index)).value()));
        }
        return expression.append(')').toString();
    }

    private static Map<String, Integer> utf8Constants(Slice slice)
    {
        Map<String, Integer> constants = new LinkedHashMap<>();
        for (Step step : slice.steps()) {
            for (Operand operand : step.operands()) {
                if (operand instanceof Utf8Constant constant) {
                    constants.computeIfAbsent(constant.value(), _ -> constants.size());
                }
            }
        }
        return constants;
    }

    private static Map<Integer, List<Integer>> utf8Categories(Slice slice, Map<String, Integer> constants)
    {
        Map<Integer, List<Integer>> categories = new LinkedHashMap<>();
        for (Step step : slice.steps()) {
            if (!step.op().equals("eq_utf8") && !step.op().equals("in_utf8")) {
                continue;
            }
            if (step.op().equals("in_utf8")) {
                ColumnOperand column = (ColumnOperand) step.operands().getFirst();
                List<Integer> columnCategories = categories.computeIfAbsent(column.slot(), _ -> new ArrayList<>());
                for (int index = 1; index < step.operands().size(); index++) {
                    int category = constants.get(((Utf8Constant) step.operands().get(index)).value());
                    if (!columnCategories.contains(category)) {
                        columnCategories.add(category);
                    }
                }
                continue;
            }
            ColumnOperand column;
            Utf8Constant constant;
            if (step.operands().get(0) instanceof ColumnOperand candidateColumn && step.operands().get(1) instanceof Utf8Constant candidateConstant) {
                column = candidateColumn;
                constant = candidateConstant;
            }
            else if (step.operands().get(1) instanceof ColumnOperand candidateColumn && step.operands().get(0) instanceof Utf8Constant candidateConstant) {
                column = candidateColumn;
                constant = candidateConstant;
            }
            else {
                throw new Unsupported();
            }
            List<Integer> columnCategories = categories.computeIfAbsent(column.slot(), _ -> new ArrayList<>());
            int category = constants.get(constant.value());
            if (!columnCategories.contains(category)) {
                columnCategories.add(category);
            }
        }
        return categories;
    }
}
