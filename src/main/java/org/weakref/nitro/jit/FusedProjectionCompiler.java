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

import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder.ValueType;
import org.weakref.nitro.core.function.projection.ProjectionCodeProvider;
import org.weakref.nitro.core.function.projection.ProjectionProgram;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.jit.ProjectionProgramBuilder.ArgumentNull;
import org.weakref.nitro.jit.ProjectionProgramBuilder.ArgumentValue;
import org.weakref.nitro.jit.ProjectionProgramBuilder.Binary;
import org.weakref.nitro.jit.ProjectionProgramBuilder.BooleanConstant;
import org.weakref.nitro.jit.ProjectionProgramBuilder.BooleanNot;
import org.weakref.nitro.jit.ProjectionProgramBuilder.Conditional;
import org.weakref.nitro.jit.ProjectionProgramBuilder.Expression;
import org.weakref.nitro.jit.ProjectionProgramBuilder.Program;
import org.weakref.nitro.jit.ProjectionProgramBuilder.Utf8Equal;
import org.weakref.nitro.jit.ProjectionProgramBuilder.Utf8StartsWith;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.Operation;
import org.weakref.nitro.operator.evaluator.ir.Producer;
import org.weakref.nitro.operator.evaluator.ir.Reference;
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
 * boolean-vector pipeline. Functions dynamically registered in the primitive registry may provide an opaque physical
 * projection program through the projection-code SPI. This compiler only composes and renders those programs; it has
 * no function-name, function-arity, or function-specific null-semantics vocabulary. Programs outside the supported
 * physical representation are left to the interpreter, so this is a speedup-only substitution behind the same ABI.
 */
public final class FusedProjectionCompiler
        implements AutoCloseable
{
    private static final String PACKAGE = "org.weakref.nitro.jit.generated";
    private static final boolean POOLED_DICTIONARY_SCRATCH =
            Boolean.parseBoolean(System.getProperty("nitro.project.pooledDictionaryScratch", "true"));
    private static final boolean MAPPED_DICTIONARY_DOUBLE_INPUTS =
            Boolean.parseBoolean(System.getProperty("nitro.project.mappedDictionaryDoubleInputs", "true"));
    private final AtomicInteger counter = new AtomicInteger();
    // Kernels are stateless, so identical projection shapes share one compiled class (amortizing javac cost across
    // operators / benchmark iterations). Keyed by the fully-rendered source with a stable placeholder class name.
    private final Map<String, FusedMultiProjection> cache = new ConcurrentHashMap<>();
    private boolean closed;

    public FusedProjectionCompiler() {}

    private enum PhysicalType { LONG, DOUBLE, BOOL, UTF8, NULLS_ONLY }

    private enum Utf8Component { DATA, START, LENGTH }

    private sealed interface Operand
            permits ColumnOperand, StepOperand, LongConstant, DoubleConstant, BoolConstant, Utf8Constant {}

    private record ColumnOperand(int slot, PhysicalType type) implements Operand {}

    private record StepOperand(int stepId, PhysicalType type) implements Operand {}

    private record LongConstant(long value) implements Operand {}

    private record DoubleConstant(double value) implements Operand {}

    private record BoolConstant(boolean value) implements Operand {}

    private record Utf8Constant(String value) implements Operand {}

    /** One shared assignment in the fused program: {@code step<id> = op(operands)}. */
    private record Step(int id, Program program, List<Operand> operands, PhysicalType type) {}

    /** The compiled shape: the ordered source columns (with their Java type) to feed, the shared steps, the roots. */
    private record Slice(List<Integer> columns, List<PhysicalType> columnTypes, List<Step> steps, List<Operand> roots) {}

    /** A fused multi-output kernel, the ordered source columns it expects, and the outputs it produces (in order). */
    public record CompiledMultiProjection(FusedMultiProjection kernel, List<Integer> columns, List<Reference> outputs) {}

    /**
     * Compile every fusible output among {@code candidateOutputs} into one shared-loop kernel. Returns empty if none
     * qualifies. An output qualifies when its whole SSA slice is in the supported set, its result has a supported
     * physical output representation,
     * and it is worth fusing (its slice has at least two operations -- a single-op projection is left to the
     * interpreter, where fusion would only add call overhead without saving an intermediate).
     */
    public Optional<CompiledMultiProjection> tryCompile(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            List<Reference> candidateOutputs)
    {
        if (closed) {
            throw new IllegalStateException("Fused projection compiler is closed");
        }
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
                SliceBuilder trial = new SliceBuilder(assignments, primitiveRegistry);
                Operand root = trial.operand(candidate, null);
                PhysicalType rootType = operandType(root);
                if ((rootType == PhysicalType.LONG || rootType == PhysicalType.DOUBLE || rootType == PhysicalType.UTF8) &&
                        worthFusing(rootType, trial.steps(), trial.columnTypes())) {
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

        SliceBuilder combined = new SliceBuilder(assignments, primitiveRegistry);
        List<Operand> roots = new ArrayList<>();
        for (Reference output : fusible) {
            roots.add(combined.operand(output, null));
        }
        Slice slice = new Slice(combined.columns(), combined.columnTypes(), combined.steps(), roots);

        FusedMultiProjection kernel = cache.computeIfAbsent(render(slice, "K"), ignored -> {
            String simpleName = "FusedProj_" + counter.incrementAndGet();
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

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        cache.clear();
    }

    private static boolean worthFusing(
            PhysicalType rootType,
            List<Step> steps,
            List<PhysicalType> columnTypes)
    {
        // A slice with a single operation writes one output from one primitive; the interpreter already does that with
        // no intermediate vector, so fusing only adds javac + call overhead. Two or more operations means the
        // interpreter materializes at least one intermediate vector that fusion keeps in a register.
        if (steps.size() < 2) {
            return false;
        }
        // Variable-width output requires a sizing pass before the write pass. Two operations do not amortize that
        // second traversal even when it reduces retired work; three operations is the minimum qualified physical
        // shape. This is independent of the provider/function that produced the UTF-8 value.
        if (rootType == PhysicalType.UTF8 && steps.size() < 3) {
            return false;
        }
        // A two-step slice that reads only null streams saves one small boolean intermediate but pays for a generated
        // dense value loop. The interpreter's mask/null-stream path is cheaper at that size. Longer null-only slices
        // amortize the loop, and any value-consuming slice retains the existing two-step threshold.
        return steps.size() >= 3 || columnTypes.stream().anyMatch(type -> type != PhysicalType.NULLS_ONLY);
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
        private final PrimitiveRegistry primitiveRegistry;
        private final List<Integer> columns = new ArrayList<>();
        private final Map<Integer, Integer> columnSlots = new LinkedHashMap<>();
        private final Map<Integer, PhysicalType> columnTypeBySlot = new LinkedHashMap<>();
        private final Map<Integer, Operand> variableSteps = new LinkedHashMap<>();
        private final List<Step> steps = new ArrayList<>();
        private final AtomicInteger nextStep = new AtomicInteger();

        SliceBuilder(Map<Integer, Assignment> assignments, PrimitiveRegistry primitiveRegistry)
        {
            this.assignments = assignments;
            this.primitiveRegistry = primitiveRegistry;
        }

        List<Integer> columns()
        {
            return columns;
        }

        List<PhysicalType> columnTypes()
        {
            List<PhysicalType> types = new ArrayList<>(columns.size());
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
        Operand operand(Reference reference, PhysicalType expected)
        {
            if (reference.stream() != Stream.VALUES) {
                throw new Unsupported();
            }
            Producer producer = reference.producer();
            if (producer instanceof Input input) {
                if (expected == null || expected == PhysicalType.BOOL) {
                    // A raw column can feed a numeric or UTF-8 operator; bool-typed column input is out of scope.
                    throw new Unsupported();
                }
                int slot = columnSlots.computeIfAbsent(input.index(), index -> {
                    columns.add(index);
                    return columns.size() - 1;
                });
                PhysicalType existing = columnTypeBySlot.get(slot);
                if (existing == null) {
                    columnTypeBySlot.put(slot, expected);
                }
                else if (existing == PhysicalType.NULLS_ONLY) {
                    columnTypeBySlot.put(slot, expected);
                }
                else if (expected != PhysicalType.NULLS_ONLY && existing != expected) {
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
                ProjectionCodeProvider provider = (call.resolvedCall() == null
                        ? primitiveRegistry.projectionCodeProvider(call.name())
                        : primitiveRegistry.projectionCodeProvider(call.resolvedCall()))
                        .orElseThrow(Unsupported::new);
                ProjectionProgramBuilder builder = new ProjectionProgramBuilder();
                List<ProjectionArgument> argumentShapes = call.arguments().stream()
                        .map(this::projectionArgument)
                        .toList();
                ProjectionProgram generated = provider.generate(builder, argumentShapes)
                        .orElseThrow(Unsupported::new);
                Program program = builder.requireProgram(generated);
                List<PhysicalType> argumentTypes = program.argumentTypes().stream()
                        .map(FusedProjectionCompiler::physicalType)
                        .toList();
                if (call.arguments().size() != argumentTypes.size()) {
                    throw new IllegalArgumentException("projection program signature does not match call arity");
                }
                List<Operand> operands = new ArrayList<>();
                for (int index = 0; index < call.arguments().size(); index++) {
                    operands.add(operand(call.arguments().get(index), argumentTypes.get(index)));
                }
                int id = nextStep.getAndIncrement();
                PhysicalType resultType = physicalType(program.value().type());
                steps.add(new Step(id, program, operands, resultType));
                return new StepOperand(id, resultType);
            }
            throw new Unsupported();
        }

        private ProjectionArgument projectionArgument(Reference reference)
        {
            if (reference.producer() instanceof Input) {
                return ProjectionArgument.input();
            }
            if (reference.producer() instanceof Variable variable) {
                Assignment assignment = assignments.get(variable.id());
                if (assignment != null && assignment.operation() instanceof Literal literal) {
                    return ProjectionArgument.literal(literal.value());
                }
            }
            return ProjectionArgument.computed();
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

    private static PhysicalType physicalType(ValueType type)
    {
        return switch (type) {
            case I64 -> PhysicalType.LONG;
            case F64 -> PhysicalType.DOUBLE;
            case BOOLEAN -> PhysicalType.BOOL;
            case UTF8 -> PhysicalType.UTF8;
            case NULLS_ONLY -> PhysicalType.NULLS_ONLY;
        };
    }

    private static PhysicalType operandType(Operand operand)
    {
        return switch (operand) {
            case ColumnOperand column -> column.type();
            case StepOperand step -> step.type();
            case LongConstant ignored -> PhysicalType.LONG;
            case DoubleConstant ignored -> PhysicalType.DOUBLE;
            case BoolConstant ignored -> PhysicalType.BOOL;
            case Utf8Constant ignored -> PhysicalType.UTF8;
        };
    }

    /**
     * Whether an operand's provider-supplied null program is provably false. This is a generic boolean possibility
     * analysis over the physical expression graph; the compiler does not infer a function's null convention.
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
                yield !truthPossibilities(step.program().isNull(), step.operands(), steps).canBeTrue();
            }
        };
    }

    private static TruthPossibilities truthPossibilities(
            Expression expression,
            List<Operand> operands,
            List<Step> steps)
    {
        return switch (expression) {
            case ArgumentNull argument ->
                    alwaysNonNull(operands.get(argument.index()), steps)
                            ? TruthPossibilities.FALSE
                            : TruthPossibilities.BOTH;
            case ArgumentValue argument -> {
                Operand operand = operands.get(argument.index());
                yield operand instanceof BoolConstant constant
                        ? TruthPossibilities.of(constant.value())
                        : TruthPossibilities.BOTH;
            }
            case BooleanConstant constant -> TruthPossibilities.of(constant.value());
            case Binary binary -> switch (binary.operation()) {
                case BOOLEAN_AND -> truthPossibilities(binary.left(), operands, steps)
                        .and(truthPossibilities(binary.right(), operands, steps));
                case BOOLEAN_OR -> truthPossibilities(binary.left(), operands, steps)
                        .or(truthPossibilities(binary.right(), operands, steps));
                case LESS_THAN, GREATER_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN_OR_EQUAL, EQUAL ->
                        TruthPossibilities.BOTH;
                case ADD, SUBTRACT, MULTIPLY ->
                        throw new IllegalArgumentException("numeric expression used as a boolean");
            };
            case BooleanNot not -> truthPossibilities(not.value(), operands, steps).not();
            case Conditional conditional -> {
                TruthPossibilities condition = truthPossibilities(conditional.condition(), operands, steps);
                TruthPossibilities whenTrue = truthPossibilities(conditional.whenTrue(), operands, steps);
                TruthPossibilities whenFalse = truthPossibilities(conditional.whenFalse(), operands, steps);
                yield new TruthPossibilities(
                        (condition.canBeTrue() && whenTrue.canBeTrue()) ||
                                (condition.canBeFalse() && whenFalse.canBeTrue()),
                        (condition.canBeTrue() && whenTrue.canBeFalse()) ||
                                (condition.canBeFalse() && whenFalse.canBeFalse()));
            }
            case Utf8Equal _ -> TruthPossibilities.BOTH;
            case Utf8StartsWith _ -> TruthPossibilities.BOTH;
        };
    }

    private record TruthPossibilities(boolean canBeTrue, boolean canBeFalse)
    {
        private static final TruthPossibilities TRUE = new TruthPossibilities(true, false);
        private static final TruthPossibilities FALSE = new TruthPossibilities(false, true);
        private static final TruthPossibilities BOTH = new TruthPossibilities(true, true);

        static TruthPossibilities of(boolean value)
        {
            return value ? TRUE : FALSE;
        }

        TruthPossibilities and(TruthPossibilities other)
        {
            return new TruthPossibilities(
                    canBeTrue && other.canBeTrue,
                    canBeFalse || other.canBeFalse);
        }

        TruthPossibilities or(TruthPossibilities other)
        {
            return new TruthPossibilities(
                    canBeTrue || other.canBeTrue,
                    canBeFalse && other.canBeFalse);
        }

        TruthPossibilities not()
        {
            return new TruthPossibilities(canBeFalse, canBeTrue);
        }
    }

    // ---- code generation -------------------------------------------------------------------------------------------

    private static String render(Slice slice, String simpleName)
    {
        int outputCount = slice.roots().size();
        boolean[] nullable = new boolean[outputCount];
        PhysicalType[] outputType = new PhysicalType[outputCount];
        for (int output = 0; output < outputCount; output++) {
            nullable[output] = !alwaysNonNull(slice.roots().get(output), slice.steps());
            outputType[output] = operandType(slice.roots().get(output));
        }
        StringBuilder out = new StringBuilder();
        out.append("package ").append(PACKAGE).append(";\n");
        out.append("import org.weakref.nitro.data.BinaryVector;\n");
        out.append("import org.weakref.nitro.data.BooleanVector;\n");
        out.append("import org.weakref.nitro.data.F64Vector;\n");
        out.append("import org.weakref.nitro.data.I32Vector;\n");
        out.append("import org.weakref.nitro.data.I64Vector;\n");
        out.append("import org.weakref.nitro.data.Vector;\n");
        out.append("import org.weakref.nitro.data.Streams;\n");
        out.append("import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;\n");
        out.append("import org.weakref.nitro.data.Stream;\n");
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
            out.append("  private static boolean startsWithUtf8(byte[] data, int start, int length, byte[] prefix) { return length >= prefix.length && java.util.Arrays.equals(data, start, start + prefix.length, prefix, 0, prefix.length); }\n");
        }
        out.append("  private static final Stream V = Stream.VALUES;\n");
        out.append("  private static final Stream N = Stream.NULLS;\n");
        out.append("  private static final Stream E = Stream.ERRORS;\n");
        out.append("  @Override public Streams[] apply(java.util.List<Streams> inputs, org.weakref.nitro.data.Mask mask, "
                + "java.util.Set<Stream> requestedStreams, PrimitiveExecutionContext context) {\n");

        if (POOLED_DICTIONARY_SCRATCH) {
            out.append("    var scratchContext = context.allocationContext(\"FusedProjectionScratch\");\n");
            for (int slot = 0; slot < slice.columns().size(); slot++) {
                if (slice.columnTypes().get(slot) == PhysicalType.DOUBLE) {
                    if (MAPPED_DICTIONARY_DOUBLE_INPUTS) {
                        out.append("    I32Vector scratchIds").append(slot).append(" = null;\n");
                    }
                    else {
                        out.append("    F64Vector scratchValues").append(slot).append(" = null;\n");
                    }
                }
                else if (slice.columnTypes().get(slot) != PhysicalType.UTF8 &&
                        slice.columnTypes().get(slot) != PhysicalType.NULLS_ONLY) {
                    out.append("    I64Vector scratchValues").append(slot).append(" = null;\n");
                }
                out.append("    BooleanVector scratchNulls").append(slot).append(" = null;\n");
            }
            out.append("    try {\n");
        }

        // Hoist each source column to a monomorphic long[]/double[] view once plus a nulls[]. If any input is an
        // unsupported flat layout, bail to null so the caller runs the interpreter for this batch.
        for (int slot = 0; slot < slice.columns().size(); slot++) {
            if (slice.columnTypes().get(slot) == PhysicalType.DOUBLE) {
                appendDoubleColumn(out, slot);
            }
            else if (slice.columnTypes().get(slot) == PhysicalType.UTF8) {
                appendUtf8Column(out, slot);
            }
            else if (slice.columnTypes().get(slot) != PhysicalType.NULLS_ONLY) {
                appendLongColumn(out, slot);
            }
            // NULLS may arrive flat (BooleanVector) or, on a column carried through joins, dictionary-wrapped over a
            // boolean base with its own ids (independent of the VALUES dictionary). Gather the dict case through its ids
            // into a per-row boolean[] so the loop stays monomorphic; bail on any other layout.
            out.append("    Vector nv").append(slot).append(" = inputs.get(").append(slot).append(").getOrNull(N);\n");
            out.append("    boolean[] nul").append(slot).append(";\n");
            out.append("    if (org.weakref.nitro.data.VectorAccess.isAllFalseNulls(nv").append(slot)
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
                    .append(" var na = org.weakref.nitro.data.VectorAccess.booleanValues(nv")
                    .append(slot).append("); for (int j = 0; j < nlen; j++) { nul").append(slot)
                    .append("[j] = na.value(j); } }\n");
        }

        out.append("    int required = mask.maxPosition() + 1;\n");
        out.append("    boolean wantNulls = requestedStreams.contains(N);\n");
        String sizingBody = sizingLoopBody(slice, utf8Constants);
        if (!sizingBody.isEmpty()) {
            for (int output = 0; output < outputCount; output++) {
                if (outputType[output] == PhysicalType.UTF8) {
                    out.append("    int bytes").append(output).append(" = 0;\n");
                }
            }
            appendPositionLoop(out, sizingBody);
        }
        for (int output = 0; output < outputCount; output++) {
            if (outputType[output] == PhysicalType.DOUBLE) {
                out.append("    F64Vector out").append(output).append(" = context.allocator().allocate("
                        + "context.allocationContext(\"FusedProjection\"), F64Vector.class, required, F64Vector::new);\n");
                out.append("    double[] o").append(output).append(" = out").append(output).append(".values();\n");
            }
            else if (outputType[output] == PhysicalType.UTF8) {
                out.append("    BinaryVector out").append(output).append(" = BinaryVector.allocate("
                        + "context.allocator(), context.allocationContext(\"FusedProjection\"), required, bytes").append(output).append(");\n");
                out.append("    out").append(output).append(".addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);\n");
                out.append("    byte[] od").append(output).append(" = out").append(output).append(".data();\n");
                out.append("    int[] oo").append(output).append(" = out").append(output).append(".offsets();\n");
                out.append("    int ob").append(output).append(" = 0;\n");
                out.append("    int op").append(output).append(" = 0;\n");
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
        appendPositionLoop(out, body);
        for (int output = 0; output < outputCount; output++) {
            if (outputType[output] == PhysicalType.UTF8) {
                out.append("    java.util.Arrays.fill(oo").append(output).append(", op").append(output)
                        .append(", required + 1, ob").append(output).append(");\n");
            }
        }

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
                if (slice.columnTypes().get(slot) != PhysicalType.UTF8 &&
                        slice.columnTypes().get(slot) != PhysicalType.NULLS_ONLY) {
                    if (slice.columnTypes().get(slot) == PhysicalType.DOUBLE && MAPPED_DICTIONARY_DOUBLE_INPUTS) {
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
                .append("org.weakref.nitro.data.VectorAccess.longValues(vals").append(slot)
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
        out.append("    org.weakref.nitro.data.VectorAccess.BinaryValues bin").append(slot)
                .append("; try { bin").append(slot).append(" = org.weakref.nitro.data.VectorAccess.binaryValues(vals")
                .append(slot).append("); } catch (IllegalArgumentException e) { return null; }\n");
    }

    private static void appendPositionLoop(StringBuilder out, String body)
    {
        out.append("    if (mask.all()) {\n");
        out.append("      int n = mask.count();\n");
        out.append("      for (int i = 0; i < n; i++) {\n").append(body).append("      }\n");
        out.append("    } else {\n");
        out.append("      int n = mask.count();\n");
        out.append("      for (int k = 0; k < n; k++) { int i = mask.position(k);\n").append(body).append("      }\n");
        out.append("    }\n");
    }

    private static String sizingLoopBody(Slice slice, Map<String, Integer> utf8Constants)
    {
        boolean hasUtf8Output = slice.roots().stream()
                .anyMatch(root -> operandType(root) == PhysicalType.UTF8);
        if (!hasUtf8Output) {
            return "";
        }
        StringBuilder body = new StringBuilder(stepBody(slice, utf8Constants));
        for (int output = 0; output < slice.roots().size(); output++) {
            Operand root = slice.roots().get(output);
            if (operandType(root) == PhysicalType.UTF8) {
                body.append("        bytes").append(output).append(" += ")
                        .append(isNull(root)).append(" ? 0 : ")
                        .append(utf8Component(root, Utf8Component.LENGTH, utf8Constants)).append(";\n");
            }
        }
        return body.toString();
    }

    /** The per-position computation shared by the variable-width sizing pass and the output pass. */
    private static String stepBody(Slice slice, Map<String, Integer> utf8Constants)
    {
        StringBuilder body = new StringBuilder();
        for (int slot = 0; slot < slice.columnTypes().size(); slot++) {
            if (slice.columnTypes().get(slot) == PhysicalType.UTF8) {
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
            if (step.type() == PhysicalType.UTF8) {
                body.append("        byte[] svd").append(step.id()).append(" = ")
                        .append(renderUtf8Expression(step.program().value(), step.operands(), Utf8Component.DATA, utf8Constants)).append(";\n");
                body.append("        int svs").append(step.id()).append(" = ")
                        .append(renderUtf8Expression(step.program().value(), step.operands(), Utf8Component.START, utf8Constants)).append(";\n");
                body.append("        int svl").append(step.id()).append(" = ")
                        .append(renderUtf8Expression(step.program().value(), step.operands(), Utf8Component.LENGTH, utf8Constants)).append(";\n");
                body.append("        boolean sn").append(step.id()).append(" = ").append(nullExpr(step, utf8Constants)).append(";\n");
                continue;
            }
            String javaType = switch (step.type()) {
                case LONG -> "long";
                case DOUBLE -> "double";
                case BOOL -> "boolean";
                case UTF8, NULLS_ONLY -> throw new Unsupported();
            };
            body.append("        ").append(javaType).append(" sv").append(step.id()).append(" = ").append(valueExpr(step, utf8Constants)).append(";\n");
            // A step's is-null local is only needed when some output (or a downstream step) reads it; the null-free
            // outputs still need the internal nulls of their inputs (e.g. a null condition steering an if), so the
            // is-null locals are always emitted -- the JIT drops the dead ones.
            body.append("        boolean sn").append(step.id()).append(" = ").append(nullExpr(step, utf8Constants)).append(";\n");
        }
        return body.toString();
    }

    /** The per-position body: one local (value, is-null) pair per shared step, then each output's writes. */
    private static String loopBody(Slice slice, boolean[] nullable, Map<String, Integer> utf8Constants)
    {
        StringBuilder body = new StringBuilder(stepBody(slice, utf8Constants));
        List<Operand> roots = slice.roots();
        for (int output = 0; output < roots.size(); output++) {
            Operand root = roots.get(output);
            if (operandType(root) == PhysicalType.UTF8) {
                String data = utf8Component(root, Utf8Component.DATA, utf8Constants);
                String start = utf8Component(root, Utf8Component.START, utf8Constants);
                String length = "(" + isNull(root) + " ? 0 : " +
                        utf8Component(root, Utf8Component.LENGTH, utf8Constants) + ")";
                body.append("        java.util.Arrays.fill(oo").append(output).append(", op").append(output)
                        .append(", i + 1, ob").append(output).append(");\n");
                body.append("        System.arraycopy(").append(data).append(", ").append(start)
                        .append(", od").append(output).append(", ob").append(output).append(", ").append(length).append(");\n");
                body.append("        ob").append(output).append(" += ").append(length).append(";\n");
                body.append("        oo").append(output).append("[i + 1] = ob").append(output).append(";\n");
                body.append("        op").append(output).append(" = i + 1;\n");
            }
            else {
                body.append("        o").append(output).append("[i] = ").append(value(root)).append(";\n");
            }
            if (nullable[output]) {
                body.append("        if (wantNulls) { on").append(output).append("[i] = ").append(isNull(root)).append("; }\n");
            }
        }
        return body.toString();
    }

    private static String valueExpr(Step step, Map<String, Integer> utf8Constants)
    {
        return renderExpression(step.program().value(), step.operands(), utf8Constants);
    }

    private static String nullExpr(Step step, Map<String, Integer> utf8Constants)
    {
        return renderExpression(step.program().isNull(), step.operands(), utf8Constants);
    }

    private static String renderExpression(
            Expression expression,
            List<Operand> operands,
            Map<String, Integer> utf8Constants)
    {
        return switch (expression) {
            case ArgumentValue argument -> value(operands.get(argument.index()));
            case ArgumentNull argument -> isNull(operands.get(argument.index()));
            case BooleanConstant constant -> Boolean.toString(constant.value());
            case Binary binary -> {
                String left = renderExpression(binary.left(), operands, utf8Constants);
                String right = renderExpression(binary.right(), operands, utf8Constants);
                String operator = switch (binary.operation()) {
                    case ADD -> "+";
                    case SUBTRACT -> "-";
                    case MULTIPLY -> "*";
                    case LESS_THAN -> "<";
                    case GREATER_THAN -> ">";
                    case LESS_THAN_OR_EQUAL -> "<=";
                    case GREATER_THAN_OR_EQUAL -> ">=";
                    case EQUAL -> "==";
                    case BOOLEAN_AND -> "&&";
                    case BOOLEAN_OR -> "||";
                };
                yield "(" + left + " " + operator + " " + right + ")";
            }
            case BooleanNot not ->
                    "(!" + renderExpression(not.value(), operands, utf8Constants) + ")";
            case Conditional conditional ->
                    "(" + renderExpression(conditional.condition(), operands, utf8Constants) +
                            " ? " + renderExpression(conditional.whenTrue(), operands, utf8Constants) +
                            " : " + renderExpression(conditional.whenFalse(), operands, utf8Constants) + ")";
            case Utf8Equal equal -> utf8Equals(
                    resolveOperand(equal.left(), operands),
                    resolveOperand(equal.right(), operands),
                    utf8Constants);
            case Utf8StartsWith startsWith -> utf8StartsWith(
                    resolveOperand(startsWith.value(), operands),
                    resolveOperand(startsWith.prefix(), operands),
                    utf8Constants);
        };
    }

    private static String renderUtf8Expression(
            Expression expression,
            List<Operand> operands,
            Utf8Component component,
            Map<String, Integer> utf8Constants)
    {
        return switch (expression) {
            case ArgumentValue argument ->
                    utf8Component(operands.get(argument.index()), component, utf8Constants);
            case Conditional conditional ->
                    "(" + renderExpression(conditional.condition(), operands, utf8Constants) +
                            " ? " + renderUtf8Expression(conditional.whenTrue(), operands, component, utf8Constants) +
                            " : " + renderUtf8Expression(conditional.whenFalse(), operands, component, utf8Constants) + ")";
            default -> throw new Unsupported();
        };
    }

    private static String utf8Component(
            Operand operand,
            Utf8Component component,
            Map<String, Integer> utf8Constants)
    {
        return switch (operand) {
            case ColumnOperand column -> {
                if (column.type() != PhysicalType.UTF8) {
                    throw new Unsupported();
                }
                yield switch (component) {
                    case DATA -> "bd" + column.slot();
                    case START -> "bs" + column.slot();
                    case LENGTH -> "bl" + column.slot();
                };
            }
            case StepOperand step -> {
                if (step.type() != PhysicalType.UTF8) {
                    throw new Unsupported();
                }
                yield switch (component) {
                    case DATA -> "svd" + step.stepId();
                    case START -> "svs" + step.stepId();
                    case LENGTH -> "svl" + step.stepId();
                };
            }
            case Utf8Constant constant -> {
                Integer category = utf8Constants.get(constant.value());
                if (category == null) {
                    throw new Unsupported();
                }
                yield switch (component) {
                    case DATA -> "U" + category;
                    case START -> "0";
                    case LENGTH -> "U" + category + ".length";
                };
            }
            case LongConstant _, DoubleConstant _, BoolConstant _ -> throw new Unsupported();
        };
    }

    private static Operand resolveOperand(Expression expression, List<Operand> operands)
    {
        if (expression instanceof ArgumentValue argument) {
            return operands.get(argument.index());
        }
        throw new IllegalArgumentException("UTF-8 equality requires direct call arguments");
    }

    private static String value(Operand operand)
    {
        return switch (operand) {
            case ColumnOperand column -> MAPPED_DICTIONARY_DOUBLE_INPUTS && column.type() == PhysicalType.DOUBLE
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

    private static String utf8StartsWith(Operand value, Operand prefix, Map<String, Integer> constants)
    {
        if (value instanceof ColumnOperand column && prefix instanceof Utf8Constant constant) {
            return "startsWithUtf8(bd" + column.slot() + ", bs" + column.slot() + ", bl" + column.slot() +
                    ", U" + constants.get(constant.value()) + ")";
        }
        throw new Unsupported();
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
            collectUtf8Categories(step.program().value(), step.operands(), constants, categories);
            collectUtf8Categories(step.program().isNull(), step.operands(), constants, categories);
        }
        return categories;
    }

    private static void collectUtf8Categories(
            Expression expression,
            List<Operand> operands,
            Map<String, Integer> constants,
            Map<Integer, List<Integer>> categories)
    {
        switch (expression) {
            case ArgumentValue _, ArgumentNull _, BooleanConstant _ -> {}
            case Binary binary -> {
                collectUtf8Categories(binary.left(), operands, constants, categories);
                collectUtf8Categories(binary.right(), operands, constants, categories);
            }
            case BooleanNot not -> collectUtf8Categories(not.value(), operands, constants, categories);
            case Conditional conditional -> {
                collectUtf8Categories(conditional.condition(), operands, constants, categories);
                collectUtf8Categories(conditional.whenTrue(), operands, constants, categories);
                collectUtf8Categories(conditional.whenFalse(), operands, constants, categories);
            }
            case Utf8Equal equal -> {
                Operand left = resolveOperand(equal.left(), operands);
                Operand right = resolveOperand(equal.right(), operands);
                ColumnOperand column;
                Utf8Constant constant;
                if (left instanceof ColumnOperand candidateColumn && right instanceof Utf8Constant candidateConstant) {
                    column = candidateColumn;
                    constant = candidateConstant;
                }
                else if (right instanceof ColumnOperand candidateColumn && left instanceof Utf8Constant candidateConstant) {
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
            case Utf8StartsWith _ -> {
                // Prefix predicates consume bytes directly and do not participate in equality category precomputation.
            }
        }
    }
}
