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

import org.weakref.nitro.operator.evaluator.PrimitiveFunction;

import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Compiles a scalar expression subtree into a single bespoke {@link PrimitiveFunction} -- one fused loop over the
 * batch, with intermediate values held in registers, honoring the engine's batch calling convention. This is the
 * expression-fusion increment of the data-centric architecture folded back onto the vectorized operator engine:
 * instead of the interpreter walking the subtree and invoking one atomic batch primitive per node (materializing a
 * buffer each), the whole subtree becomes one specialized function the operator substrate can drop in behind the
 * same ABI.
 * <p>
 * First increment scope: {@code I64} values only (no nulls/errors), flat {@link org.weakref.nitro.data.I64Vector}
 * inputs. The dense ({@code mask.all()}) path is a unit-stride contiguous loop -- the shape the JIT's auto-vectorizer
 * (or, later, an explicit Vector-API form) can lower to SIMD; the sparse path gathers through the selection.
 */
public final class BatchFunctionCompiler
{
    private static final String PACKAGE = "org.weakref.nitro.jit.generated";
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private BatchFunctionCompiler() {}

    /** Compile {@code expression} (over {@code I64} input columns referenced by {@link Plan.Col} index) into a fused batch function. */
    public static PrimitiveFunction compile(Plan.Expr expression)
    {
        return compile(expression, false);
    }

    /**
     * Compile {@code expression} into a fused batch function. When {@code explicitVector} is set and the expression
     * is lane-wise vectorizable ({@code + - *} over columns/literals), the dense path is emitted with the explicit
     * Vector API ({@link jdk.incubator.vector.LongVector}) -- full control over SIMD, for the ops C2's
     * auto-vectorizer reaches inconsistently or not at all -- with a scalar tail; otherwise the dense path is a
     * plain counted loop the JIT may auto-vectorize. The sparse (gather) path is always scalar.
     */
    public static PrimitiveFunction compile(Plan.Expr expression, boolean explicitVector)
    {
        String simpleName = "BatchFn_" + COUNTER.incrementAndGet();
        String source = render(expression, simpleName, explicitVector);
        try {
            Class<?> compiled = InMemoryCompiler.compile(PACKAGE + "." + simpleName, source);
            return (PrimitiveFunction) compiled.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate compiled batch function:\n" + source, e);
        }
    }

    /** Exposed for inspection/tests: the Java source that would be compiled (scalar dense path). */
    public static String render(Plan.Expr expression, String simpleName)
    {
        return render(expression, simpleName, false);
    }

    /** Exposed for inspection/tests: the Java source that would be compiled. */
    public static String render(Plan.Expr expression, String simpleName, boolean explicitVector)
    {
        TreeSet<Integer> columns = new TreeSet<>();
        collectColumns(expression, columns);
        boolean vector = explicitVector && vectorizable(expression);

        StringBuilder out = new StringBuilder();
        out.append("package ").append(PACKAGE).append(";\n");
        out.append("public final class ").append(simpleName)
                .append(" implements org.weakref.nitro.operator.evaluator.PrimitiveFunction {\n");
        out.append("  private static final org.weakref.nitro.operator.evaluator.ir.Stream V = org.weakref.nitro.operator.evaluator.ir.Stream.VALUES;\n");
        if (vector) {
            out.append("  private static final jdk.incubator.vector.VectorSpecies<Long> S = jdk.incubator.vector.LongVector.SPECIES_PREFERRED;\n");
        }
        out.append("  @Override public org.weakref.nitro.operator.Streams apply("
                + "java.util.List<org.weakref.nitro.operator.Streams> inputs, "
                + "org.weakref.nitro.data.Mask mask, "
                + "java.util.Set<org.weakref.nitro.operator.evaluator.ir.Stream> requestedStreams, "
                + "org.weakref.nitro.operator.Streams output, "
                + "org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext context) {\n");
        out.append("    if (!requestedStreams.contains(V)) { return org.weakref.nitro.operator.Streams.empty(); }\n");
        for (int column : columns) {
            out.append("    long[] in").append(column).append(" = ((org.weakref.nitro.data.I64Vector) inputs.get(")
                    .append(column).append(").values()).values();\n");
        }
        out.append("    int required = mask.maxPosition() + 1;\n");
        out.append("    org.weakref.nitro.data.I64Vector out = context.allocator().allocateOrGrow("
                + "context.allocationContext(\"BatchFunction\"), "
                + "output != null && output.getOrNull(V) instanceof org.weakref.nitro.data.I64Vector reuse ? reuse : null, "
                + "org.weakref.nitro.data.I64Vector.class, required, org.weakref.nitro.data.I64Vector::new);\n");
        out.append("    long[] o = out.values();\n");
        String value = expr(expression);
        // Dense path: contiguous, unit-stride. Either explicit SIMD over LongVector lanes + a scalar tail, or a
        // plain counted loop the JIT may auto-vectorize. Sparse path: gather through the mask (always scalar).
        out.append("    if (mask.all()) {\n");
        out.append("      int n = mask.count();\n");
        if (vector) {
            out.append("      int upper = S.loopBound(n); int i = 0;\n");
            out.append("      for (; i < upper; i += S.length()) { (").append(vectorExpr(expression)).append(").intoArray(o, i); }\n");
            out.append("      for (; i < n; i++) { o[i] = ").append(value).append("; }\n");
        }
        else {
            out.append("      for (int i = 0; i < n; i++) { o[i] = ").append(value).append("; }\n");
        }
        out.append("    }\n");
        out.append("    else {\n");
        out.append("      int n = mask.count();\n");
        out.append("      for (int k = 0; k < n; k++) { int i = mask.position(k); o[i] = ").append(value).append("; }\n");
        out.append("    }\n");
        out.append("    return org.weakref.nitro.operator.Streams.ofValues(out);\n");
        out.append("  }\n");
        out.append("}\n");
        return out.toString();
    }

    /** Render the per-position scalar expression, indexed by the loop variable {@code i}. */
    private static String expr(Plan.Expr expression)
    {
        return switch (expression) {
            case Plan.Col col -> "in" + col.index() + "[i]";
            case Plan.Lit lit -> lit.value() + "L";
            case Plan.LitF64 lit -> Double.doubleToRawLongBits(lit.value()) + "L /* " + lit.value() + " */";
            case Plan.LitStr ignored -> throw new UnsupportedOperationException("string literal projection is JIT-only");
            case Plan.NullLit ignored -> throw new UnsupportedOperationException("null literal projection is JIT-only");
            case Plan.Bin bin -> ScalarLibrary.get(bin.op()).emit(java.util.List.of(expr(bin.left()), expr(bin.right())));
            case Plan.Call call -> ScalarLibrary.get(call.name()).emit(call.arguments().stream().map(BatchFunctionCompiler::expr).toList());
            case Plan.Case ignored -> throw new UnsupportedOperationException("CASE not yet supported in batch functions");
            case Plan.Coalesce ignored -> throw new UnsupportedOperationException("COALESCE not yet supported in batch functions");
        };
    }

    /** Whether the expression is lane-wise vectorizable with {@link jdk.incubator.vector.LongVector}: {@code + - *} over columns/literals. */
    private static boolean vectorizable(Plan.Expr expression)
    {
        return switch (expression) {
            case Plan.Col ignored -> true;
            case Plan.Lit ignored -> true;
            case Plan.Bin bin -> (bin.op().equals("+") || bin.op().equals("-") || bin.op().equals("*"))
                    && vectorizable(bin.left()) && vectorizable(bin.right());
            default -> false;
        };
    }

    /** Render the expression as a {@link jdk.incubator.vector.LongVector} value loaded at the loop offset {@code i}. */
    private static String vectorExpr(Plan.Expr expression)
    {
        return switch (expression) {
            case Plan.Col col -> "jdk.incubator.vector.LongVector.fromArray(S, in" + col.index() + ", i)";
            case Plan.Lit lit -> "jdk.incubator.vector.LongVector.broadcast(S, " + lit.value() + "L)";
            case Plan.Bin bin -> {
                String op = switch (bin.op()) {
                    case "+" -> "add";
                    case "-" -> "sub";
                    case "*" -> "mul";
                    default -> throw new IllegalStateException("non-vectorizable op reached vectorExpr: " + bin.op());
                };
                yield "(" + vectorExpr(bin.left()) + ")." + op + "(" + vectorExpr(bin.right()) + ")";
            }
            default -> throw new IllegalStateException("non-vectorizable expression reached vectorExpr: " + expression);
        };
    }

    private static void collectColumns(Plan.Expr expression, TreeSet<Integer> into)
    {
        switch (expression) {
            case Plan.Col col -> into.add(col.index());
            case Plan.Lit ignored -> {}
            case Plan.LitF64 ignored -> {}
            case Plan.LitStr ignored -> {}
            case Plan.NullLit ignored -> {}
            case Plan.Bin bin -> {
                collectColumns(bin.left(), into);
                collectColumns(bin.right(), into);
            }
            case Plan.Call call -> call.arguments().forEach(argument -> collectColumns(argument, into));
            case Plan.Case kase -> {
                kase.branches().forEach(branch -> collectColumns(branch.value(), into));
                collectColumns(kase.defaultValue(), into);
            }
            case Plan.Coalesce coalesce -> coalesce.arguments().forEach(argument -> collectColumns(argument, into));
        }
    }
}
