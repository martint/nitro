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
        String simpleName = "BatchFn_" + COUNTER.incrementAndGet();
        String source = render(expression, simpleName);
        try {
            Class<?> compiled = InMemoryCompiler.compile(PACKAGE + "." + simpleName, source);
            return (PrimitiveFunction) compiled.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate compiled batch function:\n" + source, e);
        }
    }

    /** Exposed for inspection/tests: the Java source that would be compiled. */
    public static String render(Plan.Expr expression, String simpleName)
    {
        TreeSet<Integer> columns = new TreeSet<>();
        collectColumns(expression, columns);

        StringBuilder out = new StringBuilder();
        out.append("package ").append(PACKAGE).append(";\n");
        out.append("public final class ").append(simpleName)
                .append(" implements org.weakref.nitro.operator.evaluator.PrimitiveFunction {\n");
        out.append("  private static final org.weakref.nitro.operator.evaluator.ir.Stream V = org.weakref.nitro.operator.evaluator.ir.Stream.VALUES;\n");
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
        // Dense path: contiguous, unit-stride -- the auto-vectorizable shape. Sparse path: gather through the mask.
        out.append("    if (mask.all()) {\n");
        out.append("      int n = mask.count();\n");
        out.append("      for (int i = 0; i < n; i++) { o[i] = ").append(value).append("; }\n");
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
            case Plan.Bin bin -> ScalarLibrary.get(bin.op()).emit(java.util.List.of(expr(bin.left()), expr(bin.right())));
            case Plan.Call call -> ScalarLibrary.get(call.name()).emit(call.arguments().stream().map(BatchFunctionCompiler::expr).toList());
            case Plan.Case ignored -> throw new UnsupportedOperationException("CASE not yet supported in batch functions");
            case Plan.Coalesce ignored -> throw new UnsupportedOperationException("COALESCE not yet supported in batch functions");
        };
    }

    private static void collectColumns(Plan.Expr expression, TreeSet<Integer> into)
    {
        switch (expression) {
            case Plan.Col col -> into.add(col.index());
            case Plan.Lit ignored -> {}
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
