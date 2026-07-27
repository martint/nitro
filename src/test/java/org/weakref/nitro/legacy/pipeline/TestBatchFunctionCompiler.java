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
package org.weakref.nitro.legacy.pipeline;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBatchFunctionCompiler
{
    @Test
    void functionRegistriesAreExplicitAndIsolated()
    {
        ScalarLibrary first = new ScalarLibrary();
        ScalarLibrary second = new ScalarLibrary();
        AggregateLibrary firstAggregates = new AggregateLibrary();
        AggregateLibrary secondAggregates = new AggregateLibrary();
        first.register("custom_increment", arguments -> "(" + arguments.getFirst() + " + 1L)");
        firstAggregates.register("custom_sum", firstAggregates.get("sum"));

        String source = new BatchFunctionCompiler(new CompilerResources(new Types(), first, new AggregateLibrary()))
                .render(new Plan.Call("custom_increment", new Plan.Col(0)), "CustomIncrement");
        assertThat(source).contains("(in0[i] + 1L)");
        assertThatThrownBy(() -> new BatchFunctionCompiler(new CompilerResources(new Types(), second, new AggregateLibrary()))
                .render(new Plan.Call("custom_increment", new Plan.Col(0)), "MissingCustomIncrement"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("scalar function: custom_increment");
        assertThat(firstAggregates.get("custom_sum")).isSameAs(firstAggregates.get("sum"));
        assertThatThrownBy(() -> secondAggregates.get("custom_sum"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("aggregate: custom_sum");
    }

    @Test
    void typeRegistrySnapshotsDoNotObserveLaterRegistrations()
    {
        Types types = new Types();
        var snapshot = types.snapshotResolver();
        Type custom = new TestingType("custom");

        types.register(custom);

        assertThat(types.get("custom")).isSameAs(custom);
        assertThatThrownBy(() -> snapshot.apply("custom"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("unknown type: custom");
    }

    @Test
    void closingCompilerResourcesIsTerminal()
    {
        CompilerResources resources = CompilerResources.createDefault();
        BatchFunctionCompiler compiler = new BatchFunctionCompiler(resources);
        resources.close();

        assertThatThrownBy(() -> compiler.compile(new Plan.Bin("+", new Plan.Col(0), new Plan.Lit(1))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Compiler resources are closed");
    }

    @Test
    void compilesFusedI64Expression()
    {
        // out = a*b + c*d  -- a four-input scalar subtree fused into one batch function (one loop, no per-node buffer).
        Plan.Expr expression = new Plan.Bin("+",
                new Plan.Bin("*", new Plan.Col(0), new Plan.Col(1)),
                new Plan.Bin("*", new Plan.Col(2), new Plan.Col(3)));
        // Both the scalar (auto-vectorizable) and explicit Vector-API dense paths must match.
        for (boolean explicitVector : new boolean[] {false, true}) {
            assertFusedExpressionMatchesNaive(new BatchFunctionCompiler(CompilerResources.createDefault()).compile(expression, explicitVector));
        }
    }

    private static void assertFusedExpressionMatchesNaive(PrimitiveFunction function)
    {
        int rows = 4099;   // not a multiple of the SIMD width, so the explicit-vector path exercises its scalar tail
        long[] a = new long[rows];
        long[] b = new long[rows];
        long[] c = new long[rows];
        long[] d = new long[rows];
        for (int i = 0; i < rows; i++) {
            a[i] = i;
            b[i] = (i % 7) - 3;       // mix of negatives
            c[i] = (i % 13) + 1;
            d[i] = (i % 5) - 2;
        }
        List<Streams> inputs = List.of(
                Streams.ofValues(new I64Vector(a)), Streams.ofValues(new I64Vector(b)),
                Streams.ofValues(new I64Vector(c)), Streams.ofValues(new I64Vector(d)));
        PrimitiveExecutionContext context = new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault()));

        // Dense (mask.all): the contiguous, auto-vectorizable path.
        long[] dense = ((I64Vector) function.apply(inputs, Mask.all(rows), Set.of(Stream.VALUES), null, context).values()).values();
        for (int i = 0; i < rows; i++) {
            assertThat(dense[i]).as("dense row %d", i).isEqualTo(a[i] * b[i] + c[i] * d[i]);
        }

        // Sparse selection: gather through the mask; only selected positions are written.
        int[] selection = new int[rows / 4];
        for (int k = 0; k < selection.length; k++) {
            selection[k] = k * 4 + 1;
        }
        long[] sparse = ((I64Vector) function.apply(inputs, Mask.sparse(selection, rows), Set.of(Stream.VALUES), null, context).values()).values();
        for (int p : selection) {
            assertThat(sparse[p]).as("sparse row %d", p).isEqualTo(a[p] * b[p] + c[p] * d[p]);
        }
    }

    private record TestingType(String name)
            implements Type
    {
        @Override
        public String compare(String a, String b)
        {
            return "Long.compare(" + a + ", " + b + ")";
        }

        @Override
        public String decode(String slot)
        {
            return slot;
        }

        @Override
        public org.weakref.nitro.data.Vector toVector(long[] slots, int count, byte[][] dictionary)
        {
            return new I64Vector(java.util.Arrays.copyOf(slots, count));
        }
    }
}
