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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBatchFunctionCompiler
{
    @Test
    void compilesFusedI64Expression()
    {
        // out = a*b + c*d  -- a four-input scalar subtree fused into one batch function (one loop, no per-node buffer).
        Plan.Expr expression = new Plan.Bin("+",
                new Plan.Bin("*", new Plan.Col(0), new Plan.Col(1)),
                new Plan.Bin("*", new Plan.Col(2), new Plan.Col(3)));
        PrimitiveFunction function = BatchFunctionCompiler.compile(expression);

        int rows = 4096;
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
        PrimitiveExecutionContext context = new PrimitiveExecutionContext(new Allocator());

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
}
