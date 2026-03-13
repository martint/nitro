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
package org.weakref.nitro.function.scalar.generated;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.function.scalar.ScalarDescriptor;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.I64BinaryVectorSupport;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;

public final class LessThanI64Primitive
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("LessThanI64Primitive");

    public LessThanI64Primitive() {}

    public LessThanI64Primitive(ScalarDescriptor descriptor)
    {
        GeneratedPrimitiveSupport.verifyBinaryArity(descriptor.name(), descriptor.arity());
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Streams output, PrimitiveExecutionContext context)
    {
        GeneratedPrimitiveSupport.verifyBinaryArity("lt", inputs.size());
        return Streams.of(
                Stream.VALUES,
                I64BinaryVectorSupport.applyLongComparison(
                        "lt",
                        inputs.get(0).values(),
                        inputs.get(1).values(),
                        mask,
                        output != null && output.has(Stream.VALUES) ? output.values() : null,
                        context,
                        ALLOCATION_CONTEXT,
                        LessThanI64::apply));
    }
}
