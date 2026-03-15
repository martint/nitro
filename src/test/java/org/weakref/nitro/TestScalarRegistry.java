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
package org.weakref.nitro;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.function.scalar.ScalarDescriptor;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.function.scalar.ScalarRegistry;
import org.weakref.nitro.function.scalar.builtin.AddI64;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestScalarRegistry
{
    @Test
    void testRegistersAnnotatedScalarFunction()
    {
        ScalarRegistry registry = new ScalarRegistry();

        ScalarDescriptor descriptor = registry.register(TestAddI64.class);

        assertThat(descriptor.name()).isEqualTo("add");
        assertThat(descriptor.deterministic()).isTrue();
        assertThat(descriptor.implementation()).isInstanceOf(TestAddI64.class);
        assertThat(registry.get("add")).isEqualTo(descriptor);
    }

    @Test
    void testPrimitiveRegistryRegistersVectorizedFunction()
    {
        ScalarRegistry scalarRegistry = new ScalarRegistry();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();

        ScalarDescriptor descriptor = scalarRegistry.register(AddI64.class);
        primitiveRegistry.register(descriptor);

        assertThat(descriptor.implementation()).isInstanceOf(AddI64.class);
        assertThat(primitiveRegistry.get("add")).isInstanceOf(AddI64.class);
    }

    @ScalarFunction(name = "add")
    public static final class TestAddI64
            implements PrimitiveFunction
    {
        @Override
        public Streams apply(java.util.List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
        {
            return new AddI64().apply(inputs, mask, requestedStreams, output, context);
        }
    }
}
