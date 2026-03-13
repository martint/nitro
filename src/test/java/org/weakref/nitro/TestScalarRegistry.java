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
import org.weakref.nitro.function.scalar.ScalarDescriptor;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.function.scalar.ScalarImplementation;
import org.weakref.nitro.function.scalar.ScalarRegistry;
import org.weakref.nitro.function.scalar.builtin.AddI64;
import org.weakref.nitro.function.scalar.generated.AddI64Primitive;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import static org.assertj.core.api.Assertions.assertThat;

public class TestScalarRegistry
{
    @Test
    void testRegistersAnnotatedScalarFunction()
            throws ReflectiveOperationException
    {
        ScalarRegistry registry = new ScalarRegistry();

        ScalarDescriptor descriptor = registry.register(TestAddI64.class);

        assertThat(descriptor.name()).isEqualTo("add");
        assertThat(descriptor.arity()).isEqualTo(2);
        assertThat(descriptor.implementation().getReturnType()).isEqualTo(long.class);
        assertThat(descriptor.implementation().getParameterTypes()).containsExactly(long.class, long.class);
        assertThat(descriptor.deterministic()).isTrue();
        assertThat((long) descriptor.implementation().invoke(null, 7L, 8L)).isEqualTo(15L);
        assertThat(registry.get("add")).isEqualTo(descriptor);
    }

    @Test
    void testPrimitiveRegistryPrefersGeneratedVectorizedAdapter()
    {
        ScalarRegistry scalarRegistry = new ScalarRegistry();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();

        ScalarDescriptor descriptor = scalarRegistry.register(AddI64.class);
        primitiveRegistry.register(descriptor);

        assertThat(descriptor.vectorizedAdapter()).isEqualTo(AddI64Primitive.class);
        assertThat(primitiveRegistry.get("add")).isInstanceOf(AddI64Primitive.class);
    }

    @ScalarFunction(name = "add")
    public static final class TestAddI64
    {
        private TestAddI64() {}

        @ScalarImplementation
        public static long apply(long left, long right)
        {
            return left + right;
        }
    }
}
