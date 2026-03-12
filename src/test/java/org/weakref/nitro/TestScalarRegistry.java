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

import static org.assertj.core.api.Assertions.assertThat;

public class TestScalarRegistry
{
    @Test
    void testRegistersAnnotatedScalarFunction()
            throws ReflectiveOperationException
    {
        ScalarRegistry registry = new ScalarRegistry();

        ScalarDescriptor descriptor = registry.register(AddBigint.class);

        assertThat(descriptor.name()).isEqualTo("add");
        assertThat(descriptor.returnType()).isEqualTo("BIGINT");
        assertThat(descriptor.argumentTypes()).containsExactly("BIGINT", "BIGINT");
        assertThat(descriptor.deterministic()).isTrue();
        assertThat((long) descriptor.implementation().invoke(null, 7L, 8L)).isEqualTo(15L);
        assertThat(registry.get("add")).isEqualTo(descriptor);
    }

    @ScalarFunction(name = "add", returnType = "BIGINT", argumentTypes = {"BIGINT", "BIGINT"})
    public static final class AddBigint
    {
        private AddBigint() {}

        @ScalarImplementation
        public static long apply(long left, long right)
        {
            return left + right;
        }
    }
}
