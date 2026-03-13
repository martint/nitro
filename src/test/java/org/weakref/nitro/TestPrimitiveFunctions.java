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

import org.weakref.nitro.function.scalar.ScalarRegistry;
import org.weakref.nitro.function.scalar.builtin.AddExactI64;
import org.weakref.nitro.function.scalar.builtin.AddI64;
import org.weakref.nitro.function.scalar.builtin.DivideI64;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.function.scalar.builtin.ModuloI64;
import org.weakref.nitro.function.scalar.builtin.MultiplyI64;
import org.weakref.nitro.function.scalar.builtin.OrBoolean;
import org.weakref.nitro.function.scalar.builtin.SubtractExactI64;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.util.List;

public final class TestPrimitiveFunctions
{
    private TestPrimitiveFunctions() {}

    public static PrimitiveRegistry primitiveRegistry()
    {
        ScalarRegistry scalarRegistry = new ScalarRegistry();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        for (Class<?> functionClass : List.of(
                AddI64.class,
                LessThanI64.class,
                AddExactI64.class,
                SubtractExactI64.class,
                MultiplyI64.class,
                DivideI64.class,
                ModuloI64.class,
                OrBoolean.class)) {
            primitiveRegistry.register(scalarRegistry.register(functionClass));
        }
        return primitiveRegistry;
    }
}
