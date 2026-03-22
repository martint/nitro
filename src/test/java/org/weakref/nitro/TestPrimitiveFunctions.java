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
import org.weakref.nitro.function.scalar.builtin.AndBoolean;
import org.weakref.nitro.function.scalar.builtin.ArrayContainsI64;
import org.weakref.nitro.function.scalar.builtin.ArrayElementI64;
import org.weakref.nitro.function.scalar.builtin.ArrayMinI64;
import org.weakref.nitro.function.scalar.builtin.ArraySumI64;
import org.weakref.nitro.function.scalar.builtin.Cardinality;
import org.weakref.nitro.function.scalar.builtin.ContainsUtf8;
import org.weakref.nitro.function.scalar.builtin.DivideI64;
import org.weakref.nitro.function.scalar.builtin.ElementAtI64Utf8;
import org.weakref.nitro.function.scalar.builtin.ElementAtUtf8Utf8;
import org.weakref.nitro.function.scalar.builtin.EqualI64;
import org.weakref.nitro.function.scalar.builtin.EqualUtf8;
import org.weakref.nitro.function.scalar.builtin.ExtractHostUtf8;
import org.weakref.nitro.function.scalar.builtin.HashUtf8;
import org.weakref.nitro.function.scalar.builtin.IfUtf8;
import org.weakref.nitro.function.scalar.builtin.LengthUtf8;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.function.scalar.builtin.LessThanUtf8;
import org.weakref.nitro.function.scalar.builtin.MapContainsKeyUtf8;
import org.weakref.nitro.function.scalar.builtin.MapKeys;
import org.weakref.nitro.function.scalar.builtin.MapValues;
import org.weakref.nitro.function.scalar.builtin.ModuloI64;
import org.weakref.nitro.function.scalar.builtin.MultiplyI64;
import org.weakref.nitro.function.scalar.builtin.OrBoolean;
import org.weakref.nitro.function.scalar.builtin.StartsWithUtf8;
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
                ArrayContainsI64.class,
                ArrayElementI64.class,
                ArrayMinI64.class,
                ArraySumI64.class,
                Cardinality.class,
                ContainsUtf8.class,
                ExtractHostUtf8.class,
                ElementAtI64Utf8.class,
                ElementAtUtf8Utf8.class,
                EqualI64.class,
                EqualUtf8.class,
                HashUtf8.class,
                IfUtf8.class,
                LessThanI64.class,
                LessThanUtf8.class,
                LengthUtf8.class,
                MapContainsKeyUtf8.class,
                MapKeys.class,
                MapValues.class,
                AddExactI64.class,
                SubtractExactI64.class,
                MultiplyI64.class,
                DivideI64.class,
                ModuloI64.class,
                AndBoolean.class,
                OrBoolean.class,
                StartsWithUtf8.class)) {
            primitiveRegistry.register(scalarRegistry.register(functionClass));
        }
        return primitiveRegistry;
    }
}
