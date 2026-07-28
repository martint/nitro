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

import org.weakref.nitro.function.scalar.AnnotatedScalarLoader;
import org.weakref.nitro.function.scalar.ScalarDescriptor;
import org.weakref.nitro.function.scalar.ScalarRegistry;
import org.weakref.nitro.function.scalar.builtin.AddExactI64;
import org.weakref.nitro.function.scalar.builtin.AddF64;
import org.weakref.nitro.function.scalar.builtin.AddI64;
import org.weakref.nitro.function.scalar.builtin.AndBoolean;
import org.weakref.nitro.function.scalar.builtin.ArrayContainsI64;
import org.weakref.nitro.function.scalar.builtin.ArrayElementI64;
import org.weakref.nitro.function.scalar.builtin.ArrayMinI64;
import org.weakref.nitro.function.scalar.builtin.ArraySumI64;
import org.weakref.nitro.function.scalar.builtin.Cardinality;
import org.weakref.nitro.function.scalar.builtin.CastI64ToF64;
import org.weakref.nitro.function.scalar.builtin.CastI64ToI32;
import org.weakref.nitro.function.scalar.builtin.CastUtf8ToI64;
import org.weakref.nitro.function.scalar.builtin.CoalesceI64;
import org.weakref.nitro.function.scalar.builtin.CoalesceI64Policy;
import org.weakref.nitro.function.scalar.builtin.ConcatUtf8;
import org.weakref.nitro.function.scalar.builtin.ContainsUtf8;
import org.weakref.nitro.function.scalar.builtin.DivideF64;
import org.weakref.nitro.function.scalar.builtin.DivideI64;
import org.weakref.nitro.function.scalar.builtin.DivideI64ToF64;
import org.weakref.nitro.function.scalar.builtin.DivideRoundI64;
import org.weakref.nitro.function.scalar.builtin.DivideScaleI64;
import org.weakref.nitro.function.scalar.builtin.DivideScaleRoundI64;
import org.weakref.nitro.function.scalar.builtin.ElementAtI64Utf8;
import org.weakref.nitro.function.scalar.builtin.ElementAtUtf8Utf8;
import org.weakref.nitro.function.scalar.builtin.EqualF64;
import org.weakref.nitro.function.scalar.builtin.EqualI64;
import org.weakref.nitro.function.scalar.builtin.EqualUtf8;
import org.weakref.nitro.function.scalar.builtin.ExtractHostUtf8;
import org.weakref.nitro.function.scalar.builtin.GreaterThanF64;
import org.weakref.nitro.function.scalar.builtin.GreaterThanOrEqualF64;
import org.weakref.nitro.function.scalar.builtin.HashUtf8;
import org.weakref.nitro.function.scalar.builtin.IfF64;
import org.weakref.nitro.function.scalar.builtin.IfI32;
import org.weakref.nitro.function.scalar.builtin.IfI64;
import org.weakref.nitro.function.scalar.builtin.IfUtf8;
import org.weakref.nitro.function.scalar.builtin.InUtf8;
import org.weakref.nitro.function.scalar.builtin.IsNullI32;
import org.weakref.nitro.function.scalar.builtin.IsNullI64;
import org.weakref.nitro.function.scalar.builtin.JoniRegexpPolicy;
import org.weakref.nitro.function.scalar.builtin.LengthUtf8;
import org.weakref.nitro.function.scalar.builtin.LessThanF64;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualF64;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualI64;
import org.weakref.nitro.function.scalar.builtin.LessThanUtf8;
import org.weakref.nitro.function.scalar.builtin.LikeUtf8;
import org.weakref.nitro.function.scalar.builtin.LikeUtf8Policy;
import org.weakref.nitro.function.scalar.builtin.MapContainsKeyUtf8;
import org.weakref.nitro.function.scalar.builtin.MapKeys;
import org.weakref.nitro.function.scalar.builtin.MapValues;
import org.weakref.nitro.function.scalar.builtin.ModuloI64;
import org.weakref.nitro.function.scalar.builtin.MultiplyF64;
import org.weakref.nitro.function.scalar.builtin.MultiplyI64;
import org.weakref.nitro.function.scalar.builtin.MultiplyNullAsZeroI64;
import org.weakref.nitro.function.scalar.builtin.NotBoolean;
import org.weakref.nitro.function.scalar.builtin.NullI64;
import org.weakref.nitro.function.scalar.builtin.OrBoolean;
import org.weakref.nitro.function.scalar.builtin.RegexpReplaceUtf8;
import org.weakref.nitro.function.scalar.builtin.RegexpReplaceUtf8Policy;
import org.weakref.nitro.function.scalar.builtin.RoundF64;
import org.weakref.nitro.function.scalar.builtin.ScaledRelativeDifferenceGtI64;
import org.weakref.nitro.function.scalar.builtin.StartsWithUtf8;
import org.weakref.nitro.function.scalar.builtin.SubstringUtf8;
import org.weakref.nitro.function.scalar.builtin.SubtractExactI64;
import org.weakref.nitro.function.scalar.builtin.SubtractF64;
import org.weakref.nitro.function.scalar.builtin.SubtractI64;
import org.weakref.nitro.function.scalar.builtin.UpperUtf8;
import org.weakref.nitro.function.scalar.builtin.Utf8BinaryDispatchPolicy;
import org.weakref.nitro.function.scalar.builtin.YearOfDate;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.util.List;

public final class TestPrimitiveFunctions
{
    private TestPrimitiveFunctions() {}

    public static PrimitiveRegistry primitiveRegistry()
    {
        ScalarRegistry scalarRegistry = new ScalarRegistry();
        AnnotatedScalarLoader scalarLoader = new AnnotatedScalarLoader();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        Utf8BinaryDispatchPolicy utf8Policy = Utf8BinaryDispatchPolicy.fromSystemProperties();
        for (Class<?> functionClass : List.of(
                AddI64.class,
                ArrayContainsI64.class,
                ArrayElementI64.class,
                ArrayMinI64.class,
                ArraySumI64.class,
                Cardinality.class,
                CastI64ToF64.class,
                CastI64ToI32.class,
                CastUtf8ToI64.class,
                CoalesceI64.class,
                ConcatUtf8.class,
                ContainsUtf8.class,
                DivideF64.class,
                ExtractHostUtf8.class,
                ElementAtI64Utf8.class,
                ElementAtUtf8Utf8.class,
                EqualF64.class,
                EqualI64.class,
                EqualUtf8.class,
                GreaterThanF64.class,
                GreaterThanOrEqualF64.class,
                LessThanF64.class,
                LessThanOrEqualF64.class,
                LessThanOrEqualI64.class,
                MultiplyF64.class,
                SubtractF64.class,
                AddF64.class,
                HashUtf8.class,
                IfF64.class,
                IfI32.class,
                IfI64.class,
                IfUtf8.class,
                IsNullI32.class,
                IsNullI64.class,
                InUtf8.class,
                LessThanI64.class,
                LikeUtf8.class,
                LessThanUtf8.class,
                LengthUtf8.class,
                MapContainsKeyUtf8.class,
                MapKeys.class,
                MapValues.class,
                NotBoolean.class,
                NullI64.class,
                ScaledRelativeDifferenceGtI64.class,
                AddExactI64.class,
                SubtractI64.class,
                SubtractExactI64.class,
                MultiplyI64.class,
                MultiplyNullAsZeroI64.class,
                DivideI64.class,
                DivideI64ToF64.class,
                DivideRoundI64.class,
                DivideScaleI64.class,
                DivideScaleRoundI64.class,
                ModuloI64.class,
                AndBoolean.class,
                OrBoolean.class,
                RegexpReplaceUtf8.class,
                RoundF64.class,
                StartsWithUtf8.class,
                SubstringUtf8.class,
                UpperUtf8.class,
                YearOfDate.class)) {
            primitiveRegistry.register(scalarRegistry.register(load(scalarLoader, functionClass, utf8Policy)));
        }
        return primitiveRegistry;
    }

    private static ScalarDescriptor load(
            AnnotatedScalarLoader scalarLoader,
            Class<?> functionClass,
            Utf8BinaryDispatchPolicy utf8Policy)
    {
        if (functionClass == LikeUtf8.class) {
            return scalarLoader.load(new LikeUtf8(LikeUtf8Policy.fromSystemProperties()));
        }
        if (functionClass == CoalesceI64.class) {
            return scalarLoader.load(new CoalesceI64(CoalesceI64Policy.fromSystemProperties()));
        }
        if (functionClass == RegexpReplaceUtf8.class) {
            return scalarLoader.load(new RegexpReplaceUtf8(
                    RegexpReplaceUtf8Policy.fromSystemProperties(),
                    JoniRegexpPolicy.fromSystemProperties()));
        }
        if (functionClass == EqualUtf8.class ||
                functionClass == LessThanUtf8.class ||
                functionClass == StartsWithUtf8.class ||
                functionClass == ContainsUtf8.class ||
                functionClass == InUtf8.class) {
            if (functionClass == EqualUtf8.class) {
                return scalarLoader.load(new EqualUtf8(utf8Policy));
            }
            if (functionClass == LessThanUtf8.class) {
                return scalarLoader.load(new LessThanUtf8(utf8Policy));
            }
            if (functionClass == StartsWithUtf8.class) {
                return scalarLoader.load(new StartsWithUtf8(utf8Policy));
            }
            if (functionClass == ContainsUtf8.class) {
                return scalarLoader.load(new ContainsUtf8(utf8Policy));
            }
            return scalarLoader.load(new InUtf8(utf8Policy));
        }
        return scalarLoader.load(functionClass);
    }
}
