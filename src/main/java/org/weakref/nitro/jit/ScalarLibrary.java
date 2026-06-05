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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of scalar function code generators. A function contributes a single fragment — given the rendered
 * Java expressions of its already-evaluated arguments, it returns the Java expression for its result. The
 * compiler resolves {@link Plan.Call}s and {@link Plan.Bin}s through here, so new functions are added by
 * {@link #register registering} a generator rather than by extending a {@code switch} in the compiler.
 * <p>
 * Simple functions inline as an expression (arithmetic, {@code abs}); functions too complex to inline would
 * instead emit a call to a precompiled runtime helper, and anything unregistered could fall back to the
 * interpreted vectorized kernel — both planned, neither needed by the current long-typed set.
 */
public final class ScalarLibrary
{
    /** Emits the Java result expression for a scalar function given its arguments' rendered expressions. */
    public interface ScalarCompiler
    {
        String emit(List<String> arguments);
    }

    private static final Map<String, ScalarCompiler> REGISTRY = new ConcurrentHashMap<>();

    static {
        register("+", infix("+"));
        register("-", infix("-"));
        register("*", infix("*"));
        register("/", infix("/"));
        register("%", infix("%"));
        register("negate", arguments -> "(-" + arguments.get(0) + ")");
        register("abs", arguments -> "Math.abs(" + arguments.get(0) + ")");
        register("least", arguments -> "Math.min(" + arguments.get(0) + ", " + arguments.get(1) + ")");
        register("greatest", arguments -> "Math.max(" + arguments.get(0) + ", " + arguments.get(1) + ")");
        // Fixed-point (decimal) arithmetic on scaled longs. Names and semantics mirror the interpreted built-ins
        // so a compiled stage and the operator tree agree to the cent: multiply_i64 is a long product; the
        // rescaling round-half-up divide is computed in BigInteger via a runtime helper.
        register("multiply_i64", infix("*"));
        register("divide_scale_round_i64", arguments ->
                "org.weakref.nitro.jit.DecimalMath.roundScaledDivide(" + arguments.get(0) + ", " + arguments.get(1) + ", " + arguments.get(2) + ")");
    }

    private ScalarLibrary() {}

    public static void register(String name, ScalarCompiler compiler)
    {
        REGISTRY.put(name, compiler);
    }

    public static ScalarCompiler get(String name)
    {
        ScalarCompiler compiler = REGISTRY.get(name);
        if (compiler == null) {
            throw new UnsupportedOperationException("scalar function: " + name);
        }
        return compiler;
    }

    private static ScalarCompiler infix(String operator)
    {
        return arguments -> "(" + arguments.get(0) + " " + operator + " " + arguments.get(1) + ")";
    }
}
