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
import java.util.Set;
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

    /**
     * Names of scalar functions whose result is a DOUBLE regardless of argument types (e.g. an integer average that
     * yields a true floating-point quotient). The compiler consults this so the projection / sort machinery encodes
     * such a call's slot as raw double bits and compares it as DOUBLE.
     */
    private static final Set<String> DOUBLE_RESULTS = ConcurrentHashMap.newKeySet();

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
        register("divide_round_i64", arguments ->
                "org.weakref.nitro.jit.DecimalMath.roundDivide(" + arguments.get(0) + ", " + arguments.get(1) + ")");
        // True (non-rounded) average of two longs as a DOUBLE: sum / count in floating point. Unlike
        // divide_round_i64 (a rounded long quotient), the result is a real double, so it is registered as
        // double-returning -- the projection / sort machinery then encodes its slot via doubleToRawLongBits and
        // treats it (and any ORDER BY over it) as DOUBLE.
        register("divide_i64_to_f64", arguments ->
                "((double) " + arguments.get(0) + " / (double) " + arguments.get(1) + ")");
        DOUBLE_RESULTS.add("divide_i64_to_f64");
        // True double / double division (IEEE), mirroring the interpreted divide_f64 primitive. Both operands arrive
        // already decoded to doubles (the projection resolver decodes a DOUBLE column), so this is a plain division;
        // the projection re-encodes the double result. Used for the coefficient of variation (stddev_samp / avg).
        register("divide_f64", arguments ->
                "(" + arguments.get(0) + " / " + arguments.get(1) + ")");
        DOUBLE_RESULTS.add("divide_f64");
        // Reinterpret a long column holding raw double bits as a DOUBLE. A DOUBLE value materialized by an earlier
        // stage is stored as a flat long[] of bits; the consuming stage scans it as a long, so this names it back to a
        // DOUBLE for output and double-typed comparison. (Ordering still runs on the raw bits, which for positive
        // doubles is the same order.) The argument is the raw bits; decoding then DOUBLE-result re-encoding round-trips.
        register("reinterpret_f64", arguments -> "Double.longBitsToDouble(" + arguments.get(0) + ")");
        DOUBLE_RESULTS.add("reinterpret_f64");
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

    /** Whether the named scalar function returns a DOUBLE irrespective of its argument types. */
    public static boolean isDoubleResult(String name)
    {
        return DOUBLE_RESULTS.contains(name);
    }

    private static ScalarCompiler infix(String operator)
    {
        return arguments -> "(" + arguments.get(0) + " " + operator + " " + arguments.get(1) + ")";
    }
}
