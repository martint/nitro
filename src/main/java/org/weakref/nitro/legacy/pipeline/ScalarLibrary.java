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
package org.weakref.nitro.legacy.pipeline;

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

    private final Map<String, ScalarCompiler> registry = new ConcurrentHashMap<>();

    /**
     * Names of scalar functions whose result is a DOUBLE regardless of argument types (e.g. an integer average that
     * yields a true floating-point quotient). The compiler consults this so the projection / sort machinery encodes
     * such a call's slot as raw double bits and compares it as DOUBLE.
     */
    private final Set<String> doubleResults = ConcurrentHashMap.newKeySet();

    public ScalarLibrary()
    {
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
        // F64 arithmetic over raw-bits long lanes: unwrap, operate, rewrap.
        register("add_f64", f64Infix("+"));
        register("subtract_f64", f64Infix("-"));
        register("multiply_f64", f64Infix("*"));
        register("divide_f64", f64Infix("/"));
        // Widen a long lane to a DOUBLE riding the lane as raw bits (row-loop convention), composing with the
        // _f64 operators and predicates (TPC-H supplycost * availqty, availqty > threshold).
        register("cast_i64_to_f64", arguments ->
                "Double.doubleToRawLongBits((double) (" + arguments.get(0) + "))");
        // The civil-calendar year of an epoch-day lane (TPC-H date columns).
        register("year_of_date", arguments ->
                "org.weakref.nitro.function.scalar.builtin.YearOfDate.yearOfEpochDay(" + arguments.get(0) + ")");
        register("divide_scale_round_i64", arguments ->
                "org.weakref.nitro.legacy.pipeline.DecimalMath.roundScaledDivide(" + arguments.get(0) + ", " + arguments.get(1) + ", " + arguments.get(2) + ")");
        register("divide_round_i64", arguments ->
                "org.weakref.nitro.legacy.pipeline.DecimalMath.roundDivide(" + arguments.get(0) + ", " + arguments.get(1) + ")");
        // True (non-rounded) average of two longs as a DOUBLE: sum / count in floating point. Unlike
        // divide_round_i64 (a rounded long quotient), the result is a real double, so it is registered as
        // double-returning -- the projection / sort machinery then encodes its slot via doubleToRawLongBits and
        // treats it (and any ORDER BY over it) as DOUBLE.
        register("divide_i64_to_f64", arguments ->
                "((double) " + arguments.get(0) + " / (double) " + arguments.get(1) + ")");
        doubleResults.add("divide_i64_to_f64");
        // SQL round(DOUBLE) semantics used at result boundaries: nearest integer with exact halves away from zero.
        // Math.round differs for negative halves, so express the rule directly and keep it available to every
        // compiled shape through the scalar registry rather than embedding it in a query lowering.
        register("round_f64", arguments ->
                "Math.copySign(Math.floor(Math.abs(" + arguments.get(0) + ") + 0.5d), " + arguments.get(0) + ")");
        doubleResults.add("round_f64");
        // True double / double division (IEEE), mirroring the interpreted divide_f64 primitive. Both operands arrive
        // already decoded to doubles (the projection resolver decodes a DOUBLE column), so this is a plain division;
        // the projection re-encodes the double result. Used for the coefficient of variation (stddev_samp / avg).
        register("divide_f64", arguments ->
                "(" + arguments.get(0) + " / " + arguments.get(1) + ")");
        doubleResults.add("divide_f64");
        // Reinterpret a long column holding raw double bits as a DOUBLE. A DOUBLE value materialized by an earlier
        // stage is stored as a flat long[] of bits; the consuming stage scans it as a long, so this names it back to a
        // DOUBLE for output and double-typed comparison. (Ordering still runs on the raw bits, which for positive
        // doubles is the same order.) The argument is the raw bits; decoding then DOUBLE-result re-encoding round-trips.
        register("reinterpret_f64", arguments -> "Double.longBitsToDouble(" + arguments.get(0) + ")");
        doubleResults.add("reinterpret_f64");
    }

    public void register(String name, ScalarCompiler compiler)
    {
        registry.put(name, compiler);
    }

    public ScalarCompiler get(String name)
    {
        ScalarCompiler compiler = registry.get(name);
        if (compiler == null) {
            throw new UnsupportedOperationException("scalar function: " + name);
        }
        return compiler;
    }

    /** Whether the named scalar function returns a DOUBLE irrespective of its argument types. */
    public boolean isDoubleResult(String name)
    {
        return doubleResults.contains(name);
    }

    private static ScalarCompiler f64Infix(String operator)
    {
        return arguments -> "Double.doubleToRawLongBits(Double.longBitsToDouble(" + arguments.get(0) + ") "
                + operatorToken(operator) + " Double.longBitsToDouble(" + arguments.get(1) + "))";
    }

    private static String operatorToken(String operator)
    {
        return operator;
    }

    private static ScalarCompiler infix(String operator)
    {
        return arguments -> "(" + arguments.get(0) + " " + operator + " " + arguments.get(1) + ")";
    }
}
