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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of aggregate code generators. An aggregate contributes the three code fragments of a distributive
 * aggregation — {@code identity} (the empty-group state), {@code update} (fold one input value into the state),
 * and {@code merge} (combine two partial states) — as Java expressions over caller-supplied state and input
 * lvalues. The compiler owns all the storage machinery (global accumulators, the grouping hash table, the
 * speculative array, and the deopt migration) and simply asks each aggregate to emit these fragments at the
 * right points. New aggregates are added by {@link #register registering} a generator; the compiler needs no
 * per-aggregate {@code switch}.
 * <p>
 * Each generator here keeps a single {@code long} state cell, which is also its output. Multi-cell aggregates
 * (e.g. {@code avg} as sum + count) and non-{@code long} outputs (e.g. {@code double}) are the planned
 * extension: the generator would declare a cell count and a {@code finalize} fragment, and the compiler would
 * widen the result columns accordingly.
 */
public final class AggregateLibrary
{
    /** Emits the code fragments for one distributive aggregate over a single {@code long} state cell. */
    public interface AggregateCompiler
    {
        /** The state value for a freshly created (empty) group. */
        String identity();

        /** New state value folding {@code input} into {@code state}; {@code input} is null for nullary aggregates. */
        String update(String state, String input);

        /** New state value combining two partial states {@code left} and {@code right}. */
        String merge(String left, String right);
    }

    private static final Map<String, AggregateCompiler> REGISTRY = new ConcurrentHashMap<>();

    static {
        // sum / count are additive: identity 0, fold and merge both add.
        register("sum", new AggregateCompiler()
        {
            @Override public String identity()
            {
                return "0L";
            }

            @Override public String update(String state, String input)
            {
                return "(" + state + " + " + input + ")";
            }

            @Override public String merge(String left, String right)
            {
                return "(" + left + " + " + right + ")";
            }
        });
        register("count", new AggregateCompiler()
        {
            @Override public String identity()
            {
                return "0L";
            }

            @Override public String update(String state, String input)
            {
                return "(" + state + " + 1L)";
            }

            @Override public String merge(String left, String right)
            {
                return "(" + left + " + " + right + ")";
            }
        });
        register("min", extreme("Math.min"));
        register("max", extreme("Math.max"));
    }

    private AggregateLibrary() {}

    public static void register(String name, AggregateCompiler compiler)
    {
        REGISTRY.put(name, compiler);
    }

    public static AggregateCompiler get(String name)
    {
        AggregateCompiler compiler = REGISTRY.get(name);
        if (compiler == null) {
            throw new UnsupportedOperationException("aggregate: " + name);
        }
        return compiler;
    }

    /** min/max share a shape: identity is the absorbing element of the other side, fold and merge are the same reduction. */
    private static AggregateCompiler extreme(String reduction)
    {
        String identity = reduction.equals("Math.min") ? "Long.MAX_VALUE" : "Long.MIN_VALUE";
        return new AggregateCompiler()
        {
            @Override public String identity()
            {
                return identity;
            }

            @Override public String update(String state, String input)
            {
                return reduction + "(" + state + ", " + input + ")";
            }

            @Override public String merge(String left, String right)
            {
                return reduction + "(" + left + ", " + right + ")";
            }
        };
    }
}
