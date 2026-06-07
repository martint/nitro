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
 * Registry of aggregate code generators. An aggregate is described by the fragments of a distributive
 * aggregation over {@link #cells} {@code long} state cells: {@code identity} (empty-group state),
 * {@code update} (fold one input value), {@code merge} (combine two partial states), and {@code result}
 * (finalize the cells to one output value of {@link #outputType}). The compiler owns all the storage machinery
 * (global accumulators, the grouping hash table, the speculative array, and the deopt migration) and asks each
 * aggregate to emit these fragments at the right points; it knows no aggregate by name. New aggregates are
 * added by {@link #register registering} a generator.
 * <p>
 * Cells are passed as Java lvalue strings (e.g. {@code "agg0[gid]"}). A {@code DOUBLE}-typed result returns the
 * {@code doubleToRawLongBits} of its value, stored in the {@code long} result column.
 */
public final class AggregateLibrary
{
    public interface AggregateCompiler
    {
        /** Number of {@code long} state cells. */
        default int cells()
        {
            return 1;
        }

        /** Logical type of the finalized output. */
        default Type outputType()
        {
            return Types.LONG;
        }

        /** Emit {@code cell = identity} for each state cell of a freshly created group. */
        void emitIdentity(StringBuilder out, String indent, List<String> cells);

        /** Emit the fold of {@code input} into {@code cells}; {@code input} is null for nullary aggregates. */
        void emitUpdate(StringBuilder out, String indent, List<String> cells, String input);

        /** Emit the combine of partial state {@code other} into {@code cells}. */
        void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other);

        /** The output value expression (a {@code long}; for {@code DOUBLE} output, its {@code doubleToRawLongBits}). */
        String result(List<String> cells);

        /**
         * Boolean expression that is true when the finalized result is SQL NULL (e.g. an average over zero
         * non-null inputs). {@code null} means the aggregate is never null (the default; e.g. {@code count}).
         */
        default String resultNull(List<String> cells)
        {
            return null;
        }
    }

    private static final Map<String, AggregateCompiler> REGISTRY = new ConcurrentHashMap<>();

    static {
        register("sum", new AggregateCompiler()
        {
            @Override public int cells()
            {
                return 2;   // [0] = sum, [1] = count of non-null inputs (0 -> SQL NULL)
            }

            @Override public void emitIdentity(StringBuilder out, String indent, List<String> cells)
            {
                out.append(indent).append(cells.get(0)).append(" = 0L;\n");
                out.append(indent).append(cells.get(1)).append(" = 0L;\n");
            }

            @Override public void emitUpdate(StringBuilder out, String indent, List<String> cells, String input)
            {
                out.append(indent).append(cells.get(0)).append(" = ").append(cells.get(0)).append(" + ").append(input).append(";\n");
                out.append(indent).append(cells.get(1)).append(" = ").append(cells.get(1)).append(" + 1L;\n");
            }

            @Override public void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other)
            {
                out.append(indent).append(cells.get(0)).append(" = ").append(cells.get(0)).append(" + ").append(other.get(0)).append(";\n");
                out.append(indent).append(cells.get(1)).append(" = ").append(cells.get(1)).append(" + ").append(other.get(1)).append(";\n");
            }

            @Override public String result(List<String> cells)
            {
                return cells.get(0);
            }

            @Override public String resultNull(List<String> cells)
            {
                return cells.get(1) + " == 0L";   // sum over zero non-null inputs is NULL, not 0 (SQL semantics)
            }
        });
        register("count", additive("1L"));
        register("min", extreme("Math.min", "Long.MAX_VALUE"));
        register("max", extreme("Math.max", "Long.MIN_VALUE"));
        register("avg", new AggregateCompiler()
        {
            @Override public int cells()
            {
                return 2;   // [0] = sum, [1] = count
            }

            @Override public Type outputType()
            {
                return Types.DOUBLE;
            }

            @Override public void emitIdentity(StringBuilder out, String indent, List<String> cells)
            {
                out.append(indent).append(cells.get(0)).append(" = 0L;\n");
                out.append(indent).append(cells.get(1)).append(" = 0L;\n");
            }

            @Override public void emitUpdate(StringBuilder out, String indent, List<String> cells, String input)
            {
                out.append(indent).append(cells.get(0)).append(" = ").append(cells.get(0)).append(" + ").append(input).append(";\n");
                out.append(indent).append(cells.get(1)).append(" = ").append(cells.get(1)).append(" + 1L;\n");
            }

            @Override public void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other)
            {
                out.append(indent).append(cells.get(0)).append(" = ").append(cells.get(0)).append(" + ").append(other.get(0)).append(";\n");
                out.append(indent).append(cells.get(1)).append(" = ").append(cells.get(1)).append(" + ").append(other.get(1)).append(";\n");
            }

            @Override public String result(List<String> cells)
            {
                return "Double.doubleToRawLongBits(" + cells.get(1) + " == 0L ? 0.0 : (double) " + cells.get(0) + " / (double) " + cells.get(1) + ")";
            }

            @Override public String resultNull(List<String> cells)
            {
                return cells.get(1) + " == 0L";   // average over zero non-null inputs is NULL
            }
        });
        // Sample standard deviation: cells [count, sum, sum of squares]. Long state can overflow on large
        // inputs (the production path would keep the moments in double or 128-bit) -- adequate for the prototype.
        register("stddev", new AggregateCompiler()
        {
            @Override public int cells()
            {
                return 3;
            }

            @Override public Type outputType()
            {
                return Types.DOUBLE;
            }

            @Override public void emitIdentity(StringBuilder out, String indent, List<String> cells)
            {
                out.append(indent).append(cells.get(0)).append(" = 0L;\n");
                out.append(indent).append(cells.get(1)).append(" = 0L;\n");
                out.append(indent).append(cells.get(2)).append(" = 0L;\n");
            }

            @Override public void emitUpdate(StringBuilder out, String indent, List<String> cells, String input)
            {
                out.append(indent).append(cells.get(0)).append(" = ").append(cells.get(0)).append(" + 1L;\n");
                out.append(indent).append(cells.get(1)).append(" = ").append(cells.get(1)).append(" + ").append(input).append(";\n");
                out.append(indent).append(cells.get(2)).append(" = ").append(cells.get(2)).append(" + ").append(input).append(" * ").append(input).append(";\n");
            }

            @Override public void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other)
            {
                out.append(indent).append(cells.get(0)).append(" = ").append(cells.get(0)).append(" + ").append(other.get(0)).append(";\n");
                out.append(indent).append(cells.get(1)).append(" = ").append(cells.get(1)).append(" + ").append(other.get(1)).append(";\n");
                out.append(indent).append(cells.get(2)).append(" = ").append(cells.get(2)).append(" + ").append(other.get(2)).append(";\n");
            }

            @Override public String result(List<String> cells)
            {
                String n = "(double) " + cells.get(0);
                String sum = "(double) " + cells.get(1);
                String sumSquares = "(double) " + cells.get(2);
                String variance = "((" + sumSquares + " - " + sum + " * " + sum + " / " + n + ") / (double) (" + cells.get(0) + " - 1L))";
                return "Double.doubleToRawLongBits(" + cells.get(0) + " < 2L ? 0.0 : Math.sqrt(" + variance + "))";
            }

            @Override public String resultNull(List<String> cells)
            {
                return cells.get(0) + " < 2L";   // sample standard deviation needs at least two values
            }
        });
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

    /** Single additive cell: identity 0, fold/merge add. {@code foldValue} null means "add the input" (sum). */
    private static AggregateCompiler additive(String foldValue)
    {
        return new AggregateCompiler()
        {
            @Override public void emitIdentity(StringBuilder out, String indent, List<String> cells)
            {
                out.append(indent).append(cells.get(0)).append(" = 0L;\n");
            }

            @Override public void emitUpdate(StringBuilder out, String indent, List<String> cells, String input)
            {
                String added = foldValue != null ? foldValue : input;
                out.append(indent).append(cells.get(0)).append(" = ").append(cells.get(0)).append(" + ").append(added).append(";\n");
            }

            @Override public void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other)
            {
                out.append(indent).append(cells.get(0)).append(" = ").append(cells.get(0)).append(" + ").append(other.get(0)).append(";\n");
            }

            @Override public String result(List<String> cells)
            {
                return cells.get(0);
            }
        };
    }

    /** Single cell reduced by {@code reduction} (Math.min/Math.max) with the given absorbing {@code identity}. */
    private static AggregateCompiler extreme(String reduction, String identity)
    {
        return new AggregateCompiler()
        {
            @Override public void emitIdentity(StringBuilder out, String indent, List<String> cells)
            {
                out.append(indent).append(cells.get(0)).append(" = ").append(identity).append(";\n");
            }

            @Override public void emitUpdate(StringBuilder out, String indent, List<String> cells, String input)
            {
                out.append(indent).append(cells.get(0)).append(" = ").append(reduction).append("(").append(cells.get(0)).append(", ").append(input).append(");\n");
            }

            @Override public void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other)
            {
                out.append(indent).append(cells.get(0)).append(" = ").append(reduction).append("(").append(cells.get(0)).append(", ").append(other.get(0)).append(");\n");
            }

            @Override public String result(List<String> cells)
            {
                return cells.get(0);
            }
        };
    }
}
