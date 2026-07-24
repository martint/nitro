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

        /**
         * As {@link #emitUpdate(StringBuilder, String, List, String)}, additionally given the input column's
         * dictionary variable when the input is a dictionary-string column's id ({@code null} otherwise). Value
         * aggregates ignore it (the default); a string aggregate (e.g. {@code min_utf8}) compares entries through it.
         */
        default void emitUpdate(StringBuilder out, String indent, List<String> cells, String input, String dictionary)
        {
            emitUpdate(out, indent, cells, input);
        }

        /** Emit the combine of partial state {@code other} into {@code cells}. */
        void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other);

        /** As {@link #emitMerge(StringBuilder, String, List, List)}, with the input column's dictionary variable (see the update overload). */
        default void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other, String dictionary)
        {
            emitMerge(out, indent, cells, other);
        }

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

    private final Map<String, AggregateCompiler> registry = new ConcurrentHashMap<>();

    public AggregateLibrary()
    {
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
        // F64 aggregates over raw-bits long lanes: the cells hold the running double's bits.
        register("sum_f64", f64Fold("+", "0.0", true));
        register("min_f64", f64Fold("Math.min", "Double.POSITIVE_INFINITY", false));
        register("max_f64", f64Fold("Math.max", "Double.NEGATIVE_INFINITY", false));
        register("avg_f64", new AggregateCompiler()
        {
            @Override public int cells()
            {
                return 2;   // [0] = bits of the running sum, [1] = count of non-null inputs
            }

            @Override public Type outputType()
            {
                return Types.DOUBLE;
            }

            @Override public void emitIdentity(StringBuilder out, String indent, List<String> cells)
            {
                out.append(indent).append(cells.get(0)).append(" = 0L;\n");   // bits(0.0) == 0L
                out.append(indent).append(cells.get(1)).append(" = 0L;\n");
            }

            @Override public void emitUpdate(StringBuilder out, String indent, List<String> cells, String input)
            {
                out.append(indent).append(cells.get(0)).append(" = Double.doubleToRawLongBits(Double.longBitsToDouble(")
                        .append(cells.get(0)).append(") + Double.longBitsToDouble(").append(input).append("));\n");
                out.append(indent).append(cells.get(1)).append(" = ").append(cells.get(1)).append(" + 1L;\n");
            }

            @Override public void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other)
            {
                out.append(indent).append(cells.get(0)).append(" = Double.doubleToRawLongBits(Double.longBitsToDouble(")
                        .append(cells.get(0)).append(") + Double.longBitsToDouble(").append(other.get(0)).append("));\n");
                out.append(indent).append(cells.get(1)).append(" = ").append(cells.get(1)).append(" + ").append(other.get(1)).append(";\n");
            }

            @Override public String result(List<String> cells)
            {
                return "Double.doubleToRawLongBits(" + cells.get(1) + " == 0L ? 0.0 : Double.longBitsToDouble(" + cells.get(0) + ") / (double) " + cells.get(1) + ")";
            }

            @Override public String resultNull(List<String> cells)
            {
                return cells.get(1) + " == 0L";
            }
        });
        register("min", extreme("Math.min", "Long.MAX_VALUE"));
        register("max", extreme("Math.max", "Long.MIN_VALUE"));
        // COUNT(DISTINCT x) per group, fused into the grouping pass: the count cell only increments when the
        // (group, value) pair is new in the generated per-aggregate distinct set. The set machinery is emitted by
        // the compiler at the grouped call site (it needs the class body and a stable group identity), so the
        // registry entry only declares the cell shape and finalization.
        register("count_distinct", new AggregateCompiler()
        {
            @Override public void emitIdentity(StringBuilder out, String indent, List<String> cells)
            {
                out.append(indent).append(cells.get(0)).append(" = 0L;\n");
            }

            @Override public void emitUpdate(StringBuilder out, String indent, List<String> cells, String input)
            {
                throw new IllegalStateException("count_distinct is fused at the grouped call site");
            }

            @Override public void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other)
            {
                throw new IllegalStateException("count_distinct partial states cannot merge (the distinct sets are not mergeable cells)");
            }

            @Override public String result(List<String> cells)
            {
                return cells.get(0);
            }
        });
        // Lexicographic minimum of a dictionary-string column: state is the min entry's id (-1 = none yet);
        // candidates compare through the column's dictionary (UTF-8 lexicographic = unsigned byte order).
        // The input must be a string column id, so the dictionary-less emit forms reject generation.
        register("min_utf8", new AggregateCompiler()
        {
            @Override public Type outputType()
            {
                return Types.STRING;
            }

            @Override public void emitIdentity(StringBuilder out, String indent, List<String> cells)
            {
                out.append(indent).append(cells.get(0)).append(" = -1L;\n");
            }

            @Override public void emitUpdate(StringBuilder out, String indent, List<String> cells, String input)
            {
                throw new IllegalStateException("min_utf8 requires a dictionary-string input column");
            }

            @Override public void emitUpdate(StringBuilder out, String indent, List<String> cells, String input, String dictionary)
            {
                if (dictionary == null) {
                    throw new IllegalStateException("min_utf8 requires a dictionary-string input column");
                }
                String cell = cells.get(0);
                out.append(indent).append("{ long mu = ").append(input).append(";\n");
                out.append(indent).append("  if (").append(cell).append(" == -1L || java.util.Arrays.compareUnsigned(")
                        .append(dictionary).append("[(int) mu], ").append(dictionary).append("[(int) ").append(cell).append("]) < 0) { ")
                        .append(cell).append(" = mu; } }\n");
            }

            @Override public void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other)
            {
                throw new IllegalStateException("min_utf8 requires a dictionary-string input column");
            }

            @Override public void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other, String dictionary)
            {
                if (dictionary == null) {
                    throw new IllegalStateException("min_utf8 requires a dictionary-string input column");
                }
                String cell = cells.get(0);
                String candidate = other.get(0);
                out.append(indent).append("if (").append(candidate).append(" != -1L && (").append(cell).append(" == -1L || java.util.Arrays.compareUnsigned(")
                        .append(dictionary).append("[(int) ").append(candidate).append("], ").append(dictionary).append("[(int) ").append(cell).append("]) < 0)) { ")
                        .append(cell).append(" = ").append(candidate).append("; }\n");
            }

            @Override public String result(List<String> cells)
            {
                return cells.get(0);
            }

            @Override public String resultNull(List<String> cells)
            {
                return cells.get(0) + " == -1L";   // min over zero non-null inputs is NULL
            }
        });
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

            // Welford's online algorithm, matching the operator engine's StddevSamp byte-for-byte: cells are the
            // count (long), and the running mean and M2 (sum of squared deviations) stored as raw double bits. The
            // naive sum/sum-of-squares form would diverge from the operator in the last ULPs (catastrophic
            // cancellation), and the result is compared as an exact double. Update is order-dependent, so a query
            // using stddev must accumulate row-by-row in scan order (the hash-grouping path); the array-mode merge
            // below is a correct parallel combine for the (TPC-DS-unused) dense-single-key case.
            @Override public void emitIdentity(StringBuilder out, String indent, List<String> cells)
            {
                out.append(indent).append(cells.get(0)).append(" = 0L;\n");
                out.append(indent).append(cells.get(1)).append(" = 0L;\n");   // Double.doubleToRawLongBits(0.0) == 0L
                out.append(indent).append(cells.get(2)).append(" = 0L;\n");
            }

            @Override public void emitUpdate(StringBuilder out, String indent, List<String> cells, String input)
            {
                out.append(indent).append("{ long sdC = ").append(cells.get(0)).append(" + 1L;")
                        .append(" double sdMean = Double.longBitsToDouble(").append(cells.get(1)).append(");")
                        .append(" double sdDelta = (double) ").append(input).append(" - sdMean;")
                        .append(" sdMean += sdDelta / sdC;")
                        .append(" double sdDelta2 = (double) ").append(input).append(" - sdMean;")
                        .append(" double sdM2 = Double.longBitsToDouble(").append(cells.get(2)).append(") + sdDelta * sdDelta2;")
                        .append(" ").append(cells.get(0)).append(" = sdC;")
                        .append(" ").append(cells.get(1)).append(" = Double.doubleToRawLongBits(sdMean);")
                        .append(" ").append(cells.get(2)).append(" = Double.doubleToRawLongBits(sdM2); }\n");
            }

            @Override public void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other)
            {
                out.append(indent).append("{ long sdCa = ").append(cells.get(0)).append(", sdCb = ").append(other.get(0)).append(";")
                        .append(" if (sdCb != 0L) {")
                        .append(" if (sdCa == 0L) { ").append(cells.get(0)).append(" = sdCb; ").append(cells.get(1)).append(" = ").append(other.get(1)).append("; ").append(cells.get(2)).append(" = ").append(other.get(2)).append("; }")
                        .append(" else { double sdMa = Double.longBitsToDouble(").append(cells.get(1)).append("), sdMb = Double.longBitsToDouble(").append(other.get(1)).append(");")
                        .append(" double sdM2a = Double.longBitsToDouble(").append(cells.get(2)).append("), sdM2b = Double.longBitsToDouble(").append(other.get(2)).append(");")
                        .append(" long sdC = sdCa + sdCb; double sdDelta = sdMb - sdMa;")
                        .append(" double sdMean = sdMa + sdDelta * sdCb / sdC;")
                        .append(" double sdM2 = sdM2a + sdM2b + sdDelta * sdDelta * sdCa * sdCb / sdC;")
                        .append(" ").append(cells.get(0)).append(" = sdC; ").append(cells.get(1)).append(" = Double.doubleToRawLongBits(sdMean); ").append(cells.get(2)).append(" = Double.doubleToRawLongBits(sdM2); } } }\n");
            }

            @Override public String result(List<String> cells)
            {
                // sqrt(M2 / (count - 1)), matching the operator; NULL (handled by resultNull) when count < 2.
                return "Double.doubleToRawLongBits(" + cells.get(0) + " < 2L ? 0.0 : Math.sqrt(Double.longBitsToDouble("
                        + cells.get(2) + ") / (double) (" + cells.get(0) + " - 1L)))";
            }

            @Override public String resultNull(List<String> cells)
            {
                return cells.get(0) + " < 2L";   // sample standard deviation needs at least two values
            }
        });
    }

    /**
     * A two-cell F64 fold: [0] = bits of the running value (sum / min / max as doubles), [1] = non-null count
     * (zero means SQL NULL). {@code infix} selects {@code a + b}; otherwise {@code fold(a, b)}.
     */
    private static AggregateCompiler f64Fold(String fold, String identity, boolean infix)
    {
        return new AggregateCompiler()
        {
            @Override public int cells()
            {
                return 2;
            }

            @Override public Type outputType()
            {
                return Types.DOUBLE;
            }

            @Override public void emitIdentity(StringBuilder out, String indent, List<String> cells)
            {
                out.append(indent).append(cells.get(0)).append(" = Double.doubleToRawLongBits(").append(identity).append(");\n");
                out.append(indent).append(cells.get(1)).append(" = 0L;\n");
            }

            @Override public void emitUpdate(StringBuilder out, String indent, List<String> cells, String input)
            {
                String combined = infix
                        ? "Double.longBitsToDouble(" + cells.get(0) + ") " + fold + " Double.longBitsToDouble(" + input + ")"
                        : fold + "(Double.longBitsToDouble(" + cells.get(0) + "), Double.longBitsToDouble(" + input + "))";
                out.append(indent).append(cells.get(0)).append(" = Double.doubleToRawLongBits(").append(combined).append(");\n");
                out.append(indent).append(cells.get(1)).append(" = ").append(cells.get(1)).append(" + 1L;\n");
            }

            @Override public void emitMerge(StringBuilder out, String indent, List<String> cells, List<String> other)
            {
                String combined = infix
                        ? "Double.longBitsToDouble(" + cells.get(0) + ") " + fold + " Double.longBitsToDouble(" + other.get(0) + ")"
                        : fold + "(Double.longBitsToDouble(" + cells.get(0) + "), Double.longBitsToDouble(" + other.get(0) + "))";
                out.append(indent).append(cells.get(0)).append(" = Double.doubleToRawLongBits(").append(combined).append(");\n");
                out.append(indent).append(cells.get(1)).append(" = ").append(cells.get(1)).append(" + ").append(other.get(1)).append(";\n");
            }

            @Override public String result(List<String> cells)
            {
                return cells.get(0);
            }

            @Override public String resultNull(List<String> cells)
            {
                return cells.get(1) + " == 0L";
            }
        };
    }

    public void register(String name, AggregateCompiler compiler)
    {
        registry.put(name, compiler);
    }

    public AggregateCompiler get(String name)
    {
        AggregateCompiler compiler = registry.get(name);
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

            @Override public String resultNull(List<String> cells)
            {
                // The cell still at its identity means no non-null input was seen -- max/min over an empty (all-null)
                // group is SQL NULL, matching the operator engine. The identity is an out-of-range sentinel that real
                // values never take, so this never false-positives.
                return cells.get(0) + " == " + identity;
            }
        };
    }
}
