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

/**
 * Minimal pipeline IR for the compiling-engine prototype. A pipeline is a push shape modeled on what the
 * TPC-DS harness builds: a columnar scan, a conjunction of filters, and a (optionally grouped) aggregation.
 * Everything operates on {@code long} columns for now; this is enough to express the compute-bound core of
 * many TPC-DS queries (surrogate-key grouping, arithmetic measures, range/equality filters).
 */
public final class Plan
{
    private Plan() {}

    /** Scalar expression over input columns. */
    public sealed interface Expr
            permits Col, Lit, Bin
    {}

    /** Reference to input column {@code index}. */
    public record Col(int index)
            implements Expr
    {}

    /** Literal long. */
    public record Lit(long value)
            implements Expr
    {}

    /** Binary arithmetic: op in {@code + - *}. */
    public record Bin(String op, Expr left, Expr right)
            implements Expr
    {}

    /**
     * Boolean condition over input columns. {@code IN (a, b, ...)} desugars to {@link Or} of equality
     * {@link Predicate}s; {@code BETWEEN lo AND hi} to {@link And} of {@code >=} and {@code <=}.
     */
    public sealed interface Condition
            permits Predicate, And, Or, Not
    {}

    /** Comparison: {@code left op right}, op in {@code < <= > >= == !=}. */
    public record Predicate(String op, Expr left, Expr right)
            implements Condition
    {}

    /** Conjunction; an empty list is {@code true}. */
    public record And(List<Condition> conditions)
            implements Condition
    {
        public And
        {
            conditions = List.copyOf(conditions);
        }

        public And(Condition... conditions)
        {
            this(List.of(conditions));
        }
    }

    /** Disjunction; an empty list is {@code false}. */
    public record Or(List<Condition> conditions)
            implements Condition
    {
        public Or
        {
            conditions = List.copyOf(conditions);
        }

        public Or(Condition... conditions)
        {
            this(List.of(conditions));
        }
    }

    /** Negation. */
    public record Not(Condition condition)
            implements Condition
    {}

    /** Aggregate: fn in {@code sum count}; {@code input} is null for {@code count}. */
    public record Aggregate(String fn, Expr input)
    {}

    /**
     * Build (inner/dimension) side of an inner hash join. {@code columnCount} build columns, joined on
     * {@code keyColumns} (one or more, paired positionally with the pipeline's probe key columns). Build keys
     * are assumed unique on the composite (the TPC-DS fact-to-dimension-PK case). For a single key the compiler
     * emits an adaptive routine that measures the build key range once the build side is materialized and picks
     * direct array-mode lookup (Velox kArray-style: index by {@code key - min}, no hashing) when the domain is
     * dense and bounded, falling back to an open-addressing hash table otherwise — the structure choice is made
     * at runtime from the data, not declared in the plan.
     */
    public record Build(int columnCount, int[] keyColumns)
    {
        public Build
        {
            keyColumns = keyColumns.clone();
        }

        public Build(int columnCount, int keyColumn)
        {
            this(columnCount, new int[] {keyColumn});
        }
    }

    /**
     * One inner join of the probe stream against a {@link Build}, matching the probe's {@code probeKeyColumns}
     * positionally against {@code build.keyColumns}. Joins are applied in order; build {@code k}'s columns occupy
     * the combined column space immediately after the probe columns and all earlier builds.
     */
    public record Join(Build build, int[] probeKeyColumns)
    {
        public Join
        {
            probeKeyColumns = probeKeyColumns.clone();
        }

        public Join(Build build, int probeKeyColumn)
        {
            this(build, new int[] {probeKeyColumn});
        }
    }

    /**
     * A push pipeline. Scans {@code columnCount} probe columns; inner-joins each of {@code joins} in order
     * (combined columns address probe {@code [0,columnCount)} then each build's columns appended in turn);
     * keeps rows passing every filter; groups by {@code groupKeys} (empty = global aggregation); and computes
     * {@code aggregates}. For a single group key in a scan pipeline the compiler speculates array-mode grouping
     * and deopts to a hash table at runtime; the structure is chosen from the data, not declared in the plan.
     */
    public record Pipeline(int columnCount, List<Join> joins, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates)
    {
        public Pipeline
        {
            joins = List.copyOf(joins);
            filters = List.copyOf(filters);
            groupKeys = List.copyOf(groupKeys);
            aggregates = List.copyOf(aggregates);
        }

        /** Convenience for a single-key single-join pipeline. */
        public Pipeline(int columnCount, Build build, int probeKeyColumn, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates)
        {
            this(columnCount, List.of(new Join(build, probeKeyColumn)), filters, groupKeys, aggregates);
        }

        /** Convenience for a composite-key single-join pipeline. */
        public Pipeline(int columnCount, Build build, int[] probeKeyColumns, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates)
        {
            this(columnCount, List.of(new Join(build, probeKeyColumns)), filters, groupKeys, aggregates);
        }

        /** Convenience for a single-input pipeline (no join). */
        public Pipeline(int columnCount, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates)
        {
            this(columnCount, List.of(), filters, groupKeys, aggregates);
        }
    }
}
