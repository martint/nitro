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
            permits Col, Lit, Bin, Call, Case, Coalesce
    {}

    /** Reference to input column {@code index}. */
    public record Col(int index)
            implements Expr
    {}

    /** Literal long. */
    public record Lit(long value)
            implements Expr
    {}

    /** Binary arithmetic: op in {@code + - * / %}. Sugar for a two-argument {@link Call} on the operator name. */
    public record Bin(String op, Expr left, Expr right)
            implements Expr
    {}

    /** Named scalar function applied to {@code arguments}, resolved through the scalar function library. */
    public record Call(String name, List<Expr> arguments)
            implements Expr
    {
        public Call
        {
            arguments = List.copyOf(arguments);
        }

        public Call(String name, Expr... arguments)
        {
            this(name, List.of(arguments));
        }
    }

    /**
     * {@code CASE WHEN ... THEN ... ELSE defaultValue END}: the first branch whose condition holds yields its
     * value; otherwise {@code defaultValue}.
     */
    public record Case(List<Branch> branches, Expr defaultValue)
            implements Expr
    {
        public Case
        {
            branches = List.copyOf(branches);
        }

        /** One {@code WHEN condition THEN value} arm of a {@link Case}. */
        public record Branch(Condition condition, Expr value) {}
    }

    /** {@code COALESCE(a, b, ...)}: the first non-null argument, or null if all are null. */
    public record Coalesce(List<Expr> arguments)
            implements Expr
    {
        public Coalesce
        {
            arguments = List.copyOf(arguments);
        }

        public Coalesce(Expr... arguments)
        {
            this(List.of(arguments));
        }
    }

    /**
     * Boolean condition over input columns. {@code IN (a, b, ...)} desugars to {@link Or} of equality
     * {@link Predicate}s; {@code BETWEEN lo AND hi} to {@link And} of {@code >=} and {@code <=}.
     */
    public sealed interface Condition
            permits Predicate, And, Or, Not, StringMatch, LikeMatch, SubstringMatch, StringColumnCompare
    {}

    /**
     * Value equality (or inequality, when {@code negated}) between two dictionary-encoded string columns,
     * {@code left} and {@code right}, which may carry independent dictionaries (so a dict-id comparison is wrong).
     * Compiled by remapping the left column's dictionary ids into the right column's id space once -- for each left
     * entry, the id of the right-dictionary entry with the same bytes (or a sentinel when absent) -- so each row is
     * the integer test {@code remap[leftId] == rightId} (negated for {@code <>}). A null on either side makes the
     * comparison null (the row is dropped), matching SQL.
     */
    public record StringColumnCompare(int left, int right, boolean negated)
            implements Condition
    {}

    /**
     * SQL {@code column LIKE pattern} (or its negation) on a dictionary-encoded string column. {@code %} matches
     * any run, {@code _} any single character. Compiled as predicate-over-dictionary -- the pattern is matched
     * once per dictionary entry into an id mask.
     */
    public record LikeMatch(int column, String pattern, boolean negated)
            implements Condition
    {}

    /**
     * {@code substring(column, start, length) IN (values...)} (or its negation) on a dictionary-encoded string
     * column; {@code start} is 1-based (SQL). Compiled as predicate-over-dictionary -- each dictionary entry's
     * substring is tested into an id mask.
     */
    public record SubstringMatch(int column, int start, int length, List<String> values, boolean negated)
            implements Condition
    {
        public SubstringMatch
        {
            values = List.copyOf(values);
        }
    }

    /**
     * Set membership on a dictionary-encoded string column: {@code column IN (values...)}, or its negation
     * ({@code NOT IN} / {@code <>} for a single value). Compiled as predicate-over-dictionary -- the match is
     * evaluated once per dictionary entry into an id mask, then each row is a mask lookup.
     */
    public record StringMatch(int column, List<String> values, boolean negated)
            implements Condition
    {
        public StringMatch
        {
            values = List.copyOf(values);
        }
    }

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
    /**
     * A join of the probe to {@code build} on {@code probeKeyColumns} = the build's key columns. When {@code outer},
     * it is a LEFT join: a probe row with no matching build row is kept, with the build's columns reading as NULL
     * (the decorrelated form of a scalar/aggregate subquery joined back to its outer query).
     */
    public record Join(Build build, int[] probeKeyColumns, boolean outer)
    {
        public Join
        {
            probeKeyColumns = probeKeyColumns.clone();
        }

        public Join(Build build, int[] probeKeyColumns)
        {
            this(build, probeKeyColumns, false);
        }

        public Join(Build build, int probeKeyColumn)
        {
            this(build, new int[] {probeKeyColumn});
        }

        public Join(Build build, int probeKeyColumn, boolean outer)
        {
            this(build, new int[] {probeKeyColumn}, outer);
        }
    }

    /** One ORDER BY key: a result column index and direction. */
    public record SortKey(int column, boolean descending) {}

    /**
     * Post-aggregation ORDER BY / LIMIT applied to the pipeline's result columns. {@code limit < 0} means no
     * limit. Sort keys index the result columns (group keys first, then aggregates).
     */
    public record Ordering(List<SortKey> keys, int limit)
    {
        public Ordering
        {
            keys = List.copyOf(keys);
        }
    }

    /**
     * A push pipeline. Scans {@code columnCount} probe columns; inner-joins each of {@code joins} in order
     * (combined columns address probe {@code [0,columnCount)} then each build's columns appended in turn);
     * keeps rows passing every filter; groups by {@code groupKeys} (empty = global aggregation); computes
     * {@code aggregates}; and optionally applies {@code ordering} (ORDER BY / LIMIT) to the result. For a single
     * group key in a scan pipeline the compiler speculates array-mode grouping and deopts to a hash table at
     * runtime; the structure is chosen from the data, not declared in the plan.
     */
    public record Pipeline(int columnCount, List<Join> joins, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates, Condition having, Ordering ordering, List<Expr> projections)
    {
        public Pipeline
        {
            joins = List.copyOf(joins);
            filters = List.copyOf(filters);
            groupKeys = List.copyOf(groupKeys);
            aggregates = List.copyOf(aggregates);
            projections = List.copyOf(projections);
        }

        /** Convenience: no final projection (output is the group-key columns then the aggregate columns). */
        public Pipeline(int columnCount, List<Join> joins, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates, Condition having, Ordering ordering)
        {
            this(columnCount, joins, filters, groupKeys, aggregates, having, ordering, List.of());
        }

        /** Convenience: no HAVING / ordering. */
        public Pipeline(int columnCount, List<Join> joins, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates)
        {
            this(columnCount, joins, filters, groupKeys, aggregates, null, null);
        }

        /** Convenience for a single-key single-join pipeline. */
        public Pipeline(int columnCount, Build build, int probeKeyColumn, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates)
        {
            this(columnCount, List.of(new Join(build, probeKeyColumn)), filters, groupKeys, aggregates, null, null);
        }

        /** Convenience for a composite-key single-join pipeline. */
        public Pipeline(int columnCount, Build build, int[] probeKeyColumns, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates)
        {
            this(columnCount, List.of(new Join(build, probeKeyColumns)), filters, groupKeys, aggregates, null, null);
        }

        /** Convenience for a single-input pipeline (no join). */
        public Pipeline(int columnCount, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates)
        {
            this(columnCount, List.of(), filters, groupKeys, aggregates, null, null);
        }

        /** Same pipeline with a HAVING filter (a condition over the result columns: group keys then aggregates). */
        public Pipeline withHaving(Condition having)
        {
            return new Pipeline(columnCount, joins, filters, groupKeys, aggregates, having, ordering, projections);
        }

        /** Same pipeline with an ORDER BY / LIMIT applied to its result. */
        public Pipeline withOrdering(Ordering ordering)
        {
            return new Pipeline(columnCount, joins, filters, groupKeys, aggregates, having, ordering, projections);
        }

        /**
         * Same pipeline with a final SELECT projection over the result columns (group keys then aggregates),
         * applied after HAVING and ORDER BY / LIMIT. Each expression is a {@link Col} (select / reorder) or a
         * computation over those columns; the output columns become exactly these projections.
         */
        public Pipeline withProjections(List<Expr> projections)
        {
            return new Pipeline(columnCount, joins, filters, groupKeys, aggregates, having, ordering, projections);
        }
    }
}
