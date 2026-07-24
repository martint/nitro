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
            permits Col, Lit, LitF64, LitStr, NullLit, Bin, Call, Case, Coalesce
    {}

    /** Reference to input column {@code index}. */
    public record Col(int index)
            implements Expr
    {}

    /** Literal long. */
    public record Lit(long value)
            implements Expr
    {}

    /** A double literal; the generated code carries it as raw bits in the long lanes. */
    public record LitF64(double value)
            implements Expr
    {}

    /**
     * A NULL long literal: the value is always SQL null. Used to tag one side of a discriminated union (e.g. the store
     * cumulative column on a web-channel row) where null -- not zero -- is required, because a downstream max() skips it
     * and a comparison against it is UNKNOWN. Emits a 0 placeholder value with its null mask always set.
     */
    public record NullLit()
            implements Expr
    {}

    /**
     * Constant string projection. Typed STRING; the pipeline emits the dictionary id 0 for every row, and the consumer
     * supplies the single-entry dictionary (e.g. via a literal {@code DictRef}) that maps id 0 to {@code value}.
     */
    public record LitStr(String value)
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
            permits Predicate, And, Or, Not, StringMatch, LikeMatch, SubstringMatch, StringColumnCompare, IsNull
    {}

    /**
     * SQL {@code column IS NULL} (or {@code IS NOT NULL} when {@code negated}) on any column. Reads the column's null
     * mask directly -- unlike a comparison predicate, which a null operand makes UNKNOWN (dropping the row), this is
     * the only way to KEEP null rows. The column must be declared nullable (a non-nullable column is never null).
     */
    public record IsNull(int column, boolean negated)
            implements Condition
    {
        public IsNull(int column)
        {
            this(column, false);
        }
    }

    /**
     * Value equality (or inequality, when {@code negated}) between two dictionary-encoded string columns,
     * {@code left} and {@code right}, which may carry independent dictionaries (so a dict-id comparison is wrong).
     * Compiled by remapping the left column's dictionary ids into the right column's id space once -- for each left
     * entry, the id of the right-dictionary entry with the same bytes (or a sentinel when absent) -- so each row is
     * the integer test {@code remap[leftId] == rightId} (negated for {@code <>}). A null on either side makes the
     * comparison null (the row is dropped), matching SQL.
     * <p>
     * When {@code substringStart >= 1} the comparison is over a fixed substring of each side (1-based start,
     * {@code substringLength} characters) rather than the whole value -- SQL {@code substring(left, s, n) <>
     * substring(right, s, n)}, the zip-prefix shape in Q19. The substring case canonicalizes each side's dictionary
     * into shared prefix classes (a remap into the other side's ids is insufficient, since two right entries can share
     * a prefix). {@code substringStart < 0} means no substring (whole-value compare).
     */
    public record StringColumnCompare(int left, int right, boolean negated, int substringStart, int substringLength)
            implements Condition
    {
        public StringColumnCompare(int left, int right, boolean negated)
        {
            this(left, right, negated, -1, -1);
        }

        /** Whether this compares a fixed substring of each side rather than the whole value. */
        public boolean hasSubstring()
        {
            return substringStart >= 1;
        }
    }

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
    /**
     * A join of the probe to {@code build} on {@code probeKeyColumns}. An INNER join (no flag) emits a row for EVERY
     * matching build row -- a one-to-many build fans the probe row out, since the compiler does not assume the build
     * key is unique. {@code outer} is a LEFT join: a probe row with no match is kept with the build's columns NULL.
     * {@code anti} (NOT EXISTS) keeps only probe rows with NO match; {@code semi} (EXISTS) keeps each matching probe
     * row exactly once regardless of how many build rows match. {@code cross} pairs every probe row with every build
     * row (no key). {@code outer}, {@code anti}, {@code semi} and {@code cross} are mutually exclusive.
     */
    public record Join(Build build, int[] probeKeyColumns, boolean outer, boolean anti, boolean cross, boolean semi)
    {
        public Join
        {
            probeKeyColumns = probeKeyColumns.clone();
        }

        public Join(Build build, int[] probeKeyColumns, boolean outer, boolean anti, boolean cross)
        {
            this(build, probeKeyColumns, outer, anti, cross, false);
        }

        public Join(Build build, int[] probeKeyColumns, boolean outer, boolean anti)
        {
            this(build, probeKeyColumns, outer, anti, false, false);
        }

        public Join(Build build, int[] probeKeyColumns, boolean outer)
        {
            this(build, probeKeyColumns, outer, false, false, false);
        }

        public Join(Build build, int[] probeKeyColumns)
        {
            this(build, probeKeyColumns, false, false, false, false);
        }

        public Join(Build build, int probeKeyColumn)
        {
            this(build, new int[] {probeKeyColumn});
        }

        public Join(Build build, int probeKeyColumn, boolean outer)
        {
            this(build, new int[] {probeKeyColumn}, outer);
        }

        /** A NOT EXISTS / anti-join keeping probe rows with no match in {@code build}. */
        public static Join anti(Build build, int... probeKeyColumns)
        {
            return new Join(build, probeKeyColumns, false, true, false, false);
        }

        /** An EXISTS / semi-join keeping each matching probe row exactly once (build contributes no columns). */
        public static Join semi(Build build, int... probeKeyColumns)
        {
            return new Join(build, probeKeyColumns, false, false, false, true);
        }

        /** A cross / nested-loop join: every probe row is paired with every {@code build} row (no key). */
        public static Join cross(Build build)
        {
            return new Join(build, new int[0], false, false, true, false);
        }
    }

    /**
     * One ORDER BY key and its direction. Ordinarily it is a result column index ({@code column}); but it may
     * instead be a {@code expr} computed over the result columns (group keys, then aggregates, then -- for grouping
     * sets -- the trailing grouping_id), compared as its declared result {@code type}. The expression form lets an
     * aggregating pipeline order by a value that is only produced by the final projection (e.g. {@code ORDER BY
     * avg(x)} where the projection computes the average) without first projecting -- the expression is evaluated
     * over the same pre-projection columns the projection sees, so it does not disturb the established
     * {@code project(order(having(...)))} emission order or any existing column-index key.
     * <p>
     * Exactly one of ({@code column}) or ({@code expr} with {@code type}) is meaningful: when {@code expr} is null
     * this is a plain column key on {@code column} (the historical behavior, unchanged); when {@code expr} is set,
     * {@code column} is ignored.
     */
    public record SortKey(int column, boolean descending, Expr expr, Type type)
    {
        /** A plain ORDER BY on a result column index. */
        public SortKey(int column, boolean descending)
        {
            this(column, descending, null, null);
        }

        /** An ORDER BY on an expression computed over the result columns, compared as {@code type}. */
        public static SortKey expression(Expr expr, Type type, boolean descending)
        {
            return new SortKey(-1, descending, expr, type);
        }
    }

    /** The ranking window function: {@code RANK()} (ties share a rank, the next rank skips) or {@code ROW_NUMBER()}. */
    public enum RankFunction
    {
        RANK,
        ROW_NUMBER
    }

    /**
     * A ranking window: {@code rank() / row_number() OVER (PARTITION BY partitionColumns ORDER BY orderBy)} with the
     * common {@code WHERE rank <= rankLimit} top-N-per-partition filter folded in. {@code partitionColumns} and the
     * {@code orderBy} key columns index the pipeline's input (scan) columns. A row whose partition column is NULL
     * shares a partition with no other row (partition equality is value equality, which is false for nulls), so it is
     * its own singleton partition at rank 1 -- matching the operator harness's {@code TopNRankingOperator}.
     * <p>
     * {@code rankLimit < 0} keeps every row (no top-N filter). {@link RankFunction#RANK} gives tied rows (equal on
     * every {@code orderBy} key) the same rank and skips the next; {@link RankFunction#ROW_NUMBER} numbers rows
     * 1, 2, 3, ... within the partition with no ties.
     */
    public record Window(int[] partitionColumns, List<SortKey> orderBy, RankFunction function, int rankLimit, WindowAggregate aggregate, List<WindowAggregate> runningAggregates)
    {
        public Window
        {
            partitionColumns = partitionColumns.clone();
            orderBy = List.copyOf(orderBy);
            runningAggregates = List.copyOf(runningAggregates);
        }

        public Window(int[] partitionColumns, List<SortKey> orderBy, RankFunction function, int rankLimit, WindowAggregate aggregate)
        {
            this(partitionColumns, orderBy, function, rankLimit, aggregate, List.of());
        }

        public Window(int[] partitionColumns, List<SortKey> orderBy, RankFunction function, int rankLimit)
        {
            this(partitionColumns, orderBy, function, rankLimit, null, List.of());
        }

        /**
         * A running (cumulative) aggregate window: each {@link WindowAggregate} {@code f(inputColumn) OVER (PARTITION BY
         * partitionColumns ORDER BY orderBy ROWS UNBOUNDED PRECEDING)} -- the aggregate over the partition rows from its
         * start up to and including the current row (in {@code orderBy} order), appended as a trailing column per
         * aggregate. Nulls are skipped; the running value is NULL until the first non-null. Used for cumulative
         * comparisons (Q51: running max of web vs store sales by item over date).
         */
        public static Window running(int[] partitionColumns, List<SortKey> orderBy, List<WindowAggregate> runningAggregates)
        {
            return new Window(partitionColumns, orderBy, null, -1, null, runningAggregates);
        }

        /**
         * A partition-aggregate window: {@code avg(inputColumn) OVER (PARTITION BY partitionColumns)} -- the aggregate
         * over each whole partition assigned to every row of that partition, appended as a trailing column. Unlike a
         * ranking window there is no ORDER BY or top-N. Used for the deviation-from-average shape (Q53/Q63/Q89).
         */
        public static Window partitionAverage(int[] partitionColumns, int inputColumn)
        {
            return new Window(partitionColumns, List.of(), null, -1, new WindowAggregate("avg", inputColumn));
        }

        /**
         * A partition-sum window: {@code sum(inputColumn) OVER (PARTITION BY partitionColumns)} -- the partition total
         * (over non-null values, NULL when the partition has none) assigned to every row, appended as a trailing
         * column. Used for the revenue-ratio shape (Q12/Q20/Q98): each item's revenue over its class total.
         */
        public static Window partitionSum(int[] partitionColumns, int inputColumn)
        {
            return new Window(partitionColumns, List.of(), null, -1, new WindowAggregate("sum", inputColumn));
        }
    }

    /** A whole-partition aggregate ({@code avg} or {@code sum}) over {@code inputColumn} for a partition-aggregate {@link Window}. */
    public record WindowAggregate(String function, int inputColumn) {}

    /**
     * Post-aggregation ORDER BY / LIMIT applied to the pipeline's result columns. {@code limit < 0} means no
     * limit. Sort keys index the result columns (group keys first, then aggregates).
     */
    public record Ordering(List<SortKey> keys, int limit, int offset)
    {
        public Ordering
        {
            keys = List.copyOf(keys);
        }

        /** ORDER BY with a LIMIT and no OFFSET ({@code limit < 0} = unlimited). */
        public Ordering(List<SortKey> keys, int limit)
        {
            this(keys, limit, 0);
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
    public record Pipeline(int columnCount, List<Join> joins, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates, Condition having, Ordering ordering, List<Expr> projections, List<int[]> groupingSets, Window window)
    {
        public Pipeline
        {
            joins = List.copyOf(joins);
            filters = List.copyOf(filters);
            groupKeys = List.copyOf(groupKeys);
            aggregates = List.copyOf(aggregates);
            projections = List.copyOf(projections);
            groupingSets = copyGroupingSets(groupingSets);
        }

        /** Convenience: no ranking window (an ordinary aggregating / projecting pipeline). */
        public Pipeline(int columnCount, List<Join> joins, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates, Condition having, Ordering ordering, List<Expr> projections, List<int[]> groupingSets)
        {
            this(columnCount, joins, filters, groupKeys, aggregates, having, ordering, projections, groupingSets, null);
        }

        /** Convenience: no grouping sets (single grouping over all {@code groupKeys}). */
        public Pipeline(int columnCount, List<Join> joins, List<Condition> filters, List<Expr> groupKeys, List<Aggregate> aggregates, Condition having, Ordering ordering, List<Expr> projections)
        {
            this(columnCount, joins, filters, groupKeys, aggregates, having, ordering, projections, List.of());
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
            return new Pipeline(columnCount, joins, filters, groupKeys, aggregates, having, ordering, projections, groupingSets, window);
        }

        /**
         * Same pipeline aggregated over {@code groupingSets} (GROUPING SETS / ROLLUP / CUBE) instead of one grouping
         * over all {@code groupKeys}. Each {@code int[]} is the sorted indices into {@code groupKeys} that are ACTIVE
         * in that set; the others are grouped to NULL. An empty list (the default) means a single ordinary grouping.
         * <p>
         * The engine matches the operator harness's single-pass EXPAND: one aggregation whose hash key is
         * {@code (setId, k0', k1', ...)} where {@code k_i'} is the key value if {@code i} is in the set, else a NULL
         * sentinel; the leading {@code setId} disambiguates two sets that null to the same key when the data has real
         * nulls. The result columns are the group-key columns (NULL where inactive in that group's set), then the
         * aggregate columns, then a trailing {@code grouping_id} LONG column: the {@code GROUPING()} bitmask with bit
         * {@code i} set for each group key {@code i} that is NOT in the set (i.e. aggregated away).
         */
        public Pipeline withGroupingSets(List<int[]> groupingSets)
        {
            return new Pipeline(columnCount, joins, filters, groupKeys, aggregates, having, ordering, projections, groupingSets, window);
        }

        /**
         * Same pipeline computing a ranking {@code window} (rank / row_number per partition, with an optional
         * {@code rank <= N} top-N filter) instead of an aggregation. The pipeline scans its input, keeps the rows
         * passing every {@code filter}, then -- per partition, ordered by the window's keys -- assigns each surviving
         * row its rank, keeps the rows within the rank limit, and emits all input columns ({@code [0, columnCount)})
         * followed by a trailing LONG rank column, globally ordered by {@code (partitionColumns, orderBy)}. This
         * mirrors the operator harness's {@code TopNRankingOperator}. A window pipeline has no group keys, aggregates,
         * grouping sets, joins, HAVING, or post projections (they are unsupported in this first cut); apply ORDER BY /
         * LIMIT and projections through a following pipeline if needed.
         */
        public Pipeline withWindow(Window window)
        {
            return new Pipeline(columnCount, joins, filters, groupKeys, aggregates, having, ordering, projections, groupingSets, window);
        }

        private static List<int[]> copyGroupingSets(List<int[]> groupingSets)
        {
            List<int[]> copy = new java.util.ArrayList<>(groupingSets.size());
            for (int[] set : groupingSets) {
                copy.add(set.clone());
            }
            return List.copyOf(copy);
        }
    }
}
