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

    /** Conjunct predicate: {@code left op right}, op in {@code < <= > >= == !=}. */
    public record Predicate(String op, Expr left, Expr right)
    {}

    /** Aggregate: fn in {@code sum count}; {@code input} is null for {@code count}. */
    public record Aggregate(String fn, Expr input)
    {}

    /**
     * Declared key domain for a dense (single) group key, inclusive {@code [min, max]}. When present, the
     * compiler emits array-mode grouping (Velox kArray-style: the aggregate arrays are indexed directly by
     * {@code key - min}, no hashing) instead of an open-addressing table. This represents knowledge a planner
     * already has for dictionary IDs and small surrogate-key dimensions — the dominant TPC-DS group-by shape.
     */
    public record Domain(long min, long max)
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
     * A push pipeline. Scans {@code columnCount} probe columns; if {@code build} is non-null, inner-joins it
     * on {@code probeKeyColumn = build.keyColumn} (combined columns address probe {@code [0,columnCount)} then
     * build {@code [columnCount, columnCount+build.columnCount)}); keeps rows passing every filter; groups by
     * {@code groupKeys} (empty = global aggregation); and computes {@code aggregates}. When {@code groupDomain}
     * is non-null the single group key is treated as dense and grouped in array mode.
     */
    public record Pipeline(int columnCount, Build build, int[] probeKeyColumns, List<Predicate> filters, List<Expr> groupKeys, Domain groupDomain, List<Aggregate> aggregates)
    {
        public Pipeline
        {
            probeKeyColumns = probeKeyColumns == null ? new int[0] : probeKeyColumns.clone();
            filters = List.copyOf(filters);
            groupKeys = List.copyOf(groupKeys);
            aggregates = List.copyOf(aggregates);
        }

        /** Convenience for a single-key join pipeline with hash-mode (or no) grouping. */
        public Pipeline(int columnCount, Build build, int probeKeyColumn, List<Predicate> filters, List<Expr> groupKeys, List<Aggregate> aggregates)
        {
            this(columnCount, build, new int[] {probeKeyColumn}, filters, groupKeys, null, aggregates);
        }

        /** Convenience for a single-input pipeline (no join). */
        public Pipeline(int columnCount, List<Predicate> filters, List<Expr> groupKeys, List<Aggregate> aggregates)
        {
            this(columnCount, null, new int[0], filters, groupKeys, null, aggregates);
        }
    }
}
