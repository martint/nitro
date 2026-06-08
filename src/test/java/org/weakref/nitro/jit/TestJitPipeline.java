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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class TestJitPipeline
{
    @Test
    void compilesEqualityPredicates()
    {
        // SELECT count(*) WHERE a = 7 AND b <> 3 -- SQL '=' and '<>' must lower to Java '==' and '!='.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(
                        new Plan.Predicate("=", new Plan.Col(0), new Plan.Lit(7)),
                        new Plan.Predicate("<>", new Plan.Col(1), new Plan.Lit(3))),
                List.of(),
                List.of(new Plan.Aggregate("count", null)));

        int rows = 100_000;
        long[] a = new long[rows];
        long[] b = new long[rows];
        long expectedCount = 0;
        for (int i = 0; i < rows; i++) {
            a[i] = i % 10;
            b[i] = i % 5;
            if (a[i] == 7 && b[i] != 3) {
                expectedCount++;
            }
        }

        CompiledPipeline compiled = PipelineCompiler.compile(pipeline);
        CompiledPipeline.Result result = compiled.execute(new long[][][] {{a, b}}, new int[] {rows});

        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.columns()[0][0]).isEqualTo(expectedCount);
        assertThat(expectedCount).isGreaterThan(0);
    }

    @Test
    void compilesAndComputesGlobalAggregate()
    {
        // SELECT sum(a*b), count(*) WHERE a > 500
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(new Plan.Predicate(">", new Plan.Col(0), new Plan.Lit(500))),
                List.of(),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Bin("*", new Plan.Col(0), new Plan.Col(1))),
                        new Plan.Aggregate("count", null)));

        int rows = 100_000;
        long[] a = new long[rows];
        long[] b = new long[rows];
        long expectedSum = 0;
        long expectedCount = 0;
        for (int i = 0; i < rows; i++) {
            a[i] = i % 1000;
            b[i] = (i % 7) + 1;
            if (a[i] > 500) {
                expectedSum += a[i] * b[i];
                expectedCount++;
            }
        }

        CompiledPipeline compiled = PipelineCompiler.compile(pipeline);
        CompiledPipeline.Result result = compiled.execute(new long[][][] {{a, b}}, new int[] {rows});

        System.out.println("=== generated source ===\n" + PipelineCompiler.render(pipeline));
        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.columns()[0][0]).isEqualTo(expectedSum);
        assertThat(result.columns()[1][0]).isEqualTo(expectedCount);
    }

    @Test
    void compilesProjectionOnlyPipeline()
    {
        // SELECT v, k FROM t WHERE k > 50 ORDER BY v DESC LIMIT 5 -- no GROUP BY and no aggregates: one output row
        // per surviving input row (a materialize/SELECT shape), then ORDER BY / LIMIT.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Predicate(">", new Plan.Col(0), new Plan.Lit(50))),
                List.of(),
                List.of())
                .withProjections(List.of(new Plan.Col(1), new Plan.Col(0)))
                .withOrdering(new Plan.Ordering(List.of(new Plan.SortKey(1, true)), 5));

        int rows = 40_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        for (int i = 0; i < rows; i++) {
            k[i] = i % 100;
            v[i] = (i * 7) % 1000;
        }
        // Reference: rows with k > 50, top 5 by k descending (column 1 of the projection is k).
        List<long[]> kept = new java.util.ArrayList<>();
        for (int i = 0; i < rows; i++) {
            if (k[i] > 50) {
                kept.add(new long[] {v[i], k[i]});
            }
        }
        kept.sort((a, b) -> Long.compare(b[1], a[1]));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(5);
        for (int r = 0; r < 5; r++) {
            assertThat(result.columns()[1][r]).as("k at rank %d", r).isEqualTo(kept.get(r)[1]);
        }
        // Every output row came from a kept (k > 50) input row.
        for (int r = 0; r < 5; r++) {
            assertThat(result.columns()[1][r]).isGreaterThan(50);
        }
    }

    @Test
    void compilesProjectionOfStringColumn()
    {
        // SELECT sum(v), s GROUP BY s -- s a dictionary string key; the final projection reorders and keeps the
        // string column, which must carry its dictionary id and STRING type through (so the consumer can still
        // reconstruct the value), not be coerced to a number.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))))
                .withProjections(List.of(new Plan.Col(1), new Plan.Col(0)));   // sum, then the string key

        byte[][] dictionary = {
                "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "gamma".getBytes(java.nio.charset.StandardCharsets.UTF_8)};
        int rows = 60_000;
        int[] ids = new int[rows];
        long[] v = new long[rows];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            ids[i] = i % 3;
            v[i] = (i % 13) + 1;
            reference.merge((long) ids[i], v[i], Long::sum);
        }

        ColumnEncoding[][] encodings = {{ColumnEncoding.STRING, ColumnEncoding.FLAT}};
        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline, encodings)
                .execute(new Column[][] {{new Column.StringColumn(ids, dictionary), new Column.FlatColumn(v)}}, new int[] {rows});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        assertThat(result.types()[1]).isEqualTo(Types.STRING);          // the projected key stays STRING
        long[] sums = result.columns()[0];
        long[] keyIds = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).isEqualTo(reference.get(keyIds[g]));
            assertThat(new String(dictionary[(int) keyIds[g]], java.nio.charset.StandardCharsets.UTF_8)).isIn("alpha", "beta", "gamma");
        }
    }

    @Test
    void compilesFinalProjection()
    {
        // SELECT sum(v) AS total, k, sum(v) * 2 AS doubled FROM t GROUP BY k -- a final projection that reorders
        // the result columns and computes over them.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))))
                .withProjections(List.of(
                        new Plan.Col(1),                                              // sum
                        new Plan.Col(0),                                              // key
                        new Plan.Bin("*", new Plan.Col(1), new Plan.Lit(2))));        // sum * 2

        int rows = 80_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 20;
            v[i] = (i % 17) + 1;
            reference.merge(k[i], v[i], Long::sum);
        }

        CompiledPipeline compiled = PipelineCompiler.compile(pipeline);
        CompiledPipeline.Result result = compiled.execute(new long[][][] {{k, v}}, new int[] {rows});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] outTotal = result.columns()[0];
        long[] outKey = result.columns()[1];
        long[] outDoubled = result.columns()[2];
        for (int g = 0; g < result.rowCount(); g++) {
            long expectedTotal = reference.get(outKey[g]);
            assertThat(outTotal[g]).isEqualTo(expectedTotal);
            assertThat(outDoubled[g]).isEqualTo(expectedTotal * 2);
        }
    }

    @Test
    void ordersByComputedDoubleExpression()
    {
        // SELECT k, avg(v) AS a FROM t GROUP BY k ORDER BY avg(v) DESC LIMIT 5 -- ORDER BY a value the final
        // projection computes (a true double average), not a raw result column. The sort key carries the
        // divide_i64_to_f64 expression over the pre-projection columns (k=0, sum=1, count=2), compared as DOUBLE.
        int limit = 5;
        Plan.Expr average = new Plan.Call("divide_i64_to_f64", new Plan.Col(1), new Plan.Col(2));
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1)), new Plan.Aggregate("count", new Plan.Col(1))))
                .withOrdering(new Plan.Ordering(List.of(Plan.SortKey.expression(average, Types.DOUBLE, true)), limit))
                .withProjections(List.of(new Plan.Col(0), average));

        int rows = 60_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, long[]> reference = new HashMap<>();   // k -> {sum, count}
        for (int i = 0; i < rows; i++) {
            k[i] = i % 37;
            v[i] = (i % 53) + 1;
            reference.computeIfAbsent(k[i], ignored -> new long[2]);
            reference.get(k[i])[0] += v[i];
            reference.get(k[i])[1]++;
        }

        // Expected: the keys with the largest average, descending, truncated to the limit.
        List<Long> expectedKeys = reference.entrySet().stream()
                .sorted((x, y) -> Double.compare(
                        (double) y.getValue()[0] / y.getValue()[1],
                        (double) x.getValue()[0] / x.getValue()[1]))
                .limit(limit)
                .map(Map.Entry::getKey)
                .toList();

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});

        assertThat(result.rowCount()).isEqualTo(limit);
        assertThat(result.types()[1]).isEqualTo(Types.DOUBLE);
        long[] outKey = result.columns()[0];
        long[] outAverage = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(outKey[g]).as("key at rank %d", g).isEqualTo(expectedKeys.get(g));
            long[] sumCount = reference.get(outKey[g]);
            assertThat(Double.longBitsToDouble(outAverage[g]))
                    .as("average at rank %d", g)
                    .isCloseTo((double) sumCount[0] / sumCount[1], within(1e-9));
        }
        // The ordering must be non-increasing in the computed average.
        for (int g = 1; g < result.rowCount(); g++) {
            assertThat(Double.longBitsToDouble(outAverage[g - 1]))
                    .isGreaterThanOrEqualTo(Double.longBitsToDouble(outAverage[g]));
        }
    }

    @Test
    void compilesNullAwareCaseInAggregate()
    {
        // SELECT sum(CASE WHEN v > 5 THEN 1 ELSE 0) -- v nullable; a null v makes the WHEN unknown, so the row
        // falls to ELSE (0), matching SQL three-valued logic rather than reading the null slot as a value.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                1,
                List.of(),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Case(
                        List.of(new Plan.Case.Branch(new Plan.Predicate(">", new Plan.Col(0), new Plan.Lit(5)), new Plan.Lit(1))),
                        new Plan.Lit(0)))));

        int rows = 50_000;
        long[] v = new long[rows];
        boolean[] vNull = new boolean[rows];
        long expected = 0;
        for (int i = 0; i < rows; i++) {
            if (i % 4 == 0) {
                vNull[i] = true;        // null -> CASE else -> 0
            }
            else {
                v[i] = i % 11;          // 0..10
                if (v[i] > 5) {
                    expected++;
                }
            }
        }

        boolean[][] nullable = {{true}};
        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, null, nullable);
        CompiledPipeline.Result result = compiled.execute(
                new Column[][] {{new Column.FlatColumn(v, vNull)}}, new int[] {rows});

        assertThat(result.columns()[0][0]).isEqualTo(expected);
        assertThat(expected).isGreaterThan(0);
    }

    @Test
    void compilesRoundingDivide()
    {
        // SELECT k, round(sum(v) / count(v)) GROUP BY k -- divide_round_i64 in a projection, the integer-scaled
        // decimal average (sum/count, half-up) used by the discount-threshold subqueries. Must match the built-in's
        // sign-aware rounding to the unit.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1)), new Plan.Aggregate("count", new Plan.Col(1))))
                .withProjections(List.of(new Plan.Col(0),
                        new Plan.Call("divide_round_i64", new Plan.Col(1), new Plan.Col(2))));

        int rows = 90_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, long[]> reference = new HashMap<>();   // k -> {sum, count}
        for (int i = 0; i < rows; i++) {
            k[i] = i % 23;
            v[i] = (i % 200) - 90;          // mix of negative and positive
            reference.computeIfAbsent(k[i], ignored -> new long[2]);
            reference.get(k[i])[0] += v[i];
            reference.get(k[i])[1]++;
        }

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] averages = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] sumCount = reference.get(keys[g]);
            assertThat(averages[g]).as("rounded average for k %d", keys[g])
                    .isEqualTo(org.weakref.nitro.jit.DecimalMath.roundDivide(sumCount[0], sumCount[1]));
        }
    }

    @Test
    void compilesDecimalArithmetic()
    {
        // SELECT sum(p * q), sum(divide_scale_round_i64(p, q, 100)) -- decimal product and rescaling divide,
        // matching the interpreted built-ins' semantics (scaled longs).
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Call("multiply_i64", List.of(new Plan.Col(0), new Plan.Col(1)))),
                        new Plan.Aggregate("sum", new Plan.Call("divide_scale_round_i64",
                                List.of(new Plan.Col(0), new Plan.Col(1), new Plan.Lit(100))))));

        int rows = 10_000;
        long[] p = new long[rows];
        long[] q = new long[rows];
        long expectedProduct = 0;
        long expectedScaled = 0;
        for (int i = 0; i < rows; i++) {
            p[i] = (i % 997) + 1;
            q[i] = (i % 13) + 1;
            expectedProduct += p[i] * q[i];
            expectedScaled += org.weakref.nitro.jit.DecimalMath.roundScaledDivide(p[i], q[i], 100);
        }

        CompiledPipeline compiled = PipelineCompiler.compile(pipeline);
        CompiledPipeline.Result result = compiled.execute(new long[][][] {{p, q}}, new int[] {rows});

        assertThat(result.columns()[0][0]).isEqualTo(expectedProduct);
        assertThat(result.columns()[1][0]).isEqualTo(expectedScaled);
    }

    @Test
    void compilesJoinWithNullProbeKey()
    {
        // SELECT sum(v) FROM fact JOIN dim ON fact.k = dim.k -- some fact.k are null. A null key must match
        // nothing, even though the dimension has a row with key 0 (the raw value a null reads as).
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,                              // probe: 0=k (nullable), 1=v
                new Plan.Build(1, 0),           // build: 0=k
                0,
                List.of(),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        int dimRows = 50;
        long[] dimKey = new long[dimRows];
        for (int d = 0; d < dimRows; d++) {
            dimKey[d] = d;                       // includes key 0
        }
        int factRows = 100_000;
        long[] factKey = new long[factRows];
        boolean[] factKeyNull = new boolean[factRows];
        long[] v = new long[factRows];
        long expected = 0;
        for (int i = 0; i < factRows; i++) {
            v[i] = (i % 20) + 1;
            if (i % 5 == 0) {
                factKeyNull[i] = true;           // null FK: must not join, even to dim key 0
            }
            else {
                factKey[i] = i % dimRows;        // all match a dimension row
                expected += v[i];
            }
        }

        boolean[][] nullable = {{true, false}};
        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, null, nullable);
        Column[][] inputs = {
                {new Column.FlatColumn(factKey, factKeyNull), new Column.FlatColumn(v)},
                {new Column.FlatColumn(dimKey)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {factRows, dimRows});

        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.columns()[0][0]).isEqualTo(expected);
    }

    @Test
    void compilesMultipleStringFiltersOnOneColumn()
    {
        // SELECT count(*) WHERE s <> 'drop' AND s <> 'skip' -- two predicate-over-dictionary masks on the SAME
        // column (the shape of an OR of per-column branches); masks must be keyed per predicate, not per column.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                1,
                List.of(
                        new Plan.StringMatch(0, List.of("drop"), true),
                        new Plan.StringMatch(0, List.of("skip"), true)),
                List.of(),
                List.of(new Plan.Aggregate("count", null)));

        String[] vocabulary = {"drop", "skip", "keep", "take", "hold"};
        byte[][] dictionary = new byte[vocabulary.length][];
        for (int d = 0; d < vocabulary.length; d++) {
            dictionary[d] = vocabulary[d].getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        int rows = 60_000;
        int[] ids = new int[rows];
        long expected = 0;
        for (int i = 0; i < rows; i++) {
            ids[i] = i % vocabulary.length;
            String s = vocabulary[ids[i]];
            if (!s.equals("drop") && !s.equals("skip")) {
                expected++;
            }
        }

        ColumnEncoding[][] encodings = {{ColumnEncoding.STRING}};
        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        CompiledPipeline.Result result = compiled.execute(
                new Column[][] {{new Column.StringColumn(ids, dictionary)}}, new int[] {rows});

        assertThat(result.columns()[0][0]).isEqualTo(expected);
        assertThat(expected).isGreaterThan(0);
    }

    @Test
    void compilesLikeAndSubstringFilters()
    {
        // SELECT count(*) WHERE s LIKE 'AB%' AND substring(t,1,3) <> 'XYZ'  -- predicate-over-dictionary on
        // two string columns: a LIKE pattern and a substring-IN test.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(
                        new Plan.LikeMatch(0, "AB%", false),
                        new Plan.SubstringMatch(1, 1, 3, List.of("XYZ"), true)),
                List.of(),
                List.of(new Plan.Aggregate("count", null)));

        String[] sVocab = {"ABC", "ABX", "AXX", "ABBB", "ZZAB", "AB"};
        String[] tVocab = {"XYZ123", "XYAB", "XYZ", "PQR", "XY"};
        byte[][] sDict = new byte[sVocab.length][];
        for (int d = 0; d < sVocab.length; d++) {
            sDict[d] = sVocab[d].getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        byte[][] tDict = new byte[tVocab.length][];
        for (int d = 0; d < tVocab.length; d++) {
            tDict[d] = tVocab[d].getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        int rows = 70_000;
        int[] sIds = new int[rows];
        int[] tIds = new int[rows];
        long expected = 0;
        for (int i = 0; i < rows; i++) {
            sIds[i] = i % sVocab.length;
            tIds[i] = i % tVocab.length;
            String s = sVocab[sIds[i]];
            String t = tVocab[tIds[i]];
            String prefix = t.substring(0, Math.min(3, t.length()));
            if (s.startsWith("AB") && !prefix.equals("XYZ")) {   // LIKE 'AB%' AND substring(t,1,3) <> 'XYZ'
                expected++;
            }
        }

        ColumnEncoding[][] encodings = {{ColumnEncoding.STRING, ColumnEncoding.STRING}};
        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        CompiledPipeline.Result result = compiled.execute(
                new Column[][] {{new Column.StringColumn(sIds, sDict), new Column.StringColumn(tIds, tDict)}}, new int[] {rows});

        assertThat(result.columns()[0][0]).isEqualTo(expected);
        assertThat(expected).isGreaterThan(0);
    }

    @Test
    void compilesJoinWithStringBuildColumnFilter()
    {
        // SELECT sum(v) FROM fact JOIN dim ON fact.k = dim.k WHERE dim.s IN ('keep', 'also')
        // -- a string filter (predicate-over-dictionary) on a dimension (build) column.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,                                  // probe: 0=k, 1=v
                new Plan.Build(2, 0),               // build: 0=k, 1=s
                0,
                List.of(new Plan.StringMatch(3, List.of("keep", "also"), false)),   // dim.s combined index 3
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        byte[][] dictionary = {
                "drop".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "keep".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "also".getBytes(java.nio.charset.StandardCharsets.UTF_8)};
        int dimRows = 60;
        long[] dimKey = new long[dimRows];
        int[] dimString = new int[dimRows];
        for (int d = 0; d < dimRows; d++) {
            dimKey[d] = d;
            dimString[d] = d % 3;                    // 0=drop, 1=keep, 2=also
        }
        int factRows = 100_000;
        long[] factKey = new long[factRows];
        long[] v = new long[factRows];
        long expected = 0;
        for (int i = 0; i < factRows; i++) {
            factKey[i] = i % dimRows;
            v[i] = (i % 25) + 1;
            if (dimString[(int) factKey[i]] != 0) {   // s IN ('keep','also')
                expected += v[i];
            }
        }

        ColumnEncoding[][] encodings = {
                {ColumnEncoding.FLAT, ColumnEncoding.FLAT},
                {ColumnEncoding.FLAT, ColumnEncoding.STRING}};
        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        Column[][] inputs = {
                {new Column.FlatColumn(factKey), new Column.FlatColumn(v)},
                {new Column.FlatColumn(dimKey), new Column.StringColumn(dimString, dictionary)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {factRows, dimRows});

        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.columns()[0][0]).isEqualTo(expected);
    }

    @Test
    void compilesJoinWithStringBuildColumnGroupKey()
    {
        // SELECT s, sum(v) FROM fact JOIN dim ON fact.k = dim.k GROUP BY s -- s is a STRING column on the build
        // (dimension) side used as the group key; the join path must honor build-column encodings.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,                                  // probe: 0=k, 1=v
                new Plan.Build(2, 0),               // build: 0=k (key), 1=s
                0,                                  // probe key column
                List.of(),
                List.of(new Plan.Col(3)),           // group by build column s (combined index 3)
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        int dictSize = 6;
        byte[][] dictionary = new byte[dictSize][];
        for (int d = 0; d < dictSize; d++) {
            dictionary[d] = ("dept-" + d).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        int dimRows = 200;
        long[] dimKey = new long[dimRows];
        int[] dimCategory = new int[dimRows];
        for (int d = 0; d < dimRows; d++) {
            dimKey[d] = d;
            dimCategory[d] = d % dictSize;
        }
        int factRows = 100_000;
        long[] factKey = new long[factRows];
        long[] v = new long[factRows];
        Map<Integer, Long> reference = new HashMap<>();   // category id -> sum(v)
        for (int i = 0; i < factRows; i++) {
            factKey[i] = i % dimRows;
            v[i] = (i % 40) + 1;
            reference.merge(dimCategory[(int) factKey[i]], v[i], Long::sum);
        }

        ColumnEncoding[][] encodings = {
                {ColumnEncoding.FLAT, ColumnEncoding.FLAT},
                {ColumnEncoding.FLAT, ColumnEncoding.STRING}};
        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        Column[][] inputs = {
                {new Column.FlatColumn(factKey), new Column.FlatColumn(v)},
                {new Column.FlatColumn(dimKey), new Column.StringColumn(dimCategory, dictionary)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {factRows, dimRows});

        assertThat(result.types()[0]).isEqualTo(Types.STRING);
        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keyIds = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum for category %d", keyIds[g]).isEqualTo(reference.get((int) keyIds[g]));
        }
    }

    @Test
    void streamsGroupedAggregateInBatches()
    {
        // SELECT k, sum(v) WHERE v > 0 GROUP BY k -- computed once eagerly and once by streaming the input in
        // batches through a StreamingPipeline; the two must agree.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(new Plan.Predicate(">", new Plan.Col(1), new Plan.Lit(0))),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        int rows = 200_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        for (int i = 0; i < rows; i++) {
            k[i] = i % 5000;
            v[i] = (i % 11) - 2;
        }

        Map<Long, Long> eager = groupSums(PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows}));

        int batchSize = 4096;
        StreamingPipeline streaming = PipelineCompiler.compileStreaming(pipeline, null, null);
        CompiledPipeline.Result streamed = streaming.execute(flatBatches(batchSize, rows, k, v), new Column[0][], new int[0]);
        Map<Long, Long> result = groupSums(streamed);

        assertThat(result).isEqualTo(eager);
        assertThat(result).isNotEmpty();
        assertThat(rows / batchSize).isGreaterThan(1);   // genuinely multiple batches
    }

    @Test
    void streamsAntiJoinInBatches()
    {
        // SELECT sum(p.v) WHERE p.k NOT IN (SELECT k FROM b) -- streamed probe + materialized build; the streaming
        // late-materialization records non-matching probe rows (no build row) and matches the eager result.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(Plan.Join.anti(new Plan.Build(1, 0), 0)),
                List.of(),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        int rows = 200_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        for (int i = 0; i < rows; i++) {
            k[i] = i % 5000;
            v[i] = i % 11;
        }
        long[] buildKey = new long[2500];
        for (int i = 0; i < buildKey.length; i++) {
            buildKey[i] = i;   // keys 0..2499 present; 2500..4999 absent -> anti keeps the absent half
        }

        long eager = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{k, v}, {buildKey}}, new int[] {rows, buildKey.length}).columns()[0][0];
        CompiledPipeline.Result streamed = PipelineCompiler.compileStreaming(pipeline, null, null).execute(
                flatBatches(4096, rows, k, v), new Column[][] {{new Column.FlatColumn(buildKey)}}, new int[] {buildKey.length});

        assertThat(streamed.columns()[0][0]).isEqualTo(eager);
        assertThat(eager).isGreaterThan(0);
    }

    @Test
    void streamsCrossJoinInBatches()
    {
        // SELECT sum(p.v) FROM p, b WHERE p.v > b.t -- streamed probe crossed with a 2-row build, so a batch yields
        // up to 2x its rows in survivors, exercising the streaming selection-array growth. Must match the eager run.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                1,
                List.of(Plan.Join.cross(new Plan.Build(1, new int[0]))),
                List.of(new Plan.Predicate(">", new Plan.Col(0), new Plan.Col(1))),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Col(0))));

        int rows = 50_000;
        long[] v = new long[rows];
        for (int i = 0; i < rows; i++) {
            v[i] = i % 100;
        }
        long[] thresholds = {10, 50};

        long eager = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{v}, {thresholds}}, new int[] {rows, thresholds.length}).columns()[0][0];
        CompiledPipeline.Result streamed = PipelineCompiler.compileStreaming(pipeline, null, null).execute(
                flatBatches(4096, rows, v), new Column[][] {{new Column.FlatColumn(thresholds)}}, new int[] {thresholds.length});

        assertThat(streamed.columns()[0][0]).isEqualTo(eager);
        assertThat(eager).isGreaterThan(0);
    }

    @Test
    void streamsFanOutOverNonUniqueBuildInBatches()
    {
        // SELECT sum(b.v) FROM p JOIN b ON p.k = b.k -- streamed probe joined to a build whose key is NOT unique (each
        // key has 3 rows), so a batch yields up to 3x its rows in survivors. Exercises the streaming late-materialize
        // fan-out and selection-array growth; must match the eager run (which the eager fan-out tests pin as correct).
        Plan.Pipeline pipeline = new Plan.Pipeline(
                1,                                                    // probe [p.k]
                List.of(new Plan.Join(new Plan.Build(2, 0), 0)),      // build [b.k, b.v]
                List.of(),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))));

        int rows = 50_000;
        long[] k = new long[rows];
        for (int i = 0; i < rows; i++) {
            k[i] = i % 1000;
        }
        int distinct = 1000;
        int copies = 3;
        long[] buildKey = new long[distinct * copies];
        long[] buildVal = new long[distinct * copies];
        for (int key = 0; key < distinct; key++) {
            for (int d = 0; d < copies; d++) {
                int idx = key * copies + d;
                buildKey[idx] = key;
                buildVal[idx] = d + 1;   // each matching probe row contributes 1+2+3 = 6
            }
        }

        long eager = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{k}, {buildKey, buildVal}}, new int[] {rows, buildKey.length}).columns()[0][0];
        CompiledPipeline.Result streamed = PipelineCompiler.compileStreaming(pipeline, null, null).execute(
                flatBatches(4096, rows, k), new Column[][] {{new Column.FlatColumn(buildKey), new Column.FlatColumn(buildVal)}}, new int[] {buildKey.length});

        assertThat(streamed.columns()[0][0]).isEqualTo(eager);
        assertThat(eager).isEqualTo((long) rows * 6);
    }

    @Test
    void streamsAcrossGrowingBatchesReusesScratch()
    {
        // The streaming late-materialization reuses its per-batch survivor index (selection) and matched build-row
        // index (bsel) across batches, growing them on demand. Feed batches of strictly increasing size so the scratch
        // must grow mid-stream while a column is read at the new larger length; the result must still equal the eager
        // run (no stale/undersized buffer). SELECT k, sum(v) FROM p JOIN b ON p.k = b.k GROUP BY k -- a join + group so
        // both selection and bsel are exercised, with a probe payload column (v) materialized for survivors.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(new Plan.Join(new Plan.Build(1, 0), 0)),
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        int rows = 60_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        for (int i = 0; i < rows; i++) {
            k[i] = i % 4000;
            v[i] = i % 13;
        }
        long[] buildKey = new long[4000];
        for (int i = 0; i < buildKey.length; i++) {
            buildKey[i] = i;   // every probe key matches
        }

        CompiledPipeline.Result eager = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{k, v}, {buildKey}}, new int[] {rows, buildKey.length});
        CompiledPipeline.Result streamed = PipelineCompiler.compileStreaming(pipeline, null, null)
                .execute(growingBatches(rows, k, v), new Column[][] {{new Column.FlatColumn(buildKey)}}, new int[] {buildKey.length});

        assertThat(streamed.rowCount()).isEqualTo(eager.rowCount());
        assertThat(groupSums(streamed)).isEqualTo(groupSums(eager));
    }

    /** A streaming source whose batches strictly grow (512, 1024, 1536, ...), so a reused scratch buffer must grow mid-stream. */
    private static StreamingPipeline.Source growingBatches(int rows, long[]... columns)
    {
        return new StreamingPipeline.Source()
        {
            private int start;
            private int batchRows;
            private int step;

            @Override
            public boolean advance()
            {
                start += batchRows;
                if (start >= rows) {
                    return false;
                }
                step++;
                batchRows = Math.min(512 * step, rows - start);
                return true;
            }

            @Override
            public int rows()
            {
                return batchRows;
            }

            @Override
            public Column[] columns()
            {
                Column[] batch = new Column[columns.length];
                for (int c = 0; c < columns.length; c++) {
                    batch[c] = new Column.FlatColumn(java.util.Arrays.copyOfRange(columns[c], start, start + batchRows));
                }
                return batch;
            }
        };
    }

    private static Map<Long, Long> groupSums(CompiledPipeline.Result result)
    {
        Map<Long, Long> map = new HashMap<>();
        for (int g = 0; g < result.rowCount(); g++) {
            map.put(result.columns()[0][g], result.columns()[1][g]);
        }
        return map;
    }

    /** A streaming source that slices flat {@code long[]} columns into fixed-size batches. */
    private static StreamingPipeline.Source flatBatches(int batchSize, int rows, long[]... columns)
    {
        return new StreamingPipeline.Source()
        {
            private int start = -1;
            private int batchRows;

            @Override
            public boolean advance()
            {
                int next = start < 0 ? 0 : start + batchRows;
                if (next >= rows) {
                    return false;
                }
                start = next;
                batchRows = Math.min(batchSize, rows - start);
                return true;
            }

            @Override
            public int rows()
            {
                return batchRows;
            }

            @Override
            public Column[] columns()
            {
                Column[] batch = new Column[columns.length];
                for (int c = 0; c < columns.length; c++) {
                    batch[c] = new Column.FlatColumn(java.util.Arrays.copyOfRange(columns[c], start, start + batchRows));
                }
                return batch;
            }
        };
    }

    @Test
    void compilesAndComputesGroupedAggregate()
    {
        // SELECT k, sum(v), count(*) WHERE v > 0 GROUP BY k
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(new Plan.Predicate(">", new Plan.Col(1), new Plan.Lit(0))),
                List.of(new Plan.Col(0)),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Col(1)),
                        new Plan.Aggregate("count", null)));

        int rows = 200_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, long[]> reference = new HashMap<>();   // key -> {sum, count}
        for (int i = 0; i < rows; i++) {
            k[i] = i % 5000;        // 5000 distinct groups
            v[i] = (i % 11) - 2;    // includes negatives -> filtered
            if (v[i] > 0) {
                long[] acc = reference.computeIfAbsent(k[i], ignored -> new long[2]);
                acc[0] += v[i];
                acc[1]++;
            }
        }

        System.out.println("=== generated grouped source ===\n" + PipelineCompiler.render(pipeline));

        // Dense keys in [0, 5000): the sample speculates array mode and the bet holds (no deopt).
        verifyGroupedSumCount(pipeline, k, v, reference);

        // Deopt: the first SAMPLE_SIZE rows look narrow ([0, 50)), so the array is sized small, then later
        // rows reach up to 4999 and force a mid-stream migration into the hash table. Results must still match.
        long[] kDeopt = new long[rows];
        long[] vDeopt = new long[rows];
        Map<Long, long[]> deoptReference = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            kDeopt[i] = i < 4096 ? i % 50 : i % 5000;
            vDeopt[i] = (i % 11) - 2;
            if (vDeopt[i] > 0) {
                long[] acc = deoptReference.computeIfAbsent(kDeopt[i], ignored -> new long[2]);
                acc[0] += vDeopt[i];
                acc[1]++;
            }
        }
        verifyGroupedSumCount(pipeline, kDeopt, vDeopt, deoptReference);

        // Never speculate: the key domain exceeds the array cap, so it runs on the hash table from row 0.
        long[] kSparse = new long[rows];
        long[] vSparse = new long[rows];
        Map<Long, long[]> sparseReference = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            kSparse[i] = (i % 1000) * 1_000_000L;   // max ~1e9 > array cap
            vSparse[i] = (i % 11) - 2;
            if (vSparse[i] > 0) {
                long[] acc = sparseReference.computeIfAbsent(kSparse[i], ignored -> new long[2]);
                acc[0] += vSparse[i];
                acc[1]++;
            }
        }
        verifyGroupedSumCount(pipeline, kSparse, vSparse, sparseReference);
    }

    private static void verifyGroupedSumCount(Plan.Pipeline pipeline, long[] k, long[] v, Map<Long, long[]> reference)
    {
        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {k.length});
        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        long[] counts = result.columns()[2];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = reference.get(keys[g]);
            assertThat(expected).as("group %d", keys[g]).isNotNull();
            assertThat(sums[g]).as("sum for group %d", keys[g]).isEqualTo(expected[0]);
            assertThat(counts[g]).as("count for group %d", keys[g]).isEqualTo(expected[1]);
        }
    }

    @Test
    void compilesAndComputesMultiKeyGroupedAggregate()
    {
        // SELECT k0, k1, sum(v), count(*) GROUP BY k0, k1
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(),
                List.of(new Plan.Col(0), new Plan.Col(1)),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Col(2)),
                        new Plan.Aggregate("count", null)));

        int rows = 200_000;
        long[] k0 = new long[rows];
        long[] k1 = new long[rows];
        long[] v = new long[rows];
        Map<List<Long>, long[]> reference = new HashMap<>();   // (k0, k1) -> {sum, count}
        for (int i = 0; i < rows; i++) {
            k0[i] = i % 50;
            k1[i] = i % 37;        // 50 * 37 = 1850 distinct composite groups
            v[i] = i % 13;
            long[] acc = reference.computeIfAbsent(List.of(k0[i], k1[i]), ignored -> new long[2]);
            acc[0] += v[i];
            acc[1]++;
        }

        System.out.println("=== generated multi-key grouped source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k0, k1, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(reference.size());

        long[] outK0 = result.columns()[0];
        long[] outK1 = result.columns()[1];
        long[] sums = result.columns()[2];
        long[] counts = result.columns()[3];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = reference.get(List.of(outK0[g], outK1[g]));
            assertThat(expected).as("group (%d, %d)", outK0[g], outK1[g]).isNotNull();
            assertThat(sums[g]).as("sum for group (%d, %d)", outK0[g], outK1[g]).isEqualTo(expected[0]);
            assertThat(counts[g]).as("count for group (%d, %d)", outK0[g], outK1[g]).isEqualTo(expected[1]);
        }
    }

    @Test
    void compilesAndComputesJoinGroupedAggregate()
    {
        // fact(fk, measure) JOIN dim(dkey, dattr) ON fk=dkey GROUP BY dattr, sum(measure)
        // combined columns: probe [0=fk, 1=measure], build [2=dkey, 3=dattr]
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                new Plan.Build(2, 0),
                0,
                List.of(),
                List.of(new Plan.Col(3)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        int dim = 1000;
        int groups = 20;
        long[] dkey = new long[dim];
        long[] dattr = new long[dim];
        for (int d = 0; d < dim; d++) {
            dkey[d] = d;
            dattr[d] = d % groups;
        }
        int fact = 500_000;
        long[] fk = new long[fact];
        long[] measure = new long[fact];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < fact; i++) {
            fk[i] = i % dim;
            measure[i] = (i % 50) + 1;
            long attr = dattr[(int) fk[i]];
            reference.merge(attr, measure[i], Long::sum);
        }

        System.out.println("=== generated join source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{fk, measure}, {dkey, dattr}}, new int[] {fact, dim});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum for attr %d", keys[g]).isEqualTo(reference.get(keys[g]));
        }

        // Same adaptive plan, but sparse build keys (range >> rows) force the runtime to fall back to the
        // hash table instead of the array. Results must be identical.
        long stride = 1_000_000;
        long[] sparseKey = new long[dim];
        for (int d = 0; d < dim; d++) {
            sparseKey[d] = d * stride;
        }
        long[] sparseFk = new long[fact];
        Map<Long, Long> sparseReference = new HashMap<>();
        for (int i = 0; i < fact; i++) {
            int d = i % dim;
            sparseFk[i] = sparseKey[d];
            sparseReference.merge(dattr[d], measure[i], Long::sum);
        }
        CompiledPipeline.Result sparseResult = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{sparseFk, measure}, {sparseKey, dattr}}, new int[] {fact, dim});
        assertThat(sparseResult.rowCount()).isEqualTo(sparseReference.size());
        long[] sKeys = sparseResult.columns()[0];
        long[] sSums = sparseResult.columns()[1];
        for (int g = 0; g < sparseResult.rowCount(); g++) {
            assertThat(sSums[g]).as("sparse sum for attr %d", sKeys[g]).isEqualTo(sparseReference.get(sKeys[g]));
        }
    }

    @Test
    void compilesOrderByDoubleColumn()
    {
        // SELECT k, avg(v) GROUP BY k HAVING avg(v) > 0 ORDER BY avg(v) ASC LIMIT 5 -- sort/filter on a DOUBLE
        // column must decode the bits, not compare raw longs (v has negatives so averages are signed).
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("avg", new Plan.Col(1))))
                .withHaving(new Plan.Predicate(">", new Plan.Col(1), new Plan.Lit(0)))
                .withOrdering(new Plan.Ordering(List.of(new Plan.SortKey(1, false)), 5));

        int rows = 60_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, long[]> sumCount = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 80;
            v[i] = (i % 21) - 10;        // -10..10, signed
            long[] acc = sumCount.computeIfAbsent(k[i], ignored -> new long[2]);
            acc[0] += v[i];
            acc[1]++;
        }
        List<double[]> expected = new java.util.ArrayList<>();
        sumCount.forEach((key, sc) -> {
            double avg = (double) sc[0] / sc[1];
            if (avg > 0) {
                expected.add(new double[] {key, avg});
            }
        });
        expected.sort((a, b) -> Double.compare(a[1], b[1]));   // avg ASC
        List<double[]> top5 = expected.size() > 5 ? expected.subList(0, 5) : expected;

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(top5.size());
        long[] keys = result.columns()[0];
        long[] avgBits = result.columns()[1];
        for (int r = 0; r < top5.size(); r++) {
            assertThat((double) keys[r]).as("key at rank %d", r).isEqualTo(top5.get(r)[0]);
            assertThat(Double.longBitsToDouble(avgBits[r])).as("avg at rank %d", r).isEqualTo(top5.get(r)[1], within(1e-9));
            assertThat(Double.longBitsToDouble(avgBits[r])).isGreaterThan(0.0);
        }
    }

    @Test
    void lowersStarQueryByName()
    {
        // store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk WHERE d_year = 2001
        // GROUP BY ss_item_sk, sum(ss_quantity) ORDER BY ss_item_sk LIMIT 10 -- expressed by column name.
        QueryLowering query = QueryLowering.scan("store_sales",
                        new QueryLowering.Column("ss_sold_date_sk"),
                        new QueryLowering.Column("ss_item_sk"),
                        new QueryLowering.Column("ss_quantity"))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new QueryLowering.Column("d_date_sk"),
                        new QueryLowering.Column("d_year"));
        query.where(new Plan.Predicate("==", query.column("d_year"), new Plan.Lit(2001)))
                .groupBy("ss_item_sk")
                .aggregate("sum", "ss_quantity")
                .orderBy(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 10));
        QueryLowering.Lowered lowered = query.lower();

        // The hand-built positional equivalent (probe [0,1,2], build [3=d_date_sk, 4=d_year]).
        Plan.Pipeline hand = new Plan.Pipeline(
                3,
                new Plan.Build(2, 0),
                0,
                List.of(new Plan.Predicate("==", new Plan.Col(4), new Plan.Lit(2001))),
                List.of(new Plan.Col(1)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))))
                .withOrdering(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 10));

        // The lowering resolved the names to the same positions the hand plan uses.
        assertThat(query.position("ss_item_sk")).isEqualTo(1);
        assertThat(query.position("ss_quantity")).isEqualTo(2);
        assertThat(query.position("d_year")).isEqualTo(4);

        int dim = 2000;
        int year2001 = 700;
        long[] dKey = new long[dim];
        long[] dYear = new long[dim];
        for (int d = 0; d < dim; d++) {
            dKey[d] = d;
            dYear[d] = d < year2001 ? 2001 : 2000;
        }
        int fact = 400_000;
        long[] fk = new long[fact];
        long[] item = new long[fact];
        long[] qty = new long[fact];
        for (int i = 0; i < fact; i++) {
            fk[i] = i % dim;
            item[i] = i % 300;
            qty[i] = (i % 20) + 1;
        }
        long[][][] inputs = {{fk, item, qty}, {dKey, dYear}};
        int[] counts = {fact, dim};

        CompiledPipeline.Result loweredResult = lowered.compile().execute(inputs, counts);
        CompiledPipeline.Result handResult = PipelineCompiler.compile(hand).execute(inputs, counts);

        // Same plan, same data -> identical results.
        assertThat(loweredResult.rowCount()).isEqualTo(handResult.rowCount());
        for (int g = 0; g < handResult.rowCount(); g++) {
            assertThat(loweredResult.columns()[0][g]).isEqualTo(handResult.columns()[0][g]);
            assertThat(loweredResult.columns()[1][g]).isEqualTo(handResult.columns()[1][g]);
        }
    }

    @Test
    void compilesGroupByString()
    {
        // SELECT s, sum(v) GROUP BY s -- s is a dictionary string column; grouping is on the dense id and the
        // result column is the id typed STRING, which the consumer reconstructs via the dictionary.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        int dictSize = 30;
        byte[][] dictionary = new byte[dictSize][];
        for (int d = 0; d < dictSize; d++) {
            dictionary[d] = ("category-" + d).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        int rows = 90_000;
        int[] ids = new int[rows];
        long[] v = new long[rows];
        Map<Long, Long> reference = new HashMap<>();   // id -> sum(v)
        for (int i = 0; i < rows; i++) {
            ids[i] = i % dictSize;
            v[i] = (i % 50) + 1;
            reference.merge((long) ids[i], v[i], Long::sum);
        }

        ColumnEncoding[][] encodings = {{ColumnEncoding.STRING, ColumnEncoding.FLAT}};
        System.out.println("=== generated group-by-string source ===\n" + PipelineCompiler.render(pipeline, encodings));

        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        Column[][] inputs = {{new Column.StringColumn(ids, dictionary), new Column.FlatColumn(v)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {rows});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        assertThat(result.types()[0]).isEqualTo(Types.STRING);
        long[] keyIds = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum for id %d", keyIds[g]).isEqualTo(reference.get(keyIds[g]));
            // The consumer reconstructs the string from the id via the dictionary.
            String category = new String(dictionary[(int) keyIds[g]], java.nio.charset.StandardCharsets.UTF_8);
            assertThat(category).startsWith("category-");
        }
    }

    @Test
    void compilesStringColumnCompare()
    {
        // SELECT count(*) FROM t WHERE s0 <> s1 -- two dictionary-encoded string columns carrying INDEPENDENT
        // dictionaries (different entries and a different id order), so a dict-id comparison would be wrong; the
        // compiler must remap one column's ids into the other's id space and compare by value.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(new Plan.StringColumnCompare(0, 1, true)),
                List.of(),
                List.of(new Plan.Aggregate("count", null)));

        byte[][] dict0 = {bytes("AA"), bytes("BB"), bytes("CC")};                              // AA=0, BB=1, CC=2
        int[] ids0 = {0, 1, 2, 0, 1};                                                         // AA, BB, CC, AA, BB
        byte[][] dict1 = {bytes("YY"), bytes("XX"), bytes("CC"), bytes("BB"), bytes("AA")};    // YY=0 .. AA=4
        int[] ids1 = {4, 1, 2, 0, 3};                                                         // AA, XX, CC, YY, BB
        // row values: (AA,AA) equal, (BB,XX) differ, (CC,CC) equal, (AA,YY) differ, (BB,BB) equal -> 2 survive <>.
        long expected = 2;

        ColumnEncoding[][] encodings = {{ColumnEncoding.STRING, ColumnEncoding.STRING}};
        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        Column[][] inputs = {{new Column.StringColumn(ids0, dict0), new Column.StringColumn(ids1, dict1)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {ids0.length});

        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.columns()[0][0]).isEqualTo(expected);
    }

    @Test
    void compilesStringColumnSubstringCompare()
    {
        // SELECT count(*) FROM t WHERE substring(s0,1,2) <> substring(s1,1,2) -- the zip-prefix shape in Q19. Entries
        // that share a 2-char prefix but differ later must compare EQUAL (so the row is dropped under <>), which a
        // whole-value remap would get wrong; the compiler canonicalizes both dictionaries into shared prefix classes.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(new Plan.StringColumnCompare(0, 1, true, 1, 2)),
                List.of(),
                List.of(new Plan.Aggregate("count", null)));

        byte[][] dict0 = {bytes("12345"), bytes("12999"), bytes("67000")};   // prefixes 12, 12, 67
        int[] ids0 = {0, 1, 2, 0};                                          // 12345, 12999, 67000, 12345
        byte[][] dict1 = {bytes("67abc"), bytes("12zzz"), bytes("99xxx")};   // prefixes 67, 12, 99
        int[] ids1 = {1, 1, 0, 2};                                          // 12zzz, 12zzz, 67abc, 99xxx
        // prefixes: (12,12) equal, (12,12) equal, (67,67) equal, (12,99) differ -> only 1 survives <>.
        long expected = 1;

        ColumnEncoding[][] encodings = {{ColumnEncoding.STRING, ColumnEncoding.STRING}};
        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        Column[][] inputs = {{new Column.StringColumn(ids0, dict0), new Column.StringColumn(ids1, dict1)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {ids0.length});

        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.columns()[0][0]).isEqualTo(expected);
    }

    private static byte[] bytes(String value)
    {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void innerJoinWithKeyOnlyBuildIsSemiJoin()
    {
        // SELECT sum(p.v) FROM p WHERE p.k IN (SELECT k FROM b) -- an EXISTS/semi-join (Plan.Join.semi): the build
        // contributes only its key (no payload) and each probe row is kept at most once even if the build key repeats.
        // (A plain inner join now fans the probe row out over every matching build row, so EXISTS needs the semi flag.)
        Plan.Build keyOnly = new Plan.Build(1, 0);   // [b.k]
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,                                    // probe: [p.k, p.v]
                List.of(Plan.Join.semi(keyOnly, 0)),
                List.of(),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        long[] probeKey = {0, 1, 2, 3, 4};
        long[] probeVal = {10, 20, 30, 40, 50};
        long[] buildKey = {1, 3, 3};   // subset, with a duplicate to prove the probe row is kept once
        long expected = 20 + 40;       // only keys 1 and 3 are present

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(
                new long[][][] {{probeKey, probeVal}, {buildKey}}, new int[] {probeKey.length, buildKey.length});

        assertThat(result.columns()[0][0]).isEqualTo(expected);
    }

    @Test
    void innerJoinFansOutOverNonUniqueArrayBuild()
    {
        // SELECT sum(b.v) FROM p JOIN b ON p.k = b.k -- the build key is NOT unique, so a probe row matches several
        // build rows and is emitted once per match (the compiler must not assume a 1:1 relationship). Dense keys take
        // the array-mode build path, where duplicates chain through buildNext.
        Plan.Build build = new Plan.Build(2, 0);   // [b.k, b.v], key col 0
        Plan.Pipeline pipeline = new Plan.Pipeline(
                1,                                  // probe: [p.k]
                List.of(new Plan.Join(build, 0)),
                List.of(),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))));   // combined: p.k=0, b.k=1, b.v=2

        long[] probeKey = {1, 2, 3};
        long[] buildKey = {1, 1, 2};               // key 1 appears twice (one-to-many), key 3 is absent
        long[] buildVal = {10, 20, 30};
        long expected = 10 + 20 + 30;              // p.k=1 -> {10,20}, p.k=2 -> {30}, p.k=3 -> none

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(
                new long[][][] {{probeKey}, {buildKey, buildVal}}, new int[] {probeKey.length, buildKey.length});

        assertThat(result.columns()[0][0]).isEqualTo(expected);
    }

    @Test
    void innerJoinFansOutOverNonUniqueHashBuild()
    {
        // Same one-to-many fan-out, but sparse keys force the open-addressing hash build path, where duplicates chain
        // through buildNext with the latest row as the slot head.
        Plan.Build build = new Plan.Build(2, 0);   // [b.k, b.v], key col 0
        Plan.Pipeline pipeline = new Plan.Pipeline(
                1,                                  // probe: [p.k]
                List.of(new Plan.Join(build, 0)),
                List.of(),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))));

        long[] probeKey = {5, 1_000_000};
        long[] buildKey = {1_000_000, 1_000_000, 1_000_000, 5};   // key 1_000_000 appears three times
        long[] buildVal = {1, 2, 4, 8};
        long expected = 8 + (1 + 2 + 4);           // p.k=5 -> {8}, p.k=1_000_000 -> {1,2,4}

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(
                new long[][][] {{probeKey}, {buildKey, buildVal}}, new int[] {probeKey.length, buildKey.length});

        assertThat(result.columns()[0][0]).isEqualTo(expected);
    }

    @Test
    void compilesPartitionAverageWindow()
    {
        // SELECT p, v, avg(v) OVER (PARTITION BY p) FROM t -- the whole-partition average appended to every row,
        // round-half-up over the partition's values (matching PartitionAverageI64WindowFunction). Columns: 0 = p, 1 = v.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(),
                List.of())
                .withWindow(Plan.Window.partitionAverage(new int[] {0}, 1));

        long[] p = {1, 1, 1, 2, 2, 3};
        long[] v = {10, 11, 12, 100, 105, 7};
        // averages: p=1 -> round(33/3)=11; p=2 -> round(205/2)=103 (round half up: (205+1)/2=103); p=3 -> 7
        Map<Long, Long> expectedAvg = Map.of(1L, 11L, 2L, 103L, 3L, 7L);

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{p, v}}, new int[] {p.length});

        assertThat(result.rowCount()).isEqualTo(p.length);
        long[] outP = result.columns()[0];
        long[] outV = result.columns()[1];
        long[] outAvg = result.columns()[2];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(outAvg[g]).as("partition-average for p=%d v=%d", outP[g], outV[g]).isEqualTo(expectedAvg.get(outP[g]));
        }
        // Every input (p, v) pair is present exactly once.
        Map<Long, Long> seen = new HashMap<>();
        for (int g = 0; g < result.rowCount(); g++) {
            seen.merge(outP[g] * 1000 + outV[g], 1L, Long::sum);
        }
        for (int i = 0; i < p.length; i++) {
            assertThat(seen.get(p[i] * 1000 + v[i])).as("row p=%d v=%d present", p[i], v[i]).isEqualTo(1L);
        }
    }

    @Test
    void compilesCrossJoin()
    {
        // SELECT count(*) FROM p, b WHERE p.v > b.t -- a cross / nested-loop join (no key): each probe row pairs with
        // every build row. Uses a 2-row build to exercise the inner loop, not just the scalar (1-row) case.
        Plan.Build build = new Plan.Build(1, new int[0]);   // [b.t], no key
        Plan.Pipeline pipeline = new Plan.Pipeline(
                1,                                            // probe: [p.v]
                List.of(Plan.Join.cross(build)),
                List.of(new Plan.Predicate(">", new Plan.Col(0), new Plan.Col(1))),
                List.of(),
                List.of(new Plan.Aggregate("count", null)));

        long[] probeVal = {10, 20};
        long[] buildThreshold = {5, 15};
        // pairs: (10>5) T, (10>15) F, (20>5) T, (20>15) T -> 3
        long expected = 3;

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(
                new long[][][] {{probeVal}, {buildThreshold}}, new int[] {probeVal.length, buildThreshold.length});

        assertThat(result.columns()[0][0]).isEqualTo(expected);
    }

    @Test
    void compilesAntiJoin()
    {
        // SELECT sum(p.v) FROM p WHERE p.k NOT IN (SELECT k FROM b) -- a NOT EXISTS / anti-join keeps only the probe
        // rows whose key has no match in the build.
        Plan.Build keyOnly = new Plan.Build(1, 0);   // [b.k]
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,                                    // probe: [p.k, p.v]
                List.of(Plan.Join.anti(keyOnly, 0)),
                List.of(),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        long[] probeKey = {0, 1, 2, 3, 4};
        long[] probeVal = {10, 20, 30, 40, 50};
        long[] buildKey = {1, 3, 3};   // keys 1 and 3 are present (duplicate proves it still excludes once)
        long expected = 10 + 30 + 50;  // keys 0, 2, 4 have no match

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(
                new long[][][] {{probeKey, probeVal}, {buildKey}}, new int[] {probeKey.length, buildKey.length});

        assertThat(result.columns()[0][0]).isEqualTo(expected);
    }

    @Test
    void compilesStringColumnCompareAcrossJoin()
    {
        // SELECT count(*) FROM probe JOIN dim ON probe.k = dim.k WHERE probe.bought <> dim.current -- the two string
        // columns live on different sides of a join with independent dictionaries, so the remap must resolve one
        // operand from the probe and the other from the build.
        Plan.Build dim = new Plan.Build(2, 0);   // [dim.k, dim.current]
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,                                // probe: [probe.k, probe.bought]
                List.of(new Plan.Join(dim, 0)),   // probe.k = dim.k (combined col 2)
                List.of(new Plan.StringColumnCompare(1, 3, true)),   // probe.bought (1) <> dim.current (3)
                List.of(),
                List.of(new Plan.Aggregate("count", null)));

        byte[][] boughtDict = {bytes("AA"), bytes("BB"), bytes("CC")};        // AA=0, BB=1, CC=2
        long[] probeKey = {0, 1, 2, 0};
        int[] boughtIds = {0, 0, 2, 1};                                      // AA, AA, CC, BB
        byte[][] currentDict = {bytes("CC"), bytes("BB"), bytes("AA")};       // CC=0, BB=1, AA=2 (different order)
        long[] dimKey = {0, 1, 2};
        int[] currentIds = {2, 1, 0};                                        // AA, BB, CC
        // joined rows: (AA,AA) equal, (AA,BB) differ, (CC,CC) equal, (BB,AA) differ -> 2 survive <>.
        long expected = 2;

        ColumnEncoding[][] encodings = {{ColumnEncoding.FLAT, ColumnEncoding.STRING}, {ColumnEncoding.FLAT, ColumnEncoding.STRING}};
        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        Column[][] inputs = {
                {new Column.FlatColumn(probeKey), new Column.StringColumn(boughtIds, boughtDict)},
                {new Column.FlatColumn(dimKey), new Column.StringColumn(currentIds, currentDict)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {probeKey.length, dimKey.length});

        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.columns()[0][0]).isEqualTo(expected);
    }

    @Test
    void compilesSnowflakeJoin()
    {
        // SELECT sum(d2.v) FROM fact JOIN d1 ON fact.k = d1.k JOIN d2 ON d1.fk = d2.k -- the second join keys on a
        // column from the first dimension (d1.fk), not the fact: a snowflake. The probe key for join 1 must be read
        // from d1's matched row, not the probe row.
        Plan.Build d1 = new Plan.Build(2, 0);          // [d1.k, d1.fk]
        Plan.Build d2 = new Plan.Build(2, 0);          // [d2.k, d2.v]
        Plan.Pipeline pipeline = new Plan.Pipeline(
                1,                                       // probe: [fact.k]
                List.of(new Plan.Join(d1, 0), new Plan.Join(d2, 2)),   // join1 keys on combined col 2 = d1.fk
                List.of(),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Col(4))));  // d2.v

        int dim1 = 100;
        int dim2 = 10;
        long[] d1Key = new long[dim1];
        long[] d1Fk = new long[dim1];
        for (int i = 0; i < dim1; i++) {
            d1Key[i] = i;
            d1Fk[i] = i % dim2;            // each d1 row points at a d2 row
        }
        long[] d2Key = new long[dim2];
        long[] d2Val = new long[dim2];
        for (int j = 0; j < dim2; j++) {
            d2Key[j] = j;
            d2Val[j] = (j + 1) * 7L;
        }
        int rows = 60_000;
        long[] factKey = new long[rows];
        long expected = 0;
        for (int i = 0; i < rows; i++) {
            factKey[i] = i % dim1;
            expected += d2Val[(int) d1Fk[(int) factKey[i]]];
        }

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(
                new long[][][] {{factKey}, {d1Key, d1Fk}, {d2Key, d2Val}}, new int[] {rows, dim1, dim2});

        assertThat(result.columns()[0][0]).isEqualTo(expected);
        assertThat(expected).isGreaterThan(0);
    }

    @Test
    void compilesStringMatchInsideCaseAggregate()
    {
        // SELECT k, sum(CASE WHEN s IN ('CA') THEN v ELSE 0 END) GROUP BY k -- s is a dictionary string column and
        // the CASE condition is a predicate-over-dictionary, so its id mask must be built like a WHERE string filter
        // (the day-of-week pivot shape). Only rows with s = 'CA' contribute v; the rest contribute 0.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Case(
                        List.of(new Plan.Case.Branch(new Plan.StringMatch(2, List.of("CA"), false), new Plan.Col(1))),
                        new Plan.Lit(0)))));

        byte[][] dictionary = {
                "CA".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "NY".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "TX".getBytes(java.nio.charset.StandardCharsets.UTF_8)};
        int rows = 90_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        int[] ids = new int[rows];
        Map<Long, Long> reference = new HashMap<>();   // k -> sum(v where s='CA')
        for (int i = 0; i < rows; i++) {
            k[i] = i % 7;
            v[i] = (i % 50) + 1;
            ids[i] = i % 3;                 // 0 -> CA, else NY/TX
            reference.merge(k[i], ids[i] == 0 ? v[i] : 0, Long::sum);
        }

        ColumnEncoding[][] encodings = {{ColumnEncoding.FLAT, ColumnEncoding.FLAT, ColumnEncoding.STRING}};
        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        Column[][] inputs = {{new Column.FlatColumn(k), new Column.FlatColumn(v), new Column.StringColumn(ids, dictionary)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {rows});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("CA-only sum for k %d", keys[g]).isEqualTo(reference.get(keys[g]));
        }
    }

    @Test
    void compilesStringFilterOverDictionary()
    {
        // SELECT k, sum(v) FROM t WHERE s IN ('CA','TX') GROUP BY k -- s is a dictionary string column,
        // filtered by predicate-over-dictionary; some s are null (excluded by the IN).
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(),
                List.of(new Plan.StringMatch(2, List.of("CA", "TX"), false)),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        byte[][] dictionary = {
                "CA".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "NY".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "TX".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "WA".getBytes(java.nio.charset.StandardCharsets.UTF_8)};
        int rows = 120_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        int[] ids = new int[rows];
        boolean[] sNull = new boolean[rows];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 40;
            v[i] = (i % 50) + 1;
            ids[i] = i % 4;              // 0=CA,1=NY,2=TX,3=WA
            sNull[i] = (i % 11 == 0);
            boolean keep = !sNull[i] && (ids[i] == 0 || ids[i] == 2);   // s IN ('CA','TX')
            if (keep) {
                reference.merge(k[i], v[i], Long::sum);
            }
        }

        ColumnEncoding[][] encodings = {{ColumnEncoding.FLAT, ColumnEncoding.FLAT, ColumnEncoding.STRING}};
        boolean[][] nullable = {{false, false, true}};
        System.out.println("=== generated string-filter source ===\n" + PipelineCompiler.render(pipeline, encodings, nullable));

        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings, nullable);
        Column[][] inputs = {{
                new Column.FlatColumn(k),
                new Column.FlatColumn(v),
                new Column.StringColumn(ids, dictionary, sNull)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {rows});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum for k %d", keys[g]).isEqualTo(reference.get(keys[g]));
        }
    }

    @Test
    void compilesCoalesce()
    {
        // SELECT k, sum(COALESCE(m, 0)) GROUP BY k -- a null measure contributes 0 (returns/credits pattern).
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Coalesce(new Plan.Col(1), new Plan.Lit(0)))));

        int rows = 80_000;
        long[] k = new long[rows];
        long[] m = new long[rows];
        boolean[] mNull = new boolean[rows];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 60;
            mNull[i] = (i % 4 == 0);
            m[i] = (i % 100) + 1;
            long contribution = mNull[i] ? 0 : m[i];   // COALESCE(m, 0)
            reference.merge(k[i], contribution, Long::sum);
        }

        boolean[][] nullable = {{false, true}};
        System.out.println("=== generated coalesce source ===\n" + PipelineCompiler.render(pipeline, null, nullable));

        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, null, nullable);
        Column[][] inputs = {{new Column.FlatColumn(k), new Column.FlatColumn(m, mNull)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum(coalesce(m,0)) for k %d", keys[g]).isEqualTo(reference.get(keys[g]));
        }
    }

    @Test
    void compilesNullAwareFilterAndAggregates()
    {
        // SELECT k, sum(m), count(m), count(*) FROM t WHERE f >= 0 GROUP BY k
        // f and m are nullable: a null f fails WHERE (three-valued); a null m is skipped by sum/count(m) but
        // count(*) still counts the row.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(),
                List.of(new Plan.Predicate(">=", new Plan.Col(1), new Plan.Lit(0))),
                List.of(new Plan.Col(0)),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Col(2)),
                        new Plan.Aggregate("count", new Plan.Col(2)),
                        new Plan.Aggregate("count", null)));

        int rows = 120_000;
        long[] k = new long[rows];
        long[] f = new long[rows];
        boolean[] fNull = new boolean[rows];
        long[] m = new long[rows];
        boolean[] mNull = new boolean[rows];
        Map<Long, long[]> reference = new HashMap<>();   // k -> {sum(m), count(m), count(*)}
        for (int i = 0; i < rows; i++) {
            k[i] = i % 50;
            fNull[i] = (i % 7 == 0);
            f[i] = (i % 5) - 2;          // -2..2
            mNull[i] = (i % 3 == 0);
            m[i] = (i % 100) + 1;
            if (fNull[i] || !(f[i] >= 0)) {
                continue;                // row excluded by WHERE
            }
            long[] acc = reference.computeIfAbsent(k[i], ignored -> new long[3]);
            acc[2]++;                    // count(*)
            if (!mNull[i]) {
                acc[0] += m[i];          // sum(m)
                acc[1]++;                // count(m)
            }
        }

        ColumnEncoding[][] encodings = null;
        boolean[][] nullable = {{false, true, true}};
        System.out.println("=== generated null-aware source ===\n" + PipelineCompiler.render(pipeline, encodings, nullable));

        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings, nullable);
        Column[][] inputs = {{
                new Column.FlatColumn(k),
                new Column.FlatColumn(f, fNull),
                new Column.FlatColumn(m, mNull)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {rows});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sumM = result.columns()[1];
        long[] countM = result.columns()[2];
        long[] countAll = result.columns()[3];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = reference.get(keys[g]);
            assertThat(sumM[g]).as("sum(m) for k %d", keys[g]).isEqualTo(expected[0]);
            assertThat(countM[g]).as("count(m) for k %d", keys[g]).isEqualTo(expected[1]);
            assertThat(countAll[g]).as("count(*) for k %d", keys[g]).isEqualTo(expected[2]);
        }
    }

    @Test
    void compilesStddevAggregate()
    {
        // SELECT k, stddev(v) GROUP BY k -- 3-cell aggregate, double result. The compiled engine uses Welford's online
        // algorithm to match the operator's StddevSamp BIT-FOR-BIT (the result is compared as an exact double in the
        // TPC-DS parity tests), so the expected value here is computed by the identical Welford recurrence over each
        // group's values in scan order and asserted EXACTLY -- the naive sum/sum-of-squares form would diverge.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("stddev", new Plan.Col(1))));

        int rows = 60_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, List<Long>> groups = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 40;
            v[i] = (i % 17);
            groups.computeIfAbsent(k[i], ignored -> new java.util.ArrayList<>()).add(v[i]);
        }

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.types()[1]).isEqualTo(Types.DOUBLE);
        long[] keys = result.columns()[0];
        long[] bits = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            List<Long> values = groups.get(keys[g]);
            long count = 0;
            double mean = 0.0;
            double m2 = 0.0;
            for (long x : values) {
                count++;
                double delta = x - mean;
                mean += delta / count;
                double delta2 = x - mean;
                m2 += delta * delta2;
            }
            double expected = count < 2 ? 0.0 : Math.sqrt(m2 / (count - 1));
            assertThat(Double.longBitsToDouble(bits[g])).as("stddev for k %d", keys[g]).isEqualTo(expected);
        }
    }

    @Test
    void compilesAvgWithDoubleResult()
    {
        // SELECT k, sum(v), count(*), avg(v) GROUP BY k -- avg is a 2-cell aggregate with a double result.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Col(1)),
                        new Plan.Aggregate("count", null),
                        new Plan.Aggregate("avg", new Plan.Col(1))));

        int rows = 100_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, long[]> sumCount = new HashMap<>();   // k -> {sum, count}
        for (int i = 0; i < rows; i++) {
            k[i] = i % 137;
            v[i] = (i % 50) + 1;
            long[] acc = sumCount.computeIfAbsent(k[i], ignored -> new long[2]);
            acc[0] += v[i];
            acc[1]++;
        }

        System.out.println("=== generated avg source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(sumCount.size());
        // Result columns: 0=k, 1=sum (LONG), 2=count (LONG), 3=avg (DOUBLE).
        assertThat(result.types()[3]).isEqualTo(Types.DOUBLE);
        assertThat(result.types()[1]).isEqualTo(Types.LONG);
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        long[] counts = result.columns()[2];
        long[] avgBits = result.columns()[3];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = sumCount.get(keys[g]);
            assertThat(sums[g]).isEqualTo(expected[0]);
            assertThat(counts[g]).isEqualTo(expected[1]);
            double avg = Double.longBitsToDouble(avgBits[g]);
            assertThat(avg).as("avg for k %d", keys[g]).isEqualTo((double) expected[0] / expected[1], within(1e-9));
        }
    }

    @Test
    void compilesHavingAndOrderBy()
    {
        // SELECT k, sum(v) GROUP BY k HAVING sum(v) > T ORDER BY k LIMIT 20
        long threshold = 700_000;
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))))
                .withHaving(new Plan.Predicate(">", new Plan.Col(1), new Plan.Lit(threshold)))
                .withOrdering(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), 20));

        int rows = 400_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, Long> sums = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 50;
            v[i] = (i % 100) + 1;
            sums.merge(k[i], v[i], Long::sum);
        }
        List<long[]> qualifying = new java.util.ArrayList<>();
        sums.forEach((key, sum) -> {
            if (sum > threshold) {
                qualifying.add(new long[] {key, sum});
            }
        });
        qualifying.sort((a, b) -> Long.compare(a[0], b[0]));
        List<long[]> expected = qualifying.size() > 20 ? qualifying.subList(0, 20) : qualifying;

        System.out.println("=== generated having source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(expected.size());
        long[] outKeys = result.columns()[0];
        long[] outSums = result.columns()[1];
        for (int r = 0; r < expected.size(); r++) {
            assertThat(outKeys[r]).isEqualTo(expected.get(r)[0]);
            assertThat(outSums[r]).isEqualTo(expected.get(r)[1]);
            assertThat(outSums[r]).isGreaterThan(threshold);
        }
    }

    @Test
    void compilesOrderByLimit()
    {
        // SELECT k, sum(v) GROUP BY k ORDER BY sum(v) DESC, k ASC LIMIT 10
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))))
                .withOrdering(new Plan.Ordering(
                        List.of(new Plan.SortKey(1, true), new Plan.SortKey(0, false)),
                        10));

        int rows = 200_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, Long> sums = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 137;
            v[i] = (i % 100) + 1;
            sums.merge(k[i], v[i], Long::sum);
        }
        // Reference top-10 by sum desc, then key asc.
        List<long[]> expected = new java.util.ArrayList<>();
        sums.forEach((key, sum) -> expected.add(new long[] {key, sum}));
        expected.sort((a, b) -> a[1] != b[1] ? Long.compare(b[1], a[1]) : Long.compare(a[0], b[0]));

        System.out.println("=== generated order-by source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(10);
        long[] outKeys = result.columns()[0];
        long[] outSums = result.columns()[1];
        for (int r = 0; r < 10; r++) {
            assertThat(outKeys[r]).as("key at rank %d", r).isEqualTo(expected.get(r)[0]);
            assertThat(outSums[r]).as("sum at rank %d", r).isEqualTo(expected.get(r)[1]);
        }
    }

    @Test
    void compilesOrderByNullKeyLast()
    {
        // SELECT k, sum(v) GROUP BY k ORDER BY k ASC -- k nullable with an actual null group. SQL/operator
        // semantics put nulls last for ascending; the ORDER BY must consult the key's null mask, not its
        // canonical 0 value (which would otherwise sort the null group first).
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))))
                .withOrdering(new Plan.Ordering(List.of(new Plan.SortKey(0, false)), -1));

        int rows = 60_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        boolean[] kNull = new boolean[rows];
        for (int i = 0; i < rows; i++) {
            v[i] = 1;
            if (i % 5 == 0) {
                kNull[i] = true;        // a genuine null group, distinct from key value 0
            }
            else {
                k[i] = (i % 3) + 1;     // non-null keys 1, 2, 3
            }
        }

        boolean[][] nullable = {{true, false}};
        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline, null, nullable)
                .execute(new Column[][] {{new Column.FlatColumn(k, kNull), new Column.FlatColumn(v)}}, new int[] {rows});

        assertThat(result.rowCount()).isEqualTo(4);            // keys 1,2,3 + the null group
        long[] keys = result.columns()[0];
        assertThat(keys[0]).isEqualTo(1);
        assertThat(keys[1]).isEqualTo(2);
        assertThat(keys[2]).isEqualTo(3);
        // The null group sorts last and is marked null in the result mask (not read as a 0 key).
        assertThat(result.nulls()[0][3]).isTrue();
        assertThat(result.nulls()[0][0]).isFalse();
    }

    @Test
    void compilesScalarFunctionsAndCase()
    {
        // SELECT k, sum(CASE WHEN v < 0 THEN -v ELSE v END), sum(v / 2), max(abs(v)) GROUP BY k
        Plan.Expr absViaCase = new Plan.Case(
                List.of(new Plan.Case.Branch(
                        new Plan.Predicate("<", new Plan.Col(1), new Plan.Lit(0)),
                        new Plan.Call("negate", new Plan.Col(1)))),
                new Plan.Col(1));
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(
                        new Plan.Aggregate("sum", absViaCase),
                        new Plan.Aggregate("sum", new Plan.Bin("/", new Plan.Col(1), new Plan.Lit(2))),
                        new Plan.Aggregate("max", new Plan.Call("abs", new Plan.Col(1)))));

        int rows = 100_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, long[]> reference = new HashMap<>();   // k -> {sum|v|, sum(v/2), max|v|}
        for (int i = 0; i < rows; i++) {
            k[i] = i % 100;
            v[i] = (i % 201) - 100;     // negatives and positives
            long[] acc = reference.computeIfAbsent(k[i], ignored -> new long[] {0, 0, Long.MIN_VALUE});
            acc[0] += Math.abs(v[i]);
            acc[1] += v[i] / 2;         // Java long division (truncates toward zero), matching the codegen
            acc[2] = Math.max(acc[2], Math.abs(v[i]));
        }

        System.out.println("=== generated scalar/case source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] absSum = result.columns()[1];
        long[] halfSum = result.columns()[2];
        long[] maxAbs = result.columns()[3];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = reference.get(keys[g]);
            assertThat(absSum[g]).as("sum|v| for k %d", keys[g]).isEqualTo(expected[0]);
            assertThat(halfSum[g]).as("sum(v/2) for k %d", keys[g]).isEqualTo(expected[1]);
            assertThat(maxAbs[g]).as("max|v| for k %d", keys[g]).isEqualTo(expected[2]);
        }
    }

    @Test
    void compilesAndComputesEncodedColumns()
    {
        // SELECT k, sum(v), count(*) GROUP BY k, with k dictionary-encoded and v a constant column.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(
                        new Plan.Aggregate("sum", new Plan.Col(1)),
                        new Plan.Aggregate("count", null)));

        int rows = 100_000;
        int dictSize = 50;
        long[] dictionary = new long[dictSize];
        for (int d = 0; d < dictSize; d++) {
            dictionary[d] = 1000 + d * 7L;          // sparse-valued dictionary
        }
        int[] ids = new int[rows];
        long constant = 5;
        long[] flatKey = new long[rows];
        long[] flatValue = new long[rows];
        Map<Long, long[]> reference = new HashMap<>();   // value -> {sum, count}
        for (int i = 0; i < rows; i++) {
            ids[i] = i % dictSize;
            flatKey[i] = dictionary[ids[i]];
            flatValue[i] = constant;
            long[] acc = reference.computeIfAbsent(flatKey[i], ignored -> new long[2]);
            acc[0] += constant;
            acc[1]++;
        }

        ColumnEncoding[][] encodings = {{ColumnEncoding.DICTIONARY, ColumnEncoding.CONSTANT}};
        System.out.println("=== generated encoded source ===\n" + PipelineCompiler.render(pipeline, encodings));

        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        Column[][] inputs = {{new Column.DictionaryColumn(ids, dictionary), new Column.ConstantColumn(constant)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {rows});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        long[] counts = result.columns()[2];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = reference.get(keys[g]);
            assertThat(expected).as("group %d", keys[g]).isNotNull();
            assertThat(sums[g]).as("sum for group %d", keys[g]).isEqualTo(expected[0]);
            assertThat(counts[g]).as("count for group %d", keys[g]).isEqualTo(expected[1]);
        }

        // The flat-input plan over decoded columns must produce the identical grouping.
        CompiledPipeline.Result flat = PipelineCompiler.compile(pipeline).execute(new long[][][] {{flatKey, flatValue}}, new int[] {rows});
        assertThat(flat.rowCount()).isEqualTo(result.rowCount());
    }

    @Test
    void compilesAndComputesMinMaxGroupedAggregate()
    {
        // SELECT k, min(v), max(v), sum(v) GROUP BY k -- min/max are registered in AggregateLibrary with no
        // compiler changes. The deopt scenario exercises their identity and merge fragments.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(
                        new Plan.Aggregate("min", new Plan.Col(1)),
                        new Plan.Aggregate("max", new Plan.Col(1)),
                        new Plan.Aggregate("sum", new Plan.Col(1))));

        int rows = 200_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        // Narrow first, then wide: forces the speculative array group to deopt mid-stream into the hash table.
        Map<Long, long[]> reference = new HashMap<>();   // k -> {min, max, sum}
        for (int i = 0; i < rows; i++) {
            k[i] = i < 4096 ? i % 30 : i % 4000;
            v[i] = ((i * 2654435761L) % 1000) - 500;     // spread of positives and negatives
            long[] acc = reference.computeIfAbsent(k[i], ignored -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE, 0});
            acc[0] = Math.min(acc[0], v[i]);
            acc[1] = Math.max(acc[1], v[i]);
            acc[2] += v[i];
        }

        System.out.println("=== generated min/max source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] mins = result.columns()[1];
        long[] maxs = result.columns()[2];
        long[] sums = result.columns()[3];
        for (int g = 0; g < result.rowCount(); g++) {
            long[] expected = reference.get(keys[g]);
            assertThat(mins[g]).as("min for k %d", keys[g]).isEqualTo(expected[0]);
            assertThat(maxs[g]).as("max for k %d", keys[g]).isEqualTo(expected[1]);
            assertThat(sums[g]).as("sum for k %d", keys[g]).isEqualTo(expected[2]);
        }
    }

    @Test
    void compilesAndComputesBooleanFilterTree()
    {
        // SELECT k, sum(v) WHERE (k IN (1, 3, 5) OR k BETWEEN 10 AND 12) AND NOT (v == 0) GROUP BY k
        Plan.Condition inList = new Plan.Or(
                new Plan.Predicate("==", new Plan.Col(0), new Plan.Lit(1)),
                new Plan.Predicate("==", new Plan.Col(0), new Plan.Lit(3)),
                new Plan.Predicate("==", new Plan.Col(0), new Plan.Lit(5)));
        Plan.Condition between = new Plan.And(
                new Plan.Predicate(">=", new Plan.Col(0), new Plan.Lit(10)),
                new Plan.Predicate("<=", new Plan.Col(0), new Plan.Lit(12)));
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(
                        new Plan.Or(inList, between),
                        new Plan.Not(new Plan.Predicate("==", new Plan.Col(1), new Plan.Lit(0)))),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));

        int rows = 100_000;
        long[] k = new long[rows];
        long[] v = new long[rows];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            k[i] = i % 20;
            v[i] = (i % 5) - 2;       // includes 0 -> excluded by NOT (v == 0)
            boolean keep = (k[i] == 1 || k[i] == 3 || k[i] == 5 || (k[i] >= 10 && k[i] <= 12)) && v[i] != 0;
            if (keep) {
                reference.merge(k[i], v[i], Long::sum);
            }
        }

        System.out.println("=== generated boolean-filter source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline).execute(new long[][][] {{k, v}}, new int[] {rows});
        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum for k %d", keys[g]).isEqualTo(reference.get(keys[g]));
        }
    }

    @Test
    void compilesAndComputesTwoJoinStarGroupedAggregate()
    {
        // fact(fk_a, fk_b, measure) JOIN dimA(da_key, da_attr) ON fk_a=da_key JOIN dimB(db_key, db_attr) ON
        // fk_b=db_key WHERE db_attr >= 2 GROUP BY da_attr, sum(measure)
        // combined columns: probe [0=fk_a, 1=fk_b, 2=measure], dimA [3=da_key, 4=da_attr], dimB [5=db_key, 6=db_attr]
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(
                        new Plan.Join(new Plan.Build(2, 0), 0),
                        new Plan.Join(new Plan.Build(2, 0), 1)),
                List.of(new Plan.Predicate(">=", new Plan.Col(6), new Plan.Lit(2))),
                List.of(new Plan.Col(4)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))));

        int dimA = 100;
        int dimB = 50;
        long[] daKey = new long[dimA];
        long[] daAttr = new long[dimA];
        for (int d = 0; d < dimA; d++) {
            daKey[d] = d;
            daAttr[d] = d % 10;
        }
        long[] dbKey = new long[dimB];
        long[] dbAttr = new long[dimB];
        for (int d = 0; d < dimB; d++) {
            dbKey[d] = d;
            dbAttr[d] = d % 5;
        }
        int fact = 500_000;
        long[] fkA = new long[fact];
        long[] fkB = new long[fact];
        long[] measure = new long[fact];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < fact; i++) {
            fkA[i] = i % dimA;
            fkB[i] = i % dimB;
            measure[i] = (i % 50) + 1;
            if (dbAttr[(int) fkB[i]] >= 2) {
                reference.merge(daAttr[(int) fkA[i]], measure[i], Long::sum);
            }
        }

        System.out.println("=== generated two-join star source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{fkA, fkB, measure}, {daKey, daAttr}, {dbKey, dbAttr}}, new int[] {fact, dimA, dimB});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum for da_attr %d", keys[g]).isEqualTo(reference.get(keys[g]));
        }
    }

    @Test
    void compilesAndComputesLeftJoinKeepingUnmatchedProbeRows()
    {
        // fact(fk, measure) LEFT JOIN dim(key, attr) ON fk = key; dim has keys 0..4, fact has fk 0..9.
        // combined: probe [0=fk, 1=measure], dim [2=key, 3=attr]. Global sum(measure) + count(attr).
        // The left join keeps all 10 fact rows (so sum(measure) covers all 10); attr is NULL for the 5 unmatched
        // rows, so count(attr) -- which the aggregate-input null guard restricts to non-null -- is only 5.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(new Plan.Join(new Plan.Build(2, 0), 0, true)),
                List.of(),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1)), new Plan.Aggregate("count", new Plan.Col(3))));

        long[] key = {0, 1, 2, 3, 4};
        long[] attr = {10, 11, 12, 13, 14};
        long[] fk = new long[10];
        long[] measure = new long[10];
        long expectedSum = 0;
        for (int i = 0; i < 10; i++) {
            fk[i] = i;
            measure[i] = i + 1;
            expectedSum += measure[i];
        }

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{fk, measure}, {key, attr}}, new int[] {10, 5});

        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.columns()[0][0]).as("sum(measure) over all left-join rows").isEqualTo(expectedSum);
        assertThat(result.columns()[1][0]).as("count(attr) over matched rows only").isEqualTo(5L);
    }

    @Test
    void compilesAndComputesMultiKeyJoinGroupedAggregate()
    {
        // fact(fk0, fk1, measure) JOIN dim(dk0, dk1, dattr) ON fk0=dk0 AND fk1=dk1 GROUP BY dattr, sum(measure)
        // combined columns: probe [0=fk0, 1=fk1, 2=measure], build [3=dk0, 4=dk1, 5=dattr]
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                new Plan.Build(3, new int[] {0, 1}),
                new int[] {0, 1},
                List.of(),
                List.of(new Plan.Col(5)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))));

        int dim0 = 100;
        int dim1 = 10;
        int dim = dim0 * dim1;            // 1000 dim rows, unique (dk0, dk1)
        int groups = 20;
        long[] dk0 = new long[dim];
        long[] dk1 = new long[dim];
        long[] dattr = new long[dim];
        for (int a0 = 0; a0 < dim0; a0++) {
            for (int a1 = 0; a1 < dim1; a1++) {
                int row = a0 * dim1 + a1;
                dk0[row] = a0;
                dk1[row] = a1;
                dattr[row] = row % groups;
            }
        }
        int fact = 500_000;
        long[] fk0 = new long[fact];
        long[] fk1 = new long[fact];
        long[] measure = new long[fact];
        Map<Long, Long> reference = new HashMap<>();
        for (int i = 0; i < fact; i++) {
            fk0[i] = i % dim0;
            fk1[i] = i % dim1;
            measure[i] = (i % 50) + 1;
            long attr = dattr[(int) (fk0[i] * dim1 + fk1[i])];
            reference.merge(attr, measure[i], Long::sum);
        }

        System.out.println("=== generated multi-key join source ===\n" + PipelineCompiler.render(pipeline));

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{fk0, fk1, measure}, {dk0, dk1, dattr}}, new int[] {fact, dim});

        assertThat(result.rowCount()).isEqualTo(reference.size());
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            assertThat(sums[g]).as("sum for attr %d", keys[g]).isEqualTo(reference.get(keys[g]));
        }
    }

    @Test
    void compilesRollup()
    {
        // SELECT a, b, sum(v), grouping_id GROUP BY ROLLUP(a, b) -- the single-pass EXPAND grouping-sets path.
        // ROLLUP(a, b) = grouping sets {(a, b), (a), ()}. Result columns are [a, b, sum, grouping_id] where an
        // inactive key reads NULL and grouping_id is the GROUPING() bitmask (bit for a = 2, bit for b = 1; a set
        // bit means the key is aggregated away). So the three levels carry grouping_id 0, 1, 3 respectively.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(),
                List.of(new Plan.Col(0), new Plan.Col(1)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))))
                .withGroupingSets(List.of(new int[] {0, 1}, new int[] {0}, new int[0]));

        int rows = 60_000;
        long[] a = new long[rows];
        long[] b = new long[rows];
        long[] v = new long[rows];
        // Expected sums per level, keyed by a normalized (a, b) where -1 stands for an inactive (NULL) key.
        Map<Long, Long> full = new HashMap<>();      // (a, b)
        Map<Long, Long> byA = new HashMap<>();        // (a)
        long total = 0;
        for (int i = 0; i < rows; i++) {
            a[i] = i % 4;                 // four distinct a
            b[i] = i % 7;                 // seven distinct b
            v[i] = (i % 100) - 30;        // mix of negative and positive
            full.merge(a[i] * 1000 + b[i], v[i], Long::sum);
            byA.merge(a[i], v[i], Long::sum);
            total += v[i];
        }

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{a, b, v}}, new int[] {rows});

        long[] outA = result.columns()[0];
        long[] outB = result.columns()[1];
        long[] outSum = result.columns()[2];
        long[] outGroupingId = result.columns()[3];
        boolean[][] nulls = result.nulls();
        assertThat(nulls).isNotNull();

        // Expect every level: full (4*7 = 28 groups), by-a (4 groups), grand total (1 group).
        int expectedRows = full.size() + byA.size() + 1;
        assertThat(result.rowCount()).isEqualTo(expectedRows);

        int seenFull = 0;
        int seenByA = 0;
        int seenTotal = 0;
        for (int g = 0; g < result.rowCount(); g++) {
            boolean aNull = nulls[0] != null && nulls[0][g];
            boolean bNull = nulls[1] != null && nulls[1][g];
            long groupingId = outGroupingId[g];
            if (!aNull && !bNull) {
                // Full level (a, b): grouping_id 0, both keys active.
                assertThat(groupingId).as("grouping_id at full level").isEqualTo(0L);
                assertThat(outSum[g]).as("sum for (a=%d, b=%d)", outA[g], outB[g])
                        .isEqualTo(full.get(outA[g] * 1000 + outB[g]));
                seenFull++;
            }
            else if (!aNull) {
                // By-a level: b is NULL, grouping_id 1 (b aggregated away).
                assertThat(bNull).as("b must be null at by-a level").isTrue();
                assertThat(groupingId).as("grouping_id at by-a level").isEqualTo(1L);
                assertThat(outSum[g]).as("sum for (a=%d)", outA[g]).isEqualTo(byA.get(outA[g]));
                seenByA++;
            }
            else {
                // Grand total: both keys NULL, grouping_id 3.
                assertThat(bNull).as("b must be null at grand-total level").isTrue();
                assertThat(groupingId).as("grouping_id at grand-total level").isEqualTo(3L);
                assertThat(outSum[g]).as("grand total").isEqualTo(total);
                seenTotal++;
            }
        }
        assertThat(seenFull).isEqualTo(full.size());
        assertThat(seenByA).isEqualTo(byA.size());
        assertThat(seenTotal).isEqualTo(1);
    }

    @Test
    void streamsRollupInBatches()
    {
        // SELECT a, b, sum(v), grouping_id GROUP BY ROLLUP(a, b) -- computed once eagerly and once by streaming the
        // input in batches through a StreamingPipeline; the two must agree on every grouping level (full, by-a, grand
        // total), the per-group sums, the nulled keys, and the trailing grouping_id column.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(),
                List.of(new Plan.Col(0), new Plan.Col(1)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))))
                .withGroupingSets(List.of(new int[] {0, 1}, new int[] {0}, new int[0]));

        int rows = 200_000;
        long[] a = new long[rows];
        long[] b = new long[rows];
        long[] v = new long[rows];
        for (int i = 0; i < rows; i++) {
            a[i] = i % 4;                 // four distinct a
            b[i] = i % 7;                 // seven distinct b
            v[i] = (i % 100) - 30;        // mix of negative and positive
        }

        CompiledPipeline.Result eager = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{a, b, v}}, new int[] {rows});

        int batchSize = 4096;
        StreamingPipeline streaming = PipelineCompiler.compileStreaming(pipeline, null, null);
        CompiledPipeline.Result streamed = streaming.execute(flatBatches(batchSize, rows, a, b, v), new Column[0][], new int[0]);

        Map<String, String> eagerRows = rollupRows(eager);
        Map<String, String> streamedRows = rollupRows(streamed);

        assertThat(streamedRows).isEqualTo(eagerRows);
        assertThat(streamedRows).hasSize(eager.rowCount());
        assertThat(streamedRows).isNotEmpty();
        assertThat(rows / batchSize).isGreaterThan(1);   // genuinely multiple batches
    }

    /**
     * Index a ROLLUP(a, b) result by a normalized key encoding each key's null-ness and value, to a "sum|grouping_id"
     * string (so {@link Map#equals} compares values by content, unlike a {@code long[]} value).
     */
    private static Map<String, String> rollupRows(CompiledPipeline.Result result)
    {
        Map<String, String> map = new HashMap<>();
        long[] outA = result.columns()[0];
        long[] outB = result.columns()[1];
        long[] outSum = result.columns()[2];
        long[] outGroupingId = result.columns()[3];
        boolean[][] nulls = result.nulls();
        for (int g = 0; g < result.rowCount(); g++) {
            boolean aNull = nulls != null && nulls[0] != null && nulls[0][g];
            boolean bNull = nulls != null && nulls[1] != null && nulls[1][g];
            String key = (aNull ? "_" : Long.toString(outA[g])) + "|" + (bNull ? "_" : Long.toString(outB[g]));
            map.put(key, outSum[g] + "|" + outGroupingId[g]);
        }
        return map;
    }

    @Test
    void compilesRankingWindow()
    {
        // SELECT p, o, m, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rnk WHERE rnk <= 3 -- the top-N-per-partition
        // ranking window matching the operator harness's TopNRankingOperator. Columns: 0 = partition p, 1 = order o,
        // 2 = a passthrough measure m. RANK ties share a rank and the next rank skips; output is globally ordered by
        // (p ASC, o DESC). Ties on o are exercised so RANK semantics (not row_number) are observable.
        int limit = 3;
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(),
                List.of(),
                List.of())
                .withWindow(new Plan.Window(
                        new int[] {0},
                        List.of(new Plan.SortKey(1, true)),
                        Plan.RankFunction.RANK,
                        limit));

        int rows = 50_000;
        long[] p = new long[rows];
        long[] o = new long[rows];
        long[] m = new long[rows];
        for (int i = 0; i < rows; i++) {
            p[i] = i % 8;            // eight partitions
            o[i] = (i % 5);          // five distinct ordering values -> guaranteed ties within each partition
            m[i] = i;                // distinct passthrough payload
        }

        CompiledPipeline.Result result = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{p, o, m}}, new int[] {rows});

        long[] expectedRank = referenceRanking(p, o, m, new int[] {0}, new int[] {1}, new boolean[] {true}, limit);

        assertThat(result.rowCount()).isEqualTo(expectedRank.length);
        long[] outP = result.columns()[0];
        long[] outO = result.columns()[1];
        long[] outM = result.columns()[2];
        long[] outRank = result.columns()[3];

        // The reference walks the same global (p ASC, o DESC) order and emits the surviving rows; for distinct
        // payloads the row identity is recoverable from m, so assert row-by-row equality.
        Map<Long, Long> expectedRankByM = new HashMap<>();
        Map<Long, Long> expectedPByM = new HashMap<>();
        Map<Long, Long> expectedOByM = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            expectedPByM.put(m[i], p[i]);
            expectedOByM.put(m[i], o[i]);
        }
        // Recompute the per-row ranks against the full data to compare by row identity (m).
        long[] fullRanks = referenceRanksKeyedByPayload(p, o, m, new int[] {0}, new int[] {1}, new boolean[] {true});
        for (int i = 0; i < rows; i++) {
            expectedRankByM.put(m[i], fullRanks[i]);
        }

        for (int g = 0; g < outRank.length; g++) {
            long payload = outM[g];
            assertThat(outP[g]).as("partition for payload %d", payload).isEqualTo(expectedPByM.get(payload));
            assertThat(outO[g]).as("order for payload %d", payload).isEqualTo(expectedOByM.get(payload));
            assertThat(outRank[g]).as("rank for payload %d", payload).isEqualTo(expectedRankByM.get(payload));
            assertThat(outRank[g]).as("kept rows must be within the limit").isLessThanOrEqualTo(limit);
        }

        // Output must be globally ordered by (p ASC, o DESC).
        for (int g = 1; g < outRank.length; g++) {
            boolean ordered = outP[g - 1] < outP[g]
                    || (outP[g - 1] == outP[g] && outO[g - 1] >= outO[g]);
            assertThat(ordered).as("rows must be ordered by (p asc, o desc) at %d", g).isTrue();
        }
    }

    @Test
    void streamsRankingWindowInBatches()
    {
        // SELECT p, o, m, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rnk WHERE rnk <= 3 -- a top-N-per-partition
        // ranking window (a pipeline breaker) computed once eagerly and once by streaming the probe in batches; the
        // window must buffer all batches before ranking, so the two paths must produce an identical result.
        int limit = 3;
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(new Plan.Predicate(">=", new Plan.Col(1), new Plan.Lit(0))),   // a scan filter must apply per batch
                List.of(),
                List.of())
                .withWindow(new Plan.Window(
                        new int[] {0},
                        List.of(new Plan.SortKey(1, true)),
                        Plan.RankFunction.RANK,
                        limit));

        int rows = 200_000;
        long[] p = new long[rows];
        long[] o = new long[rows];
        long[] m = new long[rows];
        for (int i = 0; i < rows; i++) {
            p[i] = i % 8;            // eight partitions
            o[i] = (i % 5);          // five distinct ordering values -> guaranteed ties exercising RANK semantics
            m[i] = i;                // distinct passthrough payload
        }

        CompiledPipeline.Result eager = PipelineCompiler.compile(pipeline)
                .execute(new long[][][] {{p, o, m}}, new int[] {rows});

        int batchSize = 4096;
        StreamingPipeline streaming = PipelineCompiler.compileStreaming(pipeline, null, null);
        CompiledPipeline.Result streamed = streaming.execute(flatBatches(batchSize, rows, p, o, m), new Column[0][], new int[0]);

        // Batches are contiguous input-order slices, so the streamed materialization order matches the eager one; the
        // results (including the global (p asc, o desc) ordering and every tie) must be byte-for-byte identical.
        assertThat(streamed.rowCount()).isEqualTo(eager.rowCount());
        assertThat(eager.rowCount()).isGreaterThan(0);
        for (int column = 0; column < 4; column++) {
            assertThat(streamed.columns()[column])
                    .as("window result column %d (0=p,1=o,2=m,3=rank)", column)
                    .containsExactly(eager.columns()[column]);
        }
        long[] outRank = streamed.columns()[3];
        for (long rank : outRank) {
            assertThat(rank).as("kept rows must be within the limit").isLessThanOrEqualTo(limit);
        }
        assertThat(rows / batchSize).isGreaterThan(1);   // genuinely multiple batches
    }

    @Test
    void compilesRowNumberWindowWithNullPartitions()
    {
        // row_number() OVER (PARTITION BY p ORDER BY o), p nullable. A null-partition row is its own singleton at
        // rank 1 (partition equality is value equality, false for nulls). No rank limit: every row is kept. Exercises
        // the nullable-partition and ROW_NUMBER (no-tie) code paths together.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                3,
                List.of(),
                List.of(),
                List.of())
                .withWindow(new Plan.Window(
                        new int[] {0},
                        List.of(new Plan.SortKey(1, false)),
                        Plan.RankFunction.ROW_NUMBER,
                        -1));

        int rows = 20_000;
        long[] p = new long[rows];
        boolean[] pNull = new boolean[rows];
        long[] o = new long[rows];
        long[] m = new long[rows];
        for (int i = 0; i < rows; i++) {
            p[i] = i % 6;
            pNull[i] = (i % 11) == 0;   // a sprinkling of null partitions
            o[i] = i % 10;
            m[i] = i;
        }

        CompiledPipeline.Result result = PipelineCompiler.compile(
                        pipeline,
                        null,
                        new boolean[][] {{true, false, false}})
                .execute(new Column[][] {{new Column.FlatColumn(p, pNull), new Column.FlatColumn(o), new Column.FlatColumn(m)}},
                        new int[] {rows});

        // Every row is kept (no limit), so output row count equals input.
        assertThat(result.rowCount()).isEqualTo(rows);

        long[] outRank = result.columns()[3];
        // Each null-partition row is a singleton, so its row_number must be 1.
        boolean[][] nulls = result.nulls();
        assertThat(nulls).isNotNull();
        boolean[] outPNull = nulls[0];
        assertThat(outPNull).isNotNull();
        int seenNullPartitions = 0;
        for (int g = 0; g < outRank.length; g++) {
            if (outPNull[g]) {
                assertThat(outRank[g]).as("null-partition row must have row_number 1").isEqualTo(1L);
                seenNullPartitions++;
            }
        }
        int expectedNullPartitions = 0;
        for (int i = 0; i < rows; i++) {
            if (pNull[i]) {
                expectedNullPartitions++;
            }
        }
        assertThat(seenNullPartitions).isEqualTo(expectedNullPartitions);

        // Within each non-null partition, row_number is a dense 1..n sequence (no ties even on equal o).
        Map<Long, Long> maxRankByPartition = new HashMap<>();
        Map<Long, Long> countByPartition = new HashMap<>();
        long[] outP = result.columns()[0];
        for (int g = 0; g < outRank.length; g++) {
            if (!outPNull[g]) {
                maxRankByPartition.merge(outP[g], outRank[g], Math::max);
                countByPartition.merge(outP[g], 1L, Long::sum);
            }
        }
        for (Map.Entry<Long, Long> entry : countByPartition.entrySet()) {
            assertThat(maxRankByPartition.get(entry.getKey()))
                    .as("row_number must run 1..count for partition %d", entry.getKey())
                    .isEqualTo(entry.getValue());
        }
    }

    /** Per-row rank() within (partition, order), keyed positionally by input row (no top-N filter). */
    private static long[] referenceRanksKeyedByPayload(long[] p, long[] o, long[] m, int[] partitionColumns, int[] orderingColumns, boolean[] descending)
    {
        long[][] columns = {p, o, m};
        int rows = p.length;
        Integer[] order = new Integer[rows];
        for (int i = 0; i < rows; i++) {
            order[i] = i;
        }
        java.util.Arrays.sort(order, (a, b) -> compareForReference(columns, partitionColumns, orderingColumns, descending, a, b));
        long[] ranks = new long[rows];
        int prev = -1;
        long partitionRowNumber = 0;
        long rank = 0;
        for (int oi = 0; oi < rows; oi++) {
            int r = order[oi];
            if (prev == -1 || partitionChanged(columns, partitionColumns, prev, r)) {
                partitionRowNumber = 1;
                rank = 1;
            }
            else {
                partitionRowNumber++;
                if (orderingChanged(columns, orderingColumns, prev, r)) {
                    rank = partitionRowNumber;
                }
            }
            ranks[r] = rank;
            prev = r;
        }
        return ranks;
    }

    /** The set of ranks the engine should keep (rank <= limit), in global (partition, order) order. */
    private static long[] referenceRanking(long[] p, long[] o, long[] m, int[] partitionColumns, int[] orderingColumns, boolean[] descending, int limit)
    {
        long[] all = referenceRanksKeyedByPayload(p, o, m, partitionColumns, orderingColumns, descending);
        long[][] columns = {p, o, m};
        int rows = p.length;
        Integer[] order = new Integer[rows];
        for (int i = 0; i < rows; i++) {
            order[i] = i;
        }
        java.util.Arrays.sort(order, (a, b) -> compareForReference(columns, partitionColumns, orderingColumns, descending, a, b));
        int kept = 0;
        for (int i = 0; i < rows; i++) {
            if (all[i] <= limit) {
                kept++;
            }
        }
        long[] result = new long[kept];
        int w = 0;
        for (int oi = 0; oi < rows; oi++) {
            int r = order[oi];
            if (all[r] <= limit) {
                result[w++] = all[r];
            }
        }
        return result;
    }

    private static int compareForReference(long[][] columns, int[] partitionColumns, int[] orderingColumns, boolean[] descending, int a, int b)
    {
        for (int partitionColumn : partitionColumns) {
            int c = Long.compare(columns[partitionColumn][a], columns[partitionColumn][b]);
            if (c != 0) {
                return c;
            }
        }
        for (int i = 0; i < orderingColumns.length; i++) {
            int c = Long.compare(columns[orderingColumns[i]][a], columns[orderingColumns[i]][b]);
            if (descending[i]) {
                c = -c;
            }
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private static boolean partitionChanged(long[][] columns, int[] partitionColumns, int prev, int r)
    {
        for (int partitionColumn : partitionColumns) {
            if (columns[partitionColumn][prev] != columns[partitionColumn][r]) {
                return true;
            }
        }
        return false;
    }

    private static boolean orderingChanged(long[][] columns, int[] orderingColumns, int prev, int r)
    {
        for (int orderingColumn : orderingColumns) {
            if (columns[orderingColumn][prev] != columns[orderingColumn][r]) {
                return true;
            }
        }
        return false;
    }
}
