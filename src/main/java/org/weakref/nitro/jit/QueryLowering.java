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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lowers a star-shaped query described by column <em>name</em> into the positional {@link Plan.Pipeline} the
 * compiler consumes, plus the per-input load spec (which physical columns to read, in order, with their
 * {@link ColumnEncoding} and nullability). This is the front half of the compiler: it owns the
 * column-index ("information unit") bookkeeping so callers reference columns by name rather than by combined
 * position. Columns are addressed in the combined space (probe columns first, then each build's columns), and a
 * name resolves to the same position the compiled routine will use.
 * <p>
 * Scope matches what the compiler supports: a probe scan, a chain of inner equi-joins on the probe, filters,
 * grouping, aggregates, HAVING, and ORDER BY / LIMIT. Build keys are matched by name to probe keys.
 */
public final class QueryLowering
{
    /**
     * A physical source column: its logical {@code name} (unique within a query, used to wire keys/filters/outputs),
     * the {@code sourceName} to read from the table (defaults to {@code name}), and how it is encoded / nullable.
     * An explicit {@code sourceName} lets the same physical column be loaded under distinct logical names -- needed to
     * join one table more than once in a query (e.g. {@code date_dim} for both a sold and a returned date).
     */
    public record Column(String name, String sourceName, ColumnEncoding encoding, boolean nullable)
    {
        public Column(String name, ColumnEncoding encoding, boolean nullable)
        {
            this(name, name, encoding, nullable);
        }

        public Column(String name)
        {
            this(name, name, ColumnEncoding.FLAT, false);
        }
    }

    /** One relation in the query (the probe, or a join build side) with its loaded columns in order. */
    public record Input(String table, List<Column> columns)
    {
        public Input
        {
            columns = List.copyOf(columns);
        }
    }

    /** The lowered query: the positional plan and the inputs to load (probe first, then each build). */
    public record Lowered(Plan.Pipeline pipeline, List<Input> inputs)
    {
        public Lowered
        {
            inputs = List.copyOf(inputs);
        }

        public CompiledPipeline compile()
        {
            return PipelineCompiler.compile(pipeline, encodings(), nullable());
        }

        public ColumnEncoding[][] encodings()
        {
            ColumnEncoding[][] encodings = new ColumnEncoding[inputs.size()][];
            for (int s = 0; s < inputs.size(); s++) {
                List<Column> columns = inputs.get(s).columns();
                encodings[s] = new ColumnEncoding[columns.size()];
                for (int c = 0; c < columns.size(); c++) {
                    encodings[s][c] = columns.get(c).encoding();
                }
            }
            return encodings;
        }

        public boolean[][] nullable()
        {
            boolean[][] nullable = new boolean[inputs.size()][];
            for (int s = 0; s < inputs.size(); s++) {
                List<Column> columns = inputs.get(s).columns();
                nullable[s] = new boolean[columns.size()];
                for (int c = 0; c < columns.size(); c++) {
                    nullable[s][c] = columns.get(c).nullable();
                }
            }
            return nullable;
        }
    }

    private Input probe;
    private final List<Input> builds = new ArrayList<>();
    private final List<Plan.Join> joins = new ArrayList<>();
    private final Map<String, Integer> positions = new LinkedHashMap<>();   // column name -> combined position
    private final List<Plan.Condition> filters = new ArrayList<>();
    private final List<Plan.Expr> groupKeys = new ArrayList<>();
    private final List<Plan.Aggregate> aggregates = new ArrayList<>();
    private Plan.Condition having;
    private Plan.Ordering ordering;
    private final List<Plan.Expr> projections = new ArrayList<>();
    private List<int[]> groupingSets = List.of();
    private Plan.Window window;
    private int combined;   // next combined position to assign

    private QueryLowering() {}

    /** Begin a query with the probe relation and its loaded columns. */
    public static QueryLowering scan(String table, Column... columns)
    {
        QueryLowering query = new QueryLowering();
        query.probe = new Input(table, List.of(columns));
        for (Column column : columns) {
            query.assign(column.name());
        }
        return query;
    }

    /** Inner-join {@code table} on {@code probeKey = buildKey}; {@code columns} are the build columns to load (key included). */
    public QueryLowering join(String table, String probeKey, String buildKey, Column... columns)
    {
        return join(table, probeKey, buildKey, false, columns);
    }

    /** Left (outer) join: a probe row with no match is kept, with this build's columns reading as NULL. */
    public QueryLowering leftJoin(String table, String probeKey, String buildKey, Column... columns)
    {
        return join(table, probeKey, buildKey, true, columns);
    }

    /**
     * Inner-join {@code table} on a composite key: {@code probeKeys[i] = buildKeys[i]} for all i. {@code columns} are
     * the build columns to load (the build keys included). Used for fact-to-fact joins (e.g. sales to returns on
     * ticket + item + customer), where the build key need not be unique -- the join emits every matching build row.
     */
    public QueryLowering join(String table, String[] probeKeys, String[] buildKeys, Column... columns)
    {
        if (probeKeys.length != buildKeys.length) {
            throw new IllegalArgumentException("join key count mismatch: " + probeKeys.length + " probe vs " + buildKeys.length + " build");
        }
        Input build = new Input(table, List.of(columns));
        int[] buildKeyLocals = new int[buildKeys.length];
        for (int i = 0; i < buildKeys.length; i++) {
            buildKeyLocals[i] = indexOf(columns, buildKeys[i]);
        }
        for (Column column : columns) {
            assign(column.name());
        }
        builds.add(build);
        int[] probePositions = new int[probeKeys.length];
        for (int i = 0; i < probeKeys.length; i++) {
            probePositions[i] = position(probeKeys[i]);
        }
        joins.add(new Plan.Join(new Plan.Build(columns.length, buildKeyLocals), probePositions));
        return this;
    }

    /**
     * Left (outer) join on a composite key: {@code probeKeys[i] = buildKeys[i]}. A probe row with no match is kept,
     * this build's columns reading NULL -- the sales-to-returns shape (e.g. catalog_sales left join catalog_returns
     * on order + item, where most sales were never returned).
     */
    public QueryLowering leftJoin(String table, String[] probeKeys, String[] buildKeys, Column... columns)
    {
        if (probeKeys.length != buildKeys.length) {
            throw new IllegalArgumentException("join key count mismatch: " + probeKeys.length + " probe vs " + buildKeys.length + " build");
        }
        Input build = new Input(table, List.of(columns));
        int[] buildKeyLocals = new int[buildKeys.length];
        for (int i = 0; i < buildKeys.length; i++) {
            buildKeyLocals[i] = indexOf(columns, buildKeys[i]);
        }
        for (Column column : columns) {
            assign(column.name());
        }
        builds.add(build);
        int[] probePositions = new int[probeKeys.length];
        for (int i = 0; i < probeKeys.length; i++) {
            probePositions[i] = position(probeKeys[i]);
        }
        joins.add(new Plan.Join(new Plan.Build(columns.length, buildKeyLocals), probePositions, true));
        return this;
    }

    private QueryLowering join(String table, String probeKey, String buildKey, boolean outer, Column... columns)
    {
        Input build = new Input(table, List.of(columns));
        int buildKeyLocal = indexOf(columns, buildKey);
        for (Column column : columns) {
            assign(column.name());
        }
        builds.add(build);
        joins.add(new Plan.Join(new Plan.Build(columns.length, buildKeyLocal), position(probeKey), outer));
        return this;
    }

    /**
     * Anti-join (NOT EXISTS): keep probe rows with NO match in {@code table} on {@code probeKey = buildKey}. The build
     * contributes no output columns; pass just its key column. Used for "orders that were not returned"-style filters.
     */
    public QueryLowering antiJoin(String table, String probeKey, String buildKey, Column... columns)
    {
        Input build = new Input(table, List.of(columns));
        int buildKeyLocal = indexOf(columns, buildKey);
        for (Column column : columns) {
            assign(column.name());
        }
        builds.add(build);
        joins.add(Plan.Join.anti(new Plan.Build(columns.length, buildKeyLocal), position(probeKey)));
        return this;
    }

    /** Combined position of a column, for building filters/HAVING expressions by name. */
    public int position(String columnName)
    {
        Integer position = positions.get(columnName);
        if (position == null) {
            throw new IllegalArgumentException("unknown column: " + columnName);
        }
        return position;
    }

    /** A {@link Plan.Col} referencing {@code columnName}. */
    public Plan.Col column(String columnName)
    {
        return new Plan.Col(position(columnName));
    }

    public QueryLowering where(Plan.Condition... conditions)
    {
        for (Plan.Condition condition : conditions) {
            filters.add(condition);
        }
        return this;
    }

    public QueryLowering groupBy(String... columnNames)
    {
        for (String name : columnNames) {
            groupKeys.add(column(name));
        }
        return this;
    }

    /**
     * Aggregate over the given {@code groupingSets} (GROUPING SETS / ROLLUP / CUBE) rather than a single grouping over
     * all {@link #groupBy} keys. Each {@code int[]} is the sorted indices into the group keys that are ACTIVE in that
     * set; the others are nulled. The result carries a trailing {@code grouping_id} LONG column. Additive: leaving this
     * unset keeps the ordinary single-grouping behavior.
     */
    public QueryLowering groupingSets(List<int[]> groupingSets)
    {
        this.groupingSets = List.copyOf(groupingSets);
        return this;
    }

    /**
     * Apply a window over the (scan) input columns: a ranking or a partition-aggregate (see {@link Plan.Window}). The
     * window's partition/order/input columns index this query's input columns. A window is a pipeline breaker; any
     * following filter on the windowed value goes through {@link #having} (post-window), not {@link #where}.
     */
    public QueryLowering window(Plan.Window window)
    {
        this.window = window;
        return this;
    }

    public QueryLowering aggregate(String function, String columnName)
    {
        aggregates.add(new Plan.Aggregate(function, column(columnName)));
        return this;
    }

    /** Aggregate over a computed expression (e.g. {@code sum(CASE ... END)}), columns referenced by position. */
    public QueryLowering aggregate(String function, Plan.Expr input)
    {
        aggregates.add(new Plan.Aggregate(function, input));
        return this;
    }

    public QueryLowering count()
    {
        aggregates.add(new Plan.Aggregate("count", null));
        return this;
    }

    public QueryLowering having(Plan.Condition condition)
    {
        having = condition;
        return this;
    }

    public QueryLowering orderBy(Plan.Ordering ordering)
    {
        this.ordering = ordering;
        return this;
    }

    /**
     * Final SELECT projection over the result columns (group keys first, then aggregates, by position), applied
     * after HAVING and ORDER BY / LIMIT. Use to reorder or drop result columns, or compute over them.
     */
    public QueryLowering select(Plan.Expr... projections)
    {
        this.projections.addAll(List.of(projections));
        return this;
    }

    public Lowered lower()
    {
        Plan.Pipeline pipeline = new Plan.Pipeline(
                probe.columns().size(),
                joins,
                filters,
                groupKeys,
                aggregates,
                having,
                ordering,
                projections,
                groupingSets,
                window);
        List<Input> inputs = new ArrayList<>();
        inputs.add(probe);
        inputs.addAll(builds);
        return new Lowered(pipeline, inputs);
    }

    private void assign(String columnName)
    {
        if (positions.containsKey(columnName)) {
            throw new IllegalArgumentException("duplicate column name across inputs: " + columnName);
        }
        positions.put(columnName, combined++);
    }

    private static int indexOf(Column[] columns, String name)
    {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("build key not among build columns: " + name);
    }
}
