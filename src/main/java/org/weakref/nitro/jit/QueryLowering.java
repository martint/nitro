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
    /** A physical source column: its name and how it is encoded / whether it is nullable. */
    public record Column(String name, ColumnEncoding encoding, boolean nullable)
    {
        public Column(String name)
        {
            this(name, ColumnEncoding.FLAT, false);
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
        Input build = new Input(table, List.of(columns));
        int buildKeyLocal = indexOf(columns, buildKey);
        for (Column column : columns) {
            assign(column.name());
        }
        builds.add(build);
        joins.add(new Plan.Join(new Plan.Build(columns.length, buildKeyLocal), position(probeKey)));
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

    public QueryLowering aggregate(String function, String columnName)
    {
        aggregates.add(new Plan.Aggregate(function, column(columnName)));
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

    public Lowered lower()
    {
        Plan.Pipeline pipeline = new Plan.Pipeline(
                probe.columns().size(),
                joins,
                filters,
                groupKeys,
                aggregates,
                having,
                ordering);
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
