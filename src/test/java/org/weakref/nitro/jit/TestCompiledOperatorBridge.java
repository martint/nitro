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
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.CompiledOperator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;

/**
 * Validates the compiled-to-operator bridge: a compiled pipeline's columnar result, reconstructed type-by-type
 * (LONG, DOUBLE, and a dictionary STRING key), is exposed as a source {@link CompiledOperator} whose rows match
 * what the harness machinery decodes -- the foundation for comparing a compiled stage apples-to-apples against the
 * interpreted operator chains.
 */
public class TestCompiledOperatorBridge
{
    @Test
    void bridgesTypedResultIntoOperatorRows()
    {
        // SELECT s, sum(v), avg(v) GROUP BY s -- s dictionary STRING key, sum LONG, avg DOUBLE.
        Plan.Pipeline pipeline = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1)), new Plan.Aggregate("avg", new Plan.Col(1))));

        byte[][] dictionary = {
                "Books".getBytes(StandardCharsets.UTF_8),
                "Music".getBytes(StandardCharsets.UTF_8),
                "Home".getBytes(StandardCharsets.UTF_8)};
        int rows = 90_000;
        int[] ids = new int[rows];
        long[] v = new long[rows];
        Map<Integer, long[]> reference = new HashMap<>();   // id -> [sum, count]
        for (int i = 0; i < rows; i++) {
            ids[i] = i % dictionary.length;
            v[i] = (i % 50) + 1;
            long[] state = reference.computeIfAbsent(ids[i], key -> new long[2]);
            state[0] += v[i];
            state[1]++;
        }

        ColumnEncoding[][] encodings = {{ColumnEncoding.STRING, ColumnEncoding.FLAT}};
        CompiledPipeline compiled = PipelineCompiler.compile(pipeline, encodings);
        Column[][] inputs = {{new Column.StringColumn(ids, dictionary), new Column.FlatColumn(v)}};
        CompiledPipeline.Result result = compiled.execute(inputs, new int[] {rows});

        assertThat(result.types()[0]).isEqualTo(Types.STRING);
        assertThat(result.types()[1]).isEqualTo(Types.LONG);
        assertThat(result.types()[2]).isEqualTo(Types.DOUBLE);

        // The STRING key column (0) reconstructs through the dictionary; the numeric columns are self-contained.
        byte[][][] dictionaries = {dictionary, null, null};

        List<Row> expected = new ArrayList<>();
        for (Map.Entry<Integer, long[]> entry : reference.entrySet()) {
            String category = new String(dictionary[entry.getKey()], StandardCharsets.UTF_8);
            long sum = entry.getValue()[0];
            double avg = (double) sum / entry.getValue()[1];
            expected.add(Row.row(category, sum, avg));
        }

        assertThat(operator(new CompiledOperator(result, dictionaries))).matches(expected);
    }
}
