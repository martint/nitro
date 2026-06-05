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

/**
 * A pipeline compiled to a single fused routine. {@link #execute} runs the whole scan -> filter -> project
 * -> aggregate pass over the input columns and returns the result columns.
 */
public interface CompiledPipeline
{
    /**
     * @param inputs one input per relation: {@code inputs[0]} is the probe/scan input, {@code inputs[1]} the
     *               join build side (if any). Each input is one {@link Column} per column, whose encoding must
     *               match what the pipeline was compiled for.
     * @param rowCounts row count per input, parallel to {@code inputs}
     * @return the result; {@code columns} are the group-key columns followed by the aggregate columns, each of
     *         length {@link Result#rowCount} (1 for a global aggregation)
     */
    Result execute(Column[][] inputs, int[] rowCounts);

    /** Convenience for all-flat inputs: wraps each {@code long[]} as a {@link Column.FlatColumn}. */
    default Result execute(long[][][] inputs, int[] rowCounts)
    {
        Column[][] wrapped = new Column[inputs.length][];
        for (int source = 0; source < inputs.length; source++) {
            wrapped[source] = new Column[inputs[source].length];
            for (int column = 0; column < inputs[source].length; column++) {
                wrapped[source][column] = new Column.FlatColumn(inputs[source][column]);
            }
        }
        return execute(wrapped, rowCounts);
    }

    /**
     * Result columns are the group-key columns then the aggregate columns. Each column is a {@code long[]};
     * {@code types[c]} is its {@link Type} (which knows how to decode the slot -- e.g. a {@code double} stores
     * {@code doubleToRawLongBits}, a string stores its dictionary id).
     */
    record Result(int rowCount, long[][] columns, Type[] types)
    {
        /** All-{@code long} columns. */
        public Result(int rowCount, long[][] columns)
        {
            this(rowCount, columns, allLong(columns.length));
        }

        private static Type[] allLong(int count)
        {
            Type[] types = new Type[count];
            java.util.Arrays.fill(types, Types.LONG);
            return types;
        }
    }
}
