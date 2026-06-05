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
     * @param columns one {@code long[]} per input column, each at least {@code rowCount} long
     * @param rowCount number of input rows
     * @return the result; {@code columns} are the group-key columns followed by the aggregate columns,
     *         each of length {@link Result#rowCount} (1 for a global aggregation)
     */
    Result execute(long[][] columns, int rowCount);

    record Result(int rowCount, long[][] columns) {}
}
