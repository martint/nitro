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
 * A physically-encoded input column handed to a {@link CompiledPipeline}. The encoding is a property of the
 * data, not the plan; the compiler generates an accessor specialized to each column's declared
 * {@link ColumnEncoding}, so encoded columns are read in place rather than decoded at the boundary.
 */
public sealed interface Column
{
    /** Flat column: value at row {@code i} is {@code values[i]}. */
    record FlatColumn(long[] values)
            implements Column
    {}

    /** Dictionary column: value at row {@code i} is {@code dictionary[ids[i]]}; ids are dense {@code [0, dictionary.length)}. */
    record DictionaryColumn(int[] ids, long[] dictionary)
            implements Column
    {}

    /** Constant column (single-run RLE): every row holds {@code value}. */
    record ConstantColumn(long value)
            implements Column
    {}
}
