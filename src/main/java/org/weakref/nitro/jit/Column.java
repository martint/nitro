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
 * {@link ColumnEncoding}, so encoded columns are read in place rather than decoded at the boundary. A column
 * declared nullable carries a per-row {@code nulls} mask ({@code nulls[i] == true} marks a SQL null).
 */
public sealed interface Column
{
    /**
     * Gather this column to the first {@code count} positions of {@code selection} -- the row {@code j} of the
     * result is the source's row {@code selection[j]}. Used by selection-driven lazy materialization: a payload
     * column is compacted to (only) the rows that survived a pipeline's row-reducing predicates. A constant column
     * is selection-invariant and returned as is.
     */
    static Column gather(Column column, int[] selection, int count)
    {
        return switch (column) {
            case ConstantColumn constant -> constant;
            case FlatColumn flat -> {
                long[] values = new long[count];
                boolean[] nulls = flat.nulls() == null ? null : new boolean[count];
                for (int j = 0; j < count; j++) {
                    values[j] = flat.values()[selection[j]];
                    if (nulls != null) {
                        nulls[j] = flat.nulls()[selection[j]];
                    }
                }
                yield new FlatColumn(values, nulls);
            }
            case DictionaryColumn dictionary -> {
                int[] ids = new int[count];
                boolean[] nulls = dictionary.nulls() == null ? null : new boolean[count];
                for (int j = 0; j < count; j++) {
                    ids[j] = dictionary.ids()[selection[j]];
                    if (nulls != null) {
                        nulls[j] = dictionary.nulls()[selection[j]];
                    }
                }
                yield new DictionaryColumn(ids, dictionary.dictionary(), nulls);
            }
            case StringColumn string -> {
                int[] ids = new int[count];
                boolean[] nulls = string.nulls() == null ? null : new boolean[count];
                for (int j = 0; j < count; j++) {
                    ids[j] = string.ids()[selection[j]];
                    if (nulls != null) {
                        nulls[j] = string.nulls()[selection[j]];
                    }
                }
                yield new StringColumn(ids, string.dictionary(), nulls);
            }
        };
    }

    /** Flat column: value at row {@code i} is {@code values[i]}, null when {@code nulls != null && nulls[i]}. */
    record FlatColumn(long[] values, boolean[] nulls)
            implements Column
    {
        public FlatColumn(long[] values)
        {
            this(values, null);
        }
    }

    /**
     * Dictionary column: value at row {@code i} is {@code dictionary[ids[i]]}; ids are dense
     * {@code [0, dictionary.length)}. Null when {@code nulls != null && nulls[i]}.
     */
    record DictionaryColumn(int[] ids, long[] dictionary, boolean[] nulls)
            implements Column
    {
        public DictionaryColumn(int[] ids, long[] dictionary)
        {
            this(ids, dictionary, null);
        }
    }

    /** Constant column (single-run RLE): every row holds {@code value}, or is null when {@code isNull}. */
    record ConstantColumn(long value, boolean isNull)
            implements Column
    {
        public ConstantColumn(long value)
        {
            this(value, false);
        }
    }

    /**
     * Dictionary-encoded string column: row {@code i} is the UTF-8 bytes {@code dictionary[ids[i]]} (null when
     * {@code nulls != null && nulls[i]}). The compiled loop works on the dense {@code ids}; string predicates are
     * evaluated once per dictionary entry into an id-indexed mask (predicate-over-dictionary).
     */
    record StringColumn(int[] ids, byte[][] dictionary, boolean[] nulls)
            implements Column
    {
        public StringColumn(int[] ids, byte[][] dictionary)
        {
            this(ids, dictionary, null);
        }
    }
}
