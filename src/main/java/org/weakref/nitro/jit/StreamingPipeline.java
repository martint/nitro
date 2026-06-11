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
 * A pipeline compiled to consume its source one batch at a time, folding into aggregation/grouping state that
 * persists across batches and is finalized at the end. Unlike {@link CompiledPipeline}, which takes the whole
 * input materialized up front, this streams: the source decodes a batch, the fused routine processes it while it
 * is hot, and nothing holds the full column. This is the foundation for selection-driven lazy materialization and
 * runtime-refinement (dynamic) filters, which the {@link Source} will carry. (First increment: a single scanned
 * input, no joins, hash grouping.)
 */
public interface StreamingPipeline
{
    /**
     * A pull-based batch source. The driver advances it; each batch exposes its row count and one
     * {@link Column} per scanned column, in the order and encoding the pipeline was compiled for. The same
     * column objects need not persist across batches.
     */
    interface Source
    {
        /** Advance to the next batch; returns false when the input is exhausted. */
        boolean advance();

        /**
         * Optional capability: per-column winner dictionaries backing min/max-style string aggregates over
         * streamed view columns -- the aggregate compares candidate bytes in place and appends only the rare
         * winners, so the column never pays a per-row intern. {@link #winners} returns the growing backing
         * (entries beyond the current count are unset); {@link #addWinner} copies the bytes in and returns the
         * new entry's id. A source that does not serve such columns returns null from {@link #winners}.
         */
        interface StringWinners
        {
            byte[][] winners(int column);

            int addWinner(int column, byte[] data, int offset, int length);
        }

        /** Row count of the current batch. */
        int rows();

        /** Columns of the current batch (one per scanned column, in compiled order). */
        Column[] columns();

        /**
         * Selection-driven lazy materialization: materialize the requested {@code columns} of the current batch,
         * each gathered to the first {@code count} positions of {@code selection} (so output row {@code j} is the
         * batch's row {@code selection[j]}); entries for columns not requested are left null. Staged filtering
         * calls this per conjunct -- decoding only that conjunct's column(s), for only the rows that survived the
         * earlier conjuncts -- and once more for the payload columns over the final selection.
         * <p>
         * The default gathers from {@link #columns()} (correct for any source); a lazy source overrides it to defer
         * the conversion of each column until it is requested and to convert only the selected rows.
         */
        default Column[] materialize(int[] columns, int[] selection, int count)
        {
            Column[] full = columns();
            Column[] gathered = new Column[full.length];
            for (int column : columns) {
                gathered[column] = Column.gather(full[column], selection, count);
            }
            return gathered;
        }

        /**
         * Materialize the requested {@code columns} over the whole current batch (no selection). Used by
         * join-driven late materialization for the eager columns -- the probe's join keys and filter columns --
         * that must be decoded for every row to run the joins and filters; the payload columns are then
         * materialized via {@link #materialize(int[], int[], int)} for only the surviving rows.
         */
        default Column[] materialize(int[] columns)
        {
            Column[] full = columns();
            Column[] out = new Column[full.length];
            for (int column : columns) {
                out[column] = full[column];
            }
            return out;
        }
    }

    /**
     * Run the pipeline, streaming {@code source} (the probe / fact side) batch-by-batch. {@code builds} are the
     * join build (dimension) sides, materialized once into hash tables before the probe streams -- one
     * {@link Column}[] per build with its row count in {@code buildRowCounts}. Both are empty for a single-input
     * pipeline (no joins).
     */
    CompiledPipeline.Result execute(Source source, Column[][] builds, int[] buildRowCounts);
}
