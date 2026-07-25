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
package org.weakref.nitro.legacy.pipeline;

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;

/**
 * Bridges a data-centric compiled pipeline back into the pull-based operator world: it runs the compiled routine
 * once and exposes its columnar {@link CompiledPipeline.Result result} as a single-batch source {@link Operator},
 * so a compiled stage is directly comparable to (and composable with) the interpreted operator chains the
 * harnesses build. Each result column is reconstructed into a {@link Vector} by its own {@link Type}, so the
 * bridge never switches on a fixed set of types; id-encoded columns (e.g. strings) are reconstructed from a
 * per-column {@code dictionary}.
 */
public final class CompiledOperator
        implements Operator
{
    private final int rowCount;
    private final Vector[] columns;
    private final BooleanVector[] nulls;

    private boolean produced;

    /** Bridge a result with no id-encoded columns (every slot holds its value directly). */
    public CompiledOperator(CompiledPipeline.Result result)
    {
        this(result, new byte[result.columns().length][][]);
    }

    /**
     * Bridge {@code result}, reconstructing each column with its {@link Type}. {@code dictionaries[c]} supplies the
     * byte values for an id-encoded column {@code c} (such as a reconstructed string group key) and is {@code null}
     * for self-contained columns.
     */
    public CompiledOperator(CompiledPipeline.Result result, byte[][][] dictionaries)
    {
        this.rowCount = result.rowCount();
        long[][] values = result.columns();
        Type[] types = result.types();
        boolean[][] columnNulls = result.nulls();
        this.columns = new Vector[values.length];
        this.nulls = new BooleanVector[values.length];
        for (int column = 0; column < values.length; column++) {
            // A caller-supplied dictionary marks the column as id-encoded regardless of its declared type:
            // a string CASE folds to a LONG-typed id expression (value branch = dictionary id, else branch =
            // the -1 empty-string sentinel), and the supplied dictionary is what gives those ids their values.
            Type type = dictionaries[column] != null ? org.weakref.nitro.legacy.pipeline.Types.STRING : types[column];
            columns[column] = type.toVector(values[column], rowCount, dictionaries[column]);
            if (columnNulls != null && columnNulls[column] != null) {
                nulls[column] = new BooleanVector(columnNulls[column]);
            }
        }
    }

    @Override
    public int outputCount()
    {
        return columns.length;
    }

    @Override
    public boolean hasNext()
    {
        return !produced;
    }

    @Override
    public Batch next()
    {
        produced = true;
        Output[] outputs = new Output[columns.length];
        for (int column = 0; column < columns.length; column++) {
            outputs[column] = nulls[column] == null
                    ? Output.of(Streams.ofValues(columns[column]))
                    : Output.of(Streams.ofValuesAndNulls(columns[column], nulls[column]));
        }
        return new Batch(Mask.all(rowCount), outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return false;
    }

    @Override
    public void close()
    {
    }
}
