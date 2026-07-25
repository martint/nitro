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
package org.weakref.nitro.operator.source;

import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.RuntimeFilter;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.Output;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

/// Generic engine-side assembly of a core source generation into a native operator batch.
///
/// This class contains no logical-type, Java-carrier, column-encoding, or connector knowledge.
/// Those decisions belong to the constructed per-column ingress bindings.
public final class ColumnViewSourceOperatorIngress
        implements SourceOperatorIngress
{
    private final Schema schema;
    private final SelectionOperatorIngress selectionIngress;
    private final RuntimeFilterSourceIngress runtimeFilterIngress;
    private final List<ColumnViewOperatorIngress> columnIngresses;

    public ColumnViewSourceOperatorIngress(
            Schema schema,
            SelectionOperatorIngress selectionIngress,
            RuntimeFilterSourceIngress runtimeFilterIngress,
            ColumnViewOperatorIngressFactory columnIngressFactory)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.selectionIngress = requireNonNull(selectionIngress, "selectionIngress is null");
        this.runtimeFilterIngress = requireNonNull(runtimeFilterIngress, "runtimeFilterIngress is null");
        requireNonNull(columnIngressFactory, "columnIngressFactory is null");
        this.columnIngresses = IntStream.range(0, schema.size())
                .mapToObj(column -> requireNonNull(
                        columnIngressFactory.bind(schema.field(column)),
                        "columnIngressFactory returned null"))
                .toList();
        for (int column = 0; column < schema.size(); column++) {
            if (!schema.field(column).type().identity().equals(this.columnIngresses.get(column).type().identity())) {
                throw new IllegalArgumentException("column ingress type does not match schema at column " + column);
            }
        }
    }

    @Override
    public Batch adapt(SourceBatch sourceBatch)
    {
        requireNonNull(sourceBatch, "sourceBatch is null");
        if (!compatibleSchema(sourceBatch.schema())) {
            throw new IllegalArgumentException("source batch schema does not match column ingress bindings");
        }

        Mask mask = requireNonNull(selectionIngress.toMask(sourceBatch.selection()), "selection ingress returned null mask");
        Output[] outputs = new Output[columnIngresses.size()];
        LazyColumnView[] columns = new LazyColumnView[columnIngresses.size()];
        try {
            for (int column = 0; column < outputs.length; column++) {
                columns[column] = new LazyColumnView(sourceBatch, column, schema.field(column).type());
                outputs[column] = requireNonNull(
                        columnIngresses.get(column).output(columns[column]),
                        "column ingress returned null")
                        .withConstraintSensitiveResolution();
            }
        }
        catch (RuntimeException | Error failure) {
            for (Output output : outputs) {
                if (output != null) {
                    output.close();
                }
            }
            selectionIngress.releaseMask(mask);
            throw failure;
        }

        return new Batch(
                mask,
                constrained -> {
                    for (LazyColumnView column : columns) {
                        column.invalidate();
                    }
                    sourceBatch.select(selectionIngress.toSelection(constrained));
                },
                selectionIngress::takeMask,
                selectionIngress::releaseMask,
                sourceBatch::close,
                outputs);
    }

    @Override
    public Selection selection(Mask mask)
    {
        return requireNonNull(selectionIngress.toSelection(mask), "selection ingress returned null selection");
    }

    @Override
    public boolean supportsRuntimeFilter(BatchSource source, SourceColumnHandle column)
    {
        return runtimeFilterIngress.supports(source, column);
    }

    @Override
    public RuntimeFilter runtimeFilter(SourceColumnHandle column, DynamicFilter filter)
    {
        return runtimeFilterIngress.runtimeFilter(column, filter);
    }

    private static final class LazyColumnView
            implements Supplier<ColumnView>
    {
        private final SourceBatch sourceBatch;
        private final int column;
        private final TypeBinding type;
        private ColumnView view;

        private LazyColumnView(SourceBatch sourceBatch, int column, TypeBinding type)
        {
            this.sourceBatch = sourceBatch;
            this.column = column;
            this.type = type;
        }

        @Override
        public ColumnView get()
        {
            if (view == null) {
                view = requireNonNull(sourceBatch.column(column), "source batch returned null column");
                if (!view.type().identity().equals(type.identity())) {
                    throw new IllegalArgumentException("source column type does not match binding at column " + column);
                }
                Selection selection = sourceBatch.selection();
                if (selection.positionCount() != view.positionCount()) {
                    throw new IllegalArgumentException("source selection and column have different position counts at column " + column);
                }
                if (selection.count() > 0 && selection.maxPosition() >= selection.positionCount()) {
                    throw new IllegalArgumentException("source selection exceeds its position count at column " + column);
                }
            }
            return view;
        }

        private void invalidate()
        {
            view = null;
        }
    }

    private boolean compatibleSchema(Schema sourceSchema)
    {
        if (sourceSchema.size() != schema.size()) {
            return false;
        }
        for (int column = 0; column < schema.size(); column++) {
            if (sourceSchema.field(column).nullable() != schema.field(column).nullable() ||
                    !sourceSchema.field(column).type().identity().equals(schema.field(column).type().identity())) {
                return false;
            }
        }
        return true;
    }
}
