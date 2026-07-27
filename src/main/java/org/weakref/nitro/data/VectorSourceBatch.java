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
package org.weakref.nitro.data;

import org.weakref.nitro.core.batch.ColumnEncoding;
import org.weakref.nitro.core.batch.ColumnTraits;
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;

import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

/// Source-batch facade over one allocator-owned Nitro vector generation.
public final class VectorSourceBatch
        implements SourceBatch
{
    private final Schema schema;
    private final int positionCount;
    private final Mask ownedMask;
    private final VectorColumnGeneration[] columns;
    private final ColumnView[] views;
    private final VectorBatchScope buffers;
    private final Consumer<Mask> constrainer;
    private final Runnable closeAction;
    private Mask selection;
    private boolean closed;

    public VectorSourceBatch(
            Schema schema,
            Mask mask,
            VectorColumnGeneration[] columns,
            VectorBatchScope buffers,
            Consumer<Mask> constrainer,
            Runnable closeAction)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.selection = requireNonNull(mask, "mask is null");
        this.ownedMask = mask;
        this.positionCount = mask.size();
        this.columns = requireNonNull(columns, "columns is null");
        if (columns.length != schema.size()) {
            throw new IllegalArgumentException("column count does not match schema");
        }
        this.views = new ColumnView[columns.length];
        for (int index = 0; index < columns.length; index++) {
            VectorColumnGeneration column = requireNonNull(columns[index], "columns contains null");
            views[index] = new VectorColumnView(
                    schema.field(index).type(),
                    positionCount,
                    new ColumnTraits(ColumnEncoding.LAZY, schema.field(index).nullable(), false, false),
                    column);
        }
        this.buffers = requireNonNull(buffers, "buffers is null");
        this.constrainer = requireNonNull(constrainer, "constrainer is null");
        this.closeAction = requireNonNull(closeAction, "closeAction is null");
        buffers.begin(null);
    }

    @Override
    public Schema schema()
    {
        return schema;
    }

    @Override
    public Selection selection()
    {
        checkOpen();
        return new MaskSelection(selection);
    }

    @Override
    public ColumnView column(int index)
    {
        checkOpen();
        return views[checkIndex(index, views.length)];
    }

    @Override
    public void select(Selection selection)
    {
        checkOpen();
        Mask mask = toMask(requireNonNull(selection, "selection is null"));
        for (VectorColumnGeneration column : columns) {
            if (column.hasConstraintSensitiveTakenStreams()) {
                throw new IllegalStateException("Cannot select batch after a column stream was taken");
            }
        }
        for (VectorColumnGeneration column : columns) {
            column.invalidateResolvedForConstraint();
        }
        this.selection = mask;
        constrainer.accept(mask);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            for (VectorColumnGeneration column : columns) {
                column.close();
            }
            buffers.release(ownedMask);
            closeAction.run();
        }
        finally {
            buffers.endBatch();
        }
    }

    private Mask toMask(Selection selection)
    {
        if (selection.positionCount() != positionCount) {
            throw new IllegalArgumentException("selection position count does not match batch");
        }
        if (selection instanceof MaskSelection maskSelection) {
            return maskSelection.mask();
        }
        int count = selection.count();
        if (count < 0 || count > positionCount) {
            throw new IllegalArgumentException("invalid selection cardinality");
        }
        int[] positions = new int[count];
        int previous = -1;
        for (int index = 0; index < count; index++) {
            int position = selection.position(index);
            if (position <= previous || position >= positionCount) {
                throw new IllegalArgumentException("selection positions are not strictly increasing and in bounds");
            }
            positions[index] = position;
            previous = position;
        }
        if (selection.maxPosition() != previous) {
            throw new IllegalArgumentException("selection maxPosition does not match its positions");
        }
        return Mask.sparse(positions, positionCount);
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("source batch is closed");
        }
    }

    private record VectorColumnView(
            TypeBinding type,
            int positionCount,
            ColumnTraits traits,
            VectorColumnGeneration column)
            implements ColumnView
    {
        private VectorColumnView
        {
            requireNonNull(type, "type is null");
            requireNonNull(traits, "traits is null");
            requireNonNull(column, "column is null");
        }

        @Override
        public Set<Stream> streams()
        {
            return column.streams();
        }

        @Override
        public Vector borrow(Stream stream)
        {
            return column.borrow(stream);
        }

        @Override
        public Vector take(Stream stream)
        {
            return column.take(stream);
        }
    }
}
