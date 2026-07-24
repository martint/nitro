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

import org.weakref.nitro.core.batch.BatchCapability;
import org.weakref.nitro.core.batch.ColumnTraits;
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Batch;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// Compatibility batch that preserves the native batch and lazy outputs without conversion.
final class NativeSourceBatch
        implements SourceBatch
{
    private final Schema schema;
    private final boolean retained;
    private final boolean stableBorrow;
    private Batch batch;

    NativeSourceBatch(Schema schema, boolean retained, boolean stableBorrow, Batch batch)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.retained = retained;
        this.stableBorrow = stableBorrow;
        this.batch = requireNonNull(batch, "batch is null");
    }

    Batch transfer()
    {
        checkOpen();
        Batch result = batch;
        batch = null;
        return result;
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
        return new MaskSelection(batch.borrowMask());
    }

    @Override
    public ColumnView column(int index)
    {
        checkOpen();
        Mask mask = batch.borrowMask();
        return new NativeColumnView(
                schema.field(index).type(),
                mask.size(),
                new ColumnTraits(org.weakref.nitro.core.batch.ColumnEncoding.LAZY, schema.field(index).nullable(), retained, stableBorrow),
                batch.output(index));
    }

    @Override
    public void select(Selection selection)
    {
        checkOpen();
        requireNonNull(selection, "selection is null");
        if (selection instanceof MaskSelection maskSelection) {
            batch.constrain(maskSelection.mask());
            return;
        }

        int[] positions = new int[selection.count()];
        for (int index = 0; index < positions.length; index++) {
            positions[index] = selection.position(index);
        }
        batch.constrain(Mask.sparse(positions, batch.borrowMask().size()));
    }

    @Override
    public <T> Optional<T> capability(BatchCapability<T> capability)
    {
        if (capability == NativeBatchCapability.NATIVE_BATCH) {
            return Optional.of(capability.valueType().cast((NativeBatchAccess) this::transfer));
        }
        return Optional.empty();
    }

    @Override
    public void close()
    {
        if (batch != null) {
            batch.close();
            batch = null;
        }
    }

    private void checkOpen()
    {
        if (batch == null) {
            throw new IllegalStateException("source batch is closed or transferred");
        }
    }
}
