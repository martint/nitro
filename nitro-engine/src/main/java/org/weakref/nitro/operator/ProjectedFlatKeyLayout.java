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
package org.weakref.nitro.operator;

import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;

/**
 * Flat physical-key layout whose canonical fields are implemented by one exact generated subclass. The base owns
 * wrapper resolution and scratch lifetime; generated hot methods load the bound primitive arrays directly.
 */
abstract class ProjectedFlatKeyLayout
        extends FlatKeyLayout
{
    private final ResolvedPersistentKeyLayout persistentLayout;
    private final FixedWidthKeyBatchBindings projectedBindings;
    private final ProductNullBatchBindings productNullBindings;
    private Vector[] boundValues;
    private Vector[] boundNulls;
    private Vector[][] boundNullSources;

    ProjectedFlatKeyLayout(
            Construction construction,
            ResolvedPersistentKeyLayout persistentLayout,
            ResolvedFixedWidthKeyLayout projectedLayout,
            PrimitiveArrayPool arrayPool)
    {
        super(construction);
        this.persistentLayout = persistentLayout;
        projectedBindings = new FixedWidthKeyBatchBindings(projectedLayout, arrayPool);
        productNullBindings = new ProductNullBatchBindings(persistentLayout, arrayPool);
    }

    @Override
    public void beginBatch(Vector[] values, Vector[] nulls)
    {
        projectedBindings.bind(values, nulls == null ? new Vector[0] : nulls);
        productNullBindings.bind(values, nulls);
        boundValues = persistentLayout.fieldValues(values);
        boundNullSources = persistentLayout.fieldNullSources(values, nulls);
        boundNulls = new Vector[boundNullSources.length];
        for (int field = 0; field < boundNullSources.length; field++) {
            if (boundNullSources[field].length > 0) {
                // FlatKeyLayout uses this only for conservative batch metadata. inputFieldNull evaluates every
                // nullable ancestor below, so choosing one live source never establishes a false null-free proof.
                boundNulls[field] = boundNullSources[field][0];
            }
        }
        try {
            super.beginBatch(boundValues, boundNulls);
        }
        catch (RuntimeException | Error e) {
            clearBoundFields();
            productNullBindings.release();
            projectedBindings.release();
            throw e;
        }
    }

    @Override
    public long hash(Vector[] values, Vector[] nulls, int position)
    {
        return super.hash(boundValues, boundNulls, position);
    }

    @Override
    public void writeRecord(
            byte[] fixedChunk,
            int fixedOffset,
            FlatGroupingTable.FlatVariableWidthArena variableWidthArena,
            Vector[] values,
            Vector[] nulls,
            int position,
            int recordIndex)
    {
        super.writeRecord(
                fixedChunk,
                fixedOffset,
                variableWidthArena,
                boundValues,
                boundNulls,
                position,
                recordIndex);
    }

    @Override
    public boolean identicalRecordToInput(
            byte[] fixedChunk,
            int fixedOffset,
            FlatGroupingTable.FlatVariableWidthArena variableWidthArena,
            Vector[] values,
            Vector[] nulls,
            int position,
            int recordIndex)
    {
        return super.identicalRecordToInput(
                fixedChunk,
                fixedOffset,
                variableWidthArena,
                boundValues,
                boundNulls,
                position,
                recordIndex);
    }

    @Override
    boolean inputFieldNull(int fieldIndex, Vector[] nulls, int position)
    {
        for (Vector source : boundNullSources[fieldIndex]) {
            if (OperatorVectorSupport.isNull(source, position)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void endBatch()
    {
        try {
            super.endBatch();
        }
        finally {
            clearBoundFields();
            productNullBindings.release();
            projectedBindings.release();
        }
    }

    @Override
    void releaseBuffers()
    {
        clearBoundFields();
        productNullBindings.release();
        projectedBindings.release();
        super.releaseBuffers();
    }

    private void clearBoundFields()
    {
        boundValues = null;
        boundNulls = null;
        boundNullSources = null;
    }

    final Object[] projectedKeyArrays()
    {
        return projectedBindings.keyArrays();
    }

    final int[][] projectedKeyMappings()
    {
        return projectedBindings.keyMappings();
    }

    final int[] projectedKeyMappingOffsets()
    {
        return projectedBindings.keyMappingOffsets();
    }

    final int[] projectedKeyBaseOffsets()
    {
        return projectedBindings.keyBaseOffsets();
    }

    final boolean[][] productNullArrays()
    {
        return productNullBindings.arrays();
    }

    final int[][] productNullMappings()
    {
        return productNullBindings.mappings();
    }

    final int[] productNullMappingOffsets()
    {
        return productNullBindings.mappingOffsets();
    }

    final int[] productNullBaseOffsets()
    {
        return productNullBindings.baseOffsets();
    }

    @Override
    final boolean requiresBatchBinding()
    {
        return true;
    }
}
