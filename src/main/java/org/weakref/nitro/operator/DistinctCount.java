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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DistinctCountStateVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.StreamAccessor;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class DistinctCount
        implements Accumulator
{
    private final int inputColumn;
    private PrimitiveArrayPool arrayPool;
    private OperatorCodeGenerationResources codeGeneration;

    public DistinctCount(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    @Override
    public int[] distinctInputColumns()
    {
        return new int[] {inputColumn};
    }

    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        arrayPool = allocator.primitiveArrays();
        codeGeneration = allocator.engineResources().operatorCodeGeneration();
        DistinctCountStateVector stateVector = new DistinctCountStateVector();
        stateVector.ensureGroupCapacity(size);
        return Streams.ofValues(allocator.adopt(allocationContext, stateVector));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        ((DistinctCountStateVector) state.values()).ensureGroupCapacity(size);
        return state;
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        if (group != 0) {
            throw new UnsupportedOperationException("DistinctCount does not support grouped accumulation");
        }

        DistinctCountStateVector stateVector = (DistinctCountStateVector) state.values();
        Vector values = streams.values(inputColumn);
        Vector nulls = streams.nulls(inputColumn);
        DistinctIndex distinctIndex = distinctIndex(stateVector, new Vector[] {values});

        for (int position : mask) {
            if (distinctIndex.add(new Vector[] {values}, new Vector[] {nulls}, position, group)) {
                stateVector.incrementDistinctCount(group);
            }
        }
    }

    @Override
    public void accumulateDistinctSelected(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        if (group != 0) {
            throw new UnsupportedOperationException("DistinctCount does not support grouped accumulation");
        }

        DistinctCountStateVector stateVector = (DistinctCountStateVector) state.values();
        stateVector.ensureGroupCapacity(group + 1);
        stateVector.incrementDistinctCount(group, mask.count());
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        DistinctCountStateVector stateVector = (DistinctCountStateVector) state.values();
        Vector values = streams.values(inputColumn);
        Vector nulls = streams.nulls(inputColumn);
        I64Vector groupVector = (I64Vector) groups;
        DistinctIndex distinctIndex = distinctIndex(stateVector, new Vector[] {groups, values});

        for (int position : mask) {
            int group = toIntExact(groupVector.values()[position]);
            if (distinctIndex.add(new Vector[] {groups, values}, new Vector[] {null, nulls}, position, group)) {
                stateVector.incrementDistinctCount(group);
            }
        }
    }

    @Override
    public void accumulateDistinctSelected(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        DistinctCountStateVector stateVector = (DistinctCountStateVector) state.values();
        I64Vector groupVector = (I64Vector) groups;

        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                stateVector.incrementDistinctCount(toIntExact(groupVector.values()[position]));
            }
            return;
        }

        for (int position : mask) {
            stateVector.incrementDistinctCount(toIntExact(groupVector.values()[position]));
        }
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        DistinctCountStateVector stateVector = (DistinctCountStateVector) state.values();
        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (I64Vector) output.values(),
                I64Vector.class,
                maxGroup + 1,
                I64Vector::new);
        for (int group = 0; group <= maxGroup; group++) {
            values.values()[group] = stateVector.distinctCount(group);
        }

        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                maxGroup + 1);
        Arrays.fill(nulls.values(), 0, maxGroup + 1, false);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private DistinctIndex distinctIndex(DistinctCountStateVector stateVector, Vector[] keyValues)
    {
        Object implementation = stateVector.implementation();
        if (implementation != null) {
            return (DistinctIndex) implementation;
        }
        DistinctIndex index = new DelegatingDistinctIndex(DistinctKeySet.create(keyValues, arrayPool, codeGeneration));
        stateVector.setImplementation(index);
        return index;
    }

    private interface DistinctIndex
    {
        boolean add(Vector[] values, Vector[] nulls, int position, int group);
    }

    private static final class DelegatingDistinctIndex
            implements DistinctIndex
    {
        private final DistinctKeySet keys;

        private DelegatingDistinctIndex(DistinctKeySet keys)
        {
            this.keys = keys;
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position, int group)
        {
            return keys.add(values, nulls, position);
        }
    }
}
