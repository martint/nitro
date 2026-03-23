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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DistinctCountStateVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.StreamAccessor;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.toIntExact;

public class DistinctCount
        implements Accumulator
{
    private final int inputColumn;

    public DistinctCount(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
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
        BooleanVector nulls = streams.nulls(inputColumn);
        DistinctIndex distinctIndex = distinctIndex(stateVector, new Vector[] {values});

        for (int position : mask) {
            if (distinctIndex.add(new Vector[] {values}, new BooleanVector[] {nulls}, position, group)) {
                stateVector.incrementDistinctCount(group);
            }
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        DistinctCountStateVector stateVector = (DistinctCountStateVector) state.values();
        Vector values = streams.values(inputColumn);
        BooleanVector nulls = streams.nulls(inputColumn);
        I64Vector groupVector = (I64Vector) groups;
        DistinctIndex distinctIndex = distinctIndex(stateVector, new Vector[] {groups, values});

        for (int position : mask) {
            int group = toIntExact(groupVector.values()[position]);
            if (distinctIndex.add(new Vector[] {groups, values}, new BooleanVector[] {null, nulls}, position, group)) {
                stateVector.incrementDistinctCount(group);
            }
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

        BooleanVector nulls = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (BooleanVector) output.getOrNull(Stream.NULLS),
                BooleanVector.class,
                maxGroup + 1,
                BooleanVector::new);
        Arrays.fill(nulls.values(), 0, maxGroup + 1, false);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private static DistinctIndex distinctIndex(DistinctCountStateVector stateVector, Vector[] keyValues)
    {
        Object implementation = stateVector.implementation();
        if (implementation != null) {
            return (DistinctIndex) implementation;
        }
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(keyValues);
        DistinctIndex index = layout != null ? new FlatDistinctIndex(layout) : new ObjectDistinctIndex(keyValues.length);
        stateVector.setImplementation(index);
        return index;
    }

    private interface DistinctIndex
    {
        boolean add(Vector[] values, BooleanVector[] nulls, int position, int group);
    }

    private static final class FlatDistinctIndex
            implements DistinctIndex
    {
        private final FlatGroupingTable table;
        private long nextGroupId;

        private FlatDistinctIndex(FlatKeyLayout layout)
        {
            this.table = new FlatGroupingTable(layout, 1024);
        }

        @Override
        public boolean add(Vector[] values, BooleanVector[] nulls, int position, int group)
        {
            if (hasNull(nulls, position)) {
                return false;
            }
            long newGroupId = nextGroupId;
            long assigned = table.assignGroup(values, position, newGroupId);
            if (assigned == newGroupId) {
                nextGroupId++;
                return true;
            }
            return false;
        }

        private static boolean hasNull(BooleanVector[] nulls, int position)
        {
            for (BooleanVector nullsVector : nulls) {
                if (OperatorVectorSupport.isNull(nullsVector, position)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final class ObjectDistinctIndex
            implements DistinctIndex
    {
        private final Map<Integer, ObjectOpenHashSet<Object>> keysByGroup = new HashMap<>();
        private final OperatorKeySemantics.Key[] probeKeys;
        private final OperatorKeySemantics.CompositeProbeKey compositeProbeKey;

        private ObjectDistinctIndex(int keyCount)
        {
            this.probeKeys = new OperatorKeySemantics.Key[keyCount];
            this.compositeProbeKey = keyCount > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(keyCount) : null;
        }

        @Override
        public boolean add(Vector[] values, BooleanVector[] nulls, int position, int group)
        {
            OperatorKeySemantics.Key key = keyForPosition(values, nulls, position);
            if (key == null) {
                return false;
            }
            ObjectOpenHashSet<Object> keys = keysByGroup.computeIfAbsent(group, _ -> new ObjectOpenHashSet<>());
            if (keys.contains(key)) {
                return false;
            }
            keys.add(OperatorKeySemantics.ownedKey(key));
            return true;
        }

        private OperatorKeySemantics.Key keyForPosition(Vector[] values, BooleanVector[] nulls, int position)
        {
            for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
                if (probeKeys[keyIndex] == null) {
                    probeKeys[keyIndex] = OperatorKeySemantics.reusableProbeKey(values[keyIndex]);
                }
                OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values[keyIndex], nulls[keyIndex], position, probeKeys[keyIndex]);
                if (key == null) {
                    return null;
                }
                probeKeys[keyIndex] = key;
            }
            return OperatorKeySemantics.probeCompositeKey(probeKeys, compositeProbeKey);
        }
    }
}
