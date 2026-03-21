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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.DistinctCountStateVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.StreamAccessor;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class DistinctCount
        implements Accumulator
{
    private final int inputColumn;
    private Vector cachedDictionaryValues;
    private int[] dictionaryGenerations = new int[0];
    private int dictionaryGeneration;

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
        OperatorKeySemantics.Key reusableProbeKey = reusableProbeKey(stateVector, values);

        if (values instanceof DictionaryVector dictionary) {
            accumulateDictionary(stateVector, dictionary, nulls, mask, reusableProbeKey);
            return;
        }

        for (int position : mask) {
            OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values, nulls, position, reusableProbeKey);
            if (key == null) {
                continue;
            }
            if (!stateVector.keys(group).contains(key)) {
                stateVector.keys(group).add(OperatorKeySemantics.ownedKey(key));
            }
        }
    }

    private void accumulateDictionary(DistinctCountStateVector stateVector, DictionaryVector dictionary, BooleanVector nulls, Mask mask, OperatorKeySemantics.Key reusableProbeKey)
    {
        Vector dictionaryValues = dictionary.values();
        ensureDictionaryCacheCapacity(dictionaryValues.length());
        int generation = currentDictionaryGeneration(dictionaryValues);
        int[] ids = dictionary.ids();
        for (int position : mask) {
            if (OperatorVectorSupport.isNull(nulls, position)) {
                continue;
            }

            int dictionaryId = ids[position];
            if (dictionaryGenerations[dictionaryId] == generation) {
                continue;
            }
            dictionaryGenerations[dictionaryId] = generation;

            OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(dictionaryValues, null, dictionaryId, reusableProbeKey);
            if (!stateVector.keys(0).contains(key)) {
                stateVector.keys(0).add(OperatorKeySemantics.ownedKey(key));
            }
        }
    }

    private void ensureDictionaryCacheCapacity(int size)
    {
        if (dictionaryGenerations.length >= size) {
            return;
        }
        int newSize = Math.max(size, Math.max(16, dictionaryGenerations.length * 2));
        dictionaryGenerations = Arrays.copyOf(dictionaryGenerations, newSize);
    }

    private int currentDictionaryGeneration(Vector dictionaryValues)
    {
        if (cachedDictionaryValues != dictionaryValues) {
            cachedDictionaryValues = dictionaryValues;
            if (dictionaryGeneration == Integer.MAX_VALUE) {
                Arrays.fill(dictionaryGenerations, 0);
                dictionaryGeneration = 0;
            }
            return ++dictionaryGeneration;
        }
        return dictionaryGeneration;
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        DistinctCountStateVector stateVector = (DistinctCountStateVector) state.values();
        Vector values = streams.values(inputColumn);
        BooleanVector nulls = streams.nulls(inputColumn);
        I64Vector groupVector = (I64Vector) groups;
        OperatorKeySemantics.Key reusableProbeKey = reusableProbeKey(stateVector, values);

        for (int position : mask) {
            OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values, nulls, position, reusableProbeKey);
            if (key == null) {
                continue;
            }
            int group = toIntExact(groupVector.values()[position]);
            if (!stateVector.keys(group).contains(key)) {
                stateVector.keys(group).add(OperatorKeySemantics.ownedKey(key));
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

    private static OperatorKeySemantics.Key reusableProbeKey(DistinctCountStateVector stateVector, Vector values)
    {
        Object reusableProbeKey = stateVector.reusableProbeKey();
        if (reusableProbeKey == null) {
            OperatorKeySemantics.Key key = OperatorKeySemantics.reusableProbeKey(values);
            stateVector.setReusableProbeKey(key);
            return key;
        }
        return (OperatorKeySemantics.Key) reusableProbeKey;
    }
}
