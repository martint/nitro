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
package org.weakref.nitro.operator.aggregation;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.SumStateVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import static java.lang.Math.toIntExact;

public class Sum
        implements GeneratedGroupedAccumulator
{
    private final int inputColumn;

    public Sum(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    /** The single input column this Sum reads, exposed for the fused grouped-aggregation kernel. */
    public int inputColumn()
    {
        return inputColumn;
    }

    @Override
    public GeneratedGroupedAccumulatorUpdate generatedGroupedUpdate()
    {
        return GeneratedGroupedAccumulatorUpdate.inputValue(inputColumn);
    }

    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValues(
                allocator.allocate(allocationContext, SumStateVector.class, size, SumStateVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        SumStateVector values = (SumStateVector) state.values();
        if (values.length() >= size) {
            return state;
        }
        SumStateVector grown = allocator.adopt(allocationContext, SumStateVector.grow(values, size));
        allocator.discard(allocationContext, values);
        return Streams.ofValues(grown);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        ((SumStateVector) state.values()).initialize(offset, length);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        SumStateVector stateVector = (SumStateVector) state.values();
        Vector inputValues = streams.values(inputColumn);
        Vector inputNulls = streams.stream(inputColumn, Stream.NULLS);

        if (tryAccumulateNullFreeGlobal(stateVector, group, inputValues, inputNulls, mask)) {
            return;
        }

        long sum = 0;
        boolean sawNonNull = false;
        VectorAccess.LongValues values = VectorAccess.longValues(inputValues);
        VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(inputNulls);
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                if (nulls.value(position)) {
                    continue;
                }
                sawNonNull = true;
                sum += values.value(position);
            }
        }
        else {
            for (int position : mask) {
                if (nulls.value(position)) {
                    continue;
                }
                sawNonNull = true;
                sum += values.value(position);
            }
        }

        if (sawNonNull) {
            stateVector.increment(group, sum);
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        SumStateVector stateVector = (SumStateVector) state.values();
        I64Vector groupVector = (I64Vector) groups;
        Vector inputValues = streams.values(inputColumn);
        Vector inputNulls = streams.stream(inputColumn, Stream.NULLS);

        if (tryAccumulateNullFreeGrouped(stateVector, groupVector, inputValues, inputNulls, mask)) {
            return;
        }

        VectorAccess.LongValues values = VectorAccess.longValues(inputValues);
        VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(inputNulls);
        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                if (nulls.value(position)) {
                    continue;
                }
                int group = toIntExact(groupVector.values()[position]);
                stateVector.increment(group, values.value(position));
            }
        }
        else {
            for (int position : mask) {
                if (nulls.value(position)) {
                    continue;
                }
                int group = toIntExact(groupVector.values()[position]);
                stateVector.increment(group, values.value(position));
            }
        }
    }

    private static boolean tryAccumulateNullFreeGlobal(SumStateVector stateVector, int group, Vector inputValues, Vector inputNulls, Mask mask)
    {
        if (!isGuaranteedNotNull(inputNulls)) {
            return false;
        }

        switch (inputValues) {
            case I64Vector values -> {
                long sum = 0;
                if (mask.all()) {
                    for (int position = 0; position <= mask.maxPosition(); position++) {
                        sum += values.values()[position];
                    }
                }
                else {
                    for (int position : mask) {
                        sum += values.values()[position];
                    }
                }
                stateVector.increment(group, sum);
                return true;
            }
            case I32Vector values -> {
                long sum = 0;
                if (mask.all()) {
                    for (int position = 0; position <= mask.maxPosition(); position++) {
                        sum += values.values()[position];
                    }
                }
                else {
                    for (int position : mask) {
                        sum += values.values()[position];
                    }
                }
                stateVector.increment(group, sum);
                return true;
            }
            case DictionaryVector values when values.values() instanceof I64Vector dictionaryValues -> {
                long sum = sumDictionary(dictionaryValues.values(), values.ids(), mask);
                stateVector.increment(group, sum);
                return true;
            }
            case DictionaryVector values when values.values() instanceof I32Vector dictionaryValues -> {
                long sum = sumDictionary(dictionaryValues.values(), values.ids(), mask);
                stateVector.increment(group, sum);
                return true;
            }
            case RleVector values when values.values() instanceof I64Vector scalar && values.counts().length == 1 -> {
                stateVector.increment(group, scalar.values()[0] * mask.size());
                return true;
            }
            case RleVector values when values.values() instanceof I32Vector scalar && values.counts().length == 1 -> {
                stateVector.increment(group, (long) scalar.values()[0] * mask.size());
                return true;
            }
            default -> {
                return false;
            }
        }
    }

    private static boolean tryAccumulateNullFreeGrouped(SumStateVector stateVector, I64Vector groupVector, Vector inputValues, Vector inputNulls, Mask mask)
    {
        if (!isGuaranteedNotNull(inputNulls)) {
            return false;
        }

        switch (inputValues) {
            case I64Vector values -> {
                if (mask.all()) {
                    for (int position = 0; position <= mask.maxPosition(); position++) {
                        int group = toIntExact(groupVector.values()[position]);
                        stateVector.increment(group, values.values()[position]);
                    }
                }
                else {
                    for (int position : mask) {
                        int group = toIntExact(groupVector.values()[position]);
                        stateVector.increment(group, values.values()[position]);
                    }
                }
                return true;
            }
            case I32Vector values -> {
                if (mask.all()) {
                    for (int position = 0; position <= mask.maxPosition(); position++) {
                        int group = toIntExact(groupVector.values()[position]);
                        stateVector.increment(group, values.values()[position]);
                    }
                }
                else {
                    for (int position : mask) {
                        int group = toIntExact(groupVector.values()[position]);
                        stateVector.increment(group, values.values()[position]);
                    }
                }
                return true;
            }
            case DictionaryVector values when values.values() instanceof I64Vector dictionaryValues -> {
                accumulateDictionary(stateVector, groupVector.values(), dictionaryValues.values(), values.ids(), mask);
                return true;
            }
            case DictionaryVector values when values.values() instanceof I32Vector dictionaryValues -> {
                accumulateDictionary(stateVector, groupVector.values(), dictionaryValues.values(), values.ids(), mask);
                return true;
            }
            default -> {
                return false;
            }
        }
    }

    private static boolean isGuaranteedNotNull(Vector nulls)
    {
        return switch (nulls) {
            case null -> true;
            case BooleanVector vector when vector.length() == 1 -> !vector.values()[0];
            case RleVector vector when vector.values() instanceof BooleanVector values && vector.counts().length == 1 -> !values.values()[0];
            default -> false;
        };
    }

    private static long sumDictionary(long[] dictionaryValues, int[] ids, Mask mask)
    {
        if (dictionaryValues.length == 2 && dictionaryValues[0] == 0 && dictionaryValues[1] == 1) {
            long sum = 0;
            if (mask.all()) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    sum += ids[position];
                }
            }
            else {
                for (int position : mask) {
                    sum += ids[position];
                }
            }
            return sum;
        }

        long sum = 0;
        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                sum += dictionaryValues[ids[position]];
            }
        }
        else {
            for (int position : mask) {
                sum += dictionaryValues[ids[position]];
            }
        }
        return sum;
    }

    private static long sumDictionary(int[] dictionaryValues, int[] ids, Mask mask)
    {
        if (dictionaryValues.length == 2 && dictionaryValues[0] == 0 && dictionaryValues[1] == 1) {
            long sum = 0;
            if (mask.all()) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    sum += ids[position];
                }
            }
            else {
                for (int position : mask) {
                    sum += ids[position];
                }
            }
            return sum;
        }

        long sum = 0;
        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                sum += dictionaryValues[ids[position]];
            }
        }
        else {
            for (int position : mask) {
                sum += dictionaryValues[ids[position]];
            }
        }
        return sum;
    }

    private static void accumulateDictionary(SumStateVector stateVector, long[] groups, long[] dictionaryValues, int[] ids, Mask mask)
    {
        if (dictionaryValues.length == 2 && dictionaryValues[0] == 0 && dictionaryValues[1] == 1) {
            if (mask.all()) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    if (ids[position] != 0) {
                        stateVector.increment(toIntExact(groups[position]), 1);
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (ids[position] != 0) {
                        stateVector.increment(toIntExact(groups[position]), 1);
                    }
                }
            }
            return;
        }

        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                stateVector.increment(toIntExact(groups[position]), dictionaryValues[ids[position]]);
            }
        }
        else {
            for (int position : mask) {
                stateVector.increment(toIntExact(groups[position]), dictionaryValues[ids[position]]);
            }
        }
    }

    private static void accumulateDictionary(SumStateVector stateVector, long[] groups, int[] dictionaryValues, int[] ids, Mask mask)
    {
        if (dictionaryValues.length == 2 && dictionaryValues[0] == 0 && dictionaryValues[1] == 1) {
            if (mask.all()) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    if (ids[position] != 0) {
                        stateVector.increment(toIntExact(groups[position]), 1);
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (ids[position] != 0) {
                        stateVector.increment(toIntExact(groups[position]), 1);
                    }
                }
            }
            return;
        }

        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                stateVector.increment(toIntExact(groups[position]), dictionaryValues[ids[position]]);
            }
        }
        else {
            for (int position : mask) {
                stateVector.increment(toIntExact(groups[position]), dictionaryValues[ids[position]]);
            }
        }
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        SumStateVector stateVector = (SumStateVector) state.values();
        int visibleCount = Math.max(maxGroup + 1, 0);
        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (I64Vector) output.values(),
                I64Vector.class,
                visibleCount,
                I64Vector::new);
        stateVector.copySumsTo(values, visibleCount);
        // When no group has remained null (every assigned group has received at least one non-null
        // input), emit NULLS as a 1-run RLE of false — O(1) rather than allocating a flat boolean
        // vector and copying the state's null chunks. Downstream scalar functions that propagate
        // NULLS recognise the all-false RLE shape (via {@link VectorAccess#isAllFalseNulls}) and
        // short-circuit their per-row null-or loop.
        if (!stateVector.hasAnyNull()) {
            BooleanVector sentinel = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
            Vector nulls = allocator.allocateSingleRunRle(allocationContext, visibleCount, sentinel);
            return Streams.ofValues(values).with(Stream.NULLS, nulls);
        }
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                visibleCount);
        stateVector.copyNullsTo(nulls, visibleCount);
        return Streams.reuseValuesAndNulls(output, values, nulls);
    }

    @Override
    public Streams copyResultPosition(int group, int maxGroup, Streams state, Streams output, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        SumStateVector stateVector = (SumStateVector) state.values();
        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (I64Vector) output.getOrNull(Stream.VALUES),
                I64Vector.class,
                size,
                I64Vector::new);
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                size);
        values.values()[outputPosition] = stateVector.sum(group);
        nulls.values()[outputPosition] = stateVector.isNull(group);
        return Streams.reuseValuesAndNulls(output, values, nulls);
    }
}
