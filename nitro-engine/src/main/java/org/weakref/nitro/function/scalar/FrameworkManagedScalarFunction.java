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
package org.weakref.nitro.function.scalar;

import org.weakref.nitro.core.function.BoundSignature;
import org.weakref.nitro.core.function.ScalarResultWriter;
import org.weakref.nitro.core.function.ScalarResultWriterFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.FlatVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.data.VectorAllocator;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

final class FrameworkManagedScalarFunction
        implements PrimitiveFunction
{
    private final String name;
    private final BoundSignature signature;
    private final GeneratedScalarKernel kernel;
    private final ScalarResultWriterFactory resultWriterFactory;
    private final boolean mayFail;

    FrameworkManagedScalarFunction(
            String name,
            BoundSignature signature,
            GeneratedScalarKernel kernel,
            ScalarResultWriterFactory resultWriterFactory,
            boolean mayFail)
    {
        this.name = requireNonNull(name, "name is null");
        this.signature = requireNonNull(signature, "signature is null");
        this.kernel = requireNonNull(kernel, "kernel is null");
        this.resultWriterFactory = resultWriterFactory;
        this.mayFail = mayFail;
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        checkArgument(inputIndex >= 0 && inputIndex < signature.argumentTypes().size(), "Unexpected argument index: %s", inputIndex);
        return PrimitiveFunction.inputStreams(
                requestedOutputStreams.contains(Stream.VALUES),
                requestedOutputStreams.contains(Stream.NULLS) ||
                        (mayFail && requestedOutputStreams.contains(Stream.VALUES)),
                mayFail && requestedOutputStreams.contains(Stream.VALUES));
    }

    @Override
    public boolean propagatesNulls()
    {
        return true;
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == signature.argumentTypes().size(), "Unexpected argument count for %s", name);
        boolean requestValues = requestedStreams.contains(Stream.VALUES);
        boolean requestNulls = requestedStreams.contains(Stream.NULLS);
        if (!requestValues && !requestNulls) {
            return Streams.empty();
        }

        int requiredLength = mask.size();
        for (Streams input : inputs) {
            for (Stream stream : input.streams()) {
                requiredLength = Math.max(requiredLength, input.get(stream).length());
            }
        }
        Allocator.Context allocationContext = context.allocationContext(name);
        InvocationState state = context.state(this, () -> new InvocationState(inputs.size()));
        bindNulls(inputs, state, requestNulls || mayFail);
        bindExecutionBlockers(inputs, state);

        Streams result = Streams.empty();
        if (requestNulls) {
            BooleanVector nulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null ? output.getOrNull(Stream.NULLS) : null,
                    requiredLength);
            combineNulls(state.nulls, mask, nulls.values());
            result = result.with(Stream.NULLS, nulls);
        }
        if (!requestValues) {
            return result;
        }
        state.initializeResult(signature, resultWriterFactory, context.allocator(), allocationContext);

        if (mask.all() &&
                (output == null || !output.has(Stream.VALUES)) &&
                bindRleValuesIfNullFree(inputs, state, mask.size())) {
            int runCount = mergeRuns(inputs, state);
            Vector runValues;
            if (state.resultWriter == null) {
                runValues = writablePrimitiveResult(context.allocator(), allocationContext, null, runCount);
                kernel.applyDenseDictionaryNullFree(state.flatValues, state.rleIds, valueArray(runValues), runCount);
            }
            else {
                try {
                    state.resultWriter.begin(state.vectorAllocator, null, runCount, Mask.all(runCount));
                    kernel.applyDenseDictionaryNullFree(state.flatValues, state.rleIds, state.resultWriter, runCount);
                    runValues = state.resultWriter.finish();
                }
                catch (RuntimeException | Error failure) {
                    state.resultWriter.abort();
                    throw failure;
                }
            }
            return result.with(Stream.VALUES, context.allocator().allocateRle(
                    allocationContext,
                    state.rleCounts,
                    runCount,
                    runValues));
        }

        Vector values = state.resultWriter == null
                ? writablePrimitiveResult(context.allocator(), allocationContext, output, requiredLength)
                : null;
        Object kernelOutput = values == null ? state.resultWriter : valueArray(values);
        try {
            if (state.resultWriter != null) {
                Vector proposed = output != null ? output.getOrNull(Stream.VALUES) : null;
                state.resultWriter.begin(state.vectorAllocator, proposed, requiredLength, mask);
            }
            applyKernel(inputs, state, mask, kernelOutput);
            if (state.resultWriter != null) {
                values = state.resultWriter.finish();
            }
        }
        catch (RuntimeException | Error failure) {
            if (state.resultWriter != null) {
                state.resultWriter.abort();
            }
            throw failure;
        }
        return result.with(Stream.VALUES, values);
    }

    private void applyKernel(List<Streams> inputs, InvocationState state, Mask mask, Object output)
    {
        int[] positions = mask.selectedPositions();
        if (bindFlatValuesIfNullFree(inputs, state)) {
            if (positions == null) {
                kernel.applyDenseFlatNullFree(state.flatValues, output, mask.count());
            }
            else {
                kernel.applySparseFlatNullFree(state.flatValues, output, positions, mask.count());
            }
        }
        else if (bindDictionaryValuesIfNullFree(inputs, state)) {
            if (positions == null) {
                kernel.applyDenseDictionaryNullFree(state.flatValues, state.dictionaryIds, output, mask.count());
            }
            else {
                kernel.applySparseDictionaryNullFree(state.flatValues, state.dictionaryIds, output, positions, mask.count());
            }
        }
        else {
            bindValues(inputs, state);
            if (positions == null) {
                kernel.applyDense(state.values, state.executionBlockers, output, mask.count());
            }
            else {
                kernel.applySparse(state.values, state.executionBlockers, output, positions, mask.count());
            }
        }
    }

    private boolean bindFlatValuesIfNullFree(List<Streams> inputs, InvocationState state)
    {
        for (int index = 0; index < inputs.size(); index++) {
            if (state.executionBlockers[index] != null) {
                return false;
            }
            Vector values = inputs.get(index).values();
            Class<?> carrier = signature.argumentTypes().get(index).carrierType();
            if (carrier == long.class && values instanceof I64Vector vector) {
                state.flatValues[index] = vector.values();
            }
            else if (carrier == double.class && values instanceof F64Vector vector) {
                state.flatValues[index] = vector.values();
            }
            else if (carrier == boolean.class && values instanceof BooleanVector vector) {
                state.flatValues[index] = vector.values();
            }
            else if (!carrier.isPrimitive() && values instanceof FlatVector) {
                state.flatValues[index] = values;
            }
            else {
                return false;
            }
        }
        return true;
    }

    private boolean bindRleValuesIfNullFree(List<Streams> inputs, InvocationState state, int logicalLength)
    {
        if (inputs.isEmpty()) {
            return false;
        }
        for (int index = 0; index < inputs.size(); index++) {
            if (state.executionBlockers[index] != null ||
                    !(inputs.get(index).values() instanceof RleVector rle) ||
                    rle.length() != logicalLength) {
                return false;
            }
            Object values = physicalInput(signature.argumentTypes().get(index).carrierType(), rle.values());
            if (values == null) {
                return false;
            }
            state.flatValues[index] = values;
        }
        return true;
    }

    private static int mergeRuns(List<Streams> inputs, InvocationState state)
    {
        int maximumRunCount = 1;
        int logicalLength = -1;
        for (Streams input : inputs) {
            RleVector rle = (RleVector) input.values();
            if (logicalLength < 0) {
                logicalLength = rle.length();
            }
            else {
                checkArgument(rle.length() == logicalLength, "RLE argument lengths do not match");
            }
            maximumRunCount += rle.counts().length - 1;
        }
        state.ensureRleCapacity(maximumRunCount);

        int runCount = 0;
        while (true) {
            int count = Integer.MAX_VALUE;
            for (int argument = 0; argument < inputs.size(); argument++) {
                RleVector rle = (RleVector) inputs.get(argument).values();
                int runIndex = state.rleRunIndices[argument];
                if (runIndex == rle.counts().length) {
                    return runCount;
                }
                if (state.rleRemaining[argument] == 0) {
                    state.rleRemaining[argument] = rle.counts()[runIndex];
                }
                count = Math.min(count, state.rleRemaining[argument]);
                state.rleIds[argument][runCount] = runIndex;
            }

            state.rleCounts[runCount] = count;
            runCount++;
            for (int argument = 0; argument < inputs.size(); argument++) {
                state.rleRemaining[argument] -= count;
                if (state.rleRemaining[argument] == 0) {
                    state.rleRunIndices[argument]++;
                }
            }
        }
    }

    private boolean bindDictionaryValuesIfNullFree(List<Streams> inputs, InvocationState state)
    {
        for (int index = 0; index < inputs.size(); index++) {
            if (state.executionBlockers[index] != null || !(inputs.get(index).values() instanceof DictionaryVector dictionary)) {
                return false;
            }
            Vector values = dictionary.values();
            Class<?> carrier = signature.argumentTypes().get(index).carrierType();
            if (carrier == long.class && values instanceof I64Vector vector) {
                state.flatValues[index] = vector.values();
            }
            else if (carrier == double.class && values instanceof F64Vector vector) {
                state.flatValues[index] = vector.values();
            }
            else if (carrier == boolean.class && values instanceof BooleanVector vector) {
                state.flatValues[index] = vector.values();
            }
            else if (!carrier.isPrimitive() && values instanceof FlatVector) {
                state.flatValues[index] = values;
            }
            else {
                return false;
            }
            state.dictionaryIds[index] = dictionary.ids();
        }
        return true;
    }

    private void bindValues(List<Streams> inputs, InvocationState state)
    {
        for (int index = 0; index < inputs.size(); index++) {
            state.values[index] = valueAccessor(signature.argumentTypes().get(index).carrierType(), inputs.get(index).values());
        }
    }

    private static void bindNulls(List<Streams> inputs, InvocationState state, boolean required)
    {
        for (int index = 0; index < inputs.size(); index++) {
            if (!required) {
                state.nulls[index] = null;
                continue;
            }
            Streams input = inputs.get(index);
            Vector nulls = input.getOrNull(Stream.NULLS);
            state.nulls[index] = VectorAccess.isAllFalseNulls(nulls) ? null : VectorAccess.booleanValues(nulls);
        }
    }

    private static void bindExecutionBlockers(List<Streams> inputs, InvocationState state)
    {
        for (int index = 0; index < inputs.size(); index++) {
            Vector errors = inputs.get(index).getOrNull(Stream.ERRORS);
            VectorAccess.BooleanValues errorValues = VectorAccess.isAllFalseNulls(errors)
                    ? null
                    : VectorAccess.booleanValues(errors);
            VectorAccess.BooleanValues nullValues = (VectorAccess.BooleanValues) state.nulls[index];
            if (nullValues == null && errorValues == null) {
                state.executionBlockers[index] = null;
                continue;
            }
            InputBlocker blocker = state.inputBlockers[index];
            blocker.bind(nullValues, errorValues);
            state.executionBlockers[index] = blocker;
        }
    }

    private Vector writablePrimitiveResult(Allocator allocator, Allocator.Context allocationContext, Streams output, int length)
    {
        Vector proposed = output != null ? output.getOrNull(Stream.VALUES) : null;
        Class<?> carrier = signature.resultType().carrierType();
        if (carrier == long.class) {
            return allocator.allocateOrGrow(
                    allocationContext,
                    proposed instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    length,
                    I64Vector::new);
        }
        if (carrier == double.class) {
            return allocator.allocateOrGrow(
                    allocationContext,
                    proposed instanceof F64Vector vector ? vector : null,
                    F64Vector.class,
                    length,
                    F64Vector::new);
        }
        if (carrier == boolean.class) {
            return VectorAccess.writableBooleanVector(allocator, allocationContext, proposed, length);
        }
        throw new IllegalArgumentException("Unsupported framework-managed result carrier: " + carrier.getTypeName());
    }

    private static Object valueAccessor(Class<?> carrier, Vector values)
    {
        if (carrier == long.class) {
            return VectorAccess.longValues(values);
        }
        if (carrier == double.class) {
            return VectorAccess.doubleValues(values);
        }
        if (carrier == boolean.class) {
            return VectorAccess.booleanValues(values);
        }
        if (!carrier.isPrimitive()) {
            return values;
        }
        throw new IllegalArgumentException("Unsupported framework-managed argument carrier: " + carrier.getTypeName());
    }

    private static Object physicalInput(Class<?> carrier, Vector values)
    {
        if (carrier == long.class && values instanceof I64Vector vector) {
            return vector.values();
        }
        if (carrier == double.class && values instanceof F64Vector vector) {
            return vector.values();
        }
        if (carrier == boolean.class && values instanceof BooleanVector vector) {
            return vector.values();
        }
        if (!carrier.isPrimitive() && values instanceof FlatVector) {
            return values;
        }
        return null;
    }

    private static Object valueArray(Vector vector)
    {
        return switch (vector) {
            case I64Vector values -> values.values();
            case F64Vector values -> values.values();
            case BooleanVector values -> values.values();
            default -> throw new IllegalArgumentException("Unsupported framework-managed result vector: " + vector.getClass().getSimpleName());
        };
    }

    private static void combineNulls(Object[] inputs, Mask mask, boolean[] output)
    {
        for (int position : mask) {
            boolean value = false;
            for (Object input : inputs) {
                if (input != null && ((VectorAccess.BooleanValues) input).value(position)) {
                    value = true;
                    break;
                }
            }
            output[position] = value;
        }
    }

    private static final class InvocationState
    {
        private final Object[] values;
        private final Object[] flatValues;
        private final int[][] dictionaryIds;
        private final Object[] nulls;
        private final Object[] executionBlockers;
        private final InputBlocker[] inputBlockers;
        private final int[] rleRunIndices;
        private final int[] rleRemaining;
        private int[] rleCounts = new int[0];
        private final int[][] rleIds;
        private VectorAllocator vectorAllocator;
        private ScalarResultWriter resultWriter;

        private InvocationState(int arity)
        {
            values = new Object[arity];
            flatValues = new Object[arity];
            dictionaryIds = new int[arity][];
            nulls = new Object[arity];
            executionBlockers = new Object[arity];
            inputBlockers = new InputBlocker[arity];
            for (int index = 0; index < arity; index++) {
                inputBlockers[index] = new InputBlocker();
            }
            rleRunIndices = new int[arity];
            rleRemaining = new int[arity];
            rleIds = new int[arity][];
        }

        private void initializeResult(
                BoundSignature signature,
                ScalarResultWriterFactory resultWriterFactory,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            if (signature.resultType().carrierType().isPrimitive() || resultWriter != null) {
                return;
            }
            vectorAllocator = allocator.vectorAllocator(allocationContext);
            resultWriter = requireNonNull(resultWriterFactory, "resultWriterFactory is null").createWriter();
        }

        private void ensureRleCapacity(int capacity)
        {
            if (rleCounts.length < capacity) {
                rleCounts = new int[capacity];
            }
            for (int argument = 0; argument < rleIds.length; argument++) {
                if (rleIds[argument] == null || rleIds[argument].length < capacity) {
                    rleIds[argument] = new int[capacity];
                }
                rleRunIndices[argument] = 0;
                rleRemaining[argument] = 0;
            }
        }
    }

    private static final class InputBlocker
            implements VectorAccess.BooleanValues
    {
        private VectorAccess.BooleanValues nulls;
        private VectorAccess.BooleanValues errors;

        private void bind(VectorAccess.BooleanValues nulls, VectorAccess.BooleanValues errors)
        {
            this.nulls = nulls;
            this.errors = errors;
        }

        @Override
        public boolean value(int position)
        {
            return (nulls != null && nulls.value(position)) ||
                    (errors != null && errors.value(position));
        }
    }
}
