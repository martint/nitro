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
package org.weakref.nitro.operator.evaluator;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.AndMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.Copy;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.MaskExpression;
import org.weakref.nitro.operator.evaluator.ir.MemoizationPolicy;
import org.weakref.nitro.operator.evaluator.ir.Merge;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.OrMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.StreamPlan;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public final class PlanEvaluator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("PlanEvaluator");

    private final EvaluationPlan plan;
    private final PrimitiveRegistry primitiveRegistry;
    private final InputResolver input;
    private final Allocator allocator;
    private final PrimitiveExecutionContext executionContext;
    private final Map<Variable, Assignment> assignments;
    private final Map<Reference, Streams> memoizedStreams = new HashMap<>();
    private final Map<Reference, Mask> memoizedMasks = new HashMap<>();

    @FunctionalInterface
    public interface InputResolver
    {
        Vector get(int index, Mask mask);
    }

    public PlanEvaluator(EvaluationPlan plan, PrimitiveRegistry primitiveRegistry, InputResolver input, Allocator allocator)
    {
        this.plan = plan;
        this.primitiveRegistry = primitiveRegistry;
        this.input = input;
        this.allocator = allocator;
        this.executionContext = new PrimitiveExecutionContext(allocator);
        this.assignments = indexAssignments(plan.assignments());
    }

    public Streams evaluate(Reference reference, Mask mask)
    {
        if (mask.none()) {
            return Streams.empty();
        }

        StreamPlan streamPlan = plan.streamPlans().get(reference);
        if (streamPlan != null && streamPlan.memoizationPolicy() == MemoizationPolicy.MEMOIZE) {
            Streams existingOutput = memoizedStreams.get(reference);
            Mask existingMask = memoizedMasks.get(reference);
            if (existingOutput != null && existingMask != null && existingMask.containsAll(mask)) {
                return existingOutput;
            }

            Mask remaining = existingMask == null ? mask : mask.difference(existingMask);
            if (remaining.none()) {
                return existingOutput;
            }

            Streams updated = evaluateUnmemoized(reference, remaining, existingOutput);
            memoizedStreams.put(reference, updated);
            memoizedMasks.put(reference, existingMask == null ? remaining : existingMask.or(remaining));
            return updated;
        }

        return evaluateUnmemoized(reference, mask, null);
    }

    public void reset()
    {
        memoizedMasks.clear();
        memoizedStreams.clear();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private Streams evaluateUnmemoized(Reference reference, Mask mask, Streams output)
    {
        return switch (reference.producer()) {
            case org.weakref.nitro.operator.evaluator.ir.Input(int index) -> evaluateInput(reference, index, mask);
            case Variable variable -> evaluateVariable(reference, variable, mask, output);
        };
    }

    private Streams evaluateInput(Reference reference, int inputIndex, Mask mask)
    {
        checkArgument(reference.stream() == Stream.VALUES, "Input streams currently support only VALUES: %s", reference);
        return Streams.ofValues(input.get(inputIndex, mask));
    }

    private Streams evaluateVariable(Reference reference, Variable variable, Mask mask, Streams output)
    {
        Assignment assignment = assignments.get(variable);
        checkArgument(assignment != null, "Unknown variable: %s", variable);

        return switch (assignment.operation()) {
            case Literal literal -> evaluateLiteral(reference, literal, mask);
            case Copy(Reference source) -> copy(reference.stream(), source, mask, output);
            case Call call -> evaluateCall(reference.stream(), call, mask, output);
            case Merge merge -> evaluateMerge(reference.stream(), merge, mask, output);
        };
    }

    private Streams evaluateLiteral(Reference reference, Literal literal, Mask mask)
    {
        checkArgument(reference.stream() == Stream.VALUES, "Literal currently supports only VALUES: %s", reference);
        int length = mask.maxPosition() + 1;
        return switch (literal.value()) {
            case Long value -> Streams.ofValues(fillLong(value, length));
            case Boolean value -> Streams.of(Stream.VALUES, fillBoolean(value, length));
            default -> throw new IllegalArgumentException("Unsupported literal value: " + literal.value());
        };
    }

    private Streams evaluateCall(Stream stream, Call call, Mask mask, Streams output)
    {
        PrimitiveFunction function = primitiveRegistry.get(call.name());
        List<Streams> inputs = call.arguments().stream()
                .map(argument -> evaluate(argument, mask))
                .toList();
        return function.apply(inputs, mask, prepareOutput(output), executionContext);
    }

    private Streams copy(Stream targetStream, Reference source, Mask mask, Streams output)
    {
        Streams sourceStreams = evaluate(source, mask);
        Vector sourceVector = sourceStreams.get(source.stream());

        if (targetStream != source.stream()) {
            throw new IllegalArgumentException("Cross-stream copy is not yet supported: " + source + " -> " + targetStream);
        }

        if (output == null) {
            return Streams.of(targetStream, sourceVector);
        }

        Vector target = output.has(targetStream) ? output.get(targetStream) : null;
        target = copyVector(sourceVector, target, mask);
        return Streams.of(targetStream, target);
    }

    private Streams evaluateMerge(Stream stream, Merge merge, Mask mask, Streams output)
    {
        BooleanVector condition = (BooleanVector) evaluateMaskReference(merge.condition(), mask).values();
        Mask trueMask = mask.and(condition);
        Mask falseMask = mask.andNot(condition);

        Streams result = prepareOutput(output);
        if (!trueMask.none()) {
            result = copy(stream, merge.whenTrue(), trueMask, result);
        }
        if (!falseMask.none()) {
            result = copy(stream, merge.whenFalse(), falseMask, result);
        }
        return result;
    }

    private Streams evaluateMaskReference(MaskExpression condition, Mask mask)
    {
        Mask conditionMask = evaluateMask(condition, mask);
        BooleanVector values = new BooleanVector(mask.maxPosition() + 1);
        for (int position : conditionMask) {
            values.values()[position] = true;
        }
        return Streams.of(Stream.VALUES, values);
    }

    private Mask evaluateMask(MaskExpression expression, Mask mask)
    {
        return switch (expression) {
            case AllMask _ -> mask;
            case ReferenceMask(Reference reference) -> mask.and((BooleanVector) evaluate(reference, mask).get(reference.stream()));
            case NotMask(MaskExpression source) -> mask.andNot(toBooleanVector(evaluateMask(source, mask), mask.maxPosition() + 1));
            case AndMask(MaskExpression left, MaskExpression right) -> evaluateMask(left, mask).and(toBooleanVector(evaluateMask(right, mask), mask.maxPosition() + 1));
            case OrMask(MaskExpression left, MaskExpression right) -> evaluateMask(left, mask).or(evaluateMask(right, mask));
        };
    }

    private BooleanVector toBooleanVector(Mask mask, int length)
    {
        BooleanVector result = new BooleanVector(length);
        for (int position : mask) {
            result.values()[position] = true;
        }
        return result;
    }

    private Streams prepareOutput(Streams output)
    {
        return output == null ? Streams.empty() : output;
    }

    private Vector copyVector(Vector source, Vector existing, Mask mask)
    {
        return switch (source) {
            case I64Vector sourceValues -> copyLongVector(sourceValues, existing, mask);
            case BooleanVector sourceValues -> copyBooleanVector(sourceValues, existing, mask);
            case RleVector sourceValues -> copyRleVector(sourceValues, existing, mask);
            default -> throw new IllegalArgumentException("Unsupported vector type for copy: " + source.getClass().getSimpleName());
        };
    }

    private Vector copyLongVector(I64Vector source, Vector existing, Mask mask)
    {
        I64Vector target = (I64Vector) allocator.allocateOrGrow(ALLOCATION_CONTEXT, existing, source.length(), I64Vector::new);
        for (int position : mask) {
            target.values()[position] = source.values()[position];
        }
        return target;
    }

    private Vector copyBooleanVector(BooleanVector source, Vector existing, Mask mask)
    {
        BooleanVector target = (BooleanVector) allocator.allocateOrGrow(ALLOCATION_CONTEXT, existing, source.length(), BooleanVector::new);
        for (int position : mask) {
            target.values()[position] = source.values()[position];
        }
        return target;
    }

    private Vector copyRleVector(RleVector source, Vector existing, Mask mask)
    {
        return switch (source.values()) {
            case I64Vector values -> copyLongRleVector(source.counts(), values.values(), existing, mask, source.length());
            case BooleanVector values -> copyBooleanRleVector(source.counts(), values.values(), existing, mask, source.length());
            default -> throw new IllegalArgumentException("Unsupported RLE value type for copy: " + source.values().getClass().getSimpleName());
        };
    }

    private Vector copyLongRleVector(int[] counts, long[] values, Vector existing, Mask mask, int length)
    {
        I64Vector target = (I64Vector) allocator.allocateOrGrow(ALLOCATION_CONTEXT, existing, length, I64Vector::new);
        int runIndex = 0;
        int runEnd = counts[0];
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += counts[runIndex];
            }
            target.values()[position] = values[runIndex];
        }
        return target;
    }

    private Vector copyBooleanRleVector(int[] counts, boolean[] values, Vector existing, Mask mask, int length)
    {
        BooleanVector target = (BooleanVector) allocator.allocateOrGrow(ALLOCATION_CONTEXT, existing, length, BooleanVector::new);
        int runIndex = 0;
        int runEnd = counts[0];
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += counts[runIndex];
            }
            target.values()[position] = values[runIndex];
        }
        return target;
    }

    private Vector fillLong(long value, int length)
    {
        I64Vector result = new I64Vector(length);
        for (int position = 0; position < length; position++) {
            result.values()[position] = value;
        }
        return result;
    }

    private Vector fillBoolean(boolean value, int length)
    {
        BooleanVector result = new BooleanVector(length);
        for (int position = 0; position < length; position++) {
            result.values()[position] = value;
        }
        return result;
    }

    private static Map<Variable, Assignment> indexAssignments(List<Assignment> assignments)
    {
        Map<Variable, Assignment> indexedAssignments = new HashMap<>();
        for (Assignment assignment : assignments) {
            indexedAssignments.put(assignment.output(), assignment);
        }
        return indexedAssignments;
    }
}
