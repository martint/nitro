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

import java.util.ArrayList;
import java.util.Comparator;
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
    private final Map<MaskExpression, MaskTermStats> maskTermStats = new HashMap<>();

    @FunctionalInterface
    public interface InputResolver
    {
        Vector resolve(Reference reference, Mask mask);
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

            Mask remaining = existingMask == null ? mask : allocator.differenceMask(ALLOCATION_CONTEXT, mask, existingMask);
            if (remaining.none()) {
                return existingOutput;
            }

            Streams updated = evaluateUnmemoized(reference, remaining, existingOutput);
            Mask updatedMask = existingMask == null ? remaining : allocator.unionMask(ALLOCATION_CONTEXT, existingMask, remaining);
            memoizeStreams(reference, updated, updatedMask);
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
        return Streams.of(reference.stream(), input.resolve(new Reference(new org.weakref.nitro.operator.evaluator.ir.Input(inputIndex), reference.stream()), mask));
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
        Mask trueMask = evaluateMaskOutcome(merge.condition(), mask).trueMask();
        Mask falseMask = allocator.differenceMask(ALLOCATION_CONTEXT, mask, trueMask);

        Streams result = prepareOutput(output);
        if (!trueMask.none()) {
            result = copy(stream, merge.whenTrue(), trueMask, result);
        }
        if (!falseMask.none()) {
            result = copy(stream, merge.whenFalse(), falseMask, result);
        }
        return result;
    }

    private Mask evaluateMask(MaskExpression expression, Mask mask)
    {
        return evaluateMaskOutcome(expression, mask).trueMask();
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
        I64Vector target = allocator.allocateOrGrow(ALLOCATION_CONTEXT, (I64Vector) existing, I64Vector.class, source.length(), I64Vector::new);
        for (int position : mask) {
            target.values()[position] = source.values()[position];
        }
        return target;
    }

    private Vector copyBooleanVector(BooleanVector source, Vector existing, Mask mask)
    {
        BooleanVector target = allocator.allocateOrGrow(ALLOCATION_CONTEXT, (BooleanVector) existing, BooleanVector.class, source.length(), BooleanVector::new);
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
        I64Vector target = allocator.allocateOrGrow(ALLOCATION_CONTEXT, (I64Vector) existing, I64Vector.class, length, I64Vector::new);
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
        BooleanVector target = allocator.allocateOrGrow(ALLOCATION_CONTEXT, (BooleanVector) existing, BooleanVector.class, length, BooleanVector::new);
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
        I64Vector result = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, length, I64Vector::new);
        for (int position = 0; position < length; position++) {
            result.values()[position] = value;
        }
        return result;
    }

    private Vector fillBoolean(boolean value, int length)
    {
        BooleanVector result = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, length, BooleanVector::new);
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

    private void memoizeStreams(Reference reference, Streams streams, Mask mask)
    {
        for (Stream stream : streams.asMap().keySet()) {
            Reference streamReference = new Reference(reference.producer(), stream);
            StreamPlan streamPlan = plan.streamPlans().get(streamReference);
            if (streamPlan != null && streamPlan.memoizationPolicy() == MemoizationPolicy.MEMOIZE) {
                memoizedStreams.put(streamReference, streams);
                memoizedMasks.put(streamReference, mask);
            }
        }
    }

    private MaskOutcome evaluateMaskOutcome(MaskExpression expression, Mask mask)
    {
        return switch (expression) {
            case AllMask _ -> new MaskOutcome(mask, emptyMask(mask.size()), emptyMask(mask.size()));
            case ReferenceMask(Reference reference) -> evaluateReferenceMask(reference, mask);
            case NotMask(MaskExpression source) -> {
                MaskOutcome sourceOutcome = evaluateMaskOutcome(source, mask);
                yield new MaskOutcome(
                        sourceOutcome.falseMask(allocator, ALLOCATION_CONTEXT, mask),
                        sourceOutcome.nullMask(),
                        sourceOutcome.errorMask());
            }
            case AndMask(List<MaskExpression> terms) -> evaluateAdaptiveAnd(terms, mask);
            case OrMask(List<MaskExpression> terms) -> evaluateAdaptiveOr(terms, mask);
        };
    }

    private MaskOutcome evaluateReferenceMask(Reference reference, Mask mask)
    {
        BooleanVector values = (BooleanVector) evaluate(reference, mask).get(reference.stream());
        BooleanVector errors = optionalBooleanStream(reference.producer(), Stream.ERRORS, mask);
        BooleanVector nulls = optionalBooleanStream(reference.producer(), Stream.NULLS, mask);

        Mask errorMask = errors != null
                ? allocator.intersectMask(ALLOCATION_CONTEXT, mask, errors)
                : emptyMask(mask.size());

        Mask remainingAfterErrors = errorMask.none() ? mask : allocator.differenceMask(ALLOCATION_CONTEXT, mask, errorMask);
        Mask nullMask = nulls != null
                ? allocator.intersectMask(ALLOCATION_CONTEXT, remainingAfterErrors, nulls)
                : emptyMask(mask.size());

        Mask presentMask = nullMask.none() ? remainingAfterErrors : allocator.differenceMask(ALLOCATION_CONTEXT, remainingAfterErrors, nullMask);
        Mask trueMask = allocator.intersectMask(ALLOCATION_CONTEXT, presentMask, values);
        return new MaskOutcome(trueMask, nullMask, errorMask);
    }

    private BooleanVector optionalBooleanStream(org.weakref.nitro.operator.evaluator.ir.Producer producer, Stream stream, Mask mask)
    {
        try {
            Streams streams = evaluate(new Reference(producer, stream), mask);
            return streams.has(stream) ? (BooleanVector) streams.get(stream) : null;
        }
        catch (IllegalArgumentException _) {
            return null;
        }
    }

    private MaskOutcome evaluateAdaptiveAnd(List<MaskExpression> terms, Mask mask)
    {
        terms = orderTerms(terms, BooleanOperator.AND);

        Mask activeMask = mask;
        Mask nullMask = emptyMask(mask.size());
        Mask errorMask = emptyMask(mask.size());
        for (MaskExpression term : terms) {
            MaskOutcome termOutcome = evaluateMeasuredOutcome(term, activeMask, BooleanOperator.AND);
            Mask survivors = termOutcome.survivorsMask(allocator, ALLOCATION_CONTEXT);
            if (survivors.none()) {
                return new MaskOutcome(emptyMask(mask.size()), emptyMask(mask.size()), emptyMask(mask.size()));
            }

            Mask survivingNulls = nullMask.none() ? emptyMask(mask.size()) : allocator.intersectMask(ALLOCATION_CONTEXT, nullMask, survivors);
            Mask survivingErrors = errorMask.none() ? emptyMask(mask.size()) : allocator.intersectMask(ALLOCATION_CONTEXT, errorMask, survivors);
            Mask nextErrorMask = unionMasks(survivingErrors, termOutcome.errorMask(), mask.size());
            Mask nextNullMask = unionMasks(survivingNulls, termOutcome.nullMask(), mask.size());
            nextNullMask = nextErrorMask.none() ? nextNullMask : allocator.differenceMask(ALLOCATION_CONTEXT, nextNullMask, nextErrorMask);

            activeMask = survivors;
            nullMask = nextNullMask;
            errorMask = nextErrorMask;
        }
        Mask trueMask = subtractMasks(activeMask, nullMask, errorMask);
        return new MaskOutcome(trueMask, nullMask, errorMask);
    }

    private MaskOutcome evaluateAdaptiveOr(List<MaskExpression> terms, Mask mask)
    {
        terms = orderTerms(terms, BooleanOperator.OR);

        Mask acceptedMask = emptyMask(mask.size());
        Mask nullMask = emptyMask(mask.size());
        Mask errorMask = emptyMask(mask.size());
        Mask remainingMask = mask;
        for (MaskExpression term : terms) {
            if (remainingMask.none()) {
                break;
            }

            MaskOutcome termOutcome = evaluateMeasuredOutcome(term, remainingMask, BooleanOperator.OR);
            acceptedMask = unionMasks(acceptedMask, termOutcome.trueMask(), mask.size());
            remainingMask = termOutcome.trueMask().none() ? remainingMask : allocator.differenceMask(ALLOCATION_CONTEXT, remainingMask, termOutcome.trueMask());
            if (remainingMask.none()) {
                return new MaskOutcome(acceptedMask, emptyMask(mask.size()), emptyMask(mask.size()));
            }

            Mask survivingNulls = nullMask.none() ? emptyMask(mask.size()) : allocator.intersectMask(ALLOCATION_CONTEXT, nullMask, remainingMask);
            Mask survivingErrors = errorMask.none() ? emptyMask(mask.size()) : allocator.intersectMask(ALLOCATION_CONTEXT, errorMask, remainingMask);
            Mask nextErrorMask = unionMasks(survivingErrors, termOutcome.errorMask(), mask.size());
            Mask nextNullMask = unionMasks(survivingNulls, termOutcome.nullMask(), mask.size());
            nextNullMask = nextErrorMask.none() ? nextNullMask : allocator.differenceMask(ALLOCATION_CONTEXT, nextNullMask, nextErrorMask);
            nullMask = nextNullMask;
            errorMask = nextErrorMask;
        }

        return new MaskOutcome(acceptedMask, nullMask, errorMask);
    }

    private MaskOutcome evaluateMeasuredOutcome(MaskExpression term, Mask mask, BooleanOperator operator)
    {
        long start = System.nanoTime();
        MaskOutcome result = evaluateMaskOutcomeWithoutReordering(term, mask);
        long elapsed = System.nanoTime() - start;
        int decisiveRows = switch (operator) {
            case AND -> result.falseCount(mask);
            case OR -> result.trueMask().selectedCount();
        };
        maskTermStats.computeIfAbsent(term, _ -> new MaskTermStats()).record(mask.selectedCount(), decisiveRows, elapsed, operator);
        return result;
    }

    private MaskOutcome evaluateMaskOutcomeWithoutReordering(MaskExpression expression, Mask mask)
    {
        return switch (expression) {
            case AllMask _ -> new MaskOutcome(mask, emptyMask(mask.size()), emptyMask(mask.size()));
            case ReferenceMask(Reference reference) -> evaluateReferenceMask(reference, mask);
            case NotMask(MaskExpression source) -> {
                MaskOutcome sourceOutcome = evaluateMaskOutcome(source, mask);
                yield new MaskOutcome(
                        sourceOutcome.falseMask(allocator, ALLOCATION_CONTEXT, mask),
                        sourceOutcome.nullMask(),
                        sourceOutcome.errorMask());
            }
            case AndMask(List<MaskExpression> terms) -> evaluateAdaptiveAnd(terms, mask);
            case OrMask(List<MaskExpression> terms) -> evaluateAdaptiveOr(terms, mask);
        };
    }

    private List<MaskExpression> orderTerms(List<MaskExpression> terms, BooleanOperator operator)
    {
        ArrayList<IndexedTerm> indexedTerms = new ArrayList<>(terms.size());
        for (int index = 0; index < terms.size(); index++) {
            indexedTerms.add(new IndexedTerm(index, terms.get(index)));
        }
        indexedTerms.sort(Comparator
                .comparingDouble((IndexedTerm indexedTerm) -> score(indexedTerm.term(), operator))
                .thenComparingInt(IndexedTerm::index));
        return indexedTerms.stream()
                .map(IndexedTerm::term)
                .toList();
    }

    private double score(MaskExpression expression, BooleanOperator operator)
    {
        MaskTermStats stats = maskTermStats.get(expression);
        if (stats == null || stats.rowsEvaluated == 0 || stats.evaluations == 0) {
            return Double.POSITIVE_INFINITY;
        }

        double averageCostPerRow = (double) stats.elapsedNanos / stats.rowsEvaluated;
        double decisiveRate = switch (operator) {
            case AND -> (double) stats.falseRows / stats.rowsEvaluated;
            case OR -> (double) stats.trueRows / stats.rowsEvaluated;
        };
        if (decisiveRate <= 0) {
            return Double.POSITIVE_INFINITY;
        }
        return averageCostPerRow / decisiveRate;
    }

    private Mask unionMasks(Mask left, Mask right, int size)
    {
        if (left.none()) {
            return right;
        }
        if (right.none()) {
            return left;
        }
        return allocator.unionMask(ALLOCATION_CONTEXT, left, right);
    }

    private Mask subtractMasks(Mask base, Mask first, Mask second)
    {
        Mask result = first.none() ? base : allocator.differenceMask(ALLOCATION_CONTEXT, base, first);
        return second.none() ? result : allocator.differenceMask(ALLOCATION_CONTEXT, result, second);
    }

    private Mask emptyMask(int size)
    {
        return allocator.allocateSparseMask(ALLOCATION_CONTEXT, new int[0], size);
    }

    private enum BooleanOperator
    {
        AND,
        OR
    }

    private record IndexedTerm(int index, MaskExpression term) {}

    private static final class MaskTermStats
    {
        private long rowsEvaluated;
        private long trueRows;
        private long falseRows;
        private long elapsedNanos;
        private long evaluations;

        private void record(int inputRows, int decisiveRows, long nanos, BooleanOperator operator)
        {
            rowsEvaluated += inputRows;
            switch (operator) {
                case AND -> falseRows += decisiveRows;
                case OR -> trueRows += decisiveRows;
            }
            elapsedNanos += nanos;
            evaluations++;
        }
    }

    private record MaskOutcome(Mask trueMask, Mask nullMask, Mask errorMask)
    {
        private int falseCount(Mask domainMask)
        {
            return domainMask.selectedCount() - trueMask.selectedCount() - nullMask.selectedCount() - errorMask.selectedCount();
        }

        private Mask falseMask(Allocator allocator, Allocator.Context context, Mask domainMask)
        {
            Mask withoutTrue = trueMask.none() ? domainMask : allocator.differenceMask(context, domainMask, trueMask);
            Mask withoutNull = nullMask.none() ? withoutTrue : allocator.differenceMask(context, withoutTrue, nullMask);
            return errorMask.none() ? withoutNull : allocator.differenceMask(context, withoutNull, errorMask);
        }

        private Mask survivorsMask(Allocator allocator, Allocator.Context context)
        {
            Mask survivors = trueMask;
            if (!nullMask.none()) {
                survivors = survivors.none() ? nullMask : allocator.unionMask(context, survivors, nullMask);
            }
            if (!errorMask.none()) {
                survivors = survivors.none() ? errorMask : allocator.unionMask(context, survivors, errorMask);
            }
            return survivors;
        }
    }
}
