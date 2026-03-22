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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.StructVector;
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
import org.weakref.nitro.operator.evaluator.ir.MaskExpressionResolver;
import org.weakref.nitro.operator.evaluator.ir.MemoizationPolicy;
import org.weakref.nitro.operator.evaluator.ir.Merge;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.OrMask;
import org.weakref.nitro.operator.evaluator.ir.Producer;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.StreamPlan;
import org.weakref.nitro.operator.evaluator.ir.StructField;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public final class PlanEvaluator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("PlanEvaluator");
    private static final Set<Stream> VALUES_ONLY = java.util.EnumSet.of(Stream.VALUES);
    private static final Set<Stream> NULLS_ONLY = java.util.EnumSet.of(Stream.NULLS);
    private static final Set<Stream> ERRORS_ONLY = java.util.EnumSet.of(Stream.ERRORS);

    private final EvaluationPlan plan;
    private final PrimitiveRegistry primitiveRegistry;
    private final InputResolver input;
    private final Allocator allocator;
    private final PrimitiveExecutionContext executionContext;
    private final Map<Variable, Assignment> assignments;
    private final Set<Allocator.Context> primitiveAllocationContexts;
    private final Set<org.weakref.nitro.operator.evaluator.ir.Producer> memoizedProducers;
    private final Map<Producer, Set<Stream>> projectedStreamsByProducer;
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
        this.primitiveAllocationContexts = primitiveAllocationContexts(plan, primitiveRegistry);
        this.memoizedProducers = plan.streamPlans().entrySet().stream()
                .filter(entry -> entry.getValue().memoizationPolicy() == MemoizationPolicy.MEMOIZE)
                .map(entry -> entry.getKey().producer())
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        this.projectedStreamsByProducer = projectedStreamsByProducer(plan.outputs());
    }

    public Streams evaluate(Reference reference, Mask mask)
    {
        if (mask.none()) {
            return Streams.empty();
        }

        if (isMemoized(reference)) {
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

    public Mask evaluate(MaskExpression expression, Mask mask)
    {
        if (mask.none()) {
            return mask;
        }
        return evaluateMask(expression, mask);
    }

    public void reset()
    {
        memoizedMasks.clear();
        memoizedStreams.clear();
        for (Allocator.Context context : primitiveAllocationContexts) {
            allocator.releaseIfPresent(context);
        }
        allocator.releaseIfPresent(ALLOCATION_CONTEXT);
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
        Vector inputVector = input.resolve(new Reference(new org.weakref.nitro.operator.evaluator.ir.Input(inputIndex), reference.stream()), mask);
        checkArgument(inputVector != null || reference.stream() != Stream.VALUES, "Missing VALUES stream for input %s", reference);
        Streams result = inputVector == null ? Streams.empty() : Streams.of(reference.stream(), inputVector);
        return completeRequestedStreams(requestedStreams(reference.stream()), result, mask);
    }

    private Streams evaluateVariable(Reference reference, Variable variable, Mask mask, Streams output)
    {
        Assignment assignment = assignments.get(variable);
        checkArgument(assignment != null, "Unknown variable: %s", variable);

        return switch (assignment.operation()) {
            case Literal literal -> evaluateLiteral(requestedStreamsFor(reference), literal, mask);
            case Copy(Reference source) -> copy(requestedStreamsFor(reference), source, mask, output);
            case Call call -> evaluateCall(reference, call, mask, output);
            case Merge merge -> evaluateMerge(requestedStreamsFor(reference), merge, mask, output);
            case StructField field -> evaluateStructField(requestedStreamsFor(reference), field, mask, output);
        };
    }

    private Streams evaluateLiteral(Set<Stream> requestedStreams, Literal literal, Mask mask)
    {
        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.VALUES)) {
            int length = mask.maxPosition() + 1;
            result = switch (literal.value()) {
                case Long value -> Streams.ofValues(fillLongRle(value, length));
                case Boolean value -> Streams.of(Stream.VALUES, fillBoolean(value, length));
                case String value -> Streams.of(Stream.VALUES, fillUtf8(value, length));
                default -> throw new IllegalArgumentException("Unsupported literal value: " + literal.value());
            };
        }
        return completeRequestedStreams(requestedStreams, result, mask);
    }

    private Streams evaluateCall(Reference reference, Call call, Mask mask, Streams output)
    {
        PrimitiveFunction function = primitiveRegistry.get(call.name());
        List<Streams> inputs = new ArrayList<>(call.arguments().size());
        boolean requiresInputCompanionStreams = function.requiresInputCompanionStreams();
        for (Reference argument : call.arguments()) {
            inputs.add(requiresInputCompanionStreams ? evaluateArgument(argument, mask) : evaluate(argument, mask));
        }
        Set<Stream> requestedStreams = requestedStreamsFor(reference);
        Streams result = function.apply(inputs, mask, requestedStreams, prepareOutput(output), executionContext);
        return completeRequestedStreams(requestedStreams, result, mask);
    }

    private Streams evaluateArgument(Reference argument, Mask mask)
    {
        Streams bundle = evaluate(argument, mask);
        if (argument.stream() != Stream.VALUES) {
            return bundle;
        }

        Streams.Builder builder = null;
        Streams nullBundle = evaluate(new Reference(argument.producer(), Stream.NULLS), mask);
        if (nullBundle.has(Stream.NULLS)) {
            builder = Streams.builder().putAll(bundle);
            builder.put(Stream.NULLS, nullBundle.get(Stream.NULLS));
        }

        Streams errorBundle = evaluate(new Reference(argument.producer(), Stream.ERRORS), mask);
        if (errorBundle.has(Stream.ERRORS)) {
            if (builder == null) {
                builder = Streams.builder().putAll(bundle);
            }
            builder.put(Stream.ERRORS, errorBundle.get(Stream.ERRORS));
        }
        return builder == null ? bundle : builder.build();
    }

    private Set<Stream> requestedStreamsFor(Reference reference)
    {
        java.util.EnumSet<Stream> requested = java.util.EnumSet.of(reference.stream());
        Set<Stream> projectedStreams = projectedStreamsByProducer.get(reference.producer());
        if (projectedStreams != null) {
            requested.addAll(projectedStreams);
        }
        if (memoizedProducers.contains(reference.producer())) {
            for (Reference plannedReference : plan.streamPlans().keySet()) {
                if (plannedReference.producer().equals(reference.producer()) && isMemoized(plannedReference)) {
                    requested.add(plannedReference.stream());
                }
            }
        }
        return requested;
    }

    private Streams copy(Set<Stream> requestedStreams, Reference source, Mask mask, Streams output)
    {
        Streams.Builder result = Streams.builder();
        for (Stream stream : requestedStreams) {
            Reference sourceReference = remapReference(source, stream);
            if (sourceReference == null) {
                continue;
            }
            Streams sourceStreams = evaluate(sourceReference, mask);
            if (!sourceStreams.has(sourceReference.stream())) {
                continue;
            }

            Vector sourceVector = sourceStreams.get(sourceReference.stream());
            Vector existing = output != null && output.has(stream) ? output.get(stream) : null;
            Vector target = existing == null ? sourceVector : copyVector(sourceVector, existing, mask);
            result.put(stream, target);
        }
        return completeRequestedStreams(requestedStreams, result.build(), mask);
    }

    private Streams evaluateMerge(Set<Stream> requestedStreams, Merge merge, Mask mask, Streams output)
    {
        Mask trueMask = evaluateMaskOutcome(merge.condition(), mask).trueMask();
        Mask falseMask = allocator.differenceMask(ALLOCATION_CONTEXT, mask, trueMask);

        Streams.Builder result = Streams.builder();
        for (Stream stream : requestedStreams) {
            Vector merged = evaluateMergeStream(stream, merge, mask, trueMask, falseMask, output);
            if (merged != null) {
                result.put(stream, merged);
            }
        }
        return completeRequestedStreams(requestedStreams, result.build(), mask);
    }

    private Streams evaluateStructField(Set<Stream> requestedStreams, StructField field, Mask mask, Streams output)
    {
        Streams sourceStreams = evaluateArgument(field.source(), mask);
        StructVector sourceValues = (StructVector) sourceStreams.values();
        Streams fieldStreams = sourceValues.field(field.fieldName());

        Streams.Builder result = Streams.builder();
        if (requestedStreams.contains(Stream.VALUES) && fieldStreams.has(Stream.VALUES)) {
            result.put(Stream.VALUES, fieldStreams.get(Stream.VALUES));
        }

        for (Stream stream : requestedStreams) {
            if (stream == Stream.VALUES) {
                continue;
            }
            Vector existing = output != null && output.has(stream) ? output.get(stream) : null;
            Vector merged = mergeOptionalBooleanStreams(sourceStreams.getOrNull(stream), fieldStreams.getOrNull(stream), existing, mask);
            if (merged != null) {
                result.put(stream, merged);
            }
        }
        return completeRequestedStreams(requestedStreams, result.build(), mask);
    }

    private Vector evaluateMergeStream(Stream stream, Merge merge, Mask mask, Mask trueMask, Mask falseMask, Streams output)
    {
        Reference trueReference = remapReference(merge.whenTrue(), stream);
        Reference falseReference = remapReference(merge.whenFalse(), stream);
        Vector existing = output != null && output.has(stream) ? output.get(stream) : null;
        boolean singleBranch = trueMask.none() || falseMask.none();
        Vector target = existing;

        if (!trueMask.none()) {
            target = mergeBranchInto(stream, trueReference, trueMask, mask, target, singleBranch && falseMask.none());
        }
        if (!falseMask.none()) {
            target = mergeBranchInto(stream, falseReference, falseMask, mask, target, singleBranch && trueMask.none());
        }
        return target;
    }

    private Vector mergeBranchInto(Stream stream, Reference source, Mask branchMask, Mask fullMask, Vector target, boolean allowForward)
    {
        if (source == null) {
            checkArgument(stream != Stream.VALUES, "VALUES stream cannot be absent for active merge branch");
            return fillFalseBoolean(target, branchMask, fullMask.maxPosition() + 1);
        }

        Streams sourceStreams = evaluate(source, branchMask);
        if (!sourceStreams.has(source.stream())) {
            checkArgument(stream != Stream.VALUES, "VALUES stream not produced for active merge branch: %s", source);
            return fillFalseBoolean(target, branchMask, fullMask.maxPosition() + 1);
        }

        Vector sourceVector = sourceStreams.get(source.stream());
        if (allowForward && target == null) {
            return sourceVector;
        }
        return copyVector(sourceVector, target, branchMask);
    }

    private Vector mergeOptionalBooleanStreams(Vector parentStream, Vector childStream, Vector existing, Mask mask)
    {
        if (parentStream == null) {
            return childStream;
        }
        if (childStream == null) {
            return parentStream;
        }

        BooleanVector merged = allocator.allocateOrGrow(ALLOCATION_CONTEXT, (BooleanVector) existing, BooleanVector.class, mask.maxPosition() + 1, BooleanVector::new);
        BooleanVector parentValues = (BooleanVector) parentStream;
        BooleanVector childValues = (BooleanVector) childStream;
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                merged.values()[position] = parentValues.values()[position] || childValues.values()[position];
            }
            return merged;
        }

        for (int position : mask) {
            merged.values()[position] = parentValues.values()[position] || childValues.values()[position];
        }
        return merged;
    }

    private BooleanVector fillFalseBoolean(Vector existing, Mask mask, int length)
    {
        BooleanVector target = allocator.allocateOrGrow(ALLOCATION_CONTEXT, (BooleanVector) existing, BooleanVector.class, length, BooleanVector::new);
        if (mask.all()) {
            Arrays.fill(target.values(), 0, mask.size(), false);
        }
        else {
            for (int position : mask) {
                target.values()[position] = false;
            }
        }
        return target;
    }

    private Mask evaluateMask(MaskExpression expression, Mask mask)
    {
        return evaluateMaskOutcome(expression, mask).trueMask();
    }

    private Streams prepareOutput(Streams output)
    {
        return output == null ? Streams.empty() : output;
    }

    private static Reference remapReference(Reference reference, Stream requestedStream)
    {
        if (reference.stream() == requestedStream) {
            return reference;
        }
        if (reference.stream() == Stream.VALUES) {
            return new Reference(reference.producer(), requestedStream);
        }
        return null;
    }

    private Vector copyVector(Vector source, Vector existing, Mask mask)
    {
        return switch (source) {
            case I64Vector sourceValues -> copyLongVector(sourceValues, existing, mask);
            case BooleanVector sourceValues -> copyBooleanVector(sourceValues, existing, mask);
            case DictionaryVector sourceValues -> copyDictionaryVector(sourceValues, existing, mask);
            case RleVector sourceValues -> copyRleVector(sourceValues, existing, mask);
            default -> throw new IllegalArgumentException("Unsupported vector type for copy: " + source.getClass().getSimpleName());
        };
    }

    private Vector copyDictionaryVector(DictionaryVector source, Vector existing, Mask mask)
    {
        return switch (source.values()) {
            case I64Vector values -> copyLongDictionaryVector(source.ids(), values.values(), existing, mask, source.length());
            case BooleanVector values -> copyBooleanDictionaryVector(source.ids(), values.values(), existing, mask, source.length());
            default -> throw new IllegalArgumentException("Unsupported dictionary value type for copy: " + source.values().getClass().getSimpleName());
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
            case BinaryVector values -> copyBinaryRleVector(source.counts(), values, existing, mask, source.length());
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

    private Vector copyLongDictionaryVector(int[] ids, long[] values, Vector existing, Mask mask, int length)
    {
        I64Vector target = allocator.allocateOrGrow(ALLOCATION_CONTEXT, (I64Vector) existing, I64Vector.class, length, I64Vector::new);
        for (int position : mask) {
            target.values()[position] = values[ids[position]];
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

    private Vector copyBooleanDictionaryVector(int[] ids, boolean[] values, Vector existing, Mask mask, int length)
    {
        BooleanVector target = allocator.allocateOrGrow(ALLOCATION_CONTEXT, (BooleanVector) existing, BooleanVector.class, length, BooleanVector::new);
        for (int position : mask) {
            target.values()[position] = values[ids[position]];
        }
        return target;
    }

    private Vector copyBinaryRleVector(int[] counts, BinaryVector values, Vector existing, Mask mask, int length)
    {
        int totalBytes = 0;
        int runIndex = 0;
        int runEnd = counts[0];
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += counts[runIndex];
            }
            totalBytes += values.length(runIndex);
        }

        BinaryVector target = allocator.allocateOrGrowBinary(ALLOCATION_CONTEXT, (BinaryVector) existing, length, totalBytes);
        target.addTraits(values.traits());

        runIndex = 0;
        runEnd = counts[0];
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += counts[runIndex];
            }
            int valueLength = values.length(runIndex);
            if (valueLength == 0) {
                target.setNull(position);
            }
            else {
                target.setBytes(position, values.data(), values.startOffset(runIndex), valueLength);
            }
        }
        return target;
    }

    private Vector fillLongRle(long value, int length)
    {
        I64Vector values = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, 1, I64Vector::new);
        values.values()[0] = value;
        return allocator.allocateRle(ALLOCATION_CONTEXT, new int[] {length}, values);
    }

    private Vector fillBoolean(boolean value, int length)
    {
        BooleanVector result = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, length, BooleanVector::new);
        for (int position = 0; position < length; position++) {
            result.values()[position] = value;
        }
        return result;
    }

    private Vector fillUtf8(String value, int length)
    {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        BinaryVector values = allocator.allocateBinary(ALLOCATION_CONTEXT, 1, bytes.length);
        values.addTrait(BinaryVector.Trait.UTF8_STRING);
        if (bytes.length == value.length()) {
            values.addTrait(BinaryVector.Trait.ASCII_ONLY);
        }
        values.setBytes(0, bytes);
        return allocator.allocateRle(ALLOCATION_CONTEXT, new int[] {length}, values);
    }

    private static Map<Variable, Assignment> indexAssignments(List<Assignment> assignments)
    {
        Map<Variable, Assignment> indexedAssignments = new HashMap<>();
        for (Assignment assignment : assignments) {
            indexedAssignments.put(assignment.output(), assignment);
        }
        return indexedAssignments;
    }

    private static Set<Allocator.Context> primitiveAllocationContexts(EvaluationPlan plan, PrimitiveRegistry primitiveRegistry)
    {
        Set<Allocator.Context> contexts = new HashSet<>();
        for (Assignment assignment : plan.assignments()) {
            if (assignment.operation() instanceof Call call) {
                try {
                    contexts.addAll(primitiveRegistry.get(call.name()).allocationContexts());
                }
                catch (IllegalArgumentException _) {
                    // Some tests and partial plans use calls that are not backed by the active primitive registry.
                }
            }
        }
        return Set.copyOf(contexts);
    }

    private static Map<Producer, Set<Stream>> projectedStreamsByProducer(List<Reference> outputs)
    {
        Map<Producer, java.util.EnumSet<Stream>> projected = new HashMap<>();
        for (Reference output : outputs) {
            projected.computeIfAbsent(output.producer(), _ -> java.util.EnumSet.noneOf(Stream.class))
                    .add(output.stream());
        }
        return projected.entrySet().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> Set.copyOf(entry.getValue())));
    }

    private void memoizeStreams(Reference reference, Streams streams, Mask mask)
    {
        if (!memoizedProducers.contains(reference.producer())) {
            return;
        }

        for (Stream stream : streams.streams()) {
            Reference streamReference = new Reference(reference.producer(), stream);
            if (isMemoized(streamReference)) {
                memoizedStreams.put(streamReference, streams);
                memoizedMasks.put(streamReference, mask);
            }
        }
    }

    private boolean isMemoized(Reference reference)
    {
        StreamPlan streamPlan = plan.streamPlans().get(reference);
        return streamPlan != null && streamPlan.memoizationPolicy() == MemoizationPolicy.MEMOIZE;
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
        MaskExpression resolved = MaskExpressionResolver.resolve(plan, new ReferenceMask(reference));
        if (!(resolved instanceof ReferenceMask(Reference resolvedReference) && resolvedReference.equals(reference))) {
            return evaluateMaskOutcome(resolved, mask);
        }

        MaskOutcome optimized = tryEvaluateLongComparisonMask(reference, mask);
        if (optimized != null) {
            return optimized;
        }

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

    private MaskOutcome tryEvaluateLongComparisonMask(Reference reference, Mask mask)
    {
        LongComparison comparison = resolveLongComparison(reference);
        if (comparison == null) {
            return null;
        }

        Vector leftValues = comparison.left().inputIndex() >= 0
                ? input.resolve(new Reference(new org.weakref.nitro.operator.evaluator.ir.Input(comparison.left().inputIndex()), Stream.VALUES), mask)
                : null;
        Vector rightValues = comparison.right().inputIndex() >= 0
                ? input.resolve(new Reference(new org.weakref.nitro.operator.evaluator.ir.Input(comparison.right().inputIndex()), Stream.VALUES), mask)
                : null;

        BooleanVector leftNulls = comparison.left().inputIndex() >= 0
                ? (BooleanVector) input.resolve(new Reference(new org.weakref.nitro.operator.evaluator.ir.Input(comparison.left().inputIndex()), Stream.NULLS), mask)
                : null;
        BooleanVector rightNulls = comparison.right().inputIndex() >= 0
                ? (BooleanVector) input.resolve(new Reference(new org.weakref.nitro.operator.evaluator.ir.Input(comparison.right().inputIndex()), Stream.NULLS), mask)
                : null;
        BooleanVector leftErrors = comparison.left().inputIndex() >= 0
                ? (BooleanVector) input.resolve(new Reference(new org.weakref.nitro.operator.evaluator.ir.Input(comparison.left().inputIndex()), Stream.ERRORS), mask)
                : null;
        BooleanVector rightErrors = comparison.right().inputIndex() >= 0
                ? (BooleanVector) input.resolve(new Reference(new org.weakref.nitro.operator.evaluator.ir.Input(comparison.right().inputIndex()), Stream.ERRORS), mask)
                : null;

        int[] truePositions = new int[mask.selectedCount()];
        int[] nullPositions = new int[mask.selectedCount()];
        int[] errorPositions = new int[mask.selectedCount()];
        int trueCount = 0;
        int nullCount = 0;
        int errorCount = 0;

        for (int position : mask) {
            if ((leftErrors != null && leftErrors.values()[position]) || (rightErrors != null && rightErrors.values()[position])) {
                errorPositions[errorCount++] = position;
                continue;
            }
            if ((leftNulls != null && leftNulls.values()[position]) || (rightNulls != null && rightNulls.values()[position])) {
                nullPositions[nullCount++] = position;
                continue;
            }

            long left = comparison.left().inputIndex() >= 0 ? readLong(leftValues, position) : comparison.left().literal();
            long right = comparison.right().inputIndex() >= 0 ? readLong(rightValues, position) : comparison.right().literal();
            if (comparison.functionName().equals("eq") ? left == right : left < right) {
                truePositions[trueCount++] = position;
            }
        }

        return new MaskOutcome(
                allocator.allocateSparseMask(ALLOCATION_CONTEXT, Arrays.copyOf(truePositions, trueCount), mask.size()),
                allocator.allocateSparseMask(ALLOCATION_CONTEXT, Arrays.copyOf(nullPositions, nullCount), mask.size()),
                allocator.allocateSparseMask(ALLOCATION_CONTEXT, Arrays.copyOf(errorPositions, errorCount), mask.size()));
    }

    private LongComparison resolveLongComparison(Reference reference)
    {
        if (reference.stream() != Stream.VALUES || !(reference.producer() instanceof Variable variable)) {
            return null;
        }
        Assignment assignment = assignments.get(variable);
        if (assignment == null || !(assignment.operation() instanceof Call call) || call.arguments().size() != 2) {
            return null;
        }
        if (!call.name().equals("eq") && !call.name().equals("lt")) {
            return null;
        }

        LongOperand left = resolveLongOperand(call.arguments().get(0));
        LongOperand right = resolveLongOperand(call.arguments().get(1));
        if (left == null || right == null) {
            return null;
        }
        return new LongComparison(call.name(), left, right);
    }

    private LongOperand resolveLongOperand(Reference reference)
    {
        if (reference.stream() != Stream.VALUES) {
            return null;
        }
        return switch (reference.producer()) {
            case org.weakref.nitro.operator.evaluator.ir.Input(int index) -> new LongOperand(index, 0);
            case Variable variable -> {
                Assignment assignment = assignments.get(variable);
                if (assignment != null && assignment.operation() instanceof Literal(Long literal)) {
                    yield new LongOperand(-1, literal);
                }
                yield null;
            }
        };
    }

    private static long readLong(Vector vector, int position)
    {
        return switch (vector) {
            case I64Vector values -> values.values()[position];
            case I32Vector values -> values.values()[position];
            case DictionaryVector values -> readLong(values.values(), values.ids()[position]);
            case RleVector values -> readLong(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected integer vector but found " + vector.getClass().getSimpleName());
        };
    }

    private BooleanVector optionalBooleanStream(org.weakref.nitro.operator.evaluator.ir.Producer producer, Stream stream, Mask mask)
    {
        return (BooleanVector) evaluate(new Reference(producer, stream), mask).getOrNull(stream);
    }

    private Streams completeRequestedStreams(Set<Stream> requestedStreams, Streams streams, Mask mask)
    {
        boolean wantsValues = requestedStreams.contains(Stream.VALUES);
        boolean wantsNulls = requestedStreams.contains(Stream.NULLS);
        boolean wantsErrors = requestedStreams.contains(Stream.ERRORS);

        if ((!wantsValues || streams.has(Stream.VALUES)) &&
                (!wantsNulls || streams.has(Stream.NULLS)) &&
                (!wantsErrors || streams.has(Stream.ERRORS))) {
            return streams;
        }

        Streams completed = streams;
        int length = mask.maxPosition() + 1;
        if (wantsValues && !completed.has(Stream.VALUES)) {
            throw new IllegalArgumentException("VALUES stream not produced for request");
        }
        if (wantsNulls && !completed.has(Stream.NULLS)) {
            completed = completed.with(Stream.NULLS, fillFalseBoolean(null, mask, length));
        }
        if (wantsErrors && !completed.has(Stream.ERRORS)) {
            completed = completed.with(Stream.ERRORS, fillFalseBoolean(null, mask, length));
        }
        return completed;
    }

    private static Set<Stream> requestedStreams(Stream stream)
    {
        return switch (stream) {
            case VALUES -> VALUES_ONLY;
            case NULLS -> NULLS_ONLY;
            case ERRORS -> ERRORS_ONLY;
        };
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }

    private static boolean isError(BooleanVector errors, int position)
    {
        return errors != null && errors.values()[position];
    }

    private static boolean binaryEquals(BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        int leftLength = left.length(leftPosition);
        if (leftLength != right.length(rightPosition)) {
            return false;
        }
        byte[] leftData = left.data();
        byte[] rightData = right.data();
        int leftStart = left.startOffset(leftPosition);
        int rightStart = right.startOffset(rightPosition);
        for (int index = 0; index < leftLength; index++) {
            if (leftData[leftStart + index] != rightData[rightStart + index]) {
                return false;
            }
        }
        return true;
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

    private record LongOperand(int inputIndex, long literal) {}

    private record LongComparison(String functionName, LongOperand left, LongOperand right) {}

    private record KeyAccess(BinaryVector values, boolean dictionary, int[] ids)
    {
        private int position(int position)
        {
            return dictionary ? ids[position] : position;
        }
    }

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
