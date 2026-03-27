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
    private final Allocator.Context allocationContext = new Allocator.Context("PlanEvaluator");
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
    private final Map<Producer, Set<Stream>> memoizedStreamsByProducer;
    private final Map<Reference, Set<Stream>> requestedStreamsByReference = new HashMap<>();
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
        this.memoizedStreamsByProducer = memoizedStreamsByProducer(plan.streamPlans());
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

            Mask remaining = existingMask == null ? mask : allocator.differenceMask(allocationContext, mask, existingMask);
            if (remaining.none()) {
                return existingOutput;
            }

            Streams updated = evaluateUnmemoized(reference, remaining, existingOutput);
            Mask updatedMask = existingMask == null ? remaining : allocator.unionMask(allocationContext, existingMask, remaining);
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
        return evaluateTrueMask(expression, mask);
    }

    public Mask evaluateInPlace(MaskExpression expression, Mask mask)
    {
        if (mask.none()) {
            return mask;
        }
        return evaluateTrueMaskInPlace(expression, mask);
    }

    public void reset()
    {
        memoizedMasks.clear();
        memoizedStreams.clear();
        for (Allocator.Context context : primitiveAllocationContexts) {
            allocator.releaseIfPresent(context);
        }
        for (Allocator.Context context : executionContext.allocationContexts()) {
            allocator.releaseIfPresent(context);
        }
        allocator.releaseIfPresent(allocationContext);
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
        Set<Stream> requestedStreams = computeRequestedStreams(reference);
        Streams.Builder result = Streams.builder();
        for (Stream stream : requestedStreams) {
            Vector inputVector = input.resolve(new Reference(new org.weakref.nitro.operator.evaluator.ir.Input(inputIndex), stream), mask);
            if (inputVector != null) {
                result.put(stream, inputVector);
            }
        }
        return completeRequestedStreams(requestedStreams, result.build(), mask);
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
        Streams peeledResult = tryEvaluateDictionaryPeeledCall(function, inputs, requestedStreams);
        if (peeledResult != null) {
            return completeRequestedStreams(requestedStreams, peeledResult, mask);
        }
        Streams result = function.apply(inputs, mask, requestedStreams, prepareOutput(output), executionContext);
        return completeRequestedStreams(requestedStreams, result, mask);
    }

    private Streams tryEvaluateDictionaryPeeledCall(PrimitiveFunction function, List<Streams> inputs, Set<Stream> requestedStreams)
    {
        if (!function.deterministic()) {
            return null;
        }

        DictionaryPeeling peeling = tryBuildDictionaryPeeling(inputs);
        if (peeling == null) {
            return null;
        }

        try {
            Streams baseResult = function.apply(inputsForPeeling(peeling), peeling.baseMask(), requestedStreams, null, executionContext);
            return wrapDictionaryPeeledStreams(peeling.ids(), baseResult);
        }
        finally {
            allocator.release(allocationContext, peeling.baseMask());
        }
    }

    private static List<Streams> inputsForPeeling(DictionaryPeeling peeling)
    {
        return peeling.inputs();
    }

    private DictionaryPeeling tryBuildDictionaryPeeling(List<Streams> inputs)
    {
        int[] sharedIds = null;
        int rowCount = -1;
        for (Streams inputStreams : inputs) {
            Vector values = inputStreams.getOrNull(Stream.VALUES);
            if (values instanceof DictionaryVector dictionary) {
                if (sharedIds == null) {
                    sharedIds = dictionary.ids();
                    rowCount = dictionary.length();
                }
                else if (dictionary.length() != rowCount || !Arrays.equals(sharedIds, dictionary.ids())) {
                    return null;
                }
            }
        }
        if (sharedIds == null) {
            return null;
        }

        int baseLength = 0;
        for (int id : sharedIds) {
            baseLength = Math.max(baseLength, id + 1);
        }
        Mask baseMask = allocator.allocateAllMask(allocationContext, baseLength);

        List<Streams> peeledInputs = new ArrayList<>(inputs.size());
        for (Streams inputStreams : inputs) {
            Streams peeled = peelDictionaryCompatibleStreams(inputStreams, sharedIds, rowCount, baseLength);
            if (peeled == null) {
                allocator.release(allocationContext, baseMask);
                return null;
            }
            peeledInputs.add(peeled);
        }
        return new DictionaryPeeling(sharedIds, baseMask, List.copyOf(peeledInputs));
    }

    private Streams peelDictionaryCompatibleStreams(Streams streams, int[] sharedIds, int rowCount, int baseLength)
    {
        Streams.Builder peeled = Streams.builder();
        for (Stream stream : streams.streams()) {
            Vector peeledVector = peelDictionaryCompatibleVector(streams.get(stream), sharedIds, rowCount, baseLength);
            if (peeledVector == null) {
                return null;
            }
            peeled.put(stream, peeledVector);
        }
        return peeled.build();
    }

    private Vector peelDictionaryCompatibleVector(Vector vector, int[] sharedIds, int rowCount, int baseLength)
    {
        return switch (vector) {
            case DictionaryVector dictionary when dictionary.length() == rowCount && Arrays.equals(sharedIds, dictionary.ids()) -> dictionary.values();
            case RleVector rle when rle.counts().length == 1 -> executionContext.allocator().allocateRle(allocationContext, new int[] {baseLength}, rle.values());
            case BooleanVector booleans when booleans.length() == rowCount && isConstantBooleanVector(booleans) -> fillBoolean(booleans.values()[0], baseLength);
            default -> null;
        };
    }

    private static boolean isConstantBooleanVector(BooleanVector vector)
    {
        if (vector.length() == 0) {
            return true;
        }
        boolean value = vector.values()[0];
        for (int index = 1; index < vector.length(); index++) {
            if (vector.values()[index] != value) {
                return false;
            }
        }
        return true;
    }

    private Streams wrapDictionaryPeeledStreams(int[] sharedIds, Streams streams)
    {
        Streams.Builder wrapped = Streams.builder();
        for (Stream stream : streams.streams()) {
            wrapped.put(stream, executionContext.allocator().allocateDictionary(allocationContext, sharedIds, streams.get(stream)));
        }
        return wrapped.build();
    }

    private Streams evaluateArgument(Reference argument, Mask mask)
    {
        Streams result = evaluate(argument, mask);
        if (argument.stream() != Stream.VALUES) {
            return result;
        }

        Streams nullBundle = evaluate(new Reference(argument.producer(), Stream.NULLS), mask);
        if (nullBundle.has(Stream.NULLS)) {
            result = result.with(Stream.NULLS, nullBundle.get(Stream.NULLS));
        }

        Streams errorBundle = evaluate(new Reference(argument.producer(), Stream.ERRORS), mask);
        if (errorBundle.has(Stream.ERRORS)) {
            result = result.with(Stream.ERRORS, errorBundle.get(Stream.ERRORS));
        }
        return result;
    }

    private Set<Stream> requestedStreamsFor(Reference reference)
    {
        return requestedStreamsByReference.computeIfAbsent(reference, this::computeRequestedStreams);
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
        Mask falseMask = allocator.differenceMask(allocationContext, mask, trueMask);

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

        BooleanVector merged = allocator.allocateOrGrow(allocationContext, (BooleanVector) existing, BooleanVector.class, mask.maxPosition() + 1, BooleanVector::new);
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
        BooleanVector target = allocator.allocateOrGrow(allocationContext, (BooleanVector) existing, BooleanVector.class, length, BooleanVector::new);
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

    private Mask evaluateTrueMask(MaskExpression expression, Mask mask)
    {
        return switch (expression) {
            case AllMask _ -> mask;
            case ReferenceMask(Reference reference) -> evaluateTrueReferenceMask(reference, mask);
            case NotMask(MaskExpression source) -> evaluateFalseMask(source, mask);
            case AndMask(List<MaskExpression> terms) -> evaluateAdaptiveAndTrueMask(terms, mask);
            case OrMask(List<MaskExpression> terms) -> evaluateAdaptiveOrTrueMask(terms, mask);
        };
    }

    private Mask evaluateFalseMask(MaskExpression expression, Mask mask)
    {
        return switch (expression) {
            case AllMask _ -> emptyMask(mask.size());
            case ReferenceMask(Reference reference) -> evaluateFalseReferenceMask(reference, mask);
            case NotMask(MaskExpression source) -> evaluateTrueMask(source, mask);
            case AndMask _, OrMask _ -> evaluateMaskOutcome(expression, mask).falseMask(allocator, allocationContext, mask);
        };
    }

    private Mask evaluateTrueReferenceMask(Reference reference, Mask mask)
    {
        MaskExpression resolved = MaskExpressionResolver.resolve(plan, new ReferenceMask(reference));
        if (!(resolved instanceof ReferenceMask(Reference resolvedReference) && resolvedReference.equals(reference))) {
            return evaluateTrueMask(resolved, mask);
        }

        Mask primitiveMask = tryEvaluatePrimitiveTrueMask(reference, mask);
        if (primitiveMask != null) {
            return primitiveMask;
        }

        Vector values = evaluate(reference, mask).get(reference.stream());
        BooleanVector errors = optionalBooleanStream(reference.producer(), Stream.ERRORS, mask);
        BooleanVector nulls = optionalBooleanStream(reference.producer(), Stream.NULLS, mask);
        return classifyTrueBooleanMask(values, nulls, errors, mask);
    }

    private Mask evaluateFalseReferenceMask(Reference reference, Mask mask)
    {
        MaskExpression resolved = MaskExpressionResolver.resolve(plan, new ReferenceMask(reference));
        if (!(resolved instanceof ReferenceMask(Reference resolvedReference) && resolvedReference.equals(reference))) {
            return evaluateFalseMask(resolved, mask);
        }

        Mask primitiveMask = tryEvaluatePrimitiveFalseMask(reference, mask);
        if (primitiveMask != null) {
            return primitiveMask;
        }

        Vector values = evaluate(reference, mask).get(reference.stream());
        BooleanVector errors = optionalBooleanStream(reference.producer(), Stream.ERRORS, mask);
        BooleanVector nulls = optionalBooleanStream(reference.producer(), Stream.NULLS, mask);
        return classifyFalseBooleanMask(values, nulls, errors, mask);
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
        return source.copyMasked(allocator, allocationContext, existing, mask);
    }

    private Vector fillLongRle(long value, int length)
    {
        I64Vector values = allocator.allocate(allocationContext, I64Vector.class, 1, I64Vector::new);
        values.values()[0] = value;
        return allocator.allocateRle(allocationContext, new int[] {length}, values);
    }

    private Vector fillBoolean(boolean value, int length)
    {
        BooleanVector result = allocator.allocate(allocationContext, BooleanVector.class, length, BooleanVector::new);
        for (int position = 0; position < length; position++) {
            result.values()[position] = value;
        }
        return result;
    }

    private Vector fillUtf8(String value, int length)
    {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        BinaryVector values = BinaryVector.allocate(allocator, allocationContext, 1, bytes.length);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        if (bytes.length == value.length()) {
            values.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        }
        values.setBytes(0, bytes);
        return allocator.allocateRle(allocationContext, new int[] {length}, values);
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

    private Set<Stream> computeRequestedStreams(Reference reference)
    {
        java.util.EnumSet<Stream> streams = java.util.EnumSet.of(reference.stream());
        Set<Stream> projectedStreams = projectedStreamsByProducer.get(reference.producer());
        if (projectedStreams != null) {
            streams.addAll(projectedStreams);
        }
        if (memoizedProducers.contains(reference.producer())) {
            Set<Stream> memoizedStreams = memoizedStreamsByProducer.get(reference.producer());
            if (memoizedStreams != null) {
                streams.addAll(memoizedStreams);
            }
        }
        return Set.copyOf(streams);
    }

    private static Map<Producer, Set<Stream>> projectedStreamsByProducer(List<Reference> outputs)
    {
        Map<Producer, java.util.EnumSet<Stream>> projected = new HashMap<>();
        for (Reference output : outputs) {
            java.util.EnumSet<Stream> streams = projected.computeIfAbsent(output.producer(), _ -> java.util.EnumSet.noneOf(Stream.class));
            streams.add(output.stream());
            if (output.stream() == Stream.VALUES) {
                streams.add(Stream.NULLS);
                streams.add(Stream.ERRORS);
            }
        }
        return projected.entrySet().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> Set.copyOf(entry.getValue())));
    }

    private static Map<Producer, Set<Stream>> memoizedStreamsByProducer(Map<Reference, StreamPlan> streamPlans)
    {
        Map<Producer, java.util.EnumSet<Stream>> memoized = new HashMap<>();
        for (Map.Entry<Reference, StreamPlan> entry : streamPlans.entrySet()) {
            if (entry.getValue().memoizationPolicy() == MemoizationPolicy.MEMOIZE) {
                memoized.computeIfAbsent(entry.getKey().producer(), _ -> java.util.EnumSet.noneOf(Stream.class))
                        .add(entry.getKey().stream());
            }
        }
        return memoized.entrySet().stream()
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
                        sourceOutcome.falseMask(allocator, allocationContext, mask),
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

        MaskOutcome optimized = tryEvaluatePrimitiveMaskOutcome(reference, mask);
        if (optimized != null) {
            return optimized;
        }

        Vector values = evaluate(reference, mask).get(reference.stream());
        BooleanVector errors = optionalBooleanStream(reference.producer(), Stream.ERRORS, mask);
        BooleanVector nulls = optionalBooleanStream(reference.producer(), Stream.NULLS, mask);
        return classifyBooleanMask(values, nulls, errors, mask);
    }

    private MaskOutcome classifyBooleanMask(Vector values, BooleanVector nulls, BooleanVector errors, Mask mask)
    {
        boolean[] nullData = nulls == null ? null : nulls.values();
        boolean[] errorData = errors == null ? null : errors.values();

        ClassificationCounts counts = countBooleanMaskOutcomes(values, nullData, errorData, mask);
        int[] truePositions = new int[counts.trueCount()];
        int[] nullPositions = new int[counts.nullCount()];
        int[] errorPositions = new int[counts.errorCount()];
        int trueCount = 0;
        int nullCount = 0;
        int errorCount = 0;

        for (int position : mask) {
            if (errorData != null && errorData[position]) {
                errorPositions[errorCount++] = position;
            }
            else if (nullData != null && nullData[position]) {
                nullPositions[nullCount++] = position;
            }
            else if (readBoolean(values, position)) {
                truePositions[trueCount++] = position;
            }
        }

        return new MaskOutcome(
                allocator.allocateSparseMask(allocationContext, truePositions, trueCount, mask.size()),
                allocator.allocateSparseMask(allocationContext, nullPositions, nullCount, mask.size()),
                allocator.allocateSparseMask(allocationContext, errorPositions, errorCount, mask.size()));
    }

    private Mask classifyTrueBooleanMask(Vector values, BooleanVector nulls, BooleanVector errors, Mask mask)
    {
        boolean[] nullData = nulls == null ? null : nulls.values();
        boolean[] errorData = errors == null ? null : errors.values();

        int trueCount = countTrueRows(values, nullData, errorData, mask);
        int[] truePositions = new int[trueCount];
        int outputIndex = 0;
        for (int position : mask) {
            if (errorData != null && errorData[position]) {
                continue;
            }
            if (nullData != null && nullData[position]) {
                continue;
            }
            if (readBoolean(values, position)) {
                truePositions[outputIndex++] = position;
            }
        }

        return allocator.allocateSparseMask(allocationContext, truePositions, outputIndex, mask.size());
    }

    private Mask classifyFalseBooleanMask(Vector values, BooleanVector nulls, BooleanVector errors, Mask mask)
    {
        boolean[] nullData = nulls == null ? null : nulls.values();
        boolean[] errorData = errors == null ? null : errors.values();

        int falseCount = countFalseRows(values, nullData, errorData, mask);
        int[] falsePositions = new int[falseCount];
        int outputIndex = 0;
        for (int position : mask) {
            if (errorData != null && errorData[position]) {
                continue;
            }
            if (nullData != null && nullData[position]) {
                continue;
            }
            if (!readBoolean(values, position)) {
                falsePositions[outputIndex++] = position;
            }
        }

        return allocator.allocateSparseMask(allocationContext, falsePositions, outputIndex, mask.size());
    }

    private ClassificationCounts countBooleanMaskOutcomes(Vector values, boolean[] nullData, boolean[] errorData, Mask mask)
    {
        int trueCount = 0;
        int nullCount = 0;
        int errorCount = 0;
        for (int position : mask) {
            if (errorData != null && errorData[position]) {
                errorCount++;
            }
            else if (nullData != null && nullData[position]) {
                nullCount++;
            }
            else if (readBoolean(values, position)) {
                trueCount++;
            }
        }
        return new ClassificationCounts(trueCount, nullCount, errorCount);
    }

    private int countTrueRows(Vector values, boolean[] nullData, boolean[] errorData, Mask mask)
    {
        int trueCount = 0;
        for (int position : mask) {
            if (errorData != null && errorData[position]) {
                continue;
            }
            if (nullData != null && nullData[position]) {
                continue;
            }
            if (readBoolean(values, position)) {
                trueCount++;
            }
        }
        return trueCount;
    }

    private int countFalseRows(Vector values, boolean[] nullData, boolean[] errorData, Mask mask)
    {
        int falseCount = 0;
        for (int position : mask) {
            if (errorData != null && errorData[position]) {
                continue;
            }
            if (nullData != null && nullData[position]) {
                continue;
            }
            if (!readBoolean(values, position)) {
                falseCount++;
            }
        }
        return falseCount;
    }

    private Mask tryEvaluatePrimitiveTrueMask(Reference reference, Mask mask)
    {
        return tryEvaluatePrimitiveMask(reference, mask, true);
    }

    private Mask tryEvaluatePrimitiveFalseMask(Reference reference, Mask mask)
    {
        return tryEvaluatePrimitiveMask(reference, mask, false);
    }

    private boolean tryEvaluatePrimitiveTrueMaskInPlace(Reference reference, Mask mask)
    {
        return tryEvaluatePrimitiveMaskInPlace(reference, mask, true);
    }

    private boolean tryEvaluatePrimitiveFalseMaskInPlace(Reference reference, Mask mask)
    {
        return tryEvaluatePrimitiveMaskInPlace(reference, mask, false);
    }

    private MaskOutcome tryEvaluatePrimitiveMaskOutcome(Reference reference, Mask mask)
    {
        PrimitiveMaskInvocation invocation = resolveMaskPrimitiveInvocation(reference, mask);
        if (invocation == null) {
            return null;
        }
        return invocation.function().tryEvaluateMaskOutcome(invocation.inputs(), mask, executionContext);
    }

    private Mask tryEvaluatePrimitiveMask(Reference reference, Mask mask, boolean selectTrue)
    {
        PrimitiveMaskInvocation invocation = resolveMaskPrimitiveInvocation(reference, mask);
        if (invocation == null) {
            return null;
        }
        return selectTrue
                ? invocation.function().tryEvaluateTrueMask(invocation.inputs(), mask, executionContext)
                : invocation.function().tryEvaluateFalseMask(invocation.inputs(), mask, executionContext);
    }

    private boolean tryEvaluatePrimitiveMaskInPlace(Reference reference, Mask mask, boolean selectTrue)
    {
        PrimitiveMaskInvocation invocation = resolveMaskPrimitiveInvocation(reference, mask);
        if (invocation == null) {
            return false;
        }
        return selectTrue
                ? invocation.function().tryEvaluateTrueMaskInPlace(invocation.inputs(), mask, executionContext)
                : invocation.function().tryEvaluateFalseMaskInPlace(invocation.inputs(), mask, executionContext);
    }

    private PrimitiveMaskInvocation resolveMaskPrimitiveInvocation(Reference reference, Mask mask)
    {
        if (reference.stream() != Stream.VALUES || !(reference.producer() instanceof Variable variable)) {
            return null;
        }

        Assignment assignment = assignments.get(variable);
        if (assignment == null || !(assignment.operation() instanceof Call call)) {
            return null;
        }

        PrimitiveFunction function;
        try {
            function = primitiveRegistry.get(call.name());
        }
        catch (IllegalArgumentException _) {
            return null;
        }
        if (!(function instanceof MaskEvaluablePrimitiveFunction maskFunction)) {
            return null;
        }

        List<Streams> inputs = new ArrayList<>(call.arguments().size());
        for (Reference argument : call.arguments()) {
            if (maskFunction.requiresCompletedInputCompanionStreamsForMask()) {
                inputs.add(function.requiresInputCompanionStreams() ? evaluateArgument(argument, mask) : evaluate(argument, mask));
                continue;
            }
            inputs.add(function.requiresInputCompanionStreams() ? evaluateAvailableArgument(argument, mask) : evaluate(argument, mask));
        }
        return new PrimitiveMaskInvocation(maskFunction, List.copyOf(inputs));
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

    private Streams evaluateAvailableArgument(Reference argument, Mask mask)
    {
        Streams result = evaluate(argument, mask);
        if (argument.stream() != Stream.VALUES) {
            return result;
        }

        return switch (argument.producer()) {
            case org.weakref.nitro.operator.evaluator.ir.Input(int index) -> {
                Vector nulls = input.resolve(new Reference(new org.weakref.nitro.operator.evaluator.ir.Input(index), Stream.NULLS), mask);
                Vector errors = input.resolve(new Reference(new org.weakref.nitro.operator.evaluator.ir.Input(index), Stream.ERRORS), mask);
                Streams available = result;
                if (nulls != null) {
                    available = available.with(Stream.NULLS, nulls);
                }
                if (errors != null) {
                    available = available.with(Stream.ERRORS, errors);
                }
                yield available;
            }
            case Variable variable -> {
                Assignment assignment = assignments.get(variable);
                if (assignment != null && assignment.operation() instanceof Literal) {
                    yield result;
                }
                yield evaluateArgument(argument, mask);
            }
        };
    }

    private static boolean readBoolean(Vector vector, int position)
    {
        return switch (vector) {
            case BooleanVector values -> values.values()[position];
            case DictionaryVector values -> readBoolean(values.values(), values.ids()[position]);
            case RleVector values -> readBoolean(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected boolean vector but found " + vector.getClass().getSimpleName());
        };
    }

    private BooleanVector optionalBooleanStream(org.weakref.nitro.operator.evaluator.ir.Producer producer, Stream stream, Mask mask)
    {
        Vector vector = evaluate(new Reference(producer, stream), mask).getOrNull(stream);
        if (vector == null) {
            return null;
        }
        if (vector instanceof BooleanVector booleanVector) {
            return booleanVector;
        }

        BooleanVector materialized = allocator.allocate(allocationContext, BooleanVector.class, vector.length(), BooleanVector::new);
        boolean[] values = materialized.values();
        for (int position = 0; position < vector.length(); position++) {
            values[position] = readBoolean(vector, position);
        }
        return materialized;
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
            Mask survivors = termOutcome.survivorsMask(allocator, allocationContext);
            if (survivors.none()) {
                return new MaskOutcome(emptyMask(mask.size()), emptyMask(mask.size()), emptyMask(mask.size()));
            }

            Mask survivingNulls = nullMask.none() ? emptyMask(mask.size()) : allocator.intersectMask(allocationContext, nullMask, survivors);
            Mask survivingErrors = errorMask.none() ? emptyMask(mask.size()) : allocator.intersectMask(allocationContext, errorMask, survivors);
            Mask nextErrorMask = unionMasks(survivingErrors, termOutcome.errorMask(), mask.size());
            Mask nextNullMask = unionMasks(survivingNulls, termOutcome.nullMask(), mask.size());
            nextNullMask = nextErrorMask.none() ? nextNullMask : allocator.differenceMask(allocationContext, nextNullMask, nextErrorMask);

            activeMask = survivors;
            nullMask = nextNullMask;
            errorMask = nextErrorMask;
        }
        Mask trueMask = subtractMasks(activeMask, nullMask, errorMask);
        return new MaskOutcome(trueMask, nullMask, errorMask);
    }

    private Mask evaluateAdaptiveAndTrueMask(List<MaskExpression> terms, Mask mask)
    {
        terms = orderTerms(terms, BooleanOperator.AND);

        Mask activeMask = mask;
        for (MaskExpression term : terms) {
            activeMask = evaluateMeasuredTrueMask(term, activeMask, BooleanOperator.AND);
            if (activeMask.none()) {
                return emptyMask(mask.size());
            }
        }
        return activeMask;
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
            remainingMask = termOutcome.trueMask().none() ? remainingMask : allocator.differenceMask(allocationContext, remainingMask, termOutcome.trueMask());
            if (remainingMask.none()) {
                return new MaskOutcome(acceptedMask, emptyMask(mask.size()), emptyMask(mask.size()));
            }

            Mask survivingNulls = nullMask.none() ? emptyMask(mask.size()) : allocator.intersectMask(allocationContext, nullMask, remainingMask);
            Mask survivingErrors = errorMask.none() ? emptyMask(mask.size()) : allocator.intersectMask(allocationContext, errorMask, remainingMask);
            Mask nextErrorMask = unionMasks(survivingErrors, termOutcome.errorMask(), mask.size());
            Mask nextNullMask = unionMasks(survivingNulls, termOutcome.nullMask(), mask.size());
            nextNullMask = nextErrorMask.none() ? nextNullMask : allocator.differenceMask(allocationContext, nextNullMask, nextErrorMask);
            nullMask = nextNullMask;
            errorMask = nextErrorMask;
        }

        return new MaskOutcome(acceptedMask, nullMask, errorMask);
    }

    private Mask evaluateAdaptiveOrTrueMask(List<MaskExpression> terms, Mask mask)
    {
        terms = orderTerms(terms, BooleanOperator.OR);

        Mask acceptedMask = emptyMask(mask.size());
        Mask remainingMask = mask;
        for (MaskExpression term : terms) {
            if (remainingMask.none()) {
                break;
            }

            Mask termTrueMask = evaluateMeasuredTrueMask(term, remainingMask, BooleanOperator.OR);
            acceptedMask = unionMasks(acceptedMask, termTrueMask, mask.size());
            remainingMask = termTrueMask.none() ? remainingMask : allocator.differenceMask(allocationContext, remainingMask, termTrueMask);
        }
        return acceptedMask;
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

    private Mask evaluateMeasuredTrueMask(MaskExpression term, Mask mask, BooleanOperator operator)
    {
        long start = System.nanoTime();
        Mask result = evaluateTrueMaskWithoutReordering(term, mask);
        long elapsed = System.nanoTime() - start;
        int decisiveRows = switch (operator) {
            case AND -> mask.selectedCount() - result.selectedCount();
            case OR -> result.selectedCount();
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
                        sourceOutcome.falseMask(allocator, allocationContext, mask),
                        sourceOutcome.nullMask(),
                        sourceOutcome.errorMask());
            }
            case AndMask(List<MaskExpression> terms) -> evaluateAdaptiveAnd(terms, mask);
            case OrMask(List<MaskExpression> terms) -> evaluateAdaptiveOr(terms, mask);
        };
    }

    private Mask evaluateTrueMaskWithoutReordering(MaskExpression expression, Mask mask)
    {
        return switch (expression) {
            case AllMask _ -> mask;
            case ReferenceMask(Reference reference) -> evaluateTrueReferenceMask(reference, mask);
            case NotMask(MaskExpression source) -> evaluateFalseMask(source, mask);
            case AndMask(List<MaskExpression> terms) -> evaluateAdaptiveAndTrueMask(terms, mask);
            case OrMask(List<MaskExpression> terms) -> evaluateAdaptiveOrTrueMask(terms, mask);
        };
    }

    private Mask evaluateTrueMaskInPlace(MaskExpression expression, Mask mask)
    {
        return switch (expression) {
            case AllMask _ -> mask;
            case ReferenceMask(Reference reference) -> {
                if (!tryEvaluatePrimitiveTrueMaskInPlace(reference, mask)) {
                    Mask result = evaluateTrueReferenceMask(reference, mask);
                    if (result != mask) {
                        mask.copyFrom(result);
                    }
                }
                yield mask;
            }
            case NotMask(MaskExpression source) -> {
                if (!evaluateFalseMaskInPlace(source, mask)) {
                    Mask result = evaluateFalseMask(source, mask);
                    if (result != mask) {
                        mask.copyFrom(result);
                    }
                }
                yield mask;
            }
            case AndMask(List<MaskExpression> terms) -> evaluateAdaptiveAndTrueMaskInPlace(terms, mask);
            case OrMask(List<MaskExpression> terms) -> {
                Mask result = evaluateAdaptiveOrTrueMask(terms, mask);
                if (result != mask) {
                    mask.copyFrom(result);
                }
                yield mask;
            }
        };
    }

    private boolean evaluateFalseMaskInPlace(MaskExpression expression, Mask mask)
    {
        return switch (expression) {
            case AllMask _ -> {
                mask.clear(mask.size());
                yield true;
            }
            case ReferenceMask(Reference reference) -> tryEvaluatePrimitiveFalseMaskInPlace(reference, mask);
            case NotMask(MaskExpression source) -> {
                evaluateTrueMaskInPlace(source, mask);
                yield true;
            }
            default -> false;
        };
    }

    private Mask evaluateAdaptiveAndTrueMaskInPlace(List<MaskExpression> terms, Mask mask)
    {
        terms = orderTerms(terms, BooleanOperator.AND);
        for (MaskExpression term : terms) {
            if (mask.none()) {
                break;
            }
            evaluateMeasuredTrueMaskInPlace(term, mask, BooleanOperator.AND);
        }
        return mask;
    }

    private Mask evaluateMeasuredTrueMaskInPlace(MaskExpression term, Mask mask, BooleanOperator operator)
    {
        int rowsBefore = mask.selectedCount();
        long start = System.nanoTime();
        evaluateTrueMaskInPlace(term, mask);
        long elapsed = System.nanoTime() - start;
        int decisiveRows = switch (operator) {
            case AND -> rowsBefore - mask.selectedCount();
            case OR -> mask.selectedCount();
        };
        maskTermStats.computeIfAbsent(term, _ -> new MaskTermStats()).record(rowsBefore, decisiveRows, elapsed, operator);
        return mask;
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
        return allocator.unionMask(allocationContext, left, right);
    }

    private Mask subtractMasks(Mask base, Mask first, Mask second)
    {
        Mask result = first.none() ? base : allocator.differenceMask(allocationContext, base, first);
        return second.none() ? result : allocator.differenceMask(allocationContext, result, second);
    }

    private Mask emptyMask(int size)
    {
        return allocator.allocateSparseMask(allocationContext, new int[0], size);
    }

    private enum BooleanOperator
    {
        AND,
        OR
    }

    private record IndexedTerm(int index, MaskExpression term) {}

    private record ClassificationCounts(int trueCount, int nullCount, int errorCount) {}

    private record DictionaryPeeling(int[] ids, Mask baseMask, List<Streams> inputs) {}

    private record PrimitiveMaskInvocation(MaskEvaluablePrimitiveFunction function, List<Streams> inputs) {}

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
}
