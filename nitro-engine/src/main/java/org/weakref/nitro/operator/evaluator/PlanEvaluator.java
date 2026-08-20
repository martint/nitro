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

import it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap;
import org.weakref.nitro.core.function.EncodedDomainReuse;
import org.weakref.nitro.core.function.mask.DirectMaskInputProvider;
import org.weakref.nitro.core.function.mask.MaskCodeProvider;
import org.weakref.nitro.core.function.mask.RangeConstraint;
import org.weakref.nitro.core.function.mask.SourceMaskOptimization;
import org.weakref.nitro.core.function.mask.SourceMaskOptimizationProvider;
import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.source.LongDomain;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.ErrorValue;
import org.weakref.nitro.data.ErrorVector;
import org.weakref.nitro.data.ErrorVectors;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.data.VectorAllocator;
import org.weakref.nitro.function.scalar.MaskEvaluablePrimitiveFunction;
import org.weakref.nitro.function.scalar.MaskOutcome;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.jit.ProjectionMaskCompiler;
import org.weakref.nitro.operator.EvaluationOperatorPolicy;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.AndMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.Construct;
import org.weakref.nitro.operator.evaluator.ir.Copy;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.LongDomainMask;
import org.weakref.nitro.operator.evaluator.ir.MaskExpression;
import org.weakref.nitro.operator.evaluator.ir.MaskExpressionResolver;
import org.weakref.nitro.operator.evaluator.ir.MemoizationPolicy;
import org.weakref.nitro.operator.evaluator.ir.Merge;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.OrMask;
import org.weakref.nitro.operator.evaluator.ir.Producer;
import org.weakref.nitro.operator.evaluator.ir.RangeConstrainedAndMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Sequence;
import org.weakref.nitro.operator.evaluator.ir.StreamPlan;
import org.weakref.nitro.operator.evaluator.ir.StructField;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class PlanEvaluator
{
    private final Allocator.Context allocationContext;
    private static final Set<Stream> VALUES_ONLY = Set.of(Stream.VALUES);
    private static final Set<Stream> NULLS_ONLY = Set.of(Stream.NULLS);
    private static final Set<Stream> ERRORS_ONLY = Set.of(Stream.ERRORS);
    private static final byte UNKNOWN_DICTIONARY_MATCH = 0;
    private static final byte DICTIONARY_MISMATCH = 1;
    private static final byte DICTIONARY_MATCH = 2;

    private final EvaluationPlan plan;
    private final EvaluationOperatorPolicy policy;
    private final PrimitiveRegistry primitiveRegistry;
    private final InputResolver input;
    private final Allocator allocator;
    private final VectorAllocator vectorAllocator;
    private final PrimitiveExecutionContext executionContext;
    private final Map<Variable, Assignment> assignments;
    private final Map<Variable, BoundSourceMaskOptimization> sourceMaskOptimizations;
    private final Map<Variable, PreboundMask> preboundMasks;
    private final Set<Allocator.Context> primitiveAllocationContexts;
    private final Set<org.weakref.nitro.operator.evaluator.ir.Producer> memoizedProducers;
    private final Map<Producer, Set<Stream>> explicitProjectedStreamsByProducer;
    private final Map<Producer, Set<Stream>> projectedStreamsByProducer;
    private final boolean requireProjectedCompanionStreams;
    private final Map<Producer, Set<Stream>> memoizedStreamsByProducer;
    private final Map<Reference, Set<Stream>> requestedStreamsByReference = new HashMap<>();
    private final Map<Reference, Set<Stream>> hardRequestedStreamsByReference = new HashMap<>();
    private final Map<Reference, Streams> memoizedStreams = new HashMap<>();
    private final Map<Reference, Mask> memoizedMasks = new HashMap<>();
    private final Map<MaskExpression, MaskTermStats> maskTermStats = new HashMap<>();
    private final Map<Call, ArrayList<Streams>> callInputFrames = new IdentityHashMap<>();
    // A deterministic transform over an immutable dictionary domain remains valid when a later batch presents a
    // different row-id mapping over the same domain. Keep only the last domain per call site, mirroring the bounded
    // reuse window of a dictionary-aware projection without teaching the evaluator any function semantics.
    private final Map<Call, ArrayList<DictionaryDomainCacheEntry>> dictionaryDomainCache = new IdentityHashMap<>();
    private final Allocator.Context dictionaryDomainCacheContext = new Allocator.Context("PlanEvaluator.dictionaryDomainCache");
    // Dictionary peeling walks the same row-id mapping for every node in a projected expression DAG. Cache its
    // logical base cardinality for this evaluation cycle; the ids buffer can be recycled with different contents in
    // the next batch, so reset()/resetForReuse() must clear the cache.
    private final Map<int[], DictionaryIdsMetadata> dictionaryIdsMetadata = new IdentityHashMap<>();
    // Results peeled through an input dictionary can borrow that immutable row mapping while the input batch is live.
    // ProjectOperator calls prepareResultForTransfer before a result escapes the batch, which copies only then.
    private final Set<Vector> borrowedDictionaryResults = Collections.newSetFromMap(new IdentityHashMap<>());
    // Cached bases are evaluator-owned beyond the current batch. A result that escapes must copy that base instead
    // of transferring it out of the cache context.
    private final Set<Vector> cachedDictionaryResults = Collections.newSetFromMap(new IdentityHashMap<>());
    private TermOrderFrames termOrderFrames;
    private final ArrayList<Streams> maskInvocationInputs = new ArrayList<>();
    private final PrimitiveMaskInvocation maskInvocation = new PrimitiveMaskInvocation(maskInvocationInputs);
    private long sourceMaskSuccesses;
    private long compiledMaskAttempts;
    private long compiledMaskSuccesses;
    private long compiledMaskFallbacks;
    private long directPreboundMaskSuccesses;
    private long primitiveMaskAttempts;
    private long primitiveMaskSuccesses;
    private long primitiveMaskFallbacks;
    private long materializedMaskFallbacks;
    private long dictionaryDomainCacheHits;
    private long dictionaryDomainCacheMisses;
    private long dictionaryDomainCacheBypasses;
    private long dictionaryDomainCacheChanges;
    private long dictionaryDomainCacheOversizedBypasses;
    private long dictionaryDomainCacheUnstableBypasses;
    private long dictionaryDomainCacheStaticBypasses;

    public record MaskExecutionDiagnostics(
            int plannedAssignments,
            int sourceMaskOptimizations,
            int preboundMasks,
            int compiledPreboundMasks,
            long sourceMaskSuccesses,
            long compiledMaskAttempts,
            long compiledMaskSuccesses,
            long compiledMaskFallbacks,
            long directPreboundMaskSuccesses,
            long primitiveMaskAttempts,
            long primitiveMaskSuccesses,
            long primitiveMaskFallbacks,
            long materializedMaskFallbacks,
            long dictionaryDomainCacheHits,
            long dictionaryDomainCacheMisses,
            long dictionaryDomainCacheBypasses,
            long dictionaryDomainCacheChanges,
            long dictionaryDomainCacheOversizedBypasses,
            long dictionaryDomainCacheUnstableBypasses,
            long dictionaryDomainCacheStaticBypasses) {}

    @FunctionalInterface
    public interface InputResolver
    {
        Vector resolve(Reference reference, Mask mask);

        default Mask resolveMask(Reference reference, Mask mask, boolean selectTrue, Allocator allocator, Allocator.Context allocationContext)
        {
            return null;
        }
    }

    public PlanEvaluator(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            InputResolver input,
            Allocator allocator,
            ProjectionMaskCompiler projectionMaskCompiler,
            EvaluationOperatorPolicy policy)
    {
        this(plan, primitiveRegistry, input, allocator, new Allocator.Context("PlanEvaluator"), false, projectionMaskCompiler, policy);
    }

    public PlanEvaluator(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            InputResolver input,
            Allocator allocator,
            ProjectionMaskCompiler projectionMaskCompiler,
            EvaluationOperatorPolicy policy,
            Object poolGroup,
            boolean requireProjectedCompanionStreams)
    {
        this(plan, primitiveRegistry, input, allocator, new Allocator.Context("PlanEvaluator", poolGroup), requireProjectedCompanionStreams, projectionMaskCompiler, policy);
    }

    private PlanEvaluator(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            InputResolver input,
            Allocator allocator,
            Allocator.Context allocationContext,
            boolean requireProjectedCompanionStreams,
            ProjectionMaskCompiler projectionMaskCompiler,
            EvaluationOperatorPolicy policy)
    {
        this.allocationContext = allocationContext;
        this.plan = plan;
        this.policy = requireNonNull(policy, "policy is null");
        this.primitiveRegistry = primitiveRegistry;
        this.input = input;
        this.allocator = allocator;
        this.vectorAllocator = allocator.vectorAllocator(allocationContext);
        this.executionContext = new PrimitiveExecutionContext(allocator);
        this.assignments = indexAssignments(plan.assignments());
        registerResolvedCalls(plan, primitiveRegistry);
        this.sourceMaskOptimizations = bindSourceMaskOptimizations(assignments, primitiveRegistry);
        this.preboundMasks = bindPreboundMasks(
                plan,
                primitiveRegistry,
                requireNonNull(projectionMaskCompiler, "projectionMaskCompiler is null"),
                assignments);
        this.primitiveAllocationContexts = primitiveAllocationContexts(plan, primitiveRegistry);
        this.memoizedProducers = memoizedProducers(plan.streamPlans());
        this.explicitProjectedStreamsByProducer = streamsByProducer(plan.outputs());
        this.projectedStreamsByProducer = projectedStreamsByProducer(plan.outputs());
        this.requireProjectedCompanionStreams = requireProjectedCompanionStreams;
        this.memoizedStreamsByProducer = memoizedStreamsByProducer(plan.streamPlans());
    }

    private static Map<Variable, BoundSourceMaskOptimization> bindSourceMaskOptimizations(
            Map<Variable, Assignment> assignments,
            PrimitiveRegistry primitiveRegistry)
    {
        Map<Variable, BoundSourceMaskOptimization> bindings = new HashMap<>();
        for (Map.Entry<Variable, Assignment> entry : assignments.entrySet()) {
            if (!(entry.getValue().operation() instanceof Call call)) {
                continue;
            }
            SourceMaskOptimizationProvider provider =
                    primitiveRegistry.capabilityOrNull(call, SourceMaskOptimizationProvider.class);
            if (provider == null) {
                continue;
            }
            SourceMaskOptimization optimization =
                    provider.bind(new EvaluatorFunctionCallSite(call, assignments, primitiveRegistry)).orElse(null);
            if (optimization == null) {
                continue;
            }
            Reference source = resolveArgumentPath(call, optimization.sourceArgumentPath(), assignments);
            if (source != null) {
                bindings.put(entry.getKey(), new BoundSourceMaskOptimization(source, optimization.predicate()));
            }
        }
        return Map.copyOf(bindings);
    }

    private static Reference resolveArgumentPath(Call root, List<Integer> path, Map<Variable, Assignment> assignments)
    {
        Call call = root;
        Reference reference = null;
        for (int depth = 0; depth < path.size(); depth++) {
            int argument = path.get(depth);
            if (argument < 0 || argument >= call.arguments().size()) {
                return null;
            }
            reference = call.arguments().get(argument);
            if (depth + 1 == path.size()) {
                return reference;
            }
            if (!(reference.producer() instanceof Variable variable)) {
                return null;
            }
            Assignment assignment = assignments.get(variable);
            if (assignment == null || !(assignment.operation() instanceof Call nestedCall)) {
                return null;
            }
            call = nestedCall;
        }
        return reference;
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

    public MaskExecutionDiagnostics maskExecutionDiagnostics()
    {
        return new MaskExecutionDiagnostics(
                assignments.size(),
                sourceMaskOptimizations.size(),
                preboundMasks.size(),
                (int) preboundMasks.values().stream().filter(CompiledPreboundMask.class::isInstance).count(),
                sourceMaskSuccesses,
                compiledMaskAttempts,
                compiledMaskSuccesses,
                compiledMaskFallbacks,
                directPreboundMaskSuccesses,
                primitiveMaskAttempts,
                primitiveMaskSuccesses,
                primitiveMaskFallbacks,
                materializedMaskFallbacks,
                dictionaryDomainCacheHits,
                dictionaryDomainCacheMisses,
                dictionaryDomainCacheBypasses,
                dictionaryDomainCacheChanges,
                dictionaryDomainCacheOversizedBypasses,
                dictionaryDomainCacheUnstableBypasses,
                dictionaryDomainCacheStaticBypasses);
    }

    public void reset()
    {
        memoizedMasks.clear();
        memoizedStreams.clear();
        dictionaryIdsMetadata.clear();
        borrowedDictionaryResults.clear();
        cachedDictionaryResults.clear();
        // Drop, rather than pool, every buffer produced during this evaluation cycle. These contexts
        // hold the result vectors handed back to callers (e.g. projected scalar outputs, literal RLEs,
        // mask scratch). A produced result can still be referenced by a consumer once the evaluation
        // is reset for the next constrain()/re-evaluation pass; returning such a buffer to a reuse pool
        // would let a subsequent borrow overwrite a value the consumer still holds, silently aliasing
        // independent results. Discarding leaves the buffers to GC, which is the only lifetime that is
        // provably safe here.
        for (Allocator.Context context : primitiveAllocationContexts) {
            allocator.discardAllIfPresent(context);
        }
        for (Allocator.Context context : executionContext.allocationContexts()) {
            allocator.discardAllIfPresent(context);
        }
        allocator.discardAllIfPresent(allocationContext);
    }

    /**
     * Ends an evaluation cycle after every result exposed to the caller has already been released or taken.
     * Unlike {@link #reset()}, this returns the remaining evaluator-owned scratch and intermediate vectors to their
     * pools. Calling this while a returned result is still borrowed would permit a later evaluation to overwrite it.
     */
    public void resetForReuse()
    {
        memoizedMasks.clear();
        memoizedStreams.clear();
        dictionaryIdsMetadata.clear();
        borrowedDictionaryResults.clear();
        cachedDictionaryResults.clear();
        for (Allocator.Context context : primitiveAllocationContexts) {
            allocator.releaseIfPresent(context);
        }
        for (Allocator.Context context : executionContext.allocationContexts()) {
            allocator.releaseIfPresent(context);
        }
        allocator.releaseIfPresent(allocationContext);
    }

    /** Releases a borrowed result only from contexts owned by this evaluator, leaving borrowed input vectors alone. */
    public void release(Vector vector)
    {
        for (Allocator.Context context : primitiveAllocationContexts) {
            allocator.release(context, vector);
        }
        for (Allocator.Context context : executionContext.allocationContexts()) {
            allocator.release(context, vector);
        }
        allocator.release(allocationContext, vector);
    }

    /** Makes a borrowed dictionary mapping self-contained before an evaluator result escapes its input batch. */
    public Vector prepareResultForTransfer(Vector vector)
    {
        if (!(vector instanceof DictionaryVector dictionary) || !borrowedDictionaryResults.remove(vector)) {
            return vector;
        }
        Vector values = dictionary.values();
        if (cachedDictionaryResults.remove(vector)) {
            values = values.copy(allocator, allocationContext);
        }
        return allocator.allocateDictionary(allocationContext, dictionary.ids(), dictionary.length(), values);
    }

    /** Releases evaluator-owned cross-batch dictionary domains. */
    public void close()
    {
        dictionaryDomainCache.clear();
        cachedDictionaryResults.clear();
        allocator.releaseIfPresent(dictionaryDomainCacheContext);
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
            case Construct construct -> evaluateConstruct(requestedStreamsFor(reference), construct, mask, output);
            case Merge merge -> evaluateMerge(requestedStreamsFor(reference), merge, mask, output);
            case Sequence sequence -> evaluateSequence(requestedStreamsFor(reference), sequence, mask, output);
            case StructField field -> evaluateStructField(requestedStreamsFor(reference), field, mask, output);
            default -> throw new IllegalArgumentException("Unsupported operation in normalized evaluator");
        };
    }

    private Streams evaluateLiteral(Set<Stream> requestedStreams, Literal literal, Mask mask)
    {
        Streams result = Streams.empty();
        int length = mask.size();
        if (requestedStreams.contains(Stream.VALUES)) {
            Vector values = literal.type()
                    .map(type -> typedLiteralValues(type, literal.value(), length))
                    .orElseGet(() -> untypedLiteralValues(literal.value(), length));
            result = Streams.ofValues(values);
        }
        if (literal.value() == null && requestedStreams.contains(Stream.NULLS)) {
            result = result.with(Stream.NULLS, fillBoolean(true, length));
        }
        return completeRequestedStreams(requestedStreams, result, mask);
    }

    private Vector typedLiteralValues(TypeBinding type, Object value, int length)
    {
        TypeVectorFactory factory = type.vectorFactory()
                .orElseThrow(() -> new IllegalArgumentException("Type does not provide vector construction: " + type.identity()));
        Vector vector = value == null
                ? factory.nullValues(vectorAllocator, length)
                : factory.constant(vectorAllocator, value, length);
        checkArgument(vector.length() == length, "Type vector factory returned length %s for requested length %s", vector.length(), length);
        checkArgument(type.supportsVector(vector), "Type %s does not support factory result %s", type.identity(), vector.getClass().getName());
        return vector;
    }

    private Vector untypedLiteralValues(Object value, int length)
    {
        return switch (value) {
            case Long longValue -> fillLongRle(longValue, length);
            case Double doubleValue -> fillDoubleRle(doubleValue, length);
            case Boolean booleanValue -> fillBoolean(booleanValue, length);
            case String stringValue -> fillUtf8(stringValue, length);
            default -> throw new IllegalArgumentException("Unsupported literal value: " + value);
        };
    }

    private Streams evaluateConstruct(Set<Stream> requestedStreams, Construct construct, Mask mask, Streams output)
    {
        int length = mask.size();
        List<Streams> arguments = construct.arguments().stream()
                .map(argument -> evaluateArgument(argument, mask))
                .toList();

        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.VALUES)) {
            Vector values = construct.type().vectorConstructor()
                    .orElseThrow(() -> new IllegalArgumentException("Type does not provide structural construction: " + construct.type().identity()))
                    .construct(vectorAllocator, arguments, length);
            checkArgument(
                    values.length() == length,
                    "Type vector constructor returned length %s for requested length %s",
                    values.length(),
                    length);
            checkArgument(
                    construct.type().supportsVector(values),
                    "Type %s does not support constructor result %s",
                    construct.type().identity(),
                    values.getClass().getName());
            result = Streams.ofValues(values);
        }

        if (requestedStreams.contains(Stream.ERRORS)) {
            Vector errors = null;
            for (Streams argument : arguments) {
                errors = mergeOptionalBooleanStreams(errors, argument.getOrNull(Stream.ERRORS), null, mask);
            }
            Vector existing = output == null ? null : output.getOrNull(Stream.ERRORS);
            if (existing != null) {
                errors = errors == null
                        ? fillFalseBoolean(existing, mask, length)
                        : copyVector(errors, existing, mask);
            }
            if (errors != null) {
                result = result.with(Stream.ERRORS, errors);
            }
        }
        return completeRequestedStreams(requestedStreams, result, mask);
    }

    private Streams evaluateCall(Reference reference, Call call, Mask mask, Streams output)
    {
        PrimitiveFunction function = primitiveRegistry.get(call.name());
        Set<Stream> requestedStreams = requestedStreamsFor(reference);
        Set<Stream> hardRequestedStreams = hardRequestedStreamsFor(reference);
        List<Streams> inputs;
        if (policy.recycleControlFrames()) {
            ArrayList<Streams> frame = callInputFrames.computeIfAbsent(call, _ -> new ArrayList<>(call.arguments().size()));
            frame.clear();
            inputs = frame;
        }
        else {
            inputs = new ArrayList<>(call.arguments().size());
        }
        for (int index = 0; index < call.arguments().size(); index++) {
            Reference argument = call.arguments().get(index);
            Set<Stream> hardRequiredInputStreams = requiredInputStreams(
                    function.requiredInputStreams(index, hardRequestedStreams),
                    hardRequestedStreams);
            Set<Stream> requiredInputStreams = requiredInputStreams(
                    function.requiredInputStreams(index, requestedStreams),
                    requestedStreams);
            inputs.add(evaluateArgument(
                    argument,
                    mask,
                    hardRequiredInputStreams,
                    requiredInputStreams));
        }
        // Both peels require a dictionary-encoded input to do anything, so skip the machinery entirely on the common
        // flat-input case with one cheap instanceof scan (rather than building and discarding a peeling per call).
        if (hasDictionaryValues(inputs)) {
            Streams peeledResult = tryEvaluateDictionaryPeeledCall(call, function, inputs, requestedStreams, mask);
            if (peeledResult == null && mask.all()) {
                peeledResult = tryEvaluatePropagatingNullsPeeledCall(function, inputs, requestedStreams);
            }
            if (peeledResult != null) {
                return completeRequestedStreams(requestedStreams, propagateInputErrors(requestedStreams, inputs, peeledResult, mask), mask);
            }
        }
        Streams result = function.apply(inputs, mask, requestedStreams, prepareOutput(output), executionContext);
        if (output != null || !hasOnlyKnownEmptyErrors(inputs, result)) {
            result = propagateInputErrors(requestedStreams, inputs, result, mask);
        }
        return completeRequestedStreams(requestedStreams, result, mask);
    }

    private boolean hasOnlyKnownEmptyErrors(List<Streams> inputs, Streams result)
    {
        Vector resultErrors = result.getOrNull(Stream.ERRORS);
        if (resultErrors != null && !allocator.isSharedAllFalseBoolean(resultErrors)) {
            return false;
        }
        for (Streams input : inputs) {
            Vector inputErrors = input.getOrNull(Stream.ERRORS);
            if (inputErrors != null && !allocator.isSharedAllFalseBoolean(inputErrors)) {
                return false;
            }
        }
        return true;
    }

    private static Set<Stream> requiredInputStreams(Set<Stream> functionRequiredStreams, Set<Stream> requestedOutputStreams)
    {
        if (!requestedOutputStreams.contains(Stream.ERRORS) || functionRequiredStreams.contains(Stream.ERRORS)) {
            return functionRequiredStreams;
        }
        java.util.EnumSet<Stream> required = java.util.EnumSet.noneOf(Stream.class);
        required.addAll(functionRequiredStreams);
        required.add(Stream.ERRORS);
        return Set.copyOf(required);
    }

    private Streams propagateInputErrors(Set<Stream> requestedStreams, List<Streams> inputs, Streams result, Mask mask)
    {
        if (!requestedStreams.contains(Stream.ERRORS)) {
            return result;
        }
        Vector errors = result.getOrNull(Stream.ERRORS);
        for (Streams input : inputs) {
            errors = mergeOptionalBooleanStreams(input.getOrNull(Stream.ERRORS), errors, null, mask);
        }
        return errors == null ? result : result.with(Stream.ERRORS, errors);
    }

    private static boolean hasDictionaryValues(List<Streams> inputs)
    {
        for (Streams input : inputs) {
            if (input.getOrNull(Stream.VALUES) instanceof DictionaryVector) {
                return true;
            }
        }
        return false;
    }

    private Streams tryEvaluateDictionaryPeeledCall(
            Call call,
            PrimitiveFunction function,
            List<Streams> inputs,
            Set<Stream> requestedStreams,
            Mask mask)
    {
        if (!function.deterministic()) {
            return null;
        }

        DictionaryPeeling peeling = tryBuildDictionaryPeeling(inputs, mask);
        if (peeling == null) {
            return null;
        }

        try {
            boolean cacheEnabled = primitiveRegistry.capabilityOrNull(call, EncodedDomainReuse.class) != null;
            DictionaryDomainCacheKey cacheKey = cacheEnabled ? dictionaryDomainCacheKey(call, peeling, requestedStreams) : null;
            if (cacheEnabled && cacheKey == null) {
                dictionaryDomainCacheBypasses++;
            }
            ArrayList<DictionaryDomainCacheEntry> cachedDomains = cacheKey == null ? null : dictionaryDomainCache.get(call);
            DictionaryDomainCacheEntry cached = findCachedDomain(cachedDomains, cacheKey);
            if (cached != null) {
                dictionaryDomainCacheHits++;
                return wrapDictionaryPeeledStreams(peeling.ids(), peeling.rowCount(), cached.result(), true);
            }
            if (cachedDomains != null && !cachedDomains.isEmpty()) {
                dictionaryDomainCacheChanges++;
            }
            if (cacheKey != null) {
                dictionaryDomainCacheMisses++;
            }
            Streams baseResult = function.apply(inputsForPeeling(peeling), peeling.baseMask(), requestedStreams, null, executionContext);
            if (cacheKey != null) {
                replaceDictionaryDomainCache(call, cacheKey, baseResult);
            }
            return wrapDictionaryPeeledStreams(peeling.ids(), peeling.rowCount(), baseResult, false);
        }
        finally {
            allocator.release(allocationContext, peeling.baseMask());
        }
    }

    private static List<Streams> inputsForPeeling(DictionaryPeeling peeling)
    {
        return peeling.inputs();
    }

    private DictionaryDomainCacheKey dictionaryDomainCacheKey(Call call, DictionaryPeeling peeling, Set<Stream> requestedStreams)
    {
        if (policy.dictionaryDomainCacheMaxEntries() == 0 || policy.dictionaryDomainCacheSlots() == 0 ||
                peeling.baseMask().size() > policy.dictionaryDomainCacheMaxEntries()) {
            dictionaryDomainCacheOversizedBypasses++;
            return null;
        }

        List<StableVectorKey> vectors = new ArrayList<>();
        boolean dynamic = false;
        for (int inputIndex = 0; inputIndex < peeling.inputs().size(); inputIndex++) {
            if (isStaticReference(call.arguments().get(inputIndex))) {
                continue;
            }
            dynamic = true;
            Streams input = peeling.inputs().get(inputIndex);
            for (Stream stream : Stream.values()) {
                Vector vector = input.getOrNull(stream);
                StableVectorKey key = stableVectorKey(vector);
                if (key == null) {
                    dictionaryDomainCacheUnstableBypasses++;
                    return null;
                }
                vectors.add(key);
            }
        }
        if (!dynamic) {
            dictionaryDomainCacheStaticBypasses++;
            return null;
        }
        return new DictionaryDomainCacheKey(List.copyOf(vectors), Set.copyOf(requestedStreams));
    }

    private boolean isStaticReference(Reference reference)
    {
        if (!(reference.producer() instanceof Variable variable)) {
            return false;
        }
        Assignment assignment = assignments.get(variable);
        if (assignment == null) {
            return false;
        }
        return switch (assignment.operation()) {
            case Literal _ -> true;
            case Copy(Reference source) -> isStaticReference(source);
            default -> false;
        };
    }

    private static StableVectorKey stableVectorKey(Vector vector)
    {
        if (vector == null) {
            return StableVectorKey.absent();
        }
        if (vector instanceof BooleanVector booleans) {
            if (booleans.isAllFalse()) {
                return StableVectorKey.constant(false);
            }
            if (booleans.isAllTrue()) {
                return StableVectorKey.constant(true);
            }
        }
        long generation = vector.contentGeneration();
        long fingerprint = vector.contentFingerprint();
        if ((!vector.contentImmutable() || generation < 0) && fingerprint == Vector.NO_CONTENT_FINGERPRINT) {
            return null;
        }
        return StableVectorKey.vector(vector, generation, fingerprint);
    }

    private void replaceDictionaryDomainCache(Call call, DictionaryDomainCacheKey key, Streams result)
    {
        ArrayList<DictionaryDomainCacheEntry> cachedDomains = dictionaryDomainCache.computeIfAbsent(call, _ -> new ArrayList<>());
        if (cachedDomains.size() == policy.dictionaryDomainCacheSlots()) {
            DictionaryDomainCacheEntry previous = cachedDomains.removeFirst();
            releaseCachedKey(previous.key());
            releaseCachedStreams(previous.result());
        }

        Streams.Builder copy = Streams.builder();
        for (Stream stream : result.streams()) {
            copy.put(stream, result.get(stream).copy(allocator, dictionaryDomainCacheContext).freezeContent());
        }
        cachedDomains.add(new DictionaryDomainCacheEntry(stabilizeDictionaryDomainCacheKey(key), copy.build()));
    }

    private static DictionaryDomainCacheEntry findCachedDomain(
            List<DictionaryDomainCacheEntry> cachedDomains,
            DictionaryDomainCacheKey key)
    {
        if (cachedDomains == null) {
            return null;
        }
        for (DictionaryDomainCacheEntry cached : cachedDomains) {
            if (cached.key().matches(key)) {
                return cached;
            }
        }
        return null;
    }

    private DictionaryDomainCacheKey stabilizeDictionaryDomainCacheKey(DictionaryDomainCacheKey key)
    {
        List<StableVectorKey> stable = new ArrayList<>(key.vectors().size());
        for (StableVectorKey vectorKey : key.vectors()) {
            if (vectorKey.kind() != StableVectorKey.VECTOR) {
                stable.add(vectorKey);
                continue;
            }
            if (vectorKey.fingerprint() == Vector.NO_CONTENT_FINGERPRINT) {
                stable.add(vectorKey);
                continue;
            }
            Vector copy = vectorKey.vector().copy(allocator, dictionaryDomainCacheContext).freezeContent();
            stable.add(StableVectorKey.vector(copy, copy.contentGeneration(), copy.contentFingerprint()));
        }
        return new DictionaryDomainCacheKey(List.copyOf(stable), key.requestedStreams());
    }

    private void releaseCachedKey(DictionaryDomainCacheKey key)
    {
        Set<Vector> released = Collections.newSetFromMap(new IdentityHashMap<>());
        for (StableVectorKey vectorKey : key.vectors()) {
            if (vectorKey.kind() == StableVectorKey.VECTOR &&
                    vectorKey.fingerprint() != Vector.NO_CONTENT_FINGERPRINT &&
                    released.add(vectorKey.vector())) {
                allocator.release(dictionaryDomainCacheContext, vectorKey.vector());
            }
        }
    }

    private void releaseCachedStreams(Streams streams)
    {
        Set<Vector> released = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Stream stream : streams.streams()) {
            Vector vector = streams.get(stream);
            if (released.add(vector)) {
                allocator.release(dictionaryDomainCacheContext, vector);
            }
        }
    }

    /**
     * Peels a dictionary-encoded call whose NULLS stream carries a different dictionary than its VALUES stream.
     * <p>
     * The strict peel ({@link #tryEvaluateDictionaryPeeledCall}) requires every stream of every input to share one
     * id array, so it bails when a low-cardinality string column reaches a projection with a VALUES dictionary
     * (composed over the string base by upstream joins) but a NULLS dictionary carrying its own ids over a boolean
     * base. For a {@linkplain PrimitiveFunction#propagatesNulls() strictly null-propagating} function the transform
     * depends only on the input values, so it can run over the distinct base values while the original NULLS stream
     * passes straight through — turning a per-row string transform over the whole batch into one over a handful of
     * distinct values.
     */
    private Streams tryEvaluatePropagatingNullsPeeledCall(PrimitiveFunction function, List<Streams> inputs, Set<Stream> requestedStreams)
    {
        if (!function.propagatesNulls()) {
            return null;
        }

        int[] sharedIds = null;
        int rowCount = -1;
        for (Streams inputStreams : inputs) {
            if (inputStreams.getOrNull(Stream.VALUES) instanceof DictionaryVector dictionary) {
                if (sharedIds == null) {
                    sharedIds = dictionary.ids();
                    rowCount = dictionary.length();
                }
                else if (dictionary.length() != rowCount || !sameDictionaryIds(sharedIds, dictionary.ids(), rowCount)) {
                    return null;
                }
            }
        }
        if (sharedIds == null) {
            return null;
        }

        int baseLength = dictionaryBaseLength(sharedIds, rowCount);
        if (dictionaryPeelTooSparse(rowCount, baseLength)) {
            return null;
        }

        List<Streams> baseInputs = new ArrayList<>(inputs.size());
        Vector passthroughNulls = null;
        for (Streams inputStreams : inputs) {
            Vector peeledValues = peelDictionaryCompatibleVector(inputStreams.getOrNull(Stream.VALUES), sharedIds, rowCount, baseLength);
            if (peeledValues == null) {
                return null;
            }
            baseInputs.add(Streams.ofValues(peeledValues));

            Vector nulls = inputStreams.getOrNull(Stream.NULLS);
            if (nulls != null && !VectorAccess.isAllFalseNulls(nulls)) {
                if (passthroughNulls != null) {
                    // More than one input contributes nulls; combining them at full length is out of scope here.
                    return null;
                }
                if (nulls.length() != rowCount) {
                    return null;
                }
                passthroughNulls = nulls;
            }
        }

        Mask baseMask = allocator.allocateAllMask(allocationContext, baseLength);
        Streams baseResult;
        try {
            baseResult = function.apply(baseInputs, baseMask, VALUES_ONLY, null, executionContext);
        }
        finally {
            allocator.release(allocationContext, baseMask);
        }

        Vector baseValues = baseResult.getOrNull(Stream.VALUES);
        if (baseValues == null) {
            return null;
        }

        Streams.Builder result = Streams.builder();
        result.put(Stream.VALUES, wrapBorrowedDictionary(sharedIds, rowCount, baseValues));
        if (passthroughNulls != null && requestedStreams.contains(Stream.NULLS)) {
            result.put(Stream.NULLS, passthroughNulls);
        }
        return result.build();
    }

    private DictionaryPeeling tryBuildDictionaryPeeling(List<Streams> inputs, Mask mask)
    {
        int[] sharedIds = null;
        int rowCount = -1;
        DictionaryVector mapping = null;
        for (Streams inputStreams : inputs) {
            Vector values = inputStreams.getOrNull(Stream.VALUES);
            if (values instanceof DictionaryVector dictionary) {
                if (sharedIds == null) {
                    sharedIds = dictionary.ids();
                    rowCount = dictionary.length();
                    mapping = dictionary;
                }
                else if (dictionary.length() != rowCount || !sameDictionaryIds(sharedIds, dictionary.ids(), rowCount)) {
                    return null;
                }
            }
        }
        if (sharedIds == null) {
            return null;
        }

        int baseLength = dictionaryBaseLength(sharedIds, rowCount);
        if (dictionaryPeelTooSparse(rowCount, baseLength)) {
            return null;
        }
        Mask baseMask = dictionaryDomainMask(mapping, baseLength, mask);

        List<Streams> peeledInputs = new ArrayList<>(inputs.size());
        for (Streams inputStreams : inputs) {
            Streams peeled = peelDictionaryCompatibleStreams(inputStreams, sharedIds, rowCount, baseLength);
            if (peeled == null) {
                allocator.release(allocationContext, baseMask);
                return null;
            }
            peeledInputs.add(peeled);
        }
        return new DictionaryPeeling(sharedIds, rowCount, baseMask, List.copyOf(peeledInputs));
    }

    /**
     * Translates a logical branch mask onto the shared dictionary domain. A function invoked conditionally can then
     * execute once per selected physical value without touching a value that belongs exclusively to the other
     * branch. The result is wrapped in the original id mapping, so downstream operators retain the encoding.
     */
    private Mask dictionaryDomainMask(DictionaryVector mapping, int baseLength, Mask logicalMask)
    {
        int[] ids = mapping.ids();
        int rowCount = mapping.length();
        checkArgument(logicalMask.size() == rowCount, "Logical mask size %s does not match dictionary length %s", logicalMask.size(), rowCount);
        if (logicalMask.all()) {
            return allocator.allocateAllMask(allocationContext, baseLength);
        }
        if (logicalMask.none()) {
            return allocator.allocateSparseMask(allocationContext, new int[0], baseLength);
        }

        Mask.DictionaryDomainSelection domainSelection = logicalMask.dictionaryDomainSelection(mapping);
        if (domainSelection != null && domainSelection.domainSize() == baseLength) {
            long selectedBits = domainSelection.selectedDomainBits();
            int[] positions = new int[Long.bitCount(selectedBits)];
            for (int domain = 0, index = 0; domain < baseLength; domain++) {
                if (((selectedBits >>> domain) & 1L) != 0) {
                    positions[index++] = domain;
                }
            }
            return allocator.allocateSparseMask(allocationContext, positions, positions.length, baseLength);
        }

        boolean[] selected = new boolean[baseLength];
        int selectedCount = 0;
        for (int position : logicalMask) {
            int domain = ids[position];
            if (!selected[domain]) {
                selected[domain] = true;
                selectedCount++;
            }
        }
        int[] positions = new int[selectedCount];
        for (int domain = 0, index = 0; domain < baseLength; domain++) {
            if (selected[domain]) {
                positions[index++] = domain;
            }
        }
        return allocator.allocateSparseMask(allocationContext, positions, selectedCount, baseLength);
    }

    private boolean dictionaryPeelTooSparse(int rowCount, int baseLength)
    {
        return (long) baseLength > (long) rowCount * policy.dictionaryPeelSparseRatio();
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
        if (vector instanceof BooleanVector booleans && booleans.length() == rowCount) {
            // BooleanVector caches constant-content classification. Null and error streams produced as
            // known all-false vectors therefore stay O(1) here instead of being rescanned for every batch.
            if (booleans.isAllFalse()) {
                return fillBoolean(false, baseLength);
            }
            if (booleans.isAllTrue()) {
                return fillBoolean(true, baseLength);
            }
            return null;
        }
        return switch (vector) {
            case DictionaryVector dictionary when dictionary.length() == rowCount && dictionary.values().length() >= baseLength && sameDictionaryIds(sharedIds, dictionary.ids(), rowCount) -> dictionary.values();
            case RleVector rle when rle.counts().length == 1 -> executionContext.allocator().allocateSingleRunRle(allocationContext, baseLength, rle.values());
            default -> null;
        };
    }

    private int dictionaryBaseLength(int[] ids, int length)
    {
        DictionaryIdsMetadata metadata = dictionaryIdsMetadata.get(ids);
        if (metadata != null && metadata.length() == length) {
            return metadata.baseLength();
        }
        int baseLength = 0;
        for (int index = 0; index < length; index++) {
            baseLength = Math.max(baseLength, ids[index] + 1);
        }
        dictionaryIdsMetadata.put(ids, new DictionaryIdsMetadata(length, baseLength));
        return baseLength;
    }

    private static boolean sameDictionaryIds(int[] left, int[] right, int length)
    {
        if (left == right) {
            return true;
        }
        for (int index = 0; index < length; index++) {
            if (left[index] != right[index]) {
                return false;
            }
        }
        return true;
    }

    private record DictionaryIdsMetadata(int length, int baseLength) {}

    private record EncodedMergeCondition(DictionaryVector mapping, byte[] choices) {}

    private Streams wrapDictionaryPeeledStreams(int[] sharedIds, int rowCount, Streams streams)
    {
        return wrapDictionaryPeeledStreams(sharedIds, rowCount, streams, false);
    }

    private Streams wrapDictionaryPeeledStreams(int[] sharedIds, int rowCount, Streams streams, boolean cached)
    {
        Streams.Builder wrapped = Streams.builder();
        for (Stream stream : streams.streams()) {
            DictionaryVector dictionary = wrapBorrowedDictionary(sharedIds, rowCount, streams.get(stream));
            if (cached) {
                cachedDictionaryResults.add(dictionary);
            }
            wrapped.put(stream, dictionary);
        }
        return wrapped.build();
    }

    private DictionaryVector wrapBorrowedDictionary(int[] ids, int length, Vector values)
    {
        DictionaryVector dictionary = allocator.adopt(allocationContext, DictionaryVector.wrap(ids, length, values));
        borrowedDictionaryResults.add(dictionary);
        return dictionary;
    }

    private Streams evaluateArgument(Reference argument, Mask mask)
    {
        return evaluateArgument(argument, mask, PrimitiveFunction.ALL_INPUT_STREAMS, false);
    }

    private Streams evaluateArgument(Reference argument, Mask mask, Set<Stream> requiredStreams, boolean allowAvailableCompanionStreams)
    {
        if (requiredStreams.isEmpty()) {
            return Streams.empty();
        }

        // Mask-specialized primitives commonly consume VALUES plus any physically present NULLS/ERRORS streams
        // directly from an input column. Resolve that tuple in one pass instead of routing each stream through
        // evaluate(), constructing an intermediate Streams object, and then repacking it into another Streams.
        if (allowAvailableCompanionStreams && argument.producer() instanceof org.weakref.nitro.operator.evaluator.ir.Input) {
            Vector values = null;
            Vector nulls = null;
            Vector errors = null;
            for (Stream stream : requiredStreams) {
                Reference requestedReference = remapReference(argument, stream);
                if (requestedReference == null) {
                    continue;
                }
                Vector vector = input.resolve(requestedReference, mask);
                switch (stream) {
                    case VALUES -> values = vector;
                    case NULLS -> nulls = vector;
                    case ERRORS -> errors = vector;
                }
            }
            return Streams.of(values, nulls, errors);
        }

        Streams.Builder result = Streams.builder();
        for (Stream stream : requiredStreams) {
            Reference requestedReference = remapReference(argument, stream);
            if (requestedReference == null) {
                continue;
            }

            // The argument's primary stream carries the value being consumed and must cover the entire invocation
            // mask. Only companion streams may be reused opportunistically: a derived VALUES stream cached from a
            // narrower branch mask contains unwritten lanes and is not a valid input to a direct-mask primitive.
            Streams streams = allowAvailableCompanionStreams && stream != argument.stream()
                    ? evaluateAvailableReference(requestedReference, mask)
                    : evaluate(requestedReference, mask);
            if (streams.has(requestedReference.stream())) {
                result.put(stream, streams.get(requestedReference.stream()));
            }
        }
        return result.build();
    }

    private Streams evaluateArgument(Reference argument, Mask mask, Set<Stream> hardRequiredStreams, Set<Stream> requiredStreams)
    {
        if (requiredStreams.isEmpty()) {
            return Streams.empty();
        }

        Streams.Builder result = Streams.builder();
        for (Stream stream : requiredStreams) {
            Reference requestedReference = remapReference(argument, stream);
            if (requestedReference == null) {
                continue;
            }

            Streams streams = hardRequiredStreams.contains(stream)
                    ? evaluate(requestedReference, mask)
                    : evaluateAvailableReference(requestedReference, mask);
            if (streams.has(requestedReference.stream())) {
                result.put(stream, streams.get(requestedReference.stream()));
            }
        }
        return result.build();
    }

    private Set<Stream> requestedStreamsFor(Reference reference)
    {
        return requestedStreamsByReference.computeIfAbsent(reference, this::computeRequestedStreams);
    }

    private Set<Stream> hardRequestedStreamsFor(Reference reference)
    {
        return hardRequestedStreamsByReference.computeIfAbsent(reference, this::computeHardRequestedStreams);
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
        if (output == null && mask.all()) {
            Streams encoded = tryEvaluateDictionaryDomainMerge(requestedStreams, merge, mask);
            if (encoded != null) {
                return completeRequestedStreams(requestedStreams, encoded, mask);
            }
        }

        MaskOutcome conditionOutcome = evaluateMaskOutcome(merge.condition(), mask);
        Mask trueMask = conditionOutcome.trueMask();
        Mask falseMask = allocator.differenceMask(allocationContext, mask, trueMask);
        if (!conditionOutcome.errorMask().none()) {
            falseMask = allocator.differenceMask(allocationContext, falseMask, conditionOutcome.errorMask());
        }

        Streams.Builder result = Streams.builder();
        for (Stream stream : requestedStreams) {
            Vector merged = evaluateMergeStream(stream, merge, mask, trueMask, falseMask, output);
            if ((stream == Stream.NULLS || stream == Stream.ERRORS) && !conditionOutcome.errorMask().none()) {
                merged = fillFalseBoolean(merged, conditionOutcome.errorMask(), mask.size());
            }
            if (stream == Stream.ERRORS && !conditionOutcome.errorMask().none()) {
                Vector conditionErrors = materializeMaskErrors(
                        evaluateMaskExpressionErrors(merge.condition(), conditionOutcome.errorMask()),
                        conditionOutcome.errorMask(),
                        mask.size());
                merged = mergeOptionalBooleanStreams(conditionErrors, merged, null, mask);
            }
            if (merged != null) {
                result.put(stream, merged);
            }
        }
        return completeRequestedStreams(requestedStreams, result.build(), mask);
    }

    /**
     * Evaluates a conditional directly over one shared physical dictionary domain. This is the encoded equivalent
     * of branch masks: the condition selects true/false entries in the physical domain, and each logical row keeps
     * referring to the selected result through the original id mapping. Calls that have not already been evaluated
     * remain conditional; only inputs, literals, copies, nested merges, and fully memoized branch results are safe to
     * inspect without executing an unselected function.
     */
    private Streams tryEvaluateDictionaryDomainMerge(Set<Stream> requestedStreams, Merge merge, Mask mask)
    {
        EncodedMergeCondition condition = encodedMergeCondition(merge.condition(), mask, false);
        if (condition == null) {
            return null;
        }

        Mask trueMask = encodedMergeBranchMask(condition, (byte) 1);
        // The encoded condition is exhaustive: every domain entry selects exactly one branch. Preserve the first
        // mask's compact domain histogram and derive the second in O(domain cardinality), rather than rescanning the
        // full logical dictionary mapping for both branches.
        Mask falseMask = allocator.complementMask(allocationContext, trueMask);
        Map<Stream, Vector> mergedDomains = new HashMap<>();
        for (Stream stream : requestedStreams) {
            Reference trueReference = remapReference(merge.whenTrue(), stream);
            Reference falseReference = remapReference(merge.whenFalse(), stream);
            Vector trueVector = tryEvaluateMergeBranchVector(
                    trueReference,
                    safeToEvaluateEncodedMergeBranch(trueReference, mask) ? mask : trueMask);
            Vector falseVector = tryEvaluateMergeBranchVector(
                    falseReference,
                    safeToEvaluateEncodedMergeBranch(falseReference, mask) ? mask : falseMask);
            if (stream == Stream.VALUES && (trueVector == null || falseVector == null)) {
                releaseMergedDomains(mergedDomains.values());
                return null;
            }
            Vector domain = mergeDictionaryDomains(stream, condition, trueVector, falseVector);
            if (domain == null) {
                releaseMergedDomains(mergedDomains.values());
                return null;
            }
            mergedDomains.put(stream, domain);
        }
        Streams.Builder result = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : mergedDomains.entrySet()) {
            result.put(entry.getKey(), wrapBorrowedDictionary(condition.mapping().ids(), mask.size(), entry.getValue()));
        }
        return result.build();
    }

    private Vector tryEvaluateMergeBranchVector(Reference reference, Mask branchMask)
    {
        if (reference == null) {
            return null;
        }
        Streams streams = evaluate(reference, branchMask);
        return streams.getOrNull(reference.stream());
    }

    private Mask encodedMergeBranchMask(EncodedMergeCondition condition, byte branch)
    {
        boolean[] selected = new boolean[condition.choices().length];
        for (int domain = 0; domain < selected.length; domain++) {
            selected[domain] = condition.choices()[domain] == branch;
        }
        Mask mask = allocator.allocateAllMask(allocationContext, condition.mapping().length());
        mask.retainDictionaryComparison(condition.mapping().ids(), selected);
        return mask;
    }

    private EncodedMergeCondition encodedMergeCondition(MaskExpression expression, Mask mask, boolean inverted)
    {
        if (expression instanceof NotMask(MaskExpression source)) {
            return encodedMergeCondition(source, mask, !inverted);
        }
        if (!(expression instanceof ReferenceMask(Reference reference))) {
            return null;
        }

        MaskExpression resolved = MaskExpressionResolver.resolve(plan, expression);
        if (!resolved.equals(expression)) {
            return encodedMergeCondition(resolved, mask, inverted);
        }
        if (reference.producer() instanceof Variable variable) {
            if (sourceMaskOptimizations.containsKey(variable)) {
                return null;
            }
            Assignment assignment = assignments.get(variable);
            if (assignment != null && assignment.operation() instanceof Call call &&
                    !primitiveRegistry.contains(call.name())) {
                return null;
            }
        }

        Streams streams = evaluate(reference, mask);
        if (!(streams.getOrNull(reference.stream()) instanceof DictionaryVector dictionary) ||
                dictionary.length() != mask.size()) {
            return null;
        }
        // NULLS and ERRORS are companions only to a semantic VALUES reference. A ReferenceMask over a physical
        // NULLS/ERRORS stream treats that stream itself as the boolean value; it is not its own null/error companion.
        Vector nulls = reference.stream() == Stream.VALUES ? streams.getOrNull(Stream.NULLS) : null;
        Vector errors = reference.stream() == Stream.VALUES ? streams.getOrNull(Stream.ERRORS) : null;
        if (!encodedDomainBooleanCompatible(nulls, dictionary) ||
                !encodedDomainBooleanCompatible(errors, dictionary)) {
            return null;
        }

        int domainSize = dictionary.values().length();
        byte[] choices = new byte[domainSize];
        for (int domain = 0; domain < domainSize; domain++) {
            if (readEncodedDomainBoolean(errors, dictionary, domain)) {
                return null;
            }
            boolean selected = !readEncodedDomainBoolean(nulls, dictionary, domain) &&
                    readBoolean(dictionary.values(), domain);
            choices[domain] = (byte) ((inverted ? !selected : selected) ? 1 : 2);
        }
        return new EncodedMergeCondition(dictionary, choices);
    }

    private boolean safeToEvaluateEncodedMergeBranch(Reference reference, Mask mask)
    {
        if (reference == null || reference.producer() instanceof org.weakref.nitro.operator.evaluator.ir.Input) {
            return true;
        }
        Mask existingMask = memoizedMasks.get(reference);
        if (existingMask != null && existingMask.containsAll(mask)) {
            return true;
        }
        if (!(reference.producer() instanceof Variable variable)) {
            return false;
        }
        Assignment assignment = assignments.get(variable);
        if (assignment == null) {
            return false;
        }
        return switch (assignment.operation()) {
            case Literal _ -> true;
            case Copy(Reference source) -> safeToEvaluateEncodedMergeBranch(remapReference(source, reference.stream()), mask);
            case Merge _ -> true;
            default -> false;
        };
    }

    private Vector mergeDictionaryDomains(
            Stream stream,
            EncodedMergeCondition condition,
            Vector trueVector,
            Vector falseVector)
    {
        DictionaryVector mapping = condition.mapping();
        int rowCount = mapping.length();
        if (!encodedMergeBranchCompatible(trueVector, mapping, rowCount) ||
                !encodedMergeBranchCompatible(falseVector, mapping, rowCount)) {
            return null;
        }

        byte[] choices = condition.choices();
        int trueCount = 0;
        int falseCount = 0;
        for (byte choice : choices) {
            trueCount += choice == 1 ? 1 : 0;
            falseCount += choice == 2 ? 1 : 0;
        }
        int[] truePositions = new int[trueCount];
        int[] falsePositions = new int[falseCount];
        for (int domain = 0, trueIndex = 0, falseIndex = 0; domain < choices.length; domain++) {
            if (choices[domain] == 1) {
                truePositions[trueIndex++] = domain;
            }
            else if (choices[domain] == 2) {
                falsePositions[falseIndex++] = domain;
            }
        }

        Mask trueDomain = allocator.allocateSparseMask(allocationContext, truePositions, trueCount, choices.length);
        Mask falseDomain = allocator.allocateSparseMask(allocationContext, falsePositions, falseCount, choices.length);
        try {
            boolean trueFirst = trueVector instanceof DictionaryVector;
            Vector target = null;
            if (trueFirst) {
                target = copyDictionaryMergeDomain(stream, trueVector, trueDomain, choices.length, target);
                target = copyDictionaryMergeDomain(stream, falseVector, falseDomain, choices.length, target);
            }
            else {
                target = copyDictionaryMergeDomain(stream, falseVector, falseDomain, choices.length, target);
                target = copyDictionaryMergeDomain(stream, trueVector, trueDomain, choices.length, target);
            }
            return target;
        }
        finally {
            allocator.release(allocationContext, trueDomain);
            allocator.release(allocationContext, falseDomain);
        }
    }

    private static boolean encodedDomainBooleanCompatible(Vector vector, DictionaryVector mapping)
    {
        if (vector == null || VectorAccess.isAllFalseNulls(vector)) {
            return true;
        }
        if (vector instanceof DictionaryVector dictionary) {
            return dictionary.length() == mapping.length() && dictionary.ids() == mapping.ids();
        }
        return vector instanceof RleVector rle && rle.length() == mapping.length() && rle.counts().length == 1;
    }

    private static boolean readEncodedDomainBoolean(Vector vector, DictionaryVector mapping, int domain)
    {
        if (vector == null || VectorAccess.isAllFalseNulls(vector)) {
            return false;
        }
        return switch (vector) {
            case DictionaryVector dictionary -> readBoolean(dictionary.values(), domain);
            case RleVector rle -> readBoolean(rle.values(), 0);
            default -> throw new IllegalArgumentException("Unsupported encoded boolean domain: " + vector.getClass().getName());
        };
    }

    private void releaseMergedDomains(Iterable<Vector> domains)
    {
        for (Vector domain : domains) {
            allocator.release(allocationContext, domain);
        }
    }

    private Vector evaluateMaskExpressionErrors(MaskExpression expression, Mask mask)
    {
        return switch (expression) {
            case AllMask _ -> null;
            case LongDomainMask(Reference input, _) -> optionalBooleanStream(input.producer(), Stream.ERRORS, mask);
            case RangeConstrainedAndMask(_, _, _, _, _, AndMask fallback) -> evaluateMaskExpressionErrors(fallback, mask);
            case ReferenceMask(Reference reference) -> {
                MaskExpression resolved = MaskExpressionResolver.resolve(plan, new ReferenceMask(reference));
                if (!(resolved instanceof ReferenceMask(Reference resolvedReference) && resolvedReference.equals(reference))) {
                    yield evaluateMaskExpressionErrors(resolved, mask);
                }
                yield optionalBooleanStream(reference.producer(), Stream.ERRORS, mask);
            }
            case NotMask(MaskExpression source) -> evaluateMaskExpressionErrors(source, mask);
            case AndMask(List<MaskExpression> terms) -> evaluateMaskTermErrors(terms, mask);
            case OrMask(List<MaskExpression> terms) -> evaluateMaskTermErrors(terms, mask);
        };
    }

    private Vector evaluateMaskTermErrors(List<MaskExpression> terms, Mask mask)
    {
        Vector errors = null;
        for (MaskExpression term : terms) {
            errors = mergeOptionalBooleanStreams(errors, evaluateMaskExpressionErrors(term, mask), null, mask);
        }
        return errors;
    }

    private Vector materializeMaskErrors(Vector source, Mask errorMask, int length)
    {
        if (ErrorVectors.hasDiagnostics(source)) {
            ErrorVector errors = allocator.allocateOrGrow(
                    allocationContext,
                    null,
                    ErrorVector.class,
                    length,
                    ErrorVector::new);
            fillFalseBoolean(errors, Mask.all(length), length);
            for (int position : errorMask) {
                ErrorValue error = ErrorVectors.errorAt(source, position);
                if (error != null) {
                    errors.setError(position, error);
                }
                else {
                    errors.clearError(position);
                    errors.values()[position] = true;
                }
            }
            return errors;
        }

        BooleanVector errors = fillFalseBoolean(null, Mask.all(length), length);
        for (int position : errorMask) {
            errors.values()[position] = true;
        }
        return errors;
    }

    private Streams evaluateSequence(Set<Stream> requestedStreams, Sequence sequence, Mask mask, Streams output)
    {
        Streams first = evaluateArgument(sequence.first(), mask);
        Streams result = copy(requestedStreams, sequence.result(), mask, output);
        if (!requestedStreams.contains(Stream.ERRORS)) {
            return result;
        }

        Vector errors = mergeOptionalBooleanStreams(
                first.getOrNull(Stream.ERRORS),
                result.getOrNull(Stream.ERRORS),
                output == null ? null : output.getOrNull(Stream.ERRORS),
                mask);
        return errors == null ? result : result.with(Stream.ERRORS, errors);
    }

    private Streams evaluateStructField(Set<Stream> requestedStreams, StructField field, Mask mask, Streams output)
    {
        Streams sourceStreams = evaluateArgument(field.source(), mask);
        StructVector sourceValues = (StructVector) sourceStreams.values();
        Streams fieldStreams = sourceValues.field(field.field());

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

        if (!singleBranch && existing == null && mask.all() &&
                trueMask.count() + falseMask.count() == mask.count()) {
            Vector encoded = tryMergeDictionaryBranches(stream, trueReference, falseReference, trueMask, falseMask, mask.size());
            if (encoded != null) {
                return encoded;
            }
        }

        if (!trueMask.none()) {
            target = mergeBranchInto(stream, trueReference, trueMask, mask, target, singleBranch && falseMask.none());
        }
        if (!falseMask.none()) {
            target = mergeBranchInto(stream, falseReference, falseMask, mask, target, singleBranch && trueMask.none());
        }
        return target;
    }

    /**
     * Preserves a shared logical-to-physical dictionary mapping across a conditional merge when the condition is
     * constant for every occurrence of a dictionary entry. Branches are still evaluated only under their branch
     * masks; this method merely merges their already-produced physical domains instead of copying logical rows.
     */
    private Vector tryMergeDictionaryBranches(
            Stream stream,
            Reference trueReference,
            Reference falseReference,
            Mask trueMask,
            Mask falseMask,
            int rowCount)
    {
        Vector trueVector = evaluateMergeBranchVector(stream, trueReference, trueMask);
        if (stream == Stream.VALUES && trueVector == null) {
            return null;
        }
        Vector falseVector = evaluateMergeBranchVector(stream, falseReference, falseMask);
        if (stream == Stream.VALUES && falseVector == null) {
            return null;
        }

        DictionaryVector mapping = trueVector instanceof DictionaryVector dictionary
                ? dictionary
                : falseVector instanceof DictionaryVector dictionary ? dictionary : null;
        if (mapping == null || mapping.length() != rowCount ||
                !encodedMergeBranchCompatible(trueVector, mapping, rowCount) ||
                !encodedMergeBranchCompatible(falseVector, mapping, rowCount)) {
            return null;
        }

        int[] ids = mapping.ids();
        int domainSize = dictionaryBaseLength(ids, rowCount);
        byte[] choices = new byte[domainSize];
        if (!recordDictionaryBranchChoices(ids, trueMask, choices, (byte) 1) ||
                !recordDictionaryBranchChoices(ids, falseMask, choices, (byte) 2)) {
            return null;
        }

        int trueCount = 0;
        int falseCount = 0;
        for (byte choice : choices) {
            if (choice == 1) {
                trueCount++;
            }
            else if (choice == 2) {
                falseCount++;
            }
        }
        int[] truePositions = new int[trueCount];
        int[] falsePositions = new int[falseCount];
        for (int domain = 0, trueIndex = 0, falseIndex = 0; domain < choices.length; domain++) {
            if (choices[domain] == 1) {
                truePositions[trueIndex++] = domain;
            }
            else if (choices[domain] == 2) {
                falsePositions[falseIndex++] = domain;
            }
        }

        Mask trueDomain = allocator.allocateSparseMask(allocationContext, truePositions, trueCount, domainSize);
        Mask falseDomain = allocator.allocateSparseMask(allocationContext, falsePositions, falseCount, domainSize);
        try {
            // Start with a dictionary-backed branch so the result representation is known before an absent optional
            // boolean stream has to contribute its implicit false values.
            boolean trueFirst = trueVector instanceof DictionaryVector;
            Vector target = null;
            if (trueFirst) {
                target = copyDictionaryMergeDomain(stream, trueVector, trueDomain, domainSize, target);
                target = copyDictionaryMergeDomain(stream, falseVector, falseDomain, domainSize, target);
            }
            else {
                target = copyDictionaryMergeDomain(stream, falseVector, falseDomain, domainSize, target);
                target = copyDictionaryMergeDomain(stream, trueVector, trueDomain, domainSize, target);
            }
            return target == null ? null : wrapBorrowedDictionary(ids, rowCount, target);
        }
        finally {
            allocator.release(allocationContext, trueDomain);
            allocator.release(allocationContext, falseDomain);
        }
    }

    private Vector evaluateMergeBranchVector(Stream stream, Reference reference, Mask branchMask)
    {
        if (reference == null) {
            return null;
        }
        Streams streams = evaluate(reference, branchMask);
        if (!streams.has(reference.stream())) {
            checkArgument(stream != Stream.VALUES, "VALUES stream not produced for active merge branch: %s", reference);
            return null;
        }
        return streams.get(reference.stream());
    }

    private static boolean encodedMergeBranchCompatible(Vector vector, DictionaryVector mapping, int rowCount)
    {
        if (vector == null) {
            return true;
        }
        if (vector instanceof DictionaryVector dictionary) {
            return dictionary.length() == rowCount && dictionary.ids() == mapping.ids();
        }
        if (vector instanceof RleVector rle) {
            return rle.length() == rowCount && rle.counts().length == 1;
        }
        return vector instanceof BooleanVector booleans && booleans.length() == rowCount &&
                (booleans.isAllFalse() || booleans.isAllTrue());
    }

    private static boolean recordDictionaryBranchChoices(int[] ids, Mask mask, byte[] choices, byte branch)
    {
        for (int position : mask) {
            int domain = ids[position];
            byte previous = choices[domain];
            if (previous != 0 && previous != branch) {
                return false;
            }
            choices[domain] = branch;
        }
        return true;
    }

    private Vector copyDictionaryMergeDomain(Stream stream, Vector source, Mask domainMask, int domainSize, Vector target)
    {
        if (domainMask.none()) {
            return target;
        }
        if (source == null) {
            checkArgument(stream != Stream.VALUES, "VALUES stream cannot be absent for active merge branch");
            return fillFalseBoolean(target, domainMask, domainSize);
        }

        Vector domainSource = switch (source) {
            case DictionaryVector dictionary -> dictionary.values();
            case RleVector rle -> new RleVector(new int[] {domainSize}, rle.values());
            case BooleanVector booleans when booleans.isAllFalse() || booleans.isAllTrue() ->
                    new RleVector(new int[] {domainSize}, new BooleanVector(new boolean[] {booleans.isAllTrue()}));
            default -> throw new IllegalArgumentException("Unsupported encoded merge branch: " + source.getClass().getName());
        };
        return copyVector(domainSource, target, domainMask);
    }

    private Vector mergeBranchInto(Stream stream, Reference source, Mask branchMask, Mask fullMask, Vector target, boolean allowForward)
    {
        if (source == null) {
            checkArgument(stream != Stream.VALUES, "VALUES stream cannot be absent for active merge branch");
            return fillFalseBoolean(target, branchMask, fullMask.size());
        }

        Streams sourceStreams = evaluate(source, branchMask);
        if (!sourceStreams.has(source.stream())) {
            checkArgument(stream != Stream.VALUES, "VALUES stream not produced for active merge branch: %s", source);
            return fillFalseBoolean(target, branchMask, fullMask.size());
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
        // Dictionary peeling commonly produces a dictionary-wrapped all-false error stream. Merging it with an
        // all-false input error stream position by position would turn a domain-sized scalar evaluation back into
        // a row-sized boolean loop. Either identity can be forwarded without inspecting dictionary ids.
        if (VectorAccess.isAllFalseNulls(parentStream)) {
            return childStream;
        }
        if (VectorAccess.isAllFalseNulls(childStream)) {
            return parentStream;
        }

        if (parentStream instanceof ErrorVector ||
                childStream instanceof ErrorVector ||
                existing instanceof ErrorVector) {
            return mergeOptionalErrorStreams(parentStream, childStream, existing, mask);
        }

        BooleanVector merged = VectorAccess.writableBooleanVector(allocator, allocationContext, existing, mask.size());
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                merged.values()[position] = readOptionalBoolean(parentStream, position) || readOptionalBoolean(childStream, position);
            }
            return merged;
        }

        for (int position : mask) {
            merged.values()[position] = readOptionalBoolean(parentStream, position) || readOptionalBoolean(childStream, position);
        }
        return merged;
    }

    private ErrorVector mergeOptionalErrorStreams(Vector parentStream, Vector childStream, Vector existing, Mask mask)
    {
        ErrorVector merged = allocator.allocateOrGrow(
                allocationContext,
                existing instanceof ErrorVector errors ? errors : null,
                ErrorVector.class,
                mask.size(),
                ErrorVector::new);
        for (int position : mask) {
            boolean parentError = readOptionalBoolean(parentStream, position);
            boolean childError = readOptionalBoolean(childStream, position);
            ErrorValue error = parentError
                    ? ErrorVectors.errorAt(parentStream, position)
                    : ErrorVectors.errorAt(childStream, position);
            if (error != null) {
                merged.setError(position, error);
            }
            else if (parentError || childError) {
                merged.clearError(position);
                merged.values()[position] = true;
            }
            else {
                merged.clearError(position);
            }
        }
        return merged;
    }

    private BooleanVector fillFalseBoolean(Vector existing, Mask mask, int length)
    {
        BooleanVector target = VectorAccess.writableBooleanVector(allocator, allocationContext, existing, length);
        if (target instanceof ErrorVector errors) {
            for (int position : mask) {
                errors.clearError(position);
            }
            return target;
        }
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
            case LongDomainMask(Reference input, LongDomain domain) -> evaluateTrueLongDomainMask(input, domain, mask);
            case RangeConstrainedAndMask(_, _, _, _, _, AndMask fallback) -> evaluateTrueMask(fallback, mask);
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
            case LongDomainMask(Reference input, LongDomain domain) -> evaluateLongDomainMask(input, domain, mask).falseMask(allocator, allocationContext, mask);
            case RangeConstrainedAndMask(_, _, _, _, _, AndMask fallback) -> evaluateFalseMask(fallback, mask);
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

        Mask inputMask = tryResolveInputMask(reference, mask, true);
        if (inputMask != null) {
            return inputMask;
        }

        Vector values = evaluate(reference, mask).get(reference.stream());
        Vector errors = optionalBooleanStream(reference.producer(), Stream.ERRORS, mask);
        Vector nulls = optionalBooleanStream(reference.producer(), Stream.NULLS, mask);
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

        Mask inputMask = tryResolveInputMask(reference, mask, false);
        if (inputMask != null) {
            return inputMask;
        }

        Vector values = evaluate(reference, mask).get(reference.stream());
        Vector errors = optionalBooleanStream(reference.producer(), Stream.ERRORS, mask);
        Vector nulls = optionalBooleanStream(reference.producer(), Stream.NULLS, mask);
        return classifyFalseBooleanMask(values, nulls, errors, mask);
    }

    private Mask tryResolveInputMask(Reference reference, Mask mask, boolean selectTrue)
    {
        if (!policy.inputMaskResolver()) {
            return null;
        }
        return input.resolveMask(reference, mask, selectTrue, allocator, allocationContext);
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
        if (ErrorVectors.hasDiagnostics(source) || existing instanceof ErrorVector) {
            ErrorVector target = allocator.allocateOrGrow(
                    allocationContext,
                    existing instanceof ErrorVector errors ? errors : null,
                    ErrorVector.class,
                    source.length(),
                    ErrorVector::new);
            for (int position : mask) {
                ErrorValue error = ErrorVectors.errorAt(source, position);
                if (error != null) {
                    target.setError(position, error);
                }
                else if (readBoolean(source, position)) {
                    target.clearError(position);
                    target.values()[position] = true;
                }
                else {
                    target.clearError(position);
                }
            }
            return target;
        }
        if (existing instanceof I64Vector wideTarget && isIntegerVector(source)) {
            I64Vector target = allocator.allocateOrGrow(
                    allocationContext,
                    wideTarget,
                    I64Vector.class,
                    Math.max(source.length(), mask.size()),
                    I64Vector::new);
            VectorAccess.LongValues values = VectorAccess.longValues(source);
            for (int position : mask) {
                target.values()[position] = values.value(position);
            }
            return target;
        }
        if (existing instanceof I32Vector compactTarget && isWideIntegerVector(source)) {
            int length = Math.max(Math.max(source.length(), compactTarget.length()), mask.size());
            I64Vector target = allocator.allocate(allocationContext, I64Vector.class, length, I64Vector::new);
            for (int position = 0; position < compactTarget.length(); position++) {
                target.values()[position] = compactTarget.values()[position];
            }
            VectorAccess.LongValues values = VectorAccess.longValues(source);
            for (int position : mask) {
                target.values()[position] = values.value(position);
            }
            allocator.release(allocationContext, compactTarget);
            return target;
        }
        return source.copyMasked(allocator, allocationContext, existing, mask);
    }

    private static boolean isIntegerVector(Vector vector)
    {
        return switch (vector) {
            case I32Vector _, I64Vector _ -> true;
            case DictionaryVector dictionary -> isIntegerVector(dictionary.values());
            case RleVector rle -> isIntegerVector(rle.values());
            default -> false;
        };
    }

    private static boolean isWideIntegerVector(Vector vector)
    {
        return switch (vector) {
            case I64Vector _ -> true;
            case DictionaryVector dictionary -> isWideIntegerVector(dictionary.values());
            case RleVector rle -> isWideIntegerVector(rle.values());
            default -> false;
        };
    }

    private Vector fillLongRle(long value, int length)
    {
        I64Vector values = allocator.allocate(allocationContext, I64Vector.class, 1, I64Vector::new);
        values.values()[0] = value;
        return allocator.allocateSingleRunRle(allocationContext, length, values);
    }

    private Vector fillDoubleRle(double value, int length)
    {
        F64Vector values = allocator.allocate(allocationContext, F64Vector.class, 1, F64Vector::new);
        values.values()[0] = value;
        return allocator.allocateSingleRunRle(allocationContext, length, values);
    }

    private Vector fillBoolean(boolean value, int length)
    {
        BooleanVector values = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
        values.values()[0] = value;
        return allocator.allocateSingleRunRle(allocationContext, length, values);
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
        return allocator.allocateSingleRunRle(allocationContext, length, values);
    }

    private static Map<Variable, Assignment> indexAssignments(List<Assignment> assignments)
    {
        Map<Variable, Assignment> indexedAssignments = new HashMap<>();
        for (Assignment assignment : assignments) {
            indexedAssignments.put(assignment.output(), assignment);
        }
        return indexedAssignments;
    }

    private static Map<Variable, PreboundMask> bindPreboundMasks(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            ProjectionMaskCompiler projectionMaskCompiler,
            Map<Variable, Assignment> assignments)
    {
        Map<Variable, PreboundMask> bindings = new HashMap<>();
        for (Assignment assignment : plan.assignments()) {
            if (!(assignment.operation() instanceof Call call)) {
                continue;
            }
            DirectMaskInputProvider provider =
                    primitiveRegistry.capabilityOrNull(call, DirectMaskInputProvider.class);
            if (provider == null ||
                    call.arguments().size() != provider.argumentCount() ||
                    provider.argumentIndex() < 0 ||
                    provider.argumentIndex() >= call.arguments().size()) {
                continue;
            }
            Reference argument = call.arguments().get(provider.argumentIndex());
            Reference directInput = new Reference(
                    argument.producer(),
                    switch (provider.inputComponent()) {
                        case VALUES -> Stream.VALUES;
                        case NULLS -> Stream.NULLS;
                        case ERRORS -> Stream.ERRORS;
                    });
            bindings.put(assignment.output(), new DirectPreboundMask(directInput));
            continue;
        }
        for (Assignment assignment : plan.assignments()) {
            if (!(assignment.operation() instanceof Call call) || bindings.containsKey(assignment.output())) {
                continue;
            }
            MaskCodeProvider provider = primitiveRegistry.capabilityOrNull(call, MaskCodeProvider.class);
            if (provider == null) {
                continue;
            }
            List<ProjectionArgument> argumentShapes = call.arguments().stream()
                    .map(argument -> projectionArgument(argument, assignments))
                    .toList();
            projectionMaskCompiler.tryCompile(provider, argumentShapes)
                    .ifPresent(compiled -> bindings.put(
                            assignment.output(),
                            compiledPreboundMask(call.arguments(), compiled, assignments)));
        }
        return Map.copyOf(bindings);
    }

    private static CompiledPreboundMask compiledPreboundMask(
            List<Reference> arguments,
            ProjectionMaskCompiler.CompiledMask compiled,
            Map<Variable, Assignment> assignments)
    {
        List<Reference> excludedComponents = compiled.excludedComponents().stream()
                .map(component -> new Reference(
                        arguments.get(component.argumentIndex()).producer(),
                        component.stream()))
                .filter(component -> !(component.producer() instanceof Variable variable &&
                        assignments.get(variable) != null &&
                        assignments.get(variable).operation() instanceof Literal))
                .toList();
        return new CompiledPreboundMask(arguments, excludedComponents, compiled);
    }

    private static ProjectionArgument projectionArgument(
            Reference reference,
            Map<Variable, Assignment> assignments)
    {
        if (reference.producer() instanceof org.weakref.nitro.operator.evaluator.ir.Input) {
            return ProjectionArgument.input();
        }
        if (reference.producer() instanceof Variable variable) {
            Assignment assignment = assignments.get(variable);
            if (assignment != null && assignment.operation() instanceof Literal literal) {
                return ProjectionArgument.literal(literal.value());
            }
        }
        return ProjectionArgument.computed();
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

    private static void registerResolvedCalls(EvaluationPlan plan, PrimitiveRegistry primitiveRegistry)
    {
        for (Assignment assignment : plan.assignments()) {
            if (assignment.operation() instanceof Call call && call.resolvedCall() != null) {
                primitiveRegistry.register(call.resolvedCall());
            }
        }
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

    private Set<Stream> computeHardRequestedStreams(Reference reference)
    {
        java.util.EnumSet<Stream> streams = java.util.EnumSet.of(reference.stream());
        // ProjectOperator exposes a VALUES projection's companion NULLS/ERRORS streams even when the IR output list
        // names VALUES alone.  In that mode those companions are semantic outputs, not optional physical metadata:
        // a downstream arithmetic node must recursively compute an intermediate NULLS stream in order to preserve
        // SQL null propagation.  Treating the auto-added companions as "available only" caused a multi-stage
        // expression such as (nullable_a + b) + c to synthesize an all-false NULLS stream for the first add. Direct
        // evaluator clients retain the explicit-stream contract and do not pay to resolve unrequested companions.
        Set<Stream> projectedStreams = (requireProjectedCompanionStreams ? projectedStreamsByProducer : explicitProjectedStreamsByProducer)
                .get(reference.producer());
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

    private static Map<Producer, Set<Stream>> streamsByProducer(List<Reference> outputs)
    {
        Map<Producer, java.util.EnumSet<Stream>> projected = new HashMap<>();
        for (Reference output : outputs) {
            projected.computeIfAbsent(output.producer(), _ -> java.util.EnumSet.noneOf(Stream.class))
                    .add(output.stream());
        }
        return immutableStreamMap(projected);
    }

    private static Map<Producer, Set<Stream>> projectedStreamsByProducer(List<Reference> outputs)
    {
        Map<Producer, java.util.EnumSet<Stream>> projected = new HashMap<>();
        for (Reference output : outputs) {
            java.util.EnumSet<Stream> streams = projected.computeIfAbsent(output.producer(), _ -> java.util.EnumSet.noneOf(Stream.class));
            streams.add(output.stream());
        }
        // When a producer exposes only its VALUES stream, request the companion NULLS and ERRORS
        // streams alongside it so null/error information propagates with the value. When a producer
        // is projected through multiple explicit stream references (e.g. VALUES and ERRORS as
        // separate outputs), the projection is treated as fully explicit and companions are not
        // auto-added, so each requested sibling stream is memoized together exactly as specified.
        for (java.util.EnumSet<Stream> streams : projected.values()) {
            if (streams.size() == 1 && streams.contains(Stream.VALUES)) {
                streams.add(Stream.NULLS);
                streams.add(Stream.ERRORS);
            }
        }
        return immutableStreamMap(projected);
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
        return immutableStreamMap(memoized);
    }

    private static Set<Producer> memoizedProducers(Map<Reference, StreamPlan> streamPlans)
    {
        HashSet<Producer> producers = new HashSet<>();
        for (Map.Entry<Reference, StreamPlan> entry : streamPlans.entrySet()) {
            if (entry.getValue().memoizationPolicy() == MemoizationPolicy.MEMOIZE) {
                producers.add(entry.getKey().producer());
            }
        }
        return producers.isEmpty() ? Set.of() : Set.copyOf(producers);
    }

    private static Map<Producer, Set<Stream>> immutableStreamMap(Map<Producer, java.util.EnumSet<Stream>> source)
    {
        if (source.isEmpty()) {
            return Map.of();
        }
        HashMap<Producer, Set<Stream>> result = new HashMap<>(source.size());
        for (Map.Entry<Producer, java.util.EnumSet<Stream>> entry : source.entrySet()) {
            result.put(entry.getKey(), Set.copyOf(entry.getValue()));
        }
        return Map.copyOf(result);
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
            case LongDomainMask(Reference input, LongDomain domain) -> evaluateLongDomainMask(input, domain, mask);
            case RangeConstrainedAndMask(_, _, _, _, _, AndMask fallback) -> evaluateMaskOutcome(fallback, mask);
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
        Vector errors = optionalBooleanStream(reference.producer(), Stream.ERRORS, mask);
        Vector nulls = optionalBooleanStream(reference.producer(), Stream.NULLS, mask);
        return classifyBooleanMask(values, nulls, errors, mask);
    }

    private MaskOutcome evaluateLongDomainMask(Reference input, LongDomain domain, Mask mask)
    {
        Vector values = evaluate(input, mask).get(input.stream());
        Vector errors = optionalBooleanStream(input.producer(), Stream.ERRORS, mask);
        Vector nulls = optionalBooleanStream(input.producer(), Stream.NULLS, mask);

        boolean[] dictionaryMatches = null;
        int[] dictionaryIds = null;
        if (values instanceof DictionaryVector dictionary) {
            Vector dictionaryValues = dictionary.values();
            if (dictionaryValues instanceof I64Vector longs) {
                long[] entries = longs.values();
                dictionaryMatches = new boolean[dictionaryValues.length()];
                for (int index = 0; index < dictionaryMatches.length; index++) {
                    dictionaryMatches[index] = domain.test(entries[index]);
                }
                dictionaryIds = dictionary.ids();
            }
            else if (dictionaryValues instanceof I32Vector integers) {
                int[] entries = integers.values();
                dictionaryMatches = new boolean[dictionaryValues.length()];
                for (int index = 0; index < dictionaryMatches.length; index++) {
                    dictionaryMatches[index] = domain.test(entries[index]);
                }
                dictionaryIds = dictionary.ids();
            }
        }

        VectorAccess.LongValues longValues = dictionaryMatches == null ? VectorAccess.longValues(values) : null;
        int capacity = mask.count();
        Mask trueMask = allocator.allocateUninitializedSparseMask(allocationContext, capacity, mask.size());
        Mask nullMask = allocator.allocateUninitializedSparseMask(allocationContext, capacity, mask.size());
        Mask errorMask = allocator.allocateUninitializedSparseMask(allocationContext, capacity, mask.size());
        int[] truePositions = trueMask.positionsArrayForOverwrite(capacity);
        int[] nullPositions = nullMask.positionsArrayForOverwrite(capacity);
        int[] errorPositions = errorMask.positionsArrayForOverwrite(capacity);
        int trueCount = 0;
        int nullCount = 0;
        int errorCount = 0;
        for (int position : mask) {
            if (isError(errors, position)) {
                errorPositions[errorCount++] = position;
            }
            else if (isNull(nulls, position)) {
                nullPositions[nullCount++] = position;
            }
            else if (dictionaryMatches != null ? dictionaryMatches[dictionaryIds[position]] : domain.test(longValues.value(position))) {
                truePositions[trueCount++] = position;
            }
        }
        trueMask.finishRetain(trueCount);
        nullMask.finishRetain(nullCount);
        errorMask.finishRetain(errorCount);
        return new MaskOutcome(trueMask, nullMask, errorMask);
    }

    private Mask evaluateTrueLongDomainMask(Reference input, LongDomain domain, Mask mask)
    {
        Vector values = evaluate(input, mask).get(input.stream());
        Vector errors = optionalBooleanStream(input.producer(), Stream.ERRORS, mask);
        Vector nulls = optionalBooleanStream(input.producer(), Stream.NULLS, mask);
        int capacity = mask.count();
        Mask result = allocator.allocateUninitializedSparseMask(allocationContext, capacity, mask.size());
        int[] positions = result.positionsArrayForOverwrite(capacity);
        int count = 0;

        if (values instanceof DictionaryVector dictionary) {
            boolean[] dictionaryMatches = evaluateLongDictionaryDomain(dictionary.values(), domain);
            if (dictionaryMatches != null) {
                int[] ids = dictionary.ids();
                for (int position : mask) {
                    if (!isError(errors, position) && !isNull(nulls, position) && dictionaryMatches[ids[position]]) {
                        positions[count++] = position;
                    }
                }
                result.finishRetain(count);
                return result;
            }
        }

        VectorAccess.LongValues longValues = VectorAccess.longValues(values);
        for (int position : mask) {
            if (!isError(errors, position) && !isNull(nulls, position) && domain.test(longValues.value(position))) {
                positions[count++] = position;
            }
        }
        result.finishRetain(count);
        return result;
    }

    private boolean evaluateTrueLongDomainMaskInPlace(Reference input, LongDomain domain, Mask mask)
    {
        Vector values = evaluate(input, mask).get(input.stream());
        Vector errors = optionalBooleanStream(input.producer(), Stream.ERRORS, mask);
        Vector nulls = optionalBooleanStream(input.producer(), Stream.NULLS, mask);
        if (values instanceof DictionaryVector dictionary &&
                VectorAccess.isAllFalseNulls(nulls) &&
                VectorAccess.isAllFalseNulls(errors)) {
            boolean[] dictionaryMatches = evaluateLongDictionaryDomain(dictionary.values(), domain);
            if (dictionaryMatches != null) {
                mask.retainDictionaryComparison(dictionary.ids(), dictionaryMatches);
                return true;
            }
        }
        return false;
    }

    private static boolean[] evaluateLongDictionaryDomain(Vector dictionary, LongDomain domain)
    {
        if (dictionary instanceof I64Vector longs) {
            boolean[] matches = new boolean[dictionary.length()];
            long[] values = longs.values();
            for (int index = 0; index < matches.length; index++) {
                matches[index] = domain.test(values[index]);
            }
            return matches;
        }
        if (dictionary instanceof I32Vector integers) {
            boolean[] matches = new boolean[dictionary.length()];
            int[] values = integers.values();
            for (int index = 0; index < matches.length; index++) {
                matches[index] = domain.test(values[index]);
            }
            return matches;
        }
        return null;
    }

    private MaskOutcome classifyBooleanMask(Vector values, Vector nulls, Vector errors, Mask mask)
    {
        int capacity = mask.count();
        Mask trueMask = allocator.allocateUninitializedSparseMask(allocationContext, capacity, mask.size());
        Mask nullMask = allocator.allocateUninitializedSparseMask(allocationContext, capacity, mask.size());
        Mask errorMask = allocator.allocateUninitializedSparseMask(allocationContext, capacity, mask.size());
        int[] truePositions = trueMask.positionsArrayForOverwrite(capacity);
        int[] nullPositions = nullMask.positionsArrayForOverwrite(capacity);
        int[] errorPositions = errorMask.positionsArrayForOverwrite(capacity);
        int trueCount = 0;
        int nullCount = 0;
        int errorCount = 0;

        for (int position : mask) {
            if (isError(errors, position)) {
                errorPositions[errorCount++] = position;
            }
            else if (isNull(nulls, position)) {
                nullPositions[nullCount++] = position;
            }
            else if (readBoolean(values, position)) {
                truePositions[trueCount++] = position;
            }
        }

        trueMask.finishRetain(trueCount);
        nullMask.finishRetain(nullCount);
        errorMask.finishRetain(errorCount);
        return new MaskOutcome(trueMask, nullMask, errorMask);
    }

    private Mask classifyTrueBooleanMask(Vector values, Vector nulls, Vector errors, Mask mask)
    {
        if (policy.fastBooleanMaskClassifier() && values instanceof BooleanVector booleanValues && VectorAccess.isAllFalseNulls(nulls) && VectorAccess.isAllFalseNulls(errors)) {
            return allocator.intersectMask(allocationContext, mask, booleanValues);
        }

        int trueCount = countTrueRows(values, nulls, errors, mask);
        Mask result = allocator.allocateUninitializedSparseMask(allocationContext, trueCount, mask.size());
        int[] truePositions = result.positionsArrayForOverwrite(trueCount);
        int outputIndex = 0;
        for (int position : mask) {
            if (isError(errors, position)) {
                continue;
            }
            if (isNull(nulls, position)) {
                continue;
            }
            if (readBoolean(values, position)) {
                truePositions[outputIndex++] = position;
            }
        }

        return result;
    }

    private Mask classifyFalseBooleanMask(Vector values, Vector nulls, Vector errors, Mask mask)
    {
        if (policy.fastBooleanMaskClassifier() && values instanceof BooleanVector booleanValues && VectorAccess.isAllFalseNulls(nulls) && VectorAccess.isAllFalseNulls(errors)) {
            return allocator.differenceMask(allocationContext, mask, booleanValues);
        }

        int falseCount = countFalseRows(values, nulls, errors, mask);
        Mask result = allocator.allocateUninitializedSparseMask(allocationContext, falseCount, mask.size());
        int[] falsePositions = result.positionsArrayForOverwrite(falseCount);
        int outputIndex = 0;
        for (int position : mask) {
            if (isError(errors, position)) {
                continue;
            }
            if (isNull(nulls, position)) {
                continue;
            }
            if (!readBoolean(values, position)) {
                falsePositions[outputIndex++] = position;
            }
        }

        return result;
    }

    private int countTrueRows(Vector values, Vector nulls, Vector errors, Mask mask)
    {
        int trueCount = 0;
        for (int position : mask) {
            if (isError(errors, position)) {
                continue;
            }
            if (isNull(nulls, position)) {
                continue;
            }
            if (readBoolean(values, position)) {
                trueCount++;
            }
        }
        return trueCount;
    }

    private int countFalseRows(Vector values, Vector nulls, Vector errors, Mask mask)
    {
        int falseCount = 0;
        for (int position : mask) {
            if (isError(errors, position)) {
                continue;
            }
            if (isNull(nulls, position)) {
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
        MaskOutcome sourcePredicateOutcome = tryEvaluateSourcePredicateMaskOutcome(reference, mask);
        if (sourcePredicateOutcome != null) {
            sourceMaskSuccesses++;
            return sourcePredicateOutcome;
        }

        CompiledPreboundMask compiled = compiledPreboundMask(reference);
        if (compiled != null) {
            compiledMaskAttempts++;
            prepareCompiledMaskInputs(compiled, mask);
            MaskOutcome outcome = compiled.compiled().evaluateOutcome(
                    compiled.inputs(), mask, executionContext, allocationContext);
            if (outcome != null) {
                compiledMaskSuccesses++;
                return applyExcludedOutcomeComponents(compiled, outcome, mask);
            }
            compiledMaskFallbacks++;
        }
        PrimitiveMaskInvocation invocation = resolveMaskPrimitiveInvocation(reference, mask);
        if (invocation == null) {
            materializedMaskFallbacks++;
            return null;
        }
        primitiveMaskAttempts++;
        MaskOutcome outcome = invocation.function().tryEvaluateMaskOutcome(invocation.inputs(), mask, executionContext);
        if (outcome == null) {
            primitiveMaskFallbacks++;
            materializedMaskFallbacks++;
        }
        else {
            primitiveMaskSuccesses++;
        }
        return outcome;
    }

    private Mask tryEvaluatePrimitiveMask(Reference reference, Mask mask, boolean selectTrue)
    {
        Mask sourcePredicateMask = tryEvaluateSourcePredicateMask(reference, mask, selectTrue);
        if (sourcePredicateMask != null) {
            sourceMaskSuccesses++;
            return sourcePredicateMask;
        }

        CompiledPreboundMask compiled = compiledPreboundMask(reference);
        if (compiled != null) {
            compiledMaskAttempts++;
            prepareCompiledMaskInputs(compiled, mask);
            Mask result = compiled.compiled().evaluateMask(
                    compiled.inputs(), mask, selectTrue, executionContext, allocationContext);
            if (result != null) {
                compiledMaskSuccesses++;
                for (Reference component : compiled.excludedComponents()) {
                    excludeComponent(component, result);
                }
                return result;
            }
            compiledMaskFallbacks++;
        }

        PrimitiveMaskInvocation invocation = resolveMaskPrimitiveInvocation(reference, mask);
        if (invocation == null) {
            materializedMaskFallbacks++;
            return null;
        }
        primitiveMaskAttempts++;
        Mask result = selectTrue
                ? invocation.function().tryEvaluateTrueMask(invocation.inputs(), mask, executionContext)
                : invocation.function().tryEvaluateFalseMask(invocation.inputs(), mask, executionContext);
        if (result == null) {
            primitiveMaskFallbacks++;
            materializedMaskFallbacks++;
        }
        else {
            primitiveMaskSuccesses++;
        }
        return result;
    }

    private boolean tryEvaluatePrimitiveMaskInPlace(Reference reference, Mask mask, boolean selectTrue)
    {
        Mask preboundMask = tryEvaluatePreboundMask(reference, mask, selectTrue);
        if (preboundMask != null) {
            if (preboundMask != mask) {
                mask.copyFrom(preboundMask);
            }
            return true;
        }
        if (tryEvaluateSourcePredicateMaskInPlace(reference, mask, selectTrue)) {
            sourceMaskSuccesses++;
            return true;
        }

        PrimitiveMaskInvocation invocation = resolveMaskPrimitiveInvocation(reference, mask);
        if (invocation == null) {
            materializedMaskFallbacks++;
            return false;
        }
        primitiveMaskAttempts++;
        boolean success = selectTrue
                ? invocation.function().tryEvaluateTrueMaskInPlace(invocation.inputs(), mask, executionContext)
                : invocation.function().tryEvaluateFalseMaskInPlace(invocation.inputs(), mask, executionContext);
        if (success) {
            primitiveMaskSuccesses++;
        }
        else {
            primitiveMaskFallbacks++;
            materializedMaskFallbacks++;
        }
        return success;
    }

    private Mask tryEvaluatePreboundMask(Reference reference, Mask mask, boolean selectTrue)
    {
        if (!policy.directPrimitiveInputMask() || reference.stream() != Stream.VALUES ||
                !(reference.producer() instanceof Variable variable)) {
            return null;
        }
        PreboundMask preboundMask = preboundMasks.get(variable);
        if (preboundMask == null) {
            return null;
        }
        if (preboundMask instanceof CompiledPreboundMask compiled) {
            compiledMaskAttempts++;
            prepareCompiledMaskInputs(compiled, mask);
            if (!compiled.compiled().evaluate(compiled.inputs(), mask, selectTrue)) {
                compiledMaskFallbacks++;
                return null;
            }
            compiledMaskSuccesses++;
            for (Reference component : compiled.excludedComponents()) {
                excludeComponent(component, mask);
            }
            return mask;
        }
        Reference directInput = ((DirectPreboundMask) preboundMask).input();
        if (directInput.producer() instanceof org.weakref.nitro.operator.evaluator.ir.Input) {
            Mask inputMask = tryResolveInputMask(directInput, mask, selectTrue);
            if (inputMask != null) {
                directPreboundMaskSuccesses++;
                return inputMask;
            }
        }

        Vector values = evaluate(directInput, mask).get(directInput.stream());
        if (policy.inPlaceFlatBooleanClassifier() && values instanceof BooleanVector booleanValues) {
            mask.retainBooleans(booleanValues.values(), selectTrue);
            directPreboundMaskSuccesses++;
            return mask;
        }
        directPreboundMaskSuccesses++;
        return selectTrue
                ? classifyTrueBooleanMask(values, null, null, mask)
                : classifyFalseBooleanMask(values, null, null, mask);
    }

    private CompiledPreboundMask compiledPreboundMask(Reference reference)
    {
        if (reference.stream() != Stream.VALUES || !(reference.producer() instanceof Variable variable)) {
            return null;
        }
        return preboundMasks.get(variable) instanceof CompiledPreboundMask compiled ? compiled : null;
    }

    private void prepareCompiledMaskInputs(CompiledPreboundMask compiled, Mask mask)
    {
        compiled.inputs().clear();
        for (int index = 0; index < compiled.arguments().size(); index++) {
            compiled.inputs().add(evaluateArgument(
                    compiled.arguments().get(index),
                    mask,
                    compiled.compiled().requiredInputStreams(index),
                    true));
        }
    }

    private void excludeComponent(Reference componentReference, Mask mask)
    {
        if (mask.none()) {
            return;
        }
        Vector component = resolveAvailableComponent(componentReference, mask);
        if (component == null) {
            return;
        }
        retainComponentFalse(mask, component);
    }

    private MaskOutcome applyExcludedOutcomeComponents(
            CompiledPreboundMask compiled,
            MaskOutcome outcome,
            Mask domain)
    {
        Mask errorMask = outcome.errorMask();
        for (Reference componentReference : compiled.excludedComponents()) {
            checkArgument(componentReference.stream() == Stream.ERRORS, "Unsupported compiled outcome exclusion: %s", componentReference.stream());
            Vector component = resolveAvailableComponent(componentReference, domain);
            if (component == null || VectorAccess.isAllFalseNulls(component)) {
                continue;
            }

            Mask componentMask = classifyTrueBooleanMask(component, null, null, domain);
            retainComponentFalse(outcome.trueMask(), component);
            retainComponentFalse(outcome.nullMask(), component);
            errorMask = errorMask.none()
                    ? componentMask
                    : allocator.unionMask(allocationContext, errorMask, componentMask);
        }
        return errorMask == outcome.errorMask()
                ? outcome
                : new MaskOutcome(outcome.trueMask(), outcome.nullMask(), errorMask);
    }

    private Vector resolveAvailableComponent(Reference componentReference, Mask mask)
    {
        if (componentReference.producer() instanceof org.weakref.nitro.operator.evaluator.ir.Input) {
            // VALUES was resolved immediately before this call. Sources that decode companion components together
            // can therefore hand back the already-resident component without a second reader or a temporary mask.
            return input.resolve(componentReference, mask);
        }
        return evaluateAvailableReference(componentReference, mask).getOrNull(componentReference.stream());
    }

    private static void retainComponentFalse(Mask mask, Vector component)
    {
        if (VectorAccess.isAllFalseNulls(component)) {
            return;
        }
        if (component instanceof BooleanVector booleans) {
            mask.retainBooleans(booleans.values(), false);
            return;
        }
        VectorAccess.BooleanValues values = VectorAccess.booleanValues(component);
        mask.retainIf(position -> !values.value(position));
    }

    private Mask tryEvaluateSourcePredicateMask(Reference reference, Mask mask, boolean selectMatches)
    {
        SourcePredicateInputs source = resolveSourcePredicateInputs(reference, mask);
        if (source == null) {
            return null;
        }
        return evaluateSourcePredicateMask(source, mask, selectMatches);
    }

    private MaskOutcome tryEvaluateSourcePredicateMaskOutcome(Reference reference, Mask mask)
    {
        SourcePredicateInputs source = resolveSourcePredicateInputs(reference, mask);
        if (source == null) {
            return null;
        }
        Mask trueMask = evaluateSourcePredicateMask(source, mask, true);
        if (trueMask == null) {
            return null;
        }
        Mask nullMask = VectorAccess.isAllFalseNulls(source.nulls())
                ? emptyMask(mask.size())
                : classifyTrueBooleanMask(source.nulls(), null, null, mask);
        return new MaskOutcome(trueMask, nullMask, emptyMask(mask.size()));
    }

    private Mask evaluateSourcePredicateMask(SourcePredicateInputs source, Mask mask, boolean selectMatches)
    {
        if (source.values() instanceof DictionaryVector) {
            DictionaryPredicateInputs inputs = bindDictionaryPredicateInputs(source);
            return inputs == null ? null : evaluateDictionaryPredicateMask(inputs, mask, selectMatches);
        }

        SourceMaskOptimization.PositionPredicate predicate = source.predicate().bind(source.values()).orElse(null);
        if (predicate == null) {
            return null;
        }
        Mask result = allocator.copyMask(allocationContext, mask);
        retainSourcePredicate(result, source.nulls(), predicate, selectMatches);
        return result;
    }

    private boolean tryEvaluateSourcePredicateMaskInPlace(Reference reference, Mask mask, boolean selectMatches)
    {
        SourcePredicateInputs source = resolveSourcePredicateInputs(reference, mask);
        if (source == null) {
            return false;
        }
        if (!(source.values() instanceof DictionaryVector)) {
            SourceMaskOptimization.PositionPredicate predicate = source.predicate().bind(source.values()).orElse(null);
            if (predicate == null) {
                return false;
            }
            retainSourcePredicate(mask, source.nulls(), predicate, selectMatches);
            return true;
        }

        DictionaryPredicateInputs inputs = bindDictionaryPredicateInputs(source);
        if (inputs == null) {
            return false;
        }

        if (inputs.dictionary().values() instanceof DictionaryVector nestedDictionary && nestedDictionary.values() instanceof BinaryVector nestedValues) {
            int[] ids = inputs.dictionary().ids();
            int[] nestedIds = nestedDictionary.ids();
            VectorAccess.BooleanValues nulls = nullFreeValues(inputs.nulls());
            if (shouldEvaluateFullDictionary(nestedValues, mask)) {
                boolean[] nestedMatches = evaluateDictionaryPredicate(nestedValues, inputs);
                if (nulls == null) {
                    mask.retainIf(position -> nestedMatches[nestedIds[ids[position]]] == selectMatches);
                }
                else {
                    mask.retainIf(position -> !nulls.value(position) && nestedMatches[nestedIds[ids[position]]] == selectMatches);
                }
            }
            else {
                Int2ByteOpenHashMap matchByValueId = dictionaryMatchCache(mask);
                if (nulls == null) {
                    mask.retainIf(position -> dictionaryMatchStatus(matchByValueId, nestedValues, nestedIds[ids[position]], inputs) == matchStatus(selectMatches));
                }
                else {
                    mask.retainIf(position -> !nulls.value(position) && dictionaryMatchStatus(matchByValueId, nestedValues, nestedIds[ids[position]], inputs) == matchStatus(selectMatches));
                }
            }
            return true;
        }
        if (inputs.dictionary().values() instanceof DictionaryVector nestedDictionary &&
                nestedDictionary.values() instanceof DictionaryVector innerDictionary &&
                innerDictionary.values() instanceof BinaryVector innerValues) {
            int[] ids = inputs.dictionary().ids();
            int[] nestedIds = nestedDictionary.ids();
            int[] innerIds = innerDictionary.ids();
            VectorAccess.BooleanValues nulls = nullFreeValues(inputs.nulls());
            if (shouldEvaluateFullDictionary(innerValues, mask)) {
                boolean[] innerMatches = evaluateDictionaryPredicate(innerValues, inputs);
                if (nulls == null) {
                    mask.retainIf(position -> innerMatches[innerIds[nestedIds[ids[position]]]] == selectMatches);
                }
                else {
                    mask.retainIf(position -> !nulls.value(position) && innerMatches[innerIds[nestedIds[ids[position]]]] == selectMatches);
                }
            }
            else {
                Int2ByteOpenHashMap matchByValueId = dictionaryMatchCache(mask);
                if (nulls == null) {
                    mask.retainIf(position -> dictionaryMatchStatus(matchByValueId, innerValues, innerIds[nestedIds[ids[position]]], inputs) == matchStatus(selectMatches));
                }
                else {
                    mask.retainIf(position -> !nulls.value(position) && dictionaryMatchStatus(matchByValueId, innerValues, innerIds[nestedIds[ids[position]]], inputs) == matchStatus(selectMatches));
                }
            }
            return true;
        }

        if (inputs.dictionary().values() instanceof BinaryVector values) {
            int[] ids = inputs.dictionary().ids();
            VectorAccess.BooleanValues nulls = nullFreeValues(inputs.nulls());
            if (shouldEvaluateFullDictionary(values, mask)) {
                boolean[] dictionaryMatches = evaluateDictionaryPredicate(values, inputs);
                if (nulls == null) {
                    mask.retainIf(position -> dictionaryMatches[ids[position]] == selectMatches);
                }
                else {
                    mask.retainIf(position -> !nulls.value(position) && dictionaryMatches[ids[position]] == selectMatches);
                }
            }
            else {
                Int2ByteOpenHashMap matchByValueId = dictionaryMatchCache(mask);
                if (nulls == null) {
                    mask.retainIf(position -> dictionaryMatchStatus(matchByValueId, values, ids[position], inputs) == matchStatus(selectMatches));
                }
                else {
                    mask.retainIf(position -> !nulls.value(position) && dictionaryMatchStatus(matchByValueId, values, ids[position], inputs) == matchStatus(selectMatches));
                }
            }
            return true;
        }
        return tryEvaluateGenericDictionaryPredicateMaskInPlace(inputs, mask, selectMatches);
    }

    private static void retainSourcePredicate(
            Mask mask,
            Vector nullVector,
            SourceMaskOptimization.PositionPredicate predicate,
            boolean selectMatches)
    {
        VectorAccess.BooleanValues nulls = nullFreeValues(nullVector);
        if (nulls == null) {
            mask.retainIf(position -> predicate.test(position) == selectMatches);
        }
        else {
            mask.retainIf(position -> !nulls.value(position) && predicate.test(position) == selectMatches);
        }
    }

    private boolean tryEvaluateGenericDictionaryPredicateMaskInPlace(DictionaryPredicateInputs inputs, Mask mask, boolean selectMatches)
    {
        Vector values = inputs.dictionary().values();
        VectorAccess.BinaryValues binaryValues = tryBinaryValues(values);
        if (binaryValues == null) {
            return false;
        }

        int[] ids = inputs.dictionary().ids();
        VectorAccess.BooleanValues nulls = nullFreeValues(inputs.nulls());
        if (shouldEvaluateFullDictionary(values.length(), mask)) {
            boolean[] dictionaryMatches = evaluateDictionaryPredicate(binaryValues, values.length(), inputs);
            if (nulls == null) {
                mask.retainIf(position -> dictionaryMatches[ids[position]] == selectMatches);
            }
            else {
                mask.retainIf(position -> !nulls.value(position) && dictionaryMatches[ids[position]] == selectMatches);
            }
        }
        else {
            byte selectedStatus = matchStatus(selectMatches);
            Int2ByteOpenHashMap matchByValueId = dictionaryMatchCache(mask);
            if (nulls == null) {
                mask.retainIf(position -> dictionaryMatchStatus(matchByValueId, binaryValues, ids[position], inputs) == selectedStatus);
            }
            else {
                mask.retainIf(position -> !nulls.value(position) && dictionaryMatchStatus(matchByValueId, binaryValues, ids[position], inputs) == selectedStatus);
            }
        }
        return true;
    }

    private Mask evaluateDictionaryPredicateMask(DictionaryPredicateInputs inputs, Mask mask, boolean selectMatches)
    {
        if (inputs.dictionary().values() instanceof DictionaryVector nestedDictionary && nestedDictionary.values() instanceof BinaryVector nestedValues) {
            int[] ids = inputs.dictionary().ids();
            int[] nestedIds = nestedDictionary.ids();
            VectorAccess.BooleanValues nulls = nullFreeValues(inputs.nulls());
            if (!shouldEvaluateFullDictionary(nestedValues, mask)) {
                return evaluateSparseNestedDictionaryPredicateMask(inputs, mask, selectMatches, nestedValues, ids, nestedIds, nulls);
            }

            boolean[] nestedMatches = evaluateDictionaryPredicate(nestedValues, inputs);

            int selectedCount = 0;
            if (nulls == null) {
                for (int position : mask) {
                    if (nestedMatches[nestedIds[ids[position]]] == selectMatches) {
                        selectedCount++;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (!nulls.value(position) && nestedMatches[nestedIds[ids[position]]] == selectMatches) {
                        selectedCount++;
                    }
                }
            }

            int[] positions = new int[selectedCount];
            int outputIndex = 0;
            if (nulls == null) {
                for (int position : mask) {
                    if (nestedMatches[nestedIds[ids[position]]] == selectMatches) {
                        positions[outputIndex++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (!nulls.value(position) && nestedMatches[nestedIds[ids[position]]] == selectMatches) {
                        positions[outputIndex++] = position;
                    }
                }
            }
            return allocator.allocateSparseMask(allocationContext, positions, outputIndex, mask.size());
        }

        if (inputs.dictionary().values() instanceof DictionaryVector nestedDictionary &&
                nestedDictionary.values() instanceof DictionaryVector innerDictionary &&
                innerDictionary.values() instanceof BinaryVector innerValues) {
            int[] ids = inputs.dictionary().ids();
            int[] nestedIds = nestedDictionary.ids();
            int[] innerIds = innerDictionary.ids();
            VectorAccess.BooleanValues nulls = nullFreeValues(inputs.nulls());
            if (!shouldEvaluateFullDictionary(innerValues, mask)) {
                return evaluateSparseDoubleNestedDictionaryPredicateMask(inputs, mask, selectMatches, innerValues, ids, nestedIds, innerIds, nulls);
            }

            boolean[] innerMatches = evaluateDictionaryPredicate(innerValues, inputs);

            int selectedCount = 0;
            if (nulls == null) {
                for (int position : mask) {
                    if (innerMatches[innerIds[nestedIds[ids[position]]]] == selectMatches) {
                        selectedCount++;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (!nulls.value(position) && innerMatches[innerIds[nestedIds[ids[position]]]] == selectMatches) {
                        selectedCount++;
                    }
                }
            }

            int[] positions = new int[selectedCount];
            int outputIndex = 0;
            if (nulls == null) {
                for (int position : mask) {
                    if (innerMatches[innerIds[nestedIds[ids[position]]]] == selectMatches) {
                        positions[outputIndex++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (!nulls.value(position) && innerMatches[innerIds[nestedIds[ids[position]]]] == selectMatches) {
                        positions[outputIndex++] = position;
                    }
                }
            }
            return allocator.allocateSparseMask(allocationContext, positions, outputIndex, mask.size());
        }

        if (inputs.dictionary().values() instanceof BinaryVector values) {
            int[] ids = inputs.dictionary().ids();
            VectorAccess.BooleanValues nulls = nullFreeValues(inputs.nulls());
            if (!shouldEvaluateFullDictionary(values, mask)) {
                return evaluateSparseDictionaryPredicateMask(inputs, mask, selectMatches, values, ids, nulls);
            }

            boolean[] dictionaryMatches = evaluateDictionaryPredicate(values, inputs);

            int selectedCount = 0;
            if (nulls == null) {
                for (int position : mask) {
                    if (dictionaryMatches[ids[position]] == selectMatches) {
                        selectedCount++;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (!nulls.value(position) && dictionaryMatches[ids[position]] == selectMatches) {
                        selectedCount++;
                    }
                }
            }

            int[] positions = new int[selectedCount];
            int outputIndex = 0;
            if (nulls == null) {
                for (int position : mask) {
                    if (dictionaryMatches[ids[position]] == selectMatches) {
                        positions[outputIndex++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (!nulls.value(position) && dictionaryMatches[ids[position]] == selectMatches) {
                        positions[outputIndex++] = position;
                    }
                }
            }
            return allocator.allocateSparseMask(allocationContext, positions, outputIndex, mask.size());
        }
        return evaluateGenericDictionaryPredicateMask(inputs, mask, selectMatches);
    }

    private Mask evaluateGenericDictionaryPredicateMask(DictionaryPredicateInputs inputs, Mask mask, boolean selectMatches)
    {
        Vector values = inputs.dictionary().values();
        VectorAccess.BinaryValues binaryValues = tryBinaryValues(values);
        if (binaryValues == null) {
            return null;
        }

        int[] ids = inputs.dictionary().ids();
        VectorAccess.BooleanValues nulls = nullFreeValues(inputs.nulls());
        if (!shouldEvaluateFullDictionary(values.length(), mask)) {
            return evaluateSparseDictionaryPredicateMask(inputs, mask, selectMatches, binaryValues, ids, nulls);
        }

        boolean[] dictionaryMatches = evaluateDictionaryPredicate(binaryValues, values.length(), inputs);

        int selectedCount = 0;
        if (nulls == null) {
            for (int position : mask) {
                if (dictionaryMatches[ids[position]] == selectMatches) {
                    selectedCount++;
                }
            }
        }
        else {
            for (int position : mask) {
                if (!nulls.value(position) && dictionaryMatches[ids[position]] == selectMatches) {
                    selectedCount++;
                }
            }
        }

        int[] positions = new int[selectedCount];
        int outputIndex = 0;
        if (nulls == null) {
            for (int position : mask) {
                if (dictionaryMatches[ids[position]] == selectMatches) {
                    positions[outputIndex++] = position;
                }
            }
        }
        else {
            for (int position : mask) {
                if (!nulls.value(position) && dictionaryMatches[ids[position]] == selectMatches) {
                    positions[outputIndex++] = position;
                }
            }
        }
        return allocator.allocateSparseMask(allocationContext, positions, outputIndex, mask.size());
    }

    private Mask evaluateSparseNestedDictionaryPredicateMask(
            DictionaryPredicateInputs inputs,
            Mask mask,
            boolean selectMatches,
            BinaryVector nestedValues,
            int[] ids,
            int[] nestedIds,
            VectorAccess.BooleanValues nulls)
    {
        byte selectedStatus = matchStatus(selectMatches);
        Int2ByteOpenHashMap matchByValueId = dictionaryMatchCache(mask);
        boolean allSelected = true;
        int[] positions = null;
        int selectedCount = 0;
        if (nulls == null) {
            for (int position : mask) {
                if (dictionaryMatchStatus(matchByValueId, nestedValues, nestedIds[ids[position]], inputs) == selectedStatus) {
                    if (!allSelected) {
                        positions = ensurePositionCapacity(positions, selectedCount, mask.count());
                        positions[selectedCount] = position;
                    }
                    selectedCount++;
                }
                else if (allSelected) {
                    allSelected = false;
                    if (selectedCount > 0) {
                        positions = selectedPrefix(mask, selectedCount);
                    }
                }
            }
        }
        else {
            for (int position : mask) {
                if (!nulls.value(position) && dictionaryMatchStatus(matchByValueId, nestedValues, nestedIds[ids[position]], inputs) == selectedStatus) {
                    if (!allSelected) {
                        positions = ensurePositionCapacity(positions, selectedCount, mask.count());
                        positions[selectedCount] = position;
                    }
                    selectedCount++;
                }
                else if (allSelected) {
                    allSelected = false;
                    if (selectedCount > 0) {
                        positions = selectedPrefix(mask, selectedCount);
                    }
                }
            }
        }
        if (allSelected) {
            return allocator.copyMask(allocationContext, mask);
        }
        return allocator.allocateSparseMask(allocationContext, positions, selectedCount, mask.size());
    }

    private Mask evaluateSparseDoubleNestedDictionaryPredicateMask(
            DictionaryPredicateInputs inputs,
            Mask mask,
            boolean selectMatches,
            BinaryVector innerValues,
            int[] ids,
            int[] nestedIds,
            int[] innerIds,
            VectorAccess.BooleanValues nulls)
    {
        byte selectedStatus = matchStatus(selectMatches);
        Int2ByteOpenHashMap matchByValueId = dictionaryMatchCache(mask);
        boolean allSelected = true;
        int[] positions = null;
        int selectedCount = 0;
        int inputCount = mask.selectedCount();
        if (nulls == null) {
            for (int inputIndex = 0; inputIndex < inputCount; inputIndex++) {
                int position = mask.position(inputIndex);
                if (dictionaryMatchStatus(matchByValueId, innerValues, innerIds[nestedIds[ids[position]]], inputs) == selectedStatus) {
                    if (!allSelected) {
                        positions = ensurePositionCapacity(positions, selectedCount, inputCount);
                        positions[selectedCount] = position;
                    }
                    selectedCount++;
                }
                else if (allSelected) {
                    allSelected = false;
                    if (selectedCount > 0) {
                        positions = selectedPrefix(mask, selectedCount);
                    }
                }
            }
        }
        else {
            for (int inputIndex = 0; inputIndex < inputCount; inputIndex++) {
                int position = mask.position(inputIndex);
                if (!nulls.value(position) && dictionaryMatchStatus(matchByValueId, innerValues, innerIds[nestedIds[ids[position]]], inputs) == selectedStatus) {
                    if (!allSelected) {
                        positions = ensurePositionCapacity(positions, selectedCount, inputCount);
                        positions[selectedCount] = position;
                    }
                    selectedCount++;
                }
                else if (allSelected) {
                    allSelected = false;
                    if (selectedCount > 0) {
                        positions = selectedPrefix(mask, selectedCount);
                    }
                }
            }
        }
        if (allSelected) {
            return allocator.copyMask(allocationContext, mask);
        }
        return allocator.allocateSparseMask(allocationContext, positions, selectedCount, mask.size());
    }

    private Mask evaluateSparseDictionaryPredicateMask(
            DictionaryPredicateInputs inputs,
            Mask mask,
            boolean selectMatches,
            BinaryVector values,
            int[] ids,
            VectorAccess.BooleanValues nulls)
    {
        byte selectedStatus = matchStatus(selectMatches);
        Int2ByteOpenHashMap matchByValueId = dictionaryMatchCache(mask);
        boolean allSelected = true;
        int[] positions = null;
        int selectedCount = 0;
        if (nulls == null) {
            for (int position : mask) {
                if (dictionaryMatchStatus(matchByValueId, values, ids[position], inputs) == selectedStatus) {
                    if (!allSelected) {
                        positions = ensurePositionCapacity(positions, selectedCount, mask.count());
                        positions[selectedCount] = position;
                    }
                    selectedCount++;
                }
                else if (allSelected) {
                    allSelected = false;
                    if (selectedCount > 0) {
                        positions = selectedPrefix(mask, selectedCount);
                    }
                }
            }
        }
        else {
            for (int position : mask) {
                if (!nulls.value(position) && dictionaryMatchStatus(matchByValueId, values, ids[position], inputs) == selectedStatus) {
                    if (!allSelected) {
                        positions = ensurePositionCapacity(positions, selectedCount, mask.count());
                        positions[selectedCount] = position;
                    }
                    selectedCount++;
                }
                else if (allSelected) {
                    allSelected = false;
                    if (selectedCount > 0) {
                        positions = selectedPrefix(mask, selectedCount);
                    }
                }
            }
        }
        if (allSelected) {
            return allocator.copyMask(allocationContext, mask);
        }
        return allocator.allocateSparseMask(allocationContext, positions, selectedCount, mask.size());
    }

    private Mask evaluateSparseDictionaryPredicateMask(
            DictionaryPredicateInputs inputs,
            Mask mask,
            boolean selectMatches,
            VectorAccess.BinaryValues values,
            int[] ids,
            VectorAccess.BooleanValues nulls)
    {
        byte selectedStatus = matchStatus(selectMatches);
        Int2ByteOpenHashMap matchByValueId = dictionaryMatchCache(mask);
        boolean allSelected = true;
        int[] positions = null;
        int selectedCount = 0;
        if (nulls == null) {
            for (int position : mask) {
                if (dictionaryMatchStatus(matchByValueId, values, ids[position], inputs) == selectedStatus) {
                    if (!allSelected) {
                        positions = ensurePositionCapacity(positions, selectedCount, mask.count());
                        positions[selectedCount] = position;
                    }
                    selectedCount++;
                }
                else if (allSelected) {
                    allSelected = false;
                    if (selectedCount > 0) {
                        positions = selectedPrefix(mask, selectedCount);
                    }
                }
            }
        }
        else {
            for (int position : mask) {
                if (!nulls.value(position) && dictionaryMatchStatus(matchByValueId, values, ids[position], inputs) == selectedStatus) {
                    if (!allSelected) {
                        positions = ensurePositionCapacity(positions, selectedCount, mask.count());
                        positions[selectedCount] = position;
                    }
                    selectedCount++;
                }
                else if (allSelected) {
                    allSelected = false;
                    if (selectedCount > 0) {
                        positions = selectedPrefix(mask, selectedCount);
                    }
                }
            }
        }
        if (allSelected) {
            return allocator.copyMask(allocationContext, mask);
        }
        return allocator.allocateSparseMask(allocationContext, positions, selectedCount, mask.size());
    }

    private static int[] selectedPrefix(Mask mask, int selectedCount)
    {
        int[] positions = new int[Math.min(mask.count(), Math.max(16, selectedCount + 1))];
        if (mask.all()) {
            for (int index = 0; index < selectedCount; index++) {
                positions[index] = index;
            }
            return positions;
        }
        int index = 0;
        for (int position : mask) {
            if (index == selectedCount) {
                break;
            }
            positions[index++] = position;
        }
        return positions;
    }

    private static int[] ensurePositionCapacity(int[] positions, int selectedCount, int maxCount)
    {
        if (positions == null) {
            return new int[Math.min(maxCount, Math.max(16, selectedCount + 1))];
        }
        if (selectedCount < positions.length) {
            return positions;
        }
        return Arrays.copyOf(positions, Math.min(maxCount, positions.length * 2));
    }

    private SourcePredicateInputs resolveSourcePredicateInputs(Reference reference, Mask mask)
    {
        if (reference.stream() != Stream.VALUES || !(reference.producer() instanceof Variable variable)) {
            return null;
        }

        BoundSourceMaskOptimization optimization = sourceMaskOptimizations.get(variable);
        if (optimization == null) {
            return null;
        }

        Streams source = evaluateArgument(optimization.source(), mask, PrimitiveFunction.VALUES_AND_NULLS_INPUT_STREAMS, true);
        if (!source.has(Stream.VALUES)) {
            return null;
        }

        return new SourcePredicateInputs(source.values(), source.getOrNull(Stream.NULLS), optimization.predicate());
    }

    private static DictionaryPredicateInputs bindDictionaryPredicateInputs(SourcePredicateInputs source)
    {
        DictionaryVector dictionary = (DictionaryVector) source.values();

        Vector dictionaryValues = dictionary.values();
        while (dictionaryValues instanceof DictionaryVector nested) {
            dictionaryValues = nested.values();
        }
        SourceMaskOptimization.PositionPredicate predicate = source.predicate().bind(dictionaryValues).orElse(null);
        return predicate == null
                ? null
                : new DictionaryPredicateInputs(dictionary, source.nulls(), predicate);
    }

    private static boolean[] evaluateDictionaryPredicate(BinaryVector values, DictionaryPredicateInputs inputs)
    {
        boolean[] matches = new boolean[values.length()];
        for (int position = 0; position < matches.length; position++) {
            matches[position] = inputs.predicate().test(position);
        }
        return matches;
    }

    private static boolean[] evaluateDictionaryPredicate(VectorAccess.BinaryValues values, int length, DictionaryPredicateInputs inputs)
    {
        boolean[] matches = new boolean[length];
        for (int position = 0; position < matches.length; position++) {
            matches[position] = inputs.predicate().test(position);
        }
        return matches;
    }

    private boolean shouldEvaluateFullDictionary(BinaryVector values, Mask mask)
    {
        return shouldEvaluateFullDictionary(values.length(), mask);
    }

    private boolean shouldEvaluateFullDictionary(int valueCount, Mask mask)
    {
        return valueCount <= (long) mask.selectedCount() * policy.dictionaryPeelSparseRatio();
    }

    private static Int2ByteOpenHashMap dictionaryMatchCache(Mask mask)
    {
        Int2ByteOpenHashMap matchByValueId = new Int2ByteOpenHashMap(Math.min(mask.selectedCount(), 1024));
        matchByValueId.defaultReturnValue(UNKNOWN_DICTIONARY_MATCH);
        return matchByValueId;
    }

    private static VectorAccess.BooleanValues nullFreeValues(Vector nulls)
    {
        return VectorAccess.isAllFalseNulls(nulls) ? null : VectorAccess.booleanValues(nulls);
    }

    private static byte dictionaryMatchStatus(Int2ByteOpenHashMap matchByValueId, BinaryVector values, int valueId, DictionaryPredicateInputs inputs)
    {
        byte status = matchByValueId.get(valueId);
        if (status != UNKNOWN_DICTIONARY_MATCH) {
            return status;
        }
        status = inputs.predicate().test(valueId)
                ? DICTIONARY_MATCH
                : DICTIONARY_MISMATCH;
        matchByValueId.put(valueId, status);
        return status;
    }

    private static byte dictionaryMatchStatus(Int2ByteOpenHashMap matchByValueId, VectorAccess.BinaryValues values, int valueId, DictionaryPredicateInputs inputs)
    {
        byte status = matchByValueId.get(valueId);
        if (status != UNKNOWN_DICTIONARY_MATCH) {
            return status;
        }
        status = inputs.predicate().test(valueId)
                ? DICTIONARY_MATCH
                : DICTIONARY_MISMATCH;
        matchByValueId.put(valueId, status);
        return status;
    }

    private static VectorAccess.BinaryValues tryBinaryValues(Vector values)
    {
        try {
            return VectorAccess.binaryValues(values);
        }
        catch (IllegalArgumentException _) {
            return null;
        }
    }

    private static byte matchStatus(boolean matches)
    {
        return matches ? DICTIONARY_MATCH : DICTIONARY_MISMATCH;
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

        maskInvocationInputs.clear();
        for (int index = 0; index < call.arguments().size(); index++) {
            Reference argument = call.arguments().get(index);
            Set<Stream> requiredInputStreams = maskFunction.requiredMaskInputStreams(index);
            if (maskFunction.requiresCompletedInputCompanionStreamsForMask()) {
                maskInvocationInputs.add(evaluateArgument(argument, mask, requiredInputStreams, false));
                continue;
            }
            maskInvocationInputs.add(evaluateArgument(argument, mask, requiredInputStreams, true));
        }
        maskInvocation.function(maskFunction);
        return maskInvocation;
    }

    private record BoundSourceMaskOptimization(
            Reference source,
            SourceMaskOptimization.SourceValuePredicate predicate) {}

    private record SourcePredicateInputs(
            Vector values,
            Vector nulls,
            SourceMaskOptimization.SourceValuePredicate predicate) {}

    private record DictionaryPredicateInputs(
            DictionaryVector dictionary,
            Vector nulls,
            SourceMaskOptimization.PositionPredicate predicate) {}

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

    private Streams evaluateAvailableReference(Reference reference, Mask mask)
    {
        if (reference.stream() == Stream.VALUES) {
            return evaluate(reference, mask);
        }

        return switch (reference.producer()) {
            case org.weakref.nitro.operator.evaluator.ir.Input(int index) -> {
                Vector available = input.resolve(reference, mask);
                yield available == null ? Streams.empty() : Streams.of(reference.stream(), available);
            }
            case Variable variable -> {
                Assignment assignment = assignments.get(variable);
                if (assignment != null && assignment.operation() instanceof Literal) {
                    yield Streams.empty();
                }
                if (reference.stream() != Stream.VALUES && !isExplicitlyProjectedOrMemoized(reference)) {
                    yield Streams.empty();
                }
                yield evaluate(reference, mask);
            }
        };
    }

    private boolean isExplicitlyProjectedOrMemoized(Reference reference)
    {
        Set<Stream> projectedStreams = explicitProjectedStreamsByProducer.get(reference.producer());
        if (projectedStreams != null && projectedStreams.contains(reference.stream())) {
            return true;
        }
        if (!memoizedProducers.contains(reference.producer())) {
            return false;
        }
        Set<Stream> memoizedStreams = memoizedStreamsByProducer.get(reference.producer());
        return memoizedStreams != null && memoizedStreams.contains(reference.stream());
    }

    private static boolean readBoolean(Vector vector, int position)
    {
        return switch (vector) {
            case BooleanVector values -> values.values()[position];
            case org.weakref.nitro.data.ConcatenatedBooleanVector values -> values.value(position);
            case DictionaryVector values -> readBoolean(values.values(), values.ids()[position]);
            case RleVector values -> readBoolean(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected boolean vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static boolean readOptionalBoolean(Vector vector, int position)
    {
        // Companion streams can be produced under different short-circuit branch masks. Their storage only extends
        // through the last evaluated position; positions beyond that extent were not evaluated and are therefore
        // absent (false), rather than out-of-bounds errors.
        return position < vector.length() && readBoolean(vector, position);
    }

    private Vector optionalBooleanStream(org.weakref.nitro.operator.evaluator.ir.Producer producer, Stream stream, Mask mask)
    {
        return evaluate(new Reference(producer, stream), mask).getOrNull(stream);
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
        int length = mask.size();
        if (wantsValues && !completed.has(Stream.VALUES)) {
            throw new IllegalArgumentException("VALUES stream not produced for request");
        }
        if (wantsNulls && !completed.has(Stream.NULLS)) {
            // The producer had no nulls. Hand back an all-false BooleanVector, but skip the redundant
            // clear and pre-record the all-false state: a freshly allocated (or pool-reused) buffer is
            // already zeroed, and this stream is terminal (consumers only read it), so marking it
            // all-false lets downstream null-free checks (isAllFalseNulls) short-circuit in O(1) rather
            // than rescanning every batch. Mirrors Velox's absent null buffer (mayHaveNulls).
            completed = completed.with(Stream.NULLS, allocator.borrowAllFalseBoolean(allocationContext, length));
        }
        if (wantsErrors && !completed.has(Stream.ERRORS)) {
            completed = completed.with(Stream.ERRORS, allocator.borrowAllFalseBoolean(allocationContext, length));
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

    private static boolean isNull(Vector nulls, int position)
    {
        return nulls != null && readBoolean(nulls, position);
    }

    private static boolean isError(Vector errors, int position)
    {
        return errors != null && readBoolean(errors, position);
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
        try {
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
        finally {
            releaseOrderedTerms();
        }
    }

    private Mask evaluateAdaptiveAndTrueMask(List<MaskExpression> terms, Mask mask)
    {
        terms = orderTerms(terms, BooleanOperator.AND);
        try {
            Mask activeMask = mask;
            for (MaskExpression term : terms) {
                activeMask = evaluateMeasuredTrueMask(term, activeMask, BooleanOperator.AND);
                if (activeMask.none()) {
                    return emptyMask(mask.size());
                }
            }
            return activeMask;
        }
        finally {
            releaseOrderedTerms();
        }
    }

    private MaskOutcome evaluateAdaptiveOr(List<MaskExpression> terms, Mask mask)
    {
        terms = orderTerms(terms, BooleanOperator.OR);
        try {
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
        finally {
            releaseOrderedTerms();
        }
    }

    private Mask evaluateAdaptiveOrTrueMask(List<MaskExpression> terms, Mask mask)
    {
        terms = orderTerms(terms, BooleanOperator.OR);
        try {
            Mask acceptedMask = emptyMask(mask.size());
            if (!policy.orShortCircuitRemaining()) {
                for (MaskExpression term : terms) {
                    Mask termTrueMask = evaluateMeasuredTrueMask(term, mask, BooleanOperator.OR);
                    acceptedMask = unionMasks(acceptedMask, termTrueMask, mask.size());
                    if (acceptedMask.all()) {
                        break;
                    }
                }
                return acceptedMask;
            }

            Mask remainingMask = mask;
            for (int index = 0; index < terms.size(); index++) {
                if (remainingMask.none()) {
                    break;
                }

                MaskExpression term = terms.get(index);
                Mask termTrueMask = evaluateMeasuredTrueMask(term, remainingMask, BooleanOperator.OR);
                acceptedMask = unionMasks(acceptedMask, termTrueMask, mask.size());
                if (index + 1 == terms.size() || termTrueMask.selectedCount() == remainingMask.selectedCount()) {
                    break;
                }
                if (shouldEvaluateFinalOrTermOnFullMask(terms, index, mask, remainingMask, termTrueMask)) {
                    Mask finalTermTrueMask = evaluateMeasuredTrueMask(terms.get(index + 1), mask, BooleanOperator.OR);
                    acceptedMask = unionMasks(acceptedMask, finalTermTrueMask, mask.size());
                    break;
                }
                remainingMask = termTrueMask.none() ? remainingMask : allocator.differenceMask(allocationContext, remainingMask, termTrueMask);
            }
            return acceptedMask;
        }
        finally {
            releaseOrderedTerms();
        }
    }

    private boolean shouldEvaluateFinalOrTermOnFullMask(List<MaskExpression> terms, int index, Mask mask, Mask remainingMask, Mask termTrueMask)
    {
        if (!policy.orEvaluateFinalTermOnFullMask() || index + 2 != terms.size() || termTrueMask.none()) {
            return false;
        }
        int inputRows = mask.selectedCount();
        int finalRemainingRows = remainingMask.selectedCount() - termTrueMask.selectedCount();
        return finalRemainingRows >= policy.orFinalTermMinRemainingRows() &&
                finalRemainingRows * 100L >= inputRows * (long) policy.orFinalTermMinRemainingPercent();
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
            case LongDomainMask(Reference input, LongDomain domain) -> evaluateLongDomainMask(input, domain, mask);
            case RangeConstrainedAndMask(_, _, _, _, _, AndMask fallback) -> evaluateMaskOutcome(fallback, mask);
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
            case LongDomainMask(Reference input, LongDomain domain) -> evaluateTrueLongDomainMask(input, domain, mask);
            case RangeConstrainedAndMask(_, _, _, _, _, AndMask fallback) -> evaluateTrueMask(fallback, mask);
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
            case LongDomainMask(Reference input, LongDomain domain) -> {
                if (!evaluateTrueLongDomainMaskInPlace(input, domain, mask)) {
                    Mask result = evaluateTrueLongDomainMask(input, domain, mask);
                    if (result != mask) {
                        mask.copyFrom(result);
                    }
                }
                yield mask;
            }
            case RangeConstrainedAndMask(
                    Reference input,
                    Object lowerExclusive,
                    Object upperExclusive,
                    RangeConstraint.Kernel kernel,
                    List<MaskExpression> remainingTerms,
                    AndMask fallback) -> evaluateRangeConstrainedAndMaskInPlace(
                    input,
                    lowerExclusive,
                    upperExclusive,
                    kernel,
                    remainingTerms,
                    fallback,
                    mask);
            case ReferenceMask(Reference reference) -> {
                if (!tryEvaluatePrimitiveTrueMaskInPlace(reference, mask)) {
                    MaskExpression resolved = MaskExpressionResolver.resolve(plan, new ReferenceMask(reference));
                    if (!(resolved instanceof ReferenceMask(Reference resolvedReference) && resolvedReference.equals(reference))) {
                        yield evaluateTrueMaskInPlace(resolved, mask);
                    }
                    evaluateTrueReferenceMaskFallbackInPlace(reference, mask);
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

    /**
     * Completes an in-place reference-mask evaluation after expression resolution and the primitive in-place
     * contract have declined it. A flat, null-free Boolean result can compact the caller-owned mask directly;
     * materializing a second mask only to copy it back defeats the output mask's pooled high-water storage.
     */
    private void evaluateTrueReferenceMaskFallbackInPlace(Reference reference, Mask mask)
    {
        Mask primitiveMask = tryEvaluatePrimitiveTrueMask(reference, mask);
        if (primitiveMask != null) {
            if (primitiveMask != mask) {
                mask.copyFrom(primitiveMask);
            }
            return;
        }

        Mask inputMask = tryResolveInputMask(reference, mask, true);
        if (inputMask != null) {
            if (inputMask != mask) {
                mask.copyFrom(inputMask);
            }
            return;
        }

        Vector values = evaluate(reference, mask).get(reference.stream());
        Vector errors = optionalBooleanStream(reference.producer(), Stream.ERRORS, mask);
        Vector nulls = optionalBooleanStream(reference.producer(), Stream.NULLS, mask);
        if (policy.inPlaceFlatBooleanClassifier() && values instanceof BooleanVector booleanValues &&
                VectorAccess.isAllFalseNulls(nulls) && VectorAccess.isAllFalseNulls(errors)) {
            mask.retainBooleans(booleanValues.values(), true);
            return;
        }

        Mask result = classifyTrueBooleanMask(values, nulls, errors, mask);
        if (result != mask) {
            mask.copyFrom(result);
        }
    }

    private boolean evaluateFalseMaskInPlace(MaskExpression expression, Mask mask)
    {
        return switch (expression) {
            case AllMask _ -> {
                mask.clear(mask.size());
                yield true;
            }
            case LongDomainMask(Reference input, LongDomain domain) -> {
                Mask result = evaluateLongDomainMask(input, domain, mask).falseMask(allocator, allocationContext, mask);
                if (result != mask) {
                    mask.copyFrom(result);
                }
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
        try {
            for (MaskExpression term : terms) {
                if (mask.none()) {
                    break;
                }
                evaluateMeasuredTrueMaskInPlace(term, mask, BooleanOperator.AND);
            }
            return mask;
        }
        finally {
            releaseOrderedTerms();
        }
    }

    private Mask evaluateRangeConstrainedAndMaskInPlace(
            Reference inputReference,
            Object lowerExclusive,
            Object upperExclusive,
            RangeConstraint.Kernel kernel,
            List<MaskExpression> remainingTerms,
            AndMask fallback,
            Mask mask)
    {
        Streams input = evaluateArgument(inputReference, mask, PrimitiveFunction.ALL_INPUT_STREAMS, true);
        if (!kernel.apply(input, lowerExclusive, upperExclusive, mask)) {
            return evaluateTrueMaskInPlace(fallback, mask);
        }
        List<MaskExpression> terms = orderTerms(remainingTerms, BooleanOperator.AND);
        try {
            for (MaskExpression term : terms) {
                if (mask.none()) {
                    break;
                }
                evaluateMeasuredTrueMaskInPlace(term, mask, BooleanOperator.AND);
            }
            return mask;
        }
        finally {
            releaseOrderedTerms();
        }
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
        if (!policy.adaptiveMaskReordering()) {
            return terms;
        }

        if (policy.recycleControlFrames()) {
            if (termOrderFrames == null) {
                termOrderFrames = new TermOrderFrames();
            }
            ArrayList<MaskExpression> ordered = termOrderFrames.acquire(terms.size());
            ordered.addAll(terms);
            // List.sort is stable, so equal scores retain the plan's original term order.
            ordered.sort(Comparator.comparingDouble(term -> score(term, operator)));
            return ordered;
        }

        ArrayList<IndexedTerm> indexedTerms = new ArrayList<>(terms.size());
        for (int index = 0; index < terms.size(); index++) {
            indexedTerms.add(new IndexedTerm(index, terms.get(index)));
        }
        indexedTerms.sort(Comparator
                .comparingDouble((IndexedTerm indexedTerm) -> score(indexedTerm.term(), operator))
                .thenComparingInt(IndexedTerm::index));
        ArrayList<MaskExpression> ordered = new ArrayList<>(indexedTerms.size());
        for (IndexedTerm indexedTerm : indexedTerms) {
            ordered.add(indexedTerm.term());
        }
        return List.copyOf(ordered);
    }

    private void releaseOrderedTerms()
    {
        if (policy.adaptiveMaskReordering() && policy.recycleControlFrames()) {
            termOrderFrames.release();
        }
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
        return allocator.allocateEmptyMask(allocationContext, size);
    }

    private enum BooleanOperator
    {
        AND,
        OR
    }

    private record IndexedTerm(int index, MaskExpression term) {}

    private sealed interface PreboundMask
            permits DirectPreboundMask, CompiledPreboundMask {}

    private record DirectPreboundMask(Reference input)
            implements PreboundMask {}

    private static final class CompiledPreboundMask
            implements PreboundMask
    {
        private final List<Reference> arguments;
        private final List<Reference> excludedComponents;
        private final ProjectionMaskCompiler.CompiledMask compiled;
        private final ArrayList<Streams> inputs;

        private CompiledPreboundMask(
                List<Reference> arguments,
                List<Reference> excludedComponents,
                ProjectionMaskCompiler.CompiledMask compiled)
        {
            this.arguments = List.copyOf(arguments);
            this.excludedComponents = List.copyOf(excludedComponents);
            this.compiled = compiled;
            checkArgument(arguments.size() == compiled.argumentCount(), "Compiled mask argument count does not match call");
            this.inputs = new ArrayList<>(arguments.size());
        }

        private List<Reference> arguments()
        {
            return arguments;
        }

        private List<Reference> excludedComponents()
        {
            return excludedComponents;
        }

        private ProjectionMaskCompiler.CompiledMask compiled()
        {
            return compiled;
        }

        private ArrayList<Streams> inputs()
        {
            return inputs;
        }
    }

    private static final class TermOrderFrames
    {
        private final ArrayList<ArrayList<MaskExpression>> frames = new ArrayList<>();
        private int depth;

        private ArrayList<MaskExpression> acquire(int capacity)
        {
            if (depth == frames.size()) {
                frames.add(new ArrayList<>(capacity));
            }
            ArrayList<MaskExpression> frame = frames.get(depth++);
            frame.clear();
            frame.ensureCapacity(capacity);
            return frame;
        }

        private void release()
        {
            if (depth <= 0) {
                throw new IllegalStateException("No active term-order frame");
            }
            depth--;
        }
    }

    private record DictionaryPeeling(int[] ids, int rowCount, Mask baseMask, List<Streams> inputs) {}

    private record DictionaryDomainCacheEntry(DictionaryDomainCacheKey key, Streams result) {}

    private record DictionaryDomainCacheKey(List<StableVectorKey> vectors, Set<Stream> requestedStreams)
    {
        private boolean matches(DictionaryDomainCacheKey other)
        {
            if (!requestedStreams.equals(other.requestedStreams) || vectors.size() != other.vectors.size()) {
                return false;
            }
            for (int index = 0; index < vectors.size(); index++) {
                if (!vectors.get(index).matches(other.vectors.get(index))) {
                    return false;
                }
            }
            return true;
        }
    }

    private record StableVectorKey(Vector vector, long generation, long fingerprint, byte kind)
    {
        private static final byte VECTOR = 0;
        private static final byte ABSENT = 1;
        private static final byte CONSTANT_FALSE = 2;
        private static final byte CONSTANT_TRUE = 3;

        private static StableVectorKey vector(Vector vector, long generation, long fingerprint)
        {
            return new StableVectorKey(vector, generation, fingerprint, VECTOR);
        }

        private static StableVectorKey absent()
        {
            return new StableVectorKey(null, -1, Vector.NO_CONTENT_FINGERPRINT, ABSENT);
        }

        private static StableVectorKey constant(boolean value)
        {
            return new StableVectorKey(null, -1, Vector.NO_CONTENT_FINGERPRINT, value ? CONSTANT_TRUE : CONSTANT_FALSE);
        }

        private boolean matches(StableVectorKey other)
        {
            if (kind != other.kind) {
                return false;
            }
            if (kind != VECTOR) {
                return true;
            }
            if (vector == other.vector && generation == other.generation) {
                return true;
            }
            return fingerprint != Vector.NO_CONTENT_FINGERPRINT &&
                    fingerprint == other.fingerprint &&
                    vector.hasSameContent(other.vector);
        }

        @Override
        public String toString()
        {
            return kind == VECTOR
                    ? vector.getClass().getSimpleName() + '@' + Integer.toHexString(System.identityHashCode(vector)) + ':' + generation + ':' + fingerprint
                    : "constant:" + kind;
        }
    }

    private static final class PrimitiveMaskInvocation
    {
        private MaskEvaluablePrimitiveFunction function;
        private final List<Streams> inputs;

        private PrimitiveMaskInvocation(List<Streams> inputs)
        {
            this.inputs = inputs;
        }

        private MaskEvaluablePrimitiveFunction function()
        {
            return function;
        }

        private void function(MaskEvaluablePrimitiveFunction function)
        {
            this.function = function;
        }

        private List<Streams> inputs()
        {
            return inputs;
        }
    }

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
