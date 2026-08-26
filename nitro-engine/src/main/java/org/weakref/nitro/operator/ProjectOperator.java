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

import org.weakref.nitro.core.execution.ExecutionDiagnostics;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.ValueDemand;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.jit.FusedProjectionCompiler.CompiledMultiProjection;
import org.weakref.nitro.operator.evaluator.PlanEvaluator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.InputDependencies;
import org.weakref.nitro.operator.evaluator.ir.Producer;
import org.weakref.nitro.operator.evaluator.ir.Reference;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ProjectOperator
        implements Operator
{
    public static final String PLANNED_ASSIGNMENTS = "nitro.projection.planned-assignments";
    public static final String PLANNED_COMPUTED_OUTPUTS = "nitro.projection.planned-computed-outputs";
    public static final String GENERATED_OUTPUTS = "nitro.projection.generated-outputs";
    public static final String GENERATED_KERNELS = "nitro.projection.generated-kernels";
    public static final String GENERATED_ATTEMPTS = "nitro.projection.generated-attempts";
    public static final String GENERATED_SUCCESSES = "nitro.projection.generated-successes";
    public static final String GENERATED_ERROR_FALLBACKS = "nitro.projection.generated-error-fallbacks";
    public static final String GENERATED_LAYOUT_FALLBACKS = "nitro.projection.generated-layout-fallbacks";
    public static final String GENERATED_SELECTED_POSITIONS = "nitro.projection.generated-selected-positions";
    public static final String FLAT_INPUT_POSITIONS = "nitro.projection.flat-input-positions";
    public static final String DICTIONARY_INPUT_POSITIONS = "nitro.projection.dictionary-input-positions";
    public static final String RLE_INPUT_POSITIONS = "nitro.projection.rle-input-positions";
    public static final String OTHER_INPUT_POSITIONS = "nitro.projection.other-input-positions";
    public static final String INTEGER_WIDENING_POSITIONS = "nitro.projection.integer-widening-positions";
    public static final String DICTIONARY_FLATTENING_POSITIONS = "nitro.projection.dictionary-flattening-positions";
    public static final String DICTIONARY_NULL_EXPANSION_POSITIONS = "nitro.projection.dictionary-null-expansion-positions";
    public static final String OTHER_NULL_EXPANSION_POSITIONS = "nitro.projection.other-null-expansion-positions";
    public static final String DICTIONARY_DOMAIN_CACHE_HITS = "nitro.projection.dictionary-domain-cache-hits";
    public static final String DICTIONARY_DOMAIN_CACHE_MISSES = "nitro.projection.dictionary-domain-cache-misses";
    public static final String DICTIONARY_DOMAIN_CACHE_BYPASSES = "nitro.projection.dictionary-domain-cache-bypasses";
    public static final String DICTIONARY_DOMAIN_CACHE_CHANGES = "nitro.projection.dictionary-domain-cache-changes";
    public static final String DICTIONARY_DOMAIN_CACHE_OVERSIZED_BYPASSES = "nitro.projection.dictionary-domain-cache-oversized-bypasses";
    public static final String DICTIONARY_DOMAIN_CACHE_UNSTABLE_BYPASSES = "nitro.projection.dictionary-domain-cache-unstable-bypasses";
    public static final String DICTIONARY_DOMAIN_CACHE_STATIC_BYPASSES = "nitro.projection.dictionary-domain-cache-static-bypasses";

    private final Allocator.Context allocationContext = new Allocator.Context("ProjectOperator");
    private final Allocator allocator;
    private final Schema outputSchema;

    private final EvaluationPlan evaluationPlan;
    private final PrimitiveRegistry primitiveRegistry;
    private final OperatorResources operatorResources;
    private final ExecutionDiagnostics diagnostics;
    private final List<Reference> outputReferences;
    // A projection made exclusively of direct input references does not own or recompute any vectors: its outputs
    // are forwarded views of the source batch. Such a projection can safely inherit the source's retention contract.
    // Any computed producer remains mask-sensitive and must keep the conservative contract below.
    private final boolean passThroughProjection;
    // One fused monomorphic kernel producing every fusible output in a single shared loop (null if none qualifies);
    // fusedOrdinal maps a fused output's producer to its slot in the kernel's result array.
    private final CompiledMultiProjection fusedProjection;
    private final Map<Producer, Integer> fusedOrdinal = new HashMap<>();
    private final PrimitiveExecutionContext executionContext;
    private final PlanEvaluator reusablePlanEvaluator;
    private final boolean shareEvaluatorBufferPool;
    private final boolean forwardSinglePositionOnly;
    private final boolean recycleEvaluatorOutputs;

    private final Operator source;
    private BatchState currentBatchState;
    private long generatedAttempts;
    private long generatedSuccesses;
    private long generatedErrorFallbacks;
    private long generatedLayoutFallbacks;
    private long generatedSelectedPositions;
    private long flatInputPositions;
    private long dictionaryInputPositions;
    private long rleInputPositions;
    private long otherInputPositions;
    private long integerWideningPositions;
    private long dictionaryFlatteningPositions;
    private long dictionaryNullExpansionPositions;
    private long otherNullExpansionPositions;
    private boolean diagnosticsReported;

    public ProjectOperator(Allocator allocator, EvaluationPlan evaluationPlan, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        this(allocator, evaluationPlan, primitiveRegistry, source, projectedSchema(evaluationPlan, source.outputSchema()));
    }

    public ProjectOperator(Allocator allocator, EvaluationPlan evaluationPlan, PrimitiveRegistry primitiveRegistry, Operator source, Schema outputSchema)
    {
        this(allocator, evaluationPlan, primitiveRegistry, source, outputSchema, EngineResources.from(allocator).operatorResources());
    }

    public ProjectOperator(Allocator allocator, EvaluationPlan evaluationPlan, PrimitiveRegistry primitiveRegistry, Operator source, OperatorResources operatorResources)
    {
        this(allocator, evaluationPlan, primitiveRegistry, source, projectedSchema(evaluationPlan, source.outputSchema()), operatorResources);
    }

    public ProjectOperator(
            Allocator allocator,
            EvaluationPlan evaluationPlan,
            PrimitiveRegistry primitiveRegistry,
            Operator source,
            Schema outputSchema,
            OperatorResources operatorResources)
    {
        this(allocator, evaluationPlan, primitiveRegistry, source, outputSchema, operatorResources, (_, _) -> {});
    }

    public ProjectOperator(
            Allocator allocator,
            EvaluationPlan evaluationPlan,
            PrimitiveRegistry primitiveRegistry,
            Operator source,
            Schema outputSchema,
            OperatorResources operatorResources,
            ExecutionDiagnostics diagnostics)
    {
        this.allocator = allocator;
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        this.diagnostics = requireNonNull(diagnostics, "diagnostics is null");
        this.source = source;
        this.evaluationPlan = evaluationPlan;
        this.primitiveRegistry = primitiveRegistry;
        this.outputReferences = evaluationPlan.outputs();
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        ProjectOperatorPolicy policy = operatorResources.project().policy();
        this.shareEvaluatorBufferPool = policy.shareEvaluatorBufferPool();
        this.forwardSinglePositionOnly = policy.forwardSinglePositionOnly();
        this.recycleEvaluatorOutputs = policy.recycleEvaluatorOutputs();
        if (outputSchema.size() != outputReferences.size()) {
            throw new IllegalArgumentException("output schema does not match projection output count");
        }
        this.passThroughProjection = outputReferences.stream().allMatch(reference -> reference.producer() instanceof Input);
        this.executionContext = new PrimitiveExecutionContext(allocator);
        this.reusablePlanEvaluator = policy.reusePlanEvaluator() ? newPlanEvaluator(this::resolveEvaluatorInput) : null;
        CompiledMultiProjection compiled = policy.compileExpressions()
                ? operatorResources.codeGeneration().fusedProjection()
                        .tryCompile(evaluationPlan, primitiveRegistry, outputReferences)
                        .orElse(null)
                : null;
        this.fusedProjection = compiled;
        if (compiled != null) {
            for (int ordinal = 0; ordinal < compiled.outputs().size(); ordinal++) {
                fusedOrdinal.put(compiled.outputs().get(ordinal).producer(), ordinal);
            }
        }
        diagnostics.record(PLANNED_ASSIGNMENTS, evaluationPlan.assignments().size());
        diagnostics.record(PLANNED_COMPUTED_OUTPUTS, outputReferences.stream()
                .filter(reference -> !(reference.producer() instanceof Input))
                .count());
        diagnostics.record(GENERATED_OUTPUTS, compiled == null ? 0 : compiled.outputs().size());
        diagnostics.record(GENERATED_KERNELS, compiled == null ? 0 : 1);
    }

    private static Schema projectedSchema(EvaluationPlan evaluationPlan, Schema sourceSchema)
    {
        requireNonNull(evaluationPlan, "evaluationPlan is null");
        requireNonNull(sourceSchema, "sourceSchema is null");
        Schema unspecified = Schema.unspecified(evaluationPlan.outputs().size());
        List<Field> fields = new ArrayList<>(evaluationPlan.outputs().size());
        for (int outputIndex = 0; outputIndex < evaluationPlan.outputs().size(); outputIndex++) {
            Producer producer = evaluationPlan.outputs().get(outputIndex).producer();
            if (producer instanceof Input input &&
                    evaluationPlan.outputs().get(outputIndex).stream() == Stream.VALUES &&
                    input.index() >= 0 &&
                    input.index() < sourceSchema.size()) {
                fields.add(sourceSchema.field(input.index()));
            }
            else {
                fields.add(unspecified.field(outputIndex));
            }
        }
        return new Schema(fields);
    }

    @Override
    public int outputCount()
    {
        return outputReferences.size();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext();
    }

    @Override
    public Batch next()
    {
        Batch sourceBatch = source.next();
        BatchState batchState = new BatchState(sourceBatch);
        currentBatchState = batchState;

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            Reference outputReference = outputReferences.get(outputIndex);
            Field outputField = outputSchema.field(outputIndex);
            if (outputReference.producer() instanceof Input input) {
                Output selected = sourceBatch.output(input.index())
                        .select(exposedStreams(sourceBatch, outputReference));
                outputs[outputIndex] = forwardSinglePositionOnly
                        ? selected.forwardSinglePositionOnly((stream, vector) -> allocator.transfer(allocationContext, vector), (_, _) -> {})
                        : selected.forward((stream, vector) -> allocator.transfer(allocationContext, vector), (_, _) -> {});
            }
            else {
                outputs[outputIndex] = recycleEvaluatorOutputs
                        ? new Output(
                                exposedStreams(sourceBatch, outputReference),
                                stream -> evaluateOutput(batchState, outputReference, outputField, stream),
                                (stream, vector) -> allocator.transfer(allocationContext, batchState.planEvaluator().prepareResultForTransfer(vector)),
                                (stream, vector) -> batchState.releaseOutput(vector))
                        : new Output(
                                exposedStreams(sourceBatch, outputReference),
                                stream -> evaluateOutput(batchState, outputReference, outputField, stream),
                                (stream, vector) -> allocator.transfer(allocationContext, vector));
            }
            // Both computed outputs and forwarding wrappers cache their resolved vector. A later constrain must
            // re-resolve the wrapper: computed values use the new mask, while pass-through values re-borrow from the
            // correspondingly constrained source batch.
            outputs[outputIndex].withConstraintSensitiveResolution();
        }
        return new Batch(
                batchState.mask(),
                batchState::constrain,
                ignored -> sourceBatch.takeMask(),
                _ -> {},
                () -> {
                    if (currentBatchState == batchState) {
                        currentBatchState = null;
                    }
                    batchState.close();
                },
                outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    @Override
    public java.util.Optional<Map<Integer, ValueDemand>> sourceOutputDemand(Map<Integer, ValueDemand> demandedOutputs)
    {
        requireNonNull(demandedOutputs, "demandedOutputs is null");
        java.util.HashMap<Reference, ValueDemand> demandedReferences = new java.util.HashMap<>();
        for (Map.Entry<Integer, ValueDemand> entry : demandedOutputs.entrySet()) {
            int output = entry.getKey();
            if (output < 0 || output >= outputReferences.size()) {
                throw new IllegalArgumentException("demanded output is outside projection schema: " + output);
            }
            demandedReferences.merge(outputReferences.get(output), entry.getValue(), ValueDemand::merge);
        }
        return source.sourceOutputDemand(InputDependencies.valueDemands(evaluationPlan, primitiveRegistry, demandedReferences));
    }

    @Override
    public void pushDynamicFilter(org.weakref.nitro.operator.DynamicFilter filter)
    {
        // Forward a dynamic filter only through a pass-through output (a plain Input reference on the VALUES stream),
        // remapping to the underlying source column. Computed outputs aren't a direct column, so the filter can't be
        // forwarded through them.
        int outputIndex = filter.column();
        if (outputIndex >= outputReferences.size()) {
            return;
        }
        Reference reference = outputReferences.get(outputIndex);
        if (reference.stream() == Stream.VALUES && reference.producer() instanceof Input input) {
            source.pushDynamicFilter(filter.withColumn(input.index()));
        }
    }

    @Override
    public boolean supportsDynamicFilterPushdown(int column)
    {
        if (column < 0 || column >= outputReferences.size()) {
            return false;
        }
        Reference reference = outputReferences.get(column);
        return reference.stream() == Stream.VALUES &&
                reference.producer() instanceof Input input &&
                source.supportsDynamicFilterPushdown(input.index());
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        // Computed project outputs are mask-sensitive and can be recomputed after constrain(). Downstream operators
        // must not retain those batches across later constrain calls. Pure input selection/reordering forwards the
        // source vectors unchanged, so it is exactly as retainable as its source.
        return passThroughProjection && source.supportsRetainedBatches();
    }

    @Override
    public boolean supportsStableBatchBorrow()
    {
        // Direct references forward the already-open source batch and are stable until this projection batch closes,
        // even when the source must recycle them on its next advance. Computed evaluator outputs may be recycled by
        // a later output borrow and therefore remain conservative.
        return passThroughProjection;
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        return source.supportsOpenBatchHasNext();
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // A projection recomputes its outputs on demand after constrain(), so it can satisfy a
        // constrained re-borrow as long as its own source can.
        return source.supportsConstrainedReborrow();
    }

    private org.weakref.nitro.data.Vector evaluateOutput(BatchState batchState, Reference outputReference, Field outputField, Stream stream)
    {
        if (!exposedStreams(outputReference.stream()).contains(stream)) {
            throw new IllegalArgumentException("Output does not expose stream: " + stream);
        }
        if (outputReference.producer() instanceof Input input) {
            return batchState.sourceBatch().output(input.index()).borrow(stream);
        }
        Streams bundle = batchState.evaluatedOutputBundles().computeIfAbsent(outputReference.producer(), _ -> evaluateBundle(batchState, outputReference));
        if (!bundle.has(stream) && batchState.mask().none() && !batchState.schemaMask().none()) {
            bundle = batchState.schemaBundles().computeIfAbsent(outputReference.producer(), _ -> batchState.planEvaluator().evaluate(outputReference, batchState.schemaMask()));
        }
        if (!bundle.has(stream) && batchState.mask().none() && batchState.schemaMask().none()) {
            return emptyStreamVector(outputField, stream);
        }
        if (!bundle.has(stream) && (stream == Stream.NULLS || stream == Stream.ERRORS)) {
            // A fused output omits the NULLS/ERRORS stream when it is provably all-false; synthesize it at the values'
            // length on demand, exactly as the interpreter's completeRequestedStreams does.
            Vector values = bundle.getOrNull(Stream.VALUES);
            return allocator.borrowAllFalseBoolean(allocationContext, values != null ? values.length() : 0);
        }
        return bundle.get(stream);
    }

    private Vector resolveEvaluatorInput(Reference reference, Mask mask)
    {
        BatchState batchState = currentBatchState;
        if (batchState == null) {
            throw new IllegalStateException("No active project batch");
        }
        return switch (reference.producer()) {
            case org.weakref.nitro.operator.evaluator.ir.Input(int index) -> batchState.sourceBatch().output(index).borrowOrNull(reference.stream(), mask);
            default -> throw new IllegalArgumentException("Unexpected input reference: " + reference);
        };
    }

    private PlanEvaluator newPlanEvaluator(PlanEvaluator.InputResolver inputResolver)
    {
        return shareEvaluatorBufferPool
                ? new PlanEvaluator(
                        evaluationPlan,
                        primitiveRegistry,
                        inputResolver,
                        allocator,
                        operatorResources.codeGeneration().projectionMask(),
                        operatorResources.project().evaluationPolicy(),
                        operatorResources.project().evaluatorBufferPoolGroup(),
                        true)
                : new PlanEvaluator(
                        evaluationPlan,
                        primitiveRegistry,
                        inputResolver,
                        allocator,
                        operatorResources.codeGeneration().projectionMask(),
                        operatorResources.project().evaluationPolicy(),
                        new Object(),
                        true);
    }

    // Compute an output's whole stream bundle once: try the fused kernel (a single monomorphic loop over source or
    // staged inputs), falling back to the interpreter if there is no kernel or the kernel bails on an unsupported
    // runtime input layout (signalled by a null return).
    private Streams evaluateBundle(BatchState batchState, Reference outputReference)
    {
        Integer ordinal = fusedOrdinal.get(outputReference.producer());
        if (ordinal != null) {
            Streams[] results = batchState.fusedResults();
            if (results != null) {
                return results[ordinal];
            }
        }
        return batchState.planEvaluator().evaluate(outputReference, batchState.mask());
    }

    private org.weakref.nitro.data.Vector emptyStreamVector(Field outputField, Stream stream)
    {
        return switch (stream) {
            case VALUES -> {
                if (!outputField.type().isSpecified()) {
                    yield allocator.allocate(allocationContext, I64Vector.class, 0, I64Vector::new);
                }
                Vector values = outputField.type().vectorFactory()
                        .orElseThrow(() -> new IllegalArgumentException("Type does not provide empty vector construction: " + outputField.type().identity()))
                        .nullValues(allocator.vectorAllocator(allocationContext), 0);
                if (values.length() != 0) {
                    throw new IllegalArgumentException("Type vector factory returned length " + values.length() + " for empty projection output");
                }
                if (!outputField.type().supportsVector(values)) {
                    throw new IllegalArgumentException("Type " + outputField.type().identity() + " does not support empty factory result " + values.getClass().getName());
                }
                yield values;
            }
            case NULLS, ERRORS -> allocator.allocate(allocationContext, BooleanVector.class, 0, BooleanVector::new);
        };
    }

    private static Set<Stream> exposedStreams(Stream stream)
    {
        if (stream != Stream.VALUES) {
            return Set.of(stream);
        }
        return EnumSet.of(Stream.VALUES, Stream.NULLS, Stream.ERRORS);
    }

    private static Set<Stream> exposedStreams(Batch sourceBatch, Reference outputReference)
    {
        if (outputReference.producer() instanceof Input input && outputReference.stream() == Stream.VALUES) {
            return sourceBatch.output(input.index()).streams();
        }
        return exposedStreams(outputReference.stream());
    }

    @Override
    public void close()
    {
        try {
            if (currentBatchState != null) {
                currentBatchState.close();
                currentBatchState = null;
            }
            source.close();
            if (reusablePlanEvaluator != null) {
                reusablePlanEvaluator.close();
            }
            allocator.release(allocationContext);
        }
        finally {
            reportDiagnostics();
        }
    }

    private void recordInputShape(int inputIndex, Streams streams)
    {
        Vector values = streams.values();
        long positions = values.length();
        if (values instanceof DictionaryVector) {
            dictionaryInputPositions += positions;
            if (fusedProjection.flattensDictionaryValues().get(inputIndex)) {
                dictionaryFlatteningPositions += positions;
            }
        }
        else if (values instanceof RleVector) {
            rleInputPositions += positions;
        }
        else if (values instanceof I64Vector || values instanceof I32Vector || values instanceof F64Vector || values instanceof org.weakref.nitro.data.BinaryVector) {
            flatInputPositions += positions;
            if (values instanceof I32Vector &&
                    fusedProjection.inputTypes().get(inputIndex) == org.weakref.nitro.jit.FusedProjectionCompiler.InputPhysicalType.LONG) {
                integerWideningPositions += positions;
            }
        }
        else {
            otherInputPositions += positions;
        }

        Vector nulls = streams.getOrNull(Stream.NULLS);
        if (nulls instanceof DictionaryVector) {
            dictionaryNullExpansionPositions += nulls.length();
        }
        else if (!VectorAccess.isAllFalseNulls(nulls) && !(nulls instanceof BooleanVector)) {
            otherNullExpansionPositions += nulls.length();
        }
    }

    private void reportDiagnostics()
    {
        if (diagnosticsReported) {
            return;
        }
        diagnosticsReported = true;
        diagnostics.record(GENERATED_ATTEMPTS, generatedAttempts);
        diagnostics.record(GENERATED_SUCCESSES, generatedSuccesses);
        diagnostics.record(GENERATED_ERROR_FALLBACKS, generatedErrorFallbacks);
        diagnostics.record(GENERATED_LAYOUT_FALLBACKS, generatedLayoutFallbacks);
        diagnostics.record(GENERATED_SELECTED_POSITIONS, generatedSelectedPositions);
        diagnostics.record(FLAT_INPUT_POSITIONS, flatInputPositions);
        diagnostics.record(DICTIONARY_INPUT_POSITIONS, dictionaryInputPositions);
        diagnostics.record(RLE_INPUT_POSITIONS, rleInputPositions);
        diagnostics.record(OTHER_INPUT_POSITIONS, otherInputPositions);
        diagnostics.record(INTEGER_WIDENING_POSITIONS, integerWideningPositions);
        diagnostics.record(DICTIONARY_FLATTENING_POSITIONS, dictionaryFlatteningPositions);
        diagnostics.record(DICTIONARY_NULL_EXPANSION_POSITIONS, dictionaryNullExpansionPositions);
        diagnostics.record(OTHER_NULL_EXPANSION_POSITIONS, otherNullExpansionPositions);
        if (reusablePlanEvaluator != null) {
            PlanEvaluator.MaskExecutionDiagnostics evaluatorDiagnostics = reusablePlanEvaluator.maskExecutionDiagnostics();
            diagnostics.record(DICTIONARY_DOMAIN_CACHE_HITS, evaluatorDiagnostics.dictionaryDomainCacheHits());
            diagnostics.record(DICTIONARY_DOMAIN_CACHE_MISSES, evaluatorDiagnostics.dictionaryDomainCacheMisses());
            diagnostics.record(DICTIONARY_DOMAIN_CACHE_BYPASSES, evaluatorDiagnostics.dictionaryDomainCacheBypasses());
            diagnostics.record(DICTIONARY_DOMAIN_CACHE_CHANGES, evaluatorDiagnostics.dictionaryDomainCacheChanges());
            diagnostics.record(DICTIONARY_DOMAIN_CACHE_OVERSIZED_BYPASSES, evaluatorDiagnostics.dictionaryDomainCacheOversizedBypasses());
            diagnostics.record(DICTIONARY_DOMAIN_CACHE_UNSTABLE_BYPASSES, evaluatorDiagnostics.dictionaryDomainCacheUnstableBypasses());
            diagnostics.record(DICTIONARY_DOMAIN_CACHE_STATIC_BYPASSES, evaluatorDiagnostics.dictionaryDomainCacheStaticBypasses());
        }
    }

    private final class BatchState
    {
        private final Batch sourceBatch;
        private final PlanEvaluator planEvaluator;
        private final Map<Producer, Streams> evaluatedOutputBundles = new HashMap<>();
        private final Map<Producer, Streams> schemaBundles = new HashMap<>();
        private final Mask schemaSourceMask;
        private Mask schemaMask;
        private Mask mask;
        private Streams[] fusedResults;
        private boolean fusedResultsComputed;

        private BatchState(Batch sourceBatch)
        {
            this.sourceBatch = sourceBatch;
            this.mask = sourceBatch.borrowMask();
            this.schemaSourceMask = this.mask;
            this.planEvaluator = reusablePlanEvaluator != null
                    ? reusablePlanEvaluator
                    : newPlanEvaluator((reference, currentMask) -> switch (reference.producer()) {
                        case org.weakref.nitro.operator.evaluator.ir.Input(int index) -> sourceBatch.output(index).borrowOrNull(reference.stream(), currentMask);
                        default -> throw new IllegalArgumentException("Unexpected input reference: " + reference);
                    });
        }

        private Mask mask()
        {
            return mask;
        }

        // Run the fused kernel once per batch, sharing its output array across all fused outputs. Returns null (so the
        // caller uses the interpreter) when there is no kernel, the mask selects nothing (leave the empty/schema case
        // to the interpreter), or the kernel bails on an unsupported runtime input layout.
        private Streams[] fusedResults()
        {
            if (!fusedResultsComputed) {
                fusedResultsComputed = true;
                if (fusedProjection != null && !mask.none()) {
                    generatedAttempts++;
                    generatedSelectedPositions += mask.count();
                    List<Streams> inputs = new ArrayList<>(fusedProjection.inputs().size());
                    for (int inputIndex = 0; inputIndex < fusedProjection.inputs().size(); inputIndex++) {
                        Reference reference = fusedProjection.inputs().get(inputIndex);
                        Streams streams;
                        if (reference.producer() instanceof Input input) {
                            Output sourceOutput = sourceBatch.output(input.index());
                            streams = Streams.of(
                                    sourceOutput.borrow(Stream.VALUES),
                                    sourceOutput.borrowOrNull(Stream.NULLS),
                                    sourceOutput.borrowOrNull(Stream.ERRORS));
                        }
                        else {
                            Vector values = planEvaluator.evaluate(reference, mask).values();
                            Vector nulls = planEvaluator.evaluate(
                                            new Reference(reference.producer(), Stream.NULLS),
                                            mask)
                                    .getOrNull(Stream.NULLS);
                            Vector errors = planEvaluator.evaluate(
                                            new Reference(reference.producer(), Stream.ERRORS),
                                            mask)
                                    .getOrNull(Stream.ERRORS);
                            streams = Streams.of(values, nulls, errors);
                        }
                        if (!VectorAccess.isAllFalseNulls(streams.getOrNull(Stream.ERRORS))) {
                            generatedErrorFallbacks++;
                            return null;
                        }
                        Vector values = streams.values();
                        Vector nulls = streams.getOrNull(Stream.NULLS);
                        inputs.add(nulls != null ? Streams.of(values, nulls, null) : Streams.ofValues(values));
                        recordInputShape(inputIndex, streams);
                    }
                    fusedResults = fusedProjection.kernel().apply(inputs, mask, EnumSet.of(Stream.VALUES, Stream.NULLS), executionContext);
                    if (fusedResults == null) {
                        generatedLayoutFallbacks++;
                    }
                    else {
                        generatedSuccesses++;
                    }
                }
            }
            return fusedResults;
        }

        private PlanEvaluator planEvaluator()
        {
            return planEvaluator;
        }

        private Batch sourceBatch()
        {
            return sourceBatch;
        }

        private Map<Producer, Streams> evaluatedOutputBundles()
        {
            return evaluatedOutputBundles;
        }

        private Map<Producer, Streams> schemaBundles()
        {
            return schemaBundles;
        }

        private Mask schemaMask()
        {
            if (schemaMask == null) {
                schemaMask = schemaSourceMask.none()
                        ? schemaSourceMask
                        : allocator.allocateRangeMask(allocationContext, schemaSourceMask.position(0), 1);
            }
            return schemaMask;
        }

        private void constrain(Mask mask)
        {
            planEvaluator.reset();
            evaluatedOutputBundles.clear();
            schemaBundles.clear();
            fusedResults = null;
            fusedResultsComputed = false;
            this.mask = mask;
            sourceBatch.constrain(mask);
        }

        private void close()
        {
            // Batch.close() closes every Output before invoking this action, so all exposed evaluator/fused results
            // have either been released or transferred. Remaining tracked vectors are scratch/intermediates and are
            // now safe to return to their pools.
            if (recycleEvaluatorOutputs) {
                planEvaluator.resetForReuse();
                for (Allocator.Context context : executionContext.allocationContexts()) {
                    allocator.releaseIfPresent(context);
                }
                allocator.releaseIfPresent(allocationContext);
            }
            else {
                planEvaluator.reset();
            }
            evaluatedOutputBundles.clear();
            schemaBundles.clear();
            sourceBatch.close();
        }

        private void releaseOutput(Vector vector)
        {
            planEvaluator.release(vector);
            for (Allocator.Context context : executionContext.allocationContexts()) {
                allocator.release(context, vector);
            }
            allocator.release(allocationContext, vector);
        }
    }
}
