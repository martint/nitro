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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.evaluator.PlanEvaluator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Producer;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProjectOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ProjectOperator");
    private final Allocator allocator;

    private final EvaluationPlan evaluationPlan;
    private final PrimitiveRegistry primitiveRegistry;
    private final List<Reference> outputReferences;

    private final Operator source;
    private BatchState currentBatchState;

    public ProjectOperator(Allocator allocator, EvaluationPlan evaluationPlan, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        this.allocator = allocator;
        this.source = source;
        this.evaluationPlan = evaluationPlan;
        this.primitiveRegistry = primitiveRegistry;
        this.outputReferences = evaluationPlan.outputs();
    }

    @Override
    public int outputCount()
    {
        return outputReferences.size();
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
            outputs[outputIndex] = new Output(
                    exposedStreams(outputReference.stream()),
                    stream -> evaluateOutput(batchState, outputReference, stream),
                    (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
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
    public boolean supportsRetainedBatches()
    {
        return source.supportsRetainedBatches();
    }

    private org.weakref.nitro.data.Vector evaluateOutput(BatchState batchState, Reference outputReference, Stream stream)
    {
        if (!exposedStreams(outputReference.stream()).contains(stream)) {
            throw new IllegalArgumentException("Output does not expose stream: " + stream);
        }
        Streams bundle = batchState.evaluatedOutputBundles().computeIfAbsent(outputReference.producer(), _ -> batchState.planEvaluator().evaluate(outputReference, batchState.mask()));
        if (!bundle.has(stream) && batchState.mask().none() && !batchState.schemaMask().none()) {
            bundle = batchState.schemaBundles().computeIfAbsent(outputReference.producer(), _ -> batchState.planEvaluator().evaluate(outputReference, batchState.schemaMask()));
        }
        return bundle.get(stream);
    }

    private static Set<Stream> exposedStreams(Stream stream)
    {
        if (stream != Stream.VALUES) {
            return Set.of(stream);
        }
        return EnumSet.of(Stream.VALUES, Stream.NULLS, Stream.ERRORS);
    }

    @Override
    public void close()
    {
        if (currentBatchState != null) {
            currentBatchState.close();
            currentBatchState = null;
        }
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private final class BatchState
    {
        private final Batch sourceBatch;
        private final PlanEvaluator planEvaluator;
        private final Map<Producer, Streams> evaluatedOutputBundles = new HashMap<>();
        private final Map<Producer, Streams> schemaBundles = new HashMap<>();
        private final Mask schemaMask;
        private Mask mask;

        private BatchState(Batch sourceBatch)
        {
            this.sourceBatch = sourceBatch;
            this.mask = sourceBatch.borrowMask();
            this.schemaMask = this.mask.size() == 0 ? this.mask : allocator.allocateRangeMask(ALLOCATION_CONTEXT, 0, 1);
            this.planEvaluator = new PlanEvaluator(
                    evaluationPlan,
                    primitiveRegistry,
                    (reference, currentMask) -> switch (reference.producer()) {
                        case org.weakref.nitro.operator.evaluator.ir.Input(int index) -> sourceBatch.output(index).borrowOrNull(reference.stream());
                        default -> throw new IllegalArgumentException("Unexpected input reference: " + reference);
                    },
                    allocator);
        }

        private Mask mask()
        {
            return mask;
        }

        private PlanEvaluator planEvaluator()
        {
            return planEvaluator;
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
            return schemaMask;
        }

        private void constrain(Mask mask)
        {
            planEvaluator.reset();
            evaluatedOutputBundles.clear();
            schemaBundles.clear();
            this.mask = mask;
            sourceBatch.constrain(mask);
        }

        private void close()
        {
            planEvaluator.reset();
            evaluatedOutputBundles.clear();
            schemaBundles.clear();
            sourceBatch.close();
        }
    }
}
