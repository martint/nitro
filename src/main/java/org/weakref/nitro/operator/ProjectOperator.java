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
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

public class ProjectOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ProjectOperator");
    private final Allocator allocator;

    private final EvaluationPlan evaluationPlan;
    private final PlanEvaluator planEvaluator;
    private final List<Reference> outputReferences;

    private final Operator source;
    private Batch currentBatch;
    private Mask mask;

    public ProjectOperator(Allocator allocator, EvaluationPlan evaluationPlan, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        this.allocator = allocator;
        this.source = source;
        this.evaluationPlan = evaluationPlan;
        this.planEvaluator = new PlanEvaluator(evaluationPlan, primitiveRegistry, (index, currentMask) -> currentBatch.output(index).borrow(Stream.VALUES), allocator);
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
    public Batch nextBatch()
    {
        currentBatch = source.nextBatch();
        mask = currentBatch.borrowMask();
        planEvaluator.reset();

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            Reference outputReference = outputReferences.get(outputIndex);
            outputs[outputIndex] = new Output(Set.of(outputReference.stream()), stream -> evaluateOutput(outputReference, stream));
        }
        return new Batch(mask, outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        this.mask = mask;
    }

    private org.weakref.nitro.data.Vector evaluateOutput(Reference outputReference, Stream stream)
    {
        if (stream != outputReference.stream()) {
            throw new IllegalArgumentException("Output does not expose stream: " + stream);
        }
        return planEvaluator.evaluate(outputReference, mask).get(stream);
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }
}
