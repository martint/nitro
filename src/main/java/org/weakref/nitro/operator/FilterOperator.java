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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.evaluator.PlanEvaluator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Stream;

public class FilterOperator
        implements Operator
{
    private final Operator source;
    private final PlanEvaluator planEvaluator;
    private final Reference predicateReference;

    private Batch currentBatch;
    private Mask mask;

    public FilterOperator(Operator source, EvaluationPlan evaluationPlan, PrimitiveRegistry primitiveRegistry, Reference predicateReference, Allocator allocator)
    {
        this.source = source;
        this.planEvaluator = new PlanEvaluator(evaluationPlan, primitiveRegistry, (index, currentMask) -> currentBatch.output(index).borrow(Stream.VALUES), allocator);
        this.predicateReference = predicateReference;
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext();
    }

    @Override
    public Batch next()
    {
        currentBatch = source.next();
        mask = currentBatch.borrowMask();
        BooleanVector predicate = (BooleanVector) planEvaluator.evaluate(predicateReference, mask).get(predicateReference.stream());
        mask = mask.and(predicate);
        source.constrain(mask);
        planEvaluator.reset();

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            Output sourceOutput = currentBatch.output(outputIndex);
            outputs[outputIndex] = new Output(sourceOutput.streams(), sourceOutput::borrow);
        }
        return new Batch(mask, outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        this.mask = mask;
        source.constrain(mask);
    }

    @Override
    public void close()
    {
        source.close();
    }
}
