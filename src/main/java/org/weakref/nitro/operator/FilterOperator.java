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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.PlanEvaluator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Reference;

public class FilterOperator
        implements Operator, BatchOperator
{
    private final Operator source;
    private final PlanEvaluator planEvaluator;
    private final Reference predicateReference;

    private Mask mask;

    public FilterOperator(Operator source, EvaluationPlan evaluationPlan, PrimitiveRegistry primitiveRegistry, Reference predicateReference, Allocator allocator)
    {
        this.source = source;
        this.planEvaluator = new PlanEvaluator(evaluationPlan, primitiveRegistry, (index, currentMask) -> source.column(index), allocator);
        this.predicateReference = predicateReference;
    }

    @Override
    public int columnCount()
    {
        return source.columnCount();
    }

    @Override
    public int outputCount()
    {
        return columnCount();
    }

    @Override
    public Mask next()
    {
        mask = source.next();
        BooleanVector predicate = (BooleanVector) planEvaluator.evaluate(predicateReference, mask).get(predicateReference.stream());
        mask = mask.and(predicate);
        source.constrain(mask);
        planEvaluator.reset();
        return mask;
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext();
    }

    @Override
    public Batch nextBatch()
    {
        Mask batchMask = next();
        Output[] outputs = new Output[columnCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int column = outputIndex;
            outputs[outputIndex] = Output.lazyValues(() -> column(column));
        }
        return new Batch(batchMask, outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        this.mask = mask;
        source.constrain(mask);
    }

    @Override
    public Vector column(int column)
    {
        return source.column(column);
    }

    @Override
    public void close()
    {
        source.close();
    }
}
