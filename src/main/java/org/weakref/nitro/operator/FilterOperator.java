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
import org.weakref.nitro.operator.evaluator.Evaluator;
import org.weakref.nitro.operator.evaluator.Function;

import java.util.List;

public class FilterOperator
        implements Operator
{
    private final Operator source;
    private final Evaluator evaluator;
    private final int predicateExpression;

    private Mask mask;

    public FilterOperator(Operator source, List<Function> expressions, int predicateExpression, Allocator allocator)
    {
        this.source = source;
        this.predicateExpression = predicateExpression;
        this.evaluator = new Evaluator(expressions, (index, m) -> source.column(index), allocator);
    }

    @Override
    public int columnCount()
    {
        return source.columnCount();
    }

    @Override
    public Mask next()
    {
        mask = source.next();
        BooleanVector predicate = (BooleanVector) evaluator.evaluate(predicateExpression, mask);
        mask = mask.and(predicate);
        source.constrain(mask);
        evaluator.reset();
        return mask;
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext();
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
