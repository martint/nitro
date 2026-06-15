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

import org.weakref.nitro.data.Mask;

import java.util.List;
import java.util.function.Function;

public final class MultiStageOperator
        implements Operator
{
    private final int outputCount;
    private final List<?> stages;
    private final Function<Object, Operator> operatorFactory;

    private int stageIndex;
    private Operator current;
    // A pushed dynamic filter is forwarded to the current stage and to each subsequent stage as it is created,
    // so a multi-file scan applies the filter on every file.
    private DynamicFilter dynamicFilter;

    @SuppressWarnings("unchecked")
    public <T> MultiStageOperator(int outputCount, List<T> stages, Function<T, Operator> operatorFactory)
    {
        this.outputCount = outputCount;
        this.stages = List.copyOf(stages);
        this.operatorFactory = stage -> operatorFactory.apply((T) stage);
    }

    @Override
    public int outputCount()
    {
        return outputCount;
    }

    @Override
    public boolean hasNext()
    {
        advanceIfNecessary();
        return current != null && current.hasNext();
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        return current.next();
    }

    @Override
    public void constrain(Mask mask)
    {
        if (current != null) {
            current.constrain(mask);
        }
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        this.dynamicFilter = filter;
        if (current != null) {
            current.pushDynamicFilter(filter);
        }
    }

    @Override
    public void close()
    {
        if (current != null) {
            current.close();
            current = null;
        }
    }

    private void advanceIfNecessary()
    {
        while ((current == null || !current.hasNext()) && stageIndex < stages.size()) {
            if (current != null) {
                current.close();
            }
            current = operatorFactory.apply(stages.get(stageIndex++));
            if (dynamicFilter != null) {
                current.pushDynamicFilter(dynamicFilter);
            }
        }
    }
}
