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
package org.weakref.nitro.operator.pattern;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.operator.pattern.PatternAggregationSet.Scope.RUNNING;

/// Resettable, allocation-free cursor over the rows visible to one match-local aggregation invocation.
public final class PatternAggregationRows
{
    private PatternEvaluationContext context;
    private PatternAggregationSet set;
    private int relativePosition;
    private int visibleEnd;
    private boolean positioned;

    public void reset(PatternEvaluationContext context, PatternAggregationSet set)
    {
        this.context = requireNonNull(context, "context is null");
        this.set = requireNonNull(set, "set is null");
        relativePosition = -1;
        visibleEnd = set.scope() == RUNNING
                ? context.currentRow() - context.patternStart()
                : context.labels().size() - 1;
        positioned = false;
    }

    public boolean advance()
    {
        requireReset();
        do {
            relativePosition++;
        }
        while (relativePosition <= visibleEnd && !set.includes(context.labels().labelAt(relativePosition)));
        positioned = relativePosition <= visibleEnd;
        return positioned;
    }

    public int position()
    {
        requirePositioned();
        return context.patternStart() + relativePosition;
    }

    public int relativePosition()
    {
        requirePositioned();
        return relativePosition;
    }

    public int labelOrdinal()
    {
        requirePositioned();
        return context.labels().labelAt(relativePosition);
    }

    private void requireReset()
    {
        if (context == null) {
            throw new IllegalStateException("aggregation rows are not initialized");
        }
    }

    private void requirePositioned()
    {
        requireReset();
        if (!positioned) {
            throw new IllegalStateException("aggregation rows are not positioned");
        }
    }
}
