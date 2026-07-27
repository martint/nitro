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

import org.weakref.nitro.core.type.Schema;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class UnionAllOperator
        implements Operator
{
    private final Schema outputSchema;
    private final List<Operator> sources;

    private int sourceIndex;

    public UnionAllOperator(int outputCount, List<Operator> sources)
    {
        this(Schema.unspecified(outputCount), sources);
    }

    public UnionAllOperator(Schema outputSchema, List<Operator> sources)
    {
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.sources = new ArrayList<>(sources);
        checkArgument(!sources.isEmpty(), "sources is empty");
        for (Operator source : sources) {
            checkArgument(source.outputCount() == outputSchema.size(), "Mismatched output count");
        }
    }

    @Override
    public int outputCount()
    {
        return outputSchema.size();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public boolean hasNext()
    {
        advanceIfNecessary();
        return sourceIndex < sources.size();
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more batches");
        }
        return current().next();
    }

    @Override
    public void constrain(org.weakref.nitro.data.Mask mask)
    {
        if (sourceIndex < sources.size()) {
            current().constrain(mask);
        }
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        // UNION ALL preserves the output schema, so the same column filter applies independently to every remaining
        // branch. This is normally called before consumption, but starting at sourceIndex also handles a late filter
        // without touching sources that have already been closed.
        for (int index = sourceIndex; index < sources.size(); index++) {
            sources.get(index).pushDynamicFilter(filter);
        }
    }

    @Override
    public void close()
    {
        for (Operator source : sources) {
            source.close();
        }
    }

    private void advanceIfNecessary()
    {
        while (sourceIndex < sources.size() && !current().hasNext()) {
            current().close();
            sourceIndex++;
        }
    }

    private Operator current()
    {
        return sources.get(sourceIndex);
    }
}
