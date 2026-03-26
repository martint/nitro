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

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class UnionAllOperator
        implements Operator
{
    private final int outputCount;
    private final List<Operator> sources;

    private int sourceIndex;

    public UnionAllOperator(int outputCount, List<Operator> sources)
    {
        this.outputCount = outputCount;
        this.sources = new ArrayList<>(sources);
        checkArgument(!sources.isEmpty(), "sources is empty");
        for (Operator source : sources) {
            checkArgument(source.outputCount() == outputCount, "Mismatched output count");
        }
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
