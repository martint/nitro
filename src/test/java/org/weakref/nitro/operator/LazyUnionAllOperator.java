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
import org.weakref.nitro.data.Mask;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Test-harness UNION ALL that models split scheduling by owning only one source pipeline at a time.
 */
public final class LazyUnionAllOperator
        implements Operator
{
    private final Schema outputSchema;
    private final Iterator<Supplier<Operator>> sources;
    private final List<DynamicFilter> dynamicFilters = new ArrayList<>();

    private Operator current;
    private boolean closed;

    public LazyUnionAllOperator(Schema outputSchema, List<Supplier<Operator>> sources)
    {
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        checkArgument(!sources.isEmpty(), "sources is empty");
        this.sources = List.copyOf(sources).iterator();
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
        return current != null;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more batches");
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
        dynamicFilters.add(requireNonNull(filter, "filter is null"));
        if (current != null) {
            current.pushDynamicFilter(filter);
        }
    }

    @Override
    public void close()
    {
        closed = true;
        closeCurrent();
    }

    private void advanceIfNecessary()
    {
        while (!closed && (current == null || !current.hasNext())) {
            closeCurrent();
            if (!sources.hasNext()) {
                return;
            }
            current = requireNonNull(sources.next().get(), "source factory returned null");
            checkArgument(current.outputCount() == outputSchema.size(), "Mismatched output count");
            dynamicFilters.forEach(current::pushDynamicFilter);
        }
    }

    private void closeCurrent()
    {
        if (current != null) {
            current.close();
            current = null;
        }
    }
}
