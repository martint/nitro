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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Blocking full join whose outer input is scheduled incrementally by an embedding host.
 */
public final class FullJoinSession
        implements JoinSession
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("FullJoinSession", FullJoinSession.class);
    private final Schema outerSchema;
    private final List<TableOperator.Page> outerPages = new ArrayList<>();
    private final Operator inner;
    private final int[] outerJoinColumns;
    private final int[] innerJoinColumns;
    private final OperatorResources resources;
    private final boolean inputsOrderedByJoinKeys;
    private final Schema outputSchema;

    private FullJoinOperator join;
    private Batch output;
    private boolean finishing;
    private boolean closed;

    public FullJoinSession(
            OperatorResources resources,
            Allocator allocator,
            Schema outerSchema,
            int[] outerJoinColumns,
            Operator inner,
            int[] innerJoinColumns)
    {
        this(resources, allocator, outerSchema, outerJoinColumns, inner, innerJoinColumns, false);
    }

    public FullJoinSession(
            OperatorResources resources,
            Allocator allocator,
            Schema outerSchema,
            int[] outerJoinColumns,
            Operator inner,
            int[] innerJoinColumns,
            boolean inputsOrderedByJoinKeys)
    {
        this.resources = requireNonNull(resources, "resources is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.outerSchema = requireNonNull(outerSchema, "outerSchema is null");
        this.outerJoinColumns = requireNonNull(outerJoinColumns, "outerJoinColumns is null").clone();
        this.inner = requireNonNull(inner, "inner is null");
        this.innerJoinColumns = requireNonNull(innerJoinColumns, "innerJoinColumns is null").clone();
        this.inputsOrderedByJoinKeys = inputsOrderedByJoinKeys;
        List<org.weakref.nitro.core.type.Field> fields = new ArrayList<>();
        outerSchema.fields().forEach(field -> fields.add(nullable(field)));
        inner.outputSchema().fields().forEach(field -> fields.add(nullable(field)));
        outputSchema = new Schema(fields);
    }

    private static org.weakref.nitro.core.type.Field nullable(org.weakref.nitro.core.type.Field field)
    {
        return field.nullable() ? field : new org.weakref.nitro.core.type.Field(field.name(), field.type(), true);
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public void addInput(Batch batch)
    {
        checkOpen();
        if (finishing) {
            throw new IllegalStateException("full join session is finishing");
        }
        requireNonNull(batch, "batch is null");
        try (batch) {
            Mask mask = batch.borrowMask();
            if (mask.none()) {
                return;
            }
            Streams[] columns = new Streams[outerSchema.size()];
            for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                Output input = batch.output(outputIndex);
                Streams.Builder borrowed = Streams.builder();
                for (Stream stream : input.streams()) {
                    borrowed.put(stream, input.borrow(stream));
                }
                columns[outputIndex] = allocator.copyStreams(allocationContext, borrowed.build(), mask);
            }
            outerPages.add(new TableOperator.Page(mask.count(), columns, Mask.all(mask.count())));
        }
    }

    @Override
    public boolean hasOutput()
    {
        checkOpen();
        if (!finishing) {
            return false;
        }
        if (output != null) {
            return true;
        }
        if (join.hasNext()) {
            output = join.next();
            return true;
        }
        return false;
    }

    @Override
    public Batch getOutput()
    {
        checkOpen();
        if (output == null) {
            throw new IllegalStateException("no join output is ready");
        }
        Batch result = output;
        output = null;
        return result;
    }

    @Override
    public void finish()
    {
        checkOpen();
        if (finishing) {
            return;
        }
        finishing = true;
        TableOperator outer = TableOperator.retained(outerSchema, outerPages);
        join = inputsOrderedByJoinKeys
                ? FullJoinOperator.sorted(allocator, outer, outerJoinColumns, inner, innerJoinColumns, resources)
                : new FullJoinOperator(allocator, outer, outerJoinColumns, inner, innerJoinColumns, resources);
    }

    @Override
    public boolean isFinished()
    {
        checkOpen();
        return finishing && output == null && !join.hasNext();
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("full join session is closed");
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (output != null) {
            output.close();
            output = null;
        }
        if (join != null) {
            join.close();
        }
        else {
            inner.close();
        }
        allocator.release(allocationContext);
    }
}
