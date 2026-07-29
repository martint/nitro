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
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Long-lived grouped aggregation state for batches scheduled by an external host.
 *
 * <p>Inputs are consumed eagerly and remain owned by the caller. Grouping tables, aggregate
 * states, generated kernels, and reusable buffers live until this session is closed.
 */
public final class GroupedAggregationSession
        implements BatchAggregationSession
{
    private final GroupedAggregationOperator aggregation;
    private boolean finished;
    private boolean closed;

    public GroupedAggregationSession(
            Allocator allocator,
            Schema inputSchema,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            OperatorResources operatorResources)
    {
        requireNonNull(inputSchema, "inputSchema is null");
        aggregation = new GroupedAggregationOperator(
                requireNonNull(allocator, "allocator is null"),
                List.copyOf(requireNonNull(groupByColumns, "groupByColumns is null")),
                List.copyOf(requireNonNull(groupedColumns, "groupedColumns is null")),
                requireNonNull(program, "program is null"),
                new SchemaSource(inputSchema),
                requireNonNull(operatorResources, "operatorResources is null"));
    }

    public Schema outputSchema()
    {
        return aggregation.outputSchema();
    }

    public void addInput(Batch batch)
    {
        checkAcceptingInput();
        aggregation.addInput(requireNonNull(batch, "batch is null"));
    }

    public Batch finish()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("grouped aggregation session is already finished");
        }
        finished = true;
        return aggregation.finishInput();
    }

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("grouped aggregation session is finished");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("grouped aggregation session is closed");
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        aggregation.close();
    }

    private static final class SchemaSource
            implements Operator
    {
        private final Schema schema;

        private SchemaSource(Schema schema)
        {
            this.schema = requireNonNull(schema, "schema is null");
        }

        @Override
        public int outputCount()
        {
            return schema.size();
        }

        @Override
        public Schema outputSchema()
        {
            return schema;
        }

        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public Batch next()
        {
            throw new IllegalStateException("No more rows");
        }

        @Override
        public void constrain(Mask mask)
        {
            throw new IllegalStateException("No pending batch");
        }

        @Override
        public void close() {}
    }
}
