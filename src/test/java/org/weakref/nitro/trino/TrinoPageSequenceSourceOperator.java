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
package org.weakref.nitro.trino;

import io.trino.metadata.Split;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.SourceOperator;
import io.trino.operator.SourceOperatorFactory;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import static java.util.Objects.requireNonNull;

final class TrinoPageSequenceSourceOperator
        implements SourceOperator
{
    static final class Factory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final TrinoClickBenchPageReader pageReader;
        private boolean closed;

        Factory(int operatorId, PlanNodeId sourceId, TrinoClickBenchPageReader pageReader)
        {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.pageReader = requireNonNull(pageReader, "pageReader is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            if (closed) {
                throw new IllegalStateException("Factory is already closed");
            }
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, TrinoPageSequenceSourceOperator.class.getSimpleName());
            return new TrinoPageSequenceSourceOperator(operatorContext, sourceId, pageReader);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final TrinoClickBenchPageReader pageReader;
    private boolean finished;

    private TrinoPageSequenceSourceOperator(OperatorContext operatorContext, PlanNodeId sourceId, TrinoClickBenchPageReader pageReader)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.pageReader = requireNonNull(pageReader, "pageReader is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public void addSplit(Split split)
    {
        throw new UnsupportedOperationException("Split-based input is not used by the ClickBench comparison harness");
    }

    @Override
    public void noMoreSplits() {}

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException("Source operator does not accept input");
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }
        if (!pageReader.hasNext()) {
            finished = true;
            return null;
        }
        Page page = pageReader.nextPage();
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
        return page;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }
}
