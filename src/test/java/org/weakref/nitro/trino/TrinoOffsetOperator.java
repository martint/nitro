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

import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class TrinoOffsetOperator
        implements Operator
{
    static final class Factory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final long offset;
        private boolean closed;

        Factory(int operatorId, PlanNodeId planNodeId, long offset)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            checkArgument(offset >= 0, "offset must be at least zero");
            this.offset = offset;
        }

        @Override
        public Operator createOperator(io.trino.operator.DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            return new TrinoOffsetOperator(driverContext.addOperatorContext(operatorId, planNodeId, TrinoOffsetOperator.class.getSimpleName()), offset);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new Factory(operatorId, planNodeId, offset);
        }
    }

    private final OperatorContext operatorContext;
    private long remainingOffset;
    private Page nextPage;
    private boolean finishing;

    private TrinoOffsetOperator(OperatorContext operatorContext, long offset)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.remainingOffset = offset;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && nextPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && nextPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput(), "Operator does not need input");
        requireNonNull(page, "page is null");

        if (remainingOffset >= page.getPositionCount()) {
            remainingOffset -= page.getPositionCount();
            return;
        }

        if (remainingOffset > 0) {
            page = page.getRegion((int) remainingOffset, page.getPositionCount() - (int) remainingOffset);
            remainingOffset = 0;
        }

        nextPage = page;
    }

    @Override
    public Page getOutput()
    {
        Page page = nextPage;
        nextPage = null;
        return page;
    }
}
