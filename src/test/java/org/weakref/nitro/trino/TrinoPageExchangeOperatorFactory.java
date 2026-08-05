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

import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Arrays;

/** Test-harness model of a single-destination Page exchange boundary. */
final class TrinoPageExchangeOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final String name;
    private final PlanNodeId planNodeId;
    private boolean closed;

    TrinoPageExchangeOperatorFactory(int operatorId, String name)
    {
        this.operatorId = operatorId;
        this.name = name;
        this.planNodeId = new PlanNodeId(name);
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        if (closed) {
            throw new IllegalStateException("Factory is already closed");
        }
        return new PageExchangeOperator(driverContext.addOperatorContext(
                operatorId,
                planNodeId,
                PageExchangeOperator.class.getSimpleName()));
    }

    @Override
    public void noMoreOperators()
    {
        closed = true;
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new TrinoPageExchangeOperatorFactory(operatorId, name);
    }

    private static final class PageExchangeOperator
            implements Operator
    {
        private final OperatorContext operatorContext;
        private Page output;
        private boolean finishing;

        private PageExchangeOperator(OperatorContext operatorContext)
        {
            this.operatorContext = operatorContext;
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public boolean needsInput()
        {
            return !finishing && output == null;
        }

        @Override
        public void addInput(Page page)
        {
            if (!needsInput()) {
                throw new IllegalStateException("Operator does not need input");
            }
            int[] positions = new int[page.getPositionCount()];
            Arrays.setAll(positions, index -> index);
            output = page.copyPositions(positions, 0, positions.length);
        }

        @Override
        public Page getOutput()
        {
            Page result = output;
            output = null;
            return result;
        }

        @Override
        public void finish()
        {
            finishing = true;
        }

        @Override
        public boolean isFinished()
        {
            return finishing && output == null;
        }
    }
}
