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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestOperatorResources
{
    @Test
    void testOwnersAreIsolatedAndExplicitlyClosed()
    {
        OperatorResources first = OperatorResources.createDefault();
        OperatorResources second = OperatorResources.createDefault();

        assertThat(first.codeGeneration()).isNotSameAs(second.codeGeneration());
        assertThat(first.project()).isNotSameAs(second.project());
        assertThat(first.aggregation()).isNotSameAs(second.aggregation());
        assertThat(first.hashJoin()).isNotSameAs(second.hashJoin());
        assertThat(first.grouping()).isNotSameAs(second.grouping());

        first.close();
        assertThatIllegalStateException()
                .isThrownBy(first::codeGeneration)
                .withMessage("Operator resources are closed");
        assertThat(second.codeGeneration()).isNotNull();
        second.close();
    }

    @Test
    void testProjectConstructionDoesNotDiscoverServicesThroughAllocator()
    {
        try (AllocationResources allocationResources = AllocationResources.createDefault();
                OperatorResources operatorResources = OperatorResources.createDefault();
                Allocator allocator = new Allocator(allocationResources);
                Operator source = new ConstantTableOperator(allocator, 0, List.of());
                Operator project = new ProjectOperator(
                        allocator,
                        new EvaluationPlan(List.of(), List.of()),
                        new PrimitiveRegistry(),
                        source,
                        operatorResources)) {
            assertThat(project.outputCount()).isZero();
        }
    }
}
