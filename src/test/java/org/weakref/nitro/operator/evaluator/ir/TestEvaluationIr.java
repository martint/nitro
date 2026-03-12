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
package org.weakref.nitro.operator.evaluator.ir;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluationIr
{
    @Test
    void testReferenceCanPointToInputOrVariableProducer()
    {
        Reference inputValues = new Reference(new Input(3), Stream.VALUES);
        Reference variableNulls = new Reference(new Variable(7), Stream.NULLS);

        assertThat(inputValues.producer()).isEqualTo(new Input(3));
        assertThat(variableNulls.producer()).isEqualTo(new Variable(7));
        assertThat(variableNulls.stream()).isEqualTo(Stream.NULLS);
    }

    @Test
    void testPlanSupportsNormalizedOperations()
    {
        Variable condition = new Variable(0);
        Variable result = new Variable(3);
        Reference conditionValues = new Reference(condition, Stream.VALUES);
        Reference thenValues = new Reference(new Input(0), Stream.VALUES);
        Reference elseValues = new Reference(new Input(1), Stream.VALUES);

        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(condition, new Call("lt_zero", List.of(new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                        new Assignment(result, new Merge(new ReferenceMask(conditionValues), thenValues, elseValues), AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        assertThat(plan.assignments()).hasSize(2);
        assertThat(plan.assignments().getLast().operation()).isInstanceOf(Merge.class);
        assertThat(plan.outputs()).containsExactly(new Reference(result, Stream.VALUES));
    }

    @Test
    void testCallArgumentsAreDefensivelyCopied()
    {
        List<Reference> arguments = List.of(new Reference(new Input(0), Stream.VALUES));
        Call call = new Call("identity", arguments);

        assertThat(call.arguments()).containsExactlyElementsOf(arguments);
    }
}
