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
import java.util.Map;

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

    @Test
    void testNormalizerRewritesIfIntoCopiesAndMerge()
    {
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Call("if", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        EvaluationPlan normalizedPlan = IrNormalizer.normalize(plan);

        assertThat(NormalizedIrValidator.isNormalized(normalizedPlan)).isTrue();
        assertThat(normalizedPlan.assignments()).hasSize(3);
        assertThat(normalizedPlan.assignments().get(0).operation()).isInstanceOf(Copy.class);
        assertThat(normalizedPlan.assignments().get(1).operation()).isInstanceOf(Copy.class);
        assertThat(normalizedPlan.assignments().get(2).operation()).isInstanceOf(Merge.class);
        assertThat(normalizedPlan.assignments().get(0).mask()).isInstanceOf(AndMask.class);
        assertThat(normalizedPlan.assignments().get(1).mask()).isInstanceOf(AndMask.class);
    }

    @Test
    void testNormalizerRewritesCoalesce()
    {
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Call("coalesce", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)),
                Map.of(new Reference(result, Stream.VALUES), StreamPlan.MATERIALIZED));

        EvaluationPlan normalizedPlan = IrNormalizer.normalize(plan);

        assertThat(normalizedPlan.streamPlans()).containsEntry(new Reference(result, Stream.VALUES), StreamPlan.MATERIALIZED);
        assertThat(NormalizedIrValidator.isNormalized(normalizedPlan)).isTrue();
        assertThat(normalizedPlan.assignments().getLast().operation()).isInstanceOf(Merge.class);
        assertThat(normalizedPlan.assignments().get(0).mask()).isInstanceOf(AndMask.class);
        assertThat(normalizedPlan.assignments().get(1).mask()).isInstanceOf(AndMask.class);
    }

    @Test
    void testNormalizerFlattensNestedBooleanMasks()
    {
        Variable result = new Variable(0);
        MaskExpression nestedAnd = new AndMask(List.of(
                new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                new AndMask(List.of(
                        new ReferenceMask(new Reference(new Input(1), Stream.VALUES)),
                        new ReferenceMask(new Reference(new Input(2), Stream.VALUES))))));
        MaskExpression nestedOr = new OrMask(List.of(
                new ReferenceMask(new Reference(new Input(3), Stream.VALUES)),
                new OrMask(List.of(
                        new ReferenceMask(new Reference(new Input(4), Stream.VALUES)),
                        new ReferenceMask(new Reference(new Input(5), Stream.VALUES))))));

        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Merge(nestedOr, new Reference(new Input(6), Stream.VALUES), new Reference(new Input(7), Stream.VALUES)),
                        nestedAnd)),
                List.of(new Reference(result, Stream.VALUES)));

        EvaluationPlan normalizedPlan = IrNormalizer.normalize(plan);
        Assignment assignment = normalizedPlan.assignments().getFirst();

        assertThat(assignment.mask()).isInstanceOf(AndMask.class);
        assertThat(((AndMask) assignment.mask()).terms()).hasSize(3);
        assertThat(((Merge) assignment.operation()).condition()).isInstanceOf(OrMask.class);
        assertThat(((OrMask) ((Merge) assignment.operation()).condition()).terms()).hasSize(3);
        assertThat(NormalizedIrValidator.isNormalized(normalizedPlan)).isTrue();
    }

    @Test
    void testNormalizerUsesRegisteredRules()
    {
        Variable result = new Variable(0);
        Assignment assignment = new Assignment(
                result,
                new Call("identity", List.of(new Reference(new Input(0), Stream.VALUES))),
                AllMask.ALL);
        EvaluationPlan plan = new EvaluationPlan(List.of(assignment), List.of(new Reference(result, Stream.VALUES)));

        IrNormalizer normalizer = new IrNormalizer(List.of(new IrNormalizationRule()
        {
            @Override
            public boolean matches(Assignment candidate)
            {
                return candidate.operation() instanceof Call(String name, List<Reference> ignored) && name.equals("identity");
            }

            @Override
            public void apply(Assignment candidate, IrNormalizer.Context context)
            {
                Call call = (Call) candidate.operation();
                context.emit(new Assignment(candidate.output(), new Copy(call.arguments().getFirst()), candidate.mask()));
            }
        }));

        EvaluationPlan normalizedPlan = normalizer.normalizePlan(plan);

        assertThat(normalizedPlan.assignments()).singleElement()
                .extracting(Assignment::operation)
                .isInstanceOf(Copy.class);
    }
}
