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
import org.weakref.nitro.data.Stream;

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
        Variable structField = new Variable(4);
        Variable mapValue = new Variable(5);
        Variable mapContains = new Variable(6);
        Variable arrayElement = new Variable(7);
        Reference conditionValues = new Reference(condition, Stream.VALUES);
        Reference thenValues = new Reference(new Input(0), Stream.VALUES);
        Reference elseValues = new Reference(new Input(1), Stream.VALUES);

        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(condition, new Call("lt_zero", List.of(new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                        new Assignment(result, new Merge(new ReferenceMask(conditionValues), thenValues, elseValues), AllMask.ALL),
                        new Assignment(structField, new StructField(new Reference(new Input(3), Stream.VALUES), "name"), AllMask.ALL),
                        new Assignment(mapValue, new Call("element_at_i64_utf8", List.of(new Reference(new Input(4), Stream.VALUES), new Reference(new Input(5), Stream.VALUES))), AllMask.ALL),
                        new Assignment(mapContains, new Call("map_contains_key_utf8", List.of(new Reference(new Input(6), Stream.VALUES), new Reference(new Input(7), Stream.VALUES))), AllMask.ALL),
                        new Assignment(arrayElement, new Call("array_element_i64", List.of(new Reference(new Input(8), Stream.VALUES), new Reference(new Input(9), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES), new Reference(structField, Stream.VALUES), new Reference(mapValue, Stream.VALUES), new Reference(mapContains, Stream.VALUES), new Reference(arrayElement, Stream.VALUES)));

        assertThat(plan.assignments()).hasSize(6);
        assertThat(plan.assignments().get(1).operation()).isInstanceOf(Merge.class);
        assertThat(plan.assignments().get(2).operation()).isInstanceOf(StructField.class);
        assertThat(plan.assignments().get(3).operation()).isEqualTo(new Call("element_at_i64_utf8", List.of(new Reference(new Input(4), Stream.VALUES), new Reference(new Input(5), Stream.VALUES))));
        assertThat(plan.assignments().get(4).operation()).isEqualTo(new Call("map_contains_key_utf8", List.of(new Reference(new Input(6), Stream.VALUES), new Reference(new Input(7), Stream.VALUES))));
        assertThat(plan.assignments().get(5).operation()).isEqualTo(new Call("array_element_i64", List.of(new Reference(new Input(8), Stream.VALUES), new Reference(new Input(9), Stream.VALUES))));
        assertThat(plan.outputs()).containsExactly(new Reference(result, Stream.VALUES), new Reference(structField, Stream.VALUES), new Reference(mapValue, Stream.VALUES), new Reference(mapContains, Stream.VALUES), new Reference(arrayElement, Stream.VALUES));
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
                        new Conditional(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        EvaluationPlan normalizedPlan = IrNormalizer.standard().normalizePlan(plan);

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
                        new Coalesce(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)),
                Map.of(new Reference(result, Stream.VALUES), StreamPlan.MATERIALIZED));

        EvaluationPlan normalizedPlan = IrNormalizer.standard().normalizePlan(plan);

        assertThat(normalizedPlan.streamPlans()).containsEntry(new Reference(result, Stream.VALUES), StreamPlan.MATERIALIZED);
        assertThat(NormalizedIrValidator.isNormalized(normalizedPlan)).isTrue();
        assertThat(normalizedPlan.assignments().getLast().operation()).isInstanceOf(Merge.class);
        assertThat(normalizedPlan.assignments().get(0).mask()).isInstanceOf(AndMask.class);
        assertThat(normalizedPlan.assignments().get(1).mask()).isInstanceOf(AndMask.class);
    }

    @Test
    void testNormalizerMaterializesStrictSequenceBundles()
    {
        Variable first = new Variable(0);
        Variable body = new Variable(1);
        Variable sequence = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(first, new Call("first", List.of()), AllMask.ALL),
                        new Assignment(body, new Call("body", List.of()), AllMask.ALL),
                        new Assignment(sequence, new Sequence(
                                new Reference(first, Stream.VALUES),
                                new Reference(body, Stream.VALUES)), AllMask.ALL)),
                List.of(new Reference(sequence, Stream.VALUES)));

        EvaluationPlan normalizedPlan = IrNormalizer.standard().normalizePlan(plan);

        for (Variable producer : List.of(first, body, sequence)) {
            for (Stream stream : Stream.values()) {
                assertThat(normalizedPlan.streamPlans())
                        .containsEntry(new Reference(producer, stream), StreamPlan.MATERIALIZED);
            }
        }
        assertThat(NormalizedIrValidator.isNormalized(normalizedPlan)).isTrue();
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

        EvaluationPlan normalizedPlan = IrNormalizer.standard().normalizePlan(plan);
        Assignment assignment = normalizedPlan.assignments().getFirst();

        assertThat(assignment.mask()).isInstanceOf(AndMask.class);
        assertThat(((AndMask) assignment.mask()).terms()).hasSize(3);
        assertThat(((Merge) assignment.operation()).condition()).isInstanceOf(OrMask.class);
        assertThat(((OrMask) ((Merge) assignment.operation()).condition()).terms()).hasSize(3);
        assertThat(NormalizedIrValidator.isNormalized(normalizedPlan)).isTrue();
    }

    @Test
    void testNormalizerResolvesBooleanReferenceMasksIntoMaskTrees()
    {
        Variable left = new Variable(0);
        Variable right = new Variable(1);
        Variable predicate = new Variable(2);
        Variable result = new Variable(3);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(left, new Copy(new Reference(new Input(0), Stream.VALUES)), AllMask.ALL),
                        new Assignment(right, new Copy(new Reference(new Input(1), Stream.VALUES)), AllMask.ALL),
                        new Assignment(predicate, new Call("or", List.of(
                                new Reference(left, Stream.VALUES),
                                new Reference(right, Stream.VALUES))), AllMask.ALL),
                        new Assignment(result, new Merge(
                                new ReferenceMask(new Reference(predicate, Stream.VALUES)),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)), AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)),
                Map.of(),
                Map.of(
                        new Reference(predicate, Stream.VALUES),
                        new OrMask(List.of(
                                new ReferenceMask(new Reference(left, Stream.VALUES)),
                                new ReferenceMask(new Reference(right, Stream.VALUES))))));

        EvaluationPlan normalizedPlan = IrNormalizer.standard().normalizePlan(plan);
        Merge merge = (Merge) normalizedPlan.assignments().getLast().operation();

        assertThat(merge.condition()).isInstanceOf(OrMask.class);
        assertThat(((OrMask) merge.condition()).terms()).hasSize(2);
    }

    @Test
    void testNormalizerPreservesAndResolvesMaskPlans()
    {
        Variable left = new Variable(0);
        Variable right = new Variable(1);
        Variable predicate = new Variable(2);
        Reference predicateValues = new Reference(predicate, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(left, new Copy(new Reference(new Input(0), Stream.VALUES)), AllMask.ALL),
                        new Assignment(right, new Copy(new Reference(new Input(1), Stream.VALUES)), AllMask.ALL),
                        new Assignment(predicate, new Call("or", List.of(
                                new Reference(left, Stream.VALUES),
                                new Reference(right, Stream.VALUES))), AllMask.ALL)),
                List.of(),
                Map.of(predicateValues, StreamPlan.MATERIALIZED),
                Map.of(predicateValues, new OrMask(List.of(
                        new ReferenceMask(new Reference(left, Stream.VALUES)),
                        new ReferenceMask(new Reference(right, Stream.VALUES))))));

        EvaluationPlan normalizedPlan = IrNormalizer.standard().normalizePlan(plan);

        assertThat(normalizedPlan.maskPlans()).containsKey(predicateValues);
        assertThat(normalizedPlan.maskPlans().get(predicateValues)).isInstanceOf(OrMask.class);
        assertThat(((OrMask) normalizedPlan.maskPlans().get(predicateValues)).terms()).hasSize(2);
        assertThat(normalizedPlan.streamPlans()).containsEntry(predicateValues, StreamPlan.SCRATCH);
    }

    @Test
    void testCallNameDoesNotDefineMaskStructure()
    {
        Variable predicate = new Variable(0);
        Reference predicateValues = new Reference(predicate, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        predicate,
                        new Call("or", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(predicateValues));

        assertThat(MaskExpressionResolver.resolve(plan, predicateValues))
                .isEqualTo(new ReferenceMask(predicateValues));
    }

    @Test
    void testNormalizerKeepsProjectedMaskReferenceMaterialized()
    {
        Variable predicate = new Variable(0);
        Reference predicateValues = new Reference(predicate, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        predicate,
                        new Call("identity", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(predicateValues),
                Map.of(predicateValues, StreamPlan.MATERIALIZED),
                Map.of(predicateValues, new ReferenceMask(predicateValues)));

        EvaluationPlan normalizedPlan = new IrNormalizer(List.of(new IrNormalizationRule()
        {
            @Override
            public boolean matches(Assignment candidate)
            {
                return candidate.operation() instanceof Call(String name, List<Reference> ignored, _) && name.equals("identity");
            }

            @Override
            public void apply(Assignment candidate, IrNormalizer.Context context)
            {
                Call call = (Call) candidate.operation();
                context.emit(new Assignment(candidate.output(), new Copy(call.arguments().getFirst()), candidate.mask()));
            }
        })).normalizePlan(plan);

        assertThat(normalizedPlan.streamPlans()).containsEntry(predicateValues, StreamPlan.MATERIALIZED);
    }

    @Test
    void testNormalizerDowngradesSiblingStreamPlansForMaskOnlyProducer()
    {
        Variable predicate = new Variable(0);
        Reference predicateValues = new Reference(predicate, Stream.VALUES);
        Reference predicateErrors = new Reference(predicate, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        predicate,
                        new Call("predicate", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(),
                Map.of(
                        predicateValues, StreamPlan.MATERIALIZED,
                        predicateErrors, StreamPlan.MATERIALIZED),
                Map.of(predicateValues, new ReferenceMask(predicateValues)));

        EvaluationPlan normalizedPlan = IrNormalizer.standard().normalizePlan(plan);

        assertThat(normalizedPlan.streamPlans()).containsEntry(predicateValues, StreamPlan.SCRATCH);
        assertThat(normalizedPlan.streamPlans()).containsEntry(predicateErrors, StreamPlan.SCRATCH);
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
                return candidate.operation() instanceof Call(String name, List<Reference> ignored, _) && name.equals("identity");
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
