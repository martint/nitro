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
package org.weakref.nitro.operator.evaluator;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.function.scalar.ScalarRegistry;
import org.weakref.nitro.function.scalar.builtin.AddI64;
import org.weakref.nitro.function.scalar.builtin.DivideScaleRoundI64;
import org.weakref.nitro.function.scalar.builtin.EqualI64;
import org.weakref.nitro.function.scalar.builtin.InUtf8;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.function.scalar.builtin.ScaledRelativeDifferenceGtI64;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.AndMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.IrNormalizer;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.MaterializationPolicy;
import org.weakref.nitro.operator.evaluator.ir.MemoizationPolicy;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.OrMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.StreamPlan;
import org.weakref.nitro.operator.evaluator.ir.StructField;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry;

public class TestPlanEvaluator
{
    @Test
    void testEvaluatesSimpleAddPlan()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable sum = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(sum, new Call("add", List.of(
                        new Reference(new Input(0), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES),
                        new Reference(new Input(1), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(sum, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)),
                Map.of(new Reference(sum, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES), new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {10, 20, 30}))), new Allocator());

        I64Vector result = (I64Vector) evaluator.evaluate(new Reference(sum, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES), Mask.all(3)).get(Stream.VALUES);
        assertThat(result.values()).containsExactly(11L, 22L, 33L);
    }

    @Test
    void testTwoIndependentAddColumnsDoNotAliasBuffersAcrossReset()
    {
        // Two independent `add` results computed by the same primitive (AddI64) share AddI64's static
        // allocation context and therefore its vector pool. ProjectOperator reads outputs lazily: it
        // materializes one column, then re-evaluates the plan after a constrain()/reset() before
        // reading the other. If the still-referenced buffer of the first result is returned to the
        // pool by reset() and re-borrowed for the second result, both columns alias and report the
        // same values. This reproduces that lifecycle without TPC-DS data.
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable first = new Variable(0);
        Variable second = new Variable(1);
        Reference firstValues = new Reference(first, Stream.VALUES);
        Reference secondValues = new Reference(second, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(first, new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                        new Assignment(second, new Call("add", List.of(
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES))), AllMask.ALL)),
                List.of(firstValues, secondValues),
                Map.of(
                        firstValues, new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE),
                        secondValues, new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3, 4}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {10, 20, 30, 40}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {100, 200, 300, 400}),
                new Reference(new Input(3), Stream.VALUES), new I64Vector(new long[] {1000, 2000, 3000, 4000}))), new Allocator());

        // Materialize the first column and hold its result, as a downstream consumer would after the
        // ProjectOperator hands out the column's vector.
        I64Vector firstResult = (I64Vector) evaluator.evaluate(firstValues, Mask.all(4)).get(Stream.VALUES);
        assertThat(firstResult.values()).containsExactly(11L, 22L, 33L, 44L);

        // A constrain()/reset() cycle, as ProjectOperator performs when a downstream operator pushes
        // a narrower mask. reset() returns the AddI64 buffers to the pool, including firstResult's
        // buffer, even though the downstream consumer still holds firstResult.
        evaluator.reset();

        // The second column is re-evaluated after the reset. Its add re-borrows from the AddI64 pool;
        // if it re-borrows firstResult's buffer it overwrites the value the consumer still holds.
        I64Vector secondResult = (I64Vector) evaluator.evaluate(secondValues, Mask.all(4)).get(Stream.VALUES);
        assertThat(secondResult.values()).containsExactly(1100L, 2200L, 3300L, 4400L);

        // firstResult must still hold its own values, not the second column's.
        assertThat(firstResult.values()).containsExactly(11L, 22L, 33L, 44L);
    }

    @Test
    void testEvaluatesNullI64Function()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable nullValue = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(nullValue, new Call("null_i64", List.of()), AllMask.ALL)),
                List.of(new Reference(nullValue, Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator());

        Streams result = evaluator.evaluate(new Reference(nullValue, Stream.VALUES), Mask.all(3));
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(true, true, true);
        assertThat(((I64Vector) result.get(Stream.VALUES)).values()).containsExactly(0L, 0L, 0L);
    }

    @Test
    void testEvaluatesNormalizedMerge()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(new Input(0), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)),
                                new Reference(new Input(1), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES),
                                new Reference(new Input(2), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true, false}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator());

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(1L, 2L, 1L, 2L);
    }

    @Test
    void testReferenceMaskTreatsMissingOptionalInputStreamsAsAbsent()
    {
        EvaluationPlan plan = new EvaluationPlan(
                List.of(),
                List.of(new Reference(new Input(0), Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true}))),
                new Allocator());

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(new Input(0), Stream.VALUES)), Mask.all(3));
        assertThat(result.count()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(2);
    }

    @Test
    void testReferenceMaskClassifiesErrorsThenNullsThenTrueValues()
    {
        PlanEvaluator evaluator = new PlanEvaluator(
                new EvaluationPlan(List.of(), List.of()),
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, false, true}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, false, false}),
                        new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {false, false, true, false, false}))),
                new Allocator());

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(new Input(0), Stream.VALUES)), Mask.all(5));
        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(4);
    }

    @Test
    void testReferenceMaskOptimizesSimpleLongComparisonsViaPrimitiveMaskEvaluation()
    {
        Variable sixtyTwo = new Variable(0);
        Variable refreshZero = new Variable(1);
        Variable julyStart = new Variable(2);
        Variable augustStart = new Variable(3);
        Variable counterEquals = new Variable(4);
        Variable refreshEquals = new Variable(5);
        Variable afterStart = new Variable(6);
        Variable beforeEnd = new Variable(7);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(sixtyTwo, new Literal(62L), AllMask.ALL),
                        new Assignment(refreshZero, new Literal(0L), AllMask.ALL),
                        new Assignment(julyStart, new Literal(1_372_636_800L), AllMask.ALL),
                        new Assignment(augustStart, new Literal(1_375_315_200L), AllMask.ALL),
                        new Assignment(counterEquals, new Call("eq", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(sixtyTwo, Stream.VALUES))), AllMask.ALL),
                        new Assignment(refreshEquals, new Call("eq", List.of(
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(refreshZero, Stream.VALUES))), AllMask.ALL),
                        new Assignment(afterStart, new Call("lt", List.of(
                                new Reference(julyStart, Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                        new Assignment(beforeEnd, new Call("lt", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(augustStart, Stream.VALUES))), AllMask.ALL)),
                List.of(),
                Map.of(),
                Map.of());

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                builtinPrimitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {62, 62, 63, 62}),
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {1_372_636_800L, 1_373_000_000L, 1_373_000_000L, 1_375_315_200L}),
                        new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {0, 0, 0, 1}),
                        new Reference(new Input(2), Stream.NULLS), new BooleanVector(new boolean[] {false, false, false, true}))),
                new Allocator());

        Mask result = evaluator.evaluate(
                new AndMask(List.of(
                        new ReferenceMask(new Reference(counterEquals, Stream.VALUES)),
                        new ReferenceMask(new Reference(refreshEquals, Stream.VALUES)),
                        new ReferenceMask(new Reference(afterStart, Stream.VALUES)),
                        new ReferenceMask(new Reference(beforeEnd, Stream.VALUES)))),
                Mask.all(4));

        assertThat(result.selectedCount()).isEqualTo(1);
        assertThat(result.position(0)).isEqualTo(1);
    }

    @Test
    void testUtf8LiteralUsesRleAndContainsSupportsIt()
    {
        Variable needle = new Variable(0);
        Variable contains = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(needle, new Literal("go"), AllMask.ALL),
                        new Assignment(
                                contains,
                                new Call("contains_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(needle, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(
                        new Reference(needle, Stream.VALUES),
                        new Reference(contains, Stream.VALUES)),
                Map.of(
                        new Reference(needle, Stream.VALUES), new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE),
                        new Reference(contains, Stream.VALUES), new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE)));

        BinaryVector input = new BinaryVector(3, 16);
        input.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        input.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        input.setBytes(0, "google".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        input.setBytes(1, "bing".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        input.setBytes(2, "golang".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), input)),
                new Allocator());

        Streams literal = evaluator.evaluate(new Reference(needle, Stream.VALUES), Mask.all(3));
        assertThat(literal.values()).isInstanceOf(RleVector.class);

        Streams result = evaluator.evaluate(new Reference(contains, Stream.VALUES), Mask.all(3));
        assertThat(((BooleanVector) result.get(Stream.VALUES)).values()).containsExactly(true, false, true);
    }

    @Test
    void testProjectedInputValuesCanStillLoadCompanionInputStreams()
    {
        Variable length = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        length,
                        new Call("length_utf8", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(length, Stream.VALUES),
                        new Reference(new Input(0), Stream.VALUES)));

        BinaryVector inputValues = new BinaryVector(2, 16);
        inputValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        inputValues.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        inputValues.setNull(1);
        BooleanVector inputNulls = new BooleanVector(new boolean[] {false, true});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), inputValues,
                        new Reference(new Input(0), Stream.NULLS), inputNulls)),
                new Allocator());

        Streams lengths = evaluator.evaluate(new Reference(length, Stream.VALUES), Mask.all(2));
        assertThat(((I64Vector) lengths.get(Stream.VALUES)).values()).containsExactly(5L, 0L);
        assertThat(((BooleanVector) lengths.get(Stream.NULLS)).values()).containsExactly(false, true);

        Streams projectedInput = evaluator.evaluate(new Reference(new Input(0), Stream.VALUES), Mask.all(2));
        assertThat(utf8((BinaryVector) projectedInput.get(Stream.VALUES), 0)).isEqualTo("alpha");
        assertThat(((BooleanVector) projectedInput.get(Stream.NULLS)).values()).containsExactly(false, true);
    }

    @Test
    void testMultiplyPreservesNullsForProjectedValues()
    {
        Variable product = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        product,
                        new Call("multiply", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(product, Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {7, 11}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true}),
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {3, 5}))),
                new Allocator());

        Streams productStreams = evaluator.evaluate(new Reference(product, Stream.VALUES), Mask.all(2));
        assertThat(((I64Vector) productStreams.get(Stream.VALUES)).values()[0]).isEqualTo(21L);
        assertThat(((BooleanVector) productStreams.get(Stream.NULLS)).values()).containsExactly(false, true);
    }

    @Test
    void testContainsUtf8HandlesVectorCandidatesAcrossBoundaries()
    {
        Variable needle = new Variable(0);
        Variable contains = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(needle, new Literal("aaab"), AllMask.ALL),
                        new Assignment(
                                contains,
                                new Call("contains_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(needle, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(contains, Stream.VALUES)),
                Map.of(new Reference(contains, Stream.VALUES), new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE)));

        BinaryVector input = new BinaryVector(4, 512);
        input.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        input.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        input.setBytes(0, ("x".repeat(31) + "aaab" + "tail").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        input.setBytes(1, ("x".repeat(63) + "aaab").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        input.setBytes(2, ("a".repeat(96) + "b").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        input.setBytes(3, ("a".repeat(95) + "c").getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), input)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(contains, Stream.VALUES), Mask.all(4));
        assertThat(((BooleanVector) result.get(Stream.VALUES)).values()).containsExactly(true, true, true, false);
    }

    @Test
    void testExtractHostUtf8PreservesDictionaryEncoding()
    {
        Variable host = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        host,
                        new Call("extract_host_utf8", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(host, Stream.VALUES),
                        new Reference(host, Stream.NULLS)));

        BinaryVector dictionaryValues = new BinaryVector(3, 128);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionaryValues.setBytes(0, "https://www.google.com/search".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(1, "http://news.ycombinator.com/item".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(2, "https://www.google.com/maps".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector input = DictionaryVector.wrap(new int[] {0, 1, 2, 0}, dictionaryValues);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, false});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), input,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(host, Stream.VALUES), Mask.all(4));
        assertThat(result.values()).isInstanceOf(DictionaryVector.class);

        DictionaryVector hosts = (DictionaryVector) result.values();
        assertThat(hosts.ids()).containsExactly(0, 1, 2, 0);
        BinaryVector extractedValues = (BinaryVector) hosts.values();
        assertThat(utf8(extractedValues, 0)).isEqualTo("google.com");
        assertThat(utf8(extractedValues, 1)).isEqualTo("news.ycombinator.com");
        assertThat(utf8(extractedValues, 2)).isEqualTo("google.com");
        assertThat(extractedValues.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
        assertThat(extractedValues.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY)).isTrue();
    }

    @Test
    void testRegexpReplaceUtf8SparseMaskKeepsOffsetsAligned()
    {
        // A sparse mask must not shear the output: the BinaryVector offsets are a cumulative chain over ALL
        // positions, so skipped positions need their offsets filled forward (regression: masked regexp output
        // read back rotated bytes of neighboring values).
        Variable pattern = new Variable(0);
        Variable replacement = new Variable(1);
        Variable host = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(pattern, new Literal("^https?://(?:www\\.)?([^/]+)/.*$"), AllMask.ALL),
                        new Assignment(replacement, new Literal("\\1"), AllMask.ALL),
                        new Assignment(
                                host,
                                new Call("regexp_replace_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(pattern, Stream.VALUES),
                                        new Reference(replacement, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(host, Stream.VALUES)));

        BinaryVector values = new BinaryVector(5, 200);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        values.setBytes(0, "https://www.google.com/search".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(1, "http://news.ycombinator.com/item".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(2, "https://example.com/page".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(3, "http://tambov.irr.ru/0/c1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(4, "https://www.wildberries.ru/catalog".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator());

        Mask mask = Mask.sparse(new int[] {1, 3, 4}, 5);
        Streams result = evaluator.evaluate(new Reference(host, Stream.VALUES), mask);
        BinaryVector rewritten = (BinaryVector) result.values();
        assertThat(utf8(rewritten, 1)).isEqualTo("news.ycombinator.com");
        assertThat(utf8(rewritten, 3)).isEqualTo("tambov.irr.ru");
        assertThat(utf8(rewritten, 4)).isEqualTo("wildberries.ru");
    }

    @Test
    void testIfUtf8SparseMaskKeepsOffsetsAligned()
    {
        // Same sparse-mask offsets-chain regression as regexp_replace: skipped positions must be filled forward.
        Variable selected = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(selected, new Call("if_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(selected, Stream.VALUES)));

        BinaryVector trueValues = new BinaryVector(4, 64);
        trueValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        trueValues.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        trueValues.setBytes(1, "bravo".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        trueValues.setBytes(2, "charlie".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        trueValues.setBytes(3, "delta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        BinaryVector falseValues = new BinaryVector(4, 64);
        falseValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        falseValues.setBytes(0, "w".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        falseValues.setBytes(1, "x".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        falseValues.setBytes(2, "y".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        falseValues.setBytes(3, "z".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        org.weakref.nitro.data.BooleanVector conditions = new org.weakref.nitro.data.BooleanVector(new boolean[] {true, false, true, false});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), conditions,
                        new Reference(new Input(1), Stream.VALUES), trueValues,
                        new Reference(new Input(2), Stream.VALUES), falseValues)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(selected, Stream.VALUES), Mask.sparse(new int[] {1, 3}, 4));
        BinaryVector values = (BinaryVector) result.values();
        assertThat(utf8(values, 1)).isEqualTo("x");
        assertThat(utf8(values, 3)).isEqualTo("z");
    }

    @Test
    void testExtractHostUtf8SparseMaskKeepsOffsetsAligned()
    {
        Variable host = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(host, new Call("extract_host_utf8", List.of(new Reference(new Input(0), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(host, Stream.VALUES)));

        BinaryVector values = new BinaryVector(4, 160);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        values.setBytes(0, "https://www.google.com/search".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(1, "http://news.ycombinator.com/item".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(2, "https://example.com/page".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(3, "http://tambov.irr.ru/0/c1".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(host, Stream.VALUES), Mask.sparse(new int[] {1, 3}, 4));
        BinaryVector hosts = (BinaryVector) result.values();
        assertThat(utf8(hosts, 1)).isEqualTo("news.ycombinator.com");
        assertThat(utf8(hosts, 3)).isEqualTo("tambov.irr.ru");
    }

    @Test
    void testRegexpReplaceUtf8SupportsCaptureGroupReplacement()
    {
        Variable pattern = new Variable(0);
        Variable replacement = new Variable(1);
        Variable host = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(pattern, new Literal("^https?://(?:www\\.)?([^/]+)/.*$"), AllMask.ALL),
                        new Assignment(replacement, new Literal("\\1"), AllMask.ALL),
                        new Assignment(
                                host,
                                new Call("regexp_replace_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(pattern, Stream.VALUES),
                                        new Reference(replacement, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(host, Stream.VALUES)));

        BinaryVector dictionaryValues = new BinaryVector(4, 160);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionaryValues.setBytes(0, "https://www.google.com/search".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(1, "http://news.ycombinator.com/item".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(2, "https://example.com".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(3, "mailto:test@example.com".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector input = DictionaryVector.wrap(new int[] {0, 1, 2, 3}, dictionaryValues);
        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), input)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(host, Stream.VALUES), Mask.all(4));
        assertThat(result.values()).isInstanceOf(DictionaryVector.class);

        DictionaryVector hosts = (DictionaryVector) result.values();
        BinaryVector rewrittenValues = (BinaryVector) hosts.values();
        assertThat(utf8(rewrittenValues, hosts.ids()[0])).isEqualTo("google.com");
        assertThat(utf8(rewrittenValues, hosts.ids()[1])).isEqualTo("news.ycombinator.com");
        assertThat(utf8(rewrittenValues, hosts.ids()[2])).isEqualTo("https://example.com");
        assertThat(utf8(rewrittenValues, hosts.ids()[3])).isEqualTo("mailto:test@example.com");
    }

    @Test
    void testUpperUtf8ProjectsUppercaseBytes()
    {
        Variable upper = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        upper,
                        new Call("upper_utf8", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(upper, Stream.VALUES),
                        new Reference(upper, Stream.NULLS)));

        BinaryVector input = new BinaryVector(3, 32);
        input.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        input.setBytes(0, "mixed".getBytes(UTF_8));
        input.setBytes(1, "mañana".getBytes(UTF_8));
        input.setNull(2);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, true});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), input,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator());

        Streams valuesResult = evaluator.evaluate(new Reference(upper, Stream.VALUES), Mask.all(3));
        Streams nullsResult = evaluator.evaluate(new Reference(upper, Stream.NULLS), Mask.all(3));

        BinaryVector values = (BinaryVector) valuesResult.get(Stream.VALUES);
        BooleanVector resultNulls = (BooleanVector) nullsResult.get(Stream.NULLS);
        assertThat(utf8(values, 0)).isEqualTo("MIXED");
        assertThat(utf8(values, 1)).isEqualTo("MAÑANA");
        assertThat(resultNulls.values()).containsExactly(false, false, true);
    }

    @Test
    void testCastUtf8ToI64ParsesSignedDigits()
    {
        Variable cast = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        cast,
                        new Call("cast_utf8_to_i64", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(cast, Stream.VALUES),
                        new Reference(cast, Stream.NULLS)));

        BinaryVector input = new BinaryVector(3, 16);
        input.setBytes(0, "12345".getBytes(UTF_8));
        input.setBytes(1, "-7".getBytes(UTF_8));
        input.setNull(2);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, true});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), input,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator());

        Streams valuesResult = evaluator.evaluate(new Reference(cast, Stream.VALUES), Mask.all(3));
        Streams nullsResult = evaluator.evaluate(new Reference(cast, Stream.NULLS), Mask.all(3));

        I64Vector values = (I64Vector) valuesResult.get(Stream.VALUES);
        BooleanVector resultNulls = (BooleanVector) nullsResult.get(Stream.NULLS);
        assertThat(values.values()).containsExactly(12345L, -7L, 0L);
        assertThat(resultNulls.values()).containsExactly(false, false, true);
    }

    @Test
    void testStructFieldCombinesParentAndChildNulls()
    {
        Variable name = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        name,
                        new StructField(new Reference(new Input(0), Stream.VALUES), "name"),
                        AllMask.ALL)),
                List.of(
                        new Reference(name, Stream.VALUES),
                        new Reference(name, Stream.NULLS)));

        StructVector person = new StructVector(4);
        BinaryVector names = new BinaryVector(4, 16);
        names.setBytes(0, "alice".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        names.setNull(1);
        names.setNull(2);
        names.setBytes(3, "carol".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        BooleanVector childNulls = new BooleanVector(new boolean[] {false, true, false, false});
        BooleanVector parentNulls = new BooleanVector(new boolean[] {false, false, true, false});
        person.setField("name", Streams.ofValues(names).with(Stream.NULLS, childNulls));

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), person,
                        new Reference(new Input(0), Stream.NULLS), parentNulls)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(name, Stream.VALUES), Mask.all(4));
        BinaryVector values = (BinaryVector) result.get(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);

        assertThat(utf8(values, 0)).isEqualTo("alice");
        assertThat(utf8(values, 3)).isEqualTo("carol");
        assertThat(nulls.values()).containsExactly(false, true, true, false);
    }

    @Test
    void testMapLookupCombinesMapKeyAndEntryNulls()
    {
        Variable lookup = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        lookup,
                        new Call("element_at_i64_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(lookup, Stream.VALUES),
                        new Reference(lookup, Stream.NULLS)));

        MapVector maps = new MapVector(4);
        maps.offsets()[0] = 0;
        maps.offsets()[1] = 2;
        maps.offsets()[2] = 2;
        maps.offsets()[3] = 3;
        maps.offsets()[4] = 4;

        BinaryVector mapKeys = new BinaryVector(4, 19);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        mapKeys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(1, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(2, "gamma".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(3, "zeta".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        I64Vector mapValues = new I64Vector(new long[] {10, 0, 30, 0});
        BooleanVector mapValueNulls = new BooleanVector(new boolean[] {false, true, false, true});
        maps.setEntries(Streams.ofValues(mapKeys), Streams.ofValues(mapValues).with(Stream.NULLS, mapValueNulls));

        BinaryVector lookupKeys = new BinaryVector(4, 20);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        lookupKeys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(1, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(2, "missing".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setNull(3);

        BooleanVector mapNulls = new BooleanVector(new boolean[] {false, true, false, false});
        BooleanVector lookupNulls = new BooleanVector(new boolean[] {false, false, false, true});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), maps,
                        new Reference(new Input(0), Stream.NULLS), mapNulls,
                        new Reference(new Input(1), Stream.VALUES), lookupKeys,
                        new Reference(new Input(1), Stream.NULLS), lookupNulls)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(lookup, Stream.VALUES), Mask.all(4));
        I64Vector values = (I64Vector) result.get(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);

        assertThat(values.values()).containsExactly(10L, 0L, 0L, 0L);
        assertThat(nulls.values()).containsExactly(false, true, true, true);
    }

    @Test
    void testMapLookupPropagatesInputErrors()
    {
        Variable lookup = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        lookup,
                        new Call("element_at_i64_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(lookup, Stream.ERRORS)));

        MapVector maps = new MapVector(3);
        BinaryVector mapKeys = new BinaryVector(0, 0);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        maps.setEntries(Streams.ofValues(mapKeys), Streams.ofValues(new I64Vector(new long[0])));

        BinaryVector lookupKeys = new BinaryVector(3, 3);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        lookupKeys.setBytes(0, "a".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(1, "b".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(2, "c".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BooleanVector mapErrors = new BooleanVector(new boolean[] {false, true, false});
        BooleanVector lookupErrors = new BooleanVector(new boolean[] {true, false, false});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), maps,
                        new Reference(new Input(0), Stream.ERRORS), mapErrors,
                        new Reference(new Input(1), Stream.VALUES), lookupKeys,
                        new Reference(new Input(1), Stream.ERRORS), lookupErrors)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(lookup, Stream.ERRORS), Mask.all(3));
        BooleanVector errors = (BooleanVector) result.get(Stream.ERRORS);
        assertThat(errors.values()).containsExactly(true, true, false);
    }

    @Test
    void testMapContainsKeyCombinesNullsAndErrors()
    {
        Variable contains = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        contains,
                        new Call("map_contains_key_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(contains, Stream.VALUES),
                        new Reference(contains, Stream.NULLS),
                        new Reference(contains, Stream.ERRORS)));

        MapVector maps = new MapVector(4);
        maps.offsets()[0] = 0;
        maps.offsets()[1] = 2;
        maps.offsets()[2] = 2;
        maps.offsets()[3] = 3;
        maps.offsets()[4] = 4;

        BinaryVector mapKeys = new BinaryVector(4, 19);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        mapKeys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(1, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(2, "gamma".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(3, "delta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        maps.setEntries(Streams.ofValues(mapKeys), Streams.ofValues(new I64Vector(new long[] {10, 20, 30, 40})));

        BinaryVector lookupKeys = new BinaryVector(4, 22);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        lookupKeys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(1, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(2, "missing".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(3, "delta".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BooleanVector mapNulls = new BooleanVector(new boolean[] {false, true, false, false});
        BooleanVector keyNulls = new BooleanVector(new boolean[] {false, false, true, false});
        BooleanVector mapErrors = new BooleanVector(new boolean[] {false, false, false, true});
        BooleanVector keyErrors = new BooleanVector(new boolean[] {false, false, false, false});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), maps,
                        new Reference(new Input(0), Stream.NULLS), mapNulls,
                        new Reference(new Input(0), Stream.ERRORS), mapErrors,
                        new Reference(new Input(1), Stream.VALUES), lookupKeys,
                        new Reference(new Input(1), Stream.NULLS), keyNulls,
                        new Reference(new Input(1), Stream.ERRORS), keyErrors)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(contains, Stream.VALUES), Mask.all(4));
        BooleanVector values = (BooleanVector) result.get(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);
        BooleanVector errors = (BooleanVector) result.get(Stream.ERRORS);

        assertThat(values.values()).containsExactly(true, false, false, true);
        assertThat(nulls.values()).containsExactly(false, true, true, false);
        assertThat(errors.values()).containsExactly(false, false, false, true);
    }

    @Test
    void testArrayElementCombinesParentIndexAndElementSemantics()
    {
        Variable element = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        element,
                        new Call("array_element_i64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(element, Stream.VALUES),
                        new Reference(element, Stream.NULLS),
                        new Reference(element, Stream.ERRORS)));

        ArrayVector arrays = new ArrayVector(6);
        arrays.offsets()[0] = 0;
        arrays.offsets()[1] = 3;
        arrays.offsets()[2] = 3;
        arrays.offsets()[3] = 4;
        arrays.offsets()[4] = 6;
        arrays.offsets()[5] = 7;
        arrays.offsets()[6] = 8;

        I64Vector elementValues = new I64Vector(new long[] {10, 0, 30, 40, 50, 60, 0, 70});
        BooleanVector elementNulls = new BooleanVector(new boolean[] {false, true, false, false, false, false, false, false});
        BooleanVector elementErrors = new BooleanVector(new boolean[] {false, false, false, false, false, true, false, false});
        arrays.setElements(Streams.ofValues(elementValues)
                .with(Stream.NULLS, elementNulls)
                .with(Stream.ERRORS, elementErrors));

        I64Vector indices = new I64Vector(new long[] {0, 0, 0, 5, 1, 0});
        BooleanVector arrayNulls = new BooleanVector(new boolean[] {false, false, true, false, false, false});
        BooleanVector indexNulls = new BooleanVector(new boolean[] {false, false, false, false, true, false});
        BooleanVector arrayErrors = new BooleanVector(new boolean[] {false, false, false, false, false, true});
        BooleanVector indexErrors = new BooleanVector(new boolean[] {false, false, false, false, false, false});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), arrays,
                        new Reference(new Input(0), Stream.NULLS), arrayNulls,
                        new Reference(new Input(0), Stream.ERRORS), arrayErrors,
                        new Reference(new Input(1), Stream.VALUES), indices,
                        new Reference(new Input(1), Stream.NULLS), indexNulls,
                        new Reference(new Input(1), Stream.ERRORS), indexErrors)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(element, Stream.VALUES), Mask.all(6));
        I64Vector values = (I64Vector) result.get(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);
        BooleanVector errors = (BooleanVector) result.get(Stream.ERRORS);

        assertThat(values.values()).containsExactly(10L, 0L, 0L, 0L, 0L, 70L);
        assertThat(nulls.values()).containsExactly(false, true, true, true, true, false);
        assertThat(errors.values()).containsExactly(false, false, false, false, false, true);
    }

    @Test
    void testMapKeysDoesNotReuseNestedOutputForSparseMasks()
    {
        PrimitiveFunction mapKeys = primitiveRegistry().get("map_keys");
        MapVector maps = createUtf8I64MapVector();
        BooleanVector inputNulls = new BooleanVector(new boolean[] {false, true, false, false});

        ArrayVector reusableValues = new ArrayVector(4);
        reusableValues.offsets()[0] = 99;
        reusableValues.offsets()[1] = 99;
        reusableValues.offsets()[2] = 99;
        reusableValues.offsets()[3] = 99;
        reusableValues.offsets()[4] = 99;
        reusableValues.setElements(Streams.ofValues(new BinaryVector(1, 8)));
        BooleanVector reusableNulls = new BooleanVector(new boolean[] {true, true, true, true});

        Streams output = Streams.of(Stream.VALUES, reusableValues)
                .with(Stream.NULLS, reusableNulls);

        Streams result = mapKeys.apply(
                List.of(Streams.ofValuesAndNulls(maps, inputNulls)),
                Mask.sparse(new int[] {1, 3}, maps.length()),
                Set.of(Stream.VALUES, Stream.NULLS),
                output,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(result.values()).isNotSameAs(reusableValues);
        assertThat(result.get(Stream.NULLS)).isNotSameAs(reusableNulls);
        assertThat(reusableValues.offsets()).containsExactly(99, 99, 99, 99, 99);
        assertThat(reusableNulls.values()).containsExactly(true, true, true, true);

        ArrayVector arrays = (ArrayVector) result.values();
        BinaryVector keys = (BinaryVector) arrays.elementValues();
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);

        assertThat(arrays.offsets()).containsExactly(0, 0, 1, 1, 3);
        assertThat(utf8(keys, 0)).isEqualTo("gamma");
        assertThat(utf8(keys, 1)).isEqualTo("delta");
        assertThat(utf8(keys, 2)).isEqualTo("epsilon");
        assertThat(nulls.values()).containsExactly(false, true, false, false);
    }

    @Test
    void testMapValuesDoesNotReuseNestedOutputForSparseMasks()
    {
        PrimitiveFunction mapValues = primitiveRegistry().get("map_values");
        MapVector maps = createUtf8I64MapVector();
        BooleanVector inputNulls = new BooleanVector(new boolean[] {false, true, false, false});

        ArrayVector reusableValues = new ArrayVector(4);
        reusableValues.offsets()[0] = 77;
        reusableValues.offsets()[1] = 77;
        reusableValues.offsets()[2] = 77;
        reusableValues.offsets()[3] = 77;
        reusableValues.offsets()[4] = 77;
        reusableValues.setElements(Streams.ofValues(new I64Vector(new long[] {999})));
        BooleanVector reusableNulls = new BooleanVector(new boolean[] {true, true, true, true});

        Streams output = Streams.of(Stream.VALUES, reusableValues)
                .with(Stream.NULLS, reusableNulls);

        Streams result = mapValues.apply(
                List.of(Streams.ofValuesAndNulls(maps, inputNulls)),
                Mask.sparse(new int[] {0, 2}, maps.length()),
                Set.of(Stream.VALUES, Stream.NULLS),
                output,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(result.values()).isNotSameAs(reusableValues);
        assertThat(result.get(Stream.NULLS)).isNotSameAs(reusableNulls);
        assertThat(reusableValues.offsets()).containsExactly(77, 77, 77, 77, 77);
        assertThat(reusableNulls.values()).containsExactly(true, true, true, true);

        ArrayVector arrays = (ArrayVector) result.values();
        I64Vector values = (I64Vector) arrays.elementValues();
        BooleanVector elementNulls = arrays.elementNulls();
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);

        assertThat(arrays.offsets()).containsExactly(0, 2, 2, 2, 2);
        assertThat(values.values()).containsExactly(10L, 20L);
        assertThat(elementNulls.values()).containsExactly(false, false);
        assertThat(nulls.values()).containsExactly(false, false, false, false);
    }

    @Test
    void testAddFunctionPreservesRleAcrossFullBatch()
    {
        PrimitiveFunction add = builtinPrimitiveRegistry().get("add");

        Streams result = add.apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 2}, new I64Vector(new long[] {1, 5}))),
                        Streams.ofValues(new RleVector(new int[] {1, 3}, new I64Vector(new long[] {10, 20})))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(result.values()).isInstanceOf(RleVector.class);
        RleVector vector = (RleVector) result.values();
        assertThat(vector.counts()).containsExactly(1, 1, 2);
        assertThat(((I64Vector) vector.values()).values()).containsExactly(11L, 21L, 25L);
    }

    @Test
    void testComparisonFunctionPreservesRleAcrossFullBatch()
    {
        PrimitiveFunction lessThan = builtinPrimitiveRegistry().get("lt");

        Streams result = lessThan.apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 2}, new I64Vector(new long[] {1, 5}))),
                        Streams.ofValues(new RleVector(new int[] {1, 3}, new I64Vector(new long[] {10, 4})))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(result.values()).isInstanceOf(RleVector.class);
        RleVector vector = (RleVector) result.values();
        assertThat(vector.counts()).containsExactly(1, 1, 2);
        assertThat(((BooleanVector) vector.values()).values()).containsExactly(true, true, false);
    }

    @Test
    void testDivideScaleRoundFunctionRoundsScaledDivision()
    {
        PrimitiveFunction divideScaleRound = builtinPrimitiveRegistry().get("divide_scale_round_i64");

        Streams result = divideScaleRound.apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {1, 2, 5})),
                        Streams.ofValues(new I64Vector(new long[] {6, 3, 2})),
                        Streams.ofValues(new I64Vector(new long[] {100, 100, 10}))),
                Mask.all(3),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(((I64Vector) result.values()).values()).containsExactly(17L, 67L, 25L);
    }

    @Test
    void testAddFunctionSupportsDictionaryInputs()
    {
        PrimitiveFunction add = builtinPrimitiveRegistry().get("add");

        Streams flatDictionary = add.apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {1, 2, 3, 4})),
                        Streams.ofValues(new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30})))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        Streams dictionaryFlat = add.apply(
                List.of(
                        Streams.ofValues(new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30}))),
                        Streams.ofValues(new I64Vector(new long[] {1, 2, 3, 4}))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        Streams dictionaryDictionary = add.apply(
                List.of(
                        Streams.ofValues(new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30}))),
                        Streams.ofValues(new DictionaryVector(new int[] {1, 1, 0, 0}, new I64Vector(new long[] {1, 2})))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(((I64Vector) flatDictionary.values()).values()).containsExactly(31L, 12L, 23L, 34L);
        assertThat(((I64Vector) dictionaryFlat.values()).values()).containsExactly(31L, 12L, 23L, 34L);
        assertThat(((I64Vector) dictionaryDictionary.values()).values()).containsExactly(32L, 12L, 21L, 31L);
    }

    @Test
    void testBooleanFunctionSupportsDictionaryInputs()
    {
        PrimitiveFunction or = primitiveRegistry().get("or");

        Streams dictionaryFlat = or.apply(
                List.of(
                        Streams.ofValues(new DictionaryVector(new int[] {2, 0, 1, 2}, new BooleanVector(new boolean[] {false, true, false}))),
                        Streams.ofValues(new BooleanVector(new boolean[] {false, false, true, false}))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        Streams dictionaryDictionary = or.apply(
                List.of(
                        Streams.ofValues(new DictionaryVector(new int[] {2, 0, 1, 2}, new BooleanVector(new boolean[] {false, true, false}))),
                        Streams.ofValues(new DictionaryVector(new int[] {1, 1, 0, 0}, new BooleanVector(new boolean[] {false, true})))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(((BooleanVector) dictionaryFlat.values()).values()).containsExactly(false, false, true, false);
        assertThat(((BooleanVector) dictionaryDictionary.values()).values()).containsExactly(true, true, true, false);
    }

    @Test
    void testExactFunctionsSupportRleInputs()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        Streams addExact = primitiveRegistry.get("add_exact").apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 2}, new I64Vector(new long[] {1, 5}))),
                        Streams.ofValues(new RleVector(new int[] {1, 3}, new I64Vector(new long[] {10, 20})))),
                Mask.all(4),
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(addExact.values()).isInstanceOf(RleVector.class);
        assertThat(((I64Vector) ((RleVector) addExact.values()).values()).values()).containsExactly(11L, 21L, 25L);
        assertThat(addExact.get(Stream.ERRORS)).isInstanceOf(RleVector.class);
        assertThat(((BooleanVector) ((RleVector) addExact.get(Stream.ERRORS)).values()).values()).containsExactly(false, false, false);

        Streams subtractExact = primitiveRegistry.get("subtract_exact").apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 2}, new I64Vector(new long[] {10, 30}))),
                        Streams.ofValues(new I64Vector(new long[] {1, 2, 3, 4}))),
                Mask.all(4),
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(subtractExact.values()).isInstanceOf(I64Vector.class);
        assertThat(((I64Vector) subtractExact.values()).values()).containsExactly(9L, 8L, 27L, 26L);
        assertThat(((BooleanVector) subtractExact.get(Stream.ERRORS)).values()).containsExactly(false, false, false, false);
    }

    @Test
    void testExactFunctionsReportOverflowViaErrorsStream()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        Streams addExact = primitiveRegistry.get("add_exact").apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {Long.MAX_VALUE, 1})),
                        Streams.ofValues(new I64Vector(new long[] {1, 2}))),
                Mask.all(2),
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(((I64Vector) addExact.values()).values()).containsExactly(Long.MIN_VALUE, 3L);
        assertThat(((BooleanVector) addExact.get(Stream.ERRORS)).values()).containsExactly(true, false);

        Streams subtractExact = primitiveRegistry.get("subtract_exact").apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {Long.MIN_VALUE, 10})),
                        Streams.ofValues(new I64Vector(new long[] {1, 3}))),
                Mask.all(2),
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(((I64Vector) subtractExact.values()).values()).containsExactly(Long.MAX_VALUE, 7L);
        assertThat(((BooleanVector) subtractExact.get(Stream.ERRORS)).values()).containsExactly(true, false);
    }

    @Test
    void testDivideAndModuloReportErrorsViaErrorsStream()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        Streams divide = primitiveRegistry.get("divide").apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {20, 21, 22})),
                        Streams.ofValues(new I64Vector(new long[] {5, 0, 2}))),
                Mask.all(3),
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(((I64Vector) divide.values()).values()).containsExactly(4L, 0L, 11L);
        assertThat(((BooleanVector) divide.get(Stream.ERRORS)).values()).containsExactly(false, true, false);

        Streams modulo = primitiveRegistry.get("modulo").apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 1}, new I64Vector(new long[] {20, 22}))),
                        Streams.ofValues(new I64Vector(new long[] {6, 0, 5}))),
                Mask.all(3),
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(((I64Vector) modulo.values()).values()).containsExactly(2L, 0L, 2L);
        assertThat(((BooleanVector) modulo.get(Stream.ERRORS)).values()).containsExactly(false, true, false);
    }

    @Test
    void testDivideCanProduceOnlyErrorsStream()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        Streams divide = primitiveRegistry.get("divide").apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {20, 21, 22})),
                        Streams.ofValues(new I64Vector(new long[] {5, 0, 2}))),
                Mask.all(3),
                Set.of(Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(divide.has(Stream.VALUES)).isFalse();
        assertThat(((BooleanVector) divide.get(Stream.ERRORS)).values()).containsExactly(false, true, false);
    }

    @Test
    void testMergeCopiesRleInputsIntoMaskedOutput()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(new Input(0), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)),
                                new Reference(new Input(1), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES),
                                new Reference(new Input(2), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true, false}),
                new Reference(new Input(1), Stream.VALUES), new RleVector(new int[] {2, 2}, new I64Vector(new long[] {10, 20})),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}))), new Allocator());

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(10L, 1L, 20L, 1L);
    }

    @Test
    void testCopyCanForwardDictionaryInput()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        DictionaryVector dictionary = new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30}));
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Copy(new Reference(new Input(0), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), dictionary)), new Allocator());

        assertThat(evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES)).isSameAs(dictionary);
    }

    @Test
    void testMergeCopiesDictionaryInputsIntoMaskedOutput()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true, false}),
                new Reference(new Input(1), Stream.VALUES), new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30})),
                new Reference(new Input(2), Stream.VALUES), new DictionaryVector(new int[] {1, 1, 0, 0}, new I64Vector(new long[] {1, 2})))), new Allocator());

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(30L, 2L, 20L, 1L);
    }

    @Test
    void testEvaluatesCompositeMaskExpressions()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        Reference left = new Reference(new Input(0), Stream.VALUES);
        Reference right = new Reference(new Input(1), Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new OrMask(List.of(
                                        new AndMask(List.of(new ReferenceMask(left), new NotMask(new ReferenceMask(right)))),
                                        new AndMask(List.of(new NotMask(new ReferenceMask(left)), new ReferenceMask(right))))),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, false, false}),
                new Reference(new Input(1), Stream.VALUES), new BooleanVector(new boolean[] {false, true, true, false}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(3), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator());

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(1L, 2L, 1L, 2L);
    }

    @Test
    void testEvaluatesMergeConditionThroughBooleanReferenceMask()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable leftOnly = new Variable(0);
        Variable rightOnly = new Variable(1);
        Variable predicate = new Variable(2);
        Variable result = new Variable(3);
        Reference left = new Reference(new Input(0), Stream.VALUES);
        Reference right = new Reference(new Input(1), Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(leftOnly, new Call("lt", List.of(left, new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                        new Assignment(rightOnly, new Call("lt", List.of(new Reference(new Input(3), Stream.VALUES), right)), AllMask.ALL),
                        new Assignment(predicate, new Call("or", List.of(
                                new Reference(leftOnly, Stream.VALUES),
                                new Reference(rightOnly, Stream.VALUES))), AllMask.ALL),
                        new Assignment(result, new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(predicate, Stream.VALUES)),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES)),
                                AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                left, new I64Vector(new long[] {1, 5, 7, 3}),
                right, new I64Vector(new long[] {9, 2, 6, 1}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {4, 4, 4, 4}),
                new Reference(new Input(3), Stream.VALUES), new I64Vector(new long[] {4, 4, 4, 4}),
                new Reference(new Input(4), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(5), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator());

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(1L, 2L, 1L, 1L);
    }

    @Test
    void testEvaluatesErrorsStreamDirectly()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable quotient = new Variable(0);
        Reference errors = new Reference(quotient, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        quotient,
                        new Call("divide", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(errors),
                Map.of(errors, StreamPlan.MATERIALIZED));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {20, 21, 22}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {5, 0, 2}))), new Allocator());

        BooleanVector errorsVector = (BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS);
        assertThat(errorsVector.values()).containsExactly(false, true, false);
    }

    @Test
    void testSynthesizesAbsentInputNullsAndErrors()
    {
        Reference inputNulls = new Reference(new Input(0), Stream.NULLS);
        Reference inputErrors = new Reference(new Input(0), Stream.ERRORS);
        PlanEvaluator evaluator = new PlanEvaluator(
                new EvaluationPlan(List.of(), List.of(inputNulls, inputErrors)),
                new PrimitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3}))),
                new Allocator());

        assertThat(((BooleanVector) evaluator.evaluate(inputNulls, Mask.all(3)).get(Stream.NULLS)).values()).containsExactly(false, false, false);
        assertThat(((BooleanVector) evaluator.evaluate(inputErrors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, false, false);
    }

    @Test
    void testCopyOfErrorsRequestsOnlyErrors()
    {
        AtomicReference<Set<Stream>> requestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("source", (inputs, mask, requested, output, context) -> {
            requestedStreams.set(Set.copyOf(requested));
            return Streams.of(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false}));
        });

        Variable source = new Variable(0);
        Variable copied = new Variable(1);
        Reference copiedErrors = new Reference(copied, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(source, new Call("source", List.of()), AllMask.ALL),
                        new Assignment(copied, new org.weakref.nitro.operator.evaluator.ir.Copy(new Reference(source, Stream.ERRORS)), AllMask.ALL)),
                List.of(copiedErrors));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator());

        assertThat(((BooleanVector) evaluator.evaluate(copiedErrors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(requestedStreams.get()).containsExactly(Stream.ERRORS);
    }

    @Test
    void testCopyOfValuesCanProjectSiblingErrors()
    {
        AtomicReference<Set<Stream>> requestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("source", (inputs, mask, requested, output, context) -> {
            requestedStreams.set(Set.copyOf(requested));
            Streams result = Streams.empty();
            if (requested.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {1, 2, 3}));
            }
            if (requested.contains(Stream.ERRORS)) {
                result = result.with(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
        });

        Variable source = new Variable(0);
        Variable copied = new Variable(1);
        Reference copiedErrors = new Reference(copied, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(source, new Call("source", List.of()), AllMask.ALL),
                        new Assignment(copied, new org.weakref.nitro.operator.evaluator.ir.Copy(new Reference(source, Stream.VALUES)), AllMask.ALL)),
                List.of(copiedErrors));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator());

        assertThat(((BooleanVector) evaluator.evaluate(copiedErrors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(requestedStreams.get()).containsExactly(Stream.ERRORS);
    }

    @Test
    void testMergeOfErrorsRequestsOnlyErrors()
    {
        AtomicReference<Set<Stream>> trueRequestedStreams = new AtomicReference<>(Set.of());
        AtomicReference<Set<Stream>> falseRequestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("when_true", (inputs, mask, requested, output, context) -> {
            trueRequestedStreams.set(Set.copyOf(requested));
            return Streams.of(Stream.ERRORS, new BooleanVector(new boolean[] {true, true, false}));
        });
        primitiveRegistry.register("when_false", (inputs, mask, requested, output, context) -> {
            falseRequestedStreams.set(Set.copyOf(requested));
            return Streams.of(Stream.ERRORS, new BooleanVector(new boolean[] {false, false, true}));
        });

        Variable whenTrue = new Variable(0);
        Variable whenFalse = new Variable(1);
        Variable merged = new Variable(2);
        Reference mergedErrors = new Reference(merged, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(whenTrue, new Call("when_true", List.of()), AllMask.ALL),
                        new Assignment(whenFalse, new Call("when_false", List.of()), AllMask.ALL),
                        new Assignment(
                                merged,
                                new org.weakref.nitro.operator.evaluator.ir.Merge(
                                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                        new Reference(whenTrue, Stream.ERRORS),
                                        new Reference(whenFalse, Stream.ERRORS)),
                                AllMask.ALL)),
                List.of(mergedErrors));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true}))), new Allocator());

        assertThat(((BooleanVector) evaluator.evaluate(mergedErrors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(true, false, false);
        assertThat(trueRequestedStreams.get()).containsExactly(Stream.ERRORS);
        assertThat(falseRequestedStreams.get()).containsExactly(Stream.ERRORS);
    }

    @Test
    void testMergeOfValuesCanProjectSiblingErrors()
    {
        AtomicReference<Set<Stream>> trueRequestedStreams = new AtomicReference<>(Set.of());
        AtomicReference<Set<Stream>> falseRequestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("when_true", (inputs, mask, requested, output, context) -> {
            trueRequestedStreams.set(Set.copyOf(requested));
            Streams result = Streams.empty();
            if (requested.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {1, 1, 1}));
            }
            if (requested.contains(Stream.ERRORS)) {
                result = result.with(Stream.ERRORS, new BooleanVector(new boolean[] {true, true, false}));
            }
            return result;
        });
        primitiveRegistry.register("when_false", (inputs, mask, requested, output, context) -> {
            falseRequestedStreams.set(Set.copyOf(requested));
            Streams result = Streams.empty();
            if (requested.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {2, 2, 2}));
            }
            if (requested.contains(Stream.ERRORS)) {
                result = result.with(Stream.ERRORS, new BooleanVector(new boolean[] {false, false, true}));
            }
            return result;
        });

        Variable whenTrue = new Variable(0);
        Variable whenFalse = new Variable(1);
        Variable merged = new Variable(2);
        Reference mergedErrors = new Reference(merged, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(whenTrue, new Call("when_true", List.of()), AllMask.ALL),
                        new Assignment(whenFalse, new Call("when_false", List.of()), AllMask.ALL),
                        new Assignment(
                                merged,
                                new org.weakref.nitro.operator.evaluator.ir.Merge(
                                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                        new Reference(whenTrue, Stream.VALUES),
                                        new Reference(whenFalse, Stream.VALUES)),
                                AllMask.ALL)),
                List.of(mergedErrors));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true}))), new Allocator());

        assertThat(((BooleanVector) evaluator.evaluate(mergedErrors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(true, false, false);
        assertThat(trueRequestedStreams.get()).containsExactly(Stream.ERRORS);
        assertThat(falseRequestedStreams.get()).containsExactly(Stream.ERRORS);
    }

    @Test
    void testRequestsOnlyErrorsWhenOnlyErrorsAreProjected()
    {
        AtomicReference<Set<Stream>> requestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("counting", (inputs, mask, requested, output, context) -> {
            requestedStreams.set(Set.copyOf(requested));
            return Streams.of(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false}));
        });

        Variable result = new Variable(0);
        Reference errors = new Reference(result, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("counting", List.of()), AllMask.ALL)),
                List.of(errors),
                Map.of(errors, StreamPlan.MATERIALIZED));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator());

        assertThat(((BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(requestedStreams.get()).containsExactly(Stream.ERRORS);
    }

    @Test
    void testMemoizesSiblingValueAndErrorStreamsTogether()
    {
        AtomicInteger evaluations = new AtomicInteger();
        AtomicReference<Set<Stream>> requestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("counting", (inputs, mask, requested, output, context) -> {
            evaluations.incrementAndGet();
            requestedStreams.set(Set.copyOf(requested));
            Streams result = Streams.empty();
            if (requested.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {1, 2, 3}));
            }
            if (requested.contains(Stream.ERRORS)) {
                result = result.with(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
        });

        Variable result = new Variable(0);
        Reference values = new Reference(result, Stream.VALUES);
        Reference errors = new Reference(result, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("counting", List.of()), AllMask.ALL)),
                List.of(values, errors),
                Map.of(
                        values, StreamPlan.MATERIALIZED,
                        errors, StreamPlan.MATERIALIZED));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator());

        assertThat(((BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(((I64Vector) evaluator.evaluate(values, Mask.all(3)).get(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
        assertThat(evaluations).hasValue(1);
        assertThat(requestedStreams.get()).containsExactlyInAnyOrder(Stream.VALUES, Stream.ERRORS);
    }

    @Test
    void testEvaluatesNullsStreamDirectly()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("nullable", (inputs, mask, requestedStreams, output, context) -> {
            Streams result = Streams.empty();
            if (requestedStreams.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {1, 2, 3}));
            }
            if (requestedStreams.contains(Stream.NULLS)) {
                result = result.with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
        });

        Variable result = new Variable(0);
        Reference nulls = new Reference(result, Stream.NULLS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("nullable", List.of()), AllMask.ALL)),
                List.of(nulls),
                Map.of(nulls, StreamPlan.MATERIALIZED));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator());

        BooleanVector nullsVector = (BooleanVector) evaluator.evaluate(nulls, Mask.all(3)).get(Stream.NULLS);
        assertThat(nullsVector.values()).containsExactly(false, true, false);
    }

    @Test
    void testMemoizesSiblingValueAndNullStreamsTogether()
    {
        AtomicInteger evaluations = new AtomicInteger();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("nullable", (inputs, mask, requestedStreams, output, context) -> {
            evaluations.incrementAndGet();
            Streams result = Streams.empty();
            if (requestedStreams.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {1, 2, 3}));
            }
            if (requestedStreams.contains(Stream.NULLS)) {
                result = result.with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
        });

        Variable result = new Variable(0);
        Reference values = new Reference(result, Stream.VALUES);
        Reference nulls = new Reference(result, Stream.NULLS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("nullable", List.of()), AllMask.ALL)),
                List.of(values, nulls),
                Map.of(
                        values, StreamPlan.MATERIALIZED,
                        nulls, StreamPlan.MATERIALIZED));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator());

        assertThat(((BooleanVector) evaluator.evaluate(nulls, Mask.all(3)).get(Stream.NULLS)).values()).containsExactly(false, true, false);
        assertThat(((I64Vector) evaluator.evaluate(values, Mask.all(3)).get(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
        assertThat(evaluations).hasValue(1);
    }

    @Test
    void testDoesNotMemoizeScratchOnlyMaskBundle()
    {
        AtomicInteger evaluations = new AtomicInteger();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("predicate", (inputs, mask, requestedStreams, output, context) -> {
            evaluations.incrementAndGet();
            Streams result = Streams.empty();
            if (requestedStreams.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new BooleanVector(new boolean[] {true, false, true}));
            }
            if (requestedStreams.contains(Stream.ERRORS)) {
                result = result.with(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
        });

        Variable result = new Variable(0);
        Reference values = new Reference(result, Stream.VALUES);
        Reference errors = new Reference(result, Stream.ERRORS);
        EvaluationPlan normalizedPlan = IrNormalizer.normalize(new EvaluationPlan(
                List.of(new Assignment(result, new Call("predicate", List.of()), AllMask.ALL)),
                List.of(),
                Map.of(
                        values, StreamPlan.MATERIALIZED,
                        errors, StreamPlan.MATERIALIZED),
                Map.of(values, new ReferenceMask(values))));

        PlanEvaluator evaluator = new PlanEvaluator(normalizedPlan, primitiveRegistry, inputResolver(Map.of()), new Allocator());

        assertThat(((BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(((BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(evaluations).hasValue(2);
    }

    @Test
    void testAdaptiveAndReorderingUsesMoreSelectiveTermFirstOnLaterRuns()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        Reference first = new Reference(new Input(0), Stream.VALUES);
        Reference second = new Reference(new Input(1), Stream.VALUES);
        EvaluationPlan plan = IrNormalizer.normalize(new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new AndMask(List.of(new ReferenceMask(first), new ReferenceMask(second))),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES))));

        AtomicReference<List<Integer>> maskSizes = new AtomicReference<>(List.of());
        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, (reference, mask) -> {
            if (reference.equals(first) || reference.equals(second)) {
                maskSizes.updateAndGet(existing -> {
                    var updated = new java.util.ArrayList<>(existing);
                    updated.add(mask.selectedCount());
                    return List.copyOf(updated);
                });
            }

            if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                return new BooleanVector(new boolean[] {true, true, true, true, true, false, false, false});
            }
            if (reference.equals(new Reference(new Input(1), Stream.VALUES))) {
                return new BooleanVector(new boolean[] {false, false, false, false, true, false, false, false});
            }
            if (reference.equals(new Reference(new Input(2), Stream.VALUES))) {
                return new I64Vector(new long[] {1, 1, 1, 1, 1, 1, 1, 1});
            }
            if (reference.equals(new Reference(new Input(3), Stream.VALUES))) {
                return new I64Vector(new long[] {2, 2, 2, 2, 2, 2, 2, 2});
            }
            return null;
        }, new Allocator());

        evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(8));
        assertThat(maskSizes.get()).containsExactly(8, 5);

        maskSizes.set(List.of());
        evaluator.reset();
        evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(8));
        assertThat(maskSizes.get()).containsExactly(8, 1);
    }

    @Test
    void testAdaptiveOrReorderingUsesMoreSelectiveTermFirstOnLaterRuns()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        Reference first = new Reference(new Input(0), Stream.VALUES);
        Reference second = new Reference(new Input(1), Stream.VALUES);
        EvaluationPlan plan = IrNormalizer.normalize(new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new OrMask(List.of(new ReferenceMask(first), new ReferenceMask(second))),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES))));

        AtomicReference<List<Integer>> maskSizes = new AtomicReference<>(List.of());
        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, (reference, mask) -> {
            if (reference.equals(first) || reference.equals(second)) {
                maskSizes.updateAndGet(existing -> {
                    var updated = new java.util.ArrayList<>(existing);
                    updated.add(mask.selectedCount());
                    return List.copyOf(updated);
                });
            }

            if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                return new BooleanVector(new boolean[] {true, false, false, false, false, false, false, false});
            }
            if (reference.equals(new Reference(new Input(1), Stream.VALUES))) {
                return new BooleanVector(new boolean[] {true, true, true, true, true, false, false, false});
            }
            if (reference.equals(new Reference(new Input(2), Stream.VALUES))) {
                return new I64Vector(new long[] {1, 1, 1, 1, 1, 1, 1, 1});
            }
            if (reference.equals(new Reference(new Input(3), Stream.VALUES))) {
                return new I64Vector(new long[] {2, 2, 2, 2, 2, 2, 2, 2});
            }
            return null;
        }, new Allocator());

        evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(8));
        assertThat(maskSizes.get()).containsExactly(8, 7);

        maskSizes.set(List.of());
        evaluator.reset();
        evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(8));
        assertThat(maskSizes.get()).containsExactly(8, 3);
    }

    @Test
    void testResetReleasesPrimitiveScratchContexts()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Call(
                                "add",
                                List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        Allocator allocator = new Allocator();
        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {4, 5, 6}))), allocator);

        evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(3));
        assertThat(allocator.currentBytes(new Allocator.Context("AddI64"))).isPositive();

        evaluator.reset();

        assertThat(allocator.currentBytes(new Allocator.Context("AddI64"))).isZero();
        assertThat(allocator.currentBytes(new Allocator.Context("PlanEvaluator"))).isZero();
    }

    @Test
    void testAndMaskKeepsNullAndErrorRowsOutOfFinalTrueResult()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new AndMask(List.of(
                                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                        new ReferenceMask(new Reference(new Input(1), Stream.VALUES)))),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, false}),
                new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, false}),
                new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {true, false, false, false}),
                new Reference(new Input(1), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, true}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(3), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator());

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(2L, 2L, 1L, 2L);
    }

    @Test
    void testDirectAndMaskEvaluationDropsRowsThatAreNotUltimatelyTrue()
    {
        PlanEvaluator evaluator = new PlanEvaluator(
                new EvaluationPlan(List.of(), List.of()),
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, false}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, false}),
                        new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {true, false, false, false}),
                        new Reference(new Input(1), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, true}))),
                new Allocator());

        Mask result = evaluator.evaluate(
                new AndMask(List.of(
                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                        new ReferenceMask(new Reference(new Input(1), Stream.VALUES)))),
                Mask.all(4));

        assertThat(result.selectedCount()).isEqualTo(1);
        assertThat(result.position(0)).isEqualTo(2);
    }

    @Test
    void testDirectNotMaskEvaluationKeepsOnlyDefiniteFalseRows()
    {
        Variable predicate = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        predicate,
                        new Call("contains_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of());

        BinaryVector haystack = new BinaryVector(5, 64);
        haystack.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        haystack.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        haystack.setBytes(0, "google".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        haystack.setBytes(1, "bing".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        haystack.setBytes(2, "maps.google".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        haystack.setBytes(3, "".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        haystack.setBytes(4, "ask".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BooleanVector haystackNulls = new BooleanVector(new boolean[] {false, false, false, true, false});
        BinaryVector needle = new BinaryVector(1, 16);
        needle.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        needle.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        needle.setBytes(0, "google".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), haystack,
                        new Reference(new Input(0), Stream.NULLS), haystackNulls,
                        new Reference(new Input(1), Stream.VALUES), new RleVector(new int[] {5}, needle))),
                new Allocator());

        Mask result = evaluator.evaluate(
                new NotMask(new ReferenceMask(new Reference(predicate, Stream.VALUES))),
                Mask.all(5));

        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(1);
        assertThat(result.position(1)).isEqualTo(4);
    }

    @Test
    void testDirectNotMaskEvaluationOptimizesSimpleLongEquality()
    {
        Variable zero = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(
                                equals,
                                new Call("eq", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {0, 1, 0, 5, 7}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, false, true, false, false}))),
                new Allocator());

        Mask result = evaluator.evaluate(
                new NotMask(new ReferenceMask(new Reference(equals, Stream.VALUES))),
                Mask.all(5));

        assertThat(result.selectedCount()).isEqualTo(3);
        assertThat(result.position(0)).isEqualTo(1);
        assertThat(result.position(1)).isEqualTo(3);
        assertThat(result.position(2)).isEqualTo(4);
    }

    @Test
    void testCoalesceI64ReplacesNullWithFallbackValue()
    {
        Variable zero = new Variable(0);
        Variable value = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(
                                value,
                                new Call("coalesce_i64", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(
                        new Reference(value, Stream.VALUES),
                        new Reference(value, Stream.NULLS)));

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {7L, 0L, 9L}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false}))),
                new Allocator());

        Streams valuesResult = evaluator.evaluate(new Reference(value, Stream.VALUES), Mask.all(3));
        assertThat(((I64Vector) valuesResult.get(Stream.VALUES)).values()).containsExactly(7L, 0L, 9L);
        assertThat(((BooleanVector) valuesResult.get(Stream.NULLS)).values()).containsExactly(false, false, false);
    }

    @Test
    void testCastI64ToI32ProjectsDictionaryEncodedValues()
    {
        Variable castValue = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        castValue,
                        new Call("cast_i64_to_i32", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(castValue, Stream.VALUES)));

        DictionaryVector values = DictionaryVector.wrap(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {7L, 11L, 13L}));

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(castValue, Stream.VALUES), Mask.all(4));
        assertThat(result.values()).isInstanceOf(DictionaryVector.class);

        DictionaryVector encoded = (DictionaryVector) result.values();
        assertThat(encoded.ids()).containsExactly(2, 0, 1, 2);
        assertThat(((I32Vector) encoded.values()).values()).containsExactly(7, 11, 13);
    }

    @Test
    void testCastI64ToI32DoesNotRequestInputNullsWhenOnlyValuesAreNeeded()
    {
        Variable castValue = new Variable(0);
        Variable zero = new Variable(1);
        Variable sum = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(
                                castValue,
                                new Call("cast_i64_to_i32", List.of(new Reference(new Input(0), Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(
                                sum,
                                new Call("add", List.of(
                                        new Reference(castValue, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(sum, Stream.VALUES)));

        AtomicBoolean requestedNulls = new AtomicBoolean();
        DictionaryVector values = DictionaryVector.wrap(new int[] {1, 0, 1}, new I64Vector(new long[] {17L, 29L}));
        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        requestedNulls.set(true);
                        throw new AssertionError("cast_i64_to_i32 should not request input nulls for values-only output");
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        return values;
                    }
                    return null;
                },
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(sum, Stream.VALUES), Mask.all(3));
        assertThat(result.get(Stream.VALUES)).isInstanceOf(DictionaryVector.class);
        DictionaryVector encoded = (DictionaryVector) result.get(Stream.VALUES);
        assertThat(encoded.ids()).containsExactly(1, 0, 1);
        assertThat(((I64Vector) encoded.values()).values()).containsExactly(17L, 29L);
        assertThat(requestedNulls).isFalse();
    }

    @Test
    void testIsNullI64RequestsOnlyInputNullStream()
    {
        Variable isNull = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        isNull,
                        new Call("is_null_i64", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(isNull, Stream.VALUES)));

        AtomicBoolean requestedValues = new AtomicBoolean();
        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        requestedValues.set(true);
                        throw new AssertionError("is_null_i64 should not request the VALUES stream");
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        return new BooleanVector(new boolean[] {false, true, false});
                    }
                    return null;
                },
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(isNull, Stream.VALUES), Mask.all(3));
        assertThat(((BooleanVector) result.get(Stream.VALUES)).values()).containsExactly(false, true, false);
        assertThat(requestedValues).isFalse();
    }

    @Test
    void testDivideI64ToF64ProjectsFloatingPointAverage()
    {
        Variable average = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        average,
                        new Call("divide_i64_to_f64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(average, Stream.VALUES),
                        new Reference(average, Stream.NULLS)));

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {9L, 5L}),
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {2L, 0L}))),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(average, Stream.VALUES), Mask.all(2));
        assertThat(((F64Vector) result.get(Stream.VALUES)).values()).containsExactly(4.5, 0.0);
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true);
    }

    @Test
    void testSubtractExactErrorPathDoesNotRequestInputNulls()
    {
        Variable difference = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        difference,
                        new Call("subtract_exact", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(difference, Stream.ERRORS)));

        AtomicBoolean requestedNulls = new AtomicBoolean();
        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS)) || reference.equals(new Reference(new Input(1), Stream.NULLS))) {
                        requestedNulls.set(true);
                        throw new AssertionError("subtract_exact error-only path should not request input nulls");
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        return new I64Vector(new long[] {Long.MIN_VALUE, 10L});
                    }
                    if (reference.equals(new Reference(new Input(1), Stream.VALUES))) {
                        return new I64Vector(new long[] {1L, 3L});
                    }
                    return null;
                },
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(difference, Stream.ERRORS), Mask.all(2));
        assertThat(((BooleanVector) result.get(Stream.ERRORS)).values()).containsExactly(true, false);
        assertThat(requestedNulls).isFalse();
    }

    @Test
    void testLengthUtf8DoesNotRequestInputNullsWhenOnlyValuesAreNeeded()
    {
        Variable length = new Variable(0);
        Variable one = new Variable(1);
        Variable shifted = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(
                                length,
                                new Call("length_utf8", List.of(new Reference(new Input(0), Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(
                                shifted,
                                new Call("add", List.of(
                                        new Reference(length, Stream.VALUES),
                                        new Reference(one, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(shifted, Stream.VALUES)));

        AtomicBoolean requestedNulls = new AtomicBoolean();
        BinaryVector values = utf8Vector("go", "nitro", "x");
        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        requestedNulls.set(true);
                        throw new AssertionError("length_utf8 values-only path should not request input nulls");
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        return values;
                    }
                    return null;
                },
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(shifted, Stream.VALUES), Mask.all(3));
        assertThat(((I64Vector) result.get(Stream.VALUES)).values()).containsExactly(3L, 6L, 2L);
        assertThat(requestedNulls).isFalse();
    }

    @Test
    void testEqualMaskEvaluationRequestsInputNulls()
    {
        Variable seven = new Variable(0);
        Variable equalsSeven = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(seven, new Literal(7L), AllMask.ALL),
                        new Assignment(
                                equalsSeven,
                                new Call("eq", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(seven, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        AtomicBoolean requestedNulls = new AtomicBoolean();
        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        return new I64Vector(new long[] {7L, 7L, 8L});
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        requestedNulls.set(true);
                        return new BooleanVector(new boolean[] {false, true, false});
                    }
                    return null;
                },
                new Allocator());

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(equalsSeven, Stream.VALUES)), Mask.all(3));
        assertThat(result.selectedCount()).isEqualTo(1);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(requestedNulls).isTrue();
    }

    @Test
    void testLessThanNullEvaluationStillRequestsInputValues()
    {
        Variable threshold = new Variable(0);
        Variable lessThan = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(threshold, new Literal(10L), AllMask.ALL),
                        new Assignment(
                                lessThan,
                                new Call("lt", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(threshold, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(lessThan, Stream.NULLS)));

        AtomicBoolean requestedValues = new AtomicBoolean();
        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        requestedValues.set(true);
                        return new I64Vector(new long[] {1L, 2L, 3L});
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        return new BooleanVector(new boolean[] {false, true, false});
                    }
                    return null;
                },
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(lessThan, Stream.NULLS), Mask.all(3));
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true, false);
        assertThat(requestedValues).isTrue();
    }

    @Test
    void testScaledRelativeDifferenceGtI64OptimizesQuarterlyDeviationMask()
    {
        Variable ten = new Variable(0);
        Variable deviationLarge = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(ten, new Literal(10L), AllMask.ALL),
                        new Assignment(
                                deviationLarge,
                                new Call("scaled_relative_difference_gt_i64", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(ten, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                builtinPrimitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {100L, 105L, 80L, 100L}),
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {90L, 100L, 100L, 0L}),
                        new Reference(new Input(1), Stream.NULLS), new BooleanVector(new boolean[] {false, false, false, false}))),
                new Allocator());

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(deviationLarge, Stream.VALUES)), Mask.all(4));
        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(2);
    }

    @Test
    void testMultiplyNullEvaluationStillRequestsInputValues()
    {
        Variable product = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        product,
                        new Call("multiply", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(product, Stream.NULLS)));

        AtomicBoolean requestedLeftValues = new AtomicBoolean();
        AtomicBoolean requestedRightValues = new AtomicBoolean();
        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        requestedLeftValues.set(true);
                        return new I64Vector(new long[] {3L, 4L});
                    }
                    if (reference.equals(new Reference(new Input(1), Stream.VALUES))) {
                        requestedRightValues.set(true);
                        return new I64Vector(new long[] {5L, 6L});
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        return new BooleanVector(new boolean[] {false, true});
                    }
                    if (reference.equals(new Reference(new Input(1), Stream.NULLS))) {
                        return new BooleanVector(new boolean[] {false, false});
                    }
                    return null;
                },
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(product, Stream.NULLS), Mask.all(2));
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true);
        assertThat(requestedLeftValues).isTrue();
        assertThat(requestedRightValues).isTrue();
    }

    @Test
    void testOrMaskAllowsLaterTrueToSuppressNullAndError()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new OrMask(List.of(
                                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                        new ReferenceMask(new Reference(new Input(1), Stream.VALUES)))),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, false, false}),
                new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, false}),
                new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {true, false, false, false}),
                new Reference(new Input(1), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, false}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(3), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator());

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(1L, 1L, 1L, 2L);
    }

    @Test
    void testDirectOrMaskEvaluationKeepsRowsThatBecomeTrueLater()
    {
        PlanEvaluator evaluator = new PlanEvaluator(
                new EvaluationPlan(List.of(), List.of()),
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, false, false}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, false}),
                        new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {true, false, false, false}),
                        new Reference(new Input(1), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, false}))),
                new Allocator());

        Mask result = evaluator.evaluate(
                new OrMask(List.of(
                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                        new ReferenceMask(new Reference(new Input(1), Stream.VALUES)))),
                Mask.all(4));

        assertThat(result.selectedCount()).isEqualTo(3);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(1);
        assertThat(result.position(2)).isEqualTo(2);
    }

    @Test
    void testUtf8DictionaryLiteralReferenceMaskUsesPrimitiveTrueMask()
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(literal, new Literal(""), AllMask.ALL),
                        new Assignment(
                                equals,
                                new Call("eq_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(literal, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        BinaryVector dictionary = new BinaryVector(3, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(1, "iphone".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(2, "pixel".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 0, 2, 1}, dictionary);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, true, false});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), values,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator());

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(equals, Stream.VALUES)), Mask.all(5));
        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(2);
    }

    @Test
    void testUtf8DictionaryLiteralNotMaskUsesPrimitiveFalseMask()
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(literal, new Literal(""), AllMask.ALL),
                        new Assignment(
                                equals,
                                new Call("eq_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(literal, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        BinaryVector dictionary = new BinaryVector(3, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(1, "iphone".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(2, "pixel".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 0, 2, 1}, dictionary);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, true, false});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), values,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator());

        Mask result = evaluator.evaluate(new NotMask(new ReferenceMask(new Reference(equals, Stream.VALUES))), Mask.all(5));
        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(1);
        assertThat(result.position(1)).isEqualTo(4);
    }

    @Test
    void testDictionaryLiteralCallIsPeeledAndRewrappedByEvaluator()
    {
        Variable literal = new Variable(0);
        Variable lessThan = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(literal, new Literal("m"), AllMask.ALL),
                        new Assignment(
                                lessThan,
                                new Call("lt_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(literal, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(lessThan, Stream.VALUES)));

        BinaryVector dictionary = new BinaryVector(3, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "android".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(1, "iphone".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(2, "pixel".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 0, 2, 1}, dictionary);

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator());

        Streams result = evaluator.evaluate(new Reference(lessThan, Stream.VALUES), Mask.all(5));
        assertThat(result.values()).isInstanceOf(DictionaryVector.class);

        DictionaryVector encoded = (DictionaryVector) result.values();
        assertThat(encoded.ids()).containsExactly(0, 1, 0, 2, 1);
        BooleanVector dictionaryValues = (BooleanVector) encoded.values();
        assertThat(dictionaryValues.values()).containsExactly(true, true, false);
    }

    @Test
    void testInUtf8DictionaryLiteralReferenceMaskUsesPrimitiveTrueMask()
    {
        Variable firstLiteral = new Variable(0);
        Variable secondLiteral = new Variable(1);
        Variable matches = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(firstLiteral, new Literal("apple"), AllMask.ALL),
                        new Assignment(secondLiteral, new Literal("samsung"), AllMask.ALL),
                        new Assignment(
                                matches,
                                new Call("in_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(firstLiteral, Stream.VALUES),
                                        new Reference(secondLiteral, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        BinaryVector dictionary = new BinaryVector(4, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "apple".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(1, "pixel".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(2, "samsung".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(3, "nokia".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 2, 3, 2}, dictionary);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, true, false});

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), values,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator());

        Mask result = evaluator.evaluateInPlace(new ReferenceMask(new Reference(matches, Stream.VALUES)), Mask.all(5));
        assertThat(result.selectedCount()).isEqualTo(3);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(2);
        assertThat(result.position(2)).isEqualTo(4);
    }

    @Test
    void testSubstringUtf8KeepsLiteralScalarPositionsSeparateFromDictionaryValuePositions()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable substring = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(start, new Literal(1L), AllMask.ALL),
                        new Assignment(length, new Literal(2L), AllMask.ALL),
                        new Assignment(
                                substring,
                                new Call("substring_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(substring, Stream.VALUES)));

        BinaryVector dictionary = new BinaryVector(4, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "70000".getBytes(UTF_8));
        dictionary.setBytes(1, "81111".getBytes(UTF_8));
        dictionary.setBytes(2, "92222".getBytes(UTF_8));
        dictionary.setBytes(3, "10333".getBytes(UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {3, 1, 0}, dictionary);

        PlanEvaluator evaluator = new PlanEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator());

        org.weakref.nitro.data.Vector result = evaluator.evaluate(new Reference(substring, Stream.VALUES), Mask.all(3)).get(Stream.VALUES);
        assertThat(decodeUtf8(result, 0)).isEqualTo("10");
        assertThat(decodeUtf8(result, 1)).isEqualTo("81");
        assertThat(decodeUtf8(result, 2)).isEqualTo("70");
    }

    private static PlanEvaluator.InputResolver inputResolver(Map<Reference, org.weakref.nitro.data.Vector> inputs)
    {
        return (reference, mask) -> inputs.get(reference);
    }

    private static String decodeUtf8(org.weakref.nitro.data.Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector binaryVector -> new String(binaryVector.copyBytes(position), UTF_8);
            case DictionaryVector dictionaryVector -> decodeUtf8(dictionaryVector.values(), dictionaryVector.ids()[position]);
            case RleVector rleVector -> decodeUtf8(rleVector.values(), rleVector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported utf8 vector type: " + vector.getClass().getSimpleName());
        };
    }

    private static PrimitiveRegistry builtinPrimitiveRegistry()
    {
        ScalarRegistry scalarRegistry = new ScalarRegistry();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register(scalarRegistry.register(AddI64.class));
        primitiveRegistry.register(scalarRegistry.register(DivideScaleRoundI64.class));
        primitiveRegistry.register(scalarRegistry.register(EqualI64.class));
        primitiveRegistry.register(scalarRegistry.register(InUtf8.class));
        primitiveRegistry.register(scalarRegistry.register(LessThanI64.class));
        primitiveRegistry.register(scalarRegistry.register(ScaledRelativeDifferenceGtI64.class));
        return primitiveRegistry;
    }

    private static MapVector createUtf8I64MapVector()
    {
        MapVector maps = new MapVector(4);
        maps.offsets()[0] = 0;
        maps.offsets()[1] = 2;
        maps.offsets()[2] = 3;
        maps.offsets()[3] = 3;
        maps.offsets()[4] = 5;

        BinaryVector keys = new BinaryVector(5, 26);
        keys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        keys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        keys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        keys.setBytes(1, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        keys.setBytes(2, "gamma".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        keys.setBytes(3, "delta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        keys.setBytes(4, "epsilon".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        I64Vector values = new I64Vector(new long[] {10, 20, 30, 40, 50});
        BooleanVector valueNulls = new BooleanVector(new boolean[] {false, false, true, false, false});
        maps.setEntries(Streams.ofValues(keys), Streams.ofValues(values).with(Stream.NULLS, valueNulls));
        return maps;
    }

    private static BinaryVector utf8Vector(String... values)
    {
        int totalBytes = 0;
        byte[][] encoded = new byte[values.length][];
        for (int index = 0; index < values.length; index++) {
            encoded[index] = values[index].getBytes(UTF_8);
            totalBytes += encoded[index].length;
        }

        BinaryVector vector = new BinaryVector(values.length, totalBytes);
        vector.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        vector.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        for (int index = 0; index < values.length; index++) {
            vector.setBytes(index, encoded[index]);
        }
        return vector;
    }

    private static String utf8(BinaryVector vector, int position)
    {
        return new String(vector.copyBytes(position), UTF_8);
    }
}
