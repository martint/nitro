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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.function.scalar.ScalarRegistry;
import org.weakref.nitro.function.scalar.builtin.AddI64;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.AndMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.IrNormalizer;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

        assertThat(values.utf8Value(0)).isEqualTo("alice");
        assertThat(values.utf8Value(3)).isEqualTo("carol");
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
        mapKeys.addTrait(BinaryVector.Trait.UTF8_STRING);
        mapKeys.addTrait(BinaryVector.Trait.ASCII_ONLY);
        mapKeys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(1, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(2, "gamma".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(3, "zeta".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        I64Vector mapValues = new I64Vector(new long[] {10, 0, 30, 0});
        BooleanVector mapValueNulls = new BooleanVector(new boolean[] {false, true, false, true});
        maps.setEntries(Streams.ofValues(mapKeys), Streams.ofValues(mapValues).with(Stream.NULLS, mapValueNulls));

        BinaryVector lookupKeys = new BinaryVector(4, 20);
        lookupKeys.addTrait(BinaryVector.Trait.UTF8_STRING);
        lookupKeys.addTrait(BinaryVector.Trait.ASCII_ONLY);
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
        mapKeys.addTrait(BinaryVector.Trait.UTF8_STRING);
        mapKeys.addTrait(BinaryVector.Trait.ASCII_ONLY);
        maps.setEntries(Streams.ofValues(mapKeys), Streams.ofValues(new I64Vector(new long[0])));

        BinaryVector lookupKeys = new BinaryVector(3, 3);
        lookupKeys.addTrait(BinaryVector.Trait.UTF8_STRING);
        lookupKeys.addTrait(BinaryVector.Trait.ASCII_ONLY);
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
        mapKeys.addTrait(BinaryVector.Trait.UTF8_STRING);
        mapKeys.addTrait(BinaryVector.Trait.ASCII_ONLY);
        mapKeys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(1, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(2, "gamma".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(3, "delta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        maps.setEntries(Streams.ofValues(mapKeys), Streams.ofValues(new I64Vector(new long[] {10, 20, 30, 40})));

        BinaryVector lookupKeys = new BinaryVector(4, 22);
        lookupKeys.addTrait(BinaryVector.Trait.UTF8_STRING);
        lookupKeys.addTrait(BinaryVector.Trait.ASCII_ONLY);
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
        assertThat(keys.utf8Value(0)).isEqualTo("gamma");
        assertThat(keys.utf8Value(1)).isEqualTo("delta");
        assertThat(keys.utf8Value(2)).isEqualTo("epsilon");
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
            throw new IllegalArgumentException("Unexpected input " + reference);
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
            throw new IllegalArgumentException("Unexpected input " + reference);
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

    private static PlanEvaluator.InputResolver inputResolver(Map<Reference, org.weakref.nitro.data.Vector> inputs)
    {
        return (reference, mask) -> {
            org.weakref.nitro.data.Vector vector = inputs.get(reference);
            if (vector == null) {
                throw new IllegalArgumentException("Unexpected input " + reference);
            }
            return vector;
        };
    }

    private static PrimitiveRegistry builtinPrimitiveRegistry()
    {
        ScalarRegistry scalarRegistry = new ScalarRegistry();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register(scalarRegistry.register(AddI64.class));
        primitiveRegistry.register(scalarRegistry.register(LessThanI64.class));
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
        keys.addTrait(BinaryVector.Trait.UTF8_STRING);
        keys.addTrait(BinaryVector.Trait.ASCII_ONLY);
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
}
