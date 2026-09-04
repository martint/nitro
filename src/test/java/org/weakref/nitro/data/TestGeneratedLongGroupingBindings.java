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
package org.weakref.nitro.data;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.BINARY_REGION;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.BOOLEAN;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.DOUBLE;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.LONG;

class TestGeneratedLongGroupingBindings
{
    @Test
    void testBindsFlatPhysicalCarriers()
    {
        GeneratedLongGroupingBindings bindings = new GeneratedLongGroupingBindings(3, true);
        I64Vector keys = new I64Vector(new long[] {7, 8, 7});
        I32Vector values = new I32Vector(new int[] {10, 20, 30});
        F64Vector doubles = new F64Vector(new double[] {1.5, 2.5, 3.5});
        BooleanVector booleans = new BooleanVector(new boolean[] {true, false, true});
        BooleanVector nulls = new BooleanVector(new boolean[] {false, true, false});

        assertThat(bindings.bindKey(keys, null)).isTrue();
        assertThat(bindings.bindInput(0, values, nulls, true, LONG)).isTrue();
        assertThat(bindings.bindInput(1, doubles, null, true, DOUBLE)).isTrue();
        assertThat(bindings.bindInput(2, booleans, null, true, BOOLEAN)).isTrue();
        bindings.finish();

        assertThat(bindings.keyValues()).isSameAs(keys.values());
        assertThat(bindings.keyIds()).isNull();
        assertThat(bindings.intKey()).isFalse();
        assertThat(bindings.inputs()[0]).isSameAs(values.values());
        assertThat(bindings.inputs()[1]).isSameAs(doubles.values());
        assertThat(bindings.inputs()[2]).isSameAs(booleans.values());
        assertThat(bindings.intInputs()).containsExactly(true, false, false);
        assertThat(bindings.inputCarriers()).containsExactly(LONG, DOUBLE, BOOLEAN);
        assertThat(bindings.inputNulls()[0]).isSameAs(nulls.values());
        assertThat(bindings.additionalGroupUpperBound(3)).isEqualTo(3);
    }

    @Test
    void testBindsSharedDictionaryMappingOnce()
    {
        GeneratedLongGroupingBindings bindings = new GeneratedLongGroupingBindings(1, true);
        int[] ids = {0, 0, 1, 1};
        DictionaryVector keys = DictionaryVector.wrap(ids, new I32Vector(new int[] {11, 22}));
        DictionaryVector values = DictionaryVector.wrap(ids, new I64Vector(new long[] {100, 200}));
        DictionaryVector nulls = DictionaryVector.wrap(ids, new BooleanVector(new boolean[] {false, true}));

        assertThat(bindings.bindKey(keys, null)).isTrue();
        assertThat(bindings.bindInput(0, values, nulls, true, LONG)).isTrue();
        bindings.finish();

        assertThat(bindings.keyIds()).isSameAs(ids);
        assertThat(bindings.intKey()).isTrue();
        assertThat(bindings.keyMapped()).isTrue();
        assertThat(bindings.inputUsesKeyIds()).containsExactly(true);
        assertThat(bindings.inputNullUsesKeyIds()).containsExactly(true);
        assertThat(bindings.additionalGroupUpperBound(4)).isEqualTo(2);
        assertThat(bindings.sampleKeyRuns(Mask.all(4))).isEqualTo((3L << 32) | 2);
    }

    @Test
    void testBindsBinaryRegionWithoutPerPositionCarrierObjects()
    {
        GeneratedLongGroupingBindings bindings = new GeneratedLongGroupingBindings(1, true);
        BinaryVector binary = new BinaryVector(3, new int[] {0, 2, 3, 6}, new byte[] {1, 2, 3, 4, 5, 6});
        DictionaryVector values = DictionaryVector.wrap(new int[] {2, 0, 2}, binary);

        assertThat(bindings.bindInput(0, values, null, true, BINARY_REGION)).isTrue();
        bindings.finish();

        assertThat(bindings.inputs()[0]).isSameAs(binary.data());
        assertThat(bindings.inputValueOffsets()[0]).isSameAs(binary.offsets());
        assertThat(bindings.inputIds()[0]).containsExactly(2, 0, 2);
        assertThat(bindings.inputCarriers()).containsExactly(BINARY_REGION);
    }

    @Test
    void testRejectsUnsupportedPhysicalCarrierWithoutLeakingPolicyToOperator()
    {
        GeneratedLongGroupingBindings bindings = new GeneratedLongGroupingBindings(1, false);
        DictionaryVector dictionary = DictionaryVector.wrap(
                new int[] {0, 1},
                new I64Vector(new long[] {10, 20}));

        assertThat(bindings.bindKey(dictionary, null)).isFalse();
        assertThat(bindings.bindKey(new F64Vector(new double[] {1, 2}), null)).isFalse();
        assertThat(bindings.bindKey(
                new I64Vector(new long[] {1, 2}),
                new BooleanVector(new boolean[] {false, true}))).isFalse();
        assertThat(bindings.bindInput(
                0,
                new F64Vector(new double[] {1, 2}),
                null,
                true,
                LONG)).isFalse();
    }

    @Test
    void testMatchesPhysicalShapeExactlyAcrossRebinding()
    {
        GeneratedLongGroupingBindings bindings = new GeneratedLongGroupingBindings(1, true);
        int[] ids = {0, 1};
        DictionaryVector keys = DictionaryVector.wrap(ids, new I64Vector(new long[] {11, 22}));
        DictionaryVector values = DictionaryVector.wrap(ids, new I64Vector(new long[] {7, 9}));

        assertThat(bindings.bindKey(keys, null)).isTrue();
        assertThat(bindings.bindInput(0, values, null, true, LONG)).isTrue();
        bindings.finish();
        GeneratedLongGroupingBindings.PhysicalShape shape = bindings.capturePhysicalShape();
        assertThat(bindings.matchesPhysicalShape(shape)).isTrue();

        assertThat(bindings.bindKey(new I32Vector(new int[] {11, 22}), null)).isTrue();
        assertThat(bindings.bindInput(0, new RegionVector(new I64Vector(new long[] {0, 7, 9}), 1, 2), null, true, LONG)).isTrue();
        bindings.finish();
        assertThat(bindings.matchesPhysicalShape(shape)).isFalse();

        assertThat(bindings.bindKey(keys, null)).isTrue();
        assertThat(bindings.bindInput(0, values, null, true, LONG)).isTrue();
        bindings.finish();
        assertThat(bindings.matchesPhysicalShape(shape)).isTrue();
    }
}
