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

class TestGeneratedLongGroupingBindings
{
    @Test
    void testBindsFlatPhysicalCarriers()
    {
        GeneratedLongGroupingBindings bindings = new GeneratedLongGroupingBindings(2, true);
        I64Vector keys = new I64Vector(new long[] {7, 8, 7});
        I32Vector values = new I32Vector(new int[] {10, 20, 30});
        BooleanVector nulls = new BooleanVector(new boolean[] {false, true, false});

        assertThat(bindings.bindKey(keys, null)).isTrue();
        assertThat(bindings.bindInput(0, values, nulls, true)).isTrue();
        bindings.clearInput(1);
        bindings.finish();

        assertThat(bindings.keyValues()).isSameAs(keys.values());
        assertThat(bindings.keyIds()).isNull();
        assertThat(bindings.intKey()).isFalse();
        assertThat(bindings.inputs()[0]).isSameAs(values.values());
        assertThat(bindings.intInputs()).containsExactly(true, false);
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
        assertThat(bindings.bindInput(0, values, nulls, true)).isTrue();
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
                true)).isFalse();
    }
}
