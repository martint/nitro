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

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class TestValueIdInterner
{
    private static final FlatKeyTablePolicy.ValueIds DEFAULT_POLICY = FlatKeyTablePolicy.ValueIds.defaults();

    private static byte[] bytes(String value)
    {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static int intern(ValueIdInterner interner, String value)
    {
        byte[] data = bytes(value);
        return interner.intern(data, 0, data.length);
    }

    @Test
    void assignsDenseIdsInFirstSeenOrder()
    {
        ValueIdInterner interner = new ValueIdInterner(1000, DEFAULT_POLICY);
        assertThat(intern(interner, "A")).isEqualTo(0);
        assertThat(intern(interner, "N")).isEqualTo(1);
        assertThat(intern(interner, "R")).isEqualTo(2);
        assertThat(interner.distinctCount()).isEqualTo(3);
    }

    @Test
    void sameValueSameIdAcrossRepeatedCalls()
    {
        ValueIdInterner interner = new ValueIdInterner(1000, DEFAULT_POLICY);
        int a = intern(interner, "alpha");
        int b = intern(interner, "beta");
        // Re-intern in a different order, as a later batch would: ids must be stable.
        assertThat(intern(interner, "beta")).isEqualTo(b);
        assertThat(intern(interner, "alpha")).isEqualTo(a);
        assertThat(a).isNotEqualTo(b);
        assertThat(interner.distinctCount()).isEqualTo(2);
    }

    @Test
    void reconstructsTheOriginalValue()
    {
        ValueIdInterner interner = new ValueIdInterner(1000, DEFAULT_POLICY);
        int id = intern(interner, "charlie");
        assertThat(interner.value(id)).isEqualTo(bytes("charlie"));
        assertThat(interner.valueLength(id)).isEqualTo(7);
        byte[] out = new byte[16];
        int length = interner.copyValue(id, out, 3);
        assertThat(length).isEqualTo(7);
        assertThat(new String(out, 3, 7, StandardCharsets.UTF_8)).isEqualTo("charlie");
    }

    @Test
    void internsBySubrange()
    {
        ValueIdInterner interner = new ValueIdInterner(1000, DEFAULT_POLICY);
        byte[] data = bytes("xxhelloyy");
        int hello = interner.intern(data, 2, 5);
        byte[] other = bytes("hello");
        // The same value reached through a different array/offset must collide to the same id.
        assertThat(interner.intern(other, 0, 5)).isEqualTo(hello);
        assertThat(interner.value(hello)).isEqualTo(bytes("hello"));
        assertThat(interner.groupingHash(hello)).isEqualTo(OperatorVectorSupport.binaryHash(data, 2, 5));
        assertThat(interner.groupingHash(hello)).isEqualTo(OperatorVectorSupport.binaryHash(other, 0, 5));
    }

    @Test
    void acceptsPrecomputedGroupingHashWithoutWeakeningEquality()
    {
        ValueIdInterner interner = new ValueIdInterner(1000, DEFAULT_POLICY);
        byte[] alpha = bytes("collision-value-5664");
        byte[] beta = bytes("collision-value-42684");
        int sharedHash = OperatorVectorSupport.binaryHash(alpha);
        assertThat(OperatorVectorSupport.binaryHash(beta)).isEqualTo(sharedHash);

        int alphaId = interner.intern(alpha, 0, alpha.length, sharedHash);
        int betaId = interner.intern(beta, 0, beta.length, sharedHash);

        assertThat(betaId).isNotEqualTo(alphaId);
        assertThat(interner.intern(alpha, 0, alpha.length, sharedHash)).isEqualTo(alphaId);
        assertThat(interner.groupingHash(alphaId)).isEqualTo(sharedHash);
        assertThat(interner.groupingHash(betaId)).isEqualTo(sharedHash);

        byte[] gamma = bytes("gamma");
        int gammaHash = OperatorVectorSupport.binaryHash(gamma);
        int gammaId = interner.intern(gamma, 0, gamma.length);
        assertThat(interner.intern(gamma, 0, gamma.length, gammaHash)).isEqualTo(gammaId);
    }

    @Test
    void preservesCollidingValuesThroughGrowthAndOverflow()
    {
        ValueIdInterner interner = new ValueIdInterner(1024, DEFAULT_POLICY);
        byte[] first = bytes("collision-value-5664");
        byte[] second = bytes("collision-value-42684");
        int hash = OperatorVectorSupport.binaryHash(first);
        assertThat(OperatorVectorSupport.binaryHash(second)).isEqualTo(hash);
        assertThat(interner.intern(first, 0, first.length, hash)).isZero();
        assertThat(interner.intern(second, 0, second.length, hash)).isEqualTo(1);

        for (int index = 2; index < 1024; index++) {
            assertThat(intern(interner, "growth-value-" + index)).isEqualTo(index);
        }
        assertThat(interner.intern(first, 0, first.length, hash)).isZero();
        assertThat(interner.intern(second, 0, second.length, hash)).isEqualTo(1);
        assertThat(intern(interner, "overflow")).isEqualTo(ValueIdInterner.TOO_MANY);
        assertThat(interner.find(second, 0, second.length)).isEqualTo(1);
        assertThat(interner.find(first, 0, first.length)).isZero();
        assertThat(interner.value(0)).isEqualTo(first);
        assertThat(interner.value(1)).isEqualTo(second);
    }

    @Test
    void distinguishesPrefixesAndDifferentLengths()
    {
        ValueIdInterner interner = new ValueIdInterner(1000, DEFAULT_POLICY);
        int ab = intern(interner, "ab");
        int abc = intern(interner, "abc");
        int empty = intern(interner, "");
        assertThat(ab).isNotEqualTo(abc);
        assertThat(empty).isNotEqualTo(ab);
        assertThat(interner.distinctCount()).isEqualTo(3);
        assertThat(interner.value(empty)).isEmpty();
    }

    @Test
    void overflowsPastTheCeiling()
    {
        ValueIdInterner interner = new ValueIdInterner(3, DEFAULT_POLICY);
        assertThat(intern(interner, "")).isEqualTo(0);
        assertThat(intern(interner, "a")).isEqualTo(1);
        assertThat(intern(interner, "b")).isEqualTo(2);
        assertThat(interner.overflowed()).isFalse();
        // The fourth distinct value exceeds the ceiling.
        assertThat(intern(interner, "c")).isEqualTo(ValueIdInterner.TOO_MANY);
        assertThat(interner.overflowed()).isTrue();
        // The ceiling blocks new ids without another table probe, but the common empty value remains recognizable.
        assertThat(intern(interner, "")).isZero();
        assertThat(intern(interner, "a")).isEqualTo(ValueIdInterner.TOO_MANY);
        byte[] existing = bytes("a");
        byte[] missing = bytes("missing");
        assertThat(interner.find(existing, 0, existing.length)).isEqualTo(1);
        assertThat(interner.find(missing, 0, missing.length)).isEqualTo(ValueIdInterner.TOO_MANY);
    }

    @Test
    void canDisableEmptyRecognitionAfterOverflow()
    {
        ValueIdInterner interner = new ValueIdInterner(1, new FlatKeyTablePolicy.ValueIds(false));
        assertThat(intern(interner, "")).isZero();
        assertThat(intern(interner, "overflow")).isEqualTo(ValueIdInterner.TOO_MANY);

        assertThat(intern(interner, "")).isEqualTo(ValueIdInterner.TOO_MANY);
        byte[] empty = bytes("");
        assertThat(interner.find(empty, 0, empty.length)).isZero();
    }

    @Test
    void staysCorrectAcrossManyDistinctValuesWithResizes()
    {
        ValueIdInterner interner = new ValueIdInterner(100_000, DEFAULT_POLICY);
        int count = 50_000;
        for (int index = 0; index < count; index++) {
            assertThat(intern(interner, "value-" + index)).isEqualTo(index);
        }
        assertThat(interner.distinctCount()).isEqualTo(count);
        // Re-intern a sample in scrambled order; ids must be unchanged after the resizes.
        assertThat(intern(interner, "value-0")).isEqualTo(0);
        assertThat(intern(interner, "value-49999")).isEqualTo(49_999);
        assertThat(intern(interner, "value-12345")).isEqualTo(12_345);
        assertThat(interner.value(31_337)).isEqualTo(bytes("value-31337"));
    }
}
