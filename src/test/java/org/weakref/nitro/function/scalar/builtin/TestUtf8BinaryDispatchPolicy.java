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
package org.weakref.nitro.function.scalar.builtin;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestUtf8BinaryDispatchPolicy
{
    @Test
    void testDefaultsPreserveAllExistingSpecializations()
    {
        Utf8BinaryDispatchPolicy policy = Utf8BinaryDispatchPolicy.defaults();

        assertThat(policy.monomorphicDictionaryMask()).isTrue();
        assertThat(policy.directSingleDictionaryMatch()).isTrue();
        assertThat(policy.wordEquals()).isTrue();
        assertThat(policy.flattenedDictionaryEquals()).isTrue();
        assertThat(policy.flatSingleValueEqualsMask()).isTrue();
        assertThat(policy.directDictionaryPath()).isTrue();
        assertThat(policy.sparseDictionaryContains()).isTrue();
    }

    @Test
    void testEqualityMaskCompletesDerivedInputBundles()
    {
        assertThat(new EqualUtf8(Utf8BinaryDispatchPolicy.defaults())
                .requiresCompletedInputCompanionStreamsForMask()).isTrue();
    }
}
