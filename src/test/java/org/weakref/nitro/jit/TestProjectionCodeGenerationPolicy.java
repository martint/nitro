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
package org.weakref.nitro.jit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestProjectionCodeGenerationPolicy
{
    @Test
    void defaultsPreserveExistingBehavior()
    {
        ProjectionCodeGenerationPolicy policy = ProjectionCodeGenerationPolicy.defaults();

        assertThat(policy.pooledDictionaryScratch()).isTrue();
        assertThat(policy.mappedDictionaryDoubleInputs()).isTrue();
        assertThat(policy.returnedConstantComparisonMasks()).isTrue();
        assertThat(policy.dictionaryEqualityMinimumReuse()).isEqualTo(4);
        assertThat(policy.fusedDictionaryDomainMinimumReduction()).isEqualTo(4);
    }
}
