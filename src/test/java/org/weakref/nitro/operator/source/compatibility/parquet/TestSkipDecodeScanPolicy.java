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
package org.weakref.nitro.operator.source.compatibility.parquet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestSkipDecodeScanPolicy
{
    @Test
    void testDefaultsPreserveExistingScanChoices()
    {
        SkipDecodeScanPolicy policy = SkipDecodeScanPolicy.defaults();

        assertThat(policy.baseBatchSize()).isEqualTo(8_192);
        assertThat(policy.filteredBatchSize()).isEqualTo(12_288);
        assertThat(policy.scratchBatchSize()).isEqualTo(12_288);
        assertThat(policy.skipGuard()).isEqualTo(0.08);
        assertThat(policy.filterWarmupRows()).isEqualTo(256 * 1_024L);
        assertThat(policy.filterMinPruneRatio()).isEqualTo(0.30);
    }
}
