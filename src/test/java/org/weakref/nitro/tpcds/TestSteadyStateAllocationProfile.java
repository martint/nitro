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
package org.weakref.nitro.tpcds;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestSteadyStateAllocationProfile
{
    @Test
    void testSeparatesHighWaterAndSteadyStateBatches()
    {
        SteadyStateAllocationProfile.Report report = SteadyStateAllocationProfile.measure(
                () -> new GeneratorOperator(new Allocator(), 10, 2, List.of(new SequenceGenerator(0))),
                2);

        assertThat(report.highWaterBatches()).isEqualTo(2);
        assertThat(report.steadyBatches()).isEqualTo(3);
        assertThat(report.highWaterRows()).isEqualTo(4);
        assertThat(report.steadyRows()).isEqualTo(6);
        assertThat(report.sink()).isEqualTo(20);
        assertThat(report.closed()).isTrue();
        assertThat(report.constructionBytes()).isNotNegative();
        assertThat(report.executionBytes()).isNotNegative();
    }
}
