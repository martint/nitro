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
package org.weakref.nitro.core.function.table;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.ValueDemand;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.data.ValueDemand.FULL;

final class TestTableFunctionContracts
{
    @Test
    void testCopiesOutputDemand()
    {
        Map<Integer, ValueDemand> outputs = new HashMap<>();
        Set<Integer> passThrough = new HashSet<>();
        outputs.put(1, FULL);
        passThrough.add(2);

        TableFunctionOutputDemand demand = new TableFunctionOutputDemand(outputs, passThrough);
        outputs.clear();
        passThrough.clear();

        assertThat(demand.properOutputs()).containsOnlyKeys(1);
        assertThat(demand.passThroughArguments()).containsExactly(2);
    }

    @Test
    void testRejectsProgressWithoutConsumption()
    {
        assertThatThrownBy(() -> new TableFunctionProgress.Consumed(Set.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("no arguments were consumed");
    }

    @Test
    void testValidatesPassThroughArgument()
    {
        assertThatThrownBy(() -> new TableFunctionOutputBatch.PassThroughReference(-1, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("argument is negative");
    }
}
