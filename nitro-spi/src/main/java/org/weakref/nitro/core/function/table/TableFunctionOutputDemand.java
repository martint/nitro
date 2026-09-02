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

import org.weakref.nitro.data.ValueDemand;

import java.util.Map;
import java.util.Set;

/// Output content requested from a table-function invocation.
///
/// A proper output absent from [#properOutputs()] must not be computed. A pass-through argument
/// absent from [#passThroughArguments()] needs no row-reference column in the result.
public record TableFunctionOutputDemand(
        Map<Integer, ValueDemand> properOutputs,
        Set<Integer> passThroughArguments)
{
    public TableFunctionOutputDemand
    {
        properOutputs = Map.copyOf(properOutputs);
        passThroughArguments = Set.copyOf(passThroughArguments);
        if (properOutputs.keySet().stream().anyMatch(output -> output < 0)) {
            throw new IllegalArgumentException("proper output is negative");
        }
        if (passThroughArguments.stream().anyMatch(argument -> argument < 0)) {
            throw new IllegalArgumentException("pass-through argument is negative");
        }
    }
}
