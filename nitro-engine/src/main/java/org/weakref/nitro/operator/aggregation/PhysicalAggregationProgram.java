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
package org.weakref.nitro.operator.aggregation;

import org.weakref.nitro.core.type.Schema;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Planner-authored physical aggregation layout.
 *
 * <p>Execution units are scheduled once per input batch. Logical outputs explicitly bind to a
 * result slot of a unit, allowing a provider-selected unit to share traversal and state across
 * results without any aggregate recognition in execution operators.
 */
public record PhysicalAggregationProgram(List<PhysicalAggregationUnit> units, List<Output> outputs, Schema outputSchema)
{
    public PhysicalAggregationProgram(List<PhysicalAggregationUnit> units, List<Output> outputs)
    {
        this(units, outputs, Schema.unspecified(outputs.size()));
    }

    public PhysicalAggregationProgram
    {
        units = List.copyOf(requireNonNull(units, "units is null"));
        outputs = List.copyOf(requireNonNull(outputs, "outputs is null"));
        outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        if (outputSchema.size() != outputs.size()) {
            throw new IllegalArgumentException("outputSchema size does not match outputs");
        }
        boolean[][] boundResults = new boolean[units.size()][];
        for (int unit = 0; unit < units.size(); unit++) {
            int outputCount = units.get(unit).outputCount();
            if (outputCount < 1) {
                throw new IllegalArgumentException("unit " + unit + " has no results");
            }
            boundResults[unit] = new boolean[outputCount];
        }
        for (int output = 0; output < outputs.size(); output++) {
            Output binding = outputs.get(output);
            if (binding.unit() >= units.size()) {
                throw new IllegalArgumentException("output " + output + " references missing unit " + binding.unit());
            }
            int unitOutputs = units.get(binding.unit()).outputCount();
            if (binding.result() >= unitOutputs) {
                throw new IllegalArgumentException("output " + output + " references missing result " + binding.result() + " of unit " + binding.unit());
            }
            if (boundResults[binding.unit()][binding.result()]) {
                throw new IllegalArgumentException("unit " + binding.unit() + " result " + binding.result() + " is bound more than once");
            }
            boundResults[binding.unit()][binding.result()] = true;
        }
    }

    public static PhysicalAggregationProgram independent(List<? extends Accumulator> accumulators)
    {
        requireNonNull(accumulators, "accumulators is null");
        List<PhysicalAggregationUnit> units = List.copyOf(accumulators);
        List<Output> outputs = new ArrayList<>(units.size());
        for (int unit = 0; unit < units.size(); unit++) {
            outputs.add(new Output(unit, 0));
        }
        return new PhysicalAggregationProgram(units, outputs);
    }

    public record Output(int unit, int result)
    {
        public Output
        {
            if (unit < 0) {
                throw new IllegalArgumentException("unit is negative");
            }
            if (result < 0) {
                throw new IllegalArgumentException("result is negative");
            }
        }
    }
}
