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
import org.weakref.nitro.data.ValueDemand;
import org.weakref.nitro.operator.AuthoritativeHashChannel;
import org.weakref.nitro.operator.GroupingHashOutput;
import org.weakref.nitro.operator.PhysicalOrdering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Planner-authored physical aggregation layout.
 *
 * <p>Execution units are scheduled once per input batch. Logical outputs explicitly bind to a
 * result slot of a unit, allowing a provider-selected unit to share traversal and state across
 * results without any aggregate recognition in execution operators.
 */
public record PhysicalAggregationProgram(
        List<PhysicalAggregationUnit> units,
        List<Output> outputs,
        Schema outputSchema,
        Optional<AuthoritativeHashChannel> authoritativeHashChannel,
        Optional<GroupingHashOutput> groupingHashOutput,
        Optional<PhysicalOrdering> inputOrdering)
{
    public PhysicalAggregationProgram(List<PhysicalAggregationUnit> units, List<Output> outputs)
    {
        this(units, outputs, Schema.unspecified(outputs.size()), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public PhysicalAggregationProgram(List<PhysicalAggregationUnit> units, List<Output> outputs, Schema outputSchema)
    {
        this(units, outputs, outputSchema, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public PhysicalAggregationProgram(
            List<PhysicalAggregationUnit> units,
            List<Output> outputs,
            Schema outputSchema,
            Optional<AuthoritativeHashChannel> authoritativeHashChannel,
            Optional<GroupingHashOutput> groupingHashOutput)
    {
        this(units, outputs, outputSchema, authoritativeHashChannel, groupingHashOutput, Optional.empty());
    }

    public PhysicalAggregationProgram
    {
        units = List.copyOf(requireNonNull(units, "units is null"));
        outputs = List.copyOf(requireNonNull(outputs, "outputs is null"));
        outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        authoritativeHashChannel = requireNonNull(authoritativeHashChannel, "authoritativeHashChannel is null");
        groupingHashOutput = requireNonNull(groupingHashOutput, "groupingHashOutput is null");
        inputOrdering = requireNonNull(inputOrdering, "inputOrdering is null");
        if (authoritativeHashChannel.isPresent() && groupingHashOutput.isPresent()) {
            throw new IllegalArgumentException("Aggregation cannot consume and compute a grouping hash in the same program");
        }
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

    public static PhysicalAggregationProgram singleUnit(PhysicalAggregationUnit unit)
    {
        requireNonNull(unit, "unit is null");
        List<Output> outputs = new ArrayList<>(unit.outputCount());
        for (int result = 0; result < unit.outputCount(); result++) {
            outputs.add(new Output(0, result));
        }
        return new PhysicalAggregationProgram(List.of(unit), outputs);
    }

    /**
     * Returns an equivalent program whose intermediate results may use registry-provider physical streams.
     * Logical output types remain unchanged for planning and for any later explicit boundary adaptation.
     */
    public PhysicalAggregationProgram physicalIntermediateOutput()
    {
        return new PhysicalAggregationProgram(
                units.stream()
                        .map(PhysicalAggregationUnit::physicalIntermediateOutput)
                        .toList(),
                outputs,
                outputSchema,
                authoritativeHashChannel,
                groupingHashOutput,
                inputOrdering);
    }

    public PhysicalAggregationProgram withAuthoritativeHashChannel(AuthoritativeHashChannel authoritativeHashChannel)
    {
        return new PhysicalAggregationProgram(
                units,
                outputs,
                outputSchema,
                Optional.of(requireNonNull(authoritativeHashChannel, "authoritativeHashChannel is null")),
                Optional.empty(),
                inputOrdering);
    }

    public PhysicalAggregationProgram withGroupingHashOutput(GroupingHashOutput groupingHashOutput)
    {
        return new PhysicalAggregationProgram(
                units,
                outputs,
                outputSchema,
                Optional.empty(),
                Optional.of(requireNonNull(groupingHashOutput, "groupingHashOutput is null")),
                inputOrdering);
    }

    public PhysicalAggregationProgram withInputOrdering(PhysicalOrdering inputOrdering)
    {
        return new PhysicalAggregationProgram(
                units,
                outputs,
                outputSchema,
                authoritativeHashChannel,
                groupingHashOutput,
                Optional.of(requireNonNull(inputOrdering, "inputOrdering is null")));
    }

    /**
     * Whether any update reads an input value, conservatively treating units without generated
     * update metadata as value-reading.
     */
    public boolean readsInputValues()
    {
        for (PhysicalAggregationUnit unit : units) {
            if (!(unit instanceof GeneratedGroupedAggregationUnit generated) ||
                    generated.generatedGroupedUpdates().stream().anyMatch(update -> update.readsInput())) {
                return true;
            }
        }
        return false;
    }

    /** Merged physical-value demand for each input channel read by the program. */
    public Map<Integer, ValueDemand> inputValueDemands()
    {
        HashMap<Integer, ValueDemand> demands = new HashMap<>();
        for (PhysicalAggregationUnit unit : units) {
            unit.inputValueDemands().forEach((input, demand) -> demands.merge(input, demand, ValueDemand::merge));
        }
        inputOrdering.ifPresent(ordering -> ordering.keys().forEach(key -> demands.merge(key.column(), ValueDemand.FULL, ValueDemand::merge)));
        return Map.copyOf(demands);
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
