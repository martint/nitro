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
package org.weakref.nitro.core.function.aggregation;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Provider-authored grouped-update tuple in function-argument coordinates.
 */
public record GroupedAggregationUpdateTemplate(List<Contribution> contributions, GroupedAggregationUpdateTarget target)
{
    public sealed interface Contribution
            permits InputValue, DoubleInputValue, Constant {}

    public record InputValue(int argument)
            implements Contribution
    {
        public InputValue
        {
            requireArgument(argument);
        }
    }

    public record DoubleInputValue(int argument)
            implements Contribution
    {
        public DoubleInputValue
        {
            requireArgument(argument);
        }
    }

    public record Constant(long value, int nullCheckArgument)
            implements Contribution
    {
        public Constant
        {
            if (nullCheckArgument < -1) {
                throw new IllegalArgumentException("nullCheckArgument is less than -1");
            }
        }
    }

    public GroupedAggregationUpdateTemplate
    {
        contributions = List.copyOf(requireNonNull(contributions, "contributions is null"));
        if (contributions.isEmpty()) {
            throw new IllegalArgumentException("contributions is empty");
        }
        target = requireNonNull(target, "target is null");
        if (target.contributionCarriers().size() != contributions.size()) {
            throw new IllegalArgumentException("target accepts %s contributions but template declares %s"
                    .formatted(target.contributionCarriers().size(), contributions.size()));
        }
        for (int index = 0; index < contributions.size(); index++) {
            Class<?> expectedCarrier = contributions.get(index) instanceof DoubleInputValue ? double.class : long.class;
            Class<?> actualCarrier = target.contributionCarriers().get(index);
            if (actualCarrier != expectedCarrier) {
                throw new IllegalArgumentException("contribution %s requires %s but target accepts %s"
                        .formatted(index, expectedCarrier, actualCarrier));
            }
        }
    }

    public GroupedAggregationUpdateTemplate(Contribution contribution, GroupedAggregationUpdateTarget target)
    {
        this(List.of(requireNonNull(contribution, "contribution is null")), target);
    }

    public static GroupedAggregationUpdateTemplate inputs(List<Contribution> contributions, GroupedAggregationUpdateTarget target)
    {
        return new GroupedAggregationUpdateTemplate(contributions, target);
    }

    public static GroupedAggregationUpdateTemplate inputValue(int argument, GroupedAggregationUpdateTarget target)
    {
        return new GroupedAggregationUpdateTemplate(new InputValue(argument), target);
    }

    public static GroupedAggregationUpdateTemplate doubleInputValue(int argument, GroupedAggregationUpdateTarget target)
    {
        return new GroupedAggregationUpdateTemplate(new DoubleInputValue(argument), target);
    }

    public static GroupedAggregationUpdateTemplate constant(long value, GroupedAggregationUpdateTarget target)
    {
        return new GroupedAggregationUpdateTemplate(new Constant(value, -1), target);
    }

    public static GroupedAggregationUpdateTemplate constantWhenNotNull(long value, int argument, GroupedAggregationUpdateTarget target)
    {
        requireArgument(argument);
        return new GroupedAggregationUpdateTemplate(new Constant(value, argument), target);
    }

    public GroupedAggregationUpdate bind(List<AggregationArgumentBinding> arguments)
    {
        requireNonNull(arguments, "arguments is null");
        return GroupedAggregationUpdate.inputs(
                contributions.stream()
                        .map(contribution -> bind(contribution, arguments))
                        .toList(),
                target);
    }

    private static GroupedAggregationUpdate.Contribution bind(Contribution contribution, List<AggregationArgumentBinding> arguments)
    {
        return switch (contribution) {
            case InputValue input -> new GroupedAggregationUpdate.InputValue(inputColumn(arguments, input.argument()));
            case DoubleInputValue input -> new GroupedAggregationUpdate.DoubleInputValue(inputColumn(arguments, input.argument()));
            case Constant constant when constant.nullCheckArgument() >= 0 ->
                    new GroupedAggregationUpdate.Constant(constant.value(), inputColumn(arguments, constant.nullCheckArgument()));
            case Constant constant -> new GroupedAggregationUpdate.Constant(constant.value(), -1);
        };
    }

    private static int inputColumn(List<AggregationArgumentBinding> arguments, int argument)
    {
        requireArgument(argument);
        if (argument >= arguments.size()) {
            throw new IllegalArgumentException("missing argument " + argument);
        }
        AggregationArgumentBinding binding = arguments.get(argument);
        if (binding.argument().kind() != AggregationArgument.Kind.INPUT) {
            throw new IllegalArgumentException("argument " + argument + " is not a direct input");
        }
        return binding.inputColumn();
    }

    private static void requireArgument(int argument)
    {
        if (argument < 0) {
            throw new IllegalArgumentException("argument is negative");
        }
    }
}
