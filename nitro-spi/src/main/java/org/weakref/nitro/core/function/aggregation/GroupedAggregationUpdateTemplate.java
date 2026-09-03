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
            permits InputValue, Constant {}

    public record InputValue(int argument, List<String> fieldPath, ContributionCarrier carrier)
            implements Contribution
    {
        public InputValue
        {
            requireArgument(argument);
            fieldPath = copyFieldPath(fieldPath);
            carrier = requireNonNull(carrier, "carrier is null");
        }

        public InputValue(int argument)
        {
            this(argument, List.of(), ContributionCarrier.LONG);
        }

        public InputValue(int argument, ContributionCarrier carrier)
        {
            this(argument, List.of(), carrier);
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
        List<Class<?>> expectedParameters = contributions.stream()
                .flatMap(contribution -> carrier(contribution).parameterTypes().stream())
                .toList();
        if (!target.contributionParameterTypes().equals(expectedParameters)) {
            throw new IllegalArgumentException("contributions require %s but target accepts %s"
                    .formatted(expectedParameters, target.contributionParameterTypes()));
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
        return inputValue(argument, ContributionCarrier.LONG, target);
    }

    public static GroupedAggregationUpdateTemplate doubleInputValue(int argument, GroupedAggregationUpdateTarget target)
    {
        return inputValue(argument, ContributionCarrier.DOUBLE, target);
    }

    public static GroupedAggregationUpdateTemplate booleanInputValue(int argument, GroupedAggregationUpdateTarget target)
    {
        return inputValue(argument, ContributionCarrier.BOOLEAN, target);
    }

    public static GroupedAggregationUpdateTemplate inputValue(
            int argument,
            ContributionCarrier carrier,
            GroupedAggregationUpdateTarget target)
    {
        return new GroupedAggregationUpdateTemplate(new InputValue(argument, carrier), target);
    }

    public static InputValue inputField(int argument, String field, ContributionCarrier carrier)
    {
        return new InputValue(argument, List.of(requireNonNull(field, "field is null")), carrier);
    }

    public static InputValue inputPath(int argument, List<String> fieldPath, ContributionCarrier carrier)
    {
        return new InputValue(argument, fieldPath, carrier);
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
            case InputValue input -> new GroupedAggregationUpdate.InputValue(
                    inputColumn(arguments, input.argument()),
                    input.fieldPath(),
                    input.carrier());
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

    private static List<String> copyFieldPath(List<String> fieldPath)
    {
        fieldPath = List.copyOf(requireNonNull(fieldPath, "fieldPath is null"));
        if (fieldPath.stream().anyMatch(field -> field == null || field.isEmpty())) {
            throw new IllegalArgumentException("fieldPath contains a null or empty field");
        }
        return fieldPath;
    }

    private static ContributionCarrier carrier(Contribution contribution)
    {
        return switch (contribution) {
            case InputValue input -> input.carrier();
            case Constant _ -> ContributionCarrier.LONG;
        };
    }
}
