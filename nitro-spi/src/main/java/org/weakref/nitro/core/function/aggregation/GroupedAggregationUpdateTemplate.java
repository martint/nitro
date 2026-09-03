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
 * Provider-authored grouped-update description in function-argument coordinates.
 */
public record GroupedAggregationUpdateTemplate(Contribution contribution, GroupedAggregationUpdateTarget target)
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
        contribution = requireNonNull(contribution, "contribution is null");
        target = requireNonNull(target, "target is null");
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
        return switch (contribution) {
            case InputValue input -> GroupedAggregationUpdate.inputValue(inputColumn(arguments, input.argument()), target);
            case DoubleInputValue input -> GroupedAggregationUpdate.doubleInputValue(inputColumn(arguments, input.argument()), target);
            case Constant constant when constant.nullCheckArgument() >= 0 ->
                    GroupedAggregationUpdate.constantWhenNotNull(constant.value(), inputColumn(arguments, constant.nullCheckArgument()), target);
            case Constant constant -> GroupedAggregationUpdate.constant(constant.value(), target);
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
