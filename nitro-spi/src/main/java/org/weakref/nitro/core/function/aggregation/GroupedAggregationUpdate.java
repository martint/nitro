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
 * Classloader-neutral description of one primitive contribution tuple to grouped aggregation state.
 * <p>
 * This is provider metadata, not an executable operator contract. A physical lowering step may
 * combine these descriptions and generate an engine-owned grouping kernel without recognizing a
 * function identity or provider implementation class.
 *
 * @param contributions ordered primitive contributions and optional input null checks
 */
public record GroupedAggregationUpdate(List<Contribution> contributions, GroupedAggregationUpdateTarget target)
{
    public sealed interface Contribution
            permits InputValue, Constant {}

    public record InputValue(int inputColumn, PrimitiveContributionCarrier carrier)
            implements Contribution
    {
        public InputValue
        {
            if (inputColumn < 0) {
                throw new IllegalArgumentException("inputColumn is negative");
            }
            carrier = requireNonNull(carrier, "carrier is null");
        }

        public InputValue(int inputColumn)
        {
            this(inputColumn, PrimitiveContributionCarrier.LONG);
        }
    }

    public record Constant(long value, int nullCheckInputColumn)
            implements Contribution
    {
        public Constant
        {
            if (nullCheckInputColumn < -1) {
                throw new IllegalArgumentException("nullCheckInputColumn is less than -1");
            }
        }
    }

    public GroupedAggregationUpdate
    {
        contributions = List.copyOf(requireNonNull(contributions, "contributions is null"));
        if (contributions.isEmpty()) {
            throw new IllegalArgumentException("contributions is empty");
        }
        requireNonNull(target, "target is null");
        if (target.contributionCarriers().size() != contributions.size()) {
            throw new IllegalArgumentException("target accepts %s contributions but update declares %s"
                    .formatted(target.contributionCarriers().size(), contributions.size()));
        }
        for (int index = 0; index < contributions.size(); index++) {
            Class<?> expectedCarrier = carrier(contributions.get(index)).javaType();
            Class<?> actualCarrier = target.contributionCarriers().get(index);
            if (actualCarrier != expectedCarrier) {
                throw new IllegalArgumentException("contribution %s requires %s but target accepts %s"
                        .formatted(index, expectedCarrier, actualCarrier));
            }
        }
    }

    public GroupedAggregationUpdate(Contribution contribution, GroupedAggregationUpdateTarget target)
    {
        this(List.of(requireNonNull(contribution, "contribution is null")), target);
    }

    public Contribution contribution()
    {
        if (contributions.size() != 1) {
            throw new IllegalStateException("update has multiple contributions");
        }
        return contributions.getFirst();
    }

    public static GroupedAggregationUpdate inputs(List<Contribution> contributions, GroupedAggregationUpdateTarget target)
    {
        return new GroupedAggregationUpdate(contributions, target);
    }

    public static GroupedAggregationUpdate inputValue(int inputColumn, GroupedAggregationUpdateTarget target)
    {
        return inputValue(inputColumn, PrimitiveContributionCarrier.LONG, target);
    }

    public static GroupedAggregationUpdate doubleInputValue(int inputColumn, GroupedAggregationUpdateTarget target)
    {
        return inputValue(inputColumn, PrimitiveContributionCarrier.DOUBLE, target);
    }

    public static GroupedAggregationUpdate booleanInputValue(int inputColumn, GroupedAggregationUpdateTarget target)
    {
        return inputValue(inputColumn, PrimitiveContributionCarrier.BOOLEAN, target);
    }

    public static GroupedAggregationUpdate inputValue(
            int inputColumn,
            PrimitiveContributionCarrier carrier,
            GroupedAggregationUpdateTarget target)
    {
        return new GroupedAggregationUpdate(new InputValue(inputColumn, carrier), target);
    }

    public static GroupedAggregationUpdate constant(long value, GroupedAggregationUpdateTarget target)
    {
        return new GroupedAggregationUpdate(new Constant(value, -1), target);
    }

    public static GroupedAggregationUpdate constantWhenNotNull(long value, int inputColumn, GroupedAggregationUpdateTarget target)
    {
        return new GroupedAggregationUpdate(new Constant(value, inputColumn), target);
    }

    public int inputColumn()
    {
        return inputColumn(0);
    }

    public int inputColumn(int contribution)
    {
        return switch (contributions.get(contribution)) {
            case InputValue input -> input.inputColumn();
            case Constant constant -> constant.nullCheckInputColumn();
        };
    }

    public boolean readsInput()
    {
        return java.util.stream.IntStream.range(0, contributions.size()).anyMatch(this::readsInput);
    }

    public boolean readsInput(int contribution)
    {
        return inputColumn(contribution) >= 0;
    }

    public boolean readsValue()
    {
        return java.util.stream.IntStream.range(0, contributions.size()).anyMatch(this::readsValue);
    }

    public boolean readsValue(int contribution)
    {
        Contribution value = contributions.get(contribution);
        return value instanceof InputValue;
    }

    public boolean readsDoubleValue()
    {
        return contributions.stream().anyMatch(contribution -> contribution instanceof InputValue input && input.carrier() == PrimitiveContributionCarrier.DOUBLE);
    }

    public boolean readsDoubleValue(int contribution)
    {
        return carrier(contribution) == PrimitiveContributionCarrier.DOUBLE;
    }

    public PrimitiveContributionCarrier carrier(int contribution)
    {
        return carrier(contributions.get(contribution));
    }

    private static PrimitiveContributionCarrier carrier(Contribution contribution)
    {
        return switch (contribution) {
            case InputValue input -> input.carrier();
            case Constant _ -> PrimitiveContributionCarrier.LONG;
        };
    }

    public long constantValue()
    {
        return constantValue(0);
    }

    public long constantValue(int contribution)
    {
        return ((Constant) contributions.get(contribution)).value();
    }
}
