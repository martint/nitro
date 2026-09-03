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

import static java.util.Objects.requireNonNull;

/**
 * Classloader-neutral description of one long contribution to grouped aggregation state.
 * <p>
 * This is provider metadata, not an executable operator contract. A physical lowering step may
 * combine these descriptions and generate an engine-owned grouping kernel without recognizing a
 * function identity or provider implementation class.
 *
 * @param contribution the physical long contribution and optional input null check
 */
public record GroupedAggregationUpdate(Contribution contribution, GroupedAggregationUpdateTarget target)
{
    public sealed interface Contribution
            permits InputValue, DoubleInputValue, Constant {}

    public record InputValue(int inputColumn)
            implements Contribution
    {
        public InputValue
        {
            if (inputColumn < 0) {
                throw new IllegalArgumentException("inputColumn is negative");
            }
        }
    }

    public record DoubleInputValue(int inputColumn)
            implements Contribution
    {
        public DoubleInputValue
        {
            if (inputColumn < 0) {
                throw new IllegalArgumentException("inputColumn is negative");
            }
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
        requireNonNull(contribution, "contribution is null");
        requireNonNull(target, "target is null");
        Class<?> expectedCarrier = contribution instanceof DoubleInputValue ? double.class : long.class;
        if (target.contributionCarrier() != expectedCarrier) {
            throw new IllegalArgumentException("contribution requires %s but target accepts %s"
                    .formatted(expectedCarrier, target.contributionCarrier()));
        }
    }

    public static GroupedAggregationUpdate inputValue(int inputColumn, GroupedAggregationUpdateTarget target)
    {
        return new GroupedAggregationUpdate(new InputValue(inputColumn), target);
    }

    public static GroupedAggregationUpdate doubleInputValue(int inputColumn, GroupedAggregationUpdateTarget target)
    {
        return new GroupedAggregationUpdate(new DoubleInputValue(inputColumn), target);
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
        return switch (contribution) {
            case InputValue input -> input.inputColumn();
            case DoubleInputValue input -> input.inputColumn();
            case Constant constant -> constant.nullCheckInputColumn();
        };
    }

    public boolean readsInput()
    {
        return inputColumn() >= 0;
    }

    public boolean readsValue()
    {
        return contribution instanceof InputValue || contribution instanceof DoubleInputValue;
    }

    public boolean readsDoubleValue()
    {
        return contribution instanceof DoubleInputValue;
    }

    public long constantValue()
    {
        return ((Constant) contribution).value();
    }
}
