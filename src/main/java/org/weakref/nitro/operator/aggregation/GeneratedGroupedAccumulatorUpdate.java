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

import static java.util.Objects.requireNonNull;

/**
 * Describes the long contribution supplied to one accumulator's classloader-safe state-update SPI.
 * <p>
 * The kernel reads no function definition — it dispatches solely on this declarative spec — so the
 * accumulator set the generator can compile grows by accumulators implementing {@link GeneratedGroupedAccumulator},
 * not by changes to the generator.
 *
 * @param contribution the physical long contribution and optional input null check
 */
public record GeneratedGroupedAccumulatorUpdate(Contribution contribution)
{
    public sealed interface Contribution
            permits InputValue, Constant {}

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

    public GeneratedGroupedAccumulatorUpdate
    {
        requireNonNull(contribution, "contribution is null");
    }

    public static GeneratedGroupedAccumulatorUpdate inputValue(int inputColumn)
    {
        return new GeneratedGroupedAccumulatorUpdate(new InputValue(inputColumn));
    }

    public static GeneratedGroupedAccumulatorUpdate constant(long value)
    {
        return new GeneratedGroupedAccumulatorUpdate(new Constant(value, -1));
    }

    public static GeneratedGroupedAccumulatorUpdate constantWhenNotNull(long value, int inputColumn)
    {
        return new GeneratedGroupedAccumulatorUpdate(new Constant(value, inputColumn));
    }

    public int inputColumn()
    {
        return switch (contribution) {
            case InputValue input -> input.inputColumn();
            case Constant constant -> constant.nullCheckInputColumn();
        };
    }

    public boolean readsInput()
    {
        return inputColumn() >= 0;
    }

    public boolean readsValue()
    {
        return contribution instanceof InputValue;
    }

    public long constantValue()
    {
        return ((Constant) contribution).value();
    }
}
