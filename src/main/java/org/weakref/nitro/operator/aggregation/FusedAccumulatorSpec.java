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

/**
 * Describes how the fused single-long-key grouped-aggregation kernel updates one accumulator's
 * per-group state with a single {@code stateVector.increment(group, amount)} call per row.
 * <p>
 * The kernel reads no function definition — it dispatches solely on this declarative spec — so the
 * accumulator set the generator can fuse grows by accumulators implementing {@link FusedAggregator},
 * not by changes to the generator.
 *
 * @param stateVectorType the concrete state vector class; must expose {@code increment(int, long)}
 *        (e.g. {@code SumStateVector}, {@code CountStateVector})
 * @param inputColumn the input column read by this accumulator, or {@code -1} when it reads no input
 *        (for example {@code count(*)}). An input may carry a flat null stream; the generated kernel
 *        skips that accumulator, but not the row, when it is null.
 * @param update whether a non-null row contributes its input value or the constant {@code 1}
 */
public record FusedAccumulatorSpec(Class<?> stateVectorType, int inputColumn, Update update)
{
    public enum Update
    {
        INPUT_VALUE,
        CONSTANT_ONE
    }

    public FusedAccumulatorSpec(Class<?> stateVectorType, int inputColumn)
    {
        this(stateVectorType, inputColumn, inputColumn < 0 ? Update.CONSTANT_ONE : Update.INPUT_VALUE);
    }

    public static FusedAccumulatorSpec countNonNull(Class<?> stateVectorType, int inputColumn)
    {
        if (inputColumn < 0) {
            throw new IllegalArgumentException("inputColumn is negative");
        }
        return new FusedAccumulatorSpec(stateVectorType, inputColumn, Update.CONSTANT_ONE);
    }

    public boolean readsInput()
    {
        return inputColumn >= 0;
    }

    public boolean readsValue()
    {
        return update == Update.INPUT_VALUE;
    }
}
