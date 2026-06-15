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
 * @param valueColumn the input column whose value is the per-row increment amount, or {@code -1} to
 *        increment by a constant {@code 1} (e.g. {@code count(*)}). When non-negative the kernel only
 *        runs if that column is a null-free {@code I64Vector}.
 */
public record FusedAccumulatorSpec(Class<?> stateVectorType, int valueColumn)
{
    public boolean readsValue()
    {
        return valueColumn >= 0;
    }
}
