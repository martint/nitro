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

import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdateTarget;
import org.weakref.nitro.data.Streams;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;

/** Test fixture for the legacy accumulator API; production providers use registry-bound aggregation units. */
interface GeneratedGroupedAccumulator
        extends Accumulator, GeneratedGroupedAggregationUnit
{
    GroupedAggregationUpdate generatedGroupedUpdate();

    @Override
    default List<GroupedAggregationUpdate> generatedGroupedUpdates()
    {
        return List.of(generatedGroupedUpdate());
    }

    @Override
    default void bindGeneratedGroupedState(Object state, Object[] targets, int offset)
    {
        targets[offset] = ((Streams) state).values();
    }

    static GroupedAggregationUpdateTarget longUpdateTarget(Class<?> stateType)
    {
        try {
            return new GroupedAggregationUpdateTarget(MethodHandles.publicLookup().findVirtual(
                    stateType,
                    "update",
                    MethodType.methodType(void.class, int.class, long.class)));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
