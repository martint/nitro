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
package org.weakref.nitro.operator;

import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdateTarget;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;

final class FusedAggregationUpdateBootstrap
{
    private FusedAggregationUpdateBootstrap() {}

    static CallSite bootstrap(MethodHandles.Lookup lookup, String name, MethodType type)
            throws IllegalAccessException
    {
        List<?> targets = MethodHandles.classData(lookup, "_", List.class);
        boolean repeated = name.startsWith("repeated");
        String prefix = repeated ? "repeated" : "update";
        int index = Integer.parseInt(name, prefix.length(), name.length(), 10);
        GroupedAggregationUpdateTarget descriptor = (GroupedAggregationUpdateTarget) targets.get(index);
        MethodHandle target = repeated
                ? descriptor.repeatedUpdate().orElseThrow(() -> new IllegalArgumentException("missing repeated update target"))
                : descriptor.update();
        return new ConstantCallSite(target.asType(type));
    }
}
