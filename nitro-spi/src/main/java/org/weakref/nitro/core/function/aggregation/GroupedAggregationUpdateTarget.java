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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Exact provider-owned invocation targets for one generated grouped-state update.
 *
 * <p>The first argument is an opaque provider state, followed by the group id and one or more primitive
 * contributions.
 * Generated engine code links the handle as a constant, so the provider state class never appears symbolically in
 * the generated class. The optional repeated target consumes an exact logical multiplicity for encoded domains.
 */
public record GroupedAggregationUpdateTarget(MethodHandle update, Optional<MethodHandle> repeatedUpdate)
{
    public GroupedAggregationUpdateTarget
    {
        update = requireNonNull(update, "update is null");
        repeatedUpdate = requireNonNull(repeatedUpdate, "repeatedUpdate is null");
        validate(update, false);
        repeatedUpdate.ifPresent(target -> validate(target, true));
        Class<?> stateType = update.type().parameterType(0);
        List<Class<?>> contributionCarriers = contributionCarriers(update, false);
        repeatedUpdate.ifPresent(target -> {
            if (target.type().parameterType(0) != stateType) {
                throw new IllegalArgumentException("update and repeated update have different state types");
            }
            if (!contributionCarriers(target, true).equals(contributionCarriers)) {
                throw new IllegalArgumentException("update and repeated update have different contribution carriers");
            }
        });
    }

    public GroupedAggregationUpdateTarget(MethodHandle update)
    {
        this(update, Optional.empty());
    }

    public List<Class<?>> contributionCarriers()
    {
        return contributionCarriers(update, false);
    }

    public Class<?> contributionCarrier()
    {
        if (contributionCarriers().size() != 1) {
            throw new IllegalStateException("target has multiple contribution carriers");
        }
        return contributionCarriers().getFirst();
    }

    private static void validate(MethodHandle target, boolean repeated)
    {
        MethodType type = target.type();
        int minimumParameters = repeated ? 4 : 3;
        if (type.returnType() != void.class ||
                type.parameterCount() < minimumParameters ||
                type.parameterType(0).isPrimitive() ||
                type.parameterType(1) != int.class ||
                (repeated && type.lastParameterType() != int.class)) {
            throw new IllegalArgumentException("invalid grouped update target type: " + type);
        }
        int contributionLimit = type.parameterCount() - (repeated ? 1 : 0);
        for (int parameter = 2; parameter < contributionLimit; parameter++) {
            if (type.parameterType(parameter) != long.class &&
                    type.parameterType(parameter) != double.class &&
                    type.parameterType(parameter) != boolean.class) {
                throw new IllegalArgumentException("invalid grouped update target type: " + type);
            }
        }
    }

    private static List<Class<?>> contributionCarriers(MethodHandle target, boolean repeated)
    {
        MethodType type = target.type();
        return type.parameterList().subList(2, type.parameterCount() - (repeated ? 1 : 0));
    }
}
