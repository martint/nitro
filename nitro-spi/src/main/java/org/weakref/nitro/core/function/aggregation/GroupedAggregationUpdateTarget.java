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
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Exact provider-owned invocation targets for one generated grouped-state update.
 *
 * <p>The first argument is an opaque provider state, followed by the group id and one primitive contribution.
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
        Class<?> contributionCarrier = update.type().parameterType(2);
        repeatedUpdate.ifPresent(target -> {
            if (target.type().parameterType(0) != stateType) {
                throw new IllegalArgumentException("update and repeated update have different state types");
            }
            if (target.type().parameterType(2) != contributionCarrier) {
                throw new IllegalArgumentException("update and repeated update have different contribution carriers");
            }
        });
    }

    public GroupedAggregationUpdateTarget(MethodHandle update)
    {
        this(update, Optional.empty());
    }

    public Class<?> contributionCarrier()
    {
        return update.type().parameterType(2);
    }

    private static void validate(MethodHandle target, boolean repeated)
    {
        MethodType type = target.type();
        int parameters = repeated ? 4 : 3;
        if (type.returnType() != void.class ||
                type.parameterCount() != parameters ||
                type.parameterType(0).isPrimitive() ||
                type.parameterType(1) != int.class ||
                (type.parameterType(2) != long.class && type.parameterType(2) != double.class) ||
                (repeated && type.parameterType(3) != int.class)) {
            throw new IllegalArgumentException("invalid grouped update target type: " + type);
        }
    }
}
