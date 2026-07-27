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

import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;

import static java.util.Objects.requireNonNull;

/**
 * Engine-owned composition of the general hash-join index implementations.
 */
final class GenericJoinIndexFactory
{
    private final OperatorCodeGenerationResources codeGeneration;
    private final FlatKeyTablePolicy flatKeyTablePolicy;
    private final HashJoinBuildPolicy buildPolicy;
    private final HashJoinIndexPolicy joinIndexPolicy;
    private final HashJoinExecutionPolicy executionPolicy;

    GenericJoinIndexFactory(
            OperatorCodeGenerationResources codeGeneration,
            FlatKeyTablePolicy flatKeyTablePolicy,
            HashJoinBuildPolicy buildPolicy,
            HashJoinIndexPolicy joinIndexPolicy,
            HashJoinExecutionPolicy executionPolicy)
    {
        this.codeGeneration = requireNonNull(codeGeneration, "codeGeneration is null");
        this.flatKeyTablePolicy = requireNonNull(flatKeyTablePolicy, "flatKeyTablePolicy is null");
        this.buildPolicy = requireNonNull(buildPolicy, "buildPolicy is null");
        this.joinIndexPolicy = requireNonNull(joinIndexPolicy, "joinIndexPolicy is null");
        this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
    }

    JoinIndex structural(StructuralKeyKernel[] kernels)
    {
        return new StructuralHashJoinIndex(kernels);
    }

    JoinIndex create(
            Vector[] values,
            PrimitiveArrayPool arrayPool,
            int expectedSize,
            boolean pairKeyOnlyBuild,
            boolean capInitialHash)
    {
        if (values.length == 2 &&
                isLong(values[0]) &&
                isLong(values[1])) {
            return new LongPairJoinIndex(
                    joinIndexPolicy,
                    executionPolicy,
                    arrayPool,
                    expectedSize,
                    pairKeyOnlyBuild,
                    capInitialHash,
                    buildPolicy.batchLongPairBuild());
        }
        if (values.length == 3 &&
                isLong(values[0]) &&
                isLong(values[1]) &&
                isLong(values[2])) {
            return new LongTripleJoinIndex(executionPolicy, arrayPool, expectedSize);
        }
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, arrayPool, codeGeneration, flatKeyTablePolicy);
        if (layout != null) {
            return new FlatJoinIndex(joinIndexPolicy, layout, expectedSize);
        }
        return new ObjectJoinIndex(values.length);
    }

    private static boolean isLong(Vector values)
    {
        FlatTypeHandler handler = FlatTypeHandlers.forVector(values);
        return handler != null && handler.kind() == FlatTypeHandler.Kind.LONG;
    }
}
