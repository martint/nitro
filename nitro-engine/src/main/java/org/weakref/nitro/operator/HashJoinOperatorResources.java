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

import static java.util.Objects.requireNonNull;

/**
 * Engine-owner-scoped compatibility policy used by hash joins.
 *
 * <p>The shared token carries no storage itself. Each join retains a distinct local pool group; the allocator may
 * admit those groups to this compatibility domain only after enough current-execution contributors register.
 */
public final class HashJoinOperatorResources
{
    private final boolean shareBufferPoolAcrossOperators;
    private final HashJoinMaterializationListener materializationListener;
    private final HashJoinIndexPolicy indexPolicy;
    private final HashJoinDynamicFilterPolicy dynamicFilterPolicy;
    private final HashJoinBuildPolicy buildPolicy;
    private final HashJoinOutputPolicy outputPolicy;
    private final HashJoinFilterPolicy filterPolicy;
    private final HashJoinExecutionPolicy executionPolicy;
    private final Object sharedBufferPoolGroup = new Object();

    public HashJoinOperatorResources(
            boolean shareBufferPoolAcrossOperators,
            HashJoinMaterializationListener materializationListener,
            HashJoinIndexPolicy indexPolicy,
            HashJoinDynamicFilterPolicy dynamicFilterPolicy,
            HashJoinBuildPolicy buildPolicy,
            HashJoinOutputPolicy outputPolicy,
            HashJoinFilterPolicy filterPolicy,
            HashJoinExecutionPolicy executionPolicy)
    {
        this.shareBufferPoolAcrossOperators = shareBufferPoolAcrossOperators;
        this.materializationListener = materializationListener;
        this.indexPolicy = requireNonNull(indexPolicy, "indexPolicy is null");
        this.dynamicFilterPolicy = requireNonNull(dynamicFilterPolicy, "dynamicFilterPolicy is null");
        this.buildPolicy = requireNonNull(buildPolicy, "buildPolicy is null");
        this.outputPolicy = requireNonNull(outputPolicy, "outputPolicy is null");
        this.filterPolicy = requireNonNull(filterPolicy, "filterPolicy is null");
        this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
    }

    Object bufferPoolCompatibilityGroup(Object localPoolGroup)
    {
        return shareBufferPoolAcrossOperators ? sharedBufferPoolGroup : requireNonNull(localPoolGroup, "localPoolGroup is null");
    }

    HashJoinMaterializationListener materializationListener()
    {
        return materializationListener;
    }

    HashJoinIndexPolicy indexPolicy()
    {
        return indexPolicy;
    }

    HashJoinDynamicFilterPolicy dynamicFilterPolicy()
    {
        return dynamicFilterPolicy;
    }

    HashJoinBuildPolicy buildPolicy()
    {
        return buildPolicy;
    }

    HashJoinOutputPolicy outputPolicy()
    {
        return outputPolicy;
    }

    HashJoinFilterPolicy filterPolicy()
    {
        return filterPolicy;
    }

    HashJoinExecutionPolicy executionPolicy()
    {
        return executionPolicy;
    }
}
