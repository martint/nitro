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
    private final Object sharedBufferPoolGroup = new Object();

    public HashJoinOperatorResources(boolean shareBufferPoolAcrossOperators)
    {
        this(
                shareBufferPoolAcrossOperators,
                null,
                HashJoinIndexPolicy.defaults(),
                HashJoinDynamicFilterPolicy.defaults());
    }

    public HashJoinOperatorResources(
            boolean shareBufferPoolAcrossOperators,
            HashJoinMaterializationListener materializationListener)
    {
        this(
                shareBufferPoolAcrossOperators,
                materializationListener,
                HashJoinIndexPolicy.defaults(),
                HashJoinDynamicFilterPolicy.defaults());
    }

    public HashJoinOperatorResources(
            boolean shareBufferPoolAcrossOperators,
            HashJoinMaterializationListener materializationListener,
            HashJoinIndexPolicy indexPolicy)
    {
        this(
                shareBufferPoolAcrossOperators,
                materializationListener,
                indexPolicy,
                HashJoinDynamicFilterPolicy.defaults());
    }

    public HashJoinOperatorResources(
            boolean shareBufferPoolAcrossOperators,
            HashJoinMaterializationListener materializationListener,
            HashJoinIndexPolicy indexPolicy,
            HashJoinDynamicFilterPolicy dynamicFilterPolicy)
    {
        this.shareBufferPoolAcrossOperators = shareBufferPoolAcrossOperators;
        this.materializationListener = materializationListener;
        this.indexPolicy = requireNonNull(indexPolicy, "indexPolicy is null");
        this.dynamicFilterPolicy = requireNonNull(dynamicFilterPolicy, "dynamicFilterPolicy is null");
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
}
