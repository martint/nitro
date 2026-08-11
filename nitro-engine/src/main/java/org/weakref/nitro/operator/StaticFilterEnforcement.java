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

import org.weakref.nitro.core.source.RuntimeFilterAcceptance;

import static java.util.Objects.requireNonNull;

/** Completion of a static predicate's source-enforcement negotiation. */
public final class StaticFilterEnforcement
{
    private static final StaticFilterEnforcement RESIDUAL = new StaticFilterEnforcement(RuntimeFilterAcceptance.REJECTED);

    private volatile RuntimeFilterAcceptance acceptance;

    private StaticFilterEnforcement(RuntimeFilterAcceptance acceptance)
    {
        this.acceptance = acceptance;
    }

    public static StaticFilterEnforcement pending()
    {
        return new StaticFilterEnforcement(null);
    }

    public static StaticFilterEnforcement residual()
    {
        return RESIDUAL;
    }

    public boolean enforced()
    {
        return acceptance == RuntimeFilterAcceptance.ENFORCED;
    }

    public synchronized void complete(RuntimeFilterAcceptance acceptance)
    {
        requireNonNull(acceptance, "acceptance is null");
        if (this == RESIDUAL) {
            throw new IllegalStateException("fixed residual enforcement cannot be completed");
        }
        if (this.acceptance != null) {
            throw new IllegalStateException("filter enforcement is already complete");
        }
        this.acceptance = acceptance;
    }
}
