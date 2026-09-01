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
package org.weakref.nitro.operator.pattern;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.operator.pattern.PatternLabels.contains;
import static org.weakref.nitro.operator.pattern.PatternLabels.normalized;

/// Label domain and visibility scope consumed by a match-local aggregation.
public final class PatternAggregationSet
{
    private final int[] labelOrdinals;
    private final Scope scope;

    public PatternAggregationSet(int[] labelOrdinals, Scope scope)
    {
        requireNonNull(labelOrdinals, "labelOrdinals is null");
        this.labelOrdinals = normalized(labelOrdinals);
        this.scope = requireNonNull(scope, "scope is null");
    }

    public int[] labelOrdinals()
    {
        return labelOrdinals.clone();
    }

    public Scope scope()
    {
        return scope;
    }

    boolean includes(int labelOrdinal)
    {
        return contains(labelOrdinals, labelOrdinal);
    }

    public enum Scope
    {
        RUNNING,
        FINAL,
    }
}
