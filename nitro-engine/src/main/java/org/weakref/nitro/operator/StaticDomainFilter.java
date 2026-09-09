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

import org.weakref.nitro.core.source.TypedDomain;

import static java.util.Objects.requireNonNull;

/// Exact typed source predicate whose semantic responsibility may be negotiated before execution.
public record StaticDomainFilter(int column, TypedDomain domain)
{
    public StaticDomainFilter
    {
        if (column < 0) {
            throw new IllegalArgumentException("column is negative");
        }
        domain = requireNonNull(domain, "domain is null");
    }

    public StaticDomainFilter withColumn(int column)
    {
        return new StaticDomainFilter(column, domain);
    }
}
