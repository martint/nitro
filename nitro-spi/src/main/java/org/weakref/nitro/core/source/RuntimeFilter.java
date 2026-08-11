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
package org.weakref.nitro.core.source;

import static java.util.Objects.requireNonNull;

/// Typed runtime-filter semantics at the source boundary. A filter with a required residual may be used only to prune
/// source work; otherwise, an `ENFORCED` acceptance transfers complete predicate responsibility to the source.
public record RuntimeFilter(SourceColumnHandle column, TypedDomain domain, boolean approximate, boolean residualRequired)
{
    public RuntimeFilter(SourceColumnHandle column, TypedDomain domain, boolean approximate)
    {
        this(column, domain, approximate, true);
    }

    public RuntimeFilter
    {
        column = requireNonNull(column, "column is null");
        domain = requireNonNull(domain, "domain is null");
        if (!column.type().identity().equals(domain.type().identity())) {
            throw new IllegalArgumentException("column and domain types differ");
        }
    }

    public RuntimeFilter withoutResidual()
    {
        return new RuntimeFilter(column, domain, approximate, false);
    }
}
