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

import org.weakref.nitro.core.type.Field;

import static java.util.Objects.requireNonNull;

/**
 * Explicit physical-plan contract for exporting the stable hash computed while grouping.
 *
 * <p>The planner supplies the result field because logical type bindings belong to the registry. The hash is an
 * implementation channel appended after the logical aggregation outputs; it is not a query-visible expression.
 * A downstream {@link AuthoritativeHashChannel} with the same identifier may consume it without hashing the keys
 * again.
 */
public record GroupingHashOutput(String contractIdentifier, Field field)
{
    public GroupingHashOutput
    {
        contractIdentifier = requireNonNull(contractIdentifier, "contractIdentifier is null");
        if (contractIdentifier.isBlank()) {
            throw new IllegalArgumentException("contractIdentifier is blank");
        }
        field = requireNonNull(field, "field is null");
        if (field.nullable()) {
            throw new IllegalArgumentException("Grouping hash output field is nullable");
        }
    }
}
