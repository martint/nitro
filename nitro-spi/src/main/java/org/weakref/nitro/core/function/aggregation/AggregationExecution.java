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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;

import static java.util.Objects.requireNonNull;

/**
 * Provider-visible services for one aggregate execution instance.
 */
public record AggregationExecution(Allocator allocator, Allocator.Context allocationContext, Schema inputSchema)
{
    public AggregationExecution
    {
        allocator = requireNonNull(allocator, "allocator is null");
        allocationContext = requireNonNull(allocationContext, "allocationContext is null");
        inputSchema = requireNonNull(inputSchema, "inputSchema is null");
    }
}
