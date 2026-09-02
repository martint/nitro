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
package org.weakref.nitro.core.function.table;

import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.data.Allocator;

/// General batch calling convention for a registry-provided table-function implementation.
///
/// Input schemas and argument semantics are fixed when the registry creates the processor. Each call receives the
/// current selected rows for every table argument, the downstream output demand, and the island allocator. The
/// implementation may preserve dictionary, RLE, nested, and sparse physical forms; simpler implementations may ask
/// registry-provided adapters to materialize them before this call.
public interface TableFunctionProcessor
        extends AutoCloseable
{
    TableFunctionProgress process(
            TableFunctionInput input,
            TableFunctionOutputDemand outputDemand,
            Allocator allocator,
            Allocator.Context allocationContext,
            ExecutionContext executionContext);

    @Override
    void close();
}
