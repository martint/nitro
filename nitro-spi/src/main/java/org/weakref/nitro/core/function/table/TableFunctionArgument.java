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

import org.weakref.nitro.core.batch.SourceBatch;

import static java.util.Objects.requireNonNull;

/// Current input state for one table argument.
public sealed interface TableFunctionArgument
        permits TableFunctionArgument.Finished, TableFunctionArgument.Rows
{
    /// The batch remains caller-owned throughout [TableFunctionProcessor#process]. A processor may borrow streams
    /// during the call. It may take streams only when the returned progress marks this argument as consumed; after
    /// that return the caller closes the drained batch and does not offer it again.
    record Rows(SourceBatch batch)
            implements TableFunctionArgument
    {
        public Rows
        {
            batch = requireNonNull(batch, "batch is null");
        }
    }

    enum Finished
            implements TableFunctionArgument
    {
        FINISHED
    }
}
