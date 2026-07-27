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
package org.weakref.nitro.core.batch;

import org.weakref.nitro.core.type.Schema;

import java.util.Optional;

/// One source-owned batch generation.
///
/// Calling [#column(int)] may materialize that column lazily. [#select(Selection)] allows a
/// downstream predicate to narrow the generation before remaining columns are materialized.
public interface SourceBatch
        extends AutoCloseable
{
    Schema schema();

    Selection selection();

    ColumnView column(int index);

    void select(Selection selection);

    default <T> Optional<T> capability(BatchCapability<T> capability)
    {
        return Optional.empty();
    }

    @Override
    void close();
}
