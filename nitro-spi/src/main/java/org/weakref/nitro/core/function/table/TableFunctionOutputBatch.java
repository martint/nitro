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

import java.util.List;

/// Table-function output with explicit pass-through row-reference columns.
///
/// Columns before [#properOutputCount()] are function results. Each [PassThroughReference]
/// identifies one additional integral column containing zero-based logical positions in the
/// selected input argument partition; its NULLS stream requests a null-extended pass-through row.
/// The engine can use one reference to gather any number of requested columns from that argument.
/// Reference descriptors correspond, in order, to the columns following the proper outputs. The
/// processor transfers the batch to the caller, which closes it after downstream consumption.
public interface TableFunctionOutputBatch
        extends SourceBatch
{
    int properOutputCount();

    List<PassThroughReference> passThroughReferences();

    record PassThroughReference(int argument)
    {
        public PassThroughReference
        {
            if (argument < 0) {
                throw new IllegalArgumentException("argument is negative");
            }
        }
    }
}
