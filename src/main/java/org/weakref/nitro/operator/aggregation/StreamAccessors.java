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
package org.weakref.nitro.operator.aggregation;

import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.evaluator.ir.Stream;

public final class StreamAccessors
{
    private StreamAccessors() {}

    public static StreamAccessor forBatch(Batch batch)
    {
        return (column, stream) -> read(batch.output(column), stream);
    }

    public static Vector read(Output output, Stream stream)
    {
        try {
            return output.borrow(stream);
        }
        catch (IllegalArgumentException exception) {
            if (stream != Stream.NULLS) {
                throw exception;
            }
            return null;
        }
    }
}
