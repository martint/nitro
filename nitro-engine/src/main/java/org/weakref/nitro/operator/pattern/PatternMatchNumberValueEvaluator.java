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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;

/// Appends the SQL BIGINT match number to caller-owned reusable output.
public final class PatternMatchNumberValueEvaluator
        implements PatternValueEvaluator
{
    @Override
    public Streams append(
            PatternEvaluationContext context,
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            int outputPosition,
            int outputSize)
    {
        requireNonNull(context, "context is null");
        requireNonNull(allocator, "allocator is null");
        requireNonNull(allocationContext, "allocationContext is null");
        requireNonNull(output, "output is null");

        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output.hasValues() ? (I64Vector) output.values() : null,
                I64Vector.class,
                outputSize,
                I64Vector::new);
        values.values()[outputPosition] = context.matchNumber();
        return allocator.replaceValuesAndMarkPositionValid(
                allocationContext,
                output,
                values,
                outputPosition,
                outputSize);
    }
}
