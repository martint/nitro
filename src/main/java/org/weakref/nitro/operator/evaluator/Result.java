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
package org.weakref.nitro.operator.evaluator;

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Vector;

/**
 * The output of a {@link Function}: computed values, optional null flags, and optional error flags.
 * <p>
 * {@code nulls} is non-null only when the function has tracked which positions produced a null result.
 * {@code errors} is non-null only when the function has detected per-position errors (e.g., overflow,
 * divide-by-zero). Both are {@link BooleanVector}s parallel to {@code values}.
 * <p>
 * Passed as the {@code output} parameter on additive calls so that functions can reuse existing buffers.
 */
public record Result(Vector values, BooleanVector nulls, BooleanVector errors)
{
    public static Result of(Vector values)
    {
        return new Result(values, null, null);
    }

    public static Result of(Vector values, BooleanVector nulls)
    {
        return new Result(values, nulls, null);
    }
}
