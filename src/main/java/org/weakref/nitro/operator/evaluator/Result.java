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

import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

/**
 * The output of evaluator execution: computed values, optional null flags, and optional error flags.
 * <p>
 * {@code nulls} is non-null only when the function has tracked which positions produced a null result.
 * {@code errors} is non-null only when the function has detected per-position errors (e.g., overflow,
 * divide-by-zero). Both are boolean-backed {@link Vector}s parallel to {@code values}.
 * <p>
 * Passed as the {@code output} parameter on additive calls so that functions can reuse existing buffers.
 */
public record Result(Vector values, Vector nulls, Vector errors)
{
    public static Result of(Vector values)
    {
        return new Result(values, null, null);
    }

    public static Result of(Vector values, Vector nulls)
    {
        return new Result(values, nulls, null);
    }

    public Streams toStreams()
    {
        Streams streams = Streams.of(Stream.VALUES, values);
        if (nulls != null) {
            streams = streams.with(Stream.NULLS, nulls);
        }
        if (errors != null) {
            streams = streams.with(Stream.ERRORS, errors);
        }
        return streams;
    }

    public static Result fromStreams(Streams streams)
    {
        Vector nulls = streams.has(Stream.NULLS) ? streams.get(Stream.NULLS) : null;
        Vector errors = streams.has(Stream.ERRORS) ? streams.get(Stream.ERRORS) : null;
        return new Result(streams.get(Stream.VALUES), nulls, errors);
    }
}
