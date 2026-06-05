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
package org.weakref.nitro.jit;

import org.weakref.nitro.data.Vector;

/**
 * A data type. Like functions and aggregates, types are extensible: each type carries its own code fragments,
 * so the compiler core never switches on a fixed set of types. Every value rides in a {@code long} result
 * slot; a type knows how to compare two slots (for ORDER BY), how to decode a slot back to its Java value
 * (for use in HAVING and projections), and how to reconstruct a result column into an engine {@link Vector}
 * (for bridging compiled results into the operator world). New types are added by
 * {@link Types#register registering} an implementation -- the engine hard-codes none.
 */
public interface Type
{
    /** Stable type name, used to reference the type from generated code and to identify it to result consumers. */
    String name();

    /** Java {@code int} comparison of two values stored in long slots {@code a} and {@code b}. */
    String compare(String a, String b);

    /** Java expression for the value stored in {@code slot}, for use in conditions and projections. */
    String decode(String slot);

    /**
     * Reconstruct the first {@code count} result slots into an engine {@link Vector} of this type. {@code dictionary}
     * supplies the byte values for id-encoded types (e.g. strings) and is {@code null} for self-contained types
     * whose slots hold the value directly. This is how a compiled pipeline's columnar output crosses back into
     * the operator world without the bridge ever switching on a fixed set of types.
     */
    Vector toVector(long[] slots, int count, byte[][] dictionary);
}
