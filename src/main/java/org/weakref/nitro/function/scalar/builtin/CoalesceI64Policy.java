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
package org.weakref.nitro.function.scalar.builtin;

/**
 * Immutable null-stream elision and reporting choices for {@link CoalesceI64}.
 */
public record CoalesceI64Policy(boolean omitNullsForNonNullInput, boolean diagnostics)
{
    public static CoalesceI64Policy defaults()
    {
        return new CoalesceI64Policy(true, false);
    }

    public static CoalesceI64Policy fromSystemProperties()
    {
        return new CoalesceI64Policy(
                Boolean.parseBoolean(System.getProperty("nitro.coalesce.omitNullsForNonNullInput", "true")),
                Boolean.getBoolean("nitro.debug.coalesceOmittedNulls"));
    }
}
