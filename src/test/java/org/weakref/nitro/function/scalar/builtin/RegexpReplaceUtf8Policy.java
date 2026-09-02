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
 * Immutable specialization choices for {@link RegexpReplaceUtf8}.
 */
public record RegexpReplaceUtf8Policy(
        boolean constantArguments)
{
    public static RegexpReplaceUtf8Policy defaults()
    {
        return new RegexpReplaceUtf8Policy(true);
    }

    public static RegexpReplaceUtf8Policy fromSystemProperties()
    {
        return new RegexpReplaceUtf8Policy(
                Boolean.parseBoolean(System.getProperty("nitro.regexp.constantArguments", "true")));
    }
}
