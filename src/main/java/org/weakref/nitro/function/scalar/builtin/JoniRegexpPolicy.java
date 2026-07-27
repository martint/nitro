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
 * Immutable matcher-input choices for Joni regular-expression execution.
 */
public record JoniRegexpPolicy(boolean zeroCopyMatcher)
{
    public static JoniRegexpPolicy defaults()
    {
        return new JoniRegexpPolicy(true);
    }

    public static JoniRegexpPolicy fromSystemProperties()
    {
        return new JoniRegexpPolicy(
                Boolean.parseBoolean(System.getProperty("nitro.regexp.zeroCopyMatcher", "true")));
    }
}
