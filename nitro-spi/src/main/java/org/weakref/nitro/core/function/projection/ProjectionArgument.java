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
package org.weakref.nitro.core.function.projection;

/**
 * Provider-visible shape of one call argument. Literal values are planner data; input and computed arguments expose
 * no engine implementation object.
 */
public record ProjectionArgument(Kind kind, Object literal)
{
    public ProjectionArgument
    {
        if (kind == null) {
            throw new NullPointerException("kind is null");
        }
        if (kind != Kind.LITERAL && literal != null) {
            throw new IllegalArgumentException("only literal arguments may carry a literal value");
        }
    }

    public static ProjectionArgument input()
    {
        return new ProjectionArgument(Kind.INPUT, null);
    }

    public static ProjectionArgument computed()
    {
        return new ProjectionArgument(Kind.COMPUTED, null);
    }

    public static ProjectionArgument literal(Object value)
    {
        return new ProjectionArgument(Kind.LITERAL, value);
    }

    public enum Kind
    {
        INPUT,
        COMPUTED,
        LITERAL
    }
}
