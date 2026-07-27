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
package org.weakref.nitro.operator.evaluator.ir;

import static java.util.Objects.requireNonNull;

/**
 * Structural two-argument coalesce expression. Evaluation order and short-circuiting belong to the engine rather than
 * to a dynamically resolved scalar function.
 */
public record Coalesce(Reference first, Reference second)
        implements Operation
{
    public Coalesce
    {
        first = requireNonNull(first, "first is null");
        second = requireNonNull(second, "second is null");
    }
}
