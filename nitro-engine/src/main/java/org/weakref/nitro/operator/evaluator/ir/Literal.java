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

import org.weakref.nitro.core.type.TypeBinding;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// A structural constant.
///
/// Typed literals may carry a null value because nullability is emitted as a separate stream.
/// Untyped literals are retained for standalone plans and must be non-null.
public record Literal(Object value, Optional<TypeBinding> type)
        implements Operation
{
    public Literal(Object value)
    {
        this(requireNonNull(value, "value is null"), Optional.empty());
    }

    public Literal(Object value, TypeBinding type)
    {
        this(value, Optional.of(requireNonNull(type, "type is null")));
    }

    public Literal
    {
        type = requireNonNull(type, "type is null");
        if (type.isEmpty()) {
            requireNonNull(value, "untyped literal value is null");
        }
    }
}
