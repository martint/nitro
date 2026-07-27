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
 * Structural conditional expression. Unlike a {@link Call}, this operation has engine-defined control-flow semantics
 * and does not resolve through the dynamic function registry.
 */
public record Conditional(Reference condition, Reference whenTrue, Reference whenFalse)
        implements Operation
{
    public Conditional
    {
        condition = requireNonNull(condition, "condition is null");
        whenTrue = requireNonNull(whenTrue, "whenTrue is null");
        whenFalse = requireNonNull(whenFalse, "whenFalse is null");
    }
}
