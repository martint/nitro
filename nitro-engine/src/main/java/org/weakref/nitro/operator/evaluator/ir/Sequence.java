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

import org.weakref.nitro.data.Stream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Evaluates {@code first} for every active row before evaluating and returning {@code result}.
 * <p>
 * Both references name complete producer bundles. Errors from {@code first} propagate through
 * this operation even when {@code result} does not otherwise depend on it.
 */
public record Sequence(Reference first, Reference result)
        implements Operation
{
    public Sequence
    {
        checkArgument(first.stream() == Stream.VALUES, "first must reference VALUES");
        checkArgument(result.stream() == Stream.VALUES, "result must reference VALUES");
    }
}
