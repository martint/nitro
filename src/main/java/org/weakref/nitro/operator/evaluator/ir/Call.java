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

import org.weakref.nitro.core.function.ResolvedCall;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record Call(String name, List<Reference> arguments, ResolvedCall resolvedCall)
        implements Operation
{
    public Call(String name, List<Reference> arguments)
    {
        this(name, arguments, null);
    }

    public Call(ResolvedCall resolvedCall, List<Reference> arguments)
    {
        this(requireNonNull(resolvedCall, "resolvedCall is null").identity().value(), arguments, resolvedCall);
    }

    public Call
    {
        name = requireNonNull(name, "name is null");
        arguments = List.copyOf(arguments);
        if (resolvedCall != null && !name.equals(resolvedCall.identity().value())) {
            throw new IllegalArgumentException("name does not match resolved call identity");
        }
    }
}
