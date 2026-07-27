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
package org.weakref.nitro.core.function;

import java.util.List;

import static java.util.Objects.requireNonNull;

/// Semantics needed by evaluators and optimizers without recognizing a function.
public record FunctionSemantics(
        boolean deterministic,
        List<ArgumentNullConvention> argumentNullConventions,
        boolean nullableResult,
        FailureConvention failureConvention)
{
    public enum ArgumentNullConvention
    {
        RETURN_NULL_ON_NULL,
        CALLED_ON_NULL
    }

    public enum FailureConvention
    {
        NEVER_FAILS,
        MAY_FAIL
    }

    public FunctionSemantics
    {
        argumentNullConventions = List.copyOf(requireNonNull(argumentNullConventions, "argumentNullConventions is null"));
        failureConvention = requireNonNull(failureConvention, "failureConvention is null");
    }
}
