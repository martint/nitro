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
package org.weakref.nitro.operator;

import org.weakref.nitro.jit.ProjectionMaskCompiler;

import static java.util.Objects.requireNonNull;

/**
 * Explicit engine-owned dependencies used by filter operators.
 */
public record FilterOperatorResources(
        ProjectionMaskCompiler projectionMaskCompiler,
        EvaluationOperatorPolicy evaluationPolicy,
        FilterOperatorPolicy policy)
{
    public FilterOperatorResources
    {
        requireNonNull(projectionMaskCompiler, "projectionMaskCompiler is null");
        requireNonNull(evaluationPolicy, "evaluationPolicy is null");
        requireNonNull(policy, "policy is null");
    }
}
