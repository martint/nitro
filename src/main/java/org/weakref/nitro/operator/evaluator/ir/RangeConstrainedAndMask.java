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

import org.weakref.nitro.core.function.mask.RangeConstraint;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Plan-time fusion of two compatible range bounds at the head of a logical conjunction.
 *
 * <p>The complete logical fallback is retained for physical shapes declined by the provider kernel. Remaining
 * predicates keep their ordinary adaptive evaluation after the fused range has narrowed the mask.
 */
public record RangeConstrainedAndMask(
        Reference input,
        Object lowerExclusive,
        Object upperExclusive,
        RangeConstraint.Kernel kernel,
        List<MaskExpression> remainingTerms,
        AndMask fallback)
        implements MaskExpression
{
    public RangeConstrainedAndMask
    {
        input = requireNonNull(input, "input is null");
        lowerExclusive = requireNonNull(lowerExclusive, "lowerExclusive is null");
        upperExclusive = requireNonNull(upperExclusive, "upperExclusive is null");
        kernel = requireNonNull(kernel, "kernel is null");
        remainingTerms = List.copyOf(remainingTerms);
        fallback = requireNonNull(fallback, "fallback is null");
    }
}
