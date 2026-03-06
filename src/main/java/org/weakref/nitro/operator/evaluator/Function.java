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
package org.weakref.nitro.operator.evaluator;

import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

/**
 * Unified protocol for all expression nodes in the evaluator.
 * <p>
 * Functions are called by the evaluator to compute results for a set of positions
 * described by {@code mask}. They call back into the evaluator via {@code context}
 * to request evaluation of their input expressions.
 * <p>
 * The {@code output} parameter enables additive, in-place evaluation: when non-null,
 * it contains results from previous calls. The function must only write to positions
 * in {@code mask} and preserve any existing values at other positions.
 */
public interface Function
{
    /**
     * Compute this expression for the positions described by {@code mask}.
     *
     * @param output the current output vector (null on first call, may be partially filled on subsequent calls)
     * @param mask   the positions to evaluate; the evaluator guarantees these have not been computed yet
     * @param context callback for evaluating sub-expressions and accessing the allocator
     * @return the output vector (possibly newly allocated if {@code output} was null)
     */
    Vector apply(Vector output, Mask mask, EvaluationContext context);
}
