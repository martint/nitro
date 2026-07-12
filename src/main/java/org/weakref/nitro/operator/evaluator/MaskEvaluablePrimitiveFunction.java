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
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Reference;

import java.util.List;

public interface MaskEvaluablePrimitiveFunction
        extends PrimitiveFunction
{
    /**
     * Returns an input stream whose Boolean values are exactly this function's Boolean result, or {@code null} when
     * the function must execute. The evaluator can pass this alias to an upstream direct-mask resolver without
     * materializing either stream. Implementations must not use value- or query-dependent logic here.
     */
    default Reference directMaskInput(List<Reference> arguments)
    {
        return null;
    }

    default MaskOutcome tryEvaluateMaskOutcome(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return null;
    }

    default boolean requiresCompletedInputCompanionStreamsForMask()
    {
        return true;
    }

    default Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return null;
    }

    default Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return null;
    }

    default boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return false;
    }

    default boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return false;
    }
}
