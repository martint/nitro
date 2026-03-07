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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

/**
 * Callback interface passed to {@link Function#apply} implementations.
 * <p>
 * Functions use this to request lazy evaluation of their sub-expressions at
 * whatever masks they require. The evaluator handles memoization and ensures
 * that positions are not computed more than once.
 */
public interface EvaluationContext
{
    /**
     * Evaluate the expression at the given index for the given mask.
     * Results are memoized; subsequent calls with overlapping masks are cheap.
     */
    Result evaluate(int expressionIndex, Mask mask);

    /**
     * Fetch a source input column by ordinal for the given mask.
     */
    Vector input(int inputIndex, Mask mask);

    Allocator allocator();
}
