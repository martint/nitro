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
package org.weakref.nitro.jit;

import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

/**
 * A fused kernel that computes several {@link org.weakref.nitro.operator.ProjectOperator} outputs in one pass. Each
 * source column is read once, each shared subexpression is evaluated once, and the loop writes every output -- so the
 * per-output redundancy of single-output fusion (re-reading inputs, recomputing shared values) is gone.
 */
public interface FusedMultiProjection
{
    /**
     * Evaluate all fused outputs. Returns one {@link Streams} per fused output, in the order the outputs were
     * registered, or {@code null} if any input's runtime layout is unsupported (the caller then runs the interpreter).
     */
    Streams[] apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, PrimitiveExecutionContext context);
}
