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
package org.weakref.nitro.function.scalar.builtin;

import org.weakref.nitro.core.function.mask.MaskCodeProvider;
import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder;

public final class EqualF64Optimization
        extends BinaryF64ProjectionOptimization
        implements MaskCodeProvider
{
    @Override
    protected ProjectionCodeBuilder.Value value(
            ProjectionCodeBuilder builder,
            ProjectionCodeBuilder.Value left,
            ProjectionCodeBuilder.Value right)
    {
        return builder.equal(left, right);
    }
}
