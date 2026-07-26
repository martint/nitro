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
package org.weakref.nitro.core.function.mask;

import org.weakref.nitro.core.function.FunctionCapability;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Function-owned lowering of a scalar projection to a non-copying binary slice over one source argument.
 */
public interface BinarySliceProjectionProvider
        extends FunctionCapability
{
    Optional<BinarySliceProjection> bind(FunctionCallSite callSite);

    record BinarySliceProjection(int sourceArgument, BinarySliceTransform transform)
    {
        public BinarySliceProjection
        {
            if (sourceArgument < 0) {
                throw new IllegalArgumentException("sourceArgument is negative");
            }
            requireNonNull(transform, "transform is null");
        }
    }

    @FunctionalInterface
    interface BinarySliceTransform
    {
        Optional<BinarySliceAccessor> bind(Vector values);
    }

    @FunctionalInterface
    interface BinarySliceAccessor
    {
        VectorAccess.BinarySlice get(int position);
    }
}
