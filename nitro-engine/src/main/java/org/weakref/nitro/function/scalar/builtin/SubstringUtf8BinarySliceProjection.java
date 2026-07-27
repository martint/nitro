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

import org.weakref.nitro.core.function.mask.BinarySliceProjectionProvider;
import org.weakref.nitro.core.function.mask.FunctionCallSite;
import org.weakref.nitro.data.VectorAccess;

import java.util.Optional;

/**
 * Function-owned non-copying lowering for constant-bound UTF-8 substring.
 */
public final class SubstringUtf8BinarySliceProjection
        implements BinarySliceProjectionProvider
{
    @Override
    public Optional<BinarySliceProjection> bind(FunctionCallSite callSite)
    {
        if (callSite.argumentCount() != 3) {
            return Optional.empty();
        }
        Optional<Object> startLiteral = callSite.argument(1).literal();
        Optional<Object> lengthLiteral = callSite.argument(2).literal();
        if (startLiteral.isEmpty() || !(startLiteral.orElseThrow() instanceof Long start) ||
                lengthLiteral.isEmpty() || !(lengthLiteral.orElseThrow() instanceof Long length)) {
            return Optional.empty();
        }

        return Optional.of(new BinarySliceProjection(0, values -> {
            VectorAccess.BinaryValues binaryValues;
            try {
                binaryValues = VectorAccess.binaryValues(values);
            }
            catch (IllegalArgumentException _) {
                return Optional.empty();
            }
            return Optional.of(position -> {
                VectorAccess.BinarySlice input = binaryValues.value(position);
                long slice = Utf8Support.substringSlice(input.data(), input.offset(), input.length(), start, length);
                return new VectorAccess.BinarySlice(input.data(), (int) (slice >>> 32), (int) slice);
            });
        }));
    }
}
