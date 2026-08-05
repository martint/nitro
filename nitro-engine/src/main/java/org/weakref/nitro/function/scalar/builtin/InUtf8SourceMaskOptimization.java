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
import org.weakref.nitro.core.function.mask.SourceMaskOptimization;
import org.weakref.nitro.core.function.mask.SourceMaskOptimizationProvider;
import org.weakref.nitro.data.VectorAccess;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Registry-owned composition of literal UTF-8 membership with a non-copying binary projection.
 */
public final class InUtf8SourceMaskOptimization
        implements SourceMaskOptimizationProvider
{
    @Override
    public Optional<SourceMaskOptimization> bind(FunctionCallSite callSite)
    {
        if (callSite.argumentCount() < 2) {
            return Optional.empty();
        }
        FunctionCallSite projectionCall = callSite.argument(0).call().orElse(null);
        if (projectionCall == null) {
            return Optional.empty();
        }
        BinarySliceProjectionProvider projectionProvider =
                projectionCall.capability(BinarySliceProjectionProvider.class).orElse(null);
        if (projectionProvider == null) {
            return Optional.empty();
        }
        BinarySliceProjectionProvider.BinarySliceProjection projection =
                projectionProvider.bind(projectionCall).orElse(null);
        if (projection == null) {
            return Optional.empty();
        }

        byte[][] candidates = new byte[callSite.argumentCount() - 1][];
        for (int index = 1; index < callSite.argumentCount(); index++) {
            Object literal = callSite.argument(index).literal().orElse(null);
            if (!(literal instanceof String value)) {
                return Optional.empty();
            }
            candidates[index - 1] = value.getBytes(StandardCharsets.UTF_8);
        }

        return Optional.of(new SourceMaskOptimization(
                List.of(0, projection.sourceArgument()),
                values -> projection.transform().bind(values)
                        .map(accessor -> position -> matchesAny(accessor.get(position), candidates))));
    }

    private static boolean matchesAny(VectorAccess.BinarySlice value, byte[][] candidates)
    {
        for (byte[] candidate : candidates) {
            if (candidate.length != value.length()) {
                continue;
            }
            boolean matches = true;
            for (int index = 0; index < candidate.length; index++) {
                if (value.data()[value.offset() + index] != candidate[index]) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return true;
            }
        }
        return false;
    }
}
