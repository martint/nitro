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
    private static final int LINEAR_SEARCH_LIMIT = 8;

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
        LiteralSet literalSet = new LiteralSet(candidates);

        return Optional.of(new SourceMaskOptimization(
                List.of(0, projection.sourceArgument()),
                values -> projection.transform().bind(values)
                        .map(accessor -> position -> literalSet.contains(accessor.get(position)))));
    }

    private static final class LiteralSet
    {
        private final byte[][] values;
        private final int[] slots;

        private LiteralSet(byte[][] values)
        {
            this.values = values;
            if (values.length <= LINEAR_SEARCH_LIMIT) {
                slots = null;
                return;
            }

            int capacity = 1;
            while (capacity < values.length * 2) {
                capacity <<= 1;
            }
            slots = new int[capacity];
            for (int valueIndex = 0; valueIndex < values.length; valueIndex++) {
                byte[] value = values[valueIndex];
                int slot = hash(value, 0, value.length) & (slots.length - 1);
                while (slots[slot] != 0) {
                    slot = (slot + 1) & (slots.length - 1);
                }
                slots[slot] = valueIndex + 1;
            }
        }

        private boolean contains(VectorAccess.BinarySlice value)
        {
            if (slots == null) {
                for (byte[] candidate : values) {
                    if (equals(value, candidate)) {
                        return true;
                    }
                }
                return false;
            }

            int slot = hash(value.data(), value.offset(), value.length()) & (slots.length - 1);
            while (slots[slot] != 0) {
                if (equals(value, values[slots[slot] - 1])) {
                    return true;
                }
                slot = (slot + 1) & (slots.length - 1);
            }
            return false;
        }

        private static int hash(byte[] data, int offset, int length)
        {
            int hash = 0x811C9DC5;
            for (int index = 0; index < length; index++) {
                hash = (hash ^ data[offset + index]) * 0x01000193;
            }
            return hash ^ (hash >>> 16);
        }

        private static boolean equals(VectorAccess.BinarySlice value, byte[] candidate)
        {
            if (candidate.length != value.length()) {
                return false;
            }
            for (int index = 0; index < candidate.length; index++) {
                if (value.data()[value.offset() + index] != candidate[index]) {
                    return false;
                }
            }
            return true;
        }
    }
}
