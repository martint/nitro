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
package org.weakref.nitro.data;

import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

/**
 * Immutable allocation-lifetime policy for reusing pooled variable-width storage.
 */
public record VariableWidthStorageReusePolicy(OptionalInt maximumOversizeRatio)
{
    public VariableWidthStorageReusePolicy
    {
        requireNonNull(maximumOversizeRatio, "maximumOversizeRatio is null");
        if (maximumOversizeRatio.isPresent() && maximumOversizeRatio.getAsInt() < 1) {
            throw new IllegalArgumentException("maximumOversizeRatio is less than 1");
        }
    }

    public static VariableWidthStorageReusePolicy allocatorDefault()
    {
        return new VariableWidthStorageReusePolicy(OptionalInt.empty());
    }

    public static VariableWidthStorageReusePolicy maximumOversizeRatio(int maximumOversizeRatio)
    {
        return new VariableWidthStorageReusePolicy(OptionalInt.of(maximumOversizeRatio));
    }

    int maximumOversizeRatioOrElse(int defaultValue)
    {
        return maximumOversizeRatio.orElse(defaultValue);
    }
}
