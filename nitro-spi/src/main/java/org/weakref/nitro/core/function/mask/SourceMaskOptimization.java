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

import org.weakref.nitro.data.Vector;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A provider-bound predicate over one source argument.
 *
 * <p>The engine may evaluate the predicate directly over source positions or once per distinct dictionary value.
 */
public record SourceMaskOptimization(List<Integer> sourceArgumentPath, SourceValuePredicate predicate)
{
    public SourceMaskOptimization
    {
        sourceArgumentPath = List.copyOf(sourceArgumentPath);
        if (sourceArgumentPath.isEmpty()) {
            throw new IllegalArgumentException("sourceArgumentPath is empty");
        }
        requireNonNull(predicate, "predicate is null");
    }

    @FunctionalInterface
    public interface SourceValuePredicate
    {
        Optional<PositionPredicate> bind(Vector sourceValues);
    }

    @FunctionalInterface
    public interface PositionPredicate
    {
        boolean test(int position);
    }
}
