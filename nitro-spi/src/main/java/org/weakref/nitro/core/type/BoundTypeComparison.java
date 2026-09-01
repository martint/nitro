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
package org.weakref.nitro.core.type;

import org.weakref.nitro.data.Vector;

/**
 * Logical ordering bound to the physical vector representations admitted by one type binding.
 *
 * <p>The binding provider supplies leaf comparison semantics. An engine may compose those semantics
 * recursively for structural values before exposing the resulting operation to a registry-bound
 * function implementation.
 */
@FunctionalInterface
public interface BoundTypeComparison
{
    int compare(
            Vector leftValues,
            Vector leftNulls,
            int leftPosition,
            Vector rightValues,
            Vector rightNulls,
            int rightPosition);
}
