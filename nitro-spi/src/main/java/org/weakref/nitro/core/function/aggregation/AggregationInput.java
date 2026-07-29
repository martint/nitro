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
package org.weakref.nitro.core.function.aggregation;

import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;

/**
 * Read-only aggregate inputs indexed in the provider's logical input space.
 */
@FunctionalInterface
public interface AggregationInput
{
    /**
     * Returns a required or optional stream for one raw argument or intermediate-state field.
     */
    Vector stream(int input, Stream stream);
}
