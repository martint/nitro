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

import java.util.HashSet;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** Provider proof that logical key identity is repetition of aligned physical child-output tuples. */
public record RepeatedKeyLayout(Order order, List<Output> outputs)
{
    public enum Order
    {
        ORDERED,
        UNORDERED_MULTISET,
    }

    public record Output(int output, TypeBinding type)
    {
        public Output
        {
            if (output < 0) {
                throw new IllegalArgumentException("output is negative");
            }
            type = requireNonNull(type, "type is null");
        }
    }

    public RepeatedKeyLayout
    {
        order = requireNonNull(order, "order is null");
        outputs = List.copyOf(requireNonNull(outputs, "outputs is null"));
        if (outputs.isEmpty()) {
            throw new IllegalArgumentException("outputs is empty");
        }
        HashSet<Integer> ordinals = new HashSet<>();
        for (Output output : outputs) {
            requireNonNull(output, "output is null");
            if (!ordinals.add(output.output())) {
                throw new IllegalArgumentException("duplicate output: " + output.output());
            }
        }
    }
}
