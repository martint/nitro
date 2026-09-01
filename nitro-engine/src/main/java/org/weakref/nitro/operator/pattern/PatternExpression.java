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
package org.weakref.nitro.operator.pattern;

import java.util.List;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

/// Engine-neutral row-pattern expression.
///
/// Labels are resolved to stable ordinals by the integration layer. Their definitions are ordinary registry-bound
/// evaluator programs and are intentionally absent from this structural expression.
public sealed interface PatternExpression
        permits PatternExpression.Alternation,
                PatternExpression.Anchor,
                PatternExpression.Concatenation,
                PatternExpression.Empty,
                PatternExpression.Exclusion,
                PatternExpression.Label,
                PatternExpression.Permutation,
                PatternExpression.Quantified
{
    record Label(int ordinal)
            implements PatternExpression
    {
        public Label
        {
            if (ordinal < 0) {
                throw new IllegalArgumentException("ordinal is negative");
            }
        }
    }

    enum Empty
            implements PatternExpression
    {
        EMPTY
    }

    record Anchor(Type type)
            implements PatternExpression
    {
        public Anchor
        {
            type = requireNonNull(type, "type is null");
        }

        public enum Type
        {
            PARTITION_START,
            PARTITION_END,
        }
    }

    record Exclusion(PatternExpression pattern)
            implements PatternExpression
    {
        public Exclusion
        {
            pattern = requireNonNull(pattern, "pattern is null");
        }
    }

    record Alternation(List<PatternExpression> alternatives)
            implements PatternExpression
    {
        public Alternation
        {
            alternatives = List.copyOf(requireNonNull(alternatives, "alternatives is null"));
            if (alternatives.size() < 2) {
                throw new IllegalArgumentException("alternation requires at least two alternatives");
            }
        }
    }

    record Concatenation(List<PatternExpression> elements)
            implements PatternExpression
    {
        public Concatenation
        {
            elements = List.copyOf(requireNonNull(elements, "elements is null"));
        }
    }

    record Permutation(List<PatternExpression> elements)
            implements PatternExpression
    {
        public Permutation
        {
            elements = List.copyOf(requireNonNull(elements, "elements is null"));
            if (elements.size() < 2) {
                throw new IllegalArgumentException("permutation requires at least two elements");
            }
        }
    }

    record Quantified(
            PatternExpression pattern,
            int minimum,
            OptionalInt maximum,
            boolean greedy)
            implements PatternExpression
    {
        public Quantified
        {
            pattern = requireNonNull(pattern, "pattern is null");
            maximum = requireNonNull(maximum, "maximum is null");
            if (minimum < 0) {
                throw new IllegalArgumentException("minimum is negative");
            }
            if (maximum.isPresent() && maximum.getAsInt() < minimum) {
                throw new IllegalArgumentException("maximum is less than minimum");
            }
        }
    }
}
