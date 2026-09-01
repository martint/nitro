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

import org.weakref.nitro.operator.RowPositionIndex;

import java.util.List;

import static java.util.Objects.requireNonNull;

/// Dispatches stable label ordinals to registry-bound definitions over one borrowed evaluation context.
public final class PatternDefinitionEvaluator
        implements PatternLabelEvaluator
{
    private final List<PatternDefinition> definitions;
    private final PatternEvaluationContext context;
    private int patternStart;
    private boolean initialized;

    public PatternDefinitionEvaluator(RowPositionIndex rows, List<PatternDefinition> definitions)
    {
        this.context = new PatternEvaluationContext(requireNonNull(rows, "rows is null"));
        this.definitions = List.copyOf(definitions);
    }

    public void reset(int partitionStart, int partitionEnd, int patternStart, long matchNumber)
    {
        context.resetMatch(partitionStart, partitionEnd, patternStart, matchNumber);
        this.patternStart = patternStart;
        initialized = true;
    }

    @Override
    public boolean evaluate(int labelOrdinal, int inputPosition, LabelHistory history)
    {
        if (!initialized) {
            throw new IllegalStateException("pattern evaluator is not initialized");
        }
        if (labelOrdinal < 0 || labelOrdinal >= definitions.size()) {
            throw new IllegalArgumentException("label ordinal has no definition");
        }
        context.resetRow(Math.addExact(patternStart, inputPosition), history);
        return definitions.get(labelOrdinal).matches(context);
    }
}
