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

import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.type.Schema;

/// Resettable argument view for one selected row of a match-local aggregate.
public interface PatternAggregationInput
        extends AggregationInput
{
    Schema schema();

    void reset(PatternEvaluationContext context, int position, int labelOrdinal);

    int physicalPosition();

    int physicalSize();
}
