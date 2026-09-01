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

/// Evaluates one registry-bound row-pattern label definition.
///
/// The history includes the tentative label at {@code inputPosition}. It is a borrowed callback view and must not be
/// retained. The matcher deliberately knows only label ordinals; navigation, scalar calls, and match-local
/// aggregations are supplied by the integration's evaluator implementation. Evaluation is one atomic matcher step:
/// implementations must not suspend, because the matcher establishes cooperative checkpoints between label calls.
@FunctionalInterface
public interface PatternLabelEvaluator
{
    boolean evaluate(int labelOrdinal, int inputPosition, LabelHistory history);

    interface LabelHistory
    {
        int size();

        int labelAt(int position);
    }
}
