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
package org.weakref.nitro.operator;

/**
 * Engine-selected policy shared by expression evaluation in filters and projections.
 */
public record EvaluationOperatorPolicy(
        int dictionaryPeelSparseRatio,
        boolean adaptiveMaskReordering,
        boolean orShortCircuitRemaining,
        boolean orEvaluateFinalTermOnFullMask,
        int orFinalTermMinRemainingRows,
        int orFinalTermMinRemainingPercent,
        boolean fastBooleanMaskClassifier,
        boolean inPlaceFlatBooleanClassifier,
        boolean inputMaskResolver,
        boolean directPrimitiveInputMask,
        boolean recycleControlFrames)
{
    public EvaluationOperatorPolicy
    {
        if (dictionaryPeelSparseRatio <= 0) {
            throw new IllegalArgumentException("dictionaryPeelSparseRatio must be positive");
        }
        if (orFinalTermMinRemainingRows < 0) {
            throw new IllegalArgumentException("orFinalTermMinRemainingRows is negative");
        }
        if (orFinalTermMinRemainingPercent < 0 || orFinalTermMinRemainingPercent > 100) {
            throw new IllegalArgumentException("orFinalTermMinRemainingPercent must be between 0 and 100");
        }
    }

    public static EvaluationOperatorPolicy defaults()
    {
        return new EvaluationOperatorPolicy(
                8,
                true,
                true,
                false,
                1024,
                75,
                true,
                true,
                true,
                true,
                true);
    }

    public static EvaluationOperatorPolicy fromSystemProperties()
    {
        EvaluationOperatorPolicy defaults = defaults();
        return new EvaluationOperatorPolicy(
                Integer.getInteger("nitro.expression.dictionaryPeelSparseRatio", defaults.dictionaryPeelSparseRatio()),
                booleanProperty("nitro.expression.adaptiveMaskReordering", defaults.adaptiveMaskReordering()),
                booleanProperty("nitro.expression.orShortCircuitRemaining", defaults.orShortCircuitRemaining()),
                booleanProperty("nitro.expression.orEvaluateFinalTermOnFullMask", defaults.orEvaluateFinalTermOnFullMask()),
                Integer.getInteger("nitro.expression.orFinalTermMinRemainingRows", defaults.orFinalTermMinRemainingRows()),
                Integer.getInteger("nitro.expression.orFinalTermMinRemainingPercent", defaults.orFinalTermMinRemainingPercent()),
                booleanProperty("nitro.expression.fastBooleanMaskClassifier", defaults.fastBooleanMaskClassifier()),
                booleanProperty("nitro.expression.inPlaceFlatBooleanClassifier", defaults.inPlaceFlatBooleanClassifier()),
                booleanProperty("nitro.expression.inputMaskResolver", defaults.inputMaskResolver()),
                booleanProperty("nitro.expression.directPrimitiveInputMask", defaults.directPrimitiveInputMask()),
                booleanProperty("nitro.expression.recycleControlFrames", defaults.recycleControlFrames()));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
