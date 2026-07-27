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
package org.weakref.nitro.operator.evaluator.ir;

import java.util.List;

public final class NormalizedIrValidator
{
    private NormalizedIrValidator() {}

    public static boolean isNormalized(EvaluationPlan plan)
    {
        return plan.assignments().stream()
                .allMatch(assignment -> isNormalizedMask(assignment.mask()) && isNormalizedOperation(assignment.operation()));
    }

    public static void validate(EvaluationPlan plan)
    {
        if (!isNormalized(plan)) {
            throw new IllegalArgumentException("Plan is not normalized");
        }
    }

    private static boolean isNormalizedOperation(Operation operation)
    {
        return switch (operation) {
            case Coalesce _, Conditional _ -> false;
            case Merge merge -> isNormalizedMask(merge.condition());
            case StructField _ -> true;
            default -> true;
        };
    }

    private static boolean isNormalizedMask(MaskExpression expression)
    {
        return switch (expression) {
            case AllMask _, RangeConstrainedAndMask _, ReferenceMask _ -> true;
            case NotMask(MaskExpression source) -> isNormalizedMask(source);
            case AndMask(List<MaskExpression> terms) -> terms.stream().allMatch(NormalizedIrValidator::isNormalizedMask);
            case OrMask(List<MaskExpression> terms) -> terms.stream().allMatch(NormalizedIrValidator::isNormalizedMask);
        };
    }
}
