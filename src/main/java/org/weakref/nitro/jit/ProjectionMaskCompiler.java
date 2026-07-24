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
package org.weakref.nitro.jit;

import org.weakref.nitro.core.function.mask.MaskCodeProvider;
import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder.ValueType;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.jit.ProjectionProgramBuilder.ArgumentNull;
import org.weakref.nitro.jit.ProjectionProgramBuilder.ArgumentValue;
import org.weakref.nitro.jit.ProjectionProgramBuilder.Binary;
import org.weakref.nitro.jit.ProjectionProgramBuilder.BinaryOperation;
import org.weakref.nitro.jit.ProjectionProgramBuilder.Program;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Optional;

/**
 * Compiles provider-authored projection semantics to engine-owned mask programs.
 *
 * <p>The first admitted physical shape is a null-propagating comparison of two F64 arguments. Function identity,
 * logical signature, and the association between a provider and an operation remain outside the engine.
 */
public final class ProjectionMaskCompiler
{
    private ProjectionMaskCompiler() {}

    public static Optional<CompiledMask> tryCompile(
            MaskCodeProvider provider,
            List<ProjectionArgument> arguments)
    {
        ProjectionProgramBuilder builder = new ProjectionProgramBuilder();
        Program program;
        try {
            program = builder.requireProgram(provider.generate(builder, arguments).orElseThrow());
        }
        catch (IllegalArgumentException | java.util.NoSuchElementException _) {
            return Optional.empty();
        }
        if (!program.argumentTypes().equals(List.of(ValueType.F64, ValueType.F64)) ||
                !(program.value() instanceof Binary comparison) ||
                !(comparison.left() instanceof ArgumentValue left) ||
                !(comparison.right() instanceof ArgumentValue right) ||
                left.index() != 0 ||
                right.index() != 1 ||
                left.type() != ValueType.F64 ||
                right.type() != ValueType.F64 ||
                !(program.isNull() instanceof Binary nulls) ||
                nulls.operation() != BinaryOperation.BOOLEAN_OR ||
                !isArgumentNull(nulls.left(), 0) ||
                !isArgumentNull(nulls.right(), 1)) {
            return Optional.empty();
        }
        return switch (comparison.operation()) {
            case EQUAL -> Optional.of(doubleComparison((first, second) -> first == second, Mask.ComparisonOperator.EQUAL));
            case LESS_THAN -> Optional.of(doubleComparison((first, second) -> first < second, Mask.ComparisonOperator.LESS_THAN));
            case LESS_THAN_OR_EQUAL -> Optional.of(doubleComparison((first, second) -> first <= second, Mask.ComparisonOperator.LESS_THAN_OR_EQUAL));
            case GREATER_THAN -> Optional.of(doubleComparison((first, second) -> first > second, Mask.ComparisonOperator.GREATER_THAN));
            case GREATER_THAN_OR_EQUAL -> Optional.of(doubleComparison((first, second) -> first >= second, Mask.ComparisonOperator.GREATER_THAN_OR_EQUAL));
            default -> Optional.empty();
        };
    }

    private static CompiledMask doubleComparison(
            DoubleComparisonMaskSupport.ComparisonKernel kernel,
            Mask.ComparisonOperator operator)
    {
        return new DoubleComparisonMask(
                2,
                List.of(
                        new ArgumentComponent(0, Stream.NULLS),
                        new ArgumentComponent(0, Stream.ERRORS),
                        new ArgumentComponent(1, Stream.NULLS),
                        new ArgumentComponent(1, Stream.ERRORS)),
                kernel,
                operator);
    }

    private static boolean isArgumentNull(ProjectionProgramBuilder.Expression expression, int index)
    {
        return expression instanceof ArgumentNull argumentNull && argumentNull.index() == index;
    }

    public abstract static class CompiledMask
    {
        private final int argumentCount;
        private final List<ArgumentComponent> excludedComponents;

        private CompiledMask(int argumentCount, List<ArgumentComponent> excludedComponents)
        {
            this.argumentCount = argumentCount;
            this.excludedComponents = List.copyOf(excludedComponents);
        }

        public final int argumentCount()
        {
            return argumentCount;
        }

        public final List<ArgumentComponent> excludedComponents()
        {
            return excludedComponents;
        }

        public abstract boolean evaluate(List<Streams> inputs, Mask mask, boolean selectTrue);
    }

    public record ArgumentComponent(int argumentIndex, Stream stream) {}

    private static final class DoubleComparisonMask
            extends CompiledMask
    {
        private final DoubleComparisonMaskSupport.ComparisonKernel kernel;
        private final Mask.ComparisonOperator operator;

        private DoubleComparisonMask(
                int argumentCount,
                List<ArgumentComponent> excludedComponents,
                DoubleComparisonMaskSupport.ComparisonKernel kernel,
                Mask.ComparisonOperator operator)
        {
            super(argumentCount, excludedComponents);
            this.kernel = kernel;
            this.operator = operator;
        }

        @Override
        public boolean evaluate(List<Streams> inputs, Mask mask, boolean selectTrue)
        {
            return selectTrue
                    ? DoubleComparisonMaskSupport.tryEvaluateTrueMaskInPlace(inputs, mask, kernel, operator)
                    : DoubleComparisonMaskSupport.tryEvaluateFalseMaskInPlace(inputs, mask, kernel, operator);
        }
    }
}
