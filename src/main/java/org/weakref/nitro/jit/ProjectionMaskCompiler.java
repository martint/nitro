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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.jit.ProjectionProgramBuilder.ArgumentNull;
import org.weakref.nitro.jit.ProjectionProgramBuilder.ArgumentValue;
import org.weakref.nitro.jit.ProjectionProgramBuilder.Binary;
import org.weakref.nitro.jit.ProjectionProgramBuilder.BinaryOperation;
import org.weakref.nitro.jit.ProjectionProgramBuilder.Program;
import org.weakref.nitro.jit.ProjectionProgramBuilder.Utf8Equal;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.MaskOutcome;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Compiles provider-authored projection semantics to engine-owned mask programs.
 *
 * <p>Function identity, logical signature, and the association between a provider and an operation remain outside
 * the engine. The compiler admits physical IR shapes and returns an opaque executable whose type and arity are not
 * visible to the evaluator.
 */
public final class ProjectionMaskCompiler
{
    private final DynamicKernelFactory dynamicKernelFactory;
    private volatile Utf8DynamicMaskKernel utf8DynamicKernel;

    public ProjectionMaskCompiler()
    {
        this(Utf8DynamicMaskKernelGenerator::generate);
    }

    ProjectionMaskCompiler(DynamicKernelFactory dynamicKernelFactory)
    {
        this.dynamicKernelFactory = requireNonNull(dynamicKernelFactory, "dynamicKernelFactory is null");
    }

    public Optional<CompiledMask> tryCompile(
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
        if (program.argumentTypes().size() != 2 ||
                program.argumentTypes().get(0) != program.argumentTypes().get(1) ||
                !(program.isNull() instanceof Binary nulls) ||
                nulls.operation() != BinaryOperation.BOOLEAN_OR ||
                !isArgumentNull(nulls.left(), 0) ||
                !isArgumentNull(nulls.right(), 1)) {
            return Optional.empty();
        }
        if (program.value() instanceof Utf8Equal equality) {
            return compileUtf8LiteralEquality(program, equality, arguments);
        }
        if (!(program.value() instanceof Binary comparison) ||
                !(comparison.left() instanceof ArgumentValue left) ||
                !(comparison.right() instanceof ArgumentValue right) ||
                left.index() != 0 ||
                right.index() != 1 ||
                left.type() != program.argumentTypes().get(0) ||
                right.type() != program.argumentTypes().get(1)) {
            return Optional.empty();
        }
        return switch (program.argumentTypes().get(0)) {
            case F64 -> compileDoubleComparison(comparison.operation());
            case I64 -> compileLongComparison(comparison.operation());
            default -> Optional.empty();
        };
    }

    private Optional<CompiledMask> compileUtf8LiteralEquality(
            Program program,
            Utf8Equal equality,
            List<ProjectionArgument> arguments)
    {
        if (program.argumentTypes().getFirst() != ProjectionProgramBuilder.ValueType.UTF8 ||
                !(equality.left() instanceof ArgumentValue left) ||
                !(equality.right() instanceof ArgumentValue right) ||
                left.index() != 0 ||
                right.index() != 1 ||
                left.type() != ProjectionProgramBuilder.ValueType.UTF8 ||
                right.type() != ProjectionProgramBuilder.ValueType.UTF8) {
            return Optional.empty();
        }

        int inputIndex;
        String literal;
        if (arguments.get(0).kind() == ProjectionArgument.Kind.INPUT &&
                arguments.get(1).kind() == ProjectionArgument.Kind.LITERAL &&
                arguments.get(1).literal() instanceof String value) {
            inputIndex = 0;
            literal = value;
        }
        else if (arguments.get(1).kind() == ProjectionArgument.Kind.INPUT &&
                arguments.get(0).kind() == ProjectionArgument.Kind.LITERAL &&
                arguments.get(0).literal() instanceof String value) {
            inputIndex = 1;
            literal = value;
        }
        else {
            Set<Stream> streams = Set.of(Stream.VALUES, Stream.NULLS);
            return Optional.of(new Utf8DynamicMask(
                    List.of(streams, streams),
                    List.of(
                            new ArgumentComponent(0, Stream.ERRORS),
                            new ArgumentComponent(1, Stream.ERRORS)),
                    utf8DynamicKernel()));
        }

        List<Set<Stream>> requiredStreams = inputIndex == 0
                ? List.of(Set.of(Stream.VALUES), Set.of())
                : List.of(Set.of(), Set.of(Stream.VALUES));
        return Optional.of(new Utf8LiteralMask(
                inputIndex,
                requiredStreams,
                List.of(
                        new ArgumentComponent(inputIndex, Stream.NULLS),
                        new ArgumentComponent(inputIndex, Stream.ERRORS)),
                literal.getBytes(UTF_8)));
    }

    private static Optional<CompiledMask> compileDoubleComparison(BinaryOperation operation)
    {
        return switch (operation) {
            case EQUAL -> Optional.of(doubleComparison((first, second) -> first == second, Mask.ComparisonOperator.EQUAL));
            case LESS_THAN -> Optional.of(doubleComparison((first, second) -> first < second, Mask.ComparisonOperator.LESS_THAN));
            case LESS_THAN_OR_EQUAL -> Optional.of(doubleComparison((first, second) -> first <= second, Mask.ComparisonOperator.LESS_THAN_OR_EQUAL));
            case GREATER_THAN -> Optional.of(doubleComparison((first, second) -> first > second, Mask.ComparisonOperator.GREATER_THAN));
            case GREATER_THAN_OR_EQUAL -> Optional.of(doubleComparison((first, second) -> first >= second, Mask.ComparisonOperator.GREATER_THAN_OR_EQUAL));
            default -> Optional.empty();
        };
    }

    private static Optional<CompiledMask> compileLongComparison(BinaryOperation operation)
    {
        return switch (operation) {
            case EQUAL -> Optional.of(longComparison((first, second) -> first == second, Mask.ComparisonOperator.EQUAL));
            case LESS_THAN -> Optional.of(longComparison((first, second) -> first < second, Mask.ComparisonOperator.LESS_THAN));
            default -> Optional.empty();
        };
    }

    private static CompiledMask doubleComparison(
            DoubleComparisonMaskSupport.ComparisonKernel kernel,
            Mask.ComparisonOperator operator)
    {
        return new DoubleComparisonMask(
                2,
                List.of(Set.of(Stream.VALUES), Set.of(Stream.VALUES)),
                List.of(
                        new ArgumentComponent(0, Stream.NULLS),
                        new ArgumentComponent(0, Stream.ERRORS),
                        new ArgumentComponent(1, Stream.NULLS),
                        new ArgumentComponent(1, Stream.ERRORS)),
                kernel,
                operator);
    }

    private static CompiledMask longComparison(
            LongComparisonMaskSupport.ComparisonKernel kernel,
            Mask.ComparisonOperator operator)
    {
        Set<Stream> streams = Set.of(Stream.VALUES, Stream.NULLS);
        return new LongComparisonMask(
                2,
                List.of(streams, streams),
                List.of(
                        new ArgumentComponent(0, Stream.ERRORS),
                        new ArgumentComponent(1, Stream.ERRORS)),
                kernel,
                operator);
    }

    private static boolean isArgumentNull(ProjectionProgramBuilder.Expression expression, int index)
    {
        return expression instanceof ArgumentNull argumentNull && argumentNull.index() == index;
    }

    private Utf8DynamicMaskKernel utf8DynamicKernel()
    {
        Utf8DynamicMaskKernel kernel = utf8DynamicKernel;
        if (kernel != null) {
            return kernel;
        }
        return createUtf8DynamicKernel();
    }

    private synchronized Utf8DynamicMaskKernel createUtf8DynamicKernel()
    {
        if (utf8DynamicKernel == null) {
            utf8DynamicKernel = dynamicKernelFactory.create();
        }
        return utf8DynamicKernel;
    }

    @FunctionalInterface
    interface DynamicKernelFactory
    {
        Utf8DynamicMaskKernel create();
    }

    public abstract static class CompiledMask
    {
        private final int argumentCount;
        private final List<Set<Stream>> requiredInputStreams;
        private final List<ArgumentComponent> excludedComponents;

        private CompiledMask(
                int argumentCount,
                List<Set<Stream>> requiredInputStreams,
                List<ArgumentComponent> excludedComponents)
        {
            this.argumentCount = argumentCount;
            this.requiredInputStreams = List.copyOf(requiredInputStreams);
            this.excludedComponents = List.copyOf(excludedComponents);
        }

        public final int argumentCount()
        {
            return argumentCount;
        }

        public final Set<Stream> requiredInputStreams(int argumentIndex)
        {
            return requiredInputStreams.get(argumentIndex);
        }

        public final List<ArgumentComponent> excludedComponents()
        {
            return excludedComponents;
        }

        public abstract boolean evaluate(List<Streams> inputs, Mask mask, boolean selectTrue);

        public Mask evaluateMask(
                List<Streams> inputs,
                Mask mask,
                boolean selectTrue,
                PrimitiveExecutionContext context,
                Allocator.Context allocationContext)
        {
            Mask result = context.allocator().copyMask(allocationContext, mask);
            if (evaluate(inputs, result, selectTrue)) {
                return result;
            }
            context.allocator().release(allocationContext, result);
            return null;
        }

        public MaskOutcome evaluateOutcome(
                List<Streams> inputs,
                Mask mask,
                PrimitiveExecutionContext context,
                Allocator.Context allocationContext)
        {
            return null;
        }
    }

    public record ArgumentComponent(int argumentIndex, Stream stream) {}

    private static final class DoubleComparisonMask
            extends CompiledMask
    {
        private final DoubleComparisonMaskSupport.ComparisonKernel kernel;
        private final Mask.ComparisonOperator operator;

        private DoubleComparisonMask(
                int argumentCount,
                List<Set<Stream>> requiredInputStreams,
                List<ArgumentComponent> excludedComponents,
                DoubleComparisonMaskSupport.ComparisonKernel kernel,
                Mask.ComparisonOperator operator)
        {
            super(argumentCount, requiredInputStreams, excludedComponents);
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

    private static final class LongComparisonMask
            extends CompiledMask
    {
        private final LongComparisonMaskSupport.ComparisonKernel kernel;
        private final Mask.ComparisonOperator operator;

        private LongComparisonMask(
                int argumentCount,
                List<Set<Stream>> requiredInputStreams,
                List<ArgumentComponent> excludedComponents,
                LongComparisonMaskSupport.ComparisonKernel kernel,
                Mask.ComparisonOperator operator)
        {
            super(argumentCount, requiredInputStreams, excludedComponents);
            this.kernel = kernel;
            this.operator = operator;
        }

        @Override
        public boolean evaluate(List<Streams> inputs, Mask mask, boolean selectTrue)
        {
            return selectTrue
                    ? LongComparisonMaskSupport.tryEvaluateTrueMaskInPlace(inputs, mask, kernel, operator)
                    : LongComparisonMaskSupport.tryEvaluateFalseMaskInPlace(inputs, mask, kernel, operator);
        }

        @Override
        public Mask evaluateMask(
                List<Streams> inputs,
                Mask mask,
                boolean selectTrue,
                PrimitiveExecutionContext context,
                Allocator.Context allocationContext)
        {
            return selectTrue
                    ? LongComparisonMaskSupport.tryEvaluateTrueMask(inputs, mask, context, allocationContext, kernel, operator)
                    : LongComparisonMaskSupport.tryEvaluateFalseMask(inputs, mask, context, allocationContext, kernel, operator);
        }

        @Override
        public MaskOutcome evaluateOutcome(
                List<Streams> inputs,
                Mask mask,
                PrimitiveExecutionContext context,
                Allocator.Context allocationContext)
        {
            return LongComparisonMaskSupport.tryEvaluateMaskOutcome(inputs, mask, context, allocationContext, kernel);
        }
    }

    private static final class Utf8LiteralMask
            extends CompiledMask
    {
        private final int inputIndex;
        private final Utf8LiteralMaskSupport support;

        private Utf8LiteralMask(
                int inputIndex,
                List<Set<Stream>> requiredInputStreams,
                List<ArgumentComponent> excludedComponents,
                byte[] literal)
        {
            super(2, requiredInputStreams, excludedComponents);
            this.inputIndex = inputIndex;
            support = new Utf8LiteralMaskSupport(literal);
        }

        @Override
        public boolean evaluate(List<Streams> inputs, Mask mask, boolean selectTrue)
        {
            return support.evaluate(inputs.get(inputIndex), mask, selectTrue);
        }
    }

    private static final class Utf8DynamicMask
            extends CompiledMask
    {
        private final Utf8DynamicMaskSupport support;

        private Utf8DynamicMask(
                List<Set<Stream>> requiredInputStreams,
                List<ArgumentComponent> excludedComponents,
                Utf8DynamicMaskKernel kernel)
        {
            super(2, requiredInputStreams, excludedComponents);
            support = new Utf8DynamicMaskSupport(kernel);
        }

        @Override
        public boolean evaluate(List<Streams> inputs, Mask mask, boolean selectTrue)
        {
            return support.evaluate(inputs.get(0), inputs.get(1), mask, selectTrue);
        }
    }
}
