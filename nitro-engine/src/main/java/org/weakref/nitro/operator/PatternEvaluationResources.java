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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.jit.ProjectionMaskCompiler;
import org.weakref.nitro.operator.evaluator.PlanEvaluator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.pattern.PatternAggregationInput;
import org.weakref.nitro.operator.pattern.PatternDefinition;
import org.weakref.nitro.operator.pattern.PatternDefinitionErrorHandler;
import org.weakref.nitro.operator.pattern.PatternEvaluationContext;
import org.weakref.nitro.operator.pattern.PatternValueEvaluator;
import org.weakref.nitro.operator.pattern.PatternValueProgram;

import static java.util.Objects.requireNonNull;

/// Engine-owner-scoped resources for evaluating expressions in row-pattern execution.
public final class PatternEvaluationResources
{
    private final ProjectionMaskCompiler projectionMaskCompiler;
    private final EvaluationOperatorPolicy evaluationPolicy;
    private final Object bufferPoolGroup;

    PatternEvaluationResources(
            ProjectionMaskCompiler projectionMaskCompiler,
            EvaluationOperatorPolicy evaluationPolicy,
            Object bufferPoolGroup)
    {
        this.projectionMaskCompiler = requireNonNull(projectionMaskCompiler, "projectionMaskCompiler is null");
        this.evaluationPolicy = requireNonNull(evaluationPolicy, "evaluationPolicy is null");
        this.bufferPoolGroup = requireNonNull(bufferPoolGroup, "bufferPoolGroup is null");
    }

    /// Creates a reusable aggregate-argument view whose outputs remain in each retained source's physical domain.
    public PatternAggregationInput aggregationInput(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            int[] inputColumns,
            Schema outputSchema)
    {
        return new ExpressionAggregationInput(
                plan,
                primitiveRegistry,
                inputColumns,
                outputSchema,
                projectionMaskCompiler,
                evaluationPolicy,
                bufferPoolGroup);
    }

    /// Binds a compiled boolean expression to match-local value producers.
    ///
    /// The returned definition owns reusable one-row input and expression state. Its error handler remains a host
    /// boundary concern so the engine does not acquire knowledge of provider exception classes.
    public PatternDefinition definition(
            Allocator allocator,
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            PatternValueProgram inputs,
            Reference predicate,
            PatternDefinitionErrorHandler errorHandler)
    {
        return new ExpressionDefinition(
                allocator,
                plan,
                primitiveRegistry,
                inputs,
                predicate,
                errorHandler,
                projectionMaskCompiler,
                evaluationPolicy,
                bufferPoolGroup);
    }

    /// Binds one compiled measure expression to its match-local value producers.
    public PatternValueEvaluator value(
            Allocator allocator,
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            PatternValueProgram inputs,
            Reference value)
    {
        return new ExpressionValue(
                allocator,
                plan,
                primitiveRegistry,
                inputs,
                value,
                projectionMaskCompiler,
                evaluationPolicy,
                bufferPoolGroup);
    }

    private static final class ExpressionValue
            implements PatternValueEvaluator
    {
        private final Allocator.Context allocationContext = new Allocator.Context("PatternValue", ExpressionValue.class);
        private final Allocator allocator;
        private final PatternValueProgram inputs;
        private final Reference value;
        private final Streams[] inputColumns;
        private final Mask selected;
        private final PlanEvaluator evaluator;
        private boolean evaluated;
        private boolean closed;

        private ExpressionValue(
                Allocator allocator,
                EvaluationPlan plan,
                PrimitiveRegistry primitiveRegistry,
                PatternValueProgram inputs,
                Reference value,
                ProjectionMaskCompiler projectionMaskCompiler,
                EvaluationOperatorPolicy evaluationPolicy,
                Object bufferPoolGroup)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.inputs = requireNonNull(inputs, "inputs is null");
            this.value = requireNonNull(value, "value is null");
            inputColumns = new Streams[inputs.size()];
            selected = allocator.allocateRangeMask(allocationContext, 0, 1);
            evaluator = new PlanEvaluator(
                    requireNonNull(plan, "plan is null"),
                    requireNonNull(primitiveRegistry, "primitiveRegistry is null"),
                    this::resolveInput,
                    allocator,
                    requireNonNull(projectionMaskCompiler, "projectionMaskCompiler is null"),
                    requireNonNull(evaluationPolicy, "evaluationPolicy is null"),
                    requireNonNull(bufferPoolGroup, "bufferPoolGroup is null"),
                    true);
        }

        @Override
        public Streams append(
                PatternEvaluationContext context,
                Allocator outputAllocator,
                Allocator.Context outputContext,
                Streams output,
                int outputPosition,
                int outputSize)
        {
            if (closed) {
                throw new IllegalStateException("pattern value is closed");
            }
            if (outputAllocator != allocator) {
                throw new IllegalArgumentException("pattern value cannot change allocator");
            }
            if (evaluated) {
                evaluator.resetForReuse();
            }
            inputs.append(context, allocator, allocationContext, inputColumns, 0, 1);
            Streams result = evaluator.evaluate(value, selected);
            Streams copied = allocator.copySinglePositionInto(
                    requireNonNull(outputContext, "outputContext is null"),
                    result,
                    requireNonNull(output, "output is null"),
                    0,
                    outputPosition,
                    outputSize);
            evaluated = true;
            return copied;
        }

        private Vector resolveInput(Reference reference, Mask mask)
        {
            return switch (reference.producer()) {
                case Input(int input) -> inputColumns[input].getOrNull(reference.stream());
                default -> throw new IllegalArgumentException("Unexpected pattern-value input: " + reference);
            };
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            RuntimeException failure = null;
            try {
                if (evaluated) {
                    evaluator.resetForReuse();
                }
                evaluator.close();
            }
            catch (RuntimeException closeFailure) {
                failure = closeFailure;
            }
            try {
                inputs.close();
            }
            catch (RuntimeException closeFailure) {
                failure = appendFailure(failure, closeFailure);
            }
            try {
                allocator.release(allocationContext);
            }
            catch (RuntimeException closeFailure) {
                failure = appendFailure(failure, closeFailure);
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static RuntimeException appendFailure(RuntimeException failure, RuntimeException closeFailure)
    {
        if (failure == null) {
            return closeFailure;
        }
        failure.addSuppressed(closeFailure);
        return failure;
    }

    private static final class ExpressionDefinition
            implements PatternDefinition
    {
        private final Allocator.Context allocationContext = new Allocator.Context("PatternDefinition", ExpressionDefinition.class);
        private final Allocator allocator;
        private final PatternValueProgram inputs;
        private final Reference predicate;
        private final PatternDefinitionErrorHandler errorHandler;
        private final Streams[] inputColumns;
        private final Mask selected;
        private final PlanEvaluator evaluator;
        private boolean evaluated;
        private boolean closed;

        private ExpressionDefinition(
                Allocator allocator,
                EvaluationPlan plan,
                PrimitiveRegistry primitiveRegistry,
                PatternValueProgram inputs,
                Reference predicate,
                PatternDefinitionErrorHandler errorHandler,
                ProjectionMaskCompiler projectionMaskCompiler,
                EvaluationOperatorPolicy evaluationPolicy,
                Object bufferPoolGroup)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.inputs = requireNonNull(inputs, "inputs is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
            this.errorHandler = requireNonNull(errorHandler, "errorHandler is null");
            inputColumns = new Streams[inputs.size()];
            selected = allocator.allocateRangeMask(allocationContext, 0, 1);
            evaluator = new PlanEvaluator(
                    requireNonNull(plan, "plan is null"),
                    requireNonNull(primitiveRegistry, "primitiveRegistry is null"),
                    this::resolveInput,
                    allocator,
                    requireNonNull(projectionMaskCompiler, "projectionMaskCompiler is null"),
                    requireNonNull(evaluationPolicy, "evaluationPolicy is null"),
                    requireNonNull(bufferPoolGroup, "bufferPoolGroup is null"),
                    true);
        }

        @Override
        public boolean matches(PatternEvaluationContext context)
        {
            if (closed) {
                throw new IllegalStateException("pattern definition is closed");
            }
            if (evaluated) {
                evaluator.resetForReuse();
            }
            inputs.append(context, allocator, allocationContext, inputColumns, 0, 1);
            Streams result = evaluator.evaluate(predicate, selected);
            errorHandler.check(result.getOrNull(Stream.ERRORS), 0);
            evaluated = true;
            Vector nulls = result.getOrNull(Stream.NULLS);
            return (nulls == null || !org.weakref.nitro.data.VectorAccess.booleanValues(nulls).value(0)) &&
                    org.weakref.nitro.data.VectorAccess.booleanValues(result.values()).value(0);
        }

        private Vector resolveInput(Reference reference, Mask mask)
        {
            return switch (reference.producer()) {
                case Input(int input) -> inputColumns[input].getOrNull(reference.stream());
                default -> throw new IllegalArgumentException("Unexpected pattern-definition input: " + reference);
            };
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            RuntimeException failure = null;
            try {
                if (evaluated) {
                    evaluator.resetForReuse();
                }
                evaluator.close();
            }
            catch (RuntimeException closeFailure) {
                failure = closeFailure;
            }
            try {
                inputs.close();
            }
            catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                }
                else {
                    failure.addSuppressed(closeFailure);
                }
            }
            try {
                allocator.release(allocationContext);
            }
            catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                }
                else {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class ExpressionAggregationInput
            implements PatternAggregationInput
    {
        private final EvaluationPlan plan;
        private final PrimitiveRegistry primitiveRegistry;
        private final int[] inputColumns;
        private final Schema outputSchema;
        private final ProjectionMaskCompiler projectionMaskCompiler;
        private final EvaluationOperatorPolicy evaluationPolicy;
        private final Object bufferPoolGroup;
        private final int[] selectedPosition = new int[1];
        private final Streams[] outputs;

        private Allocator allocator;
        private Allocator.Context allocationContext;
        private PlanEvaluator evaluator;
        private PatternEvaluationContext context;
        private Mask selectedMask;
        private int position;
        private int physicalPosition;
        private int physicalSize;
        private boolean evaluated;
        private boolean closed;

        private ExpressionAggregationInput(
                EvaluationPlan plan,
                PrimitiveRegistry primitiveRegistry,
                int[] inputColumns,
                Schema outputSchema,
                ProjectionMaskCompiler projectionMaskCompiler,
                EvaluationOperatorPolicy evaluationPolicy,
                Object bufferPoolGroup)
        {
            this.plan = requireNonNull(plan, "plan is null");
            this.primitiveRegistry = requireNonNull(primitiveRegistry, "primitiveRegistry is null");
            this.inputColumns = requireNonNull(inputColumns, "inputColumns is null").clone();
            this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
            this.projectionMaskCompiler = requireNonNull(projectionMaskCompiler, "projectionMaskCompiler is null");
            this.evaluationPolicy = requireNonNull(evaluationPolicy, "evaluationPolicy is null");
            this.bufferPoolGroup = requireNonNull(bufferPoolGroup, "bufferPoolGroup is null");
            if (plan.outputs().size() != outputSchema.size()) {
                throw new IllegalArgumentException("plan outputs and schema have different arities");
            }
            outputs = new Streams[plan.outputs().size()];
        }

        @Override
        public Schema schema()
        {
            return outputSchema;
        }

        @Override
        public void initialize(Allocator allocator, Allocator.Context allocationContext)
        {
            if (closed) {
                throw new IllegalStateException("pattern expression input is closed");
            }
            if (evaluator != null) {
                if (this.allocator != allocator || this.allocationContext != allocationContext) {
                    throw new IllegalArgumentException("pattern expression input cannot change allocator context");
                }
                return;
            }
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.allocationContext = requireNonNull(allocationContext, "allocationContext is null");
            evaluator = new PlanEvaluator(
                    plan,
                    primitiveRegistry,
                    this::resolveInput,
                    allocator,
                    projectionMaskCompiler,
                    evaluationPolicy,
                    bufferPoolGroup,
                    true);
        }

        @Override
        public void reset(PatternEvaluationContext context, int position, int labelOrdinal)
        {
            if (evaluator == null) {
                throw new IllegalStateException("pattern expression input is not initialized");
            }
            if (evaluated) {
                evaluator.resetForReuse();
            }
            this.context = requireNonNull(context, "context is null");
            this.position = position;
            physicalPosition = context.rows().sourcePosition(position);
            physicalSize = context.rows().sourceSize(position);
            selectedPosition[0] = physicalPosition;
            if (selectedMask == null) {
                selectedMask = allocator.allocateSparseMask(allocationContext, selectedPosition, 1, physicalSize);
            }
            else {
                allocator.overwriteSparseMask(allocationContext, selectedMask, selectedPosition, 1, physicalSize);
            }
            for (int output = 0; output < outputs.length; output++) {
                outputs[output] = evaluator.evaluate(plan.outputs().get(output), selectedMask);
            }
            evaluated = true;
        }

        @Override
        public Vector stream(int input, Stream stream)
        {
            requireEvaluated();
            return outputs[input].getOrNull(stream);
        }

        @Override
        public int physicalPosition()
        {
            requireEvaluated();
            return physicalPosition;
        }

        @Override
        public int physicalSize()
        {
            requireEvaluated();
            return physicalSize;
        }

        private Vector resolveInput(Reference reference, Mask mask)
        {
            return switch (reference.producer()) {
                case Input(int input) -> context.rows().column(inputColumns[input], position).getOrNull(reference.stream());
                default -> throw new IllegalArgumentException("Unexpected pattern expression input: " + reference);
            };
        }

        private void requireEvaluated()
        {
            if (!evaluated) {
                throw new IllegalStateException("pattern expression input is not positioned");
            }
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (evaluator != null) {
                if (evaluated) {
                    evaluator.resetForReuse();
                }
                evaluator.close();
            }
            if (selectedMask != null) {
                allocator.release(allocationContext, selectedMask);
                selectedMask = null;
            }
        }
    }
}
