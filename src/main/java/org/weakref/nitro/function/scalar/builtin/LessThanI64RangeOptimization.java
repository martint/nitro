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
package org.weakref.nitro.function.scalar.builtin;

import org.weakref.nitro.core.function.mask.MaskCodeProvider;
import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder;
import org.weakref.nitro.core.function.projection.ProjectionProgram;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.RangeBoundProvider;
import org.weakref.nitro.operator.evaluator.ir.RangeConstraint;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Optional;

/**
 * Registry-owned physical lowering for integral less-than bounds.
 *
 * <p>This component is deliberately separate from the hot scalar implementation. It owns the function's operand
 * semantics and the carrier-specific fused kernel; neither is part of the evaluator's vocabulary.
 */
public final class LessThanI64RangeOptimization
        implements RangeBoundProvider, RangeConstraint.Kernel, MaskCodeProvider
{
    @Override
    public Optional<ProjectionProgram> generate(ProjectionCodeBuilder builder, List<ProjectionArgument> arguments)
    {
        if (arguments.size() != 2) {
            return Optional.empty();
        }
        var left = builder.argument(0, ProjectionCodeBuilder.ValueType.I64);
        var right = builder.argument(1, ProjectionCodeBuilder.ValueType.I64);
        return Optional.of(builder.program(
                List.of(ProjectionCodeBuilder.ValueType.I64, ProjectionCodeBuilder.ValueType.I64),
                builder.lessThan(left, right),
                builder.or(builder.isNull(0), builder.isNull(1))));
    }

    @Override
    public Optional<RangeBound> rangeBound(List<Reference> arguments, LiteralResolver literals)
    {
        if (arguments.size() != 2) {
            return Optional.empty();
        }
        Optional<Object> left = literals.resolve(arguments.get(0));
        Optional<Object> right = literals.resolve(arguments.get(1));
        if (left.orElse(null) instanceof Long lower && right.isEmpty()) {
            return Optional.of(new RangeBound(
                    arguments.get(1),
                    lower,
                    RangeConstraint.Position.LOWER_EXCLUSIVE,
                    this));
        }
        if (right.orElse(null) instanceof Long upper && left.isEmpty()) {
            return Optional.of(new RangeBound(
                    arguments.get(0),
                    upper,
                    RangeConstraint.Position.UPPER_EXCLUSIVE,
                    this));
        }
        return Optional.empty();
    }

    @Override
    public boolean apply(Streams input, Object lowerExclusive, Object upperExclusive, Mask mask)
    {
        if (!(lowerExclusive instanceof Long lower) || !(upperExclusive instanceof Long upper) ||
                !VectorAccess.isAllFalseNulls(input.getOrNull(Stream.ERRORS))) {
            return false;
        }

        boolean[] nulls = null;
        Vector nullVector = input.getOrNull(Stream.NULLS);
        if (!VectorAccess.isAllFalseNulls(nullVector)) {
            nulls = VectorAccess.flatBooleans(nullVector);
            if (nulls == null) {
                return false;
            }
        }

        switch (input.values()) {
            case I64Vector values -> mask.retainConstantRange(values.values(), lower, upper, nulls);
            case I32Vector values -> mask.retainConstantRange(values.values(), lower, upper, nulls);
            default -> {
                return false;
            }
        }
        return true;
    }
}
