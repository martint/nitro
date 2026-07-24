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

import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder;
import org.weakref.nitro.core.function.projection.ProjectionProgram;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Engine implementation of the provider-facing projection builder.
 *
 * <p>The nodes below are physical expression primitives. Function selection, signature, arity, shape admission, and
 * null behavior remain in the provider that composes them.
 */
final class ProjectionProgramBuilder
        implements ProjectionCodeBuilder
{
    private final Object owner = new Object();

    @Override
    public Value argument(int index, ValueType type)
    {
        if (index < 0) {
            throw new IllegalArgumentException("argument index is negative");
        }
        ValueType valueType = requireNonNull(type, "type is null");
        if (valueType == ValueType.NULLS_ONLY) {
            throw new IllegalArgumentException("NULLS_ONLY argument has no value");
        }
        return new ArgumentValue(index, valueType);
    }

    @Override
    public Value isNull(int index)
    {
        if (index < 0) {
            throw new IllegalArgumentException("argument index is negative");
        }
        return new ArgumentNull(index);
    }

    @Override
    public Value constant(boolean value)
    {
        return new BooleanConstant(value);
    }

    @Override
    public Value add(Value left, Value right)
    {
        return arithmetic(BinaryOperation.ADD, left, right);
    }

    @Override
    public Value subtract(Value left, Value right)
    {
        return arithmetic(BinaryOperation.SUBTRACT, left, right);
    }

    @Override
    public Value multiply(Value left, Value right)
    {
        return arithmetic(BinaryOperation.MULTIPLY, left, right);
    }

    @Override
    public Value lessThan(Value left, Value right)
    {
        return comparison(BinaryOperation.LESS_THAN, left, right);
    }

    @Override
    public Value greaterThan(Value left, Value right)
    {
        return comparison(BinaryOperation.GREATER_THAN, left, right);
    }

    @Override
    public Value lessThanOrEqual(Value left, Value right)
    {
        return comparison(BinaryOperation.LESS_THAN_OR_EQUAL, left, right);
    }

    @Override
    public Value greaterThanOrEqual(Value left, Value right)
    {
        return comparison(BinaryOperation.GREATER_THAN_OR_EQUAL, left, right);
    }

    @Override
    public Value equal(Value left, Value right)
    {
        return comparison(BinaryOperation.EQUAL, left, right);
    }

    @Override
    public Value utf8Equal(Value left, Value right)
    {
        Expression leftExpression = expression(left);
        Expression rightExpression = expression(right);
        requireType(leftExpression, ValueType.UTF8);
        requireType(rightExpression, ValueType.UTF8);
        return new Utf8Equal(leftExpression, rightExpression);
    }

    @Override
    public Value and(Value left, Value right)
    {
        return booleanBinary(BinaryOperation.BOOLEAN_AND, left, right);
    }

    @Override
    public Value or(Value left, Value right)
    {
        return booleanBinary(BinaryOperation.BOOLEAN_OR, left, right);
    }

    @Override
    public Value not(Value value)
    {
        Expression expression = expression(value);
        requireType(expression, ValueType.BOOLEAN);
        return new BooleanNot(expression);
    }

    @Override
    public Value conditional(Value condition, Value whenTrue, Value whenFalse)
    {
        Expression conditionExpression = expression(condition);
        Expression trueExpression = expression(whenTrue);
        Expression falseExpression = expression(whenFalse);
        requireType(conditionExpression, ValueType.BOOLEAN);
        if (trueExpression.type() != falseExpression.type()) {
            throw new IllegalArgumentException("conditional branches have different types");
        }
        return new Conditional(conditionExpression, trueExpression, falseExpression, trueExpression.type());
    }

    @Override
    public ProjectionProgram program(
            List<ValueType> argumentTypes,
            Value value,
            Value isNull)
    {
        List<ValueType> types = List.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        Expression valueExpression = expression(value);
        Expression nullExpression = expression(isNull);
        requireType(nullExpression, ValueType.BOOLEAN);
        validateArguments(valueExpression, types);
        validateArguments(nullExpression, types);
        return new Program(owner, types, valueExpression, nullExpression);
    }

    Program requireProgram(ProjectionProgram program)
    {
        if (!(program instanceof Program internal) || internal.owner() != owner) {
            throw new IllegalArgumentException("projection provider returned a program not created by the supplied builder");
        }
        return internal;
    }

    private static Expression arithmetic(BinaryOperation operation, Value left, Value right)
    {
        Expression leftExpression = expression(left);
        Expression rightExpression = expression(right);
        if (leftExpression.type() != rightExpression.type() ||
                (leftExpression.type() != ValueType.I64 && leftExpression.type() != ValueType.F64)) {
            throw new IllegalArgumentException("arithmetic operands must have the same numeric type");
        }
        return new Binary(operation, leftExpression, rightExpression, leftExpression.type());
    }

    private static Expression comparison(BinaryOperation operation, Value left, Value right)
    {
        Expression leftExpression = expression(left);
        Expression rightExpression = expression(right);
        if (leftExpression.type() != rightExpression.type() ||
                (leftExpression.type() != ValueType.I64 && leftExpression.type() != ValueType.F64)) {
            throw new IllegalArgumentException("comparison operands must have the same numeric type");
        }
        return new Binary(operation, leftExpression, rightExpression, ValueType.BOOLEAN);
    }

    private static Expression booleanBinary(BinaryOperation operation, Value left, Value right)
    {
        Expression leftExpression = expression(left);
        Expression rightExpression = expression(right);
        requireType(leftExpression, ValueType.BOOLEAN);
        requireType(rightExpression, ValueType.BOOLEAN);
        return new Binary(operation, leftExpression, rightExpression, ValueType.BOOLEAN);
    }

    private static Expression expression(Value value)
    {
        if (!(requireNonNull(value, "value is null") instanceof Expression expression)) {
            throw new IllegalArgumentException("value was not created by the supplied builder");
        }
        return expression;
    }

    private static void requireType(Expression expression, ValueType type)
    {
        if (expression.type() != type) {
            throw new IllegalArgumentException("expected " + type + " but got " + expression.type());
        }
    }

    private static void validateArguments(Expression expression, List<ValueType> argumentTypes)
    {
        switch (expression) {
            case ArgumentValue argument -> {
                if (argument.index() >= argumentTypes.size() ||
                        argumentTypes.get(argument.index()) != argument.type()) {
                    throw new IllegalArgumentException("argument reference does not match program signature");
                }
            }
            case ArgumentNull argument -> {
                if (argument.index() >= argumentTypes.size()) {
                    throw new IllegalArgumentException("null reference does not match program signature");
                }
            }
            case BooleanConstant _ -> {}
            case Binary binary -> {
                validateArguments(binary.left(), argumentTypes);
                validateArguments(binary.right(), argumentTypes);
            }
            case BooleanNot not -> validateArguments(not.value(), argumentTypes);
            case Conditional conditional -> {
                validateArguments(conditional.condition(), argumentTypes);
                validateArguments(conditional.whenTrue(), argumentTypes);
                validateArguments(conditional.whenFalse(), argumentTypes);
            }
            case Utf8Equal equal -> {
                validateArguments(equal.left(), argumentTypes);
                validateArguments(equal.right(), argumentTypes);
            }
        }
    }

    sealed interface Expression
            extends Value
            permits ArgumentValue, ArgumentNull, BooleanConstant, Binary, BooleanNot, Conditional, Utf8Equal {}

    record ArgumentValue(int index, ValueType type)
            implements Expression {}

    record ArgumentNull(int index)
            implements Expression
    {
        @Override
        public ValueType type()
        {
            return ValueType.BOOLEAN;
        }
    }

    record BooleanConstant(boolean value)
            implements Expression
    {
        @Override
        public ValueType type()
        {
            return ValueType.BOOLEAN;
        }
    }

    record Binary(
            BinaryOperation operation,
            Expression left,
            Expression right,
            ValueType type)
            implements Expression {}

    record BooleanNot(Expression value)
            implements Expression
    {
        @Override
        public ValueType type()
        {
            return ValueType.BOOLEAN;
        }
    }

    record Conditional(
            Expression condition,
            Expression whenTrue,
            Expression whenFalse,
            ValueType type)
            implements Expression {}

    record Utf8Equal(Expression left, Expression right)
            implements Expression
    {
        @Override
        public ValueType type()
        {
            return ValueType.BOOLEAN;
        }
    }

    record Program(
            Object owner,
            List<ValueType> argumentTypes,
            Expression value,
            Expression isNull)
            implements ProjectionProgram {}

    enum BinaryOperation
    {
        ADD,
        SUBTRACT,
        MULTIPLY,
        LESS_THAN,
        GREATER_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN_OR_EQUAL,
        EQUAL,
        BOOLEAN_AND,
        BOOLEAN_OR
    }
}
