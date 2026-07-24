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
package org.weakref.nitro.core.function.projection;

import java.util.List;

/**
 * Classloader-neutral expression builder offered to a function provider.
 *
 * <p>These operations are physical code-generation primitives, not function identities. A provider composes them to
 * define its own behavior and supplies the complete argument signature when it completes a program.
 */
public interface ProjectionCodeBuilder
{
    Value argument(int index, ValueType type);

    Value isNull(int index);

    Value constant(boolean value);

    Value add(Value left, Value right);

    Value subtract(Value left, Value right);

    Value multiply(Value left, Value right);

    Value lessThan(Value left, Value right);

    Value greaterThan(Value left, Value right);

    Value lessThanOrEqual(Value left, Value right);

    Value greaterThanOrEqual(Value left, Value right);

    Value equal(Value left, Value right);

    Value utf8Equal(Value left, Value right);

    Value utf8StartsWith(Value value, Value prefix);

    Value and(Value left, Value right);

    Value or(Value left, Value right);

    Value not(Value value);

    Value conditional(Value condition, Value whenTrue, Value whenFalse);

    ProjectionProgram program(List<ValueType> argumentTypes, Value value, Value isNull);

    interface Value
    {
        ValueType type();
    }

    enum ValueType
    {
        I64,
        F64,
        BOOLEAN,
        UTF8,
        /**
         * An argument whose projection program reads only {@link ProjectionCodeBuilder#isNull(int)}.
         *
         * <p>This is an input access shape rather than a value type. It lets a provider describe null-only behavior
         * without inventing a value carrier or making the engine aware of the function's logical type.
         */
        NULLS_ONLY
    }
}
