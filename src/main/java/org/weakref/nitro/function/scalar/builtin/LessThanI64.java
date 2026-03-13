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

import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.function.scalar.ScalarImplementation;
import org.weakref.nitro.function.scalar.generated.LessThanI64Primitive;

@ScalarFunction(name = "lt", vectorizedAdapter = LessThanI64Primitive.class)
public final class LessThanI64
{
    private LessThanI64() {}

    @ScalarImplementation
    public static boolean apply(long left, long right)
    {
        return left < right;
    }
}
