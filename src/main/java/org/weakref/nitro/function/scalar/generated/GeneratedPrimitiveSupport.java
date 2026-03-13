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
package org.weakref.nitro.function.scalar.generated;

import static com.google.common.base.Preconditions.checkArgument;

final class GeneratedPrimitiveSupport
{
    private GeneratedPrimitiveSupport() {}

    static void verifyBinaryArity(String functionName, int argumentCount)
    {
        checkArgument(argumentCount == 2, "Unexpected argument count for %s", functionName);
    }
}
