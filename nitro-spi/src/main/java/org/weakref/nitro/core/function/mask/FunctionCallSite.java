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
package org.weakref.nitro.core.function.mask;

import org.weakref.nitro.core.function.FunctionCapability;

import java.util.Optional;

/**
 * Read-only, classloader-neutral view of a bound function expression.
 */
public interface FunctionCallSite
{
    int argumentCount();

    Argument argument(int index);

    <T extends FunctionCapability> Optional<T> capability(Class<T> capabilityType);

    interface Argument
    {
        Optional<Object> literal();

        Optional<FunctionCallSite> call();
    }
}
