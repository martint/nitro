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

/**
 * Describes a boolean result that is exactly one physical component of one argument.
 *
 * <p>This is immutable resolved-function metadata. It deliberately does not expose evaluator references, vectors,
 * masks, or execution contexts, so a provider loaded in an isolated class loader can publish the capability through
 * the shared function SPI.
 */
public interface DirectMaskInputProvider
        extends FunctionCapability
{
    int argumentCount();

    int argumentIndex();

    InputComponent inputComponent();

    enum InputComponent
    {
        VALUES,
        NULLS,
        ERRORS
    }
}
