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
package org.weakref.nitro.core.function;

/**
 * Cost hint allowing a deterministic function's result over an encoded domain to be reused across batches.
 *
 * <p>This capability does not describe or replace function semantics. The evaluator still invokes the registered
 * function over the complete domain and validates exact input contents before reuse. Providers should advertise it
 * only when avoiding another invocation is expected to repay content fingerprinting and retained-result costs.
 */
public enum EncodedDomainReuse
        implements FunctionCapability
{
    ENABLED
}
