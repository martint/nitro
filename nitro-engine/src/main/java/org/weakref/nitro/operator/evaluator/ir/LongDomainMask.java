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
package org.weakref.nitro.operator.evaluator.ir;

import org.weakref.nitro.core.source.LongDomain;

import static java.util.Objects.requireNonNull;

/** A registry-lowered exact long domain, evaluated once per physical dictionary entry when encoded. */
public record LongDomainMask(Reference input, LongDomain domain)
        implements MaskExpression
{
    public LongDomainMask
    {
        requireNonNull(input, "input is null");
        requireNonNull(domain, "domain is null");
    }
}
