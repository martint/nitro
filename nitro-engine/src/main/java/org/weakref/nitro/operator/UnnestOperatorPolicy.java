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

import static com.google.common.base.Preconditions.checkArgument;

/** Immutable, composition-owned output sizing for repeated-value expansion. */
public record UnnestOperatorPolicy(int maxRowsPerBatch)
{
    public UnnestOperatorPolicy
    {
        checkArgument(maxRowsPerBatch > 0, "maxRowsPerBatch must be positive");
    }

    public static UnnestOperatorPolicy defaults()
    {
        return new UnnestOperatorPolicy(65_536);
    }

    /** Standalone composition adapter; production operators receive the resulting immutable instance. */
    public static UnnestOperatorPolicy fromSystemProperties()
    {
        return new UnnestOperatorPolicy(Integer.getInteger("nitro.unnest.maxRowsPerBatch", 65_536));
    }
}
