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

public record TopNOperatorPolicy(int columnarOrderingMinLimit)
{
    public TopNOperatorPolicy
    {
        if (columnarOrderingMinLimit < 1) {
            throw new IllegalArgumentException("columnarOrderingMinLimit must be positive");
        }
    }

    public static TopNOperatorPolicy defaults()
    {
        return new TopNOperatorPolicy(4_096);
    }

    public static TopNOperatorPolicy fromSystemProperties()
    {
        TopNOperatorPolicy defaults = defaults();
        return new TopNOperatorPolicy(Integer.getInteger(
                "nitro.topN.columnarOrderingMinLimit",
                defaults.columnarOrderingMinLimit()));
    }
}
