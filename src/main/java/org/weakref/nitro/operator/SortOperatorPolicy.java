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

/// Engine-selected physical buffering policy for full sorts.
///
/// The property-backed factory is a standalone composition adapter. Sort operators receive one immutable policy
/// from their resource owner and never consult process-global configuration.
public record SortOperatorPolicy(boolean columnarBuffer, int columnarBufferMaxColumns)
{
    public static SortOperatorPolicy defaults()
    {
        return new SortOperatorPolicy(true, 8);
    }

    public static SortOperatorPolicy fromSystemProperties()
    {
        SortOperatorPolicy defaults = defaults();
        return new SortOperatorPolicy(
                Boolean.parseBoolean(System.getProperty(
                        "nitro.sort.columnarBuffer",
                        Boolean.toString(defaults.columnarBuffer()))),
                Integer.getInteger(
                        "nitro.sort.columnarBufferMaxColumns",
                        defaults.columnarBufferMaxColumns()));
    }
}
