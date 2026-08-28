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

/// Engine-selected physical execution policy for window operators.
///
/// The property-backed factory is a standalone composition adapter. Window operators receive one immutable policy
/// from their resource owner and never consult process-global configuration.
public record WindowOperatorPolicy(
        int maxBatchRows,
        int maxCoalescedRows,
        boolean radixSort,
        boolean fusedFunctions,
        boolean binaryHashPartitionSort,
        boolean lazyOutputs,
        boolean reuseOrderedInput,
        int reuseOrderedInputMinRows,
        int reuseOrderedInputSamples,
        boolean debugReuseOrderedInput)
{
    public WindowOperatorPolicy
    {
        if (maxBatchRows <= 0) {
            throw new IllegalArgumentException("maxBatchRows must be positive");
        }
        if (maxCoalescedRows <= 0) {
            throw new IllegalArgumentException("maxCoalescedRows must be positive");
        }
    }

    public static WindowOperatorPolicy defaults()
    {
        return new WindowOperatorPolicy(
                4_096,
                16_777_216,
                true,
                true,
                true,
                true,
                true,
                1_000_000,
                64,
                false);
    }

    public static WindowOperatorPolicy fromSystemProperties()
    {
        WindowOperatorPolicy defaults = defaults();
        return new WindowOperatorPolicy(
                Integer.getInteger("nitro.window.maxBatchRows", defaults.maxBatchRows()),
                Integer.getInteger("nitro.window.maxCoalescedRows", defaults.maxCoalescedRows()),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.window.radixSort",
                        Boolean.toString(defaults.radixSort()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.window.fusedFunctions",
                        Boolean.toString(defaults.fusedFunctions()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.window.binaryHashPartitionSort",
                        Boolean.toString(defaults.binaryHashPartitionSort()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.window.lazyOutputs",
                        Boolean.toString(defaults.lazyOutputs()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.window.reuseOrderedInput",
                        Boolean.toString(defaults.reuseOrderedInput()))),
                Integer.getInteger(
                        "nitro.window.reuseOrderedInputMinRows",
                        defaults.reuseOrderedInputMinRows()),
                Integer.getInteger(
                        "nitro.window.reuseOrderedInputSamples",
                        defaults.reuseOrderedInputSamples()),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.debug.windowReuseOrderedInput",
                        Boolean.toString(defaults.debugReuseOrderedInput()))));
    }
}
