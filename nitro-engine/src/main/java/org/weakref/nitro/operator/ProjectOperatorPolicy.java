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

/**
 * Immutable engine-owner policy for projection execution.
 *
 * <p>Embedding engines construct this value directly. System-property parsing is retained only as a standalone
 * composition adapter and is never performed by an operator.
 */
public record ProjectOperatorPolicy(
        boolean shareEvaluatorBufferPool,
        boolean forwardSinglePositionOnly,
        boolean recycleEvaluatorOutputs,
        boolean reusePlanEvaluator,
        boolean compileExpressions)
{
    public static ProjectOperatorPolicy defaults()
    {
        return new ProjectOperatorPolicy(true, false, true, true, true);
    }

    public static ProjectOperatorPolicy fromSystemProperties()
    {
        ProjectOperatorPolicy defaults = defaults();
        return new ProjectOperatorPolicy(
                Boolean.parseBoolean(System.getProperty(
                        "nitro.project.shareEvaluatorBufferPool",
                        Boolean.toString(defaults.shareEvaluatorBufferPool()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.project.forwardSinglePositionOnly",
                        Boolean.toString(defaults.forwardSinglePositionOnly()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.project.recycleEvaluatorOutputs",
                        Boolean.toString(defaults.recycleEvaluatorOutputs()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.project.reusePlanEvaluator",
                        Boolean.toString(defaults.reusePlanEvaluator()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.project.compileExpressions",
                        Boolean.toString(defaults.compileExpressions()))));
    }
}
