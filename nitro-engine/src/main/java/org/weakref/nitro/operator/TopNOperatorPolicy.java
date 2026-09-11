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

public record TopNOperatorPolicy(
        int columnarOrderingMinLimit,
        int variableWidthColumnarOrderingMinLimit,
        int mixedFixedAndVariableWidthColumnarOrderingMinLimit,
        int hybridColumnarOrderingMinLimit,
        int retainedSingleBatchMinimumRowsPerLimit)
{
    public TopNOperatorPolicy(int columnarOrderingMinLimit)
    {
        this(columnarOrderingMinLimit, columnarOrderingMinLimit, columnarOrderingMinLimit, columnarOrderingMinLimit, 64);
    }

    public TopNOperatorPolicy(int columnarOrderingMinLimit, int retainedSingleBatchMinimumRowsPerLimit)
    {
        this(
                columnarOrderingMinLimit,
                columnarOrderingMinLimit,
                columnarOrderingMinLimit,
                columnarOrderingMinLimit,
                retainedSingleBatchMinimumRowsPerLimit);
    }

    public TopNOperatorPolicy(
            int columnarOrderingMinLimit,
            int variableWidthColumnarOrderingMinLimit,
            int retainedSingleBatchMinimumRowsPerLimit)
    {
        this(
                columnarOrderingMinLimit,
                variableWidthColumnarOrderingMinLimit,
                variableWidthColumnarOrderingMinLimit,
                variableWidthColumnarOrderingMinLimit,
                retainedSingleBatchMinimumRowsPerLimit);
    }

    public TopNOperatorPolicy(
            int columnarOrderingMinLimit,
            int variableWidthColumnarOrderingMinLimit,
            int hybridColumnarOrderingMinLimit,
            int retainedSingleBatchMinimumRowsPerLimit)
    {
        this(
                columnarOrderingMinLimit,
                variableWidthColumnarOrderingMinLimit,
                variableWidthColumnarOrderingMinLimit,
                hybridColumnarOrderingMinLimit,
                retainedSingleBatchMinimumRowsPerLimit);
    }

    public TopNOperatorPolicy
    {
        if (columnarOrderingMinLimit < 1) {
            throw new IllegalArgumentException("columnarOrderingMinLimit must be positive");
        }
        if (variableWidthColumnarOrderingMinLimit < 1) {
            throw new IllegalArgumentException("variableWidthColumnarOrderingMinLimit must be positive");
        }
        if (mixedFixedAndVariableWidthColumnarOrderingMinLimit < 1) {
            throw new IllegalArgumentException("mixedFixedAndVariableWidthColumnarOrderingMinLimit must be positive");
        }
        if (hybridColumnarOrderingMinLimit < 1) {
            throw new IllegalArgumentException("hybridColumnarOrderingMinLimit must be positive");
        }
        if (retainedSingleBatchMinimumRowsPerLimit < 1) {
            throw new IllegalArgumentException("retainedSingleBatchMinimumRowsPerLimit must be positive");
        }
    }

    public static TopNOperatorPolicy defaults()
    {
        return new TopNOperatorPolicy(4_096, 1, 512, 64, 64);
    }

    public static TopNOperatorPolicy fromSystemProperties()
    {
        TopNOperatorPolicy defaults = defaults();
        return new TopNOperatorPolicy(
                Integer.getInteger(
                        "nitro.topN.columnarOrderingMinLimit",
                        defaults.columnarOrderingMinLimit()),
                Integer.getInteger(
                        "nitro.topN.variableWidthColumnarOrderingMinLimit",
                        defaults.variableWidthColumnarOrderingMinLimit()),
                Integer.getInteger(
                        "nitro.topN.mixedFixedAndVariableWidthColumnarOrderingMinLimit",
                        defaults.mixedFixedAndVariableWidthColumnarOrderingMinLimit()),
                Integer.getInteger(
                        "nitro.topN.hybridColumnarOrderingMinLimit",
                        defaults.hybridColumnarOrderingMinLimit()),
                Integer.getInteger(
                        "nitro.topN.retainedSingleBatchMinimumRowsPerLimit",
                        defaults.retainedSingleBatchMinimumRowsPerLimit()));
    }
}
