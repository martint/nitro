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
package org.weakref.nitro.core.function.table;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;

/// One table-function processing result.
public sealed interface TableFunctionProgress
        permits TableFunctionProgress.Blocked, TableFunctionProgress.Consumed, TableFunctionProgress.Finished, TableFunctionProgress.Produced
{
    /// Transfers [output] to the caller. Each listed argument was consumed completely and is not offered again.
    /// An empty set permits a processor to produce more than one output batch from the same input tuple.
    record Produced(TableFunctionOutputBatch output, Set<Integer> consumedArguments)
            implements TableFunctionProgress
    {
        public Produced
        {
            output = requireNonNull(output, "output is null");
            consumedArguments = copyArgumentSet(consumedArguments);
        }
    }

    record Consumed(Set<Integer> arguments)
            implements TableFunctionProgress
    {
        public Consumed
        {
            arguments = copyArgumentSet(arguments);
            if (arguments.isEmpty()) {
                throw new IllegalArgumentException("no arguments were consumed");
            }
        }
    }

    /// No input or output ownership changes while blocked.
    record Blocked(CompletionStage<Void> continuation)
            implements TableFunctionProgress
    {
        public Blocked
        {
            continuation = requireNonNull(continuation, "continuation is null");
        }
    }

    enum Finished
            implements TableFunctionProgress
    {
        FINISHED
    }

    private static Set<Integer> copyArgumentSet(Set<Integer> arguments)
    {
        Set<Integer> copy = Set.copyOf(arguments);
        if (copy.stream().anyMatch(argument -> argument < 0)) {
            throw new IllegalArgumentException("consumed argument is negative");
        }
        return copy;
    }
}
