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

import static java.util.Objects.requireNonNull;

/**
 * Explicit physical-plan contract for a position-aligned key hash input.
 *
 * <p>The planner is responsible for proving that every producer and consumer carrying the same contract identifier
 * uses identical key order, equality, null, and hash semantics. Nitro still compares complete keys after a hash
 * match, but equal keys must carry equal hashes so they enter the same probe sequence. The identifier is diagnostic
 * and must describe a stable, versioned physical convention; it is not interpreted by operators.
 */
public record AuthoritativeHashChannel(String contractIdentifier, int inputChannel)
{
    public AuthoritativeHashChannel
    {
        contractIdentifier = requireNonNull(contractIdentifier, "contractIdentifier is null");
        if (contractIdentifier.isBlank()) {
            throw new IllegalArgumentException("contractIdentifier is blank");
        }
        if (inputChannel < 0) {
            throw new IllegalArgumentException("inputChannel is negative");
        }
    }
}
