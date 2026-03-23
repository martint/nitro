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

import org.weakref.nitro.data.Mask;

/**
 * Pull-based batch operator.
 * <p>
 * Operators expose batches one at a time through the {@link #hasNext()}/{@link #next()} contract.
 * The normal usage pattern is:
 * <pre>{@code
 * while (operator.hasNext()) {
 *     try (Batch batch = operator.next()) {
 *         ...
 *     }
 * }
 * }</pre>
 * {@link #hasNext()} may prefetch internally, so {@link #next()} must return the same staged batch
 * rather than advancing the source again.
 */
public interface Operator
        extends AutoCloseable
{
    /**
     * Returns the number of logical outputs this operator exposes in each batch.
     */
    int outputCount();

    /**
     * Returns {@code true} if another batch is available.
     */
    boolean hasNext();

    /**
     * Returns the next batch.
     * <p>
     * The caller becomes responsible for closing the batch.
     */
    Batch next();

    /**
     * Narrows the current pending batch to {@code mask}.
     * <p>
     * Operators may use this to avoid materializing work for rows that a downstream consumer has
     * already discarded.
     */
    void constrain(Mask mask);

    /**
     * Returns whether batches from this operator may outlive calls that advance the operator.
     * <p>
     * If this is {@code false}, downstream operators must finish consuming or copying any required
     * data from the current batch before asking the source for another one.
     */
    default boolean supportsRetainedBatches()
    {
        return false;
    }

    /**
     * Releases any operator-owned resources.
     */
    void close();
}
