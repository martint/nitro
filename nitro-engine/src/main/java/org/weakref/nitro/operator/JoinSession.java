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

import org.weakref.nitro.core.type.Schema;

/**
 * A join whose probe or outer input is scheduled incrementally by an embedding host.
 */
public interface JoinSession
        extends AutoCloseable
{
    Schema outputSchema();

    void addInput(Batch batch);

    boolean hasOutput();

    Batch getOutput();

    /**
     * Returns an output batch that may remain live while this session advances.
     *
     * <p>Immediate consumers should use {@link #getOutput()}; buffering consumers request this stronger ownership
     * contract explicitly so producers do not pay retention costs on synchronous paths.
     */
    default Batch getRetainedOutput()
    {
        return getOutput();
    }

    void finish();

    boolean isFinished();

    @Override
    void close();
}
