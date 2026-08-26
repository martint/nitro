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
package org.weakref.nitro.parquet;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Seekable Parquet input supplied by the embedding connector.
 *
 * <p>Returned ranges remain valid until this input is closed. Implementations may return zero-copy slices of a
 * local mapping, storage-cache leases, or connector-owned buffers populated by object-store range reads. Nitro owns
 * the input after handing it to a scan and never assumes that it has a local filesystem path. Closing an input must
 * be idempotent so partially constructed scans can release every transferred input.
 */
public interface ParquetInput
        extends AutoCloseable
{
    String id();

    long size();

    MemorySegment readRange(long offset, int length)
            throws IOException;

    @Override
    void close()
            throws IOException;
}
