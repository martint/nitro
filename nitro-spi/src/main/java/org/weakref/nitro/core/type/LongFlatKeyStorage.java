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
package org.weakref.nitro.core.type;

/**
 * Provider-owned canonical flat storage for a logical type carried as a {@code long}.
 *
 * <p>The contract covers the complete logical domain, independently of the physical vector used by any batch.
 * Implementations must reject values outside that domain rather than truncate them. Operators select this capability
 * without inspecting a logical type identity or guessing from a physical vector class.
 */
public interface LongFlatKeyStorage
{
    int fixedSize();

    void write(byte[] target, int offset, long value);

    long read(byte[] source, int offset);
}
