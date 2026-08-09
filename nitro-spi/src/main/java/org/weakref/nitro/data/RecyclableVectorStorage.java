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
package org.weakref.nitro.data;

/**
 * A vector whose raw backing storage can outlive its allocator-owned wrapper.
 *
 * <p>The allocator invokes this only after the vector's ownership has ended. Implementations must release storage at
 * most once and must not publish the vector object or any logical vector state. A later borrower receives the storage
 * through an implementation-defined reset path and constructs a new vector wrapper.
 */
public interface RecyclableVectorStorage
{
    void releaseStorage(PrimitiveArrayPool storagePool);
}
