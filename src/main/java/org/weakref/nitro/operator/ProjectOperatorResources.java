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
 * Engine-owner-scoped compatibility domains used by projection evaluation.
 *
 * <p>The token carries no storage itself. An allocator uses its identity to let closed evaluator generations recycle
 * compatible buffers without giving one generation ownership of another generation's live storage.
 */
public final class ProjectOperatorResources
{
    private final Object evaluatorBufferPoolGroup = new Object();

    Object evaluatorBufferPoolGroup()
    {
        return evaluatorBufferPoolGroup;
    }
}
