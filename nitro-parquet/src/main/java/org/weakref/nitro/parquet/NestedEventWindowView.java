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

/** Repetition/definition-level view shared by primitive and aligned multi-leaf nested readers. */
interface NestedEventWindowView
{
    int length();

    int repetitionLevel(int index);

    int definitionLevel(int index);

    boolean allDefinitionLevelsAtLeast(int count, int minimum);

    default boolean allDefinitionLevelsAtLeast(int offset, int count, int minimum)
    {
        if (offset < 0 || count < 0 || offset > length() - count) {
            throw new IndexOutOfBoundsException("Invalid nested event subwindow: " + offset + ", " + count);
        }
        for (int index = offset; index < offset + count; index++) {
            if (definitionLevel(index) < minimum) {
                return false;
            }
        }
        return true;
    }
}
