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
package org.weakref.nitro.core.batch;

/// Immutable logical row selection for a source batch.
public interface Selection
{
    /// Number of physical row positions in the source generation.
    int positionCount();

    /// Number of selected positions.
    int count();

    /// Greatest selected position, or {@code -1} when the selection is empty.
    int maxPosition();

    /// Whether every physical row position is selected.
    boolean isDense();

    int position(int index);
}
