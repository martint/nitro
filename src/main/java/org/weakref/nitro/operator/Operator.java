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
import org.weakref.nitro.data.Vector;

public interface Operator
        extends AutoCloseable
{
    int columnCount();

    boolean hasNext();

    /**
     * Advance to the next batch of rows. Invalidates any previously returned blocks and
     * active positions.
     *
     * @return positions of active rows in the current batch.
     * @throws IllegalStateException if the operator is finished
     */
    Mask next();

    /**
     * Indicate to the operator that the caller is only interested
     * in a subset of the rows.
     * <p>
     * This can be useful if a downstream operator decides that certain
     * rows are no longer of interest after processing data from a set
     * of columns. The operator can leverage this information in subsequent
     * calls to column() to avoid unnecessary work.
     */
    void constrain(Mask mask);

    Vector column(int column);

    void close();
}
