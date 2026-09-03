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
package org.weakref.nitro.function.scalar;

import org.weakref.nitro.core.function.ScalarFailureMapper;
import org.weakref.nitro.data.ErrorVector;
import org.weakref.nitro.data.Mask;

public interface GeneratedScalarKernel
{
    void applyDenseFlatNullFree(Object[] values, Object output, ErrorVector errors, ScalarFailureMapper failureMapper, int count);

    void applySparseFlatNullFree(Object[] values, Object output, ErrorVector errors, ScalarFailureMapper failureMapper, int[] positions, int count);

    void applyDenseDictionaryNullFree(Object[] values, int[][] ids, Object output, ErrorVector errors, ScalarFailureMapper failureMapper, int count);

    void applySparseDictionaryNullFree(Object[] values, int[][] ids, Object output, ErrorVector errors, ScalarFailureMapper failureMapper, int[] positions, int count);

    void applyDense(Object[] values, Object[] nulls, Object output, ErrorVector errors, ScalarFailureMapper failureMapper, int count);

    void applySparse(Object[] values, Object[] nulls, Object output, ErrorVector errors, ScalarFailureMapper failureMapper, int[] positions, int count);

    default boolean applyMask(Object[] values, Object[] blockers, Mask mask, boolean selectedValue)
    {
        return false;
    }
}
