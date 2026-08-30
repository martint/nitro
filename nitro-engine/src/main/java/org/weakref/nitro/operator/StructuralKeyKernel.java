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

import org.weakref.nitro.data.Vector;

interface StructuralKeyKernel
        extends StructuralIdentityKernel
{
    interface Bound
    {
        long hash(int position);

        boolean identical(int position, Bound other, int otherPosition);
    }

    default boolean allowsLegacyPhysicalShortcuts()
    {
        return false;
    }

    long hash(Vector values, Vector nulls, int position);

    default Bound bind(Vector values)
    {
        return new BoundVector(this, values);
    }

    final class BoundVector
            implements Bound
    {
        private final StructuralKeyKernel kernel;
        private final Vector values;

        private BoundVector(StructuralKeyKernel kernel, Vector values)
        {
            this.kernel = kernel;
            this.values = values;
        }

        @Override
        public long hash(int position)
        {
            return kernel.hash(values, null, position);
        }

        @Override
        public boolean identical(int position, Bound other, int otherPosition)
        {
            if (!(other instanceof BoundVector right) || kernel != right.kernel) {
                throw new IllegalArgumentException("bound key was created by a different kernel");
            }
            return kernel.identical(values, null, position, right.values, null, otherPosition);
        }
    }
}
