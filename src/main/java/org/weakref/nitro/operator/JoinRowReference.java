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

final class JoinRowReference
{
    private JoinRowReference() {}

    static long pack(int batchIndex, int position)
    {
        return ((long) batchIndex << Integer.SIZE) | (position & 0xFFFF_FFFFL);
    }

    static int batchIndex(long rowReference)
    {
        return (int) (rowReference >>> Integer.SIZE);
    }

    static int position(long rowReference)
    {
        return (int) rowReference;
    }

    static long unpackCompact(int rowReference)
    {
        return pack(rowReference >>> Short.SIZE, rowReference & 0xFFFF);
    }
}
