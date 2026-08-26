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

/** Physical decoder whose logical carrier is a boolean. */
interface BooleanValueDecoder
        extends PhysicalValueDecoder
{
    boolean value(int ordinal);

    default void copyPlain(int ordinal, boolean[] output, int outputOffset, int count)
    {
        for (int index = 0; index < count; index++) {
            output[outputOffset + index] = value(ordinal + index);
        }
    }
}
