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
 * Structural vector whose logical rows own variable-length ranges in one or more aligned child streams.
 *
 * <p>This is the SPI shape consumed by generic expansion operators. Arrays expose one child, maps expose two, and
 * connector-provided structural vectors can participate without teaching the execution engine their logical type.
 */
public interface RepeatedVector
        extends FlatVector
{
    /** Monotonic child-range boundaries; logical position {@code p} owns {@code [offsets[p], offsets[p + 1])}. */
    int[] offsets();

    int startOffset(int position);

    int endOffset(int position);

    default int repeatedLength(int position)
    {
        return endOffset(position) - startOffset(position);
    }

    int repeatedOutputCount();

    Streams repeatedOutput(int output);

    /** Returns whether this vector intentionally carries only row offsets and no child values. */
    default boolean structureOnly()
    {
        for (int output = 0; output < repeatedOutputCount(); output++) {
            if (repeatedOutput(output).hasValues()) {
                return false;
            }
        }
        return true;
    }
}
