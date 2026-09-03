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
package org.weakref.nitro.core.function.aggregation;

import java.util.List;

/**
 * Allocation-free physical carrier used at an exact generated aggregation-update boundary.
 *
 * <p>A carrier is deliberately not a logical type. For example, BIGINT, short DECIMAL, REAL bits, dates, and
 * intervals may all use {@link #LONG}; only the function provider assigns logical meaning to the value. A carrier
 * may expand to more than one JVM parameter when that is the allocation-free representation of one value. In
 * particular, {@link #BINARY_REGION} expands to {@code (byte[] data, int offset, int length)}.
 */
public enum ContributionCarrier
{
    LONG(List.of(long.class)),
    DOUBLE(List.of(double.class)),
    BOOLEAN(List.of(boolean.class)),
    BINARY_REGION(List.of(byte[].class, int.class, int.class));

    private final List<Class<?>> parameterTypes;

    ContributionCarrier(List<Class<?>> parameterTypes)
    {
        this.parameterTypes = List.copyOf(parameterTypes);
    }

    public List<Class<?>> parameterTypes()
    {
        return parameterTypes;
    }
}
