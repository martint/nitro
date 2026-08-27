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
 * Amount of physical value content required by a consumer.
 *
 * <p>{@link #STRUCTURE} retains row-level nullability and the offsets of a repeated value, but does not require its
 * child values. Sources which cannot expose that representation may conservatively provide {@link #FULL} values.
 * {@link #FULL_WITH_DOMAIN_COUNTS} additionally requests exact logical multiplicities for an encoded physical
 * domain when the source can provide them. A source may conservatively provide ordinary {@link #FULL} values when
 * the encoding or its admission policy cannot satisfy that request.
 */
public enum ValueDemand
{
    STRUCTURE,
    FULL,
    FULL_WITH_DOMAIN_COUNTS;

    public ValueDemand merge(ValueDemand other)
    {
        return ordinal() >= other.ordinal() ? this : other;
    }
}
