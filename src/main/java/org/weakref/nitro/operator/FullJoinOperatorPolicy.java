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

/**
 * Engine-selected physical policies for full joins.
 */
public record FullJoinOperatorPolicy(boolean retainInputBatches)
{
    public static FullJoinOperatorPolicy defaults()
    {
        return new FullJoinOperatorPolicy(true);
    }

    public static FullJoinOperatorPolicy fromSystemProperties()
    {
        return new FullJoinOperatorPolicy(Boolean.parseBoolean(System.getProperty(
                "nitro.fullJoin.retainInputBatches",
                Boolean.toString(defaults().retainInputBatches()))));
    }
}
