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

/**
 * Classloader-safe physical invocation convention for a generated grouped-aggregation state update.
 *
 * <p>The engine knows only this SPI carrier. A dynamically loaded aggregate provider may implement it on an
 * otherwise opaque state object; generated engine code does not reference the provider's state class.
 */
public interface LongStateUpdate
{
    void update(int group, long value);
}
