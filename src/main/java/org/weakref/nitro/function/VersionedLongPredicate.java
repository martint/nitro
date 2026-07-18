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
package org.weakref.nitro.function;

import java.util.function.LongPredicate;

/**
 * A predicate whose logical contents carry an explicit generation.
 *
 * <p>Consumers may reuse derived state only while both predicate identity and generation match. Implementations
 * must advance the generation before a changed result can be observed for any input.
 */
public interface VersionedLongPredicate
        extends LongPredicate
{
    long contentGeneration();
}
