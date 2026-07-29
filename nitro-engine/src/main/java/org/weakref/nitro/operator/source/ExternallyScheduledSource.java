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
package org.weakref.nitro.operator.source;

import org.weakref.nitro.operator.Operator;

/**
 * A pull source whose temporary lack of output is distinct from permanent exhaustion.
 *
 * <p>An external scheduler offers input generations independently. Consumers may retain
 * instance-owned execution state while {@link #hasNext()} is false and {@link #isFinished()}
 * is also false.
 */
public interface ExternallyScheduledSource
        extends Operator
{
    boolean isFinished();
}
