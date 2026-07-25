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
package org.weakref.nitro.operator.source.compatibility;

import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.RuntimeFilter;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.source.SourceOperatorIngress;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// Zero-copy ingress for the transitional native source facades.
public final class NativeSourceOperatorIngress
        implements SourceOperatorIngress
{
    @Override
    public Optional<Operator> directOperator(BatchSource source)
    {
        requireNonNull(source, "source is null");
        return source.protocol(NativeOperatorProtocol.NATIVE_OPERATOR)
                .map(NativeOperatorAccess::operator);
    }

    @Override
    public Batch adapt(SourceBatch batch)
    {
        requireNonNull(batch, "batch is null");
        NativeBatchAccess nativeBatch = batch.capability(NativeBatchCapability.NATIVE_BATCH)
                .orElseThrow(() -> new IllegalArgumentException("source does not expose a native batch"));
        return nativeBatch.transfer();
    }

    @Override
    public Selection selection(Mask mask)
    {
        return new MaskSelection(requireNonNull(mask, "mask is null"));
    }

    @Override
    public boolean supportsRuntimeFilter(BatchSource source, SourceColumnHandle column)
    {
        requireNonNull(source, "source is null");
        requireNonNull(column, "column is null");
        return source.protocol(NativeOperatorProtocol.NATIVE_OPERATOR).isPresent();
    }

    @Override
    public RuntimeFilter runtimeFilter(SourceColumnHandle column, DynamicFilter filter)
    {
        requireNonNull(column, "column is null");
        requireNonNull(filter, "filter is null");
        return new RuntimeFilter(
                column,
                new NativeRuntimeFilterDomain(column.type(), filter),
                false);
    }
}
