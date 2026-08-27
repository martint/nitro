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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Mask;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/// Pull-operator view of a join session whose probe input is another Nitro operator.
///
/// The session supplier may wait for shared build state before returning. It is invoked lazily from
/// [#hasNext()], so a host execution context can suspend at that point without blocking graph construction.
public final class JoinSessionOperator
        implements Operator
{
    private final Operator source;
    private final Supplier<? extends JoinSession> sessionSupplier;
    private final Schema outputSchema;
    private final Runnable afterClose;

    private JoinSession session;
    private Batch currentOutput;
    private boolean finishing;
    private boolean closed;

    public JoinSessionOperator(
            Operator source,
            Supplier<? extends JoinSession> sessionSupplier,
            Schema outputSchema)
    {
        this(source, sessionSupplier, outputSchema, () -> {});
    }

    public JoinSessionOperator(
            Operator source,
            Supplier<? extends JoinSession> sessionSupplier,
            Schema outputSchema,
            Runnable afterClose)
    {
        this.source = requireNonNull(source, "source is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
    }

    @Override
    public int outputCount()
    {
        return outputSchema.size();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public boolean hasNext()
    {
        checkOpen();
        JoinSession join = session();
        while (true) {
            if (join.hasOutput()) {
                return true;
            }
            if (finishing) {
                if (join.isFinished()) {
                    return false;
                }
                throw new IllegalStateException("join session made no progress after finish");
            }
            if (source.hasNext()) {
                join.addInput(source.next());
                continue;
            }
            finishing = true;
            join.finish();
        }
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        currentOutput = session.getOutput();
        return currentOutput;
    }

    @Override
    public void constrain(Mask mask)
    {
        requireNonNull(mask, "mask is null");
        checkOpen();
        if (currentOutput == null) {
            throw new IllegalStateException("No current batch");
        }
        currentOutput.constrain(mask);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (session != null) {
                session.close();
            }
        }
        finally {
            try {
                source.close();
            }
            finally {
                afterClose.run();
            }
        }
    }

    private JoinSession session()
    {
        if (session == null) {
            session = requireNonNull(sessionSupplier.get(), "sessionSupplier returned null");
            if (!session.outputSchema().equals(outputSchema)) {
                session.close();
                throw new IllegalArgumentException("join session output schema does not match its pull-stage contract");
            }
        }
        return session;
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("operator is closed");
        }
    }
}
