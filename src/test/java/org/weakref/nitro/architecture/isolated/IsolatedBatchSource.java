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
package org.weakref.nitro.architecture.isolated;

import org.weakref.nitro.core.batch.ColumnEncoding;
import org.weakref.nitro.core.batch.ColumnTraits;
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.OrdinalSourceColumnHandle;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAllocator;

import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class IsolatedBatchSource
        implements BatchSource
{
    private final Schema schema;
    private final VectorAllocator allocator;
    private final SourceColumnHandle column;
    private boolean emitted;
    private boolean closed;

    public IsolatedBatchSource(Schema schema, VectorAllocator allocator)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.column = new OrdinalSourceColumnHandle(0, schema.field(0).type());
    }

    @Override
    public Schema schema()
    {
        return schema;
    }

    @Override
    public SourceColumnHandle column(int outputIndex)
    {
        if (outputIndex != 0) {
            throw new IndexOutOfBoundsException(outputIndex);
        }
        return column;
    }

    @Override
    public Set<SourceCapability> capabilities()
    {
        return Set.of();
    }

    @Override
    public OptionalLong exactRows()
    {
        return OptionalLong.of(2);
    }

    @Override
    public SourcePoll poll()
    {
        if (emitted) {
            return SourcePoll.Finished.FINISHED;
        }
        emitted = true;
        I64Vector values = allocator.allocate(I64Vector.class, 2, I64Vector::new);
        values.values()[0] = 41;
        values.values()[1] = 42;
        return new SourcePoll.Ready(new IsolatedSourceBatch(schema, allocator, values));
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            allocator.close();
        }
    }

    public boolean closed()
    {
        return closed;
    }

    private static final class IsolatedSourceBatch
            implements SourceBatch
    {
        private final Schema schema;
        private final VectorAllocator allocator;
        private final I64Vector values;
        private Selection selection = new DenseSelection(2);
        private boolean transferred;
        private boolean closed;

        private IsolatedSourceBatch(Schema schema, VectorAllocator allocator, I64Vector values)
        {
            this.schema = schema;
            this.allocator = allocator;
            this.values = values;
        }

        @Override
        public Schema schema()
        {
            return schema;
        }

        @Override
        public Selection selection()
        {
            return selection;
        }

        @Override
        public ColumnView column(int index)
        {
            if (index != 0) {
                throw new IndexOutOfBoundsException(index);
            }
            return new IsolatedColumnView(schema, allocator, values, this);
        }

        @Override
        public void select(Selection selection)
        {
            requireNonNull(selection, "selection is null");
            if (selection.positionCount() != 2) {
                throw new IllegalArgumentException("selection position count differs");
            }
            this.selection = selection;
        }

        @Override
        public void close()
        {
            if (!closed) {
                closed = true;
                if (!transferred) {
                    allocator.release(values);
                }
            }
        }
    }

    private record DenseSelection(int positionCount)
            implements Selection
    {
        @Override
        public int count()
        {
            return positionCount;
        }

        @Override
        public int maxPosition()
        {
            return positionCount - 1;
        }

        @Override
        public boolean isDense()
        {
            return true;
        }

        @Override
        public int position(int index)
        {
            if (index < 0 || index >= positionCount) {
                throw new IndexOutOfBoundsException(index);
            }
            return index;
        }
    }

    private record IsolatedColumnView(
            Schema schema,
            VectorAllocator allocator,
            I64Vector values,
            IsolatedSourceBatch owner)
            implements ColumnView
    {
        @Override
        public org.weakref.nitro.core.type.TypeBinding type()
        {
            return schema.field(0).type();
        }

        @Override
        public int positionCount()
        {
            return values.length();
        }

        @Override
        public ColumnTraits traits()
        {
            return new ColumnTraits(ColumnEncoding.FLAT, false, false, false);
        }

        @Override
        public Set<Stream> streams()
        {
            return Set.of(Stream.VALUES);
        }

        @Override
        public Vector borrow(Stream stream)
        {
            requireValues(stream);
            return values;
        }

        @Override
        public Vector take(Stream stream)
        {
            requireValues(stream);
            if (!owner.transferred) {
                owner.transferred = true;
                return allocator.transfer(values);
            }
            return values;
        }

        private static void requireValues(Stream stream)
        {
            if (stream != Stream.VALUES) {
                throw new IllegalArgumentException("unsupported stream: " + stream);
            }
        }
    }
}
